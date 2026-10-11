package ingress_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
)

func (f *fixture) output(t testing.TB, origin, sequence uint64) []byte {
	h := f.header(origin)
	h.Kind, h.Codec, h.Surface = wire.SourceOutput, wire.NV12, wire.Delivered
	h.Stream, h.OutputSequence = "analysis", sequence
	h.Width, h.Height, h.StrideY, h.StrideUV = 8, 8, 8, 8
	h.Transform = f.tr.Digest()
	if origin == 0 {
		h.Correlation = 0
	}
	return encode(t, h, bytes.Repeat([]byte{byte(origin)}, 96))
}

func TestSourceOutputFailureAttributesBindingAndConsumerWithoutRetry(t *testing.T) {
	f := setup(t)
	packet := f.output(t, 147, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	cause := errors.New("fixture consumer failure")
	err := f.i.AcceptOutput(ctx, packet, func(context.Context, wire.Header, []byte) error {
		calls++
		return cause
	})
	if !errors.Is(err, cause) || calls != 1 {
		t.Fatalf("diagnostic changed failure or retried: %v calls=%d", err, calls)
	}
	var failure *in.Failure
	if !errors.As(err, &failure) || failure.Stage != "output_consumer" {
		t.Fatal("original failure stage lost", err)
	}
	for _, field := range []string{"output_commit_progress phase=consumer", "origin_sequence=147", "ticket_us=", "binding_us=", "validate_before_us=", "consumer_us=", "validate_after_us=-1", "elapsed_us=", "budget_us="} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("missing attribution %q: %v", field, err)
		}
	}
	err = f.i.AcceptOutput(ctx, packet, func(context.Context, wire.Header, []byte) error { calls++; return nil })
	if err == nil || calls != 1 {
		t.Fatal("failed output became replayable")
	}
}

func TestSourceOutputIsRetentionIndependent(t *testing.T) {
	for _, mode := range []string{"missing", "expired"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			if mode == "expired" {
				must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 7), 100, 200))
				f.c.now.Store(1200)
			}
			s := sourceConnection(t, f.output(t, 7, 1))
			r := receiveSource(t, f, s)
			calls := 0
			must(t, r.Receive(context.Background(), func(ctx context.Context, h wire.Header, pixels []byte) error {
				calls++
				if h.Kind != wire.Delivery || h.Correlation != 1 || h.OriginSequence != 7 || h.DeliveryID != 1 || !bytes.Equal(pixels, bytes.Repeat([]byte{7}, 96)) {
					t.Fatal("real analysis delivery was changed", h)
				}
				ref := fd.DeliveryRef{Fence: f.b.Fence(), Ref: fa.DeliveryRef{Stream: h.Stream, Epoch: h.PublicationEpoch, FrameID: h.DeliveryID, Graph: h.Graph}}
				result, err := f.r.Extract(ctx, fd.Request{ID: [16]byte{1}, Frame: ref, Source: h.Source, Surface: "processed_full", Regions: []fa.Region{{ID: "full", FullFrame: true}}, DeadlineNS: f.c.NowNS() + 100})
				want(t, err, fa.Expired)
				if result != nil {
					t.Fatal("missing native pixels became an artifact")
				}
				return nil
			}))
			if calls != 1 || r.Stats().Outputs != 1 || r.Stats().Failures != 0 || r.Stats().Drops != 0 {
				t.Fatal("valid analysis output was dropped", calls, r.Stats())
			}
		})
	}
}

func TestSourceOutputBecomesExactBrokerDelivery(t *testing.T) {
	f := setup(t)
	for _, origin := range []uint64{2, 1} {
		must(t, f.i.AcceptSurface(context.Background(), f.surface(t, origin), 100, 200))
	}
	var seen []wire.Header
	consume := func(ctx context.Context, h wire.Header, p []byte) error {
		if h.Kind != wire.Delivery || h.OutputSequence != 0 || h.PublicationEpoch == 0 || h.DeliveryID != uint64(len(seen)+1) || h.Transform != f.tr.Digest() || !bytes.Equal(p, bytes.Repeat([]byte{byte(h.OriginSequence)}, 96)) {
			t.Fatal("delivery authority or pixels changed", h)
		}
		ref := fd.DeliveryRef{Fence: f.b.Fence(), Ref: fa.DeliveryRef{Stream: h.Stream, Epoch: h.PublicationEpoch, FrameID: h.DeliveryID, Graph: h.Graph}}
		result, e := f.r.Extract(ctx, fd.Request{ID: [16]byte{1}, Frame: ref, Source: h.Source, Surface: "processed_full", Regions: []fa.Region{{ID: "full", FullFrame: true}}, DeadlineNS: 200})
		if h.Correlation == 0 {
			want(t, e, fa.CorrelationUnavailable)
			if result != nil {
				t.Fatal("unknown origin produced a result")
			}
			seen = append(seen, h)
			return nil
		}
		must(t, e)
		defer result.Close()
		must(t, result.Visit(0, func(a fd.Artifact, p []byte) error {
			if a.State != fa.Complete || a.Proof.Core.Origin.Sequence != h.OriginSequence || !bytes.Equal(p, pixels(h.OriginSequence)) {
				t.Fatal("crop did not use exact retained origin", a.State)
			}
			return nil
		}))
		seen = append(seen, h)
		return nil
	}
	// Reordered origins do not reorder the independently broker-minted IDs.
	for j, origin := range []uint64{2, 1, 0} {
		must(t, f.i.AcceptOutput(context.Background(), f.output(t, origin, uint64(j+7)), consume))
	}
	if len(seen) != 3 {
		t.Fatal("missing output")
	}
	for _, seq := range []uint64{9, 8, 7} {
		want(t, f.i.AcceptOutput(context.Background(), f.output(t, 1, seq), consume), fa.InvalidContract)
	}
	if len(seen) != 3 {
		t.Fatal("native replay delivered twice")
	}
}

func TestSourceOutputRejectsForeignBeforePayload(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*wire.Header)
	}{
		{"source", func(h *wire.Header) { h.Source = "rear" }},
		{"epoch", func(h *wire.Header) { h.SourceEpoch++ }},
		{"process", func(h *wire.Header) { h.Process = "other" }},
		{"namespace", func(h *wire.Header) { h.Namespace[0] ^= 1 }},
		{"graph", func(h *wire.Header) { h.Graph[0] ^= 2 }},
		{"stream", func(h *wire.Header) { h.Stream = "foreign" }},
		{"transform", func(h *wire.Header) { h.Transform[0] ^= 1 }},
		{"geometry", func(h *wire.Header) { h.Width = 4; h.Height = 16; h.StrideY = 4; h.StrideUV = 4 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setup(t)
			s := sourceConnection(t, mutate(t, f.output(t, 1, 1), tc.change))
			r := receiveSource(t, f, s)
			if e := r.Receive(context.Background(), func(context.Context, wire.Header, []byte) error { t.Fatal("foreign output delivered"); return nil }); e == nil {
				t.Fatal("foreign output accepted")
			}
			if s.reads.Load() != 1 || r.Stats().Outputs != 0 {
				t.Fatal("foreign output payload consumed")
			}
			if f.ticket(t).Ref().Ref.FrameID != 1 {
				t.Fatal("foreign output consumed broker ID")
			}
			must(t, r.WaitClosed(context.Background()))
		})
	}
}

func TestSourceOutputConsumerRetirementCannotBecomeSuccess(t *testing.T) {
	f := setup(t)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	err := f.i.AcceptOutput(context.Background(), f.output(t, 1, 1),
		func(context.Context, wire.Header, []byte) error {
			f.b.Close()
			return nil
		})
	want(t, err, fd.ErrRetired)
}

func TestSourceOutputNoPublicationAfterFailure(t *testing.T) {
	// Missing/expired native pixels are covered separately: they are not an
	// invalid analysis output. All transport, authority and consumer failures
	// remain terminal below, with no weakened deadline or replay handling.
	for _, mode := range []string{"hash", "partial", "nil_consumer", "cancelled", "retired", "consumer_failed", "consumer_capacity"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
			packet := f.output(t, 1, 1)
			if mode == "hash" {
				packet[len(packet)-1] ^= 1
			}
			s := sourceConnection(t, packet)
			if mode == "partial" {
				s.records = s.records[:len(s.records)-1]
			}
			r := receiveSource(t, f, s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			if mode == "retired" {
				f.b.Close()
			}
			calls := 0
			consumerError := errors.New("downstream send failed")
			consume := in.OutputConsumer(func(context.Context, wire.Header, []byte) error {
				calls++
				if mode == "consumer_failed" {
					return consumerError
				}
				if mode == "consumer_capacity" {
					return fa.CapacityRejected
				}
				return nil
			})
			if mode == "nil_consumer" {
				consume = nil
			}
			err := r.Receive(ctx, consume)
			if err == nil || r.Stats().Outputs != 0 {
				t.Fatal("failed output counted as delivered", err, r.Stats())
			}
			if mode == "consumer_failed" || mode == "consumer_capacity" {
				if calls != 1 {
					t.Fatal("callback count", calls)
				}
			} else if calls != 0 {
				t.Fatal("unverified output escaped")
			}
			if r.Stats().Drops != 0 {
				t.Fatal("downstream failure hidden as a retention drop")
			}
			want(t, r.Receive(context.Background(), consume), peer.ErrRetired)
			must(t, r.WaitClosed(context.Background()))
		})
	}
}
