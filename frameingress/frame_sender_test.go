package ingress_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	in "github.com/Josepavese/videosluice/frameingress"
	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

type frameWriter func(context.Context, []byte) error

func (w frameWriter) CurrentGeneration() (string, error) { return "consumer-start-1", nil }

func (w frameWriter) WritePacket(ctx context.Context, p []byte) error { return w(ctx, p) }

func senderFixture(t *testing.T) (*fixture, *in.Access, in.FrameRequest, *in.AccessResult) {
	t.Helper()
	f, a, _, q := accessFixture(t)
	result, err := a.Extract(context.Background(), principal(), q)
	must(t, err)
	t.Cleanup(result.Close)
	return f, a, in.FrameRequest{ID: q.ID, Frame: q.Frame, Source: q.Source, Surface: q.Surface, Regions: q.Regions, BudgetNS: 100}, result
}

func closeSender(t testing.TB, s *in.FrameSender) {
	t.Helper()
	s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, s.WaitClosed(ctx))
}

func TestSenderUsesOneRuntimeBufferAndDeliversVerifiedOutput(t *testing.T) {
	f, _, q, result := senderFixture(t)
	expected := in.ExpectedFrameEvidence{Origin: fa.OriginRef{Source: f.layout.Source, MediaEpoch: f.b.Scope().MediaEpoch, Sequence: 1, Graph: f.layout.Graph}, Transform: f.tr, Admission: admitted(f.b.Scope())}
	var assembly *in.FrameAssembly
	var calls int
	writer := frameWriter(func(ctx context.Context, p []byte) error {
		calls++
		if assembly == nil {
			m, err := in.DecodeFrameManifest(p)
			if err != nil {
				return err
			}
			assembly, err = in.NewFrameAssembly(p, q, expected, make([]byte, m.Bytes), 1024)
			// A source switch cannot relabel an already extracted artifact or
			// retire the broker-wide packet buffer. Its original proof survives.
			f.b.Close()
			return err
		}
		return assembly.Append(p)
	})
	before := f.r.Stats()
	sender, err := in.NewFrameSender(context.Background(), f.r, writer, 1024, uint64(time.Second))
	must(t, err)
	if stats := f.r.Stats(); stats.ReservedResourceBytes-before.ReservedResourceBytes != in.MaxFrameDataBytes || stats.LiveResources-before.LiveResources != 1 {
		t.Fatal("unaccounted packet buffer", stats)
	}
	must(t, sender.Send(context.Background(), q, result, 0))
	artifact, pixels, err := assembly.Finish()
	must(t, err)
	if len(pixels) != 24 || artifact.State != fa.Complete || calls != 2 {
		t.Fatal("wrong output")
	}
	closeSender(t, sender)
	if f.r.Stats().ReservedResourceBytes != before.ReservedResourceBytes {
		t.Fatal("packet charge retained")
	}
}

func TestSenderNegativeArtifactCarriesNoInventedProof(t *testing.T) {
	f, a, _, q := accessFixture(t)
	q.Regions[0].BBox = [4]float64{100, 100, 110, 110}
	result, err := a.Extract(context.Background(), principal(), q)
	must(t, err)
	defer result.Close()
	artifact, err := result.Describe(0)
	must(t, err)
	if artifact.State == fa.Complete || artifact.Proof != (fd.Proof{}) {
		t.Fatal("negative artifact has a fabricated domain proof")
	}
	want(t, result.Visit(0, func(fd.Artifact, []byte) error { t.Fatal("negative outcome borrowed pixels"); return nil }), fa.RegionInvalid)
	query := in.FrameRequest{ID: q.ID, Frame: q.Frame, Source: q.Source, Surface: q.Surface, Regions: q.Regions, BudgetNS: 100}
	calls := 0
	writer := frameWriter(func(ctx context.Context, p []byte) error {
		calls++
		m, err := in.DecodeFrameManifest(p)
		if err == nil && (m.Artifact.State == fa.Complete || m.Bytes != 0) {
			t.Fatal("failure converted to image")
		}
		return err
	})
	s, err := in.NewFrameSender(context.Background(), f.r, writer, 1024, uint64(time.Second))
	must(t, err)
	defer closeSender(t, s)
	must(t, s.Send(context.Background(), query, result, 0))
	if calls != 1 {
		t.Fatal("failure emitted data")
	}
}

func TestSenderStallCancellationRetainsQuotaAndRejectsConcurrentWork(t *testing.T) {
	f, _, q, result := senderFixture(t)
	entered := make(chan struct{})
	cancelSeen := make(chan struct{})
	release := make(chan struct{})
	writer := frameWriter(func(ctx context.Context, p []byte) error {
		close(entered)
		<-ctx.Done()
		close(cancelSeen)
		<-release
		return ctx.Err()
	})
	s, err := in.NewFrameSender(context.Background(), f.r, writer, 1024, uint64(time.Second))
	must(t, err)
	done := make(chan error, 1)
	go func() { done <- s.Send(context.Background(), q, result, 0) }()
	<-entered
	want(t, s.Send(context.Background(), q, result, 0), peer.ErrBusy)
	s.Close()
	<-cancelSeen
	if f.r.Stats().ReservedResourceBytes != in.MaxFrameDataBytes {
		t.Fatal("stalled writer falsely uncharged")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	want(t, s.WaitClosed(ctx), context.DeadlineExceeded)
	close(release)
	if err := <-done; err == nil {
		t.Fatal("cancelled send passed")
	}
	closeSender(t, s)
}

func TestSenderPartialFailurePoisonsChannelAndDeadlineIsNotRefreshed(t *testing.T) {
	for _, mode := range []string{"partial", "deadline", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			f, a, q, result := senderFixture(t)
			var calls atomic.Int32
			var deadline time.Time
			writer := frameWriter(func(ctx context.Context, p []byte) error {
				n := calls.Add(1)
				d, ok := ctx.Deadline()
				if !ok {
					t.Fatal("missing total deadline")
				}
				if n == 1 {
					deadline = d
					if mode == "revoked" {
						a.Revoke()
					}
					return nil
				}
				if d != deadline {
					t.Fatal("deadline refreshed")
				}
				if mode == "partial" {
					return errors.New("fixture write failed")
				}
				<-ctx.Done()
				return ctx.Err()
			})
			s, err := in.NewFrameSender(context.Background(), f.r, writer, 1024, uint64(20*time.Millisecond))
			must(t, err)
			if err = s.Send(context.Background(), q, result, 0); err == nil {
				t.Fatal("partial/expired send passed")
			}
			want(t, s.Send(context.Background(), q, result, 0), peer.ErrRetired)
			closeSender(t, s)
		})
	}
}

type otherFrameWriter struct{ frameWriter }

func (w otherFrameWriter) CurrentGeneration() (string, error) { return "other-consumer", nil }

func TestSenderCannotDeliverAnAuthorizedResultToAnotherPeer(t *testing.T) {
	f, _, q, result := senderFixture(t)
	w := otherFrameWriter{frameWriter(func(context.Context, []byte) error { t.Fatal("pixels/metadata sent to other consumer"); return nil })}
	s, err := in.NewFrameSender(context.Background(), f.r, w, 1024, uint64(time.Second))
	must(t, err)
	defer closeSender(t, s)
	want(t, s.Send(context.Background(), q, result, 0), peer.ErrUnauthorized)
}

func TestSenderAdmissionUsesSharedResourceCardinality(t *testing.T) {
	f, _, _, _ := senderFixture(t)
	var senders []*in.FrameSender
	defer func() {
		for _, s := range senders {
			closeSender(t, s)
		}
	}()
	w := frameWriter(func(context.Context, []byte) error { t.Fatal("admission wrote a packet"); return nil })
	for i := 0; i < 16; i++ {
		s, err := in.NewFrameSender(context.Background(), f.r, w, 1024, uint64(time.Second))
		must(t, err)
		senders = append(senders, s)
	}
	s, err := in.NewFrameSender(context.Background(), f.r, w, 1024, uint64(time.Second))
	want(t, err, fa.CapacityRejected)
	if s != nil {
		t.Fatal("resource limit bypassed")
	}
	if f.r.Stats().ReservedResourceBytes != 16*in.MaxFrameDataBytes {
		t.Fatal("shared buffer charge mismatch")
	}
}
