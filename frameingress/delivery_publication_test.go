//go:build linux

package ingress_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
	snapshotlinux "github.com/Josepavese/videosluice/framewire/provider/linux"
	"github.com/Josepavese/videosluice/framewire/surfaceport"
)

type snapshotFaultAllocator struct {
	surfaceport.Allocator
	fault      error
	reads      int
	afterFault func()
}

func (a *snapshotFaultAllocator) Create(mapping wire.Mapping) (surfaceport.Allocation, error) {
	allocation, err := a.Allocator.Create(mapping)
	if allocation == nil {
		return nil, err
	}
	return &snapshotFaultAllocation{Allocation: allocation, owner: a}, err
}

type snapshotFaultAllocation struct {
	surfaceport.Allocation
	owner *snapshotFaultAllocator
}

func (a *snapshotFaultAllocation) SnapshotInto(ctx context.Context, out []byte) (int, error) {
	a.owner.reads++
	if err := a.owner.fault; err != nil {
		a.owner.fault = nil
		if a.owner.afterFault != nil {
			a.owner.afterFault()
		}
		return 0, err
	}
	return a.Allocation.SnapshotInto(ctx, out)
}

func TestDeliverySessionSnapshotContentionKeepsBindingAndExactFrame(t *testing.T) {
	f, control, _ := frameSessionFixture(t)
	native, err := snapshotlinux.New(snapshotlinux.Limits{Bytes: 1 << 20, Allocations: 2, Operations: 2})
	must(t, err)
	allocator := &snapshotFaultAllocator{Allocator: native, fault: &in.Failure{Stage: "fixture_contention", Cause: wire.ErrBusy}}
	p, err := in.NewDeliveryPublication(context.Background(), f.b, "analysis", in.DeliveryLayout{
		Transform: f.tr, Codec: wire.NV12, StrideY: 8, StrideUV: 8, MaxPayload: 96}, allocator)
	must(t, err)
	defer func() { p.Close(); must(t, p.WaitClosed(context.Background())) }()
	h, pixels := publicationFrame(t, f, 2)
	must(t, p.Publish(context.Background(), h, pixels))
	reads := 0
	var packets [][]byte
	c := &sessionConnection{receive: func(ctx context.Context, out []byte) (int, peer.Principal, error) {
		reads++
		q := nextDelivery(0, 0)
		if reads > 1 {
			q = nextDelivery(h.PublicationEpoch, 0) // no frame was acknowledged
		}
		return copy(out, q), principal(), nil
	}, write: func(_ context.Context, packet []byte) error {
		packets = append(packets, append([]byte(nil), packet...))
		return nil
	}}
	s, err := in.NewDeliverySession(context.Background(), f.r, control, "reader", ownSessionFixture(t, f.r, c), p, uint64(time.Second))
	must(t, err)
	defer func() { s.Close(); waitSession(t, s) }()
	complete, err := s.Step(context.Background())
	must(t, err)
	if !complete || s.Stats().Closed || allocator.reads != 1 || len(packets) != 2 ||
		string(packets[0][:4]) != "FAPB" || string(packets[1][:4]) != "FAPI" ||
		binary.BigEndian.Uint64(packets[1][8:16]) != h.PublicationEpoch || binary.BigEndian.Uint64(packets[1][16:24]) != 0 {
		t.Fatal("snapshot contention retired, spun, changed cursor or exposed pixels", s.Stats(), allocator.reads)
	}
	complete, err = s.Step(context.Background())
	must(t, err)
	if !complete || s.Stats().Closed || s.Stats().Completed != 2 || allocator.reads != 2 || len(packets) != 4 {
		t.Fatal("same binding did not resume with bounded work", s.Stats(), allocator.reads)
	}
	actual, err := wire.ParseHeader(packets[2])
	must(t, err)
	if actual != h || !bytes.Equal(packets[3][len(packets[3])-len(pixels):], pixels) {
		t.Fatal("resumed delivery changed the real frame identity or pixels")
	}
	// Corrupt/replaced mappings are not transient contention.
	allocator.fault = wire.ErrMapping
	err = p.Snapshot(context.Background(), 0, func(context.Context, wire.Header, []byte) error {
		t.Fatal("invalid mapping reached consumer")
		return nil
	})
	want(t, err, wire.ErrMapping)
	// A failure inside the consumer must not be disguised as a safe snapshot miss.
	err = p.Snapshot(context.Background(), 0, func(context.Context, wire.Header, []byte) error { return wire.ErrBusy })
	want(t, err, wire.ErrBusy)
}

func deliveryPublication(t *testing.T, f *fixture) *in.DeliveryPublication {
	t.Helper()
	allocator, err := snapshotlinux.New(snapshotlinux.Limits{Bytes: 1 << 20, Allocations: 2, Operations: 2})
	must(t, err)
	p, err := in.NewDeliveryPublication(context.Background(), f.b, "analysis", in.DeliveryLayout{
		Transform: f.tr, Codec: wire.NV12, StrideY: 8, StrideUV: 8, MaxPayload: 96}, allocator)
	if p != nil {
		t.Cleanup(func() {
			p.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			must(t, p.WaitClosed(ctx))
		})
	}
	must(t, err)
	return p
}
func publicationFrame(t *testing.T, f *fixture, sequence uint64) (wire.Header, []byte) {
	t.Helper()
	packet := f.delivery(t, f.ticket(t), sequence)
	h, err := wire.ParseHeader(packet[:wire.HeaderSize])
	must(t, err)
	return h, packet[wire.HeaderSize:]
}
func nextDelivery(epoch, last uint64) []byte {
	p := make([]byte, 24)
	copy(p, "FAPN\x01\x00\x00\x00")
	binary.BigEndian.PutUint64(p[8:16], epoch)
	binary.BigEndian.PutUint64(p[16:24], last)
	return p
}

func TestDeliveryMetadataIsCommittedExactAndRetirementFenced(t *testing.T) {
	f := setup(t)
	p := deliveryPublication(t, f)
	ctx := context.Background()
	if _, err := p.Metadata(ctx); !errors.Is(err, in.ErrDeliveryIdle) {
		t.Fatal("unpublished metadata", err)
	}
	h, pixels := publicationFrame(t, f, (1<<53)+19)
	h.DeliveryID = (1 << 53) + 19
	must(t, p.Publish(ctx, h, pixels))
	m, err := p.Metadata(ctx)
	must(t, err)
	if m.Source != h.Source || m.Stream != h.Stream || m.DeliveryID != h.DeliveryID ||
		m.SourceEpoch != h.SourceEpoch || m.OriginSequence != h.OriginSequence ||
		m.Width != h.Width || m.Height != h.Height || time.Since(m.CommittedAt) > time.Second {
		t.Fatal("metadata is not the committed frame", m, h)
	}
	raw, err := json.Marshal(m)
	must(t, err)
	var fields map[string]any
	must(t, json.Unmarshal(raw, &fields))
	if fields["delivery_id"] != "9007199254741011" {
		t.Fatal("JSON lost exact uint64", fields)
	}
	h2, pixels2 := publicationFrame(t, f, h.DeliveryID+1)
	h2.DeliveryID = h.DeliveryID + 1
	pixels2[0] ^= 1
	if err := p.Publish(ctx, h2, pixels2); err == nil {
		t.Fatal("corrupt publication committed")
	}
	after, err := p.Metadata(ctx)
	must(t, err)
	if after != m {
		t.Fatal("failed publish changed metadata", m, after)
	}
	p.Close()
	must(t, p.WaitClosed(ctx))
	if _, err := p.Metadata(ctx); err == nil {
		t.Fatal("retired metadata remained available")
	}
}

func TestDeliveryPublicationLatestOnlyAndSlowReaderDoesNotBlockCapture(t *testing.T) {
	f := setup(t)
	before := f.r.Stats()
	p := deliveryPublication(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want(t, p.Snapshot(ctx, 0, func(context.Context, wire.Header, []byte) error { t.Fatal("idle had pixels"); return nil }), in.ErrDeliveryIdle)
	h, pixels := publicationFrame(t, f, 1)
	must(t, p.Publish(ctx, h, pixels))
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- p.Snapshot(ctx, 0, func(ctx context.Context, got wire.Header, data []byte) error {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			if got != h || !bytes.Equal(data, pixels) {
				return errors.New("snapshot mutated during publish")
			}
			return nil
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("reader did not enter")
	}
	newHeader, newPixels := publicationFrame(t, f, 2)
	must(t, p.Publish(ctx, newHeader, newPixels)) // Reader is still blocked.
	metadata, err := p.Metadata(ctx)
	must(t, err)
	if metadata.DeliveryID != newHeader.DeliveryID {
		t.Fatal("slow pixel reader blocked current metadata")
	}
	want(t, p.Snapshot(ctx, 0, func(context.Context, wire.Header, []byte) error { return nil }), fa.CapacityRejected)
	close(release)
	must(t, <-done)
	must(t, p.Snapshot(ctx, h.DeliveryID, func(_ context.Context, got wire.Header, data []byte) error {
		if got != newHeader || !bytes.Equal(data, newPixels) {
			t.Fatal("latest delivery lost")
		}
		return nil
	}))
	want(t, p.Snapshot(ctx, newHeader.DeliveryID, func(context.Context, wire.Header, []byte) error { return nil }), in.ErrDeliveryIdle)
	if err := p.Publish(ctx, h, pixels); err == nil {
		t.Fatal("replay accepted")
	}
	if err := p.Snapshot(ctx, newHeader.DeliveryID+1, func(context.Context, wire.Header, []byte) error { return nil }); err == nil {
		t.Fatal("future cursor accepted")
	}
	p.Close()
	must(t, p.WaitClosed(ctx))
	if after := f.r.Stats(); after.ReservedResourceBytes != before.ReservedResourceBytes || after.LiveResources != before.LiveResources {
		t.Fatal("publication resources not returned", before, after)
	}
}

func TestDeliverySenderRejectsUnboundRecordsAndFencesPartialTransfer(t *testing.T) {
	for _, mode := range []string{"initial_epoch", "malformed", "wrong_epoch", "future", "foreign", "revoked", "write_failure", "write_busy", "retire_during_header"} {
		t.Run(mode, func(t *testing.T) {
			f, a, _, _ := accessFixture(t)
			p := deliveryPublication(t, f)
			h, pixels := publicationFrame(t, f, 1)
			must(t, p.Publish(context.Background(), h, pixels))
			request, first, writes := nextDelivery(0, 0), true, 0
			switch mode {
			case "initial_epoch":
				request = nextDelivery(h.PublicationEpoch, 0)
			case "malformed":
				request[7] = 1
			case "wrong_epoch":
				request, first = nextDelivery(h.PublicationEpoch+1, 0), false
			case "future":
				request, first = nextDelivery(h.PublicationEpoch, h.DeliveryID+1), false
			case "foreign":
				f2 := setup(t)
				p = deliveryPublication(t, f2)
			case "revoked":
				a.Revoke()
			}
			s, err := in.NewFrameSender(context.Background(), f.r, frameWriter(func(_ context.Context, packet []byte) error {
				writes++
				if mode == "write_failure" {
					return errors.New("write failed")
				}
				if mode == "write_busy" && writes > 1 {
					return wire.ErrBusy
				}
				if mode == "retire_during_header" && string(packet[:4]) == "VBCF" {
					f.b.Close()
				}
				return nil
			}), 96, uint64(time.Second))
			must(t, err)
			err = s.SendDelivery(context.Background(), a, p, request, first)
			if err == nil {
				t.Fatal("invalid delivery session accepted")
			}
			if writes > 0 {
				want(t, s.SendDelivery(context.Background(), a, p, nextDelivery(0, 0), true), peer.ErrRetired)
			}
			closeSender(t, s)
		})
	}
}

func TestDeliverySessionUsesSharedGrantAndClosesWithoutOwningPublication(t *testing.T) {
	f, control, _ := frameSessionFixture(t)
	p := deliveryPublication(t, f)
	h, pixels := publicationFrame(t, f, 2)
	must(t, p.Publish(context.Background(), h, pixels))
	read, write := 0, 0
	c := &sessionConnection{receive: func(ctx context.Context, out []byte) (int, peer.Principal, error) {
		read++
		q := nextDelivery(0, 0)
		if read > 1 {
			q = nextDelivery(h.PublicationEpoch, h.DeliveryID)
		}
		return copy(out, q), principal(), nil
	}, write: func(context.Context, []byte) error { write++; return nil }}
	transport := ownSessionFixture(t, f.r, c)
	s, err := in.NewDeliverySession(context.Background(), f.r, control, "reader", transport, p, uint64(time.Second))
	must(t, err)
	for i := 0; i < 2; i++ {
		complete, err := s.Step(context.Background())
		must(t, err)
		if !complete {
			t.Fatal("delivery request not completed")
		}
	}
	if write != 4 {
		t.Fatal("wanted metadata, header, pixels and idle", write)
	}
	s.Close()
	waitSession(t, s)
	if c.closed.Load() != 1 {
		t.Fatal("transport release")
	}
	must(t, p.Snapshot(context.Background(), 0, func(context.Context, wire.Header, []byte) error { return nil }))
}
