//go:build linux && cgo

package ingress_test

import (
	"context"
	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/framewire/provider/linux"
	"github.com/Josepavese/videosluice/framewire/surfaceport"
	in "github.com/Josepavese/videosluice/frameingress"
	"testing"
	"time"
)

func TestActualLinuxAtomicSnapshotToNativeROI(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	m, err := linux.New(linux.Limits{Bytes: 1 << 20, Allocations: 1, Operations: 2})
	must(t, err)
	var provider surfaceport.Allocator = m
	mapping := wire.Mapping{Namespace: f.b.Fence().Namespace, Generation: f.b.Scope().MediaEpoch, Capacity: wire.HeaderSize + 384}
	a, err := in.OwnMapping(ctx, f.b, provider, mapping)
	must(t, err)
	defer a.Close()
	quote, err := provider.Footprint(mapping)
	must(t, err)
	if uint64(f.r.Stats().ReservedResourceBytes) != quote || m.Usage().Bytes != quote {
		t.Fatal("quote differs from actual charges", quote, f.r.Stats(), m.Usage())
	}
	// Provider page-rounded mapping, input and snapshot share one global/source
	// domain budget; the provider also retains its defensive local quota.
	input, err := f.b.AllocateBuffer(ctx, wire.HeaderSize+384)
	must(t, err)
	defer input.Close()
	snapshot, err := f.b.AllocateBuffer(ctx, wire.HeaderSize+384)
	must(t, err)
	defer snapshot.Close()
	for seq := uint64(1); seq <= 100; seq++ {
		tk := f.ticket(t)
		packet := f.surface(t, seq)
		must(t, input.Borrow(ctx, func(op context.Context, buf []byte) error {
			copy(buf, packet)
			return a.Borrow(op, func(op context.Context, m surfaceport.Allocation) error { return m.Publish(op, buf) })
		}))
		must(t, snapshot.Borrow(ctx, func(op context.Context, buf []byte) error {
			var used int
			err := a.Borrow(op, func(op context.Context, m surfaceport.Allocation) error {
				var e error
				used, e = m.SnapshotInto(op, buf)
				return e
			})
			if err != nil {
				return err
			}
			if err = f.i.AcceptSurface(op, buf[:used], 100, 200); err != nil {
				return err
			}
			// Mutation after admission cannot alter retained exact native pixels.
			for j := range buf {
				buf[j] = 0
			}
			return nil
		}))
		for j := range packet {
			packet[j] = 0
		}
		must(t, f.i.AcceptDelivery(ctx, tk, f.delivery(t, tk, seq)))
		f.verify(t, tk, seq)
	}
	a.Close()
	closedCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	must(t, a.WaitClosed(closedCtx))
	input.Close()
	snapshot.Close()
	if f.r.Stats().ReservedBufferBytes != 0 {
		t.Fatal("domain buffer quota leaked", f.r.Stats())
	}
	if m.Usage() != (linux.Usage{}) {
		t.Fatal("mapping quota leaked", m.Usage())
	}
	if f.r.Stats().ReservedResourceBytes != 0 || f.r.Stats().LiveResources != 0 {
		t.Fatal("resource quota leaked", f.r.Stats())
	}
}
