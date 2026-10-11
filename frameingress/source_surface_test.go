package ingress_test

import (
	"context"
	"errors"
	"testing"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
)

type sourceSnapshotFunc func(context.Context, int, uint64, []byte) (int, uint64, error)

func (f sourceSnapshotFunc) StageInto(c context.Context, s int, a uint64, p []byte) (int, uint64, error) {
	return f(c, s, a, p)
}
func surfaceReceiver(t *testing.T, f *fixture) (*in.SourceSurfaceReceiver, *sourceConnectionFixture) {
	t.Helper()
	connection := sourceConnection(t, f.surface(t, 1))
	r, err := in.NewSourceSurfaceReceiver(context.Background(), f.i, connection, "source.worker", f.c, 100)
	must(t, err)
	t.Cleanup(r.Close)
	return r, connection
}

// Fixture-only atomic staging. Payload integrity is intentionally deferred to
// the private Store; native atomic/seal/serial guarantees have separate tests.
func fixedSurface(packet []byte) sourceSnapshotFunc {
	return func(_ context.Context, slot int, after uint64, p []byte) (int, uint64, error) {
		if slot != 0 || after >= 2 {
			return 0, 0, wire.ErrBusy
		}
		if _, err := wire.ParseHeader(packet[:wire.HeaderSize]); err != nil {
			return 0, 0, err
		}
		return copy(p, packet), 2, nil
	}
}

func TestNativeStagingNeverBlessesCorruptPixelsWithANewDigest(t *testing.T) {
	f := setup(t)
	r, _ := surfaceReceiver(t, f)
	packet := f.surface(t, 1)
	packet[wire.HeaderSize] ^= 1 // Outside the small ROI; retain producer SHA.
	tk := f.ticket(t)
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
	must(t, r.Poll(context.Background(), fixedSurface(packet)))
	result, err := f.r.Extract(context.Background(), fd.Request{ID: [16]byte{1}, Frame: tk.Ref(), Source: "front", Surface: "processed_full", Regions: []fa.Region{{ID: "roi", BBox: [4]float64{1, 1, 3, 3}}}, DeadlineNS: f.c.NowNS() + 100})
	if result != nil {
		result.Close()
		t.Fatal("staged corruption exposed pixels")
	}
	if !errors.Is(err, fa.InvalidContract) {
		t.Fatal("expected full source integrity rejection", err)
	}
}
func TestNativeSurfaceMayArriveAfterDelivery(t *testing.T) {
	f := setup(t)
	baseline := f.r.Stats().ReservedResourceBytes
	r, _ := surfaceReceiver(t, f)
	if f.r.Stats().ReservedResourceBytes != baseline+wire.HeaderSize+384+512 {
		t.Fatal("private buffer not charged")
	}
	tk := f.ticket(t)
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
	must(t, r.Poll(context.Background(), fixedSurface(f.surface(t, 1))))
	f.verify(t, tk, 1)
	must(t, r.Poll(context.Background(), fixedSurface(f.surface(t, 1))))
	if stats := r.Stats(); stats.StagedFrames != 1 || stats.Busy != 3 {
		t.Fatal(stats)
	}
	r.Close()
	must(t, r.WaitClosed(context.Background()))
	if f.r.Stats().ReservedResourceBytes != baseline {
		t.Fatal("private buffer leaked")
	}
}
func TestNativeSurfaceRejectsChangedProducerAndInvalidSnapshot(t *testing.T) {
	for _, mode := range []string{"producer", "graph", "serial", "length", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			r, c := surfaceReceiver(t, f)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			packet := f.surface(t, 1)
			if mode == "graph" {
				packet = mutate(t, packet, func(h *wire.Header) { h.Graph[0]++ })
			}
			snapshot := sourceSnapshotFunc(func(_ context.Context, _ int, _ uint64, p []byte) (int, uint64, error) {
				if mode == "producer" {
					c.generation.Store("replacement")
				}
				if mode == "canceled" {
					cancel()
				}
				n := copy(p, packet)
				serial := uint64(2)
				if mode == "serial" {
					serial = 1
				}
				if mode == "length" {
					n--
				}
				return n, serial, nil
			})
			if err := r.Poll(ctx, snapshot); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
			if r.Stats().StagedFrames != 0 {
				t.Fatal("invalid surface published")
			}
		})
	}
}
func TestSlowNativeSnapshotDoesNotBlockDeliveryOrFreeBorrowedMemory(t *testing.T) {
	f := setup(t)
	baseline := f.r.Stats().ReservedResourceBytes
	r, _ := surfaceReceiver(t, f)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	packet := f.surface(t, 1)
	go func() {
		done <- r.Poll(context.Background(), sourceSnapshotFunc(func(_ context.Context, _ int, _ uint64, p []byte) (int, uint64, error) {
			close(entered)
			<-release
			return copy(p, packet), 2, nil
		}))
	}()
	<-entered
	defer close(release)
	tk := f.ticket(t)
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
	if err := r.Poll(context.Background(), fixedSurface(packet)); !errors.Is(err, peer.ErrBusy) {
		t.Fatal(err)
	}
	r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := r.WaitClosed(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("released active snapshot", err)
	}
	if f.r.Stats().ReservedResourceBytes <= baseline {
		t.Fatal("active buffer uncharged")
	}
	// The cleanup callback unblocks before fixture retirement, even on failure.
	t.Cleanup(func() {
		if err := <-done; err == nil {
			t.Error("retired snapshot accepted")
		}
		must(t, r.WaitClosed(context.Background()))
	})
}
func TestNativeSurfaceRepeatedSerialCannotRejuvenateExpiredPixels(t *testing.T) {
	f := setup(t)
	r, _ := surfaceReceiver(t, f)
	packet := f.surface(t, 1)
	must(t, r.Poll(context.Background(), fixedSurface(packet)))
	f.c.now.Store(1200)
	must(t, r.Poll(context.Background(), fixedSurface(packet)))
	tk := f.ticket(t)
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
	// Expired pixel retention stays expired; metadata is independently valid.
	if r.Stats().StagedFrames != 1 {
		t.Fatal("replayed serial republished")
	}
	result, err := f.r.Extract(context.Background(), fd.Request{ID: [16]byte{1}, Frame: tk.Ref(), Source: "front", Surface: "processed_full", Regions: []fa.Region{{ID: "roi", BBox: [4]float64{1, 1, 3, 3}}}, DeadlineNS: 1300})
	if result != nil {
		result.Close()
	}
	if !errors.Is(err, fa.Expired) {
		t.Fatal("expired pixels rejuvenated", err)
	}
}
