package ingress_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
)

func outputSnapshotReceiver(t *testing.T, f *fixture, budget time.Duration) (*in.SourceOutputReceiver, *sourceConnectionFixture) {
	t.Helper()
	connection := sourceConnection(t, f.output(t, 1, 1))
	r, err := in.NewSourceOutputReceiver(context.Background(), f.i, connection, "source.worker", uint64(budget), sha256.Sum256, 0)
	must(t, err)
	t.Cleanup(r.Close)
	return r, connection
}

func TestOutputSnapshotsOrderSlotsAndNeverReplay(t *testing.T) {
	f := setup(t)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	r, _ := outputSnapshotReceiver(t, f, time.Second)
	packets := [2][]byte{f.output(t, 1, 2), f.output(t, 1, 1)}
	snapshot := sourceSnapshotFunc(func(_ context.Context, slot int, after uint64, p []byte) (int, uint64, error) {
		if after >= 2 {
			return 0, 0, wire.ErrBusy
		}
		return copy(p, packets[slot]), 2, nil
	})
	calls := 0
	consume := func(_ context.Context, h wire.Header, _ []byte) error {
		calls++
		if h.DeliveryID != uint64(calls) {
			t.Fatal("out of order delivery", h)
		}
		return nil
	}
	must(t, r.Poll(context.Background(), snapshot, consume))
	must(t, r.Poll(context.Background(), snapshot, consume))
	if calls != 2 || r.Stats().Outputs != 2 || r.Stats().Busy != 2 {
		t.Fatal("old snapshot replayed", calls, r.Stats())
	}
}

func TestOutputSnapshotsLateCompleteSlotIsSupersededNotSourceFailure(t *testing.T) {
	f := setup(t)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	r, _ := outputSnapshotReceiver(t, f, time.Second)
	step := 0
	snapshot := sourceSnapshotFunc(func(_ context.Context, slot int, after uint64, p []byte) (int, uint64, error) {
		var sequence, serial uint64
		switch {
		case step == 0 && slot == 0:
			sequence, serial = 2, 2
		case step == 1 && slot == 1:
			sequence, serial = 1, 2
		case step == 2 && slot == 0:
			sequence, serial = 3, 4
		default:
			return 0, 0, wire.ErrBusy
		}
		if serial <= after {
			return 0, 0, wire.ErrBusy
		}
		return copy(p, f.output(t, 1, sequence)), serial, nil
	})
	var delivered []uint64
	consume := func(_ context.Context, h wire.Header, _ []byte) error {
		delivered = append(delivered, h.OriginSequence)
		return nil
	}
	for step = 0; step < 3; step++ {
		must(t, r.Poll(context.Background(), snapshot, consume))
	}
	stats := r.Stats()
	if len(delivered) != 2 || stats.Outputs != 2 || stats.Superseded != 1 || stats.Failures != 0 {
		t.Fatal("late complete slot retired source or published stale pixels", delivered, stats)
	}
}

func TestOutputSnapshotsEqualSequenceStillFailsReplay(t *testing.T) {
	f := setup(t)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	r, _ := outputSnapshotReceiver(t, f, time.Second)
	step := 0
	snapshot := sourceSnapshotFunc(func(_ context.Context, slot int, after uint64, p []byte) (int, uint64, error) {
		if slot != step || after >= 2 {
			return 0, 0, wire.ErrBusy
		}
		return copy(p, f.output(t, 1, 2)), 2, nil
	})
	consume := func(context.Context, wire.Header, []byte) error { return nil }
	must(t, r.Poll(context.Background(), snapshot, consume))
	step = 1
	want(t, r.Poll(context.Background(), snapshot, consume), fa.InvalidContract)
	if r.Stats().Superseded != 0 {
		t.Fatal("equal replay cannot be benign", r.Stats())
	}
}

func TestOutputSnapshotsLateConsumerDropsAndRecoversOnSameReceiver(t *testing.T) {
	f := setup(t)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	r, _ := outputSnapshotReceiver(t, f, 25*time.Millisecond)
	sequence := uint64(1)
	snapshot := sourceSnapshotFunc(func(_ context.Context, slot int, after uint64, p []byte) (int, uint64, error) {
		if slot == 1 || after >= 2*sequence {
			return 0, 0, wire.ErrBusy
		}
		return copy(p, f.output(t, 1, sequence)), 2 * sequence, nil
	})
	calls := 0
	consume := func(ctx context.Context, _ wire.Header, _ []byte) error {
		calls++
		if calls == 1 {
			<-ctx.Done()
		}
		return nil
	}
	must(t, r.Poll(context.Background(), snapshot, consume))
	sequence++
	must(t, r.Poll(context.Background(), snapshot, consume))
	if calls != 2 || r.Stats().Drops != 1 || r.Stats().Outputs != 1 || r.Stats().Failures != 0 {
		t.Fatal("deadline poisoned snapshots", calls, r.Stats())
	}
}

func TestOutputSnapshotsRejectCorruptionForeignEpochAndMixedAuthorityFailure(t *testing.T) {
	for _, kind := range []string{"hash", "epoch", "authority-and-deadline"} {
		t.Run(kind, func(t *testing.T) {
			f := setup(t)
			r, _ := outputSnapshotReceiver(t, f, time.Second)
			packet := f.output(t, 1, 1)
			if kind == "hash" {
				packet[len(packet)-1] ^= 1
			}
			if kind == "epoch" {
				h, err := wire.ParseHeader(packet[:wire.HeaderSize])
				must(t, err)
				h.SourceEpoch++
				packet, err = wire.Marshal(h, packet[wire.HeaderSize:])
				must(t, err)
			}
			snapshot := sourceSnapshotFunc(func(_ context.Context, slot int, after uint64, p []byte) (int, uint64, error) {
				if slot == 1 || after >= 2 {
					return 0, 0, wire.ErrBusy
				}
				return copy(p, packet), 2, nil
			})
			calls := 0
			err := r.Poll(context.Background(), snapshot, func(context.Context, wire.Header, []byte) error {
				calls++
				return errors.Join(peer.ErrUnauthorized, context.DeadlineExceeded)
			})
			if err == nil || r.Stats().Outputs != 0 || r.Stats().Drops != 0 {
				t.Fatal("structural failure became a pressure drop", err, r.Stats())
			}
			if kind == "authority-and-deadline" {
				want(t, err, peer.ErrUnauthorized)
				if calls != 1 {
					t.Fatal(calls)
				}
			} else if calls != 0 {
				t.Fatal("invalid pixels exposed")
			}
		})
	}
}

func TestOutputSnapshotsRetiredProducerAndOwnerCancellationRemainFatal(t *testing.T) {
	f := setup(t)
	r, connection := outputSnapshotReceiver(t, f, time.Second)
	connection.generation.Store("replaced.worker")
	snapshot := sourceSnapshotFunc(func(context.Context, int, uint64, []byte) (int, uint64, error) {
		t.Fatal("retired producer read")
		return 0, 0, nil
	})
	consume := func(context.Context, wire.Header, []byte) error { return fa.InvalidContract }
	want(t, r.Poll(context.Background(), snapshot, consume), peer.ErrRetired)
	connection.generation.Store("source.worker")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	want(t, r.Poll(ctx, snapshot, consume), context.Canceled)
}
