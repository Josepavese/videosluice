package ingress

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
)

// SourceSnapshot is a trusted admitted provider, NOT an RPC-supplied parser.
// Success means an atomic private VBCF snapshot validated against its immutable
// mapping identity. Payload SHA is PENDING, preserved in the header, and must
// be checked on the Store-owned copy before any pixels can leave that Store.
// It must never expose bytes on failure or claim an integrity-verified frame.
// The launcher owns its physical lifetime across every call, including a call
// that ignores cancellation. The two serials are slot counters, not origins.
type SourceSnapshot interface {
	StageInto(context.Context, int, uint64, []byte) (int, uint64, error)
}

type SourceSurfaceReceiver struct {
	ingress             *Ingress
	connection          SourceConnection
	generation          string
	clock               fa.Clock
	copyNS              uint64
	workspace           *fd.Resource[[]byte]
	operation           sync.Mutex
	last                [2]uint64
	closed              atomic.Bool
	frames, busy, drops atomic.Uint64
}

// StagedFrames counts retained inputs, not completed integrity checks or ROIs.
type SourceSurfaceStats struct{ StagedFrames, Busy, Drops uint64 }

// The mailbox allocation is admitted by its launcher. This resource charges
// the ONE private snapshot. Deferred SHA uses the Store's admitted workspace.
func NewSourceSurfaceReceiver(ctx context.Context, i *Ingress, connection SourceConnection, generation string, clock fa.Clock, copyNS uint64) (*SourceSurfaceReceiver, error) {
	if ctx == nil || i == nil || connection == nil || !controlToken(generation) || clock == nil || copyNS == 0 || copyNS > 5_000_000_000 {
		return nil, peer.ErrInvalid
	}
	r := &SourceSurfaceReceiver{ingress: i, connection: connection, generation: generation, clock: clock, copyNS: copyNS}
	if err := r.current(); err != nil {
		return nil, err
	}
	capacity := wire.HeaderSize + i.surfaceBytes
	var err error
	r.workspace, err = fd.OwnResource(ctx, i.binding, fd.ResourcePlan[[]byte]{Bytes: capacity + 512,
		Create: func(ctx context.Context) (fd.Created[[]byte], error) {
			if err := ctx.Err(); err != nil {
				return fd.Created[[]byte]{}, err
			}
			return fd.Created[[]byte]{Value: make([]byte, capacity), Owned: true}, nil
		},
		Release: func([]byte) error { return nil }})
	if err != nil {
		return nil, err
	}
	return r, nil
}
func (r *SourceSurfaceReceiver) current() error {
	if r.closed.Load() {
		return peer.ErrRetired
	}
	if err := r.ingress.binding.ValidateCurrent(); err != nil {
		return err
	}
	generation, err := r.connection.CurrentGeneration()
	if err != nil || generation != r.generation {
		return peer.ErrRetired
	}
	return nil
}
func (r *SourceSurfaceReceiver) Close() {
	if r != nil && !r.closed.Swap(true) {
		r.workspace.Close()
	}
}
func (r *SourceSurfaceReceiver) WaitClosed(ctx context.Context) error {
	return r.workspace.WaitClosed(ctx)
}
func (r *SourceSurfaceReceiver) Stats() SourceSurfaceStats {
	return SourceSurfaceStats{r.frames.Load(), r.busy.Load(), r.drops.Load()}
}

// Poll makes at most one attempt per slot; no retry, queue, lease or blocking
// dependency from the output path. The owner schedules polls independently.
func (r *SourceSurfaceReceiver) Poll(ctx context.Context, snapshot SourceSnapshot) error {
	if ctx == nil || snapshot == nil {
		return peer.ErrInvalid
	}
	if !r.operation.TryLock() {
		return peer.ErrBusy
	}
	defer r.operation.Unlock()
	return r.workspace.Borrow(ctx, func(ctx context.Context, buffer []byte) error {
		for slot := range r.last {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := r.current(); err != nil {
				return err
			}
			observed := r.clock.NowNS()
			n, serial, err := snapshot.StageInto(ctx, slot, r.last[slot], buffer)
			if err != nil {
				if singleCause(err) == wire.ErrBusy {
					r.busy.Add(1)
					continue
				}
				return fail("source_snapshot", err)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := r.current(); err != nil {
				return err
			}
			if n < wire.HeaderSize || n > len(buffer) || serial == 0 || serial&1 != 0 || serial <= r.last[slot] {
				return fail("source_snapshot_contract", peer.ErrRecord)
			}
			h, err := wire.ParseHeader(buffer[:wire.HeaderSize])
			if err != nil {
				return err
			}
			if int(h.PayloadBytes) != n-wire.HeaderSize {
				return peer.ErrRecord
			}
			if err := r.ingress.validateSurfaceHeader(h); err != nil {
				return err
			}
			// Complete atomic staging consumes a slot, not an integrity assertion.
			// Store rejection never replays an old snapshot with a new observed time.
			r.last[slot] = serial
			now := r.clock.NowNS()
			if now < observed || now > ^uint64(0)-r.copyNS {
				return fail("source_snapshot_clock", fa.DeadlineExceeded)
			}
			err = r.ingress.binding.StageSurface(ctx, r.ingress.origin(h), buffer[wire.HeaderSize:n], fa.Digest(h.PayloadSHA), observed, now+r.copyNS)
			if err != nil {
				if ctx.Err() == nil && r.current() == nil && (errors.Is(err, fa.Expired) || errors.Is(err, fa.CapacityRejected) || errors.Is(err, fa.DeadlineExceeded)) {
					r.drops.Add(1)
					continue
				}
				return fmt.Errorf("source_snapshot_store source_epoch=%d origin_sequence=%d: %w", h.SourceEpoch, h.OriginSequence, err)
			}
			r.frames.Add(1)
		}
		return nil
	})
}
