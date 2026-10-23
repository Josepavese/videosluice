package ingress

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
)

// SourceOutputReceiver polls complete immutable snapshots, not a byte stream.
// Missing intermediate publications never require resynchronizing or restarting
// the source. A pinned producer and binding remain authoritative on every poll.
type SourceOutputReceiver struct {
	ingress                                    *Ingress
	connection                                 SourceConnection
	generation                                 string
	digest                                     wire.PayloadSHA256
	budget                                     time.Duration
	workspace                                  *fd.Resource[[2][]byte]
	operation                                  sync.Mutex
	last                                       [2]uint64
	closed                                     atomic.Bool
	outputs, busy, drops, superseded, failures atomic.Uint64
}

var ErrOutputPollBudget = errors.New("complete output snapshot poll budget expired")

type SourceOutputStats struct{ Outputs, Busy, Drops, Superseded, Failures uint64 }

// Only a single causal pressure sentinel qualifies. Mixed failures, especially
// errors.Join(authority, deadline), must never hide a structural failure.
func outputPressure(err error) bool {
	for err != nil {
		if err == context.DeadlineExceeded || err == fa.DeadlineExceeded || err == fa.Expired || err == fa.CapacityRejected {
			return true
		}
		wrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = wrapped.Unwrap()
	}
	return false
}

func NewSourceOutputReceiver(ctx context.Context, i *Ingress, connection SourceConnection, generation string, budgetNS uint64, digest wire.PayloadSHA256, digestBytes int) (*SourceOutputReceiver, error) {
	if ctx == nil || i == nil || connection == nil || !controlToken(generation) || budgetNS == 0 || budgetNS > 5_000_000_000 || digest == nil || digestBytes < 0 || digestBytes > 1<<20 {
		return nil, peer.ErrInvalid
	}
	r := &SourceOutputReceiver{ingress: i, connection: connection, generation: generation, digest: digest, budget: time.Duration(budgetNS)}
	if err := r.current(); err != nil {
		return nil, err
	}
	capacity := 0
	for _, delivery := range i.deliveries {
		capacity = max(capacity, int(delivery.MaxPayload))
	}
	capacity += wire.HeaderSize
	var err error
	r.workspace, err = fd.OwnResource(ctx, i.binding, fd.ResourcePlan[[2][]byte]{Bytes: 2*capacity + digestBytes,
		Create: func(ctx context.Context) (fd.Created[[2][]byte], error) {
			if err := ctx.Err(); err != nil {
				return fd.Created[[2][]byte]{}, err
			}
			return fd.Created[[2][]byte]{Value: [2][]byte{make([]byte, capacity), make([]byte, capacity)}, Owned: true}, nil
		}, Release: func([2][]byte) error { return nil }})
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (r *SourceOutputReceiver) current() error {
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
func (r *SourceOutputReceiver) Close() {
	if r != nil && !r.closed.Swap(true) {
		r.workspace.Close()
	}
}
func (r *SourceOutputReceiver) WaitClosed(ctx context.Context) error {
	return r.workspace.WaitClosed(ctx)
}
func (r *SourceOutputReceiver) Stats() SourceOutputStats {
	return SourceOutputStats{Outputs: r.outputs.Load(), Busy: r.busy.Load(), Drops: r.drops.Load(), Superseded: r.superseded.Load(), Failures: r.failures.Load()}
}

// Poll owns two bounded private staging buffers. Copy each slot at most once,
// order complete snapshots by output sequence, verify SHA before publication,
// and never replay a consumed serial, including after a late consumer. An
// atomic snapshot racing a producer is busy, not a transport failure.
func (r *SourceOutputReceiver) Poll(parent context.Context, snapshot SourceSnapshot, consume OutputConsumer) (err error) {
	if parent == nil || snapshot == nil || consume == nil {
		return peer.ErrInvalid
	}
	if !r.operation.TryLock() {
		return peer.ErrBusy
	}
	defer r.operation.Unlock()
	ctx, cancel := context.WithTimeout(parent, r.budget)
	defer cancel()
	started := time.Now()
	phase, activeSlot := "workspace", -1
	defer func() {
		if err != nil && parent.Err() == nil && ctx.Err() == context.DeadlineExceeded && r.current() == nil && outputPressure(err) {
			r.drops.Add(1)
			// Classification must not erase the provider/authority checkpoint:
			// the sentinel alone cannot distinguish staging from commit stalls.
			err = fmt.Errorf("%w: %w", ErrOutputPollBudget, err)
		}
		if err != nil {
			err = fmt.Errorf("output_poll_progress phase=%s slot=%d elapsed_us=%d budget_us=%d: %w",
				phase, activeSlot, time.Since(started).Microseconds(), r.budget.Microseconds(), err)
		}
	}()
	return r.workspace.Borrow(parent, func(resourceCtx context.Context, buffers [2][]byte) error {
		stop := context.AfterFunc(resourceCtx, cancel)
		defer stop()
		var headers [2]wire.Header
		var used [2]int
		var serials [2]uint64
		for slot := range r.last {
			phase, activeSlot = "stage_authority", slot
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := r.current(); err != nil {
				return err
			}
			phase = "stage"
			n, serial, err := snapshot.StageInto(ctx, slot, r.last[slot], buffers[slot])
			if errors.Is(err, wire.ErrBusy) {
				r.busy.Add(1)
				continue
			}
			if err != nil {
				return fail("output_snapshot", err)
			}
			phase = "stage_validation"
			if err := r.current(); err != nil {
				return err
			}
			if n < wire.HeaderSize || n > len(buffers[slot]) || serial == 0 || serial&1 != 0 || serial <= r.last[slot] {
				return fail("output_snapshot_contract", peer.ErrRecord)
			}
			h, err := wire.ParseHeader(buffers[slot][:wire.HeaderSize])
			if err != nil {
				return err
			}
			if int(h.PayloadBytes) != n-wire.HeaderSize {
				return peer.ErrRecord
			}
			if err := r.ingress.validateOutputHeader(h); err != nil {
				return err
			}
			headers[slot], used[slot], serials[slot] = h, n, serial
		}
		order := [2]int{0, 1}
		if used[1] != 0 && (used[0] == 0 || headers[1].OutputSequence < headers[0].OutputSequence) {
			order = [2]int{1, 0}
		}
		for _, slot := range order {
			if used[slot] == 0 {
				continue
			}
			h := headers[slot]
			phase, activeSlot = "integrity", slot
			// The slot was completely staged. Any later pressure consumes it;
			// new frames can replace it without transport teardown or retries.
			r.last[slot] = serials[slot]
			payload := buffers[slot][wire.HeaderSize:used[slot]]
			if digest := r.digest(payload); digest != h.PayloadSHA {
				r.failures.Add(1)
				return fail("output_snapshot_integrity", wire.ErrInvalid)
			}
			if err := r.current(); err != nil {
				return err
			}
			err := ctx.Err()
			superseded := false
			if err == nil {
				phase = "commit"
				superseded, err = r.ingress.acceptVerifiedSnapshotOutput(ctx, h, payload, consume)
			}
			if superseded {
				// The other slot was published and consumed first while this
				// complete older snapshot was busy. Its per-slot serial was
				// consumed above; never publish it or retire the source.
				r.superseded.Add(1)
				continue
			}
			if err != nil {
				if parent.Err() == nil && r.current() == nil && outputPressure(err) {
					r.drops.Add(1)
					continue
				}
				r.failures.Add(1)
				return err
			}
			r.outputs.Add(1)
		}
		phase, activeSlot = "workspace_release", -1
		return nil
	})
}
