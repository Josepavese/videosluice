package framedomain

import (
	"context"
	"errors"
	"fmt"
	fa "github.com/Josepavese/videosluice/frameaccess"
)

type surfacePublicationFailure struct {
	phase                                  string
	begin, store, current, finish, elapsed int64
	canceled, deadline                     bool
	cause                                  error
}

func (e *surfacePublicationFailure) Error() string {
	return fmt.Sprintf("domain_surface_publish_progress phase=%s begin_us=%d store_us=%d current_us=%d finish_us=%d elapsed_us=%d context_canceled=%t context_deadline=%t: %v",
		e.phase, e.begin, e.store, e.current, e.finish, e.elapsed, e.canceled, e.deadline, e.cause)
}
func (e *surfacePublicationFailure) Unwrap() error { return e.cause }

func surfacePublicationElapsed(start, end uint64) int64 {
	if end < start {
		return -1
	}
	return int64((end - start) / 1000)
}

// PublishSurface admits native pixels before any stream delivery exists.
// Authenticated namespace/process/source/epoch/graph checks remain mandatory.
// Error-only diagnostics distinguish domain ownership waits from core copying;
// no retry, new queue, timeout or retirement decision is introduced here.
func (b *Binding) PublishSurface(ctx context.Context, o OriginRef, pixels []byte, observedNS, deadlineNS uint64) error {
	return b.publishSurface(ctx, o, pixels, nil, observedNS, deadlineNS)
}

// StageSurface preserves a producer digest for mandatory verification of the
// Store-owned copy before any extraction. It is not an unverified RPC path.
func (b *Binding) StageSurface(ctx context.Context, o OriginRef, pixels []byte, expected fa.Digest, observedNS, deadlineNS uint64) error {
	return b.publishSurface(ctx, o, pixels, &expected, observedNS, deadlineNS)
}

func (b *Binding) publishSurface(ctx context.Context, o OriginRef, pixels []byte, expected *fa.Digest, observedNS, deadlineNS uint64) (err error) {
	if o.Fence != b.runtime.fence {
		return ErrDomainMismatch
	}
	if o.Ref.Source != b.scope.Source || o.Ref.MediaEpoch != b.scope.MediaEpoch || o.Ref.Graph != b.scope.Graph {
		return ErrRetired
	}
	started := b.runtime.clock.NowNS()
	progress := surfacePublicationFailure{phase: "begin", begin: -1, store: -1, current: -1, finish: -1}
	defer func() {
		if err != nil {
			progress.elapsed, progress.cause = surfacePublicationElapsed(started, b.runtime.clock.NowNS()), err
			if ctx != nil {
				progress.canceled, progress.deadline = errors.Is(ctx.Err(), context.Canceled), errors.Is(ctx.Err(), context.DeadlineExceeded)
			}
			diagnostic := progress
			err = &diagnostic
		}
	}()
	store, opCtx, finish, err := b.begin(ctx)
	progress.begin = surfacePublicationElapsed(started, b.runtime.clock.NowNS())
	if err != nil {
		return err
	}
	defer func() {
		started := b.runtime.clock.NowNS()
		finish()
		progress.finish = surfacePublicationElapsed(started, b.runtime.clock.NowNS())
	}()
	progress.phase = "store"
	step := b.runtime.clock.NowNS()
	if expected == nil {
		err = store.TryPublishSurface(opCtx, o.Ref, pixels, observedNS, deadlineNS)
	} else {
		err = store.TryStageSurface(opCtx, o.Ref, pixels, *expected, observedNS, deadlineNS)
	}
	progress.store = surfacePublicationElapsed(step, b.runtime.clock.NowNS())
	step = b.runtime.clock.NowNS()
	current := b.current()
	progress.current = surfacePublicationElapsed(step, b.runtime.clock.NowNS())
	if current != nil {
		progress.phase = "current"
		return current
	}
	return err
}
