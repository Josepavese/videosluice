// Package surfaceport defines allocation ownership, not process authority.
// Authentication and global ingress/output budgets belong to the caller.
package surfaceport

import (
	"context"
	"errors"
	frame "github.com/Josepavese/videosluice/framewire"
)

var (
	ErrUnavailable = errors.New("surface provider unavailable")
	ErrQuota       = errors.New("surface allocation or operation quota exhausted")
	ErrClosed      = errors.New("surface allocation retired")
	ErrRelease     = errors.New("surface release not confirmed")
)

// Allocation owns one mapping. Close retires it immediately and returns Busy
// while native calls/destruction are pending. A later Close observes the final
// result: nil ONLY after confirmed release, ErrRelease if release failed.
// Failed release stays charged and is never blindly retried.
// Publish borrows immutable input; SnapshotInto borrows exclusive destination.
// All caller buffers must have separate admission. No driver lease may span a
// call. Snapshot cancellation fences results, not the in-progress native copy.
// Publish checks cancellation before admission; an admitted atomic publication
// is non-cancellable and reports its actual commit result, never a false abort.
// Namespace/generation are issued by authenticated middleware, never a client.
type Allocation interface {
	Publish(context.Context, []byte) error
	SnapshotInto(context.Context, []byte) (int, error)
	Close() error
}

type Allocator interface {
	// Footprint is a validated upper bound, including provider alignment, before
	// allocation. It must equal the charge enforced by Create for this mapping.
	Footprint(frame.Mapping) (uint64, error)
	// An error may carry a non-nil partially created Allocation. The caller
	// must retain its admission until Close confirms release. A nil allocation
	// on error asserts complete rollback; nil on success is invalid.
	Create(frame.Mapping) (Allocation, error)
}
