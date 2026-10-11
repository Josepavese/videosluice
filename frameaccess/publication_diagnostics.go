package frameaccess

import (
	"context"
	"errors"
	"fmt"
)

// Wall times diagnose waits; they are not CPU or hardware execution times.
// Only an error allocates/formats this record. The unchanged semantic Code is
// unwrapped for admission, wire responses and callers' errors.Is/errors.As.
type publicationProgress struct {
	phase                                           string
	clock                                           Clock
	started, copyStarted, copyEnded, commitAcquired publicationPoint
}

type publicationPoint struct {
	ns    uint64
	valid bool
}

func (p publicationProgress) now() publicationPoint {
	if p.clock == nil {
		return publicationPoint{}
	}
	return publicationPoint{ns: p.clock.NowNS(), valid: true}
}

type publicationFailure struct {
	progress           publicationProgress
	ended              publicationPoint
	canceled, deadline bool
	cause              error
}

func publicationElapsed(a, b publicationPoint) int64 {
	if !a.valid || !b.valid || b.ns < a.ns {
		return -1
	}
	return int64((b.ns - a.ns) / 1000)
}

func (e *publicationFailure) Error() string {
	p := e.progress
	return fmt.Sprintf("surface_publish_progress phase=%s admission_us=%d copy_us=%d commit_wait_us=%d finalization_us=%d elapsed_us=%d context_canceled=%t context_deadline=%t: %v",
		p.phase, publicationElapsed(p.started, p.copyStarted), publicationElapsed(p.copyStarted, p.copyEnded),
		publicationElapsed(p.copyEnded, p.commitAcquired), publicationElapsed(p.commitAcquired, e.ended),
		publicationElapsed(p.started, e.ended), e.canceled, e.deadline, e.cause)
}
func (e *publicationFailure) Unwrap() error { return e.cause }

func (p publicationProgress) failure(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	var canceled, deadline bool
	if ctx != nil {
		canceled, deadline = errors.Is(ctx.Err(), context.Canceled), errors.Is(ctx.Err(), context.DeadlineExceeded)
	}
	return &publicationFailure{progress: p, ended: p.now(), canceled: canceled, deadline: deadline, cause: err}
}
