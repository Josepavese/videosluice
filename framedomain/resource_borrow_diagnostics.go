package framedomain

import (
	"context"
	"errors"
	"fmt"
)

// Stack-local checkpoints; no formatting, logging or queue on successful
// borrows. A workspace timeout alone must not be attributed to pixel copying,
// SHA or storage: begin includes process authority, admission includes the
// resource ledger, and consumer includes the separately diagnosed callback.
type resourceBorrowFailure struct {
	phase                                       string
	begin, admission, consumer, current, finish int64
	elapsed                                     int64
	cause                                       error
}

func (e *resourceBorrowFailure) Error() string {
	return fmt.Sprintf("resource_borrow_progress phase=%s begin_us=%d admission_us=%d consumer_us=%d current_us=%d finish_us=%d elapsed_us=%d context_canceled=%t context_deadline=%t: %v",
		e.phase, e.begin, e.admission, e.consumer, e.current, e.finish, e.elapsed,
		errors.Is(e.cause, context.Canceled), errors.Is(e.cause, context.DeadlineExceeded), e.cause)
}

func (e *resourceBorrowFailure) Unwrap() error { return e.cause }

// Retirement/process failure must still fence the result, but must not erase
// the bounded callback checkpoint needed to explain the incident. No result,
// retry or successful transfer is manufactured by retaining the error chain.
func retainBorrowCause(fence, callback error) error {
	if callback == nil {
		return fence
	}
	return errors.Join(fence, callback)
}
