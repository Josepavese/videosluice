package framedomain

import fa "github.com/Josepavese/videosluice/frameaccess"

// RequestDeadline treats the client's duration as an upper bound, not a
// promise to spend that time. A stricter admitted source budget always wins;
// the client never has to guess a provider's private copy limit. Exact binding
// retirement and runtime-clock overflow remain errors; no request is retried.
func (b *Binding) RequestDeadline(budgetNS uint64) (uint64, error) {
	if b == nil || budgetNS == 0 || b.maxCopyNS == 0 {
		return 0, ErrInvalid
	}
	if err := b.ValidateCurrent(); err != nil {
		return 0, err
	}
	if budgetNS > b.maxCopyNS {
		budgetNS = b.maxCopyNS
	}
	return b.runtime.RequestDeadline(b.Fence(), budgetNS)
}

// RequestDeadline translates a duration with this runtime's admitted clock,
// not a clock asserted by a remote consumer. Source MaxCopyNS remains enforced
// at extraction. No queueing/retry is permitted to refresh the same deadline.
func (r *Runtime) RequestDeadline(fence Fence, budgetNS uint64) (uint64, error) {
	if r == nil || budgetNS == 0 {
		return 0, ErrInvalid
	}
	if err := r.checkFence(fence); err != nil {
		return 0, err
	}
	now := r.clock.NowNS()
	if budgetNS > ^uint64(0)-now {
		return 0, fa.DeadlineExceeded
	}
	return now + budgetNS, nil
}
