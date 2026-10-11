//go:build linux

package linux

import (
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"testing"
)

func TestPollInterruptDoesNotRetireLiveExecution(t *testing.T) {
	calls := 0
	err := pollProcessHandle(42, func(fds []unix.PollFd, timeout int) (int, error) {
		calls++
		if len(fds) != 1 || fds[0].Fd != 42 || timeout != 0 {
			t.Fatal("poll identity or no-wait contract changed")
		}
		if calls <= 3 {
			return 0, unix.EINTR
		}
		return 0, nil
	})
	if err != nil || calls != 4 {
		t.Fatalf("live execution rejected after interrupted poll calls=%d err=%v", calls, err)
	}
}

func TestPollInterruptBudgetAndRetirementRemainFailClosed(t *testing.T) {
	calls := 0
	err := pollProcessHandle(42, func([]unix.PollFd, int) (int, error) { calls++; return 0, unix.EINTR })
	want(t, err, peer.ErrUnavailable)
	wantStage(t, err, "execution_poll_interrupted")
	if calls != 4 {
		t.Fatal("interrupt retry must be bounded at four calls", calls)
	}
	err = pollProcessHandle(42, func(fds []unix.PollFd, _ int) (int, error) { fds[0].Revents = unix.POLLIN; return 1, nil })
	want(t, err, peer.ErrRetired)
	err = pollProcessHandle(42, func([]unix.PollFd, int) (int, error) { return 0, unix.EBADF })
	want(t, err, peer.ErrUnavailable)
	wantStage(t, err, "execution_poll_error")
}
