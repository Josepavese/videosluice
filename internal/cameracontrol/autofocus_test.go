package cameracontrol

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestContinuousFocusTargetsOnlyFocusAndVerifiesOnce(t *testing.T) {
	for _, test := range []struct {
		name              string
		initial, observed int
		writeError        bool
		state             string
		wantCalls         int
	}{
		{"already enabled", 1, 1, false, "already_enabled", 1},
		{"enable", 0, 1, false, "enabled", 3},
		{"drift", 0, 0, false, "failed", 3},
		{"write error", 0, 1, true, "failed", 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			b := NewV4L2Backend(func(ctx context.Context, args ...string) ([]byte, error) {
				calls++
				if len(args) != 3 || args[0] != "-d" || args[1] != "/dev/video7" || !strings.Contains(args[2], "0x009a090c") {
					t.Fatalf("unexpected I/O %v", args)
				}
				if calls == 2 {
					if test.writeError {
						return nil, errors.New("device gone")
					}
					return nil, nil
				}
				v := test.initial
				if calls == 3 {
					v = test.observed
				}
				if v == 1 {
					return []byte("focus_automatic_continuous: 1\n"), nil
				}
				return []byte("focus_auto: 0\n"), nil
			})
			state, err := b.EnableContinuousFocus(context.Background(), "/dev/video7")
			if state != test.state || calls != test.wantCalls || (err != nil) != (test.state == "failed") {
				t.Fatalf("state=%s err=%v calls=%d", state, err, calls)
			}
		})
	}
}

func TestContinuousFocusDistinguishesAbsentMalformedAndUnavailable(t *testing.T) {
	for _, test := range []struct {
		output string
		err    error
		state  string
	}{
		{"unknown control '0x009a090c'\n", errors.New("exit 1"), "unsupported"},
		{"", context.DeadlineExceeded, "failed"},
		{"focus_auto: 7", nil, "failed"},
		{"focus_auto: 0\nbrightness: 0", nil, "failed"},
	} {
		b := NewV4L2Backend(func(context.Context, ...string) ([]byte, error) { return []byte(test.output), test.err })
		state, _ := b.EnableContinuousFocus(context.Background(), "/dev/video0")
		if state != test.state {
			t.Fatalf("output=%q state=%s", test.output, state)
		}
	}
}
