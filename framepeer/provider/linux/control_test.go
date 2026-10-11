//go:build linux

package linux

import (
	"context"
	"errors"
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"os"
	"testing"
	"time"
)

func TestControlOnlyAdmitsOneFDFromItsPinnedController(t *testing.T) {
	r, controller, file := receiver(t)
	control, err := NewControlReceiver(r.connection, r.lease, 1024, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	controller.send(t, "rights")
	n, p, capability, err := control.Receive(context.Background(), make([]byte, 64))
	if err != nil || n != 13 || p.Generation != "consumer-start-1" || capability == nil {
		t.Fatal(n, p, err)
	}
	flags, err := unix.FcntlInt(capability.Fd(), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatal("received FD not CLOEXEC", err)
	}
	// Authenticated transport is not sufficient: this ordinary FD is not pidfd.
	_, err = ImportProcessCapability(capability, ProcessIdentity{PID: controller.cmd.Process.Pid, Generation: p.Generation, UID: uint32(os.Getuid()), GID: uint32(os.Getgid())})
	want(t, err, peer.ErrInvalid)
	if err = capability.Close(); err != nil {
		t.Fatal(err)
	}
	other := startChild(t, file)
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		for _, test := range []struct{ command, stage string }{
			{"rights-two", "receive_capability_cardinality"},
			{"rights-many", "receive_ancillary_truncated"},
			{"rights-large", "receive_payload_truncated"},
		} {
			controller.send(t, test.command)
			_, _, capability, err = control.Receive(context.Background(), make([]byte, 64))
			want(t, err, peer.ErrRecord)
			wantStage(t, err, test.stage)
			if capability != nil {
				capability.Close()
				t.Fatal("rejected FD escaped")
			}
		}
		other.send(t, "rights")
		_, _, capability, err = control.Receive(context.Background(), make([]byte, 64))
		want(t, err, peer.ErrUnauthorized)
		wantStage(t, err, "receive_sender_mismatch")
		if capability != nil {
			capability.Close()
			t.Fatal("unauthorized FD escaped")
		}
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatalf("control FD leak before=%d after=%d", len(before), len(after))
	}
	controller.send(t, "send")
	_, _, capability, err = control.Receive(context.Background(), make([]byte, 64))
	if err != nil || capability != nil {
		t.Fatal("FD-less revoke/status transport failed", err)
	}
}

type cancelAfterSecondHandle struct {
	inner  processHandle
	calls  int
	cancel context.CancelFunc
}

func (h *cancelAfterSecondHandle) WithHandle(visit func(uintptr)) error {
	err := h.inner.WithHandle(visit)
	h.calls++
	if h.calls == 2 {
		h.cancel()
	}
	return err
}
func TestControlCancellationDuringFinalCheckDoesNotTransferFD(t *testing.T) {
	r, controller, _ := receiver(t)
	control, err := NewControlReceiver(r.connection, r.lease, 1024, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.lease.process = &cancelAfterSecondHandle{inner: r.lease.process, cancel: cancel}
	controller.send(t, "rights")
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	n, p, capability, err := control.Receive(ctx, make([]byte, 64))
	if capability != nil {
		capability.Close()
	}
	want(t, err, context.Canceled)
	wantStage(t, err, "receive_cancelled_before_transfer")
	if n != 0 || p != (peer.Principal{}) || capability != nil {
		t.Fatal("cancelled ownership transferred")
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatal("FD leaked on cancellation")
	}
}

func wantStage(t *testing.T, err error, stage string) {
	t.Helper()
	var diagnostic *peer.Failure
	if !errors.As(err, &diagnostic) || diagnostic.Stage != stage {
		t.Fatalf("diagnostic stage got %v want %s", err, stage)
	}
	if diagnostic.Error() != "peer failure stage="+stage {
		t.Fatal("diagnostic included unbounded or private content")
	}
}
