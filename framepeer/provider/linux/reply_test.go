//go:build linux

package linux

import (
	"context"
	"errors"
	peer "github.com/Josepavese/videosluice/framepeer"
	"net"
	"testing"
	"time"
)

func TestControlReplyDeadlineCancellationBusyAndRetirement(t *testing.T) {
	r, _, _ := receiver(t)
	control, err := NewControlReceiver(r.connection, r.lease, 1024, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// Child never reads this direction. Fill the bounded kernel queue and
	// observe the writer's deadline, without spawning per-write goroutines.
	deadlineObserved := false
	for i := 0; i < 64; i++ {
		err = control.Reply(context.Background(), make([]byte, 1024))
		if errors.Is(err, peer.ErrDeadline) {
			wantStage(t, err, "reply_deadline")
			deadlineObserved = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !deadlineObserved {
		t.Fatal("bounded send queue did not stop writes")
	}
	ctx, cancel := context.WithCancel(context.Background())
	stop := time.AfterFunc(5*time.Millisecond, cancel)
	err = control.Reply(ctx, []byte("cancelled"))
	stop.Stop()
	cancel()
	want(t, err, context.Canceled)
	wantStage(t, err, "reply_cancelled_after_write")
	control.reader.write.Lock()
	err = control.Reply(context.Background(), []byte("busy"))
	control.reader.write.Unlock()
	want(t, err, peer.ErrBusy)
	r.lease.Revoke()
	err = control.Reply(context.Background(), []byte("retired"))
	want(t, err, peer.ErrRetired)
}

func TestControlReplyCancellationCanFollowActualDelivery(t *testing.T) {
	r, _, file := receiver(t)
	control, err := NewControlReceiver(r.connection, r.lease, 1024, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.lease.process = &cancelAfterSecondHandle{inner: r.lease.process, cancel: cancel}
	err = control.Reply(ctx, []byte("committed-receipt"))
	want(t, err, context.Canceled)
	wantStage(t, err, "reply_cancelled_before_return")
	// Fixture reads its own duplicated opposite endpoint to prove bytes were
	// already delivered despite the error; this is not a sender-auth test.
	other, err := net.FileConn(file)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err = other.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 64)
	n, err := other.Read(buffer)
	if err != nil || string(buffer[:n]) != "committed-receipt" {
		t.Fatal("postwrite cancellation fixture lost delivery", err)
	}
}
