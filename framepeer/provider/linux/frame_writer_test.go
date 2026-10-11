//go:build linux

package linux

import (
	"context"
	"errors"
	peer "github.com/Josepavese/videosluice/framepeer"
	"testing"
	"time"
)

func TestFrameWriterDeadlineCancellationBusyAndSocketMutation(t *testing.T) {
	r, _, _ := receiver(t)
	r.maxWait = 30 * time.Millisecond
	w, err := NewFrameWriter(r)
	if err != nil {
		t.Fatal(err)
	}
	observed := false
	for i := 0; i < 64; i++ {
		err = w.WritePacket(context.Background(), make([]byte, 1024))
		if errors.Is(err, peer.ErrDeadline) {
			observed = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !observed {
		t.Fatal("writer never met backpressure deadline")
	}
	ctx, cancel := context.WithCancel(context.Background())
	stop := time.AfterFunc(5*time.Millisecond, cancel)
	err = w.WritePacket(ctx, []byte("cancel"))
	stop.Stop()
	cancel()
	want(t, err, context.Canceled)
	r.write.Lock()
	err = w.WritePacket(context.Background(), []byte("busy"))
	r.write.Unlock()
	want(t, err, peer.ErrBusy)
	_, err = ConfigurePacketSocket(r.connection, 65536)
	if err != nil {
		t.Fatal(err)
	}
	err = w.WritePacket(context.Background(), []byte("changed"))
	wantStage(t, err, "socket_limit_changed")
	_, err = ConfigurePacketSocket(r.connection, 32768)
	if err != nil {
		t.Fatal(err)
	}
	r.lease.Revoke()
	want(t, w.WritePacket(context.Background(), []byte("retired")), peer.ErrRetired)
	_, err = NewFrameWriter(nil)
	want(t, err, peer.ErrInvalid)
	want(t, (*FrameWriter)(nil).WritePacket(context.Background(), []byte("nil")), peer.ErrInvalid)
}
