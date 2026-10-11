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

func TestPacketSocketReadbackAndChangedLimitsFailClosed(t *testing.T) {
	r, c, _ := receiver(t)
	control, err := NewControlReceiver(r.connection, r.lease, 1024, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if control.limits.SendAccountingBytes != 65536 {
		t.Fatal("unexpected kernel readback", control.limits)
	}
	if err := ValidatePacketSocket(r.connection, control.limits); err != nil {
		t.Fatal(err)
	}
	if _, err := ConfigurePacketSocket(r.connection, 65536); err != nil {
		t.Fatal(err)
	}
	err = control.Reply(context.Background(), []byte("blocked"))
	want(t, err, peer.ErrUnavailable)
	wantStage(t, err, "socket_limit_changed")
	n, p, file, err := control.Receive(context.Background(), make([]byte, 64))
	want(t, err, peer.ErrUnavailable)
	if n != 0 || p != (peer.Principal{}) || file != nil {
		t.Fatal("changed contract exposed data")
	}
	if _, err := ConfigurePacketSocket(r.connection, 32768); err != nil {
		t.Fatal(err)
	}
	// Reconfigure during final process check, after recvmsg installed a raw FD.
	r.lease.process = &cancelAfterSecondHandle{inner: r.lease.process, cancel: func() {
		if _, err := ConfigurePacketSocket(r.connection, 65536); err != nil {
			t.Fatal(err)
		}
	}}
	c.send(t, "rights")
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	n, p, file, err = control.Receive(context.Background(), make([]byte, 64))
	want(t, err, peer.ErrUnavailable)
	wantStage(t, err, "socket_limit_changed")
	if n != 0 || p != (peer.Principal{}) || file != nil {
		if file != nil {
			file.Close()
		}
		t.Fatal("postreceive changed contract transferred FD")
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatal("FD accounting changed", len(before), len(after))
	}
}

func TestUNIXReceiveSettingDoesNotBoundIncomingQueue(t *testing.T) {
	// Owned nonblocking pairs only. Observe queued payload, not total RAM.
	queued := func(rcv int) (messages, actualReceive int) {
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Close(fds[0])
		defer unix.Close(fds[1])
		if err := unix.SetsockoptInt(fds[0], unix.SOL_SOCKET, unix.SO_SNDBUF, 32768); err != nil {
			t.Fatal(err)
		}
		if err := unix.SetsockoptInt(fds[1], unix.SOL_SOCKET, unix.SO_RCVBUF, rcv); err != nil {
			t.Fatal(err)
		}
		actualReceive, err = unix.GetsockoptInt(fds[1], unix.SOL_SOCKET, unix.SO_RCVBUF)
		if err != nil {
			t.Fatal(err)
		}
		packet := make([]byte, 1024)
		for attempt := 0; attempt < 10000; attempt++ {
			n, err := unix.Write(fds[0], packet)
			if errors.Is(err, unix.EINTR) {
				continue
			}
			if errors.Is(err, unix.EAGAIN) {
				return messages, actualReceive
			}
			if err != nil || n != len(packet) {
				t.Fatal("unexpected queue write", n, err)
			}
			messages++
		}
		t.Fatal("send-side queue did not stop bounded probe")
		return
	}
	small, smallSetting := queued(1024)
	large, largeSetting := queued(131072)
	t.Logf("receiver_readback_small=%d receiver_readback_large=%d queued_messages_small=%d queued_messages_large=%d payload_bytes_per_message=1024", smallSetting, largeSetting, small, large)
	if smallSetting >= largeSetting || small <= 0 || small != large {
		t.Fatal("unexpected UNIX queue behavior; do not qualify this platform", smallSetting, largeSetting, small, large)
	}
}
