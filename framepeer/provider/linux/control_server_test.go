//go:build linux

package linux

import (
	"context"
	"errors"
	peer "github.com/Josepavese/videosluice/framepeer"
	"os"
	"testing"
	"time"
)

func TestControlServerPortOwnsPendingWithoutRawDescriptorEscape(t *testing.T) {
	r, controller, _ := receiver(t)
	control, err := NewControlReceiver(r.connection, r.lease, 4096, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var port peer.ControlServerConnection = control
	if generation, err := port.CurrentGeneration(); err != nil || generation != "consumer-start-1" {
		t.Fatal(generation, err)
	}
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	var packet [4096]byte
	for i := 0; i < 10; i++ {
		controller.send(t, "rights")
		n, principal, pending, err := port.ReceiveCommand(context.Background(), packet[:])
		if err != nil || n != 13 || principal.Generation != "consumer-start-1" || pending == nil {
			t.Fatal(n, principal, err)
		}
		file := pending.(*pendingCapability).file
		_, err = pending.Import("unavailable_provider", nil)
		if !errors.Is(err, peer.ErrUnavailable) {
			t.Fatal(err)
		}
		if _, err = file.Stat(); err != nil {
			t.Fatal("failed import consumed file", err)
		}
		if err = pending.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err = file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("pending file not closed", err)
		}
		if err = pending.Close(); err != nil {
			t.Fatal("idempotent close failed", err)
		}
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatal("server port leaked descriptors", len(before), len(after))
	}
	controller.send(t, "send")
	_, _, pending, err := port.ReceiveCommand(context.Background(), packet[:])
	if err != nil || pending != nil {
		t.Fatal("plain command gained capability", err)
	}
}
