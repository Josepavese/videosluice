//go:build linux

package linux

import (
	"context"
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"testing"
	"time"
)

type unsupportedCapability struct{}

func (unsupportedCapability) CurrentGeneration() (string, error) { return "other-provider", nil }
func (unsupportedCapability) Close() error                       { return nil }

func TestControlConnectionSendsExactBorrowedCapability(t *testing.T) {
	r, child, file := receiver(t)
	c, err := NewControlConnection(r.connection, r.lease, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	other, err := net.FileConn(file)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err = other.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = c.WriteCommand(context.Background(), []byte("grant"), r.lease); err != nil {
		t.Fatal(err)
	}
	buffer, oob := make([]byte, 32), make([]byte, unix.CmsgSpace(4))
	n, on, flags, _, err := other.(*net.UnixConn).ReadMsgUnix(buffer, oob)
	if err != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || string(buffer[:n]) != "grant" {
		t.Fatal("command record invalid", err, flags)
	}
	messages, err := unix.ParseSocketControlMessage(oob[:on])
	if err != nil || len(messages) != 1 {
		t.Fatal("expected exactly one capability", err)
	}
	rights, err := unix.ParseUnixRights(&messages[0])
	if err != nil || len(rights) != 1 {
		t.Fatal("rights invalid", err)
	}
	owned := os.NewFile(uintptr(rights[0]), "fixture-received-capability")
	fdFlags, flagErr := unix.FcntlInt(owned.Fd(), unix.F_GETFD, 0)
	if flagErr != nil || fdFlags&unix.FD_CLOEXEC == 0 {
		owned.Close()
		t.Fatal("received capability lacks atomic CLOEXEC", flagErr)
	}
	imported, err := ImportProcessCapability(owned, ProcessIdentity{PID: child.cmd.Process.Pid, UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "consumer-start-1"})
	if err != nil {
		owned.Close()
		t.Fatal(err)
	}
	if err = imported.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = r.lease.CurrentGeneration(); err != nil {
		t.Fatal("send consumed original execution handle", err)
	}
	want(t, c.WriteCommand(context.Background(), []byte("foreign"), unsupportedCapability{}), peer.ErrUnavailable)
	want(t, c.WriteCommand(context.Background(), make([]byte, 4097), nil), peer.ErrInvalid)
	want(t, c.WriteCommand(nil, []byte("invalid"), nil), peer.ErrInvalid)
	r.lease.Revoke()
	want(t, c.WriteCommand(context.Background(), []byte("retired"), nil), peer.ErrRetired)
}

func TestControlConnectionReplyRejectsCapabilities(t *testing.T) {
	r, child, _ := receiver(t)
	c, err := NewControlConnection(r.connection, r.lease, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	child.send(t, "rights")
	n, _, err := c.Receive(context.Background(), make([]byte, 16384))
	if err == nil || n != 0 {
		t.Fatal("reply accepted a capability", n, err)
	}
}
