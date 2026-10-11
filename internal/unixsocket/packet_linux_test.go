//go:build linux

package unixsocket

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestPacketOwnerCredentialsAndCheckedCleanup(t *testing.T) {
	root, err := os.MkdirTemp("", "packet-owner-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	path := filepath.Join(root, "reader.sock")
	owner, err := ListenPacketSingleOwner(path)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Cleanup()
	if other, err := ListenPacketSingleOwner(path); err == nil || other != nil {
		t.Fatal("replaced live listener")
	}
	// The liveness probe itself connects; consume its EOF before the real client.
	probe, err := owner.AcceptPacket(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	probe.Close()
	client, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	accepted, err := owner.AcceptPacket(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	var data [8]byte
	oob := make([]byte, unix.CmsgSpace(unix.SizeofUcred))
	n, oobn, _, _, err := accepted.ReadMsgUnix(data[:], oob)
	if err != nil || string(data[:n]) != "hello" {
		t.Fatal(err)
	}
	messages, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil || len(messages) != 1 {
		t.Fatal("missing credentials", err)
	}
	credentials, err := unix.ParseUnixCredentials(&messages[0])
	if err != nil || int(credentials.Pid) != os.Getpid() {
		t.Fatal("wrong peer", err)
	}
	if err := owner.CleanupChecked(); err != nil {
		t.Fatal(err)
	}
	if err := owner.CleanupChecked(); err != nil {
		t.Fatal("non-idempotent cleanup", err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("endpoint not removed", err)
	}
}

func TestPacketOwnerPartialSetupHasReleaseOwner(t *testing.T) {
	root, err := os.MkdirTemp("", "packet-partial-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	path := filepath.Join(root, "reader.sock")
	owner, err := listenSingleOwner(path, "unixpacket", func(*net.UnixListener) error { return os.ErrPermission }, true)
	if err == nil || owner == nil {
		t.Fatal("partial listener ownership lost")
	}
	if err := owner.CleanupChecked(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("partial endpoint not removed")
	}
}
