//go:build linux

package linux

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestCapabilityExportImportOwnsOnlyItsDuplicate(t *testing.T) {
	r, _, _ := receiver(t)
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		file, identity, err := r.lease.Export()
		if err != nil {
			t.Fatal(err)
		}
		flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Fatal("export not CLOEXEC", err)
		}
		imported, err := ImportProcessCapability(file, identity)
		if err != nil {
			file.Close()
			t.Fatal(err)
		}
		generation, err := imported.CurrentGeneration()
		if err != nil || generation != identity.Generation {
			t.Fatal(generation, err)
		}
		if err = imported.Close(); err != nil {
			t.Fatal(err)
		}
		if err = imported.Close(); err != nil {
			t.Fatal(err)
		}
		_, err = imported.CurrentGeneration()
		want(t, err, peer.ErrRetired)
		if _, err = r.lease.CurrentGeneration(); err != nil {
			t.Fatal("launcher handle damaged", err)
		}
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatalf("capability FD leak before=%d after=%d", len(before), len(after))
	}
}

func TestCapabilityRejectsSubstitutionAndLateImport(t *testing.T) {
	r, c, file := receiver(t)
	other := startChild(t, file)
	capability, identity, err := r.lease.Export()
	if err != nil {
		t.Fatal(err)
	}
	defer capability.Close()
	bad := identity
	bad.PID = other.cmd.Process.Pid
	_, err = ImportProcessCapability(capability, bad)
	want(t, err, peer.ErrUnauthorized)
	// Error does not take caller ownership.
	if _, err = unix.FcntlInt(capability.Fd(), unix.F_GETFD, 0); err != nil {
		t.Fatal(err)
	}
	ordinary, err := os.Open("/dev/null")
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Close()
	_, err = ImportProcessCapability(ordinary, identity)
	want(t, err, peer.ErrInvalid)
	memfd, err := unix.MemfdCreate("pidfd", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	forged := os.NewFile(uintptr(memfd), "pidfd")
	defer forged.Close()
	if _, err = fmt.Fprintf(forged, "Pid:\t%d\n", identity.PID); err != nil {
		t.Fatal(err)
	}
	_, err = ImportProcessCapability(forged, identity)
	want(t, err, peer.ErrInvalid)
	bad = identity
	bad.Generation = "untrusted/invalid"
	_, err = ImportProcessCapability(capability, bad)
	want(t, err, peer.ErrInvalid)
	fmt.Fprintln(c.input, "exit")
	if err = c.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	_, err = ImportProcessCapability(capability, identity)
	want(t, err, peer.ErrRetired)
	_, _, err = r.lease.Export()
	want(t, err, peer.ErrRetired)
}

// Bootstrap metadata in this fixture is inherited from the trusted launcher,
// not received on a public RPC channel. The two child processes are siblings.
func TestCapabilityBrokerChild(t *testing.T) {
	if os.Getenv("FAP_CAPABILITY_BROKER") != "1" {
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		os.Exit(10)
	}
	var identity ProcessIdentity
	if json.Unmarshal(scanner.Bytes(), &identity) != nil {
		os.Exit(11)
	}
	capability := os.NewFile(4, "inherited-pidfd")
	lease, err := ImportProcessCapability(capability, identity)
	if err != nil {
		capability.Close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(12)
	}
	defer lease.Close()
	flags, err := unix.FcntlInt(capability.Fd(), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		os.Exit(18)
	}
	socket := os.NewFile(3, "inherited-consumer-socket")
	connection, err := net.FileConn(socket)
	_ = socket.Close()
	if err != nil {
		os.Exit(13)
	}
	defer connection.Close()
	receiver, err := NewReceiver(connection.(*net.UnixConn), lease, 1024, time.Second)
	if err != nil {
		os.Exit(14)
	}
	fmt.Println("sent")
	for scanner.Scan() {
		switch scanner.Text() {
		case "receive":
			buffer := make([]byte, 64)
			n, principal, err := receiver.Receive(context.Background(), buffer)
			if err != nil || principal.Generation != identity.Generation || string(buffer[:n]) != "frame-request" {
				os.Exit(15)
			}
		case "retired":
			if _, err := lease.CurrentGeneration(); !errors.Is(err, peer.ErrRetired) {
				os.Exit(16)
			}
		default:
			os.Exit(17)
		}
		fmt.Println("sent")
	}
}

func TestLauncherCapabilityHandoffToSiblingBroker(t *testing.T) {
	r, consumer, _ := receiver(t)
	capability, identity, err := r.lease.Export()
	if err != nil {
		t.Fatal(err)
	}
	defer capability.Close()
	socket, err := r.connection.File()
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCapabilityBrokerChild$")
	cmd.Env = append(os.Environ(), "FAP_CAPABILITY_BROKER=1")
	cmd.ExtraFiles = []*os.File{socket, capability}
	cmd.Stderr = os.Stderr
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	if cmd.Process.Pid == consumer.cmd.Process.Pid {
		t.Fatal("not separate sibling processes")
	}
	broker := &child{cmd, input, bufio.NewReader(output)}
	packet, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	broker.send(t, string(packet))
	consumer.send(t, "send")
	broker.send(t, "receive")
	fmt.Fprintln(consumer.input, "exit")
	if err = consumer.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	broker.send(t, "retired")
}
