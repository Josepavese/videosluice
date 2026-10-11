//go:build linux

package linux

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	peer "github.com/Josepavese/videosluice/framepeer"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Real child process: all sockets, FDs and signals belong to this fixture.
func TestPeerChild(t *testing.T) {
	if os.Getenv("FAP_PEER_CHILD") != "1" {
		return
	}
	f := os.NewFile(3, "fixture-packet")
	conn, err := net.FileConn(f)
	_ = f.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "peer fixture stage=import_socket error=%v\n", err)
		os.Exit(2)
	}
	c := conn.(*net.UnixConn)
	defer c.Close()
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		command := scanner.Text()
		if command == "exit" {
			return
		}
		payload := []byte("frame-request")
		var oob []byte
		if strings.HasPrefix(command, "rights") {
			if command == "rights-large" {
				payload = make([]byte, 1024)
			}
			file, e := os.Open("/dev/null")
			if e != nil {
				fmt.Fprintf(os.Stderr, "peer fixture stage=open_capability error=%v\n", e)
				os.Exit(3)
			}
			count := 1
			if command == "rights-many" {
				count = 20
			}
			if command == "rights-two" {
				count = 2
			}
			fds := make([]int, count)
			for i := range fds {
				fds[i] = int(file.Fd())
			}
			oob = unix.UnixRights(fds...)
			_, _, err = c.WriteMsgUnix(payload, oob, nil)
			_ = file.Close()
		} else {
			if command == "large" {
				payload = make([]byte, 1024)
			}
			_, err = c.Write(payload)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "peer fixture stage=send command=%s error=%v\n", command, err)
			os.Exit(4)
		}
		fmt.Println("sent")
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "peer fixture stage=scan error=%v\n", err)
	}
}

type child struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	output *bufio.Reader
}

func startChild(t *testing.T, socket *os.File) *child {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPeerChild$")
	cmd.Env = append(os.Environ(), "FAP_PEER_CHILD=1")
	cmd.Stderr = os.Stderr // retain sanitizer/runtime failures from owned helpers
	cmd.ExtraFiles = []*os.File{socket}
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = in.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() })
	return &child{cmd, in, bufio.NewReader(out)}
}
func (c *child) send(t *testing.T, command string) {
	t.Helper()
	if _, err := fmt.Fprintln(c.input, command); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		line, err := c.output.ReadString('\n')
		if err == nil && line != "sent\n" {
			err = fmt.Errorf("unexpected helper reply %q", line)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		_ = c.cmd.Process.Kill()
		<-done
		t.Fatal("fixture helper timeout")
	}
}
func packetPair(t *testing.T) (*net.UnixConn, *os.File) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	a, b := os.NewFile(uintptr(fds[0]), "receiver"), os.NewFile(uintptr(fds[1]), "sender")
	c, err := net.FileConn(a)
	_ = a.Close()
	if err != nil {
		_ = b.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(); _ = b.Close() })
	return c.(*net.UnixConn), b
}
func receiver(t *testing.T) (*Receiver, *child, *os.File) {
	t.Helper()
	conn, file := packetPair(t)
	c := startChild(t, file)
	l, err := BindStartedProcess(c.cmd.Process, "consumer-start-1", uint32(os.Getuid()), uint32(os.Getgid()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	r, err := NewReceiver(conn, l, 2048, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return r, c, file
}
func want(t *testing.T, err, expected error) {
	t.Helper()
	if !errors.Is(err, expected) {
		t.Fatalf("got %v want %v", err, expected)
	}
}

func TestAuthenticatedRecordsRejectForwardedSocketFromSameUID(t *testing.T) {
	r, c, file := receiver(t)
	other := startChild(t, file)
	for i := 0; i < 20; i++ {
		c.send(t, "send")
		buffer := make([]byte, 64)
		n, p, err := r.Receive(context.Background(), buffer)
		if err != nil || p.Generation != "consumer-start-1" || string(buffer[:n]) != "frame-request" {
			t.Fatal(n, p, err)
		}
	}
	other.send(t, "send")
	n, p, err := r.Receive(context.Background(), make([]byte, 64))
	want(t, err, peer.ErrUnauthorized)
	if n != 0 || p != (peer.Principal{}) {
		t.Fatal("unauthorized data exposed")
	}
}

func TestRetiredExecutionCannotDrainItsQueuedRequest(t *testing.T) {
	r, c, _ := receiver(t)
	c.send(t, "send")
	fmt.Fprintln(c.input, "exit")
	if err := c.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	n, _, err := r.Receive(context.Background(), make([]byte, 64))
	want(t, err, peer.ErrRetired)
	if n != 0 {
		t.Fatal("retired request accepted")
	}
}

func TestRevocationAndUIDMismatch(t *testing.T) {
	t.Run("uid", func(t *testing.T) {
		r, c, _ := receiver(t)
		r.lease.uid++
		c.send(t, "send")
		_, _, err := r.Receive(context.Background(), make([]byte, 64))
		want(t, err, peer.ErrUnauthorized)
	})
	t.Run("revoked", func(t *testing.T) {
		r, c, _ := receiver(t)
		c.send(t, "send")
		r.lease.Revoke()
		_, _, err := r.Receive(context.Background(), make([]byte, 64))
		want(t, err, peer.ErrRetired)
	})
}

func TestTruncatedRecordsAndUnsolicitedFDsAreRejectedWithoutLeaks(t *testing.T) {
	r, c, _ := receiver(t)
	c.send(t, "large")
	_, _, err := r.Receive(context.Background(), make([]byte, 64))
	want(t, err, peer.ErrRecord)
	// Warm netpoll and all fixture descriptors before the FD-count comparison.
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, credentials := range []int{1, 0} {
		raw, _ := r.connection.SyscallConn()
		if err := raw.Control(func(fd uintptr) {
			if e := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, credentials); e != nil {
				t.Error(e)
			}
		}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 50; i++ {
			command := "rights"
			if i%2 == 1 {
				command = "rights-many"
			}
			c.send(t, command)
			_, _, err := r.Receive(context.Background(), make([]byte, 64))
			want(t, err, peer.ErrRecord)
		}
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatalf("FD leak before=%d after=%d", len(before), len(after))
	}
}

func TestCancellationBoundedReadAndSingleFlight(t *testing.T) {
	r, c, _ := receiver(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, err := r.Receive(ctx, make([]byte, 64)); done <- err }()
	deadline := time.Now().Add(time.Second)
	for {
		if !r.read.TryLock() {
			break
		}
		r.read.Unlock()
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("read not started")
		}
		time.Sleep(time.Millisecond)
	}
	_, _, err := r.Receive(context.Background(), make([]byte, 64))
	want(t, err, peer.ErrBusy)
	cancel()
	select {
	case err := <-done:
		want(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancellation stuck")
	}
	c.send(t, "send")
	_, _, err = r.Receive(context.Background(), make([]byte, 64))
	if err != nil {
		t.Fatal("cancel callback corrupted next read", err)
	}
	r.maxWait = 20 * time.Millisecond
	started := time.Now()
	_, _, err = r.Receive(context.Background(), make([]byte, 64))
	want(t, err, peer.ErrDeadline)
	if time.Since(started) > time.Second {
		t.Fatal("unbounded read")
	}
}

func TestCancellationBeforeReadProvesQueuedRecordUntouched(t *testing.T) {
	r, c, _ := receiver(t)
	c.send(t, "send")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, _, err := r.Receive(ctx, make([]byte, 64))
	if n != 0 || !errors.Is(err, context.Canceled) || !errors.Is(err, peer.ErrNoRecord) {
		t.Fatal("missing exact pre-read cancellation proof", n, err)
	}
	n, _, err = r.Receive(context.Background(), make([]byte, 64))
	if err != nil || n == 0 {
		t.Fatal("cancelled receive consumed the queued authenticated record", n, err)
	}
}

func TestInvalidAdmissionAndMissingCredentials(t *testing.T) {
	var empty *Lease
	empty.Revoke()
	_, err := empty.CurrentGeneration()
	want(t, err, peer.ErrInvalid)
	_, err = BindStartedProcess(nil, "start", 0, 0)
	want(t, err, peer.ErrInvalid)
	r, c, _ := receiver(t)
	for _, generation := range []string{"", "start/evil", strings.Repeat("x", 65)} {
		_, err = BindStartedProcess(c.cmd.Process, generation, uint32(os.Getuid()), uint32(os.Getgid()))
		want(t, err, peer.ErrInvalid)
	}
	_, err = NewReceiver(r.connection, &Lease{}, 2048, time.Second)
	want(t, err, peer.ErrInvalid)
	_, err = NewReceiver(r.connection, r.lease, 65537, time.Second)
	want(t, err, peer.ErrInvalid)
	_, _, err = r.Receive(nil, make([]byte, 64))
	want(t, err, peer.ErrInvalid)
	_, _, err = r.Receive(context.Background(), make([]byte, 2049))
	want(t, err, peer.ErrInvalid)
	raw, _ := r.connection.SyscallConn()
	if err = raw.Control(func(fd uintptr) {
		if e := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 0); e != nil {
			t.Error(e)
		}
	}); err != nil {
		t.Fatal(err)
	}
	c.send(t, "send")
	_, _, err = r.Receive(context.Background(), make([]byte, 64))
	want(t, err, peer.ErrRecord)
}

func TestStreamSocketIsNotAcceptedAsAuthenticatedRecordTransport(t *testing.T) {
	r, _, _ := receiver(t)
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	f := os.NewFile(uintptr(fds[0]), "stream-fixture")
	defer unix.Close(fds[1])
	c, err := net.FileConn(f)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_, err = NewReceiver(c.(*net.UnixConn), r.lease, 1024, time.Second)
	want(t, err, peer.ErrInvalid)
	var diagnostic *peer.Failure
	if !errors.As(err, &diagnostic) || diagnostic.Stage != "transport_type" {
		t.Fatal(err)
	}
}
