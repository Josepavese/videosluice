//go:build linux

package ingress_test

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	in "github.com/Josepavese/videosluice/frameingress"
	"fmt"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
	"golang.org/x/sys/unix"
	"io"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestGrantControllerChild(t *testing.T) {
	if os.Getenv("FAP_GRANT_CONTROLLER") != "1" {
		return
	}
	file := os.NewFile(3, "control-connection")
	conn, err := net.FileConn(file)
	file.Close()
	if err != nil {
		os.Exit(20)
	}
	defer conn.Close()
	capability := os.NewFile(4, "consumer-capability")
	var consumerLease *linux.Lease
	defer func() {
		if consumerLease != nil {
			consumerLease.Close()
		} else {
			capability.Close()
		}
	}()
	// Fixture bootstrap: pidfd of the test parent (opened for itself), passed
	// as a distinct owned inherited FD. Never reopen an input-provided PID.
	brokerFile := os.NewFile(5, "broker-capability")
	broker, err := linux.ImportProcessCapability(brokerFile, linux.ProcessIdentity{PID: os.Getppid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "authenticated-process-1"})
	if err != nil {
		brokerFile.Close()
		os.Exit(23)
	}
	defer broker.Close()
	replies, err := linux.NewControlConnection(conn.(*net.UnixConn), broker, time.Second)
	if err != nil {
		os.Exit(24)
	}
	scanner := bufio.NewScanner(os.Stdin)
	firstReply := true
	for scanner.Scan() {
		data, err := hex.DecodeString(scanner.Text())
		if err != nil {
			os.Exit(21)
		}
		packet, err := in.DecodeControl(data)
		if err != nil {
			os.Exit(21)
		}
		var borrowed peer.ProcessCapability
		if packet.Kind == in.ControlGrant {
			if consumerLease == nil {
				identity, e := linux.DecodeProcessIdentity(packet.CapabilityMetadata)
				if e != nil {
					os.Exit(27)
				}
				consumerLease, err = linux.ImportProcessCapability(capability, identity)
				if err != nil {
					os.Exit(28)
				}
			}
			borrowed = consumerLease
		}
		if err = replies.WriteCommand(context.Background(), data, borrowed); err != nil {
			os.Exit(22)
		}
		buffer := make([]byte, in.MaxControlReplyBytes)
		n, sender, err := replies.Receive(context.Background(), buffer)
		if firstReply && errors.Is(err, peer.ErrDeadline) {
			firstReply = false
			fmt.Println("response-timeout")
			continue
		}
		firstReply = false
		if err != nil || sender.Generation != "authenticated-process-1" {
			os.Exit(25)
		}
		reply, err := in.DecodeControlReply(buffer[:n])
		if err != nil || in.ValidateControlReply(reply, packet) != nil {
			os.Exit(26)
		}
		fmt.Println(hex.EncodeToString(buffer[:n]))
	}
}
func integrationPair(t *testing.T) (*net.UnixConn, *os.File) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	must(t, err)
	left, right := os.NewFile(uintptr(fds[0]), "parent"), os.NewFile(uintptr(fds[1]), "child")
	connection, err := net.FileConn(left)
	left.Close()
	must(t, err)
	t.Cleanup(func() { connection.Close(); right.Close() })
	return connection.(*net.UnixConn), right
}
func integrationChild(t *testing.T, testName, marker string, files []*os.File) (*exec.Cmd, io.WriteCloser, *bufio.Reader) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+testName+"$")
	cmd.Env = append(os.Environ(), marker+"=1")
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = files
	input, err := cmd.StdinPipe()
	must(t, err)
	output, err := cmd.StdoutPipe()
	must(t, err)
	must(t, cmd.Start())
	t.Cleanup(func() { input.Close(); cmd.Process.Kill(); cmd.Wait() })
	return cmd, input, bufio.NewReader(output)
}

func TestAuthenticatedControlCapabilityGrantROIReplayAndRevoke(t *testing.T) {
	f, _, _, q := accessFixture(t)
	_, consumerSocket := integrationPair(t)
	consumer, _, _ := integrationChild(t, "TestAccessPeerChild", "FAP_ACCESS_CHILD", []*os.File{consumerSocket})
	consumerLease, err := linux.BindStartedProcess(consumer.Process, "consumer-start-1", uint32(os.Getuid()), uint32(os.Getgid()))
	must(t, err)
	capability, identity, err := consumerLease.Export()
	must(t, err)
	defer capability.Close()
	metadata, err := linux.EncodeProcessIdentity(identity)
	must(t, err)
	controlConnection, controllerSocket := integrationPair(t)
	// Self-open is restricted to this fixture's currently executing process.
	selfFD, err := unix.PidfdOpen(os.Getpid(), 0)
	must(t, err)
	self := os.NewFile(uintptr(selfFD), "fixture-broker-self")
	defer self.Close()
	controller, input, returned := integrationChild(t, "TestGrantControllerChild", "FAP_GRANT_CONTROLLER", []*os.File{controllerSocket, capability, self})
	controllerLease, err := linux.BindStartedProcess(controller.Process, "controller-start-1", uint32(os.Getuid()), uint32(os.Getgid()))
	must(t, err)
	receiver, err := linux.NewControlReceiver(controlConnection, controllerLease, 4096, time.Second)
	must(t, err)
	c, err := in.NewGrantControl(f.r, controllerLease, "controller-start-1", 4)
	must(t, err)
	defer c.Close()
	dispatch, err := in.NewControlDispatcher(c)
	must(t, err)
	defer dispatch.Close()
	before, err := os.ReadDir("/proc/self/fd")
	must(t, err)
	var access *in.Access
	for _, step := range []string{"grant", "status", "replay", "revoke"} {
		header := command(f, 1, 0)
		operation := "grant"
		if step == "revoke" {
			header = command(f, 2, 1)
			operation = "revoke"
		}
		packet := in.ControlCommand{Kind: in.ControlRevoke, Header: header}
		if operation == "grant" {
			packet.Kind = in.ControlGrant
			packet.Binding = f.b.Reference()
			packet.Policy = controlPolicy(identity.Generation)
			packet.CapabilityType = linux.ProcessCapabilityType
			packet.CapabilityMetadata = metadata
		}
		if step == "status" {
			operation = "status"
			packet = in.ControlCommand{Kind: in.ControlStatus, Header: command(f, 0, 0)}
			packet.Header.Grant = ""
		}
		data, err := in.EncodeControl(packet)
		must(t, err)
		_, err = fmt.Fprintln(input, hex.EncodeToString(data))
		must(t, err)
		buffer := make([]byte, 4096)
		n, sender, received, err := receiver.Receive(context.Background(), buffer)
		must(t, err)
		var pending peer.PendingCapability
		if received != nil {
			pending, err = linux.OwnReceivedCapability(received)
			must(t, err)
		}
		reply, accepted, err := dispatch.Apply(context.Background(), sender, buffer[:n], pending)
		if !accepted {
			if pending != nil {
				pending.Close()
			}
			t.Fatal("dispatch unexpectedly refused admitted transport", err)
		}
		wireReply, replyErr := in.BuildControlReply(f.r.Fence(), reply, err)
		must(t, replyErr)
		encodedReply, replyErr := in.EncodeControlReply(wireReply)
		must(t, replyErr)
		if step == "grant" {
			// Actual command committed, but the response cannot be sent. The
			// controller must time out then reconcile through an authenticated
			// Status exchange, not issue another grant or undo the first one.
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			want(t, receiver.Reply(cancelled, encodedReply), context.Canceled)
		} else {
			must(t, receiver.Reply(context.Background(), encodedReply))
		}
		ack := make(chan string, 1)
		go func() { line, _ := returned.ReadString('\n'); ack <- line }()
		select {
		case line := <-ack:
			expected := hex.EncodeToString(encodedReply) + "\n"
			if step == "grant" {
				expected = "response-timeout\n"
			}
			if line != expected {
				t.Fatal("controller did not validate exact authenticated reply", line)
			}
		case <-time.After(3 * time.Second):
			controller.Process.Kill()
			<-ack
			t.Fatal("controller reply validation timeout")
		}
		if operation == "grant" {
			if step == "replay" {
				want(t, err, in.ErrControlSequence)
				continue
			}
			outcome(t, reply.Receipt, err, "installed")
			access, err = c.Lookup(principal(), "reader")
			must(t, err)
			result, err := access.Extract(context.Background(), principal(), q)
			must(t, err)
			must(t, result.Visit(0, func(a fd.Artifact, p []byte) error {
				if len(p) != 24 {
					t.Fatal("native ROI mismatch")
				}
				return fd.VerifyDomainProof(a.Proof, f.b.Fence())
			}))
			result.Close()
		} else if operation == "status" {
			must(t, err)
			if reply.Kind != in.ControlStatus || reply.ID != packet.Header.ID || reply.Stats.Receipt.Sequence != 1 || reply.Stats.Active != 1 {
				t.Fatal("status did not reconcile commit", reply)
			}
		} else {
			outcome(t, reply.Receipt, err, "revoked")
			_, err = access.Extract(context.Background(), principal(), q)
			want(t, err, peer.ErrRetired)
		}
	}
	awaitControl(t, c, func(s in.ControlStats) bool { return s.Active == 0 && s.Retiring == 0 && s.Receipt.Sequence == 2 })
	after, err := os.ReadDir("/proc/self/fd")
	must(t, err)
	if len(before) != len(after) {
		t.Fatalf("capability ownership leak before=%d after=%d", len(before), len(after))
	}
}
