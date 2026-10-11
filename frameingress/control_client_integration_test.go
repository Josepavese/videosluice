//go:build linux

package ingress_test

import (
	"bufio"
	"context"
	"encoding/json"
	in "github.com/Josepavese/videosluice/frameingress"
	"fmt"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
	"golang.org/x/sys/unix"
	"net"
	"os"
	"testing"
	"time"
)

// Fixture-only broker process: its owned stdout bootstrap carries BindingRef;
// process authority comes from the independently inherited parent pidfd. The
// first grant is committed then its ACK/socket are deliberately discarded.
func TestControlClientBrokerChild(t *testing.T) {
	if os.Getenv("FAP_CONTROL_CLIENT_BROKER") != "1" {
		return
	}
	f, _, _, _ := accessFixture(t)
	parentFile := os.NewFile(5, "fixture-controller-capability")
	parent, err := linux.ImportProcessCapability(parentFile, linux.ProcessIdentity{PID: os.Getppid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "fixture-controller-1"})
	must(t, err)
	defer parent.Close()
	control, err := in.NewGrantControl(f.r, parent, "fixture-controller-1", 4)
	must(t, err)
	dispatch, err := in.NewControlDispatcher(control)
	must(t, err)
	defer dispatch.Close()
	must(t, json.NewEncoder(os.Stdout).Encode(f.b.Reference()))
	for index := 0; index < 2; index++ {
		file := os.NewFile(uintptr(3+index), "fixture-control-channel")
		conn, err := net.FileConn(file)
		file.Close()
		must(t, err)
		receiver, err := linux.NewControlReceiver(conn.(*net.UnixConn), parent, in.MaxControlBytes, time.Second)
		must(t, err)
		count := 1
		if index == 1 {
			count = 2
		}
		for i := 0; i < count; i++ {
			buffer := make([]byte, in.MaxControlBytes)
			n, principal, received, err := receiver.Receive(context.Background(), buffer)
			must(t, err)
			var pending peer.PendingCapability
			if received != nil {
				pending, err = linux.OwnReceivedCapability(received)
				must(t, err)
			}
			result, accepted, operationErr := dispatch.Apply(context.Background(), principal, buffer[:n], pending)
			if !accepted {
				if pending != nil {
					pending.Close()
				}
				t.Fatal("dispatch not admitted")
			}
			if index == 0 {
				if operationErr != nil || result.Receipt.Outcome != "installed" {
					t.Fatal("fixture did not commit grant", operationErr)
				}
				break // Deliberately no reply. The shared control remains alive.
			}
			reply, err := in.BuildControlReply(f.b.Reference().Fence, result, operationErr)
			must(t, err)
			packet, err := in.EncodeControlReply(reply)
			must(t, err)
			must(t, receiver.Reply(context.Background(), packet))
		}
		receiver.Close()
	}
	// Keep broker execution alive until parent verifies the final response.
	_, _ = bufio.NewReader(os.Stdin).ReadByte()
}

func TestControlClientRealProcessesReconcileLostGrantACK(t *testing.T) {
	first, firstChild := integrationPair(t)
	second, secondChild := integrationPair(t)
	selfFD, err := unix.PidfdOpen(os.Getpid(), 0)
	must(t, err)
	selfFile := os.NewFile(uintptr(selfFD), "fixture-controller-self")
	identity := linux.ProcessIdentity{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "fixture-controller-1"}
	self, err := linux.ImportProcessCapability(selfFile, identity)
	must(t, err)
	defer self.Close()
	child, _, bootstrap := integrationChild(t, "TestControlClientBrokerChild", "FAP_CONTROL_CLIENT_BROKER", []*os.File{firstChild, secondChild, selfFile})
	firstChild.Close()
	secondChild.Close()
	broker, err := linux.BindStartedProcess(child.Process, "fixture-broker-1", uint32(os.Getuid()), uint32(os.Getgid()))
	must(t, err)
	line := make(chan struct {
		data string
		err  error
	}, 1)
	go func() {
		s, e := bootstrap.ReadString('\n')
		line <- struct {
			data string
			err  error
		}{s, e}
	}()
	var binding fd.BindingRef
	select {
	case item := <-line:
		must(t, item.err)
		must(t, json.Unmarshal([]byte(item.data), &binding))
	case <-time.After(3 * time.Second):
		t.Fatal("broker bootstrap timed out")
	}
	f, _, _, _ := accessFixture(t)
	makeClient := func(conn *net.UnixConn) *in.ControlClient {
		// Fixture quote only; verifies actual socket flow, not native Footprint.
		transport, err := fd.OwnRuntimeResource(context.Background(), f.r, fd.ResourcePlan[peer.ControlConnection]{Bytes: 4096,
			Create: func(context.Context) (fd.Created[peer.ControlConnection], error) {
				c, e := linux.NewControlConnection(conn, broker, time.Second)
				return fd.Created[peer.ControlConnection]{Value: c, Owned: e == nil}, e
			}, Release: func(c peer.ControlConnection) error { return c.Close() },
		})
		must(t, err)
		client, err := in.NewControlClient(context.Background(), f.r, transport, binding.Fence, "fixture-broker-1", uint64(2*time.Second))
		must(t, err)
		t.Cleanup(client.Close)
		return client
	}
	metadata, err := linux.EncodeProcessIdentity(identity)
	must(t, err)
	grant := in.ControlCommand{Kind: in.ControlGrant, Header: in.ControlHeader{Target: binding.Fence, ID: [16]byte{1}, Sequence: 1, Grant: "reader"}, Binding: binding, Policy: controlPolicy(identity.Generation), CapabilityType: linux.ProcessCapabilityType, CapabilityMetadata: metadata}
	client := makeClient(first)
	out, err := client.Exchange(context.Background(), grant, self)
	if err == nil || !out.SendStarted || out.ResponseReceived || !client.Stats().Closed {
		t.Fatal("lost ACK became a confirmed/non-delivered grant", out, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, client.WaitClosed(ctx))
	if generation, err := self.CurrentGeneration(); err != nil || generation != identity.Generation {
		t.Fatal("client consumed caller capability", err)
	}
	client = makeClient(second)
	status := in.ControlCommand{Kind: in.ControlStatus, Header: in.ControlHeader{Target: binding.Fence, ID: [16]byte{2}}}
	out, err = client.Exchange(context.Background(), status, nil)
	must(t, err)
	if !out.ResponseReceived || out.Reply.Code != in.ReplyOK || out.Reply.Stats == nil {
		t.Fatal("status not confirmed", out)
	}
	s := out.Reply.Stats
	if s.Receipt.ID != grant.Header.ID || s.Receipt.Sequence != 1 || s.Receipt.Outcome != "installed" || s.Active != 1 || len(s.Grants) != 1 || s.Grants[0].Consumer != identity.Generation {
		t.Fatal("committed grant not recovered exactly", s)
	}
	revoke := in.ControlCommand{Kind: in.ControlRevoke, Header: in.ControlHeader{Target: binding.Fence, ID: [16]byte{3}, Sequence: 2, Previous: 1, Grant: "reader"}}
	out, err = client.Exchange(context.Background(), revoke, nil)
	must(t, err)
	if !out.ResponseReceived || out.Reply.Receipt.Outcome != "revoked" {
		t.Fatal(fmt.Sprint(out))
	}
}
