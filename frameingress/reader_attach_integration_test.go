//go:build linux && nemesi_integration

package ingress_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	pl "github.com/Josepavese/videosluice/framepeer/provider/linux"
	"golang.org/x/sys/unix"
)

// A real, separate broker process. Parent test is the supervisor/launcher; the
// Python tracker is its other child. The broker does not launch the consumer.
// Source/copy/provider budgets here are explicit fixtures, NOT HW admission.
func TestAttachedReaderBrokerProcess(t *testing.T) {
	if os.Getenv("FAP_READER_BROKER") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	metadata, err := hex.DecodeString(os.Getenv("FAP_PARENT_METADATA"))
	must(t, err)
	launch, err := pl.AcceptLaunch(ctx, os.NewFile(3, "parent-control"), os.NewFile(4, "parent-pidfd"), metadata, time.Second)
	must(t, err)
	defer launch.Close()
	f := setupGeometryAuthorityCopyBudget(t, copyport.CPU{}, launch.Self, 16, 16, uint64(time.Second))
	ticket := f.ticket(t)
	must(t, f.i.AcceptSurface(ctx, f.surface(t, 1), 100, 200))
	packet := f.delivery(t, ticket, 1)
	must(t, f.i.AcceptDelivery(ctx, ticket, packet))
	p := deliveryPublication(t, f)
	h, pixels := publicationFrame(t, f, 1)
	// Use the already bound ticket's actual delivery identity for both planes.
	h.DeliveryID = ticket.Ref().Ref.FrameID
	must(t, p.Publish(ctx, h, pixels))
	parentGeneration, err := launch.Controller.CurrentGeneration()
	must(t, err)
	control, err := in.NewGrantControl(f.r, launch.Controller, parentGeneration, 4)
	must(t, err)
	dispatch, err := in.NewControlDispatcher(control)
	must(t, err)
	defer func() { dispatch.Close(); must(t, control.WaitClosed(ctx)) }()
	listen := func(name string) *net.UnixListener {
		listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: filepath.Join(os.Getenv("FAP_ENDPOINT_DIR"), name), Net: "unixpacket"})
		must(t, err)
		t.Cleanup(func() { listener.Close() })
		raw, err := listener.SyscallConn()
		must(t, err)
		var setupErr error
		must(t, raw.Control(func(handle uintptr) {
			setupErr = unix.SetsockoptInt(int(handle), unix.SOL_SOCKET, unix.SO_PASSCRED, 1)
			if setupErr == nil {
				setupErr = unix.Listen(int(handle), 1)
			}
		}))
		must(t, setupErr)
		return listener
	}
	deliveryListener, roiListener := listen("delivery.sock"), listen("roi.sock")
	// The binding travels over the already authenticated parent control socket.
	binding, err := json.Marshal(f.b.Reference())
	must(t, err)
	must(t, launch.Control.Reply(ctx, binding))
	// Test composition only: wait for the parent to finish consumer imports.
	// The real one-second command operation starts after this preparation gate.
	var prepared [1]byte
	nPrepared, err := os.Stdin.Read(prepared[:])
	if err != nil || nPrepared != 1 || prepared[0] != 1 {
		t.Fatalf("consumer preparation gate: %d/%v", nPrepared, err)
	}
	var input [4096]byte
	n, principal, pending, err := launch.Control.ReceiveCommand(ctx, input[:])
	must(t, err)
	result, accepted, operationErr := dispatch.Apply(ctx, principal, input[:n], pending)
	if !accepted {
		if pending != nil {
			pending.Close()
		}
		t.Fatal("grant dispatch rejected")
	}
	must(t, operationErr)
	if result.Receipt.Outcome != "installed" {
		t.Fatal("grant not installed")
	}
	reply, err := in.BuildControlReply(f.b.Fence(), result, nil)
	must(t, err)
	out, err := in.EncodeControlReply(reply)
	must(t, err)
	must(t, launch.Control.Reply(ctx, out))
	accept := func(listener *net.UnixListener, publication *in.DeliveryPublication) *in.FrameSession {
		var selectDelivery func(fd.BindingRef) (*in.DeliveryPublication, error)
		if publication != nil {
			selectDelivery = func(fd.BindingRef) (*in.DeliveryPublication, error) { return publication, nil }
		}
		s, owner, err := dispatch.AcceptReaderSession(ctx, "reader", selectDelivery, in.ReaderAcceptPlan{Bytes: 4096,
			Accept: func(ctx context.Context) (*net.UnixConn, error) {
				listener.SetDeadline(time.Now().Add(time.Second))
				return listener.AcceptUnix()
			},
		}, 1024, uint64(time.Second))
		if owner != nil {
			defer func() { owner.Close(); must(t, owner.WaitClosed(ctx)) }()
		}
		must(t, err)
		return s
	}
	deliverySession, roiSession := accept(deliveryListener, p), accept(roiListener, nil)
	defer func() {
		deliverySession.Close()
		roiSession.Close()
		waitSession(t, deliverySession)
		waitSession(t, roiSession)
	}()
	for _, session := range []*in.FrameSession{deliverySession, deliverySession, roiSession} {
		complete, err := session.Step(ctx)
		must(t, err)
		if !complete {
			t.Fatal("reader transaction incomplete")
		}
	}
	// Keep broker identity alive until the tracker has validated its last bytes.
	n, _, extra, err := launch.Control.ReceiveCommand(ctx, input[:])
	must(t, err)
	if extra != nil {
		extra.Close()
		t.Fatal("unexpected completion capability")
	}
	if string(input[:n]) != "consumer_finished" {
		t.Fatal("bad completion")
	}
}

func TestSupervisorStartedTrackerAttachesToSeparateBroker(t *testing.T) {
	root, err := filepath.Abs("../..")
	must(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	// Prepare the native library before the child broker starts its unchanged
	// one-second grant receive. Compilation is not a peer/control operation.
	library := filepath.Join(t.TempDir(), "libvideo_broker_frame_peer.so")
	peerNative := filepath.Join(root, "video-broker/framepeer", "provider", "linux", "native")
	compile := exec.CommandContext(ctx, "cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-fPIC", "-shared", "-I"+peerNative, "-I"+filepath.Join(root, "video-broker/framepeer", "native"), filepath.Join(root, "video-broker", "native", "frame_peer_python.c"), filepath.Join(peerNative, "launch.c"), "-o", library, "-lcrypto", "-pthread")
	if out, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile: %v\n%s", err, out)
	}
	selfFD, err := unix.PidfdOpen(os.Getpid(), 0)
	must(t, err)
	controller, err := pl.ImportProcessCapability(os.NewFile(uintptr(selfFD), "supervisor-self"), pl.ProcessIdentity{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "supervisor-1"})
	must(t, err)
	defer controller.Close()
	launch, err := pl.PrepareLaunch(controller)
	must(t, err)
	defer launch.Close()
	files, parentMetadata, err := launch.Inherited()
	must(t, err)
	// t.TempDir includes this long test name. With a separate GOTMPDIR it
	// can exceed sockaddr_un.sun_path before any peer protocol is exercised.
	endpoints, err := os.MkdirTemp("", "fap-attach-")
	must(t, err)
	t.Cleanup(func() { must(t, os.RemoveAll(endpoints)) })
	broker := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAttachedReaderBrokerProcess$")
	broker.Env = append(os.Environ(), "FAP_READER_BROKER=1", "FAP_PARENT_METADATA="+hex.EncodeToString(parentMetadata), "FAP_ENDPOINT_DIR="+endpoints)
	broker.ExtraFiles = files[:]
	// The child testing harness writes assertion failures to stdout. Preserve
	// them as well as stderr so an early exit is not only a peer receive error.
	broker.Stdout, broker.Stderr = os.Stderr, os.Stderr
	brokerGate, err := broker.StdinPipe()
	must(t, err)
	defer brokerGate.Close()
	must(t, broker.Start())
	brokerWaited := false
	defer func() {
		if !brokerWaited {
			broker.Process.Kill()
			broker.Wait()
		}
	}()
	must(t, launch.CloseInherited())
	endpoint, err := launch.Deliver(ctx, broker.Process, "broker-1", uint32(os.Getuid()), uint32(os.Getgid()), time.Second)
	must(t, err)
	var input [4096]byte
	n, principal, err := endpoint.Receive(ctx, input[:])
	must(t, err)
	if principal.Generation != "broker-1" {
		t.Fatal("broker principal")
	}
	var binding fd.BindingRef
	must(t, json.Unmarshal(input[:n], &binding))
	f := setupCopierAuthority(t, copyport.CPU{}, controller)
	transport, err := fd.OwnRuntimeResource(ctx, f.r, fd.ResourcePlan[peer.ControlConnection]{Bytes: 4096,
		Create: func(context.Context) (fd.Created[peer.ControlConnection], error) {
			return fd.Created[peer.ControlConnection]{Value: endpoint, Owned: true}, nil
		},
		Release: func(peer.ControlConnection) error { return nil }, // Launch owns the physical socket.
	})
	must(t, err)
	client, err := in.NewControlClient(ctx, f.r, transport, binding.Fence, "broker-1", uint64(time.Second))
	must(t, err)
	defer func() { client.Close(); must(t, client.WaitClosed(ctx)) }()
	brokerCapability, err := pl.BindStartedProcess(broker.Process, "broker-1", uint32(os.Getuid()), uint32(os.Getgid()))
	must(t, err)
	brokerFD1, identity, err := brokerCapability.Export()
	must(t, err)
	defer brokerFD1.Close()
	brokerFD2, _, err := brokerCapability.Export()
	must(t, err)
	defer brokerFD2.Close()
	brokerMetadata, err := pl.EncodeProcessIdentity(identity)
	must(t, err)
	trackerLaunch, err := pl.PrepareLaunch(controller)
	must(t, err)
	defer trackerLaunch.Close()
	trackerFiles, trackerParentMetadata, err := trackerLaunch.Inherited()
	must(t, err)
	tracker := exec.CommandContext(ctx, "python3", "-B", filepath.Join(root, "video-broker/frameingress", "reader_attach_integration.py"), library, hex.EncodeToString(brokerMetadata), endpoints, hex.EncodeToString(trackerParentMetadata))
	tracker.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1", "PYTHONPATH="+filepath.Join(root, "video-broker", "sdk", "python")+":"+filepath.Join(root, "tracker"))
	tracker.ExtraFiles = append(append([]*os.File{}, trackerFiles[:]...), brokerFD1, brokerFD2)
	trackerReady, trackerReadyChild, err := os.Pipe()
	must(t, err)
	defer trackerReady.Close()
	defer trackerReadyChild.Close()
	tracker.ExtraFiles = append(tracker.ExtraFiles, trackerReadyChild) // fixture FD7
	var trackerOutput bytes.Buffer
	tracker.Stdout, tracker.Stderr = &trackerOutput, &trackerOutput
	must(t, tracker.Start())
	must(t, trackerReadyChild.Close())
	must(t, trackerLaunch.CloseInherited())
	trackerWaited := false
	defer func() {
		if !trackerWaited {
			tracker.Process.Kill()
			tracker.Wait()
		}
	}()
	stopReady := context.AfterFunc(ctx, func() { trackerReady.Close() })
	var readyMarker [1]byte
	nReady, readyErr := trackerReady.Read(readyMarker[:])
	stopReady()
	if readyErr != nil || nReady != 1 || readyMarker[0] != 1 {
		t.Fatalf("tracker preparation: %d/%v", nReady, readyErr)
	}
	_, err = brokerGate.Write([]byte{1})
	must(t, err)
	consumer, err := pl.BindStartedProcess(tracker.Process, "tracker-1", uint32(os.Getuid()), uint32(os.Getgid()))
	must(t, err)
	consumerMetadata, err := pl.EncodeProcessIdentity(pl.ProcessIdentity{PID: tracker.Process.Pid, UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "tracker-1"})
	must(t, err)
	command := in.ControlCommand{Kind: in.ControlGrant, Header: in.ControlHeader{Target: binding.Fence, ID: [16]byte{1}, Sequence: 1, Grant: "reader"}, Binding: binding,
		Policy: in.AccessPolicy{Generation: "tracker-1", Streams: []string{"analysis"}, MaxRegions: 1, MaxRequests: 1, MaxResults: 2}, CapabilityType: pl.ProcessCapabilityType, CapabilityMetadata: consumerMetadata}
	exchange, err := client.Exchange(ctx, command, consumer)
	must(t, err)
	if !exchange.ResponseReceived || exchange.Reply.Receipt.Outcome != "installed" {
		t.Fatal("grant ACK")
	}
	// Send ordinary parent bootstrap only after the exact process grant ACK.
	_, err = trackerLaunch.Deliver(ctx, tracker.Process, "tracker-1", uint32(os.Getuid()), uint32(os.Getgid()), time.Second)
	must(t, err)
	trackerWaited = true
	if err := tracker.Wait(); err != nil {
		t.Fatalf("tracker attach: %v\n%s", err, &trackerOutput)
	}
	if !bytes.Contains(trackerOutput.Bytes(), []byte("SEPARATE_BROKER_TRACKER_PASS")) {
		t.Fatal(&trackerOutput)
	}
	must(t, endpoint.WriteCommand(ctx, []byte("consumer_finished"), nil))
	brokerWaited = true
	must(t, broker.Wait())
}
