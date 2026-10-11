//go:build linux

package ingress_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	in "github.com/Josepavese/videosluice/frameingress"
	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
	"golang.org/x/sys/unix"
)

type outputBootstrap struct {
	Request  in.FrameRequest
	Evidence in.ExpectedFrameEvidence
}

func TestFrameOutputConsumerChild(t *testing.T) {
	if os.Getenv("FAP_OUTPUT_CONSUMER") != "1" {
		return
	}
	// Trusted fixture bootstrap is distinct from all untrusted wire records.
	var bootstrap outputBootstrap
	must(t, json.NewDecoder(os.Stdin).Decode(&bootstrap))
	file := os.NewFile(3, "output-channel")
	connection, err := net.FileConn(file)
	file.Close()
	must(t, err)
	defer connection.Close()
	brokerFile := os.NewFile(4, "broker-capability")
	broker, err := linux.ImportProcessCapability(brokerFile, linux.ProcessIdentity{PID: os.Getppid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "authenticated-process-1"})
	if err != nil {
		brokerFile.Close()
	}
	must(t, err)
	defer broker.Close()
	receiver, err := linux.NewReceiver(connection.(*net.UnixConn), broker, in.MaxFrameDataBytes, time.Second)
	must(t, err)
	packet, err := in.EncodeFrameRequest(bootstrap.Request)
	must(t, err)
	must(t, connection.SetWriteDeadline(time.Now().Add(time.Second)))
	_, err = connection.Write(packet)
	must(t, err)
	buffer := make([]byte, in.MaxFrameDataBytes)
	n, principal, err := receiver.Receive(context.Background(), buffer)
	must(t, err)
	if principal.Generation != "authenticated-process-1" {
		t.Fatal("wrong broker")
	}
	manifest, err := in.DecodeFrameManifest(buffer[:n])
	must(t, err)
	must(t, in.ValidateFrameManifest(manifest, bootstrap.Request, 1024))
	assembly, err := in.NewFrameAssembly(buffer[:n], bootstrap.Request, bootstrap.Evidence, make([]byte, manifest.Bytes), 1024)
	must(t, err)
	defer assembly.Abort()
	for received := 0; received < manifest.Bytes; {
		n, principal, err = receiver.Receive(context.Background(), buffer)
		must(t, err)
		if principal.Generation != "authenticated-process-1" {
			t.Fatal("wrong chunk sender")
		}
		must(t, assembly.Append(buffer[:n]))
		received += n - 44
	}
	artifact, pixels, err := assembly.Finish()
	must(t, err)
	if artifact.State != fa.Complete {
		t.Fatal("no complete frame")
	}
	fmt.Println(hex.EncodeToString(pixels))
}

func TestFrameRequestAndProofVerifiedInSeparateConsumerProcess(t *testing.T) {
	f, _, _, q := accessFixture(t)
	connection, childSocket := integrationPair(t)
	selfFD, err := unix.PidfdOpen(os.Getpid(), 0)
	must(t, err)
	self := os.NewFile(uintptr(selfFD), "broker-self-fixture")
	defer self.Close()
	child, input, output := integrationChild(t, "TestFrameOutputConsumerChild", "FAP_OUTPUT_CONSUMER", []*os.File{childSocket, self})
	lease, err := linux.BindStartedProcess(child.Process, "output-consumer-1", uint32(os.Getuid()), uint32(os.Getgid()))
	must(t, err)
	defer lease.Close()
	receiver, err := linux.NewFrameConnection(connection, lease, 4096, time.Second)
	must(t, err)
	sender, err := in.NewFrameSender(context.Background(), f.r, receiver, 1024, uint64(time.Second))
	must(t, err)
	defer closeSender(t, sender)
	access, err := in.NewAccess(f.r, f.b, lease, in.AccessPolicy{Generation: "output-consumer-1", Streams: []string{"analysis"}, MaxRegions: 1, MaxRequests: 1, MaxResults: 1})
	must(t, err)
	defer access.Revoke()
	request := in.FrameRequest{ID: q.ID, Frame: q.Frame, Source: q.Source, Surface: q.Surface, Regions: q.Regions, BudgetNS: 100}
	evidence := in.ExpectedFrameEvidence{Origin: fa.OriginRef{Source: f.layout.Source, MediaEpoch: f.b.Scope().MediaEpoch, Sequence: 1, Graph: f.layout.Graph}, Transform: f.tr, Admission: admitted(f.b.Scope())}
	must(t, json.NewEncoder(input).Encode(outputBootstrap{request, evidence}))
	buffer := make([]byte, 4096)
	n, principal, err := receiver.Receive(context.Background(), buffer)
	must(t, err)
	result, err := access.ExtractPacket(context.Background(), principal, buffer[:n])
	must(t, err)
	defer result.Close()
	var pixels []byte
	must(t, result.Visit(0, func(artifact fd.Artifact, p []byte) error {
		pixels = append([]byte(nil), p...)
		return nil
	})) // Test-only expected bytes. The actual sender uses its accounted buffer.
	must(t, sender.Send(context.Background(), request, result, 0))
	returned := make(chan string, 1)
	go func() {
		line, err := output.ReadString('\n')
		if err != nil {
			returned <- "error"
			return
		}
		returned <- line
	}()
	select {
	case value := <-returned:
		if value != hex.EncodeToString(pixels)+"\n" {
			t.Fatal("consumer did not independently verify exact pixels")
		}
	case <-time.After(3 * time.Second):
		child.Process.Kill()
		<-returned
		t.Fatal("consumer output deadline")
	}
}
