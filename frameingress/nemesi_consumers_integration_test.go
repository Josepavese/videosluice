//go:build linux && nemesi_integration

package ingress_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	pl "github.com/Josepavese/videosluice/framepeer/provider/linux"
	wire "github.com/Josepavese/videosluice/framewire"
	snapshotlinux "github.com/Josepavese/videosluice/framewire/provider/linux"
)

// A failing child may still be writing its diagnostics. Snapshot those logs
// under the same lock as Write; formatting bytes.Buffer concurrently is unsafe.
type consumerFixtureLog struct {
	mu     sync.Mutex
	buffer bytes.Buffer // not embedded: io.Copy must not bypass Write via ReadFrom
}

func (b *consumerFixtureLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}
func (b *consumerFixtureLog) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func TestConsumerFixtureLogCopyCannotBypassSynchronization(t *testing.T) {
	var output consumerFixtureLog
	if _, bypass := any(&output).(io.ReaderFrom); bypass {
		t.Fatal("io.Copy can bypass the synchronized writer")
	}
	done := make(chan error, 1)
	payload := bytes.Repeat([]byte("fixture diagnostics\n"), 1000)
	go func() {
		// LimitedReader has no WriterTo: exercise io.Copy's destination path.
		_, err := io.Copy(&output, io.LimitReader(bytes.NewReader(payload), int64(len(payload))))
		done <- err
	}()
	for range 128 {
		_ = output.String()
	}
	must(t, <-done)
	if output.String() != string(payload) {
		t.Fatal("diagnostic bytes were lost")
	}
}

// Extends the existing native capture/resize/ROI integration to the production
// Python SDK, authenticated native channel, cropper/emitter and Registry
// admission. Access, extraction and FrameSender are real. Source/copy admission
// and launch composition remain fixtures, NOT deployment or HW qualification.
func verifyNemesiConsumers(t *testing.T, ctx context.Context, root, library string, controller *pl.Lease, f *fixture, h wire.Header, delivered []byte,
	tr fa.Transform, q fd.Request, artifact fd.Artifact, pixels []byte, admission fa.CopyAdmission) {
	t.Helper()
	header, err := wire.MarshalHeader(h)
	must(t, err)
	request, err := in.EncodeFrameRequest(in.FrameRequest{ID: q.ID, Frame: q.Frame,
		Source: q.Source, Surface: q.Surface, Regions: q.Regions, BudgetNS: 1_000_000_000})
	must(t, err)
	manifest := make([]byte, in.MaxFrameManifestBytes)
	n, err := in.EncodeFrameManifest(manifest, in.FrameManifest{ID: q.ID, Fence: q.Frame.Fence,
		Index: 0, Bytes: len(pixels), Artifact: artifact})
	must(t, err)
	mirror := 0
	if tr.Mirror {
		mirror = 1
	}
	metadata, err := json.Marshal(map[string]any{
		"transform": []int{tr.Full.Width, tr.Full.Height, tr.Full.Stride,
			tr.Crop.X1, tr.Crop.Y1, tr.Crop.X2, tr.Crop.Y2, tr.Rotation, mirror,
			tr.ResizedWidth, tr.ResizedHeight, tr.DeliveryWidth, tr.DeliveryHeight, tr.PadLeft, tr.PadTop},
		"bbox": q.Regions[0].BBox, "padding": q.Regions[0].PaddingRatio,
		"surface": q.Surface, "provider": admission.Provider,
		"evidence": [32]byte(admission.Evidence),
	})
	must(t, err)
	var input bytes.Buffer
	for _, data := range [][]byte{header[:], request, manifest[:n], metadata, pixels, delivered} {
		must(t, binary.Write(&input, binary.BigEndian, uint32(len(data))))
		input.Write(data)
	}
	launch, err := pl.PrepareLaunch(controller)
	must(t, err)
	defer launch.Close()
	files, launchMetadata, err := launch.Inherited()
	must(t, err)
	deliveryLaunch, err := pl.PrepareLaunch(controller)
	must(t, err)
	defer deliveryLaunch.Close()
	deliveryFiles, deliveryMetadata, err := deliveryLaunch.Inherited()
	must(t, err)
	// Own the child through interpreter shutdown. Functional completion is
	// separately acknowledged within ctx below, never inferred from a log.
	workerCtx, stopWorker := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopWorker()
	python := exec.CommandContext(workerCtx, "python3", "-B", filepath.Join(root, "video-broker/frameingress", "nemesi_consumers_integration.py"), library, hex.EncodeToString(launchMetadata), hex.EncodeToString(deliveryMetadata))
	python.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1", "PYTHONPATH="+
		filepath.Join(root, "video-broker", "sdk", "python")+":"+filepath.Join(root, "tracker")+":"+filepath.Join(root, "person-registry"))
	python.Stdin = &input
	var output consumerFixtureLog
	python.Stdout, python.Stderr = &output, &output
	python.ExtraFiles = append(append([]*os.File{}, files[:]...), deliveryFiles[:]...)
	// Python imports (OpenCV/Tracker/Registry) are process preparation, not an
	// idle frame request. Begin the unchanged one-second peer operation only
	// after the fixture reports readiness, within the existing overall ctx.
	ready, readyChild, err := os.Pipe()
	must(t, err)
	defer ready.Close()
	defer readyChild.Close()
	python.ExtraFiles = append(python.ExtraFiles, readyChild) // test-only FD 7
	must(t, python.Start())
	waited := false
	defer func() {
		if !waited {
			python.Process.Kill()
			python.Wait()
		}
	}()
	must(t, readyChild.Close())
	stopReady := context.AfterFunc(ctx, func() { ready.Close() })
	var marker [1]byte
	nReady, readyErr := ready.Read(marker[:])
	stopReady()
	if readyErr != nil || nReady != 1 || marker[0] != 1 {
		t.Fatalf("Python fixture startup: %d/%v\n%s", nReady, readyErr, &output)
	}
	must(t, launch.CloseInherited())
	must(t, deliveryLaunch.CloseInherited())
	endpoint, err := launch.Deliver(ctx, python.Process, "native.python.consumer", uint32(os.Getuid()), uint32(os.Getgid()), time.Second)
	must(t, err)
	deliveryEndpoint, err := deliveryLaunch.Deliver(ctx, python.Process, "native.python.consumer", uint32(os.Getuid()), uint32(os.Getgid()), time.Second)
	must(t, err)
	access, err := in.NewAccess(f.r, f.b, endpoint, in.AccessPolicy{Generation: "native.python.consumer",
		Streams: []string{h.Stream}, MaxRegions: 1, MaxRequests: 1, MaxResults: 1})
	must(t, err)
	defer access.Revoke()
	writer, err := endpoint.FrameWriter()
	must(t, err)
	sender, err := in.NewFrameSender(ctx, f.r, writer, len(pixels), uint64(time.Second))
	must(t, err)
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		sender.Close()
		must(t, sender.WaitClosed(cleanupCtx))
	}()
	allocator, err := snapshotlinux.New(snapshotlinux.Limits{Bytes: 8 << 20, Allocations: 2, Operations: 2})
	must(t, err)
	publication, err := in.NewDeliveryPublication(ctx, f.b, h.Stream, in.DeliveryLayout{
		Transform: tr, Codec: wire.NV12, StrideY: h.StrideY, StrideUV: h.StrideUV, MaxPayload: h.PayloadBytes}, allocator)
	if publication != nil {
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			publication.Close()
			must(t, publication.WaitClosed(cleanupCtx))
		}()
	}
	must(t, err)
	must(t, publication.Publish(ctx, h, delivered))
	deliveryWriter, err := deliveryEndpoint.FrameWriter()
	must(t, err)
	deliverySender, err := in.NewFrameSender(ctx, f.r, deliveryWriter, len(delivered), uint64(time.Second))
	must(t, err)
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		deliverySender.Close()
		must(t, deliverySender.WaitClosed(cleanupCtx))
	}()
	var incoming [4096]byte
	// The real SDK/Tracker now gets its frame AND trusted publication metadata
	// through a second authenticated transport, not stdin/hand-built VideoFrame.
	for pull := 0; pull < 2; pull++ {
		n, principal, err := deliveryEndpoint.Receive(ctx, incoming[:])
		if err != nil {
			t.Fatalf("delivery receive: %v\n%s", err, &output)
		}
		if principal.Generation != "native.python.consumer" {
			t.Fatal("delivery principal")
		}
		must(t, deliverySender.SendDelivery(ctx, access, publication, incoming[:n], pull == 0))
	}
	for exchange := 0; exchange < 2; exchange++ {
		got, principal, err := endpoint.Receive(ctx, incoming[:])
		if err != nil {
			t.Fatalf("Python request receive: %v\n%s", err, &output)
		}
		if principal.Generation != "native.python.consumer" || !bytes.Equal(incoming[:got], request) {
			t.Fatal("Python request or peer differs from admission")
		}
		query, err := in.DecodeFrameRequest(incoming[:got])
		must(t, err)
		result, err := access.ExtractPacket(ctx, principal, incoming[:got])
		must(t, err)
		err = sender.Send(ctx, query, result, 0)
		result.Close()
		must(t, err)
	}
	// Fault only the ROI transport after two complete, verified exchanges.
	// The broker process and independently admitted delivery remain alive:
	// the SDK must request a fresh pair rather than silently stop observations.
	must(t, endpoint.Close())
	stopComplete := context.AfterFunc(ctx, func() { ready.Close() })
	nComplete, completeErr := ready.Read(marker[:])
	stopComplete()
	if completeErr != nil || nComplete != 1 || marker[0] != 2 || ctx.Err() != nil {
		t.Fatalf("Python functional completion: %d/%v ctx=%v\n%s", nComplete, completeErr, ctx.Err(), &output)
	}
	waited = true
	if err := python.Wait(); err != nil || !bytes.Contains([]byte(output.String()), []byte("NEMESI_NATIVE_ROI_PASS")) {
		t.Fatalf("Nemesi native ROI consumers: %v\n%s", err, &output)
	}
}
