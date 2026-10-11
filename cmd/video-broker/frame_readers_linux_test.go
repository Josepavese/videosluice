//go:build linux && nemesi_integration

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	pl "github.com/Josepavese/videosluice/framepeer/provider/linux"
	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/internal/source"
)

// Real C producer -> source receiver/publication -> daemon packet endpoint ->
// real tracker CLI/SDK -> exact native ROI. Only pixels, detector/config policy
// and native admission quotes are fixtures; this is NOT hardware qualification.
func TestDaemonReadersNativeProducerTrackerAndSourceSwitch(t *testing.T) {
	worker := compileCaptureWorker(t)
	root, err := filepath.Abs("../../..")
	captureMust(t, err)
	path, err := os.MkdirTemp("", "fap-daemon-")
	captureMust(t, err)
	defer os.RemoveAll(path)
	library := filepath.Join(path, "libframepeer.so")
	native := filepath.Join(root, "video-broker/framepeer/provider/linux/native")
	compile := exec.Command("cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-fPIC", "-shared", "-I"+native,
		"-I"+filepath.Join(root, "video-broker/framepeer/native"), filepath.Join(root, "video-broker/native/frame_peer_python.c"), filepath.Join(native, "launch.c"), "-o", library, "-lcrypto", "-pthread")
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("SDK compile: %v\n%s", err, output)
	}
	// Camera-only fixture needs12 slots; both reader planes add simultaneous
	// listener, transport and packet-buffer owners (13 active slots here).
	b, plan := captureFixtureResources(t, 16)
	b.admission.maxGrants = 4
	b.admission.readers = []nativeFrameReaderPlan{
		{Path: filepath.Join(path, "delivery.sock"), Grant: "reader", Stream: "analysis", Delivery: true, ListenerBytes: 8192, AcceptBytes: 4096, MaxBytes: 192, MaxWorkNS: uint64(time.Second)},
		{Path: filepath.Join(path, "roi.sock"), Grant: "reader", ListenerBytes: 8192, AcceptBytes: 4096, MaxBytes: 1024, MaxWorkNS: uint64(time.Second)},
	}
	var controller *pl.Lease
	captureMust(t, b.resource.Borrow(context.Background(), func(_ context.Context, launch *pl.AcceptedLaunch) error { controller = launch.Self; return nil }))
	generation, err := controller.CurrentGeneration()
	captureMust(t, err)
	var grants *in.GrantControl
	control, err := fd.OwnRuntimeResource(context.Background(), b.runtime, fd.ResourcePlan[*in.ControlDispatcher]{Bytes: 32768,
		Create: func(context.Context) (fd.Created[*in.ControlDispatcher], error) {
			var err error
			grants, err = in.NewGrantControl(b.runtime, controller, generation, 4)
			if err != nil {
				return fd.Created[*in.ControlDispatcher]{}, err
			}
			dispatch, err := in.NewControlDispatcher(grants)
			return fd.Created[*in.ControlDispatcher]{Value: dispatch, Owned: dispatch != nil}, err
		}, Release: func(dispatch *in.ControlDispatcher) error {
			dispatch.Close()
			return grants.WaitClosed(context.Background())
		},
	})
	captureMust(t, err)
	defer func() { control.Close(); captureMust(t, control.WaitClosed(context.Background())) }()
	// Fixture service lifetime spans three independent source qualifications.
	// Every cycle below retains its twenty-second functional envelope; native
	// sanitizer exit is checked separately after the source close ACK.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ready, done := make(chan struct{}), make(chan error, 1)
	go func() { done <- b.serveFrameReaders(ctx, control, ready) }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	select {
	case <-ready:
	case err := <-done:
		joined = true
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	originalAdmission := b.admission.source
	var previous []fd.BindingRef
	for cycle, sourceID := range []string{"front", "rear", "front"} {
		plan.SourceID, plan.V4L2BridgePath = sourceID, worker
		b.admission.source = func(ctx context.Context, plan source.BrokerMuxPlan) (nativeFrameSourceAdmission, error) {
			a, err := originalAdmission(ctx, plan)
			a.Layout.Source = sourceID
			return a, err
		}
		func() {
			ctx, stopCycle := context.WithTimeout(ctx, 20*time.Second)
			defer stopCycle()
			capture, err := b.FrameCaptures()(ctx, plan)
			captureMust(t, err)
			c := capture.(*nativeFrameCapture)
			plan.MediaEpoch = c.SourceEpoch()
			for _, old := range previous {
				if _, err := b.readerPublication(old, "analysis"); err == nil {
					t.Fatal("old source publication survived switch")
				}
			}
			previous = append(previous, c.binding.Reference())
			workerCtx, stopWorker := context.WithTimeout(context.Background(), 30*time.Second)
			defer stopWorker()
			command, _, err := source.BuildBrokerMuxCommand(workerCtx, plan)
			captureMust(t, err)
			command.Args = append(command.Args, "--fixture-step-frames", "--fixture-step-surface")
			captureMust(t, c.Attach(command))
			input, err := command.StdinPipe()
			captureMust(t, err)
			output, err := command.StdoutPipe()
			captureMust(t, err)
			var workerLog bytes.Buffer
			command.Stderr = &workerLog
			captureMust(t, command.Start())
			waited := false
			runCtx, stop := context.WithCancel(ctx)
			var receiveDone chan error
			defer func() {
				stop()
				if receiveDone != nil {
					<-receiveDone
				}
				if !waited {
					input.Close()
					command.Process.Kill()
					command.Wait()
				}
				captureMust(t, c.Retire(context.Background()))
				if t.Failed() {
					t.Log(workerLog.String())
				}
			}()
			captureMust(t, c.Started(ctx, command.Process))
			lines := bufio.NewReader(output)
			line, err := lines.ReadString('\n')
			captureMust(t, err)
			if line != "bound\n" {
				t.Fatal(line)
			}
			receiveDone = make(chan error, 1)
			go func() { receiveDone <- c.Run(runCtx, func(context.Context, wire.Header, []byte) error { return nil }) }()
			_, err = input.Write([]byte{1})
			captureMust(t, err)
			stageSurface := func(want uint64) {
				line, err := lines.ReadString('\n')
				captureMust(t, err)
				if line != "surface-published\n" {
					t.Fatal(line)
				}
				for c.surfaces.Stats().StagedFrames < want {
					if ctx.Err() != nil {
						t.Fatal(ctx.Err())
					}
					time.Sleep(time.Millisecond)
				}
				_, err = input.Write([]byte{6})
				captureMust(t, err)
			}
			stageSurface(1)
			line, err = lines.ReadString('\n')
			captureMust(t, err)
			if line != "first-published\n" {
				t.Fatal(line)
			}
			for c.receiver.Stats().Outputs < 1 || c.surfaces.Stats().StagedFrames < 1 {
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				time.Sleep(time.Millisecond)
			}
			_, err = input.Write([]byte{5})
			captureMust(t, err)
			stageSurface(2)
			line, err = lines.ReadString('\n')
			captureMust(t, err)
			if line != "published\n" {
				t.Fatal(line)
			}
			for c.receiver.Stats().Outputs != 2 || c.surfaces.Stats().StagedFrames != 2 {
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				time.Sleep(time.Millisecond)
			}
			launch, err := pl.PrepareLaunch(controller)
			captureMust(t, err)
			defer launch.Close()
			files, parentMetadata, err := launch.Inherited()
			captureMust(t, err)
			brokerFD1, identity, err := controller.Export()
			captureMust(t, err)
			defer brokerFD1.Close()
			brokerFD2, _, err := controller.Export()
			captureMust(t, err)
			defer brokerFD2.Close()
			brokerMetadata, err := pl.EncodeProcessIdentity(identity)
			captureMust(t, err)
			marker := filepath.Join(path, "tracker-"+strconv.Itoa(cycle))
			tracker := exec.CommandContext(ctx, "python3", "-B", filepath.Join(root, "task_frame_tracker_fixture.py"), "--video_source", "broker_frame",
				"--frame-launch-socket-fd", "3", "--frame-launch-controller-fd", "4", "--frame-launch-controller-metadata", hex.EncodeToString(parentMetadata),
				"--frame-broker-delivery-fd", "5", "--frame-broker-roi-fd", "6", "--frame-broker-metadata", hex.EncodeToString(brokerMetadata), "--frame-peer-library", library,
				"--frame-delivery-path", b.admission.readers[0].Path, "--frame-roi-path", b.admission.readers[1].Path)
			tracker.ExtraFiles = append(files[:], brokerFD1, brokerFD2)
			tracker.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1", "PYTHONPATH="+filepath.Join(root, "tracker")+":"+filepath.Join(root, "video-broker/sdk/python"),
				"NEMESI_TRACKER_VIDEO_URL=fixture-unused", "FAP_TRACKER_READY="+marker, "FAP_FIXTURE_WIDTH=16", "FAP_FIXTURE_HEIGHT=8", "FAP_FIXTURE_SOURCE="+sourceID, "FAP_FIXTURE_PATTERN=coordinate-nv12", "FAP_FIXTURE_ORIGIN=2")
			var trackerLog bytes.Buffer
			tracker.Stdout, tracker.Stderr = &trackerLog, &trackerLog
			captureMust(t, tracker.Start())
			trackerWaited := false
			defer func() {
				if !trackerWaited {
					tracker.Process.Kill()
					tracker.Wait()
				}
				if t.Failed() {
					t.Log(trackerLog.String())
				}
			}()
			captureMust(t, launch.CloseInherited())
			consumerGeneration := fmt.Sprintf("tracker-%d", cycle)
			consumer, err := pl.BindStartedProcess(tracker.Process, consumerGeneration, uint32(os.Getuid()), uint32(os.Getgid()))
			captureMust(t, err)
			receipt, err := grants.Grant(peer.Principal{Generation: generation}, in.ControlHeader{Target: b.runtime.Fence(), ID: [16]byte{byte(cycle + 1)}, Sequence: uint64(cycle + 1), Previous: uint64(cycle), Grant: "reader"}, c.binding.Reference(), consumer,
				in.AccessPolicy{Generation: consumerGeneration, Streams: []string{"analysis"}, MaxRegions: 1, MaxRequests: 1, MaxResults: 2})
			if err != nil {
				consumer.Close()
				t.Fatal(err)
			}
			if receipt.Outcome != "installed" {
				t.Fatal(receipt)
			}
			_, err = launch.Deliver(ctx, tracker.Process, consumerGeneration, uint32(os.Getuid()), uint32(os.Getgid()), time.Second)
			captureMust(t, err)
			deadline := time.Now().Add(5 * time.Second)
			for {
				if raw, err := os.ReadFile(marker); err == nil {
					var proof struct {
						Source string `json:"source"`
						Epoch  uint64 `json:"source_epoch"`
						Origin uint64 `json:"origin_sequence"`
						Cases  int    `json:"native_pixel_cases"`
					}
					captureMust(t, json.Unmarshal(raw, &proof))
					if proof.Source != sourceID || proof.Epoch != c.SourceEpoch() || proof.Origin != 2 || proof.Cases != 7 {
						t.Fatalf("native pixel verification incomplete: %+v", proof)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("tracker did not verify native ROI")
				}
				time.Sleep(10 * time.Millisecond)
			}
			captureMust(t, tracker.Process.Signal(syscall.SIGTERM))
			trackerWaited = true
			captureMust(t, tracker.Wait())
			stop()
			<-receiveDone
			receiveDone = nil
			_, err = input.Write([]byte{2})
			captureMust(t, err)
			line, err = lines.ReadString('\n')
			captureMust(t, err)
			if line != "closed\n" {
				t.Fatal(line)
			}
			captureMust(t, ctx.Err()) // native close completes within the same 20s
			waited = true
			captureMust(t, command.Wait())
		}()
	}
	cancel()
	err = <-done
	joined = true
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, p := range b.admission.readers {
		if _, err := os.Lstat(p.Path); !os.IsNotExist(err) {
			t.Fatal("listener leaked", err)
		}
	}
	if len(b.captures) != 0 {
		t.Fatal("retired captures retained")
	}
}

func TestDaemonReaderPartialAdmissionClosesListeners(t *testing.T) {
	b, _ := captureFixture(t)
	var controller *pl.Lease
	captureMust(t, b.resource.Borrow(context.Background(), func(_ context.Context, launch *pl.AcceptedLaunch) error { controller = launch.Self; return nil }))
	generation, err := controller.CurrentGeneration()
	captureMust(t, err)
	control, err := in.OwnControlDispatcher(context.Background(), b.runtime, controller, generation, 1, 32768)
	captureMust(t, err)
	defer func() { control.Close(); captureMust(t, control.WaitClosed(context.Background())) }()
	root, err := os.MkdirTemp("", "reader-admit-")
	captureMust(t, err)
	defer os.RemoveAll(root)
	b.admission.maxGrants = 1
	b.admission.readers = []nativeFrameReaderPlan{
		{Path: filepath.Join(root, "delivery.sock"), Grant: "reader", Stream: "analysis", Delivery: true, ListenerBytes: 8192, AcceptBytes: 4096, MaxBytes: 192, MaxWorkNS: uint64(time.Second)},
		{Path: filepath.Join(root, "roi.sock"), Grant: "reader", ListenerBytes: 4 << 20, AcceptBytes: 4096, MaxBytes: 1024, MaxWorkNS: uint64(time.Second)},
	}
	before := b.runtime.Stats().LiveResources
	ready := make(chan struct{})
	err = b.serveFrameReaders(context.Background(), control, ready)
	if err == nil {
		t.Fatal("oversubscribed endpoint admitted")
	}
	select {
	case <-ready:
		t.Fatal("partial endpoints reported ready")
	default:
	}
	for _, plan := range b.admission.readers {
		if _, err := os.Lstat(plan.Path); !os.IsNotExist(err) {
			t.Fatal("partial listener retained", err)
		}
	}
	if b.runtime.Stats().LiveResources != before {
		t.Fatal("failed listener admission leaked resources", b.runtime.Stats())
	}
}
