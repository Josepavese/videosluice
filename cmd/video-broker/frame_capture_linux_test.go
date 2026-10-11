//go:build linux && cgo

package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	"github.com/Josepavese/videosluice/framedomain/entropy"
	"github.com/Josepavese/videosluice/framedomain/monotime"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	linux "github.com/Josepavese/videosluice/framepeer/provider/linux"
	wire "github.com/Josepavese/videosluice/framewire"
	snapshotlinux "github.com/Josepavese/videosluice/framewire/provider/linux"
	"github.com/Josepavese/videosluice/internal/source"
	"golang.org/x/sys/unix"
)

type sourceFixtureAdmission struct{}

// Fixture-only receipt: this helper has no camera, driver or accelerator. It
// verifies the lifecycle contract and cannot serve as production admission.
type fixtureExecution struct {
	command                           *exec.Cmd
	attachErr, startedErr, releaseErr error
	releaseGate                       <-chan struct{}
	releaseCalls                      *atomic.Int32
}

func (f *fixtureExecution) Attach(cmd *exec.Cmd) error {
	f.command = cmd
	return f.attachErr
}
func (f *fixtureExecution) Started(p *os.Process) error {
	if f.command == nil || f.command.Process != p {
		return peer.ErrInvalid
	}
	return f.startedErr
}
func (f *fixtureExecution) Release() error {
	if f.command != nil && f.command.Process != nil && f.command.ProcessState == nil {
		return peer.ErrBusy
	}
	if f.releaseCalls != nil {
		f.releaseCalls.Add(1)
	}
	if f.releaseGate != nil {
		<-f.releaseGate
	}
	return f.releaseErr
}

func TestNativeFrameCaptureRetireDeadlineObservesSameRelease(t *testing.T) {
	b, plan := captureFixture(t)
	admit := b.admission.source
	releaseGate := make(chan struct{})
	var releaseCalls atomic.Int32
	b.admission.source = func(ctx context.Context, plan source.BrokerMuxPlan) (nativeFrameSourceAdmission, error) {
		a, err := admit(ctx, plan)
		if err == nil {
			a.Execution = func(context.Context) (source.FrameExecution, error) {
				return &fixtureExecution{releaseGate: releaseGate, releaseCalls: &releaseCalls}, nil
			}
		}
		return a, err
	}
	capture, err := b.FrameCaptures()(context.Background(), plan)
	captureMust(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if err := capture.Retire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first wait must time out without claiming release: %v", err)
	}
	if releaseCalls.Load() != 1 {
		t.Fatalf("release started %d times", releaseCalls.Load())
	}
	close(releaseGate)
	settle, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	captureMust(t, capture.Retire(settle))
	captureMust(t, capture.Retire(settle))
	if releaseCalls.Load() != 1 {
		t.Fatalf("release repeated after waiter timeout: %d", releaseCalls.Load())
	}
}

func (sourceFixtureAdmission) Attest(_ context.Context, s fa.Scope) (fa.CopyAdmission, error) {
	return fa.CopyAdmission{Provider: copyport.CPU{}.Capability().Provider, MediaEpoch: s.MediaEpoch, Graph: s.Graph, Evidence: fa.Digest{9}, Attested: true}, nil
}
func captureMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func captureFixture(t *testing.T) (*nativeFrameBootstrap, source.BrokerMuxPlan) {
	return captureFixtureResources(t, 12)
}

func captureFixtureResources(t *testing.T, resourceLimit int) (*nativeFrameBootstrap, source.BrokerMuxPlan) {
	t.Helper()
	pidfd, err := unix.PidfdOpen(os.Getpid(), 0)
	captureMust(t, err)
	file := os.NewFile(uintptr(pidfd), "capture-fixture-controller")
	controller, err := linux.ImportProcessCapability(file, linux.ProcessIdentity{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: "source.controller"})
	if err != nil {
		file.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { controller.Close() })
	clock := monotime.New()
	r, err := fd.New(controller, entropy.System{}, clock, fd.Limits{MaxSources: 2, MaxStreams: 2, MaxSourceGenerations: 4, MaxMemoryBytes: 4 << 20, MaxSourceBytes: 2 << 20, MaxBuffers: 4, MaxBufferBytes: 8192, MaxResources: resourceLimit, MaxResourceOperations: 8})
	captureMust(t, err)
	t.Cleanup(r.Revoke)
	bootstrap, err := fd.OwnRuntimeResource(context.Background(), r, fd.ResourcePlan[*linux.AcceptedLaunch]{Bytes: 4096, Create: func(context.Context) (fd.Created[*linux.AcceptedLaunch], error) {
		return fd.Created[*linux.AcceptedLaunch]{Value: &linux.AcceptedLaunch{Self: controller}, Owned: true}, nil
	}, Release: func(*linux.AcceptedLaunch) error { return nil }})
	captureMust(t, err)
	a := nativeFrameSourceAdmission{Layout: fd.Layout{Source: "front", Graph: fa.Digest{1}, Streams: []string{"analysis"}, Surface: "source_decoded", Geometry: fa.Geometry{Width: 32, Height: 16, Stride: 32}, DecodedWidth: 32, DecodedHeight: 16}, Limits: fa.Limits{MaxFrames: 4, MaxBindings: 8, PoolBytes: 8192, MaxFrameBytes: 768, MaxArtifactBytes: 4096, MaxOutputPerRequest: 1024, MaxFullFrameBytes: 768, MaxRegions: 4, MaxResults: 4, MaxAgeNS: 5_000_000_000, MaxCopyNS: 100_000_000}, Copier: copyport.CPU{}, Admission: sourceFixtureAdmission{}, Policy: fa.CopyPolicy{AllowSoftware: true}, SurfaceDigest: fa.Digest{7}, CaptureBytes: 1 << 20, ReceiveNS: 50_000_000, CopyNS: 100_000_000}
	a.StartupNS = 500_000_000
	a.Deliveries = map[string]in.DeliveryLayout{"analysis": {Codec: wire.NV12, StrideY: 16, StrideUV: 16, MaxPayload: 192, Transform: fa.Transform{Full: a.Layout.Geometry, Crop: fa.Rect{X2: 32, Y2: 16}, ResizedWidth: 16, ResizedHeight: 8, DeliveryWidth: 16, DeliveryHeight: 8}}}
	a.DeliveryAllocator, err = snapshotlinux.New(snapshotlinux.Limits{Bytes: 1 << 20, Allocations: 4, Operations: 2})
	captureMust(t, err)
	a.Execution = func(context.Context) (source.FrameExecution, error) { return &fixtureExecution{}, nil }
	mailboxBytes, err := snapshotlinux.SourceMailboxBytes(uint64(wire.HeaderSize + a.Limits.MaxFrameBytes))
	captureMust(t, err)
	a.MailboxBytes = int(mailboxBytes) + 4096
	a.OutputCapacity = wire.HeaderSize + 192
	outputBytes, err := snapshotlinux.SourceMailboxBytes(uint64(a.OutputCapacity))
	captureMust(t, err)
	a.OutputMailboxBytes = int(outputBytes) + 4096
	a.Mailbox = func(mapping wire.Mapping) (nativeSurfaceMailbox, error) {
		m, err := snapshotlinux.NewSourceMailbox(mapping)
		if m == nil {
			return nil, err
		}
		return m, err
	}
	b := &nativeFrameBootstrap{runtime: r, resource: bootstrap, clock: clock, admission: nativeFrameAdmission{source: func(context.Context, source.BrokerMuxPlan) (nativeFrameSourceAdmission, error) { return a, nil }}}
	plan := source.BrokerMuxPlan{SourceID: "front", MediaEpoch: 999, Transport: "v4l2_uvc", WorkerProvider: "native_sp", CaptureDevicePath: "/dev/fixture", CaptureFormat: "mjpeg", MJPEGDecodeHWAvailable: true, H265EncodeHWAvailable: true, CaptureWidth: 32, CaptureHeight: 16, CaptureFPS: 20, LiveEnabled: true, LiveCodec: "h265", MainWidth: 32, MainHeight: 16, MainFPS: 20, SubWidth: 16, SubHeight: 8, SubFPS: 20, LiveBitrateKbps: 1024}
	return b, plan
}

func compileCaptureWorker(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../..")
	captureMust(t, err)
	native := filepath.Join(root, "native")
	wireNative := filepath.Join(root, "framewire/native")
	peerNative := filepath.Join(root, "framepeer/provider/linux/native")
	binary := filepath.Join(t.TempDir(), "capture-worker")
	cmd := exec.Command("cc", "-std=c11", "-O1", "-g", "-Wall", "-Wextra", "-Wpedantic", "-Werror", "-pthread", "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-fno-pie", "-no-pie", "-I"+native, "-I"+wireNative, "-I"+peerNative, "-I"+filepath.Join(root, "framepeer/native"), filepath.Join(native, "tests/frame_capture_worker_test.c"), filepath.Join(native, "frame_source_linux.c"), filepath.Join(native, "source_publisher.c"), filepath.Join(wireNative, "causal_envelope.c"), filepath.Join(wireNative, "causal_chunks.c"), filepath.Join(peerNative, "launch.c"), "-lcrypto", "-o", binary)
	cmd.Args = append(cmd.Args, filepath.Join(wireNative, "causal_snapshot.c"), filepath.Join(wireNative, "atomic_snapshot.c"), filepath.Join(wireNative, "../provider/linux/native/source_mailbox.c"))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("native capture compile: %v\n%s", err, output)
	}
	return binary
}

// Independent oracle for the native C fixture, not the production crop code.
func capturePatternPixel(source string, epoch, sequence uint64, x, y int, chroma bool) byte {
	seed := byte(0)
	for _, c := range []byte(source) {
		seed = seed*33 + c
	}
	seed += byte(epoch)*47 + byte(sequence)*29
	if chroma {
		return seed + byte(73+(x/2)*11+y*29+(x%2)*101)
	}
	return seed + byte(x*3+y*17)
}

func TestNativeFrameCaptureMuxLifecycleAndRestart(t *testing.T) {
	binary := compileCaptureWorker(t)
	b, plan := captureFixture(t)
	var previous uint64
	for cycle := 0; cycle < 2; cycle++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		capture, err := b.FrameCaptures()(ctx, plan)
		captureMust(t, err)
		c := capture.(*nativeFrameCapture)
		if c.SourceEpoch() <= previous || c.SourceEpoch() == plan.MediaEpoch {
			t.Fatal("capture reused/invented generation")
		}
		previous = c.SourceEpoch()
		plan.MediaEpoch = c.SourceEpoch()
		plan.V4L2BridgePath = binary
		// The 5s functional deadline still covers publication, exact pixels,
		// ROI and the native close acknowledgement. Sanitizer shutdown follows
		// main() and can suspend the worker using a tracer; it is not source I/O.
		workerCtx, workerCancel := context.WithTimeout(context.Background(), 30*time.Second)
		t.Cleanup(workerCancel)
		cmd, _, err := source.BuildBrokerMuxCommand(workerCtx, plan)
		captureMust(t, err)
		cmd.Args = append(cmd.Args, "--fixture-exec-ready", "--fixture-defer-bootstrap", "--fixture-step-frames", "--fixture-step-surface")
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		// Existing inherited descriptors are preserved: FAP starts at fd4 here.
		extra, err := os.Open(os.DevNull)
		captureMust(t, err)
		cmd.ExtraFiles = []*os.File{extra}
		captureMust(t, c.Attach(cmd))
		input, err := cmd.StdinPipe()
		captureMust(t, err)
		output, err := cmd.StdoutPipe()
		captureMust(t, err)
		captureMust(t, cmd.Start())
		extra.Close()
		waited := false
		defer func() {
			cancel()
			workerCancel()
			input.Close()
			if !waited {
				cmd.Wait()
			}
			c.Retire(context.Background())
			if t.Failed() {
				t.Log(stderr.String())
			}
		}()
		// The short 50ms receive window tests source-idle handling below, not
		// dynamic linking and AddressSanitizer initialization before C main().
		// Synchronize this synthetic fixture within the SAME 5s functional
		// deadline. The additional gate below then exercises a slow bootstrap
		// independently of the unchanged 50ms per-frame receive deadline.
		reader := bufio.NewReader(output)
		readDone := make(chan struct{})
		stopRead := context.AfterFunc(ctx, func() { output.Close(); close(readDone) })
		line, err := reader.ReadString('\n')
		if !stopRead() {
			<-readDone
		}
		captureMust(t, err)
		captureMust(t, ctx.Err())
		if line != "exec-ready\n" {
			t.Fatalf("native fixture startup marker: %q", line)
		}
		bootstrapGate := make(chan error, 1)
		go func() {
			select {
			case <-ctx.Done():
				bootstrapGate <- ctx.Err()
			case <-time.After(3 * time.Duration(c.admission.ReceiveNS)):
				_, err := input.Write([]byte{3})
				bootstrapGate <- err
			}
		}()
		captureMust(t, c.Started(ctx, cmd.Process))
		captureMust(t, <-bootstrapGate)
		if err := c.Started(ctx, cmd.Process); err == nil {
			t.Fatal("replayed launch accepted")
		}
		line, err = reader.ReadString('\n')
		captureMust(t, err)
		if line != "bound\n" {
			t.Fatal(line)
		}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		deliveries := make(chan wire.Header, 2)
		go func() {
			done <- c.Run(runCtx, func(_ context.Context, h wire.Header, p []byte) error {
				if h.Kind != wire.Delivery || h.OutputSequence != 0 || h.SourceEpoch != c.SourceEpoch() || len(p) != 192 {
					return errors.New("capture observer received invalid delivery")
				}
				deliveries <- h
				return nil
			})
		}()
		// Cross several empty receive windows; idle is not a source failure.
		select {
		case err := <-done:
			t.Fatal("warmup retired capture", err)
		case <-time.After(160 * time.Millisecond):
		}
		if err := c.Retire(ctx); !errors.Is(err, peer.ErrBusy) {
			t.Fatal("retired an active receiver", err)
		}
		_, err = input.Write([]byte{1})
		captureMust(t, err)
		stageSurface := func(want uint64) {
			line, err := reader.ReadString('\n')
			captureMust(t, err)
			if line != "surface-published\n" {
				t.Fatal(line)
			}
			for c.surfaces.Stats().StagedFrames < want {
				select {
				case err := <-done:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			_, err = input.Write([]byte{6})
			captureMust(t, err)
		}
		stageSurface(1)
		line, err = reader.ReadString('\n')
		captureMust(t, err)
		if line != "first-published\n" {
			t.Fatal(line)
		}
		for c.surfaces.Stats().StagedFrames < 1 || c.receiver.Stats().Outputs < 1 {
			select {
			case err := <-done:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
		_, err = input.Write([]byte{5})
		captureMust(t, err)
		stageSurface(2)
		line, err = reader.ReadString('\n')
		captureMust(t, err)
		if line != "published\n" {
			t.Fatal(line)
		}
		for c.surfaces.Stats().StagedFrames < 2 || c.receiver.Stats().Outputs < 2 {
			select {
			case err := <-done:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
		for seq := uint64(1); seq <= 2; seq++ {
			h := <-deliveries
			if h.OriginSequence != seq {
				t.Fatal("delivery origin replaced")
			}
			ref := fd.DeliveryRef{Fence: c.binding.Fence(), Ref: fa.DeliveryRef{Stream: h.Stream, Epoch: h.PublicationEpoch, FrameID: h.DeliveryID, Graph: h.Graph}}
			s := c.binding.Scope()
			result, err := b.runtime.Extract(ctx, fd.Request{ID: [16]byte{byte(seq)}, Frame: ref, Source: s.Source, Surface: s.Surface, Regions: []fa.Region{{ID: "roi", BBox: [4]float64{1, 1, 3, 3}}}, DeadlineNS: b.clock.NowNS() + 100_000_000})
			captureMust(t, err)
			captureMust(t, result.Visit(0, func(artifact fd.Artifact, pixels []byte) error {
				if artifact.State != fa.Complete || len(pixels) != 24 {
					return fmt.Errorf("ROI mismatch: %v/%d", artifact.State, len(pixels))
				}
				for offset, p := range pixels {
					x, y, chroma := 2+offset%4, 2+offset/4, false
					if offset >= 16 {
						x, y, chroma = 2+(offset-16)%4, 1+(offset-16)/4, true
					}
					if p != capturePatternPixel(h.Source, h.SourceEpoch, seq, x, y, chroma) {
						return fmt.Errorf("ROI pixel mismatch offset=%d origin=%d", offset, seq)
					}
				}
				return fd.VerifyDomainProof(artifact.Proof, c.binding.Fence())
			}))
			result.Close()
		}
		captureMust(t, c.Snapshot(ctx, "analysis", 0, func(_ context.Context, h wire.Header, pixels []byte) error {
			if h.OriginSequence != 2 || h.SourceEpoch != c.SourceEpoch() || len(pixels) != 192 {
				return errors.New("latest delivery identity")
			}
			for offset, pixel := range pixels {
				x, y, chroma := 2*(offset%16), 2*(offset/16), false
				if offset >= 128 {
					column := (offset - 128) % 16
					x, y, chroma = 2*(column&^1)+(column&1), 2*((offset-128)/16), true
				}
				if pixel != capturePatternPixel(h.Source, h.SourceEpoch, 2, x, y, chroma) {
					return fmt.Errorf("latest delivery pixel mismatch offset=%d", offset)
				}
			}
			return nil
		}))
		stop()
		<-done
		if err := c.Retire(ctx); !errors.Is(err, peer.ErrBusy) {
			t.Fatal("released before process reaping", err)
		}
		c.binding.Close() // retirement cannot reclaim the still-alive worker
		if b.runtime.Stats().ReservedResourceBytes < c.admission.CaptureBytes {
			t.Fatal("live worker reservation disappeared")
		}
		_, err = input.Write([]byte{2})
		captureMust(t, err)
		type closeReply struct {
			line string
			err  error
		}
		closed := make(chan closeReply, 1)
		go func() {
			line, err := reader.ReadString('\n')
			closed <- closeReply{line, err}
		}()
		select {
		case reply := <-closed:
			captureMust(t, reply.err)
			if reply.line != "closed\n" {
				t.Fatalf("native source close acknowledgement: %q", reply.line)
			}
			captureMust(t, ctx.Err())
			if deadline, ok := ctx.Deadline(); !ok || !time.Now().Before(deadline) {
				t.Fatal("native source close acknowledgement was late")
			}
		case <-ctx.Done():
			t.Fatal("native source close exceeded functional deadline", ctx.Err())
		}
		closedAt := time.Now()
		waited = true
		waitDone := make(chan error, 1)
		go func() { waitDone <- cmd.Wait() }()
		select {
		case err := <-waitDone:
			captureMust(t, err)
		case <-time.After(time.Second):
			// Keep visibility into post-main teardown; never disable leak
			// checking or accept a nonzero sanitizer/process exit.
			proc := fmt.Sprintf("/proc/%d", cmd.Process.Pid)
			wchan, wchanErr := os.ReadFile(filepath.Join(proc, "wchan"))
			children, childrenErr := os.ReadFile(filepath.Join(proc, "task", fmt.Sprint(cmd.Process.Pid), "children"))
			t.Logf("capture lifecycle phase=worker_exit_wait pid=%d wchan=%q wchan_error=%v children=%q children_error=%v",
				cmd.Process.Pid, strings.TrimSpace(string(wchan)), wchanErr, strings.TrimSpace(string(children)), childrenErr)
			captureMust(t, <-waitDone)
		}
		t.Logf("capture lifecycle phase=post_main_exit_complete elapsed_ms=%d", time.Since(closedAt).Milliseconds())
		if strings.Count(stderr.String(), "event=integrity_ready ") != 1 ||
			!strings.Contains(stderr.String(), "phase=before_source_ready") {
			t.Fatalf("missing one-time source integrity readiness diagnostic: %s", stderr.String())
		}
		workerCancel()
		retireCtx, retireCancel := context.WithTimeout(context.Background(), 5*time.Second)
		captureMust(t, c.Retire(retireCtx))
		captureMust(t, c.Retire(retireCtx))
		retireCancel()
		cancel()
		if stats := b.runtime.Stats(); stats.ReservedResourceBytes != 4096 || stats.LiveResources != 1 {
			t.Fatal("source not joined", stats)
		}
	}
}

func TestNativeFrameCaptureAdmissionAndFailedStart(t *testing.T) {
	b, plan := captureFixture(t)
	wrong := plan
	wrong.CaptureWidth = 64
	if _, err := b.FrameCaptures()(context.Background(), wrong); err == nil {
		t.Fatal("unadmitted camera layout accepted")
	}
	capture, err := b.FrameCaptures()(context.Background(), plan)
	captureMust(t, err)
	c := capture.(*nativeFrameCapture)
	cmd := exec.Command("/nonexistent/frame-capture-test", "--source-id", "front", "--media-epoch", fmt.Sprint(c.SourceEpoch()))
	bad := exec.Command("/unused", "--source-id", "rear", "--media-epoch", fmt.Sprint(c.SourceEpoch()))
	if err := c.Attach(bad); err == nil {
		t.Fatal("cross-source command accepted")
	}
	captureMust(t, c.Attach(cmd))
	if err := c.Attach(cmd); err == nil {
		t.Fatal("duplicate attach accepted")
	}
	if err := cmd.Start(); err == nil {
		t.Fatal("fixture unexpectedly started")
	}
	captureMust(t, c.Retire(context.Background()))
	if stats := b.runtime.Stats(); stats.ReservedResourceBytes != 4096 || stats.LiveResources != 1 {
		t.Fatal("failed Start leaked", stats)
	}
}

// A late consumer cannot poison the native output transport: the next complete
// mailbox snapshot recovers on the same worker, capture epoch and authority.
func TestNativeFrameCaptureSlowConsumerRecoversWithoutSourceRestart(t *testing.T) {
	binary := compileCaptureWorker(t)
	b, plan := captureFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	capture, err := b.FrameCaptures()(ctx, plan)
	captureMust(t, err)
	c := capture.(*nativeFrameCapture)
	plan.MediaEpoch, plan.V4L2BridgePath = c.SourceEpoch(), binary
	workerCtx, workerCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer workerCancel()
	cmd, _, err := source.BuildBrokerMuxCommand(workerCtx, plan)
	captureMust(t, err)
	cmd.Args = append(cmd.Args, "--fixture-exec-ready", "--fixture-defer-bootstrap")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	captureMust(t, c.Attach(cmd))
	input, err := cmd.StdinPipe()
	captureMust(t, err)
	output, err := cmd.StdoutPipe()
	captureMust(t, err)
	captureMust(t, cmd.Start())
	defer func() {
		workerCancel()
		input.Close()
		if cmd.ProcessState == nil {
			cmd.Wait()
		}
		c.Retire(context.Background())
		if t.Failed() {
			t.Log(stderr.String())
		}
	}()
	reader := bufio.NewReader(output)
	stopRead := context.AfterFunc(ctx, func() { output.Close() })
	defer stopRead()
	line, err := reader.ReadString('\n')
	captureMust(t, err)
	if line != "exec-ready\n" {
		t.Fatal(line)
	}
	_, err = input.Write([]byte{3})
	captureMust(t, err)
	captureMust(t, c.Started(ctx, cmd.Process))
	line, err = reader.ReadString('\n')
	captureMust(t, err)
	if line != "bound\n" {
		t.Fatal(line)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	var calls atomic.Uint64
	var recovered atomic.Bool
	go func() {
		done <- c.Run(runCtx, func(ctx context.Context, h wire.Header, pixels []byte) error {
			if calls.Add(1) == 1 {
				<-ctx.Done()
				return nil
			}
			if h.SourceEpoch != c.SourceEpoch() || len(pixels) != 192 {
				return errors.New("invalid recovered output")
			}
			if h.OriginSequence >= 3 {
				recovered.Store(true)
			}
			return nil
		})
	}()
	defer func() { stop(); <-done }()
	_, err = input.Write([]byte{1})
	captureMust(t, err)
	line, err = reader.ReadString('\n')
	captureMust(t, err)
	if line != "published\n" {
		t.Fatal(line)
	}
	// This unpaced producer is deliberately lossy under Store contention. The
	// exact two-surface ROI oracle is separately step-synchronized; here prove
	// deadline isolation and recovery of a NEW output, not every source frame.
	for c.receiver.Stats().Drops == 0 {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	_, err = input.Write([]byte{4})
	captureMust(t, err)
	line, err = reader.ReadString('\n')
	captureMust(t, err)
	if line != "published\n" {
		t.Fatal(line)
	}
	for !recovered.Load() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	if c.receiver.Stats().Failures != 0 || calls.Load() < 2 {
		t.Fatal("late consumer prevented next snapshot", c.receiver.Stats(), calls.Load())
	}
	select {
	case err := <-done:
		// Keep the deferred join valid even on this failing assertion.
		done <- err
		t.Fatal("analysis transport retired whole capture", err)
	default:
	}
	if err := c.Retire(ctx); !errors.Is(err, peer.ErrBusy) {
		t.Fatal("live capture ownership was released", err)
	}
	_, err = input.Write([]byte{2})
	captureMust(t, err)
	line, err = reader.ReadString('\n')
	captureMust(t, err)
	if line != "closed\n" {
		t.Fatal(line)
	}
	captureMust(t, cmd.Wait())
}

func TestNativeCaptureExecutionAdmissionAndRelease(t *testing.T) {
	for _, mode := range []string{"missing", "nil_result", "release_failed", "partial_create_failed"} {
		t.Run(mode, func(t *testing.T) {
			b, plan := captureFixture(t)
			original := b.admission.source
			execution := &fixtureExecution{releaseErr: errors.New("native release receipt missing")}
			b.admission.source = func(ctx context.Context, p source.BrokerMuxPlan) (nativeFrameSourceAdmission, error) {
				a, err := original(ctx, p)
				switch mode {
				case "missing":
					a.Execution = nil
				case "nil_result":
					a.Execution = func(context.Context) (source.FrameExecution, error) { return nil, nil }
				default:
					a.Execution = func(context.Context) (source.FrameExecution, error) {
						if mode == "partial_create_failed" {
							return execution, errors.New("partially acquired native execution")
						}
						return execution, nil
					}
				}
				return a, err
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			capture, err := b.FrameCaptures()(ctx, plan)
			if mode == "release_failed" {
				captureMust(t, err)
				if err = capture.Retire(ctx); err == nil {
					t.Fatal("missing native release receipt accepted")
				}
				if err = capture.Retire(ctx); err == nil {
					t.Fatal("release failure erased")
				}
			} else if err == nil || capture != nil {
				t.Fatal("invalid native execution admitted", capture, err)
			}
			for {
				stats := b.runtime.Stats()
				if mode == "release_failed" || mode == "partial_create_failed" {
					if stats.ResourceFailures > 0 {
						if stats.ReservedResourceBytes < (1<<20)+4096 {
							t.Fatal("failed native release uncharged", stats)
						}
						break
					}
				} else if stats.ReservedResourceBytes == 4096 && stats.LiveResources == 1 {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("resource lifecycle did not settle", stats)
				case <-time.After(time.Millisecond):
				}
			}
		})
	}
}

func TestNativeCaptureExecutionFailurePrecedesHardwareBootstrap(t *testing.T) {
	binary := compileCaptureWorker(t)
	b, plan := captureFixture(t)
	original := b.admission.source
	refused := errors.New("kernel boundary confirmation failed")
	b.admission.source = func(ctx context.Context, p source.BrokerMuxPlan) (nativeFrameSourceAdmission, error) {
		a, err := original(ctx, p)
		a.Execution = func(context.Context) (source.FrameExecution, error) {
			return &fixtureExecution{startedErr: refused}, nil
		}
		return a, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	capture, err := b.FrameCaptures()(ctx, plan)
	captureMust(t, err)
	c := capture.(*nativeFrameCapture)
	plan.MediaEpoch, plan.V4L2BridgePath = c.SourceEpoch(), binary
	cmd, _, err := source.BuildBrokerMuxCommand(ctx, plan)
	captureMust(t, err)
	captureMust(t, c.Attach(cmd))
	captureMust(t, cmd.Start())
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		_ = c.Retire(context.Background())
	}()
	if err := c.Started(ctx, cmd.Process); !errors.Is(err, refused) {
		t.Fatal(err)
	}
	if c.receiver != nil || c.started {
		t.Fatal("hardware bootstrap ran before kernel confirmation")
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	waited = true
	captureMust(t, c.Retire(ctx))
	if stats := b.runtime.Stats(); stats.ReservedResourceBytes != 4096 || stats.LiveResources != 1 {
		t.Fatal(stats)
	}
}

func TestNativeCaptureBootstrapPrecedesAcceleratorSetup(t *testing.T) {
	// Guards the real worker composition, not the synthetic source helper. GPU
	// tone setup must obey the same admission boundary as the camera and codecs.
	data, err := os.ReadFile("../../native/rdkx5_v4l2_mux_bridge.c")
	captureMust(t, err)
	text := string(data)
	start := strings.Index(text, "int main(")
	if start < 0 {
		t.Fatal("worker entrypoint missing")
	}
	text = text[start:]
	previous := -1
	for _, call := range []string{"memset(&tone_mapper, 0, sizeof(tone_mapper))", "video_broker_frame_source_open(", "video_broker_nv12_tone_mapper_init(", "video_broker_tone_accelerator_init(", "prepare_v4l2_capture("} {
		position := strings.Index(text, call)
		if position <= previous {
			t.Fatalf("hardware bootstrap ordering broken at %s", call)
		}
		previous = position
	}
	if !strings.Contains(text, "action=reject_before_hardware_init") {
		t.Fatal("missing pre-hardware failure diagnostic")
	}
}

func TestNativeCaptureStartupBudgetIsBoundedAndNotReplayed(t *testing.T) {
	b, plan := captureFixture(t)
	admit := b.admission.source
	b.admission.source = func(ctx context.Context, p source.BrokerMuxPlan) (nativeFrameSourceAdmission, error) {
		a, err := admit(ctx, p)
		a.StartupNS = 20_000_000
		return a, err
	}
	capture, err := b.FrameCaptures()(context.Background(), plan)
	captureMust(t, err)
	c := capture.(*nativeFrameCapture)
	// No hardware: exercise an exact live process which never answers the
	// private bootstrap. fixtureExecution is not an SDK release certificate.
	cmd := exec.Command("/bin/bash", "-c", "exec sleep 5", "fixture", "--source-id", plan.SourceID, "--media-epoch", fmt.Sprint(c.SourceEpoch()))
	captureMust(t, c.Attach(cmd))
	captureMust(t, cmd.Start())
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); captureMust(t, c.Retire(context.Background())) }()
	start := time.Now()
	err = c.Started(context.Background(), cmd.Process)
	if !(errors.Is(err, peer.ErrDeadline) || errors.Is(err, context.DeadlineExceeded)) || time.Since(start) > time.Second {
		t.Fatal("missing startup response escaped its deadline", err, time.Since(start))
	}
	if err = c.Started(context.Background(), cmd.Process); !errors.Is(err, peer.ErrInvalid) {
		t.Fatal("bootstrap replay accepted", err)
	}
}

func TestNativeCaptureJPEGGeometryPrecedesDecoderAdmission(t *testing.T) {
	data, err := os.ReadFile("../../native/rdkx5_v4l2_mux_bridge.c")
	captureMust(t, err)
	text := string(data)
	start := strings.Index(text, "int main(")
	if start < 0 {
		t.Fatal("worker entrypoint missing")
	}
	text = text[start:]
	guard := strings.Index(text, "video_broker_jpeg_frame_inspect(capture_source, capture_source_size, &header)")
	if guard < 0 {
		t.Fatal("missing encoded geometry gate")
	}
	for _, call := range []string{"video_broker_rdkx5_mjpeg_trial_start(", "video_broker_rdkx5_async_decoder_submit(", "decode_capture_frame_to_nv12("} {
		if at := strings.Index(text, call); at <= guard {
			t.Fatalf("%s must follow encoded geometry admission", call)
		}
	}
	if !strings.Contains(text[guard:], "header.width != cap.width || header.height != cap.height") ||
		!strings.Contains(text[guard:], "sdk_submission=false action=retire_source") {
		t.Fatal("missing geometry match or diagnostic")
	}
}
