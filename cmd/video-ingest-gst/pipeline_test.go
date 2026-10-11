// Tests pin the portable video-broker graph contract.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func buildIsolatedGStreamerArgsForTest(cfg ingestConfig, elements gstElements) ([]string, error) {
	buses := gstreamerBusPaths{
		raw:       "/run/video-broker/test/raw.sock",
		sourceRaw: "/run/video-broker/test/source-raw.sock",
		native:    "/run/video-broker/test/native.sock",
	}
	args, err := buildGStreamerCaptureArgs(cfg, elements, buses)
	if err != nil {
		return nil, err
	}
	for _, role := range []struct {
		name    string
		enabled bool
	}{
		{name: "live", enabled: cfg.liveEnabled},
		{name: "analysis", enabled: cfg.analysisEnabled},
		{name: "archive", enabled: cfg.archiveEnabled},
	} {
		if !role.enabled {
			continue
		}
		roleArgs, err := buildGStreamerRoleArgs(cfg, elements, buses, role.name)
		if err != nil {
			return nil, err
		}
		args = append(args, roleArgs...)
	}
	return args, nil
}

func TestGStreamerChildLifecycleEscalationPrecedesExecFallback(t *testing.T) {
	if gstChildTerminationGrace >= gstChildProcessWaitFallback {
		t.Fatalf("GStreamer group escalation must precede leader-only exec fallback: grace=%s fallback=%s", gstChildTerminationGrace, gstChildProcessWaitFallback)
	}
}

func TestGStreamerChildLifecycleTerminatesFrozenOwnedGroup(t *testing.T) {
	tmpDir := t.TempDir()
	parentReceipt := filepath.Join(tmpDir, "parent.closed")
	childReceipt := filepath.Join(tmpDir, "child.closed")
	childPIDPath := filepath.Join(tmpDir, "child.pid")
	parentReady := filepath.Join(tmpDir, "parent.ready")
	childReady := filepath.Join(tmpDir, "child.ready")
	childCtx, childCancel := context.WithCancel(context.Background())
	defer childCancel()
	cmd := exec.CommandContext(childCtx, "/bin/bash", "-c", `
/bin/bash -c 'trap '\''printf child >"$1"; exit 0'\'' TERM; printf ready >"$2"; while :; do sleep 0.05; done' child "$1" "$5" &
child_pid=$!
printf '%s' "$child_pid" >"$3"
trap 'printf parent >"$2"; wait "$child_pid"; exit 0' TERM
printf ready >"$4"
while :; do sleep 0.05; done
`, "gst-child", childReceipt, parentReceipt, childPIDPath, parentReady, childReady)
	configureGStreamerChildLifecycle(cmd)
	if err := cmd.Start(); err != nil {
		t.Skipf("bash unavailable: %v", err)
	}
	cleanupArmed := true
	t.Cleanup(func() {
		if cleanupArmed {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	})
	deadline := time.Now().Add(time.Second)
	for {
		_, parentErr := os.Stat(parentReady)
		_, childErr := os.Stat(childReady)
		if parentErr == nil && childErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"GStreamer child group did not reach readiness: parent=%v child=%v",
				parentErr, childErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	childPIDRaw, err := os.ReadFile(childPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(childPIDRaw))
	if err != nil || childPID <= 0 {
		t.Fatalf("invalid GStreamer descendant pid %q: %v", childPIDRaw, err)
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil || pgid != cmd.Process.Pid {
		t.Fatalf("GStreamer child is not group leader: pgid=%d err=%v", pgid, err)
	}
	if childPGID, err := syscall.Getpgid(childPID); err != nil || childPGID != pgid {
		t.Fatalf("GStreamer descendant escaped owned group: pgid=%d expected=%d err=%v", childPGID, pgid, err)
	}
	if err := syscall.Kill(-pgid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	for _, pid := range []int{cmd.Process.Pid, childPID} {
		deadline := time.Now().Add(time.Second)
		for {
			raw, readErr := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
			if readErr == nil {
				text := string(raw)
				closing := strings.LastIndex(text, ") ")
				if closing >= 0 && len(text) > closing+2 && text[closing+2] == 'T' {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("GStreamer process %d did not enter frozen state: %v", pid, readErr)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	child := &isolatedGStreamerChild{name: "lifecycle-test", cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		child.mu.Lock()
		child.waitErr = err
		child.mu.Unlock()
		close(child.done)
	}()
	waitErr := child.stop(500 * time.Millisecond)
	childCancel()
	cleanupArmed = false
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("GStreamer child group did not exit cooperatively: %v", waitErr)
	}
	for _, receipt := range []string{parentReceipt, childReceipt} {
		if _, err := os.Stat(receipt); err != nil {
			t.Fatalf("GStreamer cooperative receipt missing %s: %v", receipt, err)
		}
	}
}

func TestIsolatedGStreamerChildStopNeverBlocksAfterKill(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), "/bin/bash", "-c", "trap '' TERM; while :; do sleep 1; done")
	configureGStreamerChildLifecycle(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	}()
	child := &isolatedGStreamerChild{name: "stuck-test", cmd: cmd, done: make(chan struct{})}
	started := time.Now()
	err := child.stop(25 * time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "did not exit after group kill") {
		t.Fatalf("expected bounded post-kill error, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("post-kill wait exceeded hard bound: %s", elapsed)
	}
}

func TestIsolatedGStreamerChildStopIsIdempotentAfterExpectedSignal(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), "/bin/sh", "-c", "kill -TERM $$")
	configureGStreamerChildLifecycle(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	child := &isolatedGStreamerChild{name: "already-stopped-test", cmd: cmd, done: make(chan struct{})}
	err := cmd.Wait()
	child.mu.Lock()
	child.waitErr = err
	child.mu.Unlock()
	close(child.done)
	if err := child.stop(time.Millisecond); err != nil {
		t.Fatalf("expected signal was not normalized on repeated stop: %v", err)
	}
}

func TestWaitForGStreamerBusAcceptsOnlyUnixSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raw.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	child := &isolatedGStreamerChild{done: make(chan struct{})}
	if err := waitForGStreamerBus(context.Background(), child, path); err != nil {
		t.Fatalf("owned Unix socket was rejected: %v", err)
	}
}

func TestWaitForGStreamerBusRejectsSymlinkAndRegularFile(t *testing.T) {
	for _, kind := range []string{"regular", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "raw.sock")
			if kind == "regular" {
				if err := os.WriteFile(path, []byte("not-a-socket"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				target := filepath.Join(directory, "target")
				if err := os.WriteFile(target, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			}
			child := &isolatedGStreamerChild{done: make(chan struct{})}
			if err := waitForGStreamerBus(context.Background(), child, path); err == nil ||
				!strings.Contains(err.Error(), "not an owned Unix socket") {
				t.Fatalf("%s bus was not rejected: %v", kind, err)
			}
		})
	}
}

func TestWaitForGStreamerBusReportsCaptureExit(t *testing.T) {
	child := &isolatedGStreamerChild{done: make(chan struct{}), waitErr: errors.New("capture failed")}
	close(child.done)
	err := waitForGStreamerBus(context.Background(), child, filepath.Join(t.TempDir(), "missing.sock"))
	if err == nil || !strings.Contains(err.Error(), "capture failed") {
		t.Fatalf("capture failure was not preserved: %v", err)
	}
}

func TestBuildH265PipelinePreservesNativeLiveAndArchive(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "h265",
		liveEnabled: true, analysisEnabled: true, archiveEnabled: true,
		encodeProvider: "gstreamer_mpp", decodeProvider: "gstreamer_mpp",
		orientation:  "normal",
		liveCodec:    "h265",
		captureWidth: 1920, captureHeight: 1080, captureFPS: 30,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 15,
		liveBitrateKbps: 6000,
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{liveEncoder: "mpph265enc", decoder: "mppvideodec"})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	for _, expected := range []string{
		"h265parse", "rtph265pay pt=98",
		"shmsrc socket-path=/run/video-broker/test/native.sock",
		"mppvideodec", "rtph265pay pt=96", "fdsink fd=3",
		"video/x-raw,format=NV12,width=640,height=360,framerate=15/1",
	} {
		if !strings.Contains(pipeline, expected) {
			t.Fatalf("pipeline does not contain %q: %s", expected, pipeline)
		}
	}
}

func TestBuildNativeH264PipelineAvoidsReencode(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "h264",
		liveEnabled: true, analysisEnabled: true, archiveEnabled: true,
		liveCodec:      "h264",
		archiveCodec:   "h264",
		decodeProvider: "gstreamer_v4l2", orientation: "normal",
		captureWidth: 1280, captureHeight: 720, captureFPS: 30,
		mainWidth: 1280, mainHeight: 720, mainFPS: 30,
		subWidth: 640, subHeight: 360, subFPS: 15,
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{decoder: "v4l2h264dec"})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	if strings.Contains(pipeline, "h264enc") {
		t.Fatalf("native H264 path unexpectedly reencodes: %s", pipeline)
	}
	if strings.Count(pipeline, "rtph264pay") != 2 {
		t.Fatalf("native H264 should feed live and archive: %s", pipeline)
	}
}

func TestBuildLibcameraPipelineUsesNativeSensorSource(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "libcamera",
		liveEnabled:     true,
		analysisEnabled: true,
		archiveEnabled:  true,
		device:          "/base/soc/i2c0mux/i2c@1/imx708@1a",
		inputFormat:     "nv12",
		liveCodec:       "h265",
		orientation:     "normal",
		aspectPolicy:    "crop",
		captureWidth:    1920, captureHeight: 1080, captureFPS: 30,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 20,
		archiveWidth: 1920, archiveHeight: 1080, archiveFPS: 30,
		archiveCodec: "h265", archiveContent: "adaptive",
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{
		liveEncoder: "mpph265enc", archiveEncoder: "mpph265enc",
	})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	for _, expected := range []string{
		"libcamerasrc camera-name=/base/soc/i2c0mux/i2c@1/imx708@1a",
		"video/x-raw,width=1920,height=1080,framerate=30/1",
		"videoconvert ! video/x-raw,format=NV12 ! tee name=source",
		"mpph265enc",
	} {
		if !strings.Contains(pipeline, expected) {
			t.Fatalf("libcamera pipeline missing %q: %s", expected, pipeline)
		}
	}
	if strings.Contains(pipeline, "v4l2src") {
		t.Fatalf("libcamera pipeline fell back to a V4L2 source: %s", pipeline)
	}
}

func TestBuildH264PipelineReencodesWhenBrowserFPSDiffers(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "h264",
		liveEnabled: true, analysisEnabled: true, archiveEnabled: true,
		liveCodec:      "h264",
		archiveCodec:   "h264",
		encodeProvider: "gstreamer_v4l2", decodeProvider: "gstreamer_v4l2", orientation: "normal",
		captureWidth: 1280, captureHeight: 720, captureFPS: 30,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 15,
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{liveEncoder: "v4l2h264enc", decoder: "v4l2h264dec"})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	if !strings.Contains(pipeline, "v4l2h264enc") {
		t.Fatalf("live FPS conversion must use the selected encoder: %s", pipeline)
	}
	if strings.Count(pipeline, "rtph264pay") != 2 {
		t.Fatalf("converted H264 must keep native archive plus live stream: %s", pipeline)
	}
}

func TestBuildGStreamerPipelineMaterializesOnlyConfiguredRole(t *testing.T) {
	tests := []struct {
		name      string
		cfg       ingestConfig
		elements  gstElements
		wantFD    string
		forbidFDs []string
	}{
		{
			name: "live only",
			cfg: ingestConfig{
				sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12", liveEnabled: true,
				orientation: "normal", captureWidth: 1280, captureHeight: 720, captureFPS: 30,
				mainWidth: 1280, mainHeight: 720, mainFPS: 20,
			},
			elements: gstElements{liveEncoder: "x265enc"}, wantFD: "fdsink fd=3",
			forbidFDs: []string{"fdsink fd=4", "fdsink fd=5"},
		},
		{
			name: "analysis only",
			cfg: ingestConfig{
				sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12", analysisEnabled: true,
				orientation: "normal", captureWidth: 1280, captureHeight: 720, captureFPS: 30,
				subWidth: 640, subHeight: 360, subFPS: 15,
			},
			wantFD: "fdsink fd=3", forbidFDs: []string{"fdsink fd=4", "fdsink fd=5"},
		},
		{
			name: "archive only",
			cfg: ingestConfig{
				sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12", archiveEnabled: true,
				orientation: "normal", captureWidth: 1280, captureHeight: 720, captureFPS: 30,
				archiveWidth: 1280, archiveHeight: 720, archiveFPS: 30,
				archiveCodec: "h265", archiveContent: "adaptive",
			},
			elements: gstElements{archiveEncoder: "x265enc"}, wantFD: "fdsink fd=3",
			forbidFDs: []string{"fdsink fd=4", "fdsink fd=5"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args, err := buildIsolatedGStreamerArgsForTest(test.cfg, test.elements)
			if err != nil {
				t.Fatal(err)
			}
			pipeline := strings.Join(args, " ")
			if !strings.Contains(pipeline, test.wantFD) {
				t.Fatalf("configured role endpoint %q missing: %s", test.wantFD, pipeline)
			}
			for _, forbidden := range test.forbidFDs {
				if strings.Contains(pipeline, forbidden) {
					t.Fatalf("disabled role endpoint %q was materialized: %s", forbidden, pipeline)
				}
			}
		})
	}
}

func TestBuildGStreamerPipelineCoversEveryNonEmptyRoleCombination(t *testing.T) {
	for mask := 1; mask < 8; mask++ {
		live := mask&1 != 0
		analysis := mask&2 != 0
		archive := mask&4 != 0
		t.Run(fmt.Sprintf("mask_%d", mask), func(t *testing.T) {
			cfg := ingestConfig{
				sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12", orientation: "normal",
				captureWidth: 1280, captureHeight: 720, captureFPS: 30,
				liveEnabled: live, analysisEnabled: analysis, archiveEnabled: archive,
				mainWidth: 1280, mainHeight: 720, mainFPS: 15, liveCodec: "h264", liveBitrateKbps: 4096,
				subWidth: 640, subHeight: 360, subFPS: 15,
				archiveWidth: 1280, archiveHeight: 720, archiveFPS: 30,
				archiveCodec: "h265", archiveContent: "adaptive", archiveBitrateKbps: 6144,
			}
			args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{
				liveEncoder: "x264enc", archiveEncoder: "x265enc",
			})
			if err != nil {
				t.Fatal(err)
			}
			pipeline := strings.Join(args, " ")
			wantRoles := 0
			for _, enabled := range []bool{live, analysis, archive} {
				if enabled {
					wantRoles++
				}
			}
			if got := strings.Count(pipeline, "fdsink fd=3"); got != wantRoles {
				t.Fatalf("isolated role endpoints=%d want=%d pipeline=%s", got, wantRoles, pipeline)
			}
			if !live && strings.Contains(pipeline, "x264enc") {
				t.Fatalf("disabled live role initialized its encoder: %s", pipeline)
			}
			if !archive && strings.Contains(pipeline, "x265enc") {
				t.Fatalf("disabled archive role initialized its encoder: %s", pipeline)
			}
		})
	}
}

func TestBuildGStreamerPipelineRejectsImplicitV4L2Source(t *testing.T) {
	base := ingestConfig{
		sourceTransport: "v4l2",
		device:          "/dev/video-test",
		inputFormat:     "nv12",
		analysisEnabled: true,
		orientation:     "normal",
		captureWidth:    1280,
		captureHeight:   720,
		captureFPS:      30,
		subWidth:        640,
		subHeight:       360,
		subFPS:          15,
	}
	for _, test := range []struct {
		name string
		edit func(*ingestConfig)
	}{
		{name: "transport omitted", edit: func(cfg *ingestConfig) { cfg.sourceTransport = "" }},
		{name: "device omitted", edit: func(cfg *ingestConfig) { cfg.device = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := base
			test.edit(&cfg)
			if _, err := buildGStreamerCaptureArgs(cfg, gstElements{}, gstreamerBusPaths{raw: "/run/video-broker/test/raw.sock"}); err == nil {
				t.Fatal("implicit V4L2 source was accepted")
			}
		})
	}
}

func TestResolveElementsFailsClosedWithoutDeclaredHardware(t *testing.T) {
	original := inspectElement
	inspectElement = func(_ context.Context, element string) bool {
		return element == "shmsink" || element == "shmsrc"
	}
	t.Cleanup(func() { inspectElement = original })
	_, err := resolveGStreamerElements(context.Background(), ingestConfig{
		inputFormat: "h265", encodeProvider: "gstreamer_mpp", decodeProvider: "gstreamer_mpp",
		liveEnabled: true, analysisEnabled: true,
	})
	if err == nil {
		t.Fatal("missing hardware codec elements were accepted")
	}
}

func TestResolveElementsFailsClosedWithoutIsolatedBusPlugin(t *testing.T) {
	original := inspectElement
	inspectElement = func(_ context.Context, element string) bool { return element != "shmsrc" }
	t.Cleanup(func() { inspectElement = original })
	_, err := resolveGStreamerElements(context.Background(), ingestConfig{
		inputFormat: "nv12", analysisEnabled: true,
	})
	if err == nil || !strings.Contains(err.Error(), "shmsrc") {
		t.Fatalf("missing isolated bus plugin was accepted: %v", err)
	}
}

func TestResolveElementsSelectsProvidersIndependentlyPerRole(t *testing.T) {
	original := inspectElement
	inspectElement = func(_ context.Context, element string) bool {
		return element == "shmsink" || element == "shmsrc" ||
			element == "x264enc" || element == "mpph265enc"
	}
	t.Cleanup(func() { inspectElement = original })
	elements, err := resolveGStreamerElements(context.Background(), ingestConfig{
		inputFormat:    "nv12",
		liveEnabled:    true,
		liveCodec:      "h264",
		archiveEnabled: true,
		encodeProvider: "software", archiveEncodeProvider: "gstreamer_mpp",
		allowSWFallback: true,
		orientation:     "normal",
		captureWidth:    1920, captureHeight: 1080, captureFPS: 30,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		archiveWidth: 1920, archiveHeight: 1080, archiveFPS: 30,
		archiveCodec: "h265", archiveContent: "adaptive",
	})
	if err != nil {
		t.Fatal(err)
	}
	if elements.liveEncoder != "x264enc" || elements.archiveEncoder != "mpph265enc" {
		t.Fatalf("unexpected role-specific elements: %+v", elements)
	}
}

func TestSoftwareH265DecoderConvertsOutputToNV12(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "h265",
		liveEnabled: true, analysisEnabled: true,
		liveCodec:      "h265",
		encodeProvider: "software", decodeProvider: "software",
		orientation:  "normal",
		captureWidth: 1920, captureHeight: 1080, captureFPS: 30,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 15,
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{liveEncoder: "x264enc", decoder: "avdec_h265"})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	if !strings.Contains(pipeline, "avdec_h265 ! videoconvert ! video/x-raw,format=NV12") {
		t.Fatalf("software decoder output is not normalized to NV12: %s", pipeline)
	}
}

func TestMeasureNV12Luma(t *testing.T) {
	width, height := 8, 8
	frame := make([]byte, width*height*3/2)
	for index := 0; index < width*height; index++ {
		frame[index] = 64
	}
	metrics := measureNV12Luma(frame, width, height, 2)
	if metrics.samples == 0 || metrics.luma != 64 || metrics.lapVar != 0 || metrics.edge != 0 || !metrics.informative {
		t.Fatalf("unexpected flat-frame quality: %+v", metrics)
	}
	var regionSamples uint64
	for _, region := range metrics.regions {
		regionSamples += region.samples
	}
	if regionSamples != metrics.samples {
		t.Fatalf("spatial samples=%d differ from global=%d", regionSamples, metrics.samples)
	}
}

func TestMeasureNV12LumaRejectsBlankSignal(t *testing.T) {
	frame := make([]byte, 8*8*3/2)
	metrics := measureNV12Luma(frame, 8, 8, 1)
	if metrics.samples == 0 || metrics.blackRatio != 1 || metrics.informative {
		t.Fatalf("blank signal was not classified correctly: %+v", metrics)
	}
}

func TestQualityPipelineSeparatesSourceProcessedAndAnalysis(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "h265",
		liveEnabled: true, analysisEnabled: true,
		encodeProvider: "gstreamer_mpp", decodeProvider: "gstreamer_mpp",
		orientation:  "normal",
		captureWidth: 1920, captureHeight: 1080, captureFPS: 30,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 15,
		liveBitrateKbps: 6000, qualityEnabled: true, qualityEvery: 900,
		qualityStride: 8, qualityState: "/run/video-broker/video-quality.json",
		toneMappingMode: "auto",
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{
		liveEncoder: "mpph265enc", decoder: "mppvideodec", toneMapper: "videobrokertone",
	})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	for _, expected := range []string{
		"tee name=source source. ! queue max-size-buffers=2 leaky=downstream ! videobrokertone",
		"video/x-raw,format=NV12,width=1920,height=1080,framerate=1/30",
		"fdsink fd=3",
		"fdsink fd=4",
		"video/x-raw,format=NV12,width=640,height=360,framerate=15/1",
	} {
		if !strings.Contains(pipeline, expected) {
			t.Fatalf("quality pipeline does not contain %q: %s", expected, pipeline)
		}
	}
	if strings.Count(pipeline, "video/x-raw,format=NV12,width=1920,height=1080,framerate=1/30") != 2 {
		t.Fatalf("source and processed quality branches are not independent: %s", pipeline)
	}
}

func TestAspectPolicyNormalizesNonWidescreenCamera(t *testing.T) {
	base := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video3", inputFormat: "h265",
		liveEnabled: true, analysisEnabled: true,
		liveCodec:      "h265",
		encodeProvider: "gstreamer_mpp", decodeProvider: "gstreamer_mpp", orientation: "normal",
		captureWidth: 1280, captureHeight: 1024, captureFPS: 30,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 20,
	}

	crop := base
	crop.aspectPolicy = "crop"
	args, err := buildIsolatedGStreamerArgsForTest(crop, gstElements{liveEncoder: "mpph265enc", decoder: "mppvideodec"})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	if !strings.Contains(pipeline, "videocrop left=0 right=0 top=152 bottom=152 ! videoscale") {
		t.Fatalf("center crop is missing: %s", pipeline)
	}

	letterbox := base
	letterbox.aspectPolicy = "letterbox"
	args, err = buildIsolatedGStreamerArgsForTest(letterbox, gstElements{liveEncoder: "mpph265enc", decoder: "mppvideodec"})
	if err != nil {
		t.Fatal(err)
	}
	if pipeline = strings.Join(args, " "); !strings.Contains(pipeline, "videoscale add-borders=true") {
		t.Fatalf("letterbox normalization is missing: %s", pipeline)
	}

	exact := base
	exact.aspectPolicy = "exact"
	if _, err := buildIsolatedGStreamerArgsForTest(exact, gstElements{liveEncoder: "mpph265enc", decoder: "mppvideodec"}); err == nil {
		t.Fatal("exact aspect policy accepted a 5:4 to 16:9 conversion")
	}
}

func TestRawSourceUsesIndependentH265ArchiveEncoder(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12",
		liveEnabled: true, analysisEnabled: true, archiveEnabled: true,
		liveCodec:   "h264",
		orientation: "normal", aspectPolicy: "crop",
		captureWidth: 1920, captureHeight: 1080, captureFPS: 30,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 20,
		archiveWidth: 1920, archiveHeight: 1080, archiveFPS: 30,
		archiveCodec: "h265", archiveContent: "adaptive",
		liveBitrateKbps: 4096, archiveBitrateKbps: 8192,
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{
		liveEncoder: "mpph264enc", archiveEncoder: "mpph265enc",
	})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	for _, expected := range []string{
		"mpph264enc bps=4096000",
		"video/x-raw,format=NV12,width=1920,height=1080,framerate=30/1 ! mpph265enc bps=8192000",
		"h265parse config-interval=-1",
		"rtph265pay pt=98",
		"fdsink fd=3",
	} {
		if !strings.Contains(pipeline, expected) {
			t.Fatalf("independent H265 archive does not contain %q: %s", expected, pipeline)
		}
	}
}

func TestSoftwareH265ArchiveConvertsNV12ToSupportedI420(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12",
		liveEnabled: true, analysisEnabled: true, archiveEnabled: true,
		liveCodec:   "h264",
		orientation: "normal", aspectPolicy: "crop",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 15,
		archiveWidth: 1280, archiveHeight: 720, archiveFPS: 20,
		archiveCodec: "h265", archiveContent: "adaptive",
		allowSWFallback: true,
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{
		liveEncoder: "x264enc", archiveEncoder: "x265enc",
	})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	want := "video/x-raw,format=NV12,width=1280,height=720,framerate=20/1 ! videoconvert ! video/x-raw,format=I420 ! x265enc"
	if !strings.Contains(pipeline, want) {
		t.Fatalf("software H265 archive lacks its format-boundary conversion %q: %s", want, pipeline)
	}
}

func TestProcessedArchiveCannotShareNativeH264LivePath(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "h264",
		liveEnabled: true, analysisEnabled: true, archiveEnabled: true,
		liveCodec:   "h264",
		orientation: "normal", aspectPolicy: "crop",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 15,
		archiveWidth: 1280, archiveHeight: 720, archiveFPS: 20,
		archiveCodec: "h264", archiveContent: "processed",
		liveBitrateKbps: 4096, archiveBitrateKbps: 4096,
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{
		decoder: "mppvideodec", archiveEncoder: "mpph264enc",
	})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	for _, expected := range []string{
		"shmsrc socket-path=/run/video-broker/test/native.sock",
		"rtph264pay pt=96", "mpph264enc", "rtph264pay pt=98", "fdsink fd=3",
	} {
		if !strings.Contains(pipeline, expected) {
			t.Fatalf("processed archive pipeline does not contain %q: %s", expected, pipeline)
		}
	}
}

func TestMatchingRawH264ProfilesKeepIndependentRoleEncoders(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12",
		liveEnabled: true, analysisEnabled: true, archiveEnabled: true,
		liveCodec:   "h264",
		orientation: "normal", aspectPolicy: "crop",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 15,
		archiveWidth: 1280, archiveHeight: 720, archiveFPS: 20,
		archiveCodec: "h264", archiveContent: "adaptive",
		liveBitrateKbps: 4096, archiveBitrateKbps: 4096,
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{
		liveEncoder: "mpph264enc", archiveEncoder: "mpph264enc",
	})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	if strings.Count(pipeline, "mpph264enc") != 2 {
		t.Fatalf("matching live/archive profiles did not keep independent encoders: %s", pipeline)
	}
	if strings.Count(pipeline, "fdsink fd=3") != 3 {
		t.Fatalf("isolated live, analysis, and archive outputs are incomplete: %s", pipeline)
	}
}

func TestDifferentH264BitratesRequireIndependentArchiveEncoder(t *testing.T) {
	cfg := normalizeIngestConfig(ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12", orientation: "normal",
		liveEnabled: true, archiveEnabled: true,
		liveCodec:    "h264",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		archiveWidth: 1280, archiveHeight: 720, archiveFPS: 20,
		archiveCodec: "h264", archiveContent: "adaptive",
		liveBitrateKbps: 4096, archiveBitrateKbps: 8192,
	})
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{
		liveEncoder: "mpph264enc", archiveEncoder: "mpph264enc",
	})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	for _, bitrate := range []string{"bps=4096000", "bps=8192000"} {
		if !strings.Contains(pipeline, bitrate) {
			t.Fatalf("independent role bitrate %s is missing: %s", bitrate, pipeline)
		}
	}
}

func TestSourceArchiveSkipsOrientationProcessing(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12",
		liveEnabled: true, analysisEnabled: true, archiveEnabled: true,
		liveCodec:   "h264",
		orientation: "rotate_180", aspectPolicy: "crop",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 15,
		archiveWidth: 1280, archiveHeight: 720, archiveFPS: 20,
		archiveCodec: "h264", archiveContent: "source",
		liveBitrateKbps: 4096, archiveBitrateKbps: 4096,
	}
	args, err := buildIsolatedGStreamerArgsForTest(cfg, gstElements{
		liveEncoder: "mpph264enc", archiveEncoder: "mpph264enc",
	})
	if err != nil {
		t.Fatal(err)
	}
	pipeline := strings.Join(args, " ")
	if strings.Count(pipeline, "videoflip method=rotate-180") != 1 {
		t.Fatalf("source archive unexpectedly inherited live orientation processing: %s", pipeline)
	}
}

func TestArchiveContentPolicy(t *testing.T) {
	for _, tc := range []struct {
		name       string
		format     string
		content    string
		tone       string
		wantNative bool
	}{
		{name: "H265 adaptive", format: "h265", content: "adaptive", wantNative: true},
		{name: "H264 source", format: "h264", content: "source", wantNative: true},
		{name: "compressed processed", format: "h265", content: "processed", wantNative: false},
		{name: "raw adaptive", format: "nv12", content: "adaptive", wantNative: false},
		{name: "H265 adaptive with PAL", format: "h265", content: "adaptive", tone: "auto", wantNative: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := ingestConfig{
				inputFormat: tc.format, archiveContent: tc.content,
				toneMappingMode: tc.tone, archiveEnabled: true,
			}
			if tc.format == "h264" || tc.format == "h265" {
				input.archiveCodec = tc.format
			}
			cfg := normalizeIngestConfig(input)
			if got := archiveUsesNativeSource(cfg); got != tc.wantNative {
				t.Fatalf("archiveUsesNativeSource()=%t want %t", got, tc.wantNative)
			}
			if got := archiveRequiresEncode(cfg); got == tc.wantNative {
				t.Fatalf("archiveRequiresEncode()=%t is inconsistent with native=%t", got, tc.wantNative)
			}
		})
	}
}

func TestToneMappingPreventsNativeH264LiveBypass(t *testing.T) {
	cfg := ingestConfig{
		inputFormat: "h264", toneMappingMode: "auto", orientation: "normal",
		liveEnabled:  true,
		liveCodec:    "h264",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20,
	}
	if canUseNativeLive(cfg) {
		t.Fatal("active PAL must be upstream of the live output")
	}
}

func TestGStreamerElementBackendIncludesTransformComponents(t *testing.T) {
	for element, want := range map[string]string{
		"videoscale": "gstreamer_video",
		"rgaconvert": "gstreamer_mpp_resize",
		"mpph265enc": "gstreamer_mpp",
	} {
		if got := gstreamerElementBackend(element); got != want {
			t.Fatalf("backend(%s)=%s want %s", element, got, want)
		}
	}
}

func TestBuildIsolatedGStreamerCaptureAndRoleGraphs(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "h265",
		liveEnabled: true, analysisEnabled: true, archiveEnabled: true,
		encodeProvider: "gstreamer_mpp", archiveEncodeProvider: "gstreamer_mpp",
		decodeProvider: "gstreamer_mpp", orientation: "normal", aspectPolicy: "crop",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		mainWidth: 1280, mainHeight: 720, mainFPS: 15, liveCodec: "h265", liveBitrateKbps: 4096,
		subWidth: 640, subHeight: 360, subFPS: 20,
		archiveWidth: 640, archiveHeight: 360, archiveFPS: 15,
		archiveCodec: "h265", archiveContent: "processed", archiveBitrateKbps: 750,
	}
	buses := gstreamerBusPaths{raw: "/run/video-broker/private/raw.sock", native: "/run/video-broker/private/native.sock"}
	elements := gstElements{decoder: "mppvideodec", liveEncoder: "mpph265enc", archiveEncoder: "mpph265enc"}
	captureArgs, err := buildGStreamerCaptureArgs(cfg, elements, buses)
	if err != nil {
		t.Fatal(err)
	}
	capture := strings.Join(captureArgs, " ")
	for _, expected := range []string{
		"v4l2src device=/dev/video0", "mppvideodec",
		"shmsink socket-path=/run/video-broker/private/raw.sock",
		"shmsink socket-path=/run/video-broker/private/native.sock",
	} {
		if !strings.Contains(capture, expected) {
			t.Fatalf("capture graph lacks %q: %s", expected, capture)
		}
	}
	for _, forbidden := range []string{"rtph265pay", "fdsink fd=3", "fdsink fd=4", "fdsink fd=5"} {
		if strings.Contains(capture, forbidden) {
			t.Fatalf("capture graph owns role output %q: %s", forbidden, capture)
		}
	}
	for _, role := range []string{"live", "analysis", "archive"} {
		args, err := buildGStreamerRoleArgs(cfg, elements, buses, role)
		if err != nil {
			t.Fatalf("build %s role: %v", role, err)
		}
		graph := strings.Join(args, " ")
		if !strings.Contains(graph, "shmsrc socket-path="+buses.raw) ||
			!strings.Contains(graph, "fdsink fd=3") {
			t.Fatalf("isolated %s role is not bound to its private bus/output: %s", role, graph)
		}
	}
}

func TestToneMappedCaptureKeepsSourceArchiveOnPreProcessingBus(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12",
		archiveEnabled: true, orientation: "normal", aspectPolicy: "crop",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		archiveWidth: 640, archiveHeight: 360, archiveFPS: 15,
		archiveCodec: "h265", archiveContent: "source", archiveBitrateKbps: 750,
		toneMappingMode: "auto",
	}
	buses := gstreamerBusPaths{
		raw:       "/run/video-broker/private/raw.sock",
		sourceRaw: "/run/video-broker/private/source-raw.sock",
		native:    "/run/video-broker/private/native.sock",
	}
	elements := gstElements{archiveEncoder: "mpph265enc", toneMapper: "videobrokertone"}
	captureArgs, err := buildGStreamerCaptureArgs(cfg, elements, buses)
	if err != nil {
		t.Fatal(err)
	}
	capture := strings.Join(captureArgs, " ")
	for _, path := range []string{buses.raw, buses.sourceRaw} {
		if !strings.Contains(capture, "shmsink socket-path="+path) {
			t.Fatalf("capture graph does not publish required bus %s: %s", path, capture)
		}
	}
	archiveArgs, err := buildGStreamerRoleArgs(cfg, elements, buses, "archive")
	if err != nil {
		t.Fatal(err)
	}
	archive := strings.Join(archiveArgs, " ")
	if !strings.Contains(archive, "shmsrc socket-path="+buses.sourceRaw) ||
		strings.Contains(archive, "shmsrc socket-path="+buses.raw) {
		t.Fatalf("source archive is not isolated from processed pixels: %s", archive)
	}
}

func TestBuildIsolatedGStreamerNativeBusAvoidsReencode(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "h265",
		liveEnabled: true, archiveEnabled: true,
		orientation: "normal", aspectPolicy: "crop",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		mainWidth: 1280, mainHeight: 720, mainFPS: 20, liveCodec: "h265", liveBitrateKbps: 4096,
		archiveWidth: 1280, archiveHeight: 720, archiveFPS: 20,
		archiveCodec: "h265", archiveContent: "source", archiveBitrateKbps: 4096,
	}
	buses := gstreamerBusPaths{native: "/run/video-broker/private/native.sock"}
	captureArgs, err := buildGStreamerCaptureArgs(cfg, gstElements{}, buses)
	if err != nil {
		t.Fatal(err)
	}
	capture := strings.Join(captureArgs, " ")
	if !strings.Contains(capture, "shmsink socket-path="+buses.native) || strings.Contains(capture, "videodec") {
		t.Fatalf("native capture bus is not direct: %s", capture)
	}
	for _, role := range []string{"live", "archive"} {
		args, err := buildGStreamerRoleArgs(cfg, gstElements{}, buses, role)
		if err != nil {
			t.Fatal(err)
		}
		graph := strings.Join(args, " ")
		if !strings.Contains(graph, "shmsrc socket-path="+buses.native) ||
			strings.Contains(graph, "h265enc") || !strings.Contains(graph, "rtph265pay") {
			t.Fatalf("native %s branch reencodes or uses wrong bus: %s", role, graph)
		}
	}
}

func TestBuildIsolatedGStreamerGraphsRejectUnsafeBusPaths(t *testing.T) {
	cfg := ingestConfig{
		sourceTransport: "v4l2", device: "/dev/video0", inputFormat: "nv12",
		analysisEnabled: true, orientation: "normal", aspectPolicy: "crop",
		captureWidth: 1280, captureHeight: 720, captureFPS: 20,
		subWidth: 640, subHeight: 360, subFPS: 20,
	}
	for _, path := range []string{"", "relative.sock", "/", "/run/../var/raw.sock"} {
		if _, err := buildGStreamerCaptureArgs(
			cfg, gstElements{}, gstreamerBusPaths{raw: path},
		); err == nil {
			t.Fatalf("unsafe capture bus %q was accepted", path)
		}
	}
}
