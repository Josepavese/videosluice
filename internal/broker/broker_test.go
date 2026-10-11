package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/quality"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
)

func TestBrokerAdmittedHBNSurfacesUseCatalogRolesAndVPUAlignment(t *testing.T) {
	inventory := capability.Inventory{Components: []capability.Component{{
		ID: "vpu", Backend: "native_sp", Hardware: true, Available: true,
		Operations: []capability.Operation{
			{Stage: capability.StageEncode, Format: "h264", WidthAlignment: 8, HeightAlignment: 8},
			{Stage: capability.StageEncode, Format: "h265", WidthAlignment: 16, HeightAlignment: 8},
		},
	}}}
	profiles := []productconfig.SourceRoleProfile{
		{Role: "analysis", PixelFormat: "nv12", Width: 640, Height: 360, FPS: 20},
		{Role: "live", Codec: "h264", Width: 1280, Height: 722, FPS: 20},
		{Role: "archive", Codec: "h265", Width: 1280, Height: 722, FPS: 15},
	}
	mux := videoproviders.BrokerMuxPlan{
		Transport:    videoproviders.TransportVendorHBN,
		CaptureWidth: 1920, CaptureHeight: 1080,
	}
	surfaces, err := brokerAdmittedSurfaces(profiles, mux, inventory)
	if err != nil {
		t.Fatal(err)
	}
	liveWidth, liveHeight := videoproviders.FitArchiveGeometryWithAlignment(
		1280, 722, 1920, 1080, 8, 8,
	)
	archiveWidth, archiveHeight := videoproviders.FitArchiveGeometryWithAlignment(
		1280, 722, 1920, 1080, 16, 8,
	)
	want := map[videoproviders.BrokerMuxSurface]bool{
		{Width: 640, Height: 360}:                    true,
		{Width: liveWidth, Height: liveHeight}:       true,
		{Width: archiveWidth, Height: archiveHeight}: true,
	}
	if len(surfaces) != len(want) {
		t.Fatalf("aligned HBN surfaces=%+v want=%+v", surfaces, want)
	}
	for _, surface := range surfaces {
		if !want[surface] {
			t.Fatalf("unexpected aligned HBN surface=%+v all=%+v", surface, surfaces)
		}
	}
	profiles[0].PixelFormat = "rgb24"
	if _, err := brokerAdmittedSurfaces(profiles, mux, inventory); err == nil ||
		!strings.Contains(err.Error(), "requires NV12") {
		t.Fatalf("non-NV12 HBN analysis admitted: %v", err)
	}
}

func TestBrokerChildLifecycleEscalationPrecedesExecFallback(t *testing.T) {
	if brokerChildTerminationGrace >= brokerChildWaitFallback {
		t.Fatalf("broker group escalation must precede leader-only exec fallback: grace=%s fallback=%s", brokerChildTerminationGrace, brokerChildWaitFallback)
	}
}

func startGracefullyCancellableTestChild(t *testing.T) (*exec.Cmd, context.CancelFunc) {
	t.Helper()
	childCtx, childCancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(childCtx, "sleep", "30")
	configureBrokerChildLifecycle(cmd)
	if err := cmd.Start(); err != nil {
		childCancel()
		t.Skipf("sleep unavailable: %v", err)
	}
	return cmd, childCancel
}

func TestBrokerChildLifecycleTerminatesFrozenProcessGroupCooperatively(t *testing.T) {
	tmpDir := t.TempDir()
	parentReceipt := filepath.Join(tmpDir, "parent.closed")
	childReceipt := filepath.Join(tmpDir, "child.closed")
	childReady := filepath.Join(tmpDir, "child.ready")
	ready := filepath.Join(tmpDir, "ready")
	childPIDPath := filepath.Join(tmpDir, "child.pid")
	childCtx, childCancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(childCtx, "/bin/bash", "-c", `
set -eu
child_receipt="$1"
parent_receipt="$2"
ready="$3"
child_pid_path="$4"
child_ready="$5"
/bin/bash -c '
  trap '\''printf child >"$1"; exit 0'\'' TERM
  printf ready >"$2"
  while :; do sleep 1; done
' child "$child_receipt" "$child_ready" &
child_pid=$!
printf '%s' "$child_pid" >"$child_pid_path"
trap 'printf parent >"$parent_receipt"; wait "$child_pid"; exit 0' TERM
while [[ ! -f "$child_ready" ]]; do sleep 0.01; done
printf ready >"$ready"
while :; do sleep 1; done
`, "broker-child", childReceipt, parentReceipt, ready, childPIDPath, childReady)
	configureBrokerChildLifecycle(cmd)
	if err := cmd.Start(); err != nil {
		childCancel()
		t.Skipf("bash unavailable: %v", err)
	}
	cleanupArmed := true
	t.Cleanup(func() {
		if cleanupArmed {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("broker child group did not reach readiness")
		}
		time.Sleep(10 * time.Millisecond)
	}
	childPIDRaw, err := os.ReadFile(childPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(childPIDRaw))
	if err != nil || childPID <= 0 {
		t.Fatalf("invalid descendant pid %q: %v", childPIDRaw, err)
	}
	parentPGID, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil || parentPGID != cmd.Process.Pid {
		t.Fatalf("broker child is not isolated as its process-group leader: pgid=%d err=%v", parentPGID, err)
	}
	childPGID, err := syscall.Getpgid(childPID)
	if err != nil || childPGID != parentPGID {
		t.Fatalf("broker descendant escaped owned process group: child_pgid=%d parent_pgid=%d err=%v", childPGID, parentPGID, err)
	}
	if err := syscall.Kill(-parentPGID, syscall.SIGSTOP); err != nil {
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
				t.Fatalf("broker child process %d did not enter frozen state: %v", pid, readErr)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	exitDone := make(chan struct{})
	enforcerDone := make(chan struct{})
	go func() {
		enforceBrokerChildTermination(childCtx, cmd, exitDone, 500*time.Millisecond)
		close(enforcerDone)
	}()
	childCancel()
	waitErr := cmd.Wait()
	close(exitDone)
	select {
	case <-enforcerDone:
	case <-time.After(time.Second):
		t.Fatal("broker child lifecycle enforcer did not terminate")
	}
	cleanupArmed = false
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		t.Fatalf("cooperative frozen broker child did not exit cleanly: %v", waitErr)
	}
	for _, receipt := range []string{parentReceipt, childReceipt} {
		if _, err := os.Stat(receipt); err != nil {
			t.Fatalf("cooperative receipt missing %s: %v", receipt, err)
		}
	}
}

func TestBrokerChildLifecycleEscalatesWholeUncooperativeGroup(t *testing.T) {
	tmpDir := t.TempDir()
	ready := filepath.Join(tmpDir, "ready")
	childPIDPath := filepath.Join(tmpDir, "child.pid")
	childCtx, childCancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(childCtx, "/bin/bash", "-c", `
trap 'exit 0' TERM
/bin/bash -c 'trap '\'' '\'' TERM; while :; do sleep 1; done' &
printf '%s' "$!" >"$2"
printf ready >"$1"
while :; do sleep 1; done
`, "broker-child", ready, childPIDPath)
	configureBrokerChildLifecycle(cmd)
	if err := cmd.Start(); err != nil {
		childCancel()
		t.Skipf("bash unavailable: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("uncooperative broker child group did not reach readiness")
		}
		time.Sleep(10 * time.Millisecond)
	}
	childPIDRaw, err := os.ReadFile(childPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(childPIDRaw))
	if err != nil || childPID <= 0 {
		t.Fatalf("invalid descendant pid %q: %v", childPIDRaw, err)
	}
	exitDone := make(chan struct{})
	enforcerDone := make(chan struct{})
	go func() {
		enforceBrokerChildTermination(childCtx, cmd, exitDone, 100*time.Millisecond)
		close(enforcerDone)
	}()
	childCancel()
	if err := cmd.Wait(); err == nil {
		t.Fatal("uncooperative broker child unexpectedly exited without escalation")
	}
	close(exitDone)
	select {
	case <-enforcerDone:
	case <-time.After(time.Second):
		t.Fatal("broker child escalation enforcer did not terminate")
	}
	deadline = time.Now().Add(time.Second)
	for {
		err := syscall.Kill(childPID, 0)
		if errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker child escalation left descendant pid=%d alive err=%v", childPID, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBrokerChildLifecycleNeverRetainsReapedPGIDForLaterReuse(t *testing.T) {
	originalAlive := brokerProcessGroupAlive
	originalKill := brokerKillProcessGroup
	t.Cleanup(func() {
		brokerProcessGroupAlive = originalAlive
		brokerKillProcessGroup = originalKill
	})
	checks := 0
	brokerProcessGroupAlive = func(_ int) bool {
		checks++
		// The owned group is gone at the reap boundary. A hypothetical later
		// true value represents numeric PGID reuse and must never be queried.
		return checks > 1
	}
	kills := 0
	brokerKillProcessGroup = func(_ int) error {
		kills++
		return nil
	}
	done := make(chan struct{})
	close(done)
	enforceBrokerChildTermination(
		context.Background(),
		&exec.Cmd{Process: &os.Process{Pid: 424242}},
		done,
		10*time.Millisecond,
	)
	if checks != 1 || kills != 0 {
		t.Fatalf("reaped PGID was retained after ownership ended: checks=%d kills=%d", checks, kills)
	}
}

func TestAbortBrokerChildStartKillsWholeUnattestedGroup(t *testing.T) {
	tmpDir := t.TempDir()
	ready := filepath.Join(tmpDir, "ready")
	childPIDPath := filepath.Join(tmpDir, "child.pid")
	cmd := exec.CommandContext(context.Background(), "/bin/bash", "-c", `
/bin/bash -c 'while :; do sleep 0.05; done' &
printf '%s' "$!" >"$2"
printf ready >"$1"
while :; do sleep 0.05; done
`, "broker-child", ready, childPIDPath)
	configureBrokerChildLifecycle(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("unattested broker group did not start: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("unattested broker group did not reach readiness")
		}
		time.Sleep(10 * time.Millisecond)
	}
	childPIDRaw, err := os.ReadFile(childPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(childPIDRaw))
	if err != nil || childPID <= 0 {
		t.Fatalf("invalid descendant pid %q: %v", childPIDRaw, err)
	}
	abortBrokerChildProcessGroup(cmd)
	deadline = time.Now().Add(time.Second)
	for {
		err := syscall.Kill(childPID, 0)
		if errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("unattested broker cleanup left descendant pid=%d alive err=%v", childPID, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestResolveVideoBrokerPlanRDKX5(t *testing.T) {
	cfg := engineConfig{}
	cfg.Enabled = true
	cfg.LiveEnabled = true
	cfg.AnalysisEnabled = true
	cfg.ArchiveEnabled = true
	cfg.LiveSocketPath = "/run/video-broker/sockets/video-broker.sock"
	cfg.ArchiveSocketPath = "/run/video-broker/sockets/video-broker-archive.sock"
	cfg.AnalysisSocketPath = "/run/video-broker/sockets/video-broker-sub.sock"
	cfg.AnalysisSharedMemoryPath = "/run/video-broker/shm/video-broker-sub.nv12"
	cfg.AnalysisNotifySocketPath = "/run/video-broker/sockets/video-broker-sub.notify.sock"
	cfg.VideoTransport = "vendor_hbn"
	cfg.VideoEncodeProvider = "native_sp"
	cfg.VideoDecodeProvider = "native_sp"
	cfg.LiveFormat = "h264"
	cfg.AnalysisFormat = "nv12"
	cfg.LiveWidth = 1536
	cfg.LiveHeight = 864
	cfg.LiveFPS = 15
	cfg.RawWidth = 1920
	cfg.RawHeight = 1080
	cfg.VideoCaptureFPS = 30
	cfg.CameraHostIndex = 0
	cfg.VideoCaptureWarmupSec = 5
	cfg.LiveBitrateKbps = 6144
	cfg.ArchiveBitrateKbps = 8192
	cfg.AnalysisWidth = 512
	cfg.AnalysisHeight = 512
	cfg.AnalysisFPS = 15

	plan := resolveVideoBrokerPlan(cfg)
	if !plan.Enabled {
		t.Fatalf("expected broker plan enabled: %+v", plan)
	}
	if plan.Transport != "vendor_hbn" || plan.MainFormat != "h264" || plan.SubFormat != "nv12" {
		t.Fatalf("unexpected broker plan: %+v", plan)
	}
	if plan.SubShmPath != "/run/video-broker/shm/video-broker-sub.nv12" {
		t.Fatalf("unexpected sub shared path: %+v", plan)
	}
	if plan.SubNotifySocketPath != "/run/video-broker/sockets/video-broker-sub.notify.sock" {
		t.Fatalf("unexpected sub notify path: %+v", plan)
	}
	if plan.CaptureWidth != 1920 || plan.CaptureHeight != 1080 || plan.CaptureFPS != 30 {
		t.Fatalf("unexpected capture contract: %+v", plan)
	}
	if plan.ArchiveWidth != 1920 || plan.ArchiveHeight != 1080 {
		t.Fatalf("unexpected archive contract: %+v", plan)
	}
	if plan.CameraHostIndex != 0 {
		t.Fatalf("unexpected host index: %+v", plan)
	}
}

func TestResolveVideoBrokerPlanDisabledOutsideVendorTransport(t *testing.T) {
	cfg := engineConfig{}
	cfg.Enabled = true
	cfg.LiveEnabled = true
	cfg.AnalysisEnabled = true
	cfg.ArchiveEnabled = true
	cfg.LiveSocketPath = "/run/video-broker/sockets/video-broker.sock"
	cfg.ArchiveSocketPath = "/run/video-broker/sockets/video-broker-archive.sock"
	cfg.AnalysisSocketPath = "/run/video-broker/sockets/video-broker-sub.sock"
	cfg.VideoTransport = "v4l2"

	plan := resolveVideoBrokerPlan(cfg)
	if plan.Enabled {
		t.Fatalf("expected broker plan disabled for non-vendor transport: %+v", plan)
	}
}

func TestResolveArchiveCodecPrefersHardwareHEVC(t *testing.T) {
	inventory := capability.Inventory{Components: []capability.Component{{
		ID: "vpu", Backend: "native_sp", Hardware: true, Available: true,
		Operations: []capability.Operation{
			{Stage: capability.StageEncode, Format: "h264"},
			{Stage: capability.StageEncode, Format: "h265"},
		},
	}}}
	codec, err := resolveBrokerArchiveCodec(videoproviders.BrokerMuxPlan{
		ArchiveEnabled: true, CaptureFormat: "mjpeg", ArchiveCodec: "auto", ArchiveContent: "adaptive",
		EncodeProvider: "auto", AllowSWFallback: true,
	}, inventory)
	if err != nil || codec != "h265" {
		t.Fatalf("codec=%q err=%v", codec, err)
	}
}

func TestResolveArchiveCodecPrefersHardwareH264OverSoftwareHEVC(t *testing.T) {
	inventory := capability.Inventory{Components: []capability.Component{
		{ID: "v4l2", Backend: "gstreamer_v4l2", Hardware: true, Available: true, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h264"}}},
		{ID: "software", Backend: "software", Hardware: false, Available: true, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h264"}, {Stage: capability.StageEncode, Format: "h265"}}},
	}}
	codec, err := resolveBrokerArchiveCodec(videoproviders.BrokerMuxPlan{
		ArchiveEnabled: true, CaptureFormat: "nv12", ArchiveCodec: "auto", ArchiveContent: "adaptive",
		EncodeProvider: "auto", AllowSWFallback: true,
	}, inventory)
	if err != nil || codec != "h264" {
		t.Fatalf("codec=%q err=%v", codec, err)
	}
}

func TestResolveArchiveCodecKeepsCompressedSourceWithoutProcessing(t *testing.T) {
	codec, err := resolveBrokerArchiveCodec(videoproviders.BrokerMuxPlan{
		ArchiveEnabled: true, CaptureFormat: "h265", CaptureWidth: 1280, CaptureHeight: 720,
		CaptureFPS: 20, ArchiveCodec: "auto", ArchiveContent: "adaptive",
		ArchiveWidth: 1280, ArchiveHeight: 720, ArchiveFPS: 20, ArchiveRateControl: "cbr",
		EncodeProvider: "auto", AllowSWFallback: true, ToneMappingEnabled: false,
		VideoOrientation: videoproviders.VideoOrientationNormal,
	}, capability.Inventory{})
	if err != nil || codec != "h265" {
		t.Fatalf("codec=%q err=%v", codec, err)
	}
}

func TestBrokerArchiveEncodeRequirementMatchesNativePassthroughBoundary(t *testing.T) {
	base := videoproviders.BrokerMuxPlan{
		ArchiveEnabled: true, CaptureFormat: "h265", CaptureWidth: 1280, CaptureHeight: 720,
		CaptureFPS: 20, ArchiveCodec: "h265", ArchiveContent: "adaptive",
		ArchiveWidth: 1280, ArchiveHeight: 720, ArchiveFPS: 20, ArchiveRateControl: "cbr",
		VideoOrientation: videoproviders.VideoOrientationNormal,
	}
	if brokerArchiveEncodeRequired(base) {
		t.Fatal("exact compressed archive was not admitted as native passthrough")
	}
	tests := []struct {
		name   string
		mutate func(*videoproviders.BrokerMuxPlan)
	}{
		{name: "scaled", mutate: func(plan *videoproviders.BrokerMuxPlan) { plan.ArchiveWidth = 640 }},
		{name: "cadence", mutate: func(plan *videoproviders.BrokerMuxPlan) { plan.ArchiveFPS = 15 }},
		{name: "codec", mutate: func(plan *videoproviders.BrokerMuxPlan) { plan.ArchiveCodec = "h264" }},
		{name: "rate_control", mutate: func(plan *videoproviders.BrokerMuxPlan) { plan.ArchiveRateControl = "avbr" }},
		{name: "orientation", mutate: func(plan *videoproviders.BrokerMuxPlan) { plan.VideoOrientation = "rotate_180" }},
		{name: "processed", mutate: func(plan *videoproviders.BrokerMuxPlan) { plan.ArchiveContent = "processed" }},
		{name: "adaptive_tone", mutate: func(plan *videoproviders.BrokerMuxPlan) { plan.ToneMappingEnabled = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := base
			test.mutate(&plan)
			if !brokerArchiveEncodeRequired(plan) {
				t.Fatalf("archive change %s bypassed encoder reservation: %+v", test.name, plan)
			}
		})
	}
	sourceTone := base
	sourceTone.ArchiveContent = "source"
	sourceTone.ToneMappingEnabled = true
	if brokerArchiveEncodeRequired(sourceTone) {
		t.Fatal("source archive incorrectly inherited presentation tone mapping")
	}
}

func TestBrokerPixelTransformRolesAreOutputSpecific(t *testing.T) {
	plan := videoproviders.BrokerMuxPlan{
		CaptureWidth: 1280, CaptureHeight: 720, VideoOrientation: "normal",
		LiveEnabled: true, MainWidth: 1280, MainHeight: 720,
		AnalysisEnabled: true, SubWidth: 640, SubHeight: 360,
		ArchiveEnabled: true, ArchiveWidth: 640, ArchiveHeight: 360,
	}
	assertRoles := func(want string) {
		t.Helper()
		got := strings.Join(brokerPixelTransformRoles(plan), ",")
		if got != want {
			t.Fatalf("transform roles=%q want=%q plan=%+v", got, want, plan)
		}
	}
	assertRoles("analysis,archive")

	plan.MainWidth = 640
	plan.MainHeight = 360
	assertRoles("analysis,archive,live")

	plan.MainWidth, plan.MainHeight = plan.CaptureWidth, plan.CaptureHeight
	plan.SubWidth, plan.SubHeight = plan.CaptureWidth, plan.CaptureHeight
	plan.ArchiveWidth, plan.ArchiveHeight = plan.CaptureWidth, plan.CaptureHeight
	assertRoles("")

	plan.VideoOrientation = "rotate_180"
	assertRoles("analysis,archive,live")
}

func TestResolveVideoBrokerPlanV4L2UVC(t *testing.T) {
	cfg := engineConfig{}
	cfg.Enabled = true
	cfg.LiveEnabled = true
	cfg.AnalysisEnabled = true
	cfg.ArchiveEnabled = true
	cfg.LiveSocketPath = "/run/video-broker/sockets/video-broker.sock"
	cfg.ArchiveSocketPath = "/run/video-broker/sockets/video-broker-archive.sock"
	cfg.AnalysisSharedMemoryPath = "/run/video-broker/shm/video-broker-sub.nv12"
	cfg.VideoTransport = "v4l2_uvc"
	cfg.VideoEncodeProvider = "native_sp"
	cfg.LiveFormat = "h264"
	cfg.AnalysisFormat = "nv12"
	cfg.VideoCaptureDevice = "/dev/video0"
	cfg.VideoCaptureFormat = "mjpeg"
	cfg.VideoCapturePolicyProvider = "analysis_quality"
	cfg.VideoCaptureAllowScaledYUYV = true
	cfg.VideoCapturePreferExactYUYV = false
	cfg.VideoAllowSWFallback = true
	cfg.VideoQualityMonitorEnabled = true
	cfg.VideoQualityMonitorReportEvery = 900
	cfg.VideoQualityMonitorSampleStride = 8
	cfg.CameraControlOptimizerPolicy = "auto"
	cfg.ToneMappingTargetLuma = 96
	cfg.ToneMappingHysteresis = 12
	cfg.VideoRequireHWCodec = true
	cfg.CapabilityInventory = capability.Inventory{Components: []capability.Component{{
		ID: "test-vpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate,
		Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h264"}, {Stage: capability.StageEncode, Format: "h265"}},
	}}}
	cfg.ArchiveCodec = "h265"
	cfg.LiveWidth = 1280
	cfg.LiveHeight = 720
	cfg.LiveFPS = 30
	cfg.RawWidth = 1280
	cfg.RawHeight = 720
	cfg.VideoCaptureFPS = 30
	cfg.AnalysisWidth = 640
	cfg.AnalysisHeight = 360
	cfg.AnalysisFPS = 20

	plan := resolveVideoBrokerPlan(cfg)
	if !plan.Enabled {
		t.Fatalf("expected v4l2_uvc broker plan enabled: %+v", plan)
	}
	if plan.Transport != "v4l2_uvc" || plan.CaptureDevice != "/dev/video0" || plan.CaptureFormat != "mjpeg" || !plan.AllowSWFallback {
		t.Fatalf("unexpected v4l2_uvc broker plan: %+v", plan)
	}
	if plan.CapturePolicy != "analysis_quality" || !plan.AllowScaledYUYV || plan.PreferExactYUYV {
		t.Fatalf("unexpected capture quality policy: %+v", plan)
	}
	if !plan.QualityMonitorEnabled || plan.QualityMonitorReportEvery != 900 || plan.QualityMonitorSampleStride != 8 {
		t.Fatalf("unexpected quality monitor plan: %+v", plan)
	}
	if !plan.ToneMappingEnabled || plan.ToneMappingTarget != 96 || plan.ToneMappingHysteresis != 12 || plan.ArchiveCodec != "h265" {
		t.Fatalf("unexpected frame CameraControl plan: %+v", plan)
	}
	if plan.CaptureSelectTimeoutMs != brokerDefaultCaptureSelectTimeoutMs || plan.CaptureMaxTimeouts != brokerDefaultCaptureMaxTimeouts {
		t.Fatalf("unexpected capture watchdog defaults: %+v", plan)
	}
	if plan.FrameWatchdogSec != int(brokerDefaultFrameWatchdog/time.Second) {
		t.Fatalf("unexpected frame watchdog default: %+v", plan)
	}
	if plan.MainWriteTimeoutMs != int(brokerDefaultMainWriteTimeout/time.Millisecond) {
		t.Fatalf("unexpected main write timeout default: %+v", plan)
	}
}

func TestToneMappingRequestIsProviderAgnosticBeforeCapabilityPlanning(t *testing.T) {
	base := engineConfig{Enabled: true, VideoTransport: "v4l2", ToneMappingMode: "auto"}
	base.VideoEncodeProvider = "gstreamer_v4l2"
	if !brokerToneMappingEnabled(base) {
		t.Fatal("portable provider suppressed the product pixel contract before capability planning")
	}
	base.VideoEncodeProvider = "native_sp"
	if !brokerToneMappingEnabled(base) {
		t.Fatal("native V4L2 backend did not enable zone tone mapping")
	}
	base.VideoTransport = "vendor_hbn"
	base.VideoEncodeProvider = "native_sp"
	if !brokerToneMappingEnabled(base) {
		t.Fatal("vendor HBN backend did not enable zone tone mapping")
	}
	base.ToneMappingMode = "off"
	if brokerToneMappingEnabled(base) {
		t.Fatal("disabled tone mapping was activated")
	}
}

func TestBrokerSubFrameNotifierSendsUnixDatagram(t *testing.T) {
	notifyPath := filepath.Join(t.TempDir(), "video-broker-sub.notify.sock")
	receiver, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: notifyPath, Net: "unixgram"})
	if err != nil {
		t.Fatalf("listen notify socket: %v", err)
	}
	defer func() { _ = receiver.Close() }()

	notifier := newBrokerSubFrameNotifier(notifyPath)
	defer func() { _ = notifier.Close() }()

	result := notifier.Notify(42, 123456789, 7, time.Now())
	if result.Err != nil || !result.Attempted || !result.Sent {
		t.Fatalf("unexpected notify result: %+v", result)
	}

	buf := make([]byte, brokerFrameNotifyPacketSize)
	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	n, _, err := receiver.ReadFromUnix(buf)
	if err != nil {
		t.Fatalf("read notify packet: %v", err)
	}
	notification, err := parseBrokerFrameNotification(buf[:n])
	if err != nil {
		t.Fatalf("parse notify packet: %v", err)
	}
	if notification.StreamID != brokerStreamNV12 || notification.FrameID != 42 || notification.TimestampNS != 123456789 || notification.Flags != 7 {
		t.Fatalf("unexpected notification: %+v", notification)
	}
}

func TestResolveVideoBrokerPlanAllowsSourceWithoutAnalysisRole(t *testing.T) {
	cfg := engineConfig{}
	cfg.Enabled = true
	cfg.LiveEnabled = true
	cfg.ArchiveEnabled = true
	cfg.LiveSocketPath = "/run/video-broker/sockets/video-broker.sock"
	cfg.ArchiveSocketPath = "/run/video-broker/sockets/video-broker-archive.sock"
	cfg.VideoTransport = "vendor_hbn"
	cfg.VideoEncodeProvider = "native_sp"
	cfg.VideoDecodeProvider = "native_sp"
	cfg.LiveFormat = "h264"
	cfg.AnalysisFormat = "nv12"
	cfg.LiveWidth = 1536
	cfg.LiveHeight = 864
	cfg.LiveFPS = 15
	cfg.RawWidth = 1920
	cfg.RawHeight = 1080
	cfg.VideoCaptureFPS = 30

	plan := resolveVideoBrokerPlan(cfg)
	if !plan.Enabled || plan.AnalysisEnabled {
		t.Fatalf("live/archive source graph was not preserved without analysis: %+v", plan)
	}
}

func TestResolveVideoBrokerPlanEnabledWithSubSharedMemoryOnly(t *testing.T) {
	cfg := engineConfig{}
	cfg.Enabled = true
	cfg.AnalysisEnabled = true
	cfg.LiveSocketPath = "/run/video-broker/sockets/video-broker.sock"
	cfg.ArchiveSocketPath = "/run/video-broker/sockets/video-broker-archive.sock"
	cfg.AnalysisSharedMemoryPath = "/run/video-broker/shm/video-broker-sub.nv12"
	cfg.VideoTransport = "vendor_hbn"
	cfg.VideoEncodeProvider = "native_sp"
	cfg.VideoDecodeProvider = "native_sp"
	cfg.LiveFormat = "h264"
	cfg.AnalysisFormat = "nv12"
	cfg.LiveWidth = 1536
	cfg.LiveHeight = 864
	cfg.LiveFPS = 15
	cfg.RawWidth = 1920
	cfg.RawHeight = 1080
	cfg.VideoCaptureFPS = 30
	cfg.AnalysisWidth = 640
	cfg.AnalysisHeight = 640
	cfg.AnalysisFPS = 15

	plan := resolveVideoBrokerPlan(cfg)
	if !plan.Enabled {
		t.Fatalf("expected broker plan enabled with shared memory sub path: %+v", plan)
	}
}

func TestVideoBrokerDispatchFrameFanoutAndBootstrap(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan:        videoBrokerPlan{LiveEnabled: true, MainFormat: "h264", MainWidth: 1280, MainHeight: 720},
		mainClients: make(map[int]*brokerClient),
		subClients:  make(map[int]*brokerClient),
	}
	bootstrapPayload := []byte{0, 0, 0, 1, 0x65, 0x01, 0x02, 0x03}
	livePayload := []byte{0, 0, 0, 1, 0x41, 0x04, 0x05}

	serverA, clientA := net.Pipe()
	defer func() { _ = clientA.Close() }()
	defer func() { _ = serverA.Close() }()
	serverB, clientB := net.Pipe()
	defer func() { _ = clientB.Close() }()
	defer func() { _ = serverB.Close() }()

	rt.mainClients[1] = newBrokerClient(brokerStreamH264, serverA, brokerDefaultMainWriteTimeout, 30)
	rt.mainClients[2] = newBrokerClient(brokerStreamH264, serverB, brokerDefaultMainWriteTimeout, 30)
	go rt.mainClients[1].writeLoop()
	go rt.mainClients[2].writeLoop()

	doneA := make(chan []byte, 1)
	doneB := make(chan []byte, 1)
	go func() {
		_, payload := readBrokerPacket(t, clientA)
		doneA <- payload
	}()
	go func() {
		_, payload := readBrokerPacket(t, clientB)
		doneB <- payload
	}()

	rt.dispatchFrame(brokerStreamH264, bootstrapPayload)

	if got := <-doneA; string(got) != string(bootstrapPayload) {
		t.Fatalf("unexpected fanout payload A: %v", got)
	}
	if got := <-doneB; string(got) != string(bootstrapPayload) {
		t.Fatalf("unexpected fanout payload B: %v", got)
	}
	if len(rt.mainBootstrap) != 1 {
		t.Fatalf("unexpected bootstrap cache: %v", rt.mainBootstrap)
	}
	if hdr, payload := parseBrokerPacket(t, rt.mainBootstrap[0]); string(payload) != string(bootstrapPayload) || hdr.StreamID != brokerStreamH264 {
		t.Fatalf("unexpected bootstrap packet header=%+v payload=%v", hdr, payload)
	}
	rt.mainClients[1].close()
	rt.mainClients[2].close()
	delete(rt.mainClients, 1)
	delete(rt.mainClients, 2)

	serverLate, clientLate := net.Pipe()
	defer func() { _ = clientLate.Close() }()
	defer func() { _ = serverLate.Close() }()
	doneLate := make(chan []byte, 2)
	go func() {
		_, payload := readBrokerPacket(t, clientLate)
		doneLate <- payload
		_, payload = readBrokerPacket(t, clientLate)
		doneLate <- payload
	}()

	rt.mu.Lock()
	if err := rt.replayBootstrapLocked(serverLate); err != nil {
		rt.mu.Unlock()
		t.Fatalf("replayBootstrapLocked failed: %v", err)
	}
	rt.mainClients[3] = newBrokerClient(brokerStreamH264, serverLate, brokerDefaultMainWriteTimeout, 30)
	rt.mu.Unlock()
	go rt.mainClients[3].writeLoop()

	rt.dispatchFrame(brokerStreamH264, livePayload)

	if got := <-doneLate; string(got) != string(bootstrapPayload) {
		t.Fatalf("unexpected bootstrap replay payload: %v", got)
	}
	if got := <-doneLate; string(got) != string(livePayload) {
		t.Fatalf("unexpected live payload for late client: %v", got)
	}
}

func TestBrokerBootstrapDropsWholeOversizedGOPAndRestartsAtKeyframe(t *testing.T) {
	var bootstrap [][]byte
	var codecConfig [][]byte
	var bootstrapBytes int
	keyframe := []byte{1, 2, 3}
	pframe := []byte{4, 5, 6}

	updateBrokerBootstrap(
		&bootstrap,
		&bootstrapBytes,
		&codecConfig,
		keyframe,
		brokerFlagKeyframe,
		len(keyframe)+len(pframe)-1,
	)
	updateBrokerBootstrap(
		&bootstrap,
		&bootstrapBytes,
		&codecConfig,
		pframe,
		0,
		len(keyframe)+len(pframe)-1,
	)
	if len(bootstrap) != 0 || bootstrapBytes != 0 {
		t.Fatalf("oversized GOP was retained bootstrap=%d bytes=%d", len(bootstrap), bootstrapBytes)
	}

	updateBrokerBootstrap(
		&bootstrap,
		&bootstrapBytes,
		&codecConfig,
		keyframe,
		brokerFlagKeyframe,
		len(keyframe)+len(pframe)-1,
	)
	if len(bootstrap) != 1 || bootstrapBytes != len(keyframe) {
		t.Fatalf("next keyframe did not restart bootstrap bootstrap=%d bytes=%d", len(bootstrap), bootstrapBytes)
	}
}

func TestVideoBrokerBootstrapRequiresIDRAndPrependsCodecConfig(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan:        videoBrokerPlan{LiveEnabled: true, MainFormat: "h264", MainWidth: 1280, MainHeight: 720},
		mainClients: map[int]*brokerClient{},
		subClients:  map[int]*brokerClient{},
	}
	sps := []byte{0, 0, 0, 1, 0x67, 1}
	pps := []byte{0, 0, 0, 1, 0x68, 2}
	pframe := []byte{0, 0, 0, 1, 0x41, 3}
	rt.dispatchFrame(brokerStreamH264, sps)
	rt.dispatchFrame(brokerStreamH264, pps)
	rt.dispatchFrame(brokerStreamH264, pframe)
	if len(rt.mainBootstrap) != 0 {
		t.Fatalf("bootstrap became available without IDR: %d", len(rt.mainBootstrap))
	}
	idr := []byte{0, 0, 0, 1, 0x65, 4}
	rt.dispatchFrame(brokerStreamH264, idr)
	if len(rt.mainBootstrap) != 3 {
		t.Fatalf("expected SPS+PPS+IDR bootstrap, got %d", len(rt.mainBootstrap))
	}
	for index, want := range [][]byte{sps, pps, idr} {
		_, got := parseBrokerPacket(t, rt.mainBootstrap[index])
		if !bytes.Equal(got, want) {
			t.Fatalf("bootstrap[%d]=%v want=%v", index, got, want)
		}
	}
}

func TestVideoBrokerEncoderEpochWithholdsFramesUntilCompleteKeyframeBootstrap(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	clientState := newBrokerClient(brokerStreamH264, server, brokerDefaultMainWriteTimeout, 30)
	rt := &videoBrokerRuntime{
		plan:                     videoBrokerPlan{LiveEnabled: true, MainFormat: "h264", MainWidth: 1280, MainHeight: 720},
		mainClients:              map[int]*brokerClient{1: clientState},
		subClients:               map[int]*brokerClient{},
		childRunning:             true,
		childControlAvailable:    true,
		childControlSupported:    true,
		childLiveControlMode:     "persistent_encoder_epoch",
		mainAwaitingEncoderEpoch: true,
	}
	/* Frames from the old generation are not allowed to satisfy the keyframe
	 * gate while the requested encoder generation is still pending. */
	rt.dispatchFrame(brokerStreamH264, []byte{0, 0, 0, 1, 0x65, 0})
	if got := len(clientState.send); got != 0 || len(rt.mainBootstrap) != 0 {
		t.Fatalf("pre-epoch frame escaped gate packets=%d bootstrap=%d", got, len(rt.mainBootstrap))
	}
	epoch := make([]byte, 8)
	binary.BigEndian.PutUint64(epoch, 1)
	if err := rt.applyLiveEncoderEpoch(epoch); err != nil {
		t.Fatal(err)
	}
	if rt.observedLiveEpoch != 1 || rt.mainAwaitingEncoderEpoch || !rt.mainAwaitingKeyframe {
		t.Fatalf("epoch marker not applied epoch=%d awaiting_epoch=%t awaiting_keyframe=%t", rt.observedLiveEpoch, rt.mainAwaitingEncoderEpoch, rt.mainAwaitingKeyframe)
	}

	sps := []byte{0, 0, 0, 1, 0x67, 1}
	pps := []byte{0, 0, 0, 1, 0x68, 2}
	pframe := []byte{0, 0, 0, 1, 0x41, 3}
	idr := []byte{0, 0, 0, 1, 0x65, 4}
	rt.dispatchFrame(brokerStreamH264, sps)
	rt.dispatchFrame(brokerStreamH264, pps)
	rt.dispatchFrame(brokerStreamH264, pframe)
	if got := len(clientState.send); got != 0 {
		t.Fatalf("new epoch leaked %d packets before IDR", got)
	}

	rt.dispatchFrame(brokerStreamH264, idr)
	if rt.mainAwaitingKeyframe {
		t.Fatal("broker did not leave keyframe gate after IDR")
	}
	if got := len(clientState.send); got != 3 {
		t.Fatalf("bootstrap fanout packets=%d want=3", got)
	}
	for index, want := range [][]byte{sps, pps, idr} {
		packet := <-clientState.send
		_, got := parseBrokerPacket(t, packet)
		if !bytes.Equal(got, want) {
			t.Fatalf("bootstrap packet[%d]=%x want=%x", index, got, want)
		}
	}
}

func TestVideoBrokerH265EncoderEpochRetainsParameterSetsForIDROnlyRefresh(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			LiveEnabled: true, MainFormat: "h265", MainWidth: 1280, MainHeight: 720,
		},
		mainClients: map[int]*brokerClient{},
		subClients:  map[int]*brokerClient{},
	}
	combined := []byte{
		0, 0, 0, 1, 32 << 1, 1, 0xaa,
		0, 0, 0, 1, 33 << 1, 1, 0xbb,
		0, 0, 0, 1, 34 << 1, 1, 0xcc,
		0, 0, 0, 1, 19 << 1, 1, 0xdd,
	}
	rt.dispatchFrame(brokerStreamH264, combined)
	if len(rt.mainCodecConfig) != 1 || len(rt.mainBootstrap) != 1 {
		t.Fatalf("combined H265 bootstrap was not split for retention config=%d bootstrap=%d",
			len(rt.mainCodecConfig), len(rt.mainBootstrap))
	}
	configHeader, configPayload := parseBrokerPacket(t, rt.mainCodecConfig[0])
	if configHeader.StreamID != brokerStreamH265 ||
		configHeader.Flags != brokerFlagCodecConfig {
		t.Fatalf("retained H265 config header is invalid: %+v", configHeader)
	}
	if hasIDR, hasConfig := brokerH26xNALPresence(brokerStreamH265, configPayload); hasIDR || !hasConfig {
		t.Fatalf("retained H265 config contains IDR or lacks parameter sets: idr=%t config=%t payload=%x",
			hasIDR, hasConfig, configPayload)
	}

	epoch := make([]byte, 8)
	binary.BigEndian.PutUint64(epoch, 2)
	if err := rt.applyLiveEncoderEpoch(epoch); err != nil {
		t.Fatal(err)
	}
	if len(rt.mainCodecConfig) != 1 || len(rt.mainBootstrap) != 0 || !rt.mainAwaitingKeyframe {
		t.Fatalf("activation epoch discarded H265 parameter sets: config=%d bootstrap=%d awaiting=%t",
			len(rt.mainCodecConfig), len(rt.mainBootstrap), rt.mainAwaitingKeyframe)
	}
	idrOnly := []byte{0, 0, 0, 1, 19 << 1, 1, 0xee}
	rt.dispatchFrame(brokerStreamH264, idrOnly)
	if rt.mainAwaitingKeyframe || len(rt.mainBootstrap) != 2 {
		t.Fatalf("IDR-only refresh did not produce config+IDR bootstrap: awaiting=%t packets=%d",
			rt.mainAwaitingKeyframe, len(rt.mainBootstrap))
	}
	firstHeader, firstPayload := parseBrokerPacket(t, rt.mainBootstrap[0])
	secondHeader, secondPayload := parseBrokerPacket(t, rt.mainBootstrap[1])
	if firstHeader.Flags != brokerFlagCodecConfig || !bytes.Equal(firstPayload, configPayload) {
		t.Fatalf("refreshed H265 bootstrap did not prepend retained config: header=%+v payload=%x",
			firstHeader, firstPayload)
	}
	if secondHeader.Flags != brokerFlagKeyframe || !bytes.Equal(secondPayload, idrOnly) {
		t.Fatalf("refreshed H265 bootstrap has invalid IDR: header=%+v payload=%x",
			secondHeader, secondPayload)
	}
}

func TestVideoBrokerH265FanoutPrefixesRetainedParameterSetsOnEveryIDR(t *testing.T) {
	clientState := newBrokerClient(brokerStreamH264, nil, brokerDefaultMainWriteTimeout, 30)
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			LiveEnabled: true, MainFormat: "h265", MainWidth: 1280, MainHeight: 720,
		},
		mainClients: map[int]*brokerClient{1: clientState},
		subClients:  map[int]*brokerClient{},
	}
	combined := []byte{
		0, 0, 0, 1, 32 << 1, 1, 0xaa,
		0, 0, 0, 1, 33 << 1, 1, 0xbb,
		0, 0, 0, 1, 34 << 1, 1, 0xcc,
		0, 0, 0, 1, 19 << 1, 1, 0xdd,
	}
	rt.dispatchFrame(brokerStreamH264, combined)
	<-clientState.send

	idrOnly := []byte{0, 0, 0, 1, 19 << 1, 1, 0xee}
	rt.dispatchFrame(brokerStreamH264, idrOnly)
	if got := len(clientState.send); got != 2 {
		t.Fatalf("IDR-only fanout packets=%d want codec-config+IDR", got)
	}
	configHeader, configPayload := parseBrokerPacket(t, <-clientState.send)
	idrHeader, idrPayload := parseBrokerPacket(t, <-clientState.send)
	if configHeader.Flags != brokerFlagCodecConfig {
		t.Fatalf("prefixed packet is not codec config: %+v", configHeader)
	}
	if hasIDR, hasConfig := brokerH26xNALPresence(brokerStreamH265, configPayload); hasIDR || !hasConfig {
		t.Fatalf("prefixed config is not parameter-set only: idr=%t config=%t payload=%x", hasIDR, hasConfig, configPayload)
	}
	if idrHeader.Flags != brokerFlagKeyframe || !bytes.Equal(idrPayload, idrOnly) {
		t.Fatalf("IDR packet changed header=%+v payload=%x", idrHeader, idrPayload)
	}
}

func TestVideoBrokerH265BootstrapAcceptsEveryHardwarePacketOrdering(t *testing.T) {
	config := []byte{
		0, 0, 0, 1, 32 << 1, 1, 0xaa,
		0, 0, 0, 1, 33 << 1, 1, 0xbb,
		0, 0, 0, 1, 34 << 1, 1, 0xcc,
	}
	idr := []byte{0, 0, 0, 1, 19 << 1, 1, 0xdd}
	type input struct {
		payload []byte
	}
	for _, test := range []struct {
		name             string
		inputs           []input
		wantPackets      int
		wantRecovery     bool
		wantCombinedFlag bool
	}{
		{
			name: "configuration_before_idr",
			inputs: []input{
				{payload: config},
				{payload: idr},
			},
			wantPackets:  2,
			wantRecovery: true,
		},
		{
			name: "idr_before_configuration",
			inputs: []input{
				{payload: idr},
				{payload: config},
			},
			wantPackets:  2,
			wantRecovery: true,
		},
		{
			name: "configuration_and_idr_together",
			inputs: []input{
				{payload: append(append([]byte{}, config...), idr...)},
			},
			wantPackets:      1,
			wantCombinedFlag: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			clientState := newBrokerClient(brokerStreamH264, nil, brokerDefaultMainWriteTimeout, 30)
			controls := make(chan bridgeRuntimeControl, 1)
			rt := &videoBrokerRuntime{
				plan: videoBrokerPlan{
					LiveEnabled: true, MainFormat: "h265", MainWidth: 1280, MainHeight: 720,
				},
				mainClients:           map[int]*brokerClient{1: clientState},
				subClients:            map[int]*brokerClient{},
				childControl:          controls,
				childControlSupported: true,
				mainAwaitingKeyframe:  true,
			}
			for _, current := range test.inputs {
				rt.dispatchFrame(brokerStreamH264, current.payload)
			}
			if rt.mainAwaitingKeyframe || !brokerMainBootstrapReady("h265", rt.mainBootstrap) {
				t.Fatalf("complete H265 bootstrap did not open gate: awaiting=%t packets=%d", rt.mainAwaitingKeyframe, len(rt.mainBootstrap))
			}
			if rt.observedLiveEpoch != 0 {
				t.Fatalf("packet ordering created an encoder epoch: %d", rt.observedLiveEpoch)
			}
			if got := len(clientState.send); got != test.wantPackets {
				t.Fatalf("fanout packets=%d want=%d", got, test.wantPackets)
			}
			published := make([][]byte, 0, test.wantPackets)
			for len(clientState.send) > 0 {
				published = append(published, <-clientState.send)
			}
			if !brokerMainBootstrapReady("h265", published) {
				t.Fatal("published packets were not a decodable H265 bootstrap")
			}
			if test.wantCombinedFlag {
				header, _ := parseBrokerPacket(t, published[0])
				if header.Flags&(brokerFlagCodecConfig|brokerFlagKeyframe) != brokerFlagCodecConfig|brokerFlagKeyframe {
					t.Fatalf("combined bootstrap flags=%b", header.Flags)
				}
			}
			if test.wantRecovery {
				select {
				case control := <-controls:
					if control.RequestKeyframeToken != 1 || control.Reason != "live_bootstrap_missing_codec_configuration" {
						t.Fatalf("unexpected bootstrap recovery control: %+v", control)
					}
				default:
					t.Fatal("missing bounded hardware headers+IDR recovery request")
				}
			} else if len(controls) != 0 || rt.mainKeyframeRequestToken != 0 {
				t.Fatalf("complete combined bootstrap requested recovery controls=%d token=%d", len(controls), rt.mainKeyframeRequestToken)
			}
		})
	}
}

func TestVideoBrokerH265BootstrapRecoveryIsRateLimitedWithoutEpochChurn(t *testing.T) {
	controls := make(chan bridgeRuntimeControl, 1)
	rt := &videoBrokerRuntime{
		plan:                  videoBrokerPlan{LiveEnabled: true, MainFormat: "h265", MainFPS: 30},
		mainClients:           map[int]*brokerClient{1: newBrokerClient(brokerStreamH264, nil, brokerDefaultMainWriteTimeout, 30)},
		childControl:          controls,
		childControlSupported: true,
	}
	now := time.Unix(1_800_000_000, 0)
	rt.requestMainBootstrapRecoveryLocked(now)
	rt.requestMainBootstrapRecoveryLocked(now.Add(brokerMainBootstrapRecoveryEvery - time.Nanosecond))
	if rt.mainKeyframeRequestToken != 1 || len(controls) != 1 || rt.observedLiveEpoch != 0 {
		t.Fatalf("recovery was not bounded token=%d controls=%d epoch=%d", rt.mainKeyframeRequestToken, len(controls), rt.observedLiveEpoch)
	}
	rt.requestMainBootstrapRecoveryLocked(now.Add(brokerMainBootstrapRecoveryEvery))
	if rt.mainKeyframeRequestToken != 2 || len(controls) != 1 || rt.observedLiveEpoch != 0 {
		t.Fatalf("recovery did not refresh in-place token=%d controls=%d epoch=%d", rt.mainKeyframeRequestToken, len(controls), rt.observedLiveEpoch)
	}
	control := <-controls
	if control.RequestKeyframeToken != 2 || control.Reason != "live_bootstrap_missing_codec_configuration" {
		t.Fatalf("bounded control did not retain newest recovery request: %+v", control)
	}
}

func TestVideoBrokerH265RoutingDemandRecoversHeadersWithoutViewers(t *testing.T) {
	controls := make(chan bridgeRuntimeControl, 1)
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			LiveEnabled: true, MainFormat: "h265", MainWidth: 1280, MainHeight: 720,
			MainFPS: 30, LiveBitrateKbps: 4096, SubFPS: 20,
		},
		mainClients: map[int]*brokerClient{}, childControl: controls,
		childControlSupported: true, routingLiveDemand: true, mainAwaitingKeyframe: true,
		governor: resourceGovernorDecision{
			Mode: "thermal_hard", MainFPSCap: 15, MainBitrateKbpsCap: 1843, SubFPS: 10,
		},
	}
	idr := []byte{0, 0, 0, 1, 19 << 1, 1, 0xdd}
	rt.dispatchFrame(brokerStreamH264, idr)
	if rt.mainKeyframeRequestToken != 1 || len(controls) != 1 || !rt.mainAwaitingKeyframe {
		t.Fatalf("missing viewer-free bootstrap recovery token=%d controls=%d awaiting=%t",
			rt.mainKeyframeRequestToken, len(controls), rt.mainAwaitingKeyframe)
	}
	control := <-controls
	if !control.MainActive || control.MainFPS != 15 || control.MainBitrateKbps != 1843 ||
		control.SubFPS != 10 || control.OptionalComputeAdmitted {
		t.Fatalf("bootstrap recovery bypassed thermal controls: %+v", control)
	}
	rt.dispatchFrame(brokerStreamH264, idr)
	if len(controls) != 0 || rt.mainKeyframeRequestToken != 1 {
		t.Fatal("viewer-free hardware recovery was not rate limited")
	}
	config := []byte{
		0, 0, 0, 1, 32 << 1, 1, 0xaa,
		0, 0, 0, 1, 33 << 1, 1, 0xbb,
		0, 0, 0, 1, 34 << 1, 1, 0xcc,
	}
	rt.dispatchFrame(brokerStreamH264, config)
	if rt.mainAwaitingKeyframe || !brokerMainBootstrapReady("h265", rt.mainBootstrap) || rt.observedLiveEpoch != 0 {
		t.Fatal("hardware headers did not complete the existing encoder bootstrap")
	}
	rt.routingLiveDemand = false
	rt.requestMainBootstrapRecoveryLocked(time.Now().Add(brokerMainBootstrapRecoveryEvery))
	if len(controls) != 0 || rt.mainKeyframeRequestToken != 1 || rt.bridgeControlTargetsLocked().MainActive {
		t.Fatal("released routing demand retained encoder or header recovery work")
	}
}

func TestVideoBrokerEncoderEpochMarkerIsMonotonicAndStrict(t *testing.T) {
	rt := &videoBrokerRuntime{}
	marker := make([]byte, 8)
	binary.BigEndian.PutUint64(marker, 3)
	if err := rt.applyLiveEncoderEpoch(marker); err != nil {
		t.Fatal(err)
	}
	rt.mainAwaitingKeyframe = false
	binary.BigEndian.PutUint64(marker, 2)
	if err := rt.applyLiveEncoderEpoch(marker); err != nil || rt.mainAwaitingKeyframe || rt.observedLiveEpoch != 3 {
		t.Fatalf("stale epoch changed state err=%v epoch=%d awaiting=%t", err, rt.observedLiveEpoch, rt.mainAwaitingKeyframe)
	}
	for _, invalid := range [][]byte{{}, make([]byte, 7), make([]byte, 8)} {
		if err := rt.applyLiveEncoderEpoch(invalid); err == nil {
			t.Fatalf("invalid epoch accepted: %x", invalid)
		}
	}
}

func TestBrokerH264FlagsDistinguishCodecConfigFromIDR(t *testing.T) {
	configFlags := brokerH264Flags([]byte{0, 0, 0, 1, 0x67, 1, 0, 0, 0, 1, 0x68, 2})
	if configFlags&brokerFlagCodecConfig == 0 || configFlags&brokerFlagKeyframe != 0 {
		t.Fatalf("SPS/PPS flags=%b", configFlags)
	}
	idrFlags := brokerH264Flags([]byte{0, 0, 0, 1, 0x65, 3})
	if idrFlags&brokerFlagKeyframe == 0 || idrFlags&brokerFlagCodecConfig != 0 {
		t.Fatalf("IDR flags=%b", idrFlags)
	}
}

func TestBrokerH265FlagsAndArchiveStreamMapping(t *testing.T) {
	payload := []byte{
		0, 0, 0, 1, 32 << 1, 1, 0xaa,
		0, 0, 0, 1, 33 << 1, 1, 0xbb,
		0, 0, 0, 1, 34 << 1, 1, 0xcc,
		0, 0, 0, 1, 19 << 1, 1, 0xdd,
	}
	rt := &videoBrokerRuntime{plan: videoBrokerPlan{
		CaptureWidth: 1920, CaptureHeight: 1080,
		ArchiveWidth: 1920, ArchiveHeight: 1080,
		MainWidth: 1280, MainHeight: 720,
		ToneMappingEnabled: true, ArchiveCodec: "h265",
	}}
	rt.activeCaptureWidth = 2560
	rt.activeCaptureHeight = 1440
	packet := rt.buildBrokerPacketLocked(brokerStreamH265, payload)
	header, got := parseBrokerPacket(t, packet)
	if header.StreamID != brokerStreamH265 ||
		header.Flags&(brokerFlagKeyframe|brokerFlagCodecConfig) != brokerFlagKeyframe|brokerFlagCodecConfig {
		t.Fatalf("unexpected H265 archive header: %+v", header)
	}
	if header.Width != 1920 || header.Height != 1080 {
		t.Fatalf("CameraControl H265 archive leaked supersampled capture dimensions: %+v", header)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("unexpected H265 payload: %x", got)
	}

	packet = rt.buildBrokerPacketLocked(brokerMuxStreamArchiveH264Native, []byte{0, 0, 0, 1, 0x65, 1})
	header, _ = parseBrokerPacket(t, packet)
	if header.StreamID != brokerStreamH264 || header.Width != 2560 || header.Height != 1440 {
		t.Fatalf("internal H264 archive stream was not mapped to wire stream 1: %+v", header)
	}

	packet = rt.buildBrokerPacketLocked(brokerMuxStreamArchiveH264Generated, []byte{0, 0, 0, 1, 0x65, 2})
	header, _ = parseBrokerPacket(t, packet)
	if header.StreamID != brokerStreamH264 || header.Width != 1920 || header.Height != 1080 {
		t.Fatalf("generated H264 archive dimensions do not match the archive contract: %+v", header)
	}
}

func TestBrokerLiveCodecMapsInternalMainStreamToConfiguredWireCodec(t *testing.T) {
	h265Payload := []byte{
		0, 0, 0, 1, 32 << 1, 1,
		0, 0, 0, 1, 33 << 1, 1,
		0, 0, 0, 1, 34 << 1, 1,
		0, 0, 0, 1, 19 << 1, 1,
	}
	rt := &videoBrokerRuntime{plan: videoBrokerPlan{
		MainFormat: "h265", MainWidth: 1280, MainHeight: 720,
	}}
	packet := rt.buildBrokerPacketLocked(brokerStreamH264, h265Payload)
	header, got := parseBrokerPacket(t, packet)
	if header.StreamID != brokerStreamH265 ||
		header.Flags&(brokerFlagKeyframe|brokerFlagCodecConfig) != brokerFlagKeyframe|brokerFlagCodecConfig {
		t.Fatalf("configured H265 live stream was mislabeled: %+v", header)
	}
	if header.Width != 1280 || header.Height != 720 || !bytes.Equal(got, h265Payload) {
		t.Fatalf("configured H265 live packet mismatch header=%+v payload=%x", header, got)
	}

	rt.plan.MainFormat = "h264"
	packet = rt.buildBrokerPacketLocked(brokerStreamH264, []byte{0, 0, 0, 1, 0x65, 1})
	header, _ = parseBrokerPacket(t, packet)
	if header.StreamID != brokerStreamH264 || header.Flags&brokerFlagKeyframe == 0 {
		t.Fatalf("explicit H264 live compatibility stream was mislabeled: %+v", header)
	}
}

func TestGeneratedArchiveGeometryIsCapabilityFitted(t *testing.T) {
	rt := &videoBrokerRuntime{plan: videoBrokerPlan{
		CaptureWidth: 1920, CaptureHeight: 1080,
		ArchiveWidth: 1920, ArchiveHeight: 1080,
	}}
	rt.activeCaptureWidth = 1280
	rt.activeCaptureHeight = 720
	rt.observedArchiveDelivery = "processed_hardware_encode"
	width, height := rt.activeArchiveGeometryLocked()
	if width != 1280 || height != 720 {
		t.Fatalf("generated archive geometry=%dx%d", width, height)
	}

	rt.activeCaptureWidth = 2560
	rt.activeCaptureHeight = 1440
	width, height = rt.activeArchiveGeometryLocked()
	if width != 1920 || height != 1080 {
		t.Fatalf("bounded downscale geometry=%dx%d", width, height)
	}

	rt.observedArchiveDelivery = "native_passthrough"
	width, height = rt.activeArchiveGeometryLocked()
	if width != 2560 || height != 1440 {
		t.Fatalf("native archive geometry=%dx%d", width, height)
	}
}

func TestAttachClientDoesNotHoldBrokerLockDuringBootstrapWrite(t *testing.T) {
	publicationPath := filepath.Join(t.TempDir(), "live.sock")
	publication, err := listenBrokerSocket(publicationPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publication.Cleanup)
	packet := marshalBrokerPacket(brokerPacketHeader{
		StreamID: brokerStreamH264, Flags: brokerFlagKeyframe, FrameID: 1,
		Width: 1280, Height: 720, PayloadSize: 6,
	}, []byte{0, 0, 0, 1, 0x65, 1})
	rt := &videoBrokerRuntime{
		plan:            videoBrokerPlan{MainWriteTimeoutMs: 50, LiveEnabled: true, MainSocketPath: publicationPath},
		mainPublication: publication,
		mainClients:     map[int]*brokerClient{},
		subClients:      map[int]*brokerClient{},
		mainBootstrap:   [][]byte{packet},
		childRunning:    true,
	}
	server, client := net.Pipe()
	defer func() { _ = client.Close() }() // deliberately never read the bootstrap
	if err := rt.attachClient(context.Background(), publication, brokerStreamH264, server); err != nil {
		t.Fatal(err)
	}
	lockAcquired := make(chan struct{})
	go func() {
		rt.mu.Lock()
		_ = len(rt.mainClients)
		rt.mu.Unlock()
		close(lockAcquired)
	}()
	select {
	case <-lockAcquired:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("slow bootstrap client held the global broker lock")
	}
	_ = client.Close()
	done := make(chan struct{})
	go func() { rt.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bootstrap writer/watch goroutines did not terminate")
	}
}

func TestStartVideoBrokerSurfacesBindFailure(t *testing.T) {
	tmp := t.TempDir()
	parent := filepath.Join(tmp, "file")
	if err := os.WriteFile(parent, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime, done := startVideoBroker(context.Background(), videoBrokerPlan{
		Enabled:        true,
		LiveEnabled:    true,
		MainSocketPath: filepath.Join(parent, "broker.sock"),
	})
	if err := runtime.waitStartup(context.Background()); err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("startup boundary did not surface bind failure: %v", err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected bind failure")
		}
	case <-time.After(time.Second):
		t.Fatal("broker bind failure was not surfaced")
	}
}

func TestVideoBrokerHealthRequiresFreshMainAndSubFrames(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			LiveEnabled:      true,
			AnalysisEnabled:  true,
			SubFormat:        "nv12",
			FrameWatchdogSec: 12,
		},
		createdAt:       now.Add(-time.Minute),
		childRunning:    true,
		childStartedAt:  now.Add(-2 * time.Second),
		lastMainFrameAt: now.Add(-time.Second),
		lastSubFrameAt:  now.Add(-time.Second),
		healthState:     "initializing",
		healthChangedAt: now.Add(-2 * time.Second),
	}
	health := rt.healthSnapshot(now)
	if health.State != "running" || health.LastError != "" {
		t.Fatalf("fresh broker frames reported unhealthy: %+v", health)
	}

	rt.childStartedAt = now.Add(-30 * time.Second)
	rt.lastSubFrameAt = now.Add(-20 * time.Second)
	health = rt.healthSnapshot(now.Add(time.Second))
	if health.State != "degraded" || !strings.Contains(health.LastError, "sub") {
		t.Fatalf("stale sub stream reported healthy: %+v", health)
	}
}

func TestVideoBrokerHealthKeepsFreshArchiveDuringLiveClientStartup(t *testing.T) {
	now := time.Date(2026, 8, 26, 20, 0, 0, 0, time.UTC)
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			LiveEnabled:      true,
			AnalysisEnabled:  true,
			ArchiveEnabled:   true,
			SubFormat:        "nv12",
			FrameWatchdogSec: 12,
		},
		createdAt:          now.Add(-time.Hour),
		childRunning:       true,
		childStartedAt:     now.Add(-time.Hour),
		mainClients:        map[int]*brokerClient{1: {}},
		mainDemandedAt:     now.Add(-100 * time.Millisecond),
		lastSubFrameAt:     now.Add(-20 * time.Millisecond),
		lastArchiveFrameAt: now.Add(-20 * time.Millisecond),
		healthState:        "running",
		healthChangedAt:    now.Add(-time.Hour),
	}
	health := rt.healthSnapshot(now)
	if health.State != "initializing" || health.LastError != "" {
		t.Fatalf("live startup degraded a fresh archive/analysis data plane: %+v", health)
	}

	rt.lastMainFrameAt = now.Add(200 * time.Millisecond)
	health = rt.healthSnapshot(now.Add(250 * time.Millisecond))
	if health.State != "running" || health.LastError != "" {
		t.Fatalf("first live frame did not restore running health: %+v", health)
	}
}

func TestVideoBrokerHealthReportsNonInformativeCaptureWithoutBreakingDataPlane(t *testing.T) {
	now := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	qualityPath := filepath.Join(t.TempDir(), "video-quality.json")
	source := quality.Metrics{
		Width: 1920, Height: 1080, Samples: 32400, BlackRatio: 1,
	}
	source = withOptimizerSpatial(source)
	analysis := source
	analysis.Width, analysis.Height, analysis.Samples = 640, 360, 3600
	analysis = withOptimizerSpatial(analysis)
	if err := quality.WriteFile(qualityPath, quality.State{
		Schema: quality.Schema, TimestampUnixMS: now.UnixMilli(), Frame: 42, SampleStride: 8,
		Source: source, Processed: source, Analysis: analysis,
		Capture: quality.Capture{Format: "h265", Orientation: "normal"},
	}); err != nil {
		t.Fatal(err)
	}
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			SubFormat:                 "nv12",
			FrameWatchdogSec:          12,
			CaptureFPS:                30,
			QualityMonitorEnabled:     true,
			QualityMonitorReportEvery: 20,
			AnalysisEnabled:           true,
			QualityMonitorStatePath:   qualityPath,
		},
		createdAt:       now.Add(-time.Minute),
		childRunning:    true,
		childStartedAt:  now.Add(-30 * time.Second),
		lastMainFrameAt: now.Add(-time.Second),
		lastSubFrameAt:  now.Add(-time.Second),
		healthState:     "running",
		healthChangedAt: now.Add(-time.Second),
	}
	health := rt.healthSnapshot(now)
	if health.State != "running" || health.LastError != "" ||
		health.OpticalSignalState != "non_informative" ||
		!strings.Contains(health.OpticalSignalReason, "black_ratio=1.00000") {
		t.Fatalf("empty optical signal changed data-plane health: %+v", health)
	}
}

func TestVideoBrokerStartupGraceIncludesConfiguredWarmup(t *testing.T) {
	if got := brokerStartupFrameGrace(12*time.Second, 5*time.Second); got != 17*time.Second {
		t.Fatalf("unexpected startup grace: %s", got)
	}
	if got := brokerStartupFrameGrace(12*time.Second, 0); got != 12*time.Second {
		t.Fatalf("unexpected zero-warmup grace: %s", got)
	}
}

func TestVideoBrokerCameraAdmissionGraceIncludesSteadyStateProofWindow(t *testing.T) {
	if got := brokerCameraAdmissionGrace(12*time.Second, 8*time.Second); got != 44*time.Second {
		t.Fatalf("unexpected default admission grace: %s", got)
	}
	if got := brokerCameraAdmissionGrace(time.Second, 0); got != 16*time.Second {
		t.Fatalf("short watchdog did not retain bounded proof window: %s", got)
	}
	if got := brokerCameraAdmissionGrace(45*time.Second, 5*time.Second); got != 110*time.Second {
		t.Fatalf("long watchdog proof window was not bounded: %s", got)
	}
}

func TestVideoBrokerQualityEvidenceWaitsThroughStartupGrace(t *testing.T) {
	now := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			SubFormat:               "nv12",
			FrameWatchdogSec:        12,
			WarmupSec:               5,
			QualityMonitorEnabled:   true,
			AnalysisEnabled:         true,
			QualityMonitorStatePath: filepath.Join(t.TempDir(), "not-yet-created.json"),
		},
		createdAt:       now.Add(-time.Minute),
		childRunning:    true,
		childStartedAt:  now.Add(-11 * time.Second),
		lastMainFrameAt: now.Add(-time.Second),
		lastSubFrameAt:  now.Add(-time.Second),
		healthState:     "initializing",
		healthChangedAt: now.Add(-11 * time.Second),
	}
	health := rt.healthSnapshot(now)
	if health.State != "initializing" || health.LastError != "" {
		t.Fatalf("quality evidence startup latency reported degraded: %+v", health)
	}
	health = rt.healthSnapshot(now.Add(7 * time.Second))
	if health.State != "degraded" || !strings.Contains(health.LastError, "quality evidence") {
		t.Fatalf("missing quality evidence survived startup grace: %+v", health)
	}
}

func TestVideoBrokerHealthDegradesWhenCaptureProcessStops(t *testing.T) {
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	rt := &videoBrokerRuntime{
		plan:               videoBrokerPlan{SubFormat: "nv12", FrameWatchdogSec: 12},
		createdAt:          now.Add(-time.Minute),
		everProducedFrames: true,
		healthState:        "running",
		healthChangedAt:    now.Add(-time.Second),
	}
	health := rt.healthSnapshot(now)
	if health.State != "degraded" || !strings.Contains(health.LastError, "not running") {
		t.Fatalf("stopped capture process reported healthy: %+v", health)
	}
}

func TestArchiveCodecSwitchClearsPreviousBootstrap(t *testing.T) {
	rt := &videoBrokerRuntime{
		mainClients:    map[int]*brokerClient{},
		archiveClients: map[int]*brokerClient{},
		subClients:     map[int]*brokerClient{},
		plan: videoBrokerPlan{
			ArchiveEnabled: true,
			CaptureWidth:   1920,
			CaptureHeight:  1080,
		},
	}
	rt.dispatchFrame(
		brokerMuxStreamArchiveH264Native,
		[]byte{0, 0, 0, 1, 0x67, 1, 0, 0, 0, 1, 0x68, 2, 0, 0, 0, 1, 0x65, 3},
	)
	if rt.archiveCodecStreamID != brokerStreamH264 || len(rt.archiveBootstrap) == 0 {
		t.Fatal("H264 archive bootstrap was not established")
	}
	rt.dispatchFrame(
		brokerStreamH265,
		[]byte{
			0, 0, 0, 1, 32 << 1, 1, 0xaa,
			0, 0, 0, 1, 33 << 1, 1, 0xbb,
			0, 0, 0, 1, 34 << 1, 1, 0xcc,
			0, 0, 0, 1, 19 << 1, 1, 0xdd,
		},
	)
	if rt.archiveCodecStreamID != brokerStreamH265 || len(rt.archiveBootstrap) != 1 {
		t.Fatalf("codec switch retained stale bootstrap stream=%d packets=%d", rt.archiveCodecStreamID, len(rt.archiveBootstrap))
	}
	header, _ := parseBrokerPacket(t, rt.archiveBootstrap[0])
	if header.StreamID != brokerStreamH265 {
		t.Fatalf("stale H264 packet survived codec switch: %+v", header)
	}
}

func TestFinishChildClearsArchiveCodecState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rt := &videoBrokerRuntime{
		childRunning:          true,
		mainBootstrap:         [][]byte{{1}},
		mainCodecConfig:       [][]byte{{2}},
		mainBootstrapBytes:    1,
		archiveBootstrap:      [][]byte{{3}},
		archiveCodecConfig:    [][]byte{{4}},
		archiveBootstrapBytes: 1,
		archiveCodecStreamID:  brokerStreamH265,
	}

	rt.finishChild(ctx)

	if rt.childRunning ||
		len(rt.mainBootstrap) != 0 ||
		len(rt.mainCodecConfig) != 0 ||
		rt.mainBootstrapBytes != 0 ||
		len(rt.archiveBootstrap) != 0 ||
		len(rt.archiveCodecConfig) != 0 ||
		rt.archiveBootstrapBytes != 0 ||
		rt.archiveCodecStreamID != 0 {
		t.Fatalf("child finish retained codec state: %+v", rt)
	}
}

func TestControlledStopPreservesTerminalEpochEvidenceUntilFinish(t *testing.T) {
	cmd, childCancel := startGracefullyCancellableTestChild(t)
	var output bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		childCancel()
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})

	ctx, cancel := context.WithCancel(context.Background())
	rt := &videoBrokerRuntime{
		childCmd:                cmd,
		childCancel:             childCancel,
		childRunning:            true,
		childRecovery:           true,
		childStartedAt:          time.Now().Add(-2 * time.Second),
		mediaEpoch:              5,
		cameraAdmitted:          true,
		observedCaptureCodec:    "mjpeg",
		observedCaptureWidth:    1280,
		observedCaptureHeight:   720,
		observedCaptureFPS:      30,
		observedArchiveCodec:    "h265",
		observedArchiveFPS:      30,
		totalArchiveFrames:      75,
		totalSubFrames:          74,
		childStartArchiveFrames: 5,
		childStartSubFrames:     4,
		lastArchiveFrameAt:      time.Now().Add(-80 * time.Millisecond),
		lastSubFrameAt:          time.Now().Add(-70 * time.Millisecond),
	}
	rt.resetChildFailure(5)
	cancel()
	rt.stopChildLocked()
	if rt.childStartedAt.IsZero() || !rt.cameraAdmitted || rt.observedCaptureCodec != "mjpeg" {
		t.Fatalf("controlled stop discarded terminal evidence before wait: %+v", rt)
	}
	rt.finishChild(ctx)

	logged := output.String()
	for _, marker := range []string{
		"event=capture_epoch_ended outcome=shutdown",
		"reason=lifecycle_shutdown",
		"admitted=true",
		"capture_codec=mjpeg",
		"capture=1280x720@30",
		"archive_codec=h265",
		"frames_archive=70",
		"frames_sub=70",
	} {
		if !strings.Contains(logged, marker) {
			t.Fatalf("controlled shutdown summary missing %q: %s", marker, logged)
		}
	}
	if strings.Contains(logged, "duration_ms=0") {
		t.Fatalf("controlled shutdown lost epoch duration: %s", logged)
	}
	if !rt.childStartedAt.IsZero() || rt.cameraAdmitted || rt.observedCaptureCodec != "" {
		t.Fatalf("finish retained terminal evidence after summary: %+v", rt)
	}
}

func TestFinishChildEmitsOneCorrelatedFailureSummary(t *testing.T) {
	cmd, childCancel := startGracefullyCancellableTestChild(t)
	var output bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		childCancel()
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})

	rt := &videoBrokerRuntime{
		childCmd:                cmd,
		childCancel:             childCancel,
		childRunning:            true,
		childRecovery:           true,
		childStartedAt:          time.Now().Add(-2 * time.Second),
		mediaEpoch:              7,
		cameraAdmitted:          true,
		observedCaptureCodec:    "mjpeg",
		observedCaptureWidth:    1280,
		observedCaptureHeight:   720,
		observedCaptureFPS:      30,
		observedArchiveCodec:    "h265",
		observedArchiveFPS:      30,
		totalArchiveFrames:      90,
		totalSubFrames:          60,
		childStartArchiveFrames: 10,
		childStartSubFrames:     5,
		lastFrameAt:             time.Now().Add(-100 * time.Millisecond),
		lastArchiveFrameAt:      time.Now().Add(-120 * time.Millisecond),
		lastSubFrameAt:          time.Now().Add(-80 * time.Millisecond),
	}
	rt.resetChildFailure(7)
	if rt.recordChildFailure(6, cmd.Process.Pid, "late_previous_epoch") {
		t.Fatal("late stderr from a previous epoch contaminated the current child")
	}
	if !rt.recordChildFailure(7, cmd.Process.Pid, "uvc_dqbuf_enodev") {
		t.Fatal("first terminal reason was not recorded")
	}
	if rt.recordChildFailure(7, cmd.Process.Pid, "later_generic_failure") {
		t.Fatal("later terminal reason replaced the first causal failure")
	}
	childCancel()
	rt.finishChild(context.Background())

	logged := output.String()
	if strings.Count(logged, "event=capture_fault") != 1 {
		t.Fatalf("capture fault was not emitted exactly once: %s", logged)
	}
	for _, marker := range []string{
		"event=capture_epoch_ended outcome=failure",
		"media_epoch=7",
		fmt.Sprintf("child_pid=%d", cmd.Process.Pid),
		"reason=uvc_dqbuf_enodev",
		"admitted=true",
		"capture_codec=mjpeg",
		"capture=1280x720@30",
		"archive_codec=h265",
		"frames_archive=80",
		"frames_sub=55",
	} {
		if !strings.Contains(logged, marker) {
			t.Fatalf("correlated child summary missing %q: %s", marker, logged)
		}
	}
}

func TestFinishChildDrainsTerminalStderrBeforeCausalSummary(t *testing.T) {
	cmd := exec.Command("sh", "-c", `printf '%s\n' '[rdkx5_v4l2_mux_bridge] VIDIOC_DQBUF failed err=No such device' >&2; exit 7`)
	stderr, stderrWriter, err := brokerChildStderrPipe(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer stderr.Close()
	defer stderrWriter.Close()
	if err := cmd.Start(); err != nil {
		t.Skipf("shell unavailable: %v", err)
	}
	_ = stderrWriter.Close()

	var output bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})

	stderrDone := make(chan struct{})
	rt := &videoBrokerRuntime{
		childCmd:        cmd,
		childStderr:     stderr,
		childStderrDone: stderrDone,
		childRunning:    true,
		childRecovery:   true,
		childStartedAt:  time.Now(),
		mediaEpoch:      11,
	}
	rt.resetChildFailure(11)
	go func() {
		defer close(stderrDone)
		rt.scanStderr(context.Background(), stderr, 11, cmd.Process.Pid)
	}()
	rt.finishChild(context.Background())

	logged := output.String()
	if !strings.Contains(logged, "event=capture_fault") ||
		!strings.Contains(logged, "reason=uvc_dqbuf_enodev") ||
		!strings.Contains(logged, "event=capture_epoch_ended outcome=failure") ||
		!strings.Contains(logged, "exit_code=7") {
		t.Fatalf("terminal stderr was not correlated before child summary: %s", logged)
	}
	if strings.Contains(logged, "reason=child_exit_without_terminal_reason") {
		t.Fatalf("specific terminal stderr was replaced by a generic exit: %s", logged)
	}
	if _, err := stderr.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("broker-owned stderr descriptor not released: %v", err)
	}
}

func TestBrokerClientEnqueueDropsStaleFrames(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	brokerClient := newBrokerClient(brokerStreamNV12, server, brokerDefaultMainWriteTimeout, 30)
	packet1 := marshalBrokerPacket(brokerPacketHeader{
		StreamID:    brokerStreamNV12,
		FrameID:     1,
		TimestampNS: 1,
		Width:       1,
		Height:      1,
		PayloadSize: 1,
	}, []byte{0x01})
	packet2 := marshalBrokerPacket(brokerPacketHeader{
		StreamID:    brokerStreamNV12,
		FrameID:     2,
		TimestampNS: 2,
		Width:       1,
		Height:      1,
		PayloadSize: 1,
	}, []byte{0x02})
	ok, dropped, _ := brokerClient.enqueue(packet1)
	if !ok || dropped != 0 {
		t.Fatal("expected first enqueue to succeed")
	}
	ok, dropped, _ = brokerClient.enqueue(packet2)
	if !ok || dropped != 1 {
		t.Fatal("expected overwrite enqueue to succeed")
	}
	go brokerClient.writeLoop()

	done := make(chan []byte, 1)
	go func() {
		_, payload := readBrokerPacket(t, client)
		done <- payload
	}()

	select {
	case got := <-done:
		if len(got) != 1 || got[0] != 0x02 {
			t.Fatalf("expected latest payload after overwrite, got %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for broker payload")
	}
}

func TestBrokerClientMarksOnlyFirstBootstrapPacketAsStreamReset(t *testing.T) {
	server, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = server.Close() }()

	client := newBrokerClient(
		brokerEndpointArchive, server, brokerDefaultMainWriteTimeout, 15,
	)
	bootstrap := [][]byte{
		marshalBrokerPacket(brokerPacketHeader{
			StreamID: brokerStreamH265, Flags: brokerFlagCodecConfig,
			FrameID: 15, TimestampNS: 100, Width: 640, Height: 360,
		}, []byte{0, 0, 0, 1, 32 << 1, 1}),
		marshalBrokerPacket(brokerPacketHeader{
			StreamID: brokerStreamH265, Flags: brokerFlagKeyframe,
			FrameID: 16, TimestampNS: 101, Width: 640, Height: 360,
		}, []byte{0, 0, 0, 1, 19 << 1, 1}),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.writeLoopWithBootstrap(bootstrap)
	}()

	first, _ := readBrokerPacket(t, clientConn)
	second, _ := readBrokerPacket(t, clientConn)
	if first.Flags&brokerFlagStreamReset == 0 || first.Flags&brokerFlagCodecConfig == 0 {
		t.Fatalf("first bootstrap packet lacks stream reset/config flags: %+v", first)
	}
	if second.Flags&brokerFlagStreamReset != 0 || second.Flags&brokerFlagKeyframe == 0 {
		t.Fatalf("stream reset leaked beyond first bootstrap packet: %+v", second)
	}
	retained, _ := parseBrokerPacket(t, bootstrap[0])
	if retained.Flags&brokerFlagStreamReset != 0 {
		t.Fatalf("per-client stream reset mutated retained bootstrap: %+v", retained)
	}
	client.close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("bootstrap writer did not stop")
	}
}

func TestBrokerClientMarksFirstQueuedPacketWhenBootstrapIsNotReady(t *testing.T) {
	for _, endpoint := range []byte{brokerStreamH264, brokerEndpointArchive} {
		for _, codec := range []byte{brokerStreamH264, brokerStreamH265} {
			t.Run(fmt.Sprintf("endpoint_%d_codec_%d", endpoint, codec), func(t *testing.T) {
				server, consumer := net.Pipe()
				defer server.Close()
				defer consumer.Close()
				client := newBrokerClient(endpoint, server, brokerDefaultMainWriteTimeout, 25)
				defer client.close()
				// A consumer can attach immediately after the routing publication
				// is replaced, before a newly activated encoder fills its retained
				// bootstrap. Its first queued packet still begins a new publication.
				bootstrapPayload := []byte{
					0, 0, 0, 1, 0x67, 0xaa,
					0, 0, 0, 1, 0x68, 0xbb,
					0, 0, 0, 1, 0x65, 0xcc,
				}
				picturePayload := []byte{0, 0, 0, 1, 0x41, 0xdd}
				if codec == brokerStreamH265 {
					bootstrapPayload = []byte{
						0, 0, 0, 1, 32 << 1, 1, 0xaa,
						0, 0, 0, 1, 33 << 1, 1, 0xbb,
						0, 0, 0, 1, 34 << 1, 1, 0xcc,
						0, 0, 0, 1, 19 << 1, 1, 0xdd,
					}
					picturePayload = []byte{0, 0, 0, 1, 1 << 1, 1, 0xee}
				}
				first := marshalBrokerPacket(brokerPacketHeader{
					StreamID: codec, Flags: brokerFlagCodecConfig | brokerFlagKeyframe,
					FrameID: 169, TimestampNS: 1_000, Width: 640, Height: 360,
				}, bootstrapPayload)
				second := marshalBrokerPacket(brokerPacketHeader{
					StreamID: codec, FrameID: 170, TimestampNS: 2_000, Width: 640, Height: 360,
				}, picturePayload)
				for _, packet := range [][]byte{first, second} {
					if ok, drops, _ := client.enqueue(packet); !ok || drops != 0 {
						t.Fatal("could not queue publication bootstrap")
					}
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					client.writeLoopWithBootstrap(nil)
				}()
				_ = consumer.SetReadDeadline(time.Now().Add(2 * time.Second))
				gotFirst, _ := readBrokerPacket(t, consumer)
				gotSecond, _ := readBrokerPacket(t, consumer)
				client.close()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatal("queued bootstrap writer did not stop")
				}
				if gotFirst.Flags&brokerFlagStreamReset == 0 || gotFirst.FrameID != 169 {
					t.Fatalf("first queued bootstrap lacks publication reset: %+v", gotFirst)
				}
				if gotSecond.Flags&brokerFlagStreamReset != 0 {
					t.Fatalf("stream reset leaked into steady state: %+v", gotSecond)
				}
				original, _ := parseBrokerPacket(t, first)
				if original.Flags&brokerFlagStreamReset != 0 {
					t.Fatal("per-client reset mutated the shared queued packet")
				}
			})
		}
	}
}

func TestArchiveClientDisconnectsInsteadOfCorruptingGOPOnOverflow(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	const archiveFPS = 15
	archiveClient := newBrokerClient(brokerEndpointArchive, server, brokerDefaultMainWriteTimeout, archiveFPS)
	packet := []byte{0x01}
	queueDepth := brokerClientQueueDepth(brokerEndpointArchive, archiveFPS)
	for index := 0; index < queueDepth; index++ {
		ok, dropped, _ := archiveClient.enqueue(packet)
		if !ok || dropped != 0 {
			t.Fatalf("archive enqueue %d failed before queue was full", index)
		}
	}
	ok, dropped, depth := archiveClient.enqueue(packet)
	if ok || dropped != 0 || depth != queueDepth {
		t.Fatalf("archive overflow must disconnect without dropping frames: ok=%t dropped=%d depth=%d", ok, dropped, depth)
	}
}

func TestArchiveQueueCoversTenSecondsAndIsBounded(t *testing.T) {
	if brokerArchiveWriteTimeout <= brokerArchiveQueueSeconds*time.Second {
		t.Fatalf(
			"archive timeout=%s must exceed queue window=%s",
			brokerArchiveWriteTimeout,
			time.Duration(brokerArchiveQueueSeconds)*time.Second,
		)
	}
	if got := brokerClientQueueDepth(brokerEndpointArchive, 15); got != 150 {
		t.Fatalf("15 FPS archive queue=%d want=150", got)
	}
	if got := brokerClientQueueDepth(brokerEndpointArchive, 1); got != brokerArchiveQueueMinDepth {
		t.Fatalf("minimum archive queue=%d want=%d", got, brokerArchiveQueueMinDepth)
	}
	if got := brokerClientQueueDepth(brokerEndpointArchive, 1000); got != brokerArchiveQueueMaxDepth {
		t.Fatalf("maximum archive queue=%d want=%d", got, brokerArchiveQueueMaxDepth)
	}
	if got := brokerClientQueueByteLimit(brokerEndpointArchive); got != brokerArchiveQueueMaxBytes {
		t.Fatalf("archive queue byte limit=%d want=%d", got, brokerArchiveQueueMaxBytes)
	}
	if got := brokerClientQueueByteLimit(brokerStreamH264); got != 0 {
		t.Fatalf("main queue unexpectedly has archive byte limit=%d", got)
	}
	server, clientConn := net.Pipe()
	defer func() { _ = server.Close() }()
	defer func() { _ = clientConn.Close() }()
	client := newBrokerClient(
		brokerEndpointArchive, server, brokerDefaultMainWriteTimeout, 30,
	)
	if client.writeTimeout != brokerArchiveWriteTimeout {
		t.Fatalf(
			"archive write timeout=%s want=%s",
			client.writeTimeout,
			brokerArchiveWriteTimeout,
		)
	}
}

func TestArchiveClientSizingUsesMaximumAdmissibleArchiveRate(t *testing.T) {
	rt := &videoBrokerRuntime{plan: videoBrokerPlan{CaptureFPS: 30, ArchiveFPS: 20}}
	if got := rt.brokerClientFPSLocked(brokerEndpointArchive); got != 20 {
		t.Fatalf("archive plan rate=%d want=20", got)
	}
	rt.observedArchiveFPS = 30
	if got := rt.brokerClientFPSLocked(brokerEndpointArchive); got != 30 {
		t.Fatalf("archive observed rate=%d want=30", got)
	}
	if got := rt.brokerClientFPSLocked(brokerStreamH264); got != 30 {
		t.Fatalf("live rate=%d want capture=30", got)
	}
	rt.plan.ArchiveFPS = 0
	rt.observedArchiveFPS = 0
	if got := rt.brokerClientFPSLocked(brokerEndpointArchive); got != 30 {
		t.Fatalf("archive fallback rate=%d want capture=30", got)
	}
}

func TestActiveArchiveGeometryRestoresConfiguredModeAfterFallback(t *testing.T) {
	rt := &videoBrokerRuntime{plan: videoBrokerPlan{
		CaptureWidth: 1280, CaptureHeight: 720,
		ArchiveWidth: 1280, ArchiveHeight: 720,
		ArchiveWidthAlignment: 8, ArchiveHeightAlignment: 8,
	}}
	rt.activeCaptureWidth, rt.activeCaptureHeight = 960, 540
	rt.activeArchiveWidth, rt.activeArchiveHeight = 952, 536
	if width, height := rt.activeArchiveGeometryLocked(); width != 952 || height != 536 {
		t.Fatalf("active fallback geometry=%dx%d want=952x536", width, height)
	}
	rt.activeCaptureWidth, rt.activeCaptureHeight = 1280, 720
	rt.activeArchiveWidth, rt.activeArchiveHeight = 0, 0
	if width, height := rt.activeArchiveGeometryLocked(); width != 1280 || height != 720 {
		t.Fatalf("restored geometry=%dx%d want=1280x720", width, height)
	}
}

func TestArchiveQueueRejectsByteOverflowBeforeFrameLimit(t *testing.T) {
	server, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = server.Close() }()

	client := newBrokerClient(brokerEndpointArchive, server, brokerDefaultMainWriteTimeout, 30)
	client.queueBytesLimit = 4
	if ok, _, _ := client.enqueue([]byte{1, 2, 3}); !ok {
		t.Fatal("first archive packet was rejected below byte limit")
	}
	if ok, _, depth := client.enqueue([]byte{4, 5}); ok || depth != 1 {
		t.Fatalf("archive byte overflow was accepted ok=%t depth=%d", ok, depth)
	}
	if got := client.failure(); got != "byte_capacity" {
		t.Fatalf("archive byte overflow reason=%q want=byte_capacity", got)
	}
	if got := atomic.LoadInt64(&client.queuedBytes); got != 3 {
		t.Fatalf("archive queued bytes=%d want=3", got)
	}
}

func TestArchiveBackpressureDisconnectIsReported(t *testing.T) {
	server, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = server.Close() }()

	client := newBrokerClient(brokerEndpointArchive, server, brokerDefaultMainWriteTimeout, 15)
	for index := 0; index < cap(client.send); index++ {
		if ok, _, _ := client.enqueue([]byte{1}); !ok {
			t.Fatalf("archive queue filled early at %d", index)
		}
	}
	rt := &videoBrokerRuntime{
		mainClients:    map[int]*brokerClient{},
		archiveClients: map[int]*brokerClient{7: client},
		subClients:     map[int]*brokerClient{},
		plan: videoBrokerPlan{
			ArchiveEnabled: true,
			CaptureWidth:   1280, CaptureHeight: 720,
			ArchiveWidth: 1280, ArchiveHeight: 720,
		},
	}

	rt.dispatchFrame(
		brokerMuxStreamArchiveH264Generated,
		[]byte{0, 0, 0, 1, 0x65, 1},
	)

	if len(rt.archiveClients) != 0 {
		t.Fatal("overflowed archive client was not detached")
	}
	if rt.metrics.archiveClientDisconnects != 1 ||
		rt.metrics.archiveFrames != 1 ||
		rt.metrics.archiveMaxQueue != cap(client.send) {
		t.Fatalf("archive backpressure was not reported: %+v", rt.metrics)
	}
	if got := client.failure(); got != "frame_capacity" {
		t.Fatalf("archive frame overflow reason=%q want=frame_capacity", got)
	}
}

func TestArchiveClientSurvivesBoundedConsumerPause(t *testing.T) {
	server, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = server.Close() }()

	client := newBrokerClient(
		brokerEndpointArchive, server, brokerDefaultMainWriteTimeout, 30,
	)
	client.writeTimeout = 500 * time.Millisecond
	for frameID := uint32(1); frameID <= 3; frameID++ {
		packet := marshalBrokerPacket(brokerPacketHeader{
			StreamID: brokerStreamH265, FrameID: frameID, TimestampNS: uint64(frameID),
			Width: 1, Height: 1, PayloadSize: 1,
		}, []byte{byte(frameID)})
		if ok, _, _ := client.enqueue(packet); !ok {
			t.Fatalf("archive packet %d rejected", frameID)
		}
	}
	go client.writeLoop()
	time.Sleep(100 * time.Millisecond)
	for frameID := uint32(1); frameID <= 3; frameID++ {
		header, payload := readBrokerPacket(t, clientConn)
		if header.FrameID != frameID || !bytes.Equal(payload, []byte{byte(frameID)}) {
			t.Fatalf(
				"archive frame=%d payload=%v want frame=%d payload=[%d]",
				header.FrameID, payload, frameID, frameID,
			)
		}
	}
	if got := client.failure(); got != "" {
		t.Fatalf("bounded consumer pause recorded failure=%q", got)
	}
}

func TestArchiveClientReportsWriteTimeoutOnce(t *testing.T) {
	server, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = server.Close() }()

	client := newBrokerClient(
		brokerEndpointArchive, server, brokerDefaultMainWriteTimeout, 30,
	)
	client.writeTimeout = 50 * time.Millisecond
	packet := marshalBrokerPacket(brokerPacketHeader{
		StreamID: brokerStreamH265, FrameID: 1, TimestampNS: 1,
		Width: 1, Height: 1, PayloadSize: 1,
	}, []byte{1})
	if ok, _, _ := client.enqueue(packet); !ok {
		t.Fatal("initial archive packet rejected")
	}
	go client.writeLoop()
	select {
	case <-client.closed:
	case <-time.After(time.Second):
		t.Fatal("archive client did not close after write deadline")
	}
	if got := client.failure(); got != "write_timeout" {
		t.Fatalf("archive write failure=%q want=write_timeout", got)
	}
	client.fail("frame_capacity")
	if got := client.failure(); got != "write_timeout" {
		t.Fatalf("archive failure reason changed after close: %q", got)
	}
}

func TestBrokerWriteFailureReasonClassifiesNonTimeoutErrors(t *testing.T) {
	if got := brokerWriteFailureReason(io.ErrClosedPipe); got != "write_error" {
		t.Fatalf("closed-pipe failure=%q want=write_error", got)
	}
	if got := brokerWriteFailureReason(nil); got != "" {
		t.Fatalf("nil failure=%q want empty", got)
	}
}

func TestArchiveWriteTimeoutDetachesAndCountsExactlyOnce(t *testing.T) {
	server, clientConn := net.Pipe()
	defer func() { _ = clientConn.Close() }()

	client := newBrokerClient(
		brokerEndpointArchive, server, brokerDefaultMainWriteTimeout, 30,
	)
	client.writeTimeout = 50 * time.Millisecond
	rt := &videoBrokerRuntime{
		mainClients:    map[int]*brokerClient{},
		archiveClients: map[int]*brokerClient{9: client},
		subClients:     map[int]*brokerClient{},
	}
	go rt.watchClient(brokerEndpointArchive, 9, server)
	packet := marshalBrokerPacket(brokerPacketHeader{
		StreamID: brokerStreamH265, FrameID: 1, TimestampNS: 1,
		Width: 1, Height: 1, PayloadSize: 1,
	}, []byte{1})
	if ok, _, _ := client.enqueue(packet); !ok {
		t.Fatal("initial archive packet rejected")
	}
	go client.writeLoop()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rt.mu.Lock()
		remaining := len(rt.archiveClients)
		disconnects := rt.totalArchiveDisconnects
		windowDisconnects := rt.metrics.archiveClientDisconnects
		rt.mu.Unlock()
		if remaining == 0 {
			if disconnects != 1 || windowDisconnects != 1 {
				t.Fatalf(
					"archive disconnect counts total=%d window=%d want=1/1",
					disconnects, windowDisconnects,
				)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed-out archive client was not detached")
}

func TestVideoBrokerDispatchFrameAccountsClientDrops(t *testing.T) {
	server, client := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	rt := &videoBrokerRuntime{
		mainClients: make(map[int]*brokerClient),
		subClients: map[int]*brokerClient{
			1: newBrokerClient(brokerStreamNV12, server, brokerDefaultMainWriteTimeout, 30),
		},
		plan: videoBrokerPlan{
			AnalysisEnabled: true,
			SubWidth:        4,
			SubHeight:       2,
		},
	}

	rt.dispatchFrame(brokerStreamNV12, bytes.Repeat([]byte{0x01}, 12))
	rt.dispatchFrame(brokerStreamNV12, bytes.Repeat([]byte{0x02}, 12))

	if rt.metrics.subFrames != 2 {
		t.Fatalf("expected two sub frames, got metrics=%+v", rt.metrics)
	}
	if rt.metrics.subClientDrops != 1 {
		t.Fatalf("expected one stale queued sub frame drop, got metrics=%+v", rt.metrics)
	}
	if rt.metrics.subMaxQueue != brokerSubClientQueueDepth {
		t.Fatalf("expected max queue depth=%d, got metrics=%+v", brokerSubClientQueueDepth, rt.metrics)
	}
}

func TestVideoBrokerSubDispatchKeepsFreshestFrames(t *testing.T) {
	rt := &videoBrokerRuntime{
		subDispatch: make(chan brokerSubFrame, brokerSubDispatchQueueDepth),
	}
	ctx := context.Background()
	first := []byte{1}
	second := []byte{2}
	third := []byte{3}
	if !rt.enqueueSubFrame(ctx, 1, first) ||
		!rt.enqueueSubFrame(ctx, 1, second) ||
		!rt.enqueueSubFrame(ctx, 1, third) {
		t.Fatal("sub dispatch rejected an active producer")
	}
	if rt.metrics.subIngressDrops != 1 {
		t.Fatalf("expected one stale ingress drop, got metrics=%+v", rt.metrics)
	}
	if got := <-rt.subDispatch; !bytes.Equal(got.payload, second) {
		t.Fatalf("oldest queued frame was not discarded: %v", got)
	}
	if got := <-rt.subDispatch; !bytes.Equal(got.payload, third) {
		t.Fatalf("freshest queued frame was not retained: %v", got)
	}

	if !rt.enqueueSubFrame(ctx, 1, first) {
		t.Fatal("sub dispatch rejected frame before generation reset")
	}
	rt.mu.Lock()
	rt.drainSubFramesLocked()
	rt.mu.Unlock()
	if len(rt.subDispatch) != 0 || rt.metrics.subIngressDrops != 2 {
		t.Fatalf("generation reset did not drain stale analysis frame: %+v", rt.metrics)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if rt.enqueueSubFrame(cancelled, 1, third) {
		t.Fatal("sub dispatch accepted a frame after cancellation")
	}
}

func TestVideoBrokerSubDispatchRejectsRetiredEpoch(t *testing.T) {
	rt := &videoBrokerRuntime{mediaEpoch: 2, plan: videoBrokerPlan{AnalysisEnabled: true}}
	for _, stream := range []byte{brokerStreamH264, brokerStreamH265, brokerStreamNV12} {
		rt.dispatchFrameForEpoch(1, stream, []byte{1})
	}
	if rt.metrics.mainFrames != 0 || rt.metrics.archiveFrames != 0 ||
		rt.metrics.subFrames != 0 || rt.totalSubFrames != 0 ||
		rt.metrics.subIngressDrops != 1 {
		t.Fatalf("retired mux frame entered a new source epoch: %+v", rt.metrics)
	}
}

func TestBrokerMuxPayloadPoolUsesBoundedSizeClasses(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		capacity int
	}{
		{name: "encoded", size: 32 * 1024, capacity: 64 * 1024},
		{name: "analysis-360p", size: 640 * 360 * 3 / 2, capacity: 512 * 1024},
		{name: "analysis-1080p", size: 1920 * 1080 * 3 / 2, capacity: 4 * 1024 * 1024},
		{name: "oversize-not-pooled", size: 5 * 1024 * 1024, capacity: 5 * 1024 * 1024},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := acquireBrokerMuxPayload(test.size)
			if len(payload) != test.size || cap(payload) != test.capacity {
				t.Fatalf("unexpected pooled payload shape len=%d cap=%d", len(payload), cap(payload))
			}
			releaseBrokerMuxPayload(payload)
		})
	}
}

func TestDeriveBrokerVideoIndex(t *testing.T) {
	if got := deriveBrokerVideoIndex(0); got != 0 {
		t.Fatalf("expected explicit host index 0, got %d", got)
	}
	t.Setenv("VIDEO_BROKER_CAMERA_HOST_INDEX", "2")
	if got := deriveBrokerVideoIndex(-1); got != -1 {
		t.Fatalf("process-global host index overrode source config: %d", got)
	}
}

func TestVideoBrokerIgnoresUnmanagedCameraEnvironmentAliases(t *testing.T) {
	t.Setenv("VIDEO_BROKER_CAMERA_SENSOR_ID", "imx708")
	t.Setenv("VIDEO_BROKER_CAMERA_SENSOR_NAME", "imx708_wide")
	if got := deriveBrokerSensorPrefix("imx219"); got != "imx219" {
		t.Fatalf("process-global sensor identity affected source selection: %q", got)
	}

	t.Setenv("VIDEO_BROKER_CAMERA_HOST_INDEX", "2")
	t.Setenv("VIDEO_BROKER_CAMERA_PORT", "cam1")
	if got := deriveBrokerVideoIndex(-1); got != -1 {
		t.Fatalf("process-global camera identity affected source selection: %d", got)
	}
}

func TestVideoBrokerDetectsUVCStreamOnFailure(t *testing.T) {
	if !isBrokerUVCStreamOnFailure("[rdkx5_v4l2_mux_bridge] VIDIOC_STREAMON failed err=Protocol error") {
		t.Fatal("expected VIDIOC_STREAMON failure to require UVC recovery")
	}
	if !isBrokerUVCStreamOnFailure("[rdkx5_v4l2_mux_bridge] capture stalled after 3 consecutive timeouts timeout_ms=2000; exiting for supervisor recovery") {
		t.Fatal("expected capture stall to require UVC recovery")
	}
	if !isBrokerUVCStreamOnFailure("[rdkx5_v4l2_mux_bridge][camera-admission] status=rejected codec=h265 reason=runtime_attestation_timeout frames=130 observed_fps=12.91 expected_fps=30 vps=true sps=true pps=true keyframe=true") {
		t.Fatal("expected native cadence attestation failure to enter mode recovery")
	}
	if isBrokerUVCStreamOnFailure("[rdkx5_v4l2_mux_bridge] first sub frame ready bytes=345600") {
		t.Fatal("expected normal bridge readiness log to be ignored")
	}

	stateRoot := t.TempDir()
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{Transport: "v4l2_uvc", CaptureDevice: "/dev/video0", StateDirectory: stateRoot},
		cameraAdmission: videoproviders.CameraAdmissionDecision{
			DevicePath:            "/dev/video0",
			CapabilityFingerprint: "cap-v1:1234567890abcdef",
			Format:                "mjpeg",
			CaptureWidth:          1280,
			CaptureHeight:         720,
			CaptureFPS:            30,
		},
		captureModeFailures: make(map[string]int),
		everProducedFrames:  true,
	}
	t.Cleanup(func() { joinCameraWrites(t, rt) })
	line := "[rdkx5_v4l2_mux_bridge] VIDIOC_STREAMON failed err=Protocol error"
	rt.mediaEpoch = 2
	rt.handleUVCBridgeFailureForEpoch(line, 1)
	if len(rt.captureModeFailures) != 0 {
		t.Fatalf("late stderr from an old child poisoned current mode health: %+v", rt.captureModeFailures)
	}
	rt.handleUVCBridgeFailure(line)
	modeKey := cameraAdmissionModeKey(rt.cameraAdmission)
	if rt.captureModeFailures[modeKey] != 1 || len(rt.captureModeExclusions) != 0 {
		t.Fatalf("first stream start failure must retain the exact mode on probation: failures=%v exclusions=%v", rt.captureModeFailures, rt.captureModeExclusions)
	}
	rt.handleUVCBridgeFailure(line)
	if len(rt.captureModeExclusions) != 1 ||
		!cameraModeExclusionMatchesDecision(rt.captureModeExclusions[0], rt.cameraAdmission) {
		t.Fatalf("repeated stream start failure did not quarantine exact mode: %+v", rt.captureModeExclusions)
	}

	native := &videoBrokerRuntime{
		plan:                videoBrokerPlan{Transport: "vendor_hbn", CaptureDevice: "/dev/video0"},
		cameraAdmission:     rt.cameraAdmission,
		captureModeFailures: make(map[string]int),
	}
	native.handleUVCBridgeFailure(line)
	if len(native.captureModeFailures) != 0 || len(native.captureModeExclusions) != 0 {
		t.Fatalf("non-UVC transport recorded UVC mode failure: failures=%v exclusions=%v", native.captureModeFailures, native.captureModeExclusions)
	}
}

func TestVideoBrokerTerminalFailureReasonsAreCausalAndBounded(t *testing.T) {
	tests := map[string]string{
		"[rdkx5_v4l2_mux_bridge] VIDIOC_STREAMON failed err=Protocol error":                                                                                     "uvc_streamon_failed",
		"[rdkx5_v4l2_mux_bridge] VIDIOC_DQBUF failed err=No such device":                                                                                        "uvc_dqbuf_enodev",
		"[rdkx5_v4l2_mux_bridge] VIDIOC_DQBUF failed err=Input/output error":                                                                                    "capture_dqbuf_failed",
		"[rdkx5_v4l2_mux_bridge] capture stalled after 3 consecutive timeouts timeout_ms=2000; exiting for supervisor recovery":                                 "uvc_capture_stalled",
		"[rdkx5_v4l2_mux_bridge] corrupt capture limit reached format=mjpeg; exiting for supervisor recovery":                                                   "corrupt_capture_limit",
		"[rdkx5_v4l2_mux_bridge][camera-admission] status=rejected codec=mjpeg reason=decoder_no_output_timeout frames=0":                                       "decoder_no_output_timeout",
		"[rdkx5_v4l2_mux_bridge][camera-admission] status=rejected codec=mjpeg reason=decoder_runtime_failed backend=jpu requested=jpu frames=1":                "decoder_runtime_failed",
		"[rdkx5_v4l2_mux_bridge][camera-admission] status=rejected codec=h265 reason=runtime_attestation_timeout frames=130":                                    "runtime_attestation_timeout_after_bridge_window",
		"[video-broker][frame-source] event=frame_source_failure stage=source_surface_send code=-5 peer_stage=io_deadline source=overview action=retire_source": "frame_source_failure:source_surface_send:io_deadline",
		"[video-broker][frame-source] event=frame_source_failure stage=output_send code=-6 peer_stage=peer_closed source=front action=retire_source":            "frame_source_failure:output_send:peer_closed",
		"[video-broker][frame-source] event=frame_source_failure stage=source_authentication code=-3 peer_stage=controller_pid action=retire_source":            "frame_source_failure:source_authentication:controller_pid",
	}
	for line, expected := range tests {
		if got := brokerChildTerminalFailureReason(line); got != expected {
			t.Fatalf("unexpected terminal reason for %q: got=%q expected=%q", line, got, expected)
		}
	}
	if got := brokerChildTerminalFailureReason("[rdkx5_v4l2_mux_bridge] capture select timeout"); got != "" {
		t.Fatalf("single non-terminal timeout became a child failure: %q", got)
	}
}

func TestVideoBrokerTelemetryIntervalsRetainEventsWithoutFloodingJournal(t *testing.T) {
	if brokerMetricsLogEvery < 30*time.Second {
		t.Fatalf("broker telemetry interval is too noisy: %s", brokerMetricsLogEvery)
	}
	if brokerMetricsLogEvery > time.Minute {
		t.Fatalf("broker telemetry interval loses forensic resolution: %s", brokerMetricsLogEvery)
	}
	if brokerLiveMetricsLogEvery != 10*time.Second {
		t.Fatalf("live broker telemetry must retain ten-second forensic resolution: %s", brokerLiveMetricsLogEvery)
	}
}

func TestVideoBrokerNativeCadenceFailureQuarantinesExactMode(t *testing.T) {
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "h265",
		CaptureWidth:          1920,
		CaptureHeight:         1080,
		CaptureFPS:            30,
	}
	rt := &videoBrokerRuntime{
		plan:                videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC},
		cameraAdmission:     decision,
		captureModeFailures: make(map[string]int),
	}
	line := "[rdkx5_v4l2_mux_bridge][camera-admission] status=rejected codec=h265 reason=runtime_attestation_timeout frames=130 observed_fps=12.91 expected_fps=30 vps=true sps=true pps=true keyframe=true"
	rt.handleUVCBridgeFailure(line)
	if len(rt.captureModeExclusions) != 0 || rt.captureModeFailures[cameraAdmissionModeKey(decision)] != 1 {
		t.Fatalf("first cadence failure must retain exact mode on probation: failures=%+v exclusions=%+v", rt.captureModeFailures, rt.captureModeExclusions)
	}
	rt.handleUVCBridgeFailure(line)
	if len(rt.captureModeExclusions) != 1 || !cameraModeExclusionMatchesDecision(rt.captureModeExclusions[0], decision) {
		t.Fatalf("repeated cadence failure did not quarantine exact mode: %+v", rt.captureModeExclusions)
	}
}

func TestComponentRecoveryRequiresRepeatedFailuresAndKeepsIndependentHardwareStages(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	const jpuComponent = "native.jpu"
	inventory := capability.Inventory{
		Schema: capability.Schema,
		Components: []capability.Component{
			{ID: "native.v4l2-worker", Backend: "native_sp", Hardware: false, Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageCapture, Format: "v4l2_uvc"}}},
			{ID: jpuComponent, Backend: "native_sp", Hardware: true, Available: true, State: capability.StateAttested, Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}}},
			{ID: "native.mjpeg-cpu", Backend: "native_sp", Hardware: false, Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}}},
			{ID: "native.sp-vpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateAttested, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h264"}, {Stage: capability.StageEncode, Format: "h265"}}},
		},
	}
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{CapabilityInventory: inventory},
		capabilityPlan: capability.Plan{Stages: []capability.StageSelection{{
			Stage: capability.StageDecode, Role: "capture", Format: "mjpeg",
			Component: "native.mjpeg-cpu", Backend: "native_sp", Hardware: false,
		}}},
	}
	first, changed := rt.noteHardwareComponentFailureLocked(jpuComponent, capability.StageDecode, "mjpeg", 1, 0, now)
	if !changed || first.ConsecutiveFailures != 1 || !first.RetryAfter.Equal(now.Add(30*time.Second)) {
		t.Fatalf("unexpected first recovery state: changed=%t state=%+v", changed, first)
	}
	effective := rt.effectiveCapabilityInventoryLocked(now)
	if effective.Components[1].Available || effective.Components[1].State != capability.StateCandidate || !strings.Contains(effective.Components[1].Reason, "failures=1 threshold=3") {
		t.Fatalf("one failure incorrectly rejected or retained the decoder: %+v", effective.Components[1])
	}
	if !effective.Components[0].Available || !effective.Components[2].Available || !effective.Components[3].Available {
		t.Fatalf("unrelated components were rejected: %+v", effective.Components)
	}
	if effective.HardwareAvailable(capability.StageDecode, "mjpeg") {
		t.Fatal("cooldown inventory still advertises hardware MJPEG decode")
	}
	duplicate, changed := rt.noteHardwareComponentFailureLocked(jpuComponent, capability.StageDecode, "mjpeg", 1, 0, now.Add(time.Second))
	if changed || duplicate.ConsecutiveFailures != 1 || !duplicate.RetryAfter.Equal(first.RetryAfter) {
		t.Fatalf("duplicate epoch changed recovery history: changed=%t state=%+v", changed, duplicate)
	}
	restored := rt.effectiveCapabilityInventoryLocked(first.RetryAfter.Add(time.Second))
	if !restored.Components[1].Available || restored.Components[1].State != capability.StateAttested {
		t.Fatalf("decoder did not return for half-open retry: %+v", restored.Components[1])
	}

	secondAt := first.RetryAfter.Add(time.Second)
	second, changed := rt.noteHardwareComponentFailureLocked(jpuComponent, capability.StageDecode, "mjpeg", 2, 0, secondAt)
	if !changed || second.ConsecutiveFailures != 2 || !second.RetryAfter.Equal(secondAt.Add(time.Minute)) {
		t.Fatalf("second failure did not increase bounded backoff: changed=%t state=%+v", changed, second)
	}
	thirdAt := second.RetryAfter.Add(time.Second)
	third, changed := rt.noteHardwareComponentFailureLocked(jpuComponent, capability.StageDecode, "mjpeg", 3, 0, thirdAt)
	if !changed || third.ConsecutiveFailures != 3 || !third.RetryAfter.Equal(thirdAt.Add(2*time.Minute)) {
		t.Fatalf("third failure did not reach threshold: changed=%t state=%+v", changed, third)
	}
	effective = rt.effectiveCapabilityInventoryLocked(thirdAt)
	if effective.Components[1].State != capability.StateRejected || effective.Components[1].Available {
		t.Fatalf("three consecutive failures did not reject decoder during cooldown: %+v", effective.Components[1])
	}
	statuses := rt.hardwareRecoveryStatusesLocked(thirdAt)
	if len(statuses) != 1 || statuses[0].State != "cooldown" || !statuses[0].Rejected {
		t.Fatalf("threshold rejection was not exposed during cooldown: %+v", statuses)
	}
	if !effective.Components[0].Available || !effective.Components[2].Available || !effective.Components[3].Available {
		t.Fatalf("component failure affected independent stages: %+v", effective.Components)
	}

	retryNow := third.RetryAfter.Add(time.Second)
	effective = rt.effectiveCapabilityInventoryLocked(retryNow)
	statuses = rt.hardwareRecoveryStatusesLocked(retryNow)
	if len(statuses) != 1 || statuses[0].State != "retry_due" || statuses[0].Rejected {
		t.Fatalf("retry boundary remained rejected instead of half-open eligible: %+v", statuses)
	}
	plan, err := capability.Resolve(
		effective,
		capability.ProviderPolicy{EncodeProvider: "native_sp", DecodeProvider: "native_sp"},
		capability.Requirements{
			Transport: "v4l2_uvc", CaptureFormat: "mjpeg", LiveCodec: "h264", LiveEncode: true,
			ArchiveCodec: "h265", ArchiveEncode: true,
		},
	)
	if err != nil {
		t.Fatalf("resolve graph for JPU half-open retry: %v", err)
	}
	for _, stage := range plan.Stages {
		switch {
		case stage.Stage == capability.StageDecode:
			if stage.Component != jpuComponent || !stage.Hardware {
				t.Fatalf("decode did not prefer hardware at retry boundary: %+v", stage)
			}
		case stage.Stage == capability.StageEncode:
			if stage.Component != "native.sp-vpu" || !stage.Hardware {
				t.Fatalf("unrelated hardware encoder was lost: %+v", stage)
			}
		}
	}
}

func TestInactiveHardwareRecoveryDoesNotGateUnrelatedCaptureGraphAndReturnsFresh(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	const (
		jpuComponent = "native.jpu"
		vpuComponent = "native.sp-vpu"
	)
	inventory := capability.Inventory{
		Schema: capability.Schema,
		Components: []capability.Component{
			{
				ID: jpuComponent, Backend: "native_sp", Hardware: true, Available: true, State: capability.StateAttested,
				Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}},
			},
			{
				ID: vpuComponent, Backend: "native_sp", Hardware: true, Available: true, State: capability.StateAttested,
				Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "h265"}},
			},
		},
	}
	for _, captureFormat := range []string{"h265", "yuyv"} {
		t.Run(captureFormat, func(t *testing.T) {
			retryAt := now.Add(time.Minute)
			rt := &videoBrokerRuntime{
				plan: videoBrokerPlan{
					CaptureFormat:       captureFormat,
					FrameWatchdogSec:    12,
					CapabilityInventory: inventory,
				},
				componentRecovery: map[string]hardwareComponentRecovery{
					jpuComponent: {
						ComponentID: jpuComponent, Stage: capability.StageDecode, Format: "mjpeg",
						ConsecutiveFailures: 2, LastFailureEpoch: 7, LastFailureToken: 40,
						RetryAfter: retryAt, RetryScope: videoproviders.ComponentRetryInPlace,
						TrialToken: 41, TrialMediaEpoch: 7, TrialChildPID: 4100,
						TrialStartedAt: now.Add(-time.Second), TrialCandidateReady: now,
						TrialCommitRequested: true, TrialDeadline: now.Add(5 * time.Second),
						CancelToken: 41, CancelMediaEpoch: 7, CancelChildPID: 4100,
						StableAfter: now.Add(5 * time.Minute), HalfOpenEpoch: 7,
						HalfOpenChildPID: 4100, HalfOpenFrameBase: 90,
					},
				},
				childComponentRetryScopes: map[string]string{jpuComponent: videoproviders.ComponentRetryInPlace},
				createdAt:                 now.Add(-time.Minute),
				childRunning:              true,
				childStartedAt:            now.Add(-30 * time.Second),
				healthState:               "running",
				healthChangedAt:           now.Add(-time.Second),
			}
			if captureFormat == "h265" {
				rt.capabilityPlan = capability.Plan{Stages: []capability.StageSelection{{
					Stage: capability.StageDecode, Role: "capture", Format: "h265",
					Component: vpuComponent, Backend: "native_sp", Hardware: true,
				}}}
			}

			rt.reconcileHardwareComponentRetryScopesLocked()
			if statuses := rt.hardwareRecoveryStatusesLocked(now); len(statuses) != 0 {
				t.Fatalf("inactive MJPEG recovery gated %s graph: %+v", captureFormat, statuses)
			}
			status := serviceWithGraphs(map[string]*graphService{"primary": {sourceID: "primary", runtime: rt}}).Snapshot(now)
			sourceStatus := status.Sources["primary"]
			if status.State != "running" || len(sourceStatus.HardwareRecovery) != 0 {
				t.Fatalf("inactive recovery prevented stable %s status: %+v", captureFormat, status)
			}
			jpu := sourceStatus.CapabilityInventory.Components[0]
			if !jpu.Available || len(jpu.Operations) != 1 || capability.NormalizeFormat(jpu.Operations[0].Format) != "mjpeg" {
				t.Fatalf("inactive recovery mutated unrelated %s inventory: %+v", captureFormat, jpu)
			}
			if _, exists := rt.componentRecovery[jpuComponent]; exists {
				t.Fatalf("inactive graph retained stale recovery identity: %+v", rt.componentRecovery[jpuComponent])
			}

			rt.plan.CaptureFormat = "mjpeg"
			rt.capabilityPlan = capability.Plan{Stages: []capability.StageSelection{{
				Stage: capability.StageDecode, Role: "capture", Format: "mjpeg",
				Component: "native.mjpeg-cpu", Backend: "native_sp", Hardware: false,
			}}}
			rt.reconcileHardwareComponentRetryScopesLocked()
			if statuses := rt.hardwareRecoveryStatusesLocked(now); len(statuses) != 0 {
				t.Fatalf("returning MJPEG graph resurrected stale recovery identity: %+v", statuses)
			}
			effective := rt.effectiveCapabilityInventoryLocked(now)
			if !effective.Components[0].Available {
				t.Fatalf("fresh MJPEG graph inherited an obsolete cooldown: %+v", effective.Components[0])
			}
			fresh, changed := rt.noteHardwareComponentFailureLocked(
				jpuComponent, capability.StageDecode, "mjpeg", 8, 0, now)
			if !changed || fresh.ConsecutiveFailures != 1 ||
				!fresh.RetryAfter.Equal(now.Add(brokerComponentRetryInitial)) || fresh.TrialToken != 0 {
				t.Fatalf("fresh MJPEG failure did not start a new bounded recovery: changed=%t state=%+v", changed, fresh)
			}
		})
	}
}

func TestHardwareComponentRetryDelayIsExponentialAndBounded(t *testing.T) {
	want := []time.Duration{
		30 * time.Second,
		time.Minute,
		2 * time.Minute,
		4 * time.Minute,
		5 * time.Minute,
		5 * time.Minute,
	}
	for index, expected := range want {
		if got := brokerComponentRetryDelay(index + 1); got != expected {
			t.Fatalf("failure %d retry delay=%s want=%s", index+1, got, expected)
		}
	}
}

func TestStartedChildExitBeforeCameraAdmissionBuildsProcessBackoff(t *testing.T) {
	rt := &videoBrokerRuntime{}
	rt.noteUnadmittedChildExitLocked(false, false)
	if rt.childStartBackoff != brokerChildRestartDelay || rt.nextChildStartAt.IsZero() {
		t.Fatalf("first post-Start failure did not arm backoff: backoff=%s retry_at=%s", rt.childStartBackoff, rt.nextChildStartAt)
	}
	rt.noteUnadmittedChildExitLocked(false, false)
	if rt.childStartBackoff != 2*brokerChildRestartDelay {
		t.Fatalf("repeated S_FMT/startup exit did not increase backoff: %s", rt.childStartBackoff)
	}
	rt.noteUnadmittedChildExitLocked(false, true)
	if rt.childStartBackoff != 2*brokerChildRestartDelay {
		t.Fatalf("lifecycle shutdown changed startup backoff: %s", rt.childStartBackoff)
	}
}

func TestNormalizeBrokerChildStderrLinePreservesStructuredContent(t *testing.T) {
	clean := "[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_runtime_failed output=nv12 retry_token=0"
	if got, sanitation := normalizeBrokerChildStderrLine([]byte(clean)); got != clean || sanitation != (brokerChildStderrSanitation{}) {
		t.Fatalf("clean structured stderr changed: %q", got)
	}

	decorated := []byte("\x1b]0;vendor diagnostic\x07\x1b[1;33m" +
		strings.Replace(clean, " requested=", "\trequested=", 1) +
		"\x1b[0m\x07\x00\r\u202e")
	if got, sanitation := normalizeBrokerChildStderrLine(decorated); got != clean || sanitation.ansiSequences != 3 || sanitation.controlBytes != 5 {
		t.Fatalf("terminal-decorated stderr was not normalized: %q", got)
	}
	rawC1CSI := append([]byte{0x9b}, []byte("33m"+clean)...)
	rawC1CSI = append(rawC1CSI, 0x9b)
	rawC1CSI = append(rawC1CSI, []byte("0m")...)
	for label, raw := range map[string][]byte{
		"raw-c1-csi":     rawC1CSI,
		"unicode-c1-csi": []byte("\u009b33m" + clean + "\u009b0m"),
		"control-string": []byte("\x1bPvendor-private\x1b\\" + clean),
	} {
		if got, _ := normalizeBrokerChildStderrLine(raw); got != clean {
			t.Fatalf("%s stderr was not normalized: %q", label, got)
		}
	}

	invalid := append([]byte("vendor diagnostic "), 0xff, 0x00)
	if got, sanitation := normalizeBrokerChildStderrLine(invalid); got != "vendor diagnostic ?" || sanitation.invalidUTF8 != 1 || sanitation.controlBytes != 1 {
		t.Fatalf("invalid stderr bytes were not made log-safe: %q", got)
	}
}

func TestScanStderrNormalizesDecoratedFallbackBeforeStatusAndRecovery(t *testing.T) {
	const (
		jpuComponent = "native.jpu"
		cpuComponent = "native.mjpeg-cpu"
	)
	inventory := capability.Inventory{
		Schema: capability.Schema,
		Components: []capability.Component{
			{ID: jpuComponent, Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}}},
			{ID: cpuComponent, Backend: "native_sp", Hardware: false, Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}}},
		},
	}
	rt := &videoBrokerRuntime{
		plan:              videoBrokerPlan{CapabilityInventory: inventory},
		componentRecovery: make(map[string]hardwareComponentRecovery),
		capabilityPlan: capability.Plan{Stages: []capability.StageSelection{{
			Stage: capability.StageDecode, Role: "capture", Format: "mjpeg", Component: jpuComponent,
			Backend: "native_sp", Hardware: true, State: capability.StateCandidate,
		}}},
		mediaEpoch: 4,
		createdAt:  time.Now(),
	}
	rt.resetChildFailure(4)
	cleanFallback := "[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_runtime_failed output=nv12 retry_token=0"
	decoratedFallback := "\x1b[33m" + strings.Replace(cleanFallback, " selected=", "\tselected=", 1) + "\x1b[0m\x07\x00\r\n"
	if _, ok := parseBrokerCaptureDecoderTelemetry(decoratedFallback); ok {
		t.Fatal("ANSI fixture no longer reproduces the raw-parser failure")
	}

	var output bytes.Buffer
	previousWriter := log.Writer()
	previousFlags := log.Flags()
	previousPrefix := log.Prefix()
	log.SetOutput(&output)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(previousWriter)
		log.SetFlags(previousFlags)
		log.SetPrefix(previousPrefix)
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		rt.wg.Wait()
	})
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(decoratedFallback)), 4, 321)
	status := serviceWithGraphs(map[string]*graphService{"primary": {sourceID: "primary", runtime: rt}}).Snapshot(time.Now())
	sourceStatus := status.Sources["primary"]
	if status.Schema != "video-broker.status.v5" ||
		sourceStatus.CaptureDecodeCodec != "mjpeg" ||
		sourceStatus.CaptureDecodeRequested != "auto" ||
		sourceStatus.CaptureDecodeBackend != "cpu" ||
		sourceStatus.CaptureDecodeFallback != "jpu_runtime_failed" {
		t.Fatalf("decorated fallback did not reach current status: %+v", status)
	}
	if len(sourceStatus.HardwareRecovery) != 1 ||
		sourceStatus.HardwareRecovery[0].ComponentID != jpuComponent ||
		sourceStatus.HardwareRecovery[0].RetryAt == nil ||
		!sourceStatus.HardwareRecovery[0].RetryAt.After(status.ObservedAt) {
		t.Fatalf("hardware fallback did not schedule bounded recovery: %+v", sourceStatus.HardwareRecovery)
	}
	if len(sourceStatus.CapabilityPlan.Stages) != 1 ||
		sourceStatus.CapabilityPlan.Stages[0].Component != cpuComponent ||
		sourceStatus.CapabilityPlan.Stages[0].Hardware ||
		sourceStatus.CapabilityPlan.Stages[0].Fallback != "jpu_runtime_failed" {
		t.Fatalf("runtime graph did not attest temporary CPU continuity: %+v", sourceStatus.CapabilityPlan.Stages)
	}

	decoratedTerminal := "\x1b[31m[rdkx5_v4l2_mux_bridge] VIDIOC_DQBUF failed err=No such device\x1b[0m\x07\x00\r\n"
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(decoratedTerminal)), 4, 321)
	if reason, _ := rt.childFailure(4); reason != "uvc_dqbuf_enodev" {
		t.Fatalf("terminal matcher did not receive normalized stderr: %q", reason)
	}

	cancel()
	rt.wg.Wait()
	logged := output.String()
	if !strings.Contains(logged, cleanFallback) ||
		!strings.Contains(logged, "sanitized_ansi_sequences=2") ||
		!strings.Contains(logged, "sanitized_control_bytes=3") ||
		strings.ContainsAny(logged, "\x1b\x07\x00\r") {
		t.Fatalf("emitted bridge log was not safely normalized: %q", logged)
	}
}

func TestCaptureDecoderHardwareFailureRecognizesInitAndRuntimeFailures(t *testing.T) {
	rt := &videoBrokerRuntime{capabilityPlan: capability.Plan{Stages: []capability.StageSelection{{
		Stage: capability.StageDecode, Role: "capture", Format: "mjpeg",
		Component: "native.jpu", Backend: "native_sp", Hardware: true,
	}}}}
	for _, fallback := range []string{"jpu_init_failed", "jpu_runtime_failed"} {
		observed := brokerCaptureDecoderTelemetry{
			Codec: "mjpeg", Requested: "auto", Selected: "cpu", Fallback: fallback, Output: "nv12",
		}
		if got := rt.captureDecoderHardwareFailureComponentLocked(observed); got != "native.jpu" {
			t.Fatalf("fallback=%s mapped component=%q", fallback, got)
		}
	}
}

func TestCaptureDecoderRuntimeFailureUsesTemporaryCPUFallbackAndDeduplicatesEpoch(t *testing.T) {
	const (
		jpuComponent = "native.jpu"
		cpuComponent = "native.mjpeg-cpu"
	)
	inventory := capability.Inventory{
		Schema: capability.Schema,
		Components: []capability.Component{
			{ID: jpuComponent, Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}}},
			{ID: cpuComponent, Backend: "native_sp", Hardware: false, Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	rt := &videoBrokerRuntime{
		plan:              videoBrokerPlan{CapabilityInventory: inventory},
		componentRecovery: make(map[string]hardwareComponentRecovery),
		capabilityPlan: capability.Plan{Stages: []capability.StageSelection{{
			Stage: capability.StageDecode, Role: "capture", Format: "mjpeg", Component: jpuComponent,
			Backend: "native_sp", Hardware: true, State: capability.StateCandidate,
		}}},
		mediaEpoch: 2,
	}
	line := "[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_runtime_failed output=nv12 retry_token=0\n"
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(line)), 1, 0)
	if len(rt.componentRecovery) != 0 {
		t.Fatalf("late decoder telemetry from an old epoch changed current capability state: %+v", rt.componentRecovery)
	}
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(line)), 2, 0)
	if recovery := rt.componentRecovery[jpuComponent]; recovery.ConsecutiveFailures != 1 || !recovery.RetryAfter.After(time.Now()) {
		t.Fatalf("failed component did not enter temporary recovery: %+v", recovery)
	}
	if got := rt.capabilityPlan.Stages[0]; got.Component != cpuComponent || got.Hardware {
		t.Fatalf("runtime decoder attestation did not record CPU fallback: %+v", got)
	}

	// Duplicate telemetry belongs to the same live fallback epoch and must not
	// create a recovery action.
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(line)), 2, 0)
	if recovery := rt.componentRecovery[jpuComponent]; recovery.ConsecutiveFailures != 1 {
		t.Fatalf("duplicate fallback telemetry changed failure count: %+v", recovery)
	}
	cancel()
	rt.wg.Wait()
}

func TestHardwareComponentRetryIsInPlaceAndKeepsCaptureEpoch(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	cancels := 0
	rt := &videoBrokerRuntime{
		componentRecovery: map[string]hardwareComponentRecovery{
			"native.jpu": {
				ComponentID: "native.jpu", Stage: capability.StageDecode, Format: "mjpeg",
				ConsecutiveFailures: 1, LastFailureEpoch: 7, RetryAfter: now,
			},
		},
		childCmd:                  &exec.Cmd{Process: &os.Process{Pid: 43210}},
		childRunning:              true,
		childCancel:               func() { cancels++ },
		childControlAvailable:     true,
		childControl:              make(chan bridgeRuntimeControl, 1),
		childComponentRetryScopes: map[string]string{"native.jpu": videoproviders.ComponentRetryInPlace},
		mediaEpoch:                7,
		capabilityPlan: capability.Plan{Stages: []capability.StageSelection{{
			Stage: capability.StageDecode, Role: "capture", Format: "mjpeg",
			Component: "native.mjpeg-cpu", Backend: "native_sp", Hardware: false,
		}}},
		captureModeFailures: make(map[string]int),
	}
	rt.resetChildFailure(7)
	if _, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now.Add(-time.Nanosecond)); changed {
		t.Fatal("hardware retry started before its deadline")
	}
	requested, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now)
	if !changed || requested.TrialToken == 0 || requested.TrialMediaEpoch != 7 || requested.TrialChildPID != 43210 {
		t.Fatalf("due hardware retry did not create an identity-bound trial: changed=%t state=%+v", changed, requested)
	}
	if cancels != 0 || brokerChildPID(rt.childCmd) != 43210 || rt.mediaEpoch != 7 {
		t.Fatalf("in-place retry disrupted capture: cancels=%d pid=%d epoch=%d", cancels, brokerChildPID(rt.childCmd), rt.mediaEpoch)
	}
	control := <-rt.childControl
	if control.RetryAction != brokerBridgeRetryActionTrial ||
		control.RetryComponent != "native.jpu" || control.RetryToken != requested.TrialToken ||
		control.RetryDeadlineMS != requested.TrialDeadline.UnixMilli() {
		t.Fatalf("retry was not sent through the persistent bridge control: %+v", control)
	}
	statuses := rt.hardwareRecoveryStatusesLocked(now)
	if len(statuses) != 1 || statuses[0].State != "trial_requested" ||
		statuses[0].RetryScope != videoproviders.ComponentRetryInPlace ||
		statuses[0].RetryToken != requested.TrialToken || statuses[0].TrialMediaEpoch != 7 ||
		statuses[0].TrialChildPID != 43210 || statuses[0].TrialDeadline == nil {
		t.Fatalf("source status did not expose causal retry identity: %+v", statuses)
	}
	if len(rt.captureModeFailures) != 0 {
		t.Fatalf("component retry poisoned camera-mode health: %+v", rt.captureModeFailures)
	}
	if reason, _ := rt.childFailure(7); reason != "" {
		t.Fatalf("component retry incorrectly marked the healthy capture child failed: %q", reason)
	}

	// A stale timer may never cancel a graph after a newer failure moved the
	// retry boundary.
	rt.componentRecovery["native.jpu"] = hardwareComponentRecovery{
		ComponentID: "native.jpu", Stage: capability.StageDecode, Format: "mjpeg",
		ConsecutiveFailures: 2, RetryAfter: now.Add(time.Minute),
	}
	if _, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now.Add(time.Hour)); changed || cancels != 0 {
		t.Fatalf("stale retry boundary changed the graph: changed=%t cancels=%d", changed, cancels)
	}

	// A due timer racing an existing teardown must not overwrite its causal
	// failure. The next naturally-created graph is already hardware-eligible.
	rt.componentRecovery["native.jpu"] = hardwareComponentRecovery{
		ComponentID: "native.jpu", Stage: capability.StageDecode, Format: "mjpeg",
		ConsecutiveFailures: 2, RetryAfter: now,
	}
	rt.childFinishing = true
	deferred, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now.Add(time.Hour))
	if !changed || !deferred.AwaitNaturalEpoch || deferred.TrialToken != 0 || cancels != 0 {
		t.Fatalf("retry timer interfered with teardown instead of deferring: changed=%t state=%+v cancels=%d", changed, deferred, cancels)
	}
}

func TestHardwareComponentRetrySchedulerQueuesInPlaceTrialAtDueBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := time.Now()
	recovery := hardwareComponentRecovery{
		ComponentID: "native.jpu", Stage: capability.StageDecode, Format: "mjpeg",
		ConsecutiveFailures: 1, LastFailureEpoch: 9, RetryAfter: now,
	}
	rt := &videoBrokerRuntime{
		componentRecovery:         map[string]hardwareComponentRecovery{"native.jpu": recovery},
		childCmd:                  &exec.Cmd{Process: &os.Process{Pid: 43211}},
		childRunning:              true,
		childControlAvailable:     true,
		childControl:              make(chan bridgeRuntimeControl, 1),
		childComponentRetryScopes: map[string]string{"native.jpu": videoproviders.ComponentRetryInPlace},
		mediaEpoch:                9,
		capabilityPlan: capability.Plan{Stages: []capability.StageSelection{{
			Stage: capability.StageDecode, Role: "capture", Format: "mjpeg",
			Component: "native.mjpeg-cpu", Backend: "native_sp", Hardware: false,
		}}},
	}
	rt.resetChildFailure(9)
	rt.mu.Lock()
	rt.scheduleHardwareComponentRetryLocked(ctx, recovery)
	rt.mu.Unlock()
	select {
	case control := <-rt.childControl:
		if control.RetryAction != brokerBridgeRetryActionTrial ||
			control.RetryComponent != "native.jpu" || control.RetryToken == 0 {
			t.Fatalf("scheduler queued invalid trial: %+v", control)
		}
	case <-time.After(time.Second):
		t.Fatal("hardware retry scheduler did not queue an in-place trial")
	}
	cancel()
	rt.wg.Wait()
}

func newMJPEGComponentRecoveryRuntime(now time.Time, mediaEpoch uint64, childPID int) *videoBrokerRuntime {
	inventory := capability.Inventory{
		Schema: capability.Schema,
		Components: []capability.Component{
			{ID: "native.jpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}}},
			{ID: "native.mjpeg-cpu", Backend: "native_sp", Hardware: false, Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}}},
		},
	}
	return &videoBrokerRuntime{
		plan: videoBrokerPlan{CapabilityInventory: inventory},
		componentRecovery: map[string]hardwareComponentRecovery{
			"native.jpu": {
				ComponentID: "native.jpu", Stage: capability.StageDecode, Format: "mjpeg",
				ConsecutiveFailures: 1, LastFailureEpoch: mediaEpoch, RetryAfter: now,
			},
		},
		childCmd:                  &exec.Cmd{Process: &os.Process{Pid: childPID}},
		childRunning:              true,
		childControlAvailable:     true,
		childControl:              make(chan bridgeRuntimeControl, 1),
		childComponentRetryScopes: map[string]string{"native.jpu": videoproviders.ComponentRetryInPlace},
		mediaEpoch:                mediaEpoch,
		capabilityPlan: capability.Plan{Stages: []capability.StageSelection{{
			Stage: capability.StageDecode, Role: "capture", Format: "mjpeg",
			Component: "native.mjpeg-cpu", Backend: "native_sp", Hardware: false,
		}}},
	}
}

func TestHardwareComponentRetryAcknowledgesExactTokenFrameAndKeepsCPUUntilThen(t *testing.T) {
	now := time.Now()
	rt := newMJPEGComponentRecoveryRuntime(now, 12, 45120)
	requested, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now)
	if !changed || requested.TrialToken == 0 {
		t.Fatalf("failed to arm retry fixture: changed=%t state=%+v", changed, requested)
	}
	<-rt.childControl
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		rt.wg.Wait()
	})

	stale := fmt.Sprintf("[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=jpu fallback=none output=nv12 retry_token=%d\n", requested.TrialToken+1)
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(stale)), 12, 45120)
	if state := rt.componentRecovery["native.jpu"]; state.TrialToken != requested.TrialToken || !state.StableAfter.IsZero() {
		t.Fatalf("stale token mutated the trial: %+v", state)
	}
	if got := rt.capabilityPlan.Stages[0]; got.Hardware || got.Component != "native.mjpeg-cpu" {
		t.Fatalf("CPU continuity changed before exact hardware acknowledgement: %+v", got)
	}
	ready := fmt.Sprintf(
		"[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_ready component=native.jpu retry_token=%d completed_unix_ms=%d\n",
		requested.TrialToken, requested.TrialStartedAt.UnixMilli())
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(ready)), 11, 45120)
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(ready)), 12, 45121)
	if state := rt.componentRecovery["native.jpu"]; state.TrialCommitRequested {
		t.Fatalf("stale epoch or child PID authorized a candidate: %+v", state)
	}
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(ready)), 12, 45120)
	state := rt.componentRecovery["native.jpu"]
	if !state.TrialCommitRequested || state.TrialCandidateReady.IsZero() ||
		!state.TrialDeadline.After(requested.TrialDeadline) {
		t.Fatalf("exact ready receipt did not authorize a bounded commit: %+v", state)
	}
	commitControl := <-rt.childControl
	if commitControl.RetryAction != brokerBridgeRetryActionCommit ||
		commitControl.RetryToken != requested.TrialToken ||
		commitControl.RetryDeadlineMS != state.TrialDeadline.UnixMilli() {
		t.Fatalf("candidate ready did not publish exact commit control: %+v", commitControl)
	}

	success := fmt.Sprintf("[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=jpu fallback=none output=nv12 retry_token=%d\n", requested.TrialToken)
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(success)), 11, 45120)
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(success)), 12, 45121)
	if state := rt.componentRecovery["native.jpu"]; !state.StableAfter.IsZero() {
		t.Fatalf("stale epoch or child PID acknowledged a trial: %+v", state)
	}
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(success)), 12, 45120)
	state = rt.componentRecovery["native.jpu"]
	if state.StableAfter.IsZero() || state.HalfOpenEpoch != 12 || state.HalfOpenChildPID != 45120 || state.TrialToken != requested.TrialToken {
		t.Fatalf("exact token/frame acknowledgement did not enter half-open: %+v", state)
	}
	if got := rt.capabilityPlan.Stages[0]; !got.Hardware || got.Component != "native.jpu" {
		t.Fatalf("hardware was not attested after exact acknowledgement: %+v", got)
	}

	// A runtime failure after the acknowledged trial is a new epoch-bound
	// observation. It returns to CPU without touching capture PID or epoch.
	fallback := "[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_runtime_failed output=nv12 retry_token=0\n"
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(fallback)), 12, 45120)
	state = rt.componentRecovery["native.jpu"]
	if state.ConsecutiveFailures != 2 || state.RetryAfter.IsZero() || !state.StableAfter.IsZero() {
		t.Fatalf("post-trial fallback did not rearm cooldown: %+v", state)
	}
	if got := rt.capabilityPlan.Stages[0]; got.Hardware || got.Component != "native.mjpeg-cpu" {
		t.Fatalf("post-trial fallback did not retain CPU continuity: %+v", got)
	}
	if brokerChildPID(rt.childCmd) != 45120 || rt.mediaEpoch != 12 {
		t.Fatalf("decoder fallback reopened capture: pid=%d epoch=%d", brokerChildPID(rt.childCmd), rt.mediaEpoch)
	}
}

func TestHardwareComponentCandidateReadyReadAfterDeadlineCannotCommit(t *testing.T) {
	now := time.Now()
	rt := newMJPEGComponentRecoveryRuntime(now, 18, 45180)
	requested, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now)
	if !changed || requested.TrialToken == 0 {
		t.Fatalf("failed to arm delayed-pipe fixture: changed=%t state=%+v", changed, requested)
	}
	<-rt.childControl
	completedBeforeDeadline := requested.TrialDeadline.Add(-time.Millisecond).UnixMilli()
	failed, changed := rt.expireHardwareComponentTrialLocked(
		"native.jpu", requested.TrialToken, requested.TrialDeadline, requested.TrialDeadline)
	if !changed || failed.TrialToken != 0 || failed.CancelToken != requested.TrialToken {
		t.Fatalf("deadline did not atomically replace trial with cancel: changed=%t state=%+v", changed, failed)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lateReady := fmt.Sprintf(
		"[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_ready component=native.jpu retry_token=%d completed_unix_ms=%d\n",
		requested.TrialToken, completedBeforeDeadline)
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(lateReady)), 18, 45180)
	state := rt.componentRecovery["native.jpu"]
	if state.TrialCommitRequested || state.TrialToken != 0 || state.CancelToken != requested.TrialToken {
		t.Fatalf("late ready receipt resurrected an expired trial: %+v", state)
	}
	control := rt.effectiveBridgeControlLocked("late_ready_rejected")
	if control.RetryAction != brokerBridgeRetryActionCancel ||
		control.RetryToken != requested.TrialToken || control.RetryDeadlineMS != 0 {
		t.Fatalf("late ready receipt did not retain exact cancellation: %+v", control)
	}
	if got := rt.capabilityPlan.Stages[0]; got.Hardware || got.Component != "native.mjpeg-cpu" {
		t.Fatalf("late ready receipt changed CPU continuity plan: %+v", got)
	}
}

func TestHardwareComponentCandidateCancelAcknowledgementPreservesRetryLiveness(t *testing.T) {
	armExpiredTrial := func(t *testing.T, mediaEpoch uint64, childPID int) (*videoBrokerRuntime, hardwareComponentRecovery) {
		t.Helper()
		now := time.Now()
		rt := newMJPEGComponentRecoveryRuntime(now, mediaEpoch, childPID)
		requested, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now)
		if !changed || requested.TrialToken == 0 {
			t.Fatalf("failed to arm cancellation fixture: changed=%t state=%+v", changed, requested)
		}
		<-rt.childControl
		failed, changed := rt.expireHardwareComponentTrialLocked(
			"native.jpu", requested.TrialToken, requested.TrialDeadline, requested.TrialDeadline)
		if !changed || failed.CancelToken != requested.TrialToken || failed.RetryAfter.IsZero() {
			t.Fatalf("trial expiry did not create a bounded cancellation: changed=%t state=%+v", changed, failed)
		}
		return rt, failed
	}

	t.Run("ack_before_retry_timer", func(t *testing.T) {
		rt, failed := armExpiredTrial(t, 181, 45181)
		originalRetryAfter := failed.RetryAfter
		acknowledged, changed, scheduleRetry := rt.acknowledgeHardwareComponentCandidateCanceledLocked(
			"native.jpu", 181, 45181, failed.CancelToken, originalRetryAfter.Add(-time.Second))
		if !changed || scheduleRetry || acknowledged.CancelToken != 0 ||
			!acknowledged.RetryAfter.Equal(originalRetryAfter) || acknowledged.AwaitNaturalEpoch {
			t.Fatalf("early cancel acknowledgement disturbed the existing retry timer: changed=%t schedule=%t state=%+v",
				changed, scheduleRetry, acknowledged)
		}
		next, changed := rt.requestHardwareComponentRetryLocked(
			"native.jpu", originalRetryAfter, originalRetryAfter)
		if !changed || next.TrialToken == 0 || next.TrialMediaEpoch != 181 || next.TrialChildPID != 45181 {
			t.Fatalf("acknowledged cancellation did not permit the due trial: changed=%t state=%+v", changed, next)
		}
	})

	t.Run("ack_after_retry_timer", func(t *testing.T) {
		rt, failed := armExpiredTrial(t, 182, 45182)
		deferred, changed := rt.requestHardwareComponentRetryLocked(
			"native.jpu", failed.RetryAfter, failed.RetryAfter)
		if !changed || !deferred.AwaitNaturalEpoch || !deferred.RetryAfter.IsZero() || deferred.CancelToken == 0 ||
			deferred.RetryScope != videoproviders.ComponentRetryNextNaturalEpoch {
			t.Fatalf("pending cancellation was not deferred without overlap: changed=%t state=%+v", changed, deferred)
		}
		ackAt := failed.RetryAfter.Add(time.Second)
		acknowledged, changed, scheduleRetry := rt.acknowledgeHardwareComponentCandidateCanceledLocked(
			"native.jpu", 182, 45182, deferred.CancelToken, ackAt)
		if !changed || !scheduleRetry || acknowledged.AwaitNaturalEpoch ||
			acknowledged.RetryAfter.IsZero() || !acknowledged.RetryAfter.After(ackAt) || acknowledged.CancelToken != 0 ||
			acknowledged.RetryScope != videoproviders.ComponentRetryInPlace {
			t.Fatalf("late cancel acknowledgement did not rearm a bounded retry: changed=%t schedule=%t state=%+v",
				changed, scheduleRetry, acknowledged)
		}
		if _, changed := rt.requestHardwareComponentRetryLocked(
			"native.jpu", acknowledged.RetryAfter, acknowledged.RetryAfter.Add(-time.Nanosecond)); changed {
			t.Fatal("late acknowledgement retried before its replacement deadline")
		}
		next, changed := rt.requestHardwareComponentRetryLocked(
			"native.jpu", acknowledged.RetryAfter, acknowledged.RetryAfter)
		if !changed || next.TrialToken == 0 || next.TrialMediaEpoch != 182 || next.TrialChildPID != 45182 {
			t.Fatalf("late acknowledgement left same-epoch CPU recovery stuck: changed=%t state=%+v", changed, next)
		}
	})
}

func TestHardwareComponentCommittedCandidateRollbackCannotClearHalfOpen(t *testing.T) {
	now := time.Now()
	rt := newMJPEGComponentRecoveryRuntime(now, 19, 45190)
	requested, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now)
	if !changed || requested.TrialToken == 0 {
		t.Fatalf("failed to arm rollback fixture: changed=%t state=%+v", changed, requested)
	}
	<-rt.childControl
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		rt.wg.Wait()
	})
	ready := fmt.Sprintf(
		"[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_ready component=native.jpu retry_token=%d completed_unix_ms=%d\n",
		requested.TrialToken, requested.TrialStartedAt.UnixMilli())
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(ready)), 19, 45190)
	<-rt.childControl // exact commit envelope
	committed := fmt.Sprintf(
		"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=jpu fallback=none output=nv12 retry_token=%d\n",
		requested.TrialToken)
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(committed)), 19, 45190)
	halfOpen := rt.componentRecovery["native.jpu"]
	if halfOpen.StableAfter.IsZero() || !rt.capabilityPlan.Stages[0].Hardware {
		t.Fatalf("fixture did not enter hardware half-open: state=%+v plan=%+v", halfOpen, rt.capabilityPlan.Stages[0])
	}
	<-rt.childControl // deliberately delayed/lost `none` finalization
	rolledBack := fmt.Sprintf(
		"[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_canceled component=native.jpu retry_token=%d\n",
		requested.TrialToken)
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(rolledBack)), 19, 45190)
	state := rt.componentRecovery["native.jpu"]
	if state.StableAfter.IsZero() == false || state.RetryAfter.IsZero() ||
		state.ConsecutiveFailures != 2 || state.TrialToken != 0 {
		t.Fatalf("exact half-open rollback did not rearm cooldown: %+v", state)
	}
	if got := rt.capabilityPlan.Stages[0]; got.Hardware || got.Component != "native.mjpeg-cpu" {
		t.Fatalf("exact half-open rollback left a false JPU plan: %+v", got)
	}
	if rt.observedDecodeBackend != "cpu" || rt.observedDecodeFallback != "jpu_retry_deadline_failed" {
		t.Fatalf("rollback did not publish causal CPU status: backend=%q fallback=%q",
			rt.observedDecodeBackend, rt.observedDecodeFallback)
	}
	if rt.confirmHardwareComponentStableLocked(
		"native.jpu", 19, 45190, halfOpen.StableAfter, halfOpen.StableAfter.Add(time.Second)) {
		t.Fatal("stale half-open timer cleared a rolled-back candidate")
	}
	if brokerChildPID(rt.childCmd) != 45190 || rt.mediaEpoch != 19 {
		t.Fatalf("candidate rollback reopened capture: pid=%d epoch=%d", brokerChildPID(rt.childCmd), rt.mediaEpoch)
	}
}

func TestHardwareComponentRetryFailureTimeoutExitAndNaturalAdapterAreCausal(t *testing.T) {
	now := time.Now()
	rt := newMJPEGComponentRecoveryRuntime(now, 20, 45200)
	requested, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now)
	if !changed {
		t.Fatal("failed to arm in-place trial")
	}
	<-rt.childControl
	failed, changed := rt.expireHardwareComponentTrialLocked(
		"native.jpu", requested.TrialToken, requested.TrialDeadline, requested.TrialDeadline)
	if !changed || failed.ConsecutiveFailures != 2 || failed.RetryAfter.IsZero() || failed.TrialToken != 0 {
		t.Fatalf("trial timeout did not rearm bounded cooldown: changed=%t state=%+v", changed, failed)
	}

	failedCandidateRuntime := newMJPEGComponentRecoveryRuntime(now, 30, 45300)
	candidate, changed := failedCandidateRuntime.requestHardwareComponentRetryLocked("native.jpu", now, now)
	if !changed {
		t.Fatal("failed to arm candidate failure fixture")
	}
	<-failedCandidateRuntime.childControl
	ctx, cancel := context.WithCancel(context.Background())
	failureLine := fmt.Sprintf("[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_retry_frame_failed output=nv12 retry_token=%d\n", candidate.TrialToken)
	failedCandidateRuntime.scanStderr(ctx, io.NopCloser(strings.NewReader(failureLine)), 30, 45300)
	cancel()
	failedCandidateRuntime.wg.Wait()
	candidateFailure := failedCandidateRuntime.componentRecovery["native.jpu"]
	if candidateFailure.ConsecutiveFailures != 2 || candidateFailure.RetryAfter.IsZero() || candidateFailure.TrialToken != 0 ||
		failedCandidateRuntime.capabilityPlan.Stages[0].Hardware {
		t.Fatalf("candidate failure did not preserve CPU and rearm cooldown: %+v plan=%+v", candidateFailure, failedCandidateRuntime.capabilityPlan.Stages[0])
	}

	failed.RetryAfter = now
	if _, acknowledged, _ := rt.acknowledgeHardwareComponentCandidateCanceledLocked(
		"native.jpu", 20, 45200, requested.TrialToken, now); !acknowledged {
		t.Fatal("exact cancellation was not acknowledged")
	}
	failed = rt.componentRecovery["native.jpu"]
	failed.RetryAfter = now
	rt.componentRecovery["native.jpu"] = failed
	second, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now)
	if !changed || second.TrialToken == 0 {
		t.Fatalf("second trial was not armed: changed=%t state=%+v", changed, second)
	}
	<-rt.childControl
	rt.abortHardwareComponentTrialsForChildLocked(20, 45200)
	aborted := rt.componentRecovery["native.jpu"]
	if !aborted.AwaitNaturalEpoch || aborted.TrialToken != 0 || aborted.HalfOpenEpoch != 0 {
		t.Fatalf("child exit left a trial or half-open state stuck: %+v", aborted)
	}

	// An adapter without an in-place declaration never forces a reopen. It
	// waits until another cause creates a new epoch, then accepts token zero
	// only from that exact new child after its first hardware frame.
	aborted.RetryAfter = now
	aborted.AwaitNaturalEpoch = false
	rt.componentRecovery["native.jpu"] = aborted
	rt.childComponentRetryScopes = nil
	deferred, changed := rt.requestHardwareComponentRetryLocked("native.jpu", now, now)
	if !changed || !deferred.AwaitNaturalEpoch || deferred.TrialToken != 0 || len(rt.childControl) != 0 {
		t.Fatalf("portable adapter did not defer to a natural epoch: changed=%t state=%+v", changed, deferred)
	}
	rt.mediaEpoch = 21
	rt.childCmd = &exec.Cmd{Process: &os.Process{Pid: 45201}}
	rt.capabilityPlan.Stages[0].Component = "native.jpu"
	rt.capabilityPlan.Stages[0].Hardware = true
	halfOpen, changed := rt.noteHardwareComponentHalfOpenLocked("native.jpu", 21, 45201, 0, now.Add(time.Second))
	if !changed || halfOpen.HalfOpenEpoch != 21 || halfOpen.HalfOpenChildPID != 45201 {
		t.Fatalf("natural epoch hardware frame was not admitted half-open: changed=%t state=%+v", changed, halfOpen)
	}
}

func TestHardwareComponentNaturalEpochHardwareAdmissionBypassesOnlyOldEpochCooldown(t *testing.T) {
	now := time.Now()
	rt := newMJPEGComponentRecoveryRuntime(now, 40, 45400)
	state := rt.componentRecovery["native.jpu"]
	state.LastFailureEpoch = 40
	state.RetryAfter = now.Add(time.Hour)
	rt.componentRecovery["native.jpu"] = state
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		rt.wg.Wait()
	})
	hardware := "[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=jpu fallback=none output=nv12 retry_token=0\n"

	// The same failed epoch cannot bypass cooldown with an unbound token-zero
	// observation, and rejected telemetry cannot move the attested plan to JPU.
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(hardware)), 40, 45400)
	state = rt.componentRecovery["native.jpu"]
	if !state.StableAfter.IsZero() || state.RetryAfter.IsZero() {
		t.Fatalf("same-epoch hardware observation bypassed cooldown: %+v", state)
	}
	if got := rt.capabilityPlan.Stages[0]; got.Hardware || got.Component != "native.mjpeg-cpu" {
		t.Fatalf("rejected same-epoch telemetry mutated the CPU plan: %+v", got)
	}
	state.AwaitNaturalEpoch = true
	state.RetryAfter = time.Time{}
	state.RetryScope = videoproviders.ComponentRetryNextNaturalEpoch
	rt.componentRecovery["native.jpu"] = state
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(hardware)), 40, 45400)
	state = rt.componentRecovery["native.jpu"]
	if !state.StableAfter.IsZero() || !state.AwaitNaturalEpoch ||
		state.RetryScope != videoproviders.ComponentRetryNextNaturalEpoch {
		t.Fatalf("AwaitNaturalEpoch admitted delayed telemetry from the same child: %+v", state)
	}
	if got := rt.capabilityPlan.Stages[0]; got.Hardware || got.Component != "native.mjpeg-cpu" {
		t.Fatalf("same-epoch AwaitNaturalEpoch telemetry mutated the CPU plan: %+v", got)
	}

	// A naturally-created child is a distinct causal epoch. Its exact token-zero
	// hardware frame may enter half-open immediately even though the old epoch's
	// timer has not elapsed; no forced reopen is needed.
	rt.childRunning = false
	rt.abortHardwareComponentTrialsForChildLocked(40, 45400)
	rt.mediaEpoch = 41
	rt.childCmd = &exec.Cmd{Process: &os.Process{Pid: 45401}}
	rt.childRunning = true
	rt.scanStderr(ctx, io.NopCloser(strings.NewReader(hardware)), 41, 45401)
	halfOpen := rt.componentRecovery["native.jpu"]
	if halfOpen.StableAfter.IsZero() || halfOpen.HalfOpenEpoch != 41 ||
		halfOpen.HalfOpenChildPID != 45401 || halfOpen.TrialToken != 0 || !halfOpen.RetryAfter.IsZero() ||
		halfOpen.RetryScope != videoproviders.ComponentRetryNextNaturalEpoch {
		t.Fatalf("new natural epoch did not enter hardware half-open: %+v", halfOpen)
	}
	if got := rt.capabilityPlan.Stages[0]; !got.Hardware || got.Component != "native.jpu" {
		t.Fatalf("new natural epoch did not attest JPU: %+v", got)
	}
	rt.totalSubFrames++
	if !rt.confirmHardwareComponentStableLocked(
		"native.jpu", 41, 45401, halfOpen.StableAfter, halfOpen.StableAfter) {
		t.Fatal("healthy frames in the new natural epoch did not clear recovery")
	}
	if _, ok := rt.componentRecovery["native.jpu"]; ok {
		t.Fatalf("stable new epoch retained recovery history: %+v", rt.componentRecovery["native.jpu"])
	}
}

func TestHardwareComponentScopeReconciliationPreservesNaturalEpochBoundary(t *testing.T) {
	rt := &videoBrokerRuntime{
		componentRecovery: map[string]hardwareComponentRecovery{
			"native.jpu": {
				ComponentID:       "native.jpu",
				Stage:             capability.StageDecode,
				Format:            "mjpeg",
				RetryScope:        videoproviders.ComponentRetryNextNaturalEpoch,
				AwaitNaturalEpoch: true,
			},
		},
		capabilityPlan: capability.Plan{Stages: []capability.StageSelection{{
			Stage: capability.StageDecode, Role: "capture", Format: "mjpeg",
			Component: "native.mjpeg-cpu", Backend: "native_sp", Hardware: false,
		}}},
		childComponentRetryScopes: map[string]string{
			"native.jpu": videoproviders.ComponentRetryInPlace,
		},
	}
	rt.reconcileHardwareComponentRetryScopesLocked()
	deferred := rt.componentRecovery["native.jpu"]
	if deferred.RetryScope != videoproviders.ComponentRetryNextNaturalEpoch || !deferred.AwaitNaturalEpoch {
		t.Fatalf("new child capability discovery rewrote a natural-epoch boundary: %+v", deferred)
	}

	deferred.AwaitNaturalEpoch = false
	rt.componentRecovery["native.jpu"] = deferred
	rt.reconcileHardwareComponentRetryScopesLocked()
	rearmed := rt.componentRecovery["native.jpu"]
	if rearmed.RetryScope != videoproviders.ComponentRetryInPlace || rearmed.AwaitNaturalEpoch {
		t.Fatalf("active in-place state did not adopt the child capability: %+v", rearmed)
	}
}

func TestHardwareComponentFailureHistoryClearsOnlyAfterStableHardwareWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	const componentID = "native.jpu"
	rt := &videoBrokerRuntime{
		componentRecovery: map[string]hardwareComponentRecovery{
			componentID: {
				ComponentID: componentID, Stage: capability.StageDecode, Format: "mjpeg",
				ConsecutiveFailures: 2, LastFailureEpoch: 4, RetryAfter: now,
			},
		},
		mediaEpoch:     5,
		childRunning:   true,
		childCmd:       &exec.Cmd{Process: &os.Process{Pid: 43212}},
		totalSubFrames: 10,
		capabilityPlan: capability.Plan{Stages: []capability.StageSelection{{
			Stage: capability.StageDecode, Role: "capture", Format: "mjpeg",
			Component: componentID, Backend: "native_sp", Hardware: true,
		}}},
	}
	halfOpen, changed := rt.noteHardwareComponentHalfOpenLocked(componentID, 5, 43212, 0, now)
	if !changed || halfOpen.ConsecutiveFailures != 2 || !halfOpen.StableAfter.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("hardware retry did not enter half-open state: changed=%t state=%+v", changed, halfOpen)
	}
	if rt.confirmHardwareComponentStableLocked(componentID, 5, 43212, halfOpen.StableAfter, now.Add(time.Minute)) {
		t.Fatal("failure history cleared before stable hardware window")
	}
	statuses := rt.hardwareRecoveryStatusesLocked(now.Add(time.Minute))
	if len(statuses) != 1 || statuses[0].State != "half_open" || statuses[0].Rejected || statuses[0].StableAfter == nil {
		t.Fatalf("half-open recovery status is incomplete: %+v", statuses)
	}
	rt.totalSubFrames++
	if !rt.confirmHardwareComponentStableLocked(componentID, 5, 43212, halfOpen.StableAfter, halfOpen.StableAfter) {
		t.Fatal("healthy hardware window did not clear failure history")
	}
	if len(rt.componentRecovery) != 0 {
		t.Fatalf("recovered component retained stale history: %+v", rt.componentRecovery)
	}
}

func TestVideoBrokerFrameWatchdogRestartsBridgeWithBoundedChildRecovery(t *testing.T) {
	cmd, childCancel := startGracefullyCancellableTestChild(t)
	waited := false
	t.Cleanup(func() {
		childCancel()
		if !waited {
			_ = cmd.Wait()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			Transport:       "v4l2_uvc",
			CaptureDevice:   "/dev/video0",
			AnalysisEnabled: true,
		},
		childCmd:       cmd,
		childCancel:    childCancel,
		childRunning:   true,
		childStartedAt: time.Now().Add(-2 * time.Second),
	}

	done := make(chan struct{})
	go func() {
		rt.watchChildFrames(ctx, cmd, time.Second, 0)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for frame watchdog")
	}

	if err := cmd.Wait(); err == nil {
		waited = true
		t.Fatal("expected watchdog to terminate stale child")
	} else {
		waited = true
	}
}

func TestVideoBrokerFrameWatchdogParksOnlyUnrequestedDelivery(t *testing.T) {
	for _, dormantLive := range []bool{false, true} {
		t.Run(fmt.Sprintf("dormant_live_%t", dormantLive), func(t *testing.T) {
			cmd, childCancel := startGracefullyCancellableTestChild(t)
			waited := false
			t.Cleanup(func() {
				childCancel()
				if !waited {
					_ = cmd.Wait()
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rt := &videoBrokerRuntime{
				plan:     videoBrokerPlan{LiveEnabled: dormantLive, MainFPS: 30},
				childCmd: cmd, childCancel: childCancel, childRunning: true,
				childStartedAt:        time.Now().Add(-time.Minute),
				childControlSupported: true,
			}
			done := make(chan struct{})
			go func() {
				rt.watchChildFrames(ctx, cmd, time.Second, 0)
				close(done)
			}()
			select {
			case <-done:
				t.Fatal("watchdog restarted a source with no requested delivery")
			case <-time.After(1200 * time.Millisecond):
			}
			rt.mu.Lock()
			if dormantLive {
				rt.routingLiveDemand = true
			} else {
				rt.plan.AnalysisEnabled = true
			}
			rt.mu.Unlock()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("watchdog ignored missing frames after delivery became required")
			}
			if err := cmd.Wait(); err == nil {
				waited = true
				t.Fatal("watchdog did not terminate the stalled demanded source")
			}
			waited = true
		})
	}
}

func TestVideoBrokerCadenceWatchdogPreservesUVCChildAndRecoversHealth(t *testing.T) {
	cmd, childCancel := startGracefullyCancellableTestChild(t)
	waited := false
	t.Cleanup(func() {
		childCancel()
		if !waited {
			_ = cmd.Wait()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			Transport:       videoproviders.TransportV4L2UVC,
			AnalysisEnabled: true,
			SubFPS:          20,
			CaptureFPS:      20,
		},
		childCmd:        cmd,
		childCancel:     childCancel,
		childRunning:    true,
		childStartedAt:  time.Now(),
		lastFrameAt:     time.Now(),
		lastMainFrameAt: time.Now(),
		lastSubFrameAt:  time.Now(),
	}
	var subFramesPerTick atomic.Int64
	keepAliveDone := make(chan struct{})
	go func() {
		defer close(keepAliveDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				rt.mu.Lock()
				rt.lastFrameAt = now
				rt.lastMainFrameAt = now
				rt.nextMainFrame++
				if frames := subFramesPerTick.Load(); frames > 0 {
					rt.lastSubFrameAt = now
					rt.totalSubFrames += uint64(frames)
				}
				rt.mu.Unlock()
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		rt.watchChildFrames(ctx, cmd, time.Second, 0)
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rt.mu.Lock()
		degraded := rt.cadenceDegraded
		modeFailures := len(rt.captureModeFailures)
		rt.mu.Unlock()
		if degraded {
			if modeFailures != 0 {
				t.Fatalf("cadence degradation poisoned camera mode health: %+v", rt.captureModeFailures)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	rt.mu.Lock()
	degraded := rt.cadenceDegraded
	rt.mu.Unlock()
	if !degraded {
		t.Fatal("timed out waiting for cadence degradation")
	}
	select {
	case <-done:
		t.Fatal("cadence watchdog terminated the live UVC child")
	default:
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("cadence watchdog stopped live UVC producer: %v", err)
	}
	if health := rt.healthSnapshot(time.Now()); health.State != "running" || health.LastError != "" {
		t.Fatalf("reduced performance changed source availability: %+v", health)
	}
	rt.mu.Lock()
	warning := rt.streamPerformance["analysis"]
	rt.mu.Unlock()
	if warning.State != "reduced" || warning.TargetFPS != 20 || warning.WindowMS <= 0 {
		t.Fatalf("analysis performance warning is incomplete: %+v", warning)
	}

	subFramesPerTick.Store(3)
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		rt.mu.Lock()
		recovered := !rt.cadenceDegraded
		rt.mu.Unlock()
		if recovered {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	rt.mu.Lock()
	recovered := !rt.cadenceDegraded
	rt.mu.Unlock()
	if !recovered {
		t.Fatal("cadence health did not recover while the same child kept producing")
	}

	rt.mu.Lock()
	rt.childControlSupported = true
	rt.governor.SubFPS = 10
	rt.mu.Unlock()
	subFramesPerTick.Store(1)
	time.Sleep(2500 * time.Millisecond)
	rt.mu.Lock()
	degradedUnderGovernorCap := rt.cadenceDegraded
	rt.mu.Unlock()
	if degradedUnderGovernorCap {
		t.Fatal("thermal analysis cadence cap was treated as a producer fault")
	}

	cancel()
	<-done
	<-keepAliveDone
	childCancel()
	if err := cmd.Wait(); err == nil {
		waited = true
		t.Fatal("expected graceful cleanup to terminate child")
	}
	waited = true
}

func TestVideoBrokerCadenceWatchdogMonitorsArchiveWithoutAnalysisRole(t *testing.T) {
	cmd, childCancel := startGracefullyCancellableTestChild(t)
	waited := false
	t.Cleanup(func() {
		childCancel()
		if !waited {
			_ = cmd.Wait()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			Transport:       videoproviders.TransportV4L2UVC,
			AnalysisEnabled: false,
			ArchiveEnabled:  true,
			ArchiveFPS:      15,
			CaptureFPS:      30,
		},
		childCmd:           cmd,
		childCancel:        childCancel,
		childRunning:       true,
		childStartedAt:     time.Now(),
		lastFrameAt:        time.Now(),
		lastArchiveFrameAt: time.Now(),
	}
	keepAliveDone := make(chan struct{})
	go func() {
		defer close(keepAliveDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				rt.mu.Lock()
				rt.lastFrameAt = now
				rt.lastArchiveFrameAt = now
				rt.mu.Unlock()
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		rt.watchChildFrames(ctx, cmd, time.Second, 0)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rt.mu.Lock()
		degraded := rt.cadenceDegraded
		role := rt.cadenceRole
		rt.mu.Unlock()
		if degraded {
			if role != "archive" {
				t.Fatalf("archive cadence attributed to role %q", role)
			}
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	rt.mu.Lock()
	degraded := rt.cadenceDegraded
	role := rt.cadenceRole
	modeFailures := len(rt.captureModeFailures)
	rt.mu.Unlock()
	if !degraded || role != "archive" {
		t.Fatalf("archive cadence failure was hidden degraded=%t role=%q", degraded, role)
	}
	if modeFailures != 0 {
		t.Fatalf("archive cadence degradation poisoned camera mode health: %+v", rt.captureModeFailures)
	}
	select {
	case <-done:
		t.Fatal("archive cadence watcher terminated the capture owner")
	default:
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("archive cadence watcher stopped the capture owner: %v", err)
	}
	if health := rt.healthSnapshot(time.Now()); health.State != "running" || health.LastError != "" {
		t.Fatalf("reduced archive performance changed source availability: %+v", health)
	}
	rt.mu.Lock()
	warning := rt.streamPerformance["archive"]
	rt.mu.Unlock()
	if warning.State != "reduced" || warning.WindowMS <= 0 {
		t.Fatalf("archive performance warning is incomplete: %+v", warning)
	}

	cancel()
	<-done
	<-keepAliveDone
	childCancel()
	if err := cmd.Wait(); err == nil {
		waited = true
		t.Fatal("expected graceful cleanup to terminate child")
	}
	waited = true
}

func TestVideoBrokerCadenceTargetsIgnoreOnlyDormantControlledLive(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true,
			MainFPS: 30, SubFPS: 20, ArchiveFPS: 15,
		},
		childControlSupported: true,
		nextMainFrame:         10,
		totalSubFrames:        20,
		nextArchiveFrame:      30,
		governor:              resourceGovernorDecision{SubFPS: 10},
	}
	targets := rt.streamCadenceTargetsLocked()
	required := map[string]bool{}
	expected := map[string]int{}
	for _, target := range targets {
		required[target.role] = target.required
		expected[target.role] = target.expectedFPS
	}
	if required["live"] || !required["analysis"] || !required["archive"] {
		t.Fatalf("unexpected dormant-live cadence targets: %+v", targets)
	}
	if expected["analysis"] != 10 {
		t.Fatalf("applied analysis governor cap was ignored: %+v", targets)
	}
	rt.mainClients = map[int]*brokerClient{
		1: {mainControl: videoMainClientControl{TargetFPS: 15, TargetBitrateKbps: 2048}},
	}
	targets = rt.streamCadenceTargetsLocked()
	for _, target := range targets {
		if target.role == "live" && (!target.required || target.expectedFPS != 15) {
			t.Fatalf("applied viewer cadence was compared with the static profile: %+v", targets)
		}
	}
	rt.mainClients = nil
	rt.routingLiveDemand = true
	targets = rt.streamCadenceTargetsLocked()
	for _, target := range targets {
		if target.role == "live" && (!target.required || target.expectedFPS != 30) {
			t.Fatalf("routing live demand did not preserve profile cadence: %+v", targets)
		}
	}
	rt.childControlSupported = false
	rt.mainClients = map[int]*brokerClient{
		1: {mainControl: videoMainClientControl{TargetFPS: 15, TargetBitrateKbps: 2048}},
	}
	targets = rt.streamCadenceTargetsLocked()
	for _, target := range targets {
		if target.role == "analysis" && target.expectedFPS != 20 {
			t.Fatalf("unapplied analysis governor cap hid the configured cadence: %+v", targets)
		}
		if target.role == "live" && target.expectedFPS != 30 {
			t.Fatalf("unapplied viewer control hid the fixed producer cadence: %+v", targets)
		}
	}
}

func TestVideoBrokerCameraAdmissionAcceptsSlowProducerBeforeDeadline(t *testing.T) {
	cmd, childCancel := startGracefullyCancellableTestChild(t)
	waited := false
	t.Cleanup(func() {
		childCancel()
		if !waited {
			_ = cmd.Wait()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			LiveEnabled:     true,
			MainFormat:      "h264",
			AnalysisEnabled: true,
			ArchiveEnabled:  true,
			Transport:       videoproviders.TransportV4L2UVC,
			MainFPS:         20,
			SubFPS:          20,
			CaptureFPS:      30,
		},
		cameraAdmission: videoproviders.CameraAdmissionDecision{
			DevicePath:            "/dev/video0",
			CapabilityFingerprint: "cap-v1:1234567890abcdef",
			Format:                "mjpeg",
			CaptureWidth:          1280,
			CaptureHeight:         720,
			CaptureFPS:            30,
		},
		observedCaptureCodec:  "mjpeg",
		observedCaptureWidth:  1280,
		observedCaptureHeight: 720,
		observedCaptureFPS:    30,
		childCmd:              cmd,
		childCancel:           childCancel,
		childRunning:          true,
		lastFrameAt:           time.Now(),
		// With a one-second watchdog, admission grace is 16 seconds. Start
		// just before the steady-state reset so this test crosses the actual
		// deadline without sleeping for the whole grace period.
		childStartedAt: time.Now().Add(-14200 * time.Millisecond),
	}
	feedAfter := time.Now().Add(1200 * time.Millisecond)
	var framesPerTick atomic.Int64
	framesPerTick.Store(1)
	keepAliveDone := make(chan struct{})
	go func() {
		defer close(keepAliveDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				rt.mu.Lock()
				rt.lastFrameAt = now
				if now.Before(feedAfter) {
					rt.mu.Unlock()
					continue
				}
				rt.lastSubFrameAt = now
				rt.lastArchiveFrameAt = now
				frames := uint64(framesPerTick.Load())
				tracker := &rt.cameraAdmissionTracker
				if tracker.FirstLiveFrameAt.IsZero() {
					tracker.FirstLiveFrameAt = now
				}
				if tracker.FirstSubFrameAt.IsZero() {
					tracker.FirstSubFrameAt = now
				}
				if tracker.FirstArchiveFrameAt.IsZero() {
					tracker.FirstArchiveFrameAt = now
				}
				tracker.LastSubFrameAt = now
				tracker.LastArchiveFrameAt = now
				tracker.LastLiveFrameAt = now
				tracker.LiveFrames += frames
				tracker.LiveSeenSPS = true
				tracker.LiveSeenPPS = true
				tracker.LiveSeenKeyframe = true
				tracker.SubFrames += frames
				tracker.ArchiveFrames += frames
				tracker.ArchiveCodec = "h265"
				tracker.SeenVPS = true
				tracker.SeenSPS = true
				tracker.SeenPPS = true
				tracker.SeenKeyframe = true
				rt.maybeCompleteCameraAdmissionLocked()
				rt.mu.Unlock()
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		rt.watchChildFrames(ctx, cmd, time.Second, 0)
		close(done)
	}()

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		rt.mu.Lock()
		preserved := rt.cameraAdmitted
		rt.mu.Unlock()
		if preserved {
			break
		}
		select {
		case <-done:
			t.Fatal("admission deadline terminated a live degraded producer")
		case <-time.After(25 * time.Millisecond):
		}
	}
	rt.mu.Lock()
	preserved := rt.cameraAdmitted
	modeFailures := len(rt.captureModeFailures)
	rt.mu.Unlock()
	if !preserved {
		t.Fatal("live producer was not preserved at admission deadline")
	}
	if modeFailures != 0 {
		t.Fatalf("preserved producer poisoned camera mode health: %+v", rt.captureModeFailures)
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("admission deadline stopped live producer: %v", err)
	}

	framesPerTick.Store(4)
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rt.mu.Lock()
		admitted := rt.cameraAdmitted
		rt.mu.Unlock()
		if admitted {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	rt.mu.Lock()
	admitted := rt.cameraAdmitted
	rt.mu.Unlock()
	if !admitted {
		t.Fatal("camera admission was lost while delivery recovered")
	}

	cancel()
	<-done
	<-keepAliveDone
	childCancel()
	if err := cmd.Wait(); err == nil {
		waited = true
		t.Fatal("expected graceful cleanup to terminate child")
	} else {
		waited = true
	}
	if len(rt.captureModeExclusions) != 0 {
		t.Fatalf("startup cadence polluted camera mode health: %+v", rt.captureModeExclusions)
	}
}

func TestVideoBrokerCameraAdmissionDeadlineRejectsStaleProducer(t *testing.T) {
	cmd, childCancel := startGracefullyCancellableTestChild(t)
	waited := false
	t.Cleanup(func() {
		childCancel()
		if !waited {
			_ = cmd.Wait()
		}
	})

	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC, SubFPS: 20, CaptureFPS: 30},
		cameraAdmission: videoproviders.CameraAdmissionDecision{
			DevicePath: "/dev/video0", CapabilityFingerprint: "cap-v1:1234567890abcdef",
			Format: "mjpeg", CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 30,
		},
		childCmd: cmd, childCancel: childCancel, childRunning: true,
		childStartedAt: time.Now().Add(-16 * time.Second),
	}
	modeKey := cameraAdmissionModeKey(rt.cameraAdmission)
	done := make(chan struct{})
	go func() {
		rt.watchChildFrames(context.Background(), cmd, time.Second, 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for stale admission recovery")
	}
	if err := cmd.Wait(); err == nil {
		waited = true
		t.Fatal("expected stale admission producer to be terminated")
	}
	waited = true
	rt.mu.Lock()
	failures := rt.captureModeFailures[modeKey]
	rt.mu.Unlock()
	if failures != 1 {
		t.Fatalf("stale admission did not record exactly one mode failure: %d", failures)
	}
}

func TestVideoBrokerMainWatchdogRestartsBridgeWithoutQueuingUSBReset(t *testing.T) {
	cmd, childCancel := startGracefullyCancellableTestChild(t)
	waited := false
	t.Cleanup(func() {
		childCancel()
		if !waited {
			_ = cmd.Wait()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			Transport:     "v4l2_uvc",
			CaptureDevice: "/dev/video0",
			SubFPS:        20,
			CaptureFPS:    20,
		},
		childCmd:       cmd,
		childCancel:    childCancel,
		childRunning:   true,
		childStartedAt: time.Now(),
		lastFrameAt:    time.Now(),
		lastSubFrameAt: time.Now(),
		mainClients:    map[int]*brokerClient{1: {}},
	}
	keepAliveDone := make(chan struct{})
	go func() {
		defer close(keepAliveDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				rt.mu.Lock()
				rt.lastFrameAt = now
				rt.lastSubFrameAt = now
				rt.totalSubFrames += 3
				rt.mu.Unlock()
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		rt.watchChildFrames(ctx, cmd, time.Second, 0)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("timed out waiting for main stream watchdog")
	}
	cancel()
	<-keepAliveDone
	if err := cmd.Wait(); err == nil {
		waited = true
		t.Fatal("expected main stream watchdog to terminate stalled child")
	} else {
		waited = true
	}
}

func TestResolveVideoBrokerCaptureDevice(t *testing.T) {
	plan := videoBrokerPlan{CaptureDevice: "/dev/video0"}
	if got := resolveVideoBrokerCaptureDevice(plan); got != "/dev/video0" {
		t.Fatalf("unexpected capture device: %s", got)
	}
	t.Setenv("VIDEO_BROKER_VIDEO_CAPTURE_DEVICE", "/dev/video2")
	if got := resolveVideoBrokerCaptureDevice(plan); got != "/dev/video0" {
		t.Fatalf("process-global device overrode source config: %s", got)
	}
	if got := resolveVideoBrokerCaptureDevice(videoBrokerPlan{}); got != "" {
		t.Fatalf("empty source acquired an implicit capture device: %s", got)
	}
	if err := validateBrokerCaptureDeviceReady("v4l2_uvc", ""); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("empty v4l2 source did not fail closed: %v", err)
	}
}

func TestValidateBrokerCaptureDeviceReadyReportsUSBCameraWithoutV4L(t *testing.T) {
	origStat := brokerStatPath
	origGlob := brokerGlobPaths
	origUSBDir := brokerUSBDevicesDir
	t.Cleanup(func() {
		brokerStatPath = origStat
		brokerGlobPaths = origGlob
		brokerUSBDevicesDir = origUSBDir
	})

	tmp := t.TempDir()
	usb := filepath.Join(tmp, "1-1")
	if err := os.MkdirAll(usb, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(usb, "product"), []byte("USB 2.0 Camera\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(usb, "idVendor"), []byte("0c45\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(usb, "idProduct"), []byte("6366\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	brokerUSBDevicesDir = tmp
	brokerStatPath = func(path string) (os.FileInfo, error) {
		return nil, &os.PathError{Op: "stat", Path: path, Err: os.ErrNotExist}
	}
	brokerGlobPaths = func(pattern string) ([]string, error) {
		return []string{"/dev/video2"}, nil
	}

	err := validateBrokerCaptureDeviceReady("v4l2_uvc", "/dev/video0")
	if err == nil {
		t.Fatal("expected missing V4L device error")
	}
	msg := err.Error()
	for _, want := range []string{"reason=no_v4l2_node", "visible_video_nodes=video2", "USB 2.0 Camera", "id=0c45:6366", "v4l=no"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("diagnostic %q missing %q", msg, want)
		}
	}
}

func TestValidateBrokerCaptureDeviceReadyIgnoresNonUVCTransports(t *testing.T) {
	origStat := brokerStatPath
	t.Cleanup(func() {
		brokerStatPath = origStat
	})
	brokerStatPath = func(path string) (os.FileInfo, error) {
		t.Fatalf("non-UVC transport must not stat capture device")
		return nil, nil
	}
	if err := validateBrokerCaptureDeviceReady("vendor_hbn", "/dev/video0"); err != nil {
		t.Fatalf("unexpected non-UVC validation error: %v", err)
	}
}

func TestNormalizeCoalescedLogKeyIgnoresRetryAfterCountdown(t *testing.T) {
	first := normalizeCoalescedLogKey("[video-broker] attach failed err=video broker child start deferred retry_after=29s")
	second := normalizeCoalescedLogKey("[video-broker] attach failed err=video broker child start deferred retry_after=28s")
	if first != second {
		t.Fatalf("retry_after countdown must normalize to same key: %q != %q", first, second)
	}
}

func readBrokerPacket(t *testing.T, conn net.Conn) (brokerPacketHeader, []byte) {
	t.Helper()
	headerBuf := make([]byte, brokerPacketHeaderSize)
	if _, err := io.ReadFull(conn, headerBuf); err != nil {
		t.Fatalf("read header failed: %v", err)
	}
	header, err := parseBrokerPacketHeader(headerBuf)
	if err != nil {
		t.Fatalf("parse header failed: %v", err)
	}
	payload := make([]byte, int(header.PayloadSize))
	if _, err := io.ReadFull(conn, payload); err != nil {
		t.Fatalf("read payload failed: %v", err)
	}
	return header, payload
}

func parseBrokerPacket(t *testing.T, packet []byte) (brokerPacketHeader, []byte) {
	t.Helper()
	if len(packet) < brokerPacketHeaderSize {
		t.Fatalf("packet too short: %d", len(packet))
	}
	header, err := parseBrokerPacketHeader(packet[:brokerPacketHeaderSize])
	if err != nil {
		t.Fatalf("parse packet header failed: %v", err)
	}
	payload := packet[brokerPacketHeaderSize:]
	if uint32(len(payload)) != header.PayloadSize {
		t.Fatalf("payload size mismatch header=%d actual=%d", header.PayloadSize, len(payload))
	}
	return header, payload
}
