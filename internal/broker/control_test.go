package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/capability"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

func TestParseBrokerRuntimeCapabilityIsExactAndBounded(t *testing.T) {
	line := "[rdkx5_v4l2_mux_bridge][capability] live_enabled=true analysis_enabled=true archive_enabled=true capture_codec=h265 capture=1920x1080 capture_fps=30 live_codec=h265 archive_codec=h265 archive_delivery=processed_hardware_encode archive_fps=15 live_control_mode=persistent_encoder_epoch fanout_control_mode=branch_transaction_v1"
	got, ok := parseBrokerRuntimeCapability(line)
	if !ok || !got.LiveEnabled || !got.AnalysisEnabled || !got.ArchiveEnabled || got.CaptureCodec != "h265" || got.CaptureWidth != 1920 || got.CaptureHeight != 1080 ||
		got.ArchiveCodec != "h265" || got.ArchiveDelivery != "processed_hardware_encode" || got.ArchiveFPS != 15 || got.LiveControlMode != "persistent_encoder_epoch" ||
		got.FanoutControlMode != videoproviders.WorkerFanoutControlBranchTransactionV1 {
		t.Fatalf("unexpected capability: ok=%t got=%+v", ok, got)
	}
	for _, invalid := range []string{
		line + " legacy=true",
		strings.TrimSuffix(line, " fanout_control_mode=branch_transaction_v1"),
		"[rdkx5_v4l2_mux_bridge][capability] live_enabled=true analysis_enabled=true archive_enabled=true capture_codec=h265 capture=99999x1080 capture_fps=30 live_codec=h265 archive_codec=h265 archive_delivery=processed_hardware_encode archive_fps=30 live_control_mode=persistent_encoder_epoch fanout_control_mode=disabled",
		"[unknown][capability] live_enabled=true analysis_enabled=true archive_enabled=true capture_codec=h265 capture=1920x1080 capture_fps=30 live_codec=h265 archive_codec=h265 archive_delivery=processed_hardware_encode archive_fps=30 live_control_mode=persistent_encoder_epoch fanout_control_mode=disabled",
		"[video-ingest-gst][capability] live_enabled=false analysis_enabled=true archive_enabled=false capture_codec=nv12 capture=1280x720 capture_fps=30 live_codec=none archive_codec=h265 archive_delivery=disabled archive_fps=0 live_control_mode=disabled fanout_control_mode=disabled",
		"[video-ingest-gst][capability] live_enabled=false analysis_enabled=true archive_enabled=false capture_codec=nv12 capture=1280x720 capture_fps=30 live_codec=none archive_codec=none archive_delivery=disabled archive_fps=30 live_control_mode=disabled fanout_control_mode=disabled",
		"[video-ingest-gst][capability] live_enabled=false analysis_enabled=false archive_enabled=false capture_codec=nv12 capture=1280x720 capture_fps=30 live_codec=none archive_codec=none archive_delivery=disabled archive_fps=0 live_control_mode=disabled fanout_control_mode=disabled",
		"[video-ingest-gst][capability] live_enabled=true analysis_enabled=true archive_enabled=false capture_codec=nv12 capture=1280x720 capture_fps=30 live_codec=none archive_codec=none archive_delivery=disabled archive_fps=0 live_control_mode=backend_static fanout_control_mode=disabled",
		"[video-ingest-gst][capability] live_enabled=false analysis_enabled=true archive_enabled=false capture_codec=nv12 capture=1280x720 capture_fps=30 live_codec=none archive_codec=none archive_delivery=disabled archive_fps=0 live_control_mode=disabled fanout_control_mode=dynamic",
	} {
		if _, ok := parseBrokerRuntimeCapability(invalid); ok {
			t.Fatalf("invalid capability accepted: %q", invalid)
		}
	}
	analysisOnly, ok := parseBrokerRuntimeCapability("[video-ingest-gst][capability] live_enabled=false analysis_enabled=true archive_enabled=false capture_codec=nv12 capture=1280x720 capture_fps=30 live_codec=none archive_codec=none archive_delivery=disabled archive_fps=0 live_control_mode=disabled fanout_control_mode=disabled")
	if !ok || analysisOnly.LiveEnabled || !analysisOnly.AnalysisEnabled || analysisOnly.ArchiveEnabled || analysisOnly.ArchiveCodec != "none" {
		t.Fatalf("analysis-only capability rejected or misparsed: ok=%t got=%+v", ok, analysisOnly)
	}
}

func TestParseBrokerToneMappingTelemetry(t *testing.T) {
	line := "[rdkx5_v4l2_mux_bridge][tone-mapper] event=tone_mapping_summary requested_mode=auto active_mode=local raw_luma=92.0 target=96 gain_min=0.920 gain_max=1.340 statistics_updates=18 active=true"
	got, ok := parseBrokerToneMappingTelemetry(line)
	if !ok {
		t.Fatal("tone-mapping telemetry was not parsed")
	}
	if got.RequestedMode != "auto" || got.ActiveMode != "local" || !got.Active ||
		got.GainMin != 0.92 || got.GainMax != 1.34 || got.Updates != 18 {
		t.Fatalf("unexpected tone-mapping telemetry: %+v", got)
	}
	resolutionBounded := "[rdkx5_v4l2_mux_bridge][tone-mapper] event=tone_mapping_summary requested_mode=auto active_mode=local raw_luma=192.0 target=128 gain_min=0.700 gain_max=1.120 statistics_hz=8 effective_statistics_hz=2 statistics_updates=19 active=true"
	if got, ok = parseBrokerToneMappingTelemetry(resolutionBounded); !ok || got.Updates != 19 {
		t.Fatalf("resolution-bounded telemetry was not parsed: ok=%t got=%+v", ok, got)
	}
	accelerated := "[rdkx5_v4l2_mux_bridge][tone-mapper] event=tone_mapping_summary requested_mode=auto active_mode=local accelerator_requested=auto accelerator_selected=opencl raw_luma=80.0 target=128 gain_min=0.800 gain_max=1.800 statistics_hz=8 effective_statistics_hz=2 statistics_updates=20 active=true"
	if got, ok = parseBrokerToneMappingTelemetry(accelerated); !ok || got.Accelerator != "opencl" {
		t.Fatalf("accelerator telemetry was not parsed: ok=%t got=%+v", ok, got)
	}
	if _, ok := parseBrokerToneMappingTelemetry("[rdkx5_v4l2_mux_bridge][tone-mapper] event=tone_mapping_summary requested_mode=auto active_mode=local gain_min=2 gain_max=1 statistics_updates=1 active=true"); ok {
		t.Fatal("invalid tone-mapping gain range was accepted")
	}
	if _, ok := parseBrokerToneMappingTelemetry("[rdkx5_v4l2_mux_bridge][tone-mapper] requested_mode=auto active_mode=local gain_min=1 gain_max=2 statistics_updates=1 active=true"); ok {
		t.Fatal("legacy tone-mapping telemetry without the canonical event was accepted")
	}
}

func TestParsePortableBrokerToneMappingTelemetry(t *testing.T) {
	line := "[video-ingest-gst][tone-mapper] event=tone_mapping_summary requested_mode=auto active_mode=local accelerator_requested=auto accelerator_selected=cpu raw_luma=20.0 target=128 gain_min=1.100 gain_max=2.400 statistics_hz=8 effective_statistics_hz=8 statistics_updates=5 active=true"
	telemetry, ok := parseBrokerToneMappingTelemetry(line)
	if !ok || telemetry.Accelerator != "cpu" || !telemetry.Active || telemetry.Updates != 5 {
		t.Fatalf("portable tone telemetry not parsed: ok=%v value=%+v", ok, telemetry)
	}
}

func TestLiveV4L2ToneMappingSummaryUpdatesStatusAndAttestation(t *testing.T) {
	line := "[rdkx5_v4l2_mux_bridge][tone-mapper] event=tone_mapping_summary requested_mode=auto active_mode=local accelerator_requested=auto accelerator_selected=opencl raw_luma=41.2 raw_content_luma=65.8 output_luma=91.4 output_content_luma=112.1 raw_black_ratio=0.12000 output_black_ratio=0.01000 raw_white_ratio=0.00000 output_white_ratio=0.00200 signal_informative=true target=128 gain_min=0.800 gain_max=1.800 statistics_hz=8 effective_statistics_hz=2 statistics_updates=2113 active=true"
	tone, ok := parseBrokerToneMappingTelemetry(line)
	if !ok {
		t.Fatal("live V4L2 tone-mapping summary was not parsed")
	}
	runtime := &videoBrokerRuntime{
		plan: videoBrokerPlan{CapabilityInventory: capability.Inventory{Components: []capability.Component{{
			ID: "tone", Backend: "native_opencl", Hardware: true, Available: true,
			State:      capability.StateCandidate,
			Operations: []capability.Operation{{Stage: capability.StageToneMap, Format: "nv12"}},
		}}}},
		capabilityPlan: capability.Plan{
			EncodeProvider: "native_sp",
			ToneMapBackend: "native_opencl",
			Stages: []capability.StageSelection{{
				Stage: capability.StageToneMap, Format: "nv12", Component: "tone",
				Backend: "native_opencl", Hardware: true, State: capability.StateCandidate,
			}},
		},
		toneMappingSupported: true,
		toneMappingBackend:   "native_opencl",
	}
	runtime.applyToneMappingTelemetryLocked(tone)
	status := serviceWithGraphs(map[string]*graphService{"primary": {sourceID: "primary", runtime: runtime}}).Snapshot(time.Now())
	sourceStatus := status.Sources["primary"]
	if status.Schema != "video-broker.status.v5" || sourceStatus.ToneMappingAccelerator != "opencl" ||
		!sourceStatus.ToneMappingActive || sourceStatus.ToneMappingUpdates != 2113 {
		t.Fatalf("live tone telemetry did not reach status v5: %+v", status)
	}
	if len(sourceStatus.CapabilityPlan.Stages) != 1 || sourceStatus.CapabilityPlan.Stages[0].State != capability.StateAttested ||
		len(sourceStatus.CapabilityInventory.Components) != 1 || sourceStatus.CapabilityInventory.Components[0].State != capability.StateAttested {
		t.Fatalf("live tone telemetry did not attest the OpenCL capability: plan=%+v inventory=%+v", sourceStatus.CapabilityPlan, sourceStatus.CapabilityInventory)
	}
}

func TestParsePortableStageCapabilityTelemetry(t *testing.T) {
	line := "[video-ingest-gst][stage-capability] stage=decode role=capture format=mjpeg requested_backend=gstreamer_mpp selected_backend=software element=jpegdec fallback=gstreamer_mpp_unavailable"
	telemetry, ok := parseBrokerStageCapabilityTelemetry(line)
	if !ok || telemetry.Stage != "decode" || telemetry.Role != "capture" || telemetry.Selected != "software" || telemetry.Element != "jpegdec" {
		t.Fatalf("portable stage telemetry not parsed: ok=%v value=%+v", ok, telemetry)
	}
}

func TestParseNativeResizeCapabilityTelemetry(t *testing.T) {
	line := "[rdkx5_v4l2_mux_bridge][stage-capability] stage=resize role=analysis format=nv12 requested_backend=native_neon selected_backend=native_cpu element=nv12_cpu fallback=geometry_or_orientation_not_accelerated"
	telemetry, ok := parseBrokerStageCapabilityTelemetry(line)
	if !ok || telemetry.Stage != "resize" || telemetry.Role != "analysis" ||
		telemetry.Format != "nv12" || telemetry.Selected != "native_cpu" {
		t.Fatalf("native resize telemetry not parsed: ok=%v value=%+v", ok, telemetry)
	}
}

func TestResizeCapabilityIsReconciledBeforeRuntimeAttestation(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{CapabilityInventory: capability.Inventory{Components: []capability.Component{
			{ID: "neon", Backend: "native_neon", Hardware: true, Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageResize, Format: "nv12"}}},
			{ID: "cpu", Backend: "native_cpu", Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageResize, Format: "nv12"}}},
		}}},
		capabilityPlan: capability.Plan{
			ResizeBackend: "native_neon",
			Stages:        []capability.StageSelection{{Stage: capability.StageResize, Role: "analysis", Format: "nv12", Component: "neon", Backend: "native_neon", Hardware: true, State: capability.StateCandidate}},
		},
	}
	rt.reconcileRuntimeStageCapabilityLocked(brokerStageCapabilityTelemetry{
		Stage: capability.StageResize, Role: "analysis", Format: "nv12", Selected: "native_cpu", Fallback: "geometry_not_accelerated",
	})
	selection := rt.capabilityPlan.Stages[0]
	if rt.observedResizeBackend != "native_cpu" || rt.capabilityPlan.ResizeBackend != "native_cpu" ||
		selection.Component != "cpu" || selection.State != capability.StateCandidate {
		t.Fatalf("resize capability was not reconciled: observed=%q plan=%+v", rt.observedResizeBackend, rt.capabilityPlan)
	}
	rt.attestCapabilityStageRoleLocked(capability.StageResize, "analysis", "nv12")
	if rt.capabilityPlan.Stages[0].State != capability.StateAttested || rt.plan.CapabilityInventory.Components[1].State != capability.StateAttested {
		t.Fatalf("reconciled resize was not attested: plan=%+v inventory=%+v", rt.capabilityPlan, rt.plan.CapabilityInventory)
	}
}

func TestLiveAndArchiveResizeAttestationRequiresTelemetryAndProducedFrame(t *testing.T) {
	const componentID = "neon"
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			LiveEnabled: true, MainFormat: "h265", MainWidth: 1280, MainHeight: 720,
			ArchiveEnabled: true, ArchiveCodec: "h265", ArchiveWidth: 640, ArchiveHeight: 360,
			CapabilityInventory: capability.Inventory{Components: []capability.Component{{
				ID: componentID, Backend: "native_neon", Hardware: true, Available: true,
				State:      capability.StateCandidate,
				Operations: []capability.Operation{{Stage: capability.StageResize, Format: "nv12"}},
			}}},
		},
		capabilityPlan: capability.Plan{Stages: []capability.StageSelection{
			{Stage: capability.StageResize, Role: "archive", Format: "nv12", Component: componentID, Backend: "native_neon", Hardware: true, State: capability.StateCandidate},
			{Stage: capability.StageResize, Role: "live", Format: "nv12", Component: componentID, Backend: "native_neon", Hardware: true, State: capability.StateCandidate},
		}},
		mainClients:    map[int]*brokerClient{},
		archiveClients: map[int]*brokerClient{},
		subClients:     map[int]*brokerClient{},
	}
	h265 := []byte{
		0, 0, 0, 1, 32 << 1, 1, 0xaa,
		0, 0, 0, 1, 33 << 1, 1, 0xbb,
		0, 0, 0, 1, 34 << 1, 1, 0xcc,
		0, 0, 0, 1, 19 << 1, 1, 0xdd,
	}
	rt.dispatchFrame(brokerStreamH264, h265)
	rt.dispatchFrame(brokerStreamH265, h265)
	if err, ready := sourceMoveHardwareReadyLocked(rt); err != nil || ready {
		t.Fatalf("source move admitted resize without backend telemetry: ready=%t err=%v", ready, err)
	}
	for _, selection := range rt.capabilityPlan.Stages {
		if selection.State != capability.StateCandidate {
			t.Fatalf("%s resize was attested before backend telemetry: %+v", selection.Role, selection)
		}
	}
	for _, role := range []string{"archive", "live"} {
		rt.reconcileRuntimeStageCapabilityLocked(brokerStageCapabilityTelemetry{
			Stage: capability.StageResize, Role: role, Format: "nv12",
			Requested: "native_neon", Selected: "native_neon", Element: "nv12_neon", Fallback: "none",
		})
	}
	if rt.observedRoleResizeBackend["archive"] != "native_neon" ||
		rt.observedRoleResizeBackend["live"] != "native_neon" {
		t.Fatalf("role-scoped resize telemetry was lost: %+v", rt.observedRoleResizeBackend)
	}
	if err, ready := sourceMoveHardwareReadyLocked(rt); err != nil || ready {
		t.Fatalf("source move admitted telemetry without produced media: ready=%t err=%v", ready, err)
	}
	rt.dispatchFrame(brokerStreamH264, h265)
	rt.dispatchFrame(brokerStreamH265, h265)
	if err, ready := sourceMoveHardwareReadyLocked(rt); err != nil || !ready {
		t.Fatalf("source move rejected fully attested resize stages: ready=%t err=%v", ready, err)
	}
	for _, selection := range rt.capabilityPlan.Stages {
		if selection.State != capability.StateAttested {
			t.Fatalf("%s resize remained unattested after telemetry and media: %+v", selection.Role, selection)
		}
	}
	if rt.plan.CapabilityInventory.Components[0].State != capability.StateAttested {
		t.Fatalf("runtime resize component remained unattested: %+v", rt.plan.CapabilityInventory.Components[0])
	}
}

func TestParseBrokerCaptureDecoderTelemetry(t *testing.T) {
	line := "[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=jpu fallback=none output=nv12 retry_token=0"
	got, ok := parseBrokerCaptureDecoderTelemetry(line)
	if !ok || got.Codec != "mjpeg" || got.Requested != "auto" || got.Selected != "jpu" || got.Fallback != "none" || got.Output != "nv12" || got.RetryToken != 0 {
		t.Fatalf("unexpected capture decoder telemetry: %+v ok=%t", got, ok)
	}
	if _, ok := parseBrokerCaptureDecoderTelemetry("[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=gpu fallback=none output=nv12 retry_token=0"); ok {
		t.Fatal("invalid capture decoder backend accepted")
	}
	if _, ok := parseBrokerCaptureDecoderTelemetry("[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=jpu fallback=none output=nv12"); ok {
		t.Fatal("removed decoder telemetry without causal retry token was accepted")
	}
}

func TestParseBrokerCaptureDecoderTrialTelemetry(t *testing.T) {
	readyLine := "[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_ready component=native.jpu retry_token=41 completed_unix_ms=1700000000000"
	ready, ok := parseBrokerCaptureDecoderTrialTelemetry(readyLine)
	if !ok || ready.Event != "candidate_ready" || ready.ComponentID != "native.jpu" ||
		ready.RetryToken != 41 || ready.CompletedAtUnixMS != 1_700_000_000_000 {
		t.Fatalf("candidate-ready telemetry was not parsed: ok=%t got=%+v", ok, ready)
	}
	canceledLine := "[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_canceled component=native.jpu retry_token=41"
	canceled, ok := parseBrokerCaptureDecoderTrialTelemetry(canceledLine)
	if !ok || canceled.Event != "candidate_canceled" || canceled.RetryToken != 41 ||
		canceled.CompletedAtUnixMS != 0 {
		t.Fatalf("candidate-canceled telemetry was not parsed: ok=%t got=%+v", ok, canceled)
	}
	for _, invalid := range []string{
		"[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_ready component=native.jpu retry_token=0 completed_unix_ms=1700000000000",
		"[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_ready component=native.jpu retry_token=41",
		"[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_canceled component=native.jpu retry_token=41 completed_unix_ms=1700000000000",
	} {
		if _, ok := parseBrokerCaptureDecoderTrialTelemetry(invalid); ok {
			t.Fatalf("invalid trial telemetry accepted: %q", invalid)
		}
	}
}

func TestParseBrokerBridgeControlReadyTelemetryIsExactAndBounded(t *testing.T) {
	line := "[rdkx5_v4l2_mux_bridge][control] ready seq=41 main_active=true main_fps=20 main_bitrate_kbps=6144 sub_fps=15 encoder_epoch=7 scope=live_persistent_rc"
	ready, ok := parseBrokerBridgeControlReadyTelemetry(line)
	if !ok || ready.Sequence != 41 || !ready.MainActive || ready.MainFPS != 20 ||
		ready.MainBitrateKbps != 6144 || ready.SubFPS != 15 || ready.EncoderEpoch != 7 {
		t.Fatalf("unexpected control readiness telemetry: ok=%t ready=%+v", ok, ready)
	}
	for _, invalid := range []string{
		line + " legacy=true",
		"[rdkx5_v4l2_mux_bridge][control] ready seq=0 main_active=true main_fps=20 main_bitrate_kbps=6144 sub_fps=15 encoder_epoch=7 scope=live_persistent_rc",
		"[rdkx5_v4l2_mux_bridge][control] ready seq=41 main_active=false main_fps=20 main_bitrate_kbps=6144 sub_fps=15 encoder_epoch=7 scope=live_persistent_rc",
		"[rdkx5_v4l2_mux_bridge][control] ready seq=41 main_active=true main_fps=241 main_bitrate_kbps=6144 sub_fps=15 encoder_epoch=7 scope=live_persistent_rc",
		"[rdkx5_v4l2_mux_bridge][control] ready seq=41 main_active=true main_fps=20 main_bitrate_kbps=1048577 sub_fps=15 encoder_epoch=7 scope=live_persistent_rc",
		"[rdkx5_v4l2_mux_bridge][control] ready seq=41 main_active=true main_fps=20 main_bitrate_kbps=6144 sub_fps=0 encoder_epoch=7 scope=live_persistent_rc",
		"[rdkx5_v4l2_mux_bridge][control] ready seq=41 main_active=true main_fps=20 main_bitrate_kbps=6144 sub_fps=15 encoder_epoch=0 scope=live_persistent_rc",
		"[rdkx5_hbn_mux_bridge][control] ready seq=41 main_active=true main_fps=20 main_bitrate_kbps=6144 sub_fps=15 encoder_epoch=7 scope=live_persistent_rc",
	} {
		if _, ok := parseBrokerBridgeControlReadyTelemetry(invalid); ok {
			t.Fatalf("invalid control readiness telemetry accepted: %q", invalid)
		}
	}
}

func TestBridgeControlReadyIsBoundToCurrentCaptureIdentity(t *testing.T) {
	const (
		mediaEpoch = uint64(9)
		childPID   = 4242
	)
	rt := &videoBrokerRuntime{
		mediaEpoch: mediaEpoch, childRunning: true, childCmd: &exec.Cmd{Process: &os.Process{Pid: childPID}},
		controlSequence: 12,
	}
	ready := brokerBridgeControlReadyTelemetry{
		Sequence: 12, MainActive: true, MainFPS: 20, MainBitrateKbps: 6144,
		SubFPS: 15, EncoderEpoch: 3,
	}
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.FixedZone("test", 3600))
	if rt.noteBridgeControlReadyLocked(mediaEpoch-1, childPID, ready, now) ||
		rt.noteBridgeControlReadyLocked(mediaEpoch, childPID+1, ready, now) {
		t.Fatal("stale capture identity was accepted")
	}
	future := ready
	future.Sequence++
	if rt.noteBridgeControlReadyLocked(mediaEpoch, childPID, future, now) {
		t.Fatal("unissued control sequence was accepted")
	}
	if !rt.noteBridgeControlReadyLocked(mediaEpoch, childPID, ready, now) {
		t.Fatal("current capture readiness was rejected")
	}
	if got := rt.bridgeControlReady; got.MediaEpoch != mediaEpoch || got.ChildPID != childPID ||
		got.Sequence != ready.Sequence || !got.ObservedAt.Equal(now.UTC()) {
		t.Fatalf("readiness identity was not retained: %+v", got)
	}
	if rt.noteBridgeControlReadyLocked(mediaEpoch, childPID, ready, now.Add(time.Second)) {
		t.Fatal("duplicate readiness sequence was accepted")
	}
}

func TestScanStderrPublishesExactBridgeControlReady(t *testing.T) {
	const (
		mediaEpoch = uint64(11)
		childPID   = 4343
	)
	rt := &videoBrokerRuntime{
		mediaEpoch: mediaEpoch, childRunning: true,
		childCmd: &exec.Cmd{Process: &os.Process{Pid: childPID}}, controlSequence: 17,
	}
	line := "[rdkx5_v4l2_mux_bridge][control] ready seq=17 main_active=true main_fps=20 main_bitrate_kbps=6144 sub_fps=15 encoder_epoch=5 scope=live_persistent_rc"
	rt.scanStderr(context.Background(), io.NopCloser(strings.NewReader(line+"\n")), mediaEpoch, childPID)

	rt.mu.Lock()
	observed := rt.bridgeControlReady
	rt.mu.Unlock()
	if observed.MediaEpoch != mediaEpoch || observed.ChildPID != childPID || observed.Sequence != 17 ||
		observed.MainFPS != 20 || observed.MainBitrateKbps != 6144 || observed.SubFPS != 15 ||
		observed.EncoderEpoch != 5 || observed.ObservedAt.IsZero() {
		t.Fatalf("stderr readiness did not reach the runtime: %+v", observed)
	}
}

func TestWaitBridgeControlReadyRequiresExactTargets(t *testing.T) {
	const (
		mediaEpoch = uint64(4)
		childPID   = 3131
	)
	newRuntime := func() *videoBrokerRuntime {
		return &videoBrokerRuntime{
			mediaEpoch: mediaEpoch, childRunning: true,
			childCmd: &exec.Cmd{Process: &os.Process{Pid: childPID}}, controlSequence: 8,
		}
	}
	expected := bridgeRuntimeControl{
		Sequence: 7, MainActive: true, MainFPS: 20, MainBitrateKbps: 6144, SubFPS: 15,
	}

	rt := newRuntime()
	if !rt.noteBridgeControlReadyLocked(mediaEpoch, childPID, brokerBridgeControlReadyTelemetry{
		Sequence: 7, MainActive: true, MainFPS: 20, MainBitrateKbps: 6144,
		SubFPS: 15, EncoderEpoch: 2,
	}, time.Now()) {
		t.Fatal("could not seed exact readiness")
	}
	observed, err := rt.waitBridgeControlReady(context.Background(), mediaEpoch, childPID, expected)
	if err != nil || observed.Sequence != expected.Sequence || observed.EncoderEpoch != 2 {
		t.Fatalf("exact readiness was not returned: observed=%+v err=%v", observed, err)
	}

	rt = newRuntime()
	mismatch := expected
	mismatch.MainBitrateKbps = 3072
	if !rt.noteBridgeControlReadyLocked(mediaEpoch, childPID, brokerBridgeControlReadyTelemetry{
		Sequence: 7, MainActive: true, MainFPS: 20, MainBitrateKbps: 6144,
		SubFPS: 15, EncoderEpoch: 2,
	}, time.Now()) {
		t.Fatal("could not seed mismatched readiness")
	}
	if _, err := rt.waitBridgeControlReady(context.Background(), mediaEpoch, childPID, mismatch); err == nil {
		t.Fatal("mismatched readiness targets were accepted")
	}

	rt = newRuntime()
	if !rt.noteBridgeControlReadyLocked(mediaEpoch, childPID, brokerBridgeControlReadyTelemetry{
		Sequence: 8, MainActive: true, MainFPS: 20, MainBitrateKbps: 6144,
		SubFPS: 15, EncoderEpoch: 2,
	}, time.Now()) {
		t.Fatal("could not seed superseding readiness")
	}
	if _, err := rt.waitBridgeControlReady(context.Background(), mediaEpoch, childPID, expected); err == nil {
		t.Fatal("superseded readiness sequence was accepted")
	}

	rt = newRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rt.waitBridgeControlReady(ctx, mediaEpoch, childPID, expected); !errors.Is(err, context.Canceled) {
		t.Fatalf("readiness wait did not honor context cancellation: %v", err)
	}
}

func TestRuntimeCapabilityTransitionDoesNotGateStaticH264OnMissingEpoch(t *testing.T) {
	retainedConfig := []byte{1, 2, 3}
	rt := &videoBrokerRuntime{
		plan:        videoBrokerPlan{LiveEnabled: true, MainFormat: "h264", MainFPS: 20, LiveBitrateKbps: 6144, SubFPS: 20},
		mainClients: map[int]*brokerClient{}, subClients: map[int]*brokerClient{},
		childControl: make(chan bridgeRuntimeControl, 1), childControlAvailable: true,
		mainCodecConfig: [][]byte{retainedConfig},
	}
	dynamic := brokerRuntimeCapability{
		LiveEnabled:  true,
		CaptureCodec: "h265", CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
		LiveCodec:    "h264",
		ArchiveCodec: "h265", ArchiveDelivery: "processed_hardware_encode", ArchiveFPS: 30,
		LiveControlMode:   "persistent_encoder_epoch",
		FanoutControlMode: videoproviders.WorkerFanoutControlBranchTransactionV1,
	}
	rt.applyRuntimeCapabilityLocked(dynamic)
	if !rt.childControlSupported || !rt.childFanoutSupported ||
		rt.childFanoutControlMode != videoproviders.WorkerFanoutControlBranchTransactionV1 ||
		!rt.mainAwaitingKeyframe {
		t.Fatalf("dynamic capability not armed: live=%t fanout=%t awaiting=%t", rt.childControlSupported, rt.childFanoutSupported, rt.mainAwaitingKeyframe)
	}
	source, _ := (&graphService{sourceID: "front", runtime: rt}).snapshot(time.Now())
	if !source.FanoutControlSupported ||
		source.FanoutControlMode != videoproviders.WorkerFanoutControlBranchTransactionV1 {
		t.Fatalf("fan-out attestation missing from technical status: %+v", source)
	}
	if len(rt.mainCodecConfig) != 1 || !bytes.Equal(rt.mainCodecConfig[0], retainedConfig) {
		t.Fatalf("dynamic capability transition discarded current codec config: %x", rt.mainCodecConfig)
	}
	<-rt.childControl

	static := dynamic
	static.CaptureCodec = "h264"
	static.ArchiveCodec = "h264"
	static.ArchiveDelivery = "native_passthrough"
	static.LiveControlMode = "native_passthrough_static"
	static.FanoutControlMode = videoproviders.WorkerFanoutControlDisabled
	rt.applyRuntimeCapabilityLocked(static)
	if rt.childControlSupported || rt.childFanoutSupported ||
		rt.childFanoutControlMode != videoproviders.WorkerFanoutControlDisabled ||
		rt.mainAwaitingKeyframe {
		t.Fatalf("static capability left impossible epoch gate: live=%t fanout=%t awaiting=%t", rt.childControlSupported, rt.childFanoutSupported, rt.mainAwaitingKeyframe)
	}
	control := <-rt.childControl
	if !control.MainActive || control.Reason != "runtime_capability_static" {
		t.Fatalf("static source was not forced active: %+v", control)
	}
}

func TestParseVideoMainClientControlRejectsAliasesAndProfileEscalation(t *testing.T) {
	plan := videoBrokerPlan{LiveEnabled: true, MainFPS: 20, LiveBitrateKbps: 6144}
	got, err := parseVideoMainClientControl("NMC1 target_fps=10 target_bitrate_kbps=3072 request_keyframe_token=7", plan)
	if err != nil || got.TargetFPS != 10 || got.TargetBitrateKbps != 3072 || got.RequestKeyframeToken != 7 {
		t.Fatalf("valid control rejected got=%+v err=%v", got, err)
	}
	for _, invalid := range []string{
		"NMC1 target_fps=10 target_bitrate_kbps=3072",
		"NMC1 target_fps=21 target_bitrate_kbps=3072 request_keyframe_token=0",
		"NMC1 fps=10 target_bitrate_kbps=3072 request_keyframe_token=0",
		"NMC1 target_fps=10 target_bitrate_kbps=3072 legacy=1",
		"NMC1 target_fps=10 target_fps=10",
	} {
		if _, err := parseVideoMainClientControl(invalid, plan); err == nil {
			t.Fatalf("invalid control accepted: %q", invalid)
		}
	}
}

func TestBridgeRuntimeControlUsesCurrentRetryEnvelope(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer, reader := net.Pipe()
	defer reader.Close()
	controls := make(chan bridgeRuntimeControl, 1)
	rt := &videoBrokerRuntime{}
	done := make(chan struct{})
	go func() {
		rt.writeBridgeControls(ctx, writer, controls, nil)
		close(done)
	}()
	deadline := time.Unix(1_700_000_000, 0).UnixMilli()
	controls <- bridgeRuntimeControl{
		Sequence: 9, MainActive: true, MainFPS: 20, MainBitrateKbps: 4096, SubFPS: 15,
		OptionalComputeAdmitted: false,
		RetryAction:             "trial", RetryComponent: "native.jpu", RetryToken: 41, RetryDeadlineMS: deadline,
	}
	if err := reader.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 512)
	count, err := reader.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	want := "NMC2 seq=9 main_active=1 main_fps=20 main_bitrate_kbps=4096 sub_fps=15 optional_compute_admitted=0 request_keyframe_token=0 retry_action=trial retry_component=native.jpu retry_token=41 retry_deadline_unix_ms=1700000000000\n"
	if got := string(buffer[:count]); got != want {
		t.Fatalf("unexpected bridge control envelope: %q", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bridge control writer did not stop")
	}
}

func TestLiveEncoderEpochTracksActivationOnly(t *testing.T) {
	base := bridgeRuntimeControl{MainActive: true, MainFPS: 20, MainBitrateKbps: 6144}
	for _, changed := range []bridgeRuntimeControl{
		{MainActive: true, MainFPS: 10, MainBitrateKbps: 6144},
		{MainActive: true, MainFPS: 20, MainBitrateKbps: 3072},
		{MainActive: true, MainFPS: 10, MainBitrateKbps: 3072},
	} {
		if liveEncoderActivationChanged(base, changed) {
			t.Fatalf("rate-control change incorrectly created a media epoch: before=%+v after=%+v", base, changed)
		}
	}
	if liveEncoderActivationChanged(base, base) {
		t.Fatal("stable active control created a generation")
	}
	keyframeOnly := base
	keyframeOnly.RequestKeyframeToken = 7
	if liveEncoderActivationChanged(base, keyframeOnly) {
		t.Fatal("refresh-only IDR request created an encoder generation")
	}
	if liveEncoderActivationChanged(base, bridgeRuntimeControl{MainActive: false, MainFPS: 20, MainBitrateKbps: 6144}) {
		t.Fatal("deactivation armed an epoch that cannot arrive while inactive")
	}
	if !liveEncoderActivationChanged(
		bridgeRuntimeControl{MainActive: false, MainFPS: 20, MainBitrateKbps: 6144},
		base,
	) {
		t.Fatal("inactive-to-active transition did not create a media epoch")
	}
}

func TestResourceGovernorDoesNotClaimH264BeforeRequestedEncoderEpoch(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{LiveEnabled: true, MainFPS: 20, LiveBitrateKbps: 6144, SubFPS: 20},
		mainClients: map[int]*brokerClient{
			1: {mainControl: videoMainClientControl{TargetFPS: 20, TargetBitrateKbps: 6144}},
		},
		subClients:               map[int]*brokerClient{},
		childRunning:             true,
		childControlAvailable:    true,
		childControlSupported:    true,
		childLiveControlMode:     "persistent_encoder_epoch",
		observedCaptureCodec:     "h265",
		observedCaptureWidth:     1920,
		observedCaptureHeight:    1080,
		observedCaptureFPS:       30,
		observedArchiveCodec:     "h265",
		observedArchiveDelivery:  "processed_hardware_encode",
		observedArchiveFPS:       30,
		observedLiveEpoch:        1,
		mainAwaitingEncoderEpoch: true,
		governor:                 nominalResourceGovernorDecision(videoBrokerPlan{LiveEnabled: true, MainFPS: 20, LiveBitrateKbps: 6144, SubFPS: 20}),
	}
	state := rt.resourceGovernorStateLocked()
	if state.Effective.LiveActive || state.Effective.LiveFPS != 0 || state.Effective.LiveBitrateKbps != 0 {
		t.Fatalf("pending generation reported active: %+v", state.Effective)
	}
	marker := make([]byte, 8)
	binary.BigEndian.PutUint64(marker, 2)
	if err := rt.applyLiveEncoderEpoch(marker); err != nil {
		t.Fatal(err)
	}
	state = rt.resourceGovernorStateLocked()
	if !state.Effective.LiveActive || state.Effective.LiveFPS != 20 || state.Effective.LiveBitrateKbps != 6144 {
		t.Fatalf("ready generation not reported active: %+v", state.Effective)
	}
}

func TestBridgeControlAggregatesWorstViewerAndGovernorOnlyWhenSupported(t *testing.T) {
	leftServer, leftClient := net.Pipe()
	rightServer, rightClient := net.Pipe()
	defer leftServer.Close()
	defer leftClient.Close()
	defer rightServer.Close()
	defer rightClient.Close()
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{LiveEnabled: true, MainFPS: 30, LiveBitrateKbps: 8192, SubFPS: 20},
		mainClients: map[int]*brokerClient{
			1: {conn: leftServer, mainControl: videoMainClientControl{TargetFPS: 20, TargetBitrateKbps: 6144}},
			2: {conn: rightServer, mainControl: videoMainClientControl{TargetFPS: 10, TargetBitrateKbps: 3072}},
		},
		childControlSupported: true,
		governor:              resourceGovernorDecision{MainFPSCap: 15, MainBitrateKbpsCap: 4096, SubFPS: 12},
	}
	got := rt.bridgeControlTargetsLocked()
	if !got.MainActive || got.MainFPS != 10 || got.MainBitrateKbps != 3072 || got.SubFPS != 12 || got.OptionalComputeAdmitted {
		t.Fatalf("unexpected dynamic targets: %+v", got)
	}
	rt.childControlSupported = false
	got = rt.bridgeControlTargetsLocked()
	if !got.MainActive || got.MainFPS != 10 || got.MainBitrateKbps != 3072 || got.SubFPS != 20 || !got.OptionalComputeAdmitted {
		t.Fatalf("static adapter reported unapplied governor targets: %+v", got)
	}
}

func TestResourceGovernorPriorityAndHysteresis(t *testing.T) {
	plan := videoBrokerPlan{
		MainFPS: 30, LiveBitrateKbps: 8000, SubFPS: 20,
		ResourceGovernorSoftC: 88, ResourceGovernorHardC: 90, ResourceGovernorBPU: 90,
	}
	now := time.Date(2026, 8, 13, 23, 0, 0, 0, time.UTC)
	bpu := 95
	decision := decideResourceGovernor(plan, nominalResourceGovernorDecision(plan), map[string]float64{"soc": 91}, &bpu, now)
	if decision.Mode != "thermal_hard" || decision.OptionalComputeAdmitted || decision.MainFPSCap != 15 || decision.SubFPS != 10 {
		t.Fatalf("hard thermal priority drift: %+v", decision)
	}
	decision = decideResourceGovernor(plan, decision, map[string]float64{"soc": 87.5}, nil, now.Add(time.Second))
	if decision.Mode != "thermal_hard" {
		t.Fatalf("hard thermal hysteresis released too early: %+v", decision)
	}
	decision = decideResourceGovernor(plan, decision, map[string]float64{"soc": 86.5}, nil, now.Add(2*time.Second))
	if decision.Mode != "thermal_soft" || decision.OptionalComputeAdmitted || decision.SubFPS != 13 {
		t.Fatalf("soft thermal hysteresis drift: %+v", decision)
	}
	decision = decideResourceGovernor(plan, decision, map[string]float64{"soc": 84.5}, nil, now.Add(3*time.Second))
	if decision.Mode != "nominal" || !decision.OptionalComputeAdmitted {
		t.Fatalf("governor did not recover below hysteresis: %+v", decision)
	}
}

func TestResourceGovernorDefaultThermalPolicyReducesAnalysisBeforeHardLimit(t *testing.T) {
	plan := videoBrokerPlan{
		MainFPS: 30, LiveBitrateKbps: 6144, SubFPS: 30,
	}
	now := time.Date(2026, 8, 24, 12, 30, 0, 0, time.UTC)
	nominal := nominalResourceGovernorDecision(plan)

	soft := decideResourceGovernor(plan, nominal, map[string]float64{"thermal-ddr": 85}, nil, now)
	if soft.Mode != "thermal_soft" || soft.SubFPS != 20 || soft.OptionalComputeAdmitted {
		t.Fatalf("default soft thermal policy did not reduce analysis load: %+v", soft)
	}

	hard := decideResourceGovernor(plan, soft, map[string]float64{"thermal-ddr": 90}, nil, now.Add(time.Second))
	if hard.Mode != "thermal_hard" || hard.SubFPS != 15 || hard.OptionalComputeAdmitted {
		t.Fatalf("default hard thermal policy did not reduce analysis load: %+v", hard)
	}
}

func TestResourceGovernorAllowsExplicitBoardThermalHeadroomWithinCeilings(t *testing.T) {
	plan := videoBrokerPlan{
		MainFPS: 30, LiveBitrateKbps: 6144, SubFPS: 20,
		ResourceGovernorSoftC: 90, ResourceGovernorHardC: 95,
	}
	now := time.Date(2026, 8, 30, 19, 0, 0, 0, time.UTC)
	nominal := nominalResourceGovernorDecision(plan)

	below := decideResourceGovernor(plan, nominal, map[string]float64{"soc": 89.5}, nil, now)
	if below.Mode != "nominal" || below.SubFPS != 20 || !below.OptionalComputeAdmitted {
		t.Fatalf("explicit board headroom was ignored below soft limit: %+v", below)
	}
	soft := decideResourceGovernor(plan, below, map[string]float64{"soc": 90}, nil, now.Add(time.Second))
	if soft.Mode != "thermal_soft" || soft.SubFPS != 13 || soft.OptionalComputeAdmitted {
		t.Fatalf("explicit board soft limit was not enforced: %+v", soft)
	}
	hard := decideResourceGovernor(plan, soft, map[string]float64{"soc": 95}, nil, now.Add(2*time.Second))
	if hard.Mode != "thermal_hard" || hard.SubFPS != 10 || hard.OptionalComputeAdmitted {
		t.Fatalf("explicit board hard limit was not enforced: %+v", hard)
	}

	aboveCeiling := plan
	aboveCeiling.ResourceGovernorSoftC = 99
	aboveCeiling.ResourceGovernorHardC = 105
	clamped := decideResourceGovernor(
		aboveCeiling,
		nominalResourceGovernorDecision(aboveCeiling),
		map[string]float64{"soc": 95}, nil, now.Add(3*time.Second),
	)
	if clamped.Mode != "thermal_hard" {
		t.Fatalf("configured thermal ceilings were bypassed: %+v", clamped)
	}
}

func TestParseBPUUtilizationMatchesRDKSomStatusAndTakesWorstCore(t *testing.T) {
	output := []byte("bpu status information---->\n\t      min(M)\tcur(M)\tmax(M)\tratio\n\tbpu0: 500\t1000\t1000\t38\n\tbpu1: 500\t1000\t1000\t71\n")
	got := parseBPUUtilizationPercent(output)
	if got == nil || *got != 71 {
		t.Fatalf("unexpected BPU utilization: %v", got)
	}
	for _, invalid := range [][]byte{
		[]byte("bpu0: 500 1000 1000"),
		[]byte("bpu0: 500 1000 1000 101"),
		[]byte("BPU: 38"),
	} {
		if got := parseBPUUtilizationPercent(invalid); got != nil {
			t.Fatalf("invalid BPU status accepted: %q => %d", invalid, *got)
		}
	}
}

func TestResourceGovernorStateIsAtomicClosedAndTruthful(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resource-governor.json")
	state := resourceGovernorState{
		Schema: brokerResourceGovernorSchema, UpdatedAt: time.Now().UTC(), Mode: "nominal", Reason: "within_budget",
		Inputs: resourceGovernorStateInputs{ThermalZonesCelsius: map[string]float64{}, LiveClients: 0},
		Media: resourceGovernorStateMedia{
			CaptureCodec: "h265", CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
			ArchiveCodec: "h265", ArchiveDelivery: "processed_hardware_encode", ArchiveFPS: 30,
			LiveControlMode: "persistent_encoder_epoch", LiveRuntimeControl: true,
			LiveEncoderEpoch: 0,
		},
		Effective:  resourceGovernorStateOutputs{CaptureFPS: 30, ArchiveFPS: 30, AnalysisFPS: 20, OptionalComputeAdmitted: true},
		Priorities: []string{"capture", "archive", "analysis", "live", "optional_compute"}, ArchiveProtected: true,
	}
	if err := writeResourceGovernorState(path, state); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("unexpected state metadata info=%v err=%v", info, err)
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 9 || decoded["schema"] != brokerResourceGovernorSchema || decoded["archive_protected"] != true {
		t.Fatalf("unexpected closed state: %+v", decoded)
	}
}

func TestResourceGovernorStateNeverReportsPlannedFramesAsObserved(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan:        videoBrokerPlan{LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true, CaptureFPS: 30, MainFPS: 20, LiveBitrateKbps: 6144, SubFPS: 20},
		mainClients: map[int]*brokerClient{1: {}}, subClients: map[int]*brokerClient{},
		childControlSupported: true,
		governor:              nominalResourceGovernorDecision(videoBrokerPlan{LiveEnabled: true, MainFPS: 20, LiveBitrateKbps: 6144, SubFPS: 20}),
	}
	state := rt.resourceGovernorStateLocked()
	if state.Media.CaptureFPS != 0 || state.Media.ArchiveFPS != 0 ||
		state.Effective.CaptureFPS != 0 || state.Effective.ArchiveFPS != 0 ||
		state.Effective.AnalysisFPS != 0 || state.Effective.LiveActive ||
		state.Effective.OptionalComputeAdmitted || state.Mode != "initializing" ||
		state.Reason != "media_capability_unattested" || state.Inputs.ThermalZonesCelsius == nil {
		t.Fatalf("stopped/unattested bridge reported effective media: %+v", state)
	}
	rt.childRunning = true
	rt.observedCaptureFPS = 30
	rt.observedArchiveFPS = 30
	rt.observedCaptureCodec = "h265"
	rt.observedArchiveCodec = "h265"
	rt.observedArchiveDelivery = "processed_hardware_encode"
	rt.childLiveControlMode = "persistent_encoder_epoch"
	rt.observedLiveEpoch = 1
	state = rt.resourceGovernorStateLocked()
	if state.Effective.CaptureFPS != 30 || state.Effective.ArchiveFPS != 30 ||
		state.Effective.AnalysisFPS != 20 || !state.Effective.LiveActive ||
		state.Effective.LiveFPS != 20 || state.Effective.LiveBitrateKbps != 6144 ||
		!state.Effective.OptionalComputeAdmitted || state.Mode != "nominal" {
		t.Fatalf("attested running bridge telemetry mismatch: %+v", state)
	}
}

func TestResourceGovernorStateAttestsEnabledStreamRolesIndependently(t *testing.T) {
	base := func(plan videoBrokerPlan) *videoBrokerRuntime {
		return &videoBrokerRuntime{
			plan:                  plan,
			mainClients:           map[int]*brokerClient{},
			subClients:            map[int]*brokerClient{},
			childRunning:          true,
			observedCaptureCodec:  "h265",
			observedCaptureWidth:  1920,
			observedCaptureHeight: 1080,
			observedCaptureFPS:    30,
			governor:              nominalResourceGovernorDecision(plan),
		}
	}

	analysis := base(videoBrokerPlan{AnalysisEnabled: true, CaptureFPS: 30, SubFPS: 20})
	state := analysis.resourceGovernorStateLocked()
	if state.Mode != "nominal" || state.Reason != "within_budget" || state.Effective.CaptureFPS != 30 ||
		state.Effective.AnalysisFPS != 20 || !state.Effective.OptionalComputeAdmitted {
		t.Fatalf("analysis-only source remained unattested: %+v", state)
	}

	archive := base(videoBrokerPlan{ArchiveEnabled: true, CaptureFPS: 30, ArchiveFPS: 15})
	archive.observedArchiveCodec = "h265"
	archive.observedArchiveDelivery = "processed_hardware_encode"
	archive.observedArchiveFPS = 15
	state = archive.resourceGovernorStateLocked()
	if state.Mode != "nominal" || state.Effective.CaptureFPS != 30 || state.Effective.ArchiveFPS != 15 ||
		state.Effective.AnalysisFPS != 0 {
		t.Fatalf("archive-only source remained unattested: %+v", state)
	}

	live := base(videoBrokerPlan{LiveEnabled: true, CaptureFPS: 30, MainFPS: 30, LiveBitrateKbps: 4096})
	live.childLiveControlMode = "native_passthrough_static"
	state = live.resourceGovernorStateLocked()
	if state.Mode != "nominal" || state.Effective.CaptureFPS != 30 || !state.Effective.LiveActive ||
		state.Effective.LiveFPS != 30 || state.Effective.LiveBitrateKbps != 4096 || state.Effective.AnalysisFPS != 0 {
		t.Fatalf("live-only source remained unattested: %+v", state)
	}

	incompleteArchive := base(videoBrokerPlan{ArchiveEnabled: true, CaptureFPS: 30, ArchiveFPS: 15})
	state = incompleteArchive.resourceGovernorStateLocked()
	if state.Mode != "initializing" || state.Reason != "media_capability_unattested" || state.Effective.CaptureFPS != 0 {
		t.Fatalf("missing enabled archive evidence was accepted: %+v", state)
	}

	incompleteLive := base(videoBrokerPlan{LiveEnabled: true, CaptureFPS: 30, MainFPS: 30, LiveBitrateKbps: 4096})
	state = incompleteLive.resourceGovernorStateLocked()
	if state.Mode != "initializing" || state.Reason != "media_capability_unattested" || state.Effective.LiveActive {
		t.Fatalf("missing enabled live evidence was accepted: %+v", state)
	}
}

func TestResetResourceGovernorStateFileInvalidatesPreviousGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resource-governor.json")
	if err := os.WriteFile(path, []byte("stale\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	resetResourceGovernorStateFile(videoBrokerPlan{ResourceGovernorEnabled: true, ResourceGovernorStatePath: path})
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("previous governor generation survived reset: %v", err)
	}
}
