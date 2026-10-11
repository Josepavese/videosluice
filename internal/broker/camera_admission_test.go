package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/capability"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

func TestNativeCompressedCameraAdmissionAttestsDecoderForLiveOnlySourceMove(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			LiveEnabled: true, MainFormat: "h265", MainFPS: 25,
			Transport: videoproviders.TransportV4L2UVC,
		},
		cameraAdmission: videoproviders.CameraAdmissionDecision{
			DevicePath: "/dev/nemesi-video-sources/front", Format: "h265",
			CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 25,
		},
		observedCaptureCodec: "h265", observedCaptureWidth: 1280,
		observedCaptureHeight: 720, observedCaptureFPS: 25,
		capabilityPlan: capability.Plan{
			WorkerProvider: "native_sp",
			Stages: []capability.StageSelection{{
				Stage: capability.StageDecode, Role: "capture", Format: "h265",
				Component: "native.vpu", Backend: "native_sp", Hardware: true,
				State: capability.StateCandidate,
			}},
		},
	}
	startedAt := time.Now()
	rt.observeCameraAdmissionFrameLocked(brokerStreamH264, []byte{
		0, 0, 0, 1, 32 << 1, 1,
		0, 0, 0, 1, 33 << 1, 1,
		0, 0, 0, 1, 34 << 1, 1,
		0, 0, 0, 1, 19 << 1, 1,
	}, startedAt)
	for index := 1; index < 25; index++ {
		rt.observeCameraAdmissionFrameLocked(
			brokerStreamH264,
			[]byte{0, 0, 0, 1, 1 << 1, 1},
			startedAt.Add(time.Duration(index)*time.Second/25),
		)
	}
	if !rt.cameraAdmitted {
		t.Fatal("native H265 live-only source did not complete camera admission")
	}
	if got := rt.capabilityPlan.Stages[0].State; got != capability.StateAttested {
		t.Fatalf("native H265 decoder remained %q after decoded camera admission", got)
	}
	if err, ready := sourceMoveHardwareReadyLocked(rt); err != nil || !ready {
		t.Fatalf("admitted live-only source move remained blocked: ready=%t err=%v", ready, err)
	}
}

func TestCompressedCameraAdmissionDoesNotForgePortableOrMJPEGDecoderEvidence(t *testing.T) {
	for _, test := range []struct {
		name, worker, format string
	}{
		{name: "portable-h265", worker: "gstreamer", format: "h265"},
		{name: "native-mjpeg", worker: "native_sp", format: "mjpeg"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rt := &videoBrokerRuntime{
				plan: videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC},
				capabilityPlan: capability.Plan{
					WorkerProvider: test.worker,
					Stages: []capability.StageSelection{{
						Stage: capability.StageDecode, Role: "capture", Format: test.format,
						Component: "candidate-decoder", Backend: test.worker,
						Hardware: true, State: capability.StateCandidate,
					}},
				},
			}
			rt.attestNativeCompressedCameraDecoderLocked(test.format)
			if got := rt.capabilityPlan.Stages[0].State; got != capability.StateCandidate {
				t.Fatalf("decoder evidence was forged for worker=%s format=%s: %s", test.worker, test.format, got)
			}
		})
	}
}

func TestObserveCameraH26xNALsRequiresCompleteH265Bootstrap(t *testing.T) {
	tracker := cameraAdmissionRuntimeTracker{}
	observeCameraH26xNALs(&tracker, "h265", []byte{
		0, 0, 0, 1, 32 << 1, 1,
		0, 0, 1, 33 << 1, 1,
	})
	if !tracker.SeenVPS || !tracker.SeenSPS || tracker.SeenPPS || tracker.SeenKeyframe {
		t.Fatalf("unexpected partial NAL state: %+v", tracker)
	}
	observeCameraH26xNALs(&tracker, "h265", []byte{
		0, 0, 0, 1, 34 << 1, 1,
		0, 0, 1, 19 << 1, 1,
	})
	if !tracker.SeenVPS || !tracker.SeenSPS || !tracker.SeenPPS || !tracker.SeenKeyframe {
		t.Fatalf("complete H265 bootstrap was not recognized: %+v", tracker)
	}
}

func TestLiveH265AdmissionRequiresVPSWhileExplicitH264DoesNot(t *testing.T) {
	tracker := &cameraAdmissionRuntimeTracker{
		LiveSeenSPS: true, LiveSeenPPS: true, LiveSeenKeyframe: true,
	}
	if liveCodecBootstrapReady("h265", tracker) {
		t.Fatal("H265 live admission accepted a bootstrap without VPS")
	}
	tracker.LiveSeenVPS = true
	if !liveCodecBootstrapReady("h265", tracker) {
		t.Fatal("complete H265 live bootstrap was rejected")
	}
	tracker.LiveSeenVPS = false
	if !liveCodecBootstrapReady("h264", tracker) {
		t.Fatal("explicit H264 live bootstrap incorrectly required VPS")
	}
}

func TestCameraAdmissionPublishesOnlyAfterRuntimeContract(t *testing.T) {
	stateRoot := t.TempDir()
	statePath := filepath.Join(stateRoot, "camera-admission.json")
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			AnalysisEnabled:          true,
			ArchiveEnabled:           true,
			Transport:                videoproviders.TransportV4L2UVC,
			CaptureCodecPreference:   "h265",
			RequireHWCodec:           true,
			CaptureWidth:             1920,
			CaptureHeight:            1080,
			ArchiveWidth:             1920,
			ArchiveHeight:            1080,
			ArchiveWidthAlignment:    16,
			ArchiveHeightAlignment:   8,
			SubFPS:                   5,
			StateDirectory:           stateRoot,
			CameraAdmissionStatePath: statePath,
		},
		cameraAdmission: videoproviders.CameraAdmissionDecision{
			PreferredDevicePath:   "/dev/video0",
			DevicePath:            "/dev/video2",
			CapabilityFingerprint: "cap-v1:1234",
			Reason:                "h265_native_compressed_supersample",
			Format:                "h265",
			CodecPolicy:           "required",
			AspectPolicy:          "crop",
			CaptureWidth:          1920,
			CaptureHeight:         1080,
			CaptureFPS:            30,
		},
		observedCaptureCodec:    "h265",
		observedCaptureWidth:    1920,
		observedCaptureHeight:   1080,
		observedCaptureFPS:      30,
		observedArchiveCodec:    "h265",
		observedArchiveDelivery: "processed_hardware_encode",
		childStartBackoff:       12 * time.Second,
		nextChildStartAt:        time.Now().Add(time.Minute),
	}
	t.Cleanup(func() { joinCameraWrites(t, rt) })
	modeKey := cameraAdmissionModeKey(rt.cameraAdmission)
	rt.modeHealthFingerprint = cameraModeRuntimeFingerprint(rt.plan)
	rt.captureModeFailures = map[string]int{modeKey: cameraModeFailureThreshold}
	rt.captureModeHealthEntries = map[string]cameraModeHealthEntry{
		modeKey: {
			Mode: videoproviders.V4L2CaptureModeExclusion{
				CapabilityFingerprint: rt.cameraAdmission.CapabilityFingerprint,
				PixelFormat:           rt.cameraAdmission.Format,
				Width:                 rt.cameraAdmission.CaptureWidth,
				Height:                rt.cameraAdmission.CaptureHeight,
				FPS:                   rt.cameraAdmission.CaptureFPS,
			},
			Failures: cameraModeFailureThreshold, RetryAfter: time.Now().Add(cameraModeQuarantineBase),
		},
	}
	rt.refreshCameraModeHealthLocked(time.Now())
	now := time.Now()
	rt.observeCameraAdmissionFrameLocked(brokerStreamH265, []byte{
		0, 0, 0, 1, 32 << 1, 1,
		0, 0, 0, 1, 33 << 1, 1,
		0, 0, 0, 1, 34 << 1, 1,
		0, 0, 0, 1, 19 << 1, 1,
	}, now)
	for index := 1; index < 30; index++ {
		rt.observeCameraAdmissionFrameLocked(
			brokerStreamH265,
			[]byte{0, 0, 0, 1, 1 << 1, 1},
			now.Add(time.Duration(index)*time.Second/30),
		)
	}
	for index := 0; index < 5; index++ {
		rt.observeCameraAdmissionFrameLocked(brokerStreamNV12, []byte{1}, now.Add(time.Duration(index)*200*time.Millisecond))
	}
	if !rt.cameraAdmitted {
		t.Fatal("runtime contract did not complete camera admission")
	}
	if rt.childStartBackoff != 0 || !rt.nextChildStartAt.IsZero() {
		t.Fatalf("healthy camera admission did not reset process backoff: backoff=%s retry_at=%s", rt.childStartBackoff, rt.nextChildStartAt)
	}
	joinCameraWrites(t, rt)
	body, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var state cameraAdmissionState
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatal(err)
	}
	if state.Schema != cameraAdmissionSchema || state.Status != "admitted" ||
		state.Discovery.SelectedDevice != "/dev/video2" ||
		state.Discovery.SelectionReason != "h265_native_compressed_supersample" ||
		state.Contract.ArchiveWidth != 1920 || state.Contract.ArchiveHeight != 1080 ||
		state.Contract.ArchiveWidthAlignment != 16 || state.Contract.ArchiveHeightAlignment != 8 ||
		!state.Runtime.SeenVPS || !state.Runtime.SeenKeyframe {
		t.Fatalf("unexpected persisted admission state: %+v", state)
	}
	healthBody, err := os.ReadFile(filepath.Join(stateRoot, cameraStateDirectoryName, cameraModeHealthStateFilename))
	if err != nil {
		t.Fatal(err)
	}
	var health cameraModeHealthState
	if err := json.Unmarshal(healthBody, &health); err != nil || health.Schema != cameraModeHealthSchema ||
		len(health.Entries) != 1 || health.Entries[0].Failures != 1 ||
		!health.Entries[0].RetryAfter.IsZero() || !health.Entries[0].StableAfter.After(health.UpdatedAt) ||
		rt.captureModeFailures[modeKey] != 1 || len(rt.captureModeExclusions) != 0 {
		t.Fatalf("successful half-open admission did not retain bounded probation err=%v health=%+v failures=%v exclusions=%v", err, health, rt.captureModeFailures, rt.captureModeExclusions)
	}
}

func TestCameraAdmissionDoesNotWaitForDormantRuntimeControlledLiveStream(t *testing.T) {
	newRuntime := func() *videoBrokerRuntime {
		return &videoBrokerRuntime{
			plan: videoBrokerPlan{
				LiveEnabled: true, MainFormat: "h265", MainFPS: 15,
				AnalysisEnabled: true, SubFPS: 15,
				ArchiveEnabled: true,
				Transport:      videoproviders.TransportV4L2UVC,
			},
			cameraAdmission: videoproviders.CameraAdmissionDecision{
				DevicePath: "/dev/nemesi-video-sources/front", Format: "yuyv",
				CaptureWidth: 640, CaptureHeight: 360, CaptureFPS: 30,
			},
			observedCaptureCodec:  "yuyv",
			observedCaptureWidth:  640,
			observedCaptureHeight: 360,
			observedCaptureFPS:    30,
			childControlSupported: true,
			mainClients:           make(map[int]*brokerClient),
		}
	}
	feedActiveRoles := func(rt *videoBrokerRuntime, startedAt time.Time) {
		rt.observeCameraAdmissionFrameLocked(brokerStreamH265, []byte{
			0, 0, 0, 1, 32 << 1, 1,
			0, 0, 0, 1, 33 << 1, 1,
			0, 0, 0, 1, 34 << 1, 1,
			0, 0, 0, 1, 19 << 1, 1,
		}, startedAt)
		for index := 1; index < 30; index++ {
			rt.observeCameraAdmissionFrameLocked(
				brokerStreamH265,
				[]byte{0, 0, 0, 1, 1 << 1, 1},
				startedAt.Add(time.Duration(index)*time.Second/30),
			)
		}
		for index := 0; index < 15; index++ {
			rt.observeCameraAdmissionFrameLocked(
				brokerStreamNV12,
				[]byte{1},
				startedAt.Add(time.Duration(index)*time.Second/15),
			)
		}
	}

	now := time.Now()
	dormant := newRuntime()
	feedActiveRoles(dormant, now)
	if !dormant.cameraAdmitted {
		t.Fatal("configured runtime-controlled live stream without clients blocked source admission")
	}

	demanded := newRuntime()
	demanded.mainClients[1] = &brokerClient{}
	feedActiveRoles(demanded, now)
	if demanded.cameraAdmitted {
		t.Fatal("source admission ignored a demanded live stream without codec bootstrap")
	}
	demanded.observeCameraAdmissionFrameLocked(brokerStreamH264, []byte{
		0, 0, 0, 1, 32 << 1, 1,
		0, 0, 0, 1, 33 << 1, 1,
		0, 0, 0, 1, 34 << 1, 1,
		0, 0, 0, 1, 19 << 1, 1,
	}, now)
	for index := 1; index < 15; index++ {
		demanded.observeCameraAdmissionFrameLocked(
			brokerStreamH264,
			[]byte{0, 0, 0, 1, 1 << 1, 1},
			now.Add(time.Duration(index)*time.Second/15),
		)
	}
	if !demanded.cameraAdmitted {
		t.Fatal("demanded live stream did not complete source admission after H265 bootstrap and cadence")
	}
}

func TestCameraAdmissionHonorsIndependentArchiveCadence(t *testing.T) {
	now := time.Now()
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			AnalysisEnabled: true,
			SubFPS:          20,
			ArchiveEnabled:  true,
			ArchiveFPS:      15,
			Transport:       videoproviders.TransportV4L2UVC,
		},
		cameraAdmission: videoproviders.CameraAdmissionDecision{
			DevicePath: "/dev/nemesi-video-sources/front", Format: "h265",
			CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
		},
		observedCaptureCodec:    "h265",
		observedCaptureWidth:    1920,
		observedCaptureHeight:   1080,
		observedCaptureFPS:      30,
		observedArchiveCodec:    "h265",
		observedArchiveDelivery: "processed_hardware_encode",
		observedArchiveFPS:      15,
	}

	for index := 0; index < 20; index++ {
		rt.observeCameraAdmissionFrameLocked(
			brokerStreamNV12,
			[]byte{1},
			now.Add(time.Duration(index)*time.Second/20),
		)
	}
	rt.observeCameraAdmissionFrameLocked(brokerStreamH265, []byte{
		0, 0, 0, 1, 32 << 1, 1,
		0, 0, 0, 1, 33 << 1, 1,
		0, 0, 0, 1, 34 << 1, 1,
		0, 0, 0, 1, 19 << 1, 1,
	}, now)
	for index := 1; index < 15; index++ {
		rt.observeCameraAdmissionFrameLocked(
			brokerStreamH265,
			[]byte{0, 0, 0, 1, 1 << 1, 1},
			now.Add(time.Duration(index)*time.Second/15),
		)
	}

	if !rt.cameraAdmitted {
		t.Fatalf(
			"independent archive cadence blocked admission capture=%d archive_target=%d archive_observed=%.2f analysis_observed=%.2f",
			rt.cameraAdmission.CaptureFPS,
			rt.plan.ArchiveFPS,
			rt.observedAdmissionArchiveFPSLocked(),
			rt.observedAdmissionSubFPSLocked(),
		)
	}
}

func TestCameraAdmissionRequiresFreshCompleteMediaRegardlessOfFPS(t *testing.T) {
	now := time.Now()
	newRuntime := func() *videoBrokerRuntime {
		return &videoBrokerRuntime{
			plan: videoBrokerPlan{LiveEnabled: true, MainFormat: "h264", AnalysisEnabled: true, ArchiveEnabled: true},
			cameraAdmission: videoproviders.CameraAdmissionDecision{
				DevicePath: "/dev/video0", Format: "mjpeg",
				CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 30,
			},
			cameraAdmissionTracker: cameraAdmissionRuntimeTracker{
				FirstLiveFrameAt: now.Add(-9 * time.Second), LastLiveFrameAt: now,
				FirstSubFrameAt: now.Add(-9 * time.Second), LastSubFrameAt: now,
				FirstArchiveFrameAt: now.Add(-9 * time.Second), LastArchiveFrameAt: now,
				LiveFrames: 10, LiveSeenSPS: true, LiveSeenPPS: true, LiveSeenKeyframe: true,
				SubFrames: 10, ArchiveFrames: 10, ArchiveCodec: "h265",
				SeenVPS: true, SeenSPS: true, SeenPPS: true, SeenKeyframe: true,
			},
			observedCaptureCodec: "mjpeg", observedCaptureWidth: 1280,
			observedCaptureHeight: 720, observedCaptureFPS: 30,
			lastFrameAt: now, lastSubFrameAt: now, lastArchiveFrameAt: now,
		}
	}

	live := newRuntime()
	live.maybeCompleteCameraAdmissionLocked()
	if !live.cameraAdmitted {
		t.Fatal("fresh complete 1 fps producer was not admitted")
	}

	stale := newRuntime()
	stale.cameraAdmissionTracker.LastArchiveFrameAt = now.Add(-brokerDefaultFrameWatchdog)
	stale.maybeCompleteCameraAdmissionLocked()
	if stale.cameraAdmitted {
		t.Fatal("stale producer was admitted")
	}

	incomplete := newRuntime()
	incomplete.cameraAdmissionTracker.SeenPPS = false
	incomplete.maybeCompleteCameraAdmissionLocked()
	if incomplete.cameraAdmitted {
		t.Fatal("producer without complete codec bootstrap was admitted")
	}

	dormant := newRuntime()
	dormant.childControlSupported = true
	dormant.mainClients = make(map[int]*brokerClient)
	dormant.cameraAdmissionTracker.LiveFrames = 0
	dormant.cameraAdmissionTracker.LiveSeenSPS = false
	dormant.cameraAdmissionTracker.LiveSeenPPS = false
	dormant.cameraAdmissionTracker.LiveSeenKeyframe = false
	dormant.lastFrameAt = time.Time{}
	dormant.maybeCompleteCameraAdmissionLocked()
	if !dormant.cameraAdmitted {
		t.Fatal("dormant runtime-controlled live role blocked fresh archive and analysis evidence")
	}
}

func TestCameraModeProbationSurvivesAdmissionAndExpiresAfterHealthyWindow(t *testing.T) {
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "mjpeg",
		CaptureWidth:          1280,
		CaptureHeight:         720,
		CaptureFPS:            30,
	}
	key := cameraAdmissionModeKey(decision)
	now := time.Now()
	rt := &videoBrokerRuntime{
		plan:                     videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC},
		captureModeFailures:      map[string]int{key: 1},
		captureModeHealthEntries: map[string]cameraModeHealthEntry{},
	}
	rt.noteCameraModeRuntimeAdmissionLocked(decision, now)
	rt.cameraAdmission = decision
	rt.cameraAdmitted = true
	entry, ok := rt.captureModeHealthEntries[key]
	if !ok || entry.Failures != 1 || !entry.RetryAfter.IsZero() ||
		entry.StableAfter.Sub(now) != cameraModeProbationHealthyWindow {
		t.Fatalf("admission erased or extended probation incorrectly: %+v", rt.captureModeHealthEntries)
	}
	if changed := rt.refreshCameraModeHealthLocked(entry.StableAfter.Add(-time.Nanosecond)); changed ||
		rt.captureModeFailures[key] != 1 {
		t.Fatalf("probation expired before the healthy window: changed=%t failures=%v", changed, rt.captureModeFailures)
	}
	if changed := rt.refreshCameraModeHealthLocked(entry.StableAfter); !changed ||
		len(rt.captureModeHealthEntries) != 0 || len(rt.captureModeFailures) != 0 || len(rt.captureModeExclusions) != 0 {
		t.Fatalf("healthy window did not clear probation: changed=%t entries=%v failures=%v exclusions=%v", changed, rt.captureModeHealthEntries, rt.captureModeFailures, rt.captureModeExclusions)
	}
}

func TestCameraModeProbationRequiresFreshHealthyWindowAfterBrokerRestart(t *testing.T) {
	stateRoot := t.TempDir()
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "mjpeg",
		CaptureWidth:          1280,
		CaptureHeight:         720,
		CaptureFPS:            30,
	}
	key := cameraAdmissionModeKey(decision)
	plan := videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC, StateDirectory: stateRoot}
	fingerprint := cameraModeRuntimeFingerprint(plan)
	now := time.Now()
	updatedAt := now.Add(-2 * cameraModeProbationHealthyWindow)
	if err := persistCameraModeHealthEntries(stateRoot, fingerprint, map[string]cameraModeHealthEntry{
		key: {
			Mode: cameraModeExclusionForDecision(decision), Failures: 1,
			StableAfter: updatedAt.Add(cameraModeProbationHealthyWindow),
		},
	}, updatedAt); err != nil {
		t.Fatal(err)
	}
	loaded := loadCameraModeHealthEntries(stateRoot, fingerprint, now)
	if len(loaded) != 1 {
		t.Fatalf("restart discarded unobserved probation based on wall clock: %+v", loaded)
	}
	rt := &videoBrokerRuntime{
		plan:                     plan,
		cameraAdmission:          decision,
		cameraAdmitted:           true,
		captureModeHealthEntries: loaded,
		captureModeFailures:      map[string]int{key: 1},
	}
	rt.noteCameraModeRuntimeAdmissionLocked(decision, now)
	entry := rt.captureModeHealthEntries[key]
	if entry.StableAfter.Sub(now) != cameraModeProbationHealthyWindow {
		t.Fatalf("fresh admission did not restart healthy probation window: %+v", entry)
	}
}

func TestCameraAdmissionPersistsCapabilityFittedArchiveGeometry(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "camera-admission.json")
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			CaptureWidth: 1920, CaptureHeight: 1080,
			ArchiveWidth: 1920, ArchiveHeight: 1080,
			CameraAdmissionStatePath: statePath,
		},
		cameraAdmission: videoproviders.CameraAdmissionDecision{
			PreferredDevicePath: "/dev/video0", DevicePath: "/dev/video0",
			Format: "mjpeg", CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 30,
		},
		activeCaptureWidth:      1280,
		activeCaptureHeight:     720,
		observedArchiveDelivery: "processed_hardware_encode",
	}
	t.Cleanup(func() { joinCameraWrites(t, rt) })
	rt.writeCameraAdmissionStateLocked("probing", "runtime_attestation_pending")
	joinCameraWrites(t, rt)
	body, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var state cameraAdmissionState
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatal(err)
	}
	if state.Contract.ArchiveWidth != 1280 || state.Contract.ArchiveHeight != 720 {
		t.Fatalf("archive geometry was upscaled beyond capture: %+v", state.Contract)
	}
}

func TestCameraModeRuntimeFailureQuarantinesAndPersistsAfterRepeatedProof(t *testing.T) {
	stateRoot := t.TempDir()
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "h265",
		CaptureWidth:          2560,
		CaptureHeight:         1440,
		CaptureFPS:            30,
	}
	rt := &videoBrokerRuntime{
		plan:                videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC, StateDirectory: stateRoot},
		cameraAdmission:     decision,
		captureModeFailures: make(map[string]int),
	}
	t.Cleanup(func() { joinCameraWrites(t, rt) })
	reason := "runtime_attestation_timeout_after_20.0s"
	rt.noteCameraModeRuntimeFailureLocked(reason)
	if len(rt.captureModeExclusions) != 0 {
		t.Fatalf("one bounded runtime failure quarantined a mode: %+v", rt.captureModeExclusions)
	}
	joinCameraWrites(t, rt)
	probation := loadCameraModeHealthEntries(stateRoot, rt.modeHealthFingerprint, time.Now())
	if entry, ok := probation[cameraAdmissionModeKey(decision)]; len(probation) != 1 || !ok ||
		entry.Failures != 1 || !entry.RetryAfter.IsZero() || !entry.StableAfter.After(time.Now()) {
		t.Fatalf("first runtime failure did not persist bounded probation: %+v", probation)
	}
	rt.noteCameraModeRuntimeFailureLocked(reason)
	if len(rt.captureModeExclusions) != 1 ||
		!cameraModeExclusionMatchesDecision(rt.captureModeExclusions[0], decision) {
		t.Fatalf("repeated runtime proof did not quarantine exact mode: %+v", rt.captureModeExclusions)
	}
	joinCameraWrites(t, rt)
	loaded := loadCameraModeHealthEntries(stateRoot, rt.modeHealthFingerprint, time.Now())
	entry, ok := loaded[cameraAdmissionModeKey(decision)]
	if len(loaded) != 1 || !ok || !cameraModeExclusionMatchesDecision(entry.Mode, decision) ||
		entry.Failures != cameraModeFailureThreshold || !entry.RetryAfter.After(time.Now()) {
		t.Fatalf("persisted fingerprint-bound mode evidence was not reusable: %+v", loaded)
	}
	body, err := os.ReadFile(filepath.Join(stateRoot, cameraStateDirectoryName, cameraModeHealthStateFilename))
	if err != nil {
		t.Fatal(err)
	}
	var state cameraModeHealthState
	if err := json.Unmarshal(body, &state); err != nil || state.Schema != cameraModeHealthSchema ||
		state.RuntimeFingerprint != rt.modeHealthFingerprint {
		t.Fatalf("invalid camera mode health state err=%v state=%+v", err, state)
	}
}

func TestStartupUVCStreamOnFailureRetriesSameModeBeforeQuarantine(t *testing.T) {
	now := time.Now()
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "mjpeg",
		CaptureWidth:          1280,
		CaptureHeight:         720,
		CaptureFPS:            30,
	}
	rt := &videoBrokerRuntime{
		plan:                videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC},
		cameraAdmission:     decision,
		captureModeFailures: make(map[string]int),
		childStartedAt:      now.Add(-time.Second),
		mediaEpoch:          1,
	}
	rt.handleUVCBridgeFailure("VIDIOC_STREAMON failed: Protocol error")
	if len(rt.captureModeFailures) != 0 || len(rt.captureModeExclusions) != 0 {
		t.Fatalf("producer handoff poisoned native modes failures=%+v exclusions=%+v", rt.captureModeFailures, rt.captureModeExclusions)
	}
	if rt.uvcHandoffFailures != 1 || !rt.nextChildStartAt.After(now) {
		t.Fatalf("handoff retry was not scheduled: failures=%d retry_at=%s", rt.uvcHandoffFailures, rt.nextChildStartAt)
	}
	firstRetry := rt.nextChildStartAt
	rt.handleUVCBridgeFailure("VIDIOC_STREAMON failed: Protocol error")
	if rt.uvcHandoffFailures != 1 || !rt.nextChildStartAt.Equal(firstRetry) {
		t.Fatalf("duplicate child telemetry scheduled another retry: failures=%d retry_at=%s", rt.uvcHandoffFailures, rt.nextChildStartAt)
	}
}

func TestPreviouslyAdmittedUVCStreamOnFailureRetriesSameModeAfterRuntimeRecovery(t *testing.T) {
	now := time.Now()
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "mjpeg",
		CaptureWidth:          1280,
		CaptureHeight:         720,
		CaptureFPS:            30,
	}
	modeKey := cameraAdmissionModeKey(decision)
	rt := &videoBrokerRuntime{
		plan:                videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC},
		cameraAdmission:     decision,
		captureModeFailures: make(map[string]int),
		everProducedFrames:  true,
		mediaEpoch:          8,
		uvcAdmittedModes:    map[string]struct{}{modeKey: {}},
	}
	rt.handleUVCBridgeFailure("VIDIOC_STREAMON failed: Protocol error")
	if len(rt.captureModeFailures) != 0 || len(rt.captureModeExclusions) != 0 {
		t.Fatalf("reopen poisoned an admitted native mode failures=%+v exclusions=%+v", rt.captureModeFailures, rt.captureModeExclusions)
	}
	if rt.uvcHandoffFailures != 1 || !rt.nextChildStartAt.After(now) {
		t.Fatalf("admitted-mode reopen retry was not scheduled: failures=%d retry_at=%s", rt.uvcHandoffFailures, rt.nextChildStartAt)
	}
}

func TestUnprovenUVCStreamOnFailureUsesModeCircuitBreakerAfterRuntime(t *testing.T) {
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "mjpeg",
		CaptureWidth:          960,
		CaptureHeight:         540,
		CaptureFPS:            30,
	}
	rt := &videoBrokerRuntime{
		plan:                videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC},
		cameraAdmission:     decision,
		captureModeFailures: make(map[string]int),
		everProducedFrames:  true,
		mediaEpoch:          9,
		uvcAdmittedModes:    make(map[string]struct{}),
	}
	rt.handleUVCBridgeFailure("VIDIOC_STREAMON failed: Protocol error")
	rt.mediaEpoch++
	rt.handleUVCBridgeFailure("VIDIOC_STREAMON failed: Protocol error")
	if len(rt.captureModeExclusions) != 1 || rt.uvcHandoffFailures != 0 {
		t.Fatalf("unproven runtime mode bypassed circuit breaker exclusions=%+v handoff_failures=%d", rt.captureModeExclusions, rt.uvcHandoffFailures)
	}
}

func TestStartupUVCHandoffBudgetFallsBackToModeCircuitBreaker(t *testing.T) {
	now := time.Now()
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "mjpeg",
		CaptureWidth:          1280,
		CaptureHeight:         720,
		CaptureFPS:            30,
	}
	rt := &videoBrokerRuntime{
		plan:                videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC},
		cameraAdmission:     decision,
		captureModeFailures: make(map[string]int),
		uvcHandoffStartedAt: now.Add(-brokerUVCHandoffRetryBudget),
		childStartedAt:      now.Add(-time.Second),
		mediaEpoch:          1,
	}
	modeKey := cameraAdmissionModeKey(decision)
	rt.handleUVCBridgeFailure("VIDIOC_STREAMON failed: Protocol error")
	rt.handleUVCBridgeFailure("VIDIOC_STREAMON failed: Protocol error")
	if got := rt.captureModeFailures[modeKey]; got != 1 || len(rt.captureModeExclusions) != 0 {
		t.Fatalf("duplicate terminal line counted twice failures=%d exclusions=%+v", got, rt.captureModeExclusions)
	}
	rt.childStartedAt = now
	rt.mediaEpoch = 2
	rt.handleUVCBridgeFailure("VIDIOC_STREAMON failed: Protocol error")
	if len(rt.captureModeExclusions) != 1 {
		t.Fatalf("expired handoff budget did not restore bounded mode quarantine: %+v", rt.captureModeExclusions)
	}
}

func TestUVCQuietIntervalStartsAfterChildWaitBoundary(t *testing.T) {
	failureAt := time.Now()
	closedAt := failureAt.Add(11 * time.Second)
	delay := brokerUVCHandoffRetryInitial
	rt := &videoBrokerRuntime{
		mediaEpoch:           7,
		nextChildStartAt:     failureAt.Add(delay),
		uvcHandoffQuietEpoch: 7,
		uvcHandoffQuietDelay: delay,
	}
	rt.reanchorUVCQuietAfterChildLocked(closedAt, 7, "uvc_streamon_failed")
	if want := closedAt.Add(delay); !rt.nextChildStartAt.Equal(want) {
		t.Fatalf("quiet interval was not anchored to child close: got=%s want=%s", rt.nextChildStartAt, want)
	}
	if rt.uvcHandoffQuietEpoch != 0 || rt.uvcHandoffQuietDelay != 0 {
		t.Fatalf("quiet re-anchor token was not consumed: epoch=%d delay=%s", rt.uvcHandoffQuietEpoch, rt.uvcHandoffQuietDelay)
	}
	anchored := rt.nextChildStartAt
	rt.reanchorUVCQuietAfterChildLocked(closedAt.Add(time.Second), 7, "uvc_streamon_failed")
	if !rt.nextChildStartAt.Equal(anchored) {
		t.Fatal("duplicate finish extended an already anchored quiet interval")
	}
}

func TestCameraModeHealthEvidenceExpires(t *testing.T) {
	stateRoot := t.TempDir()
	fingerprint := cameraModeRuntimeFingerprint(videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC})
	exclusion := videoproviders.V4L2CaptureModeExclusion{
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		PixelFormat:           "h265",
		Width:                 2560,
		Height:                1440,
		FPS:                   30,
	}
	key := cameraModeExclusionKey(exclusion)
	updatedAt := time.Now().Add(-cameraModeHealthEvidenceTTL - time.Minute)
	if err := persistCameraModeHealthEntries(stateRoot, fingerprint, map[string]cameraModeHealthEntry{
		key: {
			Mode: exclusion, Failures: cameraModeFailureThreshold,
			RetryAfter: updatedAt.Add(time.Minute),
		},
	}, updatedAt); err != nil {
		t.Fatal(err)
	}
	if got := loadCameraModeHealthEntries(stateRoot, fingerprint, time.Now()); len(got) != 0 {
		t.Fatalf("expired runtime evidence remained active: %+v", got)
	}
}

func TestCameraModeQuarantineAutomaticallyEntersHalfOpenState(t *testing.T) {
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "mjpeg",
		CaptureWidth:          1280,
		CaptureHeight:         720,
		CaptureFPS:            30,
	}
	key := cameraAdmissionModeKey(decision)
	retryAfter := time.Now().Add(time.Minute)
	rt := &videoBrokerRuntime{
		captureModeHealthEntries: map[string]cameraModeHealthEntry{
			key: {
				Mode: videoproviders.V4L2CaptureModeExclusion{
					CapabilityFingerprint: decision.CapabilityFingerprint,
					PixelFormat:           decision.Format,
					Width:                 decision.CaptureWidth,
					Height:                decision.CaptureHeight,
					FPS:                   decision.CaptureFPS,
				},
				Failures: cameraModeFailureThreshold, RetryAfter: retryAfter,
			},
		},
		captureModeFailures: make(map[string]int),
	}
	rt.refreshCameraModeHealthLocked(retryAfter.Add(-time.Second))
	if len(rt.captureModeExclusions) != 1 {
		t.Fatalf("active quarantine did not exclude the mode: %+v", rt.captureModeExclusions)
	}
	rt.refreshCameraModeHealthLocked(retryAfter)
	if len(rt.captureModeExclusions) != 0 || rt.captureModeFailures[key] != cameraModeFailureThreshold {
		t.Fatalf("expired quarantine did not become half-open exclusions=%+v failures=%v", rt.captureModeExclusions, rt.captureModeFailures)
	}
	rt.refreshCameraModeHealthLocked(retryAfter.Add(cameraModeProbationHealthyWindow + time.Hour))
	if _, ok := rt.captureModeHealthEntries[key]; !ok || rt.captureModeFailures[key] != cameraModeFailureThreshold {
		t.Fatalf("untried half-open mode lost failure evidence entries=%v failures=%v", rt.captureModeHealthEntries, rt.captureModeFailures)
	}
}

func TestCameraModeQuarantineBackoffIsBounded(t *testing.T) {
	if got := cameraModeQuarantineDuration(cameraModeFailureThreshold); got != cameraModeQuarantineBase {
		t.Fatalf("initial quarantine=%s want=%s", got, cameraModeQuarantineBase)
	}
	if got := cameraModeQuarantineDuration(cameraModeFailureThreshold + 1); got != 2*cameraModeQuarantineBase {
		t.Fatalf("second quarantine=%s want=%s", got, 2*cameraModeQuarantineBase)
	}
	if got := cameraModeQuarantineDuration(cameraModeFailureThreshold + 100); got != cameraModeQuarantineMaximum {
		t.Fatalf("quarantine cap=%s want=%s", got, cameraModeQuarantineMaximum)
	}
}

func TestCameraModeHealthTrialIsReadOnly(t *testing.T) {
	stateRoot := t.TempDir()
	t.Setenv(cameraModeHealthReadOnlyEnv, "1")
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "mjpeg",
		CaptureWidth:          1280,
		CaptureHeight:         720,
		CaptureFPS:            30,
	}
	rt := &videoBrokerRuntime{
		plan:                videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC, StateDirectory: stateRoot},
		cameraAdmission:     decision,
		captureModeFailures: map[string]int{cameraAdmissionModeKey(decision): cameraModeFailureThreshold - 1},
	}
	rt.noteCameraModeRuntimeFailureLocked("uvc_streamon_failed")
	if len(rt.captureModeExclusions) != 1 {
		t.Fatalf("trial did not retain in-memory exclusion: %+v", rt.captureModeExclusions)
	}
	if _, err := os.Stat(filepath.Join(stateRoot, cameraStateDirectoryName, cameraModeHealthStateFilename)); !os.IsNotExist(err) {
		t.Fatalf("read-only trial persisted mode health err=%v", err)
	}
}

func TestCameraModeRuntimeFailureDoesNotInventV4L2ModesForSensorStack(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			Transport:      videoproviders.TransportVendorHBN,
			StateDirectory: t.TempDir(),
		},
		cameraAdmission: videoproviders.CameraAdmissionDecision{
			DevicePath:            "vendor_hbn:imx219:0",
			CapabilityFingerprint: "cap-v1:1234567890abcdef",
			Format:                "nv12",
			CaptureWidth:          1920,
			CaptureHeight:         1080,
			CaptureFPS:            30,
		},
		captureModeFailures: make(map[string]int),
	}
	rt.noteCameraModeRuntimeFailureLocked("runtime_attestation_timeout_after_20.0s")
	rt.noteCameraModeRuntimeFailureLocked("runtime_attestation_timeout_after_20.0s")
	if len(rt.captureModeFailures) != 0 || len(rt.captureModeExclusions) != 0 {
		t.Fatalf("sensor-stack admission polluted V4L2 mode health: failures=%v exclusions=%v", rt.captureModeFailures, rt.captureModeExclusions)
	}
}

func TestCameraModeHealthRequiresCurrentRuntimeFingerprint(t *testing.T) {
	stateRoot := t.TempDir()
	dir := filepath.Join(stateRoot, cameraStateDirectoryName)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(cameraModeHealthState{
		Schema:    cameraModeHealthSchema,
		UpdatedAt: time.Now().UTC(),
		Entries: []cameraModeHealthEntry{{
			Mode: videoproviders.V4L2CaptureModeExclusion{
				CapabilityFingerprint: "cap-v1:1234567890abcdef",
				PixelFormat:           "mjpeg",
				Width:                 1280,
				Height:                720,
				FPS:                   30,
			},
			Failures:   cameraModeFailureThreshold,
			RetryAfter: time.Now().Add(cameraModeQuarantineBase),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, cameraModeHealthStateFilename), []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
	fingerprint := cameraModeRuntimeFingerprint(videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC})
	if got := loadCameraModeHealthEntries(stateRoot, fingerprint, time.Now()); len(got) != 0 {
		t.Fatalf("unbound mode evidence remained authoritative: %+v", got)
	}
}

func TestCameraModeHealthRejectsPreviousCadenceRestartSchema(t *testing.T) {
	stateRoot := t.TempDir()
	dir := filepath.Join(stateRoot, cameraStateDirectoryName)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	plan := videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC}
	fingerprint := cameraModeRuntimeFingerprint(plan)
	body, err := json.Marshal(cameraModeHealthState{
		Schema:             "video-broker.camera-mode-health.v8",
		RuntimeFingerprint: fingerprint,
		UpdatedAt:          time.Now().UTC(),
		Entries: []cameraModeHealthEntry{{
			Mode: videoproviders.V4L2CaptureModeExclusion{
				CapabilityFingerprint: "cap-v1:1234567890abcdef",
				PixelFormat:           "mjpeg",
				Width:                 1280,
				Height:                720,
				FPS:                   30,
			},
			Failures:   cameraModeFailureThreshold,
			RetryAfter: time.Now().Add(cameraModeQuarantineBase),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, cameraModeHealthStateFilename), body, 0o640); err != nil {
		t.Fatal(err)
	}
	if got := loadCameraModeHealthEntries(stateRoot, fingerprint, time.Now()); len(got) != 0 {
		t.Fatalf("obsolete cadence-restart evidence remained authoritative: %+v", got)
	}
}

func TestTransientCadenceDegradationDoesNotPoisonCameraModeHealth(t *testing.T) {
	decision := videoproviders.CameraAdmissionDecision{
		DevicePath:            "/dev/video0",
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		Format:                "mjpeg",
		CaptureWidth:          1280,
		CaptureHeight:         720,
		CaptureFPS:            30,
	}
	rt := &videoBrokerRuntime{
		plan:                videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC},
		cameraAdmission:     decision,
		captureModeFailures: make(map[string]int),
	}
	rt.noteCameraModeRuntimeFailureLocked("sub_cadence_low_fps_12.75_threshold_16.50_for_12.0s")
	if len(rt.captureModeFailures) != 0 || len(rt.captureModeExclusions) != 0 {
		t.Fatalf("transient cadence poisoned native mode health failures=%+v exclusions=%+v", rt.captureModeFailures, rt.captureModeExclusions)
	}
}

func TestCameraModeHealthRejectsEvidenceFromDifferentVideoGraph(t *testing.T) {
	stateRoot := t.TempDir()
	base := videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC, MainWidth: 1280, MainHeight: 720}
	baseFingerprint := cameraModeRuntimeFingerprint(base)
	exclusion := videoproviders.V4L2CaptureModeExclusion{
		CapabilityFingerprint: "cap-v1:1234567890abcdef",
		PixelFormat:           "mjpeg",
		Width:                 1280,
		Height:                720,
		FPS:                   30,
	}
	key := cameraModeExclusionKey(exclusion)
	if err := persistCameraModeHealthEntries(stateRoot, baseFingerprint, map[string]cameraModeHealthEntry{
		key: {
			Mode: exclusion, Failures: cameraModeFailureThreshold,
			RetryAfter: time.Now().Add(cameraModeQuarantineBase),
		},
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	changed := base
	changed.MainWidth = 1920
	changedFingerprint := cameraModeRuntimeFingerprint(changed)
	if baseFingerprint == changedFingerprint {
		t.Fatal("semantic graph change did not change runtime fingerprint")
	}
	if got := loadCameraModeHealthEntries(stateRoot, changedFingerprint, time.Now()); len(got) != 0 {
		t.Fatalf("evidence from a different video graph remained authoritative: %+v", got)
	}
}

func TestCameraModeRuntimeFingerprintIgnoresOperationalPathsAndTrialReadOnlyMode(t *testing.T) {
	engineDirectory := t.TempDir()
	base := videoBrokerPlan{
		Transport:                 videoproviders.TransportV4L2UVC,
		EngineDirectory:           engineDirectory,
		MainSocketPath:            "/run/video-broker/live.sock",
		SubShmPath:                "/run/video-broker/analysis.shm",
		CaptureDevice:             "/dev/video0",
		StateDirectory:            "/var/lib/video-broker",
		QualityMonitorStatePath:   "/run/video-broker/quality.json",
		CameraAdmissionStatePath:  "/run/video-broker/admission.json",
		ResourceGovernorStatePath: "/run/video-broker/governor.json",
	}
	baseFingerprint := cameraModeRuntimeFingerprint(base)
	t.Setenv(cameraModeHealthReadOnlyEnv, "1")
	trial := base
	trial.MainSocketPath = "/run/video-broker-trial/live.sock"
	trial.SubShmPath = "/run/video-broker-trial/analysis.shm"
	trial.CaptureDevice = "/dev/video8"
	trial.StateDirectory = "/tmp/video-broker-trial"
	trial.QualityMonitorStatePath = "/tmp/video-broker-trial/quality.json"
	trial.CameraAdmissionStatePath = "/tmp/video-broker-trial/admission.json"
	trial.ResourceGovernorStatePath = "/tmp/video-broker-trial/governor.json"
	if got := cameraModeRuntimeFingerprint(trial); got != baseFingerprint {
		t.Fatalf("operational path or read-only trial changed video graph fingerprint got=%s want=%s", got, baseFingerprint)
	}
}

func TestCameraModeRuntimeFingerprintBindsCanonicalSourceIdentity(t *testing.T) {
	base := videoBrokerPlan{
		SourceIdentity: "uvc.front-camera",
		Transport:      videoproviders.TransportV4L2UVC,
		CaptureDevice:  "/dev/nemesi-video-main",
		CaptureFormat:  "mjpeg",
	}
	replacement := base
	replacement.SourceIdentity = "uvc.replacement-camera"
	if cameraModeRuntimeFingerprint(base) == cameraModeRuntimeFingerprint(replacement) {
		t.Fatal("physical source identity change reused the previous mode-health fingerprint")
	}
	reenumerated := base
	reenumerated.CaptureDevice = "/dev/video17"
	if cameraModeRuntimeFingerprint(base) != cameraModeRuntimeFingerprint(reenumerated) {
		t.Fatal("operational node re-enumeration changed an identity-bound mode-health fingerprint")
	}
}

func TestCameraModeRuntimeFingerprintTracksNativeWorkerContent(t *testing.T) {
	engineDirectory := t.TempDir()
	workerPath := filepath.Join(engineDirectory, "rdkx5_v4l2_mux_bridge")
	if err := os.WriteFile(workerPath, []byte("worker-v1"), 0o750); err != nil {
		t.Fatal(err)
	}
	plan := videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC, EngineDirectory: engineDirectory}
	before := cameraModeRuntimeFingerprint(plan)
	if err := os.WriteFile(workerPath, []byte("worker-v2"), 0o750); err != nil {
		t.Fatal(err)
	}
	after := cameraModeRuntimeFingerprint(plan)
	if before == after {
		t.Fatal("native worker change did not change runtime fingerprint")
	}
}

func TestCameraModeHealthUsesDedicatedPersistentCameraControl(t *testing.T) {
	stateRoot := t.TempDir()
	want := filepath.Join(stateRoot, cameraStateDirectoryName, cameraModeHealthStateFilename)
	if got := cameraModeHealthStatePath(stateRoot); got != want {
		t.Fatalf("camera mode health path=%q want=%q", got, want)
	}
	fingerprint := cameraModeRuntimeFingerprint(videoBrokerPlan{Transport: videoproviders.TransportV4L2UVC})
	if err := persistCameraModeHealthEntries(stateRoot, fingerprint, nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Dir(want))
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o750 {
		t.Fatalf("camera CameraControl directory mode=%v", info.Mode())
	}
}
