package broker

import (
	"testing"
	"time"

	"github.com/Josepavese/videosluice/capability"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

func TestSlowCameraAdmissionPreservesAllHardwareGates(t *testing.T) {
	bootstrap := []byte{0, 0, 0, 1, 32 << 1, 1, 0, 0, 0, 1, 33 << 1, 1,
		0, 0, 0, 1, 34 << 1, 1, 0, 0, 0, 1, 19 << 1, 1}
	for _, format := range []string{"mjpeg", "h265", "yuyv"} {
		for _, requested := range []int{25, 30} {
			for _, actual := range []int{1, 10} {
				rt := &videoBrokerRuntime{
					plan: videoBrokerPlan{LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true,
						MainFormat: "h265", MainFPS: requested, SubFPS: 20, ArchiveFPS: 15, RequireHWCodec: true},
					cameraAdmission: videoproviders.CameraAdmissionDecision{DevicePath: "/test/selected-source",
						Format: format, CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: requested},
					observedCaptureCodec: format, observedCaptureWidth: 1280, observedCaptureHeight: 720, observedCaptureFPS: requested,
					capabilityPlan: capability.Plan{Stages: []capability.StageSelection{
						{Stage: capability.StageDecode, Role: "capture", Hardware: true, State: capability.StateCandidate},
						{Stage: capability.StageEncode, Role: "live", Hardware: true, State: capability.StateCandidate},
						{Stage: capability.StageResize, Role: "analysis", Hardware: true, State: capability.StateCandidate},
						{Stage: capability.StageToneMap, Role: "processed", Hardware: true, State: capability.StateCandidate},
					}},
				}
				now := time.Now().Add(-2 * time.Second / time.Duration(actual))
				for frame := 0; frame < 3; frame++ {
					at := now.Add(time.Duration(frame) * time.Second / time.Duration(actual))
					rt.observeCameraAdmissionFrameLocked(brokerStreamH264, bootstrap, at)
					rt.observeCameraAdmissionFrameLocked(brokerStreamNV12, []byte{1}, at)
					rt.observeCameraAdmissionFrameLocked(brokerStreamH265, bootstrap, at)
				}
				if !rt.cameraAdmitted || len(rt.captureModeFailures) != 0 {
					t.Fatalf("valid slow source rejected: format=%s requested=%d actual=%d", format, requested, actual)
				}
				for index := range rt.capabilityPlan.Stages {
					if err, ready := sourceMoveHardwareReadyLocked(rt); err != nil || ready {
						t.Fatalf("unattested hardware stage %d was bypassed: ready=%t err=%v", index, ready, err)
					}
					rt.capabilityPlan.Stages[index].State = capability.StateAttested
				}
				if err, ready := sourceMoveHardwareReadyLocked(rt); err != nil || !ready {
					t.Fatalf("slow admitted hardware source remained blocked: ready=%t err=%v", ready, err)
				}
			}
		}
	}
}

func TestStreamPerformanceSeparatesTargetMeasurementAndAvailability(t *testing.T) {
	for _, requested := range []int{1, 25, 30} {
		rt := &videoBrokerRuntime{plan: videoBrokerPlan{LiveEnabled: true, MainFPS: requested}}
		watch := &brokerStreamCadenceWatch{role: "live"}
		watches := map[string]*brokerStreamCadenceWatch{"live": watch}
		now := time.Unix(1_780_000_000, 0)
		var frame uint32
		step := func(frames uint32) StreamPerformance {
			now = now.Add(2 * time.Second)
			frame += frames
			watch.observe(now, frame, requested, time.Second, 4*time.Second, 1, 42)
			rt.applyStreamCadenceHealthLocked(watches, 4*time.Second)
			return rt.streamPerformance["live"]
		}
		step(0)
		actual := uint32(10)
		if requested == 1 {
			actual = 1
		}
		step(2 * actual)
		item := step(2 * actual)
		want := "reduced"
		if requested == 1 {
			want = "on_target"
		}
		if item.State != want || item.RequestedFPS != requested || item.TargetFPS != requested || item.ObservedFPS != float64(actual) || item.WindowMS != 2000 {
			t.Fatalf("requested=%d performance=%+v", requested, item)
		}
		if len(rt.captureModeFailures) != 0 || len(rt.captureModeExclusions) != 0 {
			t.Fatal("performance warning changed camera quarantine")
		}
		if requested == 1 {
			continue
		}
		if got := step(uint32(2 * requested)); got.State != "reduced" {
			t.Fatalf("warning cleared before stable recovery: %+v", got)
		}
		if got := step(2 * actual); got.State != "reduced" {
			t.Fatalf("warning flickered after brief recovery: %+v", got)
		}
		step(uint32(2 * requested))
		if got := step(uint32(2 * requested)); got.State != "on_target" || rt.cadenceDegraded {
			t.Fatalf("sustained recovery did not clear warning: %+v", got)
		}
	}
}

func TestCameraAdmissionFrameWindowRequiresProgressAndFreshness(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		frames      uint64
		first, last time.Time
		ready       bool
	}{
		{3, now.Add(-2 * time.Second), now, true},
		{3, now.Add(-8 * time.Second), now, true},
		{2, now.Add(-time.Second), now, false},
		{3, now, now, false},
		{30, now.Add(-15 * time.Second), now.Add(-12 * time.Second), false},
	} {
		if got := cameraAdmissionFramesReady(tc.frames, tc.first, tc.last, now, 12*time.Second); got != tc.ready {
			t.Fatalf("frame window %+v ready=%t", tc, got)
		}
	}
}
