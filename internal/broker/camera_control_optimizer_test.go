package broker

import (
	"context"
	"errors"
	"github.com/Josepavese/videosluice/internal/cameracontrol"
	"github.com/Josepavese/videosluice/quality"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fixedCameraControlSampler struct {
	sample cameraQualitySample
}

func (s fixedCameraControlSampler) Name() string { return "fixed" }

func (s fixedCameraControlSampler) SampleQuality(context.Context, string, int, int) (cameraQualitySample, error) {
	return s.sample, nil
}

func writeOptimizerQualityState(t *testing.T, path string, timestamp time.Time, source quality.Metrics) {
	t.Helper()
	source = withOptimizerSpatial(source)
	processed := source
	analysis := source
	analysis.Width, analysis.Height = 640, 360
	state := quality.State{
		Schema: quality.Schema, TimestampUnixMS: timestamp.UnixMilli(), Frame: 42, SampleStride: 8,
		Source: source, Processed: processed, Analysis: analysis,
		Capture: quality.Capture{Format: "H265", Orientation: "normal"},
	}
	if err := quality.WriteFile(path, state); err != nil {
		t.Fatal(err)
	}
}

func withOptimizerSpatial(metrics quality.Metrics) quality.Metrics {
	regions := make([]quality.Region, 0, quality.SpatialRegionCount)
	base := metrics.Samples / quality.SpatialRegionCount
	remainder := metrics.Samples % quality.SpatialRegionCount
	for index := 0; index < quality.SpatialRegionCount; index++ {
		samples := base
		if uint64(index) < remainder {
			samples++
		}
		region := quality.Region{X: index % quality.SpatialColumns, Y: index / quality.SpatialColumns, Samples: samples}
		if samples > 0 {
			region.Luma = metrics.Luma
			region.ContentLuma = metrics.ContentLuma
			region.LaplacianVariance = metrics.LaplacianVariance
			region.Edge = metrics.Edge
			region.BlackRatio = metrics.BlackRatio
			region.WhiteRatio = metrics.WhiteRatio
			region.SignalInformative = metrics.SignalInformative
		}
		regions = append(regions, region)
	}
	metrics.Spatial = quality.Spatial{Columns: quality.SpatialColumns, Rows: quality.SpatialRows, Regions: regions}
	return metrics
}

func TestQualityFileSamplerReadsFreshSourceMetrics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quality.json")
	writeOptimizerQualityState(t, path, time.Now(), quality.Metrics{
		Width: 1920, Height: 1080, Samples: 123, Luma: 44.4, ContentLuma: 48.1,
		LaplacianVariance: 27.8, Edge: 2.2, BlackRatio: 0.125, WhiteRatio: 0.025,
		SignalInformative: true,
	})
	sampler := &qualityFileSampler{path: path, maxAge: time.Minute}

	sample, err := sampler.SampleQuality(context.Background(), "/dev/video0", 320, 180)
	if err != nil {
		t.Fatalf("SampleQuality failed: %v", err)
	}
	if sample.Luma != 44 || sample.LapVar != 27.8 || sample.Edge != 2.2 || sample.BlackRatio != 0.125 || sample.WhiteRatio != 0.025 || !sample.SignalInformative || sample.CaptureFormat != "h265" || sample.Source != path || len(sample.Regions) != quality.SpatialRegionCount {
		t.Fatalf("unexpected quality sample: %+v", sample)
	}
}

func TestQualityFileSamplerFencesDetachedAndPreviousAnalysisBinding(t *testing.T) {
	now := time.Now().Truncate(time.Millisecond)
	path := filepath.Join(t.TempDir(), "quality.json")
	metrics := quality.Metrics{Width: 1920, Height: 1080, Samples: 123, Luma: 44, ContentLuma: 48, SignalInformative: true}
	writeOptimizerQualityState(t, path, now, metrics)
	runtime := &videoBrokerRuntime{plan: videoBrokerPlan{QualityMonitorEnabled: true, AnalysisEnabled: true}, childRunning: true, childStartedAt: now.Add(-time.Minute)}
	sampler := &qualityFileSampler{path: path, maxAge: time.Minute, inputCurrent: runtime.qualityInputCurrent}
	if _, err := sampler.SampleQuality(context.Background(), "", 0, 0); err != nil {
		t.Fatal(err)
	}
	runtime.plan.AnalysisEnabled = false
	if _, err := sampler.SampleQuality(context.Background(), "", 0, 0); !errors.Is(err, errCameraSampleNotReady) {
		t.Fatalf("detached analysis could drive the controller: %v", err)
	}
	runtime.plan.AnalysisEnabled = true
	runtime.qualityInputSince = now.Add(time.Millisecond)
	if _, err := sampler.SampleQuality(context.Background(), "", 0, 0); !errors.Is(err, errCameraSampleNotReady) {
		t.Fatalf("rebinding accepted the previous quality file: %v", err)
	}
	writeOptimizerQualityState(t, path, now.Add(2*time.Millisecond), metrics)
	if _, err := sampler.SampleQuality(context.Background(), "", 0, 0); err != nil {
		t.Fatalf("fresh sample did not resume the configured controller: %v", err)
	}
}

func TestQualityFileSamplerNeverClosesTheLoopOnProcessedMetrics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quality.json")
	source := quality.Metrics{
		Width: 1920, Height: 1080, Samples: 123, Luma: 22, ContentLuma: 58,
		LaplacianVariance: 18, Edge: 2, BlackRatio: 0.72, SignalInformative: true,
	}
	source = withOptimizerSpatial(source)
	processed := source
	processed.Luma, processed.ContentLuma, processed.BlackRatio = 128, 132, 0.01
	analysis := processed
	analysis.Width, analysis.Height = 640, 360
	if err := quality.WriteFile(path, quality.State{
		Schema: quality.Schema, TimestampUnixMS: time.Now().UnixMilli(), Frame: 42, SampleStride: 8,
		Source: source, Processed: processed, Analysis: analysis,
		Capture: quality.Capture{Format: "h265", Orientation: "normal"},
	}); err != nil {
		t.Fatal(err)
	}
	sampler := &qualityFileSampler{path: path, maxAge: time.Minute}
	sample, err := sampler.SampleQuality(context.Background(), "/dev/video0", 320, 180)
	if err != nil {
		t.Fatal(err)
	}
	if sample.Luma != 22 || sample.BlackRatio != 0.72 {
		t.Fatalf("camera feedback used the processed domain: %+v", sample)
	}
}

func TestCameraControlOptimizerSuppressesLiveControlsOnlyForCompressedBrokerCapture(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		inPipelineToneMapping:  true,
		controlSuppressed:      map[string]string{},
		manualPrepared:         map[string]bool{"/dev/video0": true},
		recoveryCandidate:      map[string]string{},
		recoveryCandidateCount: map[string]int{},
	}
	for _, format := range []string{"h264", "H.265", "HEVC", "MJPG", "av1"} {
		if !optimizer.updateHardwareControlSuppression("/dev/video0", cameraQualitySample{CaptureFormat: format}, 128) {
			t.Fatalf("compressed format %q must suppress live UVC controls", format)
		}
	}
	if optimizer.updateHardwareControlSuppression("/dev/video0", cameraQualitySample{CaptureFormat: "YUYV"}, 128) {
		t.Fatal("raw YUYV capture must retain hardware control feedback")
	}
	if optimizer.manualPrepared["/dev/video0"] {
		t.Fatal("a compressed-to-raw transition must re-prepare explicit manual mode")
	}
	if !optimizer.updateHardwareControlSuppression("/dev/video0", cameraQualitySample{}, 128) {
		t.Fatal("missing format evidence must fail safe without live UVC controls")
	}

	optimizer.inPipelineToneMapping = false
	if optimizer.updateHardwareControlSuppression("/dev/video0", cameraQualitySample{CaptureFormat: "h265"}, 128) {
		t.Fatal("compressed capture without an in-pipeline CameraControl owner must retain its configured control policy")
	}
}

func TestCompressedBrokerCaptureAdmitsStableBlackFloorRecovery(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		inPipelineToneMapping:  true,
		targetLuma:             128,
		hysteresis:             12,
		stableSamples:          2,
		controlSuppressed:      map[string]string{},
		recoveryCandidate:      map[string]string{},
		recoveryCandidateCount: map[string]int{},
	}
	sample := cameraQualitySample{
		CaptureFormat: "h265", Luma: 22, BlackRatio: 0.72,
		LapVar: 18, Edge: 2, SignalInformative: true,
	}
	if !optimizer.updateHardwareControlSuppression("/dev/video0", sample, 128) ||
		!optimizer.updateHardwareControlSuppression("/dev/video0", sample, 128) {
		t.Fatal("compressed recovery must wait for stable evidence")
	}
	if optimizer.updateHardwareControlSuppression("/dev/video0", sample, 128) {
		t.Fatal("stable informative black-floor clipping must admit bounded sensor recovery")
	}
	if !optimizer.updateHardwareControlSuppression("/dev/video0", sample, 128) {
		t.Fatal("a second sensor adjustment must require a fresh stable-evidence window")
	}
}

func TestCompressedBrokerCaptureAdmitsStableZonalHighlightRecovery(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		inPipelineToneMapping: true, targetLuma: 128, hysteresis: 12, stableSamples: 2,
		controlSuppressed: map[string]string{}, recoveryCandidate: map[string]string{},
		recoveryCandidateCount: map[string]int{},
	}
	sample := cameraQualitySample{
		CaptureFormat: "mjpeg", Luma: 130, WhiteRatio: 0.02, SignalInformative: true,
		Regions: []cameraQualityRegion{{X: 3, Y: 0, Samples: 100, Luma: 250, WhiteRatio: 0.55}},
	}
	if !optimizer.updateHardwareControlSuppression("/dev/video0", sample, 128) ||
		!optimizer.updateHardwareControlSuppression("/dev/video0", sample, 128) {
		t.Fatal("zonal highlight recovery must wait for stable evidence")
	}
	if optimizer.updateHardwareControlSuppression("/dev/video0", sample, 128) {
		t.Fatal("stable zonal clipping must admit one bounded residual adjustment")
	}
}

func TestZonalHighlightPressurePreservesDetailBelowGlobalTarget(t *testing.T) {
	sample := cameraQualitySample{
		Luma: 105, WhiteRatio: 0.02, SignalInformative: true,
		Regions: []cameraQualityRegion{{Samples: 100, Luma: 250, WhiteRatio: 0.55}},
	}
	if brighten, adjust := desiredCameraControlDirection(sample, 128, 12); adjust || brighten {
		t.Fatalf("zonal clipping below target must be held for local PAL lift: brighten=%t adjust=%t", brighten, adjust)
	}
	sample.Luma = 130
	if brighten, adjust := desiredCameraControlDirection(sample, 128, 12); !adjust || brighten {
		t.Fatalf("near-target zonal clipping must request darkening: brighten=%t adjust=%t", brighten, adjust)
	}
}

func TestCompressedBrokerCaptureDoesNotChasePhotonlessNight(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		inPipelineToneMapping: true, targetLuma: 128, hysteresis: 12,
		controlSuppressed: map[string]string{}, recoveryCandidate: map[string]string{},
		recoveryCandidateCount: map[string]int{},
	}
	sample := cameraQualitySample{CaptureFormat: "h265", Luma: 1, BlackRatio: 1, SignalInformative: false}
	for index := 0; index < 5; index++ {
		if !optimizer.updateHardwareControlSuppression("/dev/video0", sample, 128) {
			t.Fatal("uninformative black frame must not drive sensor gain")
		}
	}
}

func TestCompressedBrokerCaptureNeverReachesHardwareControlProbe(t *testing.T) {
	probeCalls := 0
	optimizer := &cameraControlOptimizer{
		inPipelineToneMapping: true,
		deferHardwareProbe:    true,
		controlBackend: cameracontrol.NewV4L2Backend(func(context.Context, ...string) ([]byte, error) {
			probeCalls++
			return nil, errors.New("hardware probe must not run")
		}),
		targets:           []cameraControlTarget{{sampleDevice: "/run/video-broker/video-quality.json", controlDevice: "/dev/video0", sampleCandidates: []string{"/run/video-broker/video-quality.json"}}},
		sampler:           fixedCameraControlSampler{sample: cameraQualitySample{Luma: 72, CaptureFormat: "h265", Source: "/run/video-broker/video-quality.json"}},
		reportEvery:       10,
		activeSample:      map[string]string{},
		attempts:          map[string]int{},
		failed:            map[string]int{},
		failures:          map[string]int{},
		skipUntil:         map[string]time.Time{},
		controlSuppressed: map[string]string{},
		controlQualified:  map[string]bool{},
		manualPrepared:    map[string]bool{},
		controlFailures:   map[string]int{},
		controlSkipUntil:  map[string]time.Time{},
	}

	optimizer.optimizeTarget(context.Background(), optimizer.targets[0])

	if probeCalls != 0 {
		t.Fatalf("compressed capture reached hardware control probe %d times", probeCalls)
	}
	if optimizer.controlSuppressed["/dev/video0"] != "h265" {
		t.Fatalf("compressed control suppression was not retained: %+v", optimizer.controlSuppressed)
	}
}

func TestQualityFileSamplerAcceptsBlackFrameWithSamples(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quality.json")
	writeOptimizerQualityState(t, path, time.Now(), quality.Metrics{
		Width: 1920, Height: 1080, Samples: 123, BlackRatio: 1,
	})
	sampler := &qualityFileSampler{path: path, maxAge: time.Minute}

	sample, err := sampler.SampleQuality(context.Background(), "/dev/video0", 320, 180)
	if err != nil {
		t.Fatalf("SampleQuality failed: %v", err)
	}
	if sample.Luma != 1 || sample.SignalInformative || sample.BlackRatio != 1.0 {
		t.Fatalf("unexpected black-frame luma: %+v", sample)
	}
}

func TestQualityFileSamplerRejectsStaleMetrics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quality.json")
	writeOptimizerQualityState(t, path, time.Now().Add(-2*time.Minute), quality.Metrics{
		Width: 1920, Height: 1080, Samples: 123, Luma: 70, ContentLuma: 70,
		SignalInformative: true,
	})
	sampler := &qualityFileSampler{path: path, maxAge: time.Second}

	if _, err := sampler.SampleQuality(context.Background(), "/dev/video0", 320, 180); !errors.Is(err, errCameraSampleNotReady) {
		t.Fatalf("expected stale quality metrics to be not ready, got %v", err)
	}
}

func TestQualityFileSamplerRejectsFutureMetrics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quality.json")
	writeOptimizerQualityState(t, path, time.Now().Add(time.Minute), quality.Metrics{
		Width: 1920, Height: 1080, Samples: 1, Luma: 70, ContentLuma: 70,
		SignalInformative: true,
	})
	sampler := &qualityFileSampler{path: path, maxAge: time.Minute}

	if _, err := sampler.SampleQuality(context.Background(), "/dev/video0", 320, 180); err == nil {
		t.Fatal("expected future quality metrics to be rejected")
	}
}

func TestQualityFileSamplerMissingFileIsNotReady(t *testing.T) {
	sampler := &qualityFileSampler{path: filepath.Join(t.TempDir(), "missing.json"), maxAge: time.Minute}

	if _, err := sampler.SampleQuality(context.Background(), "/dev/video0", 320, 180); !errors.Is(err, errCameraSampleNotReady) {
		t.Fatalf("expected missing quality metrics to be not ready, got %v", err)
	}
}

func TestCameraControlOptimizerBacksOffAfterControlTimeout(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		controlFailures:  map[string]int{},
		controlSkipUntil: map[string]time.Time{},
	}

	optimizer.handleControlReadFailure("/dev/video0", context.DeadlineExceeded)

	if !optimizer.shouldSkipControlRead("/dev/video0") {
		t.Fatalf("expected V4L2 control read backoff after timeout")
	}
	if optimizer.controlFailures["/dev/video0"] != 1 {
		t.Fatalf("unexpected failure count: %d", optimizer.controlFailures["/dev/video0"])
	}

	optimizer.handleControlReadSuccess("/dev/video0")
	if optimizer.shouldSkipControlRead("/dev/video0") {
		t.Fatalf("expected control read backoff to clear after success")
	}
	if _, ok := optimizer.controlFailures["/dev/video0"]; ok {
		t.Fatalf("expected failure count to clear after success")
	}
}

func TestResolveCameraControlOptimizerRuntimeEnablesSharedUVCQualityMetric(t *testing.T) {
	opt := engineConfig{
		VideoTransport:                     "v4l2_uvc",
		Enabled:                            true,
		VideoCaptureFPS:                    30,
		VideoQualityMonitorReportEvery:     900,
		CameraControlOptimizerPolicy:       "auto",
		CameraControlOptimizerMode:         "profiles",
		CameraControlOptimizerMetricSource: "auto",
	}
	got := resolveCameraControlOptimizerRuntime(opt)
	if got.CameraControlOptimizerPolicy != "off" {
		t.Fatalf("UVC hardware-control optimizer must be disabled: %+v", got)
	}
	if !got.VideoQualityMonitorEnabled || got.VideoQualityMonitorReportEvery != 30 {
		t.Fatalf("UVC CameraControl must use a current broker quality metric: %+v", got)
	}
	if got.VideoQualityMonitorStatePath != "/run/video-broker/video-quality.json" || got.CameraControlOptimizerMetricSource != "quality_file" {
		t.Fatalf("unexpected UVC CameraControl metric route: %+v", got)
	}

	opt.VideoTransport = "v4l2"
	opt.CameraControlOptimizerPolicy = "auto"
	got = resolveCameraControlOptimizerRuntime(opt)
	if got.CameraControlOptimizerPolicy != "off" {
		t.Fatalf("generic V4L2 hardware-control optimizer must be disabled: %+v", got)
	}

	opt.VideoTransport = "v4l2_uvc"
	opt.CameraControlOptimizerPolicy = "off"
	got = resolveCameraControlOptimizerRuntime(opt)
	if got.VideoQualityMonitorEnabled {
		t.Fatalf("explicit CameraControl off must remain authoritative: %+v", got)
	}
}

func TestResolveCameraControlOptimizerRuntimeLeavesNativeTransportUntouched(t *testing.T) {
	opt := engineConfig{
		VideoTransport:                     "vendor_hbn",
		Enabled:                            true,
		CameraControlOptimizerPolicy:       "auto",
		CameraControlOptimizerMode:         "profiles",
		CameraControlOptimizerMetricSource: "auto",
	}
	got := resolveCameraControlOptimizerRuntime(opt)
	if got.VideoQualityMonitorEnabled || got.CameraControlOptimizerMetricSource != "auto" {
		t.Fatalf("native transport must retain its own metric/ISP contract: %+v", got)
	}
}

func TestResolveCameraControlOptimizerTargetsUsesPhysicalUVCCaptureOnce(t *testing.T) {
	targets := resolveCameraControlOptimizerTargets(engineConfig{
		VideoTransport:                       "v4l2_uvc",
		VideoCaptureDevice:                   "/dev/video7",
		CaptureSourcePath:                    "/dev/video-main",
		AnalysisSourcePath:                   "/dev/video-sub",
		CameraControlOptimizerTargetStream:   "both",
		CameraControlOptimizerSampleStrategy: "auto",
		CameraControlOptimizerMetricSource:   "quality_file",
		VideoQualityMonitorEnabled:           true,
		VideoQualityMonitorStatePath:         "/run/video-broker/video-quality.json",
	})
	if len(targets) != 1 || targets[0].controlDevice != "/dev/video7" {
		t.Fatalf("UVC CameraControl must control the physical capture once: %+v", targets)
	}
}

func TestProbeCameraControlOptimizerTargetsRequiresWritableResidualControl(t *testing.T) {
	targets := []cameraControlTarget{{controlDevice: "/dev/video0"}}
	run := func(_ context.Context, _ ...string) ([]byte, error) {
		return []byte(`
auto_exposure 0x009a0901 (menu) : min=0 max=3 step=1 default=3 value=3
brightness 0x00980900 (int) : min=0 max=255 step=1 default=128 value=128 flags=read-only
`), nil
	}
	backend := cameracontrol.NewV4L2Backend(cameracontrol.Command(run))
	if capable, _ := probeCameraControlOptimizerTargets(context.Background(), targets, backend); capable {
		t.Fatal("auto exposure alone must not qualify as a CameraControl residual control")
	}
	run = func(_ context.Context, _ ...string) ([]byte, error) {
		return []byte(`brightness 0x00980900 (int) : min=0 max=255 step=1 default=128 value=128`), nil
	}
	backend = cameracontrol.NewV4L2Backend(cameracontrol.Command(run))
	if capable, details := probeCameraControlOptimizerTargets(context.Background(), targets, backend); !capable || details != "/dev/video0:brightness" {
		t.Fatalf("expected writable brightness capability: capable=%t details=%q", capable, details)
	}
}

func TestDeferredCameraControlControlProbeRunsOnceForRawCapture(t *testing.T) {
	calls := 0
	optimizer := &cameraControlOptimizer{
		deferHardwareProbe: true,
		controlQualified:   map[string]bool{},
		controlBackend: cameracontrol.NewV4L2Backend(func(_ context.Context, _ ...string) ([]byte, error) {
			calls++
			return []byte(`brightness 0x00980900 (int) : min=0 max=255 step=1 default=128 value=128`), nil
		}),
	}
	target := cameraControlTarget{controlDevice: "/dev/video0"}
	if !optimizer.ensureHardwareControlQualified(context.Background(), target) {
		t.Fatal("raw UVC residual controls should qualify")
	}
	if !optimizer.ensureHardwareControlQualified(context.Background(), target) || calls != 1 {
		t.Fatalf("raw control capability must be cached, calls=%d", calls)
	}
}

func TestReadControlsSkipsInactiveResidualControl(t *testing.T) {
	capabilities := cameracontrol.ParseV4L2Capabilities([]byte(`
brightness 0x00980900 (int) : min=0 max=255 step=1 default=128 value=128
gain 0x00980913 (int) : min=0 max=100 step=2 default=5 value=5 flags=inactive
`))
	if !cameracontrol.HasWritableResidual(capabilities) {
		t.Fatal("writable brightness should qualify")
	}
	if capabilities["gain"].Writable() {
		t.Fatal("inactive gain must not qualify")
	}
}

func TestInitialCameraControlProfileUsesMeasuredLuma(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		mode:         "profiles",
		targetLuma:   96,
		targetByMode: map[string]int{"sun": 185, "day": 125, "sunset": 90, "night": 72},
		thresholds:   cameraProfileThresholds{sun: 185, day: 125, sunset: 90, night: 70},
		profileState: map[string]string{}, profileCandidate: map[string]string{}, profileCandidateCount: map[string]int{},
	}
	if got := optimizer.resolveTargetLuma("/dev/video0", 22); got != 72 || optimizer.profileState["/dev/video0"] != "night" {
		t.Fatalf("dark startup must select night directly: target=%d state=%q", got, optimizer.profileState["/dev/video0"])
	}
}

func TestUVCControlProfileBrightensGammaBeforeOtherControls(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		hysteresis:   8,
		baseline:     map[string]int{"gamma": 100, "brightness": 0, "backlight_compensation": 1, "gain": 0},
		controlMax:   map[string]int{"gamma": 140, "brightness": 24, "backlight_compensation": 2, "gain": 10},
		controlSteps: map[string]int{"gamma": 10, "brightness": 4, "backlight_compensation": 1, "gain": 2},
	}
	controls := map[string]v4l2ControlState{
		"gamma":                  {name: "gamma", min: 72, max: 500, value: 100},
		"brightness":             {name: "brightness", min: -64, max: 64, value: 0},
		"backlight_compensation": {name: "backlight_compensation", min: 0, max: 2, value: 1},
		"gain":                   {name: "gain", min: 0, max: 100, value: 0},
	}

	adjustment, ok := optimizer.selectUVCControlAdjustment(controls, cameraQualitySample{Luma: 44}, 80, true, 4)
	if !ok {
		t.Fatalf("expected UVC adjustment")
	}
	if adjustment.state.name != "gamma" || adjustment.next != 140 {
		t.Fatalf("expected gamma first up to cap, got %+v", adjustment)
	}
}

func TestUVCControlProfileRestoresBelowBaselineBeforeBrightening(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		hysteresis: 8,
		baseline: map[string]int{
			"gamma": 100, "brightness": 0, "backlight_compensation": 1, "gain": 0,
		},
		controlMin:   map[string]int{"backlight_compensation": 0},
		controlMax:   map[string]int{"gamma": 140, "brightness": 24, "backlight_compensation": 2, "gain": 10},
		controlSteps: map[string]int{"gamma": 10, "brightness": 4, "backlight_compensation": 1, "gain": 2},
	}
	controls := map[string]v4l2ControlState{
		"gamma":                  {name: "gamma", min: 72, max: 500, value: 100},
		"brightness":             {name: "brightness", min: -64, max: 64, value: 0},
		"backlight_compensation": {name: "backlight_compensation", min: 0, max: 2, value: 0},
		"gain":                   {name: "gain", min: 0, max: 100, value: 0},
	}

	adjustment, ok := optimizer.selectUVCControlAdjustment(controls, cameraQualitySample{Luma: 80}, 128, true, 3)
	if !ok || adjustment.state.name != "backlight_compensation" || adjustment.next != 1 {
		t.Fatalf("below-baseline residual was not restored first: ok=%t adjustment=%+v", ok, adjustment)
	}
}

func TestCompressedDarkResidualRestoresAfterHighlightLeaves(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		targetLuma: 128, hysteresis: 12,
		baseline:      map[string]int{"backlight_compensation": 1},
		residualDirty: map[string]bool{}, residualDirection: map[string]int{},
	}
	controls := map[string]v4l2ControlState{
		"backlight_compensation": {name: "backlight_compensation", min: 0, max: 2, value: 1},
	}
	optimizer.recordResidualState("/dev/video0", controls, controlAdjustment{
		state: controls["backlight_compensation"], next: 0,
	})
	recovery := optimizer.compressedRecoveryClass("/dev/video0", cameraQualitySample{
		CaptureFormat: "mjpeg", Luma: 100, SignalInformative: true,
	}, 128)
	if recovery != "dark_residual_baseline_restore" {
		t.Fatalf("dark residual did not become reversibly recoverable: %q", recovery)
	}
	recovery = optimizer.compressedRecoveryClass("/dev/video0", cameraQualitySample{
		CaptureFormat: "mjpeg", Luma: 60, WhiteRatio: 0.01, SignalInformative: true,
		Regions: []cameraQualityRegion{{Samples: 100, WhiteRatio: 0.5}},
	}, 128)
	if recovery != "dark_residual_baseline_restore" {
		t.Fatalf("deep darkness did not override a confined highlight during baseline recovery: %q", recovery)
	}
}

func TestCompressedDarkResidualHonorsEffectiveProfileTarget(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		targetLuma: 128, hysteresis: 12,
		residualDirty:     map[string]bool{"/dev/video0": true},
		residualDirection: map[string]int{"/dev/video0": -1},
	}
	sample := cameraQualitySample{
		CaptureFormat: "mjpeg", Luma: 101, SignalInformative: true,
	}
	if recovery := optimizer.compressedRecoveryClass("/dev/video0", sample, 100); recovery != "" {
		t.Fatalf("sun profile residual oscillated against the global target: %q", recovery)
	}
	if recovery := optimizer.compressedRecoveryClass("/dev/video0", sample, 128); recovery != "dark_residual_baseline_restore" {
		t.Fatalf("regression fixture does not distinguish the effective profile target: %q", recovery)
	}
}

func TestAdmittedBaselineRecoveryIgnoresDynamicProfileTarget(t *testing.T) {
	var setArgument string
	optimizer := &cameraControlOptimizer{
		hysteresis: 12,
		baseline:   map[string]int{"backlight_compensation": 1},
		admittedRecovery: map[string]string{
			"/dev/video0": "dark_residual_baseline_restore",
		},
		residualDirty: map[string]bool{"/dev/video0": true},
		residualDirection: map[string]int{
			"/dev/video0": -1,
		},
		lastAdjust: map[string]time.Time{},
		controlBackend: cameracontrol.NewV4L2Backend(func(_ context.Context, args ...string) ([]byte, error) {
			setArgument = args[len(args)-1]
			return nil, nil
		}),
	}
	controls := map[string]v4l2ControlState{
		"backlight_compensation": {name: "backlight_compensation", min: 0, max: 2, value: 0},
	}
	action, err := optimizer.applyAdjustment(context.Background(), "/dev/video0", controls, cameraQualitySample{
		Luma: 60, SignalInformative: true,
	}, 72)
	if err != nil || setArgument != "--set-ctrl=backlight_compensation=1" || !strings.Contains(action, "restore") {
		t.Fatalf("admitted baseline recovery was not applied: action=%q set=%q err=%v", action, setArgument, err)
	}
}

func TestUninformativeSignalRestoresResidualBaseline(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		baseline:   map[string]int{"brightness": 128, "gain": 5},
		lastAdjust: map[string]time.Time{},
	}
	controls := map[string]v4l2ControlState{
		"brightness": {name: "brightness", min: 0, max: 255, value: 248},
		"gain":       {name: "gain", min: 0, max: 100, value: 13},
	}
	adjustment, ok := optimizer.selectUVCResidualBaselineRestore(controls)
	if !ok || adjustment.state.name != "brightness" || adjustment.state.value != 248 || adjustment.next != 128 {
		t.Fatalf("unexpected restore adjustment: ok=%t adjustment=%+v", ok, adjustment)
	}
}

func TestUninformativeWhiteSignalRestoresResidualBaseline(t *testing.T) {
	optimizer := &cameraControlOptimizer{baseline: map[string]int{"gain": 5}}
	controls := map[string]v4l2ControlState{
		"gain": {name: "gain", min: 0, max: 100, value: 40},
	}
	adjustment, ok := optimizer.selectUVCResidualBaselineRestore(controls)
	if !ok || adjustment.state.name != "gain" || adjustment.next != 5 {
		t.Fatalf("unexpected white-signal restore adjustment: ok=%t adjustment=%+v", ok, adjustment)
	}
}

func TestUVCControlProfileUsesGainOnlyAsLastResort(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		hysteresis:   8,
		baseline:     map[string]int{"gamma": 100, "brightness": 0, "backlight_compensation": 1, "gain": 0},
		controlMax:   map[string]int{"gamma": 140, "brightness": 24, "backlight_compensation": 2, "gain": 10},
		controlSteps: map[string]int{"gamma": 10, "brightness": 4, "backlight_compensation": 1, "gain": 2},
	}
	controls := map[string]v4l2ControlState{
		"gamma":                  {name: "gamma", min: 72, max: 500, value: 140},
		"brightness":             {name: "brightness", min: -64, max: 64, value: 24},
		"backlight_compensation": {name: "backlight_compensation", min: 0, max: 2, value: 2},
		"gain":                   {name: "gain", min: 0, max: 100, value: 0},
	}

	adjustment, ok := optimizer.selectUVCControlAdjustment(controls, cameraQualitySample{Luma: 70}, 80, true, 1)
	if ok {
		t.Fatalf("gain should not rise for a small luma delta, got %+v", adjustment)
	}
	adjustment, ok = optimizer.selectUVCControlAdjustment(controls, cameraQualitySample{Luma: 57}, 80, true, 3)
	if ok {
		t.Fatalf("gain should not rise for a medium luma delta, got %+v", adjustment)
	}
	adjustment, ok = optimizer.selectUVCControlAdjustment(controls, cameraQualitySample{Luma: 50}, 80, true, 3)
	if !ok || adjustment.state.name != "gain" || adjustment.next != 6 {
		t.Fatalf("expected gain only after other controls saturate and delta is large, got ok=%t adjustment=%+v", ok, adjustment)
	}
}

func TestUVCControlProfileRecoversDeepShadowsWithoutChangingExposureDomain(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		hysteresis:   8,
		baseline:     map[string]int{"gamma": 100, "brightness": 128, "gain": 5},
		controlSteps: map[string]int{"gamma": 10, "brightness": 3, "gain": 1},
	}
	controls := map[string]v4l2ControlState{
		"gamma":                  {name: "gamma", min: 72, max: 500, value: 100},
		"brightness":             {name: "brightness", min: 0, max: 255, value: 128},
		"exposure_time_absolute": {name: "exposure_time_absolute", min: 0, max: 6500, step: 1, value: 100},
		"gain":                   {name: "gain", min: 0, max: 100, value: 5},
	}

	adjustment, ok := optimizer.selectUVCControlAdjustment(controls, cameraQualitySample{
		Luma: 28, BlackRatio: 0.67, SignalInformative: true,
	}, 80, true, 6)
	if !ok || adjustment.state.name != "gain" || adjustment.next != 11 {
		t.Fatalf("deep shadow clipping must use residual gain without touching exposure: ok=%t adjustment=%+v", ok, adjustment)
	}
}

func TestUVCControlProfileNeverSelectsExposureTime(t *testing.T) {
	optimizer := &cameraControlOptimizer{hysteresis: 8, baseline: map[string]int{}}
	controls := map[string]v4l2ControlState{
		"exposure_time_absolute": {name: "exposure_time_absolute", min: 0, max: 6500, step: 1, value: 100},
	}
	if adjustment, ok := optimizer.selectUVCControlAdjustment(controls, cameraQualitySample{
		Luma: 0, BlackRatio: 1, SignalInformative: false,
	}, 80, true, 8); ok {
		t.Fatalf("CameraControl must preserve the camera exposure domain, got %+v", adjustment)
	}
}

func TestCameraControlAdjustmentCooldownRespondsQuicklyOnlyToSevereClipping(t *testing.T) {
	optimizer := &cameraControlOptimizer{tick: 5 * time.Second, cooldown: 30 * time.Second, hysteresis: 8}
	if got := optimizer.adjustmentCooldown(cameraQualitySample{Luma: 28, BlackRatio: 0.67}, 80); got != 5*time.Second {
		t.Fatalf("unexpected severe-clipping cooldown: %s", got)
	}
	if got := optimizer.adjustmentCooldown(cameraQualitySample{Luma: 62, BlackRatio: 0.04}, 80); got != 30*time.Second {
		t.Fatalf("near-target feedback must retain conservative cooldown: %s", got)
	}
}

func TestUVCControlProfileDarkensTowardBaseline(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		baseline:     map[string]int{"gamma": 100, "brightness": 0, "backlight_compensation": 1, "gain": 0},
		controlSteps: map[string]int{"gamma": 10, "brightness": 4, "backlight_compensation": 1, "gain": 2},
	}
	controls := map[string]v4l2ControlState{
		"gamma":                  {name: "gamma", min: 72, max: 500, value: 130},
		"brightness":             {name: "brightness", min: -64, max: 64, value: 16},
		"backlight_compensation": {name: "backlight_compensation", min: 0, max: 2, value: 2},
		"gain":                   {name: "gain", min: 0, max: 100, value: 4},
	}

	adjustment, ok := optimizer.selectUVCControlAdjustment(controls, cameraQualitySample{Luma: 120}, 80, false, 2)
	if !ok || adjustment.state.name != "gain" || adjustment.next != 0 {
		t.Fatalf("expected darken to reduce gain first toward baseline, got ok=%t adjustment=%+v", ok, adjustment)
	}
}

func TestAutomaticResidualEnvelopeCanCorrectBelowBaseline(t *testing.T) {
	optimizer := &cameraControlOptimizer{
		baseline:     map[string]int{"brightness": 128},
		controlMin:   map[string]int{"brightness": 102},
		controlMax:   map[string]int{"brightness": 154},
		controlSteps: map[string]int{"brightness": 3},
	}
	controls := map[string]v4l2ControlState{
		"brightness": {name: "brightness", min: 0, max: 255, step: 1, value: 128},
	}

	adjustment, ok := optimizer.selectUVCControlAdjustment(
		controls,
		cameraQualitySample{Luma: 240, WhiteRatio: 0.8, SignalInformative: true},
		128,
		false,
		2,
	)
	if !ok || adjustment.state.name != "brightness" || adjustment.next != 122 {
		t.Fatalf("bounded daylight correction did not move below baseline: ok=%t adjustment=%+v", ok, adjustment)
	}
	minV, maxV := optimizer.uvcControlBounds(controls["brightness"])
	if minV != 102 || maxV != 154 {
		t.Fatalf("automatic residual envelope=%d..%d want=102..154", minV, maxV)
	}
}

func TestUVCControlBoundsScaleFromDeviceRange(t *testing.T) {
	optimizer := &cameraControlOptimizer{baseline: map[string]int{"brightness": 128}}
	minV, maxV := optimizer.uvcControlBounds(v4l2ControlState{
		name: "brightness", min: 0, max: 255, value: 128,
	})
	if minV != 128 || maxV != 255 {
		t.Fatalf("unexpected 0..255 semantic bounds: %d..%d", minV, maxV)
	}

	optimizer.baseline["brightness"] = 0
	minV, maxV = optimizer.uvcControlBounds(v4l2ControlState{
		name: "brightness", min: -64, max: 64, value: 0,
	})
	if minV != 0 || maxV != 64 {
		t.Fatalf("unexpected -64..64 semantic bounds: %d..%d", minV, maxV)
	}
}

func TestUVCControlStepUsesDeviceDeclaration(t *testing.T) {
	optimizer := &cameraControlOptimizer{}
	state := v4l2ControlState{name: "brightness", min: 0, max: 255, step: 3, value: 128}
	if got := optimizer.uvcControlStep(state, controlStep{name: "brightness", step: 9}); got != 3 {
		t.Fatalf("device-declared step must win: %d", got)
	}
}

func TestCameraControlOptimizerRebindsToAdmittedCameraNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "camera-admission.json")
	if err := writeCameraAdmissionState(path, cameraAdmissionState{
		Schema: cameraAdmissionSchema,
		Status: "admitted",
		Discovery: cameraAdmissionDiscoveryState{
			SelectedDevice:        "/dev/video6",
			CapabilityFingerprint: "cap-v1:abcd",
		},
	}); err != nil {
		t.Fatal(err)
	}
	optimizer := &cameraControlOptimizer{cameraAdmissionStatePath: path}
	base := cameraControlTarget{
		controlDevice:    "/dev/video0",
		sampleDevice:     "/dev/video0",
		sampleCandidates: []string{"/dev/video0", "/dev/video-statistics"},
	}
	target := optimizer.resolveAdmittedCameraTarget(base)
	if target.controlDevice != "/dev/video6" || target.sampleDevice != "/dev/video6" || target.sampleCandidates[0] != "/dev/video6" {
		t.Fatalf("CameraControl target was not rebound: %+v", target)
	}
	if base.sampleCandidates[0] != "/dev/video0" {
		t.Fatalf("rebind mutated the persistent discovery template: %+v", base)
	}
}
