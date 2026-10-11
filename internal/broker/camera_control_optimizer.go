package broker

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Josepavese/videosluice/internal/cameracontrol"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/quality"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var errCameraSampleNotReady = errors.New("camera quality sample not ready")

type cameraControlOptimizer struct {
	mode                     string
	targets                  []cameraControlTarget
	tick                     time.Duration
	initialDelay             time.Duration
	targetLuma               int
	targetByMode             map[string]int
	thresholds               cameraProfileThresholds
	hysteresis               int
	sampleWidth              int
	sampleHeight             int
	strictSample             bool
	reportEvery              int
	sampler                  cameraControlSampler
	metricSource             string
	inPipelineToneMapping    bool
	deferHardwareProbe       bool
	controlBackend           cameracontrol.Backend
	cameraAdmissionStatePath string
	lastAdmissionDevice      string

	controlProfile string
	baseline       map[string]int
	controlMin     map[string]int
	controlMax     map[string]int
	controlSteps   map[string]int
	cooldown       time.Duration
	stableSamples  int

	profileState           map[string]string
	profileCandidate       map[string]string
	profileCandidateCount  map[string]int
	activeSample           map[string]string
	failures               map[string]int
	attempts               map[string]int
	failed                 map[string]int
	skipUntil              map[string]time.Time
	controlFailures        map[string]int
	controlSkipUntil       map[string]time.Time
	lastAdjust             map[string]time.Time
	manualPrepared         map[string]bool
	controlSuppressed      map[string]string
	controlQualified       map[string]bool
	recoveryCandidate      map[string]string
	recoveryCandidateCount map[string]int
	admittedRecovery       map[string]string
	residualDirty          map[string]bool
	residualDirection      map[string]int
}

type cameraControlSampler interface {
	Name() string
	SampleQuality(ctx context.Context, device string, width int, height int) (cameraQualitySample, error)
}

type cameraQualitySample struct {
	Luma              int
	LapVar            float64
	Edge              float64
	BlackRatio        float64
	WhiteRatio        float64
	SignalInformative bool
	CaptureFormat     string
	Source            string
	Age               time.Duration
	Regions           []cameraQualityRegion
}

type cameraQualityRegion struct {
	X                 int
	Y                 int
	Samples           uint64
	Luma              int
	BlackRatio        float64
	WhiteRatio        float64
	SignalInformative bool
}

type compositeSampler struct {
	samplers []cameraControlSampler
}

func (s *compositeSampler) Name() string {
	names := make([]string, 0, len(s.samplers))
	for _, sampler := range s.samplers {
		names = append(names, sampler.Name())
	}
	return strings.Join(names, "+")
}

func (s *compositeSampler) SampleQuality(ctx context.Context, device string, width int, height int) (cameraQualitySample, error) {
	errs := make([]string, 0, len(s.samplers))
	for _, sampler := range s.samplers {
		sample, err := sampler.SampleQuality(ctx, device, width, height)
		if err == nil {
			return sample, nil
		}
		errs = append(errs, fmt.Sprintf("%s: %v", sampler.Name(), err))
	}
	if len(errs) > 0 {
		return cameraQualitySample{}, errors.New(strings.Join(errs, " | "))
	}
	return cameraQualitySample{}, fmt.Errorf("no sampler available")
}

type v4l2Sampler struct {
	name    string
	formats []string
	setFmt  bool
}

func (s *v4l2Sampler) Name() string { return s.name }

func (s *v4l2Sampler) SampleQuality(ctx context.Context, device string, width int, height int) (cameraQualitySample, error) {
	errs := make([]string, 0, len(s.formats))
	for _, format := range s.formats {
		luma, err := sampleLumaForFormat(ctx, device, format, width, height, s.setFmt)
		if err == nil {
			return cameraQualitySample{Luma: luma, Source: device}, nil
		}
		errs = append(errs, fmt.Sprintf("%s: %v", format, err))
	}
	return cameraQualitySample{}, fmt.Errorf("cannot sample luma (%s)", strings.Join(errs, " | "))
}

type rkispStatsSampler struct{}

func (s *rkispStatsSampler) Name() string { return "rkisp1_stats_meta" }

func (s *rkispStatsSampler) SampleQuality(ctx context.Context, device string, _ int, _ int) (cameraQualitySample, error) {
	tmpDir, err := os.MkdirTemp("", "video-broker-rkisp-stats-")
	if err != nil {
		return cameraQualitySample{}, err
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()
	framePath := filepath.Join(tmpDir, "stats.raw")

	tryCapture := func(args ...string) error {
		args = append([]string{"-d", device}, args...)
		return runCommandWithTimeout(ctx, cameraControlCmdTimeoutIO, "v4l2-ctl", args...)
	}
	commands := [][]string{
		{"--set-fmt-meta=dataformat=RK1S", "--stream-mmap=3", "--stream-count=1", "--stream-to=" + framePath},
		{"--set-fmt-meta=dataformat=rk1s", "--stream-mmap=3", "--stream-count=1", "--stream-to=" + framePath},
		{"--stream-mmap=3", "--stream-count=1", "--stream-to=" + framePath},
	}
	var lastErr error
	for _, cmdArgs := range commands {
		if err := tryCapture(cmdArgs...); err == nil {
			frame, readErr := os.ReadFile(framePath)
			if readErr != nil {
				return cameraQualitySample{}, readErr
			}
			luma, parseErr := parseRKISPStatsLuma(frame)
			if parseErr != nil {
				return cameraQualitySample{}, parseErr
			}
			return cameraQualitySample{Luma: luma, Source: device}, nil
		} else {
			lastErr = err
		}
	}
	if lastErr != nil {
		return cameraQualitySample{}, lastErr
	}
	return cameraQualitySample{}, fmt.Errorf("rkisp stats capture failed")
}

type qualityFileSampler struct {
	path   string
	maxAge time.Duration
	// The capture owner can remain running while analysis is detached. Reject
	// both unbound input and a file published before the latest role transition.
	inputCurrent func(time.Time) bool
}

func (s *qualityFileSampler) Name() string { return "quality_state_file" }

func (rt *videoBrokerRuntime) qualityInputCurrent(sampleTime time.Time) bool {
	if rt == nil {
		return false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.childRunning && rt.plan.AnalysisEnabled && rt.plan.QualityMonitorEnabled &&
		sampleTime.After(rt.childStartedAt) && sampleTime.After(rt.qualityInputSince)
}

func (s *qualityFileSampler) SampleQuality(ctx context.Context, _ string, _ int, _ int) (cameraQualitySample, error) {
	if strings.TrimSpace(s.path) == "" {
		return cameraQualitySample{}, fmt.Errorf("empty quality state path")
	}
	if err := ctx.Err(); err != nil {
		return cameraQualitySample{}, err
	}
	state, err := quality.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cameraQualitySample{}, fmt.Errorf("%w: quality state missing path=%s", errCameraSampleNotReady, s.path)
		}
		return cameraQualitySample{}, err
	}
	if s.inputCurrent != nil && !s.inputCurrent(time.UnixMilli(state.TimestampUnixMS)) {
		return cameraQualitySample{}, fmt.Errorf("%w: analysis input unbound or quality predates its current binding", errCameraSampleNotReady)
	}
	age := time.Since(time.UnixMilli(state.TimestampUnixMS))
	if age < -5*time.Second {
		return cameraQualitySample{}, fmt.Errorf("quality state timestamp is in the future by %s", (-age).Round(time.Second))
	}
	if age < 0 {
		age = 0
	}
	if s.maxAge > 0 && age > s.maxAge {
		return cameraQualitySample{}, fmt.Errorf("%w: quality state stale age=%s max=%s", errCameraSampleNotReady, age.Round(time.Second), s.maxAge)
	}
	source := state.Source
	regions := make([]cameraQualityRegion, 0, len(source.Spatial.Regions))
	for _, region := range source.Spatial.Regions {
		regions = append(regions, cameraQualityRegion{
			X: region.X, Y: region.Y, Samples: region.Samples,
			Luma:       clampInt(int(math.Round(region.Luma)), 0, 255),
			BlackRatio: region.BlackRatio, WhiteRatio: region.WhiteRatio,
			SignalInformative: region.SignalInformative,
		})
	}
	return cameraQualitySample{
		Luma:              clampInt(int(math.Round(source.Luma)), 1, 255),
		LapVar:            source.LaplacianVariance,
		Edge:              source.Edge,
		BlackRatio:        source.BlackRatio,
		WhiteRatio:        source.WhiteRatio,
		SignalInformative: source.SignalInformative,
		CaptureFormat:     strings.ToLower(strings.TrimSpace(state.Capture.Format)),
		Source:            s.path,
		Age:               age,
		Regions:           regions,
	}, nil
}

type cameraProfileThresholds struct {
	sun    int
	day    int
	sunset int
	night  int
}

type cameraControlTarget struct {
	sampleDevice     string
	controlDevice    string
	sampleCandidates []string
}

type v4l2ControlState struct {
	name  string
	min   int
	max   int
	step  int
	value int
}

type controlStep struct {
	name string
	step int
}

type controlAdjustment struct {
	state v4l2ControlState
	next  int
}

const (
	cameraControlCmdTimeoutFast = 4 * time.Second
	cameraControlCmdTimeoutIO   = 3 * time.Second
	rkispAEOffset               = 16
	rkispAEMaxV10               = 25
	rkispAEMaxV12               = 81
)

func resolveCameraControlOptimizerRuntime(opt engineConfig) engineConfig {
	policy := strings.ToLower(strings.TrimSpace(opt.CameraControlOptimizerPolicy))
	if policy == "" {
		policy = "auto"
	}
	if policy == "off" {
		return opt
	}
	transport := strings.ToLower(strings.TrimSpace(opt.VideoTransport))
	if (transport != videoproviders.TransportV4L2UVC && transport != videoproviders.TransportV4L2) || !opt.Enabled {
		return opt
	}
	// UVC modules own their control endpoint. The optimizer remains available
	// for native ISP transports, while USB image correction is performed in the
	// post-ISP tone pipeline and never issues live SET_CUR transactions.
	opt.CameraControlOptimizerPolicy = "off"

	// The broker already owns and decodes the camera. Publish luma from that
	// shared pre-encoder path instead of opening the UVC device a second time.
	opt.VideoQualityMonitorEnabled = true
	if strings.TrimSpace(opt.VideoQualityMonitorStatePath) == "" {
		opt.VideoQualityMonitorStatePath = "/run/video-broker/video-quality.json"
	}
	maximumCadence := opt.AnalysisFPS
	if maximumCadence <= 0 {
		maximumCadence = opt.VideoCaptureFPS
	}
	if maximumCadence <= 0 {
		maximumCadence = 30
	}
	if opt.VideoQualityMonitorReportEvery <= 0 || opt.VideoQualityMonitorReportEvery > maximumCadence {
		opt.VideoQualityMonitorReportEvery = maximumCadence
	}
	opt.CameraControlOptimizerMetricSource = "quality_file"
	opt.CameraControlOptimizerSampleStrategy = "auto"
	return opt
}

func brokerToneMappingEnabled(opt engineConfig) bool {
	mode := strings.ToLower(strings.TrimSpace(opt.ToneMappingMode))
	// Capability planning, not a board/provider name, decides whether the
	// selected graph can honor this pixel contract. Portable GStreamer graphs
	// use the product-owned videobrokertone element before their single fan-out tee.
	return mode != "off" && opt.Enabled
}

func brokerToneMappingTarget(opt engineConfig) int {
	target := opt.ToneMappingTargetLuma
	if target <= 0 {
		target = 96
	}
	return clampInt(target, 16, 235)
}

func startCameraControlOptimizer(ctx context.Context, opt engineConfig, runtime *videoBrokerRuntime) {
	controlBackend := newV4L2CameraControlBackend()
	policy := strings.ToLower(strings.TrimSpace(opt.CameraControlOptimizerPolicy))
	if policy == "" {
		policy = "auto"
	}
	if policy == "off" {
		log.Printf("[video-broker][camera-control] disabled by policy ownership=camera_or_post_isp")
		return
	}
	if policy == "auto" && nativeRkaiqOwnsCameraControlTarget(ctx, opt) {
		log.Printf("[video-broker][camera-control] disabled: native RKAIQ daemon is active")
		return
	}
	mode := strings.ToLower(strings.TrimSpace(opt.CameraControlOptimizerMode))
	if mode == "" {
		log.Printf("[video-broker][camera-control] skip: policy=%s requires a mode", policy)
		return
	}
	targets := resolveCameraControlOptimizerTargets(opt)
	if len(targets) == 0 {
		log.Printf("[video-broker][camera-control] skip: policy=%s but no resolvable camera-control targets from source config", policy)
		return
	}
	deferHardwareProbe := brokerToneMappingEnabled(opt)
	if deferHardwareProbe {
		log.Printf("[video-broker][camera-control] %s enabled: hardware control qualification deferred until negotiated capture format is available", policy)
	} else {
		capable, details := probeCameraControlOptimizerTargets(ctx, targets, controlBackend)
		if !capable {
			log.Printf("[video-broker][camera-control] %s disabled: no writable residual luma controls details=%s", policy, details)
			return
		}
		log.Printf("[video-broker][camera-control] %s enabled: writable residual luma controls details=%s", policy, details)
	}
	if isKnownUnsafeRKISPOptimizerKernel(opt, targets) {
		log.Printf("[video-broker][camera-control] warning: rk356x 5.10 + statistics sampling detected; applying conservative pacing")
	}

	sampler, err := newCameraControlSampler(opt, targets)
	if err != nil {
		log.Printf("[video-broker][camera-control] skip: sampler init failed: %v", err)
		return
	}
	if qualitySampler, ok := sampler.(*qualityFileSampler); ok {
		qualitySampler.inputCurrent = runtime.qualityInputCurrent
	}

	tickMs := opt.CameraControlOptimizerTickMs
	if tickMs <= 0 {
		tickMs = 1500
	}
	optimizer := &cameraControlOptimizer{
		mode:         mode,
		targets:      targets,
		tick:         time.Duration(tickMs) * time.Millisecond,
		initialDelay: time.Duration(maxInt(3, opt.VideoCaptureWarmupSec+2)) * time.Second,
		targetLuma:   opt.ToneMappingTargetLuma,
		targetByMode: map[string]int{
			"sun":    opt.CameraControlOptimizerTargetLumaSun,
			"day":    opt.CameraControlOptimizerTargetLumaDay,
			"sunset": opt.CameraControlOptimizerTargetLumaSunset,
			"night":  opt.CameraControlOptimizerTargetLumaNight,
		},
		thresholds: cameraProfileThresholds{
			sun:    opt.CameraControlOptimizerSunThreshold,
			day:    opt.CameraControlOptimizerDayThreshold,
			sunset: opt.CameraControlOptimizerSunsetThreshold,
			night:  opt.CameraControlOptimizerNightThreshold,
		},
		hysteresis:               opt.ToneMappingHysteresis,
		sampleWidth:              opt.CameraControlOptimizerSampleWidth,
		sampleHeight:             opt.CameraControlOptimizerSampleHeight,
		strictSample:             opt.CameraControlOptimizerStrictSample,
		reportEvery:              opt.CameraControlOptimizerReportEvery,
		sampler:                  sampler,
		metricSource:             strings.ToLower(strings.TrimSpace(opt.CameraControlOptimizerMetricSource)),
		inPipelineToneMapping:    brokerToneMappingEnabled(opt),
		deferHardwareProbe:       deferHardwareProbe,
		controlBackend:           controlBackend,
		cameraAdmissionStatePath: strings.TrimSpace(opt.VideoCameraAdmissionStatePath),
		controlProfile:           strings.ToLower(strings.TrimSpace(opt.CameraControlOptimizerControlProfile)),
		baseline:                 cloneIntMap(opt.VideoCaptureControls),
		controlMin:               cloneIntMap(opt.CameraControlOptimizerControlMin),
		controlMax:               cloneIntMap(opt.CameraControlOptimizerControlMax),
		controlSteps:             cloneIntMap(opt.CameraControlOptimizerControlSteps),
		cooldown:                 time.Duration(opt.CameraControlOptimizerCooldownSec) * time.Second,
		stableSamples:            opt.CameraControlOptimizerStableSamples,
		profileState:             map[string]string{},
		profileCandidate:         map[string]string{},
		profileCandidateCount:    map[string]int{},
		activeSample:             map[string]string{},
		failures:                 map[string]int{},
		attempts:                 map[string]int{},
		failed:                   map[string]int{},
		skipUntil:                map[string]time.Time{},
		controlFailures:          map[string]int{},
		controlSkipUntil:         map[string]time.Time{},
		lastAdjust:               map[string]time.Time{},
		manualPrepared:           map[string]bool{},
		controlSuppressed:        map[string]string{},
		controlQualified:         map[string]bool{},
		recoveryCandidate:        map[string]string{},
		recoveryCandidateCount:   map[string]int{},
		admittedRecovery:         map[string]string{},
		residualDirty:            map[string]bool{},
		residualDirection:        map[string]int{},
	}
	// Auto discovery has already replaced configured raw values with a residual
	// envelope derived from this camera's own default and range. Preserve that
	// envelope here: dropping it would turn repeated recovery into integrator
	// wind-up all the way to the device maximum.
	if optimizer.targetLuma <= 0 {
		optimizer.targetLuma = 96
	}
	if optimizer.targetByMode["sun"] <= 0 {
		optimizer.targetByMode["sun"] = 185
	}
	if optimizer.targetByMode["day"] <= 0 {
		optimizer.targetByMode["day"] = 125
	}
	if optimizer.targetByMode["sunset"] <= 0 {
		optimizer.targetByMode["sunset"] = 90
	}
	if optimizer.targetByMode["night"] <= 0 {
		optimizer.targetByMode["night"] = 72
	}
	if optimizer.thresholds.sun <= 0 {
		optimizer.thresholds.sun = 185
	}
	if optimizer.thresholds.day <= 0 {
		optimizer.thresholds.day = 125
	}
	if optimizer.thresholds.sunset <= 0 {
		optimizer.thresholds.sunset = 90
	}
	if optimizer.thresholds.night <= 0 {
		optimizer.thresholds.night = 70
	}
	if optimizer.hysteresis <= 0 {
		optimizer.hysteresis = 12
	}
	if optimizer.sampleWidth <= 0 {
		optimizer.sampleWidth = 320
	}
	if optimizer.sampleHeight <= 0 {
		optimizer.sampleHeight = 180
	}
	if optimizer.reportEvery <= 0 {
		optimizer.reportEvery = 20
	}
	if optimizer.metricSource == "" {
		optimizer.metricSource = "auto"
	}
	if optimizer.controlProfile == "" {
		optimizer.controlProfile = "auto"
	}
	if optimizer.cooldown <= 0 {
		optimizer.cooldown = 30 * time.Second
	}
	if optimizer.stableSamples <= 0 {
		optimizer.stableSamples = 2
	}

	log.Printf("[video-broker][camera-control] provider_selected=%s in_pipeline_tone_mapping=%t", optimizer.sampler.Name(), optimizer.inPipelineToneMapping)
	go optimizer.run(ctx)
}

func newCameraControlSampler(opt engineConfig, targets []cameraControlTarget) (cameraControlSampler, error) {
	metricSource := strings.ToLower(strings.TrimSpace(opt.CameraControlOptimizerMetricSource))
	if metricSource == "" {
		metricSource = "auto"
	}
	statePath := strings.TrimSpace(opt.VideoQualityMonitorStatePath)
	if metricSource == "quality_file" || (metricSource == "auto" && opt.VideoQualityMonitorEnabled && statePath != "") {
		if statePath == "" {
			return nil, fmt.Errorf("quality_file metric source requested but source processing.quality.state_path is empty")
		}
		maxAgeSec := opt.CameraControlOptimizerQualityMaxAgeSec
		if maxAgeSec <= 0 {
			maxAgeSec = 60
		}
		return &qualityFileSampler{path: statePath, maxAge: time.Duration(maxAgeSec) * time.Second}, nil
	}

	strategy := strings.ToLower(strings.TrimSpace(opt.CameraControlOptimizerSampleStrategy))
	if strategy == "" {
		strategy = "statistics_only"
	}
	statsAvailable := false
	for _, t := range targets {
		if isStatisticsDevice(t.sampleDevice) {
			statsAvailable = true
			break
		}
		for _, c := range t.sampleCandidates {
			if isStatisticsDevice(c) {
				statsAvailable = true
				break
			}
		}
		if statsAvailable {
			break
		}
	}

	switch strategy {
	case "statistics_only":
		if !statsAvailable {
			return nil, fmt.Errorf("statistics_only requested but no statistics device resolved")
		}
		return &compositeSampler{
			samplers: []cameraControlSampler{
				&v4l2Sampler{name: "stats_video_fallback", formats: []string{"GREY", "NV12", "YUYV", "UYVY", "NM12"}, setFmt: true},
				&rkispStatsSampler{},
			},
		}, nil
	case "auto":
		if statsAvailable {
			return &compositeSampler{
				samplers: []cameraControlSampler{
					&v4l2Sampler{name: "stats_video_fallback", formats: []string{"GREY", "NV12", "YUYV", "UYVY", "NM12"}, setFmt: true},
					&rkispStatsSampler{},
				},
			}, nil
		}
		return &v4l2Sampler{name: "frame_luma", formats: []string{"NV12", "YUYV", "UYVY", "GREY"}, setFmt: false}, nil
	default:
		return nil, fmt.Errorf("unsupported sample strategy %q", strategy)
	}
}

func isKnownUnsafeRKISPOptimizerKernel(opt engineConfig, targets []cameraControlTarget) bool {
	if !opt.CameraControlOptimizerStrictSample {
		return false
	}
	usesStatsSampling := false
	for _, target := range targets {
		if isStatisticsDevice(target.sampleDevice) {
			usesStatsSampling = true
			break
		}
	}
	if !usesStatsSampling {
		return false
	}
	release, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return false
	}
	kernel := strings.TrimSpace(strings.ToLower(string(release)))
	return strings.HasPrefix(kernel, "5.10.") && strings.Contains(kernel, "rk356")
}

func nativeRkaiqActive(ctx context.Context) bool {
	names := []string{"rkaiq_3A_server", "rkisp_3A_server", "camera_engine_rkaiq"}
	for _, name := range names {
		cmd := exec.CommandContext(ctx, "pgrep", "-x", name)
		if err := cmd.Run(); err == nil {
			return true
		}
	}
	return false
}

func nativeRkaiqOwnsCameraControlTarget(ctx context.Context, opt engineConfig) bool {
	transport := strings.ToLower(strings.TrimSpace(opt.VideoTransport))
	if transport == "v4l2_uvc" {
		return false
	}
	return nativeRkaiqActive(ctx)
}

func probeCameraControlOptimizerTargets(
	ctx context.Context,
	targets []cameraControlTarget,
	backend cameracontrol.Backend,
) (bool, string) {
	if backend == nil {
		return false, "missing camera-control backend"
	}
	details := make([]string, 0, len(targets))
	for _, target := range targets {
		device := strings.TrimSpace(target.controlDevice)
		if device == "" {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		capabilities, err := backend.Capabilities(probeCtx, device)
		cancel()
		if err != nil {
			details = append(details, fmt.Sprintf("%s:probe_error", device))
			continue
		}
		writable := make([]string, 0)
		for _, name := range cameracontrol.ResidualNames() {
			capability, ok := capabilities[name]
			if ok && capability.Writable() && capability.HasMin && capability.HasMax && capability.Max > capability.Min && (capability.HasDefault || capability.HasCurrent) {
				writable = append(writable, name)
			}
		}
		if len(writable) > 0 {
			details = append(details, fmt.Sprintf("%s:%s", device, strings.Join(writable, ",")))
			return true, strings.Join(details, ";")
		}
		details = append(details, fmt.Sprintf("%s:none", device))
	}
	return false, strings.Join(details, ";")
}

func resolveCameraControlOptimizerTargets(opt engineConfig) []cameraControlTarget {
	target := strings.ToLower(strings.TrimSpace(opt.CameraControlOptimizerTargetStream))
	if target == "" {
		target = "auto"
	}
	sampleStrategy := strings.ToLower(strings.TrimSpace(opt.CameraControlOptimizerSampleStrategy))
	if sampleStrategy == "" {
		sampleStrategy = "auto"
	}
	metricSource := strings.ToLower(strings.TrimSpace(opt.CameraControlOptimizerMetricSource))
	if metricSource == "quality_file" || (metricSource == "auto" && opt.VideoQualityMonitorEnabled && strings.TrimSpace(opt.VideoQualityMonitorStatePath) != "") {
		sampleStrategy = "auto"
	}
	mainPath := firstNonEmpty(opt.CaptureSourcePath, opt.CanonicalCaptureDevice)
	subPath := firstNonEmpty(opt.AnalysisSourcePath, opt.CanonicalAnalysisDevice)
	statsPath := strings.TrimSpace(opt.StatisticsSourcePath)
	physicalCapture := strings.TrimSpace(opt.VideoCaptureDevice)
	if strings.EqualFold(strings.TrimSpace(opt.VideoTransport), "v4l2_uvc") && physicalCapture != "" {
		mainPath = physicalCapture
		subPath = physicalCapture
	}

	var controlDevices []string
	switch target {
	case "main":
		controlDevices = dedupeNonEmpty([]string{mainPath})
	case "sub":
		controlDevices = dedupeNonEmpty([]string{subPath})
	case "auto":
		controlDevices = dedupeNonEmpty([]string{mainPath})
	case "both":
		fallthrough
	default:
		controlDevices = dedupeNonEmpty([]string{mainPath, subPath})
	}
	if len(controlDevices) == 0 {
		return nil
	}

	statsCandidates := dedupeNonEmpty(append([]string{statsPath, opt.CanonicalStatisticsDevice}, listRuntimeStatisticsCandidates()...))
	if sampleStrategy == "statistics_only" && len(statsCandidates) == 0 {
		return nil
	}

	out := make([]cameraControlTarget, 0, len(controlDevices))
	for _, control := range controlDevices {
		var sample string
		var candidates []string
		if sampleStrategy == "statistics_only" {
			sample = firstNonEmpty(statsCandidates...)
			if sample == "" {
				continue
			}
			candidates = statsCandidates
		} else {
			candidates = dedupeNonEmpty([]string{control, statsPath, opt.CanonicalStatisticsDevice})
			sample = firstNonEmpty(control, statsPath, opt.CanonicalStatisticsDevice)
		}
		out = append(out, cameraControlTarget{sampleDevice: sample, controlDevice: control, sampleCandidates: candidates})
	}
	return out
}

func dedupeNonEmpty(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, raw := range values {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}

func (o *cameraControlOptimizer) run(ctx context.Context) {
	log.Printf("[video-broker][camera-control] started mode=%s targets=%v tick=%s initial_delay=%s target_luma=%d hysteresis=%d sample=%dx%d control_profile=%s metric_source=%s cooldown=%s stable_samples=%d", o.mode, o.targets, o.tick, o.initialDelay, o.targetLuma, o.hysteresis, o.sampleWidth, o.sampleHeight, o.controlProfile, o.metricSource, o.cooldown, o.stableSamples)
	log.Printf("[video-broker][camera-control] strict_sample=%t report_every=%d", o.strictSample, o.reportEvery)

	ticker := time.NewTicker(o.tick)
	defer ticker.Stop()

	if o.initialDelay > 0 {
		timer := time.NewTimer(o.initialDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
	for _, target := range o.targets {
		o.optimizeTarget(ctx, target)
	}

	for {
		select {
		case <-ctx.Done():
			log.Printf("[video-broker][camera-control] stopping")
			return
		case <-ticker.C:
			for _, target := range o.targets {
				o.optimizeTarget(ctx, target)
			}
		}
	}
}

func (o *cameraControlOptimizer) prepareManualMode(ctx context.Context, device string) error {
	controls, err := o.readControls(ctx, device)
	if err != nil {
		return err
	}
	for _, prep := range []struct {
		name  string
		value int
	}{{"exposure_auto_priority", 0}, {"exposure_auto", 1}, {"auto_exposure", 1}} {
		if _, exists := controls[prep.name]; !exists {
			continue
		}
		if err := o.setControl(ctx, device, prep.name, prep.value); err != nil {
			return err
		}
	}
	return nil
}

func (o *cameraControlOptimizer) optimizeTarget(ctx context.Context, target cameraControlTarget) {
	target = o.resolveAdmittedCameraTarget(target)
	sampleDevice := strings.TrimSpace(target.sampleDevice)
	controlDevice := strings.TrimSpace(target.controlDevice)
	if sampleDevice == "" || controlDevice == "" {
		return
	}
	failKey := controlDevice
	if until, ok := o.skipUntil[failKey]; ok && time.Now().Before(until) {
		return
	}
	o.attempts[failKey]++
	sample, sampledFrom, err := o.sampleQualityWithFallback(ctx, target)
	if err != nil {
		if errors.Is(err, errCameraSampleNotReady) {
			if o.reportEvery > 0 && o.attempts[failKey]%o.reportEvery == 0 {
				log.Printf("[video-broker][camera-control] sample waiting control=%s attempts=%d err=%v", controlDevice, o.attempts[failKey], err)
			}
			return
		}
		o.failed[failKey]++
		o.failures[failKey]++
		o.logFailRate(failKey)
		if o.failures[failKey] >= 5 {
			o.skipUntil[failKey] = time.Now().Add(60 * time.Second)
			log.Printf("[video-broker][camera-control] sample failed repeatedly control=%s failures=%d backoff=60s err=%v", controlDevice, o.failures[failKey], err)
			o.failures[failKey] = 0
			return
		}
		log.Printf("[video-broker][camera-control] sample failed control=%s failures=%d err=%v", controlDevice, o.failures[failKey], err)
		return
	}
	o.failures[failKey] = 0
	o.logFailRate(failKey)
	targetLuma := o.resolveTargetLuma(controlDevice, sample.Luma)
	if o.updateHardwareControlSuppression(controlDevice, sample, targetLuma) {
		return
	}
	if !o.ensureHardwareControlQualified(ctx, target) {
		return
	}
	if o.mode == "manual" && !o.manualPrepared[controlDevice] {
		if err := o.prepareManualMode(ctx, controlDevice); err != nil {
			log.Printf("[video-broker][camera-control] manual preparation failed control=%s err=%v", controlDevice, err)
			return
		}
		if o.manualPrepared == nil {
			o.manualPrepared = map[string]bool{}
		}
		o.manualPrepared[controlDevice] = true
	}
	if o.shouldSkipControlRead(failKey) {
		return
	}
	controls, err := o.readControls(ctx, controlDevice)
	if err != nil {
		o.handleControlReadFailure(failKey, err)
		return
	}
	o.handleControlReadSuccess(failKey)
	action, err := o.applyAdjustment(ctx, controlDevice, controls, sample, targetLuma)
	if err != nil {
		log.Printf("[video-broker][camera-control] adjust failed sample=%s control=%s luma=%d err=%v", sampleDevice, controlDevice, sample.Luma, err)
		return
	}
	if action != "hold" {
		log.Printf("[video-broker][camera-control] %s sample=%s effective_sample=%s control=%s luma=%d target=%d lap_var=%.1f edge=%.1f black_ratio=%.5f white_ratio=%.5f max_region_black_ratio=%.5f max_region_white_ratio=%.5f signal_informative=%t sample_age=%s", action, sampleDevice, sampledFrom, controlDevice, sample.Luma, targetLuma, sample.LapVar, sample.Edge, sample.BlackRatio, sample.WhiteRatio, maxRegionBlackRatio(sample), maxRegionWhiteRatio(sample), sample.SignalInformative, sample.Age.Round(time.Second))
	}
}

func (o *cameraControlOptimizer) resolveAdmittedCameraTarget(target cameraControlTarget) cameraControlTarget {
	path := strings.TrimSpace(o.cameraAdmissionStatePath)
	if path == "" {
		return target
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return target
	}
	var state cameraAdmissionState
	if json.Unmarshal(body, &state) != nil || state.Schema != cameraAdmissionSchema || state.Status != "admitted" {
		return target
	}
	selected := strings.TrimSpace(state.Discovery.SelectedDevice)
	if !strings.HasPrefix(selected, "/dev/video") || filepath.Clean(selected) != selected {
		return target
	}
	previous := strings.TrimSpace(target.controlDevice)
	target.controlDevice = selected
	if strings.TrimSpace(target.sampleDevice) == previous {
		target.sampleDevice = selected
	}
	target.sampleCandidates = append([]string(nil), target.sampleCandidates...)
	for index, candidate := range target.sampleCandidates {
		if strings.TrimSpace(candidate) == previous {
			target.sampleCandidates[index] = selected
		}
	}
	if o.lastAdmissionDevice != selected {
		log.Printf("[video-broker][camera-control] physical camera rebound control=%s->%s fingerprint=%s", previous, selected, state.Discovery.CapabilityFingerprint)
		o.lastAdmissionDevice = selected
	}
	return target
}

// Compressed UVC controls are normally read-only because some camera firmwares
// pause their encoder on SET_CUR. Tone mapping cannot reconstruct samples that
// the module has already crushed into black or white, however. In that bounded
// case, after stable informative evidence, admit one residual camera adjustment
// at a time; cooldown/backoff and the broker watchdog contain faulty firmware.
// Raw capture can use the residual loop continuously. Unknown formats fail safe.
func (o *cameraControlOptimizer) updateHardwareControlSuppression(controlDevice string, sample cameraQualitySample, targetLuma int) bool {
	format := normalizeCaptureFormat(sample.CaptureFormat)
	suppressed := o.inPipelineToneMapping && !isRawCaptureFormat(format)
	if o.controlSuppressed == nil {
		o.controlSuppressed = map[string]string{}
	}
	previous, wasSuppressed := o.controlSuppressed[controlDevice]
	if suppressed {
		recovery := o.compressedRecoveryClass(controlDevice, sample, targetLuma)
		if recovery != "" {
			if o.recoveryCandidate == nil {
				o.recoveryCandidate = map[string]string{}
			}
			if o.recoveryCandidateCount == nil {
				o.recoveryCandidateCount = map[string]int{}
			}
			if o.recoveryCandidate[controlDevice] != recovery {
				o.recoveryCandidate[controlDevice] = recovery
				o.recoveryCandidateCount[controlDevice] = 1
			} else {
				o.recoveryCandidateCount[controlDevice]++
			}
			required := maxInt(o.stableSamples, 3)
			if o.recoveryCandidateCount[controlDevice] >= required {
				// Admit exactly one residual adjustment. A following adjustment
				// must earn a fresh stable-evidence window; otherwise a clipped
				// compressed stream could drive a fragile UVC firmware on every
				// optimizer tick until the control reaches its endpoint.
				delete(o.recoveryCandidate, controlDevice)
				delete(o.recoveryCandidateCount, controlDevice)
				delete(o.controlSuppressed, controlDevice)
				if o.admittedRecovery == nil {
					o.admittedRecovery = map[string]string{}
				}
				o.admittedRecovery[controlDevice] = recovery
				log.Printf("[video-broker][camera-control] hardware recovery admitted control=%s capture_format=%s reason=%s stable_samples=%d", controlDevice, displayCaptureFormat(format), recovery, required)
				return false
			}
		} else {
			delete(o.recoveryCandidate, controlDevice)
			delete(o.recoveryCandidateCount, controlDevice)
			delete(o.admittedRecovery, controlDevice)
		}
		if !wasSuppressed || previous != format {
			log.Printf("[video-broker][camera-control] hardware controls suppressed control=%s capture_format=%s reason=compressed_uvc_stable_signal", controlDevice, displayCaptureFormat(format))
		}
		o.controlSuppressed[controlDevice] = format
		return true
	}
	if wasSuppressed {
		delete(o.controlSuppressed, controlDevice)
		delete(o.manualPrepared, controlDevice)
		delete(o.admittedRecovery, controlDevice)
		log.Printf("[video-broker][camera-control] hardware controls resumed control=%s capture_format=%s reason=raw_capture_transport", controlDevice, format)
	}
	return false
}

func (o *cameraControlOptimizer) compressedRecoveryClass(controlDevice string, sample cameraQualitySample, target int) string {
	if target <= 0 {
		target = o.targetLuma
		if target <= 0 {
			target = 128
		}
	}
	hysteresis := maxInt(o.hysteresis, 8)
	highlightPressure := hasHighlightPressure(sample)
	if sample.SignalInformative && !highlightPressure && hasShadowPressure(sample) && sample.Luma < target-hysteresis {
		return "spatial_black_floor"
	}
	if sample.SignalInformative && highlightPressure && sample.Luma > target-hysteresis {
		return "spatial_white_floor"
	}
	dirty := o.residualDirty[controlDevice]
	direction := o.residualDirection[controlDevice]
	severeDarkness := target-sample.Luma > maxInt(3*hysteresis, 36)
	if dirty && direction < 0 && sample.SignalInformative && (!highlightPressure || severeDarkness) && sample.Luma < target-hysteresis {
		return "dark_residual_baseline_restore"
	}
	if dirty && direction > 0 && sample.SignalInformative && !hasShadowPressure(sample) && sample.Luma > target+hysteresis {
		return "bright_residual_baseline_restore"
	}
	if !sample.SignalInformative && (sample.BlackRatio >= 0.98 || sample.WhiteRatio >= 0.98) && len(o.baseline) > 0 {
		return "uninformative_signal_baseline_restore"
	}
	return ""
}

func (o *cameraControlOptimizer) ensureHardwareControlQualified(ctx context.Context, target cameraControlTarget) bool {
	if !o.deferHardwareProbe {
		return true
	}
	controlDevice := strings.TrimSpace(target.controlDevice)
	if qualified, known := o.controlQualified[controlDevice]; known {
		return qualified
	}
	capable, details := probeCameraControlOptimizerTargets(ctx, []cameraControlTarget{target}, o.controlBackend)
	if o.controlQualified == nil {
		o.controlQualified = map[string]bool{}
	}
	o.controlQualified[controlDevice] = capable
	if capable {
		log.Printf("[video-broker][camera-control] hardware controls qualified control=%s details=%s", controlDevice, details)
	} else {
		log.Printf("[video-broker][camera-control] hardware controls disabled control=%s details=%s", controlDevice, details)
	}
	return capable
}

func normalizeCaptureFormat(format string) string {
	format = strings.ToLower(strings.TrimSpace(format))
	return strings.NewReplacer("-", "", "_", "", ".", "", " ", "").Replace(format)
}

func displayCaptureFormat(format string) string {
	if format == "" {
		return "unknown"
	}
	return format
}

func isRawCaptureFormat(format string) bool {
	switch normalizeCaptureFormat(format) {
	case "yuyv", "yuy2", "uyvy", "yvyu", "vyuy", "nv12", "nv21", "nm12", "grey", "gray", "gray8", "rgb", "rgb24", "bgr", "bgr24":
		return true
	default:
		return false
	}
}

func (o *cameraControlOptimizer) sampleQualityWithFallback(ctx context.Context, target cameraControlTarget) (cameraQualitySample, string, error) {
	controlDevice := strings.TrimSpace(target.controlDevice)
	if controlDevice == "" {
		return cameraQualitySample{}, "", fmt.Errorf("empty control device")
	}
	candidates := dedupeNonEmpty(target.sampleCandidates)
	if active := strings.TrimSpace(o.activeSample[controlDevice]); active != "" {
		candidates = append([]string{active}, candidates...)
		candidates = dedupeNonEmpty(candidates)
	}
	errList := make([]string, 0, len(candidates))
	notReadyOnly := len(candidates) > 0
	for _, candidate := range candidates {
		if o.strictSample && candidate == controlDevice {
			continue
		}
		sample, err := o.sampler.SampleQuality(ctx, candidate, o.sampleWidth, o.sampleHeight)
		if err == nil {
			prev := strings.TrimSpace(o.activeSample[controlDevice])
			if prev != candidate {
				log.Printf("[video-broker][camera-control] sample source selected control=%s sample=%s (prev=%s)", controlDevice, candidate, prev)
			}
			o.activeSample[controlDevice] = candidate
			if sample.Source == "" {
				sample.Source = candidate
			}
			return sample, candidate, nil
		}
		if !errors.Is(err, errCameraSampleNotReady) {
			notReadyOnly = false
		}
		errList = append(errList, fmt.Sprintf("%s: %v", candidate, err))
	}
	if len(errList) == 0 && o.strictSample {
		return cameraQualitySample{}, "", fmt.Errorf("strict_sample=true no non-control sampling candidates for %s", controlDevice)
	}
	if notReadyOnly {
		return cameraQualitySample{}, "", fmt.Errorf("%w: control=%s candidates=%v", errCameraSampleNotReady, controlDevice, strings.Join(errList, " | "))
	}
	return cameraQualitySample{}, "", fmt.Errorf("sampling failed control=%s candidates=%v", controlDevice, strings.Join(errList, " | "))
}

func (o *cameraControlOptimizer) logFailRate(sampleDevice string) {
	total := o.attempts[sampleDevice]
	if total <= 0 {
		return
	}
	if total%o.reportEvery != 0 {
		return
	}
	failed := o.failed[sampleDevice]
	rate := (float64(failed) / float64(total)) * 100.0
	log.Printf("[video-broker][camera-control] sample_stats sample=%s attempts=%d failed=%d fail_rate=%.1f%%", sampleDevice, total, failed, rate)
}

func (o *cameraControlOptimizer) shouldSkipControlRead(controlDevice string) bool {
	if o.controlSkipUntil == nil {
		return false
	}
	until, ok := o.controlSkipUntil[controlDevice]
	return ok && time.Now().Before(until)
}

func (o *cameraControlOptimizer) handleControlReadSuccess(controlDevice string) {
	if o.controlFailures != nil {
		delete(o.controlFailures, controlDevice)
	}
	if o.controlSkipUntil != nil {
		delete(o.controlSkipUntil, controlDevice)
	}
}

func (o *cameraControlOptimizer) handleControlReadFailure(controlDevice string, err error) {
	if o.controlFailures == nil {
		o.controlFailures = map[string]int{}
	}
	if o.controlSkipUntil == nil {
		o.controlSkipUntil = map[string]time.Time{}
	}
	o.controlFailures[controlDevice]++
	failures := o.controlFailures[controlDevice]
	backoff := time.Duration(0)
	if isCommandTimeoutError(err) || failures >= 3 {
		backoff = 30 * time.Second
	}
	if failures >= 6 {
		backoff = 60 * time.Second
		o.controlFailures[controlDevice] = 0
	}
	if backoff > 0 {
		o.controlSkipUntil[controlDevice] = time.Now().Add(backoff)
		log.Printf("[video-broker][camera-control] controls failed control=%s failures=%d backoff=%s err=%v", controlDevice, failures, backoff, err)
		return
	}
	log.Printf("[video-broker][camera-control] controls failed control=%s failures=%d err=%v", controlDevice, failures, err)
}

func (o *cameraControlOptimizer) applyAdjustment(ctx context.Context, device string, controls map[string]v4l2ControlState, sample cameraQualitySample, targetLuma int) (string, error) {
	if reason := o.admittedRecovery[device]; strings.HasSuffix(reason, "baseline_restore") {
		delete(o.admittedRecovery, device)
		return o.restoreUVCResidualBaseline(ctx, device, controls)
	}
	delete(o.admittedRecovery, device)
	if (sample.BlackRatio >= 0.98 || sample.WhiteRatio >= 0.98) && !sample.SignalInformative && sample.LapVar < 1.0 && sample.Edge < 0.25 {
		return o.restoreUVCResidualBaseline(ctx, device, controls)
	}
	delta := targetLuma - sample.Luma
	needBrighten, shouldAdjust := desiredCameraControlDirection(sample, targetLuma, o.hysteresis)
	if !shouldAdjust {
		return "hold", nil
	}
	adjustmentCooldown := o.adjustmentCooldown(sample, targetLuma)
	if adjustmentCooldown > 0 {
		if last := o.lastAdjust[device]; !last.IsZero() && time.Since(last) < adjustmentCooldown {
			return "hold", nil
		}
	}
	magnitude := clampInt(absInt(delta)/8, 1, 8)

	if o.resolveControlProfile(controls) == "uvc_luma" {
		action, err := o.applyUVCAdjustment(ctx, device, controls, sample, targetLuma, needBrighten, magnitude)
		if err == nil && action != "hold" {
			o.lastAdjust[device] = time.Now()
		}
		return action, err
	}

	for _, step := range o.controlPlan(needBrighten) {
		state, ok := controls[step.name]
		if !ok {
			continue
		}
		inc := step.step * magnitude
		if !needBrighten {
			inc = -inc
		}
		next := clampInt(state.value+inc, state.min, state.max)
		if next == state.value {
			continue
		}
		if err := o.setControl(ctx, device, state.name, next); err != nil {
			return "", err
		}
		o.lastAdjust[device] = time.Now()
		direction := "darken"
		if needBrighten {
			direction = "brighten"
		}
		return fmt.Sprintf("%s %s %d->%d", direction, state.name, state.value, next), nil
	}

	return "hold", nil
}

func (o *cameraControlOptimizer) adjustmentCooldown(sample cameraQualitySample, targetLuma int) time.Duration {
	if o.cooldown <= 0 {
		return 0
	}
	delta := targetLuma - sample.Luma
	shadowClipping := severeShadowClipping(sample, targetLuma, o.hysteresis)
	severeHighlightClipping := -delta > maxInt(2*o.hysteresis, 20) && sample.WhiteRatio >= 0.02
	if shadowClipping || severeHighlightClipping {
		// A camera that has quantized a quarter of the frame into its black or
		// white floor needs sensor/ISP feedback promptly: a 30 second residual
		// cooldown can otherwise take many minutes to recover after startup or a
		// lighting transition. Never run faster than the sampling tick or 5 s.
		return minDuration(o.cooldown, maxDuration(o.tick, 5*time.Second))
	}
	if absInt(delta) > maxInt(2*o.hysteresis, 20) {
		return minDuration(o.cooldown, maxDuration(2*o.tick, 10*time.Second))
	}
	return o.cooldown
}

func severeShadowClipping(sample cameraQualitySample, targetLuma int, hysteresis int) bool {
	return targetLuma-sample.Luma > maxInt(2*hysteresis, 20) && hasShadowPressure(sample)
}

func desiredCameraControlDirection(sample cameraQualitySample, targetLuma int, hysteresis int) (bool, bool) {
	if hysteresis <= 0 {
		hysteresis = 8
	}
	delta := targetLuma - sample.Luma
	highlightPressure := hasHighlightPressure(sample)
	shadowPressure := hasShadowPressure(sample)

	// Preserve encoded highlight detail first. The local tone mapper can lift
	// retained shadows, but no post-ISP stage can reconstruct clipped samples.
	if highlightPressure && delta < hysteresis {
		return false, true
	}
	if !highlightPressure && shadowPressure && delta > -hysteresis {
		return true, true
	}
	if absInt(delta) <= hysteresis {
		return false, false
	}
	if delta > 0 {
		// A scene with retained highlight pressure is intentionally allowed to sit
		// below the global target; the zonal PAL owns the shadow lift.
		if highlightPressure && delta <= maxInt(3*hysteresis, 36) {
			return false, false
		}
		return true, true
	}
	if shadowPressure {
		return false, false
	}
	return false, true
}

func hasHighlightPressure(sample cameraQualitySample) bool {
	return sample.WhiteRatio >= 0.08 || maxRegionWhiteRatio(sample) >= 0.25
}

func hasShadowPressure(sample cameraQualitySample) bool {
	return sample.BlackRatio >= 0.25 || maxRegionBlackRatio(sample) >= 0.60
}

func maxRegionWhiteRatio(sample cameraQualitySample) float64 {
	maximum := 0.0
	for _, region := range sample.Regions {
		if region.Samples >= 16 && region.WhiteRatio > maximum {
			maximum = region.WhiteRatio
		}
	}
	return maximum
}

func maxRegionBlackRatio(sample cameraQualitySample) float64 {
	maximum := 0.0
	for _, region := range sample.Regions {
		if region.Samples >= 16 && region.BlackRatio > maximum {
			maximum = region.BlackRatio
		}
	}
	return maximum
}

func (o *cameraControlOptimizer) restoreUVCResidualBaseline(ctx context.Context, device string, controls map[string]v4l2ControlState) (string, error) {
	adjustment, available := o.selectUVCResidualBaselineRestore(controls)
	if !available {
		return "hold", nil
	}
	if err := o.setControl(ctx, device, adjustment.state.name, adjustment.next); err != nil {
		return "", err
	}
	o.recordResidualState(device, controls, adjustment)
	o.lastAdjust[device] = time.Now()
	return fmt.Sprintf("restore baseline %s %d->%d", adjustment.state.name, adjustment.state.value, adjustment.next), nil
}

func (o *cameraControlOptimizer) selectUVCResidualBaselineRestore(controls map[string]v4l2ControlState) (controlAdjustment, bool) {
	for _, name := range cameracontrol.ResidualNames() {
		state, available := controls[name]
		baseline, configured := o.baseline[name]
		if !available || !configured {
			continue
		}
		baseline = clampInt(baseline, state.min, state.max)
		if state.value == baseline {
			continue
		}
		return controlAdjustment{state: state, next: baseline}, true
	}
	return controlAdjustment{}, false
}

func (o *cameraControlOptimizer) applyUVCAdjustment(ctx context.Context, device string, controls map[string]v4l2ControlState, sample cameraQualitySample, targetLuma int, needBrighten bool, magnitude int) (string, error) {
	adjustment, ok := o.selectUVCControlAdjustment(controls, sample, targetLuma, needBrighten, magnitude)
	if !ok {
		return "hold", nil
	}
	if err := o.setControl(ctx, device, adjustment.state.name, adjustment.next); err != nil {
		return "", err
	}
	o.recordResidualState(device, controls, adjustment)
	direction := "darken"
	if needBrighten {
		direction = "brighten"
	}
	return fmt.Sprintf("%s uvc_luma %s %d->%d", direction, adjustment.state.name, adjustment.state.value, adjustment.next), nil
}

func (o *cameraControlOptimizer) selectUVCControlAdjustment(controls map[string]v4l2ControlState, sample cameraQualitySample, targetLuma int, needBrighten bool, magnitude int) (controlAdjustment, bool) {
	plan := []controlStep{
		{name: "gamma", step: 10},
		{name: "brightness", step: 4},
		{name: "backlight_compensation", step: 1},
		{name: "gain", step: 2},
	}
	if needBrighten && hasShadowPressure(sample) && targetLuma-sample.Luma > maxInt(2*o.hysteresis, 20) {
		// Tone controls cannot reconstruct values already crushed into the
		// sensor/codec black floor. Recover signal through residual controls,
		// but never switch the camera from automatic to manual exposure: UVC
		// standardizes the menu values, not the module firmware's working manual
		// timing range, and changing that domain can collapse a live stream.
		plan = []controlStep{
			{name: "gain", step: 2},
			{name: "backlight_compensation", step: 1},
			{name: "gamma", step: 10},
			{name: "brightness", step: 4},
		}
	}
	if !needBrighten {
		plan = []controlStep{
			{name: "gain", step: 2},
			{name: "backlight_compensation", step: 1},
			{name: "brightness", step: 4},
			{name: "gamma", step: 10},
		}
	}
	if adjustment, ok := o.selectDirectionalBaselineRestore(controls, plan, needBrighten, magnitude); ok {
		return adjustment, true
	}
	for _, step := range plan {
		state, ok := controls[step.name]
		if !ok {
			continue
		}
		if needBrighten && step.name == "gain" && !o.allowUVCGainIncrease(sample, targetLuma) {
			continue
		}
		next, ok := o.nextUVCControlValue(state, needBrighten, step, magnitude)
		if !ok || next == state.value {
			continue
		}
		return controlAdjustment{state: state, next: next}, true
	}
	return controlAdjustment{}, false
}

func (o *cameraControlOptimizer) selectDirectionalBaselineRestore(controls map[string]v4l2ControlState, plan []controlStep, needBrighten bool, magnitude int) (controlAdjustment, bool) {
	for _, step := range plan {
		state, available := controls[step.name]
		baseline, configured := o.baseline[step.name]
		if !available || !configured {
			continue
		}
		baseline = clampInt(baseline, state.min, state.max)
		if needBrighten && state.value >= baseline {
			continue
		}
		if !needBrighten && state.value <= baseline {
			continue
		}
		stepSize := o.uvcControlStep(state, step) * clampInt(magnitude, 1, 8)
		if stepSize <= 0 {
			stepSize = 1
		}
		next := state.value - stepSize
		if needBrighten {
			next = state.value + stepSize
			if next > baseline {
				next = baseline
			}
		} else if next < baseline {
			next = baseline
		}
		return controlAdjustment{state: state, next: next}, true
	}
	return controlAdjustment{}, false
}

func (o *cameraControlOptimizer) recordResidualState(device string, controls map[string]v4l2ControlState, adjustment controlAdjustment) {
	if o.residualDirty == nil {
		o.residualDirty = map[string]bool{}
	}
	if o.residualDirection == nil {
		o.residualDirection = map[string]int{}
	}
	hasBelow := false
	hasAbove := false
	for _, name := range cameracontrol.ResidualNames() {
		state, available := controls[name]
		baseline, configured := o.baseline[name]
		if !available || !configured {
			continue
		}
		value := state.value
		if name == adjustment.state.name {
			value = adjustment.next
		}
		baseline = clampInt(baseline, state.min, state.max)
		hasBelow = hasBelow || value < baseline
		hasAbove = hasAbove || value > baseline
	}
	o.residualDirty[device] = hasBelow || hasAbove
	switch {
	case hasBelow && !hasAbove:
		o.residualDirection[device] = -1
	case hasAbove && !hasBelow:
		o.residualDirection[device] = 1
	default:
		o.residualDirection[device] = 0
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func (o *cameraControlOptimizer) nextUVCControlValue(state v4l2ControlState, needBrighten bool, step controlStep, magnitude int) (int, bool) {
	minV, maxV := o.uvcControlBounds(state)
	stepSize := o.uvcControlStep(state, step)
	if stepSize <= 0 {
		stepSize = step.step
	}
	if stepSize <= 0 {
		stepSize = 1
	}
	inc := stepSize * clampInt(magnitude, 1, 8)
	if !needBrighten {
		inc = -inc
	}
	next := clampInt(state.value+inc, minV, maxV)
	if needBrighten && state.value >= maxV {
		return state.value, false
	}
	if !needBrighten && state.value <= minV {
		return state.value, false
	}
	return next, true
}

func (o *cameraControlOptimizer) uvcControlBounds(state v4l2ControlState) (int, int) {
	name := strings.ToLower(strings.TrimSpace(state.name))
	minV := state.min
	maxV := state.max
	_, hasBaseline := o.baseline[name]
	if v, ok := o.baseline[name]; ok {
		minV = clampInt(v, state.min, state.max)
	}
	if v, ok := o.controlMin[name]; ok {
		minV = clampInt(v, state.min, state.max)
	}
	if hasBaseline {
		// The declared range is the module-independent safety envelope. The
		// feedback loop remains bounded by target luma, hysteresis, normalized
		// steps and cooldown; a fixed percentage cap could leave dark modules
		// permanently outside the target range.
		maxV = state.max
	}
	if v, ok := o.controlMax[name]; ok {
		maxV = clampInt(v, state.min, state.max)
	}
	if maxV < minV {
		maxV = minV
	}
	return minV, maxV
}

func (o *cameraControlOptimizer) uvcControlStep(state v4l2ControlState, step controlStep) int {
	name := strings.ToLower(strings.TrimSpace(step.name))
	if v, ok := o.controlSteps[name]; ok && v > 0 {
		return v
	}
	if state.step > 0 {
		return state.step
	}
	return step.step
}

func (o *cameraControlOptimizer) allowUVCGainIncrease(sample cameraQualitySample, targetLuma int) bool {
	return targetLuma-sample.Luma > maxInt(3*o.hysteresis, 20)
}

func (o *cameraControlOptimizer) resolveControlProfile(controls map[string]v4l2ControlState) string {
	profile := strings.ToLower(strings.TrimSpace(o.controlProfile))
	if profile == "" || profile == "auto" {
		for _, name := range cameracontrol.ResidualNames() {
			if _, available := controls[name]; available {
				return "uvc_luma"
			}
		}
		return "generic"
	}
	return profile
}

func (o *cameraControlOptimizer) resolveTargetLuma(sampleDevice string, luma int) int {
	if o.mode != "profiles" {
		return o.targetLuma
	}
	prev := o.profileState[sampleDevice]
	if prev == "" {
		prev = selectInitialProfileMode(luma, o.thresholds)
		o.profileState[sampleDevice] = prev
		if target, ok := o.targetByMode[prev]; ok && target > 0 {
			return target
		}
		return o.targetLuma
	}
	next := selectProfileMode(luma, prev, o.thresholds)
	if next == "" {
		next = "day"
	}
	if next != prev {
		if o.stableSamples > 1 {
			if o.profileCandidate[sampleDevice] != next {
				o.profileCandidate[sampleDevice] = next
				o.profileCandidateCount[sampleDevice] = 1
				if target, ok := o.targetByMode[prev]; ok && target > 0 {
					return target
				}
				return o.targetLuma
			}
			o.profileCandidateCount[sampleDevice]++
			if o.profileCandidateCount[sampleDevice] < o.stableSamples {
				if target, ok := o.targetByMode[prev]; ok && target > 0 {
					return target
				}
				return o.targetLuma
			}
		}
		log.Printf("[video-broker][camera-control] profile switch sample=%s %s->%s luma=%d thresholds={sun:%d day:%d sunset:%d night:%d}", sampleDevice, prev, next, luma, o.thresholds.sun, o.thresholds.day, o.thresholds.sunset, o.thresholds.night)
		o.profileCandidate[sampleDevice] = ""
		o.profileCandidateCount[sampleDevice] = 0
	} else {
		o.profileCandidate[sampleDevice] = ""
		o.profileCandidateCount[sampleDevice] = 0
	}
	o.profileState[sampleDevice] = next
	if target, ok := o.targetByMode[next]; ok && target > 0 {
		return target
	}
	return o.targetLuma
}

func selectInitialProfileMode(luma int, t cameraProfileThresholds) string {
	if luma >= t.sun {
		return "sun"
	}
	if luma >= t.day {
		return "day"
	}
	if luma <= t.night {
		return "night"
	}
	if luma <= t.sunset {
		return "sunset"
	}
	return "day"
}

func selectProfileMode(luma int, prev string, t cameraProfileThresholds) string {
	switch prev {
	case "sun", "day", "sunset", "night":
	default:
		prev = "day"
	}
	if luma >= t.sun {
		return "sun"
	}
	if luma >= t.day {
		if prev == "sun" {
			return "sun"
		}
		return "day"
	}
	if luma <= t.night {
		return "night"
	}
	if luma <= t.sunset {
		if prev == "night" {
			return "night"
		}
		return "sunset"
	}
	if prev == "sunset" || prev == "night" {
		return "sunset"
	}
	return "day"
}

func (o *cameraControlOptimizer) controlPlan(darker bool) []controlStep {
	if o.mode == "manual" {
		if darker {
			return []controlStep{{name: "exposure", step: 200}, {name: "exposure_absolute", step: 8}, {name: "analogue_gain", step: 2}, {name: "gain", step: 2}, {name: "brightness", step: 1}}
		}
		return []controlStep{{name: "brightness", step: 1}, {name: "gain", step: 2}, {name: "analogue_gain", step: 2}, {name: "exposure_absolute", step: 8}, {name: "exposure", step: 200}}
	}

	if darker {
		return []controlStep{{name: "brightness", step: 1}, {name: "analogue_gain", step: 1}, {name: "gain", step: 1}, {name: "exposure_absolute", step: 5}, {name: "exposure", step: 120}}
	}
	return []controlStep{{name: "brightness", step: 1}, {name: "gain", step: 1}, {name: "analogue_gain", step: 1}, {name: "exposure_absolute", step: 5}, {name: "exposure", step: 120}}
}

func (o *cameraControlOptimizer) readControls(ctx context.Context, device string) (map[string]v4l2ControlState, error) {
	if o.controlBackend == nil {
		return nil, fmt.Errorf("missing camera-control backend")
	}
	cmdCtx, cancel := context.WithTimeout(ctx, cameraControlCmdTimeoutFast)
	defer cancel()
	capabilities, err := o.controlBackend.Capabilities(cmdCtx, device)
	if err != nil {
		return nil, fmt.Errorf("camera-control capability read through %s failed: %w", o.controlBackend.Name(), err)
	}

	controls := map[string]v4l2ControlState{}
	for name, capability := range capabilities {
		if !capability.Writable() || !capability.HasMin || !capability.HasMax || !capability.HasCurrent || capability.Max <= capability.Min {
			continue
		}
		step := 0
		if capability.HasStep && capability.Step > 0 {
			step = capability.Step
		}
		controls[name] = v4l2ControlState{
			name: name, min: capability.Min, max: capability.Max, step: step, value: capability.Current,
		}
	}
	if len(controls) == 0 {
		return nil, fmt.Errorf("no usable controls found")
	}
	return controls, nil
}

func (o *cameraControlOptimizer) setControl(ctx context.Context, device string, name string, value int) error {
	if o.controlBackend == nil {
		return fmt.Errorf("missing camera-control backend")
	}
	cmdCtx, cancel := context.WithTimeout(ctx, cameraControlCmdTimeoutFast)
	defer cancel()
	if err := o.controlBackend.Set(cmdCtx, device, name, value); err != nil {
		return fmt.Errorf("camera-control write through %s failed: %w", o.controlBackend.Name(), err)
	}
	return nil
}

func sampleLumaForFormat(ctx context.Context, device string, pixFormat string, width int, height int, useSetFmt bool) (int, error) {
	tmpDir, err := os.MkdirTemp("", "video-broker-camera-control-")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	framePath := filepath.Join(tmpDir, "frame.raw")
	args := []string{"-d", device}
	if useSetFmt {
		fmtArg := fmt.Sprintf("--set-fmt-video=width=%d,height=%d,pixelformat=%s", width, height, pixFormat)
		args = append(args, fmtArg)
	}
	args = append(args, "--stream-mmap=3", "--stream-count=1", "--stream-to="+framePath)
	if err := runCommandWithTimeout(ctx, cameraControlCmdTimeoutIO, "v4l2-ctl", args...); err != nil {
		return 0, err
	}

	frame, err := os.ReadFile(framePath)
	if err != nil {
		return 0, err
	}
	switch pixFormat {
	case "NV12", "NM12", "GREY":
		if len(frame) == 0 {
			return 0, fmt.Errorf("empty frame for %s", pixFormat)
		}
		yLen := len(frame)
		if pixFormat == "NV12" || pixFormat == "NM12" {
			yLen = (len(frame) * 2) / 3
		}
		if yLen <= 0 {
			return 0, fmt.Errorf("invalid frame length for %s", pixFormat)
		}
		maxSample := width * height
		if maxSample > 0 && yLen > maxSample {
			yLen = maxSample
		}
		return average(frame[:yLen]), nil
	case "YUYV":
		if len(frame) < 2 {
			return 0, fmt.Errorf("frame too short for YUYV")
		}
		buf := make([]byte, 0, len(frame)/2)
		for i := 0; i+1 < len(frame); i += 2 {
			buf = append(buf, frame[i])
		}
		return average(buf), nil
	case "UYVY":
		if len(frame) < 2 {
			return 0, fmt.Errorf("frame too short for UYVY")
		}
		buf := make([]byte, 0, len(frame)/2)
		for i := 1; i < len(frame); i += 2 {
			buf = append(buf, frame[i])
		}
		return average(buf), nil
	default:
		return 0, fmt.Errorf("unsupported pix format")
	}
}

func isStatisticsDevice(device string) bool {
	d := strings.ToLower(strings.TrimSpace(device))
	if d == "" {
		return false
	}
	return strings.Contains(d, "statistics") || strings.Contains(d, "iqtool")
}

func parseRKISPStatsLuma(payload []byte) (int, error) {
	if len(payload) < rkispAEOffset+rkispAEMaxV10 {
		return 0, fmt.Errorf("rkisp stats payload too short: %d", len(payload))
	}
	if len(payload) >= 4 {
		measType := binary.LittleEndian.Uint32(payload[:4])
		if (measType & 0x2) == 0 {
			return 0, fmt.Errorf("rkisp stats payload missing AE measurements")
		}
	}
	aeLen := rkispAEMaxV10
	if len(payload) >= rkispAEOffset+rkispAEMaxV12 {
		aeLen = rkispAEMaxV12
	}
	return average(payload[rkispAEOffset : rkispAEOffset+aeLen]), nil
}

func cloneIntMap(in map[string]int) map[string]int {
	out := map[string]int{}
	for k, v := range in {
		key := strings.ToLower(strings.TrimSpace(k))
		if key == "" {
			continue
		}
		out[key] = v
	}
	return out
}

func listRuntimeStatisticsCandidates() []string {
	root := "/sys/class/video4linux"
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	type candidate struct {
		order int
		path  string
	}
	candidates := make([]candidate, 0)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "video") {
			continue
		}
		labelBytes, readErr := os.ReadFile(filepath.Join(root, name, "name"))
		if readErr != nil {
			continue
		}
		label := strings.ToLower(strings.TrimSpace(string(labelBytes)))
		if !strings.Contains(label, "rkisp-statistics") && !strings.Contains(label, "iqtool") {
			continue
		}
		order := 9999
		if len(name) > 5 {
			if n, convErr := strconv.Atoi(name[5:]); convErr == nil {
				order = n
			}
		}
		candidates = append(candidates, candidate{
			order: order,
			path:  filepath.Join("/dev", name),
		})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].order == candidates[j].order {
			return candidates[i].path < candidates[j].path
		}
		return candidates[i].order < candidates[j].order
	})
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.path)
	}
	return out
}

func average(values []byte) int {
	if len(values) == 0 {
		return 0
	}
	total := 0
	for _, v := range values {
		total += int(v)
	}
	return int(math.Round(float64(total) / float64(len(values))))
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func clampInt(v int, min int, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func runCommandWithTimeout(ctx context.Context, timeout time.Duration, command string, args ...string) error {
	runCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		runCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	cmd := exec.CommandContext(runCtx, command, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		trimmed := strings.TrimSpace(string(out))
		if timeout > 0 && errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			if trimmed == "" {
				return fmt.Errorf("%s timed out after %s: %w", command, timeout, context.DeadlineExceeded)
			}
			return fmt.Errorf("%s timed out after %s: %w: %s", command, timeout, context.DeadlineExceeded, trimmed)
		}
		if trimmed == "" {
			return err
		}
		return fmt.Errorf("%w: %s", err, trimmed)
	}
	return nil
}

func isCommandTimeoutError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded)
}
