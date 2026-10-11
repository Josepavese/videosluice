package source

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	TransportVendorHBN                = "vendor_hbn"
	TransportV4L2                     = "v4l2"
	TransportV4L2UVC                  = "v4l2_uvc"
	TransportLibcamera                = "libcamera"
	ComponentRetryInPlace             = "in_place"
	ComponentRetryNextNaturalEpoch    = "next_natural_epoch"
	DefaultV4L2CaptureSelectTimeoutMs = 2000
	DefaultV4L2CaptureMaxTimeouts     = 3
	maxHBNOutputSurfaces              = 5
)

// BrokerMuxPlan is the provider-facing contract for a source adapter that emits
// the broker's private worker mux. Stream identifiers describe output roles,
// never a particular application or consumer.
type BrokerMuxPlan struct {
	Transport  string
	SourceID   string
	MediaEpoch uint64
	// AdmittedSurfaces is the de-duplicated set of output geometries authorized
	// for this source by the central routing catalog. Hardware sensor stacks may
	// preconfigure these surfaces without receiving stream endpoints or option
	// identities.
	AdmittedSurfaces []BrokerMuxSurface

	HBNBridgePath       string
	V4L2BridgePath      string
	GStreamerBridgePath string

	CaptureDevicePath                string
	CaptureFormat                    string
	CaptureCodecPreference           string
	CaptureCodecPolicy               string
	CapturePolicy                    string
	CaptureAspectPolicy              string
	CaptureModeExclusions            []V4L2CaptureModeExclusion
	AllowScaledYUYV                  bool
	PreferExactYUYV                  bool
	VideoOrientation                 string
	WorkerProvider                   string
	EncodeProvider                   string
	ArchiveEncodeProvider            string
	DecodeProvider                   string
	RequireHWCodec                   bool
	AllowSWFallback                  bool
	H264EncodeHWAvailable            bool
	H264DecodeHWAvailable            bool
	H265EncodeHWAvailable            bool
	H265DecodeHWAvailable            bool
	MJPEGDecodeHWAvailable           bool
	MJPEGDecodeInPlaceRetrySupported bool
	SensorPrefix                     string
	CameraHostIndex                  int

	CaptureWidth           int
	CaptureHeight          int
	CaptureFPS             int
	LiveEnabled            bool
	LiveCodec              string
	AnalysisEnabled        bool
	AnalysisStreamID       string
	ArchiveEnabled         bool
	ArchiveWidth           int
	ArchiveHeight          int
	ArchiveWidthAlignment  int
	ArchiveHeightAlignment int
	ArchiveFPS             int
	ArchiveRateControl     string
	ArchiveContent         string
	ArchiveCodec           string
	MainWidth              int
	MainHeight             int
	MainWidthAlignment     int
	MainHeightAlignment    int
	MainFPS                int
	MainActive             bool
	SubWidth               int
	SubHeight              int
	SubFPS                 int
	WarmupSec              int
	LiveBitrateKbps        int
	ArchiveBitrateKbps     int
	CaptureSelectTimeoutMs int
	CaptureMaxTimeouts     int

	QualityMonitorEnabled         bool
	QualityMonitorReportEvery     int
	QualityMonitorSampleStride    int
	QualityMonitorStatePath       string
	ToneMappingEnabled            bool
	ToneMappingTarget             int
	ToneMappingHysteresis         int
	ToneMappingMode               string
	ToneMappingAccelerator        string
	ToneMappingGridColumns        int
	ToneMappingGridRows           int
	ToneMappingStatisticsHz       int
	ToneMappingMaxShadowGain      float64
	ToneMappingMaxHighlightReduce float64
	ToneMappingTemporalAlpha      float64
	ToneMappingNoiseGuard         bool
}

type BrokerMuxSurface struct {
	Width  int
	Height int
}

type BrokerMuxCapabilityReport struct {
	AdapterID                     string
	InputFormat                   string
	EstimatedCost                 string
	RequiresSoftwareDecode        bool
	RequiresSoftwareResize        bool
	UsesHardwareEncode            bool
	UsesHardwareSensorStack       bool
	AcceptsRuntimeControlProtocol bool
	SupportsRuntimeLiveControl    bool
	LiveControlMode               string
	SupportsToneMapping           bool
	ToneMappingBackend            string
	ComponentRetries              []BrokerMuxComponentRetryCapability
	Notes                         []string
}

// BrokerMuxComponentRetryCapability describes how a running source adapter can
// retry one failed accelerator without implying anything about a board or
// camera model. Missing entries deliberately mean next_natural_epoch: the
// broker keeps the continuity graph alive and lets the adapter reconsider the
// component only when an epoch ends for an independent reason.
type BrokerMuxComponentRetryCapability struct {
	ComponentID string
	Stage       string
	Format      string
	Scope       string
}

func (report BrokerMuxCapabilityReport) ComponentRetryScope(componentID string) string {
	componentID = strings.TrimSpace(componentID)
	for _, retry := range report.ComponentRetries {
		if strings.TrimSpace(retry.ComponentID) == componentID && retry.Scope == ComponentRetryInPlace {
			return ComponentRetryInPlace
		}
	}
	return ComponentRetryNextNaturalEpoch
}

type V4L2FormatCapability struct {
	PixelFormat string
	Width       int
	Height      int
	FPS         float64
}

// V4L2CaptureModeExclusion quarantines one advertised mode after the live
// broker proves that the camera cannot sustain its contract.  The capability
// fingerprint keeps runtime evidence bound to the physical module and its
// advertised mode catalog instead of to an unstable /dev/videoN path.
type V4L2CaptureModeExclusion struct {
	CapabilityFingerprint string `json:"capability_fingerprint"`
	PixelFormat           string `json:"pixel_format"`
	Width                 int    `json:"width"`
	Height                int    `json:"height"`
	FPS                   int    `json:"fps"`
}

type V4L2InputFormatDecision struct {
	Format                 string
	Reason                 string
	UsedFallback           bool
	RequiresSoftwareDecode bool
	CaptureWidth           int
	CaptureHeight          int
	CaptureFPS             int
	ResizedCapture         bool
	Notes                  []string
}

type BrokerMuxAdapter interface {
	ID() string
	Supports(BrokerMuxPlan) bool
	BuildCommand(context.Context, BrokerMuxPlan) (*exec.Cmd, error)
}

func BuildBrokerMuxCommand(ctx context.Context, plan BrokerMuxPlan) (*exec.Cmd, string, error) {
	adapter, err := ResolveBrokerMuxAdapter(plan)
	if err != nil {
		return nil, "", err
	}
	cmd, err := adapter.BuildCommand(ctx, plan)
	if err != nil {
		return nil, adapter.ID(), err
	}
	return cmd, adapter.ID(), nil
}

func ResolveBrokerMuxAdapter(plan BrokerMuxPlan) (BrokerMuxAdapter, error) {
	adapters := []BrokerMuxAdapter{
		hbnMuxAdapter{},
		libcameraMuxAdapter{},
		v4l2UVCMuxAdapter{},
		v4l2MuxAdapter{},
	}
	for _, adapter := range adapters {
		if adapter.Supports(plan) {
			return adapter, nil
		}
	}
	return nil, fmt.Errorf("unsupported broker video transport %q", strings.TrimSpace(plan.Transport))
}

func DescribeBrokerMuxPlan(plan BrokerMuxPlan) (BrokerMuxCapabilityReport, error) {
	adapter, err := ResolveBrokerMuxAdapter(plan)
	if err != nil {
		return BrokerMuxCapabilityReport{}, err
	}
	report := BrokerMuxCapabilityReport{
		AdapterID:     adapter.ID(),
		InputFormat:   strings.ToLower(strings.TrimSpace(plan.CaptureFormat)),
		EstimatedCost: "medium",
	}
	if report.InputFormat == "" {
		report.InputFormat = "default"
	}
	orientation := NormalizeVideoOrientation(plan.VideoOrientation)
	if orientation == "" {
		orientation = VideoOrientationNormal
	}

	switch adapter.ID() {
	case TransportVendorHBN:
		report.EstimatedCost = "low"
		report.AcceptsRuntimeControlProtocol = workerIdentityConfigured(plan) && len(plan.AdmittedSurfaces) > 0
		report.UsesHardwareEncode = true
		report.UsesHardwareSensorStack = true
		report.LiveControlMode = "backend_static"
		report.SupportsToneMapping = true
		report.ToneMappingBackend = "native_nv12"
		report.Notes = append(report.Notes,
			"vendor sensor stack emits broker mux without UVC software decode",
			"live and archive own independent hardware encoders, cadence and codec state",
			"routed workers transact role branches without replacing the capture owner",
		)
	case TransportLibcamera:
		report.EstimatedCost = "medium"
		report.AcceptsRuntimeControlProtocol = workerIdentityConfigured(plan)
		report.UsesHardwareEncode = plan.H264EncodeHWAvailable || plan.H265EncodeHWAvailable
		report.UsesHardwareSensorStack = true
		report.LiveControlMode = "backend_static"
		report.SupportsToneMapping = true
		report.ToneMappingBackend = "portable_nv12"
		report.RequiresSoftwareResize = plan.SubWidth != plan.CaptureWidth || plan.SubHeight != plan.CaptureHeight ||
			plan.MainWidth != plan.CaptureWidth || plan.MainHeight != plan.CaptureHeight
		report.Notes = append(report.Notes,
			"libcamera owns sensor ISP and raw capture negotiation",
			"portable GStreamer graph emits the same live, analysis and archive contracts",
		)
		report.Notes = append(report.Notes, "product videobrokertone conditions NV12 once before portable fan-out")
	case TransportV4L2UVC, TransportV4L2:
		report.UsesHardwareEncode = plan.H264EncodeHWAvailable || plan.H265EncodeHWAvailable
		usesGStreamerWorker := strings.TrimSpace(plan.WorkerProvider) == "gstreamer"
		report.AcceptsRuntimeControlProtocol = !usesGStreamerWorker ||
			workerIdentityConfigured(plan)
		report.SupportsToneMapping = true
		if !usesGStreamerWorker {
			report.ToneMappingBackend = "native_nv12"
		} else {
			report.ToneMappingBackend = "portable_nv12"
			report.Notes = append(report.Notes, "product videobrokertone conditions NV12 once before portable fan-out")
		}
		inputFormat := strings.ToLower(strings.TrimSpace(plan.CaptureFormat))
		switch {
		case usesGStreamerWorker:
			report.LiveControlMode = "backend_static"
		case inputFormat == normalizedLiveCodec(plan.LiveCodec) &&
			(inputFormat == "h264" || inputFormat == "h265") && !plan.ToneMappingEnabled &&
			plan.MainWidth == plan.CaptureWidth && plan.MainHeight == plan.CaptureHeight &&
			plan.MainFPS == plan.CaptureFPS && NormalizeVideoOrientation(plan.VideoOrientation) == VideoOrientationNormal:
			report.LiveControlMode = "native_passthrough_static"
		default:
			report.SupportsRuntimeLiveControl = true
			report.LiveControlMode = "persistent_encoder_epoch"
		}
		report.RequiresSoftwareResize = plan.SubWidth != plan.CaptureWidth || plan.SubHeight != plan.CaptureHeight ||
			plan.MainWidth != plan.CaptureWidth || plan.MainHeight != plan.CaptureHeight
		if policy := normalizeV4L2CapturePolicy(plan.CapturePolicy); policy != V4L2CapturePolicyExact {
			report.Notes = append(report.Notes, "capture policy="+policy)
		}
		report.Notes = append(report.Notes,
			"codec policy="+normalizeV4L2CaptureCodecPolicy(plan.CaptureCodecPolicy),
			"aspect policy="+normalizeV4L2CaptureAspectPolicy(plan.CaptureAspectPolicy),
		)
		if strings.EqualFold(report.InputFormat, "auto") && !plan.PreferExactYUYV {
			report.Notes = append(report.Notes, "exact YUYV auto switch disabled")
		}
		if plan.AllowScaledYUYV {
			report.Notes = append(report.Notes, "scaled YUYV capture is experimental and changes the shared capture contract")
		}
		switch report.InputFormat {
		case "mjpeg", "mjpg", "jpeg":
			report.RequiresSoftwareDecode = !plan.MJPEGDecodeHWAvailable
			inPlaceRetrySupported := plan.MJPEGDecodeInPlaceRetrySupported || plan.MJPEGDecodeHWAvailable
			if plan.MJPEGDecodeHWAvailable {
				report.EstimatedCost = "medium"
				report.Notes = append(report.Notes, "UVC MJPEG is decoded by the declared hardware JPU before hardware encode")
			} else {
				report.EstimatedCost = "high"
				report.Notes = append(report.Notes, "UVC MJPEG requires software decode before hardware encode")
			}
			if inPlaceRetrySupported && !usesGStreamerWorker {
				report.ComponentRetries = append(report.ComponentRetries, BrokerMuxComponentRetryCapability{
					ComponentID: "native.jpu",
					Stage:       "decode",
					Format:      "mjpeg",
					Scope:       ComponentRetryInPlace,
				})
			}
		case "auto":
			report.Notes = append(report.Notes, "UVC input format will be probed before bridge startup")
		case "h264", "h265", "nv12":
			report.EstimatedCost = "low"
		case "yuyv", "yuy2":
			report.EstimatedCost = "medium"
			report.Notes = append(report.Notes, "UVC YUYV avoids MJPEG decode but may cap camera FPS at higher resolutions")
		default:
			report.Notes = append(report.Notes, "unknown UVC input format; runtime bridge will validate device support")
		}
		if report.RequiresSoftwareResize {
			report.Notes = append(report.Notes, "bridge must generate at least one resized output")
		}
		if orientation != VideoOrientationNormal {
			report.RequiresSoftwareResize = true
			report.Notes = append(report.Notes, "bridge applies video orientation before fan-out")
		}
		if plan.QualityMonitorEnabled {
			report.Notes = append(report.Notes, "quality monitor enabled on sub NV12 stream")
		}
	default:
		report.LiveControlMode = "backend_static"
		report.Notes = append(report.Notes, "unknown adapter capability class")
	}
	return report, nil
}

type hbnMuxAdapter struct{}

func (hbnMuxAdapter) ID() string { return "vendor_hbn" }

func (hbnMuxAdapter) Supports(plan BrokerMuxPlan) bool {
	return strings.EqualFold(strings.TrimSpace(plan.Transport), TransportVendorHBN)
}

func (hbnMuxAdapter) BuildCommand(ctx context.Context, plan BrokerMuxPlan) (*exec.Cmd, error) {
	if plan.ArchiveEnabled && normalizedArchiveRateControl(plan.ArchiveRateControl) != "cbr" {
		return nil, errors.New("vendor HBN bridge does not expose configurable archive AVBR")
	}
	if strings.TrimSpace(plan.HBNBridgePath) == "" {
		return nil, errors.New("missing HBN mux bridge path")
	}
	if strings.TrimSpace(plan.SensorPrefix) == "" {
		return nil, errors.New("vendor HBN transport requires an explicit sensor identifier")
	}
	if strings.TrimSpace(plan.WorkerProvider) != "native_sp" {
		return nil, errors.New("vendor HBN transport requires the native_sp worker")
	}
	if !plan.LiveEnabled && !plan.AnalysisEnabled && !plan.ArchiveEnabled {
		return nil, errors.New("vendor HBN transport requires at least one enabled output")
	}
	archiveCodec := normalizedArchiveCodec(plan.ArchiveCodec)
	if plan.ArchiveEnabled && !declaredHardwareEncoderAvailable(plan, archiveCodec) {
		return nil, fmt.Errorf("vendor HBN transport has no declared hardware %s archive encoder", strings.ToUpper(archiveCodec))
	}
	liveCodec := normalizedLiveCodec(plan.LiveCodec)
	if plan.LiveEnabled && !declaredHardwareEncoderAvailable(plan, liveCodec) {
		return nil, fmt.Errorf("vendor HBN transport has no declared hardware %s live encoder", strings.ToUpper(liveCodec))
	}
	mainWidth, mainHeight := fitEncoderOutputGeometry(
		plan.MainWidth,
		plan.MainHeight,
		plan.MainWidthAlignment,
		plan.MainHeightAlignment,
	)
	archiveWidth, archiveHeight := FitArchiveGeometryWithAlignment(
		plan.ArchiveWidth,
		plan.ArchiveHeight,
		plan.CaptureWidth,
		plan.CaptureHeight,
		plan.ArchiveWidthAlignment,
		plan.ArchiveHeightAlignment,
	)
	archiveFPS := plan.ArchiveFPS
	if archiveFPS <= 0 {
		archiveFPS = plan.CaptureFPS
	}
	archiveBitrate := plan.ArchiveBitrateKbps
	if archiveBitrate <= 0 {
		archiveBitrate = plan.LiveBitrateKbps
	}
	if plan.QualityMonitorEnabled && plan.AnalysisEnabled {
		mainCapturesSource := plan.LiveEnabled && mainWidth == plan.CaptureWidth && mainHeight == plan.CaptureHeight
		archiveCapturesSource := plan.ArchiveEnabled && archiveWidth == plan.CaptureWidth && archiveHeight == plan.CaptureHeight
		if !mainCapturesSource && !archiveCapturesSource {
			return nil, errors.New("vendor HBN quality monitor requires a capture-sized live or archive channel")
		}
	}
	surfacePlan := plan
	surfacePlan.MainWidth, surfacePlan.MainHeight = mainWidth, mainHeight
	surfacePlan.ArchiveWidth, surfacePlan.ArchiveHeight = archiveWidth, archiveHeight
	admittedSurfaces, err := normalizeHBNAdmittedSurfaces(surfacePlan)
	if err != nil {
		return nil, err
	}
	args := []string{
		"--sensor-prefix", plan.SensorPrefix,
		"--live-enabled", strconv.FormatBool(plan.LiveEnabled),
		"--live-codec", liveCodec,
		"--analysis-enabled", strconv.FormatBool(plan.AnalysisEnabled),
		"--archive-enabled", strconv.FormatBool(plan.ArchiveEnabled),
		"--video-index", strconv.Itoa(plan.CameraHostIndex),
		"--sensor-width", strconv.Itoa(plan.CaptureWidth),
		"--sensor-height", strconv.Itoa(plan.CaptureHeight),
		"--sub-width", strconv.Itoa(plan.SubWidth),
		"--sub-height", strconv.Itoa(plan.SubHeight),
		"--sub-fps", strconv.Itoa(plan.SubFPS),
		"--output-width", strconv.Itoa(mainWidth),
		"--output-height", strconv.Itoa(mainHeight),
		"--main-fps", strconv.Itoa(plan.MainFPS),
		"--archive-width", strconv.Itoa(archiveWidth),
		"--archive-height", strconv.Itoa(archiveHeight),
		"--archive-fps", strconv.Itoa(archiveFPS),
		"--archive-content", normalizedArchiveContent(plan.ArchiveContent),
		"--archive-codec", archiveCodec,
		"--fps", strconv.Itoa(plan.CaptureFPS),
		"--warmup-sec", strconv.Itoa(plan.WarmupSec),
		"--live-encode-bitrate-kbps", strconv.Itoa(plan.LiveBitrateKbps),
		"--archive-encode-bitrate-kbps", strconv.Itoa(archiveBitrate),
		"--video-orientation", normalizeOrientationForCommand(plan.VideoOrientation),
	}
	for _, surface := range admittedSurfaces {
		args = append(args, "--admitted-surface", fmt.Sprintf("%dx%d", surface.Width, surface.Height))
	}
	identityArgs, err := workerIdentityArgs(plan)
	if err != nil {
		return nil, err
	}
	if len(identityArgs) > 0 && len(admittedSurfaces) == 0 {
		return nil, errors.New("routed vendor HBN worker requires admitted catalog surfaces")
	}
	args = append(args, identityArgs...)
	if plan.QualityMonitorEnabled {
		args = append(args, "--quality-monitor-enabled", "true")
		if plan.QualityMonitorReportEvery > 0 {
			args = append(args, "--quality-monitor-report-every", strconv.Itoa(plan.QualityMonitorReportEvery))
		}
		if plan.QualityMonitorSampleStride > 0 {
			args = append(args, "--quality-monitor-sample-stride", strconv.Itoa(plan.QualityMonitorSampleStride))
		}
		if path := strings.TrimSpace(plan.QualityMonitorStatePath); path != "" {
			args = append(args, "--quality-monitor-state-path", path)
		}
	}
	args = append(args, brokerToneMappingArgs(plan)...)
	return exec.CommandContext(ctx, plan.HBNBridgePath, args...), nil
}

func normalizeHBNAdmittedSurfaces(plan BrokerMuxPlan) ([]BrokerMuxSurface, error) {
	if len(plan.AdmittedSurfaces) == 0 {
		return nil, nil
	}
	seen := make(map[BrokerMuxSurface]bool, len(plan.AdmittedSurfaces))
	surfaces := make([]BrokerMuxSurface, 0, len(plan.AdmittedSurfaces))
	for _, surface := range plan.AdmittedSurfaces {
		if surface.Width <= 0 || surface.Height <= 0 || surface.Width%2 != 0 || surface.Height%2 != 0 {
			return nil, fmt.Errorf("vendor HBN admitted surface is invalid: %dx%d", surface.Width, surface.Height)
		}
		if seen[surface] {
			continue
		}
		seen[surface] = true
		surfaces = append(surfaces, surface)
	}
	if len(surfaces) > maxHBNOutputSurfaces {
		return nil, fmt.Errorf(
			"vendor HBN catalog requires %d unique output surfaces; hardware limit is %d",
			len(surfaces), maxHBNOutputSurfaces,
		)
	}
	require := func(enabled bool, label string, width, height int) error {
		if !enabled {
			return nil
		}
		if !seen[BrokerMuxSurface{Width: width, Height: height}] {
			return fmt.Errorf(
				"vendor HBN active %s surface %dx%d is outside the routed admission catalog",
				label, width, height,
			)
		}
		return nil
	}
	if err := require(plan.AnalysisEnabled, "analysis", plan.SubWidth, plan.SubHeight); err != nil {
		return nil, err
	}
	if err := require(plan.LiveEnabled, "live", plan.MainWidth, plan.MainHeight); err != nil {
		return nil, err
	}
	if err := require(plan.ArchiveEnabled, "archive", plan.ArchiveWidth, plan.ArchiveHeight); err != nil {
		return nil, err
	}
	sort.Slice(surfaces, func(i, j int) bool {
		if surfaces[i].Width != surfaces[j].Width {
			return surfaces[i].Width < surfaces[j].Width
		}
		return surfaces[i].Height < surfaces[j].Height
	})
	return surfaces, nil
}

// libcameraMuxAdapter keeps sensor discovery/ISP ownership in libcamera while
// publishing the same fixed broker graph as every other adapter. The source
// selector is optional because libcamera can deterministically select its only
// available camera; when present it is passed as camera-name verbatim.
type libcameraMuxAdapter struct{}

func (libcameraMuxAdapter) ID() string { return TransportLibcamera }

func (libcameraMuxAdapter) Supports(plan BrokerMuxPlan) bool {
	return strings.EqualFold(strings.TrimSpace(plan.Transport), TransportLibcamera)
}

func (libcameraMuxAdapter) BuildCommand(ctx context.Context, plan BrokerMuxPlan) (*exec.Cmd, error) {
	if plan.ArchiveEnabled && normalizedArchiveRateControl(plan.ArchiveRateControl) != "cbr" {
		return nil, errors.New("portable GStreamer bridge does not expose configurable archive AVBR")
	}
	bridgePath := strings.TrimSpace(plan.GStreamerBridgePath)
	if bridgePath == "" {
		return nil, errors.New("missing portable GStreamer mux bridge")
	}
	format := strings.ToLower(strings.TrimSpace(plan.CaptureFormat))
	if format == "" || format == "auto" {
		format = "nv12"
	}
	if format != "nv12" {
		return nil, fmt.Errorf("libcamera transport requires raw NV12 capture, got %q", format)
	}
	if strings.TrimSpace(plan.WorkerProvider) != "gstreamer" {
		return nil, errors.New("libcamera transport requires the portable GStreamer worker")
	}
	if !plan.LiveEnabled && !plan.AnalysisEnabled && !plan.ArchiveEnabled {
		return nil, errors.New("libcamera transport requires at least one enabled output")
	}
	archiveCodec := normalizedArchiveCodec(plan.ArchiveCodec)
	encodeProvider := strings.TrimSpace(plan.EncodeProvider)
	if plan.LiveEnabled {
		var err error
		liveCodec := normalizedLiveCodec(plan.LiveCodec)
		encodeProvider, err = selectGStreamerEncodeProvider(
			encodeProvider,
			liveCodec,
			declaredHardwareEncoderAvailable(plan, liveCodec),
			plan.AllowSWFallback,
		)
		if err != nil {
			return nil, fmt.Errorf("live encoder: %w", err)
		}
	}
	archiveEncodeProvider := firstNonEmptyString(plan.ArchiveEncodeProvider, plan.EncodeProvider)
	if plan.ArchiveEnabled {
		var err error
		archiveEncodeProvider, err = selectGStreamerEncodeProvider(
			archiveEncodeProvider,
			archiveCodec,
			declaredHardwareEncoderAvailable(plan, archiveCodec),
			plan.AllowSWFallback,
		)
		if err != nil {
			return nil, fmt.Errorf("archive encoder: %w", err)
		}
	}
	mainWidth, mainHeight := fitEncoderOutputGeometry(
		plan.MainWidth,
		plan.MainHeight,
		plan.MainWidthAlignment,
		plan.MainHeightAlignment,
	)
	archiveWidth, archiveHeight := FitArchiveGeometryWithAlignment(
		plan.ArchiveWidth,
		plan.ArchiveHeight,
		plan.CaptureWidth,
		plan.CaptureHeight,
		plan.ArchiveWidthAlignment,
		plan.ArchiveHeightAlignment,
	)
	archiveFPS := positiveOrSource(plan.ArchiveFPS, plan.CaptureFPS)
	archiveBitrate := plan.ArchiveBitrateKbps
	if archiveBitrate <= 0 {
		archiveBitrate = plan.LiveBitrateKbps
	}
	args := []string{
		"--source-transport", TransportLibcamera,
		"--device", strings.TrimSpace(plan.CaptureDevicePath),
		"--input-format", format,
		"--live-enabled", strconv.FormatBool(plan.LiveEnabled),
		"--live-codec", normalizedLiveCodec(plan.LiveCodec),
		"--analysis-enabled", strconv.FormatBool(plan.AnalysisEnabled),
		"--archive-enabled", strconv.FormatBool(plan.ArchiveEnabled),
		"--capture-width", strconv.Itoa(plan.CaptureWidth),
		"--capture-height", strconv.Itoa(plan.CaptureHeight),
		"--sub-width", strconv.Itoa(plan.SubWidth),
		"--sub-height", strconv.Itoa(plan.SubHeight),
		"--sub-fps", strconv.Itoa(plan.SubFPS),
		"--output-width", strconv.Itoa(mainWidth),
		"--output-height", strconv.Itoa(mainHeight),
		"--main-fps", strconv.Itoa(plan.MainFPS),
		"--archive-width", strconv.Itoa(archiveWidth),
		"--archive-height", strconv.Itoa(archiveHeight),
		"--archive-fps", strconv.Itoa(archiveFPS),
		"--archive-content", normalizedArchiveContent(plan.ArchiveContent),
		"--archive-codec", archiveCodec,
		"--main-active", strconv.FormatBool(plan.MainActive),
		"--fps", strconv.Itoa(plan.CaptureFPS),
		"--live-encode-bitrate-kbps", strconv.Itoa(plan.LiveBitrateKbps),
		"--archive-encode-bitrate-kbps", strconv.Itoa(archiveBitrate),
		"--video-orientation", normalizeOrientationForCommand(plan.VideoOrientation),
		"--aspect-policy", normalizeV4L2CaptureAspectPolicy(plan.CaptureAspectPolicy),
		"--warmup-sec", strconv.Itoa(plan.WarmupSec),
		"--encode-provider", encodeProvider,
		"--archive-encode-provider", archiveEncodeProvider,
		"--decode-provider", strings.TrimSpace(plan.DecodeProvider),
		"--allow-sw-fallback", strconv.FormatBool(plan.AllowSWFallback),
	}
	identityArgs, err := workerIdentityArgs(plan)
	if err != nil {
		return nil, err
	}
	args = append(args, identityArgs...)
	if plan.QualityMonitorEnabled {
		args = append(args, "--quality-monitor-enabled", "true")
		if plan.QualityMonitorReportEvery > 0 {
			args = append(args, "--quality-monitor-report-every", strconv.Itoa(plan.QualityMonitorReportEvery))
		}
		if plan.QualityMonitorSampleStride > 0 {
			args = append(args, "--quality-monitor-sample-stride", strconv.Itoa(plan.QualityMonitorSampleStride))
		}
		if path := strings.TrimSpace(plan.QualityMonitorStatePath); path != "" {
			args = append(args, "--quality-monitor-state-path", path)
		}
	}
	args = append(args, brokerToneMappingArgs(plan)...)
	return exec.CommandContext(ctx, bridgePath, args...), nil
}

type v4l2UVCMuxAdapter struct{}

func (v4l2UVCMuxAdapter) ID() string { return "v4l2_uvc" }

func (v4l2UVCMuxAdapter) Supports(plan BrokerMuxPlan) bool {
	return strings.EqualFold(strings.TrimSpace(plan.Transport), TransportV4L2UVC)
}

// v4l2MuxAdapter covers CSI and other non-USB capture nodes exposed through
// the same V4L2 capability contract. It deliberately reuses the qualified
// worker graph while keeping the adapter identity truthful for telemetry.
type v4l2MuxAdapter struct{}

func (v4l2MuxAdapter) ID() string { return TransportV4L2 }

func (v4l2MuxAdapter) Supports(plan BrokerMuxPlan) bool {
	return strings.EqualFold(strings.TrimSpace(plan.Transport), TransportV4L2)
}

func (v4l2MuxAdapter) BuildCommand(ctx context.Context, plan BrokerMuxPlan) (*exec.Cmd, error) {
	return (v4l2UVCMuxAdapter{}).BuildCommand(ctx, plan)
}

func (v4l2UVCMuxAdapter) BuildCommand(ctx context.Context, plan BrokerMuxPlan) (*exec.Cmd, error) {
	if !plan.LiveEnabled && !plan.AnalysisEnabled && !plan.ArchiveEnabled {
		return nil, errors.New("V4L2 transport requires at least one enabled output")
	}
	bridgePath := strings.TrimSpace(plan.V4L2BridgePath)
	encodeProvider := strings.TrimSpace(plan.EncodeProvider)
	archiveEncodeProvider := firstNonEmptyString(plan.ArchiveEncodeProvider, encodeProvider)
	decodeProvider := strings.TrimSpace(plan.DecodeProvider)
	workerProvider := strings.TrimSpace(plan.WorkerProvider)
	if workerProvider != "native_sp" && workerProvider != "gstreamer" {
		return nil, fmt.Errorf("unsupported V4L2 worker provider %q", workerProvider)
	}
	useGStreamerWorker := workerProvider == "gstreamer"
	if useGStreamerWorker && plan.ArchiveEnabled && normalizedArchiveRateControl(plan.ArchiveRateControl) != "cbr" {
		return nil, errors.New("portable GStreamer bridge does not expose configurable archive AVBR")
	}
	if useGStreamerWorker {
		bridgePath = strings.TrimSpace(plan.GStreamerBridgePath)
	}
	if bridgePath == "" {
		return nil, errors.New("missing V4L2 mux bridge for selected codec provider")
	}
	device := strings.TrimSpace(plan.CaptureDevicePath)
	if device == "" {
		return nil, errors.New("V4L2 source requires an explicit capture device")
	}
	format := strings.TrimSpace(strings.ToLower(plan.CaptureFormat))
	if format == "" {
		format = "auto"
	}
	if format == "auto" {
		decision := ResolveV4L2UVCInputFormatDecision(plan, QueryV4L2FormatCapabilities)
		format = decision.Format
		captureWidth := plan.CaptureWidth
		captureHeight := plan.CaptureHeight
		captureFPS := plan.CaptureFPS
		if decision.CaptureWidth > 0 {
			captureWidth = decision.CaptureWidth
		}
		if decision.CaptureHeight > 0 {
			captureHeight = decision.CaptureHeight
		}
		if decision.CaptureFPS > 0 {
			captureFPS = decision.CaptureFPS
		}
		log.Printf(
			"[video-broker-source] uvc input auto selected=%s capture=%dx%d@%d reason=%s sw_decode=%t fallback=%t resized_capture=%t prefer_exact_yuyv=%t allow_scaled_yuyv=%t allow_sw_fallback=%t notes=%s",
			decision.Format,
			captureWidth,
			captureHeight,
			captureFPS,
			decision.Reason,
			decision.RequiresSoftwareDecode,
			decision.UsedFallback,
			decision.ResizedCapture,
			plan.PreferExactYUYV,
			plan.AllowScaledYUYV,
			plan.AllowSWFallback,
			strings.Join(decision.Notes, "; "),
		)
		if decision.RequiresSoftwareDecode && !plan.AllowSWFallback {
			return nil, fmt.Errorf("uvc input auto selected %q (%s) but software decode is not allowed", decision.Format, decision.Reason)
		}
		if strings.TrimSpace(decision.Format) == "" {
			return nil, fmt.Errorf("uvc input auto resolution failed: %s", decision.Reason)
		}
		plan.CaptureWidth = captureWidth
		plan.CaptureHeight = captureHeight
		plan.CaptureFPS = captureFPS
	}
	if v4l2InputFormatRequiresSoftwareDecode(format, plan) && !plan.AllowSWFallback {
		return nil, fmt.Errorf("uvc input format %q requires software decode but video.allow_sw_fallback=false", format)
	}
	if format == "h264" && !plan.H264DecodeHWAvailable && !plan.AllowSWFallback {
		return nil, errors.New("uvc h264 input requires a declared hardware decoder")
	}
	if format == "h265" && !plan.H265DecodeHWAvailable && !plan.AllowSWFallback {
		return nil, errors.New("uvc h265 input requires a declared hardware decoder")
	}
	if useGStreamerWorker {
		liveEncodeRequired := gstreamerLiveEncodeRequired(plan, format)
		archiveEncodeRequired := gstreamerArchiveEncodeRequired(plan, format)
		if liveEncodeRequired {
			liveCodec := normalizedLiveCodec(plan.LiveCodec)
			var err error
			encodeProvider, err = selectGStreamerEncodeProvider(
				encodeProvider,
				liveCodec,
				declaredHardwareEncoderAvailable(plan, liveCodec),
				plan.AllowSWFallback,
			)
			if err != nil {
				return nil, fmt.Errorf("live encoder: %w", err)
			}
		}
		if archiveEncodeRequired {
			archiveCodec := normalizedArchiveCodec(plan.ArchiveCodec)
			archiveHardwareAvailable := declaredHardwareEncoderAvailable(plan, archiveCodec)
			var err error
			archiveEncodeProvider, err = selectGStreamerEncodeProvider(
				archiveEncodeProvider,
				archiveCodec,
				archiveHardwareAvailable,
				plan.AllowSWFallback,
			)
			if err != nil {
				return nil, fmt.Errorf("archive encoder: %w", err)
			}
		}
		if gstreamerRawProcessingRequired(plan, format) && gstreamerDecodeRequired(format) {
			if decodeProvider == "" {
				return nil, fmt.Errorf("%s input requires an explicit decode provider", format)
			}
			if decodeProvider == "software" && !plan.AllowSWFallback {
				return nil, fmt.Errorf("software %s decoder is forbidden by policy", format)
			}
			if plan.AllowSWFallback {
				switch {
				case format == "h264" && !plan.H264DecodeHWAvailable:
					decodeProvider = "software"
				case format == "h265" && !plan.H265DecodeHWAvailable:
					decodeProvider = "software"
				case format == "mjpeg" && !plan.MJPEGDecodeHWAvailable:
					decodeProvider = "software"
				}
			}
		}
	}
	if !useGStreamerWorker {
		if gstreamerLiveEncodeRequired(plan, format) && !declaredHardwareEncoderAvailable(plan, normalizedLiveCodec(plan.LiveCodec)) {
			return nil, fmt.Errorf("native V4L2 transport has no declared hardware %s live encoder", strings.ToUpper(normalizedLiveCodec(plan.LiveCodec)))
		}
		if gstreamerArchiveEncodeRequired(plan, format) {
			archiveCodec := normalizedArchiveCodec(plan.ArchiveCodec)
			if !declaredHardwareEncoderAvailable(plan, archiveCodec) {
				return nil, fmt.Errorf("native V4L2 transport has no declared hardware %s archive encoder", strings.ToUpper(archiveCodec))
			}
		}
	}
	if plan.CaptureSelectTimeoutMs <= 0 {
		plan.CaptureSelectTimeoutMs = DefaultV4L2CaptureSelectTimeoutMs
	}
	if plan.CaptureMaxTimeouts <= 0 {
		plan.CaptureMaxTimeouts = DefaultV4L2CaptureMaxTimeouts
	}
	mainWidth, mainHeight := fitEncoderOutputGeometry(
		plan.MainWidth,
		plan.MainHeight,
		plan.MainWidthAlignment,
		plan.MainHeightAlignment,
	)
	archiveWidth, archiveHeight := FitArchiveGeometryWithAlignment(
		plan.ArchiveWidth,
		plan.ArchiveHeight,
		plan.CaptureWidth,
		plan.CaptureHeight,
		plan.ArchiveWidthAlignment,
		plan.ArchiveHeightAlignment,
	)
	args := []string{
		"--device", device,
		"--input-format", format,
		"--live-enabled", strconv.FormatBool(plan.LiveEnabled),
		"--live-codec", normalizedLiveCodec(plan.LiveCodec),
		"--analysis-enabled", strconv.FormatBool(plan.AnalysisEnabled),
		"--archive-enabled", strconv.FormatBool(plan.ArchiveEnabled),
		"--capture-width", strconv.Itoa(plan.CaptureWidth),
		"--capture-height", strconv.Itoa(plan.CaptureHeight),
		"--sub-width", strconv.Itoa(plan.SubWidth),
		"--sub-height", strconv.Itoa(plan.SubHeight),
		"--sub-fps", strconv.Itoa(plan.SubFPS),
		"--output-width", strconv.Itoa(mainWidth),
		"--output-height", strconv.Itoa(mainHeight),
		"--main-fps", strconv.Itoa(plan.MainFPS),
		"--archive-width", strconv.Itoa(archiveWidth),
		"--archive-height", strconv.Itoa(archiveHeight),
		"--archive-fps", strconv.Itoa(positiveOrSource(plan.ArchiveFPS, plan.CaptureFPS)),
		"--archive-content", normalizedArchiveContent(plan.ArchiveContent),
		"--archive-codec", normalizedArchiveCodec(plan.ArchiveCodec),
		"--main-active", strconv.FormatBool(plan.MainActive),
		"--fps", strconv.Itoa(plan.CaptureFPS),
		"--live-encode-bitrate-kbps", strconv.Itoa(plan.LiveBitrateKbps),
		"--archive-encode-bitrate-kbps", strconv.Itoa(plan.ArchiveBitrateKbps),
		"--video-orientation", normalizeOrientationForCommand(plan.VideoOrientation),
		"--aspect-policy", normalizeV4L2CaptureAspectPolicy(plan.CaptureAspectPolicy),
	}
	if useGStreamerWorker {
		args = append(args,
			"--warmup-sec", strconv.Itoa(plan.WarmupSec),
			"--encode-provider", encodeProvider,
			"--archive-encode-provider", archiveEncodeProvider,
			"--decode-provider", decodeProvider,
			"--allow-sw-fallback", strconv.FormatBool(plan.AllowSWFallback),
		)
		identityArgs, err := workerIdentityArgs(plan)
		if err != nil {
			return nil, err
		}
		args = append(args, identityArgs...)
	} else {
		args = append(args,
			"--warmup-sec", strconv.Itoa(plan.WarmupSec),
			"--archive-rate-control", normalizedArchiveRateControl(plan.ArchiveRateControl),
			"--capture-select-timeout-ms", strconv.Itoa(plan.CaptureSelectTimeoutMs),
			"--capture-max-consecutive-timeouts", strconv.Itoa(plan.CaptureMaxTimeouts),
			"--mjpeg-decode-accelerator", selectNativeMJPEGDecodeAccelerator(plan),
			"--mjpeg-decode-initial-backend", selectNativeMJPEGDecodeInitialBackend(plan),
			"--allow-sw-fallback", strconv.FormatBool(plan.AllowSWFallback),
		)
		identityArgs, err := workerIdentityArgs(plan)
		if err != nil {
			return nil, err
		}
		args = append(args, identityArgs...)
	}
	if plan.QualityMonitorEnabled {
		args = append(args, "--quality-monitor-enabled", "true")
		if plan.QualityMonitorReportEvery > 0 {
			args = append(args, "--quality-monitor-report-every", strconv.Itoa(plan.QualityMonitorReportEvery))
		}
		if plan.QualityMonitorSampleStride > 0 {
			args = append(args, "--quality-monitor-sample-stride", strconv.Itoa(plan.QualityMonitorSampleStride))
		}
		if strings.TrimSpace(plan.QualityMonitorStatePath) != "" {
			args = append(args, "--quality-monitor-state-path", strings.TrimSpace(plan.QualityMonitorStatePath))
		}
	}
	args = append(args, brokerToneMappingArgs(plan)...)
	return exec.CommandContext(ctx, bridgePath, args...), nil
}

func brokerToneMappingArgs(plan BrokerMuxPlan) []string {
	if !plan.ToneMappingEnabled {
		return nil
	}
	mode := strings.ToLower(strings.TrimSpace(plan.ToneMappingMode))
	if mode == "" {
		mode = "auto"
	}
	accelerator := strings.ToLower(strings.TrimSpace(plan.ToneMappingAccelerator))
	if accelerator == "" {
		accelerator = "auto"
	}
	gridColumns := plan.ToneMappingGridColumns
	if gridColumns <= 0 {
		gridColumns = 16
	}
	gridRows := plan.ToneMappingGridRows
	if gridRows <= 0 {
		gridRows = 9
	}
	statisticsHz := plan.ToneMappingStatisticsHz
	if statisticsHz <= 0 {
		statisticsHz = 8
	}
	maxShadowGain := plan.ToneMappingMaxShadowGain
	if maxShadowGain <= 0 {
		maxShadowGain = 2.4
	}
	maxHighlightReduce := plan.ToneMappingMaxHighlightReduce
	if maxHighlightReduce <= 0 {
		maxHighlightReduce = 0.20
	}
	temporalAlpha := plan.ToneMappingTemporalAlpha
	if temporalAlpha <= 0 {
		temporalAlpha = 0.18
	}
	args := []string{
		"--tone-mapping-mode", mode,
		"--tone-mapping-accelerator", accelerator,
		"--tone-mapping-target", strconv.Itoa(plan.ToneMappingTarget),
		"--tone-mapping-hysteresis", strconv.Itoa(plan.ToneMappingHysteresis),
		"--tone-mapping-grid-columns", strconv.Itoa(gridColumns),
		"--tone-mapping-grid-rows", strconv.Itoa(gridRows),
		"--tone-mapping-statistics-hz", strconv.Itoa(statisticsHz),
		"--tone-mapping-max-shadow-gain", strconv.FormatFloat(maxShadowGain, 'f', -1, 64),
		"--tone-mapping-max-highlight-reduction", strconv.FormatFloat(maxHighlightReduce, 'f', -1, 64),
		"--tone-mapping-temporal-alpha", strconv.FormatFloat(temporalAlpha, 'f', -1, 64),
		"--tone-mapping-noise-guard", strconv.FormatBool(plan.ToneMappingNoiseGuard),
	}
	return args
}

func workerIdentityArgs(plan BrokerMuxPlan) ([]string, error) {
	sourceID := strings.TrimSpace(plan.SourceID)
	if sourceID == "" && plan.MediaEpoch == 0 {
		return nil, nil
	}
	if !workerFanoutIDPattern.MatchString(sourceID) || plan.MediaEpoch == 0 {
		return nil, errors.New("capture worker identity fence is incomplete")
	}
	return []string{
		"--source-id", sourceID,
		"--media-epoch", strconv.FormatUint(plan.MediaEpoch, 10),
	}, nil
}

func workerIdentityConfigured(plan BrokerMuxPlan) bool {
	args, err := workerIdentityArgs(plan)
	return err == nil && len(args) > 0
}

func normalizedArchiveContent(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "source", "processed":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "adaptive"
	}
}

func normalizedArchiveCodec(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "h264":
		return "h264"
	default:
		return "h265"
	}
}

func normalizedArchiveRateControl(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "cbr"
	}
	return value
}

func normalizedLiveCodec(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "h264":
		return "h264"
	default:
		return "h265"
	}
}

func declaredHardwareEncoderAvailable(plan BrokerMuxPlan, codec string) bool {
	if strings.EqualFold(strings.TrimSpace(codec), "h264") {
		return plan.H264EncodeHWAvailable
	}
	return plan.H265EncodeHWAvailable
}

func gstreamerLiveEncodeRequired(plan BrokerMuxPlan, captureFormat string) bool {
	if !plan.LiveEnabled {
		return false
	}
	return plan.ToneMappingEnabled || captureFormat != normalizedLiveCodec(plan.LiveCodec) ||
		normalizeOrientationForCommand(plan.VideoOrientation) != VideoOrientationNormal ||
		plan.MainWidth != plan.CaptureWidth ||
		plan.MainHeight != plan.CaptureHeight ||
		plan.MainFPS != plan.CaptureFPS
}

func gstreamerArchiveEncodeRequired(plan BrokerMuxPlan, captureFormat string) bool {
	if !plan.ArchiveEnabled {
		return false
	}
	compressed := captureFormat == "h264" || captureFormat == "h265"
	content := normalizedArchiveContent(plan.ArchiveContent)
	return !compressed || content == "processed" || (content == "adaptive" && plan.ToneMappingEnabled)
}

func gstreamerDecodeRequired(captureFormat string) bool {
	return captureFormat == "h264" || captureFormat == "h265" || captureFormat == "mjpeg"
}

func gstreamerRawProcessingRequired(plan BrokerMuxPlan, captureFormat string) bool {
	return plan.AnalysisEnabled || plan.QualityMonitorEnabled ||
		gstreamerLiveEncodeRequired(plan, captureFormat) ||
		gstreamerArchiveEncodeRequired(plan, captureFormat)
}

func selectGStreamerEncodeProvider(
	configured string,
	codec string,
	hardwareAvailable bool,
	allowSoftwareFallback bool,
) (string, error) {
	configured = strings.TrimSpace(configured)
	if configured == "" {
		return "", fmt.Errorf("%s encoding requires an explicit provider", strings.ToUpper(codec))
	}
	if configured == "software" {
		if !allowSoftwareFallback {
			return "", fmt.Errorf("software %s encoder is forbidden by policy", strings.ToUpper(codec))
		}
		return configured, nil
	}
	if hardwareAvailable {
		return configured, nil
	}
	if allowSoftwareFallback {
		return "software", nil
	}
	return "", fmt.Errorf("no declared hardware %s encoder and software fallback is disabled", strings.ToUpper(codec))
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func positiveOrSource(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func fitEncoderOutputGeometry(width, height, widthAlignment, heightAlignment int) (int, int) {
	if widthAlignment <= 0 && heightAlignment <= 0 {
		return width, height
	}
	return FitArchiveGeometryWithAlignment(
		width,
		height,
		width,
		height,
		widthAlignment,
		heightAlignment,
	)
}

func selectNativeMJPEGDecodeAccelerator(plan BrokerMuxPlan) string {
	if plan.AllowSWFallback && (plan.MJPEGDecodeHWAvailable || plan.MJPEGDecodeInPlaceRetrySupported) {
		return "auto"
	}
	if plan.MJPEGDecodeHWAvailable {
		return "jpu"
	}
	return "cpu"
}

func selectNativeMJPEGDecodeInitialBackend(plan BrokerMuxPlan) string {
	if plan.MJPEGDecodeHWAvailable {
		return "jpu"
	}
	return "cpu"
}

func mjpegDecodePlanNote(plan BrokerMuxPlan) string {
	if plan.MJPEGDecodeHWAvailable {
		return "hardware JPU MJPEG decode with bounded software fallback"
	}
	return "software MJPEG decode with preferred hardware output encode"
}

func ResolveV4L2UVCInputFormat(
	plan BrokerMuxPlan,
	query func(context.Context, string) ([]V4L2FormatCapability, error),
) string {
	return ResolveV4L2UVCInputFormatDecision(plan, query).Format
}

func ResolveV4L2UVCInputFormatDecision(
	plan BrokerMuxPlan,
	query func(context.Context, string) ([]V4L2FormatCapability, error),
) V4L2InputFormatDecision {
	decision := ResolveV4L2UVCCaptureDecision(plan, query)
	return V4L2InputFormatDecision(decision)
}

const (
	V4L2CapturePolicyExact           = "exact"
	V4L2CapturePolicyAnalysisQuality = "analysis_quality"
	V4L2CaptureCodecPolicyPrefer     = "prefer"
	V4L2CaptureCodecPolicyRequired   = "required"
	V4L2CaptureAspectPolicyExact     = "exact"
	V4L2CaptureAspectPolicyCrop      = "crop"
	V4L2CaptureAspectPolicyLetterbox = "letterbox"

	v4l2FPSMatchTolerance      = 0.20
	v4l2AspectPenaltyScale     = 1000000.0
	v4l2AspectScoreWeight      = 100000000
	v4l2UnknownRequestedArea   = 1 << 30
	v4l2ScaledCandidateMinArea = 1
)

func ResolveV4L2UVCCaptureDecision(
	plan BrokerMuxPlan,
	query func(context.Context, string) ([]V4L2FormatCapability, error),
) V4L2InputFormatDecision {
	device := strings.TrimSpace(plan.CaptureDevicePath)
	if device == "" {
		return V4L2InputFormatDecision{
			Reason:        "missing_device",
			CaptureWidth:  plan.CaptureWidth,
			CaptureHeight: plan.CaptureHeight,
			CaptureFPS:    plan.CaptureFPS,
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultV4L2ProbeTimeout())
	defer cancel()
	caps, err := query(ctx, device)
	if err != nil || len(caps) == 0 {
		reason := "probe_failed"
		if err == nil {
			reason = "no_capabilities"
		}
		return V4L2InputFormatDecision{
			Reason:        reason,
			CaptureWidth:  plan.CaptureWidth,
			CaptureHeight: plan.CaptureHeight,
			CaptureFPS:    plan.CaptureFPS,
		}
	}
	return ResolveV4L2UVCCaptureDecisionFromCapabilities(plan, caps)
}

// ResolveV4L2UVCCaptureDecisionFromCapabilities is the pure capture-policy
// boundary used both by direct format probing and multi-device admission.
func ResolveV4L2UVCCaptureDecisionFromCapabilities(
	plan BrokerMuxPlan,
	caps []V4L2FormatCapability,
) V4L2InputFormatDecision {
	policy := normalizeV4L2CapturePolicy(plan.CapturePolicy)
	caps = filterExcludedV4L2CaptureModes(caps, plan.CaptureModeExclusions)
	if len(caps) == 0 {
		return V4L2InputFormatDecision{
			Reason:        "no_capabilities",
			CaptureWidth:  plan.CaptureWidth,
			CaptureHeight: plan.CaptureHeight,
			CaptureFPS:    plan.CaptureFPS,
		}
	}
	preference := strings.ToLower(strings.TrimSpace(plan.CaptureCodecPreference))
	if preference == "" {
		preference = "h265"
	}
	codecPolicy := normalizeV4L2CaptureCodecPolicy(plan.CaptureCodecPolicy)
	explicitFormat := normalizeV4L2Format(plan.CaptureFormat)
	if explicitFormat != "" && explicitFormat != "auto" {
		if codecPolicy == V4L2CaptureCodecPolicyRequired && explicitFormat != preference {
			return V4L2InputFormatDecision{
				Reason:        "explicit_format_conflicts_with_required_codec",
				CaptureWidth:  plan.CaptureWidth,
				CaptureHeight: plan.CaptureHeight,
				CaptureFPS:    plan.CaptureFPS,
			}
		}
		return explicitV4L2CaptureDecision(plan, caps, explicitFormat)
	}
	compressedCandidates := []struct {
		format   string
		hwDecode bool
	}{
		{format: "h265", hwDecode: plan.H265DecodeHWAvailable},
		{format: "h264", hwDecode: plan.H264DecodeHWAvailable},
	}
	if preference == "h264" {
		compressedCandidates[0], compressedCandidates[1] = compressedCandidates[1], compressedCandidates[0]
	}
	if codecPolicy == V4L2CaptureCodecPolicyRequired {
		compressedCandidates = compressedCandidates[:1]
	}
	for _, candidate := range compressedCandidates {
		if !candidate.hwDecode && !plan.AllowSWFallback {
			continue
		}
		// A analysis-quality contract may intentionally capture a modestly
		// larger native compressed mode and downscale it.  Do this before the
		// exact-mode shortcut only when the complete codec path is declared
		// hardware-only: supersampling is useful for detail/noise and also avoids
		// camera firmware modes whose ISP tuning is tied to one advertised size,
		// but it must not silently turn into a software CPU tax.  The bounded 2x
		// area ceiling prevents an exact 720p request from jumping to 4K merely
		// because the module advertises it.
		if policy == V4L2CapturePolicyAnalysisQuality &&
			plan.RequireHWCodec && candidate.hwDecode && plan.H264EncodeHWAvailable {
			if nativeMode, ok := bestNativeCompressedSupersampleCandidate(caps, candidate.format, plan); ok {
				decision := nativeCompressedDecision(candidate.format, candidate.hwDecode, plan, nativeMode, true)
				decision.Reason = candidate.format + "_native_compressed_supersample"
				decision.Notes = append(decision.Notes, "selection=analysis_quality_supersample")
				return decision
			}
		}
		// A rejected supersample is evidence against that exact mode, not against
		// the requested mode. Prefer the requested native geometry when it remains
		// available: adapting below it would add an upscale cost and cannot recover
		// detail. Only explore another compatible native mode when the requested
		// mode is itself unavailable or runtime-rejected. Exact policy remains
		// strictly exact.
		if formatSupported(caps, candidate.format, plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS) {
			decision := nativeCompressedDecision(candidate.format, candidate.hwDecode, plan, V4L2FormatCapability{
				PixelFormat: candidate.format,
				Width:       plan.CaptureWidth,
				Height:      plan.CaptureHeight,
				FPS:         float64(plan.CaptureFPS),
			}, false)
			if len(plan.CaptureModeExclusions) > 0 {
				decision.Notes = append(decision.Notes,
					"selection=requested_exact_after_runtime_exclusion",
					fmt.Sprintf("runtime_excluded_modes=%d", len(plan.CaptureModeExclusions)),
				)
			}
			return decision
		}
		if policy == V4L2CapturePolicyAnalysisQuality && len(plan.CaptureModeExclusions) > 0 {
			if nativeMode, ok := bestNativeCompressedRuntimeFallbackCandidate(caps, candidate.format, plan); ok {
				decision := nativeCompressedDecision(candidate.format, candidate.hwDecode, plan, nativeMode, true)
				decision.Reason = candidate.format + "_native_compressed_runtime_fallback"
				decision.Notes = append(decision.Notes,
					"selection=runtime_qualified_fallback",
					fmt.Sprintf("runtime_excluded_modes=%d", len(plan.CaptureModeExclusions)),
				)
				return decision
			}
		}
		if policy == V4L2CapturePolicyAnalysisQuality {
			if nativeMode, ok := bestNativeCompressedCandidate(caps, candidate.format, plan); ok {
				return nativeCompressedDecision(candidate.format, candidate.hwDecode, plan, nativeMode, true)
			}
			if nativeMode, ok := bestCompatibleNativeModeCandidate(caps, candidate.format, plan); ok {
				return nativeCompressedDecision(candidate.format, candidate.hwDecode, plan, nativeMode, true)
			}
		}
	}
	if codecPolicy == V4L2CaptureCodecPolicyRequired {
		return V4L2InputFormatDecision{
			Reason:        "required_" + preference + "_mode_unavailable",
			CaptureWidth:  plan.CaptureWidth,
			CaptureHeight: plan.CaptureHeight,
			CaptureFPS:    plan.CaptureFPS,
		}
	}
	exactYUYVSupported := formatSupported(caps, "yuyv", plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS)
	if exactYUYVSupported && plan.PreferExactYUYV {
		return V4L2InputFormatDecision{
			Format:        "yuyv",
			Reason:        "yuyv_exact_match",
			CaptureWidth:  plan.CaptureWidth,
			CaptureHeight: plan.CaptureHeight,
			CaptureFPS:    plan.CaptureFPS,
		}
	}
	if policy == V4L2CapturePolicyAnalysisQuality {
		exactMJPEGSupported := formatSupported(caps, "mjpeg", plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS)
		if !exactMJPEGSupported {
			if decision, ok := adaptedMJPEGDecision(caps, plan); ok {
				return decision
			}
		}
		if candidate, ok := bestScaledYUYVCandidate(caps, plan); ok {
			if plan.AllowScaledYUYV {
				return candidate
			}
			if formatSupported(caps, "mjpeg", plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS) {
				return V4L2InputFormatDecision{
					Format:                 "mjpeg",
					Reason:                 "mjpeg_exact_match",
					RequiresSoftwareDecode: !plan.MJPEGDecodeHWAvailable,
					CaptureWidth:           plan.CaptureWidth,
					CaptureHeight:          plan.CaptureHeight,
					CaptureFPS:             plan.CaptureFPS,
					Notes: []string{
						fmt.Sprintf(
							"scaled_yuyv_candidate=%dx%d@%d disabled_by_policy",
							candidate.CaptureWidth,
							candidate.CaptureHeight,
							candidate.CaptureFPS,
						),
					},
				}
			}
		}
	}
	if formatSupported(caps, "mjpeg", plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS) {
		notes := []string(nil)
		if exactYUYVSupported && !plan.PreferExactYUYV {
			notes = append(notes, "exact_yuyv_candidate_disabled_by_policy")
		}
		return V4L2InputFormatDecision{
			Format:                 "mjpeg",
			Reason:                 "mjpeg_exact_match",
			RequiresSoftwareDecode: !plan.MJPEGDecodeHWAvailable,
			CaptureWidth:           plan.CaptureWidth,
			CaptureHeight:          plan.CaptureHeight,
			CaptureFPS:             plan.CaptureFPS,
			Notes:                  notes,
		}
	}
	if exactYUYVSupported {
		return V4L2InputFormatDecision{
			Format:        "yuyv",
			Reason:        "yuyv_exact_only_available",
			CaptureWidth:  plan.CaptureWidth,
			CaptureHeight: plan.CaptureHeight,
			CaptureFPS:    plan.CaptureFPS,
			Notes:         []string{"prefer_exact_yuyv=false ignored because exact mjpeg is unavailable"},
		}
	}
	if policy == V4L2CapturePolicyAnalysisQuality && plan.AllowScaledYUYV {
		if candidate, ok := bestScaledYUYVCandidate(caps, plan); ok {
			return candidate
		}
	}
	return V4L2InputFormatDecision{
		Reason:        "no_exact_match",
		CaptureWidth:  plan.CaptureWidth,
		CaptureHeight: plan.CaptureHeight,
		CaptureFPS:    plan.CaptureFPS,
	}
}

func bestNativeCompressedSupersampleCandidate(
	caps []V4L2FormatCapability,
	format string,
	plan BrokerMuxPlan,
) (V4L2FormatCapability, bool) {
	format = normalizeV4L2Format(format)
	requestedWidth := maxInt(plan.CaptureWidth, 1)
	requestedHeight := maxInt(plan.CaptureHeight, 1)
	requestedArea := int64(requestedWidth) * int64(requestedHeight)
	bestIndex := -1
	bestArea := int64(1<<63 - 1)
	for idx, cap := range caps {
		if normalizeV4L2Format(cap.PixelFormat) != format ||
			cap.Width <= 0 || cap.Height <= 0 ||
			!fpsSatisfies(cap.FPS, plan.CaptureFPS) ||
			!sameAspect(cap.Width, cap.Height, requestedWidth, requestedHeight) {
			continue
		}
		area := int64(cap.Width) * int64(cap.Height)
		if area <= requestedArea || area > requestedArea*2 {
			continue
		}
		if area < bestArea {
			bestArea = area
			bestIndex = idx
		}
	}
	if bestIndex < 0 {
		return V4L2FormatCapability{}, false
	}
	return caps[bestIndex], true
}

func bestNativeCompressedRuntimeFallbackCandidate(
	caps []V4L2FormatCapability,
	format string,
	plan BrokerMuxPlan,
) (V4L2FormatCapability, bool) {
	format = normalizeV4L2Format(format)
	requestedWidth := maxInt(plan.CaptureWidth, 1)
	requestedHeight := maxInt(plan.CaptureHeight, 1)
	requestedArea := int64(requestedWidth) * int64(requestedHeight)
	minimumFPS := runtimeFallbackMinimumFPS(plan)
	// Runtime fallback may upscale the presentation/archive branches, but it
	// must never undersupply the analysis branch. This keeps an advertised
	// but faulty high-resolution mode from disabling otherwise usable modules.
	minimumWidth := plan.SubWidth
	minimumHeight := plan.SubHeight
	if minimumWidth <= 0 {
		minimumWidth = minPositiveInt(plan.MainWidth, requestedWidth)
	}
	if minimumHeight <= 0 {
		minimumHeight = minPositiveInt(plan.MainHeight, requestedHeight)
	}
	bestIndex := -1
	bestScore := int64(1<<63 - 1)
	for idx, capability := range caps {
		if normalizeV4L2Format(capability.PixelFormat) != format ||
			capability.Width < minimumWidth || capability.Height < minimumHeight ||
			!fpsSatisfies(capability.FPS, minimumFPS) ||
			(capability.Width == requestedWidth && capability.Height == requestedHeight &&
				fpsSatisfies(capability.FPS, plan.CaptureFPS)) {
			continue
		}
		area := int64(capability.Width) * int64(capability.Height)
		if area <= 0 || area > requestedArea*2 {
			continue
		}
		areaDelta := area - requestedArea
		if areaDelta < 0 {
			areaDelta = -areaDelta
		}
		aspect := float64(capability.Width) / float64(maxInt(capability.Height, 1))
		requestedAspect := float64(requestedWidth) / float64(requestedHeight)
		aspectPenalty := int64(absFloat(aspect-requestedAspect) * v4l2AspectPenaltyScale)
		score := aspectPenalty*v4l2AspectScoreWeight + areaDelta
		// If two modes are equally close, prefer the smaller workload.  It is
		// the safer recovery choice after a live cadence failure.
		if score < bestScore || score == bestScore && bestIndex >= 0 &&
			(area < int64(caps[bestIndex].Width)*int64(caps[bestIndex].Height) ||
				area == int64(caps[bestIndex].Width)*int64(caps[bestIndex].Height) &&
					capability.FPS > caps[bestIndex].FPS) {
			bestScore = score
			bestIndex = idx
		}
	}
	if bestIndex < 0 {
		return V4L2FormatCapability{}, false
	}
	return caps[bestIndex], true
}

func runtimeFallbackMinimumFPS(plan BrokerMuxPlan) int {
	// A fallback capture mode must feed every independently configured consumer.
	// Archive is not an independent cadence consumer: it is the protected master
	// timeline and is rebound to the selected native capture FPS by camera
	// admission. Live and analysis remain hard floors because capture cannot
	// synthesize frames for either downstream stream.
	minimum := maxInt(plan.MainFPS, plan.SubFPS)
	if minimum <= 0 {
		minimum = plan.CaptureFPS
	}
	if plan.CaptureFPS > 0 && minimum > plan.CaptureFPS {
		minimum = plan.CaptureFPS
	}
	return maxInt(minimum, 1)
}

func captureFPSForAdvertisedMode(requested int, advertised float64) int {
	available := int(advertised + 0.5)
	if available <= 0 {
		return requested
	}
	if requested <= 0 || available < requested {
		return available
	}
	return requested
}

func hasV4L2CaptureModeExclusionForFormat(exclusions []V4L2CaptureModeExclusion, format string) bool {
	format = normalizeV4L2Format(format)
	for _, exclusion := range exclusions {
		if normalizeV4L2Format(exclusion.PixelFormat) == format {
			return true
		}
	}
	return false
}

func positiveCaptureFPS(requested int, advertised float64) int {
	if requested > 0 {
		return requested
	}
	return int(advertised + 0.5)
}

func minPositiveInt(left int, right int) int {
	if left <= 0 {
		return maxInt(right, 1)
	}
	if right <= 0 || left < right {
		return left
	}
	return right
}

func explicitV4L2CaptureDecision(
	plan BrokerMuxPlan,
	caps []V4L2FormatCapability,
	format string,
) V4L2InputFormatDecision {
	softwareDecode := v4l2InputFormatRequiresSoftwareDecode(format, plan)
	if formatSupported(caps, format, plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS) {
		return V4L2InputFormatDecision{
			Format:                 format,
			Reason:                 format + "_explicit_exact",
			RequiresSoftwareDecode: softwareDecode,
			CaptureWidth:           plan.CaptureWidth,
			CaptureHeight:          plan.CaptureHeight,
			CaptureFPS:             plan.CaptureFPS,
		}
	}
	if normalizeV4L2CapturePolicy(plan.CapturePolicy) == V4L2CapturePolicyAnalysisQuality {
		switch format {
		case "h264", "h265":
			if mode, ok := bestNativeCompressedCandidate(caps, format, plan); ok {
				hwDecode := format == "h264" && plan.H264DecodeHWAvailable ||
					format == "h265" && plan.H265DecodeHWAvailable
				return nativeCompressedDecision(format, hwDecode, plan, mode, true)
			}
			if mode, ok := bestCompatibleNativeModeCandidate(caps, format, plan); ok {
				hwDecode := format == "h264" && plan.H264DecodeHWAvailable ||
					format == "h265" && plan.H265DecodeHWAvailable
				return nativeCompressedDecision(format, hwDecode, plan, mode, true)
			}
		case "yuyv":
			if plan.AllowScaledYUYV {
				if decision, ok := bestScaledYUYVCandidate(caps, plan); ok {
					decision.Reason = "yuyv_explicit_scaled"
					return decision
				}
			}
		case "mjpeg":
			if decision, ok := adaptedMJPEGDecision(caps, plan); ok {
				return decision
			}
		}
	}
	return V4L2InputFormatDecision{
		Reason:        "explicit_" + format + "_mode_unavailable",
		CaptureWidth:  plan.CaptureWidth,
		CaptureHeight: plan.CaptureHeight,
		CaptureFPS:    plan.CaptureFPS,
	}
}

func adaptedMJPEGDecision(
	caps []V4L2FormatCapability,
	plan BrokerMuxPlan,
) (V4L2InputFormatDecision, bool) {
	if hasV4L2CaptureModeExclusionForFormat(plan.CaptureModeExclusions, "mjpeg") {
		if mode, ok := bestNativeCompressedRuntimeFallbackCandidate(caps, "mjpeg", plan); ok {
			return V4L2InputFormatDecision{
				Format:                 "mjpeg",
				Reason:                 "mjpeg_native_runtime_fallback",
				RequiresSoftwareDecode: !plan.MJPEGDecodeHWAvailable,
				CaptureWidth:           mode.Width,
				CaptureHeight:          mode.Height,
				CaptureFPS:             captureFPSForAdvertisedMode(plan.CaptureFPS, mode.FPS),
				ResizedCapture:         mode.Width != plan.CaptureWidth || mode.Height != plan.CaptureHeight,
				Notes: []string{
					fmt.Sprintf("requested_capture=%dx%d@%d", plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS),
					fmt.Sprintf("camera_native_mode=%dx%d@%.2f", mode.Width, mode.Height, mode.FPS),
					"selection=runtime_qualified_fallback",
					fmt.Sprintf("runtime_excluded_modes=%d", len(plan.CaptureModeExclusions)),
					mjpegDecodePlanNote(plan),
				},
			}, true
		}
	}
	if mode, ok := bestCompatibleNativeModeCandidate(caps, "mjpeg", plan); ok {
		return V4L2InputFormatDecision{
			Format:                 "mjpeg",
			Reason:                 "mjpeg_native_compatible_adapted",
			RequiresSoftwareDecode: !plan.MJPEGDecodeHWAvailable,
			CaptureWidth:           mode.Width,
			CaptureHeight:          mode.Height,
			CaptureFPS:             positiveCaptureFPS(plan.CaptureFPS, mode.FPS),
			ResizedCapture:         mode.Width != plan.CaptureWidth || mode.Height != plan.CaptureHeight,
			Notes: []string{
				fmt.Sprintf("requested_capture=%dx%d@%d", plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS),
				fmt.Sprintf("camera_native_mode=%dx%d@%.2f", mode.Width, mode.Height, mode.FPS),
				mjpegDecodePlanNote(plan),
			},
		}, true
	}
	return V4L2InputFormatDecision{}, false
}

func nativeCompressedDecision(
	format string,
	hwDecode bool,
	plan BrokerMuxPlan,
	mode V4L2FormatCapability,
	resized bool,
) V4L2InputFormatDecision {
	notes := []string{"native compressed archive preserved"}
	if resized {
		notes = append(notes,
			fmt.Sprintf("requested_capture=%dx%d@%d", plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS),
			fmt.Sprintf("camera_native_mode=%dx%d@%.2f", mode.Width, mode.Height, mode.FPS),
		)
	}
	if !hwDecode {
		notes = append(notes, "analysis path uses permitted software decode")
	}
	fps := captureFPSForAdvertisedMode(plan.CaptureFPS, mode.FPS)
	reason := format + "_exact_native_compressed"
	if resized {
		reason = format + "_native_compressed_adapted"
		if mode.Width*mode.Height >= plan.CaptureWidth*plan.CaptureHeight {
			reason = format + "_native_compressed_downscale"
		}
	}
	return V4L2InputFormatDecision{
		Format:                 format,
		Reason:                 reason,
		RequiresSoftwareDecode: !hwDecode,
		CaptureWidth:           mode.Width,
		CaptureHeight:          mode.Height,
		CaptureFPS:             fps,
		ResizedCapture:         resized,
		Notes:                  notes,
	}
}

func bestCompatibleNativeModeCandidate(
	caps []V4L2FormatCapability,
	format string,
	plan BrokerMuxPlan,
) (V4L2FormatCapability, bool) {
	format = normalizeV4L2Format(format)
	minimumWidth := maxInt(plan.MainWidth, plan.SubWidth)
	minimumHeight := maxInt(plan.MainHeight, plan.SubHeight)
	if minimumWidth <= 0 {
		minimumWidth = maxInt(plan.CaptureWidth, 1)
	}
	if minimumHeight <= 0 {
		minimumHeight = maxInt(plan.CaptureHeight, 1)
	}
	requestedArea := maxInt(plan.CaptureWidth, 1) * maxInt(plan.CaptureHeight, 1)
	requestedAspect := float64(maxInt(plan.MainWidth, 1)) / float64(maxInt(plan.MainHeight, 1))
	bestIndex := -1
	bestScore := int64(1<<63 - 1)
	for idx, cap := range caps {
		if normalizeV4L2Format(cap.PixelFormat) != format || cap.Width < minimumWidth || cap.Height < minimumHeight ||
			!fpsSatisfies(cap.FPS, plan.CaptureFPS) {
			continue
		}
		if cap.Width == plan.CaptureWidth && cap.Height == plan.CaptureHeight {
			continue
		}
		area := cap.Width * cap.Height
		areaDelta := int64(area - requestedArea)
		if areaDelta < 0 {
			areaDelta = -areaDelta
		}
		aspect := float64(cap.Width) / float64(maxInt(cap.Height, 1))
		aspectPenalty := int64(absFloat(aspect-requestedAspect) * v4l2AspectPenaltyScale)
		score := aspectPenalty*v4l2AspectScoreWeight + areaDelta
		if score < bestScore {
			bestScore = score
			bestIndex = idx
		}
	}
	if bestIndex < 0 {
		return V4L2FormatCapability{}, false
	}
	return caps[bestIndex], true
}

func bestNativeCompressedCandidate(
	caps []V4L2FormatCapability,
	format string,
	plan BrokerMuxPlan,
) (V4L2FormatCapability, bool) {
	format = normalizeV4L2Format(format)
	requestedWidth := maxInt(plan.CaptureWidth, 1)
	requestedHeight := maxInt(plan.CaptureHeight, 1)
	requestedArea := requestedWidth * requestedHeight
	requestedAspect := float64(requestedWidth) / float64(requestedHeight)
	bestIndex := -1
	bestScore := int64(1<<63 - 1)
	for idx, cap := range caps {
		if normalizeV4L2Format(cap.PixelFormat) != format ||
			cap.Width < requestedWidth || cap.Height < requestedHeight ||
			(cap.Width == requestedWidth && cap.Height == requestedHeight) ||
			!fpsSatisfies(cap.FPS, plan.CaptureFPS) {
			continue
		}
		area := cap.Width * cap.Height
		if area < requestedArea {
			continue
		}
		aspect := float64(cap.Width) / float64(maxInt(cap.Height, 1))
		aspectPenalty := int64(absFloat(aspect-requestedAspect) * v4l2AspectPenaltyScale)
		areaPenalty := int64(area - requestedArea)
		score := aspectPenalty*v4l2AspectScoreWeight + areaPenalty
		if score < bestScore {
			bestScore = score
			bestIndex = idx
		}
	}
	if bestIndex < 0 {
		return V4L2FormatCapability{}, false
	}
	return caps[bestIndex], true
}

func normalizeV4L2CapturePolicy(policy string) string {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case "", "auto", V4L2CapturePolicyExact:
		return V4L2CapturePolicyExact
	case "analysis", "quality", "analysis-quality", V4L2CapturePolicyAnalysisQuality:
		return V4L2CapturePolicyAnalysisQuality
	default:
		return V4L2CapturePolicyExact
	}
}

func normalizeV4L2CaptureCodecPolicy(policy string) string {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case V4L2CaptureCodecPolicyRequired:
		return V4L2CaptureCodecPolicyRequired
	default:
		return V4L2CaptureCodecPolicyPrefer
	}
}

func normalizeV4L2CaptureAspectPolicy(policy string) string {
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case V4L2CaptureAspectPolicyExact:
		return V4L2CaptureAspectPolicyExact
	case V4L2CaptureAspectPolicyLetterbox:
		return V4L2CaptureAspectPolicyLetterbox
	default:
		return V4L2CaptureAspectPolicyCrop
	}
}

func bestScaledYUYVCandidate(caps []V4L2FormatCapability, plan BrokerMuxPlan) (V4L2InputFormatDecision, bool) {
	minWidth := plan.SubWidth
	minHeight := plan.SubHeight
	if minWidth <= 0 {
		minWidth = plan.CaptureWidth
	}
	if minHeight <= 0 {
		minHeight = plan.CaptureHeight
	}
	requestedArea := plan.CaptureWidth * plan.CaptureHeight
	if requestedArea <= 0 {
		requestedArea = v4l2UnknownRequestedArea
	}
	requestedAspect := float64(plan.CaptureWidth) / float64(maxInt(plan.CaptureHeight, 1))
	bestIndex := -1
	bestScore := int64(1<<63 - 1)
	for idx, cap := range caps {
		if normalizeV4L2Format(cap.PixelFormat) != "yuyv" {
			continue
		}
		if cap.Width < minWidth || cap.Height < minHeight {
			continue
		}
		if cap.Width == plan.CaptureWidth && cap.Height == plan.CaptureHeight {
			continue
		}
		if !fpsSatisfies(cap.FPS, plan.CaptureFPS) {
			continue
		}
		area := cap.Width * cap.Height
		if area < v4l2ScaledCandidateMinArea || area > requestedArea {
			continue
		}
		aspect := float64(cap.Width) / float64(maxInt(cap.Height, 1))
		aspectPenalty := int64(absFloat(aspect-requestedAspect) * v4l2AspectPenaltyScale)
		areaPenalty := int64(requestedArea - area)
		score := aspectPenalty*v4l2AspectScoreWeight + areaPenalty
		if score < bestScore {
			bestScore = score
			bestIndex = idx
		}
	}
	if bestIndex < 0 {
		return V4L2InputFormatDecision{}, false
	}
	cap := caps[bestIndex]
	return V4L2InputFormatDecision{
		Format:         "yuyv",
		Reason:         "analysis_quality_scaled_yuyv",
		CaptureWidth:   cap.Width,
		CaptureHeight:  cap.Height,
		CaptureFPS:     plan.CaptureFPS,
		ResizedCapture: cap.Width != plan.CaptureWidth || cap.Height != plan.CaptureHeight,
		Notes: []string{
			fmt.Sprintf("requested_capture=%dx%d@%d", plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS),
		},
	}, true
}

func defaultV4L2ProbeTimeout() time.Duration {
	return 1500 * time.Millisecond
}

func formatSupported(caps []V4L2FormatCapability, format string, width int, height int, fps int) bool {
	format = normalizeV4L2Format(format)
	for _, cap := range caps {
		if normalizeV4L2Format(cap.PixelFormat) != format {
			continue
		}
		if cap.Width != width || cap.Height != height {
			continue
		}
		if fpsSatisfies(cap.FPS, fps) {
			return true
		}
	}
	return false
}

func filterExcludedV4L2CaptureModes(
	caps []V4L2FormatCapability,
	exclusions []V4L2CaptureModeExclusion,
) []V4L2FormatCapability {
	if len(caps) == 0 || len(exclusions) == 0 {
		return caps
	}
	available := make([]V4L2FormatCapability, 0, len(caps))
	for _, capability := range caps {
		excluded := false
		for _, rejection := range exclusions {
			if normalizeV4L2Format(capability.PixelFormat) != normalizeV4L2Format(rejection.PixelFormat) ||
				capability.Width != rejection.Width || capability.Height != rejection.Height {
				continue
			}
			if rejection.FPS > 0 && !fpsSatisfies(capability.FPS, rejection.FPS) {
				continue
			}
			excluded = true
			break
		}
		if !excluded {
			available = append(available, capability)
		}
	}
	return available
}

func fpsSatisfies(actual float64, requested int) bool {
	if requested <= 0 {
		return true
	}
	return actual+v4l2FPSMatchTolerance >= float64(requested)
}

func normalizeV4L2Format(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "mjpg", "jpeg":
		return "mjpeg"
	case "yuyv422", "yuy2":
		return "yuyv"
	case "hevc", "hevc1", "h265":
		return "h265"
	case "avc", "avc1", "h264":
		return "h264"
	default:
		return strings.ToLower(strings.TrimSpace(format))
	}
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

func v4l2InputFormatRequiresSoftwareDecode(format string, plan BrokerMuxPlan) bool {
	switch normalizeV4L2Format(format) {
	case "mjpeg":
		return !plan.MJPEGDecodeHWAvailable
	default:
		return false
	}
}

func QueryV4L2FormatCapabilities(ctx context.Context, device string) ([]V4L2FormatCapability, error) {
	device = strings.TrimSpace(device)
	if device == "" {
		return nil, errors.New("V4L2 capability query requires an explicit device")
	}
	cmd := exec.CommandContext(ctx, "v4l2-ctl", "--list-formats-ext", "--device", device)
	out, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return ParseV4L2FormatCapabilities(out), nil
}

var (
	v4l2PixelFormatRe = regexp.MustCompile(`(?:Pixel Format:\s*)?'([^']+)'`)
	v4l2SizeRe        = regexp.MustCompile(`Size:\s+Discrete\s+([0-9]+)x([0-9]+)`)
	v4l2FpsRe         = regexp.MustCompile(`\(([0-9.]+)\s+fps\)`)
)

func ParseV4L2FormatCapabilities(out []byte) []V4L2FormatCapability {
	lines := bytes.Split(out, []byte{'\n'})
	currentFormat := ""
	currentWidth := 0
	currentHeight := 0
	caps := []V4L2FormatCapability{}
	for _, raw := range lines {
		line := string(raw)
		if m := v4l2PixelFormatRe.FindStringSubmatch(line); len(m) == 2 {
			currentFormat = normalizeV4L2Format(m[1])
			currentWidth = 0
			currentHeight = 0
			continue
		}
		if m := v4l2SizeRe.FindStringSubmatch(line); len(m) == 3 {
			currentWidth, _ = strconv.Atoi(m[1])
			currentHeight, _ = strconv.Atoi(m[2])
			continue
		}
		if currentFormat == "" || currentWidth <= 0 || currentHeight <= 0 {
			continue
		}
		if m := v4l2FpsRe.FindStringSubmatch(line); len(m) == 2 {
			fps, _ := strconv.ParseFloat(m[1], 64)
			if fps > 0 {
				caps = append(caps, V4L2FormatCapability{
					PixelFormat: currentFormat,
					Width:       currentWidth,
					Height:      currentHeight,
					FPS:         fps,
				})
			}
		}
	}
	return caps
}
