package broker

import (
	"path/filepath"
	"strings"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
)

// engineConfig is the normalized, implementation-facing graph contract. It is
// intentionally private: external users configure the stable product schema in
// config.Config instead of depending on worker or board implementation details.
type engineConfig struct {
	SourceID                               string
	SourceIdentity                         string
	LiveStreamID                           string
	AnalysisStreamID                       string
	ArchiveStreamID                        string
	LiveEnabled                            bool
	AnalysisEnabled                        bool
	ArchiveEnabled                         bool
	VideoOrientation                       string
	RawWidth                               int
	RawHeight                              int
	AnalysisWidth                          int
	AnalysisHeight                         int
	AnalysisFPS                            int
	LiveWidth                              int
	LiveHeight                             int
	LiveFPS                                int
	ArchiveWidth                           int
	ArchiveHeight                          int
	ArchiveFPS                             int
	ArchiveRateControl                     string
	CanonicalCaptureDevice                 string
	CanonicalAnalysisDevice                string
	CanonicalStatisticsDevice              string
	CaptureSourcePath                      string
	AnalysisSourcePath                     string
	StatisticsSourcePath                   string
	VideoRequireHWCodec                    bool
	VideoAllowSWFallback                   bool
	VideoTransport                         string
	VideoEncodeProvider                    string
	VideoDecodeProvider                    string
	LiveFormat                             string
	AnalysisFormat                         string
	VideoCaptureFPS                        int
	VideoCaptureWarmupSec                  int
	LiveBitrateKbps                        int
	ArchiveBitrateKbps                     int
	VideoCaptureSelectTimeoutMs            int
	VideoCaptureMaxTimeouts                int
	VideoCaptureDevice                     string
	VideoCaptureFormat                     string
	VideoCaptureCodecPreference            string
	VideoCaptureCodecPolicy                string
	VideoCapturePolicyProvider             string
	VideoCaptureAspectPolicy               string
	VideoCaptureAllowScaledYUYV            bool
	VideoCapturePreferExactYUYV            bool
	VideoQualityMonitorEnabled             bool
	VideoQualityMonitorReportEvery         int
	VideoQualityMonitorSampleStride        int
	VideoQualityMonitorStatePath           string
	VideoCameraAdmissionStatePath          string
	VideoCaptureControlProfile             string
	VideoCaptureControls                   map[string]int
	CameraHostIndex                        int
	Enabled                                bool
	LiveSocketPath                         string
	ArchiveSocketPath                      string
	AnalysisSocketPath                     string
	AnalysisSharedMemoryPath               string
	AnalysisNotifySocketPath               string
	FrameWatchdogSec                       int
	LiveWriteTimeoutMs                     int
	ResourceGovernorEnabled                bool
	ResourceGovernorPollSec                int
	ResourceGovernorThermalSoftC           int
	ResourceGovernorThermalHardC           int
	ResourceGovernorBPUThreshold           int
	ResourceGovernorStatePath              string
	CapabilityInventory                    capability.Inventory
	CapabilityProbeTimeoutMS               int
	CameraControlOptimizerPolicy           string
	CameraControlOptimizerMode             string
	CameraControlOptimizerTargetStream     string
	CameraControlOptimizerSampleStrategy   string
	CameraControlOptimizerTickMs           int
	ToneMappingTargetLuma                  int
	CameraControlOptimizerTargetLumaSun    int
	CameraControlOptimizerTargetLumaDay    int
	CameraControlOptimizerTargetLumaSunset int
	CameraControlOptimizerTargetLumaNight  int
	CameraControlOptimizerSunThreshold     int
	CameraControlOptimizerDayThreshold     int
	CameraControlOptimizerSunsetThreshold  int
	CameraControlOptimizerNightThreshold   int
	ToneMappingHysteresis                  int
	CameraControlOptimizerSampleWidth      int
	CameraControlOptimizerSampleHeight     int
	CameraControlOptimizerStrictSample     bool
	CameraControlOptimizerReportEvery      int
	CameraControlOptimizerMetricSource     string
	CameraControlOptimizerQualityMaxAgeSec int
	CameraControlOptimizerControlProfile   string
	CameraControlOptimizerCooldownSec      int
	CameraControlOptimizerStableSamples    int
	CameraControlOptimizerControlMin       map[string]int
	CameraControlOptimizerControlMax       map[string]int
	CameraControlOptimizerControlSteps     map[string]int
	Sensor                                 string
	StateDirectory                         string
	EngineDirectory                        string
	ArchiveContent                         string
	ArchiveCodec                           string
	ToneMappingMode                        string
	ToneMappingAccelerator                 string
	ToneMappingGridColumns                 int
	ToneMappingGridRows                    int
	ToneMappingStatisticsHz                int
	ToneMappingMaxShadowGain               float64
	ToneMappingMaxHighlightReduce          float64
	ToneMappingTemporalAlpha               float64
	ToneMappingNoiseGuard                  bool
}

func normalizeConfig(graph productconfig.SourceGraph) engineConfig {
	source := graph.Source
	processing := source.Processing
	acceleration := source.Acceleration
	var live, analysis, archive productconfig.StreamConfig
	if graph.Live != nil {
		live = graph.Live.Config
	}
	if graph.Analysis != nil {
		analysis = graph.Analysis.Config
	}
	if graph.Archive != nil {
		archive = graph.Archive.Config
	}
	toneMode := strings.ToLower(strings.TrimSpace(processing.ToneMapping.Mode))
	cameraControlMode := strings.ToLower(strings.TrimSpace(processing.CameraControl.Mode))
	if cameraControlMode == "" {
		cameraControlMode = "off"
	}
	cameraControlPolicy := strings.ToLower(strings.TrimSpace(processing.CameraControl.Policy))
	if cameraControlPolicy == "" {
		cameraControlPolicy = "auto"
	}
	if cameraControlMode == "off" || cameraControlPolicy == "off" {
		cameraControlPolicy = "off"
	}
	engineDir := strings.TrimSpace(graph.Runtime.EngineDirectory)
	if engineDir != "" {
		engineDir = filepath.Clean(engineDir)
	}
	return engineConfig{
		SourceID:                    graph.SourceID,
		SourceIdentity:              graph.Source.Identity,
		LiveStreamID:                namedStreamID(graph.Live),
		AnalysisStreamID:            namedStreamID(graph.Analysis),
		ArchiveStreamID:             namedStreamID(graph.Archive),
		LiveEnabled:                 graph.Live != nil,
		AnalysisEnabled:             graph.Analysis != nil,
		ArchiveEnabled:              graph.Archive != nil,
		VideoOrientation:            processing.Orientation,
		RawWidth:                    source.Width,
		RawHeight:                   source.Height,
		AnalysisWidth:               analysis.Width,
		AnalysisHeight:              analysis.Height,
		AnalysisFPS:                 analysis.FPS,
		LiveWidth:                   live.Width,
		LiveHeight:                  live.Height,
		LiveFPS:                     live.FPS,
		ArchiveWidth:                archive.Width,
		ArchiveHeight:               archive.Height,
		ArchiveFPS:                  archive.FPS,
		ArchiveRateControl:          archive.RateControl,
		CanonicalCaptureDevice:      source.Device,
		CanonicalAnalysisDevice:     source.Device,
		CaptureSourcePath:           source.Device,
		AnalysisSourcePath:          source.Device,
		VideoRequireHWCodec:         acceleration.RequireHardwareCodec,
		VideoAllowSWFallback:        acceleration.AllowSoftwareFallback,
		VideoTransport:              source.Transport,
		VideoEncodeProvider:         acceleration.EncodeProvider,
		VideoDecodeProvider:         acceleration.DecodeProvider,
		LiveFormat:                  live.Codec,
		AnalysisFormat:              analysis.PixelFormat,
		VideoCaptureFPS:             source.FPS,
		VideoCaptureWarmupSec:       source.WarmupSeconds,
		LiveBitrateKbps:             live.BitrateKbps,
		ArchiveBitrateKbps:          archive.BitrateKbps,
		VideoCaptureSelectTimeoutMs: source.SelectTimeoutMS,
		VideoCaptureMaxTimeouts:     source.MaxConsecutiveStalls,
		VideoCaptureDevice:          source.Device,
		VideoCaptureFormat:          source.Format,
		VideoCaptureCodecPreference: source.CodecPreference,
		VideoCaptureCodecPolicy:     source.CodecPolicy,
		VideoCapturePolicyProvider:  source.ModePolicy,
		VideoCaptureAspectPolicy:    source.AspectPolicy,
		VideoCaptureAllowScaledYUYV: source.AllowScaledYUYV,
		VideoCapturePreferExactYUYV: source.PreferExactYUYV,
		// Capture owners retain sampling intent across output attach/detach.
		// Workers and the controller gate measurement on the active analysis input.
		VideoQualityMonitorEnabled:             processing.Quality.Enabled,
		VideoQualityMonitorReportEvery:         processing.Quality.ReportEvery,
		VideoQualityMonitorSampleStride:        processing.Quality.SampleStride,
		VideoQualityMonitorStatePath:           processing.Quality.StatePath,
		VideoCameraAdmissionStatePath:          processing.Quality.AdmissionPath,
		VideoCaptureControlProfile:             source.ControlProfile,
		VideoCaptureControls:                   cloneIntMapConfig(source.Controls),
		CameraHostIndex:                        source.HostIndex,
		Enabled:                                true,
		LiveSocketPath:                         live.Socket,
		ArchiveSocketPath:                      archive.Socket,
		AnalysisSocketPath:                     analysis.Socket,
		AnalysisSharedMemoryPath:               analysis.SharedMemory,
		AnalysisNotifySocketPath:               analysis.NotifySocket,
		FrameWatchdogSec:                       graph.Runtime.FrameWatchdogSeconds,
		LiveWriteTimeoutMs:                     graph.Runtime.LiveWriteTimeoutMS,
		ResourceGovernorEnabled:                graph.Runtime.Governor.Enabled,
		ResourceGovernorPollSec:                graph.Runtime.Governor.PollSeconds,
		ResourceGovernorThermalSoftC:           graph.Runtime.Governor.ThermalSoftC,
		ResourceGovernorThermalHardC:           graph.Runtime.Governor.ThermalHardC,
		ResourceGovernorBPUThreshold:           graph.Runtime.Governor.BPUThreshold,
		ResourceGovernorStatePath:              sourceScopedRuntimePath(graph.Runtime.Governor.StatePath, graph.SourceID, graph.SourceCount),
		CapabilityProbeTimeoutMS:               acceleration.Discovery.ProbeTimeoutMS,
		CameraControlOptimizerPolicy:           cameraControlPolicy,
		CameraControlOptimizerMode:             cameraControlMode,
		CameraControlOptimizerTargetStream:     "auto",
		CameraControlOptimizerSampleStrategy:   "auto",
		CameraControlOptimizerTickMs:           processing.CameraControl.TickMS,
		ToneMappingTargetLuma:                  processing.ToneMapping.TargetLuma,
		CameraControlOptimizerTargetLumaSun:    processing.CameraControl.TargetLumaSun,
		CameraControlOptimizerTargetLumaDay:    processing.CameraControl.TargetLumaDay,
		CameraControlOptimizerTargetLumaSunset: processing.CameraControl.TargetLumaSunset,
		CameraControlOptimizerTargetLumaNight:  processing.CameraControl.TargetLumaNight,
		CameraControlOptimizerSunThreshold:     processing.CameraControl.SunThreshold,
		CameraControlOptimizerDayThreshold:     processing.CameraControl.DayThreshold,
		CameraControlOptimizerSunsetThreshold:  processing.CameraControl.SunsetThreshold,
		CameraControlOptimizerNightThreshold:   processing.CameraControl.NightThreshold,
		ToneMappingHysteresis:                  processing.ToneMapping.Hysteresis,
		CameraControlOptimizerSampleWidth:      processing.CameraControl.SampleWidth,
		CameraControlOptimizerSampleHeight:     processing.CameraControl.SampleHeight,
		CameraControlOptimizerStrictSample:     processing.CameraControl.StrictSample,
		CameraControlOptimizerReportEvery:      processing.CameraControl.ReportEvery,
		CameraControlOptimizerMetricSource:     "quality_file",
		CameraControlOptimizerQualityMaxAgeSec: processing.CameraControl.QualityMaxAgeSeconds,
		CameraControlOptimizerControlProfile:   processing.CameraControl.ControlProfile,
		CameraControlOptimizerCooldownSec:      processing.CameraControl.CooldownSeconds,
		CameraControlOptimizerStableSamples:    processing.CameraControl.StableSamples,
		CameraControlOptimizerControlMin:       cloneIntMapConfig(processing.CameraControl.ControlMin),
		CameraControlOptimizerControlMax:       cloneIntMapConfig(processing.CameraControl.ControlMax),
		CameraControlOptimizerControlSteps:     cloneIntMapConfig(processing.CameraControl.ControlSteps),
		Sensor:                                 source.Sensor,
		StateDirectory:                         filepath.Join(graph.Runtime.StateDirectory, graph.SourceID),
		EngineDirectory:                        engineDir,
		ArchiveContent:                         strings.ToLower(strings.TrimSpace(archive.Content)),
		ArchiveCodec:                           strings.ToLower(strings.TrimSpace(archive.Codec)),
		ToneMappingMode:                        toneMode,
		ToneMappingAccelerator:                 strings.ToLower(strings.TrimSpace(processing.ToneMapping.Accelerator)),
		ToneMappingGridColumns:                 processing.ToneMapping.GridColumns,
		ToneMappingGridRows:                    processing.ToneMapping.GridRows,
		ToneMappingStatisticsHz:                processing.ToneMapping.StatisticsHz,
		ToneMappingMaxShadowGain:               processing.ToneMapping.MaxShadowGain,
		ToneMappingMaxHighlightReduce:          processing.ToneMapping.MaxHighlightReduce,
		ToneMappingTemporalAlpha:               processing.ToneMapping.TemporalAlpha,
		ToneMappingNoiseGuard:                  source.NoiseGuardEnabled(),
	}
}

func sourceScopedRuntimePath(path, sourceID string, sourceCount int) string {
	path = strings.TrimSpace(path)
	if path == "" || sourceCount <= 1 {
		return path
	}
	return filepath.Join(filepath.Dir(path), "sources", sourceID, filepath.Base(path))
}

func namedStreamID(stream *productconfig.NamedStream) string {
	if stream == nil {
		return ""
	}
	return stream.ID
}

func cloneIntMapConfig(values map[string]int) map[string]int {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]int, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
