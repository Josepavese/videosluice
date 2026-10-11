package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	SchemaVersion = 3
	maxConfigSize = 2 * 1024 * 1024
)

var graphIDPattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
var sourceIdentityPattern = regexp.MustCompile(`^[a-z][a-z0-9._:-]{0,127}$`)

// Config is the product-facing video graph contract. It deliberately contains
// no application task or consumer-specific concepts. Integrators declare named
// sources and named outputs; every output binds explicitly to one source. The
// broker owns one capture graph per source and the fan-out within that graph.
type Config struct {
	SchemaVersion int                     `json:"schema_version"`
	Sources       map[string]SourceConfig `json:"sources"`
	Streams       map[string]StreamConfig `json:"streams"`
	Runtime       RuntimeConfig           `json:"runtime"`
}

type SourceConfig struct {
	Identity             string             `json:"identity"`
	Transport            string             `json:"transport"`
	Device               string             `json:"device"`
	Format               string             `json:"format"`
	CodecPreference      string             `json:"codec_preference"`
	CodecPolicy          string             `json:"codec_policy"`
	ModePolicy           string             `json:"mode_policy"`
	AspectPolicy         string             `json:"aspect_policy"`
	AllowScaledYUYV      bool               `json:"allow_scaled_yuyv"`
	PreferExactYUYV      bool               `json:"prefer_exact_yuyv"`
	Width                int                `json:"width"`
	Height               int                `json:"height"`
	FPS                  int                `json:"fps"`
	WarmupSeconds        int                `json:"warmup_seconds"`
	SelectTimeoutMS      int                `json:"select_timeout_ms"`
	MaxConsecutiveStalls int                `json:"max_consecutive_stalls"`
	ControlProfile       string             `json:"control_profile"`
	Controls             map[string]int     `json:"controls,omitempty"`
	Sensor               string             `json:"sensor,omitempty"`
	HostIndex            int                `json:"host_index"`
	Processing           ProcessingConfig   `json:"processing"`
	Acceleration         AccelerationConfig `json:"acceleration"`
}

type StreamConfig struct {
	Type         string `json:"type"`
	SourceRef    string `json:"source_ref"`
	Socket       string `json:"socket,omitempty"`
	SharedMemory string `json:"shared_memory,omitempty"`
	NotifySocket string `json:"notify_socket,omitempty"`
	Codec        string `json:"codec,omitempty"`
	PixelFormat  string `json:"pixel_format,omitempty"`
	Width        int    `json:"width"`
	Height       int    `json:"height"`
	FPS          int    `json:"fps"`
	BitrateKbps  int    `json:"bitrate_kbps,omitempty"`
	Content      string `json:"content,omitempty"`
	RateControl  string `json:"rate_control,omitempty"`
}

type NamedStream struct {
	ID     string
	Config StreamConfig
}

// SourceRoleProfile is one catalog-authorized media shape for a logical role
// on a named capture source. It is runtime admission metadata rather than a
// persisted configuration: Config remains the exact active graph, while a
// routed Service keeps this bounded universe so hardware workers can prepare a
// candidate branch without reopening the capture owner.
type SourceRoleProfile struct {
	Role        string
	Codec       string
	PixelFormat string
	Width       int
	Height      int
	FPS         int
	BitrateKbps int
	Content     string
	RateControl string
}

// SourceGraph is the validated implementation boundary for one capture owner.
// A role is nil when that source has no output of that type. Schema v3 permits
// one output per role and source; fan-out consumers share the declared endpoint.
type SourceGraph struct {
	SourceID    string
	SourceCount int
	Source      SourceConfig
	Live        *NamedStream
	Analysis    *NamedStream
	Archive     *NamedStream
	Runtime     RuntimeConfig
}

type ProcessingConfig struct {
	Orientation   string                       `json:"orientation"`
	Quality       QualityConfig                `json:"quality"`
	ToneMapping   ToneMappingConfig            `json:"tone_mapping"`
	CameraControl CameraControlOptimizerConfig `json:"camera_control_optimizer"`
}

type QualityConfig struct {
	// Enabled is saved sampling intent. The sampler is active only while an
	// analysis branch is bound to this source; moving the branch does not
	// erase that intent or force a hidden analysis stream onto the old source.
	Enabled       bool   `json:"enabled"`
	ReportEvery   int    `json:"report_every_frames"`
	SampleStride  int    `json:"sample_stride"`
	StatePath     string `json:"state_path,omitempty"`
	AdmissionPath string `json:"admission_path,omitempty"`
}

type ToneMappingConfig struct {
	Profile            string  `json:"profile"`
	Accelerator        string  `json:"accelerator"`
	Mode               string  `json:"mode"`
	TargetLuma         int     `json:"target_luma"`
	Hysteresis         int     `json:"hysteresis"`
	GridColumns        int     `json:"grid_columns"`
	GridRows           int     `json:"grid_rows"`
	StatisticsHz       int     `json:"statistics_hz"`
	MaxShadowGain      float64 `json:"max_shadow_gain"`
	MaxHighlightReduce float64 `json:"max_highlight_reduction"`
	TemporalAlpha      float64 `json:"temporal_alpha"`
	NoiseGuard         *bool   `json:"noise_guard,omitempty"`
}

type CameraControlOptimizerConfig struct {
	Policy               string         `json:"policy"`
	Mode                 string         `json:"mode"`
	TickMS               int            `json:"tick_ms"`
	TargetLumaSun        int            `json:"target_luma_sun"`
	TargetLumaDay        int            `json:"target_luma_day"`
	TargetLumaSunset     int            `json:"target_luma_sunset"`
	TargetLumaNight      int            `json:"target_luma_night"`
	SunThreshold         int            `json:"sun_threshold"`
	DayThreshold         int            `json:"day_threshold"`
	SunsetThreshold      int            `json:"sunset_threshold"`
	NightThreshold       int            `json:"night_threshold"`
	SampleWidth          int            `json:"sample_width"`
	SampleHeight         int            `json:"sample_height"`
	StrictSample         bool           `json:"strict_sample"`
	ReportEvery          int            `json:"report_every"`
	QualityMaxAgeSeconds int            `json:"quality_max_age_seconds"`
	ControlProfile       string         `json:"control_profile"`
	CooldownSeconds      int            `json:"cooldown_seconds"`
	StableSamples        int            `json:"stable_samples"`
	ControlMin           map[string]int `json:"control_min,omitempty"`
	ControlMax           map[string]int `json:"control_max,omitempty"`
	ControlSteps         map[string]int `json:"control_steps,omitempty"`
}

type RuntimeConfig struct {
	FrameAccess          *FrameAccessConfig `json:"frame_access,omitempty"`
	FrameWatchdogSeconds int                `json:"frame_watchdog_seconds"`
	LiveWriteTimeoutMS   int                `json:"live_write_timeout_ms"`
	StateDirectory       string             `json:"state_directory"`
	RuntimeDirectory     string             `json:"runtime_directory"`
	EngineDirectory      string             `json:"engine_directory,omitempty"`
	Status               StatusConfig       `json:"status"`
	Governor             GovernorConfig     `json:"governor"`
}

type StatusConfig struct {
	Socket   string `json:"socket,omitempty"`
	Interval int    `json:"interval_seconds"`
}

type GovernorConfig struct {
	Enabled      bool   `json:"enabled"`
	PollSeconds  int    `json:"poll_seconds"`
	ThermalSoftC int    `json:"thermal_soft_celsius"`
	ThermalHardC int    `json:"thermal_hard_celsius"`
	BPUThreshold int    `json:"bpu_threshold_percent"`
	StatePath    string `json:"state_path,omitempty"`
}

// StreamGovernorStatePath returns the stable governor publication owned by a
// named stream. The source-local governor files are implementation telemetry;
// consumers use this path so a routing cutover can move the stream without
// changing consumer configuration or introducing an implicit source.
func StreamGovernorStatePath(basePath, streamID string) (string, error) {
	basePath = strings.TrimSpace(basePath)
	streamID = strings.TrimSpace(streamID)
	if err := validateGovernorStatePath(basePath); err != nil {
		return "", err
	}
	if !graphIDPattern.MatchString(streamID) {
		return "", fmt.Errorf("invalid stream id=%q", streamID)
	}
	return filepath.Join(filepath.Dir(basePath), "streams", streamID, filepath.Base(basePath)), nil
}

func validateGovernorStatePath(path string) error {
	if err := validateAbsolutePath("resource governor state path", path, true); err != nil {
		return err
	}
	if filepath.Clean(path) != path || strings.HasPrefix(path, "//") {
		return errors.New("resource governor state path must be canonical")
	}
	return nil
}

type AccelerationConfig struct {
	EncodeProvider        string                    `json:"encode_provider"`
	DecodeProvider        string                    `json:"decode_provider"`
	RequireHardwareCodec  bool                      `json:"require_hardware_codec"`
	AllowSoftwareFallback bool                      `json:"allow_software_fallback"`
	Discovery             CapabilityDiscoveryConfig `json:"discovery"`
}

type CapabilityDiscoveryConfig struct {
	Mode           string `json:"mode"`
	ProbeTimeoutMS int    `json:"probe_timeout_ms"`
}

func Load(path string) (Config, error) {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) {
		return Config{}, errors.New("config path must be absolute")
	}
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open video-broker config: %w", err)
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, maxConfigSize+1))
	if err != nil {
		return Config{}, fmt.Errorf("read video-broker config: %w", err)
	}
	if len(body) > maxConfigSize {
		return Config{}, fmt.Errorf("video-broker config exceeds %d bytes", maxConfigSize)
	}
	var cfg Config
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode video-broker config: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("video-broker config contains multiple JSON documents")
		}
		return Config{}, fmt.Errorf("decode trailing video-broker config: %w", err)
	}
	return Normalize(cfg)
}

// Normalize applies stable product defaults and validates an in-memory graph.
// It lets embedders use the library API without first serializing JSON.
func Normalize(cfg Config) (Config, error) {
	cfg = Clone(cfg)
	applyDefaults(&cfg)
	if err := Validate(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Clone returns a fully detached graph value. Config contains nested operator
// control maps and optional pointers; copying the top-level maps alone would
// let a caller mutate a graph after it has crossed an admission boundary.
func Clone(cfg Config) Config {
	cloned := cfg
	cloned.Runtime.FrameAccess = cloneFrameAccess(cfg.Runtime.FrameAccess)
	cloned.Sources = make(map[string]SourceConfig, len(cfg.Sources))
	for sourceID, source := range cfg.Sources {
		source.Controls = cloneIntMap(source.Controls)
		source.Processing.CameraControl.ControlMin = cloneIntMap(source.Processing.CameraControl.ControlMin)
		source.Processing.CameraControl.ControlMax = cloneIntMap(source.Processing.CameraControl.ControlMax)
		source.Processing.CameraControl.ControlSteps = cloneIntMap(source.Processing.CameraControl.ControlSteps)
		if source.Processing.ToneMapping.NoiseGuard != nil {
			noiseGuard := *source.Processing.ToneMapping.NoiseGuard
			source.Processing.ToneMapping.NoiseGuard = &noiseGuard
		}
		cloned.Sources[sourceID] = source
	}
	cloned.Streams = make(map[string]StreamConfig, len(cfg.Streams))
	for streamID, stream := range cfg.Streams {
		cloned.Streams[streamID] = stream
	}
	return cloned
}

func cloneIntMap(input map[string]int) map[string]int {
	if input == nil {
		return nil
	}
	cloned := make(map[string]int, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func applyDefaults(cfg *Config) {
	for id, source := range cfg.Sources {
		applySourceDefaults(&source)
		cfg.Sources[id] = source
	}
	for id, stream := range cfg.Streams {
		applyStreamDefaults(&stream)
		cfg.Streams[id] = stream
	}
	applyRuntimeDefaults(&cfg.Runtime)
}

func applyStreamDefaults(stream *StreamConfig) {
	stream.Type = strings.ToLower(strings.TrimSpace(stream.Type))
	stream.Codec = normalizeCodec(stream.Codec)
	stream.RateControl = strings.ToLower(strings.TrimSpace(stream.RateControl))
	switch stream.Type {
	case "live":
		if stream.Codec == "" {
			stream.Codec = "h265"
		}
		if stream.Content == "" {
			stream.Content = "processed"
		}
		if stream.RateControl == "" {
			stream.RateControl = "cbr"
		}
	case "analysis":
		if stream.PixelFormat == "" {
			stream.PixelFormat = "nv12"
		}
	case "archive":
		if stream.Codec == "" {
			stream.Codec = "auto"
		}
		if stream.Content == "" {
			stream.Content = "adaptive"
		}
		if stream.RateControl == "" {
			stream.RateControl = "cbr"
		}
	}
}

func applyRuntimeDefaults(runtime *RuntimeConfig) {
	if runtime.FrameWatchdogSeconds <= 0 {
		runtime.FrameWatchdogSeconds = 12
	}
	if runtime.LiveWriteTimeoutMS <= 0 {
		runtime.LiveWriteTimeoutMS = 2000
	}
	if runtime.Status.Interval <= 0 {
		runtime.Status.Interval = 10
	}
}

// NormalizeSource validates one independently catalogued source without
// requiring it to be active in the current routing plan.
func NormalizeSource(sourceID string, source SourceConfig) (SourceConfig, error) {
	if !graphIDPattern.MatchString(sourceID) {
		return SourceConfig{}, fmt.Errorf("invalid source id=%q", sourceID)
	}
	applySourceDefaults(&source)
	if err := validateSource(sourceID, source); err != nil {
		return SourceConfig{}, err
	}
	return source, nil
}

// NormalizeStream validates one catalog option against its selected source.
// Cross-stream constraints remain enforced when a complete Config is built.
func NormalizeStream(streamID string, stream StreamConfig, source SourceConfig) (StreamConfig, error) {
	if !graphIDPattern.MatchString(streamID) {
		return StreamConfig{}, fmt.Errorf("invalid stream id=%q", streamID)
	}
	applyStreamDefaults(&stream)
	if err := validateStream(streamID, stream, source); err != nil {
		return StreamConfig{}, err
	}
	return stream, nil
}

// NormalizeRuntime validates the standalone runtime boundary used by both a
// static Config and a routing catalog.
func NormalizeRuntime(runtime RuntimeConfig) (RuntimeConfig, error) {
	runtime.FrameAccess = cloneFrameAccess(runtime.FrameAccess)
	applyRuntimeDefaults(&runtime)
	if err := validateFrameAccess(runtime.FrameAccess, nil, runtime.Status.Socket, runtime.Governor.StatePath); err != nil {
		return RuntimeConfig{}, err
	}
	for label, value := range map[string]string{
		"runtime directory": runtime.RuntimeDirectory,
		"state directory":   runtime.StateDirectory,
	} {
		if err := validateAbsolutePath(label, value, true); err != nil {
			return RuntimeConfig{}, err
		}
	}
	if err := validateAbsolutePath("engine directory", runtime.EngineDirectory, false); err != nil {
		return RuntimeConfig{}, err
	}
	if err := validateAbsolutePath("status socket", runtime.Status.Socket, false); err != nil {
		return RuntimeConfig{}, err
	}
	if runtime.Governor.Enabled {
		if err := validateGovernorStatePath(runtime.Governor.StatePath); err != nil {
			return RuntimeConfig{}, err
		}
	}
	return runtime, nil
}

func applySourceDefaults(source *SourceConfig) {
	if source.SelectTimeoutMS <= 0 {
		source.SelectTimeoutMS = 2000
	}
	if source.MaxConsecutiveStalls <= 0 {
		source.MaxConsecutiveStalls = 3
	}
	if source.Processing.Orientation == "" {
		source.Processing.Orientation = "normal"
	}
	if source.Processing.ToneMapping.Mode == "" {
		source.Processing.ToneMapping.Mode = "auto"
	}
	if source.Processing.ToneMapping.Profile == "" {
		source.Processing.ToneMapping.Profile = "adaptive"
	}
	if source.Processing.ToneMapping.Accelerator == "" {
		source.Processing.ToneMapping.Accelerator = "auto"
	}
	if source.Processing.ToneMapping.TargetLuma <= 0 {
		source.Processing.ToneMapping.TargetLuma = 128
	}
	if source.Processing.ToneMapping.Hysteresis <= 0 {
		source.Processing.ToneMapping.Hysteresis = 8
	}
	if source.Processing.ToneMapping.GridColumns <= 0 {
		source.Processing.ToneMapping.GridColumns = 16
	}
	if source.Processing.ToneMapping.GridRows <= 0 {
		source.Processing.ToneMapping.GridRows = 9
	}
	if source.Processing.ToneMapping.StatisticsHz <= 0 {
		source.Processing.ToneMapping.StatisticsHz = 8
	}
	if source.Processing.ToneMapping.MaxShadowGain <= 0 {
		source.Processing.ToneMapping.MaxShadowGain = 2.4
	}
	if source.Processing.ToneMapping.MaxHighlightReduce <= 0 {
		source.Processing.ToneMapping.MaxHighlightReduce = 0.20
	}
	if source.Processing.ToneMapping.TemporalAlpha <= 0 {
		source.Processing.ToneMapping.TemporalAlpha = 0.35
	}
	if source.Processing.ToneMapping.NoiseGuard == nil {
		enabled := true
		source.Processing.ToneMapping.NoiseGuard = &enabled
	}
	if strings.EqualFold(strings.TrimSpace(source.Processing.ToneMapping.Profile), "adaptive") {
		// Adaptive is a governed product policy. Reapply every managed value so a
		// materialized old configuration cannot pin stale image tuning forever.
		source.Processing.ToneMapping.TargetLuma = 128
		source.Processing.ToneMapping.Hysteresis = 12
		source.Processing.ToneMapping.GridColumns = 16
		source.Processing.ToneMapping.GridRows = 9
		source.Processing.ToneMapping.StatisticsHz = 8
		source.Processing.ToneMapping.MaxShadowGain = 2.4
		source.Processing.ToneMapping.MaxHighlightReduce = 0.20
		source.Processing.ToneMapping.TemporalAlpha = 0.35
		enabled := true
		source.Processing.ToneMapping.NoiseGuard = &enabled
	}
	if source.Processing.CameraControl.Policy == "" {
		source.Processing.CameraControl.Policy = "auto"
	}
	if source.Processing.CameraControl.Mode == "" {
		source.Processing.CameraControl.Mode = "off"
	}
	if source.Acceleration.EncodeProvider == "" {
		source.Acceleration.EncodeProvider = "auto"
	}
	if source.Acceleration.DecodeProvider == "" {
		source.Acceleration.DecodeProvider = "auto"
	}
	if source.Acceleration.Discovery.Mode == "" {
		source.Acceleration.Discovery.Mode = "auto"
	}
	if source.Acceleration.Discovery.ProbeTimeoutMS <= 0 {
		source.Acceleration.Discovery.ProbeTimeoutMS = 8000
	}
}

func Validate(cfg Config) error {
	if cfg.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported video-broker schema_version=%d", cfg.SchemaVersion)
	}
	if len(cfg.Sources) == 0 {
		return errors.New("video-broker requires at least one named source")
	}
	if len(cfg.Streams) == 0 {
		return errors.New("video-broker requires at least one named stream")
	}
	if err := validateFrameAccess(cfg.Runtime.FrameAccess, cfg.Streams, cfg.Runtime.Status.Socket, cfg.Runtime.Governor.StatePath); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"runtime directory": cfg.Runtime.RuntimeDirectory,
		"state directory":   cfg.Runtime.StateDirectory,
	} {
		if err := validateAbsolutePath(label, value, true); err != nil {
			return err
		}
	}
	if err := validateAbsolutePath("engine directory", cfg.Runtime.EngineDirectory, false); err != nil {
		return err
	}
	if err := validateAbsolutePath("status socket", cfg.Runtime.Status.Socket, false); err != nil {
		return err
	}
	if cfg.Runtime.Governor.Enabled {
		if err := validateGovernorStatePath(cfg.Runtime.Governor.StatePath); err != nil {
			return err
		}
	}
	physicalOwners := make(map[string]string, len(cfg.Sources))
	identityOwners := make(map[string]string, len(cfg.Sources))
	statePathOwners := make(map[string]string, len(cfg.Sources)*2)
	referenced := make(map[string]bool, len(cfg.Sources))
	roleOwners := make(map[string]string, len(cfg.Streams))
	for sourceID, source := range cfg.Sources {
		if !graphIDPattern.MatchString(sourceID) {
			return fmt.Errorf("invalid source id=%q", sourceID)
		}
		if err := validateSource(sourceID, source); err != nil {
			return err
		}
		if owner, exists := identityOwners[source.Identity]; exists {
			return fmt.Errorf("sources %q and %q declare the same source identity", owner, sourceID)
		}
		identityOwners[source.Identity] = sourceID
		key := physicalSourceKey(source)
		if owner, exists := physicalOwners[key]; exists {
			return fmt.Errorf("sources %q and %q declare the same physical source", owner, sourceID)
		}
		physicalOwners[key] = sourceID
		for label, path := range map[string]string{
			"quality state":    source.Processing.Quality.StatePath,
			"camera admission": source.Processing.Quality.AdmissionPath,
		} {
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}
			clean := filepath.Clean(path)
			if owner, exists := statePathOwners[clean]; exists {
				return fmt.Errorf("sources %q and %q share %s path %s", owner, sourceID, label, clean)
			}
			statePathOwners[clean] = sourceID
		}
	}
	endpoints := make(map[string]string, len(cfg.Streams)*3)
	streamIDs := make([]string, 0, len(cfg.Streams))
	for streamID := range cfg.Streams {
		streamIDs = append(streamIDs, streamID)
	}
	sort.Strings(streamIDs)
	for _, streamID := range streamIDs {
		stream := cfg.Streams[streamID]
		if !graphIDPattern.MatchString(streamID) {
			return fmt.Errorf("invalid stream id=%q", streamID)
		}
		source, exists := cfg.Sources[stream.SourceRef]
		if !exists || strings.TrimSpace(stream.SourceRef) != stream.SourceRef {
			return fmt.Errorf("stream %q references unknown source %q", streamID, stream.SourceRef)
		}
		role := strings.ToLower(strings.TrimSpace(stream.Type))
		roleKey := stream.SourceRef + "\x00" + role
		if owner, exists := roleOwners[roleKey]; exists {
			return fmt.Errorf("streams %q and %q duplicate %s role for source %q", owner, streamID, role, stream.SourceRef)
		}
		roleOwners[roleKey] = streamID
		if err := validateStream(streamID, stream, source); err != nil {
			return err
		}
		for endpointRole, endpoint := range map[string]string{
			"socket": stream.Socket, "shared_memory": stream.SharedMemory, "notify_socket": stream.NotifySocket,
		} {
			if strings.TrimSpace(endpoint) == "" {
				continue
			}
			if err := validateAbsolutePath("stream "+streamID+" "+endpointRole, endpoint, true); err != nil {
				return err
			}
			clean := filepath.Clean(endpoint)
			if owner, exists := endpoints[clean]; exists {
				return fmt.Errorf("streams %q and %q share endpoint %s", owner, streamID, clean)
			}
			endpoints[clean] = streamID
		}
		referenced[stream.SourceRef] = true
	}
	for sourceID := range cfg.Sources {
		if !referenced[sourceID] {
			return fmt.Errorf("source %q has no bound streams", sourceID)
		}
	}
	return nil
}

func validateSource(sourceID string, source SourceConfig) error {
	if !sourceIdentityPattern.MatchString(source.Identity) {
		return fmt.Errorf("source %q identity is not canonical", sourceID)
	}
	if err := validateVideoMode("source "+sourceID, source.Width, source.Height, source.FPS); err != nil {
		return err
	}
	transport := strings.ToLower(strings.TrimSpace(source.Transport))
	if !oneOf(transport, "v4l2", "v4l2_uvc", "vendor_hbn", "libcamera") {
		return fmt.Errorf("unsupported source %q transport=%q", sourceID, source.Transport)
	}
	format := strings.ToLower(strings.TrimSpace(source.Format))
	if !oneOf(format, "", "auto", "h264", "h265", "mjpeg", "yuyv", "nv12") {
		return fmt.Errorf("unsupported source %q format=%q", sourceID, source.Format)
	}
	if !oneOf(strings.ToLower(strings.TrimSpace(source.CodecPreference)), "", "h264", "h265") {
		return fmt.Errorf("unsupported source %q codec_preference=%q", sourceID, source.CodecPreference)
	}
	if !oneOf(strings.ToLower(strings.TrimSpace(source.CodecPolicy)), "", "prefer", "required") {
		return fmt.Errorf("unsupported source %q codec_policy=%q", sourceID, source.CodecPolicy)
	}
	if !oneOf(strings.ToLower(strings.TrimSpace(source.ModePolicy)), "", "auto", "exact", "analysis_quality") {
		return fmt.Errorf("unsupported source %q mode_policy=%q", sourceID, source.ModePolicy)
	}
	if !oneOf(strings.ToLower(strings.TrimSpace(source.AspectPolicy)), "", "exact", "crop", "letterbox") {
		return fmt.Errorf("unsupported source %q aspect_policy=%q", sourceID, source.AspectPolicy)
	}
	if transport == "v4l2" || transport == "v4l2_uvc" {
		if err := validateAbsolutePath("source "+sourceID+" device", source.Device, true); err != nil {
			return err
		}
	}
	if transport == "vendor_hbn" && strings.TrimSpace(source.Sensor) == "" {
		return fmt.Errorf("vendor_hbn source %q requires sensor", sourceID)
	}
	processing := source.Processing
	if processing.Quality.Enabled {
		if err := validateAbsolutePath("source "+sourceID+" quality state path", processing.Quality.StatePath, true); err != nil {
			return err
		}
		if err := validateAbsolutePath("source "+sourceID+" camera admission path", processing.Quality.AdmissionPath, true); err != nil {
			return err
		}
	}
	mode := strings.ToLower(strings.TrimSpace(processing.ToneMapping.Mode))
	if !oneOf(mode, "off", "global", "local", "auto") {
		return fmt.Errorf("source %q has unsupported tone_mapping.mode=%q", sourceID, processing.ToneMapping.Mode)
	}
	profile := strings.ToLower(strings.TrimSpace(processing.ToneMapping.Profile))
	if !oneOf(profile, "adaptive", "custom") {
		return fmt.Errorf("source %q has unsupported tone_mapping.profile=%q", sourceID, processing.ToneMapping.Profile)
	}
	accelerator := strings.ToLower(strings.TrimSpace(processing.ToneMapping.Accelerator))
	if !oneOf(accelerator, "auto", "cpu", "opencl") {
		return fmt.Errorf("source %q has unsupported tone_mapping.accelerator=%q", sourceID, processing.ToneMapping.Accelerator)
	}
	if processing.ToneMapping.GridColumns < 1 || processing.ToneMapping.GridColumns > 32 ||
		processing.ToneMapping.GridRows < 1 || processing.ToneMapping.GridRows > 24 {
		return fmt.Errorf("source %q tone mapping grid is outside supported bounds", sourceID)
	}
	if processing.ToneMapping.TemporalAlpha <= 0 || processing.ToneMapping.TemporalAlpha > 1 {
		return fmt.Errorf("source %q tone mapping temporal_alpha must be in (0,1]", sourceID)
	}
	if processing.ToneMapping.TargetLuma < 16 || processing.ToneMapping.TargetLuma > 235 ||
		processing.ToneMapping.Hysteresis < 1 || processing.ToneMapping.Hysteresis > 64 ||
		processing.ToneMapping.StatisticsHz < 1 || processing.ToneMapping.StatisticsHz > 30 ||
		processing.ToneMapping.MaxShadowGain < 1 || processing.ToneMapping.MaxShadowGain > 3 ||
		processing.ToneMapping.MaxHighlightReduce < 0 || processing.ToneMapping.MaxHighlightReduce > 0.75 {
		return fmt.Errorf("source %q tone mapping values are outside supported bounds", sourceID)
	}
	cameraPolicy := strings.ToLower(strings.TrimSpace(processing.CameraControl.Policy))
	if !oneOf(cameraPolicy, "auto", "on", "off") {
		return fmt.Errorf("source %q has unsupported camera_control_optimizer.policy=%q", sourceID, processing.CameraControl.Policy)
	}
	cameraMode := strings.ToLower(strings.TrimSpace(processing.CameraControl.Mode))
	if !oneOf(cameraMode, "off", "assist", "manual", "profiles") {
		return fmt.Errorf("source %q has unsupported camera_control_optimizer.mode=%q", sourceID, processing.CameraControl.Mode)
	}
	acceleration := source.Acceleration
	encodeProvider := strings.ToLower(strings.TrimSpace(acceleration.EncodeProvider))
	decodeProvider := strings.ToLower(strings.TrimSpace(acceleration.DecodeProvider))
	allowedProviders := []string{"auto", "native_sp", "gstreamer_mpp", "gstreamer_v4l2", "gstreamer_vendor", "software"}
	if !oneOf(encodeProvider, allowedProviders...) {
		return fmt.Errorf("source %q has unsupported encode_provider=%q", sourceID, acceleration.EncodeProvider)
	}
	if !oneOf(decodeProvider, allowedProviders...) {
		return fmt.Errorf("source %q has unsupported decode_provider=%q", sourceID, acceleration.DecodeProvider)
	}
	if encodeProvider != "auto" && decodeProvider != "auto" && (encodeProvider == "native_sp") != (decodeProvider == "native_sp") {
		return fmt.Errorf("source %q native_sp encode and decode providers must be selected together", sourceID)
	}
	if transport == "vendor_hbn" && (encodeProvider != "auto" && encodeProvider != "native_sp" || decodeProvider != "auto" && decodeProvider != "native_sp") {
		return fmt.Errorf("vendor_hbn source %q requires auto or native_sp codec providers", sourceID)
	}
	if !acceleration.AllowSoftwareFallback && (encodeProvider == "software" || decodeProvider == "software") {
		return fmt.Errorf("source %q software codec provider requires allow_software_fallback=true", sourceID)
	}
	if strings.ToLower(strings.TrimSpace(acceleration.Discovery.Mode)) != "auto" {
		return fmt.Errorf("source %q has unsupported acceleration.discovery.mode=%q", sourceID, acceleration.Discovery.Mode)
	}
	if acceleration.Discovery.ProbeTimeoutMS < 250 || acceleration.Discovery.ProbeTimeoutMS > 30000 {
		return fmt.Errorf("source %q acceleration discovery timeout is outside supported bounds", sourceID)
	}
	return nil
}

func validateStream(streamID string, stream StreamConfig, source SourceConfig) error {
	role := strings.ToLower(strings.TrimSpace(stream.Type))
	if !oneOf(role, "live", "analysis", "archive") {
		return fmt.Errorf("stream %q has unsupported type=%q", streamID, stream.Type)
	}
	if err := validateVideoMode("stream "+streamID, stream.Width, stream.Height, stream.FPS); err != nil {
		return err
	}
	if stream.FPS > source.FPS {
		return fmt.Errorf("stream %q fps exceeds source %q fps", streamID, stream.SourceRef)
	}
	switch role {
	case "live":
		if strings.TrimSpace(stream.Socket) == "" {
			return fmt.Errorf("live stream %q requires socket", streamID)
		}
		if !oneOf(strings.ToLower(strings.TrimSpace(stream.Codec)), "h264", "h265") {
			return fmt.Errorf("live stream %q requires h265 or an explicit h264 interoperability policy", streamID)
		}
		if stream.RateControl != "cbr" {
			return fmt.Errorf("live stream %q rate_control must be cbr", streamID)
		}
		if !strings.EqualFold(strings.TrimSpace(stream.Content), "processed") {
			return fmt.Errorf("live stream %q content must be processed", streamID)
		}
		if stream.PixelFormat != "" || stream.SharedMemory != "" || stream.NotifySocket != "" {
			return fmt.Errorf("live stream %q contains analysis-only fields", streamID)
		}
	case "analysis":
		if !strings.EqualFold(strings.TrimSpace(stream.PixelFormat), "nv12") {
			return fmt.Errorf("analysis stream %q currently requires nv12", streamID)
		}
		if strings.TrimSpace(stream.Socket) == "" && strings.TrimSpace(stream.SharedMemory) == "" {
			return fmt.Errorf("analysis stream %q requires socket or shared_memory", streamID)
		}
		if stream.Codec != "" || stream.Content != "" || stream.RateControl != "" || stream.BitrateKbps != 0 {
			return fmt.Errorf("analysis stream %q contains encoded-only fields", streamID)
		}
	case "archive":
		if strings.TrimSpace(stream.Socket) == "" {
			return fmt.Errorf("archive stream %q requires socket", streamID)
		}
		if !oneOf(strings.ToLower(strings.TrimSpace(stream.Codec)), "auto", "h264", "h265") {
			return fmt.Errorf("archive stream %q has unsupported codec=%q", streamID, stream.Codec)
		}
		if !oneOf(strings.ToLower(strings.TrimSpace(stream.Content)), "source", "processed", "adaptive") {
			return fmt.Errorf("archive stream %q has unsupported content=%q", streamID, stream.Content)
		}
		if !oneOf(stream.RateControl, "cbr", "avbr") {
			return fmt.Errorf("archive stream %q has unsupported rate_control=%q", streamID, stream.RateControl)
		}
		if strings.EqualFold(strings.TrimSpace(stream.Content), "source") &&
			!strings.EqualFold(strings.TrimSpace(source.Processing.ToneMapping.Mode), "off") {
			return fmt.Errorf("archive stream %q content source requires source tone_mapping.mode=off", streamID)
		}
		if stream.PixelFormat != "" || stream.SharedMemory != "" || stream.NotifySocket != "" {
			return fmt.Errorf("archive stream %q contains analysis-only fields", streamID)
		}
	}
	return nil
}

func normalizeCodec(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func validateAbsolutePath(label, value string, required bool) error {
	value = strings.TrimSpace(value)
	if value == "" && !required {
		return nil
	}
	if value == "" || !filepath.IsAbs(value) || filepath.Clean(value) == "/" {
		return fmt.Errorf("%s must be an absolute non-root path", label)
	}
	return nil
}

func physicalSourceKey(source SourceConfig) string {
	transport := strings.ToLower(strings.TrimSpace(source.Transport))
	device := strings.TrimSpace(source.Device)
	if device != "" {
		device = filepath.Clean(device)
	}
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d", transport, device, strings.TrimSpace(source.Sensor), source.HostIndex)
}

// SourceGraphs returns deterministic, source-owned runtime graphs. Config must
// already have passed Normalize or Validate.
func (cfg Config) SourceGraphs() []SourceGraph {
	sourceIDs := make([]string, 0, len(cfg.Sources))
	for sourceID := range cfg.Sources {
		sourceIDs = append(sourceIDs, sourceID)
	}
	sort.Strings(sourceIDs)
	graphs := make([]SourceGraph, 0, len(sourceIDs))
	index := make(map[string]int, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		index[sourceID] = len(graphs)
		graphs = append(graphs, SourceGraph{SourceID: sourceID, SourceCount: len(sourceIDs), Source: cfg.Sources[sourceID], Runtime: cfg.Runtime})
	}
	streamIDs := make([]string, 0, len(cfg.Streams))
	for streamID := range cfg.Streams {
		streamIDs = append(streamIDs, streamID)
	}
	sort.Strings(streamIDs)
	for _, streamID := range streamIDs {
		stream := cfg.Streams[streamID]
		named := &NamedStream{ID: streamID, Config: stream}
		graph := &graphs[index[stream.SourceRef]]
		switch stream.Type {
		case "live":
			graph.Live = named
		case "analysis":
			graph.Analysis = named
		case "archive":
			graph.Archive = named
		}
	}
	return graphs
}

func validateVideoMode(label string, width, height, fps int) error {
	if width <= 0 || height <= 0 || fps <= 0 {
		return fmt.Errorf("%s width, height and fps must be positive", label)
	}
	if width > 16384 || height > 16384 || fps > 240 {
		return fmt.Errorf("%s mode exceeds supported bounds", label)
	}
	if width%2 != 0 || height%2 != 0 {
		return fmt.Errorf("%s width and height must be even", label)
	}
	return nil
}

func oneOf(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func (cfg Config) StatusInterval() time.Duration {
	return time.Duration(cfg.Runtime.Status.Interval) * time.Second
}

func (cfg SourceConfig) NoiseGuardEnabled() bool {
	return cfg.Processing.ToneMapping.NoiseGuard == nil || *cfg.Processing.ToneMapping.NoiseGuard
}
