package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
	in "github.com/Josepavese/videosluice/frameingress"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

// Status is the current multi-source product projection. Source health and
// stream progress are deliberately separated so consumers never infer an
// output's capture owner from a process-global field.
type Status struct {
	Schema       string                  `json:"schema"`
	ObservedAt   time.Time               `json:"observed_at"`
	State        string                  `json:"state"`
	LastError    string                  `json:"last_error,omitempty"`
	LastChangeAt time.Time               `json:"last_change_at,omitempty"`
	Sources      map[string]SourceStatus `json:"sources"`
	Streams      map[string]StreamStatus `json:"streams"`
	FrameAccess  *in.MetadataStatus      `json:"frame_access,omitempty"`
}

type SourceStatus struct {
	State                  string                   `json:"state"`
	LastError              string                   `json:"last_error,omitempty"`
	LastChangeAt           time.Time                `json:"last_change_at,omitempty"`
	Identity               string                   `json:"identity"`
	Transport              string                   `json:"transport"`
	Device                 string                   `json:"device"`
	ConfiguredFormat       string                   `json:"configured_format"`
	CaptureCodec           string                   `json:"capture_codec,omitempty"`
	CaptureWidth           int                      `json:"capture_width,omitempty"`
	CaptureHeight          int                      `json:"capture_height,omitempty"`
	CaptureFPS             int                      `json:"capture_fps,omitempty"`
	CaptureDecodeCodec     string                   `json:"capture_decode_codec,omitempty"`
	CaptureDecodeRequested string                   `json:"capture_decode_requested,omitempty"`
	CaptureDecodeBackend   string                   `json:"capture_decode_backend,omitempty"`
	CaptureDecodeFallback  string                   `json:"capture_decode_fallback,omitempty"`
	CaptureChildPID        int                      `json:"capture_child_pid,omitempty"`
	CaptureChildRunning    bool                     `json:"capture_child_running"`
	FanoutControlMode      string                   `json:"fanout_control_mode,omitempty"`
	FanoutControlSupported bool                     `json:"fanout_control_supported"`
	HardwareRecovery       []HardwareRecoveryStatus `json:"hardware_recovery"`
	MediaEpoch             uint64                   `json:"media_epoch"`
	LastMediaAt            time.Time                `json:"last_media_at,omitempty"`
	CameraAdmitted         bool                     `json:"camera_admitted"`
	ToneMappingMode        string                   `json:"tone_mapping_mode"`
	ToneMappingAccelerator string                   `json:"tone_mapping_accelerator"`
	ToneMappingSupported   bool                     `json:"tone_mapping_supported"`
	ToneMappingBackend     string                   `json:"tone_mapping_backend,omitempty"`
	ToneMappingActiveMode  string                   `json:"tone_mapping_active_mode,omitempty"`
	ToneMappingActive      bool                     `json:"tone_mapping_active"`
	ToneMappingGainMin     float64                  `json:"tone_mapping_gain_min,omitempty"`
	ToneMappingGainMax     float64                  `json:"tone_mapping_gain_max,omitempty"`
	ToneMappingUpdates     uint64                   `json:"tone_mapping_updates"`
	ToneMappingGridColumns int                      `json:"tone_mapping_grid_columns,omitempty"`
	ToneMappingGridRows    int                      `json:"tone_mapping_grid_rows,omitempty"`
	OpticalSignalState     string                   `json:"optical_signal_state,omitempty"`
	OpticalSignalReason    string                   `json:"optical_signal_reason,omitempty"`
	CapabilityInventory    capability.Inventory     `json:"capability_inventory"`
	CapabilityPlan         capability.Plan          `json:"capability_plan"`
}

type StreamStatus struct {
	Performance StreamPerformance `json:"performance"`
	Type        string            `json:"type"`
	SourceRef   string            `json:"source_ref"`
	Codec       string            `json:"codec,omitempty"`
	Delivery    string            `json:"delivery,omitempty"`
	FPS         int               `json:"fps,omitempty"`
	Clients     int               `json:"clients"`
	Frames      uint64            `json:"frames"`
	Epoch       uint64            `json:"epoch"`
	LastFrameAt time.Time         `json:"last_frame_at,omitempty"`
	Drops       uint64            `json:"drops"`
	Disconnects uint64            `json:"disconnects"`
}

// StreamPerformance separates the persisted preference, current producer
// target and delivery measured over a bounded window. Reduced performance
// never changes the binding or the source's structural availability.
type StreamPerformance struct {
	State        string    `json:"state"`
	RequestedFPS int       `json:"requested_fps"`
	TargetFPS    int       `json:"target_fps"`
	ObservedFPS  float64   `json:"observed_fps"`
	WindowMS     int64     `json:"window_ms"`
	MeasuredAt   time.Time `json:"measured_at,omitempty"`
}

// HardwareRecoveryStatus exposes a bounded, provider-neutral recovery trial.
// A non-empty list means that software fallback is continuity-only and must
// not be accepted as the stable media graph.
type HardwareRecoveryStatus struct {
	ComponentID         string     `json:"component_id"`
	Stage               string     `json:"stage"`
	Format              string     `json:"format"`
	State               string     `json:"state"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
	FailureThreshold    int        `json:"failure_threshold"`
	Rejected            bool       `json:"rejected"`
	RetryScope          string     `json:"retry_scope"`
	RetryToken          uint64     `json:"retry_token,omitempty"`
	TrialMediaEpoch     uint64     `json:"trial_media_epoch,omitempty"`
	TrialChildPID       int        `json:"trial_child_pid,omitempty"`
	RetryAt             *time.Time `json:"retry_at,omitempty"`
	TrialDeadline       *time.Time `json:"trial_deadline,omitempty"`
	StableAfter         *time.Time `json:"stable_after,omitempty"`
}

type graphService struct {
	sourceID         string
	liveStreamID     string
	analysisStreamID string
	archiveStreamID  string
	runtime          *videoBrokerRuntime
	done             <-chan error
	plan             videoBrokerPlan
}

// Service owns every declared source through an independent supervisor. A
// source failure changes only the streams bound to that source; parent
// cancellation remains the sole normal whole-service shutdown boundary.
type Service struct {
	mu           sync.RWMutex
	sources      map[string]*sourceSupervisor
	config       productconfig.Config
	admission    map[string][]productconfig.SourceRoleProfile
	inventory    capability.Inventory
	allocator    *capabilityAllocator
	starter      sourceGraphStarter
	fanout       sourceFanoutBackend
	retryPolicy  sourceRestartPolicy
	managerCtx   context.Context
	done         <-chan error
	cancel       context.CancelFunc
	governorWake chan struct{}

	lifecycleMu sync.Mutex
	workers     sync.WaitGroup
	stopping    bool
}

type sourceGraphStarter func(
	context.Context,
	productconfig.SourceGraph,
	[]productconfig.SourceRoleProfile,
	capability.Inventory,
	*capabilityAllocator,
) (*graphService, error)

// StartOptions contains trusted runtime providers, never persisted user claims.
type StartOptions struct {
	FrameCapture videoproviders.FrameCaptureFactory
}

func sourceStarter(options []StartOptions) (sourceGraphStarter, error) {
	if len(options) > 1 {
		return nil, errors.New("source runtime options must be unique")
	}
	if len(options) == 0 || options[0].FrameCapture == nil {
		return startGraph, nil
	}
	factory := options[0].FrameCapture
	return func(ctx context.Context, graph productconfig.SourceGraph, admission []productconfig.SourceRoleProfile, inventory capability.Inventory, allocator *capabilityAllocator) (*graphService, error) {
		return startGraphWithFrameCapture(ctx, graph, admission, inventory, allocator, factory)
	}, nil
}

func Start(ctx context.Context, cfg productconfig.Config, options ...StartOptions) (*Service, error) {
	starter, err := sourceStarter(options)
	if err != nil {
		return nil, err
	}
	normalized, err := productconfig.Normalize(cfg)
	if err != nil {
		return nil, err
	}
	graphConfigs := normalized.SourceGraphs()
	probeTimeout := 250 * time.Millisecond
	for _, graphConfig := range graphConfigs {
		candidate := time.Duration(graphConfig.Source.Acceleration.Discovery.ProbeTimeoutMS) * time.Millisecond
		if candidate > probeTimeout {
			probeTimeout = candidate
		}
	}
	inventory := capability.Discover(ctx, capability.DiscoverOptions{
		EngineDirectory: normalized.Runtime.EngineDirectory,
		ProbeTimeout:    probeTimeout,
	})
	log.Printf("[video-broker] component discovery schema=%s candidates=%d", inventory.Schema, availableComponentCount(inventory))
	return startService(ctx, normalized, inventory, starter)
}

// StartRouted starts the selected graph while retaining the complete,
// catalog-authorized role/profile universe for every source. Inactive sources
// are included so a later transaction can start one with the same immutable
// hardware admission boundary.
func StartRouted(
	ctx context.Context,
	cfg productconfig.Config,
	admission map[string][]productconfig.SourceRoleProfile,
	options ...StartOptions,
) (*Service, error) {
	starter, err := sourceStarter(options)
	if err != nil {
		return nil, err
	}
	normalized, err := productconfig.Normalize(cfg)
	if err != nil {
		return nil, err
	}
	probeTimeout := 250 * time.Millisecond
	for _, source := range normalized.Sources {
		candidate := time.Duration(source.Acceleration.Discovery.ProbeTimeoutMS) * time.Millisecond
		if candidate > probeTimeout {
			probeTimeout = candidate
		}
	}
	inventory := capability.Discover(ctx, capability.DiscoverOptions{
		EngineDirectory: normalized.Runtime.EngineDirectory,
		ProbeTimeout:    probeTimeout,
	})
	log.Printf("[video-broker] component discovery schema=%s candidates=%d", inventory.Schema, availableComponentCount(inventory))
	return startServiceWithAdmission(
		ctx, normalized, inventory, starter, admission,
	)
}

func startService(
	ctx context.Context,
	normalized productconfig.Config,
	inventory capability.Inventory,
	starter sourceGraphStarter,
) (*Service, error) {
	return startServiceWithAdmission(
		ctx, normalized, inventory, starter, currentAdmissionProfiles(normalized),
	)
}

func startServiceWithAdmission(
	ctx context.Context,
	normalized productconfig.Config,
	inventory capability.Inventory,
	starter sourceGraphStarter,
	admission map[string][]productconfig.SourceRoleProfile,
) (*Service, error) {
	if starter == nil {
		return nil, errors.New("source graph starter is required")
	}
	return startServiceWithRestartPolicy(
		ctx, normalized, inventory, starter, defaultSourceRestartPolicy(), admission,
	)
}

func startGraph(
	ctx context.Context,
	graphConfig productconfig.SourceGraph,
	admission []productconfig.SourceRoleProfile,
	inventory capability.Inventory,
	allocator *capabilityAllocator,
) (*graphService, error) {
	return startGraphWithFrameCapture(ctx, graphConfig, admission, inventory, allocator, nil)
}

func startGraphWithFrameCapture(ctx context.Context, graphConfig productconfig.SourceGraph, admission []productconfig.SourceRoleProfile, inventory capability.Inventory, allocator *capabilityAllocator, frames videoproviders.FrameCaptureFactory) (*graphService, error) {
	engine := normalizeConfig(graphConfig)
	if graphConfig.Source.Processing.Quality.Enabled && graphConfig.Analysis == nil {
		log.Printf("[video-broker] event=quality_monitor_state source=%s configured=true state=inactive reason=analysis_unbound action=preserve_policy", graphConfig.SourceID)
	}
	engine.CapabilityInventory = inventory
	engine = resolveVideoCaptureControlProfile(ctx, engine)
	engine = resolveCameraControlOptimizerRuntime(engine)
	plan := resolveVideoBrokerPlan(engine)
	plan.SourceID = graphConfig.SourceID
	plan.FrameCapture = frames
	plan.AdmissionProfiles = cloneSourceRoleProfiles(admission)
	plan.CapabilityAllocator = allocator
	if !plan.Enabled {
		return nil, errors.New("normalized source graph is incomplete or unsupported")
	}
	if err := startVideoCaptureControlApplier(ctx, engine); err != nil {
		return nil, fmt.Errorf("pre-stream camera controls: %w", err)
	}
	resetVideoQualityStateFile(engine)
	runtime, done := startVideoBroker(ctx, plan)
	if runtime == nil || done == nil {
		return nil, errors.New("source graph did not start")
	}
	if err := runtime.waitStartup(ctx); err != nil {
		return nil, fmt.Errorf("runtime startup: %w", err)
	}
	startCameraControlOptimizer(ctx, engine, runtime)
	return &graphService{
		sourceID: graphConfig.SourceID, liveStreamID: engine.LiveStreamID,
		analysisStreamID: engine.AnalysisStreamID, archiveStreamID: engine.ArchiveStreamID,
		runtime: runtime, done: done, plan: plan,
	}, nil
}

func currentAdmissionProfiles(cfg productconfig.Config) map[string][]productconfig.SourceRoleProfile {
	profiles := make(map[string][]productconfig.SourceRoleProfile, len(cfg.Sources))
	for _, graph := range cfg.SourceGraphs() {
		for _, named := range []*productconfig.NamedStream{graph.Analysis, graph.Archive, graph.Live} {
			if named == nil {
				continue
			}
			stream := named.Config
			profiles[graph.SourceID] = append(profiles[graph.SourceID], productconfig.SourceRoleProfile{
				Role: stream.Type, Codec: stream.Codec, PixelFormat: stream.PixelFormat,
				Width: stream.Width, Height: stream.Height, FPS: stream.FPS,
				BitrateKbps: stream.BitrateKbps, Content: stream.Content,
				RateControl: stream.RateControl,
			})
		}
	}
	return profiles
}

func cloneSourceRoleProfiles(input []productconfig.SourceRoleProfile) []productconfig.SourceRoleProfile {
	return append([]productconfig.SourceRoleProfile(nil), input...)
}

func cloneAdmissionProfiles(
	input map[string][]productconfig.SourceRoleProfile,
) map[string][]productconfig.SourceRoleProfile {
	output := make(map[string][]productconfig.SourceRoleProfile, len(input))
	for sourceID, profiles := range input {
		output[sourceID] = cloneSourceRoleProfiles(profiles)
	}
	return output
}

func (service *Service) Done() <-chan error {
	if service == nil || service.done == nil {
		closed := make(chan error)
		close(closed)
		return closed
	}
	return service.done
}

func (service *Service) Wait(ctx context.Context) error {
	if service == nil {
		return errors.New("nil video-broker service")
	}
	select {
	case <-ctx.Done():
		if service.cancel != nil {
			service.cancel()
		}
		if service.done != nil {
			if err, ok := <-service.done; ok && err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
		}
		return nil
	case err, ok := <-service.done:
		if !ok || err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("video source graph stopped: %w", err)
	}
}

func (service *Service) Snapshot(now time.Time) Status {
	if now.IsZero() {
		now = time.Now()
	}
	status := Status{
		Schema: "video-broker.status.v5", ObservedAt: now.UTC(), State: "stopped",
		Sources: map[string]SourceStatus{}, Streams: map[string]StreamStatus{},
	}
	if service == nil {
		return status
	}
	service.mu.RLock()
	sources := make(map[string]*sourceSupervisor, len(service.sources))
	for sourceID, supervisor := range service.sources {
		sources[sourceID] = supervisor
	}
	service.mu.RUnlock()
	if len(sources) == 0 {
		return status
	}
	status.State = "running"
	sourceIDs := make([]string, 0, len(sources))
	for sourceID := range sources {
		sourceIDs = append(sourceIDs, sourceID)
	}
	sort.Strings(sourceIDs)
	selectedSeverity := sourceStateSeverity(status.State)
	for _, sourceID := range sourceIDs {
		supervisor := sources[sourceID]
		source, streams := supervisor.snapshot(now)
		status.Sources[sourceID] = source
		for streamID, stream := range streams {
			status.Streams[streamID] = stream
		}
		if severity := sourceStateSeverity(source.State); severity > selectedSeverity {
			selectedSeverity = severity
			status.State = source.State
			status.LastError = source.LastError
		}
		if source.LastChangeAt.After(status.LastChangeAt) {
			status.LastChangeAt = source.LastChangeAt
		}
	}
	return status
}

func sourceStateSeverity(state string) int {
	switch state {
	case "running":
		return 1
	case "initializing":
		return 2
	case "degraded":
		return 3
	case "stopped":
		return 4
	default:
		return 5
	}
}

func (service *graphService) snapshot(now time.Time) (SourceStatus, map[string]StreamStatus) {
	streams := make(map[string]StreamStatus, 3)
	if service == nil || service.runtime == nil {
		return SourceStatus{State: "stopped", HardwareRecovery: []HardwareRecoveryStatus{}}, streams
	}
	health := service.runtime.healthSnapshot(now)
	runtime := service.runtime
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	source := SourceStatus{
		State: health.State, LastError: health.LastError, LastChangeAt: health.LastChangeAt.UTC(),
		Identity:  runtime.plan.SourceIdentity,
		Transport: runtime.plan.Transport, Device: runtime.plan.CaptureDevice,
		ConfiguredFormat: runtime.plan.CaptureFormat, CaptureCodec: runtime.observedCaptureCodec,
		CaptureWidth: runtime.observedCaptureWidth, CaptureHeight: runtime.observedCaptureHeight,
		CaptureFPS: runtime.observedCaptureFPS, CaptureDecodeCodec: runtime.observedDecodeCodec,
		CaptureDecodeRequested: runtime.observedDecodeRequested, CaptureDecodeBackend: runtime.observedDecodeBackend,
		CaptureDecodeFallback: runtime.observedDecodeFallback, CaptureChildPID: brokerChildPID(runtime.childCmd),
		CaptureChildRunning: runtime.childRunning, FanoutControlMode: runtime.childFanoutControlMode,
		FanoutControlSupported: runtime.childFanoutSupported,
		HardwareRecovery:       runtime.hardwareRecoveryStatusesLocked(now),
		MediaEpoch:             runtime.mediaEpoch, LastMediaAt: runtime.lastFrameAt.UTC(), CameraAdmitted: runtime.cameraAdmitted,
		ToneMappingMode: runtime.plan.ToneMappingMode, ToneMappingSupported: runtime.toneMappingSupported,
		ToneMappingBackend: runtime.toneMappingBackend, ToneMappingActiveMode: runtime.observedToneMode,
		ToneMappingAccelerator: runtime.observedToneAccelerator, ToneMappingActive: runtime.observedToneActive,
		ToneMappingGainMin: runtime.observedToneGainMin, ToneMappingGainMax: runtime.observedToneGainMax,
		ToneMappingUpdates: runtime.observedToneUpdates, ToneMappingGridColumns: runtime.plan.ToneMappingGridColumns,
		ToneMappingGridRows: runtime.plan.ToneMappingGridRows, OpticalSignalState: health.OpticalSignalState,
		OpticalSignalReason: health.OpticalSignalReason, CapabilityInventory: runtime.effectiveCapabilityInventoryLocked(now),
		CapabilityPlan: runtime.capabilityPlan,
	}
	if source.CapabilityInventory.Schema == "" {
		source.CapabilityInventory = runtime.plan.CapabilityInventory
	}
	if service.liveStreamID != "" {
		streams[service.liveStreamID] = StreamStatus{
			Type: "live", SourceRef: service.sourceID, Codec: runtime.plan.MainFormat, FPS: runtime.plan.MainFPS,
			Clients: len(runtime.mainClients), Frames: runtime.totalMainFrames, Epoch: runtime.mediaEpoch,
			LastFrameAt: runtime.lastMainFrameAt.UTC(), Drops: runtime.totalMainDrops,
		}
	}
	if service.archiveStreamID != "" {
		streams[service.archiveStreamID] = StreamStatus{
			Type: "archive", SourceRef: service.sourceID, Codec: runtime.observedArchiveCodec,
			Delivery: runtime.observedArchiveDelivery, FPS: runtime.observedArchiveFPS,
			Clients: len(runtime.archiveClients), Frames: runtime.totalArchiveFrames, Epoch: runtime.mediaEpoch,
			LastFrameAt: runtime.lastArchiveFrameAt.UTC(), Disconnects: runtime.totalArchiveDisconnects,
		}
	}
	if service.analysisStreamID != "" {
		epoch := runtime.mediaEpoch
		if runtime.subShared != nil {
			epoch = runtime.subShared.Epoch()
		}
		streams[service.analysisStreamID] = StreamStatus{
			Type: "analysis", SourceRef: service.sourceID, FPS: runtime.plan.SubFPS,
			Clients: len(runtime.subClients), Frames: runtime.totalSubFrames, Epoch: epoch,
			LastFrameAt: runtime.lastSubFrameAt.UTC(), Drops: runtime.totalSubDrops,
		}
	}
	for streamID, stream := range streams {
		stream.Performance = runtime.streamPerformance[stream.Type]
		if stream.Performance.State == "" {
			stream.Performance = StreamPerformance{State: "measuring", RequestedFPS: stream.FPS, TargetFPS: stream.FPS}
		}
		streams[streamID] = stream
	}
	return source, streams
}

func availableComponentCount(inventory capability.Inventory) int {
	count := 0
	for _, component := range inventory.Components {
		if component.Available {
			count++
		}
	}
	return count
}
