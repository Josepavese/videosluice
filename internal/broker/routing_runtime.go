package broker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/routing"
)

const routingFramePollInterval = 10 * time.Millisecond

// NewRoutingRuntime binds the generic routing transaction contract to a
// running standalone broker Service. It deliberately has no application-level
// knowledge: stream roles, source definitions and safe media bounds come only
// from the validated v3 graph and its central catalog.
func NewRoutingRuntime(service *Service) routing.GraphRuntime {
	return &serviceRoutingRuntime{service: service, fanoutBackend: serviceFanoutBackend(service)}
}

type serviceRoutingRuntime struct {
	service       *Service
	fanoutBackend sourceFanoutBackend
}

func (runtime *serviceRoutingRuntime) ValidateGraph(
	ctx context.Context,
	current productconfig.Config,
	candidate productconfig.Config,
	_ routing.Plan,
	_ routing.Plan,
	delta routing.GraphDelta,
) error {
	if ctx == nil {
		return errors.New("routing preview context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if runtime == nil || runtime.service == nil {
		return errors.New("routing data plane service is unavailable")
	}
	if requiresSourceRestart(delta) {
		_, err := runtime.prepareSourceRestart(ctx, current, candidate, delta)
		return err
	}
	streamID, sourceID, currentGraph, _, err :=
		validateInPlaceLiveProfileChange(current, candidate, delta)
	if err != nil {
		if _, moveErr := validateWholeSourceMove(current, candidate, delta); moveErr == nil {
			return runtime.validateWholeSourceMove(ctx, current, candidate, delta)
		}
		if _, moveErr := validateMixedSourceActivation(current, candidate, delta); moveErr == nil {
			return runtime.validateMixedSourceActivation(ctx, current, candidate, delta)
		}
		return runtime.validatePreservedOwnerFanout(ctx, current, candidate, delta)
	}
	runtime.service.mu.RLock()
	supervisor := runtime.service.sources[sourceID]
	runtime.service.mu.RUnlock()
	if supervisor == nil {
		return fmt.Errorf("routing source %q is not managed", sourceID)
	}
	supervisor.mu.RLock()
	defer supervisor.mu.RUnlock()
	if supervisor.state != "running" || supervisor.graph == nil || supervisor.graph.runtime == nil {
		return fmt.Errorf("routing source %q has no running graph", sourceID)
	}
	if !reflect.DeepEqual(supervisor.config, currentGraph) {
		return fmt.Errorf("routing source %q runtime graph differs from current graph", sourceID)
	}
	graph := supervisor.graph
	if graph.sourceID != sourceID || graph.liveStreamID != streamID {
		return errors.New("running live endpoint identity differs from routing graph")
	}
	mediaRuntime := graph.runtime
	mediaRuntime.mu.Lock()
	defer mediaRuntime.mu.Unlock()
	if err := validateInPlaceLiveRuntimeLocked(mediaRuntime, currentGraph.Live.Config, 0, 0); err != nil {
		return err
	}
	return validateLiveRuntimeAdmissionLocked(mediaRuntime)
}

type liveProfileClientState struct {
	control videoMainClientControl
	user    bool
}

type liveProfilePreparedGraph struct {
	mu sync.Mutex

	service          *Service
	supervisor       *sourceSupervisor
	graph            *graphService
	runtime          *videoBrokerRuntime
	currentGraph     productconfig.SourceGraph
	candidateGraph   productconfig.SourceGraph
	currentConfig    productconfig.Config
	candidateConfig  productconfig.Config
	streamID         string
	mediaEpoch       uint64
	childPID         int
	encoderEpoch     uint64
	prepareMainFrame uint64
	cutoverMainFrame uint64
	warmupControl    bridgeRuntimeControl
	cutoverControl   bridgeRuntimeControl
	oldGovernor      resourceGovernorDecision
	oldClients       map[int]liveProfileClientState
	introducedDemand bool
	cutover          bool
	phase            int
	terminal         bool
	lockHeld         bool
}

func (runtime *serviceRoutingRuntime) PrepareGraph(
	ctx context.Context,
	current productconfig.Config,
	candidate productconfig.Config,
	_ routing.Plan,
	_ routing.Plan,
	delta routing.GraphDelta,
) (routing.PreparedGraph, error) {
	if ctx == nil {
		return nil, errors.New("routing prepare context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime == nil || runtime.service == nil {
		return nil, errors.New("routing data plane service is unavailable")
	}
	if requiresSourceRestart(delta) {
		return runtime.prepareSourceRestart(ctx, current, candidate, delta)
	}
	streamID, sourceID, currentGraph, candidateGraph, err := validateInPlaceLiveProfileChange(current, candidate, delta)
	if err != nil {
		if _, moveErr := validateWholeSourceMove(current, candidate, delta); moveErr == nil {
			return runtime.prepareWholeSourceMove(ctx, current, candidate, delta)
		}
		if _, moveErr := validateMixedSourceActivation(current, candidate, delta); moveErr == nil {
			return runtime.prepareMixedSourceActivation(ctx, current, candidate, delta)
		}
		return runtime.preparePreservedOwnerFanout(ctx, current, candidate, delta)
	}

	runtime.service.mu.RLock()
	supervisor := runtime.service.sources[sourceID]
	runtime.service.mu.RUnlock()
	if supervisor == nil {
		return nil, fmt.Errorf("routing source %q is not managed", sourceID)
	}

	// Keep the supervisor's graph/config identity stable through the complete
	// transaction. Its worker may finish concurrently, but cannot publish a
	// replacement graph from an obsolete config before Abort/Retire decides the
	// persistent outcome.
	supervisor.mu.Lock()
	fail := func(cause error) (routing.PreparedGraph, error) {
		supervisor.mu.Unlock()
		return nil, cause
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if supervisor.state != "running" || supervisor.graph == nil || supervisor.graph.runtime == nil {
		return fail(fmt.Errorf("routing source %q has no running graph", sourceID))
	}
	if !reflect.DeepEqual(supervisor.config, currentGraph) {
		return fail(fmt.Errorf("routing source %q runtime graph differs from current graph", sourceID))
	}
	graph := supervisor.graph
	if graph.sourceID != sourceID || graph.liveStreamID != streamID {
		return fail(errors.New("running live endpoint identity differs from routing graph"))
	}
	mediaRuntime := graph.runtime
	mediaRuntime.mu.Lock()
	defer mediaRuntime.mu.Unlock()
	if err := validateInPlaceLiveRuntimeLocked(mediaRuntime, currentGraph.Live.Config, 0, 0); err != nil {
		return fail(err)
	}

	return &liveProfilePreparedGraph{
		service: runtime.service, supervisor: supervisor, graph: graph, runtime: mediaRuntime,
		currentGraph: currentGraph, candidateGraph: candidateGraph,
		currentConfig: productconfig.Clone(current), candidateConfig: productconfig.Clone(candidate),
		streamID:   streamID,
		mediaEpoch: mediaRuntime.mediaEpoch, childPID: brokerChildPID(mediaRuntime.childCmd),
		encoderEpoch: mediaRuntime.observedLiveEpoch, prepareMainFrame: mediaRuntime.totalMainFrames,
		oldGovernor: cloneResourceGovernorDecision(mediaRuntime.governor),
		oldClients:  snapshotLiveProfileClientsLocked(mediaRuntime), lockHeld: true,
	}, nil
}

func validateInPlaceLiveProfileChange(
	current productconfig.Config,
	candidate productconfig.Config,
	delta routing.GraphDelta,
) (string, string, productconfig.SourceGraph, productconfig.SourceGraph, error) {
	if len(delta.ChangedStreamIDs) != 1 {
		return "", "", productconfig.SourceGraph{}, productconfig.SourceGraph{},
			errors.New("runtime currently requires exactly one changed stream per transaction")
	}
	streamID := delta.ChangedStreamIDs[0]
	change, exists := delta.Streams[streamID]
	if !exists || change.State != routing.StreamChangeReconfigure || change.CurrentSource == "" ||
		change.CurrentSource != change.CandidateSource || !change.ProfileChanged {
		return "", "", productconfig.SourceGraph{}, productconfig.SourceGraph{},
			errors.New("runtime currently supports only an in-place stream profile change")
	}
	if len(delta.ChangedSourceIDs) != 1 || delta.ChangedSourceIDs[0] != change.CurrentSource {
		return "", "", productconfig.SourceGraph{}, productconfig.SourceGraph{},
			errors.New("in-place profile change has an invalid source work set")
	}
	sourceChange, exists := delta.Sources[change.CurrentSource]
	if !exists || sourceChange.State != routing.SourceChangeReconfigure || !sourceChange.PreserveCaptureOwner {
		return "", "", productconfig.SourceGraph{}, productconfig.SourceGraph{},
			errors.New("in-place profile change must preserve its capture owner")
	}
	before, beforeExists := current.Streams[streamID]
	after, afterExists := candidate.Streams[streamID]
	if !beforeExists || !afterExists || before.Type != "live" || after.Type != "live" {
		return "", "", productconfig.SourceGraph{}, productconfig.SourceGraph{},
			errors.New("runtime profile change currently requires one live stream")
	}
	stableBefore := before
	stableAfter := after
	stableBefore.FPS, stableAfter.FPS = 0, 0
	stableBefore.BitrateKbps, stableAfter.BitrateKbps = 0, 0
	if !reflect.DeepEqual(stableBefore, stableAfter) ||
		(before.FPS == after.FPS && before.BitrateKbps == after.BitrateKbps) {
		return "", "", productconfig.SourceGraph{}, productconfig.SourceGraph{},
			errors.New("runtime live profile change may alter only fps and bitrate")
	}
	currentGraph, ok := sourceGraph(current, change.CurrentSource)
	if !ok || currentGraph.Live == nil || currentGraph.Live.ID != streamID {
		return "", "", productconfig.SourceGraph{}, productconfig.SourceGraph{},
			errors.New("current live source graph is incomplete")
	}
	candidateGraph, ok := sourceGraph(candidate, change.CurrentSource)
	if !ok || candidateGraph.Live == nil || candidateGraph.Live.ID != streamID {
		return "", "", productconfig.SourceGraph{}, productconfig.SourceGraph{},
			errors.New("candidate live source graph is incomplete")
	}
	return streamID, change.CurrentSource, currentGraph, candidateGraph, nil
}

func sourceGraph(cfg productconfig.Config, sourceID string) (productconfig.SourceGraph, bool) {
	for _, graph := range cfg.SourceGraphs() {
		if graph.SourceID == sourceID {
			return graph, true
		}
	}
	return productconfig.SourceGraph{}, false
}

func validateInPlaceLiveRuntimeLocked(
	runtime *videoBrokerRuntime,
	stream productconfig.StreamConfig,
	mediaEpoch uint64,
	childPID int,
) error {
	if runtime == nil || !runtime.childRunning || runtime.childFinishing || brokerChildPID(runtime.childCmd) <= 0 {
		return errors.New("live capture child is not running")
	}
	if mediaEpoch != 0 && (runtime.mediaEpoch != mediaEpoch || brokerChildPID(runtime.childCmd) != childPID) {
		return errors.New("live capture identity changed during routing transaction")
	}
	if !runtime.childControlSupported || runtime.childLiveControlMode != "persistent_encoder_epoch" {
		return errors.New("live backend does not attest in-place profile control")
	}
	if !runtime.plan.LiveEnabled || runtime.plan.MainSocketPath != stream.Socket ||
		runtime.plan.MainFormat != stream.Codec || runtime.plan.MainWidth != stream.Width ||
		runtime.plan.MainHeight != stream.Height || runtime.plan.MainFPS != stream.FPS ||
		runtime.plan.LiveBitrateKbps != stream.BitrateKbps {
		return errors.New("live runtime profile differs from current routing graph")
	}
	return nil
}

func (prepared *liveProfilePreparedGraph) Warmup(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(0); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	runtime := prepared.runtime
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if err := validateInPlaceLiveRuntimeLocked(runtime, prepared.currentGraph.Live.Config, prepared.mediaEpoch, prepared.childPID); err != nil {
		return err
	}
	if len(runtime.mainClients) == 0 {
		previous := runtime.bridgeControlTargetsLocked()
		runtime.routingLiveDemand = true
		prepared.introducedDemand = true
		current := runtime.bridgeControlTargetsLocked()
		if liveEncoderActivationChanged(previous, current) {
			runtime.mainAwaitingEncoderEpoch = true
			runtime.armMainKeyframeGateLocked()
		}
		control, queued := runtime.queueBridgeControlLocked("routing_live_profile_warmup")
		if !queued {
			runtime.routingLiveDemand = false
			prepared.introducedDemand = false
			return errors.New("live warmup control could not be queued")
		}
		prepared.warmupControl = control
	}
	prepared.phase = 1
	return nil
}

func (prepared *liveProfilePreparedGraph) Admit(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(1); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	runtime := prepared.runtime
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if err := validateInPlaceLiveRuntimeLocked(runtime, prepared.currentGraph.Live.Config, prepared.mediaEpoch, prepared.childPID); err != nil {
		return err
	}
	if err := validateLiveRuntimeAdmissionLocked(runtime); err != nil {
		return err
	}
	prepared.phase = 2
	return nil
}

func validateLiveRuntimeAdmissionLocked(runtime *videoBrokerRuntime) error {
	if runtime.cameraAdmission.DevicePath != "" && !runtime.cameraAdmitted {
		return errors.New("live source camera admission is incomplete")
	}
	if runtime.plan.RequireHWCodec && !liveHardwareEncoderAttestedLocked(runtime) {
		return errors.New("required live hardware encoder is not attested")
	}
	// Catalog/capability validation already proves that this profile is
	// supported. Measured delivery cadence does not authorize rejecting the
	// operator's source or rewriting their requested FPS.
	return nil
}

func liveHardwareEncoderAttestedLocked(runtime *videoBrokerRuntime) bool {
	for _, selection := range runtime.capabilityPlan.Stages {
		if selection.Stage == capability.StageEncode && selection.Role == "live" &&
			selection.Format == runtime.plan.MainFormat {
			return selection.Hardware && selection.State == capability.StateAttested
		}
	}
	return false
}

func (prepared *liveProfilePreparedGraph) AwaitFirstFrame(ctx context.Context) error {
	prepared.mu.Lock()
	if err := prepared.requirePhaseLocked(2); err != nil {
		prepared.mu.Unlock()
		return err
	}
	warmupControl := prepared.warmupControl
	prepared.mu.Unlock()
	if warmupControl.Sequence != 0 {
		observation, err := prepared.runtime.waitBridgeControlReady(
			ctx, prepared.mediaEpoch, prepared.childPID, warmupControl)
		if err != nil {
			return fmt.Errorf("live warmup readiness: %w", err)
		}
		if prepared.encoderEpoch != 0 && observation.EncoderEpoch < prepared.encoderEpoch {
			return errors.New("live warmup reported a stale encoder epoch")
		}
		prepared.encoderEpoch = observation.EncoderEpoch
	}
	if err := prepared.waitForMainFrame(ctx, prepared.prepareMainFrame, prepared.currentGraph.Live.Config); err != nil {
		return fmt.Errorf("live warmup first frame: %w", err)
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(2); err != nil {
		return err
	}
	prepared.phase = 3
	return nil
}

func (prepared *liveProfilePreparedGraph) Cutover(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(3); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	runtime := prepared.runtime
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if err := validateInPlaceLiveRuntimeLocked(runtime, prepared.currentGraph.Live.Config, prepared.mediaEpoch, prepared.childPID); err != nil {
		return err
	}
	prepared.oldGovernor = cloneResourceGovernorDecision(runtime.governor)
	prepared.oldClients = snapshotLiveProfileClientsLocked(runtime)
	prepared.cutoverMainFrame = runtime.totalMainFrames
	applyLiveProfileLocked(runtime, prepared.candidateGraph.Live.Config)
	control, queued := runtime.queueBridgeControlLocked("routing_live_profile_cutover")
	if !queued {
		restoreLiveProfileLocked(runtime, prepared.currentGraph.Live.Config, prepared.oldGovernor, prepared.oldClients)
		return errors.New("live profile cutover control could not be queued")
	}
	prepared.cutoverControl = control
	prepared.cutover = true
	prepared.phase = 4
	if prepared.service != nil {
		prepared.service.mu.Lock()
		prepared.service.setActiveRoutingConfigLocked(prepared.candidateConfig)
		prepared.service.mu.Unlock()
		prepared.service.signalStreamGovernorPublish()
	}
	return nil
}

func applyLiveProfileLocked(runtime *videoBrokerRuntime, stream productconfig.StreamConfig) {
	runtime.plan.MainFPS = stream.FPS
	runtime.plan.LiveBitrateKbps = stream.BitrateKbps
	for _, client := range runtime.mainClients {
		if !client.mainControlUser {
			client.mainControl.TargetFPS = stream.FPS
			client.mainControl.TargetBitrateKbps = stream.BitrateKbps
			continue
		}
		if client.mainControl.TargetFPS > stream.FPS {
			client.mainControl.TargetFPS = stream.FPS
		}
		if client.mainControl.TargetBitrateKbps > stream.BitrateKbps {
			client.mainControl.TargetBitrateKbps = stream.BitrateKbps
		}
	}
	previous := cloneResourceGovernorDecision(runtime.governor)
	runtime.governor = decideResourceGovernor(
		runtime.plan, previous, cloneThermalZones(previous.ThermalZonesCelsius),
		cloneOptionalInt(previous.BPUUtilizationPercent), time.Now().UTC())
}

func restoreLiveProfileLocked(
	runtime *videoBrokerRuntime,
	stream productconfig.StreamConfig,
	governor resourceGovernorDecision,
	clients map[int]liveProfileClientState,
) {
	runtime.plan.MainFPS = stream.FPS
	runtime.plan.LiveBitrateKbps = stream.BitrateKbps
	runtime.governor = cloneResourceGovernorDecision(governor)
	for clientID, state := range clients {
		if client := runtime.mainClients[clientID]; client != nil {
			client.mainControl = state.control
			client.mainControlUser = state.user
		}
	}
}

func snapshotLiveProfileClientsLocked(runtime *videoBrokerRuntime) map[int]liveProfileClientState {
	clients := make(map[int]liveProfileClientState, len(runtime.mainClients))
	for clientID, client := range runtime.mainClients {
		clients[clientID] = liveProfileClientState{control: client.mainControl, user: client.mainControlUser}
	}
	return clients
}

func cloneResourceGovernorDecision(input resourceGovernorDecision) resourceGovernorDecision {
	cloned := input
	cloned.ThermalZonesCelsius = cloneThermalZones(input.ThermalZonesCelsius)
	cloned.BPUUtilizationPercent = cloneOptionalInt(input.BPUUtilizationPercent)
	return cloned
}

func cloneThermalZones(input map[string]float64) map[string]float64 {
	if input == nil {
		return nil
	}
	cloned := make(map[string]float64, len(input))
	for zone, value := range input {
		cloned[zone] = value
	}
	return cloned
}

func (prepared *liveProfilePreparedGraph) Verify(ctx context.Context) error {
	prepared.mu.Lock()
	if err := prepared.requirePhaseLocked(4); err != nil {
		prepared.mu.Unlock()
		return err
	}
	control := prepared.cutoverControl
	prepared.mu.Unlock()

	observation, err := prepared.runtime.waitBridgeControlReady(
		ctx, prepared.mediaEpoch, prepared.childPID, control)
	if err != nil {
		return fmt.Errorf("live cutover readiness: %w", err)
	}
	if observation.EncoderEpoch != prepared.encoderEpoch {
		return fmt.Errorf(
			"in-place live profile change altered encoder epoch before=%d after=%d",
			prepared.encoderEpoch, observation.EncoderEpoch)
	}
	if err := prepared.waitForMainFrame(ctx, prepared.cutoverMainFrame, prepared.candidateGraph.Live.Config); err != nil {
		return fmt.Errorf("live cutover first frame: %w", err)
	}

	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(4); err != nil {
		return err
	}
	prepared.phase = 5
	return nil
}

func (prepared *liveProfilePreparedGraph) waitForMainFrame(
	ctx context.Context,
	after uint64,
	stream productconfig.StreamConfig,
) error {
	if ctx == nil {
		return errors.New("routing phase context is required")
	}
	ticker := time.NewTicker(routingFramePollInterval)
	defer ticker.Stop()
	for {
		prepared.runtime.mu.Lock()
		err := validateInPlaceLiveRuntimeLocked(
			prepared.runtime, stream, prepared.mediaEpoch, prepared.childPID)
		ready := err == nil && prepared.runtime.totalMainFrames > after &&
			brokerMainBootstrapReady(prepared.runtime.plan.MainFormat, prepared.runtime.mainBootstrap)
		prepared.runtime.mu.Unlock()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (prepared *liveProfilePreparedGraph) Rollback(ctx context.Context) error {
	prepared.mu.Lock()
	if prepared.terminal {
		prepared.mu.Unlock()
		return nil
	}
	if !prepared.cutover {
		prepared.releaseLocked()
		prepared.mu.Unlock()
		return nil
	}
	prepared.mu.Unlock()

	var rollbackControl bridgeRuntimeControl
	var rollbackFrame uint64
	runtime := prepared.runtime
	runtime.mu.Lock()
	identityErr := validateInPlaceLiveRuntimeLocked(
		runtime, prepared.candidateGraph.Live.Config, prepared.mediaEpoch, prepared.childPID)
	rollbackFrame = runtime.totalMainFrames
	restoreLiveProfileLocked(runtime, prepared.currentGraph.Live.Config, prepared.oldGovernor, prepared.oldClients)
	if runtime.childRunning && runtime.childControlSupported {
		var queued bool
		rollbackControl, queued = runtime.queueBridgeControlLocked("routing_live_profile_rollback")
		if !queued && identityErr == nil {
			identityErr = errors.New("live profile rollback control could not be queued")
		}
	}
	runtime.mu.Unlock()

	rollbackErr := identityErr
	if rollbackErr == nil {
		observation, err := runtime.waitBridgeControlReady(
			ctx, prepared.mediaEpoch, prepared.childPID, rollbackControl)
		if err != nil {
			rollbackErr = fmt.Errorf("live rollback readiness: %w", err)
		} else if observation.EncoderEpoch != prepared.encoderEpoch {
			rollbackErr = errors.New("live rollback changed encoder epoch")
		} else if err := prepared.waitForMainFrame(ctx, rollbackFrame, prepared.currentGraph.Live.Config); err != nil {
			rollbackErr = fmt.Errorf("live rollback first frame: %w", err)
		}
	}
	prepared.mu.Lock()
	if rollbackErr == nil && prepared.service != nil {
		prepared.service.mu.Lock()
		prepared.service.setActiveRoutingConfigLocked(prepared.currentConfig)
		prepared.service.mu.Unlock()
		prepared.service.signalStreamGovernorPublish()
	}
	prepared.clearRoutingDemandLocked()
	prepared.releaseLocked()
	prepared.mu.Unlock()
	return rollbackErr
}

func (prepared *liveProfilePreparedGraph) Abort(context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.terminal {
		return nil
	}
	if prepared.cutover {
		return errors.New("routing cutover requires rollback, not abort")
	}
	prepared.clearRoutingDemandLocked()
	prepared.releaseLocked()
	return nil
}

func (prepared *liveProfilePreparedGraph) Retire(context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.terminal {
		return nil
	}
	if err := prepared.requirePhaseLocked(5); err != nil {
		return err
	}
	prepared.supervisor.config = prepared.candidateGraph
	prepared.runtime.mu.Lock()
	prepared.graph.plan = prepared.runtime.plan
	prepared.clearRoutingDemandWithRuntimeLocked()
	prepared.runtime.mu.Unlock()
	prepared.releaseLocked()
	return nil
}

func (prepared *liveProfilePreparedGraph) clearRoutingDemandLocked() {
	prepared.runtime.mu.Lock()
	prepared.clearRoutingDemandWithRuntimeLocked()
	prepared.runtime.mu.Unlock()
}

func (prepared *liveProfilePreparedGraph) clearRoutingDemandWithRuntimeLocked() {
	if !prepared.introducedDemand {
		return
	}
	prepared.runtime.routingLiveDemand = false
	prepared.runtime.queueBridgeControlLocked("routing_live_profile_warmup_release")
	prepared.introducedDemand = false
}

func (prepared *liveProfilePreparedGraph) releaseLocked() {
	if prepared.lockHeld {
		prepared.lockHeld = false
		prepared.supervisor.mu.Unlock()
	}
	prepared.terminal = true
}

func (prepared *liveProfilePreparedGraph) requirePhaseLocked(expected int) error {
	if prepared == nil || prepared.supervisor == nil || prepared.runtime == nil || !prepared.lockHeld {
		return errors.New("routing prepared graph is invalid")
	}
	if prepared.terminal {
		return errors.New("routing prepared graph is closed")
	}
	if prepared.phase != expected {
		return fmt.Errorf("routing phase order invalid current=%d expected=%d", prepared.phase, expected)
	}
	return nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("routing phase context is required")
	}
	return ctx.Err()
}

var _ routing.GraphRuntime = (*serviceRoutingRuntime)(nil)
var _ routing.GraphValidator = (*serviceRoutingRuntime)(nil)
var _ routing.PreparedGraph = (*liveProfilePreparedGraph)(nil)
