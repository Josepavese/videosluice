package broker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"

	productconfig "github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/routing"
)

// sourceFanoutBackend is the narrow adapter boundary for changing output
// branches on capture owners that are already running. An implementation must
// attach and warm candidate branches without opening the capture device again,
// must reserve every incremental capability before publication, and must keep
// current branches alive until Cutover. Absence of an implementation is an
// explicit lack of runtime capability, never permission to restart a source or
// fall back to software.
type sourceFanoutBackend interface {
	ValidateFanout(context.Context, preservedOwnerFanout, map[string]sourceFanoutHandle) error
	PrepareFanout(context.Context, preservedOwnerFanout, map[string]sourceFanoutHandle) (routing.PreparedGraph, error)
}

type sourceFanoutHandle struct {
	SourceID       string
	Supervisor     *sourceSupervisor
	Graph          *graphService
	Runtime        *videoBrokerRuntime
	CurrentGraph   productconfig.SourceGraph
	CandidateGraph productconfig.SourceGraph
	ChildPID       int
	MediaEpoch     uint64
	// CaptureAvailable is false only for a fully retiring source whose graph
	// is still the exact current routing authority but whose capture worker is
	// temporarily down. Such a source has no candidate roles and therefore
	// needs no worker command; its incumbent publications remain the bounded
	// rollback boundary while a healthy destination is prepared.
	CaptureAvailable bool
}

type fanoutStreamMove struct {
	StreamID  string
	Type      string
	From      string
	To        string
	Current   productconfig.StreamConfig
	Candidate productconfig.StreamConfig
}

// preservedOwnerFanout is a graph transition driven through the already
// running capture owners. Reconfigured sources exist on both sides; a source
// that becomes empty stays alive only through verified cutover and is stopped
// after persistence. Starting a second graph for any of these sources would
// create two capture owners for the same physical camera.
type preservedOwnerFanout struct {
	SourceIDs         []string
	RetiringSourceIDs []string
	StreamIDs         []string
	Moves             []fanoutStreamMove
	CurrentGraphs     map[string]productconfig.SourceGraph
	CandidateGraphs   map[string]productconfig.SourceGraph
}

func serviceFanoutBackend(service *Service) sourceFanoutBackend {
	if service == nil {
		return nil
	}
	return service.fanout
}

func classifyPreservedOwnerFanout(
	current productconfig.Config,
	candidate productconfig.Config,
	delta routing.GraphDelta,
) (preservedOwnerFanout, error) {
	transition := preservedOwnerFanout{}
	derived, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		return transition, err
	}
	if !reflect.DeepEqual(derived, delta) {
		return transition, errors.New("fan-out delta differs from the validated graph transition")
	}
	if len(delta.ChangedStreamIDs) == 0 || len(delta.ChangedSourceIDs) < 2 {
		return transition, errors.New("fan-out transaction requires a stream move between running sources")
	}

	touchedSources := make(map[string]bool, len(delta.ChangedSourceIDs))
	transition.CurrentGraphs = make(map[string]productconfig.SourceGraph, len(delta.ChangedSourceIDs))
	transition.CandidateGraphs = make(map[string]productconfig.SourceGraph, len(delta.ChangedSourceIDs))
	reconfigured := 0
	for _, sourceID := range delta.ChangedSourceIDs {
		change, exists := delta.Sources[sourceID]
		currentGraph, currentExists := sourceGraph(current, sourceID)
		candidateGraph, candidateExists := sourceGraph(candidate, sourceID)
		if !exists || !currentExists || len(change.CurrentStreams) == 0 {
			return preservedOwnerFanout{}, fmt.Errorf(
				"fan-out source %q must have one running capture owner", sourceID)
		}
		switch change.State {
		case routing.SourceChangeReconfigure:
			if !change.PreserveCaptureOwner || !candidateExists || len(change.CandidateStreams) == 0 {
				return preservedOwnerFanout{}, fmt.Errorf(
					"fan-out source %q cannot preserve its capture owner", sourceID)
			}
			reconfigured++
		case routing.SourceChangeDeactivate:
			if change.PreserveCaptureOwner || candidateExists || len(change.CandidateStreams) != 0 {
				return preservedOwnerFanout{}, fmt.Errorf(
					"fan-out source %q is not a complete retirement", sourceID)
			}
			candidateGraph = productconfig.SourceGraph{SourceID: sourceID, Source: currentGraph.Source}
			transition.RetiringSourceIDs = append(transition.RetiringSourceIDs, sourceID)
		default:
			return preservedOwnerFanout{}, fmt.Errorf(
				"fan-out source %q is not already running", sourceID)
		}
		touchedSources[sourceID] = true
		transition.SourceIDs = append(transition.SourceIDs, sourceID)
		transition.CurrentGraphs[sourceID] = currentGraph
		transition.CandidateGraphs[sourceID] = candidateGraph
	}
	if reconfigured == 0 {
		return preservedOwnerFanout{}, errors.New("fan-out transaction has no running destination source")
	}
	sort.Strings(transition.SourceIDs)
	sort.Strings(transition.RetiringSourceIDs)

	for _, streamID := range delta.ChangedStreamIDs {
		change, exists := delta.Streams[streamID]
		before, beforeExists := current.Streams[streamID]
		after, afterExists := candidate.Streams[streamID]
		if !exists || !beforeExists || !afterExists || change.State != routing.StreamChangeMove ||
			change.CurrentSource == change.CandidateSource ||
			change.CurrentSource != before.SourceRef || change.CandidateSource != after.SourceRef ||
			!touchedSources[change.CurrentSource] || !touchedSources[change.CandidateSource] {
			return preservedOwnerFanout{}, fmt.Errorf("fan-out stream %q is not a valid source move", streamID)
		}
		if before.Type != after.Type || before.Socket != after.Socket ||
			before.SharedMemory != after.SharedMemory || before.NotifySocket != after.NotifySocket {
			return preservedOwnerFanout{}, fmt.Errorf(
				"fan-out stream %q cannot change its logical type or publication endpoints", streamID)
		}
		transition.StreamIDs = append(transition.StreamIDs, streamID)
		transition.Moves = append(transition.Moves, fanoutStreamMove{
			StreamID: streamID, Type: before.Type, From: before.SourceRef, To: after.SourceRef,
			Current: before, Candidate: after,
		})
	}
	sort.Strings(transition.StreamIDs)
	sort.Slice(transition.Moves, func(i, j int) bool {
		return transition.Moves[i].StreamID < transition.Moves[j].StreamID
	})
	return transition, nil
}

func (runtime *serviceRoutingRuntime) validatePreservedOwnerFanout(
	ctx context.Context,
	current productconfig.Config,
	candidate productconfig.Config,
	delta routing.GraphDelta,
) error {
	if ctx == nil {
		return errors.New("routing preview context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	transition, err := classifyPreservedOwnerFanout(current, candidate, delta)
	if err != nil {
		return err
	}
	if runtime.fanoutBackend == nil {
		return errors.New("routing backend does not attest dynamic fan-out branch transactions")
	}
	handles, err := runtime.snapshotFanoutHandles(transition)
	if err != nil {
		return err
	}
	return runtime.fanoutBackend.ValidateFanout(ctx, transition, handles)
}

func (runtime *serviceRoutingRuntime) preparePreservedOwnerFanout(
	ctx context.Context,
	current productconfig.Config,
	candidate productconfig.Config,
	delta routing.GraphDelta,
) (routing.PreparedGraph, error) {
	transition, err := classifyPreservedOwnerFanout(current, candidate, delta)
	if err != nil {
		return nil, err
	}
	if runtime.fanoutBackend == nil {
		return nil, errors.New("routing backend does not attest dynamic fan-out branch transactions")
	}
	if runtime == nil || runtime.service == nil {
		return nil, errors.New("routing data plane service is unavailable")
	}
	if ctx == nil {
		return nil, errors.New("routing prepare context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runtime.service.lifecycleMu.Lock()
	stopping := runtime.service.stopping || runtime.service.managerCtx == nil ||
		runtime.service.managerCtx.Err() != nil
	runtime.service.lifecycleMu.Unlock()
	if stopping {
		return nil, errors.New("routing source manager is stopping")
	}

	handles, peers, locked, err := runtime.lockFanoutHandles(transition)
	if err != nil {
		return nil, err
	}
	release := func() {
		for index := len(locked) - 1; index >= 0; index-- {
			locked[index].mu.Unlock()
		}
	}
	inner, err := runtime.fanoutBackend.PrepareFanout(ctx, transition, handles)
	if err != nil {
		release()
		return nil, err
	}
	if inner == nil {
		release()
		return nil, errors.New("fan-out backend returned no prepared transaction")
	}
	return &fanoutPreparedGraph{
		service: runtime.service, currentConfig: productconfig.Clone(current),
		candidateConfig: productconfig.Clone(candidate), transition: transition,
		handles: handles, peers: peers, inner: inner,
		locked: locked, locksHeld: true,
	}, nil
}

func (runtime *serviceRoutingRuntime) snapshotFanoutHandles(
	transition preservedOwnerFanout,
) (map[string]sourceFanoutHandle, error) {
	if runtime == nil || runtime.service == nil {
		return nil, errors.New("routing data plane service is unavailable")
	}
	runtime.service.mu.RLock()
	supervisors := make(map[string]*sourceSupervisor, len(transition.SourceIDs))
	for _, sourceID := range transition.SourceIDs {
		supervisors[sourceID] = runtime.service.sources[sourceID]
	}
	runtime.service.mu.RUnlock()
	handles := make(map[string]sourceFanoutHandle, len(transition.SourceIDs))
	for _, sourceID := range transition.SourceIDs {
		supervisor := supervisors[sourceID]
		if supervisor == nil {
			return nil, fmt.Errorf("fan-out source %q is not managed", sourceID)
		}
		supervisor.mu.RLock()
		handle, err := fanoutHandleLocked(sourceID, supervisor, transition)
		supervisor.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		handles[sourceID] = handle
	}
	return handles, nil
}

func (runtime *serviceRoutingRuntime) lockFanoutHandles(
	transition preservedOwnerFanout,
) (map[string]sourceFanoutHandle, map[string]sourceMovePeerIdentity, []*sourceSupervisor, error) {
	service := runtime.service
	service.mu.RLock()
	defer service.mu.RUnlock()
	peers := snapshotSourceMovePeersLocked(service, transition.SourceIDs...)
	locked := make([]*sourceSupervisor, 0, len(transition.SourceIDs))
	handles := make(map[string]sourceFanoutHandle, len(transition.SourceIDs))
	for _, sourceID := range transition.SourceIDs {
		supervisor := service.sources[sourceID]
		if supervisor == nil {
			for index := len(locked) - 1; index >= 0; index-- {
				locked[index].mu.Unlock()
			}
			return nil, nil, nil, fmt.Errorf("fan-out source %q is not managed", sourceID)
		}
		supervisor.mu.Lock()
		locked = append(locked, supervisor)
		handle, err := fanoutHandleLocked(sourceID, supervisor, transition)
		if err != nil {
			for index := len(locked) - 1; index >= 0; index-- {
				locked[index].mu.Unlock()
			}
			return nil, nil, nil, err
		}
		handles[sourceID] = handle
	}
	return handles, peers, locked, nil
}

func fanoutHandleLocked(
	sourceID string,
	supervisor *sourceSupervisor,
	transition preservedOwnerFanout,
) (sourceFanoutHandle, error) {
	handle := sourceFanoutHandle{}
	currentGraph := transition.CurrentGraphs[sourceID]
	if supervisor.state != "running" || supervisor.graph == nil || supervisor.graph.runtime == nil ||
		!reflect.DeepEqual(supervisor.config, currentGraph) {
		return handle, fmt.Errorf("fan-out source %q has no exact running graph", sourceID)
	}
	graph := supervisor.graph
	mediaRuntime := graph.runtime
	mediaRuntime.mu.Lock()
	defer mediaRuntime.mu.Unlock()
	childPID := brokerChildPID(mediaRuntime.childCmd)
	captureAvailable := mediaRuntime.childRunning && !mediaRuntime.childFinishing &&
		childPID > 0 && mediaRuntime.mediaEpoch > 0
	if !captureAvailable && !fanoutSourceIsRetiring(transition, sourceID) {
		return handle, fmt.Errorf("fan-out source %q capture owner is unavailable", sourceID)
	}
	return sourceFanoutHandle{
		SourceID: sourceID, Supervisor: supervisor, Graph: graph, Runtime: mediaRuntime,
		CurrentGraph: currentGraph, CandidateGraph: transition.CandidateGraphs[sourceID],
		ChildPID: childPID, MediaEpoch: mediaRuntime.mediaEpoch,
		CaptureAvailable: captureAvailable,
	}, nil
}

func fanoutSourceIsRetiring(transition preservedOwnerFanout, sourceID string) bool {
	for _, candidate := range transition.RetiringSourceIDs {
		if candidate == sourceID {
			return true
		}
	}
	return false
}

type fanoutPreparedGraph struct {
	mu sync.Mutex

	service         *Service
	currentConfig   productconfig.Config
	candidateConfig productconfig.Config
	transition      preservedOwnerFanout
	handles         map[string]sourceFanoutHandle
	peers           map[string]sourceMovePeerIdentity
	inner           routing.PreparedGraph
	locked          []*sourceSupervisor
	locksHeld       bool
	cutover         bool
	terminal        bool
	phase           int
}

func (prepared *fanoutPreparedGraph) Warmup(ctx context.Context) error {
	return prepared.runPhase(ctx, 0, prepared.inner.Warmup)
}

func (prepared *fanoutPreparedGraph) Admit(ctx context.Context) error {
	return prepared.runPhase(ctx, 1, prepared.inner.Admit)
}

func (prepared *fanoutPreparedGraph) AwaitFirstFrame(ctx context.Context) error {
	return prepared.runPhase(ctx, 2, prepared.inner.AwaitFirstFrame)
}

func (prepared *fanoutPreparedGraph) Cutover(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(3); err != nil {
		return err
	}
	if err := prepared.verifyCaptureOwnersLocked(); err != nil {
		return err
	}
	prepared.cutover = true
	err := prepared.inner.Cutover(ctx)
	if err == nil {
		prepared.service.mu.Lock()
		prepared.service.setActiveRoutingConfigLocked(prepared.candidateConfig)
		prepared.service.mu.Unlock()
		prepared.service.signalStreamGovernorPublish()
	}
	if ownerErr := prepared.verifyCaptureOwnersLocked(); ownerErr != nil {
		err = errors.Join(err, ownerErr)
	}
	if err == nil {
		prepared.phase = 4
	}
	return err
}

func (prepared *fanoutPreparedGraph) Verify(ctx context.Context) error {
	return prepared.runPhase(ctx, 4, prepared.inner.Verify)
}

func (prepared *fanoutPreparedGraph) runPhase(
	ctx context.Context,
	expected int,
	operation func(context.Context) error,
) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(expected); err != nil {
		return err
	}
	if err := prepared.verifyCaptureOwnersLocked(); err != nil {
		return err
	}
	err := operation(ctx)
	if ownerErr := prepared.verifyCaptureOwnersLocked(); ownerErr != nil {
		err = errors.Join(err, ownerErr)
	}
	if err == nil {
		prepared.phase++
	}
	return err
}

func (prepared *fanoutPreparedGraph) Rollback(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.terminal {
		return nil
	}
	err := prepared.inner.Rollback(ctx)
	if ownerErr := prepared.verifyCaptureOwnersLocked(); ownerErr != nil {
		err = errors.Join(err, ownerErr)
	}
	if err == nil {
		prepared.service.mu.Lock()
		prepared.service.setActiveRoutingConfigLocked(prepared.currentConfig)
		prepared.service.mu.Unlock()
		prepared.service.signalStreamGovernorPublish()
	}
	prepared.releaseLocked()
	return err
}

func (prepared *fanoutPreparedGraph) Abort(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.terminal {
		return nil
	}
	if prepared.cutover {
		return errors.New("fan-out cutover requires rollback, not abort")
	}
	err := prepared.inner.Abort(ctx)
	if ownerErr := prepared.verifyCaptureOwnersLocked(); ownerErr != nil {
		err = errors.Join(err, ownerErr)
	}
	prepared.releaseLocked()
	return err
}

func (prepared *fanoutPreparedGraph) Retire(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(5); err != nil {
		return err
	}
	err := prepared.inner.Retire(ctx)
	type retiringSource struct {
		id         string
		supervisor *sourceSupervisor
	}
	retiring := make([]retiringSource, 0, len(prepared.transition.RetiringSourceIDs))
	if ownerErr := prepared.verifyCaptureOwnersLocked(); ownerErr != nil {
		err = errors.Join(err, ownerErr)
	} else {
		retiringIDs := make(map[string]bool, len(prepared.transition.RetiringSourceIDs))
		for _, sourceID := range prepared.transition.RetiringSourceIDs {
			retiringIDs[sourceID] = true
		}
		for _, sourceID := range prepared.transition.SourceIDs {
			handle := prepared.handles[sourceID]
			if retiringIDs[sourceID] {
				retiring = append(retiring, retiringSource{id: sourceID, supervisor: handle.Supervisor})
				continue
			}
			handle.Supervisor.config = handle.CandidateGraph
			applyFanoutRoleIdentity(handle.Graph, handle.CandidateGraph)
			handle.Runtime.mu.Lock()
			handle.Graph.plan = handle.Runtime.plan
			handle.Runtime.mu.Unlock()
		}
	}
	prepared.releaseLocked()
	for _, source := range retiring {
		if stopErr := source.supervisor.stop(ctx); stopErr != nil {
			err = errors.Join(err, fmt.Errorf(
				"stop retired fan-out source %q: %w",
				source.id, stopErr))
			continue
		}
		prepared.service.mu.Lock()
		if prepared.service.sources[source.id] == source.supervisor {
			delete(prepared.service.sources, source.id)
		} else {
			err = errors.Join(err, fmt.Errorf(
				"retired fan-out source %q ownership changed", source.id))
		}
		prepared.service.mu.Unlock()
	}
	return err
}

func (prepared *fanoutPreparedGraph) verifyCaptureOwnersLocked() error {
	if !prepared.locksHeld {
		return errors.New("fan-out capture-owner locks are not held")
	}
	for _, sourceID := range prepared.transition.SourceIDs {
		handle := prepared.handles[sourceID]
		if handle.Supervisor == nil || handle.Supervisor.state != "running" ||
			handle.Supervisor.graph != handle.Graph || handle.Graph == nil ||
			handle.Graph.runtime != handle.Runtime ||
			!reflect.DeepEqual(handle.Supervisor.config, handle.CurrentGraph) {
			return fmt.Errorf("fan-out source %q capture-owner identity changed", sourceID)
		}
		handle.Runtime.mu.Lock()
		childPID, mediaEpoch := brokerChildPID(handle.Runtime.childCmd), handle.Runtime.mediaEpoch
		running := handle.Runtime.childRunning && !handle.Runtime.childFinishing
		handle.Runtime.mu.Unlock()
		if !handle.CaptureAvailable {
			if !fanoutSourceIsRetiring(prepared.transition, sourceID) {
				return fmt.Errorf("fan-out source %q unavailable without complete retirement", sourceID)
			}
			if running {
				return fmt.Errorf("fan-out retiring source %q recovered during transaction", sourceID)
			}
			continue
		}
		if !running || childPID != handle.ChildPID || mediaEpoch != handle.MediaEpoch {
			return fmt.Errorf("fan-out source %q capture PID or media epoch changed", sourceID)
		}
	}
	for sourceID, expected := range prepared.peers {
		expected.supervisor.mu.RLock()
		graph := expected.supervisor.graph
		if graph != expected.graph {
			expected.supervisor.mu.RUnlock()
			return fmt.Errorf("fan-out peer source %q graph changed", sourceID)
		}
		if graph != nil && graph.runtime != nil {
			graph.runtime.mu.Lock()
			childPID, mediaEpoch := brokerChildPID(graph.runtime.childCmd), graph.runtime.mediaEpoch
			graph.runtime.mu.Unlock()
			if childPID != expected.childPID || mediaEpoch != expected.mediaEpoch {
				expected.supervisor.mu.RUnlock()
				return fmt.Errorf("fan-out peer source %q capture identity changed", sourceID)
			}
		}
		expected.supervisor.mu.RUnlock()
	}
	return nil
}

func applyFanoutRoleIdentity(graph *graphService, candidate productconfig.SourceGraph) {
	graph.liveStreamID = ""
	graph.analysisStreamID = ""
	graph.archiveStreamID = ""
	if candidate.Live != nil {
		graph.liveStreamID = candidate.Live.ID
	}
	if candidate.Analysis != nil {
		graph.analysisStreamID = candidate.Analysis.ID
	}
	if candidate.Archive != nil {
		graph.archiveStreamID = candidate.Archive.ID
	}
}

func fanoutRuntimePlanMatchesGraph(
	plan videoBrokerPlan,
	graph productconfig.SourceGraph,
) bool {
	return fanoutRuntimePlanMatchesRole(plan, "live", graph.Live) &&
		fanoutRuntimePlanMatchesRole(plan, "analysis", graph.Analysis) &&
		fanoutRuntimePlanMatchesRole(plan, "archive", graph.Archive)
}

func (prepared *fanoutPreparedGraph) requirePhaseLocked(expected int) error {
	if prepared == nil || prepared.service == nil || prepared.inner == nil ||
		prepared.terminal || !prepared.locksHeld {
		return errors.New("fan-out prepared graph is invalid")
	}
	if prepared.phase != expected {
		return fmt.Errorf("fan-out phase order invalid current=%d expected=%d", prepared.phase, expected)
	}
	return nil
}

func (prepared *fanoutPreparedGraph) releaseLocked() {
	if prepared.locksHeld {
		for index := len(prepared.locked) - 1; index >= 0; index-- {
			prepared.locked[index].mu.Unlock()
		}
		prepared.locksHeld = false
	}
	prepared.terminal = true
}

var _ routing.PreparedGraph = (*fanoutPreparedGraph)(nil)
