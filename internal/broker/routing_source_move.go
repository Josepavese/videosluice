package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/internal/unixsocket"
	"github.com/Josepavese/videosluice/routing"
)

type wholeSourceMove struct {
	fromSourceID   string
	toSourceID     string
	streamIDs      []string
	currentGraph   productconfig.SourceGraph
	remainingGraph productconfig.SourceGraph
	candidateGraph productconfig.SourceGraph
	preserveFrom   bool
}

type sourceMovePeerIdentity struct {
	supervisor *sourceSupervisor
	graph      *graphService
	childPID   int
	mediaEpoch uint64
}

type sourceMovePreparedGraph struct {
	mu sync.Mutex

	service             *Service
	currentConfig       productconfig.Config
	candidateConfig     productconfig.Config
	move                wholeSourceMove
	stagedGraph         productconfig.SourceGraph
	oldSupervisor       *sourceSupervisor
	oldGraph            *graphService
	oldRuntime          *videoBrokerRuntime
	candidateContext    context.Context
	candidateCancel     context.CancelFunc
	candidateGraph      *graphService
	candidateRuntime    *videoBrokerRuntime
	candidateSupervisor *sourceSupervisor
	peers               map[string]sourceMovePeerIdentity
	preservedFanout     routing.PreparedGraph

	oldMainPublication    *unixsocket.Publication
	oldArchivePublication *unixsocket.Publication
	oldSubPublication     *unixsocket.Publication
	oldSubShared          *brokerSubSharedFrame
	oldSubNotifier        *brokerSubFrameNotifier

	warmupMainFrame      uint64
	warmupArchiveFrame   uint64
	warmupSubFrame       uint64
	candidatePID         int
	candidateEpoch       uint64
	cutoverMainFrame     uint64
	cutoverArchiveFrame  uint64
	cutoverSubFrame      uint64
	introducedLiveDemand bool
	mainSwapped          bool
	archiveSwapped       bool
	subSocketSwapped     bool
	sharedSwapped        bool
	cutoverStarted       bool
	adopted              bool
	oldLockHeld          bool
	candidateStopped     bool
	phase                int
	terminal             bool
}

func validateWholeSourceMove(
	current productconfig.Config,
	candidate productconfig.Config,
	delta routing.GraphDelta,
) (wholeSourceMove, error) {
	move := wholeSourceMove{}
	if len(delta.ChangedSourceIDs) != 2 || len(delta.ChangedStreamIDs) == 0 {
		return move, errors.New("runtime source move requires one activating and one deactivating source")
	}
	for _, sourceID := range delta.ChangedSourceIDs {
		change, exists := delta.Sources[sourceID]
		if !exists || change.PreserveCaptureOwner {
			return move, errors.New("whole-source move cannot preserve a changed capture owner")
		}
		switch change.State {
		case routing.SourceChangeDeactivate:
			if move.fromSourceID != "" || len(change.CurrentStreams) == 0 || len(change.CandidateStreams) != 0 {
				return wholeSourceMove{}, errors.New("runtime source move has an invalid retiring source")
			}
			move.fromSourceID = sourceID
		case routing.SourceChangeActivate:
			if move.toSourceID != "" || len(change.CurrentStreams) != 0 || len(change.CandidateStreams) == 0 {
				return wholeSourceMove{}, errors.New("runtime source move has an invalid candidate source")
			}
			move.toSourceID = sourceID
		default:
			return wholeSourceMove{}, errors.New("runtime source move must replace a complete source graph")
		}
	}
	if move.fromSourceID == "" || move.toSourceID == "" || move.fromSourceID == move.toSourceID {
		return wholeSourceMove{}, errors.New("runtime source move endpoints are invalid")
	}
	for _, streamID := range delta.ChangedStreamIDs {
		change, exists := delta.Streams[streamID]
		before, beforeExists := current.Streams[streamID]
		after, afterExists := candidate.Streams[streamID]
		if !exists || !beforeExists || !afterExists || change.State != routing.StreamChangeMove ||
			change.CurrentSource != move.fromSourceID || change.CandidateSource != move.toSourceID ||
			before.Type != after.Type || before.Socket != after.Socket ||
			before.SharedMemory != after.SharedMemory || before.NotifySocket != after.NotifySocket {
			return wholeSourceMove{}, fmt.Errorf("runtime source move has an invalid stream %q", streamID)
		}
		move.streamIDs = append(move.streamIDs, streamID)
	}
	sort.Strings(move.streamIDs)
	fromChange := delta.Sources[move.fromSourceID]
	toChange := delta.Sources[move.toSourceID]
	if !reflect.DeepEqual(fromChange.CurrentStreams, move.streamIDs) ||
		!reflect.DeepEqual(toChange.CandidateStreams, move.streamIDs) {
		return wholeSourceMove{}, errors.New("runtime source move must move every stream of the retiring source")
	}
	var ok bool
	move.currentGraph, ok = sourceGraph(current, move.fromSourceID)
	if !ok {
		return wholeSourceMove{}, errors.New("runtime source move retiring graph is missing")
	}
	move.candidateGraph, ok = sourceGraph(candidate, move.toSourceID)
	if !ok {
		return wholeSourceMove{}, errors.New("runtime source move candidate graph is missing")
	}
	if _, exists := sourceGraph(current, move.toSourceID); exists {
		return wholeSourceMove{}, errors.New("runtime source move candidate is already active")
	}
	if _, exists := sourceGraph(candidate, move.fromSourceID); exists {
		return wholeSourceMove{}, errors.New("runtime source move does not fully retire its old source")
	}
	return move, nil
}

// validateMixedSourceActivation classifies the inverse of a retiring-source
// fan-out: one running owner keeps its capture identity and remaining roles,
// while the moved roles activate one previously inactive source. The new
// capture owner must warm independently; the incumbent changes only its output
// branches through the worker fan-out transaction.
func validateMixedSourceActivation(
	current productconfig.Config,
	candidate productconfig.Config,
	delta routing.GraphDelta,
) (wholeSourceMove, error) {
	move := wholeSourceMove{preserveFrom: true}
	if len(delta.ChangedSourceIDs) != 2 || len(delta.ChangedStreamIDs) == 0 {
		return move, errors.New("mixed source activation requires one running and one inactive source")
	}
	for _, sourceID := range delta.ChangedSourceIDs {
		change, exists := delta.Sources[sourceID]
		if !exists {
			return wholeSourceMove{}, errors.New("mixed source activation delta is incomplete")
		}
		switch change.State {
		case routing.SourceChangeReconfigure:
			if move.fromSourceID != "" || !change.PreserveCaptureOwner ||
				len(change.CurrentStreams) == 0 || len(change.CandidateStreams) == 0 {
				return wholeSourceMove{}, errors.New("mixed source activation has an invalid running source")
			}
			move.fromSourceID = sourceID
		case routing.SourceChangeActivate:
			if move.toSourceID != "" || change.PreserveCaptureOwner ||
				len(change.CurrentStreams) != 0 || len(change.CandidateStreams) == 0 {
				return wholeSourceMove{}, errors.New("mixed source activation has an invalid inactive source")
			}
			move.toSourceID = sourceID
		default:
			return wholeSourceMove{}, errors.New("mixed source activation has unsupported source states")
		}
	}
	if move.fromSourceID == "" || move.toSourceID == "" || move.fromSourceID == move.toSourceID {
		return wholeSourceMove{}, errors.New("mixed source activation endpoints are invalid")
	}
	for _, streamID := range delta.ChangedStreamIDs {
		change, exists := delta.Streams[streamID]
		before, beforeExists := current.Streams[streamID]
		after, afterExists := candidate.Streams[streamID]
		if !exists || !beforeExists || !afterExists || change.State != routing.StreamChangeMove ||
			change.CurrentSource != move.fromSourceID || change.CandidateSource != move.toSourceID ||
			before.Type != after.Type || before.Socket != after.Socket ||
			before.SharedMemory != after.SharedMemory || before.NotifySocket != after.NotifySocket {
			return wholeSourceMove{}, fmt.Errorf(
				"mixed source activation has an invalid stream %q", streamID)
		}
		move.streamIDs = append(move.streamIDs, streamID)
	}
	sort.Strings(move.streamIDs)
	fromChange := delta.Sources[move.fromSourceID]
	toChange := delta.Sources[move.toSourceID]
	if !reflect.DeepEqual(toChange.CandidateStreams, move.streamIDs) {
		return wholeSourceMove{}, errors.New("mixed source activation target has unrelated candidate streams")
	}
	remaining := stringSet(fromChange.CandidateStreams)
	moved := stringSet(move.streamIDs)
	for _, streamID := range fromChange.CurrentStreams {
		if !remaining[streamID] && !moved[streamID] {
			return wholeSourceMove{}, errors.New("mixed source activation loses an unrelated stream")
		}
	}
	if len(fromChange.CurrentStreams) != len(fromChange.CandidateStreams)+len(move.streamIDs) {
		return wholeSourceMove{}, errors.New("mixed source activation running-source stream sets differ")
	}
	var ok bool
	move.currentGraph, ok = sourceGraph(current, move.fromSourceID)
	if !ok {
		return wholeSourceMove{}, errors.New("mixed source activation running graph is missing")
	}
	move.remainingGraph, ok = sourceGraph(candidate, move.fromSourceID)
	if !ok || !reflect.DeepEqual(move.currentGraph.Source, move.remainingGraph.Source) {
		return wholeSourceMove{}, errors.New("mixed source activation changes the running capture contract")
	}
	move.candidateGraph, ok = sourceGraph(candidate, move.toSourceID)
	if !ok {
		return wholeSourceMove{}, errors.New("mixed source activation candidate graph is missing")
	}
	if _, exists := sourceGraph(current, move.toSourceID); exists {
		return wholeSourceMove{}, errors.New("mixed source activation target is already active")
	}
	return move, nil
}

func stringSet(values []string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		if result[value] {
			continue
		}
		result[value] = true
	}
	return result
}

func mixedActivationFanout(move wholeSourceMove) preservedOwnerFanout {
	return preservedOwnerFanout{
		SourceIDs:       []string{move.fromSourceID},
		StreamIDs:       append([]string(nil), move.streamIDs...),
		CurrentGraphs:   map[string]productconfig.SourceGraph{move.fromSourceID: move.currentGraph},
		CandidateGraphs: map[string]productconfig.SourceGraph{move.fromSourceID: move.remainingGraph},
	}
}

func (runtime *serviceRoutingRuntime) validateWholeSourceMove(
	ctx context.Context,
	current productconfig.Config,
	candidate productconfig.Config,
	delta routing.GraphDelta,
) error {
	move, err := validateWholeSourceMove(current, candidate, delta)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	service := runtime.service
	service.lifecycleMu.Lock()
	stopping := service.stopping || service.managerCtx == nil || service.managerCtx.Err() != nil
	service.lifecycleMu.Unlock()
	if stopping {
		return errors.New("routing source manager is stopping")
	}
	service.mu.RLock()
	oldSupervisor := service.sources[move.fromSourceID]
	_, targetExists := service.sources[move.toSourceID]
	service.mu.RUnlock()
	if oldSupervisor == nil || targetExists {
		return errors.New("runtime source move ownership differs from the current graph")
	}
	oldSupervisor.mu.RLock()
	defer oldSupervisor.mu.RUnlock()
	if oldSupervisor.state != "running" || oldSupervisor.graph == nil || oldSupervisor.graph.runtime == nil ||
		!reflect.DeepEqual(oldSupervisor.config, move.currentGraph) {
		return errors.New("runtime source move retiring graph is not running exactly")
	}
	oldRuntime := oldSupervisor.graph.runtime
	oldRuntime.mu.Lock()
	defer oldRuntime.mu.Unlock()
	if !oldRuntime.childRunning || oldRuntime.childFinishing ||
		brokerChildPID(oldRuntime.childCmd) <= 0 {
		return errors.New("runtime source move retiring capture owner is unavailable")
	}
	if err := validateSourceMoveRoleIdentity(oldSupervisor.graph, move.currentGraph); err != nil {
		return err
	}
	_, err = stageSourceMoveGraph(move.candidateGraph)
	return err
}

func (runtime *serviceRoutingRuntime) validateMixedSourceActivation(
	ctx context.Context,
	current productconfig.Config,
	candidate productconfig.Config,
	delta routing.GraphDelta,
) error {
	move, err := validateMixedSourceActivation(current, candidate, delta)
	if err != nil {
		return err
	}
	if runtime.fanoutBackend == nil {
		return errors.New("routing backend does not attest dynamic fan-out branch transactions")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	service := runtime.service
	service.lifecycleMu.Lock()
	stopping := service.stopping || service.managerCtx == nil || service.managerCtx.Err() != nil
	service.lifecycleMu.Unlock()
	if stopping {
		return errors.New("routing source manager is stopping")
	}
	service.mu.RLock()
	oldSupervisor := service.sources[move.fromSourceID]
	_, targetExists := service.sources[move.toSourceID]
	service.mu.RUnlock()
	if oldSupervisor == nil || targetExists {
		return errors.New("mixed source activation ownership differs from the current graph")
	}
	transition := mixedActivationFanout(move)
	oldSupervisor.mu.RLock()
	handle, err := fanoutHandleLocked(move.fromSourceID, oldSupervisor, transition)
	oldSupervisor.mu.RUnlock()
	if err != nil {
		return err
	}
	if _, err := stageSourceMoveGraph(move.candidateGraph); err != nil {
		return err
	}
	return runtime.fanoutBackend.ValidateFanout(
		ctx, transition, map[string]sourceFanoutHandle{move.fromSourceID: handle})
}

func (runtime *serviceRoutingRuntime) prepareWholeSourceMove(
	ctx context.Context,
	current productconfig.Config,
	candidate productconfig.Config,
	delta routing.GraphDelta,
) (routing.PreparedGraph, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	move, err := validateWholeSourceMove(current, candidate, delta)
	if err != nil {
		return nil, err
	}
	return runtime.prepareSourceActivation(ctx, current, candidate, move)
}

func (runtime *serviceRoutingRuntime) prepareMixedSourceActivation(
	ctx context.Context,
	current productconfig.Config,
	candidate productconfig.Config,
	delta routing.GraphDelta,
) (routing.PreparedGraph, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	move, err := validateMixedSourceActivation(current, candidate, delta)
	if err != nil {
		return nil, err
	}
	if runtime.fanoutBackend == nil {
		return nil, errors.New("routing backend does not attest dynamic fan-out branch transactions")
	}
	return runtime.prepareSourceActivation(ctx, current, candidate, move)
}

func (runtime *serviceRoutingRuntime) prepareSourceActivation(
	ctx context.Context,
	current productconfig.Config,
	candidate productconfig.Config,
	move wholeSourceMove,
) (routing.PreparedGraph, error) {
	service := runtime.service
	service.lifecycleMu.Lock()
	if service.stopping || service.managerCtx == nil || service.managerCtx.Err() != nil {
		service.lifecycleMu.Unlock()
		return nil, errors.New("routing source manager is stopping")
	}
	candidateContext, candidateCancel := context.WithCancel(service.managerCtx)
	service.lifecycleMu.Unlock()
	service.mu.RLock()
	oldSupervisor := service.sources[move.fromSourceID]
	_, targetExists := service.sources[move.toSourceID]
	peers := snapshotSourceMovePeersLocked(service, move.fromSourceID, move.toSourceID)
	service.mu.RUnlock()
	if oldSupervisor == nil || targetExists {
		candidateCancel()
		return nil, errors.New("runtime source move ownership differs from the current graph")
	}
	oldSupervisor.mu.RLock()
	if oldSupervisor.state != "running" || oldSupervisor.graph == nil || oldSupervisor.graph.runtime == nil ||
		!reflect.DeepEqual(oldSupervisor.config, move.currentGraph) {
		oldSupervisor.mu.RUnlock()
		candidateCancel()
		return nil, errors.New("runtime source move retiring graph is not running exactly")
	}
	oldGraph := oldSupervisor.graph
	oldRuntime := oldGraph.runtime
	var handle sourceFanoutHandle
	var err error
	if move.preserveFrom {
		handle, err = fanoutHandleLocked(
			move.fromSourceID, oldSupervisor, mixedActivationFanout(move))
		if err != nil {
			oldSupervisor.mu.RUnlock()
			candidateCancel()
			return nil, err
		}
	}
	oldSupervisor.mu.RUnlock()
	oldRuntime.mu.Lock()
	if !oldRuntime.childRunning || oldRuntime.childFinishing || brokerChildPID(oldRuntime.childCmd) <= 0 {
		oldRuntime.mu.Unlock()
		candidateCancel()
		return nil, errors.New("runtime source move retiring capture owner is unavailable")
	}
	oldMainPublication := oldRuntime.mainPublication
	oldArchivePublication := oldRuntime.archivePublication
	oldSubPublication := oldRuntime.subPublication
	oldSubShared := oldRuntime.subShared
	oldSubNotifier := oldRuntime.subNotifier
	oldRuntime.mu.Unlock()
	if err := validateSourceMoveRoleIdentity(oldGraph, move.currentGraph); err != nil {
		candidateCancel()
		return nil, err
	}
	staged, err := stageSourceMoveGraph(move.candidateGraph)
	if err != nil {
		candidateCancel()
		return nil, err
	}
	var preservedFanout routing.PreparedGraph
	if move.preserveFrom {
		transition := mixedActivationFanout(move)
		preservedFanout, err = runtime.fanoutBackend.PrepareFanout(
			ctx, transition, map[string]sourceFanoutHandle{move.fromSourceID: handle})
		if err != nil || preservedFanout == nil {
			candidateCancel()
			if err == nil {
				err = errors.New("mixed source activation fan-out returned no prepared transaction")
			}
			return nil, err
		}
	}
	return &sourceMovePreparedGraph{
		service: service, currentConfig: productconfig.Clone(current),
		candidateConfig: productconfig.Clone(candidate), move: move, stagedGraph: staged,
		oldSupervisor: oldSupervisor, oldGraph: oldGraph, oldRuntime: oldRuntime,
		candidateContext: candidateContext, candidateCancel: candidateCancel, peers: peers,
		preservedFanout:    preservedFanout,
		oldMainPublication: oldMainPublication, oldArchivePublication: oldArchivePublication,
		oldSubPublication: oldSubPublication, oldSubShared: oldSubShared,
		oldSubNotifier: oldSubNotifier,
	}, nil
}

func snapshotSourceMovePeersLocked(service *Service, excluded ...string) map[string]sourceMovePeerIdentity {
	skipped := make(map[string]bool, len(excluded))
	for _, sourceID := range excluded {
		skipped[sourceID] = true
	}
	peers := make(map[string]sourceMovePeerIdentity)
	for sourceID, supervisor := range service.sources {
		if skipped[sourceID] {
			continue
		}
		identity := sourceMovePeerIdentity{supervisor: supervisor}
		supervisor.mu.RLock()
		identity.graph = supervisor.graph
		if supervisor.graph != nil && supervisor.graph.runtime != nil {
			supervisor.graph.runtime.mu.Lock()
			identity.childPID = brokerChildPID(supervisor.graph.runtime.childCmd)
			identity.mediaEpoch = supervisor.graph.runtime.mediaEpoch
			supervisor.graph.runtime.mu.Unlock()
		}
		supervisor.mu.RUnlock()
		peers[sourceID] = identity
	}
	return peers
}

func validateSourceMoveRoleIdentity(graph *graphService, expected productconfig.SourceGraph) error {
	if graph == nil || graph.sourceID != expected.SourceID {
		return errors.New("runtime source move graph identity differs")
	}
	wantLive, wantAnalysis, wantArchive := "", "", ""
	if expected.Live != nil {
		wantLive = expected.Live.ID
	}
	if expected.Analysis != nil {
		wantAnalysis = expected.Analysis.ID
	}
	if expected.Archive != nil {
		wantArchive = expected.Archive.ID
	}
	if graph.liveStreamID != wantLive || graph.analysisStreamID != wantAnalysis ||
		graph.archiveStreamID != wantArchive {
		return errors.New("runtime source move stream identity differs")
	}
	return nil
}

func stageSourceMoveGraph(graph productconfig.SourceGraph) (productconfig.SourceGraph, error) {
	nonceBytes := make([]byte, 6)
	if _, err := rand.Read(nonceBytes); err != nil {
		return productconfig.SourceGraph{}, fmt.Errorf("create routing stage identity: %w", err)
	}
	nonce := hex.EncodeToString(nonceBytes)
	staged := graph
	stageStream := func(stream *productconfig.NamedStream, role string) (*productconfig.NamedStream, error) {
		if stream == nil {
			return nil, nil
		}
		copyValue := *stream
		var err error
		copyValue.Config.Socket, err = sourceMoveStagePath(stream.Config.Socket, nonce, role+"-stream")
		if err != nil {
			return nil, err
		}
		copyValue.Config.SharedMemory, err = sourceMoveStagePath(stream.Config.SharedMemory, nonce, role+"-shm")
		if err != nil {
			return nil, err
		}
		copyValue.Config.NotifySocket, err = sourceMoveStagePath(stream.Config.NotifySocket, nonce, role+"-notify")
		if err != nil {
			return nil, err
		}
		return &copyValue, nil
	}
	var err error
	if staged.Live, err = stageStream(graph.Live, "live"); err != nil {
		return productconfig.SourceGraph{}, err
	}
	if staged.Analysis, err = stageStream(graph.Analysis, "analysis"); err != nil {
		return productconfig.SourceGraph{}, err
	}
	if staged.Archive, err = stageStream(graph.Archive, "archive"); err != nil {
		return productconfig.SourceGraph{}, err
	}
	return staged, nil
}

func sourceMoveStagePath(path, nonce, label string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", nil
	}
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) || path == "/" {
		return "", errors.New("routing source move endpoint is invalid")
	}
	return filepath.Join(filepath.Dir(path), ".vb-route-"+nonce+"-"+label), nil
}

func (prepared *sourceMovePreparedGraph) Warmup(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(0); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	graph, err := prepared.service.starter(
		prepared.candidateContext, prepared.stagedGraph,
		cloneSourceRoleProfiles(prepared.service.admission[prepared.move.toSourceID]),
		prepared.service.inventory, prepared.service.allocator,
	)
	if graph != nil && graph.done != nil {
		prepared.candidateGraph = graph
		prepared.candidateRuntime = graph.runtime
	}
	if err != nil || graph == nil || graph.runtime == nil || graph.done == nil {
		if prepared.candidateGraph == nil {
			prepared.candidateCancel()
			prepared.service.allocator.release(prepared.move.toSourceID)
		}
		if err == nil {
			err = errors.New("routing source candidate returned an incomplete graph")
		}
		return err
	}
	if err := validateSourceMoveRoleIdentity(graph, prepared.stagedGraph); err != nil {
		return err
	}
	runtime := graph.runtime
	runtime.mu.Lock()
	prepared.warmupMainFrame = runtime.totalMainFrames
	prepared.warmupArchiveFrame = runtime.totalArchiveFrames
	prepared.warmupSubFrame = runtime.totalSubFrames
	if prepared.move.candidateGraph.Live != nil && len(runtime.mainClients) == 0 {
		previous := runtime.bridgeControlTargetsLocked()
		runtime.routingLiveDemand = true
		prepared.introducedLiveDemand = true
		if runtime.childControlSupported {
			current := runtime.bridgeControlTargetsLocked()
			if liveEncoderActivationChanged(previous, current) {
				runtime.mainAwaitingEncoderEpoch = true
				runtime.armMainKeyframeGateLocked()
			}
			if _, queued := runtime.queueBridgeControlLocked("routing_source_move_warmup"); !queued {
				runtime.routingLiveDemand = false
				prepared.introducedLiveDemand = false
				runtime.mu.Unlock()
				return errors.New("routing source candidate live warmup could not be queued")
			}
		}
	}
	runtime.mu.Unlock()
	if prepared.preservedFanout != nil {
		if err := prepared.preservedFanout.Warmup(ctx); err != nil {
			return err
		}
	}
	prepared.phase = 1
	return nil
}

func (prepared *sourceMovePreparedGraph) Admit(ctx context.Context) error {
	prepared.mu.Lock()
	if err := prepared.requirePhaseLocked(1); err != nil {
		prepared.mu.Unlock()
		return err
	}
	prepared.mu.Unlock()
	if err := prepared.waitCandidate(ctx, false); err != nil {
		return err
	}
	if prepared.preservedFanout != nil {
		if err := prepared.preservedFanout.Admit(ctx); err != nil {
			return err
		}
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(1); err != nil {
		return err
	}
	prepared.candidateRuntime.mu.Lock()
	prepared.candidatePID = brokerChildPID(prepared.candidateRuntime.childCmd)
	prepared.candidateEpoch = prepared.candidateRuntime.mediaEpoch
	prepared.candidateRuntime.mu.Unlock()
	if prepared.candidatePID <= 0 || prepared.candidateEpoch == 0 {
		return errors.New("routing source candidate has no stable capture identity")
	}
	prepared.phase = 2
	return nil
}

func (prepared *sourceMovePreparedGraph) AwaitFirstFrame(ctx context.Context) error {
	prepared.mu.Lock()
	if err := prepared.requirePhaseLocked(2); err != nil {
		prepared.mu.Unlock()
		return err
	}
	prepared.mu.Unlock()
	if err := prepared.waitCandidate(ctx, true); err != nil {
		return err
	}
	if prepared.preservedFanout != nil {
		if err := prepared.preservedFanout.AwaitFirstFrame(ctx); err != nil {
			return err
		}
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(2); err != nil {
		return err
	}
	prepared.phase = 3
	return nil
}

func (prepared *sourceMovePreparedGraph) waitCandidate(ctx context.Context, requireFreshFrame bool) error {
	if ctx == nil {
		return errors.New("routing source candidate context is required")
	}
	ticker := time.NewTicker(routingFramePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-prepared.candidateGraph.done:
			return errors.New("routing source candidate stopped during preparation")
		default:
		}
		runtime := prepared.candidateRuntime
		runtime.mu.Lock()
		err, ready := prepared.candidateReadyLocked(runtime, requireFreshFrame)
		runtime.mu.Unlock()
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			runtime.mu.Lock()
			failure := fmt.Errorf("routing candidate wait source=%q fresh_frame=%t child_pid=%d media_epoch=%d frame_access=%t analysis_frames=%d baseline=%d causal_frame=%d causal_epoch=%d: %w",
				runtime.plan.SourceID, requireFreshFrame, brokerChildPID(runtime.childCmd), runtime.mediaEpoch,
				runtime.plan.FrameCapture != nil, runtime.totalSubFrames, prepared.warmupSubFrame,
				runtime.causalSubFrame, runtime.causalSubSourceEpoch, ctx.Err())
			runtime.mu.Unlock()
			return failure
		case <-ticker.C:
		}
	}
}

func (prepared *sourceMovePreparedGraph) candidateReadyLocked(
	runtime *videoBrokerRuntime,
	requireFreshFrame bool,
) (error, bool) {
	if runtime == nil || !runtime.childRunning || runtime.childFinishing || brokerChildPID(runtime.childCmd) <= 0 {
		return nil, false
	}
	if prepared.candidatePID != 0 &&
		(brokerChildPID(runtime.childCmd) != prepared.candidatePID || runtime.mediaEpoch != prepared.candidateEpoch) {
		return errors.New("routing source candidate capture identity changed"), false
	}
	if runtime.cameraAdmission.DevicePath != "" && !runtime.cameraAdmitted {
		return nil, false
	}
	if err, ready := sourceMoveHardwareReadyLocked(runtime); err != nil || !ready {
		return err, false
	}
	if !requireFreshFrame {
		return nil, true
	}
	if prepared.move.candidateGraph.Live != nil {
		if runtime.totalMainFrames <= prepared.warmupMainFrame ||
			!brokerMainBootstrapReady(runtime.plan.MainFormat, runtime.mainBootstrap) {
			return nil, false
		}
	}
	if prepared.move.candidateGraph.Archive != nil {
		archiveCodec := runtime.observedArchiveCodec
		if archiveCodec == "" {
			archiveCodec = runtime.plan.ArchiveCodec
		}
		if runtime.totalArchiveFrames <= prepared.warmupArchiveFrame ||
			!brokerMainBootstrapReady(archiveCodec, runtime.archiveBootstrap) {
			return nil, false
		}
	}
	if prepared.move.candidateGraph.Analysis != nil {
		if runtime.totalSubFrames <= prepared.warmupSubFrame {
			return nil, false
		}
		if runtime.plan.FrameCapture != nil {
			// Causal delivery deliberately has no NMSH buffer. Require a fresh
			// verified publication from the currently admitted capture instead;
			// never substitute raw mux activity or an earlier generation.
			if runtime.childFrames == nil || runtime.mediaEpoch == 0 ||
				runtime.childFrames.SourceEpoch() != runtime.mediaEpoch ||
				runtime.causalSubSourceEpoch != runtime.mediaEpoch ||
				runtime.causalSubFrame <= prepared.warmupSubFrame {
				return nil, false
			}
		} else if runtime.plan.SubShmPath != "" && (runtime.subShared == nil || runtime.subShared.Epoch() == 0) {
			return nil, false
		}
	}
	return nil, true
}

func sourceMoveHardwareReadyLocked(runtime *videoBrokerRuntime) (error, bool) {
	for _, stage := range runtime.capabilityPlan.Stages {
		if stage.State == capability.StateRejected {
			return fmt.Errorf("routing source candidate hardware stage rejected: %s/%s", stage.Stage, stage.Role), false
		}
		if stage.Hardware && stage.State != capability.StateAttested {
			return nil, false
		}
		if runtime.plan.RequireHWCodec &&
			(stage.Stage == capability.StageEncode || stage.Stage == capability.StageDecode) &&
			!stage.Hardware {
			return errors.New("routing source candidate selected a software codec under strict hardware policy"), false
		}
	}
	return nil, true
}

func (prepared *sourceMovePreparedGraph) Cutover(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(3); err != nil {
		return err
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	prepared.oldSupervisor.mu.Lock()
	prepared.oldLockHeld = true
	if prepared.oldSupervisor.state != "running" || prepared.oldSupervisor.graph != prepared.oldGraph ||
		!reflect.DeepEqual(prepared.oldSupervisor.config, prepared.move.currentGraph) {
		return errors.New("routing source retiring graph changed before cutover")
	}
	prepared.cutoverStarted = true
	if prepared.preservedFanout != nil {
		if err := prepared.preservedFanout.Cutover(ctx); err != nil {
			return err
		}
	}
	prepared.oldRuntime.mu.Lock()
	prepared.candidateRuntime.mu.Lock()
	if err := prepared.validateCutoverIdentitiesLocked(); err != nil {
		prepared.candidateRuntime.mu.Unlock()
		prepared.oldRuntime.mu.Unlock()
		return err
	}
	if err := prepared.replaceEndpointsLocked(); err != nil {
		prepared.candidateRuntime.mu.Unlock()
		prepared.oldRuntime.mu.Unlock()
		return err
	}
	prepared.cutoverMainFrame = prepared.candidateRuntime.totalMainFrames
	prepared.cutoverArchiveFrame = prepared.candidateRuntime.totalArchiveFrames
	prepared.cutoverSubFrame = prepared.candidateRuntime.totalSubFrames
	if !prepared.move.preserveFrom {
		prepared.disconnectRuntimeClientsLocked(prepared.oldRuntime)
	}
	prepared.candidateRuntime.mu.Unlock()
	prepared.oldRuntime.mu.Unlock()

	candidateSupervisor, err := prepared.service.adoptPreparedSource(
		prepared.candidateContext, prepared.candidateCancel,
		prepared.move.candidateGraph, prepared.candidateGraph,
	)
	if err != nil {
		return err
	}
	prepared.candidateSupervisor = candidateSupervisor
	prepared.adopted = true
	prepared.service.mu.Lock()
	if prepared.service.sources[prepared.move.fromSourceID] != prepared.oldSupervisor ||
		prepared.service.sources[prepared.move.toSourceID] != nil {
		prepared.service.mu.Unlock()
		return errors.New("routing source ownership changed during cutover")
	}
	if !prepared.move.preserveFrom {
		delete(prepared.service.sources, prepared.move.fromSourceID)
	}
	prepared.service.sources[prepared.move.toSourceID] = candidateSupervisor
	prepared.service.setActiveRoutingConfigLocked(prepared.candidateConfig)
	prepared.service.mu.Unlock()
	prepared.service.signalStreamGovernorPublish()
	prepared.phase = 4
	return nil
}

func (prepared *sourceMovePreparedGraph) validateCutoverIdentitiesLocked() error {
	if !prepared.oldRuntime.childRunning || prepared.oldRuntime.childFinishing ||
		brokerChildPID(prepared.oldRuntime.childCmd) <= 0 {
		return errors.New("routing source retiring capture owner is unavailable at cutover")
	}
	if !prepared.candidateRuntime.childRunning || prepared.candidateRuntime.childFinishing ||
		brokerChildPID(prepared.candidateRuntime.childCmd) != prepared.candidatePID ||
		prepared.candidateRuntime.mediaEpoch != prepared.candidateEpoch {
		return errors.New("routing source candidate identity changed before cutover")
	}
	return nil
}

func (prepared *sourceMovePreparedGraph) replaceEndpointsLocked() error {
	oldRuntime, candidateRuntime := prepared.oldRuntime, prepared.candidateRuntime
	logical := prepared.move.candidateGraph
	oldMainPublication := oldRuntime.mainPublication
	oldArchivePublication := oldRuntime.archivePublication
	oldSubPublication := oldRuntime.subPublication
	oldSubShared := oldRuntime.subShared
	oldSubNotifier := oldRuntime.subNotifier
	if prepared.move.preserveFrom {
		oldMainPublication = prepared.oldMainPublication
		oldArchivePublication = prepared.oldArchivePublication
		oldSubPublication = prepared.oldSubPublication
		oldSubShared = prepared.oldSubShared
		oldSubNotifier = prepared.oldSubNotifier
	}
	if logical.Live != nil {
		if candidateRuntime.mainPublication == nil || oldMainPublication == nil {
			return errors.New("routing live publication is unavailable")
		}
		err := candidateRuntime.mainPublication.Replace(oldMainPublication)
		prepared.mainSwapped = candidateRuntime.mainPublication.Owns(logical.Live.Config.Socket)
		if err != nil {
			return fmt.Errorf("replace live publication: %w", err)
		}
		candidateRuntime.plan.MainSocketPath = logical.Live.Config.Socket
	}
	if logical.Archive != nil {
		if candidateRuntime.archivePublication == nil || oldArchivePublication == nil {
			return errors.New("routing archive publication is unavailable")
		}
		err := candidateRuntime.archivePublication.Replace(oldArchivePublication)
		prepared.archiveSwapped = candidateRuntime.archivePublication.Owns(logical.Archive.Config.Socket)
		if err != nil {
			return fmt.Errorf("replace archive publication: %w", err)
		}
		candidateRuntime.plan.ArchiveSocketPath = logical.Archive.Config.Socket
	}
	if logical.Analysis != nil {
		if logical.Analysis.Config.Socket != "" {
			if candidateRuntime.subPublication == nil || oldSubPublication == nil {
				return errors.New("routing analysis publication is unavailable")
			}
			err := candidateRuntime.subPublication.Replace(oldSubPublication)
			prepared.subSocketSwapped = candidateRuntime.subPublication.Owns(logical.Analysis.Config.Socket)
			if err != nil {
				return fmt.Errorf("replace analysis publication: %w", err)
			}
			candidateRuntime.plan.SubSocketPath = logical.Analysis.Config.Socket
		}
		if logical.Analysis.Config.SharedMemory != "" {
			if candidateRuntime.subShared == nil || oldSubShared == nil {
				return errors.New("routing analysis shared-memory publication is unavailable")
			}
			err := candidateRuntime.subShared.Replace(oldSubShared)
			prepared.sharedSwapped = candidateRuntime.subShared.Owns(logical.Analysis.Config.SharedMemory)
			if err != nil {
				return fmt.Errorf("replace analysis shared-memory publication: %w", err)
			}
			candidateRuntime.plan.SubShmPath = logical.Analysis.Config.SharedMemory
		}
		if oldSubNotifier != nil {
			oldSubNotifier.Rebind("")
		}
		if candidateRuntime.subNotifier != nil {
			candidateRuntime.subNotifier.Rebind(logical.Analysis.Config.NotifySocket)
			candidateRuntime.plan.SubNotifySocketPath = logical.Analysis.Config.NotifySocket
		}
	}
	prepared.candidateGraph.plan = candidateRuntime.plan
	return nil
}

func (prepared *sourceMovePreparedGraph) disconnectRuntimeClientsLocked(runtime *videoBrokerRuntime) {
	for clientID := range runtime.mainClients {
		runtime.detachClientLocked(brokerStreamH264, clientID)
	}
	for clientID := range runtime.archiveClients {
		runtime.detachClientLocked(brokerEndpointArchive, clientID)
	}
	for clientID := range runtime.subClients {
		runtime.detachClientLocked(brokerStreamNV12, clientID)
	}
}

func (prepared *sourceMovePreparedGraph) Verify(ctx context.Context) error {
	prepared.mu.Lock()
	if err := prepared.requirePhaseLocked(4); err != nil {
		prepared.mu.Unlock()
		return err
	}
	prepared.mu.Unlock()
	if prepared.preservedFanout != nil {
		if err := prepared.preservedFanout.Verify(ctx); err != nil {
			return err
		}
	}
	ticker := time.NewTicker(routingFramePollInterval)
	defer ticker.Stop()
	for {
		if err := prepared.verifyCandidateState(); err != nil {
			return err
		}
		prepared.candidateRuntime.mu.Lock()
		fresh := prepared.sourceMoveFramesAdvancedLocked()
		prepared.candidateRuntime.mu.Unlock()
		if fresh {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(4); err != nil {
		return err
	}
	prepared.phase = 5
	return nil
}

func (prepared *sourceMovePreparedGraph) verifyCandidateState() error {
	prepared.service.mu.RLock()
	active := prepared.service.sources[prepared.move.toSourceID]
	retiring := prepared.service.sources[prepared.move.fromSourceID]
	currentPeers := make(map[string]*sourceSupervisor, len(prepared.peers))
	for sourceID := range prepared.peers {
		currentPeers[sourceID] = prepared.service.sources[sourceID]
	}
	prepared.service.mu.RUnlock()
	retiringMatches := retiring == nil
	if prepared.move.preserveFrom {
		retiringMatches = retiring == prepared.oldSupervisor
	}
	if active != prepared.candidateSupervisor || !retiringMatches {
		return errors.New("routing source ownership is not atomically cut over")
	}
	if prepared.move.preserveFrom {
		if prepared.oldSupervisor.state != "running" ||
			prepared.oldSupervisor.graph != prepared.oldGraph ||
			!reflect.DeepEqual(prepared.oldSupervisor.config, prepared.move.currentGraph) {
			return errors.New("mixed source activation changed the preserved owner before retire")
		}
		prepared.oldRuntime.mu.Lock()
		preservedRunning := prepared.oldRuntime.childRunning && !prepared.oldRuntime.childFinishing &&
			brokerChildPID(prepared.oldRuntime.childCmd) > 0 &&
			fanoutRuntimePlanMatchesGraph(prepared.oldRuntime.plan, prepared.move.remainingGraph)
		prepared.oldRuntime.mu.Unlock()
		if !preservedRunning {
			return errors.New("mixed source activation lost the preserved capture owner")
		}
	}
	active.mu.RLock()
	if active.state != "running" || active.graph != prepared.candidateGraph ||
		!reflect.DeepEqual(active.config, prepared.move.candidateGraph) {
		active.mu.RUnlock()
		return errors.New("routing source candidate supervisor is not running exactly")
	}
	active.mu.RUnlock()
	for sourceID, expected := range prepared.peers {
		if currentPeers[sourceID] != expected.supervisor {
			return fmt.Errorf("routing peer source %q ownership changed", sourceID)
		}
		expected.supervisor.mu.RLock()
		graph := expected.supervisor.graph
		if graph != expected.graph {
			expected.supervisor.mu.RUnlock()
			return fmt.Errorf("routing peer source %q graph changed", sourceID)
		}
		if graph != nil && graph.runtime != nil {
			graph.runtime.mu.Lock()
			pid, epoch := brokerChildPID(graph.runtime.childCmd), graph.runtime.mediaEpoch
			graph.runtime.mu.Unlock()
			if pid != expected.childPID || epoch != expected.mediaEpoch {
				expected.supervisor.mu.RUnlock()
				return fmt.Errorf("routing peer source %q capture identity changed", sourceID)
			}
		}
		expected.supervisor.mu.RUnlock()
	}
	prepared.candidateRuntime.mu.Lock()
	defer prepared.candidateRuntime.mu.Unlock()
	if err := prepared.validateCandidatePublicationsLocked(); err != nil {
		return err
	}
	err, ready := prepared.candidateReadyLocked(prepared.candidateRuntime, false)
	if err != nil {
		return err
	}
	if !ready {
		return errors.New("routing source candidate lost admission after cutover")
	}
	return nil
}

func (prepared *sourceMovePreparedGraph) validateCandidatePublicationsLocked() error {
	logical := prepared.move.candidateGraph
	if logical.Live != nil && !prepared.candidateRuntime.mainPublication.Owns(logical.Live.Config.Socket) {
		return errors.New("routing live endpoint is not owned by the candidate")
	}
	if logical.Archive != nil && !prepared.candidateRuntime.archivePublication.Owns(logical.Archive.Config.Socket) {
		return errors.New("routing archive endpoint is not owned by the candidate")
	}
	if logical.Analysis != nil {
		if logical.Analysis.Config.Socket != "" &&
			!prepared.candidateRuntime.subPublication.Owns(logical.Analysis.Config.Socket) {
			return errors.New("routing analysis socket is not owned by the candidate")
		}
		if logical.Analysis.Config.SharedMemory != "" &&
			!prepared.candidateRuntime.subShared.Owns(logical.Analysis.Config.SharedMemory) {
			return errors.New("routing analysis shared memory is not owned by the candidate")
		}
	}
	return nil
}

func (prepared *sourceMovePreparedGraph) sourceMoveFramesAdvancedLocked() bool {
	if brokerChildPID(prepared.candidateRuntime.childCmd) != prepared.candidatePID ||
		prepared.candidateRuntime.mediaEpoch != prepared.candidateEpoch {
		return false
	}
	if prepared.move.candidateGraph.Live != nil &&
		prepared.candidateRuntime.totalMainFrames <= prepared.cutoverMainFrame {
		return false
	}
	if prepared.move.candidateGraph.Archive != nil &&
		prepared.candidateRuntime.totalArchiveFrames <= prepared.cutoverArchiveFrame {
		return false
	}
	if prepared.move.candidateGraph.Analysis != nil &&
		prepared.candidateRuntime.totalSubFrames <= prepared.cutoverSubFrame {
		return false
	}
	return true
}

func (prepared *sourceMovePreparedGraph) Rollback(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.terminal {
		return nil
	}
	var rollbackErr error
	if prepared.cutoverStarted {
		prepared.oldRuntime.mu.Lock()
		prepared.candidateRuntime.mu.Lock()
		rollbackErr = prepared.restoreEndpointsLocked()
		prepared.disconnectRuntimeClientsLocked(prepared.candidateRuntime)
		prepared.candidateRuntime.mu.Unlock()
		prepared.oldRuntime.mu.Unlock()
		// Restore logical endpoint ownership while the incumbent branches are
		// still detached. Only then reattach them and clear their worker gates;
		// this prevents two generations from publishing the same role during
		// rollback.
		if prepared.preservedFanout != nil {
			rollbackErr = errors.Join(rollbackErr, prepared.preservedFanout.Rollback(ctx))
		}
		if prepared.adopted {
			prepared.service.mu.Lock()
			if prepared.service.sources[prepared.move.toSourceID] == prepared.candidateSupervisor {
				delete(prepared.service.sources, prepared.move.toSourceID)
				if !prepared.move.preserveFrom {
					prepared.service.sources[prepared.move.fromSourceID] = prepared.oldSupervisor
				}
				if rollbackErr == nil {
					prepared.service.setActiveRoutingConfigLocked(prepared.currentConfig)
				}
			} else {
				rollbackErr = errors.Join(rollbackErr, errors.New("routing rollback source ownership changed"))
			}
			prepared.service.mu.Unlock()
			prepared.service.signalStreamGovernorPublish()
		}
	}
	prepared.releaseOldLockLocked()
	rollbackErr = errors.Join(rollbackErr, prepared.stopCandidateLocked(ctx))
	prepared.terminal = true
	return rollbackErr
}

func (prepared *sourceMovePreparedGraph) restoreEndpointsLocked() error {
	var restoreErr error
	oldRuntime, candidateRuntime := prepared.oldRuntime, prepared.candidateRuntime
	logical := prepared.move.currentGraph
	oldMainPublication := oldRuntime.mainPublication
	oldArchivePublication := oldRuntime.archivePublication
	oldSubPublication := oldRuntime.subPublication
	oldSubShared := oldRuntime.subShared
	oldSubNotifier := oldRuntime.subNotifier
	if prepared.move.preserveFrom {
		oldMainPublication = prepared.oldMainPublication
		oldArchivePublication = prepared.oldArchivePublication
		oldSubPublication = prepared.oldSubPublication
		oldSubShared = prepared.oldSubShared
		oldSubNotifier = prepared.oldSubNotifier
	}
	if prepared.sharedSwapped {
		restoreErr = errors.Join(restoreErr, oldSubShared.Replace(candidateRuntime.subShared))
	}
	if prepared.subSocketSwapped {
		restoreErr = errors.Join(restoreErr, oldSubPublication.Replace(candidateRuntime.subPublication))
	}
	if prepared.archiveSwapped {
		restoreErr = errors.Join(restoreErr, oldArchivePublication.Replace(candidateRuntime.archivePublication))
	}
	if prepared.mainSwapped {
		restoreErr = errors.Join(restoreErr, oldMainPublication.Replace(candidateRuntime.mainPublication))
	}
	if logical.Analysis != nil {
		if oldSubNotifier != nil {
			oldSubNotifier.Rebind(logical.Analysis.Config.NotifySocket)
		}
		if candidateRuntime.subNotifier != nil {
			candidateRuntime.subNotifier.Rebind("")
		}
	}
	return restoreErr
}

func (prepared *sourceMovePreparedGraph) Abort(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.terminal {
		return nil
	}
	if prepared.cutoverStarted {
		return errors.New("routing source cutover requires rollback")
	}
	var err error
	if prepared.preservedFanout != nil {
		err = prepared.preservedFanout.Abort(ctx)
	}
	err = errors.Join(err, prepared.stopCandidateLocked(ctx))
	prepared.terminal = true
	return err
}

func (prepared *sourceMovePreparedGraph) Retire(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(5); err != nil {
		return err
	}
	prepared.candidateRuntime.mu.Lock()
	if prepared.introducedLiveDemand {
		prepared.candidateRuntime.routingLiveDemand = false
		prepared.candidateRuntime.queueBridgeControlLocked("routing_source_move_warmup_release")
		prepared.introducedLiveDemand = false
	}
	prepared.candidateRuntime.mu.Unlock()
	var err error
	if prepared.preservedFanout != nil {
		err = prepared.preservedFanout.Retire(ctx)
		prepared.oldSupervisor.config = prepared.move.remainingGraph
		applyFanoutRoleIdentity(prepared.oldGraph, prepared.move.remainingGraph)
		prepared.oldRuntime.mu.Lock()
		prepared.oldGraph.plan = prepared.oldRuntime.plan
		prepared.oldRuntime.mu.Unlock()
	}
	prepared.releaseOldLockLocked()
	if !prepared.move.preserveFrom {
		err = errors.Join(err, prepared.oldSupervisor.stop(ctx))
	}
	prepared.terminal = true
	return err
}

func (prepared *sourceMovePreparedGraph) stopCandidateLocked(ctx context.Context) error {
	if prepared.candidateStopped {
		return nil
	}
	prepared.candidateStopped = true
	if prepared.candidateGraph == nil {
		prepared.candidateCancel()
		return nil
	}
	if prepared.adopted && prepared.candidateSupervisor != nil {
		return prepared.candidateSupervisor.stop(ctx)
	}
	prepared.candidateCancel()
	select {
	case <-prepared.candidateGraph.done:
		prepared.service.allocator.release(prepared.move.toSourceID)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (prepared *sourceMovePreparedGraph) releaseOldLockLocked() {
	if prepared.oldLockHeld {
		prepared.oldLockHeld = false
		prepared.oldSupervisor.mu.Unlock()
	}
}

func (prepared *sourceMovePreparedGraph) requirePhaseLocked(expected int) error {
	if prepared == nil || prepared.service == nil || prepared.oldSupervisor == nil || prepared.terminal {
		return errors.New("routing source prepared graph is invalid")
	}
	if prepared.phase != expected {
		return fmt.Errorf("routing source phase order invalid current=%d expected=%d", prepared.phase, expected)
	}
	return nil
}

var _ routing.PreparedGraph = (*sourceMovePreparedGraph)(nil)
