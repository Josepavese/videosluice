package broker

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/internal/unixsocket"
	"github.com/Josepavese/videosluice/routing"
)

func TestApplyGraphMovesCompleteSourceAndPreservesPeerCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	catalog := wholeSourceMoveCatalog(t, t.TempDir())
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	starter := newSourceMoveTestStarter(t)
	service, err := startServiceWithRestartPolicy(
		ctx, current, capability.Inventory{Schema: capability.Schema}, starter.start,
		sourceRestartPolicy{InitialDelay: time.Second, MaxDelay: time.Second, StableWindow: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		if err := service.Wait(context.Background()); err != nil {
			t.Errorf("source manager shutdown: %v", err)
		}
	}()

	service.mu.RLock()
	oldSupervisor := service.sources["camera-a"]
	peerSupervisor := service.sources["camera-peer"]
	service.mu.RUnlock()
	if oldSupervisor == nil || peerSupervisor == nil || starter.callCount() != 2 {
		t.Fatalf("initial source set is incomplete calls=%d", starter.callCount())
	}
	peerSupervisor.mu.RLock()
	peerGraph := peerSupervisor.graph
	peerSupervisor.mu.RUnlock()
	peerGraph.runtime.mu.Lock()
	peerPID := brokerChildPID(peerGraph.runtime.childCmd)
	peerEpoch := peerGraph.runtime.mediaEpoch
	peerFrames := peerGraph.runtime.totalMainFrames
	peerGraph.runtime.mu.Unlock()

	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	selections := map[string]string{
		"live": "camera-b", "analysis": "camera-b", "archive": "camera-b", "peer_live": "fixed",
	}
	validator, ok := NewRoutingRuntime(service).(routing.GraphValidator)
	if !ok {
		t.Fatal("routing runtime has no read-only validator")
	}
	preview, err := routing.PreviewRuntimeGraph(
		ctx, catalog, store, initial.Revision, selections, validator,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(preview.ChangedSourceIDs, []string{"camera-a", "camera-b"}) ||
		starter.callCount() != 2 {
		t.Fatalf("preview mutated or misclassified source move: preview=%+v calls=%d", preview, starter.callCount())
	}

	applied, err := routing.ApplyGraph(ctx, catalog, store, initial.Revision, selections, NewRoutingRuntime(service))
	if err != nil {
		t.Fatal(err)
	}
	if applied.Revision != 2 || applied.Selections["live"] != "camera-b" ||
		applied.StreamGenerations["live"] != 2 || applied.StreamGenerations["analysis"] != 2 ||
		applied.StreamGenerations["archive"] != 2 || applied.StreamGenerations["peer_live"] != 1 {
		t.Fatalf("unexpected persisted source-move plan: %+v", applied)
	}
	persisted, err := store.Read(catalog)
	if err != nil || !reflect.DeepEqual(persisted, applied) {
		t.Fatalf("source-move plan was not persisted exactly: got=%+v err=%v", persisted, err)
	}
	wantActive := mustMaterializePlan(t, catalog, applied)
	service.mu.RLock()
	gotActive := productconfig.Clone(service.config)
	service.mu.RUnlock()
	if !reflect.DeepEqual(gotActive, wantActive) {
		t.Fatal("stream governor routing authority did not follow source cutover")
	}

	service.mu.RLock()
	candidateSupervisor := service.sources["camera-b"]
	retired := service.sources["camera-a"]
	currentPeer := service.sources["camera-peer"]
	service.mu.RUnlock()
	if candidateSupervisor == nil || retired != nil || currentPeer != peerSupervisor || starter.callCount() != 3 {
		t.Fatalf("source ownership did not cut over exactly calls=%d", starter.callCount())
	}
	candidateSupervisor.mu.RLock()
	candidateGraph := candidateSupervisor.graph
	candidateSupervisor.mu.RUnlock()
	if candidateGraph == nil || candidateGraph.runtime == nil {
		t.Fatal("candidate supervisor has no running graph")
	}
	assertSourceMovePublications(t, candidateGraph.runtime, current)
	peerGraph.runtime.mu.Lock()
	gotPeerPID := brokerChildPID(peerGraph.runtime.childCmd)
	gotPeerEpoch := peerGraph.runtime.mediaEpoch
	gotPeerFrames := peerGraph.runtime.totalMainFrames
	peerGraph.runtime.mu.Unlock()
	if gotPeerPID != peerPID || gotPeerEpoch != peerEpoch || gotPeerFrames <= peerFrames {
		t.Fatalf(
			"peer capture identity/progress changed pid=%d/%d epoch=%d/%d frames=%d/%d",
			gotPeerPID, peerPID, gotPeerEpoch, peerEpoch, gotPeerFrames, peerFrames,
		)
	}
	select {
	case <-oldSupervisor.done:
	case <-time.After(time.Second):
		t.Fatal("retired capture owner did not stop")
	}
	entries, err := os.ReadDir(filepath.Dir(current.Streams["live"].Socket))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".vb-route-") {
			t.Fatalf("routing staging alias survived commit: %s", entry.Name())
		}
	}
}

func TestApplyGraphActivatesNewSourceAndPreservesRunningSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	catalog := wholeSourceMoveCatalog(t, t.TempDir())
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	starter := newSourceMoveTestStarter(t)
	service, err := startServiceWithRestartPolicy(
		ctx, current, capability.Inventory{Schema: capability.Schema}, starter.start,
		sourceRestartPolicy{InitialDelay: time.Second, MaxDelay: time.Second, StableWindow: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		if err := service.Wait(context.Background()); err != nil {
			t.Errorf("source manager shutdown: %v", err)
		}
	}()
	backend := &fanoutBackendRecorder{}
	service.fanout = backend
	service.mu.RLock()
	preserved := service.sources["camera-a"]
	peer := service.sources["camera-peer"]
	service.mu.RUnlock()
	preserved.mu.RLock()
	preservedGraph := preserved.graph
	preservedRuntime := preservedGraph.runtime
	preserved.mu.RUnlock()
	preservedRuntime.mu.Lock()
	preservedPID := brokerChildPID(preservedRuntime.childCmd)
	preservedEpoch := preservedRuntime.mediaEpoch
	preservedRuntime.mu.Unlock()
	peer.mu.RLock()
	peerGraph := peer.graph
	peer.mu.RUnlock()

	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	selections := map[string]string{
		"live": "camera-b", "analysis": "camera-a", "archive": "camera-a", "peer_live": "fixed",
	}
	preview, err := routing.PreviewRuntimeGraph(
		ctx, catalog, store, initial.Revision, selections,
		NewRoutingRuntime(service).(routing.GraphValidator),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(preview.ChangedSourceIDs, []string{"camera-a", "camera-b"}) ||
		starter.callCount() != 2 {
		t.Fatalf("mixed activation preview mutated runtime: preview=%+v calls=%d", preview, starter.callCount())
	}
	applied, err := routing.ApplyGraph(
		ctx, catalog, store, initial.Revision, selections, NewRoutingRuntime(service))
	if err != nil {
		t.Fatal(err)
	}
	service.mu.RLock()
	gotPreserved := service.sources["camera-a"]
	activated := service.sources["camera-b"]
	gotPeer := service.sources["camera-peer"]
	service.mu.RUnlock()
	if gotPreserved != preserved || activated == nil || gotPeer != peer || starter.callCount() != 3 {
		t.Fatalf("mixed activation published wrong owners calls=%d", starter.callCount())
	}
	preserved.mu.RLock()
	if preserved.graph != preservedGraph || !reflect.DeepEqual(
		preserved.config, sourceGraphMust(t, mustMaterializePlan(t, catalog, applied), "camera-a"),
	) {
		preserved.mu.RUnlock()
		t.Fatal("running source did not retain its owner with the candidate graph")
	}
	preserved.mu.RUnlock()
	preservedRuntime.mu.Lock()
	if brokerChildPID(preservedRuntime.childCmd) != preservedPID ||
		preservedRuntime.mediaEpoch != preservedEpoch || preservedRuntime.plan.LiveEnabled ||
		!preservedRuntime.plan.AnalysisEnabled || !preservedRuntime.plan.ArchiveEnabled {
		preservedRuntime.mu.Unlock()
		t.Fatal("mixed activation changed the preserved capture identity or remaining roles")
	}
	if !preservedRuntime.archivePublication.Owns(current.Streams["archive"].Socket) ||
		!preservedRuntime.subPublication.Owns(current.Streams["analysis"].Socket) ||
		!preservedRuntime.subShared.Owns(current.Streams["analysis"].SharedMemory) {
		preservedRuntime.mu.Unlock()
		t.Fatal("mixed activation disturbed an unchanged publication")
	}
	preservedRuntime.mu.Unlock()
	activated.mu.RLock()
	activatedRuntime := activated.graph.runtime
	activated.mu.RUnlock()
	activatedRuntime.mu.Lock()
	if !activatedRuntime.plan.LiveEnabled || activatedRuntime.plan.AnalysisEnabled ||
		activatedRuntime.plan.ArchiveEnabled ||
		!activatedRuntime.mainPublication.Owns(current.Streams["live"].Socket) {
		activatedRuntime.mu.Unlock()
		t.Fatal("new source did not own exactly the moved live stream")
	}
	activatedRuntime.mu.Unlock()
	peer.mu.RLock()
	if peer.graph != peerGraph {
		peer.mu.RUnlock()
		t.Fatal("mixed activation changed an unrelated peer")
	}
	peer.mu.RUnlock()
	select {
	case <-preserved.done:
		t.Fatal("mixed activation stopped the preserved capture owner")
	default:
	}
	if got := backend.callSequence(); !reflect.DeepEqual(got, []string{
		"validate", "prepare", "warmup", "admission", "first_frame", "cutover", "verify", "retire",
	}) {
		t.Fatalf("mixed activation phase sequence=%v", got)
	}
}

func TestApplyGraphRejectsAbsentOrOccupiedCandidateWithoutChangingCurrentGraph(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "absent", err: os.ErrNotExist},
		{name: "occupied", err: syscall.EBUSY},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			catalog := wholeSourceMoveCatalog(t, t.TempDir())
			initial, err := routing.DefaultPlan(catalog)
			if err != nil {
				t.Fatal(err)
			}
			current, err := routing.Materialize(catalog, initial)
			if err != nil {
				t.Fatal(err)
			}
			starter := newSourceMoveTestStarter(t)
			var failureMu sync.Mutex
			var candidateFailure error
			start := func(
				graphCtx context.Context,
				graphConfig productconfig.SourceGraph,
				profiles []productconfig.SourceRoleProfile,
				inventory capability.Inventory,
				allocator *capabilityAllocator,
			) (*graphService, error) {
				failureMu.Lock()
				failure := candidateFailure
				failureMu.Unlock()
				if graphConfig.SourceID == "camera-b" && failure != nil {
					return nil, failure
				}
				return starter.start(graphCtx, graphConfig, profiles, inventory, allocator)
			}
			service, err := startServiceWithRestartPolicy(
				ctx, current, capability.Inventory{Schema: capability.Schema}, start,
				sourceRestartPolicy{InitialDelay: time.Second, MaxDelay: time.Second, StableWindow: time.Second},
			)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cancel()
				if err := service.Wait(context.Background()); err != nil {
					t.Errorf("source manager shutdown: %v", err)
				}
			}()
			service.mu.RLock()
			beforeOwners := make(map[string]*sourceSupervisor, len(service.sources))
			for sourceID, owner := range service.sources {
				beforeOwners[sourceID] = owner
			}
			service.mu.RUnlock()
			beforeIdentities := captureOwnerIdentities(t, beforeOwners)

			failureMu.Lock()
			candidateFailure = test.err
			failureMu.Unlock()
			store, err := routingTestStore(t)
			if err != nil {
				t.Fatal(err)
			}
			selections := map[string]string{
				"live": "camera-b", "analysis": "camera-b", "archive": "camera-b", "peer_live": "fixed",
			}
			_, applyErr := routing.ApplyGraph(
				ctx, catalog, store, initial.Revision, selections, NewRoutingRuntime(service))
			if applyErr == nil || !errors.Is(applyErr, test.err) {
				t.Fatalf("%s candidate was not rejected causally: %v", test.name, applyErr)
			}
			persisted, err := store.Read(catalog)
			if err != nil || !reflect.DeepEqual(persisted, initial) {
				t.Fatalf("%s candidate changed persisted plan: got=%+v err=%v", test.name, persisted, err)
			}
			service.mu.RLock()
			if len(service.sources) != len(beforeOwners) || service.sources["camera-b"] != nil {
				service.mu.RUnlock()
				t.Fatalf("%s candidate leaked into active source ownership", test.name)
			}
			for sourceID, owner := range beforeOwners {
				if service.sources[sourceID] != owner {
					service.mu.RUnlock()
					t.Fatalf("%s candidate replaced healthy source %q", test.name, sourceID)
				}
			}
			service.mu.RUnlock()
			afterIdentities := captureOwnerIdentities(t, beforeOwners)
			for sourceID, before := range beforeIdentities {
				after := afterIdentities[sourceID]
				if after.Graph != before.Graph || after.Runtime != before.Runtime ||
					after.ChildPID != before.ChildPID || after.MediaEpoch != before.MediaEpoch {
					t.Fatalf("%s candidate changed healthy source %q identity: before=%+v after=%+v",
						test.name, sourceID, before, after)
				}
			}
		})
	}
}

func mustMaterializePlan(t *testing.T, catalog routing.Catalog, plan routing.Plan) productconfig.Config {
	t.Helper()
	cfg, err := routing.Materialize(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestMixedSourceActivationRollbackRestoresIncumbent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	catalog := wholeSourceMoveCatalog(t, t.TempDir())
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	starter := newSourceMoveTestStarter(t)
	service, err := startServiceWithRestartPolicy(
		ctx, current, capability.Inventory{Schema: capability.Schema}, starter.start,
		sourceRestartPolicy{InitialDelay: time.Second, MaxDelay: time.Second, StableWindow: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		if err := service.Wait(context.Background()); err != nil {
			t.Errorf("source manager shutdown: %v", err)
		}
	}()
	backend := &fanoutBackendRecorder{}
	service.fanout = backend
	service.mu.RLock()
	preserved := service.sources["camera-a"]
	service.mu.RUnlock()
	preserved.mu.RLock()
	preservedRuntime := preserved.graph.runtime
	preserved.mu.RUnlock()
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	selections := map[string]string{
		"live": "camera-b", "analysis": "camera-a", "archive": "camera-a", "peer_live": "fixed",
	}
	preview, err := routing.PreviewGraph(catalog, store, initial.Revision, selections)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := routing.Materialize(catalog, preview.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := NewRoutingRuntime(service).PrepareGraph(
		ctx, current, candidate, initial, preview.Candidate, delta)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []func(context.Context) error{
		prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame, prepared.Cutover, prepared.Verify,
	} {
		if err := phase(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := prepared.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	service.mu.RLock()
	incumbent := service.sources["camera-a"]
	activated := service.sources["camera-b"]
	service.mu.RUnlock()
	if incumbent != preserved || activated != nil {
		t.Fatal("mixed rollback did not restore source ownership")
	}
	preservedRuntime.mu.Lock()
	if !fanoutRuntimePlanMatchesGraph(preservedRuntime.plan, sourceGraphMust(t, current, "camera-a")) ||
		!preservedRuntime.mainPublication.Owns(current.Streams["live"].Socket) ||
		!preservedRuntime.archivePublication.Owns(current.Streams["archive"].Socket) ||
		!preservedRuntime.subShared.Owns(current.Streams["analysis"].SharedMemory) {
		preservedRuntime.mu.Unlock()
		t.Fatal("mixed rollback did not restore the incumbent plan and endpoints")
	}
	preservedRuntime.mu.Unlock()
	if got := backend.callSequence(); !reflect.DeepEqual(got, []string{
		"prepare", "warmup", "admission", "first_frame", "cutover", "verify", "rollback",
	}) {
		t.Fatalf("mixed rollback phase sequence=%v", got)
	}
}

func TestWholeSourceMovePartialCutoverRestoresIncumbentEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	catalog := wholeSourceMoveCatalog(t, t.TempDir())
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	starter := newSourceMoveTestStarter(t)
	service, err := startServiceWithRestartPolicy(
		ctx, current, capability.Inventory{Schema: capability.Schema}, starter.start,
		sourceRestartPolicy{InitialDelay: time.Second, MaxDelay: time.Second, StableWindow: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		if err := service.Wait(context.Background()); err != nil {
			t.Errorf("source manager shutdown: %v", err)
		}
	}()
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	selections := map[string]string{
		"live": "camera-b", "analysis": "camera-b", "archive": "camera-b", "peer_live": "fixed",
	}
	preview, err := routing.PreviewGraph(catalog, store, initial.Revision, selections)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := routing.Materialize(catalog, preview.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	preparedValue, err := NewRoutingRuntime(service).PrepareGraph(
		ctx, current, candidate, initial, preview.Candidate, delta,
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared := preparedValue.(*sourceMovePreparedGraph)
	if err := prepared.Warmup(ctx); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Admit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := prepared.AwaitFirstFrame(ctx); err != nil {
		t.Fatal(err)
	}
	prepared.candidateRuntime.archivePublication.Cleanup()
	cutoverErr := prepared.Cutover(ctx)
	if cutoverErr == nil || !strings.Contains(cutoverErr.Error(), "archive publication") {
		_ = prepared.Rollback(context.Background())
		t.Fatalf("missing candidate archive endpoint did not fail cutover: %v", cutoverErr)
	}
	if err := prepared.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Rollback(ctx); err != nil {
		t.Fatalf("source rollback is not idempotent: %v", err)
	}
	service.mu.RLock()
	incumbent := service.sources["camera-a"]
	candidateOwner := service.sources["camera-b"]
	service.mu.RUnlock()
	if incumbent == nil || candidateOwner != nil {
		t.Fatal("partial cutover changed source ownership")
	}
	incumbent.mu.RLock()
	oldRuntime := incumbent.graph.runtime
	incumbent.mu.RUnlock()
	oldRuntime.mu.Lock()
	if !oldRuntime.mainPublication.Owns(current.Streams["live"].Socket) ||
		!oldRuntime.archivePublication.Owns(current.Streams["archive"].Socket) ||
		!oldRuntime.subPublication.Owns(current.Streams["analysis"].Socket) ||
		!oldRuntime.subShared.Owns(current.Streams["analysis"].SharedMemory) {
		oldRuntime.mu.Unlock()
		t.Fatal("rollback did not restore every incumbent endpoint")
	}
	oldRuntime.mu.Unlock()
	select {
	case <-prepared.candidateGraph.done:
	case <-time.After(time.Second):
		t.Fatal("rolled-back candidate graph did not stop")
	}
}

type sourceMoveTestStarter struct {
	t       *testing.T
	mu      sync.Mutex
	nextPID int
	calls   int
}

func newSourceMoveTestStarter(t *testing.T) *sourceMoveTestStarter {
	return &sourceMoveTestStarter{t: t, nextPID: 42000}
}

func (starter *sourceMoveTestStarter) callCount() int {
	starter.mu.Lock()
	defer starter.mu.Unlock()
	return starter.calls
}

func (starter *sourceMoveTestStarter) start(
	ctx context.Context,
	graphConfig productconfig.SourceGraph,
	_ []productconfig.SourceRoleProfile,
	_ capability.Inventory,
	allocator *capabilityAllocator,
) (*graphService, error) {
	starter.mu.Lock()
	starter.calls++
	starter.nextPID++
	pid := starter.nextPID
	starter.mu.Unlock()
	plan := sourceMoveTestPlan(graphConfig, allocator)
	var publications []*unixsocket.Publication
	cleanup := func() {
		for _, publication := range publications {
			publication.Cleanup()
		}
	}
	listen := func(enabled bool, path string) (*unixsocket.Publication, error) {
		if !enabled {
			return nil, nil
		}
		publication, err := unixsocket.Listen(path)
		if err == nil {
			publications = append(publications, publication)
		}
		return publication, err
	}
	mainPublication, err := listen(plan.LiveEnabled, plan.MainSocketPath)
	if err != nil {
		cleanup()
		return nil, err
	}
	archivePublication, err := listen(plan.ArchiveEnabled, plan.ArchiveSocketPath)
	if err != nil {
		cleanup()
		return nil, err
	}
	subPublication, err := listen(plan.AnalysisEnabled && plan.SubSocketPath != "", plan.SubSocketPath)
	if err != nil {
		cleanup()
		return nil, err
	}
	var shared *brokerSubSharedFrame
	if plan.AnalysisEnabled && plan.SubShmPath != "" {
		shared, err = newBrokerSubSharedFrame(plan.SubShmPath, plan.SubWidth*plan.SubHeight*3/2)
		if err != nil {
			cleanup()
			return nil, err
		}
	}
	now := time.Now()
	runtime := &videoBrokerRuntime{
		plan: plan, childRunning: true, childCmd: &exec.Cmd{Process: &os.Process{Pid: pid}},
		mediaEpoch: uint64(pid), cameraAdmitted: true,
		mainClients: map[int]*brokerClient{}, archiveClients: map[int]*brokerClient{}, subClients: map[int]*brokerClient{},
		mainPublication: mainPublication, archivePublication: archivePublication, subPublication: subPublication,
		subShared: shared, subNotifier: newBrokerSubFrameNotifier(plan.SubNotifySocketPath),
		capabilityPlan: sourceMoveTestCapabilityPlan(plan), observedCaptureFPS: plan.CaptureFPS,
		observedArchiveCodec: plan.ArchiveCodec, observedLiveEpoch: 1,
		createdAt: now, childStartedAt: now, lastFrameAt: now,
		componentRecovery: map[string]hardwareComponentRecovery{},
	}
	if plan.LiveEnabled {
		runtime.mainBootstrap = routingH265Bootstrap(starter.t, plan.MainWidth, plan.MainHeight)
	}
	if plan.ArchiveEnabled {
		runtime.archiveBootstrap = routingH265Bootstrap(starter.t, plan.ArchiveWidth, plan.ArchiveHeight)
	}
	done := make(chan error, 1)
	graph := &graphService{
		sourceID: graphConfig.SourceID, runtime: runtime, done: done, plan: plan,
	}
	if graphConfig.Live != nil {
		graph.liveStreamID = graphConfig.Live.ID
	}
	if graphConfig.Analysis != nil {
		graph.analysisStreamID = graphConfig.Analysis.ID
	}
	if graphConfig.Archive != nil {
		graph.archiveStreamID = graphConfig.Archive.ID
	}
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				runtime.mu.Lock()
				runtime.childRunning = false
				runtime.mu.Unlock()
				cleanup()
				if shared != nil {
					_ = shared.Close()
				}
				done <- ctx.Err()
				return
			case at := <-ticker.C:
				runtime.mu.Lock()
				if plan.LiveEnabled {
					runtime.totalMainFrames++
					runtime.lastMainFrameAt = at
				}
				if plan.ArchiveEnabled {
					runtime.totalArchiveFrames++
					runtime.lastArchiveFrameAt = at
				}
				if plan.AnalysisEnabled {
					runtime.totalSubFrames++
					runtime.lastSubFrameAt = at
				}
				runtime.lastFrameAt = at
				runtime.mu.Unlock()
			}
		}
	}()
	return graph, nil
}

func sourceMoveTestPlan(graph productconfig.SourceGraph, allocator *capabilityAllocator) videoBrokerPlan {
	plan := videoBrokerPlan{
		SourceID: graph.SourceID, SourceIdentity: graph.Source.Identity, CapabilityAllocator: allocator,
		Enabled: true, LiveEnabled: graph.Live != nil, AnalysisEnabled: graph.Analysis != nil,
		ArchiveEnabled: graph.Archive != nil, Transport: graph.Source.Transport,
		CaptureDevice: graph.Source.Device, CaptureFormat: graph.Source.Format,
		CaptureWidth: graph.Source.Width, CaptureHeight: graph.Source.Height, CaptureFPS: graph.Source.FPS,
		RequireHWCodec:   graph.Source.Acceleration.RequireHardwareCodec,
		FrameWatchdogSec: 12,
	}
	if graph.Live != nil {
		stream := graph.Live.Config
		plan.MainSocketPath, plan.MainFormat = stream.Socket, stream.Codec
		plan.MainWidth, plan.MainHeight, plan.MainFPS = stream.Width, stream.Height, stream.FPS
		plan.LiveBitrateKbps = stream.BitrateKbps
	}
	if graph.Analysis != nil {
		stream := graph.Analysis.Config
		plan.SubSocketPath, plan.SubShmPath, plan.SubNotifySocketPath = stream.Socket, stream.SharedMemory, stream.NotifySocket
		plan.SubFormat, plan.SubWidth, plan.SubHeight, plan.SubFPS = stream.PixelFormat, stream.Width, stream.Height, stream.FPS
	}
	if graph.Archive != nil {
		stream := graph.Archive.Config
		plan.ArchiveSocketPath, plan.ArchiveCodec = stream.Socket, stream.Codec
		plan.ArchiveWidth, plan.ArchiveHeight, plan.ArchiveFPS = stream.Width, stream.Height, stream.FPS
		plan.ArchiveBitrateKbps, plan.ArchiveRateControl = stream.BitrateKbps, stream.RateControl
	}
	return plan
}

func sourceMoveTestCapabilityPlan(plan videoBrokerPlan) capability.Plan {
	stages := []capability.StageSelection{{
		Stage: capability.StageDecode, Role: "capture", Format: "mjpeg",
		Component: "test-jpu", Backend: "native_sp", Hardware: true, State: capability.StateAttested,
	}}
	if plan.LiveEnabled {
		stages = append(stages, capability.StageSelection{
			Stage: capability.StageEncode, Role: "live", Format: plan.MainFormat,
			Component: "test-vpu", Backend: "native_sp", Hardware: true, State: capability.StateAttested,
		})
	}
	if plan.ArchiveEnabled {
		stages = append(stages, capability.StageSelection{
			Stage: capability.StageEncode, Role: "archive", Format: plan.ArchiveCodec,
			Component: "test-vpu", Backend: "native_sp", Hardware: true, State: capability.StateAttested,
		})
	}
	return capability.Plan{Stages: stages}
}

func assertSourceMovePublications(t *testing.T, runtime *videoBrokerRuntime, current productconfig.Config) {
	t.Helper()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.plan.SourceID != "camera-b" || runtime.plan.MainFPS != 15 || runtime.plan.SubFPS != 20 ||
		runtime.plan.ArchiveFPS != 15 || !runtime.mainPublication.Owns(current.Streams["live"].Socket) ||
		!runtime.archivePublication.Owns(current.Streams["archive"].Socket) ||
		!runtime.subPublication.Owns(current.Streams["analysis"].Socket) ||
		!runtime.subShared.Owns(current.Streams["analysis"].SharedMemory) ||
		!brokerMainBootstrapReady("h265", runtime.mainBootstrap) ||
		!brokerMainBootstrapReady("h265", runtime.archiveBootstrap) {
		t.Fatalf("candidate publications/profile/bootstrap are incomplete: plan=%+v", runtime.plan)
	}
}

func wholeSourceMoveCatalog(t *testing.T, directory string) routing.Catalog {
	t.Helper()
	source := func(identity, device string) productconfig.SourceConfig {
		return productconfig.SourceConfig{
			Identity: identity, Transport: "v4l2_uvc", Device: device, Format: "mjpeg",
			Width: 1280, Height: 720, FPS: 30,
			Acceleration: productconfig.AccelerationConfig{
				EncodeProvider: "native_sp", DecodeProvider: "native_sp",
				RequireHardwareCodec: true, AllowSoftwareFallback: false,
			},
		}
	}
	option := func(sourceID, codec, pixel string, width, height, fps, bitrate int) routing.StreamOption {
		return routing.StreamOption{
			SourceRef: sourceID, Codec: codec, PixelFormat: pixel,
			Width: width, Height: height, FPS: fps, BitrateKbps: bitrate,
		}
	}
	catalog := routing.Catalog{
		Schema: routing.CatalogSchema, Revision: "sha256:" + strings.Repeat("b", 64),
		Sources: map[string]productconfig.SourceConfig{
			"camera-a":    source("camera-a", "/dev/video-a"),
			"camera-b":    source("camera-b", "/dev/video-b"),
			"camera-peer": source("camera-peer", "/dev/video-peer"),
		},
		Streams: map[string]routing.StreamCatalog{
			"live": {
				Type: "live", Socket: filepath.Join(directory, "live.sock"),
				Options: map[string]routing.StreamOption{
					"camera-a": option("camera-a", "h265", "", 1280, 720, 20, 6144),
					"camera-b": option("camera-b", "h265", "", 1280, 720, 15, 4096),
				},
			},
			"analysis": {
				Type: "analysis", Socket: filepath.Join(directory, "analysis.sock"),
				SharedMemory: filepath.Join(directory, "analysis.nv12"),
				NotifySocket: filepath.Join(directory, "analysis.notify"),
				Options: map[string]routing.StreamOption{
					"camera-a": option("camera-a", "", "nv12", 640, 360, 15, 0),
					"camera-b": option("camera-b", "", "nv12", 640, 360, 20, 0),
				},
			},
			"archive": {
				Type: "archive", Socket: filepath.Join(directory, "archive.sock"),
				Options: map[string]routing.StreamOption{
					"camera-a": option("camera-a", "h265", "", 640, 360, 10, 750),
					"camera-b": option("camera-b", "h265", "", 640, 360, 15, 750),
				},
			},
			"peer_live": {
				Type: "live", Socket: filepath.Join(directory, "peer-live.sock"),
				Options: map[string]routing.StreamOption{
					"fixed": option("camera-peer", "h265", "", 640, 360, 10, 2048),
				},
			},
		},
		DefaultSelections: map[string]string{
			"live": "camera-a", "analysis": "camera-a", "archive": "camera-a", "peer_live": "fixed",
		},
		Runtime: productconfig.RuntimeConfig{
			StateDirectory: filepath.Join(directory, "state"), RuntimeDirectory: directory,
		},
	}
	normalized, err := routing.NormalizeCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	return normalized
}

func TestApplyGraphPersistsLiveProfileOnlyAfterVerifiedCutover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	current, candidate, _, service, supervisor, runtime := liveProfileRoutingFixture(t)
	catalog := liveProfileRoutingCatalog(t, current, candidate)
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}

	type applyResult struct {
		plan routing.Plan
		err  error
	}
	result := make(chan applyResult, 1)
	go func() {
		plan, applyErr := routing.ApplyGraph(
			ctx, catalog, store, initial.Revision,
			map[string]string{"analysis": "standard", "archive": "standard", "live": "economy"},
			NewRoutingRuntime(service),
		)
		result <- applyResult{plan: plan, err: applyErr}
	}()

	warmup := nextRoutingControl(t, runtime)
	publishRoutingReady(t, runtime, warmup, 6)
	cutover := nextRoutingControl(t, runtime)
	if cutover.MainFPS != 15 || cutover.MainBitrateKbps != 3072 {
		t.Fatalf("candidate targets were not queued: %+v", cutover)
	}
	publishRoutingReady(t, runtime, cutover, 6)

	applied := <-result
	if applied.err != nil {
		t.Fatal(applied.err)
	}
	if applied.plan.Revision != 2 || applied.plan.Selections["live"] != "economy" ||
		applied.plan.StreamGenerations["live"] != 2 {
		t.Fatalf("unexpected active plan: %+v", applied.plan)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, applied.plan) {
		t.Fatalf("persisted plan differs from verified plan: got=%+v want=%+v", persisted, applied.plan)
	}
	service.mu.RLock()
	gotActive := productconfig.Clone(service.config)
	service.mu.RUnlock()
	if !reflect.DeepEqual(gotActive, candidate) {
		t.Fatal("stream governor routing authority did not follow live profile cutover")
	}
	if supervisor.config.Live.Config.FPS != 15 || supervisor.config.Live.Config.BitrateKbps != 3072 {
		t.Fatalf("retired supervisor graph does not match active plan: %+v", supervisor.config.Live.Config)
	}
	runtime.mu.Lock()
	gotFPS, gotBitrate := runtime.plan.MainFPS, runtime.plan.LiveBitrateKbps
	gotMediaEpoch, gotChildPID := runtime.mediaEpoch, brokerChildPID(runtime.childCmd)
	runtime.mu.Unlock()
	if gotFPS != 15 || gotBitrate != 3072 || gotMediaEpoch != 4 || gotChildPID != 31337 {
		t.Fatalf(
			"verified apply violated capture identity fps=%d bitrate=%d media_epoch=%d child_pid=%d",
			gotFPS, gotBitrate, gotMediaEpoch, gotChildPID)
	}
}

func TestApplyGraphReadinessMismatchRollsBackWithoutPersistence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	current, candidate, _, service, supervisor, runtime := liveProfileRoutingFixture(t)
	catalog := liveProfileRoutingCatalog(t, current, candidate)
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}

	type applyResult struct {
		plan routing.Plan
		err  error
	}
	result := make(chan applyResult, 1)
	go func() {
		plan, applyErr := routing.ApplyGraph(
			ctx, catalog, store, initial.Revision,
			map[string]string{"analysis": "standard", "archive": "standard", "live": "economy"},
			NewRoutingRuntime(service),
		)
		result <- applyResult{plan: plan, err: applyErr}
	}()

	warmup := nextRoutingControl(t, runtime)
	publishRoutingReady(t, runtime, warmup, 6)
	cutover := nextRoutingControl(t, runtime)
	mismatched := cutover
	mismatched.MainBitrateKbps++
	publishRoutingReady(t, runtime, mismatched, 6)
	rollback := nextRoutingControl(t, runtime)
	if rollback.MainFPS != 20 || rollback.MainBitrateKbps != 6144 {
		t.Fatalf("current profile was not queued for rollback: %+v", rollback)
	}
	publishRoutingReady(t, runtime, rollback, 6)

	failed := <-result
	if failed.err == nil {
		t.Fatal("readiness mismatch did not fail the apply transaction")
	}
	var applyErr *routing.ApplyError
	if !errors.As(failed.err, &applyErr) || applyErr.Phase != routing.ApplyPhaseVerify {
		t.Fatalf("unexpected apply error: %v", failed.err)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, initial) {
		t.Fatalf("failed candidate was persisted: got=%+v want=%+v", persisted, initial)
	}
	service.mu.RLock()
	gotActive := productconfig.Clone(service.config)
	service.mu.RUnlock()
	if !reflect.DeepEqual(gotActive, current) {
		t.Fatal("stream governor routing authority did not roll back with the failed profile")
	}
	if supervisor.config.Live.Config.FPS != 20 || supervisor.config.Live.Config.BitrateKbps != 6144 {
		t.Fatalf("failed candidate reached supervisor config: %+v", supervisor.config.Live.Config)
	}
	runtime.mu.Lock()
	gotFPS, gotBitrate := runtime.plan.MainFPS, runtime.plan.LiveBitrateKbps
	gotMediaEpoch, gotChildPID := runtime.mediaEpoch, brokerChildPID(runtime.childCmd)
	runtime.mu.Unlock()
	if gotFPS != 20 || gotBitrate != 6144 || gotMediaEpoch != 4 || gotChildPID != 31337 {
		t.Fatalf(
			"rollback violated current graph fps=%d bitrate=%d media_epoch=%d child_pid=%d",
			gotFPS, gotBitrate, gotMediaEpoch, gotChildPID)
	}
}

func TestLiveProfileRoutingPreservesCaptureAndPersistsOnlyAfterRetire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	current, candidate, delta, service, supervisor, runtime := liveProfileRoutingFixture(t)
	preparedValue, err := NewRoutingRuntime(service).PrepareGraph(
		ctx, current, candidate, routing.Plan{}, routing.Plan{}, delta)
	if err != nil {
		t.Fatal(err)
	}
	prepared := preparedValue.(*liveProfilePreparedGraph)
	if err := prepared.Warmup(ctx); err != nil {
		t.Fatal(err)
	}
	warmup := nextRoutingControl(t, runtime)
	if !warmup.MainActive || warmup.MainFPS != 20 || warmup.MainBitrateKbps != 6144 {
		t.Fatalf("unexpected warmup control: %+v", warmup)
	}
	publishRoutingReady(t, runtime, warmup, 6)
	if err := prepared.Admit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := prepared.AwaitFirstFrame(ctx); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Cutover(ctx); err != nil {
		t.Fatal(err)
	}
	cutover := nextRoutingControl(t, runtime)
	if cutover.MainFPS != 15 || cutover.MainBitrateKbps != 3072 {
		t.Fatalf("candidate targets were not queued: %+v", cutover)
	}
	publishRoutingReady(t, runtime, cutover, 6)
	if err := prepared.Verify(ctx); err != nil {
		t.Fatal(err)
	}

	if supervisor.config.Live.Config.FPS != 20 {
		t.Fatal("supervisor config changed before persistent activation")
	}
	if err := prepared.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	gotFPS := runtime.plan.MainFPS
	gotBitrate := runtime.plan.LiveBitrateKbps
	gotMediaEpoch := runtime.mediaEpoch
	gotChildPID := brokerChildPID(runtime.childCmd)
	demand := runtime.routingLiveDemand
	bootstrapReady := brokerMainBootstrapReady(runtime.plan.MainFormat, runtime.mainBootstrap)
	runtime.mu.Unlock()
	if gotFPS != 15 || gotBitrate != 3072 || gotMediaEpoch != 4 || gotChildPID != 31337 ||
		demand || !bootstrapReady {
		t.Fatalf(
			"retired profile violated runtime invariants fps=%d bitrate=%d media_epoch=%d child_pid=%d demand=%t bootstrap=%t",
			gotFPS, gotBitrate, gotMediaEpoch, gotChildPID, demand, bootstrapReady)
	}
	if supervisor.config.Live.Config.FPS != 15 || supervisor.config.Live.Config.BitrateKbps != 3072 {
		t.Fatalf("candidate supervisor config was not retired: %+v", supervisor.config.Live.Config)
	}
}

func TestLiveProfileRoutingRollbackRestoresRuntimeAndIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	current, candidate, delta, service, supervisor, runtime := liveProfileRoutingFixture(t)
	preparedValue, err := NewRoutingRuntime(service).PrepareGraph(
		ctx, current, candidate, routing.Plan{}, routing.Plan{}, delta)
	if err != nil {
		t.Fatal(err)
	}
	prepared := preparedValue.(*liveProfilePreparedGraph)
	if err := prepared.Warmup(ctx); err != nil {
		t.Fatal(err)
	}
	warmup := nextRoutingControl(t, runtime)
	publishRoutingReady(t, runtime, warmup, 6)
	if err := prepared.Admit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := prepared.AwaitFirstFrame(ctx); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Cutover(ctx); err != nil {
		t.Fatal(err)
	}
	cutover := nextRoutingControl(t, runtime)
	publishRoutingReady(t, runtime, cutover, 6)
	if err := prepared.Verify(ctx); err != nil {
		t.Fatal(err)
	}

	rollbackDone := make(chan error, 1)
	go func() { rollbackDone <- prepared.Rollback(ctx) }()
	rollback := nextRoutingControl(t, runtime)
	if rollback.MainFPS != 20 || rollback.MainBitrateKbps != 6144 {
		t.Fatalf("rollback targets are not the current profile: %+v", rollback)
	}
	publishRoutingReady(t, runtime, rollback, 6)
	if err := <-rollbackDone; err != nil {
		t.Fatal(err)
	}
	if err := prepared.Rollback(ctx); err != nil {
		t.Fatalf("idempotent rollback failed: %v", err)
	}
	runtime.mu.Lock()
	gotFPS := runtime.plan.MainFPS
	gotBitrate := runtime.plan.LiveBitrateKbps
	gotMediaEpoch := runtime.mediaEpoch
	gotChildPID := brokerChildPID(runtime.childCmd)
	demand := runtime.routingLiveDemand
	runtime.mu.Unlock()
	if gotFPS != 20 || gotBitrate != 6144 || gotMediaEpoch != 4 || gotChildPID != 31337 || demand {
		t.Fatalf(
			"rollback did not restore runtime fps=%d bitrate=%d media_epoch=%d child_pid=%d demand=%t",
			gotFPS, gotBitrate, gotMediaEpoch, gotChildPID, demand)
	}
	if supervisor.config.Live.Config.FPS != 20 {
		t.Fatal("rollback changed the supervisor's persistent current graph")
	}
}

func TestLiveProfileRoutingRejectsTopologyAndGeometryChanges(t *testing.T) {
	current, _, _, _, _, _ := liveProfileRoutingFixture(t)
	for name, mutate := range map[string]func(*productconfig.Config){
		"geometry": func(candidate *productconfig.Config) {
			stream := candidate.Streams["live"]
			stream.Width = 960
			candidate.Streams["live"] = stream
		},
		"source": func(candidate *productconfig.Config) {
			stream := candidate.Streams["live"]
			stream.SourceRef = "analysis-camera"
			candidate.Streams["live"] = stream
			delete(candidate.Sources, "live-camera")
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := productconfig.Clone(current)
			mutate(&candidate)
			candidate, err := productconfig.Normalize(candidate)
			if err != nil {
				t.Fatal(err)
			}
			delta, err := routing.DiffConfigs(current, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, _, err := validateInPlaceLiveProfileChange(current, candidate, delta); err == nil {
				t.Fatal("unsupported runtime change was accepted")
			}
		})
	}
}

func TestLiveProfileChangePreservesExplicitConsumerCaps(t *testing.T) {
	runtime := &videoBrokerRuntime{
		plan: videoBrokerPlan{MainFPS: 30, LiveBitrateKbps: 8192},
		mainClients: map[int]*brokerClient{
			1: {mainControl: videoMainClientControl{TargetFPS: 30, TargetBitrateKbps: 8192}},
			2: {mainControl: videoMainClientControl{TargetFPS: 10, TargetBitrateKbps: 2048}, mainControlUser: true},
			3: {mainControl: videoMainClientControl{TargetFPS: 25, TargetBitrateKbps: 7000}, mainControlUser: true},
		},
		governor: nominalResourceGovernorDecision(videoBrokerPlan{MainFPS: 30, LiveBitrateKbps: 8192}),
	}
	beforeGovernor := cloneResourceGovernorDecision(runtime.governor)
	beforeClients := snapshotLiveProfileClientsLocked(runtime)
	applyLiveProfileLocked(runtime, productconfig.StreamConfig{FPS: 20, BitrateKbps: 4096})
	if got := runtime.mainClients[1].mainControl; got.TargetFPS != 20 || got.TargetBitrateKbps != 4096 {
		t.Fatalf("default consumer did not follow profile: %+v", got)
	}
	if got := runtime.mainClients[2].mainControl; got.TargetFPS != 10 || got.TargetBitrateKbps != 2048 {
		t.Fatalf("explicit lower consumer cap was overwritten: %+v", got)
	}
	if got := runtime.mainClients[3].mainControl; got.TargetFPS != 20 || got.TargetBitrateKbps != 4096 {
		t.Fatalf("explicit consumer cap was not bounded by profile: %+v", got)
	}
	restoreLiveProfileLocked(
		runtime, productconfig.StreamConfig{FPS: 30, BitrateKbps: 8192}, beforeGovernor, beforeClients)
	for clientID, want := range beforeClients {
		client := runtime.mainClients[clientID]
		if client.mainControl != want.control || client.mainControlUser != want.user {
			t.Fatalf("consumer %d was not restored: got=%+v want=%+v", clientID, client, want)
		}
	}
}

func TestLiveProfileRuntimePreviewMatchesPrepareAdmissionWithoutMutation(t *testing.T) {
	current, candidate, delta, service, supervisor, runtime := liveProfileRoutingFixture(t)
	validator, ok := NewRoutingRuntime(service).(routing.GraphValidator)
	if !ok {
		t.Fatal("routing runtime has no read-only validator")
	}
	if err := validator.ValidateGraph(
		context.Background(), current, candidate, routing.Plan{}, routing.Plan{}, delta,
	); err != nil {
		t.Fatal(err)
	}
	if supervisor.config.Live.Config.FPS != 20 || runtime.plan.MainFPS != 20 || len(runtime.childControl) != 0 {
		t.Fatalf(
			"runtime preview mutated data plane supervisor_fps=%d runtime_fps=%d controls=%d",
			supervisor.config.Live.Config.FPS, runtime.plan.MainFPS, len(runtime.childControl))
	}

	runtime.mu.Lock()
	runtime.observedCaptureFPS = 14
	runtime.mu.Unlock()
	if err := validator.ValidateGraph(
		context.Background(), current, candidate,
		routing.Plan{}, routing.Plan{}, delta,
	); err != nil {
		t.Fatalf("supported candidate was rejected for lower observed cadence: %v", err)
	}
	if supervisor.config.Live.Config.FPS != 20 || runtime.plan.MainFPS != 20 || len(runtime.childControl) != 0 {
		t.Fatal("runtime preview mutated the current graph")
	}
}

func liveProfileRoutingFixture(t *testing.T) (
	productconfig.Config,
	productconfig.Config,
	routing.GraphDelta,
	*Service,
	*sourceSupervisor,
	*videoBrokerRuntime,
) {
	t.Helper()
	current := threeRoleSourceManagerConfig(t)
	live := current.Streams["live"]
	live.Codec = "h265"
	live.FPS = 20
	live.BitrateKbps = 6144
	current.Streams["live"] = live
	liveSource := current.Sources["live-camera"]
	liveSource.Acceleration.RequireHardwareCodec = true
	liveSource.Acceleration.AllowSoftwareFallback = false
	current.Sources["live-camera"] = liveSource
	var err error
	current, err = productconfig.Normalize(current)
	if err != nil {
		t.Fatal(err)
	}
	candidate := productconfig.Clone(current)
	candidateLive := candidate.Streams["live"]
	candidateLive.FPS = 15
	candidateLive.BitrateKbps = 3072
	candidate.Streams["live"] = candidateLive
	candidate, err = productconfig.Normalize(candidate)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	currentGraph, ok := sourceGraph(current, "live-camera")
	if !ok {
		t.Fatal("live graph missing")
	}
	currentLive := current.Streams["live"]
	runtimePlan := videoBrokerPlan{
		SourceID: "live-camera", SourceIdentity: currentGraph.Source.Identity,
		Enabled: true, LiveEnabled: true, MainSocketPath: currentLive.Socket,
		MainFormat: currentLive.Codec, MainWidth: currentLive.Width, MainHeight: currentLive.Height,
		MainFPS: currentLive.FPS, LiveBitrateKbps: currentLive.BitrateKbps,
		CaptureFPS: 30, FrameWatchdogSec: 12, RequireHWCodec: true,
	}
	runtime := &videoBrokerRuntime{
		plan:        runtimePlan,
		mainClients: map[int]*brokerClient{}, archiveClients: map[int]*brokerClient{},
		subClients: map[int]*brokerClient{}, childControl: make(chan bridgeRuntimeControl, 8),
		childControlAvailable: true, childControlSupported: true,
		childLiveControlMode: "persistent_encoder_epoch", childRunning: true,
		childCmd: &exec.Cmd{Process: &os.Process{Pid: 31337}}, mediaEpoch: 4,
		observedLiveEpoch: 5, observedCaptureFPS: 30, cameraAdmitted: true,
		capabilityPlan: capability.Plan{Stages: []capability.StageSelection{{
			Stage: capability.StageEncode, Role: "live", Format: "h265",
			Component: "generic-vpu", Backend: "native_sp", Hardware: true, State: capability.StateAttested,
		}}},
		governor:  nominalResourceGovernorDecision(runtimePlan),
		createdAt: time.Now().Add(-time.Minute), childStartedAt: time.Now().Add(-time.Minute),
	}
	runtime.mainBootstrap = routingH265Bootstrap(t, currentLive.Width, currentLive.Height)
	graph := &graphService{
		sourceID: "live-camera", liveStreamID: "live", runtime: runtime, plan: runtimePlan,
	}
	supervisor := &sourceSupervisor{
		config: currentGraph, graph: graph, state: "running", lastChangeAt: time.Now().UTC(),
	}
	service := &Service{sources: map[string]*sourceSupervisor{"live-camera": supervisor}}
	return current, candidate, delta, service, supervisor, runtime
}

func liveProfileRoutingCatalog(
	t *testing.T,
	current productconfig.Config,
	candidate productconfig.Config,
) routing.Catalog {
	t.Helper()
	catalog := routing.Catalog{
		Schema: routing.CatalogSchema, Revision: "sha256:" + strings.Repeat("a", 64),
		Sources: current.Sources, Runtime: current.Runtime,
		Streams:           make(map[string]routing.StreamCatalog, len(current.Streams)),
		DefaultSelections: make(map[string]string, len(current.Streams)),
	}
	for streamID, stream := range current.Streams {
		options := map[string]routing.StreamOption{"standard": routingOption(stream)}
		if streamID == "live" {
			options["economy"] = routingOption(candidate.Streams[streamID])
		}
		catalog.Streams[streamID] = routing.StreamCatalog{
			Type: stream.Type, Socket: stream.Socket, SharedMemory: stream.SharedMemory,
			NotifySocket: stream.NotifySocket, Options: options,
		}
		catalog.DefaultSelections[streamID] = "standard"
	}
	normalized, err := routing.NormalizeCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := routing.DefaultPlan(normalized)
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := routing.Materialize(normalized, initial)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(materialized, current) {
		t.Fatalf("catalog default does not reproduce current graph:\n got=%+v\nwant=%+v", materialized, current)
	}
	return normalized
}

func routingOption(stream productconfig.StreamConfig) routing.StreamOption {
	return routing.StreamOption{
		SourceRef: stream.SourceRef, Codec: stream.Codec, PixelFormat: stream.PixelFormat,
		Width: stream.Width, Height: stream.Height, FPS: stream.FPS,
		BitrateKbps: stream.BitrateKbps, Content: stream.Content, RateControl: stream.RateControl,
	}
}

func routingTestStore(t *testing.T) (*routing.Store, error) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o750); err != nil {
		return nil, err
	}
	return routing.NewStore(directory)
}

func routingH265Bootstrap(t *testing.T, width, height int) [][]byte {
	t.Helper()
	payload := []byte{
		0, 0, 0, 1, 32 << 1, 1, 0xaa,
		0, 0, 0, 1, 33 << 1, 1, 0xbb,
		0, 0, 0, 1, 34 << 1, 1, 0xcc,
		0, 0, 0, 1, 19 << 1, 1, 0xdd,
	}
	packet := marshalBrokerPacket(brokerPacketHeader{
		StreamID: brokerStreamH265, Flags: brokerFlagCodecConfig | brokerFlagKeyframe,
		FrameID: 1, TimestampNS: uint64(time.Now().UnixNano()),
		Width: uint32(width), Height: uint32(height),
	}, payload)
	if len(packet) == 0 || !brokerMainBootstrapReady("h265", [][]byte{packet}) {
		t.Fatal("could not construct H265 routing bootstrap")
	}
	return [][]byte{packet}
}

func nextRoutingControl(t *testing.T, runtime *videoBrokerRuntime) bridgeRuntimeControl {
	t.Helper()
	select {
	case control := <-runtime.childControl:
		return control
	case <-time.After(time.Second):
		t.Fatal("routing control was not queued")
		return bridgeRuntimeControl{}
	}
}

func publishRoutingReady(
	t *testing.T,
	runtime *videoBrokerRuntime,
	control bridgeRuntimeControl,
	encoderEpoch uint64,
) {
	t.Helper()
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.observedLiveEpoch = encoderEpoch
	if !brokerMainBootstrapReady(runtime.plan.MainFormat, runtime.mainBootstrap) {
		runtime.mainBootstrap = routingH265Bootstrap(t, runtime.plan.MainWidth, runtime.plan.MainHeight)
	}
	runtime.totalMainFrames++
	runtime.lastMainFrameAt = time.Now()
	runtime.lastFrameAt = runtime.lastMainFrameAt
	if !runtime.noteBridgeControlReadyLocked(runtime.mediaEpoch, brokerChildPID(runtime.childCmd),
		brokerBridgeControlReadyTelemetry{
			Sequence: control.Sequence, MainActive: control.MainActive, MainFPS: control.MainFPS,
			MainBitrateKbps: control.MainBitrateKbps, SubFPS: control.SubFPS, EncoderEpoch: encoderEpoch,
		}, time.Now()) {
		t.Fatalf("control readiness was rejected: %+v", control)
	}
}
