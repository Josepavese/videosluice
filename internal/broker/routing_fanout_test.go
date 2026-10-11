package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	productconfig "github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/routing"
)

type fanoutBackendRecorder struct {
	mu             sync.Mutex
	calls          []string
	validated      preservedOwnerFanout
	prepared       preservedOwnerFanout
	cutoverMutator func(map[string]sourceFanoutHandle)
}

func (backend *fanoutBackendRecorder) record(call string) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.calls = append(backend.calls, call)
}

func (backend *fanoutBackendRecorder) callSequence() []string {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return append([]string(nil), backend.calls...)
}

func (backend *fanoutBackendRecorder) ValidateFanout(
	ctx context.Context,
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
) error {
	if ctx == nil || ctx.Err() != nil {
		return errors.New("invalid fan-out validation context")
	}
	if len(handles) != len(transition.SourceIDs) {
		return errors.New("fan-out validation handles are incomplete")
	}
	backend.validated = transition
	backend.record("validate")
	return nil
}

func (backend *fanoutBackendRecorder) PrepareFanout(
	ctx context.Context,
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
) (routing.PreparedGraph, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, errors.New("invalid fan-out prepare context")
	}
	if len(handles) != len(transition.SourceIDs) {
		return nil, errors.New("fan-out prepare handles are incomplete")
	}
	backend.prepared = transition
	backend.record("prepare")
	return &fanoutPreparedRecorder{backend: backend, handles: handles}, nil
}

type fanoutPreparedRecorder struct {
	backend *fanoutBackendRecorder
	handles map[string]sourceFanoutHandle
}

func (prepared *fanoutPreparedRecorder) phase(name string) error {
	prepared.backend.record(name)
	return nil
}

func (prepared *fanoutPreparedRecorder) Warmup(context.Context) error {
	return prepared.phase("warmup")
}

func (prepared *fanoutPreparedRecorder) Admit(context.Context) error {
	return prepared.phase("admission")
}

func (prepared *fanoutPreparedRecorder) AwaitFirstFrame(context.Context) error {
	return prepared.phase("first_frame")
}

func (prepared *fanoutPreparedRecorder) Cutover(context.Context) error {
	prepared.backend.record("cutover")
	for _, handle := range prepared.handles {
		handle.Runtime.mu.Lock()
		handle.Runtime.plan = sourceMoveTestPlan(
			handle.CandidateGraph, handle.Runtime.plan.CapabilityAllocator)
		handle.Runtime.mu.Unlock()
	}
	if prepared.backend.cutoverMutator != nil {
		prepared.backend.cutoverMutator(prepared.handles)
	}
	return nil
}

func (prepared *fanoutPreparedRecorder) Verify(context.Context) error {
	return prepared.phase("verify")
}

func (prepared *fanoutPreparedRecorder) Rollback(context.Context) error {
	prepared.backend.record("rollback")
	for _, handle := range prepared.handles {
		handle.Runtime.mu.Lock()
		handle.Runtime.plan = sourceMoveTestPlan(
			handle.CurrentGraph, handle.Runtime.plan.CapabilityAllocator)
		handle.Runtime.mu.Unlock()
	}
	return nil
}

func (prepared *fanoutPreparedRecorder) Abort(context.Context) error {
	return prepared.phase("abort")
}

func (prepared *fanoutPreparedRecorder) Retire(context.Context) error {
	return prepared.phase("retire")
}

func TestPreservedOwnerFanoutDispatchesForTwoThreeAndFiveSources(t *testing.T) {
	for _, sourceCount := range []int{2, 3, 5} {
		t.Run(fmt.Sprintf("sources_%d", sourceCount), func(t *testing.T) {
			catalog := fanoutRoutingCatalog(t, sourceCount)
			initial, err := routing.DefaultPlan(catalog)
			if err != nil {
				t.Fatal(err)
			}
			current, err := routing.Materialize(catalog, initial)
			if err != nil {
				t.Fatal(err)
			}
			backend := &fanoutBackendRecorder{}
			service, owners := fanoutRoutingService(t, current, backend)
			selections := cloneSelections(initial.Selections)
			selections["live"] = "camera-2"
			store, err := routingTestStore(t)
			if err != nil {
				t.Fatal(err)
			}

			runtime := NewRoutingRuntime(service)
			validator, ok := runtime.(routing.GraphValidator)
			if !ok {
				t.Fatal("fan-out runtime has no preview validator")
			}
			preview, err := routing.PreviewRuntimeGraph(
				context.Background(), catalog, store, initial.Revision, selections, validator)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(preview.ChangedSourceIDs, []string{"camera-1", "camera-2"}) ||
				!reflect.DeepEqual(preview.ChangedStreamIDs, []string{"live"}) {
				t.Fatalf("unexpected fan-out preview: %+v", preview)
			}

			before := captureOwnerIdentities(t, owners)
			applied, err := routing.ApplyGraph(
				context.Background(), catalog, store, initial.Revision, selections, runtime)
			if err != nil {
				t.Fatal(err)
			}
			if applied.Revision != initial.Revision+1 || applied.Selections["live"] != "camera-2" ||
				applied.StreamGenerations["live"] != initial.StreamGenerations["live"]+1 {
				t.Fatalf("fan-out plan was not advanced exactly: %+v", applied)
			}
			for streamID, generation := range initial.StreamGenerations {
				if streamID != "live" && applied.StreamGenerations[streamID] != generation {
					t.Fatalf("unrelated stream %q generation changed", streamID)
				}
			}
			if after := captureOwnerIdentities(t, owners); !reflect.DeepEqual(after, before) {
				t.Fatalf("fan-out changed capture owners: before=%v after=%v", before, after)
			}
			if owners["camera-1"].config.Live != nil || owners["camera-2"].config.Live == nil ||
				owners["camera-1"].graph.liveStreamID != "" || owners["camera-2"].graph.liveStreamID != "live" {
				t.Fatal("retired fan-out role identities do not match the active plan")
			}
			for sourceIndex := 3; sourceIndex <= sourceCount; sourceIndex++ {
				sourceID := fmt.Sprintf("camera-%d", sourceIndex)
				if !reflect.DeepEqual(owners[sourceID].config, sourceGraphMust(t, current, sourceID)) {
					t.Fatalf("peer source %q graph changed", sourceID)
				}
			}
			wantCalls := []string{
				"validate", "prepare", "warmup", "admission", "first_frame",
				"cutover", "verify", "retire",
			}
			if got := backend.callSequence(); !reflect.DeepEqual(got, wantCalls) {
				t.Fatalf("unexpected fan-out phase sequence: got=%v want=%v", got, wantCalls)
			}
		})
	}
}

func TestPreservedOwnerFanoutRetiresSourceThatBecomesEmpty(t *testing.T) {
	catalog := fanoutRoutingCatalog(t, 2)
	archive := catalog.Streams["archive"]
	archive.Options["camera-2"] = routing.StreamOption{
		SourceRef: "camera-2", Codec: "h265",
		Width: 640, Height: 360, FPS: 15, BitrateKbps: 750,
	}
	catalog.Streams["archive"] = archive
	var err error
	catalog, err = routing.NormalizeCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	selections := cloneSelections(initial.Selections)
	selections["live"] = "camera-2"
	selections["archive"] = "camera-2"
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
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
	transition, err := classifyPreservedOwnerFanout(current, candidate, delta)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(transition.RetiringSourceIDs, []string{"camera-1"}) ||
		transition.CandidateGraphs["camera-1"].Live != nil ||
		transition.CandidateGraphs["camera-1"].Archive != nil ||
		transition.CandidateGraphs["camera-1"].Analysis != nil {
		t.Fatalf("empty source retirement was not classified exactly: %+v", transition)
	}

	backend := &fanoutBackendRecorder{}
	service, owners := fanoutRoutingService(t, current, backend)
	retireContext, cancelRetiring := context.WithCancel(context.Background())
	retireDone := make(chan struct{})
	owners["camera-1"].cancel = cancelRetiring
	owners["camera-1"].done = retireDone
	go func() {
		<-retireContext.Done()
		owners["camera-1"].setStopped()
		close(retireDone)
	}()

	applied, err := routing.ApplyGraph(
		context.Background(), catalog, store, initial.Revision, selections,
		NewRoutingRuntime(service))
	if err != nil {
		t.Fatal(err)
	}
	if applied.Revision != initial.Revision+1 {
		t.Fatalf("retiring fan-out plan was not persisted: %+v", applied)
	}
	service.mu.RLock()
	retired := service.sources["camera-1"]
	remaining := service.sources["camera-2"]
	service.mu.RUnlock()
	if remaining == nil {
		t.Fatal("remaining fan-out source disappeared")
	}
	remaining.mu.RLock()
	remainingConfig := remaining.config
	remaining.mu.RUnlock()
	if retired != nil || remaining != owners["camera-2"] ||
		!reflect.DeepEqual(remainingConfig, sourceGraphMust(t, candidate, "camera-2")) {
		t.Fatalf("source retirement did not publish the candidate ownership: retired=%p remaining=%p", retired, remaining)
	}
	if got := backend.callSequence(); !reflect.DeepEqual(got, []string{
		"prepare", "warmup", "admission", "first_frame", "cutover", "verify", "retire",
	}) {
		t.Fatalf("unexpected retirement phase sequence: %v", got)
	}
}

func TestPreservedOwnerFanoutEvacuatesUnavailableRetiringSource(t *testing.T) {
	catalog := fanoutRoutingCatalog(t, 2)
	archive := catalog.Streams["archive"]
	archive.Options["camera-2"] = routing.StreamOption{
		SourceRef: "camera-2", Codec: "h265",
		Width: 640, Height: 360, FPS: 15, BitrateKbps: 750,
	}
	catalog.Streams["archive"] = archive
	var err error
	catalog, err = routing.NormalizeCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	selections := cloneSelections(initial.Selections)
	selections["live"] = "camera-2"
	selections["archive"] = "camera-2"
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fanoutBackendRecorder{}
	service, owners := fanoutRoutingService(t, current, backend)
	retiring := owners["camera-1"]
	retiring.graph.runtime.mu.Lock()
	retiring.graph.runtime.childRunning = false
	retiring.graph.runtime.childCmd = nil
	retiring.graph.runtime.mu.Unlock()
	retireContext, cancelRetiring := context.WithCancel(context.Background())
	retireDone := make(chan struct{})
	retiring.cancel = cancelRetiring
	retiring.done = retireDone
	go func() {
		<-retireContext.Done()
		retiring.setStopped()
		close(retireDone)
	}()

	validator := NewRoutingRuntime(service).(routing.GraphValidator)
	if _, err := routing.PreviewRuntimeGraph(
		context.Background(), catalog, store, initial.Revision, selections, validator,
	); err != nil {
		t.Fatalf("unavailable retiring source blocked preview: %v", err)
	}
	destinationBefore := captureOwnerIdentities(t, map[string]*sourceSupervisor{
		"camera-2": owners["camera-2"],
	})
	applied, err := routing.ApplyGraph(
		context.Background(), catalog, store, initial.Revision, selections,
		NewRoutingRuntime(service))
	if err != nil {
		t.Fatalf("unavailable retiring source blocked apply: %v", err)
	}
	if applied.Revision != initial.Revision+1 || applied.Selections["live"] != "camera-2" ||
		applied.Selections["archive"] != "camera-2" {
		t.Fatalf("evacuation plan was not persisted exactly: %+v", applied)
	}
	if after := captureOwnerIdentities(t, map[string]*sourceSupervisor{
		"camera-2": owners["camera-2"],
	}); !reflect.DeepEqual(after, destinationBefore) {
		t.Fatalf("evacuation changed healthy destination owner: before=%v after=%v", destinationBefore, after)
	}
	service.mu.RLock()
	retired := service.sources["camera-1"]
	service.mu.RUnlock()
	if retired != nil {
		t.Fatal("unavailable source remained managed after complete retirement")
	}
	wantCalls := []string{
		"validate", "prepare", "warmup", "admission", "first_frame",
		"cutover", "verify", "retire",
	}
	if got := backend.callSequence(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("unexpected evacuation phases: got=%v want=%v", got, wantCalls)
	}
}

func TestPreservedOwnerFanoutKeepsRetiringSourceManagedWhenStopFails(t *testing.T) {
	catalog := fanoutRoutingCatalog(t, 2)
	archive := catalog.Streams["archive"]
	archive.Options["camera-2"] = routing.StreamOption{
		SourceRef: "camera-2", Codec: "h265",
		Width: 640, Height: 360, FPS: 15, BitrateKbps: 750,
	}
	catalog.Streams["archive"] = archive
	var err error
	catalog, err = routing.NormalizeCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	selections := cloneSelections(initial.Selections)
	selections["live"] = "camera-2"
	selections["archive"] = "camera-2"
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
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

	backend := &fanoutBackendRecorder{}
	service, owners := fanoutRoutingService(t, current, backend)
	owners["camera-1"].cancel = func() {}
	owners["camera-1"].done = make(chan struct{})
	prepared, err := NewRoutingRuntime(service).PrepareGraph(
		context.Background(), current, candidate, initial, preview.Candidate, delta)
	if err != nil {
		t.Fatal(err)
	}
	phases := []struct {
		name string
		run  func(context.Context) error
	}{
		{name: "warmup", run: prepared.Warmup},
		{name: "admission", run: prepared.Admit},
		{name: "first_frame", run: prepared.AwaitFirstFrame},
		{name: "cutover", run: prepared.Cutover},
		{name: "verify", run: prepared.Verify},
	}
	for _, phase := range phases {
		if err := phase.run(context.Background()); err != nil {
			t.Fatalf("%s: %v", phase.name, err)
		}
	}
	retireContext, cancelRetire := context.WithCancel(context.Background())
	cancelRetire()
	err = prepared.Retire(retireContext)
	if err == nil || !errors.Is(err, context.Canceled) ||
		!strings.Contains(err.Error(), "stop retired fan-out source") {
		t.Fatalf("retiring-source stop failure was not surfaced exactly: %v", err)
	}
	service.mu.RLock()
	retained := service.sources["camera-1"]
	remaining := service.sources["camera-2"]
	service.mu.RUnlock()
	if retained != owners["camera-1"] || remaining != owners["camera-2"] {
		t.Fatalf("stop failure lost managed source ownership: retained=%p remaining=%p", retained, remaining)
	}
	if !owners["camera-1"].mu.TryLock() {
		t.Fatal("stop failure leaked the retiring source-supervisor lock")
	}
	owners["camera-1"].mu.Unlock()
}

func TestPreservedOwnerFanoutFailsClosedWithoutAttestedBackend(t *testing.T) {
	catalog := fanoutRoutingCatalog(t, 2)
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	service, _ := fanoutRoutingService(t, current, nil)
	selections := cloneSelections(initial.Selections)
	selections["live"] = "camera-2"
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	validator := NewRoutingRuntime(service).(routing.GraphValidator)
	_, err = routing.PreviewRuntimeGraph(
		context.Background(), catalog, store, initial.Revision, selections, validator)
	if err == nil || !strings.Contains(err.Error(), "does not attest dynamic fan-out") {
		t.Fatalf("unattested backend was not rejected explicitly: %v", err)
	}
	if _, err := routing.ApplyGraph(
		context.Background(), catalog, store, initial.Revision, selections,
		NewRoutingRuntime(service)); err == nil || !strings.Contains(err.Error(), "does not attest dynamic fan-out") {
		t.Fatalf("unattested fan-out apply was not rejected: %v", err)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, initial) {
		t.Fatal("rejected fan-out changed the persistent plan")
	}
}

func TestPreservedOwnerFanoutRejectsCaptureEpochMutationAndRollsBack(t *testing.T) {
	catalog := fanoutRoutingCatalog(t, 3)
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	backend := &fanoutBackendRecorder{
		cutoverMutator: func(handles map[string]sourceFanoutHandle) {
			handle := handles["camera-2"]
			handle.Runtime.mu.Lock()
			handle.Runtime.mediaEpoch++
			handle.Runtime.mu.Unlock()
		},
	}
	service, owners := fanoutRoutingService(t, current, backend)
	selections := cloneSelections(initial.Selections)
	selections["live"] = "camera-2"
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	_, err = routing.ApplyGraph(
		context.Background(), catalog, store, initial.Revision, selections,
		NewRoutingRuntime(service))
	if err == nil || !strings.Contains(err.Error(), "capture PID or media epoch changed") {
		t.Fatalf("capture-owner mutation did not fail the transaction: %v", err)
	}
	var applyErr *routing.ApplyError
	if !errors.As(err, &applyErr) || applyErr.Phase != routing.ApplyPhaseCutover {
		t.Fatalf("unexpected fan-out failure phase: %v", err)
	}
	if got := backend.callSequence(); !reflect.DeepEqual(got, []string{
		"prepare", "warmup", "admission", "first_frame", "cutover", "rollback",
	}) {
		t.Fatalf("capture mutation did not take the bounded rollback path: %v", got)
	}
	for _, supervisor := range owners {
		if !supervisor.mu.TryLock() {
			t.Fatal("fan-out failure leaked a source-supervisor lock")
		}
		supervisor.mu.Unlock()
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, initial) {
		t.Fatal("failed fan-out was persisted")
	}
}

func TestClassifyPreservedOwnerFanoutRejectsSingleSourceReconfiguration(t *testing.T) {
	catalog := fanoutRoutingCatalog(t, 2)
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	candidate := productconfig.Clone(current)
	live := candidate.Streams["live"]
	live.FPS--
	candidate.Streams["live"] = live
	candidate, err = productconfig.Normalize(candidate)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := classifyPreservedOwnerFanout(current, candidate, delta); err == nil {
		t.Fatal("single-source profile reconfiguration was classified as fan-out")
	}
}

func fanoutRoutingCatalog(t *testing.T, sourceCount int) routing.Catalog {
	t.Helper()
	if sourceCount < 2 {
		t.Fatal("fan-out fixture requires at least two sources")
	}
	directory := t.TempDir()
	sources := make(map[string]productconfig.SourceConfig, sourceCount)
	for sourceIndex := 1; sourceIndex <= sourceCount; sourceIndex++ {
		sourceID := fmt.Sprintf("camera-%d", sourceIndex)
		sources[sourceID] = productconfig.SourceConfig{
			Identity: sourceID, Transport: "v4l2_uvc",
			Device: filepath.Join("/dev/nemesi-video-sources", sourceID),
			Format: "mjpeg", Width: 1280, Height: 720, FPS: 20,
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
	streams := map[string]routing.StreamCatalog{
		"live": {
			Type: "live", Socket: filepath.Join(directory, "live.sock"),
			Options: map[string]routing.StreamOption{
				"camera-1": option("camera-1", "h265", "", 1280, 720, 20, 4096),
				"camera-2": option("camera-2", "h265", "", 1280, 720, 20, 4096),
			},
		},
		"archive": {
			Type: "archive", Socket: filepath.Join(directory, "archive.sock"),
			Options: map[string]routing.StreamOption{
				"fixed": option("camera-1", "h265", "", 640, 360, 15, 750),
			},
		},
		"analysis": {
			Type: "analysis", SharedMemory: filepath.Join(directory, "analysis.nv12"),
			NotifySocket: filepath.Join(directory, "analysis.notify"),
			Options: map[string]routing.StreamOption{
				"fixed": option("camera-2", "", "nv12", 640, 360, 20, 0),
			},
		},
	}
	selections := map[string]string{
		"live": "camera-1", "archive": "fixed", "analysis": "fixed",
	}
	for sourceIndex := 3; sourceIndex <= sourceCount; sourceIndex++ {
		sourceID := fmt.Sprintf("camera-%d", sourceIndex)
		streamID := fmt.Sprintf("peer-live-%d", sourceIndex)
		streams[streamID] = routing.StreamCatalog{
			Type: "live", Socket: filepath.Join(directory, streamID+".sock"),
			Options: map[string]routing.StreamOption{
				"fixed": option(sourceID, "h265", "", 640, 360, 10, 2048),
			},
		}
		selections[streamID] = "fixed"
	}
	catalog, err := routing.NormalizeCatalog(routing.Catalog{
		Schema: routing.CatalogSchema, Revision: "sha256:" + strings.Repeat("f", 64),
		Sources: sources, Streams: streams, DefaultSelections: selections,
		Runtime: productconfig.RuntimeConfig{
			StateDirectory: filepath.Join(directory, "state"), RuntimeDirectory: directory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func fanoutRoutingService(
	t *testing.T,
	cfg productconfig.Config,
	backend sourceFanoutBackend,
) (*Service, map[string]*sourceSupervisor) {
	t.Helper()
	managerCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	allocator := newCapabilityAllocator()
	owners := make(map[string]*sourceSupervisor, len(cfg.Sources))
	for sourceIndex, graphConfig := range cfg.SourceGraphs() {
		plan := sourceMoveTestPlan(graphConfig, allocator)
		runtime := &videoBrokerRuntime{
			plan: plan, childRunning: true,
			childCmd:   &exec.Cmd{Process: &os.Process{Pid: 51000 + sourceIndex}},
			mediaEpoch: uint64(61000 + sourceIndex),
		}
		graph := &graphService{
			sourceID: graphConfig.SourceID, runtime: runtime, plan: plan,
		}
		applyFanoutRoleIdentity(graph, graphConfig)
		owners[graphConfig.SourceID] = &sourceSupervisor{
			config: graphConfig, graph: graph, state: "running",
		}
	}
	return &Service{
		sources: owners, config: cfg, allocator: allocator, managerCtx: managerCtx,
		fanout: backend,
	}, owners
}

type captureOwnerIdentity struct {
	Graph      *graphService
	Runtime    *videoBrokerRuntime
	ChildPID   int
	MediaEpoch uint64
}

func captureOwnerIdentities(
	t *testing.T,
	owners map[string]*sourceSupervisor,
) map[string]captureOwnerIdentity {
	t.Helper()
	identities := make(map[string]captureOwnerIdentity, len(owners))
	for sourceID, supervisor := range owners {
		supervisor.mu.RLock()
		graph := supervisor.graph
		supervisor.mu.RUnlock()
		graph.runtime.mu.Lock()
		identities[sourceID] = captureOwnerIdentity{
			Graph: graph, Runtime: graph.runtime,
			ChildPID: brokerChildPID(graph.runtime.childCmd), MediaEpoch: graph.runtime.mediaEpoch,
		}
		graph.runtime.mu.Unlock()
	}
	return identities
}

func sourceGraphMust(
	t *testing.T,
	cfg productconfig.Config,
	sourceID string,
) productconfig.SourceGraph {
	t.Helper()
	graph, ok := sourceGraph(cfg, sourceID)
	if !ok {
		t.Fatalf("source graph %q is missing", sourceID)
	}
	return graph
}

func cloneSelections(input map[string]string) map[string]string {
	cloned := make(map[string]string, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}
