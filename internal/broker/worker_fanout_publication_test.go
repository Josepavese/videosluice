package broker

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	productconfig "github.com/Josepavese/videosluice/config"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/internal/unixsocket"
	"github.com/Josepavese/videosluice/routing"
)

type fanoutPublicationTestBinding struct {
	publication *unixsocket.Publication
	shared      *brokerSubSharedFrame
	notifier    *brokerSubFrameNotifier
}

func TestRuntimeFanoutPublicationRequiresExactWorkerRuntimeAttestation(t *testing.T) {
	candidate := &productconfig.NamedStream{
		ID: "live",
		Config: productconfig.StreamConfig{
			SourceRef: "front", Codec: "h265", Width: 1280, Height: 720, FPS: 20,
			BitrateKbps: 4096,
		},
	}
	role := &runtimeFanoutPublicationRole{
		key:       fanoutPublicationRoleKey{sourceID: "front", role: "live"},
		candidate: candidate,
	}
	prepared := &runtimeFanoutPreparedPublications{
		transactionID: "fanout-attestation", phase: 2,
		sources: []*runtimeFanoutPublicationSource{{
			sourceID: "front", workerParticipates: true,
			roles: []*runtimeFanoutPublicationRole{role},
		}},
		roles: map[fanoutPublicationRoleKey]*runtimeFanoutPublicationRole{role.key: role},
	}
	receipt := videoproviders.WorkerFanoutReceipt{
		Schema:        videoproviders.WorkerFanoutReceiptSchema,
		TransactionID: "fanout-attestation", Sequence: 1, SourceID: "front",
		CapturePID: 100, MediaEpoch: 1, Event: videoproviders.WorkerFanoutEventFirstFrame,
		Branches: []videoproviders.WorkerFanoutBranchReceipt{{
			Role: "live", BranchEpoch: 1, FrameID: 1,
			CodecBootstrap: videoproviders.WorkerFanoutBootstrapComplete,
		}},
	}
	if err := prepared.AttestFirstFrame(
		map[string]videoproviders.WorkerFanoutReceipt{"front": receipt},
	); err == nil {
		t.Fatal("worker evidence without a runtime mode was accepted")
	}
	if err := prepared.AwaitFirstFrame(context.Background()); err == nil {
		t.Fatal("publication first-frame phase advanced without worker attestation")
	}
	receipt.Branches[0].RuntimeMode = videoproviders.WorkerFanoutRuntimePersistentEncoder
	foreign := receipt
	foreign.TransactionID = "fanout-other"
	if err := prepared.AttestFirstFrame(
		map[string]videoproviders.WorkerFanoutReceipt{"front": foreign},
	); err == nil {
		t.Fatal("foreign transaction evidence was accepted")
	}
	if err := prepared.AttestFirstFrame(
		map[string]videoproviders.WorkerFanoutReceipt{"front": receipt},
	); err != nil {
		t.Fatalf("exact worker runtime evidence was rejected: %v", err)
	}
	if role.candidateRuntimeMode != videoproviders.WorkerFanoutRuntimePersistentEncoder {
		t.Fatalf("worker runtime mode was not bound: %q", role.candidateRuntimeMode)
	}
	if err := prepared.AwaitFirstFrame(context.Background()); err != nil {
		t.Fatalf("attested publication first-frame phase did not advance: %v", err)
	}
}

func TestRuntimeFanoutPublicationCoordinatorCutsOverOnlyMovedRole(t *testing.T) {
	tests := []struct {
		name        string
		role        string
		sourceCount int
	}{
		{name: "live_two_sources", role: "live", sourceCount: 2},
		{name: "live_three_sources", role: "live", sourceCount: 3},
		{name: "live_five_sources", role: "live", sourceCount: 5},
		{name: "archive_two_sources", role: "archive", sourceCount: 2},
		{name: "archive_three_sources", role: "archive", sourceCount: 3},
		{name: "archive_five_sources", role: "archive", sourceCount: 5},
		{name: "analysis_two_sources", role: "analysis", sourceCount: 2},
		{name: "analysis_three_sources", role: "analysis", sourceCount: 3},
		{name: "analysis_five_sources", role: "analysis", sourceCount: 5},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transition := fanoutTransitionForRoleTest(t, test.role, test.sourceCount)
			fixture := newWorkerFanoutBackendFixtureForTransition(t, transition, 3)
			installFanoutCurrentPublications(t, &fixture)
			unchanged := snapshotUnchangedFanoutBindings(transition, fixture.handles)
			coordinator := newRuntimeFanoutPublicationCoordinator()
			fixture.backend.publications = coordinator

			if err := fixture.backend.ValidateFanout(
				context.Background(), transition, fixture.handles,
			); err != nil {
				t.Fatal(err)
			}
			prepared, err := fixture.backend.PrepareFanout(
				context.Background(), transition, fixture.handles)
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []func(context.Context) error{
				prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame, prepared.Cutover,
			} {
				if err := phase(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			move := transition.Moves[0]
			assertFanoutRuntimeMetadataPublished(t, move, fixture.handles)
			assertFanoutRetiredSourceDropsFrame(t, fixture.handles[move.From].Runtime, test.role)
			attestFanoutCandidateFrame(t, fixture.handles[move.To].Runtime, test.role)
			if err := prepared.Verify(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := prepared.Retire(context.Background()); err != nil {
				t.Fatal(err)
			}
			assertFanoutMovedRolePublished(t, move, fixture.handles)
			assertUnchangedFanoutBindings(t, unchanged, fixture.handles)
			if len(fixture.allocator.reservations) != 0 {
				t.Fatalf("successful publication retire retained capacity: %+v",
					fixture.allocator.reservations)
			}
		})
	}
}

func TestRuntimeFanoutPublicationDetachesRoleForExternalSourceActivation(t *testing.T) {
	full := fanoutTransitionForTest(t, 2)
	const sourceID = "camera-1"
	transition := preservedOwnerFanout{
		SourceIDs: []string{sourceID},
		StreamIDs: append([]string(nil), full.StreamIDs...),
		CurrentGraphs: map[string]productconfig.SourceGraph{
			sourceID: full.CurrentGraphs[sourceID],
		},
		CandidateGraphs: map[string]productconfig.SourceGraph{
			sourceID: full.CandidateGraphs[sourceID],
		},
	}
	fixture := newWorkerFanoutBackendFixtureForTransition(t, transition, 3)
	initial := installFanoutCurrentPublications(t, &fixture)
	fixture.backend.publications = newRuntimeFanoutPublicationCoordinator()
	prepared, err := fixture.backend.PrepareFanout(
		context.Background(), transition, fixture.handles)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []func(context.Context) error{
		prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame,
		prepared.Cutover, prepared.Verify, prepared.Retire,
	} {
		if err := phase(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	runtime := fixture.handles[sourceID].Runtime
	runtime.mu.Lock()
	mainPublication := runtime.mainPublication
	archivePublication := runtime.archivePublication
	liveEnabled, archiveEnabled := runtime.plan.LiveEnabled, runtime.plan.ArchiveEnabled
	liveMode := runtime.childLiveControlMode
	runtime.mu.Unlock()
	if mainPublication != nil || liveEnabled || liveMode != "disabled" {
		t.Fatalf(
			"external activation detachment retained live state publication=%p enabled=%t mode=%q",
			mainPublication, liveEnabled, liveMode)
	}
	archiveKey := fanoutPublicationRoleKey{sourceID: sourceID, role: "archive"}
	if !archiveEnabled || archivePublication != initial[archiveKey].publication {
		t.Fatal("external activation detachment changed the remaining archive role")
	}
}

func TestRuntimeFanoutPublicationEvacuatesUnavailableRetiringWorker(t *testing.T) {
	transition := fanoutRetiringTransitionForTest(t)
	fixture := newWorkerFanoutBackendFixtureForTransition(t, transition, 4)
	installFanoutCurrentPublications(t, &fixture)
	retiring := fixture.handles["camera-1"]
	retiring.Runtime.mu.Lock()
	retiring.Runtime.childRunning = false
	retiring.Runtime.childCmd = nil
	retiring.Runtime.mu.Unlock()
	retiring.ChildPID = 0
	retiring.CaptureAvailable = false
	fixture.handles["camera-1"] = retiring
	fixture.backend.publications = newRuntimeFanoutPublicationCoordinator()

	if err := fixture.backend.ValidateFanout(
		context.Background(), transition, fixture.handles,
	); err != nil {
		t.Fatalf("unavailable retiring worker blocked preview: %v", err)
	}
	prepared, err := fixture.backend.PrepareFanout(
		context.Background(), transition, fixture.handles)
	if err != nil {
		t.Fatalf("unavailable retiring worker blocked prepare: %v", err)
	}
	for _, phase := range []func(context.Context) error{
		prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame, prepared.Cutover,
	} {
		if err := phase(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, move := range transition.Moves {
		attestFanoutCandidateFrame(t, fixture.handles[move.To].Runtime, move.Type)
	}
	if err := prepared.Verify(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if phases := fixture.workers["camera-1"].phases(); len(phases) != 0 {
		t.Fatalf("unavailable retiring worker received commands: %v", phases)
	}
	want := []string{"prepare", "warmup", "admit", "first_frame", "cutover", "verify", "retire"}
	if phases := fixture.workers["camera-2"].phases(); !reflect.DeepEqual(phases, want) {
		t.Fatalf("healthy destination phases=%v want=%v", phases, want)
	}
	for _, move := range transition.Moves {
		assertFanoutMovedRolePublished(t, move, fixture.handles)
	}
}

func assertFanoutRuntimeMetadataPublished(
	t *testing.T,
	move fanoutStreamMove,
	handles map[string]sourceFanoutHandle,
) {
	t.Helper()
	from := handles[move.From].Runtime
	to := handles[move.To].Runtime
	from.mu.Lock()
	defer from.mu.Unlock()
	to.mu.Lock()
	defer to.mu.Unlock()
	switch move.Type {
	case "live":
		if from.childLiveControlMode != "disabled" ||
			to.childLiveControlMode != "persistent_encoder_epoch" {
			t.Fatalf(
				"live runtime metadata was not switched atomically from=%q to=%q",
				from.childLiveControlMode, to.childLiveControlMode)
		}
	case "archive":
		if from.observedArchiveCodec != "" || from.observedArchiveDelivery != "" ||
			from.observedArchiveFPS != 0 {
			t.Fatalf(
				"retired archive metadata was not cleared codec=%q delivery=%q fps=%d",
				from.observedArchiveCodec, from.observedArchiveDelivery,
				from.observedArchiveFPS)
		}
		if to.observedArchiveCodec != to.plan.ArchiveCodec ||
			to.observedArchiveDelivery != "processed_hardware_encode" ||
			to.observedArchiveFPS != to.plan.ArchiveFPS {
			t.Fatalf(
				"candidate archive metadata differs codec=%q/%q delivery=%q fps=%d/%d",
				to.observedArchiveCodec, to.plan.ArchiveCodec,
				to.observedArchiveDelivery, to.observedArchiveFPS, to.plan.ArchiveFPS)
		}
	}
}

func TestRuntimeFanoutPublicationCoordinatorRestoresPartialMultiRoleCutover(t *testing.T) {
	transition := fanoutTwoRoleSwapForTest(t)
	fixture := newWorkerFanoutBackendFixtureForTransition(t, transition, 4)
	initial := installFanoutCurrentPublications(t, &fixture)
	initialPlans := snapshotFanoutRuntimePlans(fixture.handles)
	fixture.backend.publications = newRuntimeFanoutPublicationCoordinator()
	preparedGraph, err := fixture.backend.PrepareFanout(
		context.Background(), transition, fixture.handles)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []func(context.Context) error{
		preparedGraph.Warmup, preparedGraph.Admit, preparedGraph.AwaitFirstFrame,
	} {
		if err := phase(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	backendPrepared, ok := preparedGraph.(*workerFanoutPreparedGraph)
	if !ok {
		t.Fatalf("unexpected prepared graph %T", preparedGraph)
	}
	publications, ok := backendPrepared.publications.(*runtimeFanoutPreparedPublications)
	if !ok || len(publications.moves) != 2 {
		t.Fatalf("unexpected publication transaction %#v", backendPrepared.publications)
	}
	// The canonical swap orders analysis before live. Destroy only the second
	// private candidate so analysis is already replaced when live fails.
	if publications.moves[0].role != "analysis" || publications.moves[1].role != "live" ||
		publications.moves[1].to.candidatePublication == nil {
		t.Fatalf("unexpected canonical move order: %+v", transition.Moves)
	}
	publications.moves[1].to.candidatePublication.Cleanup()
	if err := preparedGraph.Cutover(context.Background()); err == nil {
		t.Fatal("destroyed second candidate did not fail partial cutover")
	}
	if !publications.moves[0].sharedSwapped || publications.moves[1].socketSwapped {
		t.Fatalf("partial cutover boundary was not observed first=%+v second=%+v",
			publications.moves[0], publications.moves[1])
	}
	if err := preparedGraph.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	for key, binding := range initial {
		assertFanoutBindingEquals(t, fixture.handles[key.sourceID].Runtime, key.role, binding)
	}
	for _, move := range transition.Moves {
		assertFanoutCurrentEndpointOwned(
			t, fixture.handles[move.From].Runtime, move.Type, move.Current)
	}
	for sourceID, handle := range fixture.handles {
		handle.Runtime.mu.Lock()
		got := handle.Runtime.plan
		handle.Runtime.mu.Unlock()
		if !reflect.DeepEqual(got, initialPlans[sourceID]) {
			t.Fatalf("partial rollback source %s did not restore its plan", sourceID)
		}
	}
	if len(fixture.allocator.reservations) != 0 {
		t.Fatalf("partial rollback retained capacity: %+v", fixture.allocator.reservations)
	}
}

func TestRuntimeFanoutPublicationCoordinatorRollsBackSocketAndSharedMemory(t *testing.T) {
	for _, role := range []string{"live", "archive", "analysis"} {
		t.Run(role, func(t *testing.T) {
			transition := fanoutTransitionForRoleTest(t, role, 2)
			fixture := newWorkerFanoutBackendFixtureForTransition(t, transition, 3)
			initial := installFanoutCurrentPublications(t, &fixture)
			initialPlans := snapshotFanoutRuntimePlans(fixture.handles)
			initialTelemetry := seedAndSnapshotFanoutRuntimeTelemetry(fixture.handles)
			fixture.backend.publications = newRuntimeFanoutPublicationCoordinator()
			prepared, err := fixture.backend.PrepareFanout(
				context.Background(), transition, fixture.handles)
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []func(context.Context) error{
				prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame, prepared.Cutover,
			} {
				if err := phase(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if err := prepared.Rollback(context.Background()); err != nil {
				t.Fatal(err)
			}
			move := transition.Moves[0]
			fromKey := fanoutPublicationRoleKey{sourceID: move.From, role: role}
			toKey := fanoutPublicationRoleKey{sourceID: move.To, role: role}
			assertFanoutBindingEquals(t, fixture.handles[move.From].Runtime, role, initial[fromKey])
			assertFanoutBindingEquals(t, fixture.handles[move.To].Runtime, role, initial[toKey])
			assertFanoutCurrentEndpointOwned(t, fixture.handles[move.From].Runtime, role, move.Current)
			if len(fixture.allocator.reservations) != 0 {
				t.Fatalf("attested publication rollback retained capacity: %+v",
					fixture.allocator.reservations)
			}
			for sourceID, handle := range fixture.handles {
				handle.Runtime.mu.Lock()
				if !reflect.DeepEqual(handle.Runtime.plan, initialPlans[sourceID]) {
					handle.Runtime.mu.Unlock()
					t.Fatalf("rollback source %s did not restore its runtime plan", sourceID)
				}
				telemetry := fanoutRuntimeTelemetryLocked(handle.Runtime)
				handle.Runtime.mu.Unlock()
				if telemetry != initialTelemetry[sourceID] {
					t.Fatalf(
						"rollback source %s did not restore runtime telemetry got=%+v want=%+v",
						sourceID, telemetry, initialTelemetry[sourceID])
				}
			}
		})
	}
}

type fanoutRuntimeTelemetry struct {
	liveMode        string
	archiveCodec    string
	archiveDelivery string
	archiveFPS      int
}

func seedAndSnapshotFanoutRuntimeTelemetry(
	handles map[string]sourceFanoutHandle,
) map[string]fanoutRuntimeTelemetry {
	result := make(map[string]fanoutRuntimeTelemetry, len(handles))
	for sourceID, handle := range handles {
		handle.Runtime.mu.Lock()
		handle.Runtime.childLiveControlMode = sourceID + "-live-incumbent"
		handle.Runtime.observedArchiveCodec = "h264"
		handle.Runtime.observedArchiveDelivery = sourceID + "-archive-incumbent"
		handle.Runtime.observedArchiveFPS = 7
		result[sourceID] = fanoutRuntimeTelemetryLocked(handle.Runtime)
		handle.Runtime.mu.Unlock()
	}
	return result
}

func fanoutRuntimeTelemetryLocked(runtime *videoBrokerRuntime) fanoutRuntimeTelemetry {
	return fanoutRuntimeTelemetry{
		liveMode:        runtime.childLiveControlMode,
		archiveCodec:    runtime.observedArchiveCodec,
		archiveDelivery: runtime.observedArchiveDelivery,
		archiveFPS:      runtime.observedArchiveFPS,
	}
}

func TestRuntimeFanoutPublicationCoordinatorPreservesWarmupAttestation(t *testing.T) {
	transition := fanoutTransitionForRoleTest(t, "live", 2)
	fixture := newWorkerFanoutBackendFixtureForTransition(t, transition, 3)
	installFanoutCurrentPublications(t, &fixture)
	fixture.backend.publications = newRuntimeFanoutPublicationCoordinator()
	prepared, err := fixture.backend.PrepareFanout(
		context.Background(), transition, fixture.handles)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []func(context.Context) error{
		prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame,
	} {
		if err := phase(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, handle := range fixture.handles {
		handle.Runtime.mu.Lock()
		if len(handle.Runtime.capabilityPlan.Stages) == 0 {
			handle.Runtime.mu.Unlock()
			t.Fatal("fixture has no capability stages")
		}
		handle.Runtime.capabilityPlan.Stages[0].State = "attested"
		handle.Runtime.mu.Unlock()
	}
	if err := prepared.Cutover(context.Background()); err != nil {
		t.Fatalf("runtime attestation invalidated cutover: %v", err)
	}
	if err := prepared.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	for sourceID, handle := range fixture.handles {
		handle.Runtime.mu.Lock()
		state := handle.Runtime.capabilityPlan.Stages[0].State
		handle.Runtime.mu.Unlock()
		if state != "attested" {
			t.Fatalf("rollback source %s lost warmup attestation state=%q", sourceID, state)
		}
	}
}

func TestRuntimeFanoutPublicationCoordinatorRejectsCapabilityFallbackRace(t *testing.T) {
	transition := fanoutTransitionForRoleTest(t, "live", 2)
	fixture := newWorkerFanoutBackendFixtureForTransition(t, transition, 3)
	installFanoutCurrentPublications(t, &fixture)
	fixture.backend.publications = newRuntimeFanoutPublicationCoordinator()
	prepared, err := fixture.backend.PrepareFanout(
		context.Background(), transition, fixture.handles)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []func(context.Context) error{
		prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame,
	} {
		if err := phase(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	runtime := fixture.handles[transition.SourceIDs[0]].Runtime
	runtime.mu.Lock()
	runtime.capabilityPlan.Stages[0].Fallback = "hardware_failed"
	runtime.mu.Unlock()
	if err := prepared.Cutover(context.Background()); err == nil {
		t.Fatal("capability fallback race was accepted")
	}
	if err := prepared.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(fixture.allocator.reservations) != 0 {
		t.Fatalf("rejected capability race retained capacity: %+v", fixture.allocator.reservations)
	}
}

func TestFanoutRoleGateDropsOnlyTheAffectedRole(t *testing.T) {
	runtime := &videoBrokerRuntime{
		plan: videoBrokerPlan{
			LiveEnabled: true, MainFormat: "h265", MainWidth: 1280, MainHeight: 720,
			ArchiveEnabled: true, ArchiveCodec: "h265", ArchiveWidth: 640, ArchiveHeight: 360,
			AnalysisEnabled: true, SubFormat: "nv12", SubWidth: 640, SubHeight: 360,
		},
		mainClients: make(map[int]*brokerClient), archiveClients: make(map[int]*brokerClient),
		subClients: make(map[int]*brokerClient),
	}
	runtime.mu.Lock()
	if err := setFanoutRoleGateLocked(runtime, "live", "fanout-gate-test"); err != nil {
		runtime.mu.Unlock()
		t.Fatal(err)
	}
	runtime.mu.Unlock()
	runtime.dispatchFrame(brokerStreamH264, []byte{0, 0, 0, 1, 19 << 1, 1})
	runtime.dispatchFrame(brokerStreamH265, []byte{0, 0, 0, 1, 19 << 1, 1})
	runtime.mu.Lock()
	if runtime.totalMainFrames != 0 || runtime.totalArchiveFrames != 1 {
		runtime.mu.Unlock()
		t.Fatalf("live gate affected wrong roles main=%d archive=%d",
			runtime.totalMainFrames, runtime.totalArchiveFrames)
	}
	if err := clearFanoutRoleGateLocked(runtime, "live", "fanout-gate-test"); err != nil {
		runtime.mu.Unlock()
		t.Fatal(err)
	}
	runtime.mu.Unlock()
	runtime.dispatchFrame(brokerStreamH264, []byte{0, 0, 0, 1, 19 << 1, 1})
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.totalMainFrames != 1 || runtime.totalArchiveFrames != 1 {
		t.Fatalf("cleared live gate did not restore only live main=%d archive=%d",
			runtime.totalMainFrames, runtime.totalArchiveFrames)
	}
}

func fanoutTransitionForRoleTest(
	t *testing.T,
	role string,
	sourceCount int,
) preservedOwnerFanout {
	t.Helper()
	if role == "live" {
		return fanoutTransitionForTest(t, sourceCount)
	}
	catalog := fanoutRoutingCatalog(t, sourceCount)
	if role == "analysis" {
		catalog.Streams["keep-camera-2"] = routing.StreamCatalog{
			Type: "live", Socket: filepath.Join(catalog.Runtime.RuntimeDirectory, "keep-camera-2.sock"),
			Options: map[string]routing.StreamOption{
				"fixed": {
					SourceRef: "camera-2", Codec: "h265",
					Width: 640, Height: 360, FPS: 10, BitrateKbps: 2048,
				},
			},
		}
		catalog.DefaultSelections["keep-camera-2"] = "fixed"
	}
	stream, exists := catalog.Streams[role]
	if !exists {
		t.Fatalf("role %s is absent from fan-out catalog", role)
	}
	options := make(map[string]routing.StreamOption, len(stream.Options)+1)
	for optionID, option := range stream.Options {
		options[optionID] = option
	}
	var candidateSource string
	for _, option := range options {
		candidateSource = option.SourceRef
		break
	}
	if candidateSource == "camera-1" {
		candidateSource = "camera-2"
	} else {
		candidateSource = "camera-1"
	}
	base := options[catalog.DefaultSelections[role]]
	base.SourceRef = candidateSource
	options["move"] = base
	stream.Options = options
	catalog.Streams[role] = stream
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
	selections[role] = "move"
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
	return transition
}

func fanoutTwoRoleSwapForTest(t *testing.T) preservedOwnerFanout {
	t.Helper()
	catalog := fanoutRoutingCatalog(t, 2)
	live := catalog.Streams["live"]
	analysis := catalog.Streams["analysis"]
	analysis.Options["camera-1"] = routing.StreamOption{
		SourceRef: "camera-1", PixelFormat: "nv12", Width: 640, Height: 360, FPS: 20,
	}
	catalog.Streams["analysis"] = analysis
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
	selections["analysis"] = "camera-1"
	// Keep the read explicit: both choices belong to the same normalized
	// catalog and no free-form profile is introduced by this fault fixture.
	if _, ok := live.Options[selections["live"]]; !ok {
		t.Fatal("live swap option is unavailable")
	}
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
	if len(transition.Moves) != 2 {
		t.Fatalf("two-role swap produced %d moves", len(transition.Moves))
	}
	return transition
}

func installFanoutCurrentPublications(
	t *testing.T,
	fixture *workerFanoutBackendFixture,
) map[fanoutPublicationRoleKey]fanoutPublicationTestBinding {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	initial := make(map[fanoutPublicationRoleKey]fanoutPublicationTestBinding)
	allInitialPublications := make([]*unixsocket.Publication, 0)
	allInitialShared := make([]*brokerSubSharedFrame, 0)
	for _, sourceID := range fixture.transition.SourceIDs {
		handle := fixture.handles[sourceID]
		runtime := handle.Runtime
		runtime.lifecycleMu.Lock()
		runtime.runtimeContext = ctx
		runtime.lifecycleMu.Unlock()
		runtime.mu.Lock()
		if runtime.mainClients == nil {
			runtime.mainClients = make(map[int]*brokerClient)
		}
		if runtime.archiveClients == nil {
			runtime.archiveClients = make(map[int]*brokerClient)
		}
		if runtime.subClients == nil {
			runtime.subClients = make(map[int]*brokerClient)
		}
		runtime.mu.Unlock()
		for _, role := range fanoutPublicationRoles {
			stream := workerFanoutNamedStream(handle.CurrentGraph, role)
			var publication *unixsocket.Publication
			var shared *brokerSubSharedFrame
			var notifier *brokerSubFrameNotifier
			if stream != nil && stream.Config.Socket != "" {
				value, err := listenBrokerSocket(stream.Config.Socket)
				if err != nil {
					t.Fatal(err)
				}
				publication = value
				allInitialPublications = append(allInitialPublications, value)
			}
			if stream != nil && role == "analysis" && stream.Config.SharedMemory != "" {
				payloadCap := (stream.Config.Width * stream.Config.Height * 3) / 2
				var err error
				shared, err = newBrokerSubSharedFrame(stream.Config.SharedMemory, payloadCap)
				if err != nil {
					t.Fatal(err)
				}
				allInitialShared = append(allInitialShared, shared)
				notifier = newBrokerSubFrameNotifier(stream.Config.NotifySocket)
			}
			runtime.mu.Lock()
			switch role {
			case "live":
				if publication != nil {
					runtime.mainPublication = publication
				}
			case "archive":
				if publication != nil {
					runtime.archivePublication = publication
				}
			case "analysis":
				if publication != nil {
					runtime.subPublication = publication
				}
				runtime.subShared, runtime.subNotifier = shared, notifier
			}
			boundPublication, _, boundShared, boundNotifier :=
				fanoutRuntimeRoleBindingLocked(runtime, role)
			initial[fanoutPublicationRoleKey{sourceID: sourceID, role: role}] =
				fanoutPublicationTestBinding{
					publication: boundPublication, shared: boundShared, notifier: boundNotifier,
				}
			runtime.mu.Unlock()
		}
	}
	t.Cleanup(func() {
		cancel()
		for _, handle := range fixture.handles {
			handle.Runtime.mu.Lock()
			mainPublication, archivePublication, subPublication :=
				handle.Runtime.mainPublication, handle.Runtime.archivePublication, handle.Runtime.subPublication
			mainCancel, archiveCancel, subCancel :=
				handle.Runtime.mainPublicationCancel,
				handle.Runtime.archivePublicationCancel,
				handle.Runtime.subPublicationCancel
			shared, notifier := handle.Runtime.subShared, handle.Runtime.subNotifier
			handle.Runtime.mu.Unlock()
			for _, stop := range []context.CancelFunc{mainCancel, archiveCancel, subCancel} {
				if stop != nil {
					stop()
				}
			}
			for _, publication := range []*unixsocket.Publication{
				mainPublication, archivePublication, subPublication,
			} {
				if publication != nil {
					publication.Cleanup()
				}
			}
			if shared != nil {
				_ = shared.Close()
			}
			if notifier != nil {
				_ = notifier.Close()
			}
		}
		for _, publication := range allInitialPublications {
			publication.Cleanup()
		}
		for _, shared := range allInitialShared {
			_ = shared.Close()
		}
		for _, handle := range fixture.handles {
			handle.Runtime.wg.Wait()
		}
	})
	return initial
}

func attestFanoutCandidateFrame(t *testing.T, runtime *videoBrokerRuntime, role string) {
	t.Helper()
	h265 := []byte{
		0, 0, 0, 1, 32 << 1, 1, 0xaa,
		0, 0, 0, 1, 33 << 1, 1, 0xbb,
		0, 0, 0, 1, 34 << 1, 1, 0xcc,
		0, 0, 0, 1, 19 << 1, 1, 0xdd,
	}
	switch role {
	case "live":
		runtime.dispatchFrame(brokerStreamH264, h265)
	case "archive":
		runtime.dispatchFrame(brokerStreamH265, h265)
	case "analysis":
		runtime.mu.Lock()
		size := runtime.plan.SubWidth * runtime.plan.SubHeight * 3 / 2
		runtime.mu.Unlock()
		runtime.dispatchFrame(brokerStreamNV12, make([]byte, size))
	default:
		t.Fatalf("unsupported role %s", role)
	}
}

func assertFanoutRetiredSourceDropsFrame(
	t *testing.T,
	runtime *videoBrokerRuntime,
	role string,
) {
	t.Helper()
	runtime.mu.Lock()
	before := fanoutRoleFrameCountLocked(runtime, role)
	runtime.mu.Unlock()
	switch role {
	case "live":
		runtime.dispatchFrame(brokerStreamH264, []byte{0, 0, 0, 1, 19 << 1, 1})
	case "archive":
		runtime.dispatchFrame(brokerStreamH265, []byte{0, 0, 0, 1, 19 << 1, 1})
	case "analysis":
		runtime.dispatchFrame(brokerStreamNV12, []byte{0})
	default:
		t.Fatalf("unsupported role %s", role)
	}
	runtime.mu.Lock()
	after := fanoutRoleFrameCountLocked(runtime, role)
	runtime.mu.Unlock()
	if after != before {
		t.Fatalf("retired source published stale %s frame before=%d after=%d", role, before, after)
	}
}

func assertFanoutMovedRolePublished(
	t *testing.T,
	move fanoutStreamMove,
	handles map[string]sourceFanoutHandle,
) {
	t.Helper()
	to := handles[move.To].Runtime
	from := handles[move.From].Runtime
	to.mu.Lock()
	publication, _, shared, notifier := fanoutRuntimeRoleBindingLocked(to, move.Type)
	if !fanoutRuntimePlanMatchesRole(to.plan, move.Type,
		workerFanoutNamedStream(handles[move.To].CandidateGraph, move.Type)) {
		to.mu.Unlock()
		t.Fatal("candidate runtime plan was not published")
	}
	to.mu.Unlock()
	if move.Candidate.Socket != "" && (publication == nil || !publication.Owns(move.Candidate.Socket)) {
		t.Fatal("candidate socket does not own the logical endpoint")
	}
	if move.Type == "analysis" && move.Candidate.SharedMemory != "" &&
		(shared == nil || !shared.Owns(move.Candidate.SharedMemory)) {
		t.Fatal("candidate shared memory does not own the logical endpoint")
	}
	if move.Type == "analysis" && move.Candidate.NotifySocket != "" && notifier == nil {
		t.Fatal("candidate analysis notifier was not installed")
	}
	from.mu.Lock()
	fromPublication, _, fromShared, _ := fanoutRuntimeRoleBindingLocked(from, move.Type)
	fromEnabled := fanoutRuntimeRoleEnabled(from.plan, move.Type)
	from.mu.Unlock()
	if fromPublication != nil || fromShared != nil || fromEnabled {
		t.Fatal("retiring source retained the moved publication role")
	}
}

func snapshotUnchangedFanoutBindings(
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
) map[fanoutPublicationRoleKey]fanoutPublicationTestBinding {
	unchanged := make(map[fanoutPublicationRoleKey]fanoutPublicationTestBinding)
	for _, sourceID := range transition.SourceIDs {
		runtime := handles[sourceID].Runtime
		runtime.mu.Lock()
		for _, role := range fanoutPublicationRoles {
			if !reflect.DeepEqual(
				workerFanoutNamedStream(transition.CurrentGraphs[sourceID], role),
				workerFanoutNamedStream(transition.CandidateGraphs[sourceID], role),
			) {
				continue
			}
			publication, _, shared, notifier := fanoutRuntimeRoleBindingLocked(runtime, role)
			unchanged[fanoutPublicationRoleKey{sourceID: sourceID, role: role}] =
				fanoutPublicationTestBinding{publication: publication, shared: shared, notifier: notifier}
		}
		runtime.mu.Unlock()
	}
	return unchanged
}

func assertUnchangedFanoutBindings(
	t *testing.T,
	expected map[fanoutPublicationRoleKey]fanoutPublicationTestBinding,
	handles map[string]sourceFanoutHandle,
) {
	t.Helper()
	for key, want := range expected {
		assertFanoutBindingEquals(t, handles[key.sourceID].Runtime, key.role, want)
	}
}

func assertFanoutBindingEquals(
	t *testing.T,
	runtime *videoBrokerRuntime,
	role string,
	want fanoutPublicationTestBinding,
) {
	t.Helper()
	runtime.mu.Lock()
	publication, _, shared, notifier := fanoutRuntimeRoleBindingLocked(runtime, role)
	runtime.mu.Unlock()
	if publication != want.publication || shared != want.shared || notifier != want.notifier {
		t.Fatalf("role %s binding changed got=(%p,%p,%p) want=(%p,%p,%p)",
			role, publication, shared, notifier, want.publication, want.shared, want.notifier)
	}
}

func snapshotFanoutRuntimePlans(
	handles map[string]sourceFanoutHandle,
) map[string]videoBrokerPlan {
	plans := make(map[string]videoBrokerPlan, len(handles))
	for sourceID, handle := range handles {
		handle.Runtime.mu.Lock()
		plans[sourceID] = handle.Runtime.plan
		handle.Runtime.mu.Unlock()
	}
	return plans
}

func assertFanoutCurrentEndpointOwned(
	t *testing.T,
	runtime *videoBrokerRuntime,
	role string,
	stream productconfig.StreamConfig,
) {
	t.Helper()
	runtime.mu.Lock()
	publication, _, shared, _ := fanoutRuntimeRoleBindingLocked(runtime, role)
	runtime.mu.Unlock()
	if stream.Socket != "" && (publication == nil || !publication.Owns(stream.Socket)) {
		t.Fatalf("restored %s socket does not own %s", role, stream.Socket)
	}
	if role == "analysis" && stream.SharedMemory != "" &&
		(shared == nil || !shared.Owns(stream.SharedMemory)) {
		t.Fatalf("restored analysis shared memory does not own %s", stream.SharedMemory)
	}
}
