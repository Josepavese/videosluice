package broker

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/routing"
)

func TestWorkerFanoutCapabilitySetReservesFiveSourceMovePeak(t *testing.T) {
	catalog := fanoutRoutingCatalog(t, 5)
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	selections := cloneSelections(initial.Selections)
	selections["live"] = "camera-2"
	preview, err := routing.PreviewGraph(catalog, store, initial.Revision, selections)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
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
	inventory := workerFanoutCapabilityInventory(3, 2)
	allocator := newCapabilityAllocator()
	handles := make(map[string]sourceFanoutHandle, len(transition.SourceIDs))
	for _, sourceID := range transition.SourceIDs {
		currentGraph := transition.CurrentGraphs[sourceID]
		runtime := workerFanoutCapabilityRuntime(currentGraph)
		plan, _, err := resolveWorkerFanoutGraphCapabilityLocked(runtime, currentGraph, inventory)
		if err != nil {
			t.Fatal(err)
		}
		runtime.capabilityPlan = plan
		handles[sourceID] = sourceFanoutHandle{
			SourceID: sourceID, Runtime: runtime,
			CurrentGraph: currentGraph, CandidateGraph: transition.CandidateGraphs[sourceID],
			CaptureAvailable: true,
		}
	}
	set, err := buildWorkerFanoutCapabilitySet(transition, handles, inventory)
	if err != nil {
		t.Fatal(err)
	}
	exhaustedInventory := workerFanoutCapabilityInventory(2, 2)
	exhausted := newCapabilityAllocator()
	seedCapabilityClaims(t, exhausted, exhaustedInventory, set.Current)
	if _, err := exhausted.reserveTransition(
		"route-exhausted", exhaustedInventory, set.Current, set.Candidate, set.Warmup,
	); err == nil || !strings.Contains(err.Error(), "lacks prepare capacity") {
		t.Fatalf("move exceeded peak VPU capacity without failing closed: %v", err)
	}
	if len(exhausted.reservations) != 0 {
		t.Fatalf("rejected peak reservation leaked allocator state: %+v", exhausted.reservations)
	}
	seedCapabilityClaims(t, allocator, inventory, set.Current)
	reservation, err := allocator.reserveTransition(
		"route-five", inventory, set.Current, set.Candidate, set.Warmup)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(allocator.reservations["route-five"], map[string]int{"video": 1}) {
		t.Fatalf("move did not reserve exactly one candidate live encoder: %+v", allocator.reservations)
	}
	if len(set.Warmup) != 1 || len(set.Warmup["camera-2"].Stages) != 1 ||
		set.Warmup["camera-2"].Stages[0].Role != "live" {
		t.Fatalf("unaffected or retiring sources entered warmup: %+v", set.Warmup)
	}
	if err := reservation.Commit(); err != nil {
		t.Fatal(err)
	}
	if allocator.claims["video"]["camera-1"] != 1 || allocator.claims["video"]["camera-2"] != 1 {
		t.Fatalf("final VPU claims do not match moved live branch: %+v", allocator.claims["video"])
	}
}

func TestWorkerFanoutCapabilityPlansOnlyIncrementalRoleWarmup(t *testing.T) {
	inventory := workerFanoutCapabilityInventory(4, 16)
	current := workerFanoutCapabilityGraph(false)
	candidate := workerFanoutCapabilityGraph(true)
	runtime := workerFanoutCapabilityRuntime(current)
	currentPlan, _, err := resolveWorkerFanoutGraphCapabilityLocked(runtime, current, inventory)
	if err != nil {
		t.Fatal(err)
	}
	runtime.capabilityPlan = currentPlan
	transition := preservedOwnerFanout{
		CurrentGraphs:   map[string]productconfig.SourceGraph{"camera-a": current},
		CandidateGraphs: map[string]productconfig.SourceGraph{"camera-a": candidate},
	}
	changes, err := workerFanoutChangesForSource(transition, "camera-a", "h265")
	if err != nil {
		t.Fatal(err)
	}
	candidatePlan, warmup, archiveCodec, err := resolveWorkerFanoutCapabilityTransitionLocked(
		runtime, candidate, changes, inventory)
	if err != nil {
		t.Fatal(err)
	}
	if archiveCodec != "h265" {
		t.Fatalf("archive auto was not resolved concretely: %q", archiveCodec)
	}
	warmStages := map[string]int{}
	for _, stage := range warmup.Stages {
		warmStages[stage.Stage+":"+stage.Role]++
		if stage.Stage == capability.StageCapture || stage.Stage == capability.StageDecode ||
			stage.Role == "analysis" || stage.Role == "archive" {
			t.Fatalf("warmup plan contains incumbent work: %+v", warmup.Stages)
		}
	}
	if warmStages[capability.StageEncode+":live"] != 1 ||
		warmStages[capability.StageResize+":live"] != 1 || len(warmup.Stages) != 2 {
		t.Fatalf("warmup plan does not contain exactly the new live branch: %+v", warmup.Stages)
	}
	roles := map[string]bool{}
	for _, stage := range candidatePlan.Stages {
		roles[stage.Stage+":"+stage.Role] = true
	}
	for _, expected := range []string{
		capability.StageDecode + ":capture",
		capability.StageEncode + ":archive",
		capability.StageEncode + ":live",
		capability.StageResize + ":analysis",
		capability.StageResize + ":archive",
		capability.StageResize + ":live",
	} {
		if !roles[expected] {
			t.Fatalf("candidate capability plan lacks %s: %+v", expected, candidatePlan.Stages)
		}
	}
}

func TestWorkerFanoutCapabilityRejectsWorkerCaptureChangeAndMisalignment(t *testing.T) {
	inventory := workerFanoutCapabilityInventory(4, 16)
	current := workerFanoutCapabilityGraph(false)
	candidate := workerFanoutCapabilityGraph(true)
	runtime := workerFanoutCapabilityRuntime(current)
	currentPlan, _, err := resolveWorkerFanoutGraphCapabilityLocked(runtime, current, inventory)
	if err != nil {
		t.Fatal(err)
	}
	runtime.capabilityPlan = currentPlan
	transition := preservedOwnerFanout{
		CurrentGraphs:   map[string]productconfig.SourceGraph{"camera-a": current},
		CandidateGraphs: map[string]productconfig.SourceGraph{"camera-a": candidate},
	}
	changes, err := workerFanoutChangesForSource(transition, "camera-a", "h265")
	if err != nil {
		t.Fatal(err)
	}
	runtime.capabilityPlan.WorkerProvider = "gstreamer"
	if _, _, _, err := resolveWorkerFanoutCapabilityTransitionLocked(
		runtime, candidate, changes, inventory); err == nil || !strings.Contains(err.Error(), "worker provider") {
		t.Fatalf("capture worker provider change was accepted: %v", err)
	}
	runtime.capabilityPlan = currentPlan
	candidate.Live.Config.Width = 642
	if _, _, err := resolveWorkerFanoutGraphCapabilityLocked(runtime, candidate, inventory); err == nil ||
		!strings.Contains(err.Error(), "violates admitted alignment") {
		t.Fatalf("misaligned dynamic encoder geometry was accepted: %v", err)
	}
}

func workerFanoutCapabilityGraph(candidate bool) productconfig.SourceGraph {
	source := productconfig.SourceConfig{
		Identity: "camera-a", Transport: "v4l2_uvc", Device: "/dev/nemesi-video-sources/camera-a",
		Format: "mjpeg", Width: 1280, Height: 720, FPS: 30,
		Processing: productconfig.ProcessingConfig{Orientation: "normal"},
		Acceleration: productconfig.AccelerationConfig{
			EncodeProvider: "native_sp", DecodeProvider: "native_sp",
			RequireHardwareCodec: true, AllowSoftwareFallback: false,
		},
	}
	graph := productconfig.SourceGraph{
		SourceID: "camera-a", Source: source,
		Analysis: workerFanoutTestStream("analysis", "analysis", "camera-a", "/run/analysis.sock"),
		Archive:  workerFanoutTestStream("archive", "archive", "camera-a", "/run/archive.sock"),
	}
	graph.Analysis.Config.Width, graph.Analysis.Config.Height, graph.Analysis.Config.FPS = 640, 360, 20
	graph.Archive.Config.Codec = "auto"
	if candidate {
		graph.Live = workerFanoutTestStream("live", "live", "camera-a", "/run/live.sock")
		graph.Live.Config.Width, graph.Live.Config.Height, graph.Live.Config.FPS = 640, 360, 20
	}
	return graph
}

func workerFanoutCapabilityRuntime(graph productconfig.SourceGraph) *videoBrokerRuntime {
	plan := sourceMoveTestPlan(graph, nil)
	plan.EncodeProvider = "native_sp"
	plan.DecodeProvider = "native_sp"
	plan.VideoOrientation = "normal"
	return &videoBrokerRuntime{
		plan: plan, observedCaptureCodec: "mjpeg",
		activeCaptureWidth: graph.Source.Width, activeCaptureHeight: graph.Source.Height,
		activeCaptureFPS: graph.Source.FPS,
	}
}

func workerFanoutCapabilityInventory(vpuCapacity, alignment int) capability.Inventory {
	return capability.Inventory{Schema: capability.Schema, Components: []capability.Component{
		{
			ID: "native-worker", Backend: "native_sp", Available: true, State: capability.StateAttested,
			ConcurrencyMode: capability.ConcurrencyShareable,
			Operations:      []capability.Operation{{Stage: capability.StageCapture, Format: "v4l2_uvc"}},
		},
		{
			ID: "jpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateAttested,
			ConcurrencyMode: capability.ConcurrencyFinite, ConcurrencyGroup: "jpeg", MaxConcurrentOperations: 2,
			Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}},
		},
		{
			ID: "vpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateAttested,
			ConcurrencyMode: capability.ConcurrencyFinite, ConcurrencyGroup: "video", MaxConcurrentOperations: vpuCapacity,
			Operations: []capability.Operation{{
				Stage: capability.StageEncode, Format: "h265",
				WidthAlignment: alignment, HeightAlignment: 2,
			}},
		},
		{
			ID: "neon", Backend: "native_neon", Hardware: true, Available: true, State: capability.StateAttested,
			ConcurrencyMode: capability.ConcurrencyShareable,
			Operations:      []capability.Operation{{Stage: capability.StageResize, Format: "nv12"}},
		},
	}}
}
