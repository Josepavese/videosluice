package broker

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Josepavese/videosluice/capability"
)

func TestCapabilityAllocatorIsolatesHardwareAcrossSourcesAndReleasesIt(t *testing.T) {
	inventory := capability.Inventory{Schema: capability.Schema, Components: []capability.Component{
		{ID: "native.v4l2-worker", Backend: "native_sp", Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyShareable, Operations: []capability.Operation{{Stage: capability.StageCapture, Format: "v4l2_uvc"}}},
		{ID: "gstreamer.worker", Backend: "gstreamer", Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyShareable, Operations: []capability.Operation{{Stage: capability.StageCapture, Format: "v4l2_uvc"}}},
		{ID: "native.sp-vpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyExclusive, ConcurrencyGroup: "video-codec", MaxConcurrentOperations: 1, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h264"}}},
		{ID: "gstreamer.vendor-codec", Backend: "gstreamer_vendor", Hardware: true, Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyExclusive, ConcurrencyGroup: "video-codec", MaxConcurrentOperations: 1, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h264"}}},
		{ID: "gstreamer.software-codec", Backend: "software", Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyShareable, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h264"}}},
	}}
	policy := capability.ProviderPolicy{EncodeProvider: "auto", DecodeProvider: "auto", AllowSoftwareFallback: true}
	requirements := capability.Requirements{Transport: "v4l2_uvc", CaptureFormat: "nv12", LiveCodec: "h264", LiveEncode: true}
	allocator := newCapabilityAllocator()

	first, err := allocator.resolve("camera-a", inventory, policy, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if first.WorkerProvider != "native_sp" || first.EncodeProvider != "native_sp" {
		t.Fatalf("first source did not receive deterministic hardware ownership: %+v", first)
	}
	second, err := allocator.resolve("camera-b", inventory, policy, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if second.WorkerProvider != "gstreamer" || second.EncodeProvider != "software" {
		t.Fatalf("second source reused leased hardware instead of permitted fallback: %+v", second)
	}
	firstAgain, err := allocator.resolve("camera-a", inventory, policy, requirements)
	if err != nil || firstAgain.EncodeProvider != "native_sp" {
		t.Fatalf("source did not retain its own hardware claim: plan=%+v err=%v", firstAgain, err)
	}

	allocator.release("camera-a")
	secondAfterRelease, err := allocator.resolve("camera-b", inventory, policy, requirements)
	if err != nil || secondAfterRelease.WorkerProvider != "native_sp" || secondAfterRelease.EncodeProvider != "native_sp" {
		t.Fatalf("released hardware was not reassigned: plan=%+v err=%v", secondAfterRelease, err)
	}
}

func TestCapabilityAllocatorDeniedPeerCannotMutateActiveSourcePlan(t *testing.T) {
	inventory := capability.Inventory{Schema: capability.Schema, Components: []capability.Component{
		{ID: "native.v4l2-worker", Backend: "native_sp", Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyShareable, Operations: []capability.Operation{{Stage: capability.StageCapture, Format: "v4l2_uvc"}}},
		{ID: "native.sp-vpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyExclusive, ConcurrencyGroup: "video-codec", MaxConcurrentOperations: 1, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h265"}}},
	}}
	policy := capability.ProviderPolicy{
		EncodeProvider: "native_sp", DecodeProvider: "native_sp",
		RequireHardwareCodec: true, AllowSoftwareFallback: false,
	}
	requirements := capability.Requirements{
		Transport: "v4l2_uvc", CaptureFormat: "nv12", LiveCodec: "h265", LiveEncode: true,
	}
	allocator := newCapabilityAllocator()
	first, err := allocator.resolve("front", inventory, policy, requirements)
	if err != nil || first.EncodeProvider != "native_sp" {
		t.Fatalf("first source did not acquire hardware: plan=%+v err=%v", first, err)
	}
	if _, err := allocator.resolve("side", inventory, policy, requirements); err == nil {
		t.Fatal("second strict source reused unasserted hardware capacity")
	}
	firstAfterDenial, err := allocator.resolve("front", inventory, policy, requirements)
	if err != nil || firstAfterDenial.WorkerProvider != first.WorkerProvider ||
		firstAfterDenial.EncodeProvider != first.EncodeProvider {
		t.Fatalf("peer lease denial mutated the active source plan: before=%+v after=%+v err=%v", first, firstAfterDenial, err)
	}
}

func TestCapabilityAllocatorHonorsAttestedConcurrentOperationCapacity(t *testing.T) {
	inventory := capability.Inventory{Schema: capability.Schema, Components: []capability.Component{
		{ID: "native.v4l2-worker", Backend: "native_sp", Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyShareable, Operations: []capability.Operation{{Stage: capability.StageCapture, Format: "v4l2_uvc"}}},
		{ID: "native.sp-vpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyFinite, ConcurrencyGroup: "video-codec", MaxConcurrentOperations: 3, Operations: []capability.Operation{
			{Stage: capability.StageEncode, Format: "h265"},
			{Stage: capability.StageDecode, Format: "h265"},
		}},
	}}
	policy := capability.ProviderPolicy{
		EncodeProvider: "native_sp", DecodeProvider: "native_sp",
		RequireHardwareCodec: true, AllowSoftwareFallback: false,
	}
	front := capability.Requirements{
		Transport: "v4l2_uvc", CaptureFormat: "h265", ArchiveCodec: "h265", ArchiveEncode: true,
	}
	overview := capability.Requirements{
		Transport: "v4l2_uvc", CaptureFormat: "h265",
	}
	allocator := newCapabilityAllocator()
	if _, err := allocator.resolve("front", inventory, policy, front); err != nil {
		t.Fatalf("two-operation front graph was rejected: %v", err)
	}
	if _, err := allocator.resolve("overview", inventory, policy, overview); err != nil {
		t.Fatalf("third VPU operation was rejected: %v", err)
	}
	if _, err := allocator.resolve("side", inventory, policy, overview); err == nil {
		t.Fatal("allocator exceeded the declared three-operation capacity")
	}
	allocator.release("front")
	if _, err := allocator.resolve("side", inventory, policy, overview); err != nil {
		t.Fatalf("released VPU capacity was not reusable: %v", err)
	}
}

func TestCapabilityAllocatorDoesNotLeaseShareableHardwareImplementation(t *testing.T) {
	allocator := newCapabilityAllocator()
	inventory := capability.Inventory{Schema: capability.Schema, Components: []capability.Component{
		{ID: "native.v4l2-worker", Backend: "native_sp", Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyShareable, Operations: []capability.Operation{{Stage: capability.StageCapture, Format: "v4l2_uvc"}}},
		{ID: "cpu.neon-nv12", Backend: "native_neon", Hardware: true, ConcurrencyMode: capability.ConcurrencyShareable, Available: true, State: capability.StateCandidate, Operations: []capability.Operation{{Stage: capability.StageResize, Format: "nv12"}}},
	}}
	policy := capability.ProviderPolicy{EncodeProvider: "native_sp", DecodeProvider: "native_sp"}
	requirements := capability.Requirements{Transport: "v4l2_uvc", CaptureFormat: "nv12", ResizeRoles: []string{"analysis"}}
	for _, sourceID := range []string{"front", "overview", "side", "rear", "ceiling"} {
		if _, err := allocator.resolve(sourceID, inventory, policy, requirements); err != nil {
			t.Fatalf("shareable NEON component rejected source %q: %v", sourceID, err)
		}
	}
	if len(allocator.claims) != 0 {
		t.Fatalf("shareable component created exclusive claims: %+v", allocator.claims)
	}
}

func TestCapabilityAllocatorRejectsMissingOrContradictoryResourceContracts(t *testing.T) {
	worker := capability.Component{
		ID: "native.v4l2-worker", Backend: "native_sp", Available: true,
		State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyShareable,
		Operations: []capability.Operation{{Stage: capability.StageCapture, Format: "v4l2_uvc"}},
	}
	requirements := capability.Requirements{Transport: "v4l2_uvc", CaptureFormat: "nv12", LiveCodec: "h265", LiveEncode: true}
	policy := capability.ProviderPolicy{EncodeProvider: "native_sp", RequireHardwareCodec: true}
	for label, components := range map[string][]capability.Component{
		"missing mode": {
			worker,
			{ID: "native.sp-vpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate, ConcurrencyGroup: "video-codec", MaxConcurrentOperations: 1, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h265"}}},
		},
		"contradictory aliases": {
			worker,
			{ID: "native.sp-vpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyFinite, ConcurrencyGroup: "video-codec", MaxConcurrentOperations: 32, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h265"}}},
			{ID: "gstreamer.vendor-codec", Backend: "gstreamer_vendor", Hardware: true, Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyExclusive, ConcurrencyGroup: "video-codec", MaxConcurrentOperations: 1, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h265"}}},
		},
	} {
		if _, err := newCapabilityAllocator().resolve("front", capability.Inventory{Schema: capability.Schema, Components: components}, policy, requirements); err == nil {
			t.Fatalf("allocator accepted %s concurrency contract", label)
		}
	}
}

func TestCapabilityAllocatorTransitionReservesPeakWarmupAndCommitsAtomically(t *testing.T) {
	inventory := capabilityTransitionInventory(3, 2)
	allocator := newCapabilityAllocator()
	current := map[string]capability.Plan{
		"camera-a": capabilityTestPlan("generic-jpu", "generic-vpu", "generic-vpu"),
		"camera-b": capabilityTestPlan("generic-jpu"),
	}
	candidate := map[string]capability.Plan{
		"camera-a": capabilityTestPlan("generic-jpu", "generic-vpu"),
		"camera-b": capabilityTestPlan("generic-jpu", "generic-vpu"),
	}
	seedCapabilityClaims(t, allocator, inventory, current)

	warmup := map[string]capability.Plan{"camera-b": capabilityTestPlan("generic-vpu")}
	if _, err := allocator.reserveTransition(
		"route-under", inventory, current, candidate, map[string]capability.Plan{}); err == nil ||
		!strings.Contains(err.Error(), "under-reserves") {
		t.Fatalf("candidate capacity increase was accepted without a warmup reservation: %v", err)
	}
	reservation, err := allocator.reserveTransition("route-1", inventory, current, candidate, warmup)
	if err != nil {
		t.Fatal(err)
	}
	if got := allocator.reservations["route-1"]; !reflect.DeepEqual(got, map[string]int{"video-codec": 1}) {
		t.Fatalf("transition did not reserve only incremental warmup capacity: %+v", got)
	}
	if _, err := allocator.reserveTransition(
		"route-overlap", inventory, current, candidate, warmup); err == nil ||
		!strings.Contains(err.Error(), "already reserved") {
		t.Fatalf("overlapping source transition was accepted: %v", err)
	}
	thirdCurrent := map[string]capability.Plan{"camera-c": {}}
	thirdCandidate := map[string]capability.Plan{"camera-c": capabilityTestPlan("generic-vpu")}
	if _, err := allocator.reserveTransition(
		"route-2", inventory, thirdCurrent, thirdCandidate, thirdCandidate); err == nil ||
		!strings.Contains(err.Error(), "lacks prepare capacity") {
		t.Fatalf("parallel reservation exceeded attested peak capacity: %v", err)
	}
	if err := reservation.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := reservation.Commit(); err != nil {
		t.Fatalf("transition commit is not idempotent: %v", err)
	}
	if got := allocator.claims["video-codec"]; !reflect.DeepEqual(got, map[string]int{
		"camera-a": 1, "camera-b": 1,
	}) {
		t.Fatalf("committed claims do not match the candidate graph: %+v", got)
	}
	if _, exists := allocator.reservations["route-1"]; exists {
		t.Fatal("committed transition retained temporary capacity")
	}

	third, err := allocator.reserveTransition(
		"route-3", inventory, thirdCurrent, thirdCandidate, thirdCandidate)
	if err != nil {
		t.Fatalf("capacity released by commit was not reusable: %v", err)
	}
	if err := third.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := third.Abort(); err != nil {
		t.Fatalf("transition abort is not idempotent: %v", err)
	}
	if len(allocator.reservations) != 0 {
		t.Fatalf("aborted transition leaked reservations: %+v", allocator.reservations)
	}
}

func TestCapabilityAllocatorTransitionRejectsStaleOwnershipAtCommit(t *testing.T) {
	inventory := capabilityTransitionInventory(2, 1)
	allocator := newCapabilityAllocator()
	current := map[string]capability.Plan{"camera-a": capabilityTestPlan("generic-vpu")}
	candidate := map[string]capability.Plan{"camera-a": capabilityTestPlan("generic-vpu")}
	seedCapabilityClaims(t, allocator, inventory, current)
	reservation, err := allocator.reserveTransition(
		"route-stale", inventory, current, candidate, map[string]capability.Plan{})
	if err != nil {
		t.Fatal(err)
	}
	allocator.release("camera-a")
	if err := reservation.Commit(); err == nil || !strings.Contains(err.Error(), "changed before commit") {
		t.Fatalf("stale source ownership was committed: %v", err)
	}
	if err := reservation.Abort(); err != nil {
		t.Fatal(err)
	}
	if len(allocator.reservations) != 0 {
		t.Fatal("failed stale commit leaked its temporary reservation")
	}
}

func TestCapabilityAllocatorTransitionIsStageAndBackendNeutral(t *testing.T) {
	for _, test := range []struct {
		name      string
		component capability.Component
	}{
		{name: "vpu", component: exclusiveCapabilityComponent("generic-vpu", "video-codec", capability.StageEncode)},
		{name: "jpu", component: exclusiveCapabilityComponent("generic-jpu", "jpeg-codec", capability.StageDecode)},
		{name: "gpu", component: exclusiveCapabilityComponent("generic-gpu", "compute-engine", capability.StageToneMap)},
		{name: "scaler", component: exclusiveCapabilityComponent("generic-scaler", "transform-engine", capability.StageResize)},
		{name: "npu", component: exclusiveCapabilityComponent("generic-npu", "inference-engine", "inference")},
	} {
		t.Run(test.name, func(t *testing.T) {
			inventory := capability.Inventory{Schema: capability.Schema, Components: []capability.Component{test.component}}
			allocator := newCapabilityAllocator()
			current := map[string]capability.Plan{
				"camera-a": capabilityTestPlan(test.component.ID),
				"camera-b": {},
			}
			candidate := map[string]capability.Plan{
				"camera-a": {},
				"camera-b": capabilityTestPlan(test.component.ID),
			}
			seedCapabilityClaims(t, allocator, inventory, current)
			if _, err := allocator.reserveTransition(
				"route-exclusive", inventory, current, candidate,
				map[string]capability.Plan{"camera-b": capabilityTestPlan(test.component.ID)}); err == nil ||
				!strings.Contains(err.Error(), "lacks prepare capacity") {
				t.Fatalf("exclusive %s was overcommitted during warmup: %v", test.name, err)
			}
			if got := allocator.claims[test.component.ConcurrencyGroup]; !reflect.DeepEqual(got, map[string]int{"camera-a": 1}) {
				t.Fatalf("rejected %s transition mutated current claims: %+v", test.name, got)
			}
		})
	}
}

func capabilityTransitionInventory(vpuCapacity, jpuCapacity int) capability.Inventory {
	return capability.Inventory{Schema: capability.Schema, Components: []capability.Component{
		{
			ID: "generic-vpu", Backend: "native_sp", Hardware: true, Available: true,
			State: capability.StateAttested, ConcurrencyMode: capability.ConcurrencyFinite,
			ConcurrencyGroup: "video-codec", MaxConcurrentOperations: vpuCapacity,
			Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h265"}},
		},
		{
			ID: "generic-jpu", Backend: "native_sp", Hardware: true, Available: true,
			State: capability.StateAttested, ConcurrencyMode: capability.ConcurrencyFinite,
			ConcurrencyGroup: "jpeg-codec", MaxConcurrentOperations: jpuCapacity,
			Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}},
		},
	}}
}

func exclusiveCapabilityComponent(id, group, stage string) capability.Component {
	return capability.Component{
		ID: id, Backend: "test", Hardware: true, Available: true,
		State: capability.StateAttested, ConcurrencyMode: capability.ConcurrencyExclusive,
		ConcurrencyGroup: group, MaxConcurrentOperations: 1,
		Operations: []capability.Operation{{Stage: stage}},
	}
}

func capabilityTestPlan(componentIDs ...string) capability.Plan {
	plan := capability.Plan{}
	for index, componentID := range componentIDs {
		stage, format := capability.StageEncode, "h265"
		switch {
		case strings.Contains(componentID, "jpu"):
			stage, format = capability.StageDecode, "mjpeg"
		case strings.Contains(componentID, "gpu"):
			stage, format = capability.StageToneMap, "nv12"
		case strings.Contains(componentID, "scaler"):
			stage, format = capability.StageResize, "nv12"
		case strings.Contains(componentID, "npu"):
			stage, format = "inference", "nv12"
		}
		plan.Stages = append(plan.Stages, capability.StageSelection{
			Stage: stage, Role: fmt.Sprintf("role-%d", index), Format: format,
			Component: componentID, Backend: "test", Hardware: true,
			State: capability.StateAttested,
		})
	}
	return plan
}

func seedCapabilityClaims(
	t *testing.T,
	allocator *capabilityAllocator,
	inventory capability.Inventory,
	plans map[string]capability.Plan,
) {
	t.Helper()
	for sourceID, plan := range plans {
		claims, err := strictPlanHardwareClaims(plan, inventory)
		if err != nil {
			t.Fatal(err)
		}
		for resourceID, claim := range claims {
			if allocator.claims[resourceID] == nil {
				allocator.claims[resourceID] = map[string]int{}
			}
			allocator.claims[resourceID][sourceID] = claim.units
		}
	}
}
