package broker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/routing"
)

func TestFrameAccessStreamMoveUsesExistingExclusiveRestart(t *testing.T) {
	catalog := wholeSourceMoveCatalog(t, t.TempDir())
	plan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	current.Runtime.FrameAccess = &productconfig.FrameAccessConfig{CopyProvider: "cpu_nv12_copy", Readers: []productconfig.FrameReaderConfig{{ID: "reader", Stream: "analysis", DeliverySocket: "/run/video/frames.sock", ROISocket: "/run/video/roi.sock"}}}
	candidate := productconfig.Clone(current)
	stream := candidate.Streams["analysis"]
	stream.SourceRef = "camera-peer"
	candidate.Streams["analysis"] = stream
	delta, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !requiresSourceRestart(delta) {
		t.Fatal("analysis move reused an immutable frame capture")
	}
	candidate = productconfig.Clone(current)
	stream = candidate.Streams["live"]
	stream.FPS--
	candidate.Streams["live"] = stream
	delta, err = routing.DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if requiresSourceRestart(delta) {
		t.Fatal("unrelated live cadence restarted capture")
	}
}

func TestRoutingSourceModeRestartJoinsOwnerAndPreservesPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	catalog := wholeSourceMoveCatalog(t, t.TempDir())
	base := catalog.Sources["camera-a"]
	catalog.SourceModes = map[string]map[string]routing.SourceMode{"camera-a": {"qualified": {Format: base.Format, Width: base.Width + 16, Height: base.Height, FPS: base.FPS}}}
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	starter := newSourceMoveTestStarter(t)
	service, err := startServiceWithRestartPolicy(ctx, current, restartTestInventory(), starter.start, defaultSourceRestartPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = service.Wait(context.Background()) }()
	service.mu.RLock()
	old, peer := service.sources["camera-a"], service.sources["camera-peer"]
	service.mu.RUnlock()
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	modes := map[string]string{"camera-a": "qualified"}
	validator := NewRoutingRuntime(service).(routing.GraphValidator)
	preview, err := routing.PreviewRuntimeGraph(ctx, catalog, store, 1, initial.Selections, validator, modes)
	if err != nil {
		t.Fatal(err)
	}
	if starter.callCount() != 2 || len(preview.ChangedStreamIDs) != 3 {
		t.Fatal("preview opened a camera or lost shared consumers")
	}
	applied, err := routing.ApplyGraph(ctx, catalog, store, 1, initial.Selections, NewRoutingRuntime(service), modes)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-old.done:
	default:
		t.Fatal("old capture not joined")
	}
	service.mu.RLock()
	active, samePeer := service.sources["camera-a"], service.sources["camera-peer"]
	service.mu.RUnlock()
	if active == old || samePeer != peer || starter.callCount() != 3 {
		t.Fatal("capture ownership or peer changed incorrectly")
	}
	if applied.StreamGenerations["live"] != 2 || applied.StreamGenerations["analysis"] != 2 || applied.StreamGenerations["archive"] != 2 || applied.StreamGenerations["peer_live"] != 1 {
		t.Fatal("consumer generations not fenced")
	}
	active.mu.RLock()
	media := active.graph.runtime
	active.mu.RUnlock()
	media.mu.Lock()
	demand := media.routingLiveDemand
	media.mu.Unlock()
	if demand {
		t.Fatal("live warmup demand leaked after commit")
	}
	persisted, err := store.Read(catalog)
	if err != nil || !reflect.DeepEqual(applied, persisted) {
		t.Fatal("mode plan not durable")
	}
	rolled, err := routing.RollbackGraph(ctx, catalog, store, applied.Revision, NewRoutingRuntime(service))
	if err != nil {
		t.Fatal(err)
	}
	if len(rolled.SourceSelections) != 0 || starter.callCount() != 4 {
		t.Fatal("rollback did not restart base mode")
	}
}

func restartTestInventory() capability.Inventory {
	return capability.Inventory{Schema: capability.Schema, Components: []capability.Component{
		{ID: "worker", Backend: "native_sp", Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyShareable, Operations: []capability.Operation{{Stage: capability.StageCapture, Format: "v4l2_uvc"}}},
		{ID: "gpu", Backend: "native_opencl", Hardware: true, Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyFinite, ConcurrencyGroup: "gpu", MaxConcurrentOperations: 8, Operations: []capability.Operation{{Stage: capability.StageToneMap, Format: "nv12"}}},
		{ID: "scaler", Backend: "native_neon", Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyShareable, Operations: []capability.Operation{{Stage: capability.StageResize, Format: "nv12"}}},
		{ID: "jpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyFinite, ConcurrencyGroup: "jpu", MaxConcurrentOperations: 8, Operations: []capability.Operation{{Stage: capability.StageDecode, Format: "mjpeg"}}},
		{ID: "vpu", Backend: "native_sp", Hardware: true, Available: true, State: capability.StateCandidate, ConcurrencyMode: capability.ConcurrencyFinite, ConcurrencyGroup: "vpu", MaxConcurrentOperations: 8, Operations: []capability.Operation{{Stage: capability.StageEncode, Format: "h265"}, {Stage: capability.StageResize, Format: "nv12"}, {Stage: capability.StageToneMap, Format: "nv12"}}},
	}}
}

func TestRoutingSourceRestartPreflightDoesNotMutateOwnersOrLeases(t *testing.T) {
	for _, fault := range []string{"missing_decoder", "capacity_exhausted", "pending_transition"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			catalog := wholeSourceMoveCatalog(t, t.TempDir())
			base := catalog.Sources["camera-a"]
			catalog.SourceModes = map[string]map[string]routing.SourceMode{"camera-a": {"qualified": {Format: base.Format, Width: base.Width + 16, Height: base.Height, FPS: base.FPS}}}
			initial, _ := routing.DefaultPlan(catalog)
			current, _ := routing.Materialize(catalog, initial)
			starter := newSourceMoveTestStarter(t)
			service, err := startServiceWithRestartPolicy(ctx, current, restartTestInventory(), starter.start, defaultSourceRestartPolicy())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { cancel(); _ = service.Wait(context.Background()) }()
			service.mu.Lock()
			owner := service.sources["camera-a"]
			if fault == "missing_decoder" {
				for i := range service.inventory.Components {
					if service.inventory.Components[i].ID == "jpu" {
						service.inventory.Components[i].Available = false
					}
				}
			}
			service.mu.Unlock()
			service.allocator.mu.Lock()
			if fault == "capacity_exhausted" {
				service.allocator.claims["jpu"] = map[string]int{"external-owner": 8}
			}
			if fault == "pending_transition" {
				service.allocator.reservedBy["another-operation"] = map[string]bool{"camera-a": true}
			}
			before := fmt.Sprintf("%v/%v/%v", service.allocator.claims, service.allocator.reservations, service.allocator.reservedBy)
			service.allocator.mu.Unlock()
			store, _ := routingTestStore(t)
			modes := map[string]string{"camera-a": "qualified"}
			_, err = routing.PreviewRuntimeGraph(ctx, catalog, store, 1, initial.Selections, NewRoutingRuntime(service).(routing.GraphValidator), modes)
			if err == nil {
				t.Fatal("unsafe restart passed preview")
			}
			_, err = routing.ApplyGraph(ctx, catalog, store, 1, initial.Selections, NewRoutingRuntime(service), modes)
			if err == nil {
				t.Fatal("unsafe restart applied")
			}
			service.mu.RLock()
			unchanged := service.sources["camera-a"] == owner && reflect.DeepEqual(service.config, current)
			service.mu.RUnlock()
			service.allocator.mu.Lock()
			after := fmt.Sprintf("%v/%v/%v", service.allocator.claims, service.allocator.reservations, service.allocator.reservedBy)
			service.allocator.mu.Unlock()
			plan, err := store.Read(catalog)
			if !unchanged || before != after || starter.callCount() != 2 || err != nil || plan.Revision != 1 {
				t.Fatal("failed preflight changed active authority")
			}
		})
	}
}

func TestRoutingOutputResizeRestartsOnlySharedOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	catalog := wholeSourceMoveCatalog(t, t.TempDir())
	option := catalog.Streams["live"].Options["camera-a"]
	option.Width, option.Height = 640, 360
	catalog.Streams["live"].Options["small"] = option
	initial, _ := routing.DefaultPlan(catalog)
	current, _ := routing.Materialize(catalog, initial)
	starter := newSourceMoveTestStarter(t)
	service, err := startServiceWithRestartPolicy(ctx, current, restartTestInventory(), starter.start, defaultSourceRestartPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); _ = service.Wait(context.Background()) }()
	service.mu.RLock()
	owner, peer := service.sources["camera-a"], service.sources["camera-peer"]
	service.mu.RUnlock()
	store, _ := routingTestStore(t)
	selections := map[string]string{}
	for id, value := range initial.Selections {
		selections[id] = value
	}
	selections["live"] = "small"
	applied, err := routing.ApplyGraph(ctx, catalog, store, 1, selections, NewRoutingRuntime(service))
	if err != nil {
		t.Fatal(err)
	}
	service.mu.RLock()
	unchanged := service.sources["camera-peer"] == peer && reflect.DeepEqual(service.config.Sources["camera-a"], current.Sources["camera-a"])
	service.mu.RUnlock()
	select {
	case <-owner.done:
	default:
		t.Fatal("resized output kept old capture owner")
	}
	if !unchanged || starter.callCount() != 3 || applied.StreamGenerations["analysis"] != 2 || applied.StreamGenerations["archive"] != 2 || applied.StreamGenerations["peer_live"] != 1 {
		t.Fatal("resize did not fence shared consumers in isolation")
	}
}

func TestRoutingSourceRestartFailureRestoresAndAttestsPreviousMode(t *testing.T) {
	for _, fault := range []string{"camera_removed", "hardware_rejected", "first_frame_timeout"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			catalog := wholeSourceMoveCatalog(t, t.TempDir())
			base := catalog.Sources["camera-a"]
			catalog.SourceModes = map[string]map[string]routing.SourceMode{"camera-a": {"qualified": {Format: base.Format, Width: base.Width + 16, Height: base.Height, FPS: base.FPS}}}
			initial, err := routing.DefaultPlan(catalog)
			if err != nil {
				t.Fatal(err)
			}
			current, _ := routing.Materialize(catalog, initial)
			starter := newSourceMoveTestStarter(t)
			start := func(ctx context.Context, cfg productconfig.SourceGraph, admission []productconfig.SourceRoleProfile, inventory capability.Inventory, allocator *capabilityAllocator) (*graphService, error) {
				bad := cfg.SourceID == "camera-a" && cfg.Source.Width != base.Width
				if bad && fault == "camera_removed" {
					return nil, errors.New("test camera removed")
				}
				graph, err := starter.start(ctx, cfg, admission, inventory, allocator)
				if err == nil && bad {
					graph.runtime.mu.Lock()
					if fault == "hardware_rejected" {
						graph.runtime.capabilityPlan.Stages[0].State = capability.StateRejected
					} else if fault == "first_frame_timeout" {
						graph.runtime.mainBootstrap = nil
					}
					graph.runtime.mu.Unlock()
				}
				return graph, err
			}
			service, err := startServiceWithRestartPolicy(ctx, current, restartTestInventory(), start, defaultSourceRestartPolicy())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { cancel(); _ = service.Wait(context.Background()) }()
			store, _ := routingTestStore(t)
			applyCtx, cancelApply := context.WithTimeout(ctx, 200*time.Millisecond)
			defer cancelApply()
			_, err = routing.ApplyGraph(applyCtx, catalog, store, 1, initial.Selections, NewRoutingRuntime(service), map[string]string{"camera-a": "qualified"})
			var applyErr *routing.ApplyError
			if !errors.As(err, &applyErr) || applyErr.CleanupErr != nil {
				t.Fatalf("expected fully recovered failure: %v", err)
			}
			persisted, err := store.Read(catalog)
			if err != nil || persisted.Revision != 1 || len(persisted.SourceSelections) != 0 {
				t.Fatal("failed mode persisted")
			}
			service.mu.RLock()
			cfg := productconfig.Clone(service.config)
			supervisor := service.sources["camera-a"]
			service.mu.RUnlock()
			if !reflect.DeepEqual(cfg, current) {
				t.Fatal("rollback config differs")
			}
			supervisor.mu.RLock()
			media := supervisor.graph.runtime
			supervisor.mu.RUnlock()
			media.mu.Lock()
			demand, frames := media.routingLiveDemand, media.totalMainFrames
			media.mu.Unlock()
			if demand || frames == 0 {
				t.Fatal("rollback was not verified or left warmup demand")
			}
		})
	}
}
