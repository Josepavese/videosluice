package videobroker

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/internal/broker"
	"github.com/Josepavese/videosluice/routing"
)

func TestEmptyV3ConfigurationIsRejected(t *testing.T) {
	service, err := Start(context.Background(), config.Config{SchemaVersion: config.SchemaVersion})
	if err == nil || service != nil {
		t.Fatalf("empty configuration unexpectedly started: service=%v err=%v", service, err)
	}
}

func TestNilServiceOperationsAreExplicit(t *testing.T) {
	var service *Service
	if err := service.Wait(context.Background()); err == nil {
		t.Fatal("nil service wait unexpectedly succeeded")
	}
	status := service.Snapshot(time.Unix(1, 0))
	if status.Schema != "video-broker.status.v5" || status.State != "stopped" || status.Sources == nil || status.Streams == nil {
		t.Fatalf("nil service status=%+v", status)
	}
}

func TestRoutingPlanSnapshotIsDetachedAndRawServiceIsRejected(t *testing.T) {
	plan := routing.Plan{
		Schema:             routing.PlanSchema,
		CatalogRevision:    "sha256:" + strings.Repeat("a", 64),
		Revision:           4,
		Selections:         map[string]string{"live": "front_balanced"},
		StreamGenerations:  map[string]uint64{"live": 7},
		RollbackSelections: map[string]string{"live": "overview_balanced"},
	}
	service := &Service{inner: &broker.Service{}, routingPlan: cloneRoutingPlan(plan), routingBound: true}
	view, err := service.RoutingPlan()
	if err != nil {
		t.Fatal(err)
	}
	view.Selections["live"] = "tampered"
	view.StreamGenerations["live"] = 99
	view.RollbackSelections["live"] = "tampered"
	fresh, err := service.RoutingPlan()
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Selections["live"] != "front_balanced" || fresh.StreamGenerations["live"] != 7 ||
		fresh.RollbackSelections["live"] != "overview_balanced" {
		t.Fatalf("routing authority was mutable through returned view: %+v", fresh)
	}
	if _, err := (&Service{inner: &broker.Service{}}).RoutingPlan(); err == nil {
		t.Fatal("raw-config service exposed a fabricated routing identity")
	}
}

func TestStartRoutedRejectsPlanBeforeStartingInvalidGraph(t *testing.T) {
	catalog := serviceRoutingCatalog()
	plan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	plan.Selections["live"] = "untrusted_raw_override"
	service, err := StartRouted(context.Background(), catalog, plan)
	if err == nil || service != nil {
		t.Fatalf("invalid routed plan reached service startup: service=%v err=%v", service, err)
	}
}

type serviceRoutingRuntimeStub struct {
	calls       []string
	prepareN    int
	validateN   int
	prepared    *servicePreparedGraphStub
	prepareErr  error
	validateErr error
}

func (runtime *serviceRoutingRuntimeStub) ValidateGraph(
	_ context.Context,
	_ config.Config,
	_ config.Config,
	_ routing.Plan,
	_ routing.Plan,
	_ routing.GraphDelta,
) error {
	runtime.validateN++
	return runtime.validateErr
}

func (runtime *serviceRoutingRuntimeStub) PrepareGraph(
	_ context.Context,
	_ config.Config,
	_ config.Config,
	_ routing.Plan,
	_ routing.Plan,
	_ routing.GraphDelta,
) (routing.PreparedGraph, error) {
	runtime.prepareN++
	runtime.calls = append(runtime.calls, routing.ApplyPhasePrepare)
	if runtime.prepareErr != nil {
		return nil, runtime.prepareErr
	}
	if runtime.prepared == nil {
		return nil, nil
	}
	runtime.prepared.runtime = runtime
	return runtime.prepared, nil
}

type servicePreparedGraphStub struct {
	runtime   *serviceRoutingRuntimeStub
	verifyErr error
	retireErr error
}

func (prepared *servicePreparedGraphStub) record(phase string) {
	prepared.runtime.calls = append(prepared.runtime.calls, phase)
}

func (prepared *servicePreparedGraphStub) Warmup(context.Context) error {
	prepared.record(routing.ApplyPhaseWarmup)
	return nil
}

func (prepared *servicePreparedGraphStub) Admit(context.Context) error {
	prepared.record(routing.ApplyPhaseAdmission)
	return nil
}

func (prepared *servicePreparedGraphStub) AwaitFirstFrame(context.Context) error {
	prepared.record(routing.ApplyPhaseFirstFrame)
	return nil
}

func (prepared *servicePreparedGraphStub) Cutover(context.Context) error {
	prepared.record(routing.ApplyPhaseCutover)
	return nil
}

func (prepared *servicePreparedGraphStub) Verify(context.Context) error {
	prepared.record(routing.ApplyPhaseVerify)
	return prepared.verifyErr
}

func (prepared *servicePreparedGraphStub) Rollback(context.Context) error {
	prepared.record("rollback")
	return nil
}

func (prepared *servicePreparedGraphStub) Abort(context.Context) error {
	prepared.record("abort")
	return nil
}

func (prepared *servicePreparedGraphStub) Retire(context.Context) error {
	prepared.record(routing.ApplyPhaseRetire)
	return prepared.retireErr
}

func TestRoutedServiceAdvancesIdentityOnlyAfterVerifiedPersistence(t *testing.T) {
	catalog := serviceRoutingCatalog()
	currentPlan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	currentConfig, err := routing.Materialize(catalog, currentPlan)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &serviceRoutingRuntimeStub{prepared: &servicePreparedGraphStub{}}
	service := &Service{
		inner: &broker.Service{}, routingConfig: currentConfig,
		routingPlan: cloneRoutingPlan(currentPlan), routingRuntime: runtime, routingBound: true,
	}
	store, err := routing.NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := service.ApplyRouting(
		context.Background(), catalog, store, 1, map[string]string{"live": "more_fluid"},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantCalls := []string{
		routing.ApplyPhasePrepare, routing.ApplyPhaseWarmup, routing.ApplyPhaseAdmission,
		routing.ApplyPhaseFirstFrame, routing.ApplyPhaseCutover, routing.ApplyPhaseVerify,
		routing.ApplyPhaseRetire,
	}
	if !reflect.DeepEqual(runtime.calls, wantCalls) {
		t.Fatalf("service transaction phases=%v want=%v", runtime.calls, wantCalls)
	}
	bound, err := service.RoutingPlan()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bound, candidate) || bound.Revision != 2 || bound.StreamGenerations["live"] != 2 {
		t.Fatalf("running identity=%+v candidate=%+v", bound, candidate)
	}

	runtime.calls = nil
	runtime.prepared = &servicePreparedGraphStub{}
	rolledBack, err := service.RollbackRouting(context.Background(), catalog, store, 2)
	if err != nil {
		t.Fatal(err)
	}
	bound, err = service.RoutingPlan()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bound, rolledBack) || bound.Revision != 3 ||
		bound.Selections["live"] != "balanced" || bound.StreamGenerations["live"] != 3 {
		t.Fatalf("running rollback identity=%+v result=%+v", bound, rolledBack)
	}
	if !reflect.DeepEqual(runtime.calls, wantCalls) {
		t.Fatalf("service rollback phases=%v want=%v", runtime.calls, wantCalls)
	}
}

func TestRoutedServicePreviewUsesRuntimeWithoutPreparingOrPersisting(t *testing.T) {
	catalog := serviceRoutingCatalog()
	currentPlan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	currentConfig, err := routing.Materialize(catalog, currentPlan)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &serviceRoutingRuntimeStub{}
	service := &Service{
		inner: &broker.Service{}, routingConfig: currentConfig,
		routingPlan: cloneRoutingPlan(currentPlan), routingRuntime: runtime, routingBound: true,
	}
	store, err := routing.NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	preview, err := service.PreviewRouting(
		context.Background(), catalog, store, 1, map[string]string{"live": "more_fluid"})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.validateN != 1 || runtime.prepareN != 0 || preview.Candidate.Revision != 2 {
		t.Fatalf("unexpected preview boundary: preview=%+v validate=%d prepare=%d", preview, runtime.validateN, runtime.prepareN)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := service.RoutingPlan()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, currentPlan) || !reflect.DeepEqual(bound, currentPlan) {
		t.Fatalf("preview changed authority: persisted=%+v bound=%+v", persisted, bound)
	}
}

func TestRoutedServicePreviewRejectsRuntimeAndAuthorityDivergence(t *testing.T) {
	catalog := serviceRoutingCatalog()
	currentPlan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	currentConfig, err := routing.Materialize(catalog, currentPlan)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("profile is not currently admissible")
	for name, configure := range map[string]func(*Service, *serviceRoutingRuntimeStub){
		"runtime": func(_ *Service, runtime *serviceRoutingRuntimeStub) { runtime.validateErr = cause },
		"plan divergence": func(service *Service, _ *serviceRoutingRuntimeStub) {
			service.routingPlan.Revision++
		},
		"graph divergence": func(service *Service, _ *serviceRoutingRuntimeStub) {
			stream := service.routingConfig.Streams["live"]
			stream.FPS++
			service.routingConfig.Streams["live"] = stream
		},
	} {
		t.Run(name, func(t *testing.T) {
			runtime := &serviceRoutingRuntimeStub{}
			service := &Service{
				inner: &broker.Service{}, routingConfig: currentConfig,
				routingPlan: cloneRoutingPlan(currentPlan), routingRuntime: runtime, routingBound: true,
			}
			configure(service, runtime)
			store, err := routing.NewStore(filepath.Join(t.TempDir(), "routing"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.PreviewRouting(
				context.Background(), catalog, store, 1,
				map[string]string{"live": "more_fluid"},
			); err == nil {
				t.Fatal("unsafe runtime preview was accepted")
			}
			persisted, err := store.Read(catalog)
			if err != nil {
				t.Fatal(err)
			}
			if persisted.Revision != 1 {
				t.Fatalf("rejected preview persisted revision=%d", persisted.Revision)
			}
		})
	}
}

func TestRoutedServiceRetireFailureStillTracksPersistedCandidate(t *testing.T) {
	catalog := serviceRoutingCatalog()
	currentPlan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	currentConfig, err := routing.Materialize(catalog, currentPlan)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("superseded graph retirement failed")
	runtime := &serviceRoutingRuntimeStub{prepared: &servicePreparedGraphStub{retireErr: cause}}
	service := &Service{
		inner: &broker.Service{}, routingConfig: currentConfig,
		routingPlan: cloneRoutingPlan(currentPlan), routingRuntime: runtime, routingBound: true,
	}
	store, err := routing.NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := service.ApplyRouting(
		context.Background(), catalog, store, 1, map[string]string{"live": "more_fluid"},
	)
	if err == nil || !errors.Is(err, cause) {
		t.Fatalf("retire failure was hidden: %v", err)
	}
	bound, readErr := service.RoutingPlan()
	if readErr != nil {
		t.Fatal(readErr)
	}
	persisted, readErr := store.Read(catalog)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if candidate.Revision != 2 || !reflect.DeepEqual(bound, candidate) || !reflect.DeepEqual(persisted, candidate) {
		t.Fatalf("persisted candidate identity diverged after retire failure: result=%+v bound=%+v persisted=%+v", candidate, bound, persisted)
	}
}

func TestRoutedServiceFailureKeepsCurrentIdentityAndPlan(t *testing.T) {
	catalog := serviceRoutingCatalog()
	currentPlan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	currentConfig, err := routing.Materialize(catalog, currentPlan)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("candidate health failed")
	runtime := &serviceRoutingRuntimeStub{prepared: &servicePreparedGraphStub{verifyErr: cause}}
	service := &Service{
		inner: &broker.Service{}, routingConfig: currentConfig,
		routingPlan: cloneRoutingPlan(currentPlan), routingRuntime: runtime, routingBound: true,
	}
	store, err := routing.NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ApplyRouting(
		context.Background(), catalog, store, 1, map[string]string{"live": "more_fluid"},
	); err == nil || !errors.Is(err, cause) {
		t.Fatalf("failed service transition returned: %v", err)
	}
	bound, err := service.RoutingPlan()
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bound, currentPlan) || !reflect.DeepEqual(persisted, currentPlan) {
		t.Fatalf("failed transition advanced authority: bound=%+v persisted=%+v", bound, persisted)
	}
	if runtime.calls[len(runtime.calls)-1] != "rollback" {
		t.Fatalf("failed cutover was not rolled back: %v", runtime.calls)
	}
}

func TestRoutedServiceRejectsMissingDataPlaneAndAuthorityDivergence(t *testing.T) {
	catalog := serviceRoutingCatalog()
	currentPlan, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	currentConfig, err := routing.Materialize(catalog, currentPlan)
	if err != nil {
		t.Fatal(err)
	}
	for name, configure := range map[string]func(*Service, *serviceRoutingRuntimeStub){
		"missing data plane": func(service *Service, _ *serviceRoutingRuntimeStub) {
			service.routingRuntime = nil
		},
		"plan divergence": func(service *Service, _ *serviceRoutingRuntimeStub) {
			service.routingPlan.Revision = 9
		},
		"graph divergence": func(service *Service, _ *serviceRoutingRuntimeStub) {
			stream := service.routingConfig.Streams["live"]
			stream.FPS = 1
			service.routingConfig.Streams["live"] = stream
		},
	} {
		t.Run(name, func(t *testing.T) {
			runtime := &serviceRoutingRuntimeStub{prepared: &servicePreparedGraphStub{}}
			service := &Service{
				inner: &broker.Service{}, routingConfig: currentConfig,
				routingPlan: cloneRoutingPlan(currentPlan), routingRuntime: runtime, routingBound: true,
			}
			configure(service, runtime)
			store, err := routing.NewStore(filepath.Join(t.TempDir(), "routing"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.ApplyRouting(
				context.Background(), catalog, store, 1, map[string]string{"live": "more_fluid"},
			); err == nil {
				t.Fatal("unsafe service transition was accepted")
			}
			if runtime.prepareN != 0 {
				t.Fatalf("unsafe transition reached data plane: calls=%v", runtime.calls)
			}
			persisted, err := store.Read(catalog)
			if err != nil {
				t.Fatal(err)
			}
			if persisted.Revision != 1 {
				t.Fatalf("rejected transition persisted revision=%d", persisted.Revision)
			}
		})
	}
}

func serviceRoutingCatalog() routing.Catalog {
	return routing.Catalog{
		Schema:   routing.CatalogSchema,
		Revision: "sha256:" + strings.Repeat("a", 64),
		Sources: map[string]config.SourceConfig{
			"front": {
				Identity: "sha256:" + strings.Repeat("b", 64), Transport: "v4l2_uvc",
				Device: "/dev/nemesi-video-sources/front", Format: "h265",
				CodecPreference: "h265", CodecPolicy: "required", ModePolicy: "exact",
				AspectPolicy: "crop", Width: 1280, Height: 720, FPS: 20,
				Processing: config.ProcessingConfig{
					Orientation: "normal", ToneMapping: config.ToneMappingConfig{Profile: "adaptive", Mode: "off"},
					CameraControl: config.CameraControlOptimizerConfig{Policy: "off", Mode: "off"},
				},
				Acceleration: config.AccelerationConfig{
					EncodeProvider: "native_sp", DecodeProvider: "native_sp", RequireHardwareCodec: true,
					Discovery: config.CapabilityDiscoveryConfig{Mode: "auto", ProbeTimeoutMS: 8000},
				},
			},
		},
		Streams: map[string]routing.StreamCatalog{
			"live": {Type: "live", Socket: "/run/video/live.sock", Options: map[string]routing.StreamOption{
				"balanced":   {SourceRef: "front", Codec: "h265", Width: 1280, Height: 720, FPS: 20, BitrateKbps: 4096},
				"more_fluid": {SourceRef: "front", Codec: "h265", Width: 1280, Height: 720, FPS: 15, BitrateKbps: 3072},
			}},
		},
		DefaultSelections: map[string]string{"live": "balanced"},
		Runtime:           config.RuntimeConfig{RuntimeDirectory: "/run/video", StateDirectory: "/var/lib/video"},
	}
}
