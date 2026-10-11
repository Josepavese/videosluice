// Package videobroker provides a camera-independent capture, processing and
// multi-consumer fan-out service.
package videobroker

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/internal/broker"
	"github.com/Josepavese/videosluice/routing"
)

type Status = broker.Status
type SourceStatus = broker.SourceStatus
type StreamStatus = broker.StreamStatus
type StreamPerformance = broker.StreamPerformance
type StartOptions = broker.StartOptions

type Service struct {
	inner *broker.Service

	routingMu      sync.RWMutex
	routingConfig  config.Config
	routingPlan    routing.Plan
	routingRuntime routing.GraphRuntime
	routingBound   bool
}

func Start(ctx context.Context, cfg config.Config, options ...StartOptions) (*Service, error) {
	inner, err := broker.Start(ctx, cfg, options...)
	if err != nil {
		return nil, err
	}
	return &Service{inner: inner}, nil
}

// StartRouted starts a graph whose runtime identity is bound to the exact
// current-only operator plan that materialized it. Keeping revision and stream
// generations beside the running Service is required for a later live cutover;
// reconstructing them from media endpoints would be ambiguous and unsafe.
func StartRouted(ctx context.Context, catalog routing.Catalog, plan routing.Plan, options ...StartOptions) (*Service, error) {
	plan = cloneRoutingPlan(plan)
	normalizedCatalog, err := routing.NormalizeCatalog(catalog)
	if err != nil {
		return nil, err
	}
	cfg, err := routing.Materialize(normalizedCatalog, plan)
	if err != nil {
		return nil, err
	}
	admission, err := routing.AdmissionProfiles(normalizedCatalog)
	if err != nil {
		return nil, err
	}
	inner, err := broker.StartRouted(ctx, cfg, admission, options...)
	if err != nil {
		return nil, err
	}
	return &Service{
		inner: inner, routingConfig: config.Clone(cfg), routingPlan: plan,
		routingRuntime: broker.NewRoutingRuntime(inner), routingBound: true,
	}, nil
}

// RoutingPlan returns a detached copy of the plan bound to the running graph.
// Services started from a raw --config intentionally have no routing identity
// and cannot participate in runtime ApplyGraph transactions.
func (service *Service) RoutingPlan() (routing.Plan, error) {
	if service == nil || service.inner == nil {
		return routing.Plan{}, errors.New("nil video-broker service")
	}
	service.routingMu.RLock()
	defer service.routingMu.RUnlock()
	if !service.routingBound {
		return routing.Plan{}, errors.New("video-broker service is not bound to a routing plan")
	}
	return cloneRoutingPlan(service.routingPlan), nil
}

// PreviewRouting validates an operator selection against both the persistent
// catalog authority and the currently running standalone data plane. It does
// not allocate, warm, cut over or persist anything.
func (service *Service) PreviewRouting(
	ctx context.Context,
	catalog routing.Catalog,
	store *routing.Store,
	expectedRevision uint64,
	selections map[string]string,
	sourceSelections ...map[string]string,
) (routing.Preview, error) {
	return routing.PreviewRuntimeGraph(
		ctx, catalog, store, expectedRevision, selections, serviceGraphRuntime{service: service}, sourceSelections...)
}

// ApplyRouting executes an operator selection against the running routed
// Service. The persistent Store still owns CAS and durability; Service owns the
// runtime identity fence and the data-plane transaction.
func (service *Service) ApplyRouting(
	ctx context.Context,
	catalog routing.Catalog,
	store *routing.Store,
	expectedRevision uint64,
	selections map[string]string,
) (routing.Plan, error) {
	return service.ApplyRoutingObserved(ctx, catalog, store, expectedRevision, selections, nil)
}

// ApplyRoutingObserved exposes phase-only progress without weakening the
// Service/Store identity fence used by ApplyRouting.
func (service *Service) ApplyRoutingObserved(
	ctx context.Context,
	catalog routing.Catalog,
	store *routing.Store,
	expectedRevision uint64,
	selections map[string]string,
	observer routing.PhaseObserver,
	sourceSelections ...map[string]string,
) (routing.Plan, error) {
	return routing.ApplyGraphObserved(
		ctx, catalog, store, expectedRevision, selections,
		serviceGraphRuntime{service: service}, observer, sourceSelections...,
	)
}

// RollbackRouting applies the bounded previous selection set through the same
// runtime fence as a forward change.
func (service *Service) RollbackRouting(
	ctx context.Context,
	catalog routing.Catalog,
	store *routing.Store,
	expectedRevision uint64,
) (routing.Plan, error) {
	return service.RollbackRoutingObserved(ctx, catalog, store, expectedRevision, nil)
}

// RollbackRoutingObserved executes rollback with the same observable phase
// contract as a forward apply.
func (service *Service) RollbackRoutingObserved(
	ctx context.Context,
	catalog routing.Catalog,
	store *routing.Store,
	expectedRevision uint64,
	observer routing.PhaseObserver,
) (routing.Plan, error) {
	return routing.RollbackGraphObserved(
		ctx, catalog, store, expectedRevision,
		serviceGraphRuntime{service: service}, observer,
	)
}

// serviceGraphRuntime is deliberately private: callers can mutate a routed
// Service only through ApplyRouting or RollbackRouting, which guarantee that
// Store activation occurs before the terminal Retire operation advances the
// in-memory identity.
type serviceGraphRuntime struct {
	service *Service
}

func (runtime serviceGraphRuntime) ValidateGraph(
	ctx context.Context,
	current config.Config,
	candidate config.Config,
	currentPlan routing.Plan,
	candidatePlan routing.Plan,
	delta routing.GraphDelta,
) error {
	service := runtime.service
	if service == nil || service.inner == nil {
		return errors.New("nil video-broker service")
	}
	if ctx == nil {
		return errors.New("routing preview context is required")
	}
	service.routingMu.RLock()
	defer service.routingMu.RUnlock()
	if !service.routingBound {
		return errors.New("video-broker service is not bound to a routing plan")
	}
	validator, ok := service.routingRuntime.(routing.GraphValidator)
	if !ok {
		return errors.New("video-broker routing data plane has no read-only validator")
	}
	if !reflect.DeepEqual(service.routingPlan, currentPlan) {
		return errors.New("running service routing plan differs from persistent current plan")
	}
	if !reflect.DeepEqual(service.routingConfig, current) {
		return errors.New("running service graph differs from persistent current graph")
	}
	derived, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(derived, delta) {
		return errors.New("routing delta differs from the validated graph transition")
	}
	return validator.ValidateGraph(
		ctx, config.Clone(current), config.Clone(candidate),
		cloneRoutingPlan(currentPlan), cloneRoutingPlan(candidatePlan), delta)
}

// PrepareGraph rejects raw-config services, missing data-plane integration and
// any divergence between the Store view and the exact plan/config currently
// bound to this Service. routingMu remains held until the prepared graph reaches
// one terminal operation.
func (runtime serviceGraphRuntime) PrepareGraph(
	ctx context.Context,
	current config.Config,
	candidate config.Config,
	currentPlan routing.Plan,
	candidatePlan routing.Plan,
	delta routing.GraphDelta,
) (routing.PreparedGraph, error) {
	service := runtime.service
	if service == nil || service.inner == nil {
		return nil, errors.New("nil video-broker service")
	}
	if ctx == nil {
		return nil, errors.New("routing prepare context is required")
	}
	service.routingMu.Lock()
	fail := func(err error) (routing.PreparedGraph, error) {
		service.routingMu.Unlock()
		return nil, err
	}
	if !service.routingBound {
		return fail(errors.New("video-broker service is not bound to a routing plan"))
	}
	if service.routingRuntime == nil {
		return fail(errors.New("video-broker live routing data plane is unavailable"))
	}
	if !reflect.DeepEqual(service.routingPlan, currentPlan) {
		return fail(errors.New("running service routing plan differs from persistent current plan"))
	}
	if !reflect.DeepEqual(service.routingConfig, current) {
		return fail(errors.New("running service graph differs from persistent current graph"))
	}
	derived, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		return fail(err)
	}
	if !reflect.DeepEqual(derived, delta) {
		return fail(errors.New("routing delta differs from the validated graph transition"))
	}
	prepared, err := service.routingRuntime.PrepareGraph(
		ctx, config.Clone(current), config.Clone(candidate),
		cloneRoutingPlan(currentPlan), cloneRoutingPlan(candidatePlan), delta,
	)
	if err != nil {
		return fail(err)
	}
	if prepared == nil {
		return fail(errors.New("routing data plane returned no prepared graph"))
	}
	return &servicePreparedGraph{
		service: service, inner: prepared,
		candidateConfig: config.Clone(candidate), candidatePlan: cloneRoutingPlan(candidatePlan),
	}, nil
}

type servicePreparedGraph struct {
	service         *Service
	inner           routing.PreparedGraph
	candidateConfig config.Config
	candidatePlan   routing.Plan

	mu     sync.Mutex
	closed bool
}

func (prepared *servicePreparedGraph) RequiresExclusiveCapture() bool {
	inner, ok := prepared.inner.(interface{ RequiresExclusiveCapture() bool })
	return ok && inner.RequiresExclusiveCapture()
}

func (prepared *servicePreparedGraph) Warmup(ctx context.Context) error {
	return prepared.runOpen(func() error { return prepared.inner.Warmup(ctx) })
}

func (prepared *servicePreparedGraph) Admit(ctx context.Context) error {
	return prepared.runOpen(func() error { return prepared.inner.Admit(ctx) })
}

func (prepared *servicePreparedGraph) AwaitFirstFrame(ctx context.Context) error {
	return prepared.runOpen(func() error { return prepared.inner.AwaitFirstFrame(ctx) })
}

func (prepared *servicePreparedGraph) Cutover(ctx context.Context) error {
	return prepared.runOpen(func() error { return prepared.inner.Cutover(ctx) })
}

func (prepared *servicePreparedGraph) Verify(ctx context.Context) error {
	return prepared.runOpen(func() error { return prepared.inner.Verify(ctx) })
}

func (prepared *servicePreparedGraph) Rollback(ctx context.Context) error {
	return prepared.finish(false, func() error { return prepared.inner.Rollback(ctx) })
}

func (prepared *servicePreparedGraph) Abort(ctx context.Context) error {
	return prepared.finish(false, func() error { return prepared.inner.Abort(ctx) })
}

func (prepared *servicePreparedGraph) Retire(ctx context.Context) error {
	return prepared.finish(true, func() error { return prepared.inner.Retire(ctx) })
}

func (prepared *servicePreparedGraph) runOpen(operation func() error) error {
	if prepared == nil || prepared.service == nil || prepared.inner == nil {
		return errors.New("routing prepared graph is invalid")
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.closed {
		return errors.New("routing prepared graph is closed")
	}
	return operation()
}

func (prepared *servicePreparedGraph) finish(commit bool, operation func() error) error {
	if prepared == nil || prepared.service == nil || prepared.inner == nil {
		return errors.New("routing prepared graph is invalid")
	}
	prepared.mu.Lock()
	if prepared.closed {
		prepared.mu.Unlock()
		return errors.New("routing prepared graph is closed")
	}
	prepared.closed = true
	prepared.mu.Unlock()
	err := operation()
	if commit {
		prepared.service.routingConfig = config.Clone(prepared.candidateConfig)
		prepared.service.routingPlan = cloneRoutingPlan(prepared.candidatePlan)
	}
	prepared.service.routingMu.Unlock()
	return err
}

func (service *Service) Wait(ctx context.Context) error {
	if service == nil || service.inner == nil {
		return errors.New("nil video-broker service")
	}
	return service.inner.Wait(ctx)
}

func (service *Service) Snapshot(now time.Time) Status {
	if service == nil || service.inner == nil {
		return (*broker.Service)(nil).Snapshot(now)
	}
	return service.inner.Snapshot(now)
}

func Run(ctx context.Context, cfg config.Config) error {
	service, err := Start(ctx, cfg)
	if err != nil {
		return err
	}
	return service.Wait(ctx)
}

func cloneRoutingPlan(plan routing.Plan) routing.Plan {
	cloned := plan
	cloned.Selections = cloneStringMap(plan.Selections)
	cloned.StreamGenerations = make(map[string]uint64, len(plan.StreamGenerations))
	for streamID, generation := range plan.StreamGenerations {
		cloned.StreamGenerations[streamID] = generation
	}
	cloned.RollbackSelections = cloneStringMap(plan.RollbackSelections)
	cloned.SourceSelections = cloneStringMap(plan.SourceSelections)
	cloned.RollbackSourceSelections = cloneStringMap(plan.RollbackSourceSelections)
	return cloned
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	cloned := make(map[string]string, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

var _ routing.GraphValidator = serviceGraphRuntime{}
