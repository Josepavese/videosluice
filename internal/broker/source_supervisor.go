package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
)

type sourceRestartPolicy struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	StableWindow time.Duration
}

func defaultSourceRestartPolicy() sourceRestartPolicy {
	return sourceRestartPolicy{
		InitialDelay: time.Second,
		MaxDelay:     30 * time.Second,
		StableWindow: 30 * time.Second,
	}
}

func (policy sourceRestartPolicy) validate() error {
	if policy.InitialDelay <= 0 || policy.MaxDelay < policy.InitialDelay || policy.StableWindow <= 0 {
		return errors.New("source restart policy is invalid")
	}
	return nil
}

func (policy sourceRestartPolicy) delay(consecutiveFailures uint64) time.Duration {
	delay := policy.InitialDelay
	for failure := uint64(1); failure < consecutiveFailures && delay < policy.MaxDelay; failure++ {
		if delay > policy.MaxDelay/2 {
			return policy.MaxDelay
		}
		delay *= 2
	}
	if delay > policy.MaxDelay {
		return policy.MaxDelay
	}
	return delay
}

type sourceSupervisor struct {
	mu          sync.RWMutex
	config      productconfig.SourceGraph
	admission   []productconfig.SourceRoleProfile
	inventory   capability.Inventory
	allocator   *capabilityAllocator
	starter     sourceGraphStarter
	retryPolicy sourceRestartPolicy

	graph        *graphService
	state        string
	lastError    string
	lastChangeAt time.Time
	cancel       context.CancelFunc
	done         <-chan struct{}
}

func startServiceWithRestartPolicy(
	ctx context.Context,
	normalized productconfig.Config,
	inventory capability.Inventory,
	starter sourceGraphStarter,
	retryPolicy sourceRestartPolicy,
	admissionOverride ...map[string][]productconfig.SourceRoleProfile,
) (*Service, error) {
	if starter == nil {
		return nil, errors.New("source graph starter is required")
	}
	if err := retryPolicy.validate(); err != nil {
		return nil, err
	}
	admission := currentAdmissionProfiles(normalized)
	if len(admissionOverride) > 1 {
		return nil, errors.New("source admission override must be unique")
	}
	if len(admissionOverride) == 1 {
		admission = admissionOverride[0]
	}
	managerCtx, cancel := context.WithCancel(ctx)
	allocator := newCapabilityAllocator()
	fanout, err := newWorkerFanoutBackend(
		inventory, allocator, newRuntimeFanoutPublicationCoordinator(),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("initialize dynamic fan-out backend: %w", err)
	}
	done := make(chan error, 1)
	service := &Service{
		sources: make(map[string]*sourceSupervisor, len(normalized.Sources)),
		config:  normalized, admission: cloneAdmissionProfiles(admission),
		inventory: inventory, allocator: allocator,
		starter: starter, fanout: fanout, retryPolicy: retryPolicy, managerCtx: managerCtx,
		done: done, cancel: cancel, governorWake: make(chan struct{}, 1),
	}
	for _, graphConfig := range normalized.SourceGraphs() {
		if _, err := service.startSource(graphConfig, true); err != nil {
			cancel()
			service.workers.Wait()
			return nil, err
		}
	}
	service.workers.Add(1)
	go func() {
		defer service.workers.Done()
		service.runStreamGovernorPublisher(managerCtx)
	}()
	go func() {
		<-managerCtx.Done()
		service.lifecycleMu.Lock()
		service.stopping = true
		service.lifecycleMu.Unlock()
		service.workers.Wait()
		done <- nil
		close(done)
	}()
	return service, nil
}

func (service *Service) startSource(graphConfig productconfig.SourceGraph, waitFirstAttempt bool) (*sourceSupervisor, error) {
	if service == nil || service.managerCtx == nil {
		return nil, errors.New("source manager is not initialized")
	}
	service.lifecycleMu.Lock()
	if service.stopping || service.managerCtx.Err() != nil {
		service.lifecycleMu.Unlock()
		return nil, context.Canceled
	}
	sourceCtx, cancelSource := context.WithCancel(service.managerCtx)
	workerDone := make(chan struct{})
	supervisor := &sourceSupervisor{
		config: graphConfig, admission: cloneSourceRoleProfiles(service.admission[graphConfig.SourceID]),
		inventory: service.inventory, allocator: service.allocator,
		starter: service.starter, retryPolicy: service.retryPolicy,
		state: "initializing", lastChangeAt: time.Now().UTC(),
		cancel: cancelSource, done: workerDone,
	}
	firstAttemptDone := make(chan struct{})
	service.workers.Add(1)
	service.lifecycleMu.Unlock()

	service.mu.Lock()
	if _, exists := service.sources[graphConfig.SourceID]; exists {
		service.mu.Unlock()
		cancelSource()
		service.workers.Done()
		return nil, fmt.Errorf("source %q is already managed", graphConfig.SourceID)
	}
	service.sources[graphConfig.SourceID] = supervisor
	service.mu.Unlock()

	go func() {
		defer service.workers.Done()
		defer close(workerDone)
		supervisor.run(sourceCtx, firstAttemptDone, nil)
	}()
	if waitFirstAttempt {
		select {
		case <-firstAttemptDone:
		case <-service.managerCtx.Done():
			cancelSource()
			<-workerDone
			return nil, service.managerCtx.Err()
		}
	}
	return supervisor, nil
}

// adoptPreparedSource transfers a graph that has already acquired its hardware
// leases and completed routing admission into the normal source supervision
// lifecycle. It deliberately does not publish the supervisor in service.sources:
// the routing transaction performs that map swap together with endpoint cutover.
func (service *Service) adoptPreparedSource(
	sourceCtx context.Context,
	cancelSource context.CancelFunc,
	graphConfig productconfig.SourceGraph,
	graph *graphService,
) (*sourceSupervisor, error) {
	if service == nil || sourceCtx == nil || cancelSource == nil || graph == nil || graph.done == nil {
		return nil, errors.New("prepared source graph is incomplete")
	}
	service.lifecycleMu.Lock()
	if service.stopping || service.managerCtx == nil || service.managerCtx.Err() != nil || sourceCtx.Err() != nil {
		service.lifecycleMu.Unlock()
		return nil, errors.New("source manager is stopping")
	}
	workerDone := make(chan struct{})
	supervisor := &sourceSupervisor{
		config: graphConfig, admission: cloneSourceRoleProfiles(service.admission[graphConfig.SourceID]),
		inventory: service.inventory, allocator: service.allocator,
		starter: service.starter, retryPolicy: service.retryPolicy,
		state: "initializing", lastChangeAt: time.Now().UTC(),
		cancel: cancelSource, done: workerDone,
	}
	firstAttemptDone := make(chan struct{})
	service.workers.Add(1)
	service.lifecycleMu.Unlock()

	go func() {
		defer service.workers.Done()
		defer close(workerDone)
		supervisor.run(sourceCtx, firstAttemptDone, graph)
	}()

	select {
	case <-firstAttemptDone:
	case <-sourceCtx.Done():
		cancelSource()
		<-workerDone
		return nil, sourceCtx.Err()
	}
	supervisor.mu.RLock()
	running := supervisor.state == "running" && supervisor.graph == graph &&
		reflect.DeepEqual(supervisor.config, graphConfig)
	supervisor.mu.RUnlock()
	if running {
		return supervisor, nil
	}
	cancelSource()
	<-workerDone
	return nil, errors.New("prepared source graph stopped before adoption")
}

func (supervisor *sourceSupervisor) stop(ctx context.Context) error {
	if supervisor == nil || supervisor.cancel == nil || supervisor.done == nil {
		return errors.New("source supervisor is not managed")
	}
	supervisor.cancel()
	select {
	case <-supervisor.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (service *Service) stopSource(ctx context.Context, sourceID string) error {
	if service == nil {
		return errors.New("source manager is not initialized")
	}
	service.mu.RLock()
	supervisor, exists := service.sources[sourceID]
	service.mu.RUnlock()
	if !exists {
		return fmt.Errorf("source %q is not managed", sourceID)
	}
	if err := supervisor.stop(ctx); err != nil {
		return fmt.Errorf("stop source %q: %w", sourceID, err)
	}
	service.mu.Lock()
	if service.sources[sourceID] == supervisor {
		delete(service.sources, sourceID)
	}
	service.mu.Unlock()
	return nil
}

func (supervisor *sourceSupervisor) run(
	ctx context.Context,
	firstAttemptDone chan<- struct{},
	graph *graphService,
) {
	firstAttempt := true
	signalFirstAttempt := func() {
		if firstAttempt {
			close(firstAttemptDone)
			firstAttempt = false
		}
	}
	consecutiveFailures := uint64(0)
	if graph != nil {
		supervisor.setGraph(graph)
		signalFirstAttempt()
	}
	for {
		if ctx.Err() != nil {
			supervisor.setStopped()
			signalFirstAttempt()
			return
		}
		supervisor.mu.RLock()
		graphConfig := supervisor.config
		admission := cloneSourceRoleProfiles(supervisor.admission)
		inventory := supervisor.inventory
		allocator := supervisor.allocator
		starter := supervisor.starter
		supervisor.mu.RUnlock()
		var cancelAttempt context.CancelFunc
		if graph == nil {
			supervisor.setInitializing()
			var attemptCtx context.Context
			attemptCtx, cancelAttempt = context.WithCancel(ctx)
			var startErr error
			graph, startErr = starter(
				attemptCtx, graphConfig, admission, inventory, allocator,
			)
			if startErr == nil && (graph == nil || graph.done == nil) {
				startErr = errors.New("source graph starter returned an incomplete runtime")
			}
			if startErr != nil {
				cancelAttempt()
				allocator.release(graphConfig.SourceID)
				if ctx.Err() != nil {
					supervisor.setStopped()
					signalFirstAttempt()
					return
				}
				consecutiveFailures++
				delay := supervisor.retryPolicy.delay(consecutiveFailures)
				supervisor.setFailure(fmt.Errorf("startup: %w", startErr))
				log.Printf(
					"[video-broker] source=%s startup failed retry_in=%s error=%v",
					graphConfig.SourceID, delay, startErr,
				)
				signalFirstAttempt()
				if !waitSourceRestart(ctx, delay) {
					supervisor.setStopped()
					return
				}
				continue
			}
			supervisor.setGraph(graph)
			signalFirstAttempt()
		} else {
			cancelAttempt = func() {}
		}

		startedAt := time.Now()
		var runtimeErr error
		select {
		case <-ctx.Done():
			cancelAttempt()
			<-graph.done
			allocator.release(graphConfig.SourceID)
			supervisor.setStopped()
			return
		case err, ok := <-graph.done:
			if ok {
				runtimeErr = err
			}
		}
		cancelAttempt()
		allocator.release(graphConfig.SourceID)
		graph = nil
		if ctx.Err() != nil {
			supervisor.setStopped()
			return
		}
		if runtimeErr == nil || errors.Is(runtimeErr, context.Canceled) {
			runtimeErr = errors.New("source graph stopped unexpectedly")
		}
		if time.Since(startedAt) >= supervisor.retryPolicy.StableWindow {
			consecutiveFailures = 0
		}
		consecutiveFailures++
		delay := supervisor.retryPolicy.delay(consecutiveFailures)
		supervisor.setFailure(fmt.Errorf("runtime: %w", runtimeErr))
		log.Printf(
			"[video-broker] source=%s runtime failed retry_in=%s error=%v",
			graphConfig.SourceID, delay, runtimeErr,
		)
		if !waitSourceRestart(ctx, delay) {
			supervisor.setStopped()
			return
		}
	}
}

func waitSourceRestart(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (supervisor *sourceSupervisor) setInitializing() {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.graph = nil
	supervisor.state = "initializing"
	supervisor.lastChangeAt = time.Now().UTC()
}

func (supervisor *sourceSupervisor) setGraph(graph *graphService) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.graph = graph
	supervisor.state = "running"
	supervisor.lastError = ""
	supervisor.lastChangeAt = time.Now().UTC()
}

func (supervisor *sourceSupervisor) setFailure(err error) {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.graph = nil
	supervisor.state = "degraded"
	supervisor.lastError = err.Error()
	supervisor.lastChangeAt = time.Now().UTC()
}

func (supervisor *sourceSupervisor) setStopped() {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.graph = nil
	supervisor.state = "stopped"
	supervisor.lastError = ""
	supervisor.lastChangeAt = time.Now().UTC()
}

func (supervisor *sourceSupervisor) snapshot(now time.Time) (SourceStatus, map[string]StreamStatus) {
	if supervisor == nil {
		return SourceStatus{State: "stopped", HardwareRecovery: []HardwareRecoveryStatus{}}, map[string]StreamStatus{}
	}
	supervisor.mu.RLock()
	graph := supervisor.graph
	state := supervisor.state
	lastError := supervisor.lastError
	lastChangeAt := supervisor.lastChangeAt
	graphConfig := supervisor.config
	inventory := supervisor.inventory
	supervisor.mu.RUnlock()
	if graph != nil {
		return graph.snapshot(now)
	}
	if state == "" {
		state = "initializing"
	}
	source := SourceStatus{
		State: state, LastError: lastError, LastChangeAt: lastChangeAt,
		Identity:  graphConfig.Source.Identity,
		Transport: graphConfig.Source.Transport, Device: graphConfig.Source.Device,
		ConfiguredFormat: graphConfig.Source.Format,
		HardwareRecovery: []HardwareRecoveryStatus{}, CapabilityInventory: inventory,
	}
	return source, configuredStreamStatuses(graphConfig)
}

func configuredStreamStatuses(graphConfig productconfig.SourceGraph) map[string]StreamStatus {
	streams := make(map[string]StreamStatus, 3)
	if graphConfig.Live != nil {
		streams[graphConfig.Live.ID] = StreamStatus{
			Type: "live", SourceRef: graphConfig.SourceID,
			Codec: graphConfig.Live.Config.Codec, FPS: graphConfig.Live.Config.FPS,
		}
	}
	if graphConfig.Analysis != nil {
		streams[graphConfig.Analysis.ID] = StreamStatus{
			Type: "analysis", SourceRef: graphConfig.SourceID,
			FPS: graphConfig.Analysis.Config.FPS,
		}
	}
	if graphConfig.Archive != nil {
		streams[graphConfig.Archive.ID] = StreamStatus{
			Type: "archive", SourceRef: graphConfig.SourceID,
			Codec: graphConfig.Archive.Config.Codec, FPS: graphConfig.Archive.Config.FPS,
		}
	}
	return streams
}
