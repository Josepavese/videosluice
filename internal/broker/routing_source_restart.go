package broker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/Josepavese/videosluice/capability"
	productconfig "github.com/Josepavese/videosluice/config"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/routing"
)

// Capture-mode and structural output changes need an exclusive physical
// owner. This path is deliberately separate from no-interruption fan-out and
// bitrate changes. It joins every affected old owner before starting any
// candidate, and repeats exactly the same admission on rollback.
func requiresSourceRestart(delta routing.GraphDelta) bool {
	if !delta.Changed {
		return true
	} // saved preference for an inactive source
	for _, change := range delta.Sources {
		if change.CaptureRestartRequired {
			return true
		}
	}
	return false
}

type sourceRestartPreparedGraph struct {
	service            *Service
	current, candidate productconfig.Config
	ids                []string
	before             map[string]*sourceSupervisor
	peers              map[string]*sourceSupervisor
	checks             map[string]*sourceMovePreparedGraph
	cutover, terminal  bool
}

func (runtime *serviceRoutingRuntime) prepareSourceRestart(ctx context.Context, current, candidate productconfig.Config, delta routing.GraphDelta) (*sourceRestartPreparedGraph, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	p := &sourceRestartPreparedGraph{service: runtime.service, current: productconfig.Clone(current), candidate: productconfig.Clone(candidate), ids: append([]string(nil), delta.ChangedSourceIDs...), before: map[string]*sourceSupervisor{}, peers: map[string]*sourceSupervisor{}, checks: map[string]*sourceMovePreparedGraph{}}
	service := runtime.service
	service.mu.RLock()
	defer service.mu.RUnlock()
	if !reflect.DeepEqual(service.config, current) {
		return nil, errors.New("capture restart current authority differs")
	}
	for id, supervisor := range service.sources {
		change := delta.Sources[id]
		if change.State == routing.SourceChangeUnchanged {
			p.peers[id] = supervisor
			continue
		}
		graph, ok := sourceGraph(current, id)
		if !ok {
			return nil, errors.New("capture restart has an unexpected managed source")
		}
		supervisor.mu.RLock()
		exact := supervisor.state == "running" && supervisor.graph != nil && supervisor.graph.runtime != nil && reflect.DeepEqual(supervisor.config, graph)
		supervisor.mu.RUnlock()
		if !exact {
			return nil, fmt.Errorf("capture restart source %q is not running exactly", id)
		}
		p.before[id] = supervisor
	}
	for _, id := range p.ids {
		if _, exists := sourceGraph(current, id); exists && p.before[id] == nil {
			return nil, fmt.Errorf("capture restart source %q is not managed", id)
		}
		if graph, exists := sourceGraph(candidate, id); exists {
			// No camera probe here: enumerating/opening a device in preview can
			// interfere with its live owner. Runtime admission after the joined
			// stop remains mandatory for every provider and accelerator stage.
			if !resolveVideoBrokerPlan(normalizeConfig(graph)).Enabled {
				return nil, fmt.Errorf("capture restart graph %q is unsupported", id)
			}
		}
	}
	if err := p.validateResources(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *sourceRestartPreparedGraph) validateResources() error {
	// Resolve against a detached copy of leases. Preview must not reserve or
	// release actual hardware. Unaffected owners and pending reservations keep
	// consuming capacity; affected owners are joined before this future layout.
	allocator := newCapabilityAllocator()
	active := p.service.allocator
	if active != nil {
		active.mu.Lock()
		for resource, owners := range active.claims {
			allocator.claims[resource] = map[string]int{}
			for id, units := range owners {
				allocator.claims[resource][id] = units
			}
		}
		for reservation, resources := range active.reservations {
			allocator.reservations[reservation] = map[string]int{}
			for id, units := range resources {
				allocator.reservations[reservation][id] = units
			}
		}
		for reservation, sources := range active.reservedBy {
			allocator.reservedBy[reservation] = map[string]bool{}
			for id, reserved := range sources {
				allocator.reservedBy[reservation][id] = reserved
			}
		}
		active.mu.Unlock()
	}
	for _, id := range p.ids {
		allocator.release(id)
	}
	for _, id := range p.ids {
		graph, exists := sourceGraph(p.candidate, id)
		if !exists {
			continue
		}
		source := p.candidate.Sources[id]
		plan := resolveVideoBrokerPlan(normalizeConfig(graph))
		mux := videoproviders.BrokerMuxPlan{
			Transport: source.Transport, CaptureFormat: source.Format, CaptureWidth: source.Width, CaptureHeight: source.Height, CaptureFPS: source.FPS,
			VideoOrientation: plan.VideoOrientation, ToneMappingEnabled: plan.ToneMappingEnabled,
			LiveEnabled: graph.Live != nil, LiveCodec: plan.MainFormat, MainWidth: plan.MainWidth, MainHeight: plan.MainHeight, MainFPS: plan.MainFPS,
			ArchiveEnabled: graph.Archive != nil, ArchiveCodec: plan.ArchiveCodec, ArchiveWidth: plan.ArchiveWidth, ArchiveHeight: plan.ArchiveHeight, ArchiveFPS: plan.ArchiveFPS,
			ArchiveContent: plan.ArchiveContent, ArchiveRateControl: plan.ArchiveRateControl,
			AnalysisEnabled: graph.Analysis != nil, SubWidth: plan.SubWidth, SubHeight: plan.SubHeight,
		}
		policy := capability.ProviderPolicy{EncodeProvider: plan.EncodeProvider, DecodeProvider: plan.DecodeProvider, ToneMapAccelerator: plan.ToneMappingAccelerator, RequireHardwareCodec: plan.RequireHWCodec, AllowSoftwareFallback: plan.AllowSWFallback}
		liveCodec, archiveCodec := "", ""
		if mux.LiveEnabled {
			liveCodec = mux.LiveCodec
		}
		if mux.ArchiveEnabled {
			archiveCodec = mux.ArchiveCodec
		}
		requirements := capability.Requirements{Transport: mux.Transport, CaptureFormat: mux.CaptureFormat, LiveCodec: liveCodec, LiveEncode: brokerLiveEncodeRequired(mux), ArchiveCodec: archiveCodec, ArchiveEncode: brokerArchiveEncodeRequired(mux), ToneMap: mux.ToneMappingEnabled, ResizeRoles: brokerPixelTransformRoles(mux)}
		if _, err := allocator.resolve(id, p.service.inventory, policy, requirements); err != nil {
			return fmt.Errorf("capture restart resources for %q: %w", id, err)
		}
	}
	return nil
}

func (p *sourceRestartPreparedGraph) RequiresExclusiveCapture() bool { return len(p.ids) > 0 }
func (p *sourceRestartPreparedGraph) Warmup(ctx context.Context) error {
	if p.terminal {
		return errors.New("capture restart closed")
	}
	return contextError(ctx)
}

func (p *sourceRestartPreparedGraph) Cutover(ctx context.Context) error {
	if p.cutover || p.terminal {
		return errors.New("capture restart already started")
	}
	if err := p.verifyPeers(); err != nil {
		return err
	}
	p.service.mu.RLock()
	for id, before := range p.before {
		if p.service.sources[id] != before {
			p.service.mu.RUnlock()
			return errors.New("capture owner changed after preview")
		}
	}
	p.service.mu.RUnlock()
	p.cutover = true // partial stops also require a verified rollback
	return p.replace(ctx, p.candidate)
}

func (p *sourceRestartPreparedGraph) replace(ctx context.Context, cfg productconfig.Config) error {
	// All old owners join, release hardware leases and close their endpoints
	// before any replacement starts. No duplicate camera opens or lease keys.
	for _, id := range p.ids {
		p.service.mu.RLock()
		exists := p.service.sources[id] != nil
		p.service.mu.RUnlock()
		if exists {
			if err := p.service.stopSource(ctx, id); err != nil {
				return err
			}
		}
	}
	p.checks = map[string]*sourceMovePreparedGraph{}
	p.service.mu.Lock()
	p.service.setActiveRoutingConfigLocked(cfg)
	p.service.mu.Unlock()
	p.service.signalStreamGovernorPublish()
	for _, id := range p.ids {
		graphConfig, exists := sourceGraph(cfg, id)
		if !exists {
			continue
		}
		supervisor, err := p.service.startSource(graphConfig, false)
		if err != nil {
			return err
		}
		graph, err := waitRestartGraph(ctx, supervisor)
		if err != nil {
			return fmt.Errorf("start capture %q: %w", id, err)
		}
		check := &sourceMovePreparedGraph{candidateGraph: graph, candidateRuntime: graph.runtime, move: wholeSourceMove{candidateGraph: graphConfig}}
		p.checks[id] = check
		media := graph.runtime
		media.mu.Lock()
		check.warmupMainFrame, check.warmupArchiveFrame, check.warmupSubFrame = media.totalMainFrames, media.totalArchiveFrames, media.totalSubFrames
		if graphConfig.Live != nil && len(media.mainClients) == 0 {
			before := media.bridgeControlTargetsLocked()
			media.routingLiveDemand, check.introducedLiveDemand = true, true
			if media.childControlSupported {
				if liveEncoderActivationChanged(before, media.bridgeControlTargetsLocked()) {
					media.mainAwaitingEncoderEpoch = true
					media.armMainKeyframeGateLocked()
				}
				if _, queued := media.queueBridgeControlLocked("routing_capture_restart_admission"); !queued {
					media.mu.Unlock()
					return errors.New("capture restart admission demand not queued")
				}
			}
		}
		media.mu.Unlock()
	}
	return nil
}

func waitRestartGraph(ctx context.Context, supervisor *sourceSupervisor) (*graphService, error) {
	ticker := time.NewTicker(routingFramePollInterval)
	defer ticker.Stop()
	for {
		supervisor.mu.RLock()
		graph, state, lastError := supervisor.graph, supervisor.state, supervisor.lastError
		supervisor.mu.RUnlock()
		if state == "running" && graph != nil && graph.runtime != nil {
			return graph, nil
		}
		if state == "degraded" || state == "stopped" {
			return nil, fmt.Errorf("capture startup %s: %s", state, lastError)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-supervisor.done:
			return nil, errors.New("capture supervisor stopped")
		case <-ticker.C:
		}
	}
}

func (p *sourceRestartPreparedGraph) Admit(ctx context.Context) error {
	for _, id := range p.ids {
		check := p.checks[id]
		if check == nil {
			continue
		}
		if err := check.waitCandidate(ctx, false); err != nil {
			return err
		}
		check.candidateRuntime.mu.Lock()
		check.candidatePID, check.candidateEpoch = brokerChildPID(check.candidateRuntime.childCmd), check.candidateRuntime.mediaEpoch
		check.candidateRuntime.mu.Unlock()
	}
	return nil
}
func (p *sourceRestartPreparedGraph) AwaitFirstFrame(ctx context.Context) error {
	for _, id := range p.ids {
		if check := p.checks[id]; check != nil {
			if err := check.waitCandidate(ctx, true); err != nil {
				return err
			}
		}
	}
	return nil
}
func (p *sourceRestartPreparedGraph) Verify(ctx context.Context) error {
	if err := p.verifyPeers(); err != nil {
		return err
	}
	for _, check := range p.checks {
		media := check.candidateRuntime
		media.mu.Lock()
		check.warmupMainFrame, check.warmupArchiveFrame, check.warmupSubFrame = media.totalMainFrames, media.totalArchiveFrames, media.totalSubFrames
		media.mu.Unlock()
	}
	return p.AwaitFirstFrame(ctx)
}
func (p *sourceRestartPreparedGraph) verifyPeers() error {
	p.service.mu.RLock()
	defer p.service.mu.RUnlock()
	for id, peer := range p.peers {
		if p.service.sources[id] != peer {
			return fmt.Errorf("unrelated source %q changed", id)
		}
	}
	return nil
}
func (p *sourceRestartPreparedGraph) releaseDemand() error {
	var err error
	for _, check := range p.checks {
		if !check.introducedLiveDemand {
			continue
		}
		media := check.candidateRuntime
		media.mu.Lock()
		media.routingLiveDemand, check.introducedLiveDemand = false, false
		if media.childControlSupported {
			if _, queued := media.queueBridgeControlLocked("routing_capture_restart_admission_release"); !queued {
				err = errors.Join(err, errors.New("capture restart live demand release not queued"))
			}
		}
		media.mu.Unlock()
	}
	return err
}
func (p *sourceRestartPreparedGraph) Rollback(ctx context.Context) error {
	if p.terminal {
		return errors.New("capture restart closed")
	}
	defer func() { p.terminal = true }()
	if !p.cutover {
		return nil
	}
	if err := p.replace(ctx, p.current); err != nil {
		return errors.Join(err, p.releaseDemand())
	}
	if err := p.Admit(ctx); err != nil {
		return errors.Join(err, p.releaseDemand())
	}
	if err := p.AwaitFirstFrame(ctx); err != nil {
		return errors.Join(err, p.releaseDemand())
	}
	return errors.Join(p.verifyPeers(), p.releaseDemand())
}
func (p *sourceRestartPreparedGraph) Abort(ctx context.Context) error {
	if p.cutover {
		return p.Rollback(ctx)
	}
	p.terminal = true
	return nil
}
func (p *sourceRestartPreparedGraph) Retire(ctx context.Context) error {
	if p.terminal {
		return errors.New("capture restart closed")
	}
	p.terminal = true
	return errors.Join(contextError(ctx), p.verifyPeers(), p.releaseDemand())
}
