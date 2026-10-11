package broker

import (
	"context"
	"errors"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	productconfig "github.com/Josepavese/videosluice/config"
)

const streamGovernorPublishFallbackInterval = time.Second

type streamGovernorProjection struct {
	streamID   string
	streamType string
	sourceID   string
	path       string
	supervisor *sourceSupervisor
}

// streamGovernorStatePath is the stable consumer contract for a named stream.
// Source-local state remains useful implementation telemetry, while this path
// follows the stream's active source across transactional routing cutovers.
func streamGovernorStatePath(basePath, streamID string) string {
	path, err := productconfig.StreamGovernorStatePath(basePath, streamID)
	if err != nil {
		return ""
	}
	return path
}

func (service *Service) runStreamGovernorPublisher(ctx context.Context) {
	if service == nil || ctx == nil {
		return
	}
	defer service.resetStreamGovernorStates()
	for {
		service.publishStreamGovernorStates()
		interval := service.streamGovernorPublishInterval()
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-service.governorWake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

func (service *Service) resetStreamGovernorStates() {
	for _, projection := range service.streamGovernorProjections() {
		if err := os.Remove(projection.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf(
				"[video-broker][resource-governor] stream state reset failed stream=%s path=%s err=%v",
				projection.streamID, projection.path, err,
			)
		}
	}
}

func (service *Service) streamGovernorPublishInterval() time.Duration {
	service.mu.RLock()
	pollSeconds := service.config.Runtime.Governor.PollSeconds
	service.mu.RUnlock()
	if pollSeconds <= 0 {
		return streamGovernorPublishFallbackInterval
	}
	return time.Duration(pollSeconds) * time.Second
}

func (service *Service) signalStreamGovernorPublish() {
	if service == nil || service.governorWake == nil {
		return
	}
	select {
	case service.governorWake <- struct{}{}:
	default:
	}
}

func (service *Service) publishStreamGovernorStates() {
	projections := service.streamGovernorProjections()
	for _, projection := range projections {
		state, available, locked := projection.resourceGovernorState()
		if locked {
			// A routing transaction owns this source. Keep the last valid state
			// until Cutover/Rollback publishes the new atomic graph selection.
			continue
		}
		if !available {
			if err := os.Remove(projection.path); err != nil && !errors.Is(err, os.ErrNotExist) {
				log.Printf(
					"[video-broker][resource-governor] stream state reset failed stream=%s path=%s err=%v",
					projection.streamID, projection.path, err,
				)
			}
			continue
		}
		if err := writeResourceGovernorState(projection.path, state); err != nil {
			log.Printf(
				"[video-broker][resource-governor] stream state publish failed stream=%s source=%s path=%s err=%v",
				projection.streamID, projection.sourceID, projection.path, err,
			)
		}
	}
}

func (service *Service) streamGovernorProjections() []streamGovernorProjection {
	if service == nil {
		return nil
	}
	service.mu.RLock()
	defer service.mu.RUnlock()
	governor := service.config.Runtime.Governor
	if !governor.Enabled {
		return nil
	}
	streamIDs := make([]string, 0, len(service.config.Streams))
	for streamID := range service.config.Streams {
		streamIDs = append(streamIDs, streamID)
	}
	sort.Strings(streamIDs)
	projections := make([]streamGovernorProjection, 0, len(streamIDs))
	for _, streamID := range streamIDs {
		stream := service.config.Streams[streamID]
		projections = append(projections, streamGovernorProjection{
			streamID: streamID, streamType: stream.Type, sourceID: stream.SourceRef,
			path:       streamGovernorStatePath(governor.StatePath, streamID),
			supervisor: service.sources[stream.SourceRef],
		})
	}
	return projections
}

func (projection streamGovernorProjection) resourceGovernorState() (resourceGovernorState, bool, bool) {
	if projection.supervisor == nil || projection.path == "" {
		return resourceGovernorState{}, false, false
	}
	if !projection.supervisor.mu.TryRLock() {
		return resourceGovernorState{}, false, true
	}
	graph := projection.supervisor.graph
	state := projection.supervisor.state
	projection.supervisor.mu.RUnlock()
	if state != "running" || graph == nil || graph.runtime == nil {
		return resourceGovernorState{}, false, false
	}
	runtime := graph.runtime
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if !runtime.plan.ResourceGovernorEnabled || !runtimePlanHasStreamType(runtime.plan, projection.streamType) {
		return resourceGovernorState{}, false, false
	}
	return runtime.resourceGovernorStateLocked(), true, false
}

func runtimePlanHasStreamType(plan videoBrokerPlan, streamType string) bool {
	switch strings.ToLower(strings.TrimSpace(streamType)) {
	case "live":
		return plan.LiveEnabled
	case "analysis":
		return plan.AnalysisEnabled
	case "archive":
		return plan.ArchiveEnabled
	default:
		return false
	}
}

func (service *Service) setActiveRoutingConfigLocked(config productconfig.Config) {
	service.config = productconfig.Clone(config)
}
