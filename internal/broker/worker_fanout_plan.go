package broker

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	productconfig "github.com/Josepavese/videosluice/config"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

// workerFanoutChangesForSource compiles one source-local graph delta into the
// private worker contract. Publication paths, source devices and provider
// selections intentionally never cross this boundary.
func workerFanoutChangesForSource(
	transition preservedOwnerFanout,
	sourceID string,
	resolvedArchiveCodec string,
) ([]videoproviders.WorkerBranchChange, error) {
	current, currentExists := transition.CurrentGraphs[sourceID]
	candidate, candidateExists := transition.CandidateGraphs[sourceID]
	if !currentExists || !candidateExists || current.SourceID != sourceID || candidate.SourceID != sourceID {
		return nil, fmt.Errorf("worker fan-out source %q has no exact graph pair", sourceID)
	}
	if !reflect.DeepEqual(current.Source, candidate.Source) {
		return nil, fmt.Errorf("worker fan-out source %q changes its capture contract", sourceID)
	}

	roles := [...]string{"analysis", "archive", "live"}
	changes := make([]videoproviders.WorkerBranchChange, 0, len(roles))
	for _, role := range roles {
		before := workerFanoutNamedStream(current, role)
		after := workerFanoutNamedStream(candidate, role)
		if reflect.DeepEqual(before, after) {
			continue
		}
		change := videoproviders.WorkerBranchChange{Role: role}
		switch {
		case before == nil && after != nil:
			change.Action = videoproviders.WorkerFanoutActionAttach
		case before != nil && after == nil:
			change.Action = videoproviders.WorkerFanoutActionDetach
		case before != nil && after != nil:
			change.Action = videoproviders.WorkerFanoutActionReplace
		default:
			return nil, errors.New("worker fan-out branch delta is empty")
		}
		if after != nil {
			profile, err := workerFanoutProfile(role, after.Config, sourceID, resolvedArchiveCodec)
			if err != nil {
				return nil, err
			}
			change.Profile = &profile
		}
		changes = append(changes, change)
	}
	if len(changes) == 0 {
		return nil, fmt.Errorf("worker fan-out source %q has no branch changes", sourceID)
	}
	probe := videoproviders.WorkerFanoutCommand{
		Schema: videoproviders.WorkerFanoutCommandSchema, TransactionID: "compile",
		Sequence: 1, SourceID: sourceID, CapturePID: 1, MediaEpoch: 1,
		Phase: videoproviders.WorkerFanoutPhasePrepare, Changes: changes,
	}
	if err := videoproviders.ValidateWorkerFanoutCommand(probe); err != nil {
		return nil, fmt.Errorf("worker fan-out source %q branch plan: %w", sourceID, err)
	}
	return changes, nil
}

func workerFanoutNamedStream(graph productconfig.SourceGraph, role string) *productconfig.NamedStream {
	switch role {
	case "analysis":
		return graph.Analysis
	case "archive":
		return graph.Archive
	case "live":
		return graph.Live
	default:
		return nil
	}
}

func workerFanoutProfile(
	role string,
	stream productconfig.StreamConfig,
	sourceID string,
	resolvedArchiveCodec string,
) (videoproviders.WorkerBranchProfile, error) {
	if strings.ToLower(strings.TrimSpace(stream.Type)) != role || stream.SourceRef != sourceID {
		return videoproviders.WorkerBranchProfile{}, fmt.Errorf(
			"worker fan-out %s branch does not belong to source %q", role, sourceID)
	}
	profile := videoproviders.WorkerBranchProfile{
		Width: stream.Width, Height: stream.Height, FPS: stream.FPS,
	}
	switch role {
	case "analysis":
		profile.PixelFormat = strings.ToLower(strings.TrimSpace(stream.PixelFormat))
	case "live":
		profile.Codec = strings.ToLower(strings.TrimSpace(stream.Codec))
		profile.BitrateKbps = stream.BitrateKbps
		profile.Content = strings.ToLower(strings.TrimSpace(stream.Content))
		profile.RateControl = strings.ToLower(strings.TrimSpace(stream.RateControl))
	case "archive":
		profile.Codec = strings.ToLower(strings.TrimSpace(stream.Codec))
		if profile.Codec == "auto" {
			profile.Codec = strings.ToLower(strings.TrimSpace(resolvedArchiveCodec))
		}
		profile.BitrateKbps = stream.BitrateKbps
		profile.Content = strings.ToLower(strings.TrimSpace(stream.Content))
		profile.RateControl = strings.ToLower(strings.TrimSpace(stream.RateControl))
	default:
		return videoproviders.WorkerBranchProfile{}, fmt.Errorf("unsupported worker fan-out role %q", role)
	}
	return profile, nil
}
