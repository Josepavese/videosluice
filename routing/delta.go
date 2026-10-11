package routing

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/Josepavese/videosluice/config"
)

const (
	SourceChangeUnchanged   = "unchanged"
	SourceChangeActivate    = "activate"
	SourceChangeReconfigure = "reconfigure"
	SourceChangeDeactivate  = "deactivate"

	StreamChangeUnchanged   = "unchanged"
	StreamChangeMove        = "move"
	StreamChangeReconfigure = "reconfigure"
)

// GraphDelta is the deterministic runtime work set between two normalized
// routing plans. It contains no device paths or operator-provided media values;
// those remain in the already validated Config arguments passed to the runtime.
type GraphDelta struct {
	Changed          bool                    `json:"changed"`
	SourceIDs        []string                `json:"source_ids"`
	StreamIDs        []string                `json:"stream_ids"`
	ChangedSourceIDs []string                `json:"changed_source_ids"`
	ChangedStreamIDs []string                `json:"changed_stream_ids"`
	Sources          map[string]SourceChange `json:"sources"`
	Streams          map[string]StreamChange `json:"streams"`
}

type SourceChange struct {
	State                  string   `json:"state"`
	CurrentStreams         []string `json:"current_streams"`
	CandidateStreams       []string `json:"candidate_streams"`
	PreserveCaptureOwner   bool     `json:"preserve_capture_owner"`
	CaptureModeChanged     bool     `json:"capture_mode_changed,omitempty"`
	CaptureRestartRequired bool     `json:"capture_restart_required,omitempty"`
}

type StreamChange struct {
	State           string `json:"state"`
	CurrentSource   string `json:"current_source"`
	CandidateSource string `json:"candidate_source"`
	ProfileChanged  bool   `json:"profile_changed"`
}

// DiffConfigs validates both graphs and returns the exact source-local work
// set. Runtime and active source definitions are immutable inside one catalog
// revision; accepting either change here would let an operator routing request
// bypass the central capability boundary.
func DiffConfigs(current, candidate config.Config) (GraphDelta, error) {
	current, err := config.Normalize(current)
	if err != nil {
		return GraphDelta{}, fmt.Errorf("current routing graph: %w", err)
	}
	candidate, err = config.Normalize(candidate)
	if err != nil {
		return GraphDelta{}, fmt.Errorf("candidate routing graph: %w", err)
	}
	if !reflect.DeepEqual(current.Runtime, candidate.Runtime) {
		return GraphDelta{}, errors.New("routing apply cannot change runtime configuration")
	}

	delta := GraphDelta{
		Sources: make(map[string]SourceChange),
		Streams: make(map[string]StreamChange),
	}
	sourceSet := make(map[string]struct{}, len(current.Sources)+len(candidate.Sources))
	for sourceID := range current.Sources {
		sourceSet[sourceID] = struct{}{}
	}
	for sourceID := range candidate.Sources {
		sourceSet[sourceID] = struct{}{}
	}
	for sourceID := range sourceSet {
		delta.SourceIDs = append(delta.SourceIDs, sourceID)
	}
	sort.Strings(delta.SourceIDs)

	streamSet := make(map[string]struct{}, len(current.Streams)+len(candidate.Streams))
	for streamID := range current.Streams {
		streamSet[streamID] = struct{}{}
	}
	for streamID := range candidate.Streams {
		streamSet[streamID] = struct{}{}
	}
	for streamID := range streamSet {
		delta.StreamIDs = append(delta.StreamIDs, streamID)
	}
	sort.Strings(delta.StreamIDs)
	restartSources := make(map[string]bool)
	for id, source := range current.Sources {
		if next, exists := candidate.Sources[id]; exists && !reflect.DeepEqual(source, next) {
			restartSources[id] = true
		}
	}
	for id, before := range current.Streams {
		after, exists := candidate.Streams[id]
		if exists && before.SourceRef == after.SourceRef && (before.Width != after.Width || before.Height != after.Height || before.Codec != after.Codec || before.PixelFormat != after.PixelFormat) {
			restartSources[before.SourceRef] = true
		}
	}
	if access := current.Runtime.FrameAccess; access != nil {
		for _, reader := range access.Readers {
			before, beforeExists := current.Streams[reader.Stream]
			after, afterExists := candidate.Streams[reader.Stream]
			if beforeExists && afterExists && !reflect.DeepEqual(before, after) {
				// Frame handles and output geometry are attached to the capture
				// execution. Current providers must replace affected owners to
				// rebind them; a fan-out command cannot install inherited FDs.
				restartSources[before.SourceRef] = true
				restartSources[after.SourceRef] = true
			}
		}
	}
	if len(restartSources) != 0 {
		// The exclusive transaction replaces its complete changed source set.
		// Advertise that exact work set, including combined routing edits, and
		// fence every publication on those captures. Never claim that an output
		// kept its execution merely because its selected profile is unchanged.
		for id, before := range current.Streams {
			if after, exists := candidate.Streams[id]; exists && !reflect.DeepEqual(before, after) {
				restartSources[before.SourceRef] = true
				restartSources[after.SourceRef] = true
			}
		}
	}

	for _, streamID := range delta.StreamIDs {
		before, beforeExists := current.Streams[streamID]
		after, afterExists := candidate.Streams[streamID]
		if !beforeExists || !afterExists {
			return GraphDelta{}, fmt.Errorf("routing apply must preserve logical stream %q", streamID)
		}
		change := StreamChange{
			State: StreamChangeUnchanged, CurrentSource: before.SourceRef,
			CandidateSource: after.SourceRef,
		}
		if before.SourceRef != after.SourceRef {
			change.State = StreamChangeMove
			change.ProfileChanged = !sameStreamProfile(before, after)
		} else if !reflect.DeepEqual(before, after) || restartSources[before.SourceRef] {
			change.State = StreamChangeReconfigure
			change.ProfileChanged = !reflect.DeepEqual(before, after)
		}
		delta.Streams[streamID] = change
		if change.State != StreamChangeUnchanged {
			delta.Changed = true
			delta.ChangedStreamIDs = append(delta.ChangedStreamIDs, streamID)
		}
	}

	currentStreams := streamsBySource(current)
	candidateStreams := streamsBySource(candidate)
	for _, sourceID := range delta.SourceIDs {
		_, beforeExists := current.Sources[sourceID]
		_, afterExists := candidate.Sources[sourceID]
		modeChanged := beforeExists && afterExists && !reflect.DeepEqual(current.Sources[sourceID], candidate.Sources[sourceID])
		if modeChanged && !SourceModeChangeOnly(current.Sources[sourceID], candidate.Sources[sourceID]) {
			return GraphDelta{}, fmt.Errorf("routing apply cannot change active source definition %q", sourceID)
		}
		change := SourceChange{
			State: SourceChangeUnchanged, CurrentStreams: currentStreams[sourceID],
			CandidateStreams:       candidateStreams[sourceID],
			PreserveCaptureOwner:   beforeExists && afterExists && !restartSources[sourceID],
			CaptureModeChanged:     modeChanged,
			CaptureRestartRequired: restartSources[sourceID],
		}
		switch {
		case !beforeExists && afterExists:
			change.State = SourceChangeActivate
		case beforeExists && !afterExists:
			change.State = SourceChangeDeactivate
		case !reflect.DeepEqual(change.CurrentStreams, change.CandidateStreams) || sourceHasReconfiguredStream(sourceID, delta.Streams):
			change.State = SourceChangeReconfigure
		}
		delta.Sources[sourceID] = change
		if change.State != SourceChangeUnchanged {
			delta.Changed = true
			delta.ChangedSourceIDs = append(delta.ChangedSourceIDs, sourceID)
		}
	}
	return delta, nil
}

func streamsBySource(cfg config.Config) map[string][]string {
	result := make(map[string][]string, len(cfg.Sources))
	for sourceID := range cfg.Sources {
		result[sourceID] = []string{}
	}
	for streamID, stream := range cfg.Streams {
		result[stream.SourceRef] = append(result[stream.SourceRef], streamID)
	}
	for sourceID := range result {
		sort.Strings(result[sourceID])
	}
	return result
}

func sourceHasReconfiguredStream(sourceID string, streams map[string]StreamChange) bool {
	for _, stream := range streams {
		if stream.State == StreamChangeReconfigure && stream.CurrentSource == sourceID {
			return true
		}
	}
	return false
}

func sameStreamProfile(before, after config.StreamConfig) bool {
	before.SourceRef = ""
	after.SourceRef = ""
	return reflect.DeepEqual(before, after)
}
