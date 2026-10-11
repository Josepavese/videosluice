package routing

import (
	"context"
	"errors"
	"fmt"

	"github.com/Josepavese/videosluice/config"
)

// Preview is the operator-safe projection of one proposed routing change. It
// contains only catalog option IDs and logical source/stream IDs; physical
// devices and unrestricted media values never cross this boundary.
type Preview struct {
	CurrentRevision  uint64                  `json:"current_revision"`
	Candidate        Plan                    `json:"candidate"`
	ChangedSourceIDs []string                `json:"changed_source_ids"`
	ChangedStreamIDs []string                `json:"changed_stream_ids"`
	Sources          map[string]SourceChange `json:"sources"`
	Streams          map[string]StreamChange `json:"streams"`
}

// PreviewGraph performs the same catalog, revision and graph validation as an
// apply transaction, then releases the store lock without changing the plan.
// A later apply must still present Candidate.Revision-1 as its expected
// revision; preview is deliberately not a reservation.
func PreviewGraph(
	catalog Catalog,
	store *Store,
	expectedRevision uint64,
	selections map[string]string,
	sourceSelections ...map[string]string,
) (Preview, error) {
	return previewGraph(catalog, store, expectedRevision, selections, nil, sourceSelections...)
}

// PreviewRuntimeGraph extends the catalog-only preview with a read-only check
// against the actual data plane. It is the safe surface for an operator API:
// syntactically valid catalog choices that the running backend cannot apply are
// rejected before being shown as actionable. No worker, lease, endpoint or
// persistent plan may change during validation.
func PreviewRuntimeGraph(
	ctx context.Context,
	catalog Catalog,
	store *Store,
	expectedRevision uint64,
	selections map[string]string,
	validator GraphValidator,
	sourceSelections ...map[string]string,
) (Preview, error) {
	if ctx == nil {
		return Preview{}, errors.New("routing preview context is required")
	}
	if validator == nil {
		return Preview{}, errors.New("routing graph validator is required")
	}
	if err := ctx.Err(); err != nil {
		return Preview{}, err
	}
	return previewGraph(catalog, store, expectedRevision, selections, func(
		currentConfig, candidateConfig config.Config,
		currentPlan, candidatePlan Plan,
		delta GraphDelta,
	) error {
		return validator.ValidateGraph(
			ctx, currentConfig, candidateConfig, currentPlan, candidatePlan, delta)
	}, sourceSelections...)
}

type previewRuntimeValidator func(config.Config, config.Config, Plan, Plan, GraphDelta) error

func previewGraph(
	catalog Catalog,
	store *Store,
	expectedRevision uint64,
	selections map[string]string,
	validator previewRuntimeValidator,
	sourceSelections ...map[string]string,
) (Preview, error) {
	transaction, err := store.Begin(catalog, expectedRevision, selections, sourceSelections...)
	if err != nil {
		return Preview{}, err
	}
	currentPlan := transaction.CurrentPlan()
	candidatePlan := transaction.Plan()
	currentConfig := transaction.CurrentConfig()
	candidateConfig := transaction.Config()
	delta, err := DiffConfigs(currentConfig, candidateConfig)
	if err != nil {
		return Preview{}, errors.Join(err, transaction.Abort())
	}
	if !delta.Changed && selectionsEqual(currentPlan.SourceSelections, candidatePlan.SourceSelections) {
		return Preview{}, errors.Join(
			errors.New("routing selections do not change the materialized graph"), transaction.Abort())
	}
	if validator != nil {
		if err := validator(
			cloneConfig(currentConfig), cloneConfig(candidateConfig),
			transaction.CurrentPlan(), transaction.Plan(), cloneGraphDelta(delta),
		); err != nil {
			return Preview{}, errors.Join(
				fmt.Errorf("routing runtime preview: %w", err), transaction.Abort())
		}
	}
	if abortErr := transaction.Abort(); abortErr != nil {
		return Preview{}, abortErr
	}
	return Preview{
		CurrentRevision:  currentPlan.Revision,
		Candidate:        candidatePlan,
		ChangedSourceIDs: append([]string(nil), delta.ChangedSourceIDs...),
		ChangedStreamIDs: append([]string(nil), delta.ChangedStreamIDs...),
		Sources:          cloneSourceChanges(delta.Sources),
		Streams:          cloneStreamChanges(delta.Streams),
	}, nil
}

func cloneGraphDelta(input GraphDelta) GraphDelta {
	return GraphDelta{
		Changed:          input.Changed,
		SourceIDs:        append([]string(nil), input.SourceIDs...),
		StreamIDs:        append([]string(nil), input.StreamIDs...),
		ChangedSourceIDs: append([]string(nil), input.ChangedSourceIDs...),
		ChangedStreamIDs: append([]string(nil), input.ChangedStreamIDs...),
		Sources:          cloneSourceChanges(input.Sources),
		Streams:          cloneStreamChanges(input.Streams),
	}
}

func cloneSourceChanges(input map[string]SourceChange) map[string]SourceChange {
	output := make(map[string]SourceChange, len(input))
	for sourceID, change := range input {
		change.CurrentStreams = append([]string(nil), change.CurrentStreams...)
		change.CandidateStreams = append([]string(nil), change.CandidateStreams...)
		output[sourceID] = change
	}
	return output
}

func cloneStreamChanges(input map[string]StreamChange) map[string]StreamChange {
	output := make(map[string]StreamChange, len(input))
	for streamID, change := range input {
		output[streamID] = change
	}
	return output
}
