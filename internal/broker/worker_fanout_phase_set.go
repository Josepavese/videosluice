package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"

	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

type workerFanoutPhaseTarget struct {
	Runtime         *videoBrokerRuntime
	CapturePID      int
	MediaEpoch      uint64
	Changes         []videoproviders.WorkerBranchChange
	PreparedChanges []videoproviders.WorkerBranchChange
}

type workerFanoutPhaseResult struct {
	sourceID string
	receipt  videoproviders.WorkerFanoutReceipt
	err      error
}

// sendWorkerFanoutPhaseSet applies one phase concurrently to all affected
// capture owners. Every target is validated before the first command is sent;
// results and errors are then folded in canonical source order.
func sendWorkerFanoutPhaseSet(
	ctx context.Context,
	transactionID string,
	phase string,
	targets map[string]workerFanoutPhaseTarget,
) (map[string]videoproviders.WorkerFanoutReceipt, error) {
	sourceIDs, normalizedTargets, err := validateWorkerFanoutPhaseSet(
		ctx, transactionID, phase, targets)
	if err != nil {
		return nil, err
	}

	results := make(chan workerFanoutPhaseResult, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		target := normalizedTargets[sourceID]
		go func() {
			receipt, err := target.Runtime.sendWorkerFanoutPhase(
				ctx, transactionID, sourceID, target.CapturePID, target.MediaEpoch,
				phase, target.Changes,
			)
			results <- workerFanoutPhaseResult{sourceID: sourceID, receipt: receipt, err: err}
		}()
	}
	bySource := make(map[string]workerFanoutPhaseResult, len(sourceIDs))
	for range sourceIDs {
		result := <-results
		bySource[result.sourceID] = result
	}
	receipts := make(map[string]videoproviders.WorkerFanoutReceipt, len(sourceIDs))
	var combined error
	for _, sourceID := range sourceIDs {
		result := bySource[sourceID]
		if result.err == nil && phase == videoproviders.WorkerFanoutPhaseFirstFrame {
			result.err = videoproviders.ValidateWorkerFanoutFirstFrameEvidence(
				result.receipt, normalizedTargets[sourceID].PreparedChanges)
		}
		if result.err != nil {
			combined = errors.Join(combined, fmt.Errorf(
				"worker fan-out source %q phase %q: %w", sourceID, phase, result.err))
			continue
		}
		receipts[sourceID] = result.receipt
	}
	return receipts, combined
}

// sendWorkerFanoutCleanupPhaseSet deliberately does not use the all-or-nothing
// preflight of a forward phase. Once a transaction must be aborted or rolled
// back, one capture owner may already have exited. Every owner that is still
// alive must nevertheless receive its cleanup command so an unavailable peer
// cannot strand healthy branches in candidate state.
func sendWorkerFanoutCleanupPhaseSet(
	ctx context.Context,
	transactionID string,
	phase string,
	targets map[string]workerFanoutPhaseTarget,
) (map[string]videoproviders.WorkerFanoutReceipt, error) {
	if ctx == nil {
		return nil, errors.New("worker fan-out cleanup context is required")
	}
	if phase != videoproviders.WorkerFanoutPhaseAbort &&
		phase != videoproviders.WorkerFanoutPhaseRollback {
		return nil, fmt.Errorf("worker fan-out phase %q is not a cleanup phase", phase)
	}
	if len(targets) == 0 {
		return nil, errors.New("worker fan-out cleanup requires at least one source")
	}
	sourceIDs := make([]string, 0, len(targets))
	for sourceID := range targets {
		sourceIDs = append(sourceIDs, sourceID)
	}
	sort.Strings(sourceIDs)

	results := make(chan workerFanoutPhaseResult, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		target := targets[sourceID]
		go func(sourceID string, target workerFanoutPhaseTarget) {
			if target.Runtime == nil {
				results <- workerFanoutPhaseResult{
					sourceID: sourceID, err: errors.New("worker fan-out runtime is unavailable"),
				}
				return
			}
			receipt, err := target.Runtime.sendWorkerFanoutPhase(
				ctx, transactionID, sourceID, target.CapturePID, target.MediaEpoch,
				phase, nil,
			)
			results <- workerFanoutPhaseResult{sourceID: sourceID, receipt: receipt, err: err}
		}(sourceID, target)
	}

	receipts := make(map[string]videoproviders.WorkerFanoutReceipt, len(sourceIDs))
	var combined error
	for range sourceIDs {
		result := <-results
		if result.err != nil {
			combined = errors.Join(combined, fmt.Errorf(
				"worker fan-out source %q phase %q: %w", result.sourceID, phase, result.err))
			continue
		}
		receipts[result.sourceID] = result.receipt
	}
	return receipts, combined
}

// validateWorkerFanoutCleanupSettlement distinguishes an unavailable frozen
// worker (its process-owned candidate resources are already gone) from a live
// worker that failed to acknowledge cleanup. Only the latter remains unsafe
// and must retain the peak-capacity reservation fail-closed.
func validateWorkerFanoutCleanupSettlement(
	targets map[string]workerFanoutPhaseTarget,
	receipts map[string]videoproviders.WorkerFanoutReceipt,
) error {
	sourceIDs := make([]string, 0, len(targets))
	for sourceID := range targets {
		sourceIDs = append(sourceIDs, sourceID)
	}
	sort.Strings(sourceIDs)
	var combined error
	for _, sourceID := range sourceIDs {
		if _, acknowledged := receipts[sourceID]; acknowledged {
			continue
		}
		target := targets[sourceID]
		if target.Runtime == nil {
			combined = errors.Join(combined, fmt.Errorf(
				"worker fan-out source %q cleanup has no runtime", sourceID))
			continue
		}
		target.Runtime.mu.Lock()
		frozenOwnerAlive := target.Runtime.childRunning && !target.Runtime.childFinishing &&
			brokerChildPID(target.Runtime.childCmd) == target.CapturePID &&
			target.Runtime.mediaEpoch == target.MediaEpoch
		target.Runtime.mu.Unlock()
		if frozenOwnerAlive {
			combined = errors.Join(combined, fmt.Errorf(
				"worker fan-out source %q cleanup was not acknowledged by the live frozen owner",
				sourceID))
		}
	}
	return combined
}

func validateWorkerFanoutPhaseSet(
	ctx context.Context,
	transactionID string,
	phase string,
	targets map[string]workerFanoutPhaseTarget,
) ([]string, map[string]workerFanoutPhaseTarget, error) {
	if ctx == nil {
		return nil, nil, errors.New("worker fan-out phase-set context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if len(targets) == 0 {
		return nil, nil, errors.New("worker fan-out phase-set requires at least one source")
	}
	sourceIDs := make([]string, 0, len(targets))
	for sourceID := range targets {
		sourceIDs = append(sourceIDs, sourceID)
	}
	sort.Strings(sourceIDs)
	normalizedTargets := make(map[string]workerFanoutPhaseTarget, len(targets))
	for _, sourceID := range sourceIDs {
		target := targets[sourceID]
		if target.Runtime == nil {
			return nil, nil, fmt.Errorf("worker fan-out source %q runtime is unavailable", sourceID)
		}
		changes := videoproviders.CanonicalizeWorkerFanoutChanges(target.Changes)
		probe := videoproviders.WorkerFanoutCommand{
			Schema: videoproviders.WorkerFanoutCommandSchema, TransactionID: transactionID,
			Sequence: 1, SourceID: sourceID, CapturePID: target.CapturePID,
			MediaEpoch: target.MediaEpoch, Phase: phase, Changes: changes,
		}
		if err := videoproviders.ValidateWorkerFanoutCommand(probe); err != nil {
			return nil, nil, fmt.Errorf("worker fan-out source %q phase %q: %w", sourceID, phase, err)
		}
		if phase == videoproviders.WorkerFanoutPhaseFirstFrame {
			preparedChanges := videoproviders.CanonicalizeWorkerFanoutChanges(target.PreparedChanges)
			prepared := probe
			prepared.Phase = videoproviders.WorkerFanoutPhasePrepare
			prepared.Changes = preparedChanges
			if err := videoproviders.ValidateWorkerFanoutCommand(prepared); err != nil {
				return nil, nil, fmt.Errorf(
					"worker fan-out source %q first-frame prepared changes: %w", sourceID, err)
			}
			target.PreparedChanges = preparedChanges
		}
		target.Runtime.mu.Lock()
		err := target.Runtime.validateWorkerFanoutOwnerLocked(
			sourceID, target.CapturePID, target.MediaEpoch)
		target.Runtime.mu.Unlock()
		if err != nil {
			return nil, nil, fmt.Errorf("worker fan-out source %q phase %q: %w", sourceID, phase, err)
		}
		target.Changes = changes
		normalizedTargets[sourceID] = target
	}
	return sourceIDs, normalizedTargets, nil
}
