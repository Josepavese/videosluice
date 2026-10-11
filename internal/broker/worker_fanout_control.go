package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

type workerFanoutWrite struct {
	command videoproviders.WorkerFanoutCommand
	written chan error
}

type workerFanoutReceiptKey struct {
	transactionID string
	sequence      uint64
}

type workerFanoutReceiptResult struct {
	receipt videoproviders.WorkerFanoutReceipt
	err     error
}

// sendWorkerFanoutPhase sends one exact phase to the capture owner and waits
// for the receipt observed on that same child's stderr pipe. The runtime fills
// PID, media epoch and sequence itself, so an adapter cannot accidentally issue
// a command against a stale snapshot or reuse a prior phase's fence.
func (rt *videoBrokerRuntime) sendWorkerFanoutPhase(
	ctx context.Context,
	transactionID string,
	sourceID string,
	expectedCapturePID int,
	expectedMediaEpoch uint64,
	phase string,
	changes []videoproviders.WorkerBranchChange,
) (videoproviders.WorkerFanoutReceipt, error) {
	if ctx == nil {
		return videoproviders.WorkerFanoutReceipt{}, errors.New("worker fan-out context is required")
	}
	if err := ctx.Err(); err != nil {
		return videoproviders.WorkerFanoutReceipt{}, err
	}
	if rt == nil {
		return videoproviders.WorkerFanoutReceipt{}, errors.New("worker fan-out runtime is unavailable")
	}

	rt.mu.Lock()
	if err := rt.validateWorkerFanoutOwnerLocked(sourceID, expectedCapturePID, expectedMediaEpoch); err != nil {
		rt.mu.Unlock()
		return videoproviders.WorkerFanoutReceipt{}, err
	}
	rt.workerFanoutSequence++
	if rt.workerFanoutSequence == 0 {
		rt.workerFanoutSequence++
	}
	command := videoproviders.WorkerFanoutCommand{
		Schema:        videoproviders.WorkerFanoutCommandSchema,
		TransactionID: transactionID, Sequence: rt.workerFanoutSequence,
		SourceID: sourceID, CapturePID: expectedCapturePID,
		MediaEpoch: expectedMediaEpoch, Phase: phase,
		Changes: videoproviders.CanonicalizeWorkerFanoutChanges(changes),
	}
	if err := videoproviders.ValidateWorkerFanoutCommand(command); err != nil {
		rt.mu.Unlock()
		return videoproviders.WorkerFanoutReceipt{}, err
	}
	key := workerFanoutReceiptKey{transactionID: command.TransactionID, sequence: command.Sequence}
	if rt.workerFanoutWaiters == nil {
		rt.workerFanoutWaiters = make(map[workerFanoutReceiptKey]chan workerFanoutReceiptResult)
	}
	if _, exists := rt.workerFanoutWaiters[key]; exists {
		rt.mu.Unlock()
		return videoproviders.WorkerFanoutReceipt{}, errors.New("worker fan-out phase fence is already pending")
	}
	receipts := make(chan workerFanoutReceiptResult, 1)
	rt.workerFanoutWaiters[key] = receipts
	writes := rt.childFanoutControl
	rt.mu.Unlock()

	defer func() {
		rt.mu.Lock()
		if rt.workerFanoutWaiters[key] == receipts {
			delete(rt.workerFanoutWaiters, key)
		}
		rt.mu.Unlock()
	}()

	written := make(chan error, 1)
	select {
	case <-ctx.Done():
		return videoproviders.WorkerFanoutReceipt{}, ctx.Err()
	case writes <- workerFanoutWrite{command: command, written: written}:
	}
	select {
	case <-ctx.Done():
		return videoproviders.WorkerFanoutReceipt{}, ctx.Err()
	case err := <-written:
		if err != nil {
			return videoproviders.WorkerFanoutReceipt{}, err
		}
	}
	select {
	case <-ctx.Done():
		return videoproviders.WorkerFanoutReceipt{}, ctx.Err()
	case result := <-receipts:
		if result.err != nil {
			return videoproviders.WorkerFanoutReceipt{}, result.err
		}
		if err := videoproviders.MatchWorkerFanoutReceipt(command, result.receipt); err != nil {
			return videoproviders.WorkerFanoutReceipt{}, err
		}
		return result.receipt, nil
	}
}

func (rt *videoBrokerRuntime) validateWorkerFanoutOwnerLocked(
	sourceID string,
	expectedCapturePID int,
	expectedMediaEpoch uint64,
) error {
	childPID := brokerChildPID(rt.childCmd)
	if !rt.childRunning || rt.childFinishing || childPID <= 0 || rt.mediaEpoch == 0 {
		return errors.New("worker fan-out capture owner is unavailable")
	}
	if childPID != expectedCapturePID || rt.mediaEpoch != expectedMediaEpoch {
		return errors.New("worker fan-out capture owner differs from frozen transaction")
	}
	if rt.plan.SourceID != sourceID {
		return errors.New("worker fan-out source differs from capture owner")
	}
	if !rt.childFanoutSupported || rt.childFanoutControl == nil {
		return errors.New("capture worker does not attest dynamic fan-out control")
	}
	return nil
}

// acceptWorkerFanoutReceipt is called only by the stderr reader that owns the
// child PID/media epoch arguments. It returns true for a protocol-looking line,
// including a malformed one, so such input cannot be interpreted again as
// unrelated native telemetry.
func (rt *videoBrokerRuntime) acceptWorkerFanoutReceipt(line string, mediaEpoch uint64, childPID int) bool {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "{") {
		return false
	}
	var header struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal([]byte(trimmed), &header); err != nil {
		if strings.Contains(trimmed, videoproviders.WorkerFanoutReceiptSchema) {
			rt.logf("[video-broker] event=worker_fanout_receipt_rejected reason=invalid_envelope")
			return true
		}
		return false
	}
	if header.Schema != videoproviders.WorkerFanoutReceiptSchema {
		return false
	}
	receipt, err := videoproviders.DecodeWorkerFanoutReceipt(append([]byte(trimmed), '\n'))
	if err != nil {
		rt.logf("[video-broker] event=worker_fanout_receipt_rejected reason=invalid_envelope")
		return true
	}
	key := workerFanoutReceiptKey{transactionID: receipt.TransactionID, sequence: receipt.Sequence}
	rt.mu.Lock()
	waiter := rt.workerFanoutWaiters[key]
	current := rt.childRunning && !rt.childFinishing && rt.mediaEpoch == mediaEpoch &&
		brokerChildPID(rt.childCmd) == childPID && receipt.MediaEpoch == mediaEpoch &&
		receipt.CapturePID == childPID && receipt.SourceID == rt.plan.SourceID
	if waiter != nil && current {
		delete(rt.workerFanoutWaiters, key)
	}
	rt.mu.Unlock()
	if waiter == nil || !current {
		rt.logf(
			"[video-broker] event=worker_fanout_receipt_ignored transaction_id=%s sequence=%d reason=stale_or_unowned",
			receipt.TransactionID, receipt.Sequence)
		return true
	}
	select {
	case waiter <- workerFanoutReceiptResult{receipt: receipt}:
	default:
		rt.logf(
			"[video-broker] event=worker_fanout_receipt_ignored transaction_id=%s sequence=%d reason=waiter_saturated",
			receipt.TransactionID, receipt.Sequence)
	}
	return true
}

func (rt *videoBrokerRuntime) failWorkerFanoutWaitersLocked(err error) {
	for key, waiter := range rt.workerFanoutWaiters {
		delete(rt.workerFanoutWaiters, key)
		select {
		case waiter <- workerFanoutReceiptResult{err: err}:
		default:
		}
	}
}

func writeWorkerFanoutCommand(input io.Writer, command videoproviders.WorkerFanoutCommand) error {
	payload, err := videoproviders.EncodeWorkerFanoutCommand(command)
	if err != nil {
		return err
	}
	written := 0
	for written < len(payload) {
		count, writeErr := input.Write(payload[written:])
		if writeErr != nil {
			return fmt.Errorf("write worker fan-out command: %w", writeErr)
		}
		if count <= 0 {
			return errors.New("write worker fan-out command: short write")
		}
		written += count
	}
	return nil
}
