package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Josepavese/videosluice/capability"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/routing"
)

const workerFanoutCleanupTimeout = 10 * time.Second

// fanoutPublicationCoordinator owns the broker-side candidate publication
// paths. It must prepare private endpoints and branch sinks without changing a
// logical endpoint; only its prepared Cutover may switch affected publications.
// The worker backend deliberately has no knowledge of socket or shared-memory
// implementation details.
type fanoutPublicationCoordinator interface {
	ValidateFanoutPublications(
		context.Context,
		preservedOwnerFanout,
		map[string]sourceFanoutHandle,
		workerFanoutCapabilitySet,
	) error
	PrepareFanoutPublications(
		context.Context,
		string,
		preservedOwnerFanout,
		map[string]sourceFanoutHandle,
		workerFanoutCapabilitySet,
	) (fanoutPreparedPublications, error)
}

// fanoutPreparedPublications follows the same phase vocabulary as the graph
// transaction, but controls only publication state. The composing backend
// orders every worker phase before the corresponding publication phase.
type fanoutPreparedPublications interface {
	routing.PreparedGraph
	// AttestFirstFrame binds worker-reported role runtime modes to the private
	// candidate publications before their own first-frame gate advances.
	AttestFirstFrame(map[string]videoproviders.WorkerFanoutReceipt) error
	// StageCutover installs role-local delivery gates before the worker changes
	// branch ownership. Cutover then publishes the already-gated candidate.
	StageCutover(context.Context) error
	// StageRollback reinstalls those gates before the worker restores incumbent
	// branches, preventing either generation from reaching the wrong endpoint.
	StageRollback(context.Context) error
}

// workerFanoutBackend composes three independent authorities: hardware peak
// capacity, the capture worker branch state machine and broker publication
// cutover. It is not installed on Service until a real publication coordinator
// and real workers attest the complete current-only contract.
type workerFanoutBackend struct {
	inventory      capability.Inventory
	allocator      *capabilityAllocator
	publications   fanoutPublicationCoordinator
	transactionID  func() (string, error)
	cleanupTimeout time.Duration
}

func newWorkerFanoutBackend(
	inventory capability.Inventory,
	allocator *capabilityAllocator,
	publications fanoutPublicationCoordinator,
) (*workerFanoutBackend, error) {
	backend := &workerFanoutBackend{
		inventory: inventory, allocator: allocator, publications: publications,
		transactionID: newWorkerFanoutTransactionID, cleanupTimeout: workerFanoutCleanupTimeout,
	}
	if err := backend.validateDependencies(); err != nil {
		return nil, err
	}
	return backend, nil
}

func (backend *workerFanoutBackend) ValidateFanout(
	ctx context.Context,
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
) error {
	set, err := backend.inspectFanout(ctx, transition, handles)
	if err != nil {
		return err
	}
	if err := backend.allocator.validateTransition(
		"fanout-preview-check", backend.inventory,
		set.Current, set.Candidate, set.Warmup,
	); err != nil {
		return fmt.Errorf("worker fan-out capacity preview: %w", err)
	}
	if err := backend.publications.ValidateFanoutPublications(
		ctx, transition, handles, set,
	); err != nil {
		return fmt.Errorf("worker fan-out publication preview: %w", err)
	}
	return nil
}

func (backend *workerFanoutBackend) PrepareFanout(
	ctx context.Context,
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
) (routing.PreparedGraph, error) {
	set, err := backend.inspectFanout(ctx, transition, handles)
	if err != nil {
		return nil, err
	}
	if err := backend.publications.ValidateFanoutPublications(
		ctx, transition, handles, set,
	); err != nil {
		return nil, fmt.Errorf("worker fan-out publication prepare validation: %w", err)
	}
	transactionID, err := backend.transactionID()
	if err != nil {
		return nil, fmt.Errorf("create worker fan-out transaction identity: %w", err)
	}
	targets := workerFanoutTargets(transition.SourceIDs, handles, set.Changes, true, false)
	if _, _, err := validateWorkerFanoutPhaseSet(
		ctx, transactionID, videoproviders.WorkerFanoutPhasePrepare, targets,
	); err != nil {
		return nil, fmt.Errorf("worker fan-out prepare preflight: %w", err)
	}
	reservation, err := backend.allocator.reserveTransition(
		transactionID, backend.inventory, set.Current, set.Candidate, set.Warmup)
	if err != nil {
		return nil, fmt.Errorf("worker fan-out capacity reservation: %w", err)
	}
	publications, err := backend.publications.PrepareFanoutPublications(
		ctx, transactionID, transition, handles, set,
	)
	if err != nil || publications == nil {
		if err == nil {
			err = errors.New("publication coordinator returned no prepared transaction")
		}
		cleanupErr := backend.cleanupPublicationPrepare(publications, reservation)
		return nil, joinFanoutPrepareFailure(
			fmt.Errorf("worker fan-out publication prepare: %w", err), cleanupErr)
	}
	receipts, err := sendWorkerFanoutPhaseSet(
		ctx, transactionID, videoproviders.WorkerFanoutPhasePrepare, targets)
	if err != nil {
		cleanupErr := backend.cleanupPrepare(
			transactionID, targets, receipts, publications, reservation)
		return nil, joinFanoutPrepareFailure(err, cleanupErr)
	}
	return &workerFanoutPreparedGraph{
		transactionID: transactionID, sourceIDs: append([]string(nil), transition.SourceIDs...),
		handles: handles, changes: set.Changes, publications: publications,
		reservation: reservation,
	}, nil
}

func (backend *workerFanoutBackend) cleanupPublicationPrepare(
	publications fanoutPreparedPublications,
	reservation *capabilityTransitionReservation,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), backend.cleanupTimeout)
	defer cancel()
	var cleanupErr error
	if publications != nil {
		cleanupErr = publications.Abort(ctx)
	}
	if cleanupErr == nil {
		cleanupErr = reservation.Abort()
	}
	return cleanupErr
}

func (backend *workerFanoutBackend) inspectFanout(
	ctx context.Context,
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
) (workerFanoutCapabilitySet, error) {
	if err := backend.validateDependencies(); err != nil {
		return workerFanoutCapabilitySet{}, err
	}
	if err := contextError(ctx); err != nil {
		return workerFanoutCapabilitySet{}, err
	}
	set, err := buildWorkerFanoutCapabilitySet(transition, handles, backend.inventory)
	if err != nil {
		return workerFanoutCapabilitySet{}, err
	}
	for _, sourceID := range transition.SourceIDs {
		handle := handles[sourceID]
		if !handle.CaptureAvailable {
			if !fanoutSourceIsRetiring(transition, sourceID) {
				return workerFanoutCapabilitySet{}, fmt.Errorf(
					"worker fan-out source %q is unavailable without complete retirement", sourceID)
			}
			continue
		}
		handle.Runtime.mu.Lock()
		err := handle.Runtime.validateWorkerFanoutOwnerLocked(
			sourceID, handle.ChildPID, handle.MediaEpoch)
		handle.Runtime.mu.Unlock()
		if err != nil {
			return workerFanoutCapabilitySet{}, fmt.Errorf(
				"worker fan-out source %q: %w", sourceID, err)
		}
	}
	return set, nil
}

func (backend *workerFanoutBackend) validateDependencies() error {
	if backend == nil || backend.allocator == nil {
		return errors.New("worker fan-out capability allocator is unavailable")
	}
	if backend.publications == nil {
		return errors.New("worker fan-out publication coordinator is unavailable")
	}
	if backend.transactionID == nil {
		return errors.New("worker fan-out transaction identity source is unavailable")
	}
	if backend.cleanupTimeout <= 0 {
		return errors.New("worker fan-out cleanup timeout is invalid")
	}
	return nil
}

func (backend *workerFanoutBackend) cleanupPrepare(
	transactionID string,
	targets map[string]workerFanoutPhaseTarget,
	preparedReceipts map[string]videoproviders.WorkerFanoutReceipt,
	publications fanoutPreparedPublications,
	reservation *capabilityTransitionReservation,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), backend.cleanupTimeout)
	defer cancel()
	var cleanupErr error
	if len(preparedReceipts) > 0 {
		succeeded := make(map[string]workerFanoutPhaseTarget, len(preparedReceipts))
		for sourceID := range preparedReceipts {
			target := targets[sourceID]
			target.Changes = nil
			target.PreparedChanges = nil
			succeeded[sourceID] = target
		}
		receipts, workerErr := sendWorkerFanoutCleanupPhaseSet(
			ctx, transactionID, videoproviders.WorkerFanoutPhaseAbort, succeeded,
		)
		settlementErr := validateWorkerFanoutCleanupSettlement(succeeded, receipts)
		if settlementErr != nil {
			cleanupErr = errors.Join(cleanupErr, workerErr, settlementErr)
		}
	}
	if err := publications.Abort(ctx); err != nil {
		cleanupErr = errors.Join(cleanupErr, err)
	}
	if cleanupErr == nil {
		cleanupErr = reservation.Abort()
	}
	return cleanupErr
}

func joinFanoutPrepareFailure(cause, cleanupErr error) error {
	if cleanupErr == nil {
		return cause
	}
	return errors.Join(cause, fmt.Errorf("worker fan-out prepare cleanup: %w", cleanupErr))
}

func newWorkerFanoutTransactionID() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return "fanout-" + hex.EncodeToString(nonce[:]), nil
}

func workerFanoutTargets(
	sourceIDs []string,
	handles map[string]sourceFanoutHandle,
	changes map[string][]videoproviders.WorkerBranchChange,
	includeChanges bool,
	includePreparedChanges bool,
) map[string]workerFanoutPhaseTarget {
	targets := make(map[string]workerFanoutPhaseTarget, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		handle := handles[sourceID]
		if !handle.CaptureAvailable {
			continue
		}
		target := workerFanoutPhaseTarget{
			Runtime: handle.Runtime, CapturePID: handle.ChildPID, MediaEpoch: handle.MediaEpoch,
		}
		if includeChanges {
			target.Changes = changes[sourceID]
		}
		if includePreparedChanges {
			target.PreparedChanges = changes[sourceID]
		}
		targets[sourceID] = target
	}
	return targets
}

type workerFanoutPreparedGraph struct {
	mu sync.Mutex

	transactionID string
	sourceIDs     []string
	handles       map[string]sourceFanoutHandle
	changes       map[string][]videoproviders.WorkerBranchChange
	publications  fanoutPreparedPublications
	reservation   *capabilityTransitionReservation
	phase         int
	cutover       bool
	terminal      bool
}

func (prepared *workerFanoutPreparedGraph) Warmup(ctx context.Context) error {
	return prepared.runPhase(ctx, 0, videoproviders.WorkerFanoutPhaseWarmup, prepared.publications.Warmup)
}

func (prepared *workerFanoutPreparedGraph) Admit(ctx context.Context) error {
	return prepared.runPhase(ctx, 1, videoproviders.WorkerFanoutPhaseAdmit, prepared.publications.Admit)
}

func (prepared *workerFanoutPreparedGraph) AwaitFirstFrame(ctx context.Context) error {
	return prepared.runPhase(
		ctx, 2, videoproviders.WorkerFanoutPhaseFirstFrame, prepared.publications.AwaitFirstFrame)
}

func (prepared *workerFanoutPreparedGraph) Cutover(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(3); err != nil {
		return err
	}
	if err := prepared.publications.StageCutover(ctx); err != nil {
		return err
	}
	prepared.cutover = true
	if _, err := sendWorkerFanoutPhaseSet(
		ctx, prepared.transactionID, videoproviders.WorkerFanoutPhaseCutover,
		prepared.targets(false),
	); err != nil {
		return err
	}
	if err := prepared.publications.Cutover(ctx); err != nil {
		return err
	}
	prepared.phase = 4
	return nil
}

func (prepared *workerFanoutPreparedGraph) Verify(ctx context.Context) error {
	return prepared.runPhase(ctx, 4, videoproviders.WorkerFanoutPhaseVerify, prepared.publications.Verify)
}

func (prepared *workerFanoutPreparedGraph) runPhase(
	ctx context.Context,
	expected int,
	workerPhase string,
	publicationPhase func(context.Context) error,
) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(expected); err != nil {
		return err
	}
	receipts, err := sendWorkerFanoutPhaseSet(
		ctx, prepared.transactionID, workerPhase,
		prepared.targets(workerPhase == videoproviders.WorkerFanoutPhaseFirstFrame),
	)
	if err != nil {
		return err
	}
	if workerPhase == videoproviders.WorkerFanoutPhaseFirstFrame {
		if err := prepared.publications.AttestFirstFrame(receipts); err != nil {
			return err
		}
	}
	if err := publicationPhase(ctx); err != nil {
		return err
	}
	prepared.phase++
	return nil
}

func (prepared *workerFanoutPreparedGraph) Rollback(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared == nil || prepared.terminal {
		return nil
	}
	if !prepared.cutover {
		targets := prepared.targets(false)
		receipts, phaseErr := sendWorkerFanoutCleanupPhaseSet(
			ctx, prepared.transactionID, videoproviders.WorkerFanoutPhaseAbort, targets)
		workerErr := validateWorkerFanoutCleanupSettlement(targets, receipts)
		if workerErr != nil {
			workerErr = errors.Join(phaseErr, workerErr)
		}
		publicationErr := prepared.publications.Abort(ctx)
		err := errors.Join(workerErr, publicationErr)
		if err == nil {
			err = prepared.reservation.Abort()
		}
		prepared.terminal = true
		return err
	}
	if err := prepared.publications.StageRollback(ctx); err != nil {
		prepared.terminal = true
		return err
	}
	targets := prepared.targets(false)
	receipts, phaseErr := sendWorkerFanoutCleanupPhaseSet(
		ctx, prepared.transactionID, videoproviders.WorkerFanoutPhaseRollback, targets)
	workerErr := validateWorkerFanoutCleanupSettlement(targets, receipts)
	if workerErr != nil {
		workerErr = errors.Join(phaseErr, workerErr)
	}
	// StageRollback has already closed every changed logical role. Restore the
	// incumbent publications even when a dead peer cannot acknowledge worker
	// rollback; otherwise one failed candidate can black-hole a healthy source.
	publicationErr := prepared.publications.Rollback(ctx)
	err := errors.Join(workerErr, publicationErr)
	if err == nil {
		err = prepared.reservation.Abort()
	}
	prepared.terminal = true
	return err
}

func (prepared *workerFanoutPreparedGraph) Abort(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared == nil || prepared.terminal {
		return nil
	}
	if prepared.cutover {
		return errors.New("worker fan-out cutover requires rollback, not abort")
	}
	targets := prepared.targets(false)
	receipts, phaseErr := sendWorkerFanoutCleanupPhaseSet(
		ctx, prepared.transactionID, videoproviders.WorkerFanoutPhaseAbort, targets)
	workerErr := validateWorkerFanoutCleanupSettlement(targets, receipts)
	if workerErr != nil {
		workerErr = errors.Join(phaseErr, workerErr)
	}
	publicationErr := prepared.publications.Abort(ctx)
	err := errors.Join(workerErr, publicationErr)
	if err == nil {
		err = prepared.reservation.Abort()
	}
	prepared.terminal = true
	return err
}

func (prepared *workerFanoutPreparedGraph) Retire(ctx context.Context) error {
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if err := prepared.requirePhaseLocked(5); err != nil {
		return err
	}
	_, workerErr := sendWorkerFanoutPhaseSet(
		ctx, prepared.transactionID, videoproviders.WorkerFanoutPhaseRetire, prepared.targets(false))
	var publicationErr error
	if workerErr == nil {
		publicationErr = prepared.publications.Retire(ctx)
	}
	err := errors.Join(workerErr, publicationErr)
	if err == nil {
		err = prepared.reservation.Commit()
	}
	prepared.terminal = true
	return err
}

func (prepared *workerFanoutPreparedGraph) targets(
	includePreparedChanges bool,
) map[string]workerFanoutPhaseTarget {
	return workerFanoutTargets(
		prepared.sourceIDs, prepared.handles, prepared.changes, false, includePreparedChanges)
}

func (prepared *workerFanoutPreparedGraph) requirePhaseLocked(expected int) error {
	if prepared == nil || prepared.publications == nil || prepared.reservation == nil ||
		prepared.transactionID == "" || prepared.terminal {
		return errors.New("worker fan-out prepared transaction is invalid")
	}
	if prepared.phase != expected {
		return fmt.Errorf(
			"worker fan-out phase order invalid current=%d expected=%d", prepared.phase, expected)
	}
	return nil
}

var _ sourceFanoutBackend = (*workerFanoutBackend)(nil)
var _ routing.PreparedGraph = (*workerFanoutPreparedGraph)(nil)
