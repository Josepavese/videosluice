package routing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Josepavese/videosluice/config"
)

const defaultRollbackTimeout = 10 * time.Second

const (
	ApplyPhaseValidate   = "validate"
	ApplyPhasePrepare    = "prepare"
	ApplyPhaseWarmup     = "warmup"
	ApplyPhaseAdmission  = "admission"
	ApplyPhaseFirstFrame = "first_frame"
	ApplyPhaseCutover    = "cutover"
	ApplyPhaseVerify     = "verify"
	ApplyPhasePersist    = "persist"
	ApplyPhaseRetire     = "retire"
	ApplyPhaseAbort      = "abort"
	ApplyPhaseRollback   = "rollback"
)

// PhaseObserver receives the current transaction phase. Implementations must
// return quickly and must not call back into the routing Store. The observer is
// deliberately best-effort telemetry: it cannot change transaction semantics.
type PhaseObserver func(phase string)

// GraphRuntime prepares one candidate graph without changing the currently
// published stream bindings. A failed Prepare must release everything it
// allocated before returning.
type GraphRuntime interface {
	PrepareGraph(context.Context, config.Config, config.Config, Plan, Plan, GraphDelta) (PreparedGraph, error)
}

// GraphValidator performs the read-only data-plane part of a preview. It must
// apply the same transition-support and current-runtime admission rules as
// PrepareGraph without allocating resources, starting workers or changing
// publication state. Keeping this separate from GraphRuntime prevents a
// preview from becoming a hidden warmup transaction.
type GraphValidator interface {
	ValidateGraph(context.Context, config.Config, config.Config, Plan, Plan, GraphDelta) error
}

// PreparedGraph is one runtime transaction. Warmup starts only the candidate
// work set without publishing it. Admit proves camera identity, negotiated
// media and every required hardware lease. AwaitFirstFrame proves that each
// changed stream has produced a fresh frame (and a complete decoder bootstrap
// for compressed video). Only Cutover may change published stream bindings.
// Cutover must leave unrelated sources and streams untouched. Rollback must be
// idempotent after any Cutover attempt, including a partial failure. Abort is
// used only before Cutover. Retire runs after persistence and permanently
// releases the superseded generation.
type PreparedGraph interface {
	Warmup(context.Context) error
	Admit(context.Context) error
	AwaitFirstFrame(context.Context) error
	Cutover(context.Context) error
	Verify(context.Context) error
	Rollback(context.Context) error
	Abort(context.Context) error
	Retire(context.Context) error
}

// ApplyError identifies the exact failed transaction phase. CleanupErr is
// populated when abort or rollback also failed; callers must treat that state
// as degraded rather than hiding it behind the original error.
type ApplyError struct {
	Phase      string
	Err        error
	CleanupErr error
}

func (err *ApplyError) Error() string {
	if err == nil {
		return "routing apply failed"
	}
	if err.CleanupErr != nil {
		return fmt.Sprintf("routing apply phase=%s: %v; cleanup: %v", err.Phase, err.Err, err.CleanupErr)
	}
	return fmt.Sprintf("routing apply phase=%s: %v", err.Phase, err.Err)
}

func (err *ApplyError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// ApplyGraph executes one current-only routing transaction. The Store lock is
// held from CAS validation through runtime verification and plan persistence,
// which serializes competing local controllers. Runtime rollback deliberately
// uses a short context detached from request cancellation so a disconnected
// caller cannot strand a partial cutover.
func ApplyGraph(
	ctx context.Context,
	catalog Catalog,
	store *Store,
	expectedRevision uint64,
	selections map[string]string,
	runtime GraphRuntime,
	sourceSelections ...map[string]string,
) (Plan, error) {
	return ApplyGraphObserved(ctx, catalog, store, expectedRevision, selections, runtime, nil, sourceSelections...)
}

// ApplyGraphObserved is ApplyGraph with a phase feed suitable for an
// asynchronous control plane. The observer never receives physical paths,
// provider details or unrestricted media values.
func ApplyGraphObserved(
	ctx context.Context,
	catalog Catalog,
	store *Store,
	expectedRevision uint64,
	selections map[string]string,
	runtime GraphRuntime,
	observer PhaseObserver,
	sourceSelections ...map[string]string,
) (Plan, error) {
	if ctx == nil {
		return Plan{}, errors.New("routing apply context is required")
	}
	if runtime == nil {
		return Plan{}, errors.New("routing graph runtime is required")
	}
	notifyPhase(observer, ApplyPhaseValidate)
	transaction, err := store.Begin(catalog, expectedRevision, selections, sourceSelections...)
	if err != nil {
		return Plan{}, &ApplyError{Phase: ApplyPhaseValidate, Err: err}
	}
	return applyTransaction(ctx, transaction, runtime, observer)
}

// RollbackGraph applies the last replaced selection set as a new transaction.
// It uses the same runtime gates and persistence ordering as a forward apply;
// rollback never reinstates an old revision or stream generation.
func RollbackGraph(
	ctx context.Context,
	catalog Catalog,
	store *Store,
	expectedRevision uint64,
	runtime GraphRuntime,
) (Plan, error) {
	return RollbackGraphObserved(ctx, catalog, store, expectedRevision, runtime, nil)
}

// RollbackGraphObserved reports the same phases as a forward transaction while
// applying the last bounded selection set as a new monotonic generation.
func RollbackGraphObserved(
	ctx context.Context,
	catalog Catalog,
	store *Store,
	expectedRevision uint64,
	runtime GraphRuntime,
	observer PhaseObserver,
) (Plan, error) {
	if ctx == nil {
		return Plan{}, errors.New("routing rollback context is required")
	}
	if runtime == nil {
		return Plan{}, errors.New("routing graph runtime is required")
	}
	notifyPhase(observer, ApplyPhaseValidate)
	transaction, err := store.BeginRollback(catalog, expectedRevision)
	if err != nil {
		return Plan{}, &ApplyError{Phase: ApplyPhaseValidate, Err: err}
	}
	return applyTransaction(ctx, transaction, runtime, observer)
}

func applyTransaction(
	ctx context.Context,
	transaction *Transaction,
	runtime GraphRuntime,
	observer PhaseObserver,
) (Plan, error) {
	candidate := transaction.Plan()
	notifyPhase(observer, ApplyPhasePrepare)
	delta, err := DiffConfigs(transaction.CurrentConfig(), transaction.Config())
	if err != nil {
		return Plan{}, finishBeforeCutover(transaction, nil, ApplyPhasePrepare, err, observer)
	}
	if !delta.Changed && selectionsEqual(transaction.CurrentPlan().SourceSelections, candidate.SourceSelections) {
		return Plan{}, finishBeforeCutover(
			transaction, nil, ApplyPhasePrepare,
			errors.New("routing selections do not change the materialized graph"),
			observer,
		)
	}
	prepared, err := runtime.PrepareGraph(
		ctx,
		transaction.CurrentConfig(),
		transaction.Config(),
		transaction.CurrentPlan(),
		candidate,
		delta,
	)
	if err != nil {
		return Plan{}, finishBeforeCutover(transaction, nil, ApplyPhasePrepare, err, observer)
	}
	if prepared == nil {
		return Plan{}, finishBeforeCutover(
			transaction, nil, ApplyPhasePrepare,
			errors.New("runtime returned no prepared graph"),
			observer,
		)
	}
	notifyPhase(observer, ApplyPhaseWarmup)
	if err := prepared.Warmup(ctx); err != nil {
		return Plan{}, finishBeforeCutover(transaction, prepared, ApplyPhaseWarmup, err, observer)
	}
	exclusive := false
	if capture, ok := prepared.(interface{ RequiresExclusiveCapture() bool }); ok {
		exclusive = capture.RequiresExclusiveCapture()
	}
	if exclusive {
		// A physical camera may have only one capture owner. Make the declared
		// break-before-make cutover first, then attest hardware and fresh frames.
		// Every subsequent failure (including admission) requires rollback.
		notifyPhase(observer, ApplyPhaseCutover)
		if err := prepared.Cutover(ctx); err != nil {
			return Plan{}, finishAfterCutover(transaction, prepared, ApplyPhaseCutover, err, observer)
		}
	}
	notifyPhase(observer, ApplyPhaseAdmission)
	if err := prepared.Admit(ctx); err != nil {
		if exclusive {
			return Plan{}, finishAfterCutover(transaction, prepared, ApplyPhaseAdmission, err, observer)
		}
		return Plan{}, finishBeforeCutover(transaction, prepared, ApplyPhaseAdmission, err, observer)
	}
	notifyPhase(observer, ApplyPhaseFirstFrame)
	if err := prepared.AwaitFirstFrame(ctx); err != nil {
		if exclusive {
			return Plan{}, finishAfterCutover(transaction, prepared, ApplyPhaseFirstFrame, err, observer)
		}
		return Plan{}, finishBeforeCutover(transaction, prepared, ApplyPhaseFirstFrame, err, observer)
	}
	if !exclusive {
		notifyPhase(observer, ApplyPhaseCutover)
		if err := prepared.Cutover(ctx); err != nil {
			return Plan{}, finishAfterCutover(transaction, prepared, ApplyPhaseCutover, err, observer)
		}
	}
	notifyPhase(observer, ApplyPhaseVerify)
	if err := prepared.Verify(ctx); err != nil {
		return Plan{}, finishAfterCutover(transaction, prepared, ApplyPhaseVerify, err, observer)
	}
	notifyPhase(observer, ApplyPhasePersist)
	if err := transaction.activate(); err != nil {
		if transaction.Activated() {
			notifyPhase(observer, ApplyPhaseRetire)
			cleanupCtx, cancel := applyCleanupContext()
			retireErr := prepared.Retire(cleanupCtx)
			cancel()
			transaction.close()
			return candidate, &ApplyError{Phase: ApplyPhasePersist, Err: err, CleanupErr: retireErr}
		}
		rollbackErr := finishRuntimeRollback(prepared, observer)
		transaction.close()
		return Plan{}, &ApplyError{Phase: ApplyPhasePersist, Err: err, CleanupErr: rollbackErr}
	}
	notifyPhase(observer, ApplyPhaseRetire)
	cleanupCtx, cancel := applyCleanupContext()
	retireErr := prepared.Retire(cleanupCtx)
	cancel()
	transaction.close()
	if retireErr != nil {
		return candidate, &ApplyError{Phase: ApplyPhaseRetire, Err: retireErr}
	}
	return candidate, nil
}

func finishBeforeCutover(
	transaction *Transaction,
	prepared PreparedGraph,
	phase string,
	cause error,
	observer PhaseObserver,
) error {
	var cleanupErr error
	if prepared != nil {
		notifyPhase(observer, ApplyPhaseAbort)
		cleanupCtx, cancel := applyCleanupContext()
		cleanupErr = prepared.Abort(cleanupCtx)
		cancel()
	}
	if err := transaction.Abort(); err != nil {
		cleanupErr = errors.Join(cleanupErr, err)
	}
	return &ApplyError{Phase: phase, Err: cause, CleanupErr: cleanupErr}
}

func finishAfterCutover(
	transaction *Transaction,
	prepared PreparedGraph,
	phase string,
	cause error,
	observer PhaseObserver,
) error {
	notifyPhase(observer, ApplyPhaseRollback)
	cleanupCtx, cancel := applyCleanupContext()
	rollbackErr := prepared.Rollback(cleanupCtx)
	cancel()
	if err := transaction.Abort(); err != nil {
		rollbackErr = errors.Join(rollbackErr, err)
	}
	return &ApplyError{Phase: phase, Err: cause, CleanupErr: rollbackErr}
}

func finishRuntimeRollback(prepared PreparedGraph, observer PhaseObserver) error {
	notifyPhase(observer, ApplyPhaseRollback)
	cleanupCtx, cancel := applyCleanupContext()
	rollbackErr := prepared.Rollback(cleanupCtx)
	cancel()
	return rollbackErr
}

func notifyPhase(observer PhaseObserver, phase string) {
	if observer != nil {
		observer(phase)
	}
}

func applyCleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), defaultRollbackTimeout)
}
