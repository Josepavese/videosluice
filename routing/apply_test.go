package routing

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/config"
)

type fakeGraphRuntime struct {
	mu          sync.Mutex
	calls       []string
	prepareErr  error
	prepared    *fakePreparedGraph
	current     config.Config
	candidate   config.Config
	currentID   Plan
	candidateID Plan
	delta       GraphDelta
}

func (runtime *fakeGraphRuntime) PrepareGraph(
	_ context.Context,
	current config.Config,
	candidate config.Config,
	currentPlan Plan,
	candidatePlan Plan,
	delta GraphDelta,
) (PreparedGraph, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.calls = append(runtime.calls, ApplyPhasePrepare)
	runtime.current = current
	runtime.candidate = candidate
	runtime.currentID = currentPlan
	runtime.candidateID = candidatePlan
	runtime.delta = delta
	if runtime.prepareErr != nil {
		return nil, runtime.prepareErr
	}
	if runtime.prepared == nil {
		return nil, nil
	}
	runtime.prepared.runtime = runtime
	return runtime.prepared, nil
}

func (runtime *fakeGraphRuntime) record(call string) {
	runtime.mu.Lock()
	runtime.calls = append(runtime.calls, call)
	runtime.mu.Unlock()
}

func (runtime *fakeGraphRuntime) callSequence() []string {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return append([]string(nil), runtime.calls...)
}

type fakePreparedGraph struct {
	runtime       *fakeGraphRuntime
	warmupErr     error
	admissionErr  error
	firstFrameErr error
	cutoverErr    error
	verifyErr     error
	rollbackErr   error
	abortErr      error
	retireErr     error
	rollbackHook  func(context.Context)
	retireHook    func(context.Context)
}

func (prepared *fakePreparedGraph) Warmup(context.Context) error {
	prepared.runtime.record(ApplyPhaseWarmup)
	return prepared.warmupErr
}

func (prepared *fakePreparedGraph) Admit(context.Context) error {
	prepared.runtime.record(ApplyPhaseAdmission)
	return prepared.admissionErr
}

func (prepared *fakePreparedGraph) AwaitFirstFrame(context.Context) error {
	prepared.runtime.record(ApplyPhaseFirstFrame)
	return prepared.firstFrameErr
}

func (prepared *fakePreparedGraph) Cutover(context.Context) error {
	prepared.runtime.record(ApplyPhaseCutover)
	return prepared.cutoverErr
}

func (prepared *fakePreparedGraph) Verify(context.Context) error {
	prepared.runtime.record(ApplyPhaseVerify)
	return prepared.verifyErr
}

func (prepared *fakePreparedGraph) Rollback(ctx context.Context) error {
	prepared.runtime.record("rollback")
	if prepared.rollbackHook != nil {
		prepared.rollbackHook(ctx)
	}
	return prepared.rollbackErr
}

func (prepared *fakePreparedGraph) Abort(context.Context) error {
	prepared.runtime.record("abort")
	return prepared.abortErr
}

func (prepared *fakePreparedGraph) Retire(ctx context.Context) error {
	prepared.runtime.record(ApplyPhaseRetire)
	if prepared.retireHook != nil {
		prepared.retireHook(ctx)
	}
	return prepared.retireErr
}

func applySelection(source string) map[string]string {
	suffix := source[len(source)-1:]
	return map[string]string{
		"live": "live_" + suffix, "analysis": "analysis_" + suffix,
		"archive": "archive_" + suffix,
	}
}

func newApplyStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestApplyGraphCommitsOnlyAfterRuntimeVerification(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	runtime := &fakeGraphRuntime{prepared: &fakePreparedGraph{}}
	plan, err := ApplyGraph(context.Background(), catalog, store, 1, applySelection("camera_b"), runtime)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Revision != 2 || plan.Selections["live"] != "live_b" ||
		plan.StreamGenerations["live"] != 2 || plan.StreamGenerations["analysis"] != 2 ||
		plan.StreamGenerations["archive"] != 2 ||
		runtime.current.Streams["analysis"].SourceRef != "camera_a" || runtime.currentID.Revision != 1 ||
		runtime.currentID.StreamGenerations["analysis"] != 1 ||
		runtime.candidate.Streams["analysis"].SourceRef != "camera_b" || runtime.candidateID.Revision != 2 ||
		runtime.candidateID.StreamGenerations["analysis"] != 2 ||
		!runtime.delta.Changed || len(runtime.delta.ChangedStreamIDs) != 3 {
		t.Fatalf("unexpected committed candidate: plan=%+v config=%+v", plan, runtime.candidate)
	}
	wantCalls := []string{
		ApplyPhasePrepare, ApplyPhaseWarmup, ApplyPhaseAdmission,
		ApplyPhaseFirstFrame, ApplyPhaseCutover, ApplyPhaseVerify, ApplyPhaseRetire,
	}
	if got := runtime.callSequence(); !reflect.DeepEqual(got, wantCalls) {
		t.Fatalf("transaction phase order=%v want=%v", got, wantCalls)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, plan) {
		t.Fatalf("persisted plan=%+v want=%+v", persisted, plan)
	}
}

func TestApplyGraphObservedPublishesExactSuccessAndRollbackPhases(t *testing.T) {
	for _, test := range []struct {
		name       string
		cutoverErr error
		want       []string
	}{
		{
			name: "success",
			want: []string{
				ApplyPhaseValidate, ApplyPhasePrepare, ApplyPhaseWarmup,
				ApplyPhaseAdmission, ApplyPhaseFirstFrame, ApplyPhaseCutover,
				ApplyPhaseVerify, ApplyPhasePersist, ApplyPhaseRetire,
			},
		},
		{
			name:       "rollback",
			cutoverErr: errors.New("cutover failed"),
			want: []string{
				ApplyPhaseValidate, ApplyPhasePrepare, ApplyPhaseWarmup,
				ApplyPhaseAdmission, ApplyPhaseFirstFrame, ApplyPhaseCutover,
				ApplyPhaseRollback,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := testCatalog(2)
			store := newApplyStore(t)
			runtime := &fakeGraphRuntime{prepared: &fakePreparedGraph{cutoverErr: test.cutoverErr}}
			var observed []string
			_, err := ApplyGraphObserved(
				context.Background(), catalog, store, 1, applySelection("camera_b"), runtime,
				func(phase string) { observed = append(observed, phase) },
			)
			if (test.cutoverErr == nil) != (err == nil) {
				t.Fatalf("unexpected apply error: %v", err)
			}
			if !reflect.DeepEqual(observed, test.want) {
				t.Fatalf("observed phases=%v want=%v", observed, test.want)
			}
		})
	}
}

func TestApplyGraphRejectsBeforeRuntimeMutation(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	for name, selections := range map[string]map[string]string{
		"unchanged": cloneSelections(catalog.DefaultSelections),
		"invalid":   {"live": "live_b", "analysis": "analysis_b", "archive": "raw_device"},
	} {
		t.Run(name, func(t *testing.T) {
			runtime := &fakeGraphRuntime{prepared: &fakePreparedGraph{}}
			if _, err := ApplyGraph(context.Background(), catalog, store, 1, selections, runtime); err == nil {
				t.Fatal("invalid/no-op routing apply succeeded")
			}
			if calls := runtime.callSequence(); len(calls) != 0 {
				t.Fatalf("runtime mutated before validation: %v", calls)
			}
		})
	}
}

func TestApplyGraphCleanupMatchesFailureBoundary(t *testing.T) {
	cause := errors.New("phase failed")
	for _, test := range []struct {
		name      string
		configure func(*fakePreparedGraph, *fakeGraphRuntime)
		wantPhase string
		wantLast  string
	}{
		{
			name: "prepare", wantPhase: ApplyPhasePrepare, wantLast: ApplyPhasePrepare,
			configure: func(_ *fakePreparedGraph, runtime *fakeGraphRuntime) { runtime.prepareErr = cause },
		},
		{
			name: "warmup", wantPhase: ApplyPhaseWarmup, wantLast: "abort",
			configure: func(prepared *fakePreparedGraph, _ *fakeGraphRuntime) { prepared.warmupErr = cause },
		},
		{
			name: "admission", wantPhase: ApplyPhaseAdmission, wantLast: "abort",
			configure: func(prepared *fakePreparedGraph, _ *fakeGraphRuntime) { prepared.admissionErr = cause },
		},
		{
			name: "first-frame", wantPhase: ApplyPhaseFirstFrame, wantLast: "abort",
			configure: func(prepared *fakePreparedGraph, _ *fakeGraphRuntime) { prepared.firstFrameErr = cause },
		},
		{
			name: "cutover", wantPhase: ApplyPhaseCutover, wantLast: "rollback",
			configure: func(prepared *fakePreparedGraph, _ *fakeGraphRuntime) { prepared.cutoverErr = cause },
		},
		{
			name: "verify", wantPhase: ApplyPhaseVerify, wantLast: "rollback",
			configure: func(prepared *fakePreparedGraph, _ *fakeGraphRuntime) { prepared.verifyErr = cause },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := testCatalog(2)
			store := newApplyStore(t)
			prepared := &fakePreparedGraph{}
			runtime := &fakeGraphRuntime{prepared: prepared}
			test.configure(prepared, runtime)
			_, err := ApplyGraph(context.Background(), catalog, store, 1, applySelection("camera_b"), runtime)
			var applyErr *ApplyError
			if !errors.As(err, &applyErr) || applyErr.Phase != test.wantPhase || !errors.Is(err, cause) {
				t.Fatalf("unexpected error: %#v", err)
			}
			calls := runtime.callSequence()
			if len(calls) == 0 || calls[len(calls)-1] != test.wantLast {
				t.Fatalf("cleanup sequence=%v want last=%s", calls, test.wantLast)
			}
			persisted, readErr := store.Read(catalog)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if persisted.Revision != 1 || !reflect.DeepEqual(persisted.Selections, catalog.DefaultSelections) {
				t.Fatalf("failed transaction changed persisted plan: %+v", persisted)
			}
		})
	}
}

func TestApplyGraphRollbackOutlivesCanceledRequest(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	requestCtx, cancel := context.WithCancel(context.Background())
	prepared := &fakePreparedGraph{cutoverErr: errors.New("partial cutover")}
	prepared.rollbackHook = func(cleanupCtx context.Context) {
		if cleanupCtx.Err() != nil {
			t.Fatalf("rollback inherited canceled request: %v", cleanupCtx.Err())
		}
	}
	runtime := &fakeGraphRuntime{prepared: prepared}
	cancel()
	_, err := ApplyGraph(requestCtx, catalog, store, 1, applySelection("camera_b"), runtime)
	if err == nil {
		t.Fatal("canceled cutover unexpectedly succeeded")
	}
	if got := runtime.callSequence(); len(got) == 0 || got[len(got)-1] != "rollback" {
		t.Fatalf("rollback was not attempted: %v", got)
	}
}

func TestApplyGraphRetireFailureKeepsVerifiedPlanCommitted(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	cause := errors.New("old generation did not retire")
	runtime := &fakeGraphRuntime{prepared: &fakePreparedGraph{retireErr: cause}}
	plan, err := ApplyGraph(context.Background(), catalog, store, 1, applySelection("camera_b"), runtime)
	var applyErr *ApplyError
	if !errors.As(err, &applyErr) || applyErr.Phase != ApplyPhaseRetire || !errors.Is(err, cause) {
		t.Fatalf("unexpected retire error: %#v", err)
	}
	if plan.Revision != 2 {
		t.Fatalf("verified persisted plan was hidden: %+v", plan)
	}
	persisted, readErr := store.Read(catalog)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if persisted.Revision != 2 || persisted.Selections["live"] != "live_b" {
		t.Fatalf("retire failure rolled back persisted plan: %+v", persisted)
	}
}

func TestApplyGraphKeepsRuntimeAlignedWhenDirectorySyncFailsAfterRename(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	cause := errors.New("directory sync failed")
	previousSync := routingDirectorySync
	routingDirectorySync = func(*os.File) error { return cause }
	t.Cleanup(func() { routingDirectorySync = previousSync })
	runtime := &fakeGraphRuntime{prepared: &fakePreparedGraph{}}
	plan, err := ApplyGraph(context.Background(), catalog, store, 1, applySelection("camera_b"), runtime)
	var applyErr *ApplyError
	if !errors.As(err, &applyErr) || applyErr.Phase != ApplyPhasePersist || !errors.Is(err, cause) {
		t.Fatalf("unexpected durability error: %#v", err)
	}
	if plan.Revision != 2 || plan.Selections["analysis"] != "analysis_b" {
		t.Fatalf("visible candidate plan was hidden: %+v", plan)
	}
	calls := runtime.callSequence()
	if len(calls) == 0 || calls[len(calls)-1] != ApplyPhaseRetire {
		t.Fatalf("visible plan incorrectly rolled runtime back: %v", calls)
	}
	for _, call := range calls {
		if call == "rollback" {
			t.Fatalf("runtime rolled back after candidate became visible: %v", calls)
		}
	}
	persisted, readErr := store.Read(catalog)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if persisted.Revision != 2 || persisted.Selections["archive"] != "archive_b" {
		t.Fatalf("visible plan and runtime diverged: %+v", persisted)
	}
}

func TestApplyGraphSerializesCompetingControllerThroughRetirement(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	retireEntered := make(chan struct{})
	retireRelease := make(chan struct{})
	firstPrepared := &fakePreparedGraph{}
	firstPrepared.retireHook = func(ctx context.Context) {
		close(retireEntered)
		select {
		case <-retireRelease:
		case <-ctx.Done():
		}
	}
	firstRuntime := &fakeGraphRuntime{prepared: firstPrepared}
	type applyOutcome struct {
		plan Plan
		err  error
	}
	firstDone := make(chan applyOutcome, 1)
	go func() {
		plan, err := ApplyGraph(context.Background(), catalog, store, 1, applySelection("camera_b"), firstRuntime)
		firstDone <- applyOutcome{plan: plan, err: err}
	}()
	select {
	case <-retireEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first apply did not reach retirement")
	}

	secondRuntime := &fakeGraphRuntime{prepared: &fakePreparedGraph{}}
	secondDone := make(chan applyOutcome, 1)
	go func() {
		plan, err := ApplyGraph(context.Background(), catalog, store, 1, map[string]string{
			"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
		}, secondRuntime)
		secondDone <- applyOutcome{plan: plan, err: err}
	}()
	select {
	case outcome := <-secondDone:
		t.Fatalf("competing apply escaped transaction lock: %+v", outcome)
	case <-time.After(50 * time.Millisecond):
	}
	if calls := secondRuntime.callSequence(); len(calls) != 0 {
		t.Fatalf("competing runtime prepared before retirement: %v", calls)
	}
	close(retireRelease)
	select {
	case outcome := <-firstDone:
		if outcome.err != nil || outcome.plan.Revision != 2 {
			t.Fatalf("first apply failed: %+v", outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first apply did not finish")
	}
	select {
	case outcome := <-secondDone:
		if !errors.Is(outcome.err, ErrStaleRevision) {
			t.Fatalf("competing apply did not revalidate CAS: %+v", outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("competing apply remained blocked after retirement")
	}
	if calls := secondRuntime.callSequence(); len(calls) != 0 {
		t.Fatalf("stale competing runtime was prepared: %v", calls)
	}
}

func TestApplyGraphRequiresRuntimeAndContext(t *testing.T) {
	catalog := testCatalog(1)
	store := newApplyStore(t)
	if _, err := ApplyGraph(nil, catalog, store, 1, applySelection("camera_a"), &fakeGraphRuntime{}); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := ApplyGraph(context.Background(), catalog, store, 1, applySelection("camera_a"), nil); err == nil {
		t.Fatal("nil runtime accepted")
	}
}

func TestRollbackGraphUsesFullRuntimeTransactionAndPersistsFreshEpoch(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	forwardSelections := map[string]string{
		"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
	}
	forwardRuntime := &fakeGraphRuntime{prepared: &fakePreparedGraph{}}
	forward, err := ApplyGraph(context.Background(), catalog, store, 1, forwardSelections, forwardRuntime)
	if err != nil {
		t.Fatal(err)
	}
	if forward.Revision != 2 || forward.StreamGenerations["analysis"] != 2 {
		t.Fatalf("unexpected forward plan: %+v", forward)
	}

	rollbackRuntime := &fakeGraphRuntime{prepared: &fakePreparedGraph{}}
	rolledBack, err := RollbackGraph(context.Background(), catalog, store, 2, rollbackRuntime)
	if err != nil {
		t.Fatal(err)
	}
	wantCalls := []string{
		ApplyPhasePrepare, ApplyPhaseWarmup, ApplyPhaseAdmission,
		ApplyPhaseFirstFrame, ApplyPhaseCutover, ApplyPhaseVerify, ApplyPhaseRetire,
	}
	if calls := rollbackRuntime.callSequence(); !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("rollback transaction phase order=%v want=%v", calls, wantCalls)
	}
	if rolledBack.Revision != 3 || rolledBack.Selections["analysis"] != "analysis_a" ||
		rolledBack.RollbackSelections["analysis"] != "analysis_b" ||
		rolledBack.StreamGenerations["analysis"] != 3 ||
		rolledBack.StreamGenerations["live"] != 1 || rolledBack.StreamGenerations["archive"] != 1 ||
		rollbackRuntime.currentID.Revision != 2 || rollbackRuntime.candidateID.Revision != 3 ||
		!reflect.DeepEqual(rollbackRuntime.delta.ChangedStreamIDs, []string{"analysis"}) {
		t.Fatalf("rollback did not create the expected fresh epoch: plan=%+v delta=%+v", rolledBack, rollbackRuntime.delta)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, rolledBack) {
		t.Fatalf("persisted rollback=%+v want=%+v", persisted, rolledBack)
	}
}

func TestRollbackGraphFailureLeavesForwardPlanCurrent(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	forwardSelections := map[string]string{
		"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
	}
	forward, err := ApplyGraph(
		context.Background(), catalog, store, 1, forwardSelections,
		&fakeGraphRuntime{prepared: &fakePreparedGraph{}},
	)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("rollback candidate verification failed")
	runtime := &fakeGraphRuntime{prepared: &fakePreparedGraph{verifyErr: cause}}
	if _, err := RollbackGraph(context.Background(), catalog, store, 2, runtime); err == nil || !errors.Is(err, cause) {
		t.Fatalf("failed rollback returned unexpected error: %v", err)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persisted, forward) {
		t.Fatalf("failed rollback changed current plan: got=%+v want=%+v", persisted, forward)
	}
	calls := runtime.callSequence()
	if len(calls) == 0 || calls[len(calls)-1] != "rollback" {
		t.Fatalf("failed rollback candidate was not reverted in runtime: %v", calls)
	}
}

func TestRollbackGraphRequiresRuntimeAndContext(t *testing.T) {
	catalog := testCatalog(1)
	store := newApplyStore(t)
	if _, err := RollbackGraph(nil, catalog, store, 1, &fakeGraphRuntime{}); err == nil {
		t.Fatal("nil rollback context accepted")
	}
	if _, err := RollbackGraph(context.Background(), catalog, store, 1, nil); err == nil {
		t.Fatal("nil rollback runtime accepted")
	}
}
