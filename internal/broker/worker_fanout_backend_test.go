package broker

import (
	"bufio"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/Josepavese/videosluice/capability"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/routing"
)

type fanoutBackendTrace struct {
	mu      sync.Mutex
	entries []string
}

func (trace *fanoutBackendTrace) add(entry string) {
	trace.mu.Lock()
	trace.entries = append(trace.entries, entry)
	trace.mu.Unlock()
}

func (trace *fanoutBackendTrace) snapshot() []string {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return append([]string(nil), trace.entries...)
}

type fanoutPublicationFixture struct {
	trace     *fanoutBackendTrace
	failPhase string
}

func (fixture *fanoutPublicationFixture) ValidateFanoutPublications(
	ctx context.Context,
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
	set workerFanoutCapabilitySet,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if len(handles) != len(transition.SourceIDs) || len(set.Changes) != len(transition.SourceIDs) {
		return errors.New("publication fixture received an incomplete transition")
	}
	fixture.trace.add("publication:validate")
	return fixture.fail("validate")
}

func (fixture *fanoutPublicationFixture) PrepareFanoutPublications(
	ctx context.Context,
	transactionID string,
	transition preservedOwnerFanout,
	handles map[string]sourceFanoutHandle,
	set workerFanoutCapabilitySet,
) (fanoutPreparedPublications, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if transactionID == "" || len(handles) != len(transition.SourceIDs) ||
		len(set.Changes) != len(transition.SourceIDs) {
		return nil, errors.New("publication fixture received an incomplete prepare")
	}
	fixture.trace.add("publication:prepare")
	if err := fixture.fail("prepare"); err != nil {
		return &fanoutPreparedPublicationFixture{owner: fixture}, err
	}
	return &fanoutPreparedPublicationFixture{owner: fixture}, nil
}

func (fixture *fanoutPublicationFixture) fail(phase string) error {
	if fixture.failPhase == phase {
		return errors.New("publication_" + phase + "_failed")
	}
	return nil
}

type fanoutPreparedPublicationFixture struct {
	owner *fanoutPublicationFixture
}

func (prepared *fanoutPreparedPublicationFixture) phase(name string) error {
	prepared.owner.trace.add("publication:" + name)
	return prepared.owner.fail(name)
}

func (prepared *fanoutPreparedPublicationFixture) Warmup(context.Context) error {
	return prepared.phase("warmup")
}
func (prepared *fanoutPreparedPublicationFixture) Admit(context.Context) error {
	return prepared.phase("admit")
}
func (prepared *fanoutPreparedPublicationFixture) AwaitFirstFrame(context.Context) error {
	return prepared.phase("first_frame")
}
func (prepared *fanoutPreparedPublicationFixture) AttestFirstFrame(
	map[string]videoproviders.WorkerFanoutReceipt,
) error {
	return prepared.phase("attest_first_frame")
}
func (prepared *fanoutPreparedPublicationFixture) StageCutover(context.Context) error {
	return prepared.phase("stage_cutover")
}
func (prepared *fanoutPreparedPublicationFixture) StageRollback(context.Context) error {
	return prepared.phase("stage_rollback")
}
func (prepared *fanoutPreparedPublicationFixture) Cutover(context.Context) error {
	return prepared.phase("cutover")
}
func (prepared *fanoutPreparedPublicationFixture) Verify(context.Context) error {
	return prepared.phase("verify")
}
func (prepared *fanoutPreparedPublicationFixture) Rollback(context.Context) error {
	return prepared.phase("rollback")
}
func (prepared *fanoutPreparedPublicationFixture) Abort(context.Context) error {
	return prepared.phase("abort")
}
func (prepared *fanoutPreparedPublicationFixture) Retire(context.Context) error {
	return prepared.phase("retire")
}

type fanoutWorkerFixture struct {
	sourceID    string
	runtime     *videoBrokerRuntime
	trace       *fanoutBackendTrace
	rejectPhase string
	stopPipe    func()
	done        chan error

	mu       sync.Mutex
	commands []videoproviders.WorkerFanoutCommand
}

func startFanoutWorkerFixture(
	t *testing.T,
	sourceID string,
	childPID int,
	mediaEpoch uint64,
	trace *fanoutBackendTrace,
) *fanoutWorkerFixture {
	t.Helper()
	runtime, reader, stopPipe := workerFanoutControlFixtureFor(
		t, sourceID, childPID, mediaEpoch)
	fixture := &fanoutWorkerFixture{
		sourceID: sourceID, runtime: runtime, trace: trace,
		stopPipe: stopPipe, done: make(chan error, 1),
	}
	go fixture.respond(reader)
	t.Cleanup(func() {
		fixture.stopPipe()
		if err := <-fixture.done; err != nil {
			t.Errorf("worker responder %s: %v", sourceID, err)
		}
	})
	return fixture
}

func (fixture *fanoutWorkerFixture) respond(reader *bufio.Reader) {
	preparedChanges := []videoproviders.WorkerBranchChange(nil)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				fixture.done <- nil
				return
			}
			fixture.done <- err
			return
		}
		command, err := videoproviders.DecodeWorkerFanoutCommand(line)
		if err != nil {
			fixture.done <- err
			return
		}
		fixture.mu.Lock()
		fixture.commands = append(fixture.commands, command)
		fixture.mu.Unlock()
		fixture.trace.add("worker:" + fixture.sourceID + ":" + command.Phase)
		if command.Phase == videoproviders.WorkerFanoutPhasePrepare {
			preparedChanges = videoproviders.CanonicalizeWorkerFanoutChanges(command.Changes)
		}
		event := workerFanoutEventForPhase(command.Phase)
		receipt := workerFanoutReceiptFor(command, event)
		if fixture.rejectPhase == command.Phase {
			receipt.Event = videoproviders.WorkerFanoutEventRejected
			receipt.ErrorCode = "fixture_rejected"
		} else if command.Phase == videoproviders.WorkerFanoutPhaseFirstFrame {
			for _, change := range preparedChanges {
				if change.Action != videoproviders.WorkerFanoutActionAttach &&
					change.Action != videoproviders.WorkerFanoutActionReplace {
					continue
				}
				bootstrap := videoproviders.WorkerFanoutBootstrapComplete
				if change.Role == "analysis" {
					bootstrap = videoproviders.WorkerFanoutBootstrapNotApplicable
				}
				runtimeMode := ""
				switch change.Role {
				case "archive":
					runtimeMode = videoproviders.WorkerFanoutRuntimeProcessedHWEncode
				case "live":
					runtimeMode = videoproviders.WorkerFanoutRuntimePersistentEncoder
				}
				receipt.Branches = append(receipt.Branches, videoproviders.WorkerFanoutBranchReceipt{
					Role: change.Role, BranchEpoch: 1, FrameID: 1,
					CodecBootstrap: bootstrap, RuntimeMode: runtimeMode,
				})
			}
		}
		encoded, err := videoproviders.EncodeWorkerFanoutReceipt(receipt)
		if err != nil {
			fixture.done <- err
			return
		}
		if !fixture.runtime.acceptWorkerFanoutReceipt(
			string(encoded), command.MediaEpoch, command.CapturePID,
		) {
			fixture.done <- errors.New("worker receipt was not accepted")
			return
		}
	}
}

func (fixture *fanoutWorkerFixture) phases() []string {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	phases := make([]string, 0, len(fixture.commands))
	for _, command := range fixture.commands {
		phases = append(phases, command.Phase)
	}
	return phases
}

func workerFanoutEventForPhase(phase string) string {
	switch phase {
	case videoproviders.WorkerFanoutPhasePrepare:
		return videoproviders.WorkerFanoutEventPrepared
	case videoproviders.WorkerFanoutPhaseWarmup:
		return videoproviders.WorkerFanoutEventWarmed
	case videoproviders.WorkerFanoutPhaseAdmit:
		return videoproviders.WorkerFanoutEventAdmitted
	case videoproviders.WorkerFanoutPhaseFirstFrame:
		return videoproviders.WorkerFanoutEventFirstFrame
	case videoproviders.WorkerFanoutPhaseCutover:
		return videoproviders.WorkerFanoutEventCutover
	case videoproviders.WorkerFanoutPhaseVerify:
		return videoproviders.WorkerFanoutEventVerified
	case videoproviders.WorkerFanoutPhaseRollback:
		return videoproviders.WorkerFanoutEventRolledBack
	case videoproviders.WorkerFanoutPhaseAbort:
		return videoproviders.WorkerFanoutEventAborted
	case videoproviders.WorkerFanoutPhaseRetire:
		return videoproviders.WorkerFanoutEventRetired
	default:
		return "unknown"
	}
}

type workerFanoutBackendFixture struct {
	transition  preservedOwnerFanout
	handles     map[string]sourceFanoutHandle
	inventory   capability.Inventory
	allocator   *capabilityAllocator
	workers     map[string]*fanoutWorkerFixture
	publication *fanoutPublicationFixture
	backend     *workerFanoutBackend
}

func newWorkerFanoutBackendFixture(t *testing.T, sourceCount, vpuCapacity int) workerFanoutBackendFixture {
	t.Helper()
	transition := fanoutTransitionForTest(t, sourceCount)
	return newWorkerFanoutBackendFixtureForTransition(t, transition, vpuCapacity)
}

func newWorkerFanoutBackendFixtureForTransition(
	t *testing.T,
	transition preservedOwnerFanout,
	vpuCapacity int,
) workerFanoutBackendFixture {
	t.Helper()
	inventory := workerFanoutCapabilityInventory(vpuCapacity, 2)
	allocator := newCapabilityAllocator()
	trace := &fanoutBackendTrace{}
	handles := make(map[string]sourceFanoutHandle, len(transition.SourceIDs))
	workers := make(map[string]*fanoutWorkerFixture, len(transition.SourceIDs))
	currentPlans := make(map[string]capability.Plan, len(transition.SourceIDs))
	for sourceIndex, sourceID := range transition.SourceIDs {
		currentGraph := transition.CurrentGraphs[sourceID]
		worker := startFanoutWorkerFixture(
			t, sourceID, 71000+sourceIndex, uint64(72000+sourceIndex), trace)
		template := workerFanoutCapabilityRuntime(currentGraph)
		plan, _, err := resolveWorkerFanoutGraphCapabilityLocked(template, currentGraph, inventory)
		if err != nil {
			t.Fatal(err)
		}
		worker.runtime.mu.Lock()
		worker.runtime.plan = template.plan
		worker.runtime.capabilityPlan = plan
		worker.runtime.observedCaptureCodec = template.observedCaptureCodec
		worker.runtime.activeCaptureWidth = template.activeCaptureWidth
		worker.runtime.activeCaptureHeight = template.activeCaptureHeight
		worker.runtime.activeCaptureFPS = template.activeCaptureFPS
		worker.runtime.mu.Unlock()
		workers[sourceID] = worker
		currentPlans[sourceID] = plan
		handles[sourceID] = sourceFanoutHandle{
			SourceID: sourceID, Runtime: worker.runtime,
			CurrentGraph: currentGraph, CandidateGraph: transition.CandidateGraphs[sourceID],
			ChildPID: 71000 + sourceIndex, MediaEpoch: uint64(72000 + sourceIndex),
			CaptureAvailable: true,
		}
	}
	seedCapabilityClaims(t, allocator, inventory, currentPlans)
	publication := &fanoutPublicationFixture{trace: trace}
	backend, err := newWorkerFanoutBackend(inventory, allocator, publication)
	if err != nil {
		t.Fatal(err)
	}
	backend.transactionID = func() (string, error) { return "fanout-test", nil }
	return workerFanoutBackendFixture{
		transition: transition, handles: handles, inventory: inventory,
		allocator: allocator, workers: workers, publication: publication, backend: backend,
	}
}

func fanoutTransitionForTest(t *testing.T, sourceCount int) preservedOwnerFanout {
	t.Helper()
	catalog := fanoutRoutingCatalog(t, sourceCount)
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	selections := cloneSelections(initial.Selections)
	selections["live"] = "camera-2"
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	preview, err := routing.PreviewGraph(catalog, store, initial.Revision, selections)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := routing.Materialize(catalog, preview.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := classifyPreservedOwnerFanout(current, candidate, delta)
	if err != nil {
		t.Fatal(err)
	}
	return transition
}

func fanoutRetiringTransitionForTest(t *testing.T) preservedOwnerFanout {
	t.Helper()
	catalog := fanoutRoutingCatalog(t, 2)
	archive := catalog.Streams["archive"]
	archive.Options["camera-2"] = routing.StreamOption{
		SourceRef: "camera-2", Codec: "h265",
		Width: 640, Height: 360, FPS: 15, BitrateKbps: 750,
	}
	catalog.Streams["archive"] = archive
	var err error
	catalog, err = routing.NormalizeCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	selections := cloneSelections(initial.Selections)
	selections["live"] = "camera-2"
	selections["archive"] = "camera-2"
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	preview, err := routing.PreviewGraph(catalog, store, initial.Revision, selections)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := routing.Materialize(catalog, preview.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := classifyPreservedOwnerFanout(current, candidate, delta)
	if err != nil {
		t.Fatal(err)
	}
	return transition
}

func TestWorkerFanoutBackendCoordinatesReservationWorkersAndPublications(t *testing.T) {
	fixture := newWorkerFanoutBackendFixture(t, 5, 3)
	if err := fixture.backend.ValidateFanout(
		context.Background(), fixture.transition, fixture.handles,
	); err != nil {
		t.Fatal(err)
	}
	if len(fixture.allocator.reservations) != 0 {
		t.Fatalf("preview retained a reservation: %+v", fixture.allocator.reservations)
	}
	prepared, err := fixture.backend.PrepareFanout(
		context.Background(), fixture.transition, fixture.handles)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fixture.allocator.reservations["fanout-test"], map[string]int{"video": 1}) {
		t.Fatalf("prepare did not retain exact peak capacity: %+v", fixture.allocator.reservations)
	}
	for _, phase := range []func(context.Context) error{
		prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame,
		prepared.Cutover, prepared.Verify, prepared.Retire,
	} {
		if err := phase(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(fixture.allocator.reservations) != 0 {
		t.Fatalf("retire retained a committed reservation: %+v", fixture.allocator.reservations)
	}
	if fixture.allocator.claims["video"]["camera-1"] != 1 ||
		fixture.allocator.claims["video"]["camera-2"] != 1 {
		t.Fatalf("retire did not commit candidate hardware claims: %+v", fixture.allocator.claims)
	}
	wantWorkerPhases := []string{
		"prepare", "warmup", "admit", "first_frame", "cutover", "verify", "retire",
	}
	for sourceID, worker := range fixture.workers {
		if got := worker.phases(); !reflect.DeepEqual(got, wantWorkerPhases) {
			t.Fatalf("source %s phases=%v want=%v", sourceID, got, wantWorkerPhases)
		}
	}
	trace := fixture.publication.trace.snapshot()
	publicationPrepare := traceIndex(trace, "publication:prepare")
	for _, sourceID := range fixture.transition.SourceIDs {
		workerPrepare := traceIndex(trace, "worker:"+sourceID+":prepare")
		if publicationPrepare < 0 || workerPrepare < publicationPrepare {
			t.Fatalf("private publication was not prepared before worker %s: %v", sourceID, trace)
		}
	}
	assertWorkerBeforePublication(t, trace, wantWorkerPhases[1:])
}

func TestWorkerFanoutBackendDetachesEveryRoleBeforeSourceRetirement(t *testing.T) {
	transition := fanoutRetiringTransitionForTest(t)
	fixture := newWorkerFanoutBackendFixtureForTransition(t, transition, 4)
	if err := fixture.backend.ValidateFanout(
		context.Background(), transition, fixture.handles,
	); err != nil {
		t.Fatal(err)
	}
	prepared, err := fixture.backend.PrepareFanout(
		context.Background(), transition, fixture.handles)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []func(context.Context) error{
		prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame,
		prepared.Cutover, prepared.Verify, prepared.Retire,
	} {
		if err := phase(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	retiringChanges := fixture.handles["camera-1"].CandidateGraph
	if retiringChanges.Live != nil || retiringChanges.Analysis != nil || retiringChanges.Archive != nil {
		t.Fatalf("retiring worker kept a candidate role: %+v", retiringChanges)
	}
	if len(fixture.allocator.reservations) != 0 {
		t.Fatalf("retiring transaction leaked capacity: %+v", fixture.allocator.reservations)
	}
}

func TestWorkerFanoutBackendPreviewChecksPeakCapacityWithoutMutation(t *testing.T) {
	fixture := newWorkerFanoutBackendFixture(t, 2, 2)
	err := fixture.backend.ValidateFanout(
		context.Background(), fixture.transition, fixture.handles)
	if err == nil || !strings.Contains(err.Error(), "lacks prepare capacity") {
		t.Fatalf("capacity-exhausted preview was accepted: %v", err)
	}
	if len(fixture.allocator.reservations) != 0 || len(fixture.allocator.reservedBy) != 0 {
		t.Fatalf("preview mutated allocator state: reservations=%+v owners=%+v",
			fixture.allocator.reservations, fixture.allocator.reservedBy)
	}
	for sourceID, worker := range fixture.workers {
		if phases := worker.phases(); len(phases) != 0 {
			t.Fatalf("preview wrote to worker %s: %v", sourceID, phases)
		}
	}
}

func TestWorkerFanoutBackendCleansPartialPrepareWithoutLeakingCapacity(t *testing.T) {
	fixture := newWorkerFanoutBackendFixture(t, 2, 3)
	fixture.workers["camera-2"].rejectPhase = videoproviders.WorkerFanoutPhasePrepare
	_, err := fixture.backend.PrepareFanout(
		context.Background(), fixture.transition, fixture.handles)
	if err == nil || !strings.Contains(err.Error(), "fixture_rejected") {
		t.Fatalf("partial worker rejection was not reported: %v", err)
	}
	if len(fixture.allocator.reservations) != 0 {
		t.Fatalf("successful prepare cleanup leaked capacity: %+v", fixture.allocator.reservations)
	}
	if got := fixture.workers["camera-1"].phases(); !reflect.DeepEqual(got, []string{"prepare", "abort"}) {
		t.Fatalf("prepared worker was not aborted exactly: %v", got)
	}
	if got := fixture.workers["camera-2"].phases(); !reflect.DeepEqual(got, []string{"prepare"}) {
		t.Fatalf("rejected worker received an invalid cleanup phase: %v", got)
	}
	if entries := fixture.publication.trace.snapshot(); !containsTrace(entries, "publication:abort") {
		t.Fatalf("prepared publications were not aborted: %v", entries)
	}
}

func TestWorkerFanoutBackendCleansPartiallyPreparedPublicationsBeforeWorkerWrites(t *testing.T) {
	fixture := newWorkerFanoutBackendFixture(t, 2, 3)
	fixture.publication.failPhase = "prepare"
	_, err := fixture.backend.PrepareFanout(
		context.Background(), fixture.transition, fixture.handles)
	if err == nil || !strings.Contains(err.Error(), "publication_prepare_failed") {
		t.Fatalf("partial publication prepare failure was lost: %v", err)
	}
	if len(fixture.allocator.reservations) != 0 {
		t.Fatalf("partial publication cleanup leaked capacity: %+v", fixture.allocator.reservations)
	}
	if entries := fixture.publication.trace.snapshot(); !containsTrace(entries, "publication:abort") {
		t.Fatalf("partially prepared publication was not aborted: %v", entries)
	}
	for sourceID, worker := range fixture.workers {
		if phases := worker.phases(); len(phases) != 0 {
			t.Fatalf("publication prepare failure wrote to worker %s: %v", sourceID, phases)
		}
	}
}

func TestWorkerFanoutBackendRollsBackPublicationCutoverAndRetainsUnsafeRetireReservation(t *testing.T) {
	t.Run("cutover rollback", func(t *testing.T) {
		fixture := newWorkerFanoutBackendFixture(t, 2, 3)
		fixture.publication.failPhase = "cutover"
		prepared, err := fixture.backend.PrepareFanout(
			context.Background(), fixture.transition, fixture.handles)
		if err != nil {
			t.Fatal(err)
		}
		for _, phase := range []func(context.Context) error{
			prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame,
		} {
			if err := phase(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if err := prepared.Cutover(context.Background()); err == nil ||
			!strings.Contains(err.Error(), "publication_cutover_failed") {
			t.Fatalf("publication cutover failure was lost: %v", err)
		}
		if err := prepared.Rollback(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(fixture.allocator.reservations) != 0 {
			t.Fatalf("attested rollback retained peak capacity: %+v", fixture.allocator.reservations)
		}
		for sourceID, worker := range fixture.workers {
			phases := worker.phases()
			if !reflect.DeepEqual(phases[len(phases)-2:], []string{"cutover", "rollback"}) {
				t.Fatalf("source %s did not roll back cutover: %v", sourceID, phases)
			}
		}
	})

	t.Run("retire failure stays reserved", func(t *testing.T) {
		fixture := newWorkerFanoutBackendFixture(t, 2, 3)
		fixture.publication.failPhase = "retire"
		prepared, err := fixture.backend.PrepareFanout(
			context.Background(), fixture.transition, fixture.handles)
		if err != nil {
			t.Fatal(err)
		}
		for _, phase := range []func(context.Context) error{
			prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame,
			prepared.Cutover, prepared.Verify,
		} {
			if err := phase(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if err := prepared.Retire(context.Background()); err == nil ||
			!strings.Contains(err.Error(), "publication_retire_failed") {
			t.Fatalf("retire failure was lost: %v", err)
		}
		if !reflect.DeepEqual(fixture.allocator.reservations["fanout-test"], map[string]int{"video": 1}) {
			t.Fatalf("unsafe retire undercounted possible live hardware: %+v", fixture.allocator.reservations)
		}
	})
}

func TestWorkerFanoutBackendRollbackRestoresHealthyPeersWhenOneFrozenOwnerExited(t *testing.T) {
	fixture := newWorkerFanoutBackendFixture(t, 2, 3)
	prepared, err := fixture.backend.PrepareFanout(
		context.Background(), fixture.transition, fixture.handles)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []func(context.Context) error{
		prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame, prepared.Cutover,
	} {
		if err := phase(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	dead := fixture.workers["camera-2"].runtime
	dead.mu.Lock()
	dead.childRunning = false
	dead.childFinishing = true
	dead.failWorkerFanoutWaitersLocked(errors.New("fixture worker exited"))
	dead.mu.Unlock()
	fixture.allocator.release("camera-2")
	if err := prepared.Verify(context.Background()); err == nil {
		t.Fatal("verify accepted an exited frozen owner")
	}
	if err := prepared.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback did not settle an exited frozen owner: %v", err)
	}
	if len(fixture.allocator.reservations) != 0 || len(fixture.allocator.reservedBy) != 0 {
		t.Fatalf("exited owner stranded capacity: reservations=%+v owners=%+v",
			fixture.allocator.reservations, fixture.allocator.reservedBy)
	}
	if phases := fixture.workers["camera-1"].phases(); len(phases) == 0 || phases[len(phases)-1] != videoproviders.WorkerFanoutPhaseRollback {
		t.Fatalf("healthy peer did not receive rollback after peer exit: %v", phases)
	}
	if entries := fixture.publication.trace.snapshot(); !containsTrace(entries, "publication:rollback") {
		t.Fatalf("incumbent publications were not restored after peer exit: %v", entries)
	}
}

func TestWorkerFanoutBackendRollbackRetainsCapacityForLiveUnreconciledOwner(t *testing.T) {
	fixture := newWorkerFanoutBackendFixture(t, 2, 3)
	prepared, err := fixture.backend.PrepareFanout(
		context.Background(), fixture.transition, fixture.handles)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []func(context.Context) error{
		prepared.Warmup, prepared.Admit, prepared.AwaitFirstFrame, prepared.Cutover,
	} {
		if err := phase(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	fixture.workers["camera-2"].rejectPhase = videoproviders.WorkerFanoutPhaseRollback
	err = prepared.Rollback(context.Background())
	if err == nil || !strings.Contains(err.Error(), "live frozen owner") {
		t.Fatalf("live unreconciled owner was not rejected: %v", err)
	}
	if !reflect.DeepEqual(fixture.allocator.reservations["fanout-test"], map[string]int{"video": 1}) {
		t.Fatalf("unsafe live owner released peak capacity: %+v", fixture.allocator.reservations)
	}
	if entries := fixture.publication.trace.snapshot(); !containsTrace(entries, "publication:rollback") {
		t.Fatalf("logical publications were not restored fail-closed: %v", entries)
	}
}

func assertWorkerBeforePublication(t *testing.T, trace []string, phases []string) {
	t.Helper()
	for _, phase := range phases {
		publicationIndex := traceIndex(trace, "publication:"+phase)
		if publicationIndex < 0 {
			t.Fatalf("trace lacks publication phase %s: %v", phase, trace)
		}
		workerCount := 0
		for index, entry := range trace {
			if strings.HasPrefix(entry, "worker:") && strings.HasSuffix(entry, ":"+phase) {
				workerCount++
				if index > publicationIndex {
					t.Fatalf("publication %s preceded worker evidence: %v", phase, trace)
				}
			}
		}
		if workerCount == 0 {
			t.Fatalf("trace lacks worker phase %s: %v", phase, trace)
		}
	}
}

func traceIndex(trace []string, value string) int {
	for index, entry := range trace {
		if entry == value {
			return index
		}
	}
	return -1
}

func containsTrace(trace []string, value string) bool {
	return traceIndex(trace, value) >= 0
}

var _ fanoutPublicationCoordinator = (*fanoutPublicationFixture)(nil)
var _ fanoutPreparedPublications = (*fanoutPreparedPublicationFixture)(nil)
