package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	productconfig "github.com/Josepavese/videosluice/config"
	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/internal/source"
)

func TestFrameSourceTerminalReasonRejectsUnboundedOrNonterminalFields(t *testing.T) {
	const valid = "[video-broker][frame-source] event=frame_source_failure stage=source_surface_send code=-5 peer_stage=io_deadline action=retire_source"
	for _, line := range []string{
		"unrelated " + valid,
		strings.Replace(valid, "frame_source_failure", "frame_source_ready", 1),
		strings.Replace(valid, "code=-5", "code=0", 1),
		strings.Replace(valid, "code=-5", "code=invalid", 1),
		strings.Replace(valid, "retire_source", "retry_source", 1),
		valid + " stage=other",
		valid + strings.Repeat("x", 2048),
	} {
		if got := brokerFrameSourceFailureReason(line); got != "" {
			t.Fatalf("invalid or nonterminal record became a failure: %q", got)
		}
	}
	for _, stage := range []string{"/private/path", "", strings.Repeat("x", 65)} {
		line := strings.Replace(valid, "stage=source_surface_send", "stage="+stage, 1)
		if got := brokerFrameSourceFailureReason(line); got != "frame_source_failure" {
			t.Fatalf("unsafe stage should retain only the generic event: %q", got)
		}
	}
}

func TestNativeWorkerCompletionKeepsPhaseWithoutTreatingRecoverableErrorsAsTerminal(t *testing.T) {
	const valid = "[video-broker][worker-operation] event=operation_failed adapter=rdkx5_v4l2 phase=archive_transform_encode_send last_result=0 loop_completed=false stop_signal=0 action=checked_cleanup_before_recovery"
	if got := brokerChildTerminalFailureReason(valid); got != "native_operation_failed:archive_transform_encode_send" {
		t.Fatalf("lost terminal encoder phase: %q", got)
	}
	for _, line := range []string{
		"unrelated " + valid, valid + " phase=other", valid + strings.Repeat("x", 2048),
		strings.Replace(valid, "loop_completed=false", "loop_completed=true", 1),
		strings.Replace(valid, "operation_failed", "encode_failed", 1),
		strings.Replace(valid, "last_result=0", "last_result=invalid", 1),
		strings.Replace(valid, "checked_cleanup_before_recovery", "retry_branch", 1),
		"[video-broker][encoder] event=encode_failed stage=dequeue_output sdk_code=-268435443 action=return_failure",
	} {
		if got := brokerChildTerminalFailureReason(line); got != "" {
			t.Fatalf("nonterminal/unbounded record accepted: %q", got)
		}
	}
	if got := brokerChildTerminalFailureReason(strings.Replace(valid, "phase=archive_transform_encode_send", "phase=/private/path", 1)); got != "native_operation_failed" {
		t.Fatalf("unsafe diagnostic copied as cause: %q", got)
	}
}

func TestFrameSourceTerminalFailureSurvivesChildExitWithoutUSBQuarantine(t *testing.T) {
	const failure = "[video-broker][frame-source] event=frame_source_failure stage=source_surface_send code=-5 peer_stage=io_deadline source=overview source_epoch=11 action=retire_source"
	cmd := exec.Command("sh", "-c", `printf '%s\n' "$1" >&2; exit 64`, "frame-source-test", failure)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	done := make(chan struct{})
	rt := &videoBrokerRuntime{
		plan:     videoBrokerPlan{Transport: "v4l2_uvc"},
		childCmd: cmd, childStderr: stderr, childStderrDone: done,
		childRunning: true, childRecovery: true, childStartedAt: time.Now(), mediaEpoch: 11,
	}
	rt.resetChildFailure(11)
	go func() {
		defer close(done)
		rt.scanStderr(context.Background(), stderr, 11, cmd.Process.Pid)
	}()
	rt.finishChild(context.Background())
	logged := output.String()
	for _, marker := range []string{"event=capture_fault", "event=capture_epoch_ended outcome=failure", "exit_code=64", "reason=frame_source_failure:source_surface_send:io_deadline"} {
		if !strings.Contains(logged, marker) {
			t.Fatalf("capture summary lost %q: %s", marker, logged)
		}
	}
	if strings.Contains(logged, "child_exit_without_terminal_reason") || len(rt.captureModeFailures) != 0 || len(rt.captureModeExclusions) != 0 {
		t.Fatalf("frame IPC failure became generic or poisoned USB mode health: %s", logged)
	}
}

func TestFrameReceiveExitPreservesFailureDuringCancellation(t *testing.T) {
	failure := fmt.Errorf("source_receive_progress: %w", context.DeadlineExceeded)
	for _, test := range []struct {
		name           string
		err, childErr  error
		report, retire bool
	}{
		{"unexpected_end", nil, nil, true, true},
		{"receive_failure", failure, nil, true, true},
		{"failure_before_worker_exit", failure, context.Canceled, true, false},
		{"normal_stop", context.Canceled, context.Canceled, false, false},
		{"wrapped_normal_stop", fmt.Errorf("receive: %w", context.Canceled), context.Canceled, false, false},
		{"already_stopped", nil, context.Canceled, false, false},
		{"canceled_without_parent_stop", context.Canceled, nil, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err, retire := frameReceiveExit(test.err, test.childErr)
			if (err != nil) != test.report || retire != test.retire {
				t.Fatalf("report=%v retire=%t; want report=%t retire=%t", err, retire, test.report, test.retire)
			}
			if test.err != nil && test.report && err != test.err {
				t.Fatal("receive failure was replaced")
			}
		})
	}
}

func TestCausalDeliveryCadenceUsesCommittedFramesWithoutChangingIdentity(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan:       videoBrokerPlan{SourceID: "overview", AnalysisStreamID: "analysis", AnalysisEnabled: true, SubFPS: 20},
		mediaEpoch: 1,
	}
	header := wire.Header{Kind: wire.Delivery, Codec: wire.NV12, Source: "overview", Stream: "analysis", SourceEpoch: 1, PayloadBytes: 24}
	observer := rt.newCausalDeliveryObserverLocked(context.Background())
	for i := 0; i < 40; i++ {
		if err := observer.observe(context.Background(), header, make([]byte, 24)); err != nil {
			t.Fatal(err)
		}
	}
	rt.applyCausalDeliveryLocked(observer)
	if rt.nextSubFrame != 0 {
		t.Fatal("causal delivery must not allocate a replacement frame identity")
	}
	var analysis brokerStreamCadenceTarget
	for _, target := range rt.streamCadenceTargetsLocked() {
		if target.role == "analysis" {
			analysis = target
		}
	}
	if analysis.frame != 40 || !analysis.required || analysis.expectedFPS != 20 {
		t.Fatalf("cadence did not count committed causal deliveries: %+v", analysis)
	}
	now := time.Now()
	watch := brokerStreamCadenceWatch{role: "analysis"}
	watch.reset(now.Add(-2*time.Second), 0)
	watch.observe(now, analysis.frame, 20, time.Second, time.Second, 1, 0)
	if watch.measuredFPS != 20 {
		t.Fatalf("causal frame cadence falsely reported %.2f fps", watch.measuredFPS)
	}
	// The existing cadence arithmetic is modulo uint32, independent of the
	// monotonic uint64 publication counter and the wire delivery identity.
	rt.totalSubFrames = (1 << 32) + 40
	if got := rt.streamCadenceTargetsLocked()[0].frame; got != 40 {
		t.Fatalf("publication counter wrap changed cadence semantics: %d", got)
	}
}

func TestCausalDeliveryReceptionDoesNotWaitForRuntimeBookkeeping(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan:       videoBrokerPlan{SourceID: "front", AnalysisStreamID: "analysis", AnalysisEnabled: true},
		mediaEpoch: 1,
	}
	observer := rt.newCausalDeliveryObserverLocked(context.Background())
	rt.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- observer.observe(ctx, wire.Header{Kind: wire.Delivery, Codec: wire.NV12,
			Source: "front", Stream: "analysis", SourceEpoch: 1, PayloadBytes: 24}, make([]byte, 24))
	}()
	select {
	case err := <-done:
		rt.mu.Unlock()
		if err != nil {
			t.Fatalf("valid committed frame rejected: %v", err)
		}
	case <-ctx.Done():
		rt.mu.Unlock()
		<-done
		t.Fatal("runtime bookkeeping blocked the committed frame receiver")
	}
}

type causalFirstCheckContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (c *causalFirstCheckContext) Err() error {
	err := c.Context.Err()
	c.once.Do(func() { close(c.checked) })
	return err
}

func TestCausalObserverCanceledWhileWaitingDoesNotRefreshHealth(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan:       videoBrokerPlan{SourceID: "overview", AnalysisStreamID: "analysis", AnalysisEnabled: true, SubFPS: 20},
		mediaEpoch: 1,
	}
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &causalFirstCheckContext{Context: base, checked: make(chan struct{})}
	observer := rt.newCausalDeliveryObserverLocked(context.Background())
	observer.mu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- observer.observe(ctx, wire.Header{Kind: wire.Delivery, Codec: wire.NV12,
			Source: "overview", Stream: "analysis", SourceEpoch: 1, PayloadBytes: 24}, make([]byte, 24))
	}()
	<-ctx.checked // First check returned nil; no metadata has been accepted yet.
	cancel()
	observer.mu.Unlock()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "phase=accumulator") {
			t.Fatalf("lock wait lost its cause: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not return after lock release")
	}
	rt.applyCausalDeliveryLocked(observer)
	if rt.totalSubFrames != 0 || !rt.lastFrameAt.IsZero() || rt.causalSubFrame != 0 || rt.everProducedFrames {
		t.Fatal("expired observation became fresh runtime health evidence")
	}
}

// context timers can be scheduled late when the host is stalled. Err()==nil
// alone is not evidence that the frame's absolute deadline is still valid.
type delayedCausalDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c delayedCausalDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestCausalObserverRejectsElapsedDeadlineBeforeContextTimerRuns(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan:       videoBrokerPlan{SourceID: "front", AnalysisStreamID: "analysis", AnalysisEnabled: true},
		mediaEpoch: 1,
	}
	ctx := delayedCausalDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Second)}
	observer := rt.newCausalDeliveryObserverLocked(context.Background())
	err := observer.observe(ctx, wire.Header{Kind: wire.Delivery, Codec: wire.NV12,
		Source: "front", Stream: "analysis", SourceEpoch: 1, PayloadBytes: 24}, make([]byte, 24))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired absolute deadline accepted while its timer was delayed: %v", err)
	}
	for _, field := range []string{"phase=before_accumulator"} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("observer diagnostic lost %q: %v", field, err)
		}
	}
	if rt.totalSubFrames != 0 || rt.everProducedFrames || !rt.lastFrameAt.IsZero() {
		t.Fatal("expired absolute deadline became fresh health evidence")
	}
}

func TestRoutingFirstFrameUsesCurrentCausalPublicationNotRemovedSHM(t *testing.T) {
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{SourceID: "front", AnalysisStreamID: "analysis", AnalysisEnabled: true,
			SubShmPath: "/not-opened-by-frame-access", FrameCapture: func(context.Context, source.BrokerMuxPlan) (source.FrameCapture, error) {
				return nil, errors.New("not used by this fixture")
			}},
		mediaEpoch: 7, childRunning: true,
		childCmd: &exec.Cmd{Process: &os.Process{Pid: 42}}, childFrames: &captureStub{epoch: 7},
	}
	prepared := &sourceMovePreparedGraph{candidatePID: 42, candidateEpoch: 7,
		move: wholeSourceMove{candidateGraph: productconfig.SourceGraph{Analysis: &productconfig.NamedStream{}}}}
	assertReady := func(want bool) {
		t.Helper()
		err, ready := prepared.candidateReadyLocked(rt, true)
		if err != nil || ready != want {
			t.Fatalf("causal readiness=%v, want=%v, err=%v", ready, want, err)
		}
	}
	assertReady(false)
	// A counter increment from another output path is not causal evidence.
	rt.totalSubFrames = 1
	assertReady(false)
	header := wire.Header{Kind: wire.Delivery, Codec: wire.NV12, Source: "front", Stream: "analysis", SourceEpoch: 7, PayloadBytes: 24}
	observer := rt.newCausalDeliveryObserverLocked(context.Background())
	if err := observer.observe(context.Background(), header, make([]byte, 24)); err != nil {
		t.Fatal(err)
	}
	rt.applyCausalDeliveryLocked(observer)
	assertReady(true)
	if rt.subShared != nil || rt.nextSubFrame != 0 {
		t.Fatal("causal readiness created a legacy buffer or replacement frame identity")
	}
	prepared.warmupSubFrame = rt.totalSubFrames
	assertReady(false)
	// Repeated verification needs another publication, not the warmup frame.
	if err := observer.observe(context.Background(), header, make([]byte, 24)); err != nil {
		t.Fatal(err)
	}
	rt.applyCausalDeliveryLocked(observer)
	assertReady(true)
	// Reopened captures, missing providers and invalid callbacks cannot borrow
	// an earlier generation's successful publication, even with rising counters.
	rt.mediaEpoch, prepared.candidateEpoch = 8, 8
	observer = rt.newCausalDeliveryObserverLocked(context.Background())
	rt.childFrames = &captureStub{epoch: 8}
	rt.totalSubFrames++
	assertReady(false)
	if err := observer.observe(context.Background(), header, make([]byte, 24)); err == nil {
		t.Fatal("stale source epoch accepted")
	}
	assertReady(false)
	header.SourceEpoch = 8
	if err := observer.observe(context.Background(), header, make([]byte, 24)); err != nil {
		t.Fatal(err)
	}
	rt.applyCausalDeliveryLocked(observer)
	assertReady(true)
	rt.childFrames = nil
	assertReady(false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := observer.observe(ctx, header, make([]byte, 24)); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled delivery accepted", err)
	}
	prepared.candidateRuntime = rt
	prepared.candidateGraph = &graphService{done: make(chan error)}
	err := prepared.waitCandidate(ctx, true)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "frame_access=true") ||
		!strings.Contains(err.Error(), "causal_epoch=8") {
		t.Fatal("pending candidate lost its cancellation or causal diagnostic", err)
	}
	rt.childFrames = &captureStub{epoch: 8}
	prepared.warmupSubFrame = rt.totalSubFrames
	if err := setFanoutRoleGateLocked(rt, "analysis", "cutover"); err != nil {
		t.Fatal(err)
	}
	if err := observer.observe(context.Background(), header, make([]byte, 24)); err != nil {
		t.Fatal(err)
	}
	rt.applyCausalDeliveryLocked(observer)
	assertReady(false)
	if err := clearFanoutRoleGateLocked(rt, "analysis", "cutover"); err != nil {
		t.Fatal(err)
	}
	if err := observer.observe(context.Background(), header, make([]byte, 24)); err != nil {
		t.Fatal(err)
	}
	rt.applyCausalDeliveryLocked(observer)
	assertReady(true)
}
