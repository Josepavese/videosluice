package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Josepavese/videosluice/capability"
	wire "github.com/Josepavese/videosluice/framewire"
)

func brokerCaptureIntegrityFailureReason(line string) string {
	const prefix = "[rdkx5_v4l2_mux_bridge][capture-integrity] "
	if len(line) > 2048 || !strings.HasPrefix(line, prefix) {
		return ""
	}
	fields := make(map[string]string)
	for _, field := range strings.Fields(strings.TrimPrefix(line, prefix)) {
		key, value, ok := strings.Cut(field, "=")
		if _, duplicate := fields[key]; !ok || duplicate {
			return ""
		}
		fields[key] = value
	}
	format := fields["format"]
	if format != "h265" && format != "h264" {
		return ""
	}
	// A tolerated discontinuity rebuilds the reference gate instead of retiring
	// the source; only a resync that never reaches a fresh IDR inside its own
	// budget is terminal, and it keeps its own attributable reason.
	if fields["event"] == "interframe_resync_timeout" {
		if fields["action"] != "retire_source_before_publication" {
			return ""
		}
		return "interframe_resync_timeout:" + format
	}
	if fields["event"] != "interframe_capture_discontinuity" || fields["admitted"] != "true" ||
		fields["action"] != "retire_source_before_publication" {
		return ""
	}
	return "interframe_capture_discontinuity:" + format
}

// The common native frame-source emits this terminal record for every adapter.
// Keep its bounded stage/peer cause in the capture summary, without treating
// an IPC/deadline failure as a USB mode failure or copying arbitrary stderr.
func brokerFrameSourceFailureReason(line string) string {
	const prefix = "[video-broker][frame-source] "
	if len(line) > 2048 || !strings.HasPrefix(line, prefix) {
		return ""
	}
	fields := make(map[string]string)
	for _, field := range strings.Fields(strings.TrimPrefix(line, prefix)) {
		key, value, ok := strings.Cut(field, "=")
		if _, duplicate := fields[key]; !ok || duplicate {
			return ""
		}
		fields[key] = value
	}
	if fields["event"] != "frame_source_failure" || fields["action"] != "retire_source" {
		return ""
	}
	code, err := strconv.Atoi(fields["code"])
	if err != nil || code >= 0 {
		return ""
	}
	for _, key := range []string{"stage", "peer_stage"} {
		value := fields[key]
		if value == "" || len(value) > 64 {
			return "frame_source_failure"
		}
		for _, character := range value {
			if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_') {
				return "frame_source_failure"
			}
		}
	}
	return "frame_source_failure:" + fields["stage"] + ":" + fields["peer_stage"]
}

// A native worker's final completion record is terminal, unlike a single
// encoder error that may still be contained by a branch transaction. Keep the
// phase in the epoch summary without trusting arbitrary stderr as a reason.
func brokerWorkerOperationFailureReason(line string) string {
	const prefix = "[video-broker][worker-operation] "
	if len(line) > 2048 || !strings.HasPrefix(line, prefix) {
		return ""
	}
	fields := make(map[string]string)
	for _, field := range strings.Fields(strings.TrimPrefix(line, prefix)) {
		key, value, ok := strings.Cut(field, "=")
		if _, duplicate := fields[key]; !ok || duplicate {
			return ""
		}
		fields[key] = value
	}
	if fields["event"] != "operation_failed" || fields["loop_completed"] != "false" ||
		fields["action"] != "checked_cleanup_before_recovery" {
		return ""
	}
	if _, err := strconv.ParseInt(fields["last_result"], 10, 32); err != nil {
		return ""
	}
	phase := fields["phase"]
	if len(phase) == 0 || len(phase) > 64 {
		return "native_operation_failed"
	}
	for _, character := range phase {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_') {
			return "native_operation_failed"
		}
	}
	return "native_operation_failed:" + phase
}

// A worker can cancel childCtx before its receive goroutine reports an earlier
// transport failure. Keep that cause, but do not request another retirement.
// Only an ordinary cancellation/clean exit of an already stopped owner is quiet.
func frameReceiveExit(err, childErr error) (report error, retire bool) {
	if childErr != nil {
		if err == nil || errors.Is(err, childErr) {
			return nil, false
		}
		return err, false
	}
	if err == nil {
		err = errors.New("frame source receive loop ended unexpectedly")
	}
	return err, true
}

// One fixed-size metadata accumulator per capture epoch. The receiver must not
// wait for lifecycle/status/logging under rt.mu after committing a real frame.
// No pixels, frame contexts, queue of frames or per-frame goroutines are kept.
type causalDeliveryObserver struct {
	ctx      context.Context
	source   string
	epoch    uint64
	wake     chan struct{}
	mu       sync.Mutex // Only fixed-size RAM operations; never rt.mu or I/O.
	stream   string
	enabled  bool
	watchdog time.Duration
	pending  causalDeliveryBatch
}

type causalDeliveryBatch struct {
	frames, bytes, tailFrames uint64
	first, last, tailFirst    time.Time
	maxGap                    time.Duration
	windowBroken              bool
}

func (rt *videoBrokerRuntime) newCausalDeliveryObserverLocked(ctx context.Context) *causalDeliveryObserver {
	o := &causalDeliveryObserver{ctx: ctx, source: rt.plan.SourceID, epoch: rt.mediaEpoch, wake: make(chan struct{}, 1)}
	o.configureLocked(rt.plan, rt.fanoutRoleGates["analysis"] == "")
	rt.causalObserver = o
	return o
}

// Called under rt.mu at both edges of an analysis gate. Observations preceding
// a gate/configuration change cannot later attest the newly enabled role.
func (o *causalDeliveryObserver) configureLocked(plan videoBrokerPlan, ungated bool) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.enabled, o.stream = ungated && plan.AnalysisEnabled, plan.AnalysisStreamID
	o.watchdog = time.Duration(plan.FrameWatchdogSec) * time.Second
	if o.watchdog <= 0 {
		o.watchdog = brokerDefaultFrameWatchdog
	}
	o.pending = causalDeliveryBatch{}
}

// Only native output validation, exact domain binding and publication may call
// this observer. It never replaces a Delivery identity or retains borrowed bytes.
func (o *causalDeliveryObserver) observe(ctx context.Context, header wire.Header, pixels []byte) error {
	if err := causalObserverContextError(ctx); err != nil {
		return fmt.Errorf("causal_observer_progress phase=before_accumulator: %w", err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := causalObserverContextError(ctx); err != nil {
		return fmt.Errorf("causal_observer_progress phase=accumulator: %w", err)
	}
	if err := o.ctx.Err(); err != nil {
		return fmt.Errorf("causal_observer_progress phase=source_retired: %w", err)
	}
	if header.Kind != wire.Delivery || header.Codec != wire.NV12 || header.Stream == "" || header.Stream != o.stream ||
		header.Source != o.source || header.SourceEpoch != o.epoch || len(pixels) != int(header.PayloadBytes) {
		return errors.New("causal delivery differs from running source")
	}
	if !o.enabled {
		return nil
	}
	o.pending.add(uint64(len(pixels)), time.Now(), o.watchdog)
	select {
	case o.wake <- struct{}{}:
	default:
	}
	return nil
}

func (b *causalDeliveryBatch) add(bytes uint64, now time.Time, watchdog time.Duration) {
	if b.frames == 0 {
		b.first, b.tailFirst = now, now
	} else {
		gap := now.Sub(b.last)
		if gap > b.maxGap {
			b.maxGap = gap
		}
		if gap >= watchdog {
			b.windowBroken, b.tailFirst, b.tailFrames = true, now, 0
		}
	}
	b.frames++
	b.bytes += bytes
	b.tailFrames++
	b.last = now
}

func (o *causalDeliveryObserver) run(rt *videoBrokerRuntime) {
	for {
		select {
		case <-o.ctx.Done():
			return
		case <-o.wake:
			rt.mu.Lock()
			rt.applyCausalDeliveryLocked(o)
			rt.mu.Unlock()
		}
	}
}

// The sole bookkeeping worker is joined with the frame receiver at shutdown.
// Take the newest aggregate AFTER acquiring rt.mu. Preserve reception times:
// processing an old aggregate is never fresh frame evidence at processing time.
func (rt *videoBrokerRuntime) applyCausalDeliveryLocked(o *causalDeliveryObserver) {
	o.mu.Lock()
	b := o.pending
	o.pending = causalDeliveryBatch{}
	o.mu.Unlock()
	if b.frames == 0 || o.ctx.Err() != nil || rt.causalObserver != o || rt.mediaEpoch != o.epoch ||
		rt.plan.SourceID != o.source || rt.plan.AnalysisStreamID != o.stream || !rt.plan.AnalysisEnabled || rt.fanoutRoleGates["analysis"] != "" {
		return
	}
	if b.last.After(rt.lastFrameAt) {
		rt.lastFrameAt = b.last
	}
	rt.everProducedFrames = true
	rt.attestCapabilityStageLocked(capability.StageDecode, rt.observedCaptureCodec)
	if rt.observedResizeBackend != "" {
		rt.attestCapabilityStageRoleLocked(capability.StageResize, "analysis", "nv12")
	}
	if rt.cameraAdmission.DevicePath != "" && !rt.cameraAdmitted {
		t := &rt.cameraAdmissionTracker
		if b.windowBroken || (!t.LastSubFrameAt.IsZero() && b.first.Sub(t.LastSubFrameAt) >= o.watchdog) {
			t.FirstSubFrameAt, t.SubFrames = time.Time{}, 0
		}
		if t.FirstSubFrameAt.IsZero() {
			t.FirstSubFrameAt = b.tailFirst
		}
		t.LastSubFrameAt, t.SubFrames = b.last, t.SubFrames+b.tailFrames
		rt.maybeCompleteCameraAdmissionLocked()
	}
	gap := b.maxGap
	if !rt.lastSubFrameAt.IsZero() && b.first.Sub(rt.lastSubFrameAt) > gap {
		gap = b.first.Sub(rt.lastSubFrameAt)
	}
	if gap > rt.metrics.subFrameGapMax {
		rt.metrics.subFrameGapMax = gap
	}
	rt.totalSubFrames += b.frames
	rt.metrics.subFrames += b.frames
	rt.metrics.subBytes += b.bytes
	rt.metrics.subNoClientFrames += b.frames
	rt.lastSubFrameAt = b.last
	rt.causalSubFrame, rt.causalSubSourceEpoch = rt.totalSubFrames, o.epoch
	// Logging uses its processing clock, not a synthetic frame clock. It runs
	// outside the accumulator mutex and cannot backpressure the receiver.
	rt.maybeLogBrokerMetricsLocked(time.Now())
}

// A context's timer goroutine can itself be delayed by host scheduling or I/O
// pressure. Compare the absolute monotonic deadline too; never make late
// accounting appear timely merely because cancellation has not been scheduled.
func causalObserverContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
