package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/internal/source"
)

func causalObserverFixture(ctx context.Context) (*videoBrokerRuntime, *causalDeliveryObserver, wire.Header) {
	rt := &videoBrokerRuntime{
		plan:       videoBrokerPlan{SourceID: "front", AnalysisStreamID: "analysis", AnalysisEnabled: true, FrameWatchdogSec: 3},
		mediaEpoch: 7, childRunning: true, childStartedAt: time.Now().Add(-time.Minute),
	}
	rt.metrics.lastLog = time.Now()
	return rt, rt.newCausalDeliveryObserverLocked(ctx), wire.Header{Kind: wire.Delivery, Codec: wire.NV12,
		Source: "front", Stream: "analysis", SourceEpoch: 7, PayloadBytes: 24}
}

func TestCausalObserverCoalescesWithoutBlockingAndJoins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, observer, header := causalObserverFixture(ctx)
	rt.mu.Lock()
	done := make(chan struct{})
	go func() { observer.run(rt); close(done) }()
	frameCtx, stopFrame := context.WithTimeout(ctx, time.Second)
	defer stopFrame()
	for i := 0; i < 10000; i++ {
		header.DeliveryID, header.OriginSequence = uint64(i+1), uint64(i+100)
		if err := observer.observe(frameCtx, header, make([]byte, 24)); err != nil {
			rt.mu.Unlock()
			t.Fatal(err)
		}
	}
	observer.mu.Lock()
	batch := observer.pending
	observer.mu.Unlock()
	if batch.frames != 10000 || batch.bytes != 240000 || cap(observer.wake) != 1 {
		rt.mu.Unlock()
		t.Fatalf("lost accounting or unbounded wake queue: %+v", batch)
	}
	// A completed frame's context is normally canceled by Receive. Metadata
	// must not retain that context, and must keep its original reception time.
	stopFrame()
	rt.mu.Unlock()
	deadline := time.Now().Add(time.Second)
	for {
		rt.mu.Lock()
		count, last, next := rt.totalSubFrames, rt.lastSubFrameAt, rt.nextSubFrame
		rt.mu.Unlock()
		if count == 10000 {
			if last != batch.last || next != 0 {
				t.Fatal("bookkeeping changed frame time or identity")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("bookkeeping did not drain: %d", count)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("epoch observer did not join")
	}
}

func TestCausalObserverOldMetadataDoesNotBecomeFreshHealth(t *testing.T) {
	rt, observer, _ := causalObserverFixture(context.Background())
	observed := time.Now().Add(-30 * time.Second)
	observer.pending.add(24, observed, observer.watchdog)
	rt.applyCausalDeliveryLocked(observer)
	if rt.lastFrameAt != observed || rt.lastSubFrameAt != observed || rt.totalSubFrames != 1 {
		t.Fatal("deferred metadata was timestamped as new input")
	}
	if state := rt.healthSnapshot(time.Now()); state.State != "degraded" {
		t.Fatalf("stale reception attested a healthy stream: %+v", state)
	}
}

func TestCausalObserverCanceledReplacedAndGatedEpochsCannotAttest(t *testing.T) {
	for _, mode := range []string{"canceled", "replacement", "epoch", "source", "gate_roundtrip", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rt, observer, header := causalObserverFixture(ctx)
			if err := observer.observe(ctx, header, make([]byte, 24)); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "canceled":
				cancel()
				if err := observer.observe(context.Background(), header, make([]byte, 24)); !errors.Is(err, context.Canceled) {
					t.Fatal("retired source accepted metadata", err)
				}
			case "replacement":
				rt.newCausalDeliveryObserverLocked(ctx)
			case "epoch":
				rt.mediaEpoch++
			case "source":
				rt.plan.SourceID = "overview"
			case "gate_roundtrip":
				if err := setFanoutRoleGateLocked(rt, "analysis", "switch"); err != nil {
					t.Fatal(err)
				}
				if err := observer.observe(ctx, header, make([]byte, 24)); err != nil {
					t.Fatal(err)
				}
				if err := clearFanoutRoleGateLocked(rt, "analysis", "switch"); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				rt.plan.AnalysisEnabled = false
			}
			rt.applyCausalDeliveryLocked(observer)
			if rt.totalSubFrames != 0 || rt.everProducedFrames || !rt.lastFrameAt.IsZero() || rt.causalSubFrame != 0 {
				t.Fatal("retired/gated evidence attested the current role")
			}
		})
	}
}

func TestCausalObserverBatchPreservesAdmissionWindowsAndCadence(t *testing.T) {
	for _, flushEvery := range []int{1, 2, 4, 100} {
		rt, observer, _ := causalObserverFixture(context.Background())
		baseline, _, _ := causalObserverFixture(context.Background())
		rt.cameraAdmission = source.CameraAdmissionDecision{DevicePath: "/fixture-camera"}
		baseline.cameraAdmission = rt.cameraAdmission
		start := time.Now().Add(-time.Minute)
		// Includes a break within a batch, between batches and at the watchdog
		// boundary. Only the last contiguous window may satisfy admission.
		for i, offset := range []time.Duration{0, 50, 100, 3100, 3150, 8000, 8050, 8100, 8150} {
			now := start.Add(offset * time.Millisecond)
			baseline.observeCameraAdmissionFrameLocked(brokerStreamNV12, nil, now)
			baseline.recordBrokerFrameLocked(brokerStreamNV12, 24, 0, now)
			observer.pending.add(24, now, observer.watchdog)
			if (i+1)%flushEvery == 0 {
				rt.applyCausalDeliveryLocked(observer)
			}
		}
		rt.applyCausalDeliveryLocked(observer)
		if rt.cameraAdmissionTracker != baseline.cameraAdmissionTracker || rt.totalSubFrames != baseline.totalSubFrames ||
			rt.metrics.subFrameGapMax != baseline.metrics.subFrameGapMax || rt.lastSubFrameAt != baseline.lastSubFrameAt ||
			rt.metrics.subBytes != baseline.metrics.subBytes {
			t.Fatalf("batch=%d changed admission/cadence: got=%+v want=%+v", flushEvery, rt.cameraAdmissionTracker, baseline.cameraAdmissionTracker)
		}
	}
}

func TestCausalObserverRejectsInvalidIdentityAndLayout(t *testing.T) {
	for _, mutate := range []func(*wire.Header){
		func(h *wire.Header) { h.Source = "overview" }, func(h *wire.Header) { h.Stream = "archive" },
		func(h *wire.Header) { h.SourceEpoch++ }, func(h *wire.Header) { h.Kind = wire.SourceOutput },
		func(h *wire.Header) { h.Codec = wire.H265 }, func(h *wire.Header) { h.PayloadBytes++ },
	} {
		rt, observer, header := causalObserverFixture(context.Background())
		mutate(&header)
		if err := observer.observe(context.Background(), header, make([]byte, 24)); err == nil {
			t.Fatal("invalid delivery accepted")
		}
		rt.applyCausalDeliveryLocked(observer)
		if rt.totalSubFrames != 0 {
			t.Fatal("invalid delivery refreshed health")
		}
	}
}

func BenchmarkCausalDeliveryObservation(b *testing.B) {
	_, observer, header := causalObserverFixture(context.Background())
	pixels := make([]byte, 24)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := observer.observe(context.Background(), header, pixels); err != nil {
			b.Fatal(err)
		}
	}
}
