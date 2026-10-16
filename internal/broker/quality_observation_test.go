package broker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/quality"
)

func TestQualityReadDoesNotBlockArchiveDispatch(t *testing.T) {
	now := time.Now()
	rt := &videoBrokerRuntime{
		plan: videoBrokerPlan{ArchiveEnabled: true, ArchiveWidth: 640, ArchiveHeight: 360,
			AnalysisEnabled: true, QualityMonitorEnabled: true, QualityMonitorStatePath: "/quality-fixture.json",
			FrameWatchdogSec: 12},
		createdAt: now.Add(-time.Minute), childRunning: true, childStartedAt: now.Add(-time.Minute),
		lastSubFrameAt: now, lastArchiveFrameAt: now,
	}
	rt.metrics.lastLog = now
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	rt.qualityObservation.readFile = func(path string) ([]byte, error) {
		once.Do(func() { close(entered) })
		<-release
		return nil, os.ErrNotExist
	}
	ctx, cancel := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	go func() { rt.observeQuality(ctx); close(workerDone) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		cancel()
		close(release)
		t.Fatal("quality reader was not entered")
	}
	healthDone := make(chan struct{})
	go func() { rt.healthSnapshot(now); close(healthDone) }()
	dispatchDone := make(chan struct{})
	go func() {
		rt.dispatchFrame(brokerStreamH265, []byte{0, 0, 0, 1, 0x26, 1, 0x80})
		close(dispatchDone)
	}()
	var blocked error
	select {
	case <-dispatchDone:
	case <-time.After(150 * time.Millisecond):
		blocked = errors.New("quality filesystem read retained the media mutex and stopped archive dispatch")
	}
	cancel()
	close(release)
	<-healthDone
	<-dispatchDone
	<-workerDone
	if blocked != nil {
		t.Fatal(blocked)
	}
	if rt.totalArchiveFrames != 1 {
		t.Fatalf("archive frame was not delivered: %d", rt.totalArchiveFrames)
	}
}

func TestQualityObservationShutdownJoinsActualRead(t *testing.T) {
	rt := &videoBrokerRuntime{plan: videoBrokerPlan{AnalysisEnabled: true,
		QualityMonitorEnabled: true, QualityMonitorStatePath: "/fixture.json"}}
	entered, release := make(chan struct{}), make(chan struct{})
	rt.qualityObservation.readFile = func(string) ([]byte, error) { close(entered); <-release; return nil, os.ErrNotExist }
	ctx, cancel := context.WithCancel(context.Background())
	if !rt.startGoroutine(func() { rt.observeQuality(ctx) }) {
		t.Fatal("observer not owned by runtime")
	}
	<-entered
	cancel()
	joined := make(chan struct{})
	go func() { rt.wg.Wait(); close(joined) }()
	premature := false
	select {
	case <-joined:
		premature = true
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	<-joined
	if premature || rt.qualityObservation.ready {
		t.Fatal("unreturned read hidden by cancellation or late result published")
	}
}

func qualityObservationFixture(t *testing.T, now time.Time) []byte {
	t.Helper()
	source := withOptimizerSpatial(quality.Metrics{Width: 1920, Height: 1080, Samples: 32400, BlackRatio: 1})
	analysis := source
	analysis.Width, analysis.Height, analysis.Samples = 640, 360, 3600
	analysis = withOptimizerSpatial(analysis)
	data, err := json.Marshal(quality.State{Schema: quality.Schema, TimestampUnixMS: now.UnixMilli(),
		Frame: 42, SampleStride: 8, Source: source, Processed: source, Analysis: analysis,
		Capture: quality.Capture{Format: "h265", Orientation: "normal"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := quality.Decode(data); err != nil {
		t.Fatal(err)
	}
	return data
}

func TestQualityObservationKeepsProducerTimestampAndDecodeFailures(t *testing.T) {
	now := time.Now()
	data := qualityObservationFixture(t, now)
	rt := &videoBrokerRuntime{plan: videoBrokerPlan{AnalysisEnabled: true, QualityMonitorEnabled: true,
		QualityMonitorStatePath: "/fixture.json", CaptureFPS: 30}, mediaEpoch: 7}
	rt.qualityObservation.readFile = func(string) ([]byte, error) { return data, nil }
	rt.refreshQualityObservation(context.Background())
	if _, err := rt.qualityStateLocked(now); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.qualityStateLocked(now.Add(11 * time.Second)); err == nil || !strings.Contains(err.Error(), "stale state") {
		t.Fatalf("cached observation was renamed fresh: %v", err)
	}
	data = qualityObservationFixture(t, now.Add(6*time.Second))
	rt.refreshQualityObservation(context.Background())
	if _, err := rt.qualityStateLocked(now); err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("future observation accepted: %v", err)
	}
	data = []byte(`{"schema":"untrusted-invalid-state"}`)
	rt.refreshQualityObservation(context.Background())
	if _, err := rt.qualityStateLocked(now); err == nil {
		t.Fatal("malformed replacement hidden by cached success")
	}
}

func TestQualityStatusRequestsDoNotAmplifyFileReads(t *testing.T) {
	now := time.Now()
	data := qualityObservationFixture(t, now)
	rt := &videoBrokerRuntime{plan: videoBrokerPlan{AnalysisEnabled: true, QualityMonitorEnabled: true,
		QualityMonitorStatePath: "/fixture.json"}, mediaEpoch: 7,
		childRunning: true, childStartedAt: now.Add(-time.Minute), lastSubFrameAt: now}
	reads := 0
	rt.qualityObservation.readFile = func(string) ([]byte, error) { reads++; return data, nil }
	rt.refreshQualityObservation(context.Background())
	for i := 0; i < 1000; i++ {
		if value := rt.healthSnapshot(now); value.State != "running" || value.OpticalSignalState != "non_informative" {
			t.Fatalf("cached observation semantics changed: %+v", value)
		}
	}
	if reads != 1 {
		t.Fatalf("status amplified filesystem I/O: %d reads", reads)
	}
}

func TestQualityObservationDiscardsOldEpochAndCanceledRead(t *testing.T) {
	for _, scenario := range []string{"epoch", "path", "disabled", "cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			data := qualityObservationFixture(t, time.Now())
			rt := &videoBrokerRuntime{plan: videoBrokerPlan{AnalysisEnabled: true, QualityMonitorEnabled: true,
				QualityMonitorStatePath: "/fixture.json"}, mediaEpoch: 7}
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			rt.qualityObservation.readFile = func(string) ([]byte, error) { close(entered); <-release; return data, nil }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { rt.refreshQualityObservation(ctx); close(done) }()
			<-entered
			if scenario == "cancellation" {
				cancel()
			} else {
				rt.mu.Lock()
				switch scenario {
				case "epoch":
					rt.mediaEpoch++
				case "path":
					rt.plan.QualityMonitorStatePath = "/replacement.json"
				case "disabled":
					rt.plan.AnalysisEnabled = false
				}
				rt.mu.Unlock()
			}
			close(release)
			<-done
			if rt.qualityObservation.ready {
				t.Fatal("late old/canceled observation published")
			}
		})
	}
}

func TestQualityObservationReadHasFiniteMetadataBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quality.json")
	if err := os.WriteFile(path, []byte(strings.Repeat(" ", maxQualityObservationBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readQualityObservation(path); err == nil || !strings.Contains(err.Error(), "metadata budget") {
		t.Fatalf("unbounded metadata read: %v", err)
	}
}
