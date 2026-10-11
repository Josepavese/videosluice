package broker

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/internal/diagnosticlog"
)

type heldDiagnosticSink struct{ entered, release chan struct{} }

func (s *heldDiagnosticSink) Write(p []byte) (int, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	return len(p), nil
}

// This recreates the actual fan-out hazard: metrics are logged with rt.mu held.
// The daemon's bounded diagnostic writer must keep dispatch/status independent
// of the downstream logger; otherwise native stdout fills and camera loses GOP.
func TestMetricsLoggingCannotHoldMediaMutexOnBlockedSink(t *testing.T) {
	sink := &heldDiagnosticSink{entered: make(chan struct{}, 1), release: make(chan struct{})}
	w, _ := diagnosticlog.New(sink, 2)
	prior := log.Writer()
	log.SetOutput(w)
	t.Cleanup(func() {
		log.SetOutput(prior)
		close(sink.release)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := w.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	rt := &videoBrokerRuntime{}
	// Use the real metrics path without needing a camera or synthetic corpus.
	rt.metrics.lastLog = time.Now().Add(-time.Hour)
	done := make(chan struct{})
	go func() { rt.mu.Lock(); rt.maybeLogBrokerMetricsLocked(time.Now()); rt.mu.Unlock(); close(done) }()
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("metrics did not reach diagnostic sink")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("diagnostics retained media lock")
	}
	if !rt.mu.TryLock() {
		t.Fatal("media/status mutex remains blocked")
	}
	rt.mu.Unlock()
}
