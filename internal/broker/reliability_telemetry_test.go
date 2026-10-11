package broker

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"
)

func TestCaptureDiscontinuityKeepsTerminalCauseOnlyAfterAdmission(t *testing.T) {
	line := "[rdkx5_v4l2_mux_bridge][capture-integrity] event=interframe_capture_discontinuity format=h265 previous=983457 current=983650 delta=193 bytes=8690 flags=73729 admitted=true action=retire_source_before_publication"
	if got := brokerChildTerminalFailureReason(line); got != "interframe_capture_discontinuity:h265" {
		t.Fatal(got)
	}
	for _, nonterminal := range []string{
		strings.ReplaceAll(line, "admitted=true", "admitted=false"),
		strings.ReplaceAll(line, "retire_source_before_publication", "await_fresh_idr_no_publication"),
		strings.ReplaceAll(line, "retire_source_before_publication", "rebuild_reference_gate"),
		line + " admitted=true", strings.Repeat("x", 2049),
	} {
		if got := brokerChildTerminalFailureReason(nonterminal); got != "" {
			t.Fatalf("not a terminal record: %s", got)
		}
	}
	// A tolerated discontinuity stays non-terminal; the bounded resync that
	// follows it does have its own terminal reason when it never reaches IDR.
	tolerated := "[rdkx5_v4l2_mux_bridge][capture-integrity] event=interframe_capture_discontinuity format=h265 previous=21991 current=21998 delta=7 bytes=5017 flags=73729 admitted=true tolerated=3 action=rebuild_reference_gate"
	if got := brokerChildTerminalFailureReason(tolerated); got != "" {
		t.Fatalf("tolerated discontinuity must not be terminal: %s", got)
	}
	resync := "[rdkx5_v4l2_mux_bridge][capture-integrity] event=interframe_resync_timeout format=h265 action=retire_source_before_publication"
	if got := brokerChildTerminalFailureReason(resync); got != "interframe_resync_timeout:h265" {
		t.Fatal(got)
	}
	for _, nonterminal := range []string{
		strings.ReplaceAll(resync, "format=h265", "format=mjpeg"),
		strings.ReplaceAll(resync, "retire_source_before_publication", "await_fresh_idr_no_publication"),
	} {
		if got := brokerChildTerminalFailureReason(nonterminal); got != "" {
			t.Fatalf("not a terminal record: %s", got)
		}
	}
}

func TestPipelineSummaryRefreshesHealthWithoutStatusConsumer(t *testing.T) {
	now := time.Now()
	rt := &videoBrokerRuntime{healthState: "initializing", childRunning: true,
		childStartedAt: now.Add(-time.Minute), plan: videoBrokerPlan{AnalysisEnabled: true},
		lastSubFrameAt: now}
	rt.metrics.lastLog = now.Add(-time.Hour)
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	rt.maybeLogBrokerMetricsLocked(now)
	if !strings.Contains(output.String(), "health_state=running") {
		t.Fatal(output.String())
	}
}
