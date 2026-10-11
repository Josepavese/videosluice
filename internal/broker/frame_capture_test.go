package broker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/Josepavese/videosluice/internal/source"
)

type captureStub struct {
	epoch    uint64
	retired  int
	closeErr error
}

func (c *captureStub) SourceEpoch() uint64                        { return c.epoch }
func (c *captureStub) Attach(*exec.Cmd) error                     { return nil }
func (c *captureStub) Started(context.Context, *os.Process) error { return nil }
func (c *captureStub) Run(ctx context.Context, _ source.FrameDeliveryConsumer) error {
	<-ctx.Done()
	return ctx.Err()
}
func (c *captureStub) Snapshot(context.Context, string, uint64, source.FrameDeliveryConsumer) error {
	return errors.New("fixture has no delivery")
}
func (c *captureStub) Retire(context.Context) error { c.retired++; return c.closeErr }

func TestPendingFrameCleanupKeepsAdmissionHeldUntilSameReleaseCompletes(t *testing.T) {
	joined := make(chan struct{})
	capture := &captureStub{epoch: 7, closeErr: context.DeadlineExceeded}
	state := t.TempDir()
	persistFrameCleanupGuard(state, captureIncidentID(7), context.DeadlineExceeded)
	rt := &videoBrokerRuntime{plan: videoBrokerPlan{StateDirectory: state}, frameCleanupErr: context.DeadlineExceeded,
		pendingFrameCleanup: capture, pendingFrameJoin: joined, mediaEpoch: 7}
	rt.reconcilePendingFrameCleanup(context.Background())
	if capture.retired != 0 || rt.pendingFrameCleanup == nil {
		t.Fatal("retired while frame reader still owned resources")
	}
	close(joined)
	rt.reconcilePendingFrameCleanup(context.Background())
	if capture.retired != 1 || rt.pendingFrameCleanup == nil || rt.frameCleanupErr == nil {
		t.Fatal("waiter deadline incorrectly released admission")
	}
	capture.closeErr = nil
	rt.reconcilePendingFrameCleanup(context.Background())
	if capture.retired != 2 || rt.pendingFrameCleanup != nil || rt.frameCleanupErr != nil {
		t.Fatal("confirmed completion did not clear hold")
	}
	if err := loadFrameCleanupGuard(state); err != nil {
		t.Fatalf("confirmed completion left a restart guard: %v", err)
	}
	confirmed := errors.New("sdk release unconfirmed")
	rt.frameCleanupErr, rt.pendingFrameCleanup = context.DeadlineExceeded, capture
	capture.closeErr = confirmed
	rt.reconcilePendingFrameCleanup(context.Background())
	if !errors.Is(rt.frameCleanupErr, confirmed) || rt.pendingFrameCleanup != nil {
		t.Fatal("physical release failure did not remain quarantined")
	}
	if err := loadFrameCleanupGuard(state); !errors.Is(err, errFrameCleanupGuardActive) {
		t.Fatalf("unconfirmed physical release lost restart guard: %v", err)
	}
}

func TestCapturePreparationUsesAdmittedEpochAndNeverFallsBack(t *testing.T) {
	for _, mode := range []string{"admitted", "error", "partial", "nil", "zero"} {
		t.Run(mode, func(t *testing.T) {
			plan := source.BrokerMuxPlan{SourceID: "front", MediaEpoch: 99}
			c := &captureStub{epoch: 7}
			failure := errors.New("admission rejected")
			cleanup := errors.New("release unconfirmed")
			factory := func(ctx context.Context, p source.BrokerMuxPlan) (source.FrameCapture, error) {
				if p.SourceID != "front" || p.MediaEpoch != 99 {
					t.Fatal("input plan changed before admission")
				}
				switch mode {
				case "error":
					return nil, failure
				case "partial":
					c.closeErr = cleanup
					return c, failure
				case "nil":
					return nil, nil
				case "zero":
					c.epoch = 0
				}
				return c, nil
			}
			got, err := prepareCaptureFrames(context.Background(), factory, &plan)
			if mode == "admitted" {
				if err != nil || got != c || plan.MediaEpoch != 7 || c.retired != 0 {
					t.Fatal(got, err, plan.MediaEpoch, c.retired)
				}
				return
			}
			if err == nil || got != nil || plan.MediaEpoch != 99 {
				t.Fatal("failed admission enabled capture", got, err, plan.MediaEpoch)
			}
			if mode == "partial" && (!errors.Is(err, failure) || !errors.Is(err, cleanup) || c.retired != 1) {
				t.Fatal("partial cleanup discarded", err, c.retired)
			}
			if mode == "zero" && c.retired != 1 {
				t.Fatal("invalid generation not retired")
			}
		})
	}
}

func TestFrameProviderDoesNotBecomePersistedConfiguration(t *testing.T) {
	p := videoBrokerPlan{SourceID: "front", FrameCapture: func(context.Context, source.BrokerMuxPlan) (source.FrameCapture, error) { return nil, nil }}
	with, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	p.FrameCapture = nil
	without, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(with) != string(without) {
		t.Fatal("provider mutated persisted graph identity")
	}
	if _, err := sourceStarter([]StartOptions{{}, {}}); err == nil {
		t.Fatal("ambiguous runtime providers accepted")
	}
}

func TestFramePreparationExplicitlyUnrequestedIsNotUnavailable(t *testing.T) {
	for _, mode := range []string{"not_requested", "requested_unavailable", "mixed_failure", "owned_not_requested"} {
		t.Run(mode, func(t *testing.T) {
			plan := source.BrokerMuxPlan{SourceID: "encoded-only", MediaEpoch: 99}
			owned := &captureStub{epoch: 7}
			unavailable := errors.New("requested provider unavailable")
			calls := 0
			factory := func(context.Context, source.BrokerMuxPlan) (source.FrameCapture, error) {
				calls++
				switch mode {
				case "not_requested":
					return nil, source.ErrFrameCaptureNotRequested
				case "mixed_failure":
					return nil, errors.Join(source.ErrFrameCaptureNotRequested, unavailable)
				case "owned_not_requested":
					return owned, source.ErrFrameCaptureNotRequested
				default:
					return nil, unavailable
				}
			}
			got, err := prepareCaptureFrames(context.Background(), factory, &plan)
			if got != nil || plan.MediaEpoch != 99 || calls != 1 {
				t.Fatalf("invented owner/epoch: %v %d calls=%d", got, plan.MediaEpoch, calls)
			}
			if (err == nil) != (mode == "not_requested") {
				t.Fatalf("decision/failure confused: %v", err)
			}
			if mode == "owned_not_requested" && owned.retired != 1 {
				t.Fatal("contradictory decision lost owned resource")
			}
			if mode == "mixed_failure" && !errors.Is(err, unavailable) {
				t.Fatal("requested admission failure was suppressed")
			}
		})
	}
}
