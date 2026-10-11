package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/internal/source"
)

func TestDeliveryCommitDiagnosticsPreservePhaseAndFailure(t *testing.T) {
	failure := errors.New("provider failure")
	for _, stage := range []string{"atomic_publication", "runtime_observer"} {
		t.Run(stage, func(t *testing.T) {
			calls := 0
			operation := func(name string) source.FrameDeliveryConsumer {
				return func(context.Context, wire.Header, []byte) error {
					calls++
					if name == stage {
						return failure
					}
					return nil
				}
			}
			err := publishObservedDelivery(context.Background(), wire.Header{SourceEpoch: 2, DeliveryID: 7, OriginSequence: 11}, nil,
				operation("atomic_publication"), operation("runtime_observer"))
			if !errors.Is(err, failure) {
				t.Fatalf("cause lost: %v", err)
			}
			for _, marker := range []string{"phase=" + stage, "source_epoch=2", "delivery_id=7", "origin_sequence=11", "publish_us=", "observer_us=", "elapsed_us=", "budget_us="} {
				if !strings.Contains(err.Error(), marker) {
					t.Fatalf("missing %s: %v", marker, err)
				}
			}
			if stage == "atomic_publication" && calls != 1 {
				t.Fatal("observer ran after failed publication")
			}
			if stage == "runtime_observer" && calls != 2 {
				t.Fatal("unexpected retry")
			}
		})
	}
}

func TestDeliveryCommitDoesNotObserveLateSuccessfulPublication(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := publishObservedDelivery(ctx, wire.Header{}, nil,
		func(context.Context, wire.Header, []byte) error { cancel(); return nil },
		func(context.Context, wire.Header, []byte) error {
			t.Fatal("expired frame reached observer")
			return nil
		})
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "phase=atomic_publication") {
		t.Fatalf("late commit misclassified: %v", err)
	}
}

func TestDeliveryCommitCanceledBeforeAdmissionDoesNoWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	operation := func(context.Context, wire.Header, []byte) error {
		t.Fatal("canceled request performed work")
		return nil
	}
	err := publishObservedDelivery(ctx, wire.Header{}, nil, operation, operation)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "phase=before_publication") {
		t.Fatalf("canceled admission misclassified: %v", err)
	}
}

func TestDeliveryCommitLateObserverCannotClaimSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := publishObservedDelivery(ctx, wire.Header{}, nil,
		func(context.Context, wire.Header, []byte) error { return nil },
		func(context.Context, wire.Header, []byte) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "phase=runtime_observer") {
		t.Fatalf("late observer misclassified: %v", err)
	}
}

func TestDeliveryCommitHealthyPathBorrowsExactBytesOnce(t *testing.T) {
	pixels := []byte{1, 2, 3}
	header := wire.Header{DeliveryID: 19, OriginSequence: 23}
	calls := 0
	operation := func(_ context.Context, h wire.Header, p []byte) error {
		calls++
		if h != header || &p[0] != &pixels[0] {
			t.Fatal("frame copied or replaced")
		}
		return nil
	}
	if err := publishObservedDelivery(context.Background(), header, pixels, operation, operation); err != nil || calls != 2 {
		t.Fatalf("healthy publication changed: calls=%d err=%v", calls, err)
	}
}

func TestDeliveryCommitHealthyDiagnosticsDoNotAllocate(t *testing.T) {
	ctx := context.Background()
	header := wire.Header{DeliveryID: 19, OriginSequence: 23}
	pixels := make([]byte, 640*360*3/2)
	operation := func(context.Context, wire.Header, []byte) error { return nil }
	allocs := testing.AllocsPerRun(100, func() {
		if err := publishObservedDelivery(ctx, header, pixels, operation, operation); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("healthy per-frame diagnostics allocated: %g allocations/run", allocs)
	}
}
