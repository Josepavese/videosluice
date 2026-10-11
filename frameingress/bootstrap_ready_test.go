package ingress_test

import (
	"context"
	"errors"
	"testing"
	"time"

	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
)

func TestBootstrapReadyAuthenticatesExactExecutionBeforeConsumer(t *testing.T) {
	for _, mode := range []string{"ready", "idle_then_ready", "wrong_record", "wrong_principal", "changed_generation", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			c := &controlConnection{generation: "broker-1"}
			c.write = func(context.Context, []byte, peer.ProcessCapability) error {
				t.Fatal("startup readiness must never send a command or grant")
				return nil
			}
			c.receive = func(ctx context.Context, p []byte) (int, peer.Principal, error) {
				calls++
				if mode == "deadline" {
					<-ctx.Done()
					return 0, peer.Principal{}, errors.Join(peer.ErrDeadline, peer.ErrNoRecord)
				}
				if mode == "idle_then_ready" && calls == 1 {
					return 0, peer.Principal{}, errors.Join(peer.ErrDeadline, peer.ErrNoRecord)
				}
				packet := []byte{'V', 'B', 'R', 'E', 'A', 'D', 'Y', 1}
				principal := peer.Principal{Generation: "broker-1"}
				if mode == "wrong_record" {
					packet[0] = 'X'
				}
				if mode == "wrong_principal" {
					principal.Generation = "broker-2"
				}
				if mode == "changed_generation" {
					c.generation = "broker-2"
				}
				return copy(p, packet), principal, nil
			}
			err := in.AwaitBootstrapReady(context.Background(), c, "broker-1", 20*time.Millisecond)
			wantOK := mode == "ready" || mode == "idle_then_ready"
			if (err == nil) != wantOK {
				t.Fatalf("mode=%s err=%v", mode, err)
			}
			if c.closed.Load() != 0 {
				t.Fatal("only the launcher owns transport close")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &controlConnection{generation: "broker-1"}
	if err := in.AwaitBootstrapReady(ctx, c, "broker-1", time.Second); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := in.AwaitBootstrapReady(context.Background(), c, "broker-1", 31*time.Second); !errors.Is(err, peer.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestBootstrapReadyPureWaitCanRetryButConsumedRecordCannot(t *testing.T) {
	for _, mode := range []string{"idle", "cancel_before_read", "cancel_after_read", "unproven_deadline"} {
		t.Run(mode, func(t *testing.T) {
			c := &controlConnection{generation: "broker-cold"}
			c.receive = func(ctx context.Context, _ []byte) (int, peer.Principal, error) {
				<-ctx.Done()
				err := ctx.Err()
				if mode == "idle" || mode == "cancel_before_read" {
					err = errors.Join(err, peer.ErrNoRecord)
				}
				if mode == "idle" || mode == "unproven_deadline" {
					err = errors.Join(err, peer.ErrDeadline)
				}
				// Error returns zero usable bytes even when a record was read.
				return 0, peer.Principal{}, err
			}
			err := in.AwaitBootstrapReady(context.Background(), c, "broker-cold", 10*time.Millisecond)
			pureWait := mode == "idle" || mode == "cancel_before_read"
			if err == nil || errors.Is(err, peer.ErrNoRecord) != pureWait {
				t.Fatalf("mode=%s pure_wait=%t err=%v", mode, pureWait, err)
			}
			if !pureWait {
				return // Caller must retire: do not reinterpret the next ACK.
			}
			c.receive = func(_ context.Context, data []byte) (int, peer.Principal, error) {
				return copy(data, []byte{'V', 'B', 'R', 'E', 'A', 'D', 'Y', 1}), peer.Principal{Generation: "broker-cold"}, nil
			}
			if err = in.AwaitBootstrapReady(context.Background(), c, "broker-cold", time.Second); err != nil {
				t.Fatal("fresh bounded wait lost the same execution's ready record", err)
			}
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &controlConnection{generation: "broker-cold"}
	c.receive = func(context.Context, []byte) (int, peer.Principal, error) {
		t.Fatal("cancelled wait consumed a record")
		return 0, peer.Principal{}, nil
	}
	if err := in.AwaitBootstrapReady(ctx, c, "broker-cold", time.Second); !errors.Is(err, peer.ErrNoRecord) {
		t.Fatal("pre-read cancellation lost no-consumption proof", err)
	}
}
