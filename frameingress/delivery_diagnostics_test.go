//go:build linux

package ingress_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/frameaccess/copyport"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
	snapshotlinux "github.com/Josepavese/videosluice/framewire/provider/linux"
	"github.com/Josepavese/videosluice/framewire/surfaceport"
)

func TestDeliverySnapshotOnlyPureContentionMayBecomeIdle(t *testing.T) {
	for _, cause := range []error{peer.ErrUnauthorized, wire.ErrMapping, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			f := setup(t)
			native, err := snapshotlinux.New(snapshotlinux.Limits{Bytes: 1 << 20, Allocations: 2, Operations: 2})
			must(t, err)
			allocator := &snapshotFaultAllocator{Allocator: native, fault: fmt.Errorf("provider: %w", errors.Join(wire.ErrBusy, cause))}
			p, err := in.NewDeliveryPublication(context.Background(), f.b, "analysis", in.DeliveryLayout{
				Transform: f.tr, Codec: wire.NV12, StrideY: 8, StrideUV: 8, MaxPayload: 96}, allocator)
			must(t, err)
			defer func() { p.Close(); must(t, p.WaitClosed(context.Background())) }()
			h, pixels := publicationFrame(t, f, 1)
			must(t, p.Publish(context.Background(), h, pixels))
			err = p.Snapshot(context.Background(), 0, func(context.Context, wire.Header, []byte) error {
				t.Fatal("mixed provider failure exposed pixels")
				return nil
			})
			if !errors.Is(err, cause) || errors.Is(err, in.ErrDeliveryIdle) {
				t.Fatalf("structural/expired cause became a harmless miss: %v", err)
			}
		})
	}
}

func TestDeliverySenderIdleDoesNotHideCancellationAtResourceFence(t *testing.T) {
	authority := &snapshotCancelAuthority{}
	f := setupCopierAuthority(t, copyport.CPU{}, authority)
	consumer := &cancelingSenderAuthority{}
	access, err := in.NewAccess(f.r, f.b, consumer, in.AccessPolicy{Generation: "consumer-start-1", Streams: []string{"analysis"}, MaxRegions: 1, MaxRequests: 1, MaxResults: 1})
	must(t, err)
	defer access.Revoke()
	native, err := snapshotlinux.New(snapshotlinux.Limits{Bytes: 1 << 20, Allocations: 2, Operations: 2})
	must(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allocator := &snapshotFaultAllocator{Allocator: native, fault: wire.ErrBusy, afterFault: func() { authority.cancel = cancel }}
	p, err := in.NewDeliveryPublication(context.Background(), f.b, "analysis", in.DeliveryLayout{
		Transform: f.tr, Codec: wire.NV12, StrideY: 8, StrideUV: 8, MaxPayload: 96}, allocator)
	must(t, err)
	defer func() { p.Close(); must(t, p.WaitClosed(context.Background())) }()
	h, pixels := publicationFrame(t, f, 1)
	must(t, p.Publish(context.Background(), h, pixels))
	sender, err := in.NewFrameSender(context.Background(), f.r, consumer, 96, uint64(time.Second))
	must(t, err)
	defer closeSender(t, sender)
	err = sender.SendDelivery(ctx, access, p, nextDelivery(h.PublicationEpoch, 0), false)
	if !errors.Is(err, context.Canceled) || errors.Is(err, in.ErrDeliveryIdle) || consumer.writes != 0 {
		t.Fatalf("idle hid resource cancellation or reached wire: writes=%d err=%v", consumer.writes, err)
	}
	for _, field := range []string{"delivery_send_progress phase=snapshot", "delivery_snapshot_progress phase=provider", "provider_us=", "confirmed_records=0 ", "context_canceled=true"} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("missing idle-fence checkpoint %q: %v", field, err)
		}
	}
}

// A live authority can finish a check after cancellation. Snapshot must fence
// that late result BEFORE invoking the consumer, not only in Borrow's epilogue.
type snapshotCancelAuthority struct{ cancel context.CancelFunc }

type cancelingSenderAuthority struct {
	cancel                   context.CancelFunc
	checks, cancelAt, writes int
}

func (w *cancelingSenderAuthority) CurrentGeneration() (string, error) {
	w.checks++
	if w.checks == w.cancelAt {
		w.cancel()
	}
	return "consumer-start-1", nil
}

func (w *cancelingSenderAuthority) WritePacket(context.Context, []byte) error {
	w.writes++
	return nil
}

func TestDeliverySenderFencesLatePeerAuthorityAtEachWriteBoundary(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		cancelAt, wantWrites int
		stage                string
	}{
		{"before_write", 1, 0, "frame_send_peer_authority_deadline"},
		{"after_write", 2, 1, "frame_send_peer_confirmation_deadline"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, access, _, _ := accessFixture(t)
			p := deliveryPublication(t, f)
			h, pixels := publicationFrame(t, f, 7)
			must(t, p.Publish(context.Background(), h, pixels))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			writer := &cancelingSenderAuthority{cancel: cancel, cancelAt: tc.cancelAt}
			sender, err := in.NewFrameSender(context.Background(), f.r, writer, 96, uint64(time.Second))
			must(t, err)
			defer closeSender(t, sender)
			err = sender.SendDelivery(ctx, access, p, nextDelivery(0, 0), true)
			if !errors.Is(err, context.Canceled) || writer.writes != tc.wantWrites {
				t.Fatalf("late authority crossed write boundary: writes=%d want=%d err=%v", writer.writes, tc.wantWrites, err)
			}
			if !strings.Contains(err.Error(), tc.stage) {
				t.Fatalf("late authority has no precise checkpoint: %v", err)
			}
			if !strings.Contains(err.Error(), "peer_cause=context_canceled") {
				t.Fatalf("context cancellation classified as an unknown transport failure: %v", err)
			}
			before := writer.writes
			err = sender.SendDelivery(context.Background(), access, p, nextDelivery(0, 0), true)
			if tc.wantWrites == 0 {
				must(t, err) // No bytes attempted: the authenticated sender is reusable.
			} else if !errors.Is(err, peer.ErrRetired) || writer.writes != before {
				t.Fatalf("partial delivery reused after cancellation: writes=%d err=%v", writer.writes, err)
			}
		})
	}
}

func (a *snapshotCancelAuthority) CurrentGeneration() (string, error) {
	if a.cancel != nil {
		cancel := a.cancel
		a.cancel = nil
		cancel()
	}
	return "authenticated-process-1", nil
}

type snapshotHookAllocator struct {
	surfaceport.Allocator
	afterSnapshot func()
}

func (a *snapshotHookAllocator) Create(mapping wire.Mapping) (surfaceport.Allocation, error) {
	allocation, err := a.Allocator.Create(mapping)
	if allocation == nil {
		return nil, err
	}
	return &snapshotHookAllocation{Allocation: allocation, owner: a}, err
}

type snapshotHookAllocation struct {
	surfaceport.Allocation
	owner *snapshotHookAllocator
}

func (a *snapshotHookAllocation) SnapshotInto(ctx context.Context, out []byte) (int, error) {
	n, err := a.Allocation.SnapshotInto(ctx, out)
	if err == nil && a.owner.afterSnapshot != nil {
		a.owner.afterSnapshot()
	}
	return n, err
}

func TestDeliverySnapshotLateAuthorityDoesNotInvokeConsumer(t *testing.T) {
	authority := &snapshotCancelAuthority{}
	f := setupCopierAuthority(t, copyport.CPU{}, authority)
	native, err := snapshotlinux.New(snapshotlinux.Limits{Bytes: 1 << 20, Allocations: 2, Operations: 2})
	must(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allocator := &snapshotHookAllocator{Allocator: native, afterSnapshot: func() { authority.cancel = cancel }}
	p, err := in.NewDeliveryPublication(context.Background(), f.b, "analysis", in.DeliveryLayout{
		Transform: f.tr, Codec: wire.NV12, StrideY: 8, StrideUV: 8, MaxPayload: 96}, allocator)
	must(t, err)
	defer func() { p.Close(); must(t, p.WaitClosed(context.Background())) }()
	h, pixels := publicationFrame(t, f, 1)
	must(t, p.Publish(context.Background(), h, pixels))
	called := false
	err = p.Snapshot(ctx, 0, func(context.Context, wire.Header, []byte) error { called = true; return nil })
	if called || !errors.Is(err, context.Canceled) {
		t.Fatalf("late authority reached consumer: called=%t err=%v", called, err)
	}
	for _, field := range []string{"delivery_snapshot_progress phase=validate", "provider_us=", "validate_us=", "consume_us=-1"} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("missing phase attribution %q: %v", field, err)
		}
	}
}

func TestDeliverySendFailureExplainsPhaseAndPreservesCause(t *testing.T) {
	for _, phase := range []string{"metadata", "header", "payload"} {
		t.Run(phase, func(t *testing.T) {
			f, access, _, _ := accessFixture(t)
			p := deliveryPublication(t, f)
			h, pixels := publicationFrame(t, f, 7)
			must(t, p.Publish(context.Background(), h, pixels))
			calls := 0
			failure := &peer.Failure{Stage: "fixture_send", Cause: errors.Join(peer.ErrDeadline, errors.New("private-packet-do-not-log"))}
			sender, err := in.NewFrameSender(context.Background(), f.r, frameWriter(func(_ context.Context, packet []byte) error {
				calls++
				kind := string(packet[:4])
				if (phase == "metadata" && kind == "FAPB") || (phase == "header" && kind == "VBCF") || (phase == "payload" && kind == "FAPD") {
					return failure
				}
				return nil
			}), 96, uint64(time.Second))
			must(t, err)
			defer closeSender(t, sender)
			err = sender.SendDelivery(context.Background(), access, p, nextDelivery(0, 0), true)
			if !errors.Is(err, peer.ErrDeadline) || !errors.Is(err, failure) {
				t.Fatalf("original peer cause lost: %v", err)
			}
			if strings.Contains(err.Error(), "private-packet-do-not-log") {
				t.Fatal("private provider data leaked")
			}
			for _, field := range []string{"delivery_send_progress phase=" + phase, "step=authorized_write", "elapsed_us=", "budget_us=", "snapshot_us=", "transaction_us=", "max_transaction_us=", "confirmed_records=", "confirmed_wire_bytes=", "confirmed_payload_bytes=0 ", "peer_cause=deadline"} {
				if !strings.Contains(err.Error(), field) {
					t.Fatalf("missing %q: %v", field, err)
				}
			}
			before := calls
			want(t, sender.SendDelivery(context.Background(), access, p, nextDelivery(0, 0), true), peer.ErrRetired)
			if calls != before {
				t.Fatal("uncertain transfer was retried")
			}
		})
	}
}

func TestDeliverySendCancellationRetainsSanitizedPeerCheckpoint(t *testing.T) {
	f, access, _, _ := accessFixture(t)
	p := deliveryPublication(t, f)
	h, pixels := publicationFrame(t, f, 7)
	must(t, p.Publish(context.Background(), h, pixels))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failure := &peer.Failure{Stage: "fixture_late_write", Cause: errors.Join(peer.ErrDeadline, errors.New("private-packet-do-not-log"))}
	sender, err := in.NewFrameSender(context.Background(), f.r, frameWriter(func(_ context.Context, packet []byte) error {
		if string(packet[:4]) == "FAPD" {
			cancel()
			return failure
		}
		return nil
	}), 96, uint64(time.Second))
	must(t, err)
	defer closeSender(t, sender)
	err = sender.SendDelivery(ctx, access, p, nextDelivery(0, 0), true)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, failure) || !errors.Is(err, peer.ErrDeadline) {
		t.Fatalf("lost late peer failure: %v", err)
	}
	for _, field := range []string{"delivery_snapshot_progress phase=consumer", "frame_send_write", "fixture_late_write", "peer_cause=deadline"} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("missing %q: %v", field, err)
		}
	}
	if strings.Contains(err.Error(), "private-packet-do-not-log") {
		t.Fatal("private provider cause leaked")
	}
}

func TestDeliverySendSnapshotFailureIsNotReportedAsNetworkWrite(t *testing.T) {
	f, access, _, _ := accessFixture(t)
	native, err := snapshotlinux.New(snapshotlinux.Limits{Bytes: 1 << 20, Allocations: 2, Operations: 2})
	must(t, err)
	allocator := &snapshotFaultAllocator{Allocator: native, fault: context.DeadlineExceeded}
	p, err := in.NewDeliveryPublication(context.Background(), f.b, "analysis", in.DeliveryLayout{
		Transform: f.tr, Codec: wire.NV12, StrideY: 8, StrideUV: 8, MaxPayload: 96}, allocator)
	must(t, err)
	defer func() { p.Close(); must(t, p.WaitClosed(context.Background())) }()
	h, pixels := publicationFrame(t, f, 1)
	must(t, p.Publish(context.Background(), h, pixels))
	sender, err := in.NewFrameSender(context.Background(), f.r, frameWriter(func(context.Context, []byte) error {
		t.Fatal("snapshot failure reached wire")
		return nil
	}), 96, uint64(time.Second))
	must(t, err)
	defer closeSender(t, sender)
	err = sender.SendDelivery(context.Background(), access, p, nextDelivery(h.PublicationEpoch, 0), false)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost snapshot cause: %v", err)
	}
	for _, field := range []string{"delivery_send_progress phase=snapshot", "confirmed_records=0 ", "confirmed_wire_bytes=0 ", "context_deadline=true"} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("missing %q: %v", field, err)
		}
	}
}
