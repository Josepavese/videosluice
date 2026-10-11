//go:build linux

package ingress_test

import (
	"context"
	"errors"
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

// A live authority can finish a check after cancellation. Snapshot must fence
// that late result BEFORE invoking the consumer, not only in Borrow's epilogue.
type snapshotCancelAuthority struct{ cancel context.CancelFunc }

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
