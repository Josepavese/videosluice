package ingress

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Fixed-size, stack-local checkpoints; formatted on failure only. A snapshot
// contains authentication and resource admission as well as the native copy:
// its aggregate duration alone must not be blamed on the pixel/hash provider.
type deliverySnapshotProgress struct {
	phase                                             string
	started, authorityDone, borrowStarted, borrowed   time.Time
	providerDone, headerDone, validated, consumerDone time.Time
}

func (p deliverySnapshotProgress) failure(err error) error {
	now := time.Now()
	end := func(completed time.Time, phase string) time.Time {
		if completed.IsZero() && p.phase == phase {
			return now
		}
		return completed
	}
	return fmt.Errorf("delivery_snapshot_progress phase=%s elapsed_us=%d authority_us=%d resource_us=%d provider_us=%d header_us=%d validate_us=%d consume_us=%d release_us=%d: %w",
		p.phase, diagnosticMicros(p.started, now),
		diagnosticMicros(p.started, end(p.authorityDone, "authority")),
		diagnosticMicros(p.borrowStarted, end(p.borrowed, "resource")),
		diagnosticMicros(p.borrowed, end(p.providerDone, "provider")),
		diagnosticMicros(p.providerDone, end(p.headerDone, "header")),
		diagnosticMicros(p.headerDone, end(p.validated, "validate")),
		diagnosticMicros(p.validated, end(p.consumerDone, "consumer")),
		diagnosticMicros(p.consumerDone, end(time.Time{}, "release")), err)
}

// Stack-local, bounded progress only. No queue, pixels or per-frame log; an
// error retains its original Is/As chain and the provider's private cause stays
// hidden. Confirmed counts exclude writes whose final authorization failed:
// they must never be presented as an exact count of physical bytes sent.
type deliverySendProgress struct {
	phase, step                                              string
	started, snapshotStarted, snapshotFinished               time.Time
	budget, transactionTime, maxTransactionTime              time.Duration
	confirmedRecords, confirmedWire, confirmedPayload        uint64
	lastDelivery, delivery, origin, sourceEpoch, publication uint64
}

func (p deliverySendProgress) failure(err error) error {
	now := time.Now()
	snapshotEnd := p.snapshotFinished
	if p.phase == "snapshot" {
		snapshotEnd = now
	}
	return fmt.Errorf("delivery_send_progress phase=%s step=%s source_epoch=%d publication_epoch=%d last_delivery_id=%d attempted_delivery_id=%d attempted_origin_sequence=%d confirmed_records=%d confirmed_wire_bytes=%d confirmed_payload_bytes=%d elapsed_us=%d budget_us=%d snapshot_us=%d transaction_us=%d max_transaction_us=%d peer_cause=%s context_canceled=%t context_deadline=%t: %w",
		p.phase, p.step, p.sourceEpoch, p.publication, p.lastDelivery, p.delivery, p.origin,
		p.confirmedRecords, p.confirmedWire, p.confirmedPayload,
		diagnosticMicros(p.started, now), p.budget.Microseconds(),
		diagnosticMicros(p.snapshotStarted, snapshotEnd), p.transactionTime.Microseconds(), p.maxTransactionTime.Microseconds(),
		framePeerCause(err), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), err)
}
