package ingress

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
)

// SourceConnection is an authenticated, caller-owned native producer channel.
// Physical transport/process ownership and footprint remain with its launcher.
// The launcher must retain that ownership through Receive/WaitClosed, even if
// a faulty provider ignores cancellation. No transport ownership is duplicated.
type SourceConnection interface {
	CurrentGeneration() (string, error)
	Receive(context.Context, []byte) (int, peer.Principal, error)
}

// ErrSourceIdle means no header/record was consumed during a bounded wait.
// Camera warmup, pause or no input is not malformed framing or a restart cause.
var ErrSourceIdle = errors.New("source idle before frame header")

type sourceWorkspace struct{ packet, frame []byte }
type SourceReceiver struct {
	ingress                                          *Ingress
	connection                                       SourceConnection
	generation                                       string
	clock                                            fa.Clock
	maxReceive                                       time.Duration
	maxCopy                                          uint64
	digest                                           wire.PayloadSHA256
	workspace                                        *fd.Resource[sourceWorkspace]
	operation                                        sync.Mutex
	closed                                           atomic.Bool
	frames, outputs, packets, bytes, failures, drops atomic.Uint64
	peerFailures                                     atomic.Uint64
	localDeadlineFailures, localCancelFailures       atomic.Uint64
}

// Drops is the subset of Failures where a complete verified surface was
// rejected by store capacity/retention/copy budget, without losing source framing.
//
// PeerFailures, LocalDeadlineFailures and LocalCancelFailures split the total
// Failures by cause, so the epoch churn can be attributed instead of inferred
// from the failure text: "the source failed" and "the frame missed its deadline
// while waiting" retire the receiver identically but mean different things. The
// three need not add up to Failures; the remainder is unclassified.
type SourceReceiveStats struct {
	Frames, Outputs, Packets, Bytes, Failures, Drops uint64
	PeerFailures, LocalDeadlineFailures              uint64
	LocalCancelFailures                              uint64
}

// ReceiveFailureCause names why a frame receive ended. The values are stable
// and appear in the broker's failure line.
type ReceiveFailureCause string

const (
	ReceiveFailurePeer          ReceiveFailureCause = "peer"
	ReceiveFailureLocalDeadline ReceiveFailureCause = "local_deadline"
	ReceiveFailureLocalCancel   ReceiveFailureCause = "local_cancel"
	ReceiveFailureUnclassified  ReceiveFailureCause = "unclassified"
)

// ClassifyReceiveFailure reports why a frame receive ended. Our own context is
// checked first on purpose: a frame that missed the deadline while waiting is
// our budget expiring, even when the peer reports its own idle deadline at the
// same instant, and that is the case where a gentler policy would be arguable.
// A peer cause that is not our context is the source failing, which must keep
// retiring the receiver.
func ClassifyReceiveFailure(err error) ReceiveFailureCause {
	switch {
	case err == nil:
		return ReceiveFailureUnclassified
	case errors.Is(err, context.DeadlineExceeded):
		return ReceiveFailureLocalDeadline
	case errors.Is(err, context.Canceled):
		return ReceiveFailureLocalCancel
	case framePeerCause(err) != "unclassified":
		return ReceiveFailurePeer
	default:
		return ReceiveFailureUnclassified
	}
}

// Operation-local diagnostics only: no pixels, queue, clock authority or retry.
// Format on failure, not for each healthy frame; preserve the underlying error
// chain so idle/drop/retirement policy remains independent from observability.
type sourceReceiveProgress struct {
	sourceEpoch, originSequence                 uint64
	kind                                        uint8
	phase                                       string
	started, headerStart, headerEnd, payloadEnd time.Time
	integrityEnd                                time.Time
	recordsStart, bytesStart                    uint64
	payloadExpected, payloadReceived            uint64
	budget                                      time.Duration
}

func diagnosticMicros(begin, end time.Time) int64 {
	if begin.IsZero() || end.IsZero() || end.Before(begin) {
		return -1
	}
	return end.Sub(begin).Microseconds()
}

// Do not stringify the provider's arbitrary Cause: peer failures intentionally
// hide record contents and credentials. Classify only public sentinel values.
func framePeerCause(err error) string {
	for _, item := range [...]struct {
		err  error
		name string
	}{
		{peer.ErrDeadline, "deadline"}, {peer.ErrUnavailable, "unavailable"},
		{peer.ErrUnauthorized, "unauthorized"}, {peer.ErrRetired, "retired"},
		{peer.ErrInvalid, "invalid"}, {peer.ErrBusy, "busy"},
		{peer.ErrRecord, "record"}, {peer.ErrNoRecord, "no_record"},
		{context.DeadlineExceeded, "context_deadline"}, {context.Canceled, "context_canceled"},
	} {
		if errors.Is(err, item.err) {
			return item.name
		}
	}
	return "unclassified"
}

func (p sourceReceiveProgress) failure(err error, records, bytes uint64) error {
	now := time.Now()
	headerEnd, payloadEnd, integrityEnd := p.headerEnd, p.payloadEnd, p.integrityEnd
	if p.phase == "header" {
		headerEnd = now
	} else if p.phase == "payload" {
		payloadEnd = now
	} else if p.phase == "integrity" {
		integrityEnd = now
	}
	publication := int64(-1)
	if p.phase == "publication" {
		publication = diagnosticMicros(p.integrityEnd, now)
	}
	return fmt.Errorf("source_receive_progress phase=%s accepted_records=%d accepted_wire_bytes=%d assembled_payload_bytes=%d declared_payload_bytes=%d record_kind=%d record_source_epoch=%d record_origin_sequence=%d elapsed_us=%d header_us=%d assembly_us=%d integrity_us=%d publication_us=%d budget_us=%d peer_cause=%s peer_no_record=%t context_canceled=%t context_deadline=%t: %w",
		p.phase, records-p.recordsStart, bytes-p.bytesStart, p.payloadReceived, p.payloadExpected,
		p.kind, p.sourceEpoch, p.originSequence,
		diagnosticMicros(p.started, now), diagnosticMicros(p.headerStart, headerEnd),
		diagnosticMicros(p.headerEnd, payloadEnd), diagnosticMicros(p.payloadEnd, integrityEnd),
		publication, p.budget.Microseconds(), framePeerCause(err),
		errors.Is(err, peer.ErrNoRecord), errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded), err)
}

// NewSourceReceiver admits packet + full surface storage BEFORE allocation in
// the binding's SAME global/per-source ledger. Clock/generation/budgets are
// trusted composition inputs, never worker/RPC-selected. The clock must be the
// broker's admitted monotonic clock. digest and digestWorkspaceBytes are the
// trusted provider and its per-call workspace quote, never RPC options. Native
// capture, SDK and launch transport memory remain the source owner's quote.
func NewSourceReceiver(ctx context.Context, ingress *Ingress, connection SourceConnection, generation string, clock fa.Clock, maxReceiveNS, maxCopyNS uint64, digest wire.PayloadSHA256, digestWorkspaceBytes int) (*SourceReceiver, error) {
	if ctx == nil || ingress == nil || connection == nil || !controlToken(generation) || clock == nil || digest == nil || digestWorkspaceBytes < 0 || digestWorkspaceBytes > 1<<20 || maxReceiveNS == 0 || maxReceiveNS > 5_000_000_000 || maxCopyNS == 0 || maxCopyNS > 5_000_000_000 {
		return nil, fail("source_receiver_contract", peer.ErrInvalid)
	}
	if err := ingress.binding.ValidateCurrent(); err != nil {
		return nil, err
	}
	actual, err := connection.CurrentGeneration()
	if err != nil || actual != generation {
		return nil, fail("source_receiver_sender", peer.ErrUnauthorized)
	}
	maxPayload := ingress.surfaceBytes
	for _, d := range ingress.deliveries {
		maxPayload = max(maxPayload, int(d.MaxPayload))
	}
	frameBytes := wire.HeaderSize + maxPayload
	workspace, err := fd.OwnResource(ctx, ingress.binding, fd.ResourcePlan[sourceWorkspace]{Bytes: wire.MaxDataPacketBytes + frameBytes + digestWorkspaceBytes,
		Create: func(ctx context.Context) (fd.Created[sourceWorkspace], error) {
			if err := ctx.Err(); err != nil {
				return fd.Created[sourceWorkspace]{}, err
			}
			return fd.Created[sourceWorkspace]{Value: sourceWorkspace{packet: make([]byte, wire.MaxDataPacketBytes), frame: make([]byte, frameBytes)}, Owned: true}, nil
		}, Release: func(sourceWorkspace) error { return nil },
	})
	if err != nil {
		return nil, fail("source_receiver_workspace", err)
	}
	return &SourceReceiver{ingress: ingress, connection: connection, generation: generation, clock: clock, maxReceive: time.Duration(maxReceiveNS), maxCopy: maxCopyNS, workspace: workspace, digest: digest}, nil
}
func (r *SourceReceiver) Close() {
	if r != nil && !r.closed.Swap(true) {
		r.workspace.Close()
	}
}
func (r *SourceReceiver) WaitClosed(ctx context.Context) error {
	if r == nil {
		return peer.ErrInvalid
	}
	return r.workspace.WaitClosed(ctx)
}
func (r *SourceReceiver) Stats() SourceReceiveStats {
	return SourceReceiveStats{
		Frames: r.frames.Load(), Outputs: r.outputs.Load(), Packets: r.packets.Load(),
		Bytes: r.bytes.Load(), Failures: r.failures.Load(), Drops: r.drops.Load(),
		PeerFailures: r.peerFailures.Load(), LocalDeadlineFailures: r.localDeadlineFailures.Load(),
		LocalCancelFailures: r.localCancelFailures.Load(),
	}
}

// Receive consumes exactly one VBCF header and its sequential FAPD chunks.
// ONE deadline covers header, all reads, verification and publication. No frame
// or partial payload escapes. Framing/identity/I/O failures retire the receiver:
// no implicit resync or camera restart. Only a FULLY consumed, verified surface
// rejected for store capacity/retention/copy budget may be dropped preserving the
// channel. No retry/queue: the next call starts the next frame. The owner decides
// recovery/idle policy. Concurrent calls are rejected, not queued.
func (r *SourceReceiver) Receive(ctx context.Context, consume OutputConsumer) (err error) {
	if r == nil || ctx == nil {
		return fail("source_receive_contract", peer.ErrInvalid)
	}
	if r.closed.Load() {
		return fail("source_receive_closed", peer.ErrRetired)
	}
	if !r.operation.TryLock() {
		return fail("source_receive_busy", peer.ErrBusy)
	}
	defer r.operation.Unlock()
	if r.closed.Load() {
		return fail("source_receive_closed", peer.ErrRetired)
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, r.maxReceive)
	defer cancel()
	progress := sourceReceiveProgress{phase: "workspace", started: time.Now(),
		recordsStart: r.packets.Load(), bytesStart: r.bytes.Load()}
	if deadline, ok := ctx.Deadline(); ok {
		progress.budget = max(0, deadline.Sub(progress.started))
	}
	complete := false
	isOutput := false
	defer func() {
		if errors.Is(err, ErrSourceIdle) {
			return
		}
		if err != nil {
			err = progress.failure(err, r.packets.Load(), r.bytes.Load())
			r.failures.Add(1)
			switch ClassifyReceiveFailure(err) {
			case ReceiveFailurePeer:
				r.peerFailures.Add(1)
			case ReceiveFailureLocalDeadline:
				r.localDeadlineFailures.Add(1)
			case ReceiveFailureLocalCancel:
				r.localCancelFailures.Add(1)
			}
			var failure *Failure
			hasStage := errors.As(err, &failure)
			storeRejection := !isOutput && hasStage && failure.Stage == "surface_store"
			// A complete verified surface can miss its optional copy budget before
			// admission. It is unavailable, not a broken transport. Never swallow
			// a receive/consumer deadline, clock error, retirement or partial frame.
			copyBudgetExpired := !isOutput && hasStage && failure.Stage == "surface_store" && errors.Is(err, fa.DeadlineExceeded)
			if complete && storeRejection && ctx.Err() == nil && (errors.Is(err, fa.CapacityRejected) || errors.Is(err, fa.Expired) || copyBudgetExpired) && r.current() == nil && ctx.Err() == nil {
				r.drops.Add(1)
				return
			}
			r.Close()
		}
	}()
	// The resource borrow follows the caller lifetime. Its cancellation still
	// interrupts the bounded frame read, but a frame-local idle timeout must not
	// be replaced by a generic borrow timeout after the provider proved no read.
	err = r.workspace.Borrow(parent, func(resourceCtx context.Context, w sourceWorkspace) error {
		stop := context.AfterFunc(resourceCtx, cancel)
		defer stop()
		progress.phase, progress.headerStart = "header", time.Now()
		n, err := r.read(ctx, w.packet)
		progress.headerEnd = time.Now()
		if err != nil {
			if errors.Is(err, peer.ErrNoRecord) && parent.Err() == nil && r.current() == nil {
				return ErrSourceIdle
			}
			return err
		}
		h, err := wire.ParseHeader(w.packet[:n])
		if err != nil {
			return fail("source_header", err)
		}
		progress.payloadExpected = uint64(h.PayloadBytes)
		progress.kind, progress.sourceEpoch, progress.originSequence = h.Kind, h.SourceEpoch, h.OriginSequence
		isOutput = h.Kind == wire.SourceOutput
		if isOutput {
			if consume == nil {
				return fail("output_consumer", fa.InvalidContract)
			}
			err = r.ingress.validateOutputHeader(h)
		} else {
			err = r.ingress.validateSurfaceHeader(h)
		}
		if err != nil {
			return err
		}
		observed := r.clock.NowNS()
		assembly, err := wire.NewSourceAssembly(w.packet[:n], w.frame[:wire.HeaderSize+int(h.PayloadBytes)], r.digest)
		if err != nil {
			return fail("source_assembly", err)
		}
		defer assembly.Abort()
		progress.phase = "payload"
		for !assembly.Complete() {
			n, err = r.read(ctx, w.packet)
			if err != nil {
				return err
			}
			if err = assembly.Append(w.packet[:n]); err != nil {
				return fail("source_chunk", err)
			}
			progress.payloadReceived += uint64(n - wire.DataHeaderSize)
		}
		progress.phase, progress.payloadEnd = "integrity", time.Now()
		h, packet, err := assembly.Finish()
		progress.integrityEnd = time.Now()
		if err != nil {
			return fail("source_integrity", err)
		}
		complete = true
		if err = ctx.Err(); err != nil {
			return fail("source_publish_deadline", err)
		}
		if err = r.current(); err != nil {
			return err
		}
		progress.phase = "publication"
		if isOutput {
			return r.ingress.acceptVerifiedOutput(ctx, h, packet[wire.HeaderSize:], consume)
		}
		now := r.clock.NowNS()
		if now < observed || now > ^uint64(0)-r.maxCopy {
			return fail("source_clock", fa.DeadlineExceeded)
		}
		// Finish verified the final pixels. Avoid a second full-frame SHA pass.
		return r.ingress.acceptVerifiedSurface(ctx, h, packet[wire.HeaderSize:], observed, now+r.maxCopy)
	})
	// The borrow uses the parent's lifetime for proven idle reads. A completed
	// frame still owes its own deadline after publication and borrow finalization.
	if err == nil {
		err = fail("source_publish_deadline", ctx.Err())
	}
	if err == nil {
		if isOutput {
			r.outputs.Add(1)
		} else {
			r.frames.Add(1)
		}
	}
	return err
}
func (r *SourceReceiver) current() error {
	if r.closed.Load() {
		return fail("source_receive_closed", peer.ErrRetired)
	}
	if err := r.ingress.binding.ValidateCurrent(); err != nil {
		return err
	}
	generation, err := r.connection.CurrentGeneration()
	if err != nil || generation != r.generation {
		return fail("source_sender_changed", peer.ErrRetired)
	}
	return nil
}
func (r *SourceReceiver) read(ctx context.Context, p []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, fail("source_receive_deadline", err)
	}
	if err := r.current(); err != nil {
		return 0, err
	}
	n, principal, err := r.connection.Receive(ctx, p)
	if err != nil {
		return 0, fail("source_receive", err)
	}
	if n < 1 || n > len(p) || principal.Generation != r.generation {
		return 0, fail("source_principal", peer.ErrUnauthorized)
	}
	if err = ctx.Err(); err != nil {
		return 0, fail("source_receive_deadline", err)
	}
	if err = r.current(); err != nil {
		return 0, err
	}
	r.packets.Add(1)
	r.bytes.Add(uint64(n))
	return n, nil
}
