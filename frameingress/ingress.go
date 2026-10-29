// Package ingress connects authenticated causal media to retained native frames.
// It neither authenticates peers nor selects a platform or a software fallback.
package ingress

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	wire "github.com/Josepavese/videosluice/framewire"
)

// Failure exposes a bounded diagnostic stage, not frame bytes or credentials.
// Callers can aggregate Stage and errors.Is(Cause) without per-frame log floods.
type Failure struct {
	Stage string
	Cause error
}

func (e *Failure) Error() string { return fmt.Sprintf("frame ingress stage=%s: %v", e.Stage, e.Cause) }
func (e *Failure) Unwrap() error { return e.Cause }
func fail(stage string, err error) error {
	if err == nil {
		return nil
	}
	return &Failure{stage, err}
}

// A recoverable sentinel must be the sole cause. Peel diagnostic wrappers,
// but retain joined causes as compound failures rather than matching one leaf.
func singleCause(err error) error {
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return err
		}
		err = next
	}
}

// DeliveryLayout is an immutable, admitted source-to-delivery transform. A
// native surface has its own independent descriptor, never the analysis hash.
type DeliveryLayout struct {
	Codec             uint8
	StrideY, StrideUV uint32
	Transform         fa.Transform
	MaxPayload        uint32
}
type Ingress struct {
	binding       *fd.Binding
	scope         fa.Scope
	fence         fd.Fence
	surface       uint8
	surfaceDigest fa.Digest
	surfaceBytes  int
	deliveries    map[string]DeliveryLayout
	outputMu      sync.Mutex
	outputHigh    map[string]uint64
}

func New(b *fd.Binding, surfaceDigest fa.Digest, layouts map[string]DeliveryLayout) (*Ingress, error) {
	if b == nil || surfaceDigest == (fa.Digest{}) {
		return nil, fail("admission", fa.InvalidContract)
	}
	s := b.Scope()
	if len(layouts) == 0 || len(layouts) > 16 || len(layouts) != len(s.Streams) {
		return nil, fail("admission", fa.InvalidContract)
	}
	n, err := s.Geometry.Bytes()
	if err != nil {
		return nil, fail("admission", err)
	}
	i := &Ingress{binding: b, scope: s, fence: b.Fence(), surfaceDigest: surfaceDigest, surfaceBytes: n, deliveries: make(map[string]DeliveryLayout, len(layouts)), outputHigh: make(map[string]uint64, len(layouts))}
	switch s.Surface {
	case "processed_full":
		i.surface = wire.ProcessedFull
	case "source_decoded":
		i.surface = wire.SourceDecoded
	default:
		return nil, fail("admission", fa.InvalidContract)
	}
	for name, d := range layouts {
		if _, ok := s.Streams[name]; !ok || d.Transform.Validate() != nil || d.Transform.Full != s.Geometry || d.MaxPayload == 0 || d.MaxPayload > wire.MaxPayload {
			return nil, fail("admission", fa.InvalidContract)
		}
		switch d.Codec {
		case wire.NV12:
			if (d.StrideY|d.StrideUV)&1 != 0 || d.StrideY < uint32(d.Transform.DeliveryWidth) || d.StrideUV < uint32(d.Transform.DeliveryWidth) || uint64(d.StrideY)*uint64(d.Transform.DeliveryHeight)+uint64(d.StrideUV)*uint64(d.Transform.DeliveryHeight/2) != uint64(d.MaxPayload) {
				return nil, fail("admission", fa.InvalidContract)
			}
		case wire.H264, wire.H265:
			if d.StrideY != 0 || d.StrideUV != 0 {
				return nil, fail("admission", fa.InvalidContract)
			}
		default:
			return nil, fail("admission", fa.InvalidContract)
		}
		i.deliveries[name] = d
		i.outputHigh[name] = 0
	}
	return i, nil
}

// OutputConsumer borrows immutable pixels only during this synchronous call.
// Header is a broker-issued Delivery, not the native worker's output counter.
// Consumers must preserve this exact header with its pixels; no local renaming.
// Implementations obey ctx and may not retain or enqueue the borrowed storage.
type OutputConsumer func(context.Context, wire.Header, []byte) error

func (i *Ingress) validateOutputHeader(h wire.Header) error {
	if err := i.validateIdentity(h); err != nil {
		return err
	}
	d, ok := i.deliveries[h.Stream]
	if !ok || h.Kind != wire.SourceOutput || h.Codec != d.Codec || h.PayloadBytes > d.MaxPayload || h.Width != uint32(d.Transform.DeliveryWidth) || h.Height != uint32(d.Transform.DeliveryHeight) || h.StrideY != d.StrideY || h.StrideUV != d.StrideUV || h.Transform != d.Transform.Digest() {
		return fail("output_layout", fa.TransformMismatch)
	}
	return nil
}

// AcceptOutput is the authenticated worker ingress, NOT a public publishing
// API. It validates complete bytes before consuming the per-stream replay
// fence or minting any delivery. No per-frame request/ACK, queue or pixel copy.
func (i *Ingress) AcceptOutput(ctx context.Context, packet []byte, consume OutputConsumer) error {
	if ctx == nil || consume == nil {
		return fail("output_consumer", fa.InvalidContract)
	}
	h, payload, err := i.parse(ctx, packet, wire.MaxPayload)
	if err != nil {
		return err
	}
	return i.acceptVerifiedOutput(ctx, h, payload, consume)
}

func (i *Ingress) acceptVerifiedOutput(ctx context.Context, h wire.Header, payload []byte, consume OutputConsumer) (result error) {
	return i.acceptVerifiedOutputWithPolicy(ctx, h, payload, consume, nil)
}

// The snapshot receiver may discard a verified, strictly older mailbox slot.
// Classification and the replay fence must share the same lock: a second
// ingress path can otherwise advance the fence between check and commit.
func (i *Ingress) acceptVerifiedSnapshotOutput(ctx context.Context, h wire.Header, payload []byte, consume OutputConsumer) (bool, error) {
	superseded := false
	err := i.acceptVerifiedOutputWithPolicy(ctx, h, payload, consume, &superseded)
	return superseded, err
}

func (i *Ingress) acceptVerifiedOutputWithPolicy(ctx context.Context, h wire.Header, payload []byte, consume OutputConsumer, superseded *bool) (result error) {
	started := time.Now()
	phase := "validation"
	outputSequence := h.OutputSequence
	previousHigh := uint64(0)
	ticketUS, bindingUS, beforeUS, consumerUS, afterUS := int64(-1), int64(-1), int64(-1), int64(-1), int64(-1)
	defer func() {
		if result == nil {
			return
		}
		budgetUS := int64(-1)
		if deadline, ok := ctx.Deadline(); ok {
			budgetUS = deadline.Sub(started).Microseconds()
		}
		result = fmt.Errorf("output_commit_progress phase=%s source_epoch=%d origin_sequence=%d output_sequence=%d previous_high=%d ticket_us=%d binding_us=%d validate_before_us=%d consumer_us=%d validate_after_us=%d elapsed_us=%d budget_us=%d: %w",
			phase, h.SourceEpoch, h.OriginSequence, outputSequence, previousHigh, ticketUS, bindingUS, beforeUS, consumerUS, afterUS, time.Since(started).Microseconds(), budgetUS, result)
	}()
	if consume == nil {
		return fail("output_consumer", fa.InvalidContract)
	}
	if err := i.validateOutputHeader(h); err != nil {
		return err
	}
	if !i.outputMu.TryLock() {
		return fail("output_concurrent", fa.CapacityRejected)
	}
	defer i.outputMu.Unlock()
	previousHigh = i.outputHigh[h.Stream]
	if superseded != nil && h.OutputSequence < previousHigh {
		*superseded = true
		return nil
	}
	if h.OutputSequence <= previousHigh {
		return fail("output_replay", fa.InvalidContract)
	}
	if err := ctx.Err(); err != nil {
		return fail("output_deadline", err)
	}
	phase = "ticket"
	stageStarted := time.Now()
	ticket, err := i.binding.AllocateDelivery(h.Stream)
	ticketUS = time.Since(stageStarted).Microseconds()
	if err != nil {
		return fail("output_ticket", err)
	}
	// A fully verified output is consumed even when retention has expired or
	// the downstream send fails. Neither condition authorizes a replay/retry.
	i.outputHigh[h.Stream] = h.OutputSequence
	phase = "binding"
	stageStarted = time.Now()
	if h.Correlation == 0 {
		err = i.binding.BindUnavailable(ctx, ticket)
	} else {
		err = i.binding.BindExact(ctx, i.origin(h), ticket, i.deliveries[h.Stream].Transform)
	}
	bindingUS = time.Since(stageStarted).Microseconds()
	if err != nil {
		return fail("output_binding", err)
	}
	if err = ctx.Err(); err != nil {
		return fail("output_deadline", err)
	}
	phase = "validate_before"
	stageStarted = time.Now()
	err = i.binding.ValidateCurrent()
	beforeUS = time.Since(stageStarted).Microseconds()
	if err != nil {
		return fail("output_retired", err)
	}
	h.Kind, h.OutputSequence = wire.Delivery, 0
	h.PublicationEpoch, h.DeliveryID = ticket.Ref().Ref.Epoch, ticket.Ref().Ref.FrameID
	phase = "consumer"
	stageStarted = time.Now()
	err = consume(ctx, h, payload)
	consumerUS = time.Since(stageStarted).Microseconds()
	if err != nil {
		return fail("output_consumer", err)
	}
	// A consumer may return nil after waiting past its budget or retirement.
	// Its side effects cannot be retried, but they must not count as a timely,
	// current publication. Keep a real consumer failure above as the first cause.
	if err = ctx.Err(); err != nil {
		return fail("output_consumer_deadline", err)
	}
	phase = "validate_after"
	stageStarted = time.Now()
	err = i.binding.ValidateCurrent()
	afterUS = time.Since(stageStarted).Microseconds()
	if err != nil {
		return fail("output_consumer_retired", err)
	}
	return fail("output_consumer_deadline", ctx.Err())
}

func (i *Ingress) parse(ctx context.Context, packet []byte, max int) (wire.Header, []byte, error) {
	if ctx == nil {
		return wire.Header{}, nil, fail("context", fa.InvalidContract)
	}
	if err := ctx.Err(); err != nil {
		return wire.Header{}, nil, fail("context", err)
	}
	if len(packet) > wire.HeaderSize+max {
		return wire.Header{}, nil, fail("payload_limit", fa.CapacityRejected)
	}
	h, p, err := wire.Parse(packet)
	if err != nil {
		return h, nil, fail("envelope", err)
	}
	return h, p, i.validateIdentity(h)
}
func (i *Ingress) validateIdentity(h wire.Header) error {
	if h.Namespace != i.fence.Namespace || h.Process != i.fence.ProcessGeneration {
		return fail("domain", fd.ErrDomainMismatch)
	}
	if h.Source != i.scope.Source {
		return fail("source", fa.SourceMismatch)
	}
	if h.SourceEpoch != i.scope.MediaEpoch {
		return fail("source_epoch", fa.EpochMismatch)
	}
	if h.Graph != i.scope.Graph {
		return fail("graph", fa.GraphMismatch)
	}
	return nil
}
func (i *Ingress) origin(h wire.Header) fd.OriginRef {
	// Preserve the validated on-wire identity. No current counter or nearest PTS.
	return fd.OriginRef{Fence: fd.Fence{Namespace: h.Namespace, ProcessGeneration: h.Process}, Ref: fa.OriginRef{Source: h.Source, MediaEpoch: h.SourceEpoch, Sequence: h.OriginSequence, Graph: h.Graph}}
}

// AcceptSurface borrows an immutable detached packet only for this call. The
// caller charges snapshot/input memory separately and releases driver leases
// before entry. observedNS is trusted monotonic ingress age, not worker PTS.
func (i *Ingress) AcceptSurface(ctx context.Context, packet []byte, observedNS, deadlineNS uint64) error {
	h, p, err := i.parse(ctx, packet, i.surfaceBytes)
	if err != nil {
		return err
	}
	return i.acceptVerifiedSurface(ctx, h, p, observedNS, deadlineNS)
}
func (i *Ingress) validateSurfaceHeader(h wire.Header) error {
	if err := i.validateIdentity(h); err != nil {
		return err
	}
	g := i.scope.Geometry
	if h.Kind != wire.Surface || h.Surface != i.surface || h.Codec != wire.NV12 || h.Width != uint32(g.Width) || h.Height != uint32(g.Height) || h.StrideY != uint32(g.Stride) || h.StrideUV != uint32(g.Stride) || int(h.PayloadBytes) != i.surfaceBytes {
		return fail("surface_layout", fa.InvalidContract)
	}
	if h.Transform != i.surfaceDigest {
		return fail("surface_transform", fa.TransformMismatch)
	}
	return nil
}

// Only validated wire parsing/assembly inside this package may call this path.
// Keep it private: accepting caller-fabricated parsed headers would bypass SHA.
func (i *Ingress) acceptVerifiedSurface(ctx context.Context, h wire.Header, p []byte, observedNS, deadlineNS uint64) error {
	if err := i.validateSurfaceHeader(h); err != nil {
		return err
	}
	return fail("surface_store", i.binding.PublishSurface(ctx, i.origin(h), p, observedNS, deadlineNS))
}

// SourceDescriptor exports ONLY this live admitted binding's immutable source
// layout. It is sent over the authenticated worker bootstrap, never treated as
// a frame or accepted from an RPC caller as native hardware admission.
func (i *Ingress) SourceDescriptor() ([wire.HeaderSize]byte, error) {
	if i == nil {
		return [wire.HeaderSize]byte{}, fa.InvalidContract
	}
	if err := i.binding.ValidateCurrent(); err != nil {
		return [wire.HeaderSize]byte{}, err
	}
	g := i.scope.Geometry
	return wire.MarshalHeader(wire.Header{Kind: wire.SourceDefinition, Codec: wire.NV12, Surface: i.surface,
		Source: i.scope.Source, SourceEpoch: i.scope.MediaEpoch, Namespace: i.fence.Namespace,
		Process: i.fence.ProcessGeneration, Graph: i.scope.Graph, Transform: i.surfaceDigest,
		Width: uint32(g.Width), Height: uint32(g.Height), StrideY: uint32(g.Stride), StrideUV: uint32(g.Stride), PayloadSHA: sha256.Sum256(nil)})
}

// AcceptDelivery accepts ONLY a ticket issued by the broker before the worker
// encodes this envelope. The validated exact origin consumes the ticket once,
// even if optional native pixels are missing. Only extraction returns Expired;
// late pixels never authorize replaying or changing this delivery's identity.
// There is no implicit pending queue, origin fabrication or ticket allocation.
func (i *Ingress) AcceptDelivery(ctx context.Context, ticket fd.Ticket, packet []byte) error {
	r := ticket.Ref()
	d, ok := i.deliveries[r.Ref.Stream]
	if !ok {
		return fail("ticket", fa.SourceMismatch)
	}
	h, _, err := i.parse(ctx, packet, int(d.MaxPayload))
	if err != nil {
		return err
	}
	if h.Kind != wire.Delivery || h.Stream != r.Ref.Stream || h.PublicationEpoch != r.Ref.Epoch || h.DeliveryID != r.Ref.FrameID || h.Graph != r.Ref.Graph || r.Fence != i.fence {
		return fail("ticket", fd.ErrDomainMismatch)
	}
	if h.Codec != d.Codec || h.Width != uint32(d.Transform.DeliveryWidth) || h.Height != uint32(d.Transform.DeliveryHeight) || h.StrideY != d.StrideY || h.StrideUV != d.StrideUV {
		return fail("delivery_layout", fa.InvalidContract)
	}
	if h.Transform != d.Transform.Digest() {
		return fail("delivery_transform", fa.TransformMismatch)
	}
	if h.Correlation == 0 {
		return fail("binding", i.binding.BindUnavailable(ctx, ticket))
	}
	return fail("binding", i.binding.BindExact(ctx, i.origin(h), ticket, d.Transform))
}
