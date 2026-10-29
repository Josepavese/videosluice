package ingress

import (
	"context"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/framewire/surfaceport"
)

var ErrDeliveryIdle = errors.New("no new delivery in this publication")

// DeliveryPublication retains only the latest REAL delivered frame, not a
// re-created resize of the source and not a camera buffer. Its mapping and two
// workspaces belong to the source's existing global resource ledger. Kernel,
// descriptor and transport limits remain part of platform admission, not RSS.
type DeliveryPublication struct {
	binding     *fd.Binding
	layout      DeliveryLayout
	stream      string
	scope       fa.Scope
	fence       fd.Fence
	digest      fa.Digest
	copy        fa.CopyAdmission
	resource    *fd.Resource[*deliveryMemory]
	write, read sync.Mutex
	metadataMu  sync.Mutex // fixed header copy only; never pixel or client I/O
	last        atomic.Uint64
}

type deliveryMemory struct {
	allocation      surfaceport.Allocation
	write, read     []byte
	committedHeader [wire.HeaderSize]byte
	committedAt     time.Time
}

// DeliveryMetadata describes an actually committed publication, not a camera
// timestamp estimate. Decimal strings preserve exact counters in JSON clients.
type DeliveryMetadata struct {
	Namespace        string    `json:"namespace"`
	Process          string    `json:"process_generation"`
	Source           string    `json:"source"`
	Stream           string    `json:"stream"`
	SourceEpoch      uint64    `json:"source_epoch,string"`
	PublicationEpoch uint64    `json:"publication_epoch,string"`
	DeliveryID       uint64    `json:"delivery_id,string"`
	OriginSequence   uint64    `json:"origin_sequence,string"`
	Graph            string    `json:"graph_digest"`
	Transform        string    `json:"transform_digest"`
	Width            uint32    `json:"width"`
	Height           uint32    `json:"height"`
	CommittedAt      time.Time `json:"committed_at"`
}

// MetadataStatus is the optional current frame-plane projection on the daemon
// status endpoint. It contains no pixels, credentials or consumer authority.
type MetadataStatus struct {
	Schema  string                                 `json:"schema"`
	Sources map[string]map[string]DeliveryMetadata `json:"sources"`
}

// NewDeliveryPublication is trusted composition. It never obtains authority
// from a packet and does not expose a mapped pointer or descriptor to a client.
// A nonnil result with error still owns cleanup obligations.
func NewDeliveryPublication(ctx context.Context, binding *fd.Binding, stream string,
	layout DeliveryLayout, allocator surfaceport.Allocator) (*DeliveryPublication, error) {
	if ctx == nil || binding == nil || allocator == nil || !controlToken(stream) ||
		layout.MaxPayload == 0 || layout.MaxPayload > wire.MaxPayload || layout.Codec != wire.NV12 ||
		layout.Transform.Validate() != nil || layout.StrideY != uint32(layout.Transform.DeliveryWidth) ||
		layout.StrideUV != layout.StrideY || layout.MaxPayload != uint32(layout.Transform.DeliveryWidth*layout.Transform.DeliveryHeight*3/2) {
		return nil, peer.ErrInvalid
	}
	if err := binding.ValidateCurrent(); err != nil {
		return nil, err
	}
	if binding.Scope().Streams[stream] == 0 {
		return nil, peer.ErrUnauthorized
	}
	admission, err := binding.CopyAdmission()
	if err != nil {
		return nil, err
	}
	serial, err := wire.NextMappingGeneration()
	if err != nil {
		return nil, err
	}
	capacity := wire.HeaderSize + int(layout.MaxPayload)
	mapping := wire.Mapping{Namespace: binding.Fence().Namespace, Generation: serial, Capacity: uint64(capacity)}
	footprint, err := allocator.Footprint(mapping)
	if err != nil {
		return nil, err
	}
	if footprint == 0 || footprint > 1<<28 {
		return nil, fa.CapacityRejected
	}
	p := &DeliveryPublication{binding: binding, stream: stream, layout: layout,
		scope: binding.Scope(), fence: binding.Fence(), digest: layout.Transform.Digest(), copy: admission}
	p.resource, err = fd.OwnResource(ctx, binding, fd.ResourcePlan[*deliveryMemory]{Bytes: int(footprint) + 2*capacity + 512,
		Create: func(ctx context.Context) (fd.Created[*deliveryMemory], error) {
			if err := ctx.Err(); err != nil {
				return fd.Created[*deliveryMemory]{}, err
			}
			allocation, err := allocator.Create(mapping)
			if allocation == nil {
				if err == nil {
					err = surfaceport.ErrUnavailable
				}
				return fd.Created[*deliveryMemory]{}, err
			}
			memory := &deliveryMemory{allocation: allocation}
			if err == nil {
				memory.write, memory.read = make([]byte, capacity), make([]byte, capacity)
			}
			return fd.Created[*deliveryMemory]{Value: memory, Owned: true}, err
		},
		Release: func(memory *deliveryMemory) error { return memory.allocation.Close() },
	})
	return p, err
}

func (p *DeliveryPublication) validate(h wire.Header) error {
	if err := p.binding.ValidateCurrent(); err != nil {
		return err
	}
	scope, fence, l := p.scope, p.fence, p.layout
	if h.Kind != wire.Delivery || h.Namespace != fence.Namespace || h.Process != fence.ProcessGeneration ||
		h.Source != scope.Source || h.SourceEpoch != scope.MediaEpoch || h.Graph != scope.Graph ||
		h.Stream != p.stream || h.PublicationEpoch != scope.Streams[p.stream] ||
		h.Codec != l.Codec || h.PayloadBytes != l.MaxPayload || h.Width != uint32(l.Transform.DeliveryWidth) ||
		h.Height != uint32(l.Transform.DeliveryHeight) || h.StrideY != l.StrideY || h.StrideUV != l.StrideUV || h.Transform != p.digest {
		return fail("delivery_publication_scope", peer.ErrUnauthorized)
	}
	return nil
}

func (p *DeliveryPublication) Publish(ctx context.Context, h wire.Header, pixels []byte) error {
	if p == nil || p.resource == nil || ctx == nil || len(pixels) != int(h.PayloadBytes) {
		return peer.ErrInvalid
	}
	if err := p.validate(h); err != nil {
		return err
	}
	header, err := wire.MarshalHeader(h)
	if err != nil {
		return err
	}
	if !p.write.TryLock() {
		return fa.CapacityRejected
	}
	defer p.write.Unlock()
	if h.DeliveryID <= p.last.Load() {
		return fail("delivery_publication_replay", peer.ErrInvalid)
	}
	return p.resource.Borrow(ctx, func(ctx context.Context, memory *deliveryMemory) error {
		copy(memory.write, header[:])
		copy(memory.write[wire.HeaderSize:], pixels)
		// Native atomic publisher validates the complete payload hash, then
		// commits; no borrowed ingress memory survives this synchronous call.
		if err := memory.allocation.Publish(ctx, memory.write); err != nil {
			return err
		}
		p.metadataMu.Lock()
		copy(memory.committedHeader[:], header[:])
		memory.committedAt = time.Now().UTC()
		p.metadataMu.Unlock()
		p.last.Store(h.DeliveryID)
		return nil
	})
}

// Metadata reads only the existing committed header under the publisher's
// bounded ownership. It never snapshots pixel data or holds a driver lease.
func (p *DeliveryPublication) Metadata(ctx context.Context) (result DeliveryMetadata, err error) {
	if p == nil || p.resource == nil || ctx == nil {
		return result, peer.ErrInvalid
	}
	err = p.resource.Borrow(ctx, func(ctx context.Context, memory *deliveryMemory) error {
		p.metadataMu.Lock()
		header, committedAt := memory.committedHeader, memory.committedAt
		p.metadataMu.Unlock()
		if committedAt.IsZero() {
			return ErrDeliveryIdle
		}
		h, parseErr := wire.ParseHeader(header[:])
		if parseErr != nil {
			return parseErr
		}
		if err := p.validate(h); err != nil {
			return err
		}
		result = DeliveryMetadata{Namespace: hex.EncodeToString(h.Namespace[:]), Process: h.Process,
			Source: h.Source, Stream: h.Stream, SourceEpoch: h.SourceEpoch,
			PublicationEpoch: h.PublicationEpoch, DeliveryID: h.DeliveryID, OriginSequence: h.OriginSequence,
			Graph: hex.EncodeToString(h.Graph[:]), Transform: hex.EncodeToString(h.Transform[:]),
			Width: h.Width, Height: h.Height, CommittedAt: committedAt}
		return nil
	})
	return result, err
}

// Snapshot serializes one consumer operation, while the producer keeps
// publishing independently through the native atomic mapping. The callback
// borrows immutable bytes only during this call; no unbounded copy/queue.
func (p *DeliveryPublication) Snapshot(ctx context.Context, last uint64, consume func(context.Context, wire.Header, []byte) error) (err error) {
	if p == nil || p.resource == nil || ctx == nil || consume == nil {
		return peer.ErrInvalid
	}
	progress := deliverySnapshotProgress{phase: "authority", started: time.Now()}
	defer func() {
		if err != nil && err != ErrDeliveryIdle {
			err = progress.failure(err)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.binding.ValidateCurrent(); err != nil {
		return err
	}
	progress.authorityDone = time.Now()
	if err := ctx.Err(); err != nil {
		return err
	}
	latest := p.last.Load()
	if last > latest {
		return fail("delivery_snapshot_future_cursor", peer.ErrInvalid)
	}
	if last == latest {
		return ErrDeliveryIdle
	}
	if !p.read.TryLock() {
		return fa.CapacityRejected
	}
	defer p.read.Unlock()
	progress.phase, progress.borrowStarted = "resource", time.Now()
	idle := false
	err = p.resource.Borrow(ctx, func(ctx context.Context, memory *deliveryMemory) error {
		progress.phase, progress.borrowed = "provider", time.Now()
		used, err := memory.allocation.SnapshotInto(ctx, memory.read)
		progress.providerDone = time.Now()
		if singleCause(err) == wire.ErrBusy {
			// The producer may advance its atomic mapping during this bounded
			// copy. No pixels reached the callback: answer with the existing
			// no-delivery record instead of retiring a healthy reader binding.
			// Do not retry/spin, reuse partial bytes, or mask consumer/write errors.
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := p.binding.ValidateCurrent(); err != nil {
				return err
			}
			idle = true
			return nil
		}
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		progress.phase = "header"
		if used != len(memory.read) {
			return fail("delivery_snapshot_extent", peer.ErrRecord)
		}
		header, err := wire.ParseHeader(memory.read[:wire.HeaderSize])
		if err != nil {
			return err
		}
		progress.phase, progress.headerDone = "validate", time.Now()
		if err = p.validate(header); err != nil {
			return err
		}
		progress.validated = time.Now()
		// ValidateCurrent fences process/source identity but cannot cancel a
		// provider already inside its check. Never expose a late snapshot to
		// consumer side effects and rely on Borrow's final check to undo them.
		if err := ctx.Err(); err != nil {
			return err
		}
		if header.DeliveryID <= last {
			idle = true
			return nil
		}
		progress.phase = "consumer"
		err = consume(ctx, header, memory.read[wire.HeaderSize:used:used])
		progress.consumerDone = time.Now()
		if err == nil {
			progress.phase = "release"
		}
		return err
	})
	// Resource retirement/cancellation fences run after the callback. Produce
	// the harmless miss only when that complete borrow succeeded; otherwise an
	// idle callback joined with a hard fence could conceal the original cause.
	if err == nil && idle {
		return ErrDeliveryIdle
	}
	return err
}

func (p *DeliveryPublication) Close() {
	if p != nil && p.resource != nil {
		p.resource.Close()
	}
}
func (p *DeliveryPublication) WaitClosed(ctx context.Context) error {
	if p == nil || p.resource == nil {
		return nil
	}
	return p.resource.WaitClosed(ctx)
}
