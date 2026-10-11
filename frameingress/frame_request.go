package ingress

import (
	"context"
	"encoding/binary"
	"math"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

const MaxFrameRequestBytes = 4096
const MaxFrameRequestBudgetNS uint64 = 5_000_000_000

// FrameRequest carries a duration, never a timestamp from the client's clock.
// Its fence/reference identifies an exact publication; no nearest-frame lookup.
// Authentication and grant selection are external to this untrusted packet.
type FrameRequest struct {
	ID              [16]byte
	Frame           fd.DeliveryRef
	Source, Surface string
	Regions         []fa.Region
	BudgetNS        uint64
}

func (q FrameRequest) validate() error {
	if q.ID == ([16]byte{}) || q.Frame.Fence.Namespace == ([16]byte{}) ||
		!controlToken(q.Frame.Fence.ProcessGeneration) || !controlToken(q.Frame.Ref.Stream) ||
		q.Frame.Ref.Epoch == 0 || q.Frame.Ref.FrameID == 0 || q.Frame.Ref.Graph == (fa.Digest{}) ||
		!controlToken(q.Source) || !controlToken(q.Surface) {
		return fail("frame_request_identity", peer.ErrInvalid)
	}
	if q.BudgetNS == 0 || q.BudgetNS > MaxFrameRequestBudgetNS {
		return fail("frame_request_budget", peer.ErrInvalid)
	}
	if len(q.Regions) < 1 || len(q.Regions) > 16 {
		return fail("frame_request_regions", peer.ErrInvalid)
	}
	for i, region := range q.Regions {
		if !controlToken(region.ID) {
			return fail("frame_request_region_id", peer.ErrInvalid)
		}
		for j := 0; j < i; j++ {
			if region.ID == q.Regions[j].ID {
				return fail("frame_request_region_id", peer.ErrInvalid)
			}
		}
		for _, x := range [5]float64{region.BBox[0], region.BBox[1], region.BBox[2], region.BBox[3], region.PaddingRatio} {
			if math.IsNaN(x) || math.IsInf(x, 0) || math.Abs(x) > 1<<24 || x == 0 && math.Signbit(x) {
				return fail("frame_request_geometry", peer.ErrInvalid)
			}
		}
		if region.PaddingRatio < 0 || region.PaddingRatio > 1 ||
			region.FullFrame && (region.BBox != ([4]float64{}) || region.PaddingRatio != 0) ||
			!region.FullFrame && (region.BBox[2] <= region.BBox[0] || region.BBox[3] <= region.BBox[1]) {
			return fail("frame_request_geometry", peer.ErrInvalid)
		}
	}
	return nil
}

// EncodeFrameRequest snapshots bounded metadata. uint64 identities never pass
// through JSON Number. Binary64 is used only for finite ROI geometry.
func EncodeFrameRequest(q FrameRequest) ([]byte, error) {
	if err := q.validate(); err != nil {
		return nil, err
	}
	// Maximum valid representation < 2200 bytes, including 16 named regions.
	w := controlWriter{data: make([]byte, MaxControlBytes), n: 8}
	w.bytes(q.ID[:])
	w.bytes(q.Frame.Fence.Namespace[:])
	w.text(q.Frame.Fence.ProcessGeneration)
	w.text(q.Frame.Ref.Stream)
	w.u64(q.Frame.Ref.Epoch)
	w.u64(q.Frame.Ref.FrameID)
	w.bytes(q.Frame.Ref.Graph[:])
	w.text(q.Source)
	w.text(q.Surface)
	w.u64(q.BudgetNS)
	w.byte(byte(len(q.Regions)))
	for _, region := range q.Regions {
		w.text(region.ID)
		if region.FullFrame {
			w.byte(1)
		} else {
			w.byte(0)
		}
		for _, x := range region.BBox {
			w.u64(math.Float64bits(x))
		}
		w.u64(math.Float64bits(region.PaddingRatio))
	}
	copy(w.data[:4], "FAPQ")
	w.data[4] = 1
	binary.BigEndian.PutUint16(w.data[6:8], uint16(w.n))
	return append([]byte(nil), w.data[:w.n]...), nil
}

// DecodeFrameRequest accepts exactly one current packet and returns no partial
// request on failure. Strings/regions do not alias the receive buffer.
func DecodeFrameRequest(packet []byte) (FrameRequest, error) {
	invalid := func(stage string) (FrameRequest, error) { return FrameRequest{}, fail(stage, peer.ErrInvalid) }
	if len(packet) < 8 || len(packet) > MaxFrameRequestBytes || string(packet[:4]) != "FAPQ" ||
		packet[4] != 1 || packet[5] != 0 || int(binary.BigEndian.Uint16(packet[6:8])) != len(packet) {
		return invalid("frame_request_framing")
	}
	r := controlReader{data: packet, n: 8}
	var q FrameRequest
	copy(q.ID[:], r.bytes(16))
	copy(q.Frame.Fence.Namespace[:], r.bytes(16))
	q.Frame.Fence.ProcessGeneration = r.text()
	q.Frame.Ref.Stream = r.text()
	q.Frame.Ref.Epoch = r.u64()
	q.Frame.Ref.FrameID = r.u64()
	copy(q.Frame.Ref.Graph[:], r.bytes(32))
	q.Source = r.text()
	q.Surface = r.text()
	q.BudgetNS = r.u64()
	count := int(r.byte())
	if r.bad || count < 1 || count > 16 {
		return invalid("frame_request_regions")
	}
	q.Regions = make([]fa.Region, count)
	for i := range q.Regions {
		region := &q.Regions[i]
		region.ID = r.text()
		flags := r.byte()
		if flags > 1 {
			return invalid("frame_request_flags")
		}
		region.FullFrame = flags == 1
		for j := range region.BBox {
			region.BBox[j] = math.Float64frombits(r.u64())
		}
		region.PaddingRatio = math.Float64frombits(r.u64())
	}
	if r.bad || r.n != len(packet) {
		return invalid("frame_request_length")
	}
	if err := q.validate(); err != nil {
		return FrameRequest{}, err
	}
	return q, nil
}

// ExtractPacket is the bounded frame-ingress entry point after per-record
// authentication and trusted grant selection. It does not accept SCM_RIGHTS,
// establish a session or serialize output. ACLs and source limits still apply.
func (a *Access) ExtractPacket(ctx context.Context, principal peer.Principal, packet []byte) (*AccessResult, error) {
	if ctx == nil {
		return nil, fail("frame_request_context", peer.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := a.current(); err != nil {
		return nil, err
	}
	if principal.Generation != a.generation {
		return nil, fail("access_principal", peer.ErrUnauthorized)
	}
	q, err := DecodeFrameRequest(packet)
	if err != nil {
		return nil, err
	}
	return a.extractFrameQuery(ctx, principal, q)
}

// Caller has already decoded/validated q; the underlying Access still enforces
// authenticated consumer, exact source scope and output admission.
func (a *Access) extractFrameQuery(ctx context.Context, principal peer.Principal, q FrameRequest) (*AccessResult, error) {
	binding, err := a.runtime.ResolveBinding(a.binding)
	if err != nil {
		return nil, fail("frame_request_binding", err)
	}
	deadline, err := binding.RequestDeadline(q.BudgetNS)
	if err != nil {
		return nil, fail("frame_request_deadline", err)
	}
	return a.Extract(ctx, principal, fd.Request{ID: q.ID, Frame: q.Frame, Source: q.Source,
		Surface: q.Surface, Regions: q.Regions, DeadlineNS: deadline})
}
