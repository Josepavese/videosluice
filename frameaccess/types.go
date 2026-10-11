// Package frameaccess is the provider-neutral semantic Frame Access core.
// Its publisher is a trusted broker adapter, never an RPC client. Digests prove
// integrity of that adapter's binding; they are NOT signatures or authentication.
package frameaccess

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
)

type Code string

const (
	Complete               Code = "complete"
	Expired                Code = "expired"
	EpochMismatch          Code = "epoch_mismatch"
	SourceMismatch         Code = "source_mismatch"
	GraphMismatch          Code = "graph_revision_mismatch"
	TransformMismatch      Code = "transform_mismatch"
	CorrelationUnavailable Code = "correlation_unavailable"
	RegionInvalid          Code = "region_invalid"
	CapacityRejected       Code = "capacity_rejected"
	DeadlineExceeded       Code = "deadline_exceeded"
	Canceled               Code = "canceled"
	ProviderUnavailable    Code = "provider_unavailable"
	InvalidContract        Code = "invalid_contract"
	ExactSameOrigin        Code = "exact_same_origin"
	DifferentOrigin        Code = "different_origin"
	DifferentSource        Code = "different_source"
)

func (c Code) Error() string { return string(c) }

type Digest [32]byte
type OriginRef struct {
	Source     string
	MediaEpoch uint64
	Sequence   uint64
	Graph      Digest
}
type DeliveryRef struct {
	Stream  string
	Epoch   uint64 // Publication epoch, NOT capture epoch.
	FrameID uint64
	Graph   Digest
}

func token(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}
func (r OriginRef) valid() bool {
	return token(r.Source) && r.MediaEpoch > 0 && r.Sequence > 0 && r.Graph != (Digest{})
}
func (r DeliveryRef) valid() bool {
	return token(r.Stream) && r.Epoch > 0 && r.FrameID > 0 && r.Graph != (Digest{})
}

// Geometry is NV12, Y followed by interleaved UV with the same byte stride.
// Every provider receives explicit geometry; padding is never image content.
type Geometry struct{ Width, Height, Stride int }

func (g Geometry) Bytes() (int, error) {
	if g.Width < 2 || g.Height < 2 || g.Width > 8192 || g.Height > 8192 || g.Width%2 != 0 || g.Height%2 != 0 || g.Stride < g.Width || g.Stride > 16384 || g.Stride%2 != 0 {
		return 0, InvalidContract
	}
	return g.Stride * (g.Height + g.Height/2), nil
}

type Rect struct{ X1, Y1, X2, Y2 int }

func (r Rect) valid(g Geometry) bool {
	return r.X1 >= 0 && r.Y1 >= 0 && r.X2 > r.X1 && r.Y2 > r.Y1 && r.X2 <= g.Width && r.Y2 <= g.Height && r.X1%2 == 0 && r.Y1%2 == 0 && r.X2%2 == 0 && r.Y2%2 == 0
}

// Transform maps full-surface crop -> clockwise rotation -> optional horizontal
// mirror -> resize -> even letterbox padding. All coordinates are half-open.
// The matrix is described by the trusted worker, not invented by the consumer.
type Transform struct {
	Full                          Geometry
	Crop                          Rect
	Rotation                      int
	Mirror                        bool
	ResizedWidth, ResizedHeight   int
	DeliveryWidth, DeliveryHeight int
	PadLeft, PadTop               int
}

func (t Transform) Validate() error {
	if _, err := t.Full.Bytes(); err != nil || !t.Crop.valid(t.Full) {
		return TransformMismatch
	}
	if t.Rotation != 0 && t.Rotation != 90 && t.Rotation != 180 && t.Rotation != 270 {
		return TransformMismatch
	}
	for _, x := range []int{t.ResizedWidth, t.ResizedHeight, t.DeliveryWidth, t.DeliveryHeight} {
		if x < 2 || x > 8192 || x%2 != 0 {
			return TransformMismatch
		}
	}
	if t.PadLeft < 0 || t.PadTop < 0 || t.PadLeft%2 != 0 || t.PadTop%2 != 0 || t.PadLeft > t.DeliveryWidth-t.ResizedWidth || t.PadTop > t.DeliveryHeight-t.ResizedHeight {
		return TransformMismatch
	}
	return nil
}

type Region struct {
	ID           string
	BBox         [4]float64
	PaddingRatio float64
	FullFrame    bool
}

func finite(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }

// MapRegion rounds outward to whole NV12 chroma samples AFTER mapping. Pixels
// outside the letterbox content are clipped; padding-only ROIs are rejected.
// Extracted pixels are never resized or upscaled by this core.
func MapRegion(t Transform, r Region) (Rect, error) {
	if err := t.Validate(); err != nil {
		return Rect{}, err
	}
	if !token(r.ID) || !finite(r.PaddingRatio) || r.PaddingRatio < 0 || r.PaddingRatio > 1 {
		return Rect{}, RegionInvalid
	}
	for _, v := range r.BBox {
		if !finite(v) || math.Abs(v) > 1<<24 {
			return Rect{}, RegionInvalid
		}
	}
	if r.FullFrame {
		if r.BBox != ([4]float64{}) || r.PaddingRatio != 0 {
			return Rect{}, RegionInvalid
		}
		return Rect{0, 0, t.Full.Width, t.Full.Height}, nil
	}
	x1, y1, x2, y2 := r.BBox[0], r.BBox[1], r.BBox[2], r.BBox[3]
	if x2 <= x1 || y2 <= y1 {
		return Rect{}, RegionInvalid
	}
	// Explicit binary64 rounding is part of the cross-language ROI contract.
	// Without these conversions ARM64 may fuse the multiply with x1-px/x2+px;
	// one ULP at an even-pixel boundary then changes the crop by two pixels.
	// Python consumers perform separate operations. Do not tolerate mismatched
	// rectangles or add an epsilon: make the admitted computation identical.
	px, py := float64((x2-x1)*r.PaddingRatio), float64((y2-y1)*r.PaddingRatio)
	x1 = math.Max(x1-px, float64(t.PadLeft))
	x2 = math.Min(x2+px, float64(t.PadLeft+t.ResizedWidth))
	y1 = math.Max(y1-py, float64(t.PadTop))
	y2 = math.Min(y2+py, float64(t.PadTop+t.ResizedHeight))
	if x2 <= x1 || y2 <= y1 {
		return Rect{}, RegionInvalid
	}
	cw, ch := float64(t.Crop.X2-t.Crop.X1), float64(t.Crop.Y2-t.Crop.Y1)
	rw, rh := cw, ch
	if t.Rotation == 90 || t.Rotation == 270 {
		rw, rh = ch, cw
	}
	minx, miny, maxx, maxy := math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)
	for _, p := range [][2]float64{{x1, y1}, {x1, y2}, {x2, y1}, {x2, y2}} {
		u := (p[0] - float64(t.PadLeft)) * rw / float64(t.ResizedWidth)
		v := (p[1] - float64(t.PadTop)) * rh / float64(t.ResizedHeight)
		if t.Mirror {
			u = rw - u
		}
		x, y := u, v
		switch t.Rotation {
		case 90:
			x, y = v, ch-u
		case 180:
			x, y = cw-u, ch-v
		case 270:
			x, y = cw-v, u
		}
		x += float64(t.Crop.X1)
		y += float64(t.Crop.Y1)
		minx = math.Min(minx, x)
		miny = math.Min(miny, y)
		maxx = math.Max(maxx, x)
		maxy = math.Max(maxy, y)
	}
	out := Rect{max(0, int(math.Floor(minx/2))*2), max(0, int(math.Floor(miny/2))*2), min(t.Full.Width, int(math.Ceil(maxx/2))*2), min(t.Full.Height, int(math.Ceil(maxy/2))*2)}
	if !out.valid(t.Full) {
		return Rect{}, RegionInvalid
	}
	return out, nil
}

type Clock interface{ NowNS() uint64 } // Must be monotonic, bounded, nonblocking.
type CopyCapability struct {
	Provider string
	Hardware bool
}
type CopyAdmission struct {
	Provider   string
	MediaEpoch uint64
	Graph      Digest
	Evidence   Digest // Trusted runtime admission receipt, NOT a consumer boolean.
	Attested   bool
}
type CopyPolicy struct{ RequireHardware, AllowSoftware bool }
type CopyBudget struct {
	MaxBytes   int
	DeadlineNS uint64
	Clock      Clock
}

func (b CopyBudget) Check(ctx context.Context) error {
	if ctx == nil {
		return InvalidContract
	}
	if err := ctx.Err(); err != nil {
		return Canceled
	}
	if b.Clock == nil || b.DeadlineNS == 0 || b.Clock.NowNS() >= b.DeadlineNS {
		return DeadlineExceeded
	}
	return nil
}

type Copier interface {
	Capability() CopyCapability
	// CopyNV12 must obey cancellation/deadline and never retain input/output.
	// Source memory is already detached from capture ownership. A slow provider
	// consumes the optional feature's single copy slot, NOT the capture thread.
	CopyNV12(context.Context, CopyBudget, []byte, []byte, Geometry, Rect) error
}

func checkAdmission(c Copier, a CopyAdmission, p CopyPolicy, epoch uint64, graph Digest) error {
	if c == nil {
		return ProviderUnavailable
	}
	cap := c.Capability()
	if !token(cap.Provider) || a.Provider != cap.Provider || !a.Attested || a.Evidence == (Digest{}) || a.MediaEpoch != epoch || a.Graph != graph || p.RequireHardware && !cap.Hardware || !cap.Hardware && !p.AllowSoftware {
		return ProviderUnavailable
	}
	return nil
}

// Proof is canonical metadata plus payload integrity, not authenticity. The
// future RPC must bind the trusted writer, peer/process generation and ACL.
type Proof struct {
	Version       uint8
	RequestID     [16]byte
	Delivery      DeliveryRef
	Origin        OriginRef
	Surface       string
	Transform     Digest
	Region        string
	BBox          Rect
	Geometry      Geometry
	PayloadSHA256 Digest
	CopyProvider  string
	CopyEvidence  Digest
	Digest        Digest
}
type encoder struct{ data []byte }

func (e *encoder) n(x uint64)   { e.data = binary.BigEndian.AppendUint64(e.data, x) }
func (e *encoder) s(x string)   { e.n(uint64(len(x))); e.data = append(e.data, x...) }
func (e *encoder) d(x Digest)   { e.data = append(e.data, x[:]...) }
func (e *encoder) g(g Geometry) { e.n(uint64(g.Width)); e.n(uint64(g.Height)); e.n(uint64(g.Stride)) }
func (e *encoder) r(r Rect) {
	e.n(uint64(r.X1))
	e.n(uint64(r.Y1))
	e.n(uint64(r.X2))
	e.n(uint64(r.Y2))
}
func (t Transform) Digest() Digest {
	e := encoder{}
	e.s("video-broker.frame-transform.v1")
	e.g(t.Full)
	e.r(t.Crop)
	e.n(uint64(t.Rotation))
	if t.Mirror {
		e.n(1)
	} else {
		e.n(0)
	}
	for _, n := range []int{t.ResizedWidth, t.ResizedHeight, t.DeliveryWidth, t.DeliveryHeight, t.PadLeft, t.PadTop} {
		e.n(uint64(n))
	}
	return sha256.Sum256(e.data)
}
func proofDigest(p Proof) Digest {
	e := encoder{}
	e.s("video-broker.frame-proof.v1")
	e.n(uint64(p.Version))
	e.data = append(e.data, p.RequestID[:]...)
	e.s(p.Delivery.Stream)
	e.n(p.Delivery.Epoch)
	e.n(p.Delivery.FrameID)
	e.d(p.Delivery.Graph)
	e.s(p.Origin.Source)
	e.n(p.Origin.MediaEpoch)
	e.n(p.Origin.Sequence)
	e.d(p.Origin.Graph)
	e.s(p.Surface)
	e.d(p.Transform)
	e.s(p.Region)
	e.r(p.BBox)
	e.g(p.Geometry)
	e.d(p.PayloadSHA256)
	e.s(p.CopyProvider)
	e.d(p.CopyEvidence)
	return sha256.Sum256(e.data)
}
func VerifyProof(p Proof, payload []byte, requestID [16]byte, ref DeliveryRef, origin OriginRef, transform Transform, region Region, surface string, copyAdmission CopyAdmission, maxBytes int) error {
	if p.Version != 1 || p.RequestID == ([16]byte{}) || p.RequestID != requestID || !p.Delivery.valid() || p.Delivery != ref || !p.Origin.valid() || p.Origin != origin || p.Delivery.Graph != p.Origin.Graph || p.Surface != surface || (surface != "processed_full" && surface != "source_decoded") || p.Region != region.ID {
		return InvalidContract
	}
	if !copyAdmission.Attested || copyAdmission.MediaEpoch != origin.MediaEpoch || copyAdmission.Graph != origin.Graph || !token(p.CopyProvider) || p.CopyEvidence == (Digest{}) || p.CopyProvider != copyAdmission.Provider || p.CopyEvidence != copyAdmission.Evidence {
		return ProviderUnavailable
	}
	bbox, err := MapRegion(transform, region)
	if err != nil || bbox != p.BBox || transform.Digest() != p.Transform {
		return TransformMismatch
	}
	expected := Geometry{bbox.X2 - bbox.X1, bbox.Y2 - bbox.Y1, bbox.X2 - bbox.X1}
	size, err := p.Geometry.Bytes()
	if err != nil || p.Geometry != expected || size != len(payload) || size > maxBytes || size <= 0 {
		return InvalidContract
	}
	if sha256.Sum256(payload) != p.PayloadSHA256 || proofDigest(p) != p.Digest {
		return fmt.Errorf("frame proof integrity mismatch: %w", InvalidContract)
	}
	return nil
}
