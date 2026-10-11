package ingress

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	wire "github.com/Josepavese/videosluice/framewire"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

const MaxFrameManifestBytes = 2048
const MaxFrameDataBytes = wire.MaxDataPacketBytes
const frameDataHeaderBytes = wire.DataHeaderSize

// FrameManifest has no pixel allocation. The authenticated manifest precedes
// sequential data records and is specific to one region of one exact request.
type FrameManifest struct {
	ID       [16]byte
	Fence    fd.Fence
	Index    int
	Bytes    int
	Artifact fd.Artifact
}

var frameStates = [...]fa.Code{fa.Complete, fa.Expired, fa.EpochMismatch,
	fa.SourceMismatch, fa.GraphMismatch, fa.TransformMismatch, fa.CorrelationUnavailable,
	fa.RegionInvalid, fa.CapacityRejected, fa.DeadlineExceeded, fa.Canceled,
	fa.ProviderUnavailable, fa.InvalidContract}

func frameStateCode(state fa.Code) byte {
	for i, s := range frameStates {
		if state == s {
			return byte(i + 1)
		}
	}
	return 0
}

func (m FrameManifest) validate() error {
	if m.ID == ([16]byte{}) || m.Fence.Namespace == ([16]byte{}) || !controlToken(m.Fence.ProcessGeneration) ||
		m.Index < 0 || m.Index >= 16 || !controlToken(m.Artifact.Region) || frameStateCode(m.Artifact.State) == 0 {
		return fail("frame_manifest_identity", peer.ErrInvalid)
	}
	if m.Artifact.State != fa.Complete {
		if m.Bytes != 0 || m.Artifact.Proof != (fd.Proof{}) {
			return fail("frame_manifest_failure_payload", peer.ErrInvalid)
		}
		return nil
	}
	p := m.Artifact.Proof.Core
	if m.Artifact.Proof.Fence != m.Fence || p.Version != 1 || p.RequestID != m.ID || p.Region != m.Artifact.Region ||
		!controlToken(p.Delivery.Stream) || p.Delivery.Epoch == 0 || p.Delivery.FrameID == 0 || p.Delivery.Graph == (fa.Digest{}) ||
		!controlToken(p.Origin.Source) || p.Origin.MediaEpoch == 0 || p.Origin.Sequence == 0 || p.Origin.Graph != p.Delivery.Graph ||
		(p.Surface != "source_decoded" && p.Surface != "processed_full") || !controlToken(p.CopyProvider) ||
		p.Transform == (fa.Digest{}) || p.CopyEvidence == (fa.Digest{}) {
		return fail("frame_manifest_proof", peer.ErrInvalid)
	}
	n, err := p.Geometry.Bytes()
	if err != nil || n != m.Bytes || m.Bytes <= 0 || p.BBox.X1 < 0 || p.BBox.Y1 < 0 ||
		p.BBox.X2 > 8192 || p.BBox.Y2 > 8192 || p.BBox.X1%2 != 0 || p.BBox.Y1%2 != 0 ||
		p.BBox.X2-p.BBox.X1 != p.Geometry.Width || p.BBox.Y2-p.BBox.Y1 != p.Geometry.Height ||
		p.Geometry.Stride != p.Geometry.Width {
		return fail("frame_manifest_geometry", peer.ErrInvalid)
	}
	if err := fd.VerifyDomainProof(m.Artifact.Proof, m.Fence); err != nil {
		return fail("frame_manifest_domain_proof", err)
	}
	return nil
}

// EncodeFrameManifest writes only into caller-admitted storage. No payload copy
// or resource allocation. The caller owns m and dst exclusively for the call.
func EncodeFrameManifest(dst []byte, m FrameManifest) (int, error) {
	if err := m.validate(); err != nil {
		return 0, err
	}
	w := controlWriter{data: make([]byte, MaxControlBytes), n: 8}
	w.bytes(m.ID[:])
	w.bytes(m.Fence.Namespace[:])
	w.text(m.Fence.ProcessGeneration)
	w.byte(byte(m.Index))
	w.byte(frameStateCode(m.Artifact.State))
	w.text(m.Artifact.Region)
	w.u64(uint64(m.Bytes))
	if m.Artifact.State == fa.Complete {
		p := m.Artifact.Proof.Core
		w.text(p.Delivery.Stream)
		w.u64(p.Delivery.Epoch)
		w.u64(p.Delivery.FrameID)
		w.bytes(p.Delivery.Graph[:])
		w.text(p.Origin.Source)
		w.u64(p.Origin.MediaEpoch)
		w.u64(p.Origin.Sequence)
		w.text(p.Surface)
		w.bytes(p.Transform[:])
		for _, n := range [7]int{p.BBox.X1, p.BBox.Y1, p.BBox.X2, p.BBox.Y2, p.Geometry.Width, p.Geometry.Height, p.Geometry.Stride} {
			w.u64(uint64(n))
		}
		w.bytes(p.PayloadSHA256[:])
		w.text(p.CopyProvider)
		w.bytes(p.CopyEvidence[:])
		w.bytes(p.Digest[:])
		w.bytes(m.Artifact.Proof.Digest[:])
	}
	copy(w.data[:4], "FAPM")
	w.data[4] = 1
	binary.BigEndian.PutUint16(w.data[6:8], uint16(w.n))
	if w.n > MaxFrameManifestBytes || len(dst) < w.n {
		return 0, fail("frame_manifest_buffer", fa.CapacityRejected)
	}
	return copy(dst, w.data[:w.n]), nil
}

func DecodeFrameManifest(packet []byte) (FrameManifest, error) {
	invalid := func(stage string) (FrameManifest, error) { return FrameManifest{}, fail(stage, peer.ErrInvalid) }
	if len(packet) < 8 || len(packet) > MaxFrameManifestBytes || string(packet[:4]) != "FAPM" || packet[4] != 1 || packet[5] != 0 || int(binary.BigEndian.Uint16(packet[6:8])) != len(packet) {
		return invalid("frame_manifest_framing")
	}
	r := controlReader{data: packet, n: 8}
	var m FrameManifest
	copy(m.ID[:], r.bytes(16))
	copy(m.Fence.Namespace[:], r.bytes(16))
	m.Fence.ProcessGeneration = r.text()
	m.Index = int(r.byte())
	state := int(r.byte())
	if state < 1 || state > len(frameStates) {
		return invalid("frame_manifest_state")
	}
	m.Artifact.State = frameStates[state-1]
	m.Artifact.Region = r.text()
	n := r.u64()
	if n > 1<<28 {
		return invalid("frame_manifest_size")
	}
	m.Bytes = int(n)
	if m.Artifact.State == fa.Complete {
		m.Artifact.Proof.Fence = m.Fence
		p := &m.Artifact.Proof.Core
		p.Version = 1
		p.RequestID = m.ID
		p.Region = m.Artifact.Region
		p.Delivery.Stream = r.text()
		p.Delivery.Epoch = r.u64()
		p.Delivery.FrameID = r.u64()
		copy(p.Delivery.Graph[:], r.bytes(32))
		p.Origin.Source = r.text()
		p.Origin.MediaEpoch = r.u64()
		p.Origin.Sequence = r.u64()
		p.Origin.Graph = p.Delivery.Graph
		p.Surface = r.text()
		copy(p.Transform[:], r.bytes(32))
		values := [7]int{}
		for i := range values {
			v := r.u64()
			if v > 16384 {
				return invalid("frame_manifest_geometry")
			}
			values[i] = int(v)
		}
		p.BBox = fa.Rect{X1: values[0], Y1: values[1], X2: values[2], Y2: values[3]}
		p.Geometry = fa.Geometry{Width: values[4], Height: values[5], Stride: values[6]}
		copy(p.PayloadSHA256[:], r.bytes(32))
		p.CopyProvider = r.text()
		copy(p.CopyEvidence[:], r.bytes(32))
		copy(p.Digest[:], r.bytes(32))
		copy(m.Artifact.Proof.Digest[:], r.bytes(32))
	}
	if r.bad || r.n != len(packet) {
		return invalid("frame_manifest_length")
	}
	if err := m.validate(); err != nil {
		return FrameManifest{}, err
	}
	return m, nil
}

// ExpectedFrameEvidence MUST come from independently authenticated publication
// metadata/admission, not be copied from the received artifact's own proof.
// Digest validation alone is integrity, not authorization or HW attestation.
type ExpectedFrameEvidence struct {
	Origin    fa.OriginRef
	Transform fa.Transform
	Admission fa.CopyAdmission
}

func ValidateFrameManifest(m FrameManifest, q FrameRequest, maxBytes int) error {
	if err := q.validate(); err != nil {
		return err
	}
	if err := m.validate(); err != nil {
		return err
	}
	if maxBytes < 1 || maxBytes > 1<<28 || m.Bytes > maxBytes {
		return fail("frame_output_capacity", fa.CapacityRejected)
	}
	if m.ID != q.ID || m.Fence != q.Frame.Fence || m.Index >= len(q.Regions) || m.Artifact.Region != q.Regions[m.Index].ID {
		return fail("frame_output_correlation", peer.ErrInvalid)
	}
	if m.Artifact.State == fa.Complete && (m.Artifact.Proof.Core.Delivery != q.Frame.Ref || m.Artifact.Proof.Core.Origin.Source != q.Source || m.Artifact.Proof.Core.Surface != q.Surface) {
		return fail("frame_output_scope", peer.ErrUnauthorized)
	}
	return nil
}

// EncodeFrameData takes a SHA256 of the exact authenticated manifest packet,
// binding every sequential chunk to that artifact (including its content hash).
func EncodeFrameData(dst []byte, manifestID [32]byte, offset int, payload []byte) (int, error) {
	n, err := wire.EncodeDataChunk(dst, manifestID, offset, payload)
	if errors.Is(err, wire.ErrBuffer) {
		return 0, fail("frame_data_buffer", fa.CapacityRejected)
	}
	if err != nil {
		return 0, fail("frame_data_contract", peer.ErrInvalid)
	}
	return n, nil
}

// FrameAssembly exclusively borrows caller-admitted output storage. It starts
// after sender authentication, never allocates pixels and cannot grow a queue.
// On any append/verification error it is terminal: discard the partial buffer.
// This is single-owner, not concurrency-safe. Caller retains physical ownership
// and must release its budget only when that storage is actually relinquished.
type FrameAssembly struct {
	manifest   FrameManifest
	manifestID [32]byte
	request    FrameRequest
	region     fa.Region
	expected   ExpectedFrameEvidence
	buffer     []byte
	next       int
	terminal   bool
}

// Abort ends a cancelled/expired transport without publishing partial pixels.
// It drops this borrow, not the caller's allocation or its resource charge.
func (a *FrameAssembly) Abort() {
	if a != nil {
		a.terminal = true
		a.buffer = nil
	}
}

func NewFrameAssembly(packet []byte, q FrameRequest, expected ExpectedFrameEvidence, buffer []byte, maxBytes int) (*FrameAssembly, error) {
	m, err := DecodeFrameManifest(packet)
	if err != nil {
		return nil, err
	}
	if err = ValidateFrameManifest(m, q, maxBytes); err != nil {
		return nil, err
	}
	if len(buffer) != m.Bytes {
		return nil, fail("frame_output_buffer", fa.CapacityRejected)
	}
	region := q.Regions[m.Index]
	q.Regions = nil // Keep only this region, not caller's mutable slice.
	return &FrameAssembly{manifest: m, manifestID: sha256.Sum256(packet), request: q, region: region, expected: expected, buffer: buffer}, nil
}

func (a *FrameAssembly) Append(packet []byte) error {
	if a == nil || a.terminal {
		return fail("frame_output_terminal", peer.ErrRetired)
	}
	bad := func(stage string) error { a.Abort(); return fail(stage, peer.ErrInvalid) }
	d, err := wire.ParseDataChunk(packet)
	if err != nil {
		return bad("frame_data_framing")
	}
	if d.Manifest != a.manifestID || uint64(d.Offset) != uint64(a.next) {
		return bad("frame_data_sequence")
	}
	n := len(d.Payload)
	if n > len(a.buffer)-a.next {
		return bad("frame_data_overrun")
	}
	copy(a.buffer[a.next:], d.Payload)
	a.next += n
	return nil
}

// Finish releases no allocation and returns usable pixels only once, after
// full independent proof verification. Caller must not consume buffer earlier.
func (a *FrameAssembly) Finish() (fd.Artifact, []byte, error) {
	if a == nil || a.terminal {
		return fd.Artifact{}, nil, fail("frame_output_terminal", peer.ErrRetired)
	}
	a.terminal = true
	defer func() { a.buffer = nil }()
	if a.next != len(a.buffer) {
		return fd.Artifact{}, nil, fail("frame_output_incomplete", peer.ErrRecord)
	}
	m := a.manifest
	if m.Artifact.State == fa.Complete {
		err := fa.VerifyProof(m.Artifact.Proof.Core, a.buffer, a.request.ID, a.request.Frame.Ref, a.expected.Origin, a.expected.Transform, a.region, a.request.Surface, a.expected.Admission, len(a.buffer))
		if err != nil {
			return fd.Artifact{}, nil, fail("frame_output_proof", err)
		}
	}
	return m.Artifact, a.buffer, nil
}
