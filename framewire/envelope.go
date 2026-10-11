// Package envelope is a portable strict causal media framing candidate.
// Digests prove byte integrity, not peer identity or trusted provenance.
package envelope

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const HeaderSize = 448
const MaxPayload = 16 * 1024 * 1024
const (
	Surface          = 1
	Delivery         = 2
	Epoch            = 3
	SourceDefinition = 4 // Immutable admitted layout, never a captured frame.
	SourceOutput     = 5 // Worker output, not yet a broker-issued delivery.
)
const (
	H264 = 1
	H265 = 2
	NV12 = 3
)
const (
	SourceDecoded = 1
	ProcessedFull = 2
	Delivered     = 3
)
const (
	CaptureTime = 1
	MediaTime   = 2
	Keyframe    = 4
	CodecConfig = 8
	StreamReset = 16
)

var ErrInvalid = errors.New("invalid causal envelope")

type Header struct {
	Kind, Codec, Surface, Correlation                                    uint8
	Flags, PayloadBytes, Width, Height, StrideY, StrideUV                uint32
	SourceEpoch, OriginSequence, PublicationEpoch, DeliveryID, CaptureNS uint64
	MediaPTS                                                             int64
	Namespace                                                            [16]byte
	Graph, Transform, PayloadSHA                                         [32]byte
	Source, Stream, Process                                              string
	OutputSequence                                                       uint64
}

func token(s string, empty bool) bool {
	if len(s) == 0 {
		return empty
	}
	if len(s) > 64 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
func (h Header) Validate() error {
	if (h.Kind == SourceOutput) != (h.OutputSequence != 0) {
		return ErrInvalid
	}
	if !token(h.Source, false) || !token(h.Process, false) || !token(h.Stream, true) || h.Namespace == [16]byte{} || h.Graph == [32]byte{} || h.SourceEpoch == 0 || h.PayloadBytes > MaxPayload || h.Flags & ^uint32(31) != 0 || h.Correlation > 1 {
		return ErrInvalid
	}
	if h.Kind == Epoch {
		if h.Codec != 0 || h.Surface != 0 || h.Correlation != 0 || h.Flags != 0 || h.PayloadBytes != 0 || h.Width != 0 || h.Height != 0 || h.StrideY != 0 || h.StrideUV != 0 || h.OriginSequence != 0 || h.PublicationEpoch == 0 || h.DeliveryID != 0 || h.CaptureNS != 0 || h.MediaPTS != 0 || h.Transform != [32]byte{} || !token(h.Stream, false) || h.PayloadSHA != sha256.Sum256(nil) {
			return ErrInvalid
		}
		return nil
	}
	if h.Kind == SourceDefinition {
		expected := uint64(h.StrideY)*uint64(h.Height) + uint64(h.StrideUV)*uint64(h.Height/2)
		if h.Codec != NV12 || (h.Surface != SourceDecoded && h.Surface != ProcessedFull) || h.Correlation != 0 || h.Flags != 0 || h.PayloadBytes != 0 || h.OriginSequence != 0 || h.PublicationEpoch != 0 || h.DeliveryID != 0 || h.CaptureNS != 0 || h.MediaPTS != 0 || h.Stream != "" || h.PayloadSHA != sha256.Sum256(nil) || h.Transform == [32]byte{} || h.Width == 0 || h.Height == 0 || h.Width > 16384 || h.Height > 16384 || (h.Width|h.Height|h.StrideY|h.StrideUV)&1 != 0 || h.StrideY < h.Width || h.StrideUV < h.Width || expected > MaxPayload {
			return ErrInvalid
		}
		return nil
	}
	if h.Kind != Surface && h.Kind != Delivery && h.Kind != SourceOutput {
		return ErrInvalid
	}
	if h.Width == 0 || h.Height == 0 || h.Width > 16384 || h.Height > 16384 || h.PayloadBytes == 0 || h.Transform == [32]byte{} {
		return ErrInvalid
	}
	// A frame that carries pixels always carries their digest. An all-zero
	// PayloadSHA is never a real payload digest, and accepting it would let a
	// digest computation that could not run match a frame claiming zero, which
	// would publish an unverified frame. Transform already uses this convention.
	if h.PayloadSHA == [32]byte{} {
		return ErrInvalid
	}
	if (h.Correlation == 1) != (h.OriginSequence != 0) {
		return ErrInvalid
	}
	if h.Flags&CaptureTime == 0 && h.CaptureNS != 0 || h.Flags&CaptureTime != 0 && (h.CaptureNS == 0 || h.Correlation == 0) || h.Flags&MediaTime == 0 && h.MediaPTS != 0 {
		return ErrInvalid
	}
	if h.Kind == Surface {
		if h.Stream != "" || h.PublicationEpoch != 0 || h.DeliveryID != 0 || h.Correlation != 1 || h.Codec != NV12 || (h.Surface != SourceDecoded && h.Surface != ProcessedFull) {
			return ErrInvalid
		}
	} else if h.Kind == SourceOutput {
		if !token(h.Stream, false) || h.PublicationEpoch != 0 || h.DeliveryID != 0 || h.Surface != Delivered {
			return ErrInvalid
		}
	} else if !token(h.Stream, false) || h.PublicationEpoch == 0 || h.DeliveryID == 0 || h.Surface != Delivered {
		return ErrInvalid
	}
	switch h.Codec {
	case NV12:
		if (h.Width|h.Height|h.StrideY|h.StrideUV)&1 != 0 || h.StrideY < h.Width || h.StrideUV < h.Width || h.Flags&uint32(Keyframe|CodecConfig|StreamReset) != 0 {
			return ErrInvalid
		}
		expected := uint64(h.StrideY)*uint64(h.Height) + uint64(h.StrideUV)*uint64(h.Height/2)
		if expected != uint64(h.PayloadBytes) {
			return ErrInvalid
		}
	case H264, H265:
		if h.StrideY != 0 || h.StrideUV != 0 {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

// MarshalHeader never computes provenance or substitutes missing fields.
func MarshalHeader(h Header) ([HeaderSize]byte, error) {
	var b [HeaderSize]byte
	if err := h.Validate(); err != nil {
		return b, err
	}
	copy(b[:4], "VBCF")
	binary.BigEndian.PutUint16(b[4:6], 1)
	binary.BigEndian.PutUint16(b[6:8], HeaderSize)
	b[8] = h.Kind
	b[9] = h.Codec
	b[10] = h.Surface
	b[11] = h.Correlation
	for i, v := range []uint32{h.Flags, h.PayloadBytes, h.Width, h.Height, h.StrideY, h.StrideUV} {
		binary.BigEndian.PutUint32(b[12+4*i:16+4*i], v)
	}
	for i, v := range []uint64{h.SourceEpoch, h.OriginSequence, h.PublicationEpoch, h.DeliveryID, h.CaptureNS, uint64(h.MediaPTS)} {
		binary.BigEndian.PutUint64(b[40+8*i:48+8*i], v)
	}
	copy(b[88:104], h.Namespace[:])
	copy(b[104:136], h.Graph[:])
	copy(b[136:168], h.Transform[:])
	copy(b[168:200], h.PayloadSHA[:])
	copy(b[200:264], h.Source)
	copy(b[264:328], h.Stream)
	copy(b[328:392], h.Process)
	binary.BigEndian.PutUint64(b[392:400], h.OutputSequence)
	return b, nil
}
func text(b []byte) (string, error) {
	n := len(b)
	for i, c := range b {
		if c == 0 {
			n = i
			break
		}
	}
	for _, c := range b[n:] {
		if c != 0 {
			return "", ErrInvalid
		}
	}
	s := string(b[:n])
	if !token(s, true) {
		return "", ErrInvalid
	}
	return s, nil
}
func ParseHeader(b []byte) (Header, error) {
	var h Header
	if len(b) != HeaderSize || string(b[:4]) != "VBCF" || binary.BigEndian.Uint16(b[4:6]) != 1 || binary.BigEndian.Uint16(b[6:8]) != HeaderSize {
		return h, ErrInvalid
	}
	for _, span := range [][]byte{b[36:40], b[400:448]} {
		for _, c := range span {
			if c != 0 {
				return h, ErrInvalid
			}
		}
	}
	h.Kind = b[8]
	h.Codec = b[9]
	h.Surface = b[10]
	h.Correlation = b[11]
	u32 := []*uint32{&h.Flags, &h.PayloadBytes, &h.Width, &h.Height, &h.StrideY, &h.StrideUV}
	for i, p := range u32 {
		*p = binary.BigEndian.Uint32(b[12+4*i : 16+4*i])
	}
	u64 := []*uint64{&h.SourceEpoch, &h.OriginSequence, &h.PublicationEpoch, &h.DeliveryID, &h.CaptureNS}
	for i, p := range u64 {
		*p = binary.BigEndian.Uint64(b[40+8*i : 48+8*i])
	}
	h.MediaPTS = int64(binary.BigEndian.Uint64(b[80:88]))
	h.OutputSequence = binary.BigEndian.Uint64(b[392:400])
	copy(h.Namespace[:], b[88:104])
	copy(h.Graph[:], b[104:136])
	copy(h.Transform[:], b[136:168])
	copy(h.PayloadSHA[:], b[168:200])
	var err error
	if h.Source, err = text(b[200:264]); err != nil {
		return Header{}, err
	}
	if h.Stream, err = text(b[264:328]); err != nil {
		return Header{}, err
	}
	if h.Process, err = text(b[328:392]); err != nil {
		return Header{}, err
	}
	if err = h.Validate(); err != nil {
		return Header{}, err
	}
	return h, nil
}
func Marshal(h Header, payload []byte) ([]byte, error) {
	if len(payload) > MaxPayload {
		return nil, ErrInvalid
	}
	h.PayloadBytes = uint32(len(payload))
	h.PayloadSHA = sha256.Sum256(payload)
	b, err := MarshalHeader(h)
	if err != nil {
		return nil, err
	}
	out := make([]byte, HeaderSize+len(payload))
	copy(out, b[:])
	copy(out[HeaderSize:], payload)
	return out, nil
}

// Parse borrows payload from the caller, which owns its immutable lifetime.
func Parse(b []byte) (Header, []byte, error) {
	if len(b) < HeaderSize {
		return Header{}, nil, ErrInvalid
	}
	h, err := ParseHeader(b[:HeaderSize])
	if err != nil {
		return Header{}, nil, err
	}
	if uint64(len(b)) != uint64(HeaderSize)+uint64(h.PayloadBytes) || sha256.Sum256(b[HeaderSize:]) != h.PayloadSHA {
		return Header{}, nil, ErrInvalid
	}
	return h, b[HeaderSize:], nil
}
