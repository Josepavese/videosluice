package envelope

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const ControlSize = 64

var ErrBusy = errors.New("publication changed or is in progress")
var ErrMapping = errors.New("mapping is not the expected allocation")

// Mapping is issued by trusted middleware for one allocation lifetime. A
// namespace/generation pair must never be reused, including across restarts.
// Capacity is packet area size, excluding the 64-byte control. It is not a
// camera capability or a global memory budget.
type Mapping struct {
	Namespace            [16]byte
	Generation, Capacity uint64
}

func (m Mapping) valid() bool {
	return m.Namespace != [16]byte{} && m.Generation != 0 &&
		m.Capacity >= HeaderSize && m.Capacity <= HeaderSize+MaxPayload
}

type Control struct {
	Begin, Capacity, Used, Generation uint64
	Namespace                         [16]byte
	End                               uint64
}

func (c Control) validate() error {
	if !(Mapping{c.Namespace, c.Generation, c.Capacity}).valid() || c.Used < HeaderSize || c.Used > c.Capacity {
		return ErrInvalid
	}
	if c.Begin == 0 || c.End == 0 {
		return ErrInvalid
	}
	if c.Begin&1 != 0 || c.Begin != c.End {
		return ErrBusy
	}
	return nil
}

// MarshalControl encodes completed publications only. Provider atomic store
// order and byte order conversion are NOT implemented by this codec.
func MarshalControl(c Control) ([ControlSize]byte, error) {
	var b [ControlSize]byte
	if err := c.validate(); err != nil {
		return b, err
	}
	copy(b[:4], "VBSC")
	binary.BigEndian.PutUint16(b[4:6], 1)
	binary.BigEndian.PutUint16(b[6:8], ControlSize)
	for i, v := range []uint64{c.Begin, c.Capacity, c.Used, c.Generation} {
		binary.BigEndian.PutUint64(b[8+i*8:16+i*8], v)
	}
	copy(b[40:56], c.Namespace[:])
	binary.BigEndian.PutUint64(b[56:64], c.End)
	return b, nil
}

func ParseControl(b []byte) (Control, error) {
	var c Control
	if len(b) != ControlSize || string(b[:4]) != "VBSC" || binary.BigEndian.Uint16(b[4:6]) != 1 || binary.BigEndian.Uint16(b[6:8]) != ControlSize {
		return c, ErrInvalid
	}
	c.Begin = binary.BigEndian.Uint64(b[8:16])
	c.Capacity = binary.BigEndian.Uint64(b[16:24])
	c.Used = binary.BigEndian.Uint64(b[24:32])
	c.Generation = binary.BigEndian.Uint64(b[32:40])
	copy(c.Namespace[:], b[40:56])
	c.End = binary.BigEndian.Uint64(b[56:64])
	if err := c.validate(); err != nil {
		return Control{}, err
	}
	return c, nil
}

// ValidateSnapshot validates immutable provider-owned COPIES: control before,
// exactly Used packet bytes, control after. It never reads mapped memory or
// retries. Providers must guarantee atomic acquisition, correct memory ordering
// and safe lifetime; passing concurrently mutated byte slices is invalid use.
// Output payload borrows packet's immutable lifetime, as Parse does. Middleware
// must independently authenticate peer/graph/source and account all copies.
func ValidateSnapshot(before, packet, after []byte, expected Mapping) (Header, []byte, error) {
	var zero Header
	if !expected.valid() {
		return zero, nil, ErrMapping
	}
	a, err := ParseControl(before)
	if err != nil {
		return zero, nil, err
	}
	b, err := ParseControl(after)
	if err != nil {
		return zero, nil, err
	}
	if a.Namespace != expected.Namespace || a.Generation != expected.Generation || a.Capacity != expected.Capacity || b.Namespace != expected.Namespace || b.Generation != expected.Generation || b.Capacity != expected.Capacity {
		return zero, nil, ErrMapping
	}
	if !bytes.Equal(before, after) {
		return zero, nil, ErrBusy
	}
	if uint64(len(packet)) != a.Used {
		return zero, nil, ErrInvalid
	}
	h, p, err := Parse(packet)
	if err != nil {
		return zero, nil, err
	}
	if h.Namespace != expected.Namespace {
		return zero, nil, ErrMapping
	}
	return h, p, nil
}
