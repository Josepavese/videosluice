package ingress

import (
	"encoding/binary"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

const MaxControlBytes = 4096

type ControlKind uint8

const (
	ControlGrant  ControlKind = 1
	ControlRevoke ControlKind = 2
	ControlStatus ControlKind = 3
)

// ControlCommand is a current-only wire contract for an authenticated control
// channel. It carries no Principal and cannot establish process authority.
// CapabilityMetadata is opaque to middleware; the selected provider must
// decode it and validate the separately transferred owned capability.
type ControlCommand struct {
	Kind               ControlKind
	Header             ControlHeader
	Binding            fd.BindingRef
	Policy             AccessPolicy
	CapabilityType     string
	CapabilityMetadata []byte
}

func (c ControlCommand) validate() error {
	h := c.Header
	if h.Target.Namespace == ([16]byte{}) || !controlToken(h.Target.ProcessGeneration) || h.ID == ([16]byte{}) {
		return fail("control_wire_header", peer.ErrInvalid)
	}
	if c.Kind == ControlStatus {
		if h.Sequence != 0 || h.Previous != 0 || h.Grant != "" {
			return fail("control_wire_status", peer.ErrInvalid)
		}
	} else if c.Kind == ControlGrant || c.Kind == ControlRevoke {
		if h.Sequence == 0 || !controlToken(h.Grant) {
			return fail("control_wire_header", peer.ErrInvalid)
		}
	} else {
		return fail("control_wire_operation", peer.ErrInvalid)
	}
	if c.Kind != ControlGrant {
		p := c.Policy
		if c.Binding != (fd.BindingRef{}) || c.CapabilityType != "" || len(c.CapabilityMetadata) != 0 || p.Generation != "" || len(p.Streams) != 0 || p.MaxRegions != 0 || p.MaxRequests != 0 || p.MaxResults != 0 || p.AllowFullFrame {
			return fail("control_wire_unexpected_fields", peer.ErrInvalid)
		}
		return nil
	}
	if c.Binding.Fence != h.Target || !controlToken(c.Binding.Source) || c.Binding.Epoch == 0 {
		return fail("control_wire_binding", peer.ErrInvalid)
	}
	p := c.Policy
	if !controlToken(p.Generation) || p.MaxRegions < 1 || p.MaxRegions > 16 || p.MaxRequests < 1 || p.MaxRequests > 64 || p.MaxResults < 1 || p.MaxResults > 64 || len(p.Streams) < 1 || len(p.Streams) > 16 {
		return fail("control_wire_policy", peer.ErrInvalid)
	}
	// A unique sorted stream set gives one canonical encoding, independent of
	// map iteration and language. No normalization of malformed input on decode.
	for i, name := range p.Streams {
		if !controlToken(name) || i > 0 && name <= p.Streams[i-1] {
			return fail("control_wire_streams", peer.ErrInvalid)
		}
	}
	if !controlToken(c.CapabilityType) || len(c.CapabilityMetadata) < 1 || len(c.CapabilityMetadata) > 256 {
		return fail("control_wire_capability", peer.ErrInvalid)
	}
	return nil
}

type controlWriter struct {
	data []byte
	n    int
}

func (w *controlWriter) bytes(p []byte) { w.n += copy(w.data[w.n:], p) }
func (w *controlWriter) byte(b byte)    { w.data[w.n] = b; w.n++ }
func (w *controlWriter) text(s string)  { w.byte(byte(len(s))); w.bytes([]byte(s)) }
func (w *controlWriter) u64(n uint64)   { binary.BigEndian.PutUint64(w.data[w.n:], n); w.n += 8 }

// EncodeControl snapshots admitted metadata into one owned bounded packet.
// uint64 fields are binary exact; they must never pass through float64/JS Number.
// Callers must exclusively own the command while it is being encoded.
func EncodeControl(c ControlCommand) ([]byte, error) {
	buffer := make([]byte, MaxControlBytes)
	n, err := EncodeControlInto(buffer, c)
	if err != nil {
		return nil, err
	}
	return buffer[:n], nil
}

// EncodeControlInto borrows an admitted fixed-capacity packet. No allocation
// or retained command/storage; invalid input leaves the buffer unchanged.
func EncodeControlInto(buffer []byte, c ControlCommand) (int, error) {
	if err := c.validate(); err != nil {
		return 0, err
	}
	if len(buffer) < MaxControlBytes {
		return 0, fail("control_encode_buffer", peer.ErrInvalid)
	}
	// Validated maxima total < 1800B; fixed buffer has no growth path.
	w := controlWriter{data: buffer[:MaxControlBytes], n: 8}
	w.bytes(c.Header.Target.Namespace[:])
	w.text(c.Header.Target.ProcessGeneration)
	w.bytes(c.Header.ID[:])
	w.u64(c.Header.Sequence)
	w.u64(c.Header.Previous)
	w.text(c.Header.Grant)
	if c.Kind == ControlGrant {
		w.text(c.Binding.Source)
		w.u64(c.Binding.Epoch)
		w.text(c.Policy.Generation)
		w.byte(byte(c.Policy.MaxRegions))
		w.byte(byte(c.Policy.MaxRequests))
		w.byte(byte(c.Policy.MaxResults))
		flags := byte(0)
		if c.Policy.AllowFullFrame {
			flags = 1
		}
		w.byte(flags)
		w.byte(byte(len(c.Policy.Streams)))
		for _, name := range c.Policy.Streams {
			w.text(name)
		}
		w.text(c.CapabilityType)
		binary.BigEndian.PutUint16(w.data[w.n:], uint16(len(c.CapabilityMetadata)))
		w.n += 2
		w.bytes(c.CapabilityMetadata)
	}
	copy(w.data[:4], "FAPC")
	w.data[4] = 1
	w.data[5] = byte(c.Kind)
	binary.BigEndian.PutUint16(w.data[6:8], uint16(w.n))
	return w.n, nil
}

type controlReader struct {
	data []byte
	n    int
	bad  bool
}

func (r *controlReader) bytes(n int) []byte {
	if r.bad || n < 0 || n > len(r.data)-r.n {
		r.bad = true
		return nil
	}
	p := r.data[r.n : r.n+n]
	r.n += n
	return p
}
func (r *controlReader) byte() byte {
	p := r.bytes(1)
	if p == nil {
		return 0
	}
	return p[0]
}
func (r *controlReader) text() string {
	n := int(r.byte())
	if n > 64 {
		r.bad = true
		return ""
	}
	return string(r.bytes(n))
}
func (r *controlReader) u64() uint64 {
	p := r.bytes(8)
	if p == nil {
		return 0
	}
	return binary.BigEndian.Uint64(p)
}

// DecodeControl accepts exactly one packet. No trailing data, ignored fields,
// coercions, alternate versions, wildcard binding or unbounded collections.
// Returned slices and strings do not alias the reusable receive buffer.
func DecodeControl(data []byte) (ControlCommand, error) {
	invalid := func(stage string) (ControlCommand, error) { return ControlCommand{}, fail(stage, peer.ErrInvalid) }
	if len(data) < 8 || len(data) > MaxControlBytes || string(data[:4]) != "FAPC" || data[4] != 1 || int(binary.BigEndian.Uint16(data[6:8])) != len(data) {
		return invalid("control_wire_framing")
	}
	c := ControlCommand{Kind: ControlKind(data[5])}
	if c.Kind != ControlGrant && c.Kind != ControlRevoke && c.Kind != ControlStatus {
		return invalid("control_wire_operation")
	}
	r := controlReader{data: data, n: 8}
	copy(c.Header.Target.Namespace[:], r.bytes(16))
	c.Header.Target.ProcessGeneration = r.text()
	copy(c.Header.ID[:], r.bytes(16))
	c.Header.Sequence = r.u64()
	c.Header.Previous = r.u64()
	c.Header.Grant = r.text()
	if c.Kind == ControlGrant {
		c.Binding.Fence = c.Header.Target
		c.Binding.Source = r.text()
		c.Binding.Epoch = r.u64()
		c.Policy.Generation = r.text()
		c.Policy.MaxRegions = int(r.byte())
		c.Policy.MaxRequests = int(r.byte())
		c.Policy.MaxResults = int(r.byte())
		flags := r.byte()
		if flags > 1 {
			return invalid("control_wire_flags")
		}
		c.Policy.AllowFullFrame = flags == 1
		count := int(r.byte())
		if count < 1 || count > 16 {
			return invalid("control_wire_streams")
		}
		c.Policy.Streams = make([]string, count)
		for i := range c.Policy.Streams {
			c.Policy.Streams[i] = r.text()
		}
		c.CapabilityType = r.text()
		p := r.bytes(2)
		if p == nil {
			return invalid("control_wire_payload")
		}
		n := int(binary.BigEndian.Uint16(p))
		if n < 1 || n > 256 {
			return invalid("control_wire_capability")
		}
		c.CapabilityMetadata = append([]byte(nil), r.bytes(n)...)
	}
	if r.bad || r.n != len(data) {
		return invalid("control_wire_payload")
	}
	if err := c.validate(); err != nil {
		return ControlCommand{}, err
	}
	return c, nil
}
