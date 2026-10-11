package envelope

import (
	"crypto/sha256"
	"errors"
	"io"
)

// Decoder belongs to one ordered connection and one caller at a time.
// The transport owns deadlines/cancellation. No retry goroutine is spawned.
// A timeout retains the consumed prefix; framing/fatal transport errors poison
// this instance. Never replace it around each Read on the same connection.
// Storage is exclusively borrowed from the caller's admitted packet workspace.
// Returned pixels/packets remain borrowed only until the next Read or Abort.
type Decoder struct {
	limit    uint32
	storage  []byte
	headerN  int
	parsed   Header
	payload  []byte
	payloadN int
	ready    bool
	terminal error
}

// NewDecoder requires exactly header+maximum payload bytes of exclusively owned
// storage. There is no per-frame payload allocation or implicit fallback arena.
// The composition owner charges this workspace and decoder metadata before
// construction and retains that charge until all reads/borrows have finished.
func NewDecoder(storage []byte) (*Decoder, error) {
	if len(storage) <= HeaderSize || len(storage)-HeaderSize > MaxPayload {
		return nil, ErrInvalid
	}
	return &Decoder{limit: uint32(len(storage) - HeaderSize), storage: storage[:len(storage):len(storage)]}, nil
}

// Abort releases retained payload and permanently rejects further reads.
// It is not concurrent with Read: cancel the transport's blocking read first.
func (d *Decoder) Abort(reason error) {
	if reason == nil {
		reason = ErrInvalid
	}
	if d.terminal == nil {
		d.terminal = reason
	}
	d.payload = nil
	d.storage = nil
	d.payloadN = 0
	d.headerN = 0
	d.ready = false
	d.parsed = Header{}
}

func transient(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.As(err, &timeout) && timeout.Timeout()
}

// fill never reads beyond the current frame. Invalid Reader counts and zero
// progress are terminal, preventing framing loss and unbounded busy loops.
func fill(r io.Reader, b []byte, n *int) error {
	for *n < len(b) {
		remaining := b[*n:]
		count, err := r.Read(remaining)
		if count < 0 || count > len(remaining) {
			return ErrInvalid
		}
		*n += count
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func (d *Decoder) Read(r io.Reader) (Header, []byte, error) {
	if d == nil || r == nil {
		return Header{}, nil, ErrInvalid
	}
	if d.terminal != nil {
		return Header{}, nil, d.terminal
	}
	if d.limit == 0 || d.limit > MaxPayload || len(d.storage) != HeaderSize+int(d.limit) {
		d.Abort(ErrInvalid)
		return Header{}, nil, d.terminal
	}
	if !d.ready {
		err := fill(r, d.storage[:HeaderSize], &d.headerN)
		if d.headerN < HeaderSize {
			if err != nil && !transient(err) {
				d.Abort(err)
			}
			return Header{}, nil, err
		}
		h, parseErr := ParseHeader(d.storage[:HeaderSize])
		if parseErr != nil || h.PayloadBytes > d.limit {
			d.Abort(ErrInvalid)
			return Header{}, nil, d.terminal
		}
		d.parsed = h
		d.ready = true
		// A fatal read that completed only the header cannot be resumed for payload.
		if err != nil && !transient(err) && h.PayloadBytes != 0 {
			d.Abort(err)
			return Header{}, nil, d.terminal
		}
		end := HeaderSize + int(h.PayloadBytes)
		d.payload = d.storage[HeaderSize:end:end]
		if err != nil {
			if h.PayloadBytes == 0 {
				return d.complete(err)
			}
			return Header{}, nil, err
		}
	}
	err := fill(r, d.payload, &d.payloadN)
	if d.payloadN < len(d.payload) {
		if err != nil && !transient(err) {
			d.Abort(err)
		}
		return Header{}, nil, err
	}
	return d.complete(err)
}

// ReadPacket exposes the same verified frame as one contiguous borrowed packet
// for ingress. It performs no header re-encoding or second full-frame copy.
func (d *Decoder) ReadPacket(r io.Reader) ([]byte, error) {
	h, _, err := d.Read(r)
	if err != nil {
		return nil, err
	}
	end := HeaderSize + int(h.PayloadBytes)
	return d.storage[:end:end], nil
}

func (d *Decoder) complete(readErr error) (Header, []byte, error) {
	if sha256.Sum256(d.payload) != d.parsed.PayloadSHA {
		d.Abort(ErrInvalid)
		return Header{}, nil, d.terminal
	}
	h, p := d.parsed, d.payload
	d.parsed = Header{}
	d.payload = nil
	d.payloadN = 0
	d.headerN = 0
	d.ready = false
	// io.Reader may return bytes and EOF together; return that complete frame
	// once, then preserve the terminal error instead of attempting resync.
	if readErr != nil && !transient(readErr) {
		d.terminal = readErr
	}
	return h, p, nil
}
