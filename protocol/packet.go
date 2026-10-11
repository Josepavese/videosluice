// Package protocol defines the stable wire contract between video-broker and
// consumers. The four-byte magic is retained as an opaque compatibility token
// so existing recorded packet data remains readable.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	ProtocolMagic   = "NMB2"
	ProtocolVersion = 1
	HeaderSize      = 32
	MaxPayloadBytes = 16 * 1024 * 1024
	StreamH264      = 1
	StreamNV12      = 2
	StreamH265      = 3
	FlagKeyframe    = 1 << 0
	FlagCodecConfig = 1 << 1
	// FlagStreamReset marks the first decoder-bootstrap packet of a newly
	// attached broker publication. Frame IDs are scoped to that publication and
	// must not be compared with the preceding connection's source-local counter.
	FlagStreamReset = 1 << 2
)

type Header struct {
	StreamID    byte
	Flags       uint16
	FrameID     uint32
	TimestampNS uint64
	Width       uint32
	Height      uint32
	PayloadSize uint32
}

type Packet struct {
	Header  Header
	Payload []byte
}

// PacketDecoder retains partial header and payload bytes across transient read
// errors. Network consumers must keep one decoder for the lifetime of a
// connection: restarting ReadPacket after a timeout can otherwise discard a
// prefix already consumed from the stream and desynchronise framing.
type PacketDecoder struct {
	header      [HeaderSize]byte
	headerRead  int
	parsed      Header
	payload     []byte
	payloadRead int
}

func MarshalPacket(packet Packet) ([]byte, error) {
	if err := ValidatePacket(packet); err != nil {
		return nil, err
	}
	header := packet.Header
	header.PayloadSize = uint32(len(packet.Payload))
	out := make([]byte, HeaderSize+len(packet.Payload))
	copy(out[:4], []byte(ProtocolMagic))
	out[4] = ProtocolVersion
	out[5] = header.StreamID
	binary.BigEndian.PutUint16(out[6:8], header.Flags)
	binary.BigEndian.PutUint32(out[8:12], header.FrameID)
	binary.BigEndian.PutUint64(out[12:20], header.TimestampNS)
	binary.BigEndian.PutUint32(out[20:24], header.Width)
	binary.BigEndian.PutUint32(out[24:28], header.Height)
	binary.BigEndian.PutUint32(out[28:32], header.PayloadSize)
	copy(out[HeaderSize:], packet.Payload)
	return out, nil
}

func ReadPacket(r io.Reader) (Packet, error) {
	return new(PacketDecoder).ReadPacket(r)
}

// ReadPacket returns one bounded packet. If the underlying reader reports a
// timeout after consuming bytes, the error is returned while the consumed
// prefix remains in the decoder for the next call.
func (d *PacketDecoder) ReadPacket(r io.Reader) (Packet, error) {
	if d == nil {
		return Packet{}, errors.New("nil video packet decoder")
	}
	if d.payload == nil {
		if err := readFramedPart(r, d.header[:], &d.headerRead); err != nil {
			return Packet{}, err
		}
		header, err := ParseHeader(d.header[:])
		if err != nil {
			d.Reset()
			return Packet{}, err
		}
		d.parsed = header
		d.payload = make([]byte, int(header.PayloadSize))
	}
	if err := readFramedPart(r, d.payload, &d.payloadRead); err != nil {
		return Packet{}, err
	}
	packet := Packet{Header: d.parsed, Payload: d.payload}
	d.Reset()
	return packet, nil
}

// Reset discards an incomplete frame. It is intended for connection teardown;
// callers must not reset after a retryable timeout on the same stream.
func (d *PacketDecoder) Reset() {
	if d == nil {
		return
	}
	d.header = [HeaderSize]byte{}
	d.headerRead = 0
	d.parsed = Header{}
	d.payload = nil
	d.payloadRead = 0
}

func readFramedPart(r io.Reader, dst []byte, offset *int) error {
	for *offset < len(dst) {
		n, err := r.Read(dst[*offset:])
		if n < 0 || n > len(dst)-*offset {
			return errors.New("invalid framed reader byte count")
		}
		*offset += n
		if *offset == len(dst) {
			return nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) && *offset > 0 {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func WritePacket(w io.Writer, packet Packet) error {
	if err := ValidatePacket(packet); err != nil {
		return err
	}
	header := packet.Header
	header.PayloadSize = uint32(len(packet.Payload))
	var out [HeaderSize]byte
	copy(out[:4], []byte(ProtocolMagic))
	out[4] = ProtocolVersion
	out[5] = header.StreamID
	binary.BigEndian.PutUint16(out[6:8], header.Flags)
	binary.BigEndian.PutUint32(out[8:12], header.FrameID)
	binary.BigEndian.PutUint64(out[12:20], header.TimestampNS)
	binary.BigEndian.PutUint32(out[20:24], header.Width)
	binary.BigEndian.PutUint32(out[24:28], header.Height)
	binary.BigEndian.PutUint32(out[28:32], header.PayloadSize)
	if err := writeFull(w, out[:]); err != nil {
		return err
	}
	if len(packet.Payload) == 0 {
		return nil
	}
	return writeFull(w, packet.Payload)
}

func ParseHeader(header []byte) (Header, error) {
	if len(header) != HeaderSize {
		return Header{}, errors.New("invalid video header size")
	}
	if string(header[:4]) != ProtocolMagic {
		return Header{}, errors.New("invalid video magic")
	}
	if header[4] != ProtocolVersion {
		return Header{}, errors.New("unsupported video protocol version")
	}
	parsed := Header{
		StreamID:    header[5],
		Flags:       binary.BigEndian.Uint16(header[6:8]),
		FrameID:     binary.BigEndian.Uint32(header[8:12]),
		TimestampNS: binary.BigEndian.Uint64(header[12:20]),
		Width:       binary.BigEndian.Uint32(header[20:24]),
		Height:      binary.BigEndian.Uint32(header[24:28]),
		PayloadSize: binary.BigEndian.Uint32(header[28:32]),
	}
	if err := ValidateHeader(parsed); err != nil {
		return Header{}, err
	}
	return parsed, nil
}

func ValidatePacket(packet Packet) error {
	if len(packet.Payload) > MaxPayloadBytes {
		return fmt.Errorf("video packet payload too large: %d", len(packet.Payload))
	}
	header := packet.Header
	header.PayloadSize = uint32(len(packet.Payload))
	return ValidateHeader(header)
}

func ValidateHeader(header Header) error {
	switch header.StreamID {
	case StreamH264, StreamH265, StreamNV12:
	default:
		return fmt.Errorf("unsupported video stream=%d", header.StreamID)
	}
	if header.Width == 0 || header.Height == 0 || header.Width > 8192 || header.Height > 8192 {
		return fmt.Errorf("invalid video dimensions=%dx%d", header.Width, header.Height)
	}
	if header.PayloadSize == 0 {
		return errors.New("empty video packet payload")
	}
	if header.PayloadSize > MaxPayloadBytes {
		return fmt.Errorf("video packet payload too large: %d", header.PayloadSize)
	}
	if header.StreamID == StreamNV12 {
		expected := uint64(header.Width) * uint64(header.Height) * 3 / 2
		if uint64(header.PayloadSize) != expected {
			return fmt.Errorf("invalid NV12 payload size=%d expected=%d", header.PayloadSize, expected)
		}
	}
	return nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
