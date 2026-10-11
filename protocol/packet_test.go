package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

type boundedShortWriter struct {
	buf bytes.Buffer
	max int
}

type timeoutAfterChunkError struct{}

func (timeoutAfterChunkError) Error() string   { return "injected read timeout" }
func (timeoutAfterChunkError) Timeout() bool   { return true }
func (timeoutAfterChunkError) Temporary() bool { return true }

type timeoutAfterChunkReader struct {
	data  []byte
	chunk int
}

func (r *timeoutAfterChunkReader) Read(dst []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := r.chunk
	if n <= 0 || n > len(r.data) {
		n = len(r.data)
	}
	if n > len(dst) {
		n = len(dst)
	}
	copy(dst, r.data[:n])
	r.data = r.data[n:]
	return n, timeoutAfterChunkError{}
}

func (w *boundedShortWriter) Write(p []byte) (int, error) {
	if len(p) > w.max {
		p = p[:w.max]
	}
	return w.buf.Write(p)
}

func TestPacketRoundTrip(t *testing.T) {
	want := Packet{
		Header: Header{
			StreamID:    StreamH264,
			Flags:       FlagKeyframe | FlagStreamReset,
			FrameID:     42,
			TimestampNS: 1000,
			Width:       1280,
			Height:      720,
		},
		Payload: []byte{0, 0, 0, 1, 0x65},
	}
	var buf bytes.Buffer
	if err := WritePacket(&buf, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadPacket(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.StreamID != want.Header.StreamID ||
		got.Header.Flags != want.Header.Flags ||
		got.Header.FrameID != want.Header.FrameID ||
		got.Header.TimestampNS != want.Header.TimestampNS ||
		got.Header.Width != want.Header.Width ||
		got.Header.Height != want.Header.Height ||
		!bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("packet mismatch got=%+v want=%+v", got, want)
	}
}

func TestPacketDecoderRetainsPartialBytesAcrossTimeouts(t *testing.T) {
	want := Packet{
		Header: Header{
			StreamID: StreamH265, Flags: FlagKeyframe, FrameID: 77,
			TimestampNS: 123456, Width: 1920, Height: 1080,
		},
		Payload: []byte{0, 0, 0, 1, 0x26, 0x01, 0xaa, 0xbb},
	}
	encoded, err := MarshalPacket(want)
	if err != nil {
		t.Fatal(err)
	}
	want.Header.PayloadSize = uint32(len(want.Payload))
	reader := &timeoutAfterChunkReader{data: encoded, chunk: 1}
	decoder := new(PacketDecoder)
	var got Packet
	for attempts := 0; attempts <= len(encoded); attempts++ {
		got, err = decoder.ReadPacket(reader)
		if err == nil {
			break
		}
		if timeout, ok := err.(interface{ Timeout() bool }); !ok || !timeout.Timeout() {
			t.Fatalf("unexpected decoder error after partial read: %v", err)
		}
	}
	if err != nil {
		t.Fatalf("decoder did not complete packet: %v", err)
	}
	if got.Header != want.Header || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("packet mismatch after timeout recovery got=%+v want=%+v", got, want)
	}
}

func TestWritePacketRejectsOversizeBeforeAllocation(t *testing.T) {
	packet := Packet{Header: Header{StreamID: StreamH264, Width: 1, Height: 1}, Payload: make([]byte, MaxPayloadBytes+1)}
	if err := WritePacket(&bytes.Buffer{}, packet); err == nil {
		t.Fatal("expected oversized packet rejection")
	}
	if _, err := MarshalPacket(packet); err == nil {
		t.Fatal("expected oversized marshal rejection")
	}
}

func TestReadPacketRejectsInvalidNV12Shape(t *testing.T) {
	var header [HeaderSize]byte
	copy(header[:4], ProtocolMagic)
	header[4] = ProtocolVersion
	header[5] = StreamNV12
	binary.BigEndian.PutUint32(header[20:24], 16)
	binary.BigEndian.PutUint32(header[24:28], 16)
	binary.BigEndian.PutUint32(header[28:32], 1)
	if _, err := ReadPacket(bytes.NewReader(header[:])); err == nil {
		t.Fatal("expected invalid NV12 payload shape rejection")
	}
}

func TestWritePacketHandlesShortWrites(t *testing.T) {
	w := &boundedShortWriter{max: 2}
	packet := Packet{Header: Header{StreamID: StreamH264, Width: 1280, Height: 720}, Payload: []byte{0, 0, 1, 0x65}}
	if err := WritePacket(w, packet); err != nil {
		t.Fatalf("WritePacket failed on short writer: %v", err)
	}
	if _, err := ReadPacket(bytes.NewReader(w.buf.Bytes())); err != nil {
		t.Fatalf("short-written packet is invalid: %v", err)
	}
}

func FuzzReadPacketBounded(f *testing.F) {
	f.Add([]byte(ProtocolMagic))
	f.Add(make([]byte, HeaderSize))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxPayloadBytes+HeaderSize+1 {
			data = data[:MaxPayloadBytes+HeaderSize+1]
		}
		_, _ = ReadPacket(bytes.NewReader(data))
	})
}
