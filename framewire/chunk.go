package envelope

import (
	"encoding/binary"
	"errors"
)

const DataHeaderSize = 44
const MaxDataPacketBytes = 16384
const MaxDataExtent = 1 << 28

var ErrBuffer = errors.New("causal data workspace too small")

// DataChunk is transport framing only. The authenticated preceding manifest
// supplies its identity and meaning (source VBCF or consumer FAPM), not this
// payload. Parsing never authenticates, allocates pixels or invents provenance.
type DataChunk struct {
	Manifest [32]byte
	Offset   uint32
	Payload  []byte
}

func EncodeDataChunk(dst []byte, manifest [32]byte, offset int, payload []byte) (int, error) {
	if manifest == ([32]byte{}) || offset < 0 || offset > MaxDataExtent || len(payload) < 1 || len(payload) > MaxDataPacketBytes-DataHeaderSize || len(payload) > MaxDataExtent-offset {
		return 0, ErrInvalid
	}
	n := DataHeaderSize + len(payload)
	if len(dst) < n {
		return 0, ErrBuffer
	}
	copy(dst[:4], "FAPD")
	dst[4], dst[5] = 1, 0
	binary.BigEndian.PutUint16(dst[6:8], uint16(n))
	copy(dst[8:40], manifest[:])
	binary.BigEndian.PutUint32(dst[40:44], uint32(offset))
	copy(dst[DataHeaderSize:n], payload)
	return n, nil
}

// ParseDataChunk borrows packet storage until the caller reuses it.
func ParseDataChunk(packet []byte) (DataChunk, error) {
	if len(packet) <= DataHeaderSize || len(packet) > MaxDataPacketBytes || string(packet[:4]) != "FAPD" || packet[4] != 1 || packet[5] != 0 || int(binary.BigEndian.Uint16(packet[6:8])) != len(packet) {
		return DataChunk{}, ErrInvalid
	}
	var d DataChunk
	copy(d.Manifest[:], packet[8:40])
	d.Offset = binary.BigEndian.Uint32(packet[40:44])
	if d.Manifest == ([32]byte{}) || uint64(d.Offset)+uint64(len(packet)-DataHeaderSize) > MaxDataExtent {
		return DataChunk{}, ErrInvalid
	}
	d.Payload = packet[DataHeaderSize:len(packet):len(packet)]
	return d, nil
}
