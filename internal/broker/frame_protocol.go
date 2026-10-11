package broker

import (
	"encoding/binary"
	"time"

	videoipc "github.com/Josepavese/videosluice/protocol"
)

const (
	brokerProtocolMagic           = videoipc.ProtocolMagic
	brokerProtocolVersion         = videoipc.ProtocolVersion
	brokerPacketHeaderSize        = videoipc.HeaderSize
	brokerFlagKeyframe     uint16 = videoipc.FlagKeyframe
	brokerFlagCodecConfig  uint16 = videoipc.FlagCodecConfig
	brokerFlagStreamReset  uint16 = videoipc.FlagStreamReset
)

type brokerPacketHeader = videoipc.Header

func marshalBrokerPacket(header brokerPacketHeader, payload []byte) []byte {
	packet, err := videoipc.MarshalPacket(videoipc.Packet{Header: header, Payload: payload})
	if err != nil {
		return nil
	}
	return packet
}

func parseBrokerPacketHeader(header []byte) (brokerPacketHeader, error) {
	return videoipc.ParseHeader(header)
}

func brokerPacketWithAdditionalFlags(packet []byte, flags uint16) []byte {
	if len(packet) < brokerPacketHeaderSize {
		return nil
	}
	header, err := parseBrokerPacketHeader(packet[:brokerPacketHeaderSize])
	if err != nil || int(header.PayloadSize) != len(packet)-brokerPacketHeaderSize {
		return nil
	}
	header.Flags |= flags
	return marshalBrokerPacket(header, packet[brokerPacketHeaderSize:])
}

func decodeBrokerMuxPayloadSize(header []byte) uint32 {
	return binary.BigEndian.Uint32(header)
}

func brokerTimestampNow() uint64 {
	return uint64(time.Now().UnixNano())
}

func brokerH264Flags(payload []byte) uint16 {
	return brokerH26xFlags(brokerStreamH264, payload)
}

func brokerH26xFlags(streamID byte, payload []byte) uint16 {
	hasIDR, hasConfig := brokerH26xNALPresence(streamID, payload)
	var flags uint16
	if hasIDR {
		flags |= brokerFlagKeyframe
	}
	if hasConfig {
		flags |= brokerFlagCodecConfig
	}
	return flags
}

func brokerH264NALPresence(payload []byte) (hasIDR bool, hasConfig bool) {
	return brokerH26xNALPresence(brokerStreamH264, payload)
}

func brokerH26xNALPresence(streamID byte, payload []byte) (hasIDR bool, hasConfig bool) {
	for i := 0; i+4 < len(payload); i++ {
		startCodeLen := 0
		if payload[i] == 0 && payload[i+1] == 0 && payload[i+2] == 1 {
			startCodeLen = 3
		} else if i+5 < len(payload) && payload[i] == 0 && payload[i+1] == 0 && payload[i+2] == 0 && payload[i+3] == 1 {
			startCodeLen = 4
		}
		if startCodeLen == 0 || i+startCodeLen >= len(payload) {
			continue
		}
		nalType := payload[i+startCodeLen] & 0x1F
		if streamID == brokerStreamH265 {
			nalType = (payload[i+startCodeLen] >> 1) & 0x3F
			if nalType >= 16 && nalType <= 23 {
				hasIDR = true
			}
			if nalType == 32 || nalType == 33 || nalType == 34 {
				hasConfig = true
			}
		} else {
			if nalType == 5 {
				hasIDR = true
			}
			if nalType == 7 || nalType == 8 {
				hasConfig = true
			}
		}
		if hasIDR && hasConfig {
			return hasIDR, hasConfig
		}
	}
	return hasIDR, hasConfig
}

type brokerAnnexBNALRange struct {
	start    int
	nalStart int
	end      int
}

func brokerAnnexBNALRanges(payload []byte) []brokerAnnexBNALRange {
	ranges := make([]brokerAnnexBNALRange, 0, 8)
	starts := make([]brokerAnnexBNALRange, 0, 8)
	for offset := 0; offset+3 <= len(payload); {
		startCodeLength := 0
		if offset+4 <= len(payload) &&
			payload[offset] == 0 && payload[offset+1] == 0 &&
			payload[offset+2] == 0 && payload[offset+3] == 1 {
			startCodeLength = 4
		} else if payload[offset] == 0 && payload[offset+1] == 0 && payload[offset+2] == 1 {
			startCodeLength = 3
		}
		if startCodeLength == 0 {
			offset++
			continue
		}
		starts = append(starts, brokerAnnexBNALRange{
			start: offset, nalStart: offset + startCodeLength,
		})
		offset += startCodeLength
	}
	for index := range starts {
		entry := starts[index]
		entry.end = len(payload)
		if index+1 < len(starts) {
			entry.end = starts[index+1].start
		}
		if entry.nalStart < entry.end {
			ranges = append(ranges, entry)
		}
	}
	return ranges
}

func brokerH26xCodecConfigPayload(streamID byte, payload []byte) []byte {
	if streamID != brokerStreamH264 && streamID != brokerStreamH265 {
		return nil
	}
	var config []byte
	for _, nal := range brokerAnnexBNALRanges(payload) {
		nalType := payload[nal.nalStart] & 0x1f
		isConfig := nalType == 7 || nalType == 8
		if streamID == brokerStreamH265 {
			nalType = (payload[nal.nalStart] >> 1) & 0x3f
			isConfig = nalType == 32 || nalType == 33 || nalType == 34
		}
		if isConfig {
			config = append(config, payload[nal.start:nal.end]...)
		}
	}
	return config
}

func brokerCodecConfigPacket(packet []byte) []byte {
	if len(packet) < brokerPacketHeaderSize {
		return nil
	}
	header, err := parseBrokerPacketHeader(packet[:brokerPacketHeaderSize])
	if err != nil || int(header.PayloadSize) != len(packet)-brokerPacketHeaderSize {
		return nil
	}
	payload := brokerH26xCodecConfigPayload(
		header.StreamID,
		packet[brokerPacketHeaderSize:],
	)
	if len(payload) == 0 {
		return nil
	}
	header.Flags = brokerFlagCodecConfig
	return marshalBrokerPacket(header, payload)
}
