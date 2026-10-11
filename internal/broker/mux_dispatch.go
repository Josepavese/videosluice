package broker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

// The native bridge writes one ordered mux to a finite pipe. Reading that pipe
// must not wait for packet parsing, status I/O or a client fan-out. This queue
// is source-local and bounded by both bytes and records; an exhausted budget
// retires the source rather than silently omitting H.26x or archive packets.
const (
	brokerMuxDispatchQueueDepth    = 256
	brokerMuxDispatchQueueMaxBytes = 24 * 1024 * 1024
)

var errBrokerMuxDispatchOverflow = errors.New("mux dispatch budget exhausted")

type brokerMuxDispatchFrame struct {
	streamID byte
	payload  []byte
}

type brokerMuxDispatchQueue struct {
	frames   chan brokerMuxDispatchFrame
	maxBytes int
	mu       sync.Mutex
	bytes    int
}

func newBrokerMuxDispatchQueue(depth, maxBytes int) *brokerMuxDispatchQueue {
	return &brokerMuxDispatchQueue{
		frames: make(chan brokerMuxDispatchFrame, depth), maxBytes: maxBytes,
	}
}

func (q *brokerMuxDispatchQueue) enqueue(frame brokerMuxDispatchFrame) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	// Count retained allocation, not wire bytes: pooled slices can reserve a
	// larger class than len(payload), and that RAM remains live until release.
	retained := cap(frame.payload)
	if retained > q.maxBytes-q.bytes {
		return false
	}
	select {
	case q.frames <- frame:
		q.bytes += retained
		return true
	default:
		return false
	}
}

func (q *brokerMuxDispatchQueue) complete(frame brokerMuxDispatchFrame, transferred bool) {
	q.mu.Lock()
	q.bytes -= cap(frame.payload)
	q.mu.Unlock()
	if !transferred {
		releaseBrokerMuxPayload(frame.payload)
	}
}

// The sole producer closes frames. A consumer always calls complete, including
// when the child epoch was fenced while frames were queued.
func readBrokerMuxFrames(ctx context.Context, source io.Reader, q *brokerMuxDispatchQueue) error {
	defer close(q.frames)
	reader := bufio.NewReader(source)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var header [5]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			return err
		}
		size := decodeBrokerMuxPayloadSize(header[1:])
		if size == 0 {
			continue
		}
		switch header[0] {
		case brokerStreamH264, brokerStreamNV12, brokerStreamH265,
			brokerMuxStreamArchiveH264Native, brokerMuxStreamArchiveH264Generated,
			brokerMuxStreamLiveEncoderEpoch:
		default:
			return fmt.Errorf("unsupported mux stream: %d", header[0])
		}
		if size > brokerMuxMaxPayloadBytes {
			return fmt.Errorf("mux payload exceeds limit: %d", size)
		}
		payload := acquireBrokerMuxPayload(int(size))
		if _, err := io.ReadFull(reader, payload); err != nil {
			releaseBrokerMuxPayload(payload)
			return err
		}
		if !q.enqueue(brokerMuxDispatchFrame{streamID: header[0], payload: payload}) {
			releaseBrokerMuxPayload(payload)
			return errBrokerMuxDispatchOverflow
		}
	}
}
