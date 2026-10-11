package broker

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// The analysis shared frame is a published contract: a write that does not
// happen must never be reported as success, because the caller then notifies
// the consumer with the new frame's ID and timestamp while the shared memory
// still holds the previous frame's pixels. The write metric would also count a
// write that never happened, so the failure would be invisible everywhere.
func TestBrokerSubSharedFrameWriteReportsWhatItDidNotWrite(t *testing.T) {
	const capacity = 1024
	path := filepath.Join(t.TempDir(), "analysis-shm")
	frame, err := newBrokerSubSharedFrame(path, capacity)
	if err != nil {
		t.Fatalf("shared frame not created: %v", err)
	}
	defer func() { _ = frame.Close() }()

	valid := make([]byte, capacity)
	if err := frame.Write(7, 1_700_000_000_000_000_000, 32, 16, 0, valid); err != nil {
		t.Fatalf("a payload of exactly the capacity was refused: %v", err)
	}

	oversized := make([]byte, capacity+1)
	if err := frame.Write(8, 1, 32, 16, 0, oversized); err == nil {
		t.Fatal("a payload larger than the capacity was reported as written")
	}

	if err := frame.Write(9, 1, 32, 16, 0, nil); err == nil {
		t.Fatal("an empty payload was reported as written")
	}

	// The failed writes must not have touched the published frame: the consumer
	// would otherwise see a frame ID that was never backed by pixels.
	body, err := readBrokerSubSharedHeaderForTest(path)
	if err != nil {
		t.Fatalf("header not readable: %v", err)
	}
	// frameID sits at header[24:32]; the sequence words at [8:16] and [16:24].
	if got := binary.BigEndian.Uint64(body[24:32]); got != 7 {
		t.Fatalf("published frame ID = %d, want 7: a refused write reached the shared frame", got)
	}

	if err := frame.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := frame.Write(10, 1, 32, 16, 0, valid); err == nil {
		t.Fatal("a write to a closed shared frame was reported as written")
	}
}

// readBrokerSubSharedHeaderForTest reads the header region of the shared frame
// file. The producer writes the header with the same layout the consumers
// validate, so this reads the published state, not an internal buffer.
func readBrokerSubSharedHeaderForTest(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	buffer := make([]byte, 64)
	if _, err := file.ReadAt(buffer, 0); err != nil {
		return nil, err
	}
	return buffer, nil
}
