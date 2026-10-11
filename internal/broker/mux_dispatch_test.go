package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"
)

func muxTestRecord(streamID byte, payload []byte) []byte {
	record := make([]byte, 5+len(payload))
	record[0] = streamID
	binary.BigEndian.PutUint32(record[1:5], uint32(len(payload)))
	copy(record[5:], payload)
	return record
}

func TestMuxDispatchBudgetAndOwnership(t *testing.T) {
	q := newBrokerMuxDispatchQueue(3, 10)
	first := brokerMuxDispatchFrame{streamID: brokerStreamH264, payload: make([]byte, 6)}
	second := brokerMuxDispatchFrame{streamID: brokerStreamH265, payload: make([]byte, 5)}
	if !q.enqueue(first) {
		t.Fatal("first frame rejected")
	}
	if q.enqueue(second) {
		t.Fatal("byte budget admitted an archive frame beyond capacity")
	}
	got := <-q.frames
	if got.streamID != first.streamID {
		t.Fatal("mux order changed")
	}
	q.complete(got, false)
	if !q.enqueue(second) {
		t.Fatal("released budget was not reusable")
	}
	q.complete(<-q.frames, false)
	if q.bytes != 0 {
		t.Fatalf("retained mux bytes: %d", q.bytes)
	}
}

func TestMuxDispatchCountsPooledAllocationNotWireLength(t *testing.T) {
	q := newBrokerMuxDispatchQueue(3, 2*64*1024)
	frames := []brokerMuxDispatchFrame{
		{streamID: brokerStreamH264, payload: acquireBrokerMuxPayload(2)},
		{streamID: brokerStreamH265, payload: acquireBrokerMuxPayload(2)},
		{streamID: brokerStreamH264, payload: acquireBrokerMuxPayload(2)},
	}
	if !q.enqueue(frames[0]) || !q.enqueue(frames[1]) || q.enqueue(frames[2]) {
		t.Fatal("pooled allocation quota was not enforced")
	}
	releaseBrokerMuxPayload(frames[2].payload)
	for i := 0; i < 2; i++ {
		q.complete(<-q.frames, false)
	}
	if q.bytes != 0 {
		t.Fatalf("retained mux allocation: %d", q.bytes)
	}
}

func TestMuxReaderDrainsPipeWithoutDispatch(t *testing.T) {
	const frames = 96
	q := newBrokerMuxDispatchQueue(frames, frames*64*1024)
	reader, writer := io.Pipe()
	readDone := make(chan error, 1)
	writeDone := make(chan error, 1)
	go func() { readDone <- readBrokerMuxFrames(context.Background(), reader, q) }()
	go func() {
		var writeErr error
		for i := 0; i < frames && writeErr == nil; i++ {
			stream := byte(brokerStreamH264)
			if i%3 == 0 {
				stream = brokerStreamH265
			}
			_, writeErr = writer.Write(muxTestRecord(stream, bytes.Repeat([]byte{byte(i)}, 4096)))
		}
		_ = writer.Close()
		writeDone <- writeErr
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("native pipe writer waited for a blocked dispatcher")
	}
	if err := <-readDone; !errors.Is(err, io.EOF) {
		t.Fatalf("reader result: %v", err)
	}
	count := 0
	for frame := range q.frames {
		if frame.payload[0] != byte(count) {
			t.Fatalf("mux order changed at %d", count)
		}
		q.complete(frame, false)
		count++
	}
	if count != frames || q.bytes != 0 {
		t.Fatalf("incomplete drain count=%d bytes=%d", count, q.bytes)
	}
}

func TestMuxReaderFailsClosedWhenDispatchBudgetExhausted(t *testing.T) {
	input := append(muxTestRecord(brokerStreamH264, []byte{1, 2}),
		muxTestRecord(brokerStreamH265, []byte{3, 4})...)
	q := newBrokerMuxDispatchQueue(1, 64*1024)
	err := readBrokerMuxFrames(context.Background(), bytes.NewReader(input), q)
	if !errors.Is(err, errBrokerMuxDispatchOverflow) {
		t.Fatalf("expected explicit backpressure, got %v", err)
	}
	count := 0
	for frame := range q.frames {
		q.complete(frame, false)
		count++
	}
	if count != 1 || q.bytes != 0 {
		t.Fatalf("overflow retained frames count=%d bytes=%d", count, q.bytes)
	}
}

func TestMuxReaderRejectsUnknownStreamBeforeAllocation(t *testing.T) {
	q := newBrokerMuxDispatchQueue(1, 64*1024)
	err := readBrokerMuxFrames(context.Background(),
		bytes.NewReader(muxTestRecord(255, []byte{1})), q)
	if err == nil || q.bytes != 0 {
		t.Fatalf("unknown mux stream was not rejected: err=%v bytes=%d", err, q.bytes)
	}
	if _, open := <-q.frames; open {
		t.Fatal("unknown stream entered the dispatch queue")
	}
}
