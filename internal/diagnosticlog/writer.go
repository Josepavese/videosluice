// Package diagnosticlog prevents diagnostic output backpressure from becoming
// media or control-plane backpressure. It has no framework or hardware binding.
package diagnosticlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

const MaxRecordBytes = 32 * 1024

// Writer owns one sink goroutine and a fixed-size queue of immutable records.
// A stalled sink never blocks Write or retains caller-owned pixels/resources.
// Queue exhaustion is accounted explicitly, not retried in the media thread.
type Writer struct {
	mu             sync.Mutex
	queue          chan []byte
	done           chan struct{}
	closed         bool
	dropped        uint64
	errors         uint64
	pendingDropped uint64
	sink           io.Writer
}

type Stats struct {
	Dropped, Errors uint64
	Queued          int
	Closed          bool
}

func New(sink io.Writer, capacity int) (*Writer, error) {
	if sink == nil || capacity < 1 || capacity > 1024 {
		return nil, errors.New("invalid diagnostic sink or queue capacity")
	}
	w := &Writer{sink: sink, queue: make(chan []byte, capacity), done: make(chan struct{})}
	go w.drain()
	return w, nil
}

func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	if len(p) > MaxRecordBytes {
		w.drop()
		return len(p), nil
	}
	// Check capacity before allocating; overload does not produce heap churn.
	if len(w.queue) == cap(w.queue) {
		w.drop()
		return len(p), nil
	}
	record := append([]byte(nil), p...)
	select {
	case w.queue <- record:
	default:
		w.drop()
	}
	return len(p), nil
}

func (w *Writer) drop() { w.dropped++; w.pendingDropped++ }

func (w *Writer) Stats() Stats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Stats{Dropped: w.dropped, Errors: w.errors, Queued: len(w.queue), Closed: w.closed}
}

func (w *Writer) account(err error) {
	if err != nil {
		w.mu.Lock()
		w.errors++
		w.mu.Unlock()
	}
}

func (w *Writer) write(p []byte) error {
	n, err := w.sink.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return err
}

func (w *Writer) reportDrops() {
	w.mu.Lock()
	n := w.pendingDropped
	w.pendingDropped = 0
	w.mu.Unlock()
	if n == 0 {
		return
	}
	err := w.write([]byte(fmt.Sprintf("[video-broker] event=diagnostic_backpressure dropped_records=%d queue_capacity=%d max_record_bytes=%d action=preserve_media_and_control\n", n, cap(w.queue), MaxRecordBytes)))
	w.account(err)
	if err != nil {
		w.mu.Lock()
		w.pendingDropped += n
		w.mu.Unlock()
	}
}

func (w *Writer) drain() {
	defer close(w.done)
	for p := range w.queue {
		w.reportDrops()
		w.account(w.write(p))
	}
	w.reportDrops()
}

// Close seals admission and waits only for the caller's budget. It does not
// close the externally owned sink, kill an in-flight syscall or claim a flush
// succeeded on a stuck sink. At most one bounded queue/goroutine remains until
// the sink returns or its owning process exits.
func (w *Writer) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil diagnostic close context")
	}
	w.mu.Lock()
	if !w.closed {
		w.closed = true
		close(w.queue)
	}
	w.mu.Unlock()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
