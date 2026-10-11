package diagnosticlog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockedSink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	data    bytes.Buffer
}

func (b *blockedSink) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered); <-b.release })
	return b.data.Write(p)
}
func closeWriter(t *testing.T, w *Writer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestBlockedSinkPreservesNonblockingBoundedAdmissionAndReportsDrops(t *testing.T) {
	b := &blockedSink{entered: make(chan struct{}), release: make(chan struct{})}
	w, err := New(b, 2)
	if err != nil {
		t.Fatal(err)
	}
	w.Write([]byte("first\n"))
	<-b.entered
	completed := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			w.Write([]byte("queued\n"))
		}
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		close(b.release)
		t.Fatal("blocked log sink stopped producer")
	}
	if got := w.Stats(); got.Queued != 2 || got.Dropped != 9998 {
		t.Fatalf("unbounded/lost accounting: %+v", got)
	}
	close(b.release)
	closeWriter(t, w)
	if text := b.data.String(); !strings.HasPrefix(text, "first\n") || !strings.Contains(text, "dropped_records=9998") || strings.Count(text, "queued\n") != 2 {
		t.Fatalf("diagnostic output: %q", text)
	}
}

func TestRecordsAreOwnedImmutableAndFIFO(t *testing.T) {
	b := &blockedSink{entered: make(chan struct{}), release: make(chan struct{})}
	w, _ := New(b, 2)
	w.Write([]byte("first\n"))
	<-b.entered
	p := []byte("second\n")
	w.Write(p)
	copy(p, []byte("mutate\n"))
	w.Write([]byte("third\n"))
	close(b.release)
	closeWriter(t, w)
	if got := b.data.String(); got != "first\nsecond\nthird\n" {
		t.Fatal(got)
	}
}

func TestCloseHasCallerBudgetAndCanReconcileSameDrain(t *testing.T) {
	b := &blockedSink{entered: make(chan struct{}), release: make(chan struct{})}
	w, _ := New(b, 1)
	w.Write([]byte("first"))
	<-b.entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("late")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	close(b.release)
	closeWriter(t, w)
}

type failedSink struct{ short bool }

func (b failedSink) Write(p []byte) (int, error) {
	if b.short {
		return 0, nil
	}
	return 0, io.ErrClosedPipe
}
func TestSinkErrorsAndOversizedRecordsAreAccounted(t *testing.T) {
	for _, short := range []bool{true, false} {
		w, _ := New(failedSink{short: short}, 1)
		w.Write(bytes.Repeat([]byte{'x'}, MaxRecordBytes+1))
		w.Write([]byte("normal"))
		closeWriter(t, w)
		if got := w.Stats(); got.Dropped != 1 || got.Errors < 1 {
			t.Fatal(got)
		}
	}
}

func TestConcurrentWritersAndClose(t *testing.T) {
	w, _ := New(io.Discard, 4)
	var jobs sync.WaitGroup
	for i := 0; i < 8; i++ {
		jobs.Add(1)
		go func() {
			defer jobs.Done()
			for n := 0; n < 500; n++ {
				w.Write([]byte("line\n"))
			}
		}()
	}
	jobs.Add(1)
	go func() { defer jobs.Done(); closeWriter(t, w) }()
	jobs.Wait()
	closeWriter(t, w)
}

func TestCompositionRejectsInvalidLimits(t *testing.T) {
	for _, capacity := range []int{0, -1, 1025} {
		if _, err := New(io.Discard, capacity); err == nil {
			t.Fatal(capacity)
		}
	}
	if _, err := New(nil, 1); err == nil {
		t.Fatal("nil sink")
	}
}
