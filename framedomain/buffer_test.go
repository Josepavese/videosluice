package framedomain_test

import (
	"context"
	"errors"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	"math"
	"sync"
	"testing"
)

func bufferRuntime(t testing.TB, limit fd.Limits) *fd.Runtime {
	t.Helper()
	r, err := fd.New(auth(), testEntropy{}, clock{}, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Revoke)
	return r
}
func bufferAlloc(t testing.TB, b *fd.Binding, n int) *fd.Buffer {
	t.Helper()
	buffer, err := b.AllocateBuffer(context.Background(), n)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(buffer.Close)
	return buffer
}

func TestBufferLateFailureRetainsCallbackCause(t *testing.T) {
	r := bufferRuntime(t, runtimeLimits())
	b := open(t, r, "front", "analysis", copyport.CPU{})
	buffer := bufferAlloc(t, b, 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cause := errors.New("bounded callback checkpoint")
	err := buffer.Borrow(ctx, func(context.Context, []byte) error { cancel(); return cause })
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatalf("cancellation erased callback failure: %v", err)
	}
}

func TestBufferRetirementRetainsCallbackCause(t *testing.T) {
	r := bufferRuntime(t, runtimeLimits())
	b := open(t, r, "front", "analysis", copyport.CPU{})
	buffer := bufferAlloc(t, b, 16)
	cause := errors.New("bounded pre-retirement buffer checkpoint")
	err := buffer.Borrow(context.Background(), func(context.Context, []byte) error {
		buffer.Close()
		if r.Stats().ReservedBufferBytes != 16 {
			t.Fatal("retired callback lost its charged buffer", r.Stats())
		}
		return cause
	})
	if !errors.Is(err, fd.ErrRetired) || !errors.Is(err, cause) {
		t.Fatalf("buffer retirement erased callback failure: %v", err)
	}
	if r.Stats().ReservedBufferBytes != 0 || r.Stats().LiveBuffers != 0 {
		t.Fatal("returned callback retained its buffer", r.Stats())
	}
}

func TestBufferProcessFenceRetainsCallbackCause(t *testing.T) {
	a := auth()
	r, err := fd.New(a, testEntropy{}, clock{}, runtimeLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.Revoke)
	b := open(t, r, "front", "analysis", copyport.CPU{})
	buffer := bufferAlloc(t, b, 16)
	cause := errors.New("bounded pre-revocation buffer checkpoint")
	err = buffer.Borrow(context.Background(), func(context.Context, []byte) error {
		a.value.Store("unavailable")
		return cause
	})
	if !errors.Is(err, fd.ErrProcessChanged) || !errors.Is(err, cause) {
		t.Fatalf("process fence erased buffer callback failure: %v", err)
	}
	if r.Stats().ReservedBufferBytes != 0 {
		t.Fatal("revoked buffer charge retained", r.Stats())
	}
}

func TestBuffersShareArenaAndPerSourceMemoryBudgets(t *testing.T) {
	l := runtimeLimits()
	l.MaxMemoryBytes = 12288
	l.MaxSourceBytes = 8192
	r := bufferRuntime(t, l)
	front := open(t, r, "front", "analysis", copyport.CPU{})
	buf := bufferAlloc(t, front, 4096)
	_, err := front.AllocateBuffer(context.Background(), 1)
	code(t, err, fa.CapacityRejected) // source limit, despite spare global space
	rear := open(t, r, "rear", "live", copyport.CPU{})
	_, err = rear.AllocateBuffer(context.Background(), 1)
	code(t, err, fa.CapacityRejected) // global arena + packet cap
	if s := r.Stats(); s.ReservedMemoryBytes != 12288 || s.ReservedBufferBytes != 4096 || s.ReservedStoreBytes != 8192 {
		t.Fatal(s)
	}
	buf.Close()
	buf.Close()
	rearBuf := bufferAlloc(t, rear, 4096)
	rearBuf.Close()
	front.Close()
	rear.Close()
	if s := r.Stats(); s.ReservedMemoryBytes != 0 || s.LiveBuffers != 0 || s.LiveGenerations != 0 {
		t.Fatal(s)
	}
}
func TestBufferAdmissionCardinalityCancellationAndBounds(t *testing.T) {
	l := runtimeLimits()
	l.MaxBuffers = 2
	r := bufferRuntime(t, l)
	b := open(t, r, "front", "analysis", copyport.CPU{})
	for _, n := range []int{-1, 0, l.MaxBufferBytes + 1, math.MaxInt} {
		_, err := b.AllocateBuffer(context.Background(), n)
		code(t, err, fd.ErrInvalid)
	}
	_, err := b.AllocateBuffer(nil, 1)
	code(t, err, fd.ErrInvalid)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = b.AllocateBuffer(ctx, 1)
	code(t, err, context.Canceled)
	a := bufferAlloc(t, b, 1)
	bufferAlloc(t, b, 1)
	_, err = b.AllocateBuffer(context.Background(), 1)
	code(t, err, fa.CapacityRejected)
	if r.Stats().ReservedBufferBytes != 2 {
		t.Fatal(r.Stats())
	}
	a.Close()
	c := bufferAlloc(t, b, 2)
	err = c.Borrow(context.Background(), func(_ context.Context, p []byte) error {
		if len(p) != 2 || p[0] != 0 || p[1] != 0 {
			t.Fatal("uninitialized or incorrect buffer")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal(r.Stats())
	}
	_, err = b.AllocateBuffer(context.Background(), 1)
	code(t, err, fd.ErrRetired)
}
func TestRetiredSourceKeepsBufferAndArenaChargedUntilActualReturn(t *testing.T) {
	l := runtimeLimits()
	l.MaxSourceBytes = 8192
	r := bufferRuntime(t, l)
	b := open(t, r, "front", "analysis", copyport.CPU{})
	buf := bufferAlloc(t, b, 4096)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- buf.Borrow(context.Background(), func(ctx context.Context, p []byte) error {
			p[0] = 91
			close(entered)
			<-release
			if p[0] != 91 {
				return errors.New("retired buffer reused while pinned")
			}
			return nil
		})
	}()
	wait(t, entered)
	code(t, buf.Borrow(context.Background(), func(context.Context, []byte) error { return nil }), fa.CapacityRejected)
	buf.Close()
	b.Close()
	rear := open(t, r, "rear", "live", copyport.CPU{})
	_, err := r.Open(context.Background(), layout("front", "analysis"), coreLimits(), copyport.CPU{}, admission{}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	code(t, err, fa.CapacityRejected) // same source across epochs, global has room
	if s := r.Stats(); s.ReservedMemoryBytes != 12288 || s.ReservedBufferBytes != 4096 || s.LiveGenerations != 2 {
		t.Fatal(s)
	}
	close(release)
	code(t, <-done, fd.ErrRetired)
	if s := r.Stats(); s.ReservedMemoryBytes != 4096 || s.LiveBuffers != 0 {
		t.Fatal(s)
	}
	next := open(t, r, "front", "analysis", copyport.CPU{})
	if next.Scope().MediaEpoch <= b.Scope().MediaEpoch {
		t.Fatal("reused epoch")
	}
	next.Close()
	rear.Close()
}
func TestBufferRevokeCancelsButDoesNotPrematurelyUncharge(t *testing.T) {
	r := bufferRuntime(t, runtimeLimits())
	b := open(t, r, "front", "analysis", copyport.CPU{})
	buf := bufferAlloc(t, b, 96)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- buf.Borrow(context.Background(), func(ctx context.Context, _ []byte) error {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return nil
		})
	}()
	wait(t, entered)
	r.Revoke()
	wait(t, canceled)
	if s := r.Stats(); s.ReservedMemoryBytes != 4192 || !s.Revoked {
		t.Fatal(s)
	}
	close(release)
	code(t, <-done, fd.ErrRetired)
	if s := r.Stats(); s.ReservedMemoryBytes != 0 || s.LiveGenerations != 0 {
		t.Fatal(s)
	}
}
func TestBufferErrorsPanicsAndSelfCloseDoNotLeakPins(t *testing.T) {
	r := bufferRuntime(t, runtimeLimits())
	b := open(t, r, "front", "analysis", copyport.CPU{})
	buf := bufferAlloc(t, b, 96)
	code(t, buf.Borrow(nil, func(context.Context, []byte) error { return nil }), fd.ErrInvalid)
	code(t, buf.Borrow(context.Background(), nil), fd.ErrInvalid)
	failed := errors.New("provider failure")
	code(t, buf.Borrow(context.Background(), func(context.Context, []byte) error { return failed }), failed)
	func() {
		defer func() {
			if recover() != "fixture panic" {
				t.Fatal("panic not preserved")
			}
		}()
		_ = buf.Borrow(context.Background(), func(context.Context, []byte) error { panic("fixture panic") })
	}()
	code(t, buf.Borrow(context.Background(), func(_ context.Context, p []byte) error {
		buf.Close()
		if r.Stats().ReservedBufferBytes != 96 {
			t.Fatal("prematurely uncharged")
		}
		p[0] = 1
		return nil
	}), fd.ErrRetired)
	if r.Stats().ReservedBufferBytes != 0 {
		t.Fatal(r.Stats())
	}
	b.Close()
	if r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal(r.Stats())
	}
}
func TestBufferRepeatedSourceRetirementReleasesAllPayloads(t *testing.T) {
	r := bufferRuntime(t, runtimeLimits())
	for i := 0; i < 1000; i++ {
		b := open(t, r, "front", "analysis", copyport.CPU{})
		buf, err := b.AllocateBuffer(context.Background(), 32)
		if err != nil {
			t.Fatal(err)
		}
		b.Close()
		buf.Close()
		if s := r.Stats(); s.ReservedMemoryBytes != 0 || s.LiveBuffers != 0 || s.LiveGenerations != 0 || s.KnownSources != 1 {
			t.Fatal(s)
		}
	}
}

func TestConcurrentMultiSourceBufferAdmissionCannotOversubscribe(t *testing.T) {
	l := runtimeLimits()
	l.MaxSourceBytes = 8192
	r := bufferRuntime(t, l)
	bindings := []*fd.Binding{open(t, r, "front", "analysis", copyport.CPU{}), open(t, r, "rear", "live", copyport.CPU{})}
	type outcome struct {
		buffer *fd.Buffer
		err    error
	}
	replies := make(chan outcome, 128)
	var workers sync.WaitGroup
	for i := 0; i < 128; i++ {
		workers.Add(1)
		go func(b *fd.Binding) {
			defer workers.Done()
			v, err := b.AllocateBuffer(context.Background(), 4096)
			replies <- outcome{v, err}
		}(bindings[i%2])
	}
	workers.Wait()
	close(replies)
	var held []*fd.Buffer
	for reply := range replies {
		if reply.err != nil {
			code(t, reply.err, fa.CapacityRejected)
		} else {
			held = append(held, reply.buffer)
		}
	}
	if s := r.Stats(); len(held) != 2 || s.ReservedMemoryBytes != 16384 || s.ReservedBufferBytes != 8192 || s.LiveBuffers != 2 {
		t.Fatal(len(held), s)
	}
	for _, buf := range held {
		buf.Close()
	}
	for _, b := range bindings {
		b.Close()
	}
	if r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal(r.Stats())
	}
}

func TestMemoryLimitsRequireExplicitValidBounds(t *testing.T) {
	for _, change := range []func(*fd.Limits){
		func(l *fd.Limits) { l.MaxMemoryBytes = 0 }, func(l *fd.Limits) { l.MaxMemoryBytes = math.MaxInt },
		func(l *fd.Limits) { l.MaxSourceBytes = 0 }, func(l *fd.Limits) { l.MaxSourceBytes = l.MaxMemoryBytes + 1 },
		func(l *fd.Limits) { l.MaxBuffers = 0 }, func(l *fd.Limits) { l.MaxBuffers = 1025 },
		func(l *fd.Limits) { l.MaxBufferBytes = 0 }, func(l *fd.Limits) { l.MaxBufferBytes = l.MaxSourceBytes + 1 },
	} {
		l := runtimeLimits()
		change(&l)
		_, err := fd.New(auth(), testEntropy{}, clock{}, l)
		code(t, err, fd.ErrInvalid)
	}
}

func TestBufferCanceledLateResultAndUninitializedHandles(t *testing.T) {
	for _, buf := range []*fd.Buffer{nil, {}} {
		code(t, buf.Borrow(context.Background(), func(context.Context, []byte) error { return nil }), fd.ErrInvalid)
		buf.Close()
	}
	r := bufferRuntime(t, runtimeLimits())
	b := open(t, r, "front", "analysis", copyport.CPU{})
	buf := bufferAlloc(t, b, 96)
	ctx, cancel := context.WithCancel(context.Background())
	code(t, buf.Borrow(ctx, func(context.Context, []byte) error { cancel(); return nil }), context.Canceled)
	invoked := false
	code(t, buf.Borrow(ctx, func(context.Context, []byte) error { invoked = true; return nil }), context.Canceled)
	if invoked {
		t.Fatal("canceled callback invoked")
	}
	buf.Close()
	code(t, buf.Borrow(context.Background(), func(context.Context, []byte) error { return nil }), fd.ErrRetired)
	b.Close()
	if r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal(r.Stats())
	}
}
