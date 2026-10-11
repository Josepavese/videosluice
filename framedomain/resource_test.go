package framedomain_test

import (
	"context"
	"errors"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func awaitResource(t testing.TB, o *fd.Resource[int]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := o.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
}
func awaitNoResources(t testing.TB, r *fd.Runtime) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for r.Stats().LiveResources != 0 {
		if time.Now().After(deadline) {
			t.Fatal("resource retained", r.Stats())
		}
		time.Sleep(time.Millisecond)
	}
}
func intPlan(n int, closed *atomic.Int32) fd.ResourcePlan[int] {
	return fd.ResourcePlan[int]{Bytes: n, Create: func(context.Context) (fd.Created[int], error) { return fd.Created[int]{Value: 7, Owned: true}, nil }, Release: func(v int) error {
		if v != 7 {
			return errors.New("wrong resource")
		}
		closed.Add(1)
		return nil
	}}
}

func TestResourceLateFailureRetainsCallbackCause(t *testing.T) {
	r := bufferRuntime(t, runtimeLimits())
	b := open(t, r, "front", "analysis", copyport.CPU{})
	var released atomic.Int32
	o, err := fd.OwnResource(context.Background(), b, intPlan(16, &released))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { o.Close(); awaitResource(t, o) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cause := errors.New("bounded provider checkpoint")
	err = o.Borrow(ctx, func(context.Context, int) error { cancel(); return cause })
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatalf("cancellation erased callback failure: %v", err)
	}
}

func TestResourceSharesAllMemoryAndReleasesExactlyOnce(t *testing.T) {
	l := runtimeLimits()
	l.MaxMemoryBytes = 8192
	l.MaxSourceBytes = 8192
	r := bufferRuntime(t, l)
	b := open(t, r, "front", "analysis", copyport.CPU{})
	bufferAlloc(t, b, 2048)
	var released atomic.Int32
	o, err := fd.OwnResource(context.Background(), b, intPlan(2048, &released))
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats().ReservedMemoryBytes != 8192 {
		t.Fatal(r.Stats())
	}
	_, err = b.AllocateBuffer(context.Background(), 1)
	code(t, err, fa.CapacityRejected)
	_, err = fd.OwnResource(context.Background(), b, intPlan(1, &released))
	code(t, err, fa.CapacityRejected)
	if err = o.Borrow(context.Background(), func(_ context.Context, v int) error {
		if v != 7 {
			t.Fatal(v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		o.Close()
	}
	awaitResource(t, o)
	if released.Load() != 1 || r.Stats().ReservedResourceBytes != 0 {
		t.Fatal(released.Load(), r.Stats())
	}
	b.Close()
	if r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal(r.Stats())
	}
}

func TestPendingCreateRemainsChargedAfterRetirement(t *testing.T) {
	l := runtimeLimits()
	l.MaxMemoryBytes = 8192
	l.MaxSourceBytes = 8192
	r := bufferRuntime(t, l)
	b := open(t, r, "front", "analysis", copyport.CPU{})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var destroyed atomic.Int32
	done := make(chan error, 1)
	p := intPlan(4096, &destroyed)
	p.Create = func(context.Context) (fd.Created[int], error) {
		close(entered)
		<-release
		return fd.Created[int]{Value: 7, Owned: true}, nil
	}
	go func() { _, err := fd.OwnResource(context.Background(), b, p); done <- err }()
	wait(t, entered)
	b.Close()
	if s := r.Stats(); s.ReservedMemoryBytes != 8192 || s.LiveGenerations != 1 || s.LiveResources != 1 {
		t.Fatal(s)
	}
	_, err := r.Open(context.Background(), layout("rear", "live"), coreLimits(), copyport.CPU{}, admission{}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	code(t, err, fa.CapacityRejected)
	unblock()
	if err = <-done; err == nil {
		t.Fatal("late create accepted")
	}
	awaitNoResources(t, r)
	if destroyed.Load() != 1 || r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal(destroyed.Load(), r.Stats())
	}
}

func TestStalledBorrowAndCloseRetainTheOldGeneration(t *testing.T) {
	l := runtimeLimits()
	l.MaxResourceOperations = 1
	l.MaxMemoryBytes = 8192
	l.MaxSourceBytes = 8192
	r := bufferRuntime(t, l)
	b := open(t, r, "front", "analysis", copyport.CPU{})
	entered, copyRelease, closeEntered, closeRelease := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var copyOnce, closeOnce sync.Once
	defer copyOnce.Do(func() { close(copyRelease) })
	defer closeOnce.Do(func() { close(closeRelease) })
	var destroyed atomic.Int32
	p := intPlan(4096, &destroyed)
	p.Release = func(int) error { close(closeEntered); <-closeRelease; destroyed.Add(1); return nil }
	o, err := fd.OwnResource(context.Background(), b, p)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- o.Borrow(context.Background(), func(ctx context.Context, _ int) error { close(entered); <-copyRelease; return nil })
	}()
	wait(t, entered)
	code(t, o.Borrow(context.Background(), func(context.Context, int) error { return nil }), fa.CapacityRejected)
	b.Close()
	o.Close()
	select {
	case <-closeEntered:
		t.Fatal("destroyed while in use")
	default:
	}
	if r.Stats().ReservedMemoryBytes != 8192 {
		t.Fatal(r.Stats())
	}
	copyOnce.Do(func() { close(copyRelease) })
	code(t, <-done, fd.ErrRetired)
	wait(t, closeEntered)
	if s := r.Stats(); s.ReservedMemoryBytes != 8192 || s.LiveGenerations != 1 {
		t.Fatal(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	code(t, o.WaitClosed(ctx), context.Canceled)
	if r.Stats().ReservedMemoryBytes != 8192 {
		t.Fatal("wait cancellation uncharged resources")
	}
	closeOnce.Do(func() { close(closeRelease) })
	awaitResource(t, o)
	if destroyed.Load() != 1 || r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal(destroyed.Load(), r.Stats())
	}
}

func TestCreationErrorRollsBackButPanicCannotClaimRelease(t *testing.T) {
	for _, panicCreate := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panicCreate], func(t *testing.T) {
			r := bufferRuntime(t, runtimeLimits())
			b := open(t, r, "front", "analysis", copyport.CPU{})
			var destroyed atomic.Int32
			p := intPlan(4096, &destroyed)
			failed := errors.New("injected failed allocation with rollback")
			p.Create = func(context.Context) (fd.Created[int], error) {
				if panicCreate {
					panic("unknown partial allocation")
				}
				return fd.Created[int]{}, failed
			}
			_, err := fd.OwnResource(context.Background(), b, p)
			if panicCreate {
				code(t, err, fa.ProviderUnavailable)
			} else {
				code(t, err, failed)
			}
			b.Close()
			if destroyed.Load() != 0 {
				t.Fatal("destroyed nonexistent value")
			}
			s := r.Stats()
			if panicCreate {
				if s.ResourceFailures != 1 || s.ReservedMemoryBytes != 8192 {
					t.Fatal(s)
				}
			} else if s.ResourceFailures != 0 || s.ReservedMemoryBytes != 0 {
				t.Fatal(s)
			}
		})
	}
}

func TestFailedReleaseStaysChargedAndNeverRetries(t *testing.T) {
	for _, panicClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panicClose], func(t *testing.T) {
			l := runtimeLimits()
			l.MaxResources = 1
			r := bufferRuntime(t, l)
			b := open(t, r, "front", "analysis", copyport.CPU{})
			var attempts atomic.Int32
			p := intPlan(4096, &attempts)
			p.Release = func(int) error {
				attempts.Add(1)
				if panicClose {
					panic("release unknown")
				}
				return errors.New("release not confirmed")
			}
			o, err := fd.OwnResource(context.Background(), b, p)
			if err != nil {
				t.Fatal(err)
			}
			o.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			code(t, o.WaitClosed(ctx), fa.ProviderUnavailable)
			for i := 0; i < 50; i++ {
				o.Close()
			}
			b.Close()
			if s := r.Stats(); attempts.Load() != 1 || s.ReservedMemoryBytes != 8192 || s.ResourceFailures != 1 || s.LiveGenerations != 1 {
				t.Fatal(attempts.Load(), s)
			}
		})
	}
}

func TestResourceCardinalityInvalidPlanAndBorrowPanic(t *testing.T) {
	l := runtimeLimits()
	l.MaxResources = 1
	r := bufferRuntime(t, l)
	b := open(t, r, "front", "analysis", copyport.CPU{})
	var destroyed atomic.Int32
	_, err := fd.OwnResource[int](context.Background(), b, fd.ResourcePlan[int]{})
	code(t, err, fd.ErrInvalid)
	o, err := fd.OwnResource(context.Background(), b, intPlan(1, &destroyed))
	if err != nil {
		t.Fatal(err)
	}
	_, err = fd.OwnResource(context.Background(), b, intPlan(1, &destroyed))
	code(t, err, fa.CapacityRejected)
	func() {
		defer func() {
			if recover() != "borrow panic" {
				t.Fatal("panic lost")
			}
		}()
		_ = o.Borrow(context.Background(), func(context.Context, int) error { panic("borrow panic") })
	}()
	o.Close()
	awaitResource(t, o)
	if destroyed.Load() != 1 {
		t.Fatal(destroyed.Load())
	}
	var empty *fd.Resource[int]
	empty.Close()
	code(t, empty.WaitClosed(context.Background()), fd.ErrInvalid)
}

func TestPartialCreationKeepsAdmissionUntilConfirmedRelease(t *testing.T) {
	for _, releaseFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "released", true: "quarantined"}[releaseFails], func(t *testing.T) {
			l := runtimeLimits()
			l.MaxResources = 1
			r := bufferRuntime(t, l)
			b := open(t, r, "front", "analysis", copyport.CPU{})
			failed := errors.New("partial create")
			entered, unblock := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(unblock) })
			var attempts atomic.Int32
			p := intPlan(4096, &attempts)
			p.Create = func(context.Context) (fd.Created[int], error) {
				return fd.Created[int]{Value: 7, Owned: true}, failed
			}
			p.Release = func(v int) error {
				attempts.Add(1)
				close(entered)
				<-unblock
				if v != 7 {
					return errors.New("wrong owner")
				}
				if releaseFails {
					return errors.New("unmap failed")
				}
				return nil
			}
			o, err := fd.OwnResource(context.Background(), b, p)
			code(t, err, failed)
			if o != nil {
				t.Fatal("partial resource published")
			}
			wait(t, entered)
			_, err = fd.OwnResource(context.Background(), b, p)
			code(t, err, fa.CapacityRejected)
			b.Close()
			if s := r.Stats(); s.ReservedMemoryBytes != 8192 || s.LiveGenerations != 1 {
				t.Fatal(s)
			}
			once.Do(func() { close(unblock) })
			if releaseFails {
				deadline := time.Now().Add(time.Second)
				for r.Stats().ResourceFailures == 0 {
					if time.Now().After(deadline) {
						t.Fatal("cleanup not reported")
					}
					time.Sleep(time.Millisecond)
				}
				if s := r.Stats(); s.ReservedMemoryBytes != 8192 || s.LiveResources != 1 || s.LiveGenerations != 1 {
					t.Fatal(s)
				}
			} else {
				awaitNoResources(t, r)
				if r.Stats().ReservedMemoryBytes != 0 {
					t.Fatal(r.Stats())
				}
			}
			if attempts.Load() != 1 {
				t.Fatal(attempts.Load())
			}
		})
	}
}

func TestMissingCreationOwnershipFailsClosed(t *testing.T) {
	r := bufferRuntime(t, runtimeLimits())
	b := open(t, r, "front", "analysis", copyport.CPU{})
	var destroyed atomic.Int32
	p := intPlan(4096, &destroyed)
	p.Create = func(context.Context) (fd.Created[int], error) { return fd.Created[int]{}, nil }
	_, err := fd.OwnResource(context.Background(), b, p)
	code(t, err, fd.ErrInvalid)
	var failure *fd.ResourceFailure
	if !errors.As(err, &failure) || failure.Stage != "create_ownership_missing" {
		t.Fatal(err)
	}
	b.Close()
	if s := r.Stats(); s.ResourceFailures != 1 || s.ReservedMemoryBytes != 8192 || destroyed.Load() != 0 {
		t.Fatal(s)
	}
}
