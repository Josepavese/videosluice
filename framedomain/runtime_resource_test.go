package framedomain_test

import (
	"context"
	"errors"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeResourcesShareGlobalQuotaButNotCameraLifetime(t *testing.T) {
	l := runtimeLimits()
	l.MaxMemoryBytes = 8192
	l.MaxSourceBytes = 4096
	r := bufferRuntime(t, l)
	defer r.Revoke()
	var closed atomic.Int32
	o, err := fd.OwnRuntimeResource(context.Background(), r, intPlan(4096, &closed))
	if err != nil {
		t.Fatal(err)
	}
	b := open(t, r, "front", "analysis", copyport.CPU{})
	if r.Stats().ReservedMemoryBytes != 8192 {
		t.Fatal("runtime control omitted from global budget", r.Stats())
	}
	_, err = fd.OwnRuntimeResource(context.Background(), r, intPlan(1, &closed))
	code(t, err, fa.CapacityRejected)
	b.Close()
	if err = o.Borrow(context.Background(), func(context.Context, int) error { return nil }); err != nil {
		t.Fatal("camera close retired broker-wide resource", err)
	}
	b = open(t, r, "front", "analysis", copyport.CPU{})
	b.Close()
	if r.Stats().ReservedMemoryBytes != 4096 {
		t.Fatal("source restart lost shared reservation", r.Stats())
	}
	o.Close()
	awaitResource(t, o)
	if closed.Load() != 1 || r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal(closed.Load(), r.Stats())
	}
}

func TestRuntimeResourceCardinalityIncludesSourceResources(t *testing.T) {
	l := runtimeLimits()
	l.MaxResources = 2
	r := bufferRuntime(t, l)
	defer r.Revoke()
	b := open(t, r, "front", "analysis", copyport.CPU{})
	defer b.Close()
	var closed atomic.Int32
	one, err := fd.OwnRuntimeResource(context.Background(), r, intPlan(1, &closed))
	if err != nil {
		t.Fatal(err)
	}
	two, err := fd.OwnResource(context.Background(), b, intPlan(1, &closed))
	if err != nil {
		t.Fatal(err)
	}
	_, err = fd.OwnRuntimeResource(context.Background(), r, intPlan(1, &closed))
	code(t, err, fa.CapacityRejected)
	_, err = fd.OwnResource(context.Background(), b, intPlan(1, &closed))
	code(t, err, fa.CapacityRejected)
	one.Close()
	two.Close()
	awaitResource(t, one)
	awaitResource(t, two)
}

func TestRuntimeResourceDoesNotCreateFakeSourceOrBypassSourceQuota(t *testing.T) {
	l := runtimeLimits()
	l.MaxMemoryBytes = 16384
	l.MaxSourceBytes = 4096
	l.MaxSources = 1
	r := bufferRuntime(t, l)
	defer r.Revoke()
	var closed atomic.Int32
	o, err := fd.OwnRuntimeResource(context.Background(), r, intPlan(8192, &closed))
	if err != nil {
		t.Fatal("broker resource incorrectly subject to one camera limit", err)
	}
	b := open(t, r, "front", "analysis", copyport.CPU{})
	defer b.Close()
	_, err = b.AllocateBuffer(context.Background(), 1)
	code(t, err, fa.CapacityRejected)
	if r.Stats().KnownSources != 1 || r.Stats().ReservedMemoryBytes != 12288 {
		t.Fatal("control plane minted a source or lost quota", r.Stats())
	}
	o.Close()
	awaitResource(t, o)
}

func TestRuntimeResourceCreateOwnershipOutcomes(t *testing.T) {
	for _, mode := range []string{"rolled_back", "partial", "missing_ownership", "panic"} {
		t.Run(mode, func(t *testing.T) {
			r, _ := fixture(t)
			defer r.Revoke()
			var closed atomic.Int32
			p := intPlan(1024, &closed)
			p.Create = func(context.Context) (fd.Created[int], error) {
				switch mode {
				case "rolled_back":
					return fd.Created[int]{}, errors.New("rolled back before returning")
				case "partial":
					return fd.Created[int]{Value: 7, Owned: true}, errors.New("partial setup")
				case "missing_ownership":
					return fd.Created[int]{}, nil
				default:
					panic("ownership unknown")
				}
			}
			o, err := fd.OwnRuntimeResource(context.Background(), r, p)
			if err == nil || o != nil {
				t.Fatal("failed create returned usable resource")
			}
			if mode == "rolled_back" || mode == "partial" {
				awaitNoResources(t, r)
				expected := int32(0)
				if mode == "partial" {
					expected = 1
				}
				if closed.Load() != expected || r.Stats().ReservedResourceBytes != 0 {
					t.Fatal("rollback ownership mismatch", r.Stats())
				}
			} else if r.Stats().ResourceFailures != 1 || r.Stats().LiveResources != 1 || r.Stats().ReservedResourceBytes != 1024 {
				t.Fatal("unknown ownership released budget", r.Stats())
			}
		})
	}
}

func TestResourceCloseCancelsBorrowWithoutReleasingItsLiveReservation(t *testing.T) {
	for _, root := range []bool{false, true} {
		t.Run(map[bool]string{false: "source", true: "runtime"}[root], func(t *testing.T) {
			r, _ := fixture(t)
			defer r.Revoke()
			var closed atomic.Int32
			var o *fd.Resource[int]
			var err error
			if root {
				o, err = fd.OwnRuntimeResource(context.Background(), r, intPlan(1024, &closed))
			} else {
				b := open(t, r, "front", "analysis", copyport.CPU{})
				defer b.Close()
				o, err = fd.OwnResource(context.Background(), b, intPlan(1024, &closed))
			}
			if err != nil {
				t.Fatal(err)
			}
			entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- o.Borrow(context.Background(), func(ctx context.Context, _ int) error {
					close(entered)
					<-ctx.Done()
					close(cancelled)
					<-release
					return nil
				})
			}()
			<-entered
			o.Close()
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				close(release)
				t.Fatal("resource retirement did not cancel borrow")
			}
			stats := r.Stats()
			if closed.Load() != 0 || stats.LiveResources != 1 || stats.ReservedResourceBytes != 1024 {
				close(release)
				<-done
				t.Fatal("cancel released live borrowed ownership", stats)
			}
			waitCtx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := o.WaitClosed(waitCtx); !errors.Is(err, context.Canceled) {
				close(release)
				<-done
				t.Fatal(err)
			}
			close(release)
			code(t, <-done, fd.ErrRetired)
			awaitResource(t, o)
			if closed.Load() != 1 {
				t.Fatal("physical close not exactly once")
			}
		})
	}
}

func TestRuntimeRevokeRetainsBlockedCreateAndFailedClose(t *testing.T) {
	r, _ := fixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	var closed atomic.Int32
	p := intPlan(4096, &closed)
	p.Create = func(context.Context) (fd.Created[int], error) {
		close(entered)
		<-release
		return fd.Created[int]{Value: 7, Owned: true}, nil
	}
	p.Release = func(int) error { closed.Add(1); return errors.New("physical release not confirmed") }
	go func() { _, err := fd.OwnRuntimeResource(context.Background(), r, p); done <- err }()
	<-entered
	r.Revoke()
	if r.Stats().ReservedResourceBytes != 4096 {
		close(release)
		<-done
		t.Fatal("pending create uncharged")
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("retired create exposed")
	}
	deadline := time.Now().Add(time.Second)
	for r.Stats().ResourceFailures == 0 {
		if time.Now().After(deadline) {
			t.Fatal("missing release failure")
		}
		time.Sleep(time.Millisecond)
	}
	r.Revoke()
	if closed.Load() != 1 || r.Stats().LiveResources != 1 || r.Stats().ReservedResourceBytes != 4096 {
		t.Fatal("failed resource released/retried", r.Stats())
	}
}
