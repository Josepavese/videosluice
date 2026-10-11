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

type bootstrapAuthorityFunc func() (string, error)

func (f bootstrapAuthorityFunc) CurrentGeneration() (string, error) { return f() }

func bootstrapPlan(closed *atomic.Int32) fd.BootstrapPlan[int] {
	return fd.BootstrapPlan[int]{Bytes: 4096,
		Create: func(context.Context) (fd.Created[int], fd.ProcessAuthority, error) {
			return fd.Created[int]{Value: 7, Owned: true}, auth(), nil
		}, Release: func(value int) error { closed.Add(1); return nil },
	}
}

func waitBootstrapStats(t *testing.T, r *fd.Runtime, done func(fd.Stats) bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		stats := r.Stats()
		if done(stats) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("bootstrap cleanup not confirmed", stats)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBootstrapReservationSharesActualRuntimeBudget(t *testing.T) {
	l := runtimeLimits()
	l.MaxMemoryBytes, l.MaxSourceBytes = 8192, 4096
	var closed atomic.Int32
	r, resource, err := fd.NewFromBootstrap(context.Background(), testEntropy{}, clock{}, l, bootstrapPlan(&closed))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Revoke()
	if r.Fence().ProcessGeneration != "authenticated-process-1" || r.Stats().ReservedResourceBytes != 4096 {
		t.Fatal("bootstrap not bound/charged", r.Fence(), r.Stats())
	}
	if err = resource.ValidateRuntimeScope(r); err != nil {
		t.Fatal(err)
	}
	b := open(t, r, "front", "analysis", copyport.CPU{})
	if _, err = fd.OwnRuntimeResource(context.Background(), r, intPlan(1, &closed)); !errors.Is(err, fa.CapacityRejected) {
		t.Fatal("bootstrap missing from shared source budget", err, r.Stats())
	}
	b.Close()
	if err = resource.Borrow(context.Background(), func(_ context.Context, value int) error {
		if value != 7 {
			t.Fatal(value)
		}
		return nil
	}); err != nil {
		t.Fatal("camera close retired bootstrap", err)
	}
	r.Revoke()
	awaitResource(t, resource)
	if closed.Load() != 1 || r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal(closed.Load(), r.Stats())
	}
}

func TestBootstrapAdmissionPrecedesProviderCreation(t *testing.T) {
	for _, mode := range []string{"capacity", "invalid_limits", "cancelled", "missing_release", "bad_entropy"} {
		t.Run(mode, func(t *testing.T) {
			l := runtimeLimits()
			var creates, closed atomic.Int32
			p := bootstrapPlan(&closed)
			create := p.Create
			p.Create = func(ctx context.Context) (fd.Created[int], fd.ProcessAuthority, error) {
				creates.Add(1)
				return create(ctx)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := testEntropy{}
			switch mode {
			case "capacity":
				p.Bytes = l.MaxMemoryBytes + 1
			case "invalid_limits":
				l.MaxResources = 0
			case "cancelled":
				cancel()
			case "missing_release":
				p.Release = nil
			case "bad_entropy":
				e.fail = true
			}
			r, resource, err := fd.NewFromBootstrap(ctx, e, clock{}, l, p)
			if err == nil || r != nil || resource != nil || creates.Load() != 0 || closed.Load() != 0 {
				t.Fatal("provider ran before admission", mode, err, creates.Load(), closed.Load())
			}
		})
	}
}

func TestBootstrapFailuresPreserveOwnershipAndNeverPublishUsableDomain(t *testing.T) {
	for _, mode := range []string{"partial_create", "nil_authority", "invalid_generation", "changed", "authority_panic", "final_check_panic", "cancelled_after_create", "create_rollback", "create_panic", "ownership_missing", "release_failed"} {
		t.Run(mode, func(t *testing.T) {
			var closed atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p := bootstrapPlan(&closed)
			p.Create = func(context.Context) (fd.Created[int], fd.ProcessAuthority, error) {
				value := fd.Created[int]{Value: 7, Owned: true}
				a := fd.ProcessAuthority(auth())
				switch mode {
				case "partial_create", "release_failed":
					return value, nil, errors.New("native creation failed")
				case "nil_authority":
					a = nil
				case "invalid_generation":
					a = bootstrapAuthorityFunc(func() (string, error) { return "invalid generation", nil })
				case "changed", "final_check_panic":
					calls := 0
					a = bootstrapAuthorityFunc(func() (string, error) {
						calls++
						if mode == "final_check_panic" && calls == 3 {
							panic("native generation check")
						}
						if mode == "changed" && calls > 1 {
							return "changed", nil
						}
						return "first", nil
					})
				case "authority_panic":
					a = bootstrapAuthorityFunc(func() (string, error) { panic("provider") })
				case "cancelled_after_create":
					cancel()
				case "create_rollback":
					return fd.Created[int]{}, nil, errors.New("confirmed rollback")
				case "create_panic":
					panic("ownership unknown")
				case "ownership_missing":
					return fd.Created[int]{}, a, nil
				}
				return value, a, nil
			}
			if mode == "release_failed" {
				p.Release = func(int) error { closed.Add(1); return errors.New("still owned") }
			}
			r, resource, err := fd.NewFromBootstrap(ctx, testEntropy{}, clock{}, runtimeLimits(), p)
			if err == nil || r == nil || resource != nil || !r.Stats().Revoked {
				t.Fatal("failed bootstrap published", mode, err)
			}
			if mode == "final_check_panic" {
				var failure *fd.BootstrapFailure
				if !errors.As(err, &failure) || failure.Stage != "finalize" {
					t.Fatal("final generation failure lacks stage", err)
				}
			}
			if err = r.ValidateDomain(r.Fence()); err == nil {
				t.Fatal("failed bootstrap still usable")
			}
			var ignored atomic.Int32
			if _, err = fd.OwnRuntimeResource(context.Background(), r, intPlan(1, &ignored)); err == nil {
				t.Fatal("failed runtime admitted work")
			}
			uncertain := mode == "create_panic" || mode == "ownership_missing" || mode == "release_failed"
			if uncertain {
				waitBootstrapStats(t, r, func(s fd.Stats) bool { return s.ResourceFailures == 1 })
				if s := r.Stats(); s.ReservedResourceBytes != 4096 || s.LiveResources != 1 {
					t.Fatal("unconfirmed ownership freed", s)
				}
			} else {
				waitBootstrapStats(t, r, func(s fd.Stats) bool { return s.LiveResources == 0 })
			}
			wantClosed := int32(1)
			if mode == "create_rollback" || mode == "create_panic" || mode == "ownership_missing" {
				wantClosed = 0
			}
			if closed.Load() != wantClosed {
				t.Fatal("wrong close ownership", mode, closed.Load())
			}
		})
	}
}

func TestBootstrapStuckReleaseStaysChargedUntilPhysicalClose(t *testing.T) {
	var closed atomic.Int32
	p := bootstrapPlan(&closed)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	defer finish()
	p.Release = func(int) error { close(entered); <-release; return nil }
	p.Create = func(context.Context) (fd.Created[int], fd.ProcessAuthority, error) {
		return fd.Created[int]{Value: 7, Owned: true}, nil, errors.New("partial native bootstrap")
	}
	r, _, err := fd.NewFromBootstrap(context.Background(), testEntropy{}, clock{}, runtimeLimits(), p)
	if err == nil {
		t.Fatal("partial bootstrap accepted")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("close not started")
	}
	if s := r.Stats(); s.ReservedResourceBytes != 4096 || s.LiveResources != 1 {
		t.Fatal("blocked close uncharged", s)
	}
	finish()
	waitBootstrapStats(t, r, func(s fd.Stats) bool { return s.LiveResources == 0 && s.ReservedMemoryBytes == 0 })
}

func TestBootstrapLaterAuthorityFailureRevokesAndReleases(t *testing.T) {
	var closed atomic.Int32
	var fail atomic.Bool
	p := bootstrapPlan(&closed)
	p.Create = func(context.Context) (fd.Created[int], fd.ProcessAuthority, error) {
		return fd.Created[int]{Value: 7, Owned: true}, bootstrapAuthorityFunc(func() (string, error) {
			if fail.Load() {
				panic("native authority failed after publication")
			}
			return "verified-child", nil
		}), nil
	}
	r, resource, err := fd.NewFromBootstrap(context.Background(), testEntropy{}, clock{}, runtimeLimits(), p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Revoke()
	fence := r.Fence()
	fail.Store(true)
	if err = r.ValidateDomain(fence); !errors.Is(err, fd.ErrProcessChanged) {
		t.Fatal("late authority failure accepted", err)
	}
	awaitResource(t, resource)
	if closed.Load() != 1 || !r.Stats().Revoked || r.Stats().ReservedMemoryBytes != 0 {
		t.Fatal("late failure leaked runtime", r.Stats())
	}
}

func TestBootstrapFailureStageDoesNotExposeProviderPayload(t *testing.T) {
	var closed atomic.Int32
	p := bootstrapPlan(&closed)
	sentinel := errors.New("private-provider-payload")
	p.Create = func(context.Context) (fd.Created[int], fd.ProcessAuthority, error) {
		return fd.Created[int]{}, nil, sentinel
	}
	r, _, err := fd.NewFromBootstrap(context.Background(), testEntropy{}, clock{}, runtimeLimits(), p)
	var failure *fd.BootstrapFailure
	if !errors.As(err, &failure) || failure.Stage != "create" || err.Error() != "frame bootstrap failed: stage=create" || !errors.Is(err, sentinel) {
		t.Fatal("bootstrap stage/cause contract lost", err)
	}
	if r == nil || !r.Stats().Revoked || r.Stats().LiveResources != 0 {
		t.Fatal("rollback incorrectly charged", r)
	}
}
