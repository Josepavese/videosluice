package ingress_test

import (
	"context"
	"errors"
	in "github.com/Josepavese/videosluice/frameingress"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sync/atomic"
	"testing"
	"time"
)

type countedController struct {
	calls    atomic.Int32
	panicNow atomic.Bool
}

type panicImportCapability struct{ closes atomic.Int32 }

func (*panicImportCapability) Import(string, []byte) (peer.ProcessCapability, error) {
	panic("private provider payload")
}
func (p *panicImportCapability) Close() error { p.closes.Add(1); return nil }

func TestControlOwnerImportPanicRetainsUncertainAdmission(t *testing.T) {
	f := setup(t)
	resource := ownedControlFixture(t, f)
	pending := &panicImportCapability{}
	err := resource.Borrow(context.Background(), func(ctx context.Context, d *in.ControlDispatcher) error {
		result, accepted, err := d.Apply(ctx, controllerPrincipal(), dispatchPacket(t, f, in.ControlGrant, 1, 0), pending)
		if !accepted || result.ID != ([16]byte{}) || !d.State().OwnershipUncertain || !d.State().Closed {
			t.Fatal(result, d.State())
		}
		return err
	})
	if !errors.Is(err, peer.ErrUnavailable) || pending.closes.Load() != 1 {
		t.Fatal(err)
	}
	resource.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = resource.WaitClosed(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if f.r.Stats().ReservedResourceBytes != 65536 || pending.closes.Load() != 1 {
		t.Fatal("unknown ownership freed or retried", f.r.Stats())
	}
}

func (a *countedController) CurrentGeneration() (string, error) {
	a.calls.Add(1)
	if a.panicNow.Load() {
		panic("private authority failure")
	}
	return "controller-start-1", nil
}

func TestControlOwnerDiscoveryPanicRetainsQuoteAndClearsCatalog(t *testing.T) {
	f := setup(t)
	authority := &countedController{}
	resource, err := in.OwnControlDispatcher(context.Background(), f.r, authority, "controller-start-1", 4, 65536)
	must(t, err)
	defer resource.Close()
	var packet [in.DiscoveryQueryBytes]byte
	n, err := in.EncodeDiscoveryQueryInto(packet[:], [16]byte{1})
	must(t, err)
	refs := make([]fd.BindingRef, fd.MaxBindingReferences)
	refs[0] = f.b.Reference()
	authority.panicNow.Store(true)
	err = resource.Borrow(context.Background(), func(ctx context.Context, d *in.ControlDispatcher) error {
		catalog, accepted, err := d.Discover(ctx, controllerPrincipal(), packet[:n], refs, nil)
		if !accepted || !d.State().OwnershipUncertain || catalog.Broker != (fd.Fence{}) || len(catalog.References) != 0 || refs[0] != (fd.BindingRef{}) {
			t.Fatal(catalog, d.State())
		}
		return err
	})
	want(t, err, peer.ErrUnavailable)
	resource.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = resource.WaitClosed(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if f.r.Stats().ReservedResourceBytes != 65536 {
		t.Fatal("uncertain admission freed", f.r.Stats())
	}
}

func TestControlOwnerRuntimeRevocationClosesGrantExactlyOnce(t *testing.T) {
	f := setup(t)
	resource := ownedControlFixture(t, f)
	owner := consumer()
	applyOwned(t, resource, dispatchPacket(t, f, in.ControlGrant, 1, 0), &pendingFixture{owner: owner})
	f.r.Revoke()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, resource.WaitClosed(ctx))
	resource.Close()
	if owner.closed.Load() != 1 || f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal(owner.closed.Load(), f.r.Stats())
	}
}

func ownedControlFixture(t *testing.T, f *fixture) *fd.Resource[*in.ControlDispatcher] {
	t.Helper()
	// Synthetic test quote; not native capability/kernel memory qualification.
	resource, err := in.OwnControlDispatcher(context.Background(), f.r, &countedController{}, "controller-start-1", 4, 65536)
	must(t, err)
	t.Cleanup(resource.Close)
	return resource
}

func applyOwned(t *testing.T, resource *fd.Resource[*in.ControlDispatcher], packet []byte, pending peer.PendingCapability) in.ControlResult {
	t.Helper()
	var result in.ControlResult
	err := resource.Borrow(context.Background(), func(ctx context.Context, d *in.ControlDispatcher) error {
		var err error
		var accepted bool
		result, accepted, err = d.Apply(ctx, controllerPrincipal(), packet, pending)
		if !accepted {
			t.Error("fixture command not admitted")
		}
		return err
	})
	must(t, err)
	return result
}

func TestControlOwnerAdmissionPrecedesAuthorityAndConstruction(t *testing.T) {
	f := setup(t)
	authority := &countedController{}
	resource, err := in.OwnControlDispatcher(context.Background(), f.r, authority, "controller-start-1", 4, 2<<20)
	if err == nil || resource != nil || authority.calls.Load() != 0 || f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal(resource, err, f.r.Stats())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resource, err = in.OwnControlDispatcher(ctx, f.r, authority, "controller-start-1", 4, 65536)
	if !errors.Is(err, context.Canceled) || resource != nil || authority.calls.Load() != 0 {
		t.Fatal(resource, err)
	}
}

func TestControlOwnerRetainsGrantsAcrossBorrowAndJoinsReleases(t *testing.T) {
	f := setup(t)
	resource := ownedControlFixture(t, f)
	unblock := make(chan struct{})
	owner := consumer()
	owner.unblock = unblock
	pending := &pendingFixture{owner: owner}
	installed := applyOwned(t, resource, dispatchPacket(t, f, in.ControlGrant, 1, 0), pending)
	// A new channel/borrow must find the committed receipt and original grant.
	status := applyOwned(t, resource, dispatchPacket(t, f, in.ControlStatus, 0, 0), nil)
	if status.Stats.Receipt != installed.Receipt || status.Stats.Active != 1 || owner.closed.Load() != 0 {
		t.Fatal(status)
	}
	resource.Close()
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := resource.WaitClosed(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("release did not remain pending", err)
	}
	if f.r.Stats().ReservedResourceBytes != 65536 || f.r.Stats().LiveResources != 1 {
		t.Fatal("stuck grant lost its charge", f.r.Stats())
	}
	close(unblock)
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	must(t, resource.WaitClosed(ctx))
	resource.Close()
	must(t, resource.WaitClosed(ctx))
	if owner.closed.Load() != 1 || pending.closed.Load() != 1 || f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal("release ownership", f.r.Stats())
	}
}

func TestControlOwnerFailedCapabilityOrPendingReleaseKeepsQuote(t *testing.T) {
	for _, mode := range []string{"consumer", "pending", "pending_panic"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			resource := ownedControlFixture(t, f)
			owner := consumer()
			pending := &pendingFixture{owner: owner}
			if mode == "consumer" {
				owner.closeErr = peer.ErrUnavailable
				applyOwned(t, resource, dispatchPacket(t, f, in.ControlGrant, 1, 0), pending)
			} else {
				pending.closeErr = peer.ErrUnavailable
				pending.closePanic = mode == "pending_panic"
				err := resource.Borrow(context.Background(), func(ctx context.Context, d *in.ControlDispatcher) error {
					_, accepted, err := d.Apply(ctx, controllerPrincipal(), []byte("malformed"), pending)
					if !accepted || d.State().ReleaseFailures != 1 {
						t.Fatal(d.State())
					}
					return err
				})
				if err == nil {
					t.Fatal("release failure hidden")
				}
			}
			resource.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := resource.WaitClosed(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("failure not terminal", err)
			}
			resource.Close()
			_ = resource.WaitClosed(ctx)
			if f.r.Stats().ReservedResourceBytes != 65536 || f.r.Stats().ResourceFailures != 1 || pending.closed.Load() != 1 {
				t.Fatal(f.r.Stats())
			}
			if mode == "consumer" && owner.closed.Load() != 1 {
				t.Fatal("consumer retried")
			}
			if mode != "consumer" && owner.closed.Load() != 0 {
				t.Fatal("unimported capability consumed")
			}
		})
	}
}

func TestControlOwnerCloseDrainsActiveDispatchBeforeReleasing(t *testing.T) {
	f := setup(t)
	resource := ownedControlFixture(t, f)
	pending := &pendingFixture{owner: consumer(), entered: make(chan struct{})}
	unblock := make(chan struct{})
	pending.unblock = unblock
	done := make(chan error, 1)
	packet := dispatchPacket(t, f, in.ControlGrant, 1, 0)
	go func() {
		done <- resource.Borrow(context.Background(), func(ctx context.Context, d *in.ControlDispatcher) error {
			_, _, err := d.Apply(ctx, controllerPrincipal(), packet, pending)
			return err
		})
	}()
	select {
	case <-pending.entered:
	case <-time.After(time.Second):
		t.Fatal("import did not enter")
	}
	resource.Close()
	if f.r.Stats().ReservedResourceBytes != 65536 || pending.closed.Load() != 0 {
		t.Fatal("in-flight resource freed", f.r.Stats())
	}
	close(unblock)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed borrow confirmed")
		}
	case <-time.After(time.Second):
		t.Fatal("borrow stuck")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, resource.WaitClosed(ctx))
	if pending.closed.Load() != 1 || pending.owner.closed.Load() != 1 || f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal("in-flight cleanup", f.r.Stats())
	}
}

func TestGrantControlWaitClosedIncludesPreviouslyRetiringOwner(t *testing.T) {
	f, c, _, _ := controlFixture(t, 4)
	unblock := make(chan struct{})
	owner := consumer()
	owner.unblock = unblock
	_, err := c.Grant(controllerPrincipal(), command(f, 1, 0), f.b.Reference(), owner, controlPolicy(owner.generation))
	must(t, err)
	_, err = c.Revoke(controllerPrincipal(), command(f, 2, 1))
	must(t, err)
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	want(t, c.WaitClosed(ctx), context.DeadlineExceeded)
	close(unblock)
	join, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	must(t, c.WaitClosed(join))
	if owner.closed.Load() != 1 {
		t.Fatal("prior retirement retried")
	}
}
