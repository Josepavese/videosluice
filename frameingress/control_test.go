package ingress_test

import (
	"context"
	"errors"
	in "github.com/Josepavese/videosluice/frameingress"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ownedConsumer struct {
	generation string
	closed     atomic.Int32
	unblock    <-chan struct{}
	closeErr   error
}

func (o *ownedConsumer) CurrentGeneration() (string, error) {
	if o.closed.Load() != 0 {
		return "", peer.ErrRetired
	}
	return o.generation, nil
}
func (o *ownedConsumer) Close() error {
	o.closed.Add(1)
	if o.unblock != nil {
		<-o.unblock
	}
	return o.closeErr
}
func consumer() *ownedConsumer { return &ownedConsumer{generation: "consumer-start-1"} }
func controlPolicy(generation string) in.AccessPolicy {
	return in.AccessPolicy{Generation: generation, Streams: []string{"analysis"}, MaxRegions: 1, MaxRequests: 1, MaxResults: 1}
}
func controllerPrincipal() peer.Principal { return peer.Principal{Generation: "controller-start-1"} }
func command(f *fixture, sequence, previous uint64) in.ControlHeader {
	return in.ControlHeader{Target: f.r.Fence(), Sequence: sequence, Previous: previous, ID: [16]byte{1, byte(sequence)}, Grant: "reader"}
}
func controlFixture(t *testing.T, limit int) (*fixture, *in.GrantControl, *authority, fd.Request) {
	t.Helper()
	f, _, _, q := accessFixture(t)
	authority := &authority{}
	authority.generation.Store("controller-start-1")
	c, err := in.NewGrantControl(f.r, authority, "controller-start-1", limit)
	must(t, err)
	t.Cleanup(c.Close)
	return f, c, authority, q
}
func outcome(t *testing.T, receipt in.ControlReceipt, err error, expected string) {
	t.Helper()
	must(t, err)
	if receipt.Outcome != expected {
		t.Fatalf("receipt=%+v want=%s", receipt, expected)
	}
}
func awaitControl(t *testing.T, c *in.GrantControl, predicate func(in.ControlStats) bool) in.ControlStats {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		s, _ := c.Status(controllerPrincipal())
		if predicate(s) {
			return s
		}
		if time.Now().After(deadline) {
			t.Fatalf("control did not settle: %+v", s)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestControlRejectsReplayForeignBrokerAndUnauthorizedPrincipal(t *testing.T) {
	f, c, _, q := controlFixture(t, 3)
	owner := consumer()
	r, err := c.Grant(controllerPrincipal(), command(f, 1, 0), f.b.Reference(), owner, controlPolicy(owner.generation))
	outcome(t, r, err, "installed")
	_, err = c.Revoke(controllerPrincipal(), command(f, 1, 1))
	want(t, err, in.ErrControlSequence)
	_, err = c.Revoke(controllerPrincipal(), command(f, 3, 1))
	want(t, err, in.ErrControlSequence)
	_, err = c.Revoke(principal(), command(f, 2, 1))
	want(t, err, peer.ErrUnauthorized)
	wrong := command(f, 2, 1)
	wrong.Target.Namespace[0] ^= 1
	_, err = c.Revoke(controllerPrincipal(), wrong)
	want(t, err, fd.ErrDomainMismatch)
	s, err := c.Status(controllerPrincipal())
	must(t, err)
	if s.Receipt != r || len(s.Grants) != 1 || s.Grants[0].Sequence != 1 || owner.closed.Load() != 0 {
		t.Fatal(s)
	}
	a, err := c.Lookup(principal(), "reader")
	must(t, err)
	result, err := a.Extract(context.Background(), principal(), q)
	must(t, err)
	result.Close()
	// Same controller generation, different runtime namespace: old command denied.
	other, otherControl, _, _ := controlFixture(t, 3)
	_ = other
	_, err = otherControl.Revoke(controllerPrincipal(), command(f, 1, 0))
	want(t, err, fd.ErrDomainMismatch)
}

func TestControlCASAndReceiptsDoNotRepeatUncertainOperations(t *testing.T) {
	f, c, _, _ := controlFixture(t, 3)
	one := consumer()
	r, err := c.Grant(controllerPrincipal(), command(f, 1, 0), f.b.Reference(), one, controlPolicy(one.generation))
	outcome(t, r, err, "installed")
	two := consumer()
	r, err = c.Grant(controllerPrincipal(), command(f, 2, 0), f.b.Reference(), two, controlPolicy(two.generation))
	outcome(t, r, err, "conflict")
	if two.closed.Load() != 0 {
		t.Fatal("rejected capability consumed")
	}
	must(t, two.Close())
	r, err = c.Revoke(controllerPrincipal(), command(f, 3, 2))
	outcome(t, r, err, "conflict")
	s, err := c.Status(controllerPrincipal())
	must(t, err)
	if s.Receipt.Sequence != 3 || s.Grants[0].Sequence != 1 {
		t.Fatal(s)
	}
	r, err = c.Revoke(controllerPrincipal(), command(f, 4, 1))
	outcome(t, r, err, "revoked")
	_, err = c.Revoke(controllerPrincipal(), command(f, 4, 1))
	want(t, err, in.ErrControlSequence)
	awaitControl(t, c, func(s in.ControlStats) bool { return s.Active == 0 && s.Retiring == 0 })
	if one.closed.Load() != 1 {
		t.Fatal(one.closed.Load())
	}
}

func TestControlReplacementCountsSlowClosingCapability(t *testing.T) {
	f, c, _, q := controlFixture(t, 2)
	unblock := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(unblock) })
	one := consumer()
	one.unblock = unblock
	r, err := c.Grant(controllerPrincipal(), command(f, 1, 0), f.b.Reference(), one, controlPolicy(one.generation))
	outcome(t, r, err, "installed")
	old, err := c.Lookup(principal(), "reader")
	must(t, err)
	two := consumer()
	two.generation = "consumer-start-2"
	r, err = c.Grant(controllerPrincipal(), command(f, 2, 1), f.b.Reference(), two, controlPolicy(two.generation))
	outcome(t, r, err, "installed")
	_, err = old.Extract(context.Background(), principal(), q)
	want(t, err, peer.ErrRetired)
	s, err := c.Status(controllerPrincipal())
	must(t, err)
	if s.Active != 1 || s.Retiring != 1 {
		t.Fatal(s)
	}
	third := consumer()
	r, err = c.Grant(controllerPrincipal(), command(f, 3, 2), f.b.Reference(), third, controlPolicy(third.generation))
	outcome(t, r, err, "capacity")
	if third.closed.Load() != 0 {
		t.Fatal("rejected owner consumed")
	}
	must(t, third.Close())
	once.Do(func() { close(unblock) })
	awaitControl(t, c, func(s in.ControlStats) bool { return s.Retiring == 0 })
	_, err = c.Lookup(principal(), "reader")
	want(t, err, peer.ErrUnauthorized)
	currentPrincipal := peer.Principal{Generation: two.generation}
	current, err := c.Lookup(currentPrincipal, "reader")
	must(t, err)
	result, err := current.Extract(context.Background(), currentPrincipal, q)
	must(t, err)
	result.Close()
	c.Close()
	awaitControl(t, c, func(s in.ControlStats) bool { return s.Closed && s.Retiring == 0 })
	if one.closed.Load() != 1 || two.closed.Load() != 1 {
		t.Fatal("wrong close count")
	}
}

func TestControlCloseFailureStaysChargedWithoutRetry(t *testing.T) {
	f, c, _, _ := controlFixture(t, 1)
	owner := consumer()
	owner.closeErr = errors.New("injected close failure")
	r, err := c.Grant(controllerPrincipal(), command(f, 1, 0), f.b.Reference(), owner, controlPolicy(owner.generation))
	outcome(t, r, err, "installed")
	r, err = c.Revoke(controllerPrincipal(), command(f, 2, 1))
	outcome(t, r, err, "revoked")
	awaitControl(t, c, func(s in.ControlStats) bool { return s.ReleaseFailures == 1 && s.Retiring == 1 })
	fresh := consumer()
	r, err = c.Grant(controllerPrincipal(), command(f, 3, 0), f.b.Reference(), fresh, controlPolicy(fresh.generation))
	outcome(t, r, err, "capacity")
	must(t, fresh.Close())
	for i := 0; i < 20; i++ {
		c.Close()
	}
	if owner.closed.Load() != 1 {
		t.Fatal("failed release retried")
	}
}

func TestCachedAccessRetiresWhenControllerRestarts(t *testing.T) {
	f, c, authority, q := controlFixture(t, 2)
	owner := consumer()
	r, err := c.Grant(controllerPrincipal(), command(f, 1, 0), f.b.Reference(), owner, controlPolicy(owner.generation))
	outcome(t, r, err, "installed")
	a, err := c.Lookup(principal(), "reader")
	must(t, err)
	result, err := a.Extract(context.Background(), principal(), q)
	must(t, err)
	defer result.Close()
	authority.generation.Store("controller-start-2")
	called := false
	err = result.Visit(0, func(fd.Artifact, []byte) error { called = true; return nil })
	want(t, err, peer.ErrRetired)
	if called {
		t.Fatal("dead controller authorized further delivery")
	}
	_, err = a.Extract(context.Background(), principal(), q)
	want(t, err, peer.ErrRetired)
	s, err := c.Status(controllerPrincipal())
	want(t, err, peer.ErrRetired)
	if !s.Closed || s.Active != 0 {
		t.Fatal(s)
	}
	awaitControl(t, c, func(s in.ControlStats) bool { return s.Retiring == 0 })
}

func TestControlRejectsRetiredBindingWithoutTakingCapability(t *testing.T) {
	f, c, _, _ := controlFixture(t, 2)
	owner := consumer()
	f.b.Close()
	r, err := c.Grant(controllerPrincipal(), command(f, 1, 0), f.b.Reference(), owner, controlPolicy(owner.generation))
	outcome(t, r, err, "invalid")
	if r.Stage != "control_binding_reference" {
		t.Fatal("missing rejection stage", r)
	}
	if owner.closed.Load() != 0 {
		t.Fatal("rejected owner consumed")
	}
	must(t, owner.Close())
	s, err := c.Status(controllerPrincipal())
	must(t, err)
	if s.Active != 0 || s.Retiring != 0 || s.Receipt.Sequence != 1 {
		t.Fatal(s)
	}
}

func TestConcurrentDuplicateControlCommandsInstallExactlyOneOwner(t *testing.T) {
	f, c, _, _ := controlFixture(t, 2)
	type answer struct {
		owner   *ownedConsumer
		receipt in.ControlReceipt
		err     error
	}
	done := make(chan answer, 64)
	for i := 0; i < 64; i++ {
		go func() {
			owner := consumer()
			receipt, err := c.Grant(controllerPrincipal(), command(f, 1, 0), f.b.Reference(), owner, controlPolicy(owner.generation))
			done <- answer{owner, receipt, err}
		}()
	}
	installed := 0
	for i := 0; i < 64; i++ {
		a := <-done
		if a.err == nil {
			outcome(t, a.receipt, a.err, "installed")
			installed++
		} else {
			want(t, a.err, in.ErrControlSequence)
			if a.owner.closed.Load() != 0 {
				t.Fatal("rejected owner consumed")
			}
			must(t, a.owner.Close())
		}
	}
	if installed != 1 {
		t.Fatal(installed)
	}
	c.Close()
	awaitControl(t, c, func(s in.ControlStats) bool { return s.Closed && s.Retiring == 0 })
}
