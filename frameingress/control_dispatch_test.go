package ingress_test

import (
	"context"
	"errors"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sync/atomic"
	"testing"
)

type pendingFixture struct {
	owner      *ownedConsumer
	imports    atomic.Int32
	closed     atomic.Int32
	importErr  error
	closeErr   error
	closePanic bool
	entered    chan struct{}
	unblock    <-chan struct{}
}

func (p *pendingFixture) Import(_ string, _ []byte) (peer.ProcessCapability, error) {
	p.imports.Add(1)
	if p.entered != nil {
		close(p.entered)
		<-p.unblock
	}
	if p.importErr != nil {
		return nil, p.importErr
	}
	return p.owner, nil
}
func (p *pendingFixture) Close() error {
	p.closed.Add(1)
	if p.closePanic {
		panic("injected release failure")
	}
	return p.closeErr
}
func dispatchPacket(t *testing.T, f *fixture, kind in.ControlKind, seq, prev uint64) []byte {
	t.Helper()
	c := in.ControlCommand{Kind: kind, Header: command(f, seq, prev)}
	if kind == in.ControlGrant {
		c.Binding = f.b.Reference()
		c.Policy = controlPolicy("consumer-start-1")
		c.CapabilityType = "fixture"
		c.CapabilityMetadata = []byte{1}
	} else if kind == in.ControlStatus {
		c.Header.Sequence = 0
		c.Header.Previous = 0
		c.Header.Grant = ""
	}
	p, err := in.EncodeControl(c)
	must(t, err)
	return p
}

func TestDispatcherOwnsAllAdmittedRejectsAndReconcilesLostACK(t *testing.T) {
	f, control, _, q := controlFixture(t, 4)
	d, err := in.NewControlDispatcher(control)
	must(t, err)
	defer d.Close()
	for _, test := range []struct {
		packet  []byte
		sender  peer.Principal
		want    error
		imports int32
	}{
		{[]byte("broken"), controllerPrincipal(), peer.ErrInvalid, 0},
		{dispatchPacket(t, f, in.ControlGrant, 1, 0), principal(), peer.ErrUnauthorized, 0},
		{dispatchPacket(t, f, in.ControlRevoke, 1, 0), controllerPrincipal(), peer.ErrInvalid, 0},
	} {
		pending := &pendingFixture{owner: consumer()}
		_, accepted, err := d.Apply(context.Background(), test.sender, test.packet, pending)
		want(t, err, test.want)
		if !accepted || pending.closed.Load() != 1 || pending.imports.Load() != test.imports || pending.owner.closed.Load() != 0 {
			t.Fatal("reject ownership wrong")
		}
	}
	pending := &pendingFixture{owner: consumer()}
	packet := dispatchPacket(t, f, in.ControlGrant, 1, 0)
	result, accepted, err := d.Apply(context.Background(), controllerPrincipal(), packet, pending)
	must(t, err)
	if !accepted || result.Receipt.Outcome != "installed" || pending.closed.Load() != 1 || pending.owner.closed.Load() != 0 {
		t.Fatal("grant ownership wrong", result)
	}
	// Simulate dropping the response, without undoing the actual commit.
	status, _, err := d.Apply(context.Background(), controllerPrincipal(), dispatchPacket(t, f, in.ControlStatus, 0, 0), nil)
	must(t, err)
	if status.Stats.Receipt != result.Receipt || len(status.Stats.Grants) != 1 || status.Stats.Grants[0].Sequence != 1 {
		t.Fatal("lost ACK not reconcilable")
	}
	duplicate := &pendingFixture{owner: consumer()}
	_, _, err = d.Apply(context.Background(), controllerPrincipal(), packet, duplicate)
	want(t, err, in.ErrControlSequence)
	if duplicate.imports.Load() != 0 || duplicate.closed.Load() != 1 {
		t.Fatal("replay imported capability")
	}
	a, err := control.Lookup(principal(), "reader")
	must(t, err)
	output, err := a.Extract(context.Background(), principal(), q)
	must(t, err)
	output.Close()
	result, _, err = d.Apply(context.Background(), controllerPrincipal(), dispatchPacket(t, f, in.ControlRevoke, 2, 1), nil)
	outcome(t, result.Receipt, err, "revoked")
	awaitControl(t, control, func(s in.ControlStats) bool { return s.Retiring == 0 })
	if pending.owner.closed.Load() != 1 {
		t.Fatal("committed capability not released once")
	}
}

func TestDispatcherMissingOrInvalidCapabilityDoesNotConsumeSequence(t *testing.T) {
	f, control, _, _ := controlFixture(t, 2)
	d, err := in.NewControlDispatcher(control)
	must(t, err)
	defer d.Close()
	packet := dispatchPacket(t, f, in.ControlGrant, 1, 0)
	_, accepted, err := d.Apply(context.Background(), controllerPrincipal(), packet, nil)
	want(t, err, peer.ErrInvalid)
	if !accepted {
		t.Fatal("valid processing slot not admitted")
	}
	p := &pendingFixture{importErr: peer.ErrUnavailable}
	_, _, err = d.Apply(context.Background(), controllerPrincipal(), packet, p)
	want(t, err, peer.ErrUnavailable)
	if p.closed.Load() != 1 {
		t.Fatal("failed import raw owner leaked")
	}
	s, err := control.Status(controllerPrincipal())
	must(t, err)
	if s.Receipt.Sequence != 0 || s.Active != 0 {
		t.Fatal("transport failure consumed sequence")
	}
}

func TestDispatcherCancellationAfterImportAndBusyOwnership(t *testing.T) {
	f, control, _, _ := controlFixture(t, 2)
	d, err := in.NewControlDispatcher(control)
	must(t, err)
	defer d.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gate := make(chan struct{})
	pending := &pendingFixture{owner: consumer(), entered: make(chan struct{}), unblock: gate}
	done := make(chan error, 1)
	packet := dispatchPacket(t, f, in.ControlGrant, 1, 0)
	go func() {
		_, accepted, err := d.Apply(ctx, controllerPrincipal(), packet, pending)
		if !accepted {
			err = errors.New("expected admission")
		}
		done <- err
	}()
	<-pending.entered
	state := d.State()
	if !state.Busy || state.Stage != "capability_import" {
		cancel()
		close(gate)
		<-done
		t.Fatal("blocked import phase not observable", state)
	}
	other := &pendingFixture{owner: consumer()}
	_, accepted, err := d.Apply(context.Background(), controllerPrincipal(), packet, other)
	if accepted || !errors.Is(err, peer.ErrBusy) || other.closed.Load() != 0 {
		close(gate)
		<-done
		t.Fatal("busy request ownership transferred")
	}
	must(t, other.Close())
	cancel()
	close(gate)
	want(t, <-done, context.Canceled)
	if pending.closed.Load() != 1 || pending.owner.closed.Load() != 1 {
		t.Fatal("cancelled owner not released")
	}
	s, err := control.Status(controllerPrincipal())
	must(t, err)
	if s.Active != 0 || s.Receipt.Sequence != 0 {
		t.Fatal("cancelled grant committed")
	}
}

func TestDispatcherReleaseFailureQuarantinesAndStopsAdmission(t *testing.T) {
	for _, panicClose := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panicClose], func(t *testing.T) {
			_, control, _, _ := controlFixture(t, 2)
			d, err := in.NewControlDispatcher(control)
			must(t, err)
			defer d.Close()
			pending := &pendingFixture{closeErr: errors.New("injected close error"), closePanic: panicClose}
			_, accepted, err := d.Apply(context.Background(), controllerPrincipal(), []byte("malformed"), pending)
			want(t, err, peer.ErrUnavailable)
			if !accepted || pending.closed.Load() != 1 || d.State() != (in.ControlDispatchState{Closed: true, ReleaseFailures: 1, Stage: "release_failed"}) {
				t.Fatal("failed close freed admission")
			}
			other := &pendingFixture{}
			_, accepted, err = d.Apply(context.Background(), controllerPrincipal(), []byte("anything"), other)
			want(t, err, peer.ErrRetired)
			if accepted || other.closed.Load() != 0 || pending.closed.Load() != 1 {
				t.Fatal("retired dispatch consumed/retried handle")
			}
			must(t, other.Close())
		})
	}
}
