package ingress_test

import (
	"bytes"
	"context"
	"errors"
	in "github.com/Josepavese/videosluice/frameingress"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sync/atomic"
	"testing"
	"time"
)

type serverConnection struct {
	receive    func(context.Context, []byte) (int, peer.Principal, peer.PendingCapability, error)
	reply      func(context.Context, []byte) error
	generation string
	closed     atomic.Int32
	closeErr   error
}

func (c *serverConnection) CurrentGeneration() (string, error) { return c.generation, nil }
func (c *serverConnection) ReceiveCommand(ctx context.Context, p []byte) (int, peer.Principal, peer.PendingCapability, error) {
	return c.receive(ctx, p)
}
func (c *serverConnection) Reply(ctx context.Context, p []byte) error { return c.reply(ctx, p) }
func (c *serverConnection) Close() error                              { c.closed.Add(1); return c.closeErr }

func serverSessionFixture(t *testing.T, f *fixture, control *fd.Resource[*in.ControlDispatcher], c peer.ControlServerConnection) (*in.ControlSession, *fd.Resource[*in.DiscoveryWorkspace]) {
	t.Helper()
	// Synthetic fixture quote, not native kernel/handle admission evidence.
	transport, err := fd.OwnRuntimeResource(context.Background(), f.r, fd.ResourcePlan[peer.ControlServerConnection]{Bytes: 4096,
		Create: func(context.Context) (fd.Created[peer.ControlServerConnection], error) {
			return fd.Created[peer.ControlServerConnection]{Value: c, Owned: true}, nil
		}, Release: func(c peer.ControlServerConnection) error { return c.Close() }})
	must(t, err)
	workspace, err := fd.OwnRuntimeResource(context.Background(), f.r, fd.ResourcePlan[*in.DiscoveryWorkspace]{Bytes: 32768,
		Create: func(context.Context) (fd.Created[*in.DiscoveryWorkspace], error) {
			return fd.Created[*in.DiscoveryWorkspace]{Value: &in.DiscoveryWorkspace{}, Owned: true}, nil
		}, Release: func(*in.DiscoveryWorkspace) error { return nil }})
	must(t, err)
	t.Cleanup(workspace.Close)
	session, err := in.NewControlSession(context.Background(), f.r, control, transport, workspace, uint64(time.Second))
	must(t, err)
	t.Cleanup(session.Close)
	return session, workspace
}

func TestControlSessionDiscoveryGrantDenialAndStatus(t *testing.T) {
	f := setup(t)
	control := ownedControlFixture(t, f)
	c := &serverConnection{generation: "controller-start-1"}
	var request, response []byte
	var pending peer.PendingCapability
	c.receive = func(_ context.Context, p []byte) (int, peer.Principal, peer.PendingCapability, error) {
		next := pending
		pending = nil
		return copy(p, request), controllerPrincipal(), next, nil
	}
	c.reply = func(_ context.Context, p []byte) error { response = append([]byte(nil), p...); return nil }
	session, w := serverSessionFixture(t, f, control, c)
	var discovery [in.DiscoveryQueryBytes]byte
	n, err := in.EncodeDiscoveryQueryInto(discovery[:], [16]byte{1})
	must(t, err)
	request = discovery[:n]
	complete, err := session.Step(context.Background())
	must(t, err)
	if !complete {
		t.Fatal("discovery incomplete")
	}
	var refs [fd.MaxBindingReferences]fd.BindingRef
	catalog, err := in.DecodeDiscoveryCatalogInto(response, refs[:])
	must(t, err)
	if catalog.Broker != f.r.Fence() || len(catalog.References) != 1 || catalog.References[0] != f.b.Reference() {
		t.Fatal(catalog)
	}
	installed := consumer()
	pending = &pendingFixture{owner: installed}
	request = dispatchPacket(t, f, in.ControlGrant, 1, 0)
	complete, err = session.Step(context.Background())
	must(t, err)
	if !complete {
		t.Fatal("grant incomplete")
	}
	reply, err := in.DecodeControlReply(response)
	must(t, err)
	receipt := reply.Receipt
	if reply.Code != in.ReplyOK || receipt.Outcome != "installed" {
		t.Fatal(reply)
	}
	request = dispatchPacket(t, f, in.ControlRevoke, 1, 1) // sequence replay denied, never applied
	complete, err = session.Step(context.Background())
	must(t, err)
	if !complete {
		t.Fatal("denial not delivered")
	}
	reply, err = in.DecodeControlReply(response)
	must(t, err)
	if reply.Code != in.ReplySequence {
		t.Fatal(reply)
	}
	request = dispatchPacket(t, f, in.ControlStatus, 0, 0)
	complete, err = session.Step(context.Background())
	must(t, err)
	reply, err = in.DecodeControlReply(response)
	must(t, err)
	if !complete || reply.Stats.Receipt != receipt || reply.Stats.Active != 1 || installed.closed.Load() != 0 {
		t.Fatal(reply)
	}
	stats := session.Stats()
	if stats.Completed != 4 || stats.Received != 4 || stats.Negative != 1 || stats.Uncertain != 0 || stats.Closed {
		t.Fatal(stats)
	}
	session.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, session.WaitClosed(ctx))
	if f.r.Stats().ReservedResourceBytes != 65536+32768 {
		t.Fatal("session closed shared owner/workspace", f.r.Stats())
	}
	w.Close()
	must(t, w.WaitClosed(ctx))
	control.Close()
	must(t, control.WaitClosed(ctx))
	if installed.closed.Load() != 1 || f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal(f.r.Stats())
	}
}

func TestControlSessionLostReplyPreservesCommittedGrantForNewChannel(t *testing.T) {
	f := setup(t)
	control := ownedControlFixture(t, f)
	owner := consumer()
	pending := &pendingFixture{owner: owner}
	packet := dispatchPacket(t, f, in.ControlGrant, 1, 0)
	c := &serverConnection{generation: "controller-start-1"}
	c.receive = func(_ context.Context, p []byte) (int, peer.Principal, peer.PendingCapability, error) {
		return copy(p, packet), controllerPrincipal(), pending, nil
	}
	c.reply = func(context.Context, []byte) error { return peer.ErrDeadline }
	session, _ := serverSessionFixture(t, f, control, c)
	complete, err := session.Step(context.Background())
	if err == nil || complete || !session.Stats().Closed || session.Stats().Uncertain != 1 || owner.closed.Load() != 0 {
		t.Fatal(complete, err, session.Stats())
	}
	next := &serverConnection{generation: "controller-start-1"}
	status := dispatchPacket(t, f, in.ControlStatus, 0, 0)
	var response []byte
	next.receive = func(_ context.Context, p []byte) (int, peer.Principal, peer.PendingCapability, error) {
		return copy(p, status), controllerPrincipal(), nil, nil
	}
	next.reply = func(_ context.Context, p []byte) error { response = append([]byte(nil), p...); return nil }
	another, _ := serverSessionFixture(t, f, control, next)
	complete, err = another.Step(context.Background())
	must(t, err)
	reply, err := in.DecodeControlReply(response)
	must(t, err)
	if !complete || reply.Stats.Active != 1 || reply.Stats.Receipt.Sequence != 1 || pending.imports.Load() != 1 || owner.closed.Load() != 0 {
		t.Fatal(reply)
	}
}

func TestControlSessionIdleRejectsAndPartialOwnership(t *testing.T) {
	for _, mode := range []string{"idle", "idle_with_fd", "unexpected_fd", "malformed", "oversize", "sender", "generation", "receive_error", "receive_panic", "release_error"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			control := ownedControlFixture(t, f)
			c := &serverConnection{generation: "controller-start-1"}
			pending := &pendingFixture{owner: consumer()}
			var request [in.DiscoveryQueryBytes]byte
			n, err := in.EncodeDiscoveryQueryInto(request[:], [16]byte{1})
			must(t, err)
			c.receive = func(_ context.Context, p []byte) (int, peer.Principal, peer.PendingCapability, error) {
				sender := controllerPrincipal()
				switch mode {
				case "idle":
					return 0, sender, nil, peer.ErrDeadline
				case "idle_with_fd":
					return 0, sender, pending, peer.ErrDeadline
				case "unexpected_fd":
					return copy(p, request[:n]), sender, pending, nil
				case "malformed":
					return copy(p, []byte("bad")), sender, pending, nil
				case "oversize":
					return len(p) + 1, sender, pending, nil
				case "sender":
					sender.Generation = "foreign"
				case "generation":
					c.generation = "foreign"
				case "receive_error":
					return 0, sender, pending, peer.ErrUnavailable
				case "receive_panic":
					panic("private receive payload")
				case "release_error":
					pending.closeErr = peer.ErrUnavailable
					return 0, sender, pending, peer.ErrUnavailable
				}
				return copy(p, request[:n]), sender, nil, nil
			}
			c.reply = func(context.Context, []byte) error { t.Fatal("invalid input got a reply"); return nil }
			session, _ := serverSessionFixture(t, f, control, c)
			complete, err := session.Step(context.Background())
			if complete {
				t.Fatal("invalid completed")
			}
			if mode == "idle" {
				must(t, err)
				if session.Stats().Closed || session.Stats().Idle != 1 {
					t.Fatal(session.Stats())
				}
				return
			}
			if err == nil || !session.Stats().Closed || session.Stats().Idle != 0 {
				t.Fatal(err, session.Stats())
			}
			hasPending := mode == "idle_with_fd" || mode == "unexpected_fd" || mode == "malformed" || mode == "oversize" || mode == "receive_error" || mode == "release_error"
			if hasPending && pending.closed.Load() != 1 || pending.imports.Load() != 0 {
				t.Fatal("pending leaked/imported", pending.closed.Load())
			}
			if mode == "receive_panic" || mode == "release_error" {
				control.Close()
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err = control.WaitClosed(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("unknown resources released", err)
				}
				if f.r.Stats().ReservedResourceBytes < 65536 {
					t.Fatal(f.r.Stats())
				}
			}
		})
	}
}

func TestControlSessionBusyCloseJoinsReplyBorrow(t *testing.T) {
	f := setup(t)
	control := ownedControlFixture(t, f)
	packet := dispatchPacket(t, f, in.ControlStatus, 0, 0)
	c := &serverConnection{generation: "controller-start-1"}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	c.receive = func(_ context.Context, p []byte) (int, peer.Principal, peer.PendingCapability, error) {
		return copy(p, packet), controllerPrincipal(), nil, nil
	}
	c.reply = func(context.Context, []byte) error { close(entered); <-release; return nil }
	session, _ := serverSessionFixture(t, f, control, c)
	go func() { _, err := session.Step(context.Background()); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reply not entered")
	}
	_, err := session.Step(context.Background())
	want(t, err, peer.ErrBusy)
	session.Close()
	if c.closed.Load() != 0 || f.r.Stats().ReservedResourceBytes != 122880 {
		t.Fatal("in-flight freed", f.r.Stats())
	}
	close(release)
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("closed reply confirmed")
		}
	case <-time.After(time.Second):
		t.Fatal("reply stuck")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, session.WaitClosed(ctx))
	if c.closed.Load() != 1 || session.Stats().Completed != 0 || session.Stats().Uncertain != 1 {
		t.Fatal(session.Stats())
	}
}

func TestControlReplyIntoMatchesCanonicalAndRejectsWithoutWriting(t *testing.T) {
	f := setup(t)
	q, err := in.DecodeControl(dispatchPacket(t, f, in.ControlRevoke, 1, 0))
	must(t, err)
	encoded := clientReply(t, q, nil)
	reply, err := in.DecodeControlReply(encoded)
	must(t, err)
	buffer := make([]byte, in.MaxControlReplyBytes)
	n, err := in.EncodeControlReplyInto(buffer, reply)
	must(t, err)
	if !bytes.Equal(buffer[:n], encoded) {
		t.Fatal("wire changed")
	}
	before := append([]byte(nil), buffer...)
	_, err = in.EncodeControlReplyInto(buffer, in.ControlReply{})
	if err == nil || !bytes.Equal(buffer, before) {
		t.Fatal("invalid encode mutated")
	}
	_, err = in.EncodeControlReplyInto(buffer[:10], reply)
	if err == nil || !bytes.Equal(buffer, before) {
		t.Fatal("short encode mutated")
	}
}

func TestControlSessionAdmissionFailurePreservesCallerResources(t *testing.T) {
	f, g := setup(t), setup(t)
	control := ownedControlFixture(t, f)
	foreign := ownedControlFixture(t, g)
	c := &serverConnection{generation: "controller-start-1"}
	transport, err := fd.OwnRuntimeResource(context.Background(), f.r, fd.ResourcePlan[peer.ControlServerConnection]{Bytes: 4096,
		Create: func(context.Context) (fd.Created[peer.ControlServerConnection], error) {
			return fd.Created[peer.ControlServerConnection]{Value: c, Owned: true}, nil
		}, Release: func(c peer.ControlServerConnection) error { return c.Close() }})
	must(t, err)
	defer transport.Close()
	w, err := fd.OwnRuntimeResource(context.Background(), f.r, fd.ResourcePlan[*in.DiscoveryWorkspace]{Bytes: 32768,
		Create: func(context.Context) (fd.Created[*in.DiscoveryWorkspace], error) {
			return fd.Created[*in.DiscoveryWorkspace]{Value: &in.DiscoveryWorkspace{}, Owned: true}, nil
		}, Release: func(*in.DiscoveryWorkspace) error { return nil }})
	must(t, err)
	defer w.Close()
	before := f.r.Stats().ReservedResourceBytes
	session, err := in.NewControlSession(context.Background(), f.r, foreign, transport, w, uint64(time.Second))
	if err == nil || session != nil || c.closed.Load() != 0 || f.r.Stats().ReservedResourceBytes != before {
		t.Fatal(session, err, f.r.Stats())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	session, err = in.NewControlSession(ctx, f.r, control, transport, w, uint64(time.Second))
	if !errors.Is(err, context.Canceled) || session != nil || c.closed.Load() != 0 || f.r.Stats().ReservedResourceBytes != before {
		t.Fatal(session, err, f.r.Stats())
	}
	must(t, transport.Borrow(context.Background(), func(context.Context, peer.ControlServerConnection) error { return nil }))
}

func TestControlSessionFailedPhysicalCloseRetainsTransportQuote(t *testing.T) {
	f := setup(t)
	control := ownedControlFixture(t, f)
	c := &serverConnection{generation: "controller-start-1", closeErr: peer.ErrUnavailable}
	session, w := serverSessionFixture(t, f, control, c)
	session.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := session.WaitClosed(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	session.Close()
	_ = session.WaitClosed(ctx)
	w.Close()
	must(t, w.WaitClosed(ctx))
	control.Close()
	must(t, control.WaitClosed(ctx))
	if f.r.Stats().ReservedResourceBytes != 4096 || f.r.Stats().ResourceFailures != 1 || c.closed.Load() != 1 {
		t.Fatal("failed transport freed/retried", f.r.Stats())
	}
}
