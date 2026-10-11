package ingress_test

import (
	"context"
	"errors"
	in "github.com/Josepavese/videosluice/frameingress"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"testing"
	"time"
)

type discoveryEntropy struct {
	err  error
	zero bool
}

func (e discoveryEntropy) Fill(b []byte) error {
	if !e.zero {
		b[0] = 7
	}
	return e.err
}

func discoveryClientFixture(t *testing.T, f *fixture, c *controlConnection) (*in.ControlClient, *fd.Resource[*in.DiscoveryWorkspace]) {
	return discoveryAdmittedFixture(t, f, c, c.generation)
}

func discoveryAdmittedFixture(t *testing.T, f *fixture, c peer.ControlConnection, broker string) (*in.ControlClient, *fd.Resource[*in.DiscoveryWorkspace]) {
	t.Helper()
	// Synthetic fixture admission, NOT a native provider footprint qualification.
	transport, err := fd.OwnRuntimeResource(context.Background(), f.r, fd.ResourcePlan[peer.ControlConnection]{Bytes: 4096,
		Create: func(context.Context) (fd.Created[peer.ControlConnection], error) {
			return fd.Created[peer.ControlConnection]{Value: c, Owned: true}, nil
		},
		Release: func(c peer.ControlConnection) error { return c.Close() }})
	must(t, err)
	client, err := in.NewBootstrapControlClient(context.Background(), f.r, transport, broker, discoveryEntropy{}, uint64(time.Second))
	must(t, err)
	t.Cleanup(client.Close)
	workspace, err := fd.OwnRuntimeResource(context.Background(), f.r, fd.ResourcePlan[*in.DiscoveryWorkspace]{Bytes: 32768,
		Create: func(context.Context) (fd.Created[*in.DiscoveryWorkspace], error) {
			return fd.Created[*in.DiscoveryWorkspace]{Value: &in.DiscoveryWorkspace{}, Owned: true}, nil
		},
		Release: func(*in.DiscoveryWorkspace) error { return nil }})
	must(t, err)
	t.Cleanup(workspace.Close)
	return client, workspace
}

func TestDiscoveryClientEntropyFailurePreservesCallerTransport(t *testing.T) {
	for _, mode := range []string{"nil", "zero", "error", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			c := &controlConnection{generation: "broker-execution-1"}
			transport, err := fd.OwnRuntimeResource(context.Background(), f.r, fd.ResourcePlan[peer.ControlConnection]{Bytes: 4096,
				Create: func(context.Context) (fd.Created[peer.ControlConnection], error) {
					return fd.Created[peer.ControlConnection]{Value: c, Owned: true}, nil
				}, Release: func(c peer.ControlConnection) error { return c.Close() }})
			must(t, err)
			defer transport.Close()
			var entropy fd.Entropy = discoveryEntropy{}
			switch mode {
			case "nil":
				entropy = nil
			case "zero":
				entropy = discoveryEntropy{zero: true}
			case "error":
				entropy = discoveryEntropy{err: peer.ErrUnavailable}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			client, err := in.NewBootstrapControlClient(ctx, f.r, transport, c.generation, entropy, uint64(time.Second))
			if err == nil || client != nil || c.closed.Load() != 0 || f.r.Stats().ReservedResourceBytes != 4096 {
				t.Fatal(client, err, f.r.Stats())
			}
			must(t, transport.Borrow(context.Background(), func(context.Context, peer.ControlConnection) error { return nil }))
		})
	}
}

func discoveryReply(t *testing.T, packet []byte, target fd.Fence) []byte {
	t.Helper()
	challenge, err := in.DecodeDiscoveryQuery(packet)
	must(t, err)
	catalog := in.DiscoveryCatalog{Challenge: challenge, Broker: target, References: []fd.BindingRef{{Fence: target, Source: "front", Epoch: 1}}}
	response := make([]byte, in.MaxDiscoveryBytes)
	n, err := in.EncodeDiscoveryCatalogInto(response, catalog)
	must(t, err)
	return response[:n]
}

func TestDiscoveryClientPinsTargetAndSharesControlChannel(t *testing.T) {
	f := setup(t)
	target := f.r.Fence()
	target.ProcessGeneration = "broker-execution-1"
	var response []byte
	var previous [16]byte
	writes := 0
	c := &controlConnection{generation: target.ProcessGeneration}
	c.write = func(_ context.Context, p []byte, cap peer.ProcessCapability) error {
		writes++
		if cap != nil {
			t.Fatal("unexpected capability")
		}
		if string(p[:4]) == "FAPB" {
			challenge, err := in.DecodeDiscoveryQuery(p)
			must(t, err)
			if challenge == previous {
				t.Fatal("challenge reused")
			}
			previous = challenge
			response = discoveryReply(t, p, target)
		} else {
			q, err := in.DecodeControl(p)
			must(t, err)
			response = clientReply(t, q, nil)
		}
		return nil
	}
	c.receive = func(_ context.Context, p []byte) (int, peer.Principal, error) {
		return copy(p, response), peer.Principal{Generation: c.generation}, nil
	}
	client, w := discoveryClientFixture(t, f, c)
	if _, ok := client.BoundTarget(); ok {
		t.Fatal("target published before discovery")
	}
	q := in.ControlCommand{Kind: in.ControlRevoke, Header: command(f, 1, 0)}
	q.Header.Target = target
	out, err := client.Exchange(context.Background(), q, nil)
	if err == nil || out.SendStarted || writes != 0 {
		t.Fatal("unbound command sent", out, err)
	}
	for i := 0; i < 2; i++ {
		d, err := client.Discover(context.Background(), w, func(catalog in.DiscoveryCatalog) error {
			if catalog.Broker != target || len(catalog.References) != 1 || catalog.References[0].Source != "front" {
				t.Fatal(catalog)
			}
			if _, ok := client.BoundTarget(); ok != (i > 0) {
				t.Fatal("target committed before completion")
			}
			return nil
		})
		must(t, err)
		if !d.SendStarted || !d.ResponseReceived {
			t.Fatal(d)
		}
	}
	if got, ok := client.BoundTarget(); !ok || got != target {
		t.Fatal(got, ok)
	}
	out, err = client.Exchange(context.Background(), q, nil)
	must(t, err)
	if !out.ResponseReceived || writes != 3 || client.Stats().Responses != 3 {
		t.Fatal(out, client.Stats())
	}
	client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, client.WaitClosed(ctx))
	if f.r.Stats().ReservedResourceBytes != 32768 {
		t.Fatal("client consumed caller workspace", f.r.Stats())
	}
	w.Close()
	must(t, w.WaitClosed(ctx))
	if f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal(f.r.Stats())
	}
}

func TestDiscoveryClientRejectsUncertainAndChangingDomain(t *testing.T) {
	for _, mode := range []string{"replay", "namespace", "sender", "generation", "receive", "oversize", "visitor", "panic", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			target := f.r.Fence()
			target.ProcessGeneration = "broker-execution-1"
			c := &controlConnection{generation: target.ProcessGeneration}
			var response, first []byte
			writes, visits := 0, 0
			c.write = func(_ context.Context, p []byte, _ peer.ProcessCapability) error {
				writes++
				sentTarget := target
				if writes == 2 && mode == "namespace" {
					sentTarget.Namespace[0]++
				}
				response = discoveryReply(t, p, sentTarget)
				if writes == 1 {
					first = append([]byte(nil), response...)
				} else if mode == "replay" {
					response = first
				}
				return nil
			}
			c.receive = func(_ context.Context, p []byte) (int, peer.Principal, error) {
				principal := peer.Principal{Generation: target.ProcessGeneration}
				if writes == 2 {
					switch mode {
					case "sender":
						principal.Generation = "other"
					case "generation":
						c.generation = "other"
					case "receive":
						return 0, principal, peer.ErrDeadline
					case "oversize":
						return len(p) + 1, principal, nil
					}
				}
				return copy(p, response), principal, nil
			}
			client, w := discoveryClientFixture(t, f, c)
			_, err := client.Discover(context.Background(), w, func(in.DiscoveryCatalog) error { return nil })
			must(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			panicked := false
			func() {
				defer func() {
					if recover() != nil {
						panicked = true
					}
				}()
				out, e := client.Discover(ctx, w, func(in.DiscoveryCatalog) error {
					visits++
					switch mode {
					case "visitor":
						return peer.ErrUnavailable
					case "panic":
						panic("fixture")
					case "cancel":
						cancel()
					}
					return nil
				})
				if e == nil || out.ResponseReceived || !out.SendStarted {
					t.Fatal(out, e)
				}
			}()
			if panicked != (mode == "panic") {
				t.Fatal("panic contract")
			}
			if (mode == "replay" || mode == "namespace" || mode == "sender" || mode == "generation" || mode == "receive" || mode == "oversize") && visits != 0 {
				t.Fatal("untrusted catalog visited")
			}
			if !client.Stats().Closed || client.Stats().Uncertain != 1 {
				t.Fatal(client.Stats())
			}
			if _, ok := client.BoundTarget(); ok {
				t.Fatal("retired target exported")
			}
			_, err = client.Discover(context.Background(), w, func(in.DiscoveryCatalog) error { return nil })
			if !errors.Is(err, peer.ErrRetired) || writes != 2 {
				t.Fatal("automatic retry", writes, err)
			}
		})
	}
}

func TestDiscoveryClientBusyCloseRetainsBorrow(t *testing.T) {
	f := setup(t)
	target := f.r.Fence()
	target.ProcessGeneration = "broker-execution-1"
	c := &controlConnection{generation: target.ProcessGeneration}
	var response []byte
	c.write = func(_ context.Context, p []byte, _ peer.ProcessCapability) error {
		response = discoveryReply(t, p, target)
		return nil
	}
	c.receive = func(_ context.Context, p []byte) (int, peer.Principal, error) {
		return copy(p, response), peer.Principal{Generation: c.generation}, nil
	}
	client, w := discoveryClientFixture(t, f, c)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := client.Discover(context.Background(), w, func(in.DiscoveryCatalog) error { close(entered); <-release; return nil })
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("visitor not entered")
	}
	_, err := client.Discover(context.Background(), w, func(in.DiscoveryCatalog) error { return nil })
	want(t, err, peer.ErrBusy)
	_, err = client.Exchange(context.Background(), in.ControlCommand{}, nil)
	want(t, err, peer.ErrBusy)
	// Another client cannot mutate or clear the same workspace during a visit.
	otherConnection := &controlConnection{generation: target.ProcessGeneration, write: func(context.Context, []byte, peer.ProcessCapability) error {
		t.Error("busy workspace sent a request")
		return nil
	}}
	other, otherWorkspace := discoveryClientFixture(t, f, otherConnection)
	otherWorkspace.Close()
	_, err = other.Discover(context.Background(), w, func(in.DiscoveryCatalog) error { t.Error("shared visitor entered"); return nil })
	want(t, err, peer.ErrBusy)
	if !other.Stats().Closed || other.Stats().Uncertain != 0 {
		t.Fatal(other.Stats())
	}
	joinCtx, joinCancel := context.WithTimeout(context.Background(), time.Second)
	defer joinCancel()
	must(t, other.WaitClosed(joinCtx))
	must(t, otherWorkspace.WaitClosed(joinCtx))
	client.Close()
	w.Close()
	if c.closed.Load() != 0 || f.r.Stats().ReservedResourceBytes != 57344 {
		t.Fatal("borrow freed early", f.r.Stats())
	}
	close(release)
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("closed discovery confirmed")
		}
	case <-time.After(time.Second):
		t.Fatal("visitor did not finish")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, client.WaitClosed(ctx))
	must(t, w.WaitClosed(ctx))
	if c.closed.Load() != 1 || f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal(f.r.Stats())
	}
}

func TestDiscoveryClientRejectsForeignWorkspaceBeforeSend(t *testing.T) {
	f, g := setup(t), setup(t)
	c := &controlConnection{generation: "broker-execution-1", write: func(context.Context, []byte, peer.ProcessCapability) error {
		t.Fatal("foreign workspace sent")
		return nil
	}}
	client, _ := discoveryClientFixture(t, f, c)
	other, foreign := discoveryClientFixture(t, g, &controlConnection{generation: "broker-execution-1"})
	defer other.Close()
	out, err := client.Discover(context.Background(), foreign, func(in.DiscoveryCatalog) error { return nil })
	if err == nil || out.SendStarted || client.Stats().Closed || c.closed.Load() != 0 {
		t.Fatal(out, err)
	}
}
