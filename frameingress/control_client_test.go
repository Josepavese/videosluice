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

type controlConnection struct {
	write      func(context.Context, []byte, peer.ProcessCapability) error
	receive    func(context.Context, []byte) (int, peer.Principal, error)
	generation string
	closed     atomic.Int32
	closeErr   error
}

func (c *controlConnection) CurrentGeneration() (string, error) { return c.generation, nil }
func (c *controlConnection) WriteCommand(ctx context.Context, p []byte, cap peer.ProcessCapability) error {
	return c.write(ctx, p, cap)
}
func (c *controlConnection) Receive(ctx context.Context, p []byte) (int, peer.Principal, error) {
	return c.receive(ctx, p)
}
func (c *controlConnection) Close() error { c.closed.Add(1); return c.closeErr }
func clientFixture(t testing.TB, f *fixture, connection *controlConnection) *in.ControlClient {
	t.Helper()
	// Synthetic accounting only: no native transport and no Linux footprint claim.
	transport, err := fd.OwnRuntimeResource(context.Background(), f.r, fd.ResourcePlan[peer.ControlConnection]{Bytes: 4096,
		Create: func(context.Context) (fd.Created[peer.ControlConnection], error) {
			return fd.Created[peer.ControlConnection]{Value: connection, Owned: true}, nil
		},
		Release: func(c peer.ControlConnection) error { return c.Close() },
	})
	must(t, err)
	client, err := in.NewControlClient(context.Background(), f.r, transport, command(f, 0, 0).Target, "broker-execution-1", uint64(time.Second))
	must(t, err)
	t.Cleanup(client.Close)
	return client
}
func clientReply(t testing.TB, q in.ControlCommand, cause error) []byte {
	t.Helper()
	result := in.ControlResult{ID: q.Header.ID, Kind: q.Kind}
	if q.Kind != in.ControlStatus && cause == nil {
		result.Receipt = in.ControlReceipt{ID: q.Header.ID, Sequence: q.Header.Sequence, Grant: q.Header.Grant, Outcome: "revoked", Stage: "done"}
	}
	reply, err := in.BuildControlReply(q.Header.Target, result, cause)
	must(t, err)
	packet, err := in.EncodeControlReply(reply)
	must(t, err)
	return packet
}
func TestControlClientCorrelatedResponsesAndServerDenial(t *testing.T) {
	f, _, _, _ := accessFixture(t)
	var response []byte
	var denial error
	writes := 0
	c := &controlConnection{generation: "broker-execution-1"}
	c.write = func(ctx context.Context, p []byte, cap peer.ProcessCapability) error {
		writes++
		q, err := in.DecodeControl(p)
		must(t, err)
		response = clientReply(t, q, denial)
		return nil
	}
	c.receive = func(ctx context.Context, p []byte) (int, peer.Principal, error) {
		return copy(p, response), peer.Principal{Generation: c.generation}, nil
	}
	client := clientFixture(t, f, c)
	q := in.ControlCommand{Kind: in.ControlRevoke, Header: command(f, 1, 0)}
	for _, cause := range []error{nil, peer.ErrUnauthorized} {
		denial = cause
		out, err := client.Exchange(context.Background(), q, nil)
		must(t, err)
		if !out.SendStarted || !out.ResponseReceived || (out.Reply.Code == in.ReplyOK) != (cause == nil) {
			t.Fatal("confirmation is not a success assumption", out)
		}
	}
	if writes != 2 || client.Stats().Closed || client.Stats().Responses != 2 || client.Stats().Uncertain != 0 {
		t.Fatal(client.Stats())
	}
	if f.r.Stats().ReservedResourceBytes != 24576 {
		t.Fatal("packet storage escaped shared budget")
	}
}
func TestControlClientNoReplayOnUncertainOrForgedResponse(t *testing.T) {
	for _, mode := range []string{"write_error", "lost_ack", "wrong_sender", "wrong_id", "oversize", "generation_changed", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			f, _, _, _ := accessFixture(t)
			q := in.ControlCommand{Kind: in.ControlRevoke, Header: command(f, 1, 0)}
			packet := clientReply(t, q, nil)
			writes := 0
			c := &controlConnection{generation: "broker-execution-1"}
			c.write = func(context.Context, []byte, peer.ProcessCapability) error {
				writes++
				if mode == "write_error" {
					return peer.ErrDeadline
				}
				return nil
			}
			c.receive = func(ctx context.Context, p []byte) (int, peer.Principal, error) {
				principal := peer.Principal{Generation: "broker-execution-1"}
				switch mode {
				case "lost_ack":
					return 0, principal, peer.ErrDeadline
				case "wrong_sender":
					principal.Generation = "foreign"
				case "wrong_id":
					other := q
					other.Header.ID[0]++
					packet = clientReply(t, other, nil)
				case "oversize":
					return len(p) + 1, principal, nil
				case "generation_changed":
					c.generation = "broker-execution-2"
				case "malformed":
					packet = []byte("bad")
				}
				return copy(p, packet), principal, nil
			}
			client := clientFixture(t, f, c)
			out, err := client.Exchange(context.Background(), q, nil)
			if err == nil || !out.SendStarted || out.ResponseReceived || out.Reply.ID != ([16]byte{}) {
				t.Fatal("ambiguous delivery reported confirmed", out, err)
			}
			if !client.Stats().Closed || client.Stats().Uncertain != 1 {
				t.Fatal(client.Stats())
			}
			_, err = client.Exchange(context.Background(), q, nil)
			if !errors.Is(err, peer.ErrRetired) || writes != 1 {
				t.Fatal("replayed mutation", writes, err)
			}
		})
	}
}
func TestControlClientInvalidRequestDoesNotConsumeChannel(t *testing.T) {
	f, _, _, _ := accessFixture(t)
	c := &controlConnection{generation: "broker-execution-1"}
	c.write = func(context.Context, []byte, peer.ProcessCapability) error {
		t.Fatal("invalid command reached transport")
		return nil
	}
	client := clientFixture(t, f, c)
	q := in.ControlCommand{Kind: in.ControlRevoke, Header: command(f, 1, 0)}
	wrong := q
	wrong.Header.Target.Namespace[0]++
	for _, item := range []in.ControlCommand{wrong, {}} {
		out, err := client.Exchange(context.Background(), item, nil)
		if err == nil || out.SendStarted || client.Stats().Closed {
			t.Fatal(out, err)
		}
	}
	out, err := client.Exchange(context.Background(), q, consumer())
	if err == nil || out.SendStarted || client.Stats().Closed {
		t.Fatal(out, err)
	}
}
func TestControlClientBusyCloseRetainsBorrowAndProviderFailure(t *testing.T) {
	f, _, _, _ := accessFixture(t)
	started, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	c := &controlConnection{generation: "broker-execution-1", closeErr: peer.ErrUnavailable}
	c.write = func(context.Context, []byte, peer.ProcessCapability) error { return nil }
	c.receive = func(context.Context, []byte) (int, peer.Principal, error) {
		close(started)
		<-release
		return 0, peer.Principal{}, peer.ErrDeadline
	}
	client := clientFixture(t, f, c)
	q := in.ControlCommand{Kind: in.ControlRevoke, Header: command(f, 1, 0)}
	go func() { _, err := client.Exchange(context.Background(), q, nil); result <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("receive did not start")
	}
	_, err := client.Exchange(context.Background(), q, nil)
	if !errors.Is(err, peer.ErrBusy) {
		t.Fatal(err)
	}
	client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err = client.WaitClosed(ctx); err == nil {
		t.Fatal("stuck borrow released early")
	}
	if f.r.Stats().ReservedResourceBytes != 24576 || c.closed.Load() != 0 {
		t.Fatal("stuck resources no longer charged")
	}
	close(release)
	select {
	case err = <-result:
		if err == nil {
			t.Fatal("closed client confirmed")
		}
	case <-time.After(time.Second):
		t.Fatal("receive did not return")
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err = client.WaitClosed(ctx2); err == nil {
		t.Fatal("physical release failure hidden")
	}
	if c.closed.Load() != 1 || f.r.Stats().ReservedResourceBytes != 4096 {
		t.Fatal("failed transport released or retried", f.r.Stats())
	}
}

func TestEncodeControlIntoCanonicalAndNonMutatingFailure(t *testing.T) {
	f, _, _, _ := accessFixture(t)
	q := in.ControlCommand{Kind: in.ControlRevoke, Header: command(f, 1, 0)}
	buffer := make([]byte, in.MaxControlBytes)
	n, err := in.EncodeControlInto(buffer, q)
	must(t, err)
	owned, err := in.EncodeControl(q)
	must(t, err)
	if string(buffer[:n]) != string(owned) {
		t.Fatal("wire changed")
	}
	before := string(buffer)
	_, err = in.EncodeControlInto(buffer, in.ControlCommand{})
	if err == nil || string(buffer) != before {
		t.Fatal("invalid encode changed admitted bytes")
	}
	_, err = in.EncodeControlInto(buffer[:10], q)
	if err == nil {
		t.Fatal("short buffer accepted")
	}
}

func TestControlClientTotalDeadlineAndPanicRetireChannel(t *testing.T) {
	for _, mode := range []string{"deadline", "panic"} {
		t.Run(mode, func(t *testing.T) {
			f, _, _, _ := accessFixture(t)
			c := &controlConnection{generation: "broker-execution-1"}
			var deadline time.Time
			c.write = func(ctx context.Context, _ []byte, _ peer.ProcessCapability) error {
				var ok bool
				deadline, ok = ctx.Deadline()
				if !ok {
					t.Fatal("send has no bounded deadline")
				}
				return nil
			}
			c.receive = func(ctx context.Context, _ []byte) (int, peer.Principal, error) {
				if mode == "panic" {
					panic("fixture provider panic")
				}
				end, ok := ctx.Deadline()
				if !ok || end != deadline {
					t.Fatal("receive restarted total budget")
				}
				<-ctx.Done()
				return 0, peer.Principal{}, ctx.Err()
			}
			client := clientFixture(t, f, c)
			q := in.ControlCommand{Kind: in.ControlRevoke, Header: command(f, 1, 0)}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			if mode == "panic" {
				func() {
					defer func() {
						if recover() == nil {
							t.Fatal("provider panic was silently swallowed")
						}
					}()
					_, _ = client.Exchange(ctx, q, nil)
				}()
			} else {
				out, err := client.Exchange(ctx, q, nil)
				if !errors.Is(err, context.DeadlineExceeded) || out.ResponseReceived {
					t.Fatal(out, err)
				}
			}
			if !client.Stats().Closed || client.Stats().Uncertain != 1 {
				t.Fatal(client.Stats())
			}
			wait, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			must(t, client.WaitClosed(wait))
			if f.r.Stats().ReservedResourceBytes != 0 {
				t.Fatal("completed unwind retained packet storage")
			}
		})
	}
}
