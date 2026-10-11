package ingress_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
)

type sessionConnection struct {
	receive  func(context.Context, []byte) (int, peer.Principal, error)
	write    func(context.Context, []byte) error
	closeErr error
	closed   atomic.Int32
}

func (c *sessionConnection) Receive(ctx context.Context, p []byte) (int, peer.Principal, error) {
	return c.receive(ctx, p)
}
func (c *sessionConnection) WritePacket(ctx context.Context, p []byte) error { return c.write(ctx, p) }
func (c *sessionConnection) CurrentGeneration() (string, error) {
	if c.closed.Load() != 0 {
		return "", peer.ErrRetired
	}
	return "consumer-start-1", nil
}
func (c *sessionConnection) Close() error { c.closed.Add(1); return c.closeErr }

func ownSessionFixture(t testing.TB, r *fd.Runtime, c *sessionConnection) *fd.Resource[peer.Connection] {
	t.Helper()
	// Synthetic provider accounting only: these 4096B are NOT a Linux socket
	// footprint, and no native socket is created by this fixture plan.
	o, err := fd.OwnRuntimeResource(context.Background(), r, fd.ResourcePlan[peer.Connection]{Bytes: 4096,
		Create: func(context.Context) (fd.Created[peer.Connection], error) {
			return fd.Created[peer.Connection]{Value: c, Owned: true}, nil
		},
		Release: func(c peer.Connection) error { return c.Close() },
	})
	must(t, err)
	return o
}

func frameSessionFixture(t *testing.T) (*fixture, *in.GrantControl, in.FrameRequest) {
	t.Helper()
	f, control, _, q := controlFixture(t, 4)
	policy := controlPolicy("consumer-start-1")
	policy.MaxRegions = 2
	r, err := control.Grant(controllerPrincipal(), command(f, 1, 0), f.b.Reference(), consumer(), policy)
	outcome(t, r, err, "installed")
	return f, control, in.FrameRequest{ID: q.ID, Frame: q.Frame, Source: q.Source, Surface: q.Surface, Regions: q.Regions, BudgetNS: 100}
}

func waitSession(t testing.TB, s *in.FrameSession) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, s.WaitClosed(ctx))
}

func TestFrameSessionIdleThenAllRegionsWithOneBoundedLifetime(t *testing.T) {
	f, control, q := frameSessionFixture(t)
	q.Regions = append(q.Regions, fa.Region{ID: "outside", BBox: [4]float64{100, 100, 110, 110}})
	packet, err := in.EncodeFrameRequest(q)
	must(t, err)
	receives, writes := 0, 0
	c := &sessionConnection{receive: func(ctx context.Context, p []byte) (int, peer.Principal, error) {
		receives++
		if receives == 1 {
			return 0, peer.Principal{}, peer.ErrDeadline
		}
		return copy(p, packet), principal(), nil
	},
		write: func(ctx context.Context, p []byte) error { writes++; return nil }}
	transport := ownSessionFixture(t, f.r, c)
	s, err := in.NewFrameSession(context.Background(), f.r, control, "reader", transport, 1024, uint64(time.Second))
	must(t, err)
	if f.r.Stats().ReservedResourceBytes != 4096+4096+16384 {
		t.Fatal("session buffers bypass global accounting")
	}
	complete, err := s.Step(context.Background())
	must(t, err)
	if complete || s.Stats().Idle != 1 || s.Stats().Closed {
		t.Fatal("idle treated as failure")
	}
	complete, err = s.Step(context.Background())
	must(t, err)
	if !complete || writes != 3 || s.Stats().Received != 1 || s.Stats().Completed != 1 {
		t.Fatal("multi-region request incomplete", s.Stats(), writes)
	}
	s.Close()
	waitSession(t, s)
	if c.closed.Load() != 1 || f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal("session ownership not released exactly once")
	}
	if stats, _ := control.Status(controllerPrincipal()); stats.Closed || stats.Active != 1 {
		t.Fatal("session closed shared controller")
	}
}

func TestFrameSessionRejectsBadRecordsAndClosesFailedWriter(t *testing.T) {
	for _, mode := range []string{"malformed", "foreign", "write_failure", "revoked", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			f, control, q := frameSessionFixture(t)
			packet, err := in.EncodeFrameRequest(q)
			must(t, err)
			if mode == "malformed" {
				packet = []byte("bad")
			}
			if mode == "revoked" {
				_, err := control.Revoke(controllerPrincipal(), command(f, 2, 1))
				must(t, err)
			}
			writes := 0
			c := &sessionConnection{receive: func(ctx context.Context, p []byte) (int, peer.Principal, error) {
				who := principal()
				if mode == "foreign" {
					who.Generation = "foreign"
				}
				if mode == "oversize" {
					return len(p) + 1, who, nil
				}
				return copy(p, packet), who, nil
			}, write: func(ctx context.Context, p []byte) error { writes++; return errors.New("fixture blocked output") }}
			s, err := in.NewFrameSession(context.Background(), f.r, control, "reader", ownSessionFixture(t, f.r, c), 1024, uint64(time.Second))
			must(t, err)
			complete, err := s.Step(context.Background())
			if err == nil || complete || !s.Stats().Closed || s.Stats().Completed != 0 {
				t.Fatal("bad request remained live")
			}
			waitSession(t, s)
			if mode != "write_failure" && writes != 0 {
				t.Fatal("unauthorized data written")
			}
			if c.closed.Load() != 1 || s.Stats().LastStage == "" {
				t.Fatal("missing close/stage evidence")
			}
		})
	}
}

func TestFrameSessionCancellationAndStubbornReceiveKeepAdmission(t *testing.T) {
	f, control, _ := frameSessionFixture(t)
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c := &sessionConnection{receive: func(ctx context.Context, p []byte) (int, peer.Principal, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return 0, peer.Principal{}, ctx.Err()
	}, write: func(context.Context, []byte) error { t.Fatal("unexpected write"); return nil }}
	s, err := in.NewFrameSession(context.Background(), f.r, control, "reader", ownSessionFixture(t, f.r, c), 1024, uint64(time.Second))
	must(t, err)
	done := make(chan error, 1)
	go func() { _, err := s.Step(context.Background()); done <- err }()
	<-entered
	_, err = s.Step(context.Background())
	want(t, err, peer.ErrBusy)
	s.Close()
	<-cancelled
	if c.closed.Load() != 0 || f.r.Stats().ReservedResourceBytes < 8192 {
		t.Fatal("live receive ownership falsely released")
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("cancelled receive succeeded")
	}
	waitSession(t, s)
	if c.closed.Load() != 1 || f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal("cancelled session leaked")
	}
}

func TestFrameSessionFailedPhysicalCloseRemainsCharged(t *testing.T) {
	f, control, _ := frameSessionFixture(t)
	c := &sessionConnection{closeErr: errors.New("fixture close failed")}
	s, err := in.NewFrameSession(context.Background(), f.r, control, "reader", ownSessionFixture(t, f.r, c), 1024, uint64(time.Second))
	must(t, err)
	s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = s.WaitClosed(ctx); err == nil {
		t.Fatal("failed close claimed success")
	}
	if stats := f.r.Stats(); stats.ReservedResourceBytes != 4096 || stats.ResourceFailures != 1 || c.closed.Load() != 1 {
		t.Fatal("failed transport close uncharged/retried", stats)
	}
}

func TestFrameSessionRejectsForeignResourceAndPreservesCallerOwnership(t *testing.T) {
	f, control, _ := frameSessionFixture(t)
	other := setup(t)
	c := &sessionConnection{}
	transport := ownSessionFixture(t, other.r, c)
	s, err := in.NewFrameSession(context.Background(), f.r, control, "reader", transport, 1024, uint64(time.Second))
	want(t, err, fd.ErrDomainMismatch)
	if s != nil || c.closed.Load() != 0 || f.r.Stats().ReservedResourceBytes != 0 {
		t.Fatal("failed admission took ownership")
	}
	transport.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	must(t, transport.WaitClosed(ctx))
	cameraConnection := &sessionConnection{}
	cameraOwned, err := fd.OwnResource(context.Background(), f.b, fd.ResourcePlan[peer.Connection]{Bytes: 4096,
		Create: func(context.Context) (fd.Created[peer.Connection], error) {
			return fd.Created[peer.Connection]{Value: cameraConnection, Owned: true}, nil
		},
		Release: func(c peer.Connection) error { return c.Close() },
	})
	must(t, err)
	s, err = in.NewFrameSession(context.Background(), f.r, control, "reader", cameraOwned, 1024, uint64(time.Second))
	want(t, err, fd.ErrDomainMismatch)
	if s != nil || cameraConnection.closed.Load() != 0 {
		t.Fatal("camera ownership accepted or consumed")
	}
	cameraOwned.Close()
	must(t, cameraOwned.WaitClosed(ctx))
}

func TestFrameSessionReturnsExplicitNegativeForMissingOrExpiredFrame(t *testing.T) {
	for _, mode := range []string{"missing", "expired"} {
		t.Run(mode, func(t *testing.T) {
			f, control, q := frameSessionFixture(t)
			if mode == "missing" {
				q.Frame.Ref.FrameID++
			} else {
				f.c.now.Store(5000)
			}
			packet, err := in.EncodeFrameRequest(q)
			must(t, err)
			writes := 0
			c := &sessionConnection{receive: func(ctx context.Context, p []byte) (int, peer.Principal, error) {
				return copy(p, packet), principal(), nil
			}, write: func(ctx context.Context, p []byte) error {
				writes++
				m, err := in.DecodeFrameManifest(p)
				if err != nil {
					return err
				}
				if m.Artifact.State != fa.CorrelationUnavailable && m.Artifact.State != fa.Expired {
					t.Fatal("absence lost its semantic reason", m.Artifact.State)
				}
				if m.Bytes != 0 || m.Artifact.Proof != (fd.Proof{}) {
					t.Fatal("negative reply carried pixels/proof")
				}
				return in.ValidateFrameManifest(m, q, 1024)
			}}
			s, err := in.NewFrameSession(context.Background(), f.r, control, "reader", ownSessionFixture(t, f.r, c), 1024, uint64(time.Second))
			must(t, err)
			complete, err := s.Step(context.Background())
			must(t, err)
			if !complete || writes != 1 || s.Stats().Closed || s.Stats().Negative != 1 {
				t.Fatal("semantic negative became network failure", s.Stats())
			}
			s.Close()
			waitSession(t, s)
		})
	}
}

func TestFrameSessionPanicCannotLeaveReusablePartialChannel(t *testing.T) {
	f, control, q := frameSessionFixture(t)
	packet, err := in.EncodeFrameRequest(q)
	must(t, err)
	c := &sessionConnection{receive: func(ctx context.Context, p []byte) (int, peer.Principal, error) {
		return copy(p, packet), principal(), nil
	}, write: func(context.Context, []byte) error { panic("fixture provider fault") }}
	s, err := in.NewFrameSession(context.Background(), f.r, control, "reader", ownSessionFixture(t, f.r, c), 1024, uint64(time.Second))
	must(t, err)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("fixture did not panic")
			}
		}()
		s.Step(context.Background())
	}()
	if !s.Stats().Closed {
		t.Fatal("partial channel reusable after panic")
	}
	waitSession(t, s)
	if c.closed.Load() != 1 {
		t.Fatal("provider panic leaked transport")
	}
}

func TestFrameSessionBlockedWriteRetainsTransportUntilActualReturn(t *testing.T) {
	for _, mode := range []string{"session", "transport", "runtime"} {
		t.Run(mode, func(t *testing.T) {
			f, control, q := frameSessionFixture(t)
			packet, err := in.EncodeFrameRequest(q)
			must(t, err)
			entered, release := make(chan struct{}), make(chan struct{})
			var writes atomic.Int32
			c := &sessionConnection{
				receive: func(ctx context.Context, p []byte) (int, peer.Principal, error) {
					return copy(p, packet), principal(), nil
				},
				write: func(ctx context.Context, p []byte) error {
					if writes.Add(1) != 1 {
						return errors.New("write after revocation")
					}
					close(entered)
					<-release
					// A provider returning success despite cancellation must
					// not turn a partial, revoked request into a completion.
					return nil
				},
			}
			transport := ownSessionFixture(t, f.r, c)
			s, err := in.NewFrameSession(context.Background(), f.r, control, "reader", transport, 1024, uint64(time.Second))
			must(t, err)
			type outcome struct {
				complete bool
				err      error
			}
			done := make(chan outcome, 1)
			go func() { complete, err := s.Step(context.Background()); done <- outcome{complete, err} }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("write did not start")
			}
			switch mode {
			case "session":
				s.Close()
			case "transport":
				transport.Close()
			case "runtime":
				f.r.Revoke()
			}
			if c.closed.Load() != 0 || f.r.Stats().ReservedResourceBytes < 4096 {
				t.Fatal("blocked I/O released its transport or budget")
			}
			close(release)
			select {
			case result := <-done:
				if result.complete || result.err == nil || !s.Stats().Closed || s.Stats().Completed != 0 {
					t.Fatal("revoked partial request reported success", result, s.Stats())
				}
			case <-time.After(time.Second):
				t.Fatal("request failed to terminate")
			}
			waitSession(t, s)
			if c.closed.Load() != 1 || writes.Load() != 1 || f.r.Stats().ReservedResourceBytes != 0 {
				t.Fatal("close leaked or emitted another packet", c.closed.Load(), writes.Load(), f.r.Stats())
			}
		})
	}
}
