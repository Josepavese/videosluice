package ingress_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	in "github.com/Josepavese/videosluice/frameingress"
	peer "github.com/Josepavese/videosluice/framepeer"
	wire "github.com/Josepavese/videosluice/framewire"
)

type sourceConnectionFixture struct {
	generation atomic.Value
	reads      atomic.Int32
	records    [][]byte
	hook       func(context.Context, int) error
	principal  string
}

func (s *sourceConnectionFixture) CurrentGeneration() (string, error) {
	return s.generation.Load().(string), nil
}
func (s *sourceConnectionFixture) Receive(ctx context.Context, p []byte) (int, peer.Principal, error) {
	j := int(s.reads.Add(1)) - 1
	if s.hook != nil {
		if e := s.hook(ctx, j); e != nil {
			return 0, peer.Principal{}, e
		}
	}
	if j >= len(s.records) {
		return 0, peer.Principal{}, io.EOF
	}
	n := copy(p, s.records[j])
	return n, peer.Principal{Generation: s.principal}, nil
}
func sourceConnection(t *testing.T, p []byte) *sourceConnectionFixture {
	s := &sourceConnectionFixture{records: [][]byte{p[:wire.HeaderSize]}, principal: "source.worker"}
	s.generation.Store(s.principal)
	id := sha256.Sum256(p[:wire.HeaderSize])
	p = p[wire.HeaderSize:]
	chunk := 64
	if len(p) > wire.MaxDataPacketBytes {
		chunk = wire.MaxDataPacketBytes - wire.DataHeaderSize
	}
	for off := 0; off < len(p); off += chunk {
		storage := make([]byte, wire.MaxDataPacketBytes)
		n, e := wire.EncodeDataChunk(storage, id, off, p[off:min(off+chunk, len(p))])
		must(t, e)
		s.records = append(s.records, storage[:n:n])
	}
	return s
}
func receiveSource(t *testing.T, f *fixture, s *sourceConnectionFixture) *in.SourceReceiver {
	t.Helper()
	r, e := in.NewSourceReceiver(context.Background(), f.i, s, "source.worker", f.c, uint64(time.Second), 100, sha256.Sum256, 0)
	must(t, e)
	t.Cleanup(r.Close)
	return r
}
func TestSourceReceiverPublishesOnlyCompleteFrame(t *testing.T) {
	f := setup(t)
	baseline := f.r.Stats().ReservedResourceBytes
	s := sourceConnection(t, f.surface(t, 1))
	r := receiveSource(t, f, s)
	if f.r.Stats().ReservedResourceBytes != baseline+wire.MaxDataPacketBytes+wire.HeaderSize+384 {
		t.Fatal("workspace not admitted")
	}
	must(t, r.Receive(context.Background(), nil))
	tk := f.ticket(t)
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
	f.verify(t, tk, 1)
	if a := r.Stats(); a.Frames != 1 || a.Packets != 7 || a.Failures != 0 {
		t.Fatal(a)
	}
	r.Close()
	must(t, r.WaitClosed(context.Background()))
	if f.r.Stats().ReservedResourceBytes != baseline {
		t.Fatal("arena not released")
	}
	want(t, r.Receive(context.Background(), nil), peer.ErrRetired)
}

func TestSourceReceiverIdleOnlyBeforeAnyFrameRecord(t *testing.T) {
	for _, mode := range []string{"empty_then_frame", "partial", "unproven_timeout", "parent_cancelled", "retired"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			s := sourceConnection(t, f.surface(t, 1))
			r := receiveSource(t, f, s)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s.hook = func(_ context.Context, index int) error {
				if mode == "partial" && index == 0 {
					return nil
				}
				if mode == "unproven_timeout" {
					return peer.ErrDeadline
				}
				if mode == "parent_cancelled" {
					cancel()
				}
				if mode == "retired" {
					s.generation.Store("retired.worker")
				}
				return errors.Join(peer.ErrNoRecord, peer.ErrDeadline)
			}
			err := r.Receive(ctx, nil)
			if mode == "empty_then_frame" {
				want(t, err, in.ErrSourceIdle)
				if r.Stats().Frames != 0 || r.Stats().Failures != 0 || r.Stats().Packets != 0 {
					t.Fatal("idle became progress/failure", r.Stats())
				}
				s.hook = nil
				s.reads.Store(0)
				must(t, r.Receive(ctx, nil))
				if r.Stats().Frames != 1 {
					t.Fatal("idle prevented the first real frame")
				}
			} else {
				if err == nil || errors.Is(err, in.ErrSourceIdle) {
					t.Fatal("uncertain/partial receive treated as idle", err)
				}
				want(t, r.Receive(context.Background(), nil), peer.ErrRetired)
				must(t, r.WaitClosed(context.Background()))
			}
		})
	}
}
func TestSourceReceiverRejectsIdentityBeforePayload(t *testing.T) {
	cases := []struct {
		name   string
		change func(*wire.Header)
	}{
		{"source", func(h *wire.Header) { h.Source = "rear" }},
		{"epoch", func(h *wire.Header) { h.SourceEpoch++ }},
		{"process", func(h *wire.Header) { h.Process = "other" }},
		{"namespace", func(h *wire.Header) { h.Namespace[0] ^= 1 }},
		{"graph", func(h *wire.Header) { h.Graph[0] ^= 2 }},
		{"transform", func(h *wire.Header) { h.Transform[0] ^= 1 }},
		{"geometry", func(h *wire.Header) { h.Width = 8; h.Height = 32; h.StrideY = 8; h.StrideUV = 8 }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := setup(t)
			p := mutate(t, f.surface(t, 1), c.change)
			s := sourceConnection(t, p)
			r := receiveSource(t, f, s)
			if e := r.Receive(context.Background(), nil); e == nil {
				t.Fatal("foreign source accepted")
			}
			if s.reads.Load() != 1 || r.Stats().Frames != 0 || r.Stats().Failures != 1 {
				t.Fatal("foreign payload consumed")
			}
			must(t, r.WaitClosed(context.Background()))
			want(t, r.Receive(context.Background(), nil), peer.ErrRetired)
		})
	}
}
func TestSourceReceiverMidTransferFaultDoesNotPublishOrResync(t *testing.T) {
	for _, mode := range []string{"generation", "principal", "binding", "duplicate", "manifest", "hash", "missing", "header"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			s := sourceConnection(t, f.surface(t, 1))
			r := receiveSource(t, f, s)
			s.hook = func(_ context.Context, j int) error {
				if j == 2 {
					switch mode {
					case "generation":
						s.generation.Store("another.worker")
					case "principal":
						s.principal = "another.worker"
					case "binding":
						f.b.Close()
					}
				}
				return nil
			}
			switch mode {
			case "duplicate":
				s.records[2] = s.records[1]
			case "manifest":
				s.records[2][8] ^= 1
			case "hash":
				s.records[len(s.records)-1][wire.DataHeaderSize] ^= 1
			case "missing":
				s.records = s.records[:len(s.records)-1]
			case "header":
				s.records[2] = s.records[0]
			}
			if e := r.Receive(context.Background(), nil); e == nil {
				t.Fatal("broken frame published")
			}
			if r.Stats().Frames != 0 || r.Stats().Failures != 1 {
				t.Fatal(r.Stats())
			}
			must(t, r.WaitClosed(context.Background()))
			n := s.reads.Load()
			want(t, r.Receive(context.Background(), nil), peer.ErrRetired)
			if s.reads.Load() != n {
				t.Fatal("implicit resync consumed more data")
			}
			if mode != "binding" {
				tk := f.ticket(t)
				must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
				f.expectExpiredSurface(t, tk)
			}
		})
	}
}
func TestSourceReceiverOneDeadlineForEveryChunk(t *testing.T) {
	f := setup(t)
	s := sourceConnection(t, f.surface(t, 1))
	var deadline time.Time
	s.hook = func(ctx context.Context, j int) error {
		d, ok := ctx.Deadline()
		if !ok {
			return errors.New("missing receive deadline")
		}
		if j == 0 {
			deadline = d
		} else if !deadline.Equal(d) {
			return errors.New("deadline reset for a chunk")
		}
		if j < 2 {
			return nil
		}
		<-ctx.Done()
		return ctx.Err()
	}
	r, e := in.NewSourceReceiver(context.Background(), f.i, s, "source.worker", f.c, uint64(30*time.Millisecond), 100, sha256.Sum256, 0)
	must(t, e)
	defer r.Close()
	want(t, r.Receive(context.Background(), nil), context.DeadlineExceeded)
	if s.reads.Load() != 3 || r.Stats().Frames != 0 {
		t.Fatal("deadline did not abort incomplete frame")
	}
	must(t, r.WaitClosed(context.Background()))
}
func TestSourceReceiverCloseRetainsMemoryUntilSlowProviderJoins(t *testing.T) {
	f := setup(t)
	s := sourceConnection(t, f.surface(t, 1))
	base := f.r.Stats().ReservedResourceBytes
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	s.hook = func(context.Context, int) error { close(entered); <-release; return io.EOF }
	r := receiveSource(t, f, s)
	done := make(chan error, 1)
	go func() { done <- r.Receive(context.Background(), nil) }()
	<-entered
	want(t, r.Receive(context.Background(), nil), peer.ErrBusy)
	if r.Stats().Failures != 0 {
		t.Fatal("contention retired in-flight frame")
	}
	r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	want(t, r.WaitClosed(ctx), context.DeadlineExceeded)
	if f.r.Stats().ReservedResourceBytes != base+wire.MaxDataPacketBytes+wire.HeaderSize+384 {
		t.Fatal("blocked provider memory falsely released")
	}
	// Allow the ignored-cancellation provider to return without double-close.
	release <- struct{}{}
	if e := <-done; e == nil {
		t.Fatal("closed receiver published")
	}
	must(t, r.WaitClosed(context.Background()))
	if f.r.Stats().ReservedResourceBytes != base {
		t.Fatal("joined arena leaked")
	}
}
func TestSourceReceiverAdmissionRejectsBeforeReceive(t *testing.T) {
	f := setup(t)
	s := sourceConnection(t, f.surface(t, 1))
	fill, e := fd.OwnResource(context.Background(), f.b, fd.ResourcePlan[int]{Bytes: (1 << 19) - 8192 - 100, Create: func(context.Context) (fd.Created[int], error) { return fd.Created[int]{Owned: true}, nil }, Release: func(int) error { return nil }})
	must(t, e)
	defer fill.Close()
	base := f.r.Stats().ReservedResourceBytes
	r, e := in.NewSourceReceiver(context.Background(), f.i, s, "source.worker", f.c, uint64(time.Second), 100, sha256.Sum256, 0)
	want(t, e, fa.CapacityRejected)
	if r != nil || s.reads.Load() != 0 || f.r.Stats().ReservedResourceBytes != base {
		t.Fatal("unadmitted receiver allocated/published")
	}
}

func TestSourceReceiverHashWorkspaceAdmissionAndLifetime(t *testing.T) {
	f := setup(t)
	s := sourceConnection(t, f.surface(t, 1))
	base := f.r.Stats().ReservedResourceBytes
	entered, release := make(chan struct{}), make(chan struct{})
	hash := func(p []byte) [32]byte { close(entered); <-release; return sha256.Sum256(p) }
	r, err := in.NewSourceReceiver(context.Background(), f.i, s, "source.worker", f.c, uint64(time.Second), 100, hash, 4096)
	must(t, err)
	wantCharge := base + wire.MaxDataPacketBytes + wire.HeaderSize + 384 + 4096
	if f.r.Stats().ReservedResourceBytes != wantCharge {
		t.Fatal("hash workspace not charged")
	}
	done := make(chan error, 1)
	go func() { done <- r.Receive(context.Background(), nil) }()
	<-entered
	r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	want(t, r.WaitClosed(ctx), context.DeadlineExceeded)
	if f.r.Stats().ReservedResourceBytes != wantCharge {
		t.Fatal("active hash workspace released")
	}
	close(release)
	if err := <-done; err == nil {
		t.Fatal("closed hash published")
	}
	must(t, r.WaitClosed(context.Background()))
	if f.r.Stats().ReservedResourceBytes != base {
		t.Fatal("hash workspace leaked")
	}
	for _, workspace := range []int{-1, 1<<20 + 1, 1 << 20} {
		r, err := in.NewSourceReceiver(context.Background(), f.i, s, "source.worker", f.c, uint64(time.Second), 100, sha256.Sum256, workspace)
		if err == nil || r != nil {
			t.Fatal("invalid or unadmitted workspace accepted", workspace)
		}
	}
}

type sourceBusyCopy struct {
	copyport.CPU
	once          atomic.Bool
	cause         fa.Code
	beforeFailure func()
}

func (c *sourceBusyCopy) CopyNV12(ctx context.Context, budget fa.CopyBudget, dst, src []byte, g fa.Geometry, box fa.Rect) error {
	if c.once.Swap(false) {
		if c.beforeFailure != nil {
			c.beforeFailure()
		}
		if c.cause != "" {
			return c.cause
		}
		return fa.CapacityRejected
	}
	return c.CPU.CopyNV12(ctx, budget, dst, src, g, box)
}
func TestSourceReceiverCompleteRejectedFrameKeepsFraming(t *testing.T) {
	for _, mode := range []string{"capacity", "expired", "copy_deadline"} {
		t.Run(mode, func(t *testing.T) {
			copier := &sourceBusyCopy{}
			copier.once.Store(mode != "expired")
			if mode == "copy_deadline" {
				copier.cause = fa.DeadlineExceeded
			}
			f := setupCopier(t, copier)
			s := sourceConnection(t, f.surface(t, 1))
			firstRecords := len(s.records)
			s.records = append(s.records, sourceConnection(t, f.surface(t, 2)).records...)
			if mode == "expired" {
				s.hook = func(_ context.Context, j int) error {
					if j == firstRecords-1 {
						f.c.now.Store(1200)
					}
					return nil
				}
			}
			r := receiveSource(t, f, s)
			code := fa.CapacityRejected
			if mode == "expired" {
				code = fa.Expired
			} else if mode == "copy_deadline" {
				code = fa.DeadlineExceeded
			}
			want(t, r.Receive(context.Background(), nil), code)
			tk := f.ticket(t)
			must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
			f.expectExpiredSurface(t, tk)
			must(t, r.Receive(context.Background(), nil))
			tk = f.ticket(t)
			must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 2)))
			f.verify(t, tk, 2)
			if r.Stats().Frames != 1 || r.Stats().Failures != 1 || r.Stats().Drops != 1 {
				t.Fatal(r.Stats())
			}
		})
	}
}

func TestSourceReceiverCopyBudgetDoesNotMaskTransportOrClockFailure(t *testing.T) {
	for _, mode := range []string{"canceled", "clock_backwards"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			copier := &sourceBusyCopy{cause: fa.DeadlineExceeded}
			copier.once.Store(true)
			f := setupCopier(t, copier)
			s := sourceConnection(t, f.surface(t, 1))
			if mode == "canceled" {
				copier.beforeFailure = cancel
			} else {
				s.hook = func(_ context.Context, index int) error {
					if index == len(s.records)-1 {
						f.c.now.Store(99)
					}
					return nil
				}
			}
			r := receiveSource(t, f, s)
			if err := r.Receive(ctx, nil); err == nil {
				t.Fatal("unsafe publication accepted")
			}
			if r.Stats().Drops != 0 || r.Stats().Frames != 0 {
				t.Fatal("transport/clock failure counted as optional drop", r.Stats())
			}
			must(t, r.WaitClosed(context.Background()))
			want(t, r.Receive(context.Background(), nil), peer.ErrRetired)
		})
	}
}

type sourcePublicationDelayAuthority struct {
	clock *clock
	armed atomic.Bool
	calls atomic.Int32
}

func (a *sourcePublicationDelayAuthority) CurrentGeneration() (string, error) {
	if a.armed.Load() && a.calls.Add(1) == 3 {
		// After the last-read and pre-publication identity checks and budget
		// creation: reproduce a delayed domain begin before store admission,
		// without sleeping.
		a.clock.now.Store(300)
	}
	return "authenticated-process-1", nil
}

func TestSourceReceiverSurfaceBudgetExpiredBeforeAdmissionKeepsNextFrame(t *testing.T) {
	a := &sourcePublicationDelayAuthority{}
	f := setupCopierAuthority(t, copyport.CPU{}, a)
	a.clock = f.c
	s := sourceConnection(t, f.surface(t, 1))
	firstRecords := len(s.records)
	s.records = append(s.records, sourceConnection(t, f.surface(t, 2)).records...)
	s.hook = func(_ context.Context, index int) error {
		if index == firstRecords-1 {
			a.armed.Store(true)
		}
		return nil
	}
	r := receiveSource(t, f, s)
	err := r.Receive(context.Background(), nil)
	want(t, err, fa.DeadlineExceeded)
	if !strings.Contains(err.Error(), "surface_publish_progress phase=admission") {
		t.Fatalf("fixture did not reproduce pre-admission expiry: %v", err)
	}
	missing := f.ticket(t)
	must(t, f.i.AcceptDelivery(context.Background(), missing, f.delivery(t, missing, 1)))
	f.expectExpiredSurface(t, missing)
	must(t, r.Receive(context.Background(), nil))
	ticket := f.ticket(t)
	must(t, f.i.AcceptDelivery(context.Background(), ticket, f.delivery(t, ticket, 2)))
	f.verify(t, ticket, 2)
	if r.Stats().Drops != 1 || r.Stats().Frames != 1 || r.Stats().Failures != 1 {
		t.Fatal(r.Stats())
	}
}

func TestSourceReceiverHDFaultOnLastChunkNeverPublishes(t *testing.T) {
	for _, mode := range []string{"tamper", "source_restart"} {
		t.Run(mode, func(t *testing.T) {
			f := setupGeometryAuthority(t, copyport.CPU{}, nil, 1280, 720)
			s := sourceConnection(t, f.surface(t, 1))
			if len(s.records) != 86 {
				t.Fatal("HD chunk count", len(s.records))
			}
			if mode == "tamper" {
				s.records[85][wire.DataHeaderSize] ^= 1
			} else {
				s.hook = func(_ context.Context, j int) error {
					if j == 85 {
						s.generation.Store("replacement.worker")
					}
					return nil
				}
			}
			r := receiveSource(t, f, s)
			if err := r.Receive(context.Background(), nil); err == nil {
				t.Fatal("unproven HD source accepted")
			}
			if r.Stats().Frames != 0 || r.Stats().Drops != 0 {
				t.Fatal("partial HD treated as valid drop")
			}
			origin := fd.OriginRef{Fence: f.b.Fence(), Ref: fa.OriginRef{Source: "front", MediaEpoch: f.b.Scope().MediaEpoch, Sequence: 1, Graph: f.b.Scope().Graph}}
			ticket := f.ticket(t)
			must(t, f.b.BindExact(context.Background(), origin, ticket, f.tr))
			f.expectExpiredSurface(t, ticket)
			must(t, r.WaitClosed(context.Background()))
		})
	}
}

// The epoch churn must be attributable from the counters alone: an epoch is
// retired today both when the source failed and when a frame missed its own
// deadline while waiting, and those two are different events with different
// responses. Before this the only distinction lived in the failure text.
func TestSourceReceiverCountsFailuresByCause(t *testing.T) {
	t.Run("local deadline", func(t *testing.T) {
		f := setup(t)
		s := sourceConnection(t, f.surface(t, 1))
		s.hook = func(ctx context.Context, j int) error {
			if j < 2 {
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		}
		r, e := in.NewSourceReceiver(context.Background(), f.i, s, "source.worker", f.c, uint64(30*time.Millisecond), 100, sha256.Sum256, 0)
		must(t, e)
		defer r.Close()
		want(t, r.Receive(context.Background(), nil), context.DeadlineExceeded)
		stats := r.Stats()
		if stats.Failures != 1 || stats.LocalDeadlineFailures != 1 || stats.PeerFailures != 0 || stats.LocalCancelFailures != 0 {
			t.Fatalf("deadline failure not attributed: %+v", stats)
		}
	})

	t.Run("peer failure", func(t *testing.T) {
		f := setup(t)
		s := sourceConnection(t, f.surface(t, 1))
		s.hook = func(context.Context, int) error { return peer.ErrUnavailable }
		r, e := in.NewSourceReceiver(context.Background(), f.i, s, "source.worker", f.c, uint64(time.Second), 100, sha256.Sum256, 0)
		must(t, e)
		defer r.Close()
		if err := r.Receive(context.Background(), nil); !errors.Is(err, peer.ErrUnavailable) {
			t.Fatalf("peer failure not reported: %v", err)
		}
		stats := r.Stats()
		if stats.Failures != 1 || stats.PeerFailures != 1 || stats.LocalDeadlineFailures != 0 || stats.LocalCancelFailures != 0 {
			t.Fatalf("peer failure not attributed: %+v", stats)
		}
	})

	t.Run("owner cancel", func(t *testing.T) {
		f := setup(t)
		s := sourceConnection(t, f.surface(t, 1))
		s.hook = func(ctx context.Context, j int) error {
			if j < 2 {
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		}
		r, e := in.NewSourceReceiver(context.Background(), f.i, s, "source.worker", f.c, uint64(time.Second), 100, sha256.Sum256, 0)
		must(t, e)
		defer r.Close()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- r.Receive(ctx, nil) }()
		time.Sleep(50 * time.Millisecond)
		cancel()
		want(t, <-done, context.Canceled)
		stats := r.Stats()
		if stats.Failures != 1 || stats.LocalCancelFailures != 1 || stats.PeerFailures != 0 || stats.LocalDeadlineFailures != 0 {
			t.Fatalf("owner cancel not attributed: %+v", stats)
		}
	})
}

func TestClassifyReceiveFailure(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want in.ReceiveFailureCause
	}{
		"nil":                 {nil, in.ReceiveFailureUnclassified},
		"local deadline":      {fmt.Errorf("stage=source_receive_deadline: %w", context.DeadlineExceeded), in.ReceiveFailureLocalDeadline},
		"local cancel":        {fmt.Errorf("stage=source_receive_deadline: %w", context.Canceled), in.ReceiveFailureLocalCancel},
		"peer unavailable":    {fmt.Errorf("stage=source_receive: %w", peer.ErrUnavailable), in.ReceiveFailurePeer},
		"peer retired":        {fmt.Errorf("stage=source_receive: %w", peer.ErrRetired), in.ReceiveFailurePeer},
		"unknown transport":   {errors.New("broken pipe"), in.ReceiveFailureUnclassified},
		"deadline beats peer": {fmt.Errorf("peer idle: %w and %w", peer.ErrDeadline, context.DeadlineExceeded), in.ReceiveFailureLocalDeadline},
	} {
		t.Run(name, func(t *testing.T) {
			if got := in.ClassifyReceiveFailure(tc.err); got != tc.want {
				t.Fatalf("cause=%s, want %s", got, tc.want)
			}
		})
	}
}
