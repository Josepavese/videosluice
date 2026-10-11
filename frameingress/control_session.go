package ingress

import (
	"context"
	"errors"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sync"
	"time"
)

type ControlSessionStats struct {
	Stage, LastStage                               string
	Closed                                         bool
	Received, Completed, Idle, Negative, Uncertain uint64
}

type ControlSession struct {
	control   *fd.Resource[*ControlDispatcher]
	transport *fd.Resource[peer.ControlServerConnection]
	workspace *fd.Resource[*DiscoveryWorkspace]
	buffer    *fd.Resource[[]byte]
	runtime   *fd.Runtime
	ctx       context.Context
	cancel    context.CancelFunc
	work      time.Duration
	operation sync.Mutex
	mu        sync.Mutex
	stats     ControlSessionStats
}

// NewControlSession consumes transport on success only. Shared control and
// workspace remain caller-owned; their admitted root resources must outlive
// this session. Only transport and a new20KiB packet resource close with the
// session. Metadata/native capability/socket quotes are separate, trusted
// admission, never inferred from packet sizes. No goroutine, queue or retry.
func NewControlSession(ctx context.Context, runtime *fd.Runtime, control *fd.Resource[*ControlDispatcher], transport *fd.Resource[peer.ControlServerConnection], workspace *fd.Resource[*DiscoveryWorkspace], maxWorkNS uint64) (*ControlSession, error) {
	if ctx == nil || runtime == nil || control == nil || transport == nil || workspace == nil || maxWorkNS == 0 || maxWorkNS > MaxFrameRequestBudgetNS {
		return nil, fail("control_session_admission", peer.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, validate := range []func(*fd.Runtime) error{control.ValidateRuntimeScope, transport.ValidateRuntimeScope, workspace.ValidateRuntimeScope} {
		if err := validate(runtime); err != nil {
			return nil, fail("control_session_scope", err)
		}
	}
	buffer, err := fd.OwnRuntimeResource(ctx, runtime, fd.ResourcePlan[[]byte]{Bytes: MaxControlBytes + MaxControlReplyBytes,
		Create: func(context.Context) (fd.Created[[]byte], error) {
			return fd.Created[[]byte]{Value: make([]byte, MaxControlBytes+MaxControlReplyBytes), Owned: true}, nil
		},
		Release: func([]byte) error { return nil },
	})
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	return &ControlSession{control: control, transport: transport, workspace: workspace, buffer: buffer, runtime: runtime, ctx: life, cancel: cancel, work: time.Duration(maxWorkNS), stats: ControlSessionStats{Stage: "idle"}}, nil
}

func (s *ControlSession) Stats() ControlSessionStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}
func (s *ControlSession) stage(stage string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stats.Closed {
		s.stats.Stage = stage
	}
}
func (s *ControlSession) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.stats.Closed {
		s.mu.Unlock()
		return
	}
	s.stats.Closed = true
	s.stats.LastStage = s.stats.Stage
	s.stats.Stage = "closed"
	s.mu.Unlock()
	s.cancel()
	s.transport.Close()
	s.buffer.Close()
}
func (s *ControlSession) WaitClosed(ctx context.Context) error {
	if s == nil || ctx == nil {
		return peer.ErrInvalid
	}
	return errors.Join(s.transport.WaitClosed(ctx), s.buffer.WaitClosed(ctx))
}

// Step consumes at most one record and replies to discovery or a control
// command. false,nil means a verified idle receive timeout. A semantic denial
// with a valid correlated reply counts as completed+negative, not successful
// mutation. A failed/uncertain send retires only this channel: committed grants
// and receipts in the shared control owner must remain reconcilable.
func (s *ControlSession) Step(ctx context.Context) (completed bool, err error) {
	if s == nil || ctx == nil {
		return false, peer.ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if !s.operation.TryLock() {
		return false, peer.ErrBusy
	}
	defer s.operation.Unlock()
	if s.Stats().Closed {
		return false, peer.ErrRetired
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	returned, sent, negative, idle := false, false, false, false
	defer func() {
		if !returned || err != nil {
			completed = false
			s.Close()
			if sent {
				s.mu.Lock()
				if s.stats.Uncertain != ^uint64(0) {
					s.stats.Uncertain++
				}
				s.mu.Unlock()
			}
		}
	}()
	err = s.control.Borrow(ctx, func(ctx context.Context, d *ControlDispatcher) error {
		if d == nil || d.control.runtime != s.runtime {
			return fail("control_session_owner", peer.ErrInvalid)
		}
		return s.workspace.Borrow(ctx, func(ctx context.Context, w *DiscoveryWorkspace) error {
			if w == nil {
				return peer.ErrInvalid
			}
			if !w.operation.TryLock() {
				return fail("control_session_workspace_busy", peer.ErrBusy)
			}
			defer w.operation.Unlock()
			defer clear(w.references[:])
			return s.buffer.Borrow(ctx, func(ctx context.Context, buffer []byte) error {
				return s.transport.Borrow(ctx, func(ctx context.Context, c peer.ControlServerConnection) (err error) {
					var pending peer.PendingCapability
					defer func() {
						if recover() != nil {
							d.uncertainOperation()
							err = fail("control_session_ownership_uncertain", peer.ErrUnavailable)
						}
						if pending != nil {
							err = errors.Join(err, d.release(pending))
						}
					}()
					verify := func() error {
						generation, err := c.CurrentGeneration()
						if err != nil {
							return err
						}
						if generation != d.control.generation {
							return peer.ErrUnauthorized
						}
						return ctx.Err()
					}
					s.stage("authenticate")
					if err := verify(); err != nil {
						return err
					}
					if d.State().Closed {
						return peer.ErrRetired
					}
					s.stage("receive")
					n, principal, received, err := c.ReceiveCommand(ctx, buffer[:MaxControlBytes])
					pending = received
					if errors.Is(err, peer.ErrDeadline) && ctx.Err() == nil && pending == nil {
						if err := verify(); err != nil {
							return err
						}
						idle = true
						return nil
					}
					if err != nil {
						return fail("control_session_receive", err)
					}
					if n < 1 || n > MaxControlBytes {
						return fail("control_session_record", peer.ErrRecord)
					}
					s.mu.Lock()
					if s.stats.Received != ^uint64(0) {
						s.stats.Received++
					}
					s.mu.Unlock()
					if principal.Generation != d.control.generation {
						return fail("control_session_sender", peer.ErrUnauthorized)
					}
					if err := verify(); err != nil {
						return fail("control_session_pre_dispatch", err)
					}
					ctx, done := context.WithTimeout(ctx, s.work)
					defer done()
					var replyBytes int
					if n >= 4 && string(buffer[:4]) == "FAPB" {
						s.stage("discovery")
						catalog, accepted, err := d.Discover(ctx, principal, buffer[:n], w.references[:], pending)
						if accepted {
							pending = nil
						}
						if err != nil {
							return err
						}
						if !accepted {
							return peer.ErrBusy
						}
						replyBytes, err = EncodeDiscoveryCatalogInto(buffer[MaxControlBytes:], catalog)
						if err != nil {
							return err
						}
					} else {
						s.stage("dispatch")
						result, accepted, cause := d.Apply(ctx, principal, buffer[:n], pending)
						if accepted {
							pending = nil
						}
						if !accepted {
							return errors.Join(peer.ErrBusy, cause)
						}
						if d.State().Closed {
							return errors.Join(peer.ErrRetired, cause)
						}
						reply, err := BuildControlReply(d.control.fence, result, cause)
						if err != nil {
							return err
						}
						replyBytes, err = EncodeControlReplyInto(buffer[MaxControlBytes:], reply)
						if err != nil {
							return err
						}
						negative = cause != nil
					}
					if err := ctx.Err(); err != nil {
						return err
					}
					if err := verify(); err != nil {
						return err
					}
					s.stage("reply")
					sent = true
					if err := c.Reply(ctx, buffer[MaxControlBytes:MaxControlBytes+replyBytes]); err != nil {
						return fail("control_session_reply", err)
					}
					if err := ctx.Err(); err != nil {
						return err
					}
					if err := verify(); err != nil {
						return err
					}
					completed = true
					return nil
				})
			})
		})
	})
	if err == nil {
		s.mu.Lock()
		if s.stats.Closed || ctx.Err() != nil {
			err = peer.ErrRetired
		} else {
			if completed && s.stats.Completed != ^uint64(0) {
				s.stats.Completed++
			}
			if completed && negative && s.stats.Negative != ^uint64(0) {
				s.stats.Negative++
			}
			if idle && s.stats.Idle != ^uint64(0) {
				s.stats.Idle++
			}
			s.stats.Stage = "idle"
		}
		s.mu.Unlock()
	}
	returned = true
	return completed, err
}
