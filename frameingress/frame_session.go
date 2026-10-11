package ingress

import (
	"context"
	"errors"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"sync"
	"time"

	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
)

type FrameSessionStats struct {
	Stage, LastStage                    string
	Closed                              bool
	Received, Completed, Idle, Negative uint64
}
type FrameSession struct {
	transport    *fd.Resource[peer.Connection]
	buffer       *fd.Resource[[]byte]
	sender       *FrameSender
	writer       *sessionWriter
	control      *GrantControl
	grant        string
	work         time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	operation    sync.Mutex
	mu           sync.Mutex
	stats        FrameSessionStats
	publication  *DeliveryPublication
	metadataSent bool
}

// NewDeliverySession shares the same admitted/authenticated session lifecycle
// as ROI, but serves only the fixed publication provided by the broker owner.
// It never owns the publication or starts a Nemesi consumer process.
func NewDeliverySession(ctx context.Context, runtime *fd.Runtime, control *GrantControl, grant string, transport *fd.Resource[peer.Connection], publication *DeliveryPublication, maxWorkNS uint64) (*FrameSession, error) {
	if publication == nil || publication.binding == nil || runtime == nil || runtime.Fence() != publication.fence {
		return nil, peer.ErrInvalid
	}
	if err := publication.binding.ValidateCurrent(); err != nil {
		return nil, err
	}
	s, err := NewFrameSession(ctx, runtime, control, grant, transport, int(publication.layout.MaxPayload), maxWorkNS)
	if err != nil {
		return nil, err
	}
	s.publication = publication
	return s, nil
}

// NewFrameSession takes ownership of an already admitted transport resource on
// success ONLY. Its provider's complete footprint must have been reserved by
// OwnRuntimeResource before creation. Session packet buffers add another20KiB
// to the same global budget, not a substitute estimate of socket/kernel RAM.
// Control remains shared and is not owned/closed by this session.
func NewFrameSession(ctx context.Context, runtime *fd.Runtime, control *GrantControl, grant string, transport *fd.Resource[peer.Connection], maxBytes int, maxWorkNS uint64) (*FrameSession, error) {
	if ctx == nil || runtime == nil || control == nil || control.runtime != runtime || !controlToken(grant) || transport == nil || maxWorkNS == 0 || maxWorkNS > MaxFrameRequestBudgetNS {
		return nil, fail("frame_session_admission", peer.ErrInvalid)
	}
	if err := transport.ValidateRuntimeScope(runtime); err != nil {
		return nil, fail("frame_session_resource_scope", err)
	}
	buffer, err := fd.OwnRuntimeResource(ctx, runtime, fd.ResourcePlan[[]byte]{Bytes: MaxFrameRequestBytes,
		Create: func(ctx context.Context) (fd.Created[[]byte], error) {
			if err := ctx.Err(); err != nil {
				return fd.Created[[]byte]{}, err
			}
			return fd.Created[[]byte]{Value: make([]byte, MaxFrameRequestBytes), Owned: true}, nil
		},
		Release: func([]byte) error { return nil },
	})
	if err != nil {
		return nil, err
	}
	writer := &sessionWriter{resource: transport, runtime: runtime}
	sender, err := NewFrameSender(ctx, runtime, writer, maxBytes, maxWorkNS)
	if err != nil {
		buffer.Close()
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	return &FrameSession{transport: transport, buffer: buffer, sender: sender, writer: writer, control: control, grant: grant, work: time.Duration(maxWorkNS), ctx: life, cancel: cancel, stats: FrameSessionStats{Stage: "idle"}}, nil
}

func (s *FrameSession) Stats() FrameSessionStats { s.mu.Lock(); defer s.mu.Unlock(); return s.stats }
func (s *FrameSession) stage(stage string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stats.Closed {
		s.stats.Stage = stage
	}
}
func (s *FrameSession) Close() {
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
	s.sender.Close()
	s.buffer.Close()
}
func (s *FrameSession) WaitClosed(ctx context.Context) error {
	if s == nil || ctx == nil {
		return peer.ErrInvalid
	}
	return errors.Join(s.transport.WaitClosed(ctx), s.sender.WaitClosed(ctx), s.buffer.WaitClosed(ctx))
}

// Step handles at most one authenticated request, including ALL its regions.
// false,nil is a verified idle receive deadline, not a failed request/restart.
// Any malformed/unauthorized request, execution failure, partial output or
// cancellation closes this connection; no automatic replay/retry or new queue.
func (s *FrameSession) Step(ctx context.Context) (completed bool, err error) {
	if s == nil || ctx == nil {
		return false, peer.ErrInvalid
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
	returned := false
	defer func() {
		if !returned || err != nil {
			s.Close()
		}
	}()
	negative := false
	err = s.buffer.Borrow(ctx, func(ctx context.Context, buffer []byte) error {
		return s.transport.Borrow(ctx, func(ctx context.Context, c peer.Connection) error {
			s.stage("receive")
			n, principal, err := c.Receive(ctx, buffer)
			if errors.Is(err, peer.ErrDeadline) && ctx.Err() == nil {
				s.mu.Lock()
				if s.stats.Idle != ^uint64(0) {
					s.stats.Idle++
				}
				s.mu.Unlock()
				s.stage("idle")
				return nil
			}
			if err != nil {
				return fail("frame_session_receive", err)
			}
			if n < 1 || n > len(buffer) {
				return fail("frame_session_record", peer.ErrRecord)
			}
			s.mu.Lock()
			if s.stats.Received != ^uint64(0) {
				s.stats.Received++
			}
			s.mu.Unlock()
			// Total processing deadline spans extraction and every region's send.
			ctx, done := context.WithTimeout(ctx, s.work)
			defer done()
			return s.writer.with(ctx, c, func() error {
				s.stage("authorize")
				access, err := s.control.Lookup(principal, s.grant)
				if err != nil {
					return fail("frame_session_grant", err)
				}
				if s.publication != nil {
					s.stage("send_delivery")
					if err := s.sender.SendDelivery(ctx, access, s.publication, buffer[:n], !s.metadataSent); err != nil {
						return fail("delivery_session_send", err)
					}
					s.metadataSent, completed = true, true
					return nil
				}
				query, err := DecodeFrameRequest(buffer[:n])
				if err != nil {
					return err
				}
				s.stage("extract")
				result, err := access.extractFrameQuery(ctx, principal, query)
				if err != nil {
					for _, state := range []fa.Code{fa.Expired, fa.CorrelationUnavailable, fa.RegionInvalid, fa.CapacityRejected, fa.DeadlineExceeded, fa.ProviderUnavailable} {
						if errors.Is(err, state) {
							s.stage("send_negative")
							if sendErr := s.sender.sendNegative(ctx, query, access, state); sendErr != nil {
								return fail("frame_session_negative", sendErr)
							}
							completed = true
							negative = true
							return nil
						}
					}
					return fail("frame_session_extract", err)
				}
				defer result.Close()
				s.stage("send")
				for i := range query.Regions {
					if err := s.sender.Send(ctx, query, result, i); err != nil {
						return fail("frame_session_send", err)
					}
				}
				completed = true
				return nil
			})
		})
	})
	if err != nil {
		completed = false
	} else if completed {
		s.mu.Lock()
		if s.stats.Completed != ^uint64(0) {
			s.stats.Completed++
		}
		if negative && s.stats.Negative != ^uint64(0) {
			s.stats.Negative++
		}
		s.mu.Unlock()
		s.stage("idle")
	}
	returned = true
	return completed, err
}
