package ingress

import (
	"context"
	"errors"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sync"
	"time"
)

// SendStarted does NOT prove delivery or non-delivery. ResponseReceived means
// an authenticated, correlated response, not that its Code/Receipt is success.
// An uncertain mutation must be reconciled through ControlStatus on a new
// admitted channel to the same execution/controller, never blindly replayed.
type ControlExchange struct {
	SendStarted, ResponseReceived bool
	Reply                         ControlReply
}
type ControlClientStats struct {
	Stage, LastStage     string
	Closed               bool
	Responses, Uncertain uint64
}
type ControlClient struct {
	runtime           *fd.Runtime
	transport         *fd.Resource[peer.ControlConnection]
	buffer            *fd.Resource[[]byte]
	target            fd.Fence
	broker            string
	work              time.Duration
	ctx               context.Context
	cancel            context.CancelFunc
	operation         sync.Mutex
	mu                sync.Mutex
	stats             ControlClientStats
	discoveryNonce    [16]byte
	discoverySequence uint64
}

// NewControlClient consumes the root-scoped transport resource on success only.
// target (broker frame domain) and broker (launcher execution) are independently
// trusted bootstrap values, not interchangeable tokens decoded from replies.
// Packet storage adds20KiB to the shared runtime budget. This is not an estimate
// of the provider's separately admitted socket/kernel/handle footprint.
func NewControlClient(ctx context.Context, runtime *fd.Runtime, transport *fd.Resource[peer.ControlConnection], target fd.Fence, broker string, maxWorkNS uint64) (*ControlClient, error) {
	if target.Namespace == ([16]byte{}) || !controlToken(target.ProcessGeneration) {
		return nil, fail("control_client_admission", peer.ErrInvalid)
	}
	return newControlClient(ctx, runtime, transport, target, broker, maxWorkNS)
}

func newControlClient(ctx context.Context, runtime *fd.Runtime, transport *fd.Resource[peer.ControlConnection], target fd.Fence, broker string, maxWorkNS uint64) (*ControlClient, error) {
	if ctx == nil || runtime == nil || transport == nil || !controlToken(broker) || maxWorkNS == 0 || maxWorkNS > MaxFrameRequestBudgetNS {
		return nil, fail("control_client_admission", peer.ErrInvalid)
	}
	if err := transport.ValidateRuntimeScope(runtime); err != nil {
		return nil, err
	}
	buffer, err := fd.OwnRuntimeResource(ctx, runtime, fd.ResourcePlan[[]byte]{Bytes: MaxControlBytes + MaxControlReplyBytes,
		Create: func(ctx context.Context) (fd.Created[[]byte], error) {
			if err := ctx.Err(); err != nil {
				return fd.Created[[]byte]{}, err
			}
			return fd.Created[[]byte]{Value: make([]byte, MaxControlBytes+MaxControlReplyBytes), Owned: true}, nil
		}, Release: func([]byte) error { return nil },
	})
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.Background())
	return &ControlClient{runtime: runtime, transport: transport, buffer: buffer, target: target, broker: broker, work: time.Duration(maxWorkNS), ctx: life, cancel: cancel, stats: ControlClientStats{Stage: "idle"}}, nil
}
func (c *ControlClient) Stats() ControlClientStats { c.mu.Lock(); defer c.mu.Unlock(); return c.stats }
func (c *ControlClient) stage(stage string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stats.Closed {
		c.stats.Stage = stage
	}
}
func (c *ControlClient) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.stats.Closed {
		c.mu.Unlock()
		return
	}
	c.stats.Closed = true
	c.stats.LastStage = c.stats.Stage
	c.stats.Stage = "closed"
	c.mu.Unlock()
	c.cancel()
	c.transport.Close()
	c.buffer.Close()
}
func (c *ControlClient) WaitClosed(ctx context.Context) error {
	if c == nil || ctx == nil {
		return peer.ErrInvalid
	}
	return errors.Join(c.transport.WaitClosed(ctx), c.buffer.WaitClosed(ctx))
}

// Exchange is single-flight, no queue/retry. The caller exclusively borrows the
// command and capability until return and retains capability ownership. Any
// post-send failure retires this channel so a late ACK cannot become the next
// operation's reply. Shared server grants must outlive connection teardown.
func (c *ControlClient) Exchange(ctx context.Context, command ControlCommand, capability peer.ProcessCapability) (out ControlExchange, err error) {
	if c == nil || ctx == nil {
		return out, peer.ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return out, err
	}
	if !c.operation.TryLock() {
		return out, peer.ErrBusy
	}
	defer c.operation.Unlock()
	if c.Stats().Closed {
		return out, peer.ErrRetired
	}
	if err = command.validate(); err != nil {
		return out, err
	}
	if command.Header.Target != c.target {
		return out, fail("control_client_target", peer.ErrUnauthorized)
	}
	if (command.Kind == ControlGrant) != (capability != nil) {
		return out, fail("control_client_capability", peer.ErrInvalid)
	}
	if capability != nil {
		var generation string
		generation, err = capability.CurrentGeneration()
		if err != nil {
			return out, err
		}
		if generation != command.Policy.Generation {
			return out, fail("control_client_consumer", peer.ErrUnauthorized)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, c.work)
	stop := context.AfterFunc(c.ctx, cancel)
	defer func() { stop(); cancel() }()
	returned := false
	defer func() {
		if !returned || err != nil {
			out.ResponseReceived = false
			out.Reply = ControlReply{}
			if out.SendStarted {
				c.mu.Lock()
				if c.stats.Uncertain != ^uint64(0) {
					c.stats.Uncertain++
				}
				c.mu.Unlock()
			}
			c.Close()
		}
	}()
	err = c.buffer.Borrow(ctx, func(ctx context.Context, buffer []byte) error {
		c.stage("encode")
		n, err := EncodeControlInto(buffer[:MaxControlBytes], command)
		if err != nil {
			return err
		}
		return c.transport.Borrow(ctx, func(ctx context.Context, transport peer.ControlConnection) error {
			verify := func() error {
				generation, err := transport.CurrentGeneration()
				if err != nil {
					return err
				}
				if generation != c.broker {
					return fail("control_client_broker", peer.ErrUnauthorized)
				}
				return ctx.Err()
			}
			c.stage("authenticate")
			if err := verify(); err != nil {
				return err
			}
			c.stage("send")
			out.SendStarted = true
			if err := transport.WriteCommand(ctx, buffer[:n], capability); err != nil {
				return fail("control_client_send", err)
			}
			c.stage("receive")
			n, principal, err := transport.Receive(ctx, buffer[MaxControlBytes:])
			if err != nil {
				return fail("control_client_receive", err)
			}
			if principal.Generation != c.broker {
				return fail("control_client_sender", peer.ErrUnauthorized)
			}
			if n < 1 || n > MaxControlReplyBytes {
				return fail("control_client_record", peer.ErrRecord)
			}
			c.stage("correlate")
			reply, err := DecodeControlReply(buffer[MaxControlBytes : MaxControlBytes+n])
			if err != nil {
				return err
			}
			if err = ValidateControlReply(reply, command); err != nil {
				return err
			}
			if err = verify(); err != nil {
				return err
			}
			out.Reply = reply
			return nil
		})
	})
	if err == nil {
		out.ResponseReceived = true
		c.mu.Lock()
		if c.stats.Responses != ^uint64(0) {
			c.stats.Responses++
		}
		c.mu.Unlock()
		c.stage("idle")
	}
	returned = true
	return out, err
}
