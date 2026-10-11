package ingress

import (
	"context"
	"encoding/binary"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sync"
)

// DiscoveryWorkspace is reusable caller-owned storage. Its root-scoped
// Resource must be admitted by trusted composition, including decoded strings
// and allocation overhead, before Discover. A packet size is not its quote.
type DiscoveryWorkspace struct {
	operation  sync.Mutex
	references [fd.MaxBindingReferences]fd.BindingRef
}

type DiscoveryExchange struct{ SendStarted, ResponseReceived bool }

// NewBootstrapControlClient starts without a trusted frame namespace. It
// consumes transport on success only, exactly like NewControlClient. The
// expected execution comes from the launcher; no target is guessed from input.
// A CSPRNG seed plus an exact nonwrapping counter makes each client challenge
// distinct, including failed attempts. Zero challenges are skipped, never sent.
func NewBootstrapControlClient(ctx context.Context, runtime *fd.Runtime, transport *fd.Resource[peer.ControlConnection], broker string, entropy fd.Entropy, maxWorkNS uint64) (*ControlClient, error) {
	if ctx == nil || entropy == nil || runtime == nil || transport == nil || !controlToken(broker) || maxWorkNS == 0 || maxWorkNS > MaxFrameRequestBudgetNS {
		return nil, fail("discovery_client_admission", peer.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := transport.ValidateRuntimeScope(runtime); err != nil {
		return nil, err
	}
	var nonce [16]byte
	if err := entropy.Fill(nonce[:]); err != nil {
		return nil, fail("discovery_client_entropy", err)
	}
	if nonce == ([16]byte{}) {
		return nil, fail("discovery_client_entropy", peer.ErrUnavailable)
	}
	c, err := newControlClient(ctx, runtime, transport, fd.Fence{}, broker, maxWorkNS)
	if err != nil {
		return nil, err
	}
	c.discoveryNonce = nonce
	return c, nil
}

// BoundTarget reports only a completed, authenticated discovery (or the target
// explicitly supplied to NewControlClient). It does not promise ready frames.
func (c *ControlClient) BoundTarget() (fd.Fence, bool) {
	if c == nil {
		return fd.Fence{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stats.Closed || c.target.Namespace == ([16]byte{}) {
		return fd.Fence{}, false
	}
	return c.target, true
}

// Discover shares the mutation client's single-flight slot, transport and20KiB
// packet budget. Workspace stays caller-owned and must belong to the same root
// Runtime. The visitor borrows references only until return; honor the final
// method result before using copied data. A stale/domain-changing/uncertain
// response closes the channel. Discovery never changes server grants.
func (c *ControlClient) Discover(ctx context.Context, workspace *fd.Resource[*DiscoveryWorkspace], visit func(DiscoveryCatalog) error) (out DiscoveryExchange, err error) {
	if c == nil || ctx == nil || workspace == nil || visit == nil {
		return out, peer.ErrInvalid
	}
	if err = ctx.Err(); err != nil {
		return out, err
	}
	if err = workspace.ValidateRuntimeScope(c.runtime); err != nil {
		return out, err
	}
	if !c.operation.TryLock() {
		return out, peer.ErrBusy
	}
	defer c.operation.Unlock()
	if c.Stats().Closed {
		return out, peer.ErrRetired
	}
	if c.discoveryNonce == ([16]byte{}) {
		return out, fail("discovery_client_unconfigured", peer.ErrInvalid)
	}
	challenge, sequence, err := nextDiscoveryChallenge(c.discoveryNonce, c.discoverySequence)
	c.discoverySequence = sequence
	if err != nil {
		c.Close()
		return out, fail("discovery_client_sequence", err)
	}
	ctx, cancel := context.WithTimeout(ctx, c.work)
	stop := context.AfterFunc(c.ctx, cancel)
	defer func() { stop(); cancel() }()
	returned := false
	defer func() {
		if !returned || err != nil {
			out.ResponseReceived = false
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
	var target fd.Fence
	err = workspace.Borrow(ctx, func(ctx context.Context, w *DiscoveryWorkspace) error {
		if w == nil {
			return peer.ErrInvalid
		}
		if !w.operation.TryLock() {
			return fail("discovery_client_workspace_busy", peer.ErrBusy)
		}
		defer w.operation.Unlock()
		defer clear(w.references[:])
		return c.buffer.Borrow(ctx, func(ctx context.Context, buffer []byte) error {
			c.stage("discovery_encode")
			n, err := EncodeDiscoveryQueryInto(buffer[:MaxControlBytes], challenge)
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
						return fail("discovery_client_broker", peer.ErrUnauthorized)
					}
					return ctx.Err()
				}
				c.stage("discovery_authenticate")
				if err := verify(); err != nil {
					return err
				}
				c.stage("discovery_send")
				out.SendStarted = true
				if err := transport.WriteCommand(ctx, buffer[:n], nil); err != nil {
					return fail("discovery_client_send", err)
				}
				c.stage("discovery_receive")
				n, principal, err := transport.Receive(ctx, buffer[MaxControlBytes:])
				if err != nil {
					return fail("discovery_client_receive", err)
				}
				if n < 1 || n > MaxDiscoveryBytes {
					return fail("discovery_client_record", peer.ErrRecord)
				}
				if principal.Generation != c.broker {
					return fail("discovery_client_sender", peer.ErrUnauthorized)
				}
				c.stage("discovery_correlate")
				catalog, err := DecodeDiscoveryCatalogInto(buffer[MaxControlBytes:MaxControlBytes+n], w.references[:])
				if err != nil {
					return err
				}
				if err = ValidateDiscoveryCatalog(catalog, challenge, c.broker, principal); err != nil {
					return err
				}
				if c.target.Namespace != ([16]byte{}) && catalog.Broker != c.target {
					return fail("discovery_client_domain_changed", peer.ErrUnauthorized)
				}
				if err = verify(); err != nil {
					return err
				}
				c.stage("discovery_visit")
				if err = visit(catalog); err != nil {
					return err
				}
				if err = verify(); err != nil {
					return err
				}
				target = catalog.Broker
				return nil
			})
		})
	})
	if err == nil {
		c.mu.Lock()
		if c.stats.Closed || ctx.Err() != nil {
			err = peer.ErrRetired
		} else {
			c.target = target
			out.ResponseReceived = true
			if c.stats.Responses != ^uint64(0) {
				c.stats.Responses++
			}
		}
		c.mu.Unlock()
		if err == nil {
			c.stage("idle")
		}
	}
	returned = true
	return out, err
}

// Called with the client's operation slot held. Pure boundary logic keeps
// counter exhaustion testable without exposing a runtime reset hook.
func nextDiscoveryChallenge(nonce [16]byte, sequence uint64) ([16]byte, uint64, error) {
	var challenge [16]byte
	if nonce == ([16]byte{}) {
		return challenge, sequence, peer.ErrInvalid
	}
	for challenge == ([16]byte{}) {
		if sequence == ^uint64(0) {
			return challenge, sequence, peer.ErrRetired
		}
		sequence++
		challenge = nonce
		binary.BigEndian.PutUint64(challenge[8:], binary.BigEndian.Uint64(challenge[8:])^sequence)
	}
	return challenge, sequence, nil
}
