package ingress

import (
	"context"
	"errors"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sort"
	"sync"
)

var ErrControlSequence = errors.New("control command sequence is not next")

// OwnedConsumer arrives through authenticated capability admission, NOT a frame
// request. Close releases only that owned capability, never kills a process.
type OwnedConsumer interface {
	ConsumerAuthority
	Close() error
}
type ControlHeader struct {
	Target   fd.Fence
	Sequence uint64
	ID       [16]byte
	Grant    string
	Previous uint64 // compare-and-swap revision, zero only for a new grant
}
type ControlReceipt struct {
	Sequence       uint64
	ID             [16]byte
	Grant, Outcome string
	Stage          string
}
type ControlStats struct {
	Receipt                           ControlReceipt
	Active, Retiring, ReleaseFailures int
	Closed                            bool
	Grants                            []GrantRevision
}
type GrantRevision struct {
	Name, Consumer string
	Sequence       uint64
}
type controlledGrant struct {
	access   *Access
	consumer OwnedConsumer
	sequence uint64
}
type GrantControl struct {
	mu         sync.Mutex
	runtime    *fd.Runtime
	authority  ConsumerAuthority
	generation string
	fence      fd.Fence
	limit      int
	active     map[string]*controlledGrant
	retiring   map[*controlledGrant]bool // value=true means terminal close failure
	last       ControlReceipt
	closed     bool
	changed    chan struct{}
}

// NewGrantControl admits one controller for this exact broker domain. limit
// includes active AND retiring/quarantined capabilities, not only map entries.
// Server connection/FD admission must occur before constructing OwnedConsumer.
func NewGrantControl(runtime *fd.Runtime, authority ConsumerAuthority, generation string, limit int) (*GrantControl, error) {
	if runtime == nil || authority == nil || !controlToken(generation) || limit < 1 || limit > 64 {
		return nil, peer.ErrInvalid
	}
	current, err := authority.CurrentGeneration()
	if err != nil || current != generation {
		return nil, peer.ErrUnauthorized
	}
	if err = runtime.ValidateDomain(runtime.Fence()); err != nil {
		return nil, err
	}
	return &GrantControl{runtime: runtime, authority: authority, generation: generation, fence: runtime.Fence(), limit: limit, active: make(map[string]*controlledGrant), retiring: make(map[*controlledGrant]bool), changed: make(chan struct{})}, nil
}

func (c *GrantControl) notifyLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// WaitClosed joins physical capability retirement, including releases already
// started by revoke/replacement. Cancellation only stops the wait: it neither
// frees quota nor retries Close. A stuck provider remains owned and charged by
// the enclosing runtime resource until it actually returns.
func (c *GrantControl) WaitClosed(ctx context.Context) error {
	if c == nil || ctx == nil {
		return peer.ErrInvalid
	}
	for {
		c.mu.Lock()
		running, failed := 0, false
		for _, terminalFailure := range c.retiring {
			if terminalFailure {
				failed = true
			} else {
				running++
			}
		}
		closed, changed := c.closed, c.changed
		c.mu.Unlock()
		if closed && running == 0 {
			if failed {
				return fail("control_capability_release", peer.ErrUnavailable)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func controlToken(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}
func (c *GrantControl) retireLocked(g *controlledGrant) {
	g.access.Revoke()
	c.retiring[g] = false
	// At most limit jobs, including stuck release. No queue or blind retry.
	go func() {
		var err error
		func() {
			defer func() {
				if recover() != nil {
					err = peer.ErrUnavailable
				}
			}()
			err = g.consumer.Close()
		}()
		c.mu.Lock()
		defer c.mu.Unlock()
		defer c.notifyLocked()
		if err != nil {
			c.retiring[g] = true
			return
		}
		delete(c.retiring, g)
	}()
}
func (c *GrantControl) closeLocked() {
	if c.closed {
		return
	}
	c.closed = true
	for key, g := range c.active {
		delete(c.active, key)
		c.retireLocked(g)
	}
	c.notifyLocked()
}
func (c *GrantControl) Close() {
	if c != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.closeLocked()
	}
}

// Discover is a read-only snapshot for this control plane's authenticated
// launcher, not a grant or proof that pixels are currently available. It does
// not consume mutation sequence numbers. Later grants resolve exact epochs.
func (c *GrantControl) Discover(principal peer.Principal, buffer []fd.BindingRef) (fd.Fence, int, error) {
	if c == nil {
		return fd.Fence{}, 0, peer.ErrInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkLocked(); err != nil {
		return fd.Fence{}, 0, err
	}
	if principal.Generation != c.generation {
		return fd.Fence{}, 0, peer.ErrUnauthorized
	}
	n, err := c.runtime.SnapshotBindingsInto(buffer)
	if err != nil {
		return fd.Fence{}, 0, err
	}
	if err = c.checkLocked(); err != nil {
		clear(buffer[:n])
		return fd.Fence{}, 0, err
	}
	return c.fence, n, nil
}
func (c *GrantControl) checkLocked() error {
	if c.closed {
		return peer.ErrRetired
	}
	generation, err := c.authority.CurrentGeneration()
	if err != nil || generation != c.generation {
		c.closeLocked()
		return peer.ErrRetired
	}
	if err = c.runtime.ValidateDomain(c.fence); err != nil {
		c.closeLocked()
		return err
	}
	return nil
}
func (c *GrantControl) headerLocked(principal peer.Principal, h ControlHeader) error {
	if err := c.checkLocked(); err != nil {
		return err
	}
	if principal.Generation != c.generation {
		return peer.ErrUnauthorized
	}
	if h.Target != c.fence {
		return fd.ErrDomainMismatch
	}
	if h.ID == ([16]byte{}) || !controlToken(h.Grant) {
		return peer.ErrInvalid
	}
	if c.last.Sequence == ^uint64(0) {
		c.closeLocked()
		return ErrControlSequence
	}
	if h.Sequence == 0 || h.Sequence != c.last.Sequence+1 {
		return ErrControlSequence
	}
	return nil
}

// Preflight validates authority/target/order before importing a capability.
// Grant/Revoke repeat these checks at commit; this is not a reservation.
func (c *GrantControl) Preflight(principal peer.Principal, command ControlCommand) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := command.validate(); err != nil {
		return err
	}
	if principal.Generation != c.generation {
		return peer.ErrUnauthorized
	}
	if command.Header.Target != c.fence {
		return fd.ErrDomainMismatch
	}
	if command.Kind == ControlStatus {
		return c.checkLocked()
	}
	return c.headerLocked(principal, command.Header)
}
func (c *GrantControl) recordLocked(h ControlHeader, outcome string) ControlReceipt {
	c.last = ControlReceipt{Sequence: h.Sequence, ID: h.ID, Grant: h.Grant, Outcome: outcome, Stage: "control_" + outcome}
	return c.last
}
func (c *GrantControl) matchesLocked(h ControlHeader) bool {
	g := c.active[h.Grant]
	if g == nil {
		return h.Previous == 0
	}
	return h.Previous == g.sequence
}

// A valid authenticated command consumes its sequence even when rejected by
// policy/capacity/CAS. Status exposes the latest receipt after an uncertain ACK.
// A replay, wrong target or unauthenticated command has no effect. Ownership is
// transferred ONLY with outcome "installed"; caller closes rejected handles.
func (c *GrantControl) Grant(principal peer.Principal, h ControlHeader, reference fd.BindingRef, consumer OwnedConsumer, policy AccessPolicy) (ControlReceipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.headerLocked(principal, h); err != nil {
		return ControlReceipt{}, err
	}
	if !c.matchesLocked(h) {
		return c.recordLocked(h, "conflict"), nil
	}
	if len(c.active)+len(c.retiring) >= c.limit {
		return c.recordLocked(h, "capacity"), nil
	}
	if consumer == nil {
		return c.recordLocked(h, "invalid"), nil
	}
	binding, err := c.runtime.ResolveBinding(reference)
	if err != nil {
		c.recordLocked(h, "invalid")
		c.last.Stage = "control_binding_reference"
		return c.last, nil
	}
	delegated := &delegatedAuthority{controller: c.authority, expected: c.generation, consumer: consumer}
	a, err := NewAccess(c.runtime, binding, delegated, policy)
	// Creation/admission may race a launcher restart. No stale grant is installed.
	if current := c.checkLocked(); current != nil {
		if a != nil {
			a.Revoke()
		}
		return ControlReceipt{}, current
	}
	if err != nil {
		c.recordLocked(h, "invalid")
		var diagnostic *Failure
		if errors.As(err, &diagnostic) {
			c.last.Stage = diagnostic.Stage
		}
		return c.last, nil
	}
	old := c.active[h.Grant]
	c.active[h.Grant] = &controlledGrant{a, consumer, h.Sequence}
	if old != nil {
		c.retireLocked(old)
	}
	return c.recordLocked(h, "installed"), nil
}
func (c *GrantControl) Revoke(principal peer.Principal, h ControlHeader) (ControlReceipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.headerLocked(principal, h); err != nil {
		return ControlReceipt{}, err
	}
	if !c.matchesLocked(h) {
		return c.recordLocked(h, "conflict"), nil
	}
	g := c.active[h.Grant]
	if g == nil {
		return c.recordLocked(h, "absent"), nil
	}
	delete(c.active, h.Grant)
	c.retireLocked(g)
	return c.recordLocked(h, "revoked"), nil
}
func (c *GrantControl) Lookup(principal peer.Principal, name string) (*Access, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkLocked(); err != nil {
		return nil, err
	}
	g := c.active[name]
	if g == nil || principal.Generation != g.access.generation {
		return nil, peer.ErrUnauthorized
	}
	return g.access, nil
}

// WithConsumer is a trusted server-composition boundary, not a consumer RPC.
// The endpoint has already been assigned a fixed grant by its owner. The
// callback borrows the exact capability to authenticate its accepted socket;
// it must not close/export it or infer authority from a socket pathname.
// Concurrent revocation remains effective and invalidates the operation.
func (c *GrantControl) WithConsumer(name string, visit func(*Access, OwnedConsumer) error) error {
	if c == nil || !controlToken(name) || visit == nil {
		return peer.ErrInvalid
	}
	c.mu.Lock()
	if err := c.checkLocked(); err != nil {
		c.mu.Unlock()
		return err
	}
	g := c.active[name]
	c.mu.Unlock()
	if g == nil {
		return peer.ErrUnauthorized
	}
	if err := g.access.current(); err != nil {
		return err
	}
	if err := visit(g.access, g.consumer); err != nil {
		return err
	}
	return g.access.current()
}
func (c *GrantControl) Status(principal peer.Principal) (ControlStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if principal.Generation != c.generation {
		return ControlStats{}, peer.ErrUnauthorized
	}
	err := c.checkLocked()
	s := ControlStats{Receipt: c.last, Active: len(c.active), Retiring: len(c.retiring), Closed: c.closed}
	for _, failed := range c.retiring {
		if failed {
			s.ReleaseFailures++
		}
	}
	for name, g := range c.active {
		s.Grants = append(s.Grants, GrantRevision{name, g.access.generation, g.sequence})
	}
	sort.Slice(s.Grants, func(i, j int) bool { return s.Grants[i].Name < s.Grants[j].Name })
	return s, err
}

// A cached Access also checks controller continuity. A dead controller does not
// require the next control command to prevent further consumer operations.
type delegatedAuthority struct {
	controller ConsumerAuthority
	expected   string
	consumer   ConsumerAuthority
}

func (d *delegatedAuthority) CurrentGeneration() (string, error) {
	generation, err := d.controller.CurrentGeneration()
	if err != nil || generation != d.expected {
		return "", peer.ErrRetired
	}
	return d.consumer.CurrentGeneration()
}
