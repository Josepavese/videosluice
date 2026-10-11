package framedomain

import (
	"context"
	"errors"
	fa "github.com/Josepavese/videosluice/frameaccess"
)

// ResourcePlan comes from trusted middleware/provider admission, never RPC
// arguments. Bytes is the provider's verified upper bound BEFORE creation,
// including its allocation alignment. Create reports ownership even on error;
// Release returns nil ONLY after physical resource ownership is relinquished.
type Created[T any] struct {
	Value T
	Owned bool
}

// Owned=false with an error asserts that rollback left no resource. Owned=true
// transfers even a partially initialized resource for release or quarantine.
// Success without ownership is an invalid, untrusted accounting outcome.
type ResourcePlan[T any] struct {
	Bytes   int
	Create  func(context.Context) (Created[T], error)
	Release func(T) error
}
type resourceState struct {
	owner                               resourceOwner
	ctx                                 context.Context
	cancel                              context.CancelFunc
	bytes, operations                   int
	creating, closed, closing, terminal bool
	value                               any
	destroy                             func() error
	err                                 error
	done                                chan struct{}
}
type Resource[T any] struct{ state *resourceState }

// ValidateRuntimeScope prevents a control/session resource being charged to
// another runtime, or hidden inside a camera's independently retired scope.
func (o *Resource[T]) ValidateRuntimeScope(r *Runtime) error {
	if o == nil || o.state == nil || r == nil {
		return ErrInvalid
	}
	s := o.state
	if s.owner.runtime != r || s.owner.binding != nil {
		return ErrDomainMismatch
	}
	if err := s.owner.current(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if s.closed {
		return ErrRetired
	}
	return nil
}

type resourceValue[T any] struct{ value T }
type ResourceFailure struct {
	Stage string
	Cause error
}

func (e *ResourceFailure) Error() string {
	message := "resource lifecycle not confirmed: stage=" + e.Stage
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}
func (e *ResourceFailure) Unwrap() []error { return []error{fa.ProviderUnavailable, e.Cause} }

// OwnResource charges pending create, live use, retired use and physical close
// under the SAME arena/buffer budget. A hung Create/Release remains charged;
// no timeout manufactures free memory. Closing goroutines are bounded by the
// admitted MaxResources, and there is no unbounded close queue or retry loop.
func OwnResource[T any](ctx context.Context, b *Binding, p ResourcePlan[T]) (*Resource[T], error) {
	if b == nil {
		return nil, ErrInvalid
	}
	return ownResource(ctx, resourceOwner{runtime: b.runtime, binding: b}, p)
}

// OwnRuntimeResource admits a broker-wide control/session resource before
// creation. It shares the SAME global bytes/cardinality as source stores,
// buffers and resources, but is not tied to an arbitrary camera's lifetime.
// Source-specific resources must continue to use OwnResource with a Binding.
func OwnRuntimeResource[T any](ctx context.Context, runtime *Runtime, p ResourcePlan[T]) (*Resource[T], error) {
	if runtime == nil {
		return nil, ErrInvalid
	}
	return ownResource(ctx, resourceOwner{runtime: runtime}, p)
}

func ownResource[T any](ctx context.Context, owner resourceOwner, p ResourcePlan[T]) (*Resource[T], error) {
	return ownResourceMode(ctx, owner, p, false)
}

// initial is reserved for NewFromBootstrap's unpublished runtime. All normal
// resource admissions and every post-create/use check require authentication.
func ownResourceMode[T any](ctx context.Context, owner resourceOwner, p ResourcePlan[T], initial bool) (*Resource[T], error) {
	if ctx == nil || p.Bytes <= 0 || p.Create == nil || p.Release == nil {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var opCtx context.Context
	var finish func()
	var err error
	if initial {
		if owner.binding != nil || owner.runtime.authority != nil {
			return nil, ErrInvalid
		}
		var cancel context.CancelFunc
		opCtx, cancel = context.WithCancel(ctx)
		stop := context.AfterFunc(owner.runtime.ctx, cancel)
		finish = func() { stop(); cancel() }
	} else {
		opCtx, finish, err = owner.begin(ctx)
	}
	if err != nil {
		return nil, err
	}
	defer finish()
	r := owner.runtime
	r.mu.Lock()
	if initial && r.resources != 0 {
		r.mu.Unlock()
		return nil, ErrInvalid
	}
	if owner.closedLocked() {
		r.mu.Unlock()
		return nil, ErrRetired
	}
	if r.resources >= r.limits.MaxResources || !r.canChargeLocked(owner.source(), p.Bytes) {
		r.mu.Unlock()
		return nil, fa.CapacityRejected
	}
	resourceCtx, cancelResource := context.WithCancel(context.Background())
	s := &resourceState{owner: owner, ctx: resourceCtx, cancel: cancelResource, bytes: p.Bytes, creating: true, done: make(chan struct{})}
	owner.addLocked(s)
	r.resources++
	r.resourceBytes += p.Bytes
	r.mu.Unlock()
	var acquired Created[T]
	panicked := true
	func() {
		defer func() {
			if recover() != nil {
				err = fa.ProviderUnavailable
			}
		}()
		err = opCtx.Err()
		if err == nil {
			acquired, err = p.Create(opCtx)
		}
		panicked = false
	}()
	if acquired.Owned && err == nil {
		if current := owner.current(); current != nil {
			err = current
		}
		if err == nil && opCtx.Err() != nil {
			err = opCtx.Err()
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s.creating = false
	if panicked {
		s.closed = true
		s.failLocked("create_panic", fa.ProviderUnavailable)
		return nil, s.err
	}
	if !acquired.Owned {
		s.closed = true
		if err == nil {
			s.failLocked("create_ownership_missing", ErrInvalid)
			return nil, s.err
		}
		// Only this explicit outcome confirms a complete rollback.
		s.releaseLocked()
		return nil, err
	}
	s.value = resourceValue[T]{acquired.Value}
	s.destroy = func() error { return p.Release(acquired.Value) }
	if err != nil || s.closed || owner.closedLocked() {
		s.retireLocked()
		if err == nil {
			err = ErrRetired
		}
		return nil, err
	}
	return &Resource[T]{s}, nil
}

func (s *resourceState) failLocked(stage string, err error) {
	s.cancel()
	s.terminal = true
	s.err = &ResourceFailure{stage, err}
	s.owner.runtime.resourceFailures++
	close(s.done)
	// Deliberately retained: failure is not evidence of physical release.
}
func (s *resourceState) releaseLocked() {
	s.cancel()
	r := s.owner.runtime
	r.resourceBytes -= s.bytes
	r.resources--
	s.owner.removeLocked(s)
	s.value = nil
	s.destroy = nil
	s.terminal = true
	close(s.done)
	if s.owner.binding != nil {
		r.cleanupLocked(s.owner.binding)
	}
}
func (s *resourceState) retireLocked() {
	s.closed = true
	s.cancel()
	if s.creating || s.operations > 0 || s.closing || s.terminal {
		return
	}
	s.closing = true
	go func() {
		var err error
		stage := "release_failed"
		func() {
			defer func() {
				if recover() != nil {
					err = fa.ProviderUnavailable
					stage = "release_panic"
				}
			}()
			err = s.destroy()
		}()
		r := s.owner.runtime
		r.mu.Lock()
		defer r.mu.Unlock()
		if err != nil {
			s.failLocked(stage, err)
			return
		}
		s.releaseLocked()
	}()
}

// Borrow is a bounded trusted callback, not an escaping provider/FD reference.
// The callback must preserve the atomic commit semantics of its own operations;
// source retirement fences its result but cannot undo already committed work.
func (o *Resource[T]) Borrow(ctx context.Context, visit func(context.Context, T) error) (err error) {
	if o == nil || o.state == nil || ctx == nil || visit == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s := o.state
	r := s.owner.runtime
	started := r.clock.NowNS()
	progress := resourceBorrowFailure{phase: "begin", begin: -1, admission: -1, consumer: -1, current: -1}
	defer func() {
		if err != nil {
			progress.elapsed = surfacePublicationElapsed(started, r.clock.NowNS())
			progress.cause = err
			diagnostic := progress
			err = &diagnostic
		}
	}()
	opCtx, finish, err := s.owner.begin(ctx)
	progress.begin = surfacePublicationElapsed(started, r.clock.NowNS())
	if err != nil {
		return err
	}
	defer func() {
		step := r.clock.NowNS()
		finish()
		progress.finish += surfacePublicationElapsed(step, r.clock.NowNS())
	}()
	progress.phase = "admission"
	step := r.clock.NowNS()
	opCtx, cancel := context.WithCancel(opCtx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() { stop(); cancel() }()
	r.mu.Lock()
	if s.closed || s.owner.closedLocked() {
		r.mu.Unlock()
		progress.admission = surfacePublicationElapsed(step, r.clock.NowNS())
		return ErrRetired
	}
	if s.operations >= r.limits.MaxResourceOperations {
		r.mu.Unlock()
		progress.admission = surfacePublicationElapsed(step, r.clock.NowNS())
		return fa.CapacityRejected
	}
	s.operations++
	value := s.value.(resourceValue[T]).value
	r.mu.Unlock()
	progress.admission = surfacePublicationElapsed(step, r.clock.NowNS())
	defer func() {
		step := r.clock.NowNS()
		r.mu.Lock()
		defer func() {
			r.mu.Unlock()
			progress.finish += surfacePublicationElapsed(step, r.clock.NowNS())
		}()
		s.operations--
		if s.closed {
			s.retireLocked()
		}
	}()
	if err = opCtx.Err(); err != nil {
		return err
	}
	progress.phase = "consumer"
	step = r.clock.NowNS()
	err = visit(opCtx, value)
	progress.consumer = surfacePublicationElapsed(step, r.clock.NowNS())
	step = r.clock.NowNS()
	r.mu.Lock()
	closed := s.closed
	r.mu.Unlock()
	if closed {
		progress.phase = "current"
		progress.current = surfacePublicationElapsed(step, r.clock.NowNS())
		return retainBorrowCause(ErrRetired, err)
	}
	if current := s.owner.current(); current != nil {
		progress.phase = "current"
		progress.current = surfacePublicationElapsed(step, r.clock.NowNS())
		return retainBorrowCause(current, err)
	}
	progress.current = surfacePublicationElapsed(step, r.clock.NowNS())
	if cancel := opCtx.Err(); cancel != nil {
		if err != nil && err != cancel {
			return errors.Join(cancel, err)
		}
		return cancel
	}
	return err
}
func (o *Resource[T]) Close() {
	if o == nil || o.state == nil {
		return
	}
	s := o.state
	r := s.owner.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	s.retireLocked()
}
func (o *Resource[T]) WaitClosed(ctx context.Context) error {
	if o == nil || o.state == nil || ctx == nil {
		return ErrInvalid
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.state.done:
	}
	r := o.state.owner.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	return o.state.err
}
