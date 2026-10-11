package framedomain

import (
	"context"
	"errors"
	fa "github.com/Josepavese/videosluice/frameaccess"
)

// BootstrapPlan admits the trusted provider's complete creation footprint
// before acquiring the process authority needed to publish a Runtime. It is
// composition code, never a plan accepted from configuration or an RPC peer.
// Create follows ResourcePlan's partial ownership contract. Authority is
// borrowed from the owned value and must remain live for its whole lifetime.
type BootstrapPlan[T any] struct {
	Bytes   int
	Create  func(context.Context) (Created[T], ProcessAuthority, error)
	Release func(T) error
}

type BootstrapFailure struct {
	Stage string
	Cause error
}

func (e *BootstrapFailure) Error() string { return "frame bootstrap failed: stage=" + e.Stage }
func (e *BootstrapFailure) Unwrap() error { return e.Cause }

// NewFromBootstrap keeps one global ledger from native creation through all
// later source/control resources. No second budget, late adoption or fake
// authority is used. Only success publishes a usable domain.
// On error a nonnil Runtime is revoked and retained for cleanup diagnostics:
// failed, panicking or still-closing resources remain charged and referenced.
// The returned resource, when successful, must outlive the domain. Revoke the
// Runtime before closing it. Neither success nor a Fence promises ready frames.
func NewFromBootstrap[T any](ctx context.Context, entropy Entropy, clock fa.Clock, limits Limits, plan BootstrapPlan[T]) (*Runtime, *Resource[T], error) {
	if ctx == nil || !validRuntimeInputs(entropy, clock, limits) || plan.Bytes <= 0 || plan.Create == nil || plan.Release == nil {
		return nil, nil, &BootstrapFailure{"admission", ErrInvalid}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, &BootstrapFailure{"admission", err}
	}
	// Reject before even minting the unpublished namespace or calling a provider.
	if plan.Bytes > limits.MaxMemoryBytes {
		return nil, nil, &BootstrapFailure{"admission", fa.CapacityRejected}
	}
	r, err := prepareRuntime(entropy, clock, limits)
	if err != nil {
		return nil, nil, &BootstrapFailure{"namespace", err}
	}
	p := ResourcePlan[T]{Bytes: plan.Bytes, Release: plan.Release, Create: func(operation context.Context) (Created[T], error) {
		value, authority, err := plan.Create(operation)
		if err != nil {
			return value, &BootstrapFailure{"create", err}
		}
		if !value.Owned {
			return value, nil // preserve unconfirmed-ownership quarantine semantics
		}
		if err = operation.Err(); err != nil {
			return value, &BootstrapFailure{"authenticate", err}
		}
		safeAuthority := bootstrapAuthority{authority}
		generation, err := authenticateBootstrap(safeAuthority)
		if err != nil {
			return value, &BootstrapFailure{"authenticate", err}
		}
		r.authority = safeAuthority
		r.fence.ProcessGeneration = generation
		return value, nil
	}}
	resource, err := ownResourceMode(ctx, resourceOwner{runtime: r}, p, true)
	if err != nil {
		r.Revoke()
		var bootstrapFailure *BootstrapFailure
		var resourceFailure *ResourceFailure
		if !errors.As(err, &bootstrapFailure) && !errors.As(err, &resourceFailure) {
			err = &BootstrapFailure{"finalize", err}
		}
		return r, nil, err
	}
	return r, resource, nil
}

// Keep every later generation check guarded too, including the resource
// engine's final post-create check and checks after the constructor returns.
type bootstrapAuthority struct{ provider ProcessAuthority }

func (a bootstrapAuthority) CurrentGeneration() (generation string, err error) {
	defer func() {
		if recover() != nil {
			generation, err = "", ErrProcessChanged
		}
	}()
	if a.provider == nil {
		return "", ErrProcessChanged
	}
	return a.provider.CurrentGeneration()
}

func authenticateBootstrap(authority ProcessAuthority) (generation string, err error) {
	generation, err = authority.CurrentGeneration()
	if err != nil || !validToken(generation) {
		return "", ErrProcessChanged
	}
	current, err := authority.CurrentGeneration()
	if err != nil || current != generation {
		return "", ErrProcessChanged
	}
	return generation, nil
}
