package ingress

import (
	"context"
	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sync"
	"sync/atomic"
)

// ConsumerAuthority is supplied by authenticated launcher integration. It is
// distinct from the broker process authority. No request may implement it.
type ConsumerAuthority interface{ CurrentGeneration() (string, error) }

// AccessPolicy is trusted admission, not client configuration. It grants only
// extraction from this binding's fixed source/surface/graph/publication epochs.
// Full-frame access is explicit; there is no wildcard or implicit new epoch.
type AccessPolicy struct {
	Generation              string
	Streams                 []string
	MaxRegions              int
	AllowFullFrame          bool
	MaxRequests, MaxResults int
}
type Access struct {
	runtime                                   *fd.Runtime
	authority                                 ConsumerAuthority
	generation                                string
	fence                                     fd.Fence
	binding                                   fd.BindingRef
	source, surface                           string
	graph                                     fa.Digest
	streams                                   map[string]uint64
	maxRegions                                int
	full                                      bool
	ctx                                       context.Context
	cancel                                    context.CancelFunc
	revoked                                   atomic.Bool
	mu                                        sync.Mutex
	pending, results, maxRequests, maxResults int
}

// NewAccess neither starts nor authenticates a process. The owning server must
// admit session cardinality and provide a Principal from its Receiver, never
// decode a Principal from RPC data. This API is a trusted middleware boundary.
func NewAccess(runtime *fd.Runtime, binding *fd.Binding, authority ConsumerAuthority, policy AccessPolicy) (*Access, error) {
	if runtime == nil || binding == nil || authority == nil || policy.Generation == "" || len(policy.Generation) > 64 || len(policy.Streams) == 0 || len(policy.Streams) > 16 || policy.MaxRegions < 1 || policy.MaxRegions > 16 {
		return nil, fail("access_admission", peer.ErrInvalid)
	}
	if policy.MaxRequests < 1 || policy.MaxRequests > 64 || policy.MaxResults < 1 || policy.MaxResults > 64 {
		return nil, fail("access_limits", peer.ErrInvalid)
	}
	if runtime.Fence() != binding.Fence() {
		return nil, fail("access_binding", fd.ErrDomainMismatch)
	}
	if err := binding.ValidateCurrent(); err != nil {
		return nil, fail("access_binding_retired", err)
	}
	generation, err := authority.CurrentGeneration()
	if err != nil || generation != policy.Generation {
		return nil, fail("access_consumer", peer.ErrUnauthorized)
	}
	scope := binding.Scope()
	streams := make(map[string]uint64, len(policy.Streams))
	for _, name := range policy.Streams {
		epoch, ok := scope.Streams[name]
		if !ok || epoch == 0 || streams[name] != 0 {
			return nil, fail("access_stream", peer.ErrInvalid)
		}
		streams[name] = epoch
	}
	if err := binding.ValidateCurrent(); err != nil {
		return nil, fail("access_binding_retired", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Access{runtime: runtime, authority: authority, generation: generation, fence: binding.Fence(), binding: binding.Reference(), source: scope.Source, surface: scope.Surface, graph: scope.Graph, streams: streams, maxRegions: policy.MaxRegions, full: policy.AllowFullFrame, ctx: ctx, cancel: cancel, maxRequests: policy.MaxRequests, maxResults: policy.MaxResults}, nil
}
func (a *Access) Revoke() {
	if a != nil && !a.revoked.Swap(true) {
		if a.cancel != nil {
			a.cancel()
		}
	}
}
func (a *Access) current() error {
	if a == nil || a.authority == nil || a.revoked.Load() {
		return fail("access_revoked", peer.ErrRetired)
	}
	generation, err := a.authority.CurrentGeneration()
	if err != nil || generation != a.generation {
		a.Revoke()
		return fail("access_consumer_changed", peer.ErrRetired)
	}
	if a.revoked.Load() {
		return fail("access_revoked", peer.ErrRetired)
	}
	return nil
}

type AccessResult struct {
	access *Access
	inner  *fd.Result
	close  sync.Once
}

func (a *Access) Extract(ctx context.Context, principal peer.Principal, request fd.Request) (*AccessResult, error) {
	if ctx == nil {
		return nil, fail("access_context", peer.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := a.current(); err != nil {
		return nil, err
	}
	if principal.Generation != a.generation {
		return nil, fail("access_principal", peer.ErrUnauthorized)
	}
	epoch, ok := a.streams[request.Frame.Ref.Stream]
	if !ok || request.Frame.Fence != a.fence || request.Source != a.source || request.Surface != a.surface || request.Frame.Ref.Graph != a.graph || request.Frame.Ref.Epoch != epoch {
		return nil, fail("access_scope", peer.ErrUnauthorized)
	}
	if len(request.Regions) == 0 || len(request.Regions) > a.maxRegions {
		return nil, fail("access_regions", peer.ErrUnauthorized)
	}
	// Snapshot bounded metadata before authorizing its content. Payloads and
	// output buffers remain charged by the domain; this never copies pixels.
	request.Regions = append([]fa.Region(nil), request.Regions...)
	for _, region := range request.Regions {
		if region.FullFrame && !a.full {
			return nil, fail("access_full_frame", peer.ErrUnauthorized)
		}
	}
	a.mu.Lock()
	if a.pending >= a.maxRequests || a.pending+a.results >= a.maxResults {
		a.mu.Unlock()
		return nil, fail("access_capacity", fa.CapacityRejected)
	}
	a.pending++
	a.mu.Unlock()
	defer func() { a.mu.Lock(); a.pending--; a.mu.Unlock() }()
	opCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(a.ctx, cancel)
	defer func() { stop(); cancel() }()
	inner, err := a.runtime.Extract(opCtx, request)
	if current := a.current(); current != nil {
		if inner != nil {
			inner.Close()
		}
		return nil, current
	}
	if err != nil {
		return nil, err
	}
	if err = opCtx.Err(); err != nil {
		inner.Close()
		return nil, err
	}
	a.mu.Lock()
	a.results++
	a.mu.Unlock()
	return &AccessResult{access: a, inner: inner}, nil
}

// Visit prepares output for this authenticated consumer. Do not commit output
// until the final consumer check succeeds. Source retirement does NOT erase an
// already extracted artifact: its original proof and pixels remain immutable.
// Previously delivered consumer-owned data is outside this access lifecycle.
func (r *AccessResult) Visit(index int, visit func(fd.Artifact, []byte) error) error {
	if r == nil || r.inner == nil {
		return peer.ErrInvalid
	}
	if err := r.access.current(); err != nil {
		return err
	}
	err := r.inner.Visit(index, visit)
	if current := r.access.current(); current != nil {
		return current
	}
	return err
}

// Describe returns an outcome only while the authenticated consumer remains
// authorized. Negative outcomes contain no pixel or fabricated success proof.
func (r *AccessResult) Describe(index int) (fd.Artifact, error) {
	if r == nil || r.inner == nil {
		return fd.Artifact{}, peer.ErrInvalid
	}
	if err := r.access.current(); err != nil {
		return fd.Artifact{}, err
	}
	a, err := r.inner.Describe(index)
	if current := r.access.current(); current != nil {
		return fd.Artifact{}, current
	}
	return a, err
}

func (r *AccessResult) Close() {
	if r != nil && r.inner != nil {
		r.close.Do(func() { r.inner.Close(); r.access.mu.Lock(); r.access.results--; r.access.mu.Unlock() })
	}
}
