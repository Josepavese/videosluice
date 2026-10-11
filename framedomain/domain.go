// Package framedomain owns the process-fenced allocation/binding boundary above
// the semantic frameaccess core. Its ports are trusted middleware integrations,
// not configurable assertions accepted from RPC clients.
package framedomain

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"sync"
	"sync/atomic"
)

var (
	ErrDomainMismatch = errors.New("runtime_domain_mismatch")
	ErrProcessChanged = errors.New("authenticated_process_generation_changed")
	ErrRetired        = errors.New("source_generation_retired")
	ErrInvalid        = errors.New("invalid_identity_contract")
)

type Fence struct {
	Namespace         [16]byte
	ProcessGeneration string
}
type DeliveryRef struct {
	Fence Fence
	Ref   fa.DeliveryRef
}
type OriginRef struct {
	Fence Fence
	Ref   fa.OriginRef
}
type Proof struct {
	Fence  Fence
	Core   fa.Proof
	Digest fa.Digest
}
type Artifact struct {
	Region string
	State  fa.Code
	Proof  Proof
}
type Entropy interface{ Fill([]byte) error }

// ProcessAuthority must return the current CANONICAL authenticated process
// generation (not PID alone, wall time, a user argument or a constant). A
// changed/unavailable generation permanently revokes this Runtime.
type ProcessAuthority interface{ CurrentGeneration() (string, error) }
type Admission interface {
	Attest(context.Context, fa.Scope) (fa.CopyAdmission, error)
}
type Limits struct {
	MaxSources, MaxStreams, MaxSourceGenerations int
	MaxMemoryBytes, MaxSourceBytes               int
	MaxBuffers, MaxBufferBytes                   int
	MaxResources, MaxResourceOperations          int
}

func validToken(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}

var runtimeSerial atomic.Uint64

func nextSerial() (uint64, error) {
	for {
		old := runtimeSerial.Load()
		if old == ^uint64(0) {
			return 0, fa.CapacityRejected
		}
		if runtimeSerial.CompareAndSwap(old, old+1) {
			return old + 1, nil
		}
	}
}

type streamState struct {
	epoch, frame uint64
	owner        *Binding
}
type Runtime struct {
	mu                                         sync.Mutex
	ctx                                        context.Context
	cancel                                     context.CancelFunc
	ownedResources                             map[*resourceState]struct{}
	fence                                      Fence
	authority                                  ProcessAuthority
	limits                                     Limits
	clock                                      fa.Clock
	closed                                     bool
	sourceEpochs                               map[string]uint64
	streams                                    map[string]*streamState
	active                                     map[string]*Binding
	stores, generations                        int
	bufferBytes, buffers                       int
	resourceBytes, resources, resourceFailures int
	sourceBytes                                map[string]int
}

func validRuntimeInputs(entropy Entropy, clock fa.Clock, limits Limits) bool {
	if limits.MaxResources < 1 || limits.MaxResources > 1024 || limits.MaxResourceOperations < 1 || limits.MaxResourceOperations > 64 {
		return false
	}
	return entropy != nil && clock != nil && limits.MaxSources >= 1 && limits.MaxSources <= MaxBindingReferences && limits.MaxStreams >= 1 && limits.MaxStreams <= 1024 && limits.MaxSourceGenerations >= 1 && limits.MaxSourceGenerations <= 128 && limits.MaxMemoryBytes >= 1 && limits.MaxMemoryBytes <= 1<<30 && limits.MaxSourceBytes >= 1 && limits.MaxSourceBytes <= limits.MaxMemoryBytes && limits.MaxBuffers >= 1 && limits.MaxBuffers <= 1024 && limits.MaxBufferBytes >= 1 && limits.MaxBufferBytes <= limits.MaxSourceBytes
}

func New(authority ProcessAuthority, entropy Entropy, clock fa.Clock, limits Limits) (*Runtime, error) {
	if authority == nil || !validRuntimeInputs(entropy, clock, limits) {
		return nil, ErrInvalid
	}
	generation, err := authority.CurrentGeneration()
	if err != nil || !validToken(generation) {
		return nil, ErrProcessChanged
	}
	r, err := prepareRuntime(entropy, clock, limits)
	if err != nil {
		return nil, err
	}
	current, err := authority.CurrentGeneration()
	if err != nil || current != generation {
		r.Revoke()
		return nil, ErrProcessChanged
	}
	r.authority = authority
	r.fence.ProcessGeneration = generation
	return r, nil
}

// prepareRuntime is private: no domain operation can use its unpublished
// state until the constructor binds an authenticated process authority.
func prepareRuntime(entropy Entropy, clock fa.Clock, limits Limits) (*Runtime, error) {
	serial, err := nextSerial()
	if err != nil {
		return nil, err
	}
	var nonce [16]byte
	if err = entropy.Fill(nonce[:]); err != nil || nonce == ([16]byte{}) {
		return nil, ErrInvalid
	}
	// A CSPRNG yields a uniform 128-bit nonce. XOR of the process-local serial
	// preserves that entropy and prevents literal reuse from a repeated test
	// source inside one process. No claim is made against a malicious provider.
	binary.BigEndian.PutUint64(nonce[8:], binary.BigEndian.Uint64(nonce[8:])^serial)
	if nonce == ([16]byte{}) {
		return nil, ErrInvalid
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Runtime{ctx: ctx, cancel: cancel, ownedResources: make(map[*resourceState]struct{}), fence: Fence{Namespace: nonce}, limits: limits, clock: clock, sourceEpochs: make(map[string]uint64), streams: make(map[string]*streamState), active: make(map[string]*Binding), sourceBytes: make(map[string]int)}, nil
}
func (r *Runtime) Fence() Fence { return r.fence }

// ValidateDomain fences trusted control operations as well as frame requests.
func (r *Runtime) ValidateDomain(f Fence) error { return r.checkFence(f) }
func (r *Runtime) checkProcess() error {
	if r.authority == nil {
		return ErrProcessChanged
	}
	generation, err := r.authority.CurrentGeneration()
	if err != nil || generation != r.fence.ProcessGeneration {
		r.Revoke()
		return ErrProcessChanged
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrProcessChanged
	}
	return nil
}
func (r *Runtime) checkFence(f Fence) error {
	if f != r.fence {
		return ErrDomainMismatch
	}
	return r.checkProcess()
}

// Layout excludes every counter/epoch: the allocator, never its caller, creates
// those values. Closing/reopening a source consumes fresh media/publication
// epochs. Failed admissions consume their reserved counters too.
type Layout struct {
	Source                                       string
	Graph                                        fa.Digest
	Streams                                      []string
	Surface                                      string
	Geometry                                     fa.Geometry
	DecodedWidth, DecodedHeight, SurfaceRotation int
}
type Binding struct {
	runtime                         *Runtime
	scope                           fa.Scope
	core                            *fa.Store
	copyAdmission                   fa.CopyAdmission
	maxCopyNS                       uint64
	ctx                             context.Context
	cancel                          context.CancelFunc
	closed, released                bool
	operations, results, storeBytes int
	buffers                         map[*Buffer]struct{}
	resources                       map[*resourceState]struct{}
}

func cloneScope(s fa.Scope) fa.Scope {
	streams := make(map[string]uint64, len(s.Streams))
	for k, v := range s.Streams {
		streams[k] = v
	}
	s.Streams = streams
	return s
}
func (b *Binding) Scope() fa.Scope { return cloneScope(b.scope) }
func (b *Binding) Fence() Fence    { return b.runtime.fence }

// ValidateCurrent is for trusted policy admission, not an artifact-retention
// check. Already extracted immutable results keep their original ownership.
func (b *Binding) ValidateCurrent() error { return b.current() }

// CopyAdmission returns the immutable receipt actually accepted when this
// binding was opened. It is metadata, not authority to open a consumer channel.
func (b *Binding) CopyAdmission() (fa.CopyAdmission, error) {
	if err := b.current(); err != nil {
		return fa.CopyAdmission{}, err
	}
	return b.copyAdmission, nil
}

// Open constructs the real core internally, so a caller cannot attach a Store
// built with a reused epoch. Admission is provided by trusted runtime telemetry.
// This API does not start/restart workers or fake an attestation procedure.
func (r *Runtime) Open(ctx context.Context, l Layout, limits fa.Limits, copier fa.Copier, admission Admission, policy fa.CopyPolicy, integrity fa.PayloadIntegrity) (*Binding, error) {
	if ctx == nil || ctx.Err() != nil || !validToken(l.Source) || l.Graph == (fa.Digest{}) || len(l.Streams) < 1 || len(l.Streams) > 16 || admission == nil || limits.PoolBytes < 1 {
		return nil, ErrInvalid
	}
	// Reserve the immutable pixel arena AND the selected digest provider's
	// workspace before either can be allocated; retained generations stay charged.
	storeBytes, err := fa.StoreMemoryBytes(limits, integrity)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(l.Streams))
	for _, name := range l.Streams {
		if !validToken(name) || seen[name] {
			return nil, ErrInvalid
		}
		seen[name] = true
	}
	if err := r.checkProcess(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, ErrProcessChanged
	}
	if r.active[l.Source] != nil {
		r.mu.Unlock()
		return nil, ErrInvalid
	}
	_, knownSource := r.sourceEpochs[l.Source]
	if !knownSource && len(r.sourceEpochs) >= r.limits.MaxSources || r.generations >= r.limits.MaxSourceGenerations || !r.canChargeLocked(l.Source, storeBytes) {
		r.mu.Unlock()
		return nil, fa.CapacityRejected
	}
	if r.sourceEpochs[l.Source] == ^uint64(0) {
		r.mu.Unlock()
		return nil, fa.CapacityRejected
	}
	newStreams := 0
	for _, name := range l.Streams {
		state := r.streams[name]
		if state == nil {
			newStreams++
		} else if state.owner != nil || state.epoch == ^uint64(0) {
			r.mu.Unlock()
			return nil, fa.CapacityRejected
		}
	}
	if newStreams > r.limits.MaxStreams-len(r.streams) {
		r.mu.Unlock()
		return nil, fa.CapacityRejected
	}
	r.sourceEpochs[l.Source]++
	scope := fa.Scope{Source: l.Source, MediaEpoch: r.sourceEpochs[l.Source], Graph: l.Graph, Streams: make(map[string]uint64, len(l.Streams)), Surface: l.Surface, Geometry: l.Geometry, DecodedWidth: l.DecodedWidth, DecodedHeight: l.DecodedHeight, SurfaceRotation: l.SurfaceRotation}
	boundCtx, cancel := context.WithCancel(context.Background())
	b := &Binding{runtime: r, scope: scope, ctx: boundCtx, cancel: cancel, storeBytes: storeBytes, maxCopyNS: limits.MaxCopyNS, operations: 1, buffers: make(map[*Buffer]struct{}), resources: make(map[*resourceState]struct{})}
	for _, name := range l.Streams {
		state := r.streams[name]
		if state == nil {
			state = &streamState{}
			r.streams[name] = state
		}
		state.epoch++
		state.frame = 0
		state.owner = b
		scope.Streams[name] = state.epoch
	}
	r.active[l.Source] = b
	r.stores += storeBytes
	r.sourceBytes[l.Source] += storeBytes
	r.generations++
	r.mu.Unlock()
	admissionCtx, cancelAdmission := context.WithCancel(ctx)
	stopAdmission := context.AfterFunc(boundCtx, cancelAdmission)
	defer func() { stopAdmission(); cancelAdmission() }()
	a, err := admission.Attest(admissionCtx, cloneScope(scope))
	var store *fa.Store
	if err == nil && admissionCtx.Err() != nil {
		err = fa.Canceled
	}
	if err == nil {
		err = r.checkProcess()
	}
	if err == nil {
		store, err = fa.NewStore(scope, limits, r.clock, copier, a, policy, integrity)
	}
	if err == nil && admissionCtx.Err() != nil {
		err = fa.Canceled
	}
	if err == nil {
		err = r.checkProcess()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	b.operations--
	if err != nil || b.closed || r.closed {
		if err == nil {
			err = ErrRetired
		}
		r.closeLocked(b)
		return nil, err
	}
	b.core = store
	b.copyAdmission = a
	return b, nil
}
func (r *Runtime) cleanupLocked(b *Binding) {
	if b.closed && !b.released && b.operations == 0 && b.results == 0 && len(b.buffers) == 0 && len(b.resources) == 0 {
		b.released = true
		b.core = nil
		r.stores -= b.storeBytes
		r.sourceBytes[b.scope.Source] -= b.storeBytes
		r.generations--
	}
}
func (r *Runtime) closeLocked(b *Binding) {
	if !b.closed {
		b.closed = true
		b.cancel()
		for buffer := range b.buffers {
			buffer.closeLocked()
		}
		for resource := range b.resources {
			resource.retireLocked()
		}
		if r.active[b.scope.Source] == b {
			delete(r.active, b.scope.Source)
		}
		for _, state := range r.streams {
			if state.owner == b {
				state.owner = nil
			}
		}
	}
	r.cleanupLocked(b)
}
func (b *Binding) Close() { r := b.runtime; r.mu.Lock(); defer r.mu.Unlock(); r.closeLocked(b) }
func (r *Runtime) Revoke() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.cancel()
	for _, b := range r.active {
		r.closeLocked(b)
	}
	for resource := range r.ownedResources {
		resource.retireLocked()
	}
}

// Ticket is opaque and minted only after source/process binding validation.
// RPC clients get Ref(), not publisher authority or an editable ticket.
type Ticket struct {
	binding *Binding
	ref     DeliveryRef
}

func (t Ticket) Ref() DeliveryRef { return t.ref }
func (b *Binding) AllocateDelivery(stream string) (Ticket, error) {
	r := b.runtime
	if !validToken(stream) {
		return Ticket{}, ErrInvalid
	}
	if err := r.checkProcess(); err != nil {
		return Ticket{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if b.closed || b.core == nil || r.active[b.scope.Source] != b {
		return Ticket{}, ErrRetired
	}
	state := r.streams[stream]
	if state == nil || state.owner != b {
		return Ticket{}, fa.SourceMismatch
	}
	if state.frame == ^uint64(0) {
		return Ticket{}, fa.CapacityRejected
	}
	state.frame++
	return Ticket{b, DeliveryRef{r.fence, fa.DeliveryRef{Stream: stream, Epoch: state.epoch, FrameID: state.frame, Graph: b.scope.Graph}}}, nil
}
func (b *Binding) begin(ctx context.Context) (*fa.Store, context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, nil, ErrInvalid
	}
	r := b.runtime
	if err := r.checkProcess(); err != nil {
		return nil, nil, nil, err
	}
	r.mu.Lock()
	if b.closed || b.core == nil || r.active[b.scope.Source] != b {
		r.mu.Unlock()
		return nil, nil, nil, ErrRetired
	}
	b.operations++
	store := b.core
	r.mu.Unlock()
	opCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(b.ctx, cancel)
	finish := func() {
		stop()
		cancel()
		r.mu.Lock()
		defer r.mu.Unlock()
		b.operations--
		r.cleanupLocked(b)
	}
	return store, opCtx, finish, nil
}
func (b *Binding) current() error {
	r := b.runtime
	if err := r.checkProcess(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if b.closed || r.active[b.scope.Source] != b {
		return ErrRetired
	}
	return nil
}
func (b *Binding) validateTicket(t Ticket) error {
	if t.binding != b || t.ref.Fence != b.runtime.fence {
		return ErrDomainMismatch
	}
	return nil
}

type Publication struct {
	Ticket    Ticket
	Transform fa.Transform
}

func (b *Binding) Publish(ctx context.Context, o OriginRef, deliveries []Publication, pixels []byte, observedNS, deadlineNS uint64) error {
	if len(deliveries) < 1 || len(deliveries) > 16 {
		return ErrInvalid
	}
	if o.Fence != b.runtime.fence {
		return ErrDomainMismatch
	}
	if o.Ref.Source != b.scope.Source || o.Ref.MediaEpoch != b.scope.MediaEpoch || o.Ref.Graph != b.scope.Graph {
		return ErrRetired
	}
	bindings := make([]fa.DeliveryBinding, len(deliveries))
	for i, d := range deliveries {
		if err := b.validateTicket(d.Ticket); err != nil {
			return err
		}
		bindings[i] = fa.DeliveryBinding{Ref: d.Ticket.ref.Ref, Transform: d.Transform}
	}
	store, opCtx, finish, err := b.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	err = store.TryPublish(opCtx, o.Ref, bindings, pixels, observedNS, deadlineNS)
	if current := b.current(); current != nil {
		return current
	}
	return err
}
func (b *Binding) BindUnavailable(ctx context.Context, t Ticket) error {
	if err := b.validateTicket(t); err != nil {
		return err
	}
	store, _, finish, err := b.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	err = store.BindUnavailable(t.ref.Ref)
	if current := b.current(); current != nil {
		return current
	}
	return err
}

// BindExact records authenticated capture-to-delivery metadata independently
// from native surface retention. It never manufactures pixels or waits for them.
func (b *Binding) BindExact(ctx context.Context, o OriginRef, t Ticket, tr fa.Transform) error {
	if err := b.validateTicket(t); err != nil {
		return err
	}
	if o.Fence != b.runtime.fence {
		return ErrDomainMismatch
	}
	store, _, finish, err := b.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	err = store.Bind(o.Ref, fa.DeliveryBinding{Ref: t.ref.Ref, Transform: tr})
	if current := b.current(); current != nil {
		return current
	}
	return err
}

type Request struct {
	ID              [16]byte
	Frame           DeliveryRef
	Source, Surface string
	Regions         []fa.Region
	DeadlineNS      uint64
}
type Result struct {
	mu      sync.Mutex
	binding *Binding
	inner   *fa.Result
}

func (r *Runtime) Extract(ctx context.Context, q Request) (*Result, error) {
	if err := r.checkFence(q.Frame.Fence); err != nil {
		return nil, err
	}
	if !validToken(q.Frame.Ref.Stream) || !validToken(q.Source) {
		return nil, ErrInvalid
	}
	r.mu.Lock()
	state := r.streams[q.Frame.Ref.Stream]
	var b *Binding
	if state != nil {
		b = state.owner
	}
	r.mu.Unlock()
	if b == nil {
		return nil, ErrRetired
	}
	if b.scope.Source != q.Source {
		return nil, fa.SourceMismatch
	}
	store, opCtx, finish, err := b.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	inner, err := store.Extract(opCtx, fa.Request{ID: q.ID, Frame: q.Frame.Ref, Source: q.Source, Surface: q.Surface, Regions: q.Regions, DeadlineNS: q.DeadlineNS})
	if err != nil {
		return nil, err
	}
	if err = b.current(); err != nil {
		inner.Close()
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if b.closed || r.active[b.scope.Source] != b {
		inner.Close()
		return nil, ErrRetired
	}
	b.results++
	return &Result{binding: b, inner: inner}, nil
}
func domainDigest(f Fence, inner fa.Digest) fa.Digest {
	buf := []byte("video-broker.domain-frame-proof.v1\x00")
	buf = append(buf, f.Namespace[:]...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(f.ProcessGeneration)))
	buf = append(buf, f.ProcessGeneration...)
	buf = append(buf, inner[:]...)
	return sha256.Sum256(buf)
}
func VerifyDomainProof(p Proof, expected Fence) error {
	if expected.Namespace == ([16]byte{}) || !validToken(expected.ProcessGeneration) || p.Fence != expected {
		return ErrDomainMismatch
	}
	if p.Digest != domainDigest(p.Fence, p.Core.Digest) {
		return ErrInvalid
	}
	return nil
}

func (r *Result) Describe(index int) (Artifact, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inner == nil {
		return Artifact{}, ErrRetired
	}
	a, err := r.inner.Describe(index)
	if err != nil {
		return Artifact{}, err
	}
	var proof Proof
	if a.State == fa.Complete {
		f := r.binding.runtime.fence
		proof = Proof{f, a.Proof, domainDigest(f, a.Proof.Digest)}
	}
	return Artifact{a.Region, a.State, proof}, nil
}

func (r *Result) Visit(index int, visit func(Artifact, []byte) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inner == nil || visit == nil {
		return ErrRetired
	}
	return r.inner.Visit(index, func(a fa.Artifact, p []byte) error {
		f := r.binding.runtime.fence
		proof := Proof{f, a.Proof, domainDigest(f, a.Proof.Digest)}
		return visit(Artifact{a.Region, a.State, proof}, p)
	})
}
func (r *Result) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inner == nil {
		return
	}
	r.inner.Close()
	r.inner = nil
	owner := r.binding.runtime
	owner.mu.Lock()
	defer owner.mu.Unlock()
	r.binding.results--
	owner.cleanupLocked(r.binding)
}

type Stats struct {
	ActiveSources, KnownSources, KnownStreams, LiveGenerations, ReservedStoreBytes int
	Revoked                                                                        bool
	ReservedBufferBytes, LiveBuffers, ReservedMemoryBytes                          int
	ReservedResourceBytes, LiveResources, ResourceFailures                         int
}

func (r *Runtime) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{len(r.active), len(r.sourceEpochs), len(r.streams), r.generations, r.stores, r.closed, r.bufferBytes, r.buffers, r.stores + r.bufferBytes + r.resourceBytes, r.resourceBytes, r.resources, r.resourceFailures}
}
