package frameaccess

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
)

type Scope struct {
	Source                      string
	MediaEpoch                  uint64
	Graph                       Digest
	Streams                     map[string]uint64 // Broker-owned binding + publication generation.
	Surface                     string
	Geometry                    Geometry
	DecodedWidth, DecodedHeight int
	SurfaceRotation             int
}

func (s Scope) validate() error {
	if !token(s.Source) || s.MediaEpoch == 0 || s.Graph == (Digest{}) || len(s.Streams) == 0 || len(s.Streams) > 16 {
		return InvalidContract
	}
	if _, err := s.Geometry.Bytes(); err != nil {
		return err
	}
	if _, err := (Geometry{s.DecodedWidth, s.DecodedHeight, s.DecodedWidth}).Bytes(); err != nil {
		return err
	}
	if s.Surface != "processed_full" && s.Surface != "source_decoded" {
		return InvalidContract
	}
	if s.SurfaceRotation != 0 && s.SurfaceRotation != 90 && s.SurfaceRotation != 180 && s.SurfaceRotation != 270 {
		return InvalidContract
	}
	if s.Surface == "source_decoded" && s.SurfaceRotation != 0 {
		return InvalidContract
	}
	w, h := s.DecodedWidth, s.DecodedHeight
	if s.SurfaceRotation == 90 || s.SurfaceRotation == 270 {
		w, h = h, w
	}
	// A resized analysis image may NEVER be relabelled as processed_full.
	if s.Geometry.Width != w || s.Geometry.Height != h {
		return InvalidContract
	}
	for stream, epoch := range s.Streams {
		if !token(stream) || epoch == 0 {
			return InvalidContract
		}
	}
	return nil
}
func cloneScope(s Scope) Scope {
	m := make(map[string]uint64, len(s.Streams))
	for k, v := range s.Streams {
		m[k] = v
	}
	s.Streams = m
	return s
}

type Limits struct {
	MaxFrames, MaxBindings                     int
	PoolBytes, MaxFrameBytes, MaxArtifactBytes int
	MaxOutputPerRequest, MaxFullFrameBytes     int
	MaxRegions, MaxResults                     int
	MaxAgeNS, MaxCopyNS                        uint64
}

// PayloadIntegrity is a trusted composition port, never an RPC option. Sum256
// must compute exactly SHA-256, borrow the payload only until return and fail
// closed if unavailable. WorkspaceBytes bounds all per-call provider memory,
// including kernel workspace. Extract's copyBusy slot serializes its calls.
type PayloadIntegrity struct {
	Sum256         func([]byte) [32]byte
	WorkspaceBytes int
}

// StoreMemoryBytes is charged before construction, in the same source/global
// ledger as the pixel arena. Provider memory must not silently consume arena
// capacity or escape accounting. A provider cannot change within a Store.
func StoreMemoryBytes(l Limits, p PayloadIntegrity) (int, error) {
	if l.PoolBytes < 1 || l.PoolBytes > 256<<20 || p.Sum256 == nil || p.WorkspaceBytes < 0 || p.WorkspaceBytes > 1<<20 {
		return 0, InvalidContract
	}
	return l.PoolBytes + p.WorkspaceBytes, nil
}

func (l Limits) validate(s Scope) error {
	n, _ := s.Geometry.Bytes()
	if l.MaxFrames < 1 || l.MaxFrames > 64 || l.MaxBindings < 1 || l.MaxBindings > 4096 || l.PoolBytes < 1 || l.PoolBytes > 256<<20 || l.MaxFrameBytes < n || l.MaxFrameBytes > l.PoolBytes || l.MaxArtifactBytes < 1 || l.MaxArtifactBytes > l.PoolBytes-l.MaxFrameBytes || l.MaxOutputPerRequest < 1 || l.MaxOutputPerRequest > l.MaxArtifactBytes || l.MaxFullFrameBytes < 1 || l.MaxFullFrameBytes > l.MaxOutputPerRequest || l.MaxRegions < 1 || l.MaxRegions > 16 || l.MaxResults < 1 || l.MaxResults > 16 || l.MaxAgeNS == 0 || l.MaxAgeNS > 10_000_000_000 || l.MaxCopyNS == 0 || l.MaxCopyNS > 1_000_000_000 {
		return InvalidContract
	}
	return nil
}

type block struct {
	start, size, refs int
	artifact          bool
}
type storedFrame struct {
	origin   OriginRef
	geometry Geometry
	pixels   *block
	at       uint64
	// Only the Store's detached, immutable copy can carry deferred integrity.
	// Never expose a region until the complete source digest has matched.
	pendingIntegrity bool
	expectedDigest   Digest
	integrityFailed  bool
}
type binding struct {
	ref         DeliveryRef
	origin      OriginRef
	transform   Transform
	at          uint64
	unavailable bool
}
type DeliveryBinding struct {
	Ref       DeliveryRef
	Transform Transform
}

// A fixed 64-position replay window permits bounded asynchronous completion.
// IDs are never reassigned. Consumed IDs stay rejected after data eviction;
// unseen IDs older than the window are rejected too, with no unbounded ledger.
type sequenceWindow struct{ high, seen uint64 }

func (w sequenceWindow) accepts(n uint64) bool {
	return n != 0 && (n > w.high || (w.high-n < 64 && w.seen&(uint64(1)<<(w.high-n)) == 0))
}
func (w *sequenceWindow) commit(n uint64) {
	if n > w.high {
		delta := n - w.high
		if delta >= 64 {
			w.seen = 0
		} else {
			w.seen <<= delta
		}
		w.high = n
	}
	w.seen |= uint64(1) << (w.high - n)
}

type deliveryWatermark struct {
	epoch  uint64
	window sequenceWindow
}

var storeSequence atomic.Uint64

// Store owns a fixed byte arena INCLUDING retained frames, staging copies,
// pinned old-generation inputs, and outstanding result payloads. No camera
// buffer, ring slice or mutable binding is returned to a consumer.
type Store struct {
	mu                                    sync.Mutex
	id                                    uint64
	scope                                 Scope
	limits                                Limits
	clock                                 Clock
	copier                                Copier
	admission                             CopyAdmission
	policy                                CopyPolicy
	integrity                             PayloadIntegrity
	arena                                 []byte
	blocks                                []*block
	frames                                []*storedFrame
	bindings                              map[DeliveryRef]*binding
	deliveryWatermarks                    map[string]deliveryWatermark
	order                                 []DeliveryRef
	generation, lastNow                   uint64
	origins                               sequenceWindow
	poisoned                              bool
	copyBusy                              atomic.Bool
	openResults, artifactBytes, peakBytes int
	integrityChecks, integrityFailures    uint64
}

func NewStore(s Scope, l Limits, clock Clock, copier Copier, admission CopyAdmission, policy CopyPolicy, integrity PayloadIntegrity) (*Store, error) {
	if clock == nil {
		return nil, InvalidContract
	}
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := l.validate(s); err != nil {
		return nil, err
	}
	if _, err := StoreMemoryBytes(l, integrity); err != nil {
		return nil, err
	}
	if err := checkAdmission(copier, admission, policy, s.MediaEpoch, s.Graph); err != nil {
		return nil, err
	}
	watermarks := make(map[string]deliveryWatermark, len(s.Streams))
	for stream, epoch := range s.Streams {
		watermarks[stream] = deliveryWatermark{epoch: epoch}
	}
	return &Store{id: storeSequence.Add(1), scope: cloneScope(s), limits: l, clock: clock, copier: copier, admission: admission, policy: policy, integrity: integrity, arena: make([]byte, l.PoolBytes), bindings: make(map[DeliveryRef]*binding), deliveryWatermarks: watermarks, generation: 1, lastNow: clock.NowNS()}, nil
}
func (s *Store) usedLocked() int {
	n := 0
	for _, b := range s.blocks {
		n += b.size
	}
	return n
}
func (s *Store) allocateLocked(n int, artifact bool) *block {
	if n <= 0 || n > len(s.arena) || len(s.blocks) >= s.limits.MaxFrames+s.limits.MaxRegions*(s.limits.MaxResults+1)+1 {
		return nil
	}
	start := 0
	for _, b := range s.blocks {
		if n <= b.start-start {
			break
		}
		start = b.start + b.size
	}
	if n > len(s.arena)-start {
		return nil
	}
	b := &block{start: start, size: n, refs: 1, artifact: artifact}
	s.blocks = append(s.blocks, b)
	sort.Slice(s.blocks, func(i, j int) bool { return s.blocks[i].start < s.blocks[j].start })
	s.peakBytes = max(s.peakBytes, s.usedLocked())
	return b
}
func (s *Store) releaseLocked(b *block) {
	b.refs--
	if b.refs != 0 {
		return
	}
	for i, item := range s.blocks {
		if item == b {
			s.blocks = append(s.blocks[:i], s.blocks[i+1:]...)
			return
		}
	}
	panic("frameaccess: double release")
}
func (s *Store) bytes(b *block) []byte { return s.arena[b.start : b.start+b.size] }
func (s *Store) forgetBindingLocked(ref DeliveryRef) {
	delete(s.bindings, ref)
	for i, r := range s.order {
		if r == ref {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}
func (s *Store) removeFrameLocked(f *storedFrame) {
	// Pixel eviction does not erase an attested delivery's causal identity.
	// Metadata has its own existing MaxBindings/MaxAgeNS limits and pins no pixels.
	for i, item := range s.frames {
		if item == f {
			s.frames = append(s.frames[:i], s.frames[i+1:]...)
			break
		}
	}
	s.releaseLocked(f.pixels)
}
func (s *Store) frameLocked(origin OriginRef) *storedFrame {
	for _, f := range s.frames {
		if f.origin == origin {
			return f
		}
	}
	return nil
}
func (s *Store) clearLocked() {
	for len(s.frames) > 0 {
		s.removeFrameLocked(s.frames[0])
	}
	s.bindings = make(map[DeliveryRef]*binding)
	s.order = nil
}
func (s *Store) tickLocked() (uint64, error) {
	now := s.clock.NowNS()
	if s.poisoned || now < s.lastNow {
		s.poisoned = true
		s.clearLocked()
		return now, ProviderUnavailable
	}
	s.lastNow = now
	for i := len(s.frames) - 1; i >= 0; i-- {
		if now-s.frames[i].at >= s.limits.MaxAgeNS {
			s.removeFrameLocked(s.frames[i])
		}
	}
	for ref, b := range s.bindings {
		if now-b.at >= s.limits.MaxAgeNS {
			s.forgetBindingLocked(ref)
		}
	}
	return now, nil
}
func (s *Store) validateDeliveryLocked(r DeliveryRef) error {
	if !r.valid() {
		return InvalidContract
	}
	if r.Graph != s.scope.Graph {
		return GraphMismatch
	}
	epoch, ok := s.scope.Streams[r.Stream]
	if !ok {
		return SourceMismatch
	}
	if r.Epoch != epoch {
		return EpochMismatch
	}
	return nil
}
func (s *Store) validateOriginLocked(o OriginRef) error {
	if !o.valid() {
		return InvalidContract
	}
	if o.Source != s.scope.Source {
		return SourceMismatch
	}
	if o.MediaEpoch != s.scope.MediaEpoch {
		return EpochMismatch
	}
	if o.Graph != s.scope.Graph {
		return GraphMismatch
	}
	return nil
}

// Fixed per-stream windows reject replay after lookup eviction while permitting
// bounded output reordering, independent from capture and delivery ID order.
func (s *Store) validateNewDeliveryLocked(r DeliveryRef) error {
	if err := s.validateDeliveryLocked(r); err != nil {
		return err
	}
	if !s.deliveryWatermarks[r.Stream].window.accepts(r.FrameID) {
		return InvalidContract
	}
	return nil
}
func (s *Store) deadlineLocked(now, deadline uint64) error {
	if deadline <= now {
		return DeadlineExceeded
	}
	if deadline-now > s.limits.MaxCopyNS {
		return InvalidContract
	}
	return nil
}
func (s *Store) insertBindingLocked(b *binding) {
	for len(s.order) >= s.limits.MaxBindings {
		s.forgetBindingLocked(s.order[0])
	}
	s.bindings[b.ref] = b
	mark := s.deliveryWatermarks[b.ref.Stream]
	mark.window.commit(b.ref.FrameID)
	s.deliveryWatermarks[b.ref.Stream] = mark
	s.order = append(s.order, b.ref)
}

// TryPublish is best-effort feature work: a busy copy or mutex returns immediately.
// The integration MUST call it with source-owned detached input on an optional
// worker, never while holding a V4L2/HBN/codec lease. No publish goroutine/queue
// is spawned here. A provider that hangs cannot block later TryPublish calls.
func (s *Store) TryPublish(ctx context.Context, o OriginRef, deliveries []DeliveryBinding, pixels []byte, observedNS, deadline uint64) error {
	if len(deliveries) < 1 {
		return InvalidContract
	}
	return s.tryPublish(ctx, o, deliveries, pixels, nil, observedNS, deadline)
}

// TryPublishSurface retains exact native pixels independently from asynchronous
// output delivery. A later Bind must name this precise origin; absence/eviction
// stays unavailable/expired. No fabricated stream delivery or timestamp match.
func (s *Store) TryPublishSurface(ctx context.Context, o OriginRef, pixels []byte, observedNS, deadline uint64) error {
	return s.tryPublish(ctx, o, nil, pixels, nil, observedNS, deadline)
}

// TryStageSurface is a trusted ingress port, not an RPC option. It takes a
// bounded detached copy, preserving the producer's expected SHA-256 verbatim.
// Structural/source checks remain immediate; payload verification occurs once
// before the first successful pixel extraction. Unused/expired surfaces need
// no second whole-frame hash. Caller mutations after return cannot affect it.
func (s *Store) TryStageSurface(ctx context.Context, o OriginRef, pixels []byte, expected Digest, observedNS, deadline uint64) error {
	return s.tryPublish(ctx, o, nil, pixels, &expected, observedNS, deadline)
}

func (s *Store) tryPublish(ctx context.Context, o OriginRef, deliveries []DeliveryBinding, pixels []byte, expected *Digest, observedNS, deadline uint64) (err error) {
	progress := publicationProgress{phase: "admission", clock: s.clock}
	progress.started = progress.now()
	defer func() { err = progress.failure(ctx, err) }()
	if ctx == nil {
		return InvalidContract
	}
	if ctx.Err() != nil {
		return Canceled
	}
	// Reject untrusted batch cardinality BEFORE allocating validation metadata.
	if len(deliveries) > 16 || len(deliveries) > s.limits.MaxBindings {
		return InvalidContract
	}
	if !s.copyBusy.CompareAndSwap(false, true) {
		return CapacityRejected
	}
	defer s.copyBusy.Store(false)
	if !s.mu.TryLock() {
		return CapacityRejected
	}
	now, err := s.tickLocked()
	if err == nil {
		err = s.validateOriginLocked(o)
	}
	if err == nil {
		err = s.deadlineLocked(now, deadline)
	}
	if err == nil && observedNS > now {
		err = InvalidContract
	}
	if err == nil && now-observedNS >= s.limits.MaxAgeNS {
		err = Expired
	}
	if err == nil && !s.origins.accepts(o.Sequence) {
		err = InvalidContract
	}
	n, _ := s.scope.Geometry.Bytes()
	if err == nil && len(pixels) != n {
		err = InvalidContract
	}
	if err != nil {
		s.mu.Unlock()
		return err
	}
	seen := make(map[DeliveryRef]bool, len(deliveries))
	seenStreams := make(map[string]bool, len(deliveries))
	for _, d := range deliveries {
		if err != nil {
			break
		}
		err = s.validateNewDeliveryLocked(d.Ref)
		if err == nil {
			err = d.Transform.Validate()
		}
		if err == nil && (d.Transform.Full != s.scope.Geometry || seen[d.Ref] || seenStreams[d.Ref.Stream] || s.bindings[d.Ref] != nil) {
			err = InvalidContract
		}
		if err != nil {
			break
		}
		seen[d.Ref] = true
		seenStreams[d.Ref.Stream] = true
	}
	if err != nil {
		s.mu.Unlock()
		return err
	}
	for len(s.frames) >= s.limits.MaxFrames {
		s.removeFrameLocked(s.frames[0])
	}
	b := s.allocateLocked(n, false)
	for b == nil && len(s.frames) > 0 {
		s.removeFrameLocked(s.frames[0])
		b = s.allocateLocked(n, false)
	}
	if b == nil {
		s.mu.Unlock()
		return CapacityRejected
	}
	gen, g := s.generation, s.scope.Geometry
	s.mu.Unlock()
	progress.phase, progress.copyStarted = "copy", progress.now()
	budget := CopyBudget{n, deadline, s.clock}
	err = budget.Check(ctx)
	if err == nil {
		err = s.copier.CopyNV12(ctx, budget, s.bytes(b), pixels, g, Rect{0, 0, g.Width, g.Height})
	}
	if err == nil {
		err = budget.Check(ctx)
	}
	progress.copyEnded = progress.now()
	s.mu.Lock()
	progress.commitAcquired = progress.now()
	if err == nil {
		progress.phase = "finalization"
	}
	defer s.mu.Unlock()
	now, tickErr := s.tickLocked()
	if err == nil {
		err = tickErr
	}
	if err == nil && gen != s.generation {
		err = s.generationErrorLocked(o)
	}
	if err == nil && now-observedNS >= s.limits.MaxAgeNS {
		err = Expired
	}
	// Metadata-only Bind/BindUnavailable is allowed while the provider copies.
	// Revalidate the complete set at commit: a delivery is write-once and may
	// not silently rebound after another operation already exposed it.
	if err == nil {
		for _, d := range deliveries {
			if err = s.validateNewDeliveryLocked(d.Ref); err != nil {
				break
			}
		}
	}
	if err != nil {
		s.releaseLocked(b)
		return normalizedCopyError(err)
	}
	f := &storedFrame{origin: o, geometry: g, pixels: b, at: observedNS}
	if expected != nil {
		f.pendingIntegrity, f.expectedDigest = true, *expected
	}
	s.frames = append(s.frames, f)
	s.origins.commit(o.Sequence)
	for _, d := range deliveries {
		s.insertBindingLocked(&binding{ref: d.Ref, origin: o, transform: d.Transform, at: now})
	}
	return nil
}
func normalizedCopyError(err error) error {
	var code Code
	if errors.As(err, &code) {
		return code
	}
	if errors.Is(err, context.Canceled) {
		return Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return DeadlineExceeded
	}
	return ProviderUnavailable
}
func (s *Store) generationErrorLocked(o OriginRef) error {
	if o.MediaEpoch != s.scope.MediaEpoch {
		return EpochMismatch
	}
	if o.Graph != s.scope.Graph {
		return GraphMismatch
	}
	return Expired
}

// Bind records the exact origin attested by the authenticated capture provider,
// independently from optional native pixel retention. It is not a public claim
// accepted from a frame requester. Missing pixels make Extract return Expired;
// they do not invalidate a real analysis output or erase its correlation.
// A late surface can satisfy only this exact origin, never the nearest frame.
func (s *Store) Bind(o OriginRef, d DeliveryBinding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now, err := s.tickLocked()
	if err != nil {
		return err
	}
	if err = s.validateOriginLocked(o); err != nil {
		return err
	}
	if err = s.validateNewDeliveryLocked(d.Ref); err != nil {
		return err
	}
	if err = d.Transform.Validate(); err != nil {
		return err
	}
	if d.Transform.Full != s.scope.Geometry || s.bindings[d.Ref] != nil {
		return InvalidContract
	}
	s.insertBindingLocked(&binding{ref: d.Ref, origin: o, transform: d.Transform, at: now})
	return nil
}

// BindUnavailable records a real delivery without fabricating any origin.
func (s *Store) BindUnavailable(ref DeliveryRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now, err := s.tickLocked()
	if err != nil {
		return err
	}
	if err = s.validateNewDeliveryLocked(ref); err != nil {
		return err
	}
	if s.bindings[ref] != nil {
		return InvalidContract
	}
	s.insertBindingLocked(&binding{ref: ref, at: now, unavailable: true})
	return nil
}
func (s *Store) Rebind(scope Scope, admission CopyAdmission) error {
	if err := scope.validate(); err != nil {
		return err
	}
	if err := s.limits.validate(scope); err != nil {
		return err
	}
	if err := checkAdmission(s.copier, admission, s.policy, scope.MediaEpoch, scope.Graph); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if scope.Source != s.scope.Source {
		return SourceMismatch
	}
	if scope.MediaEpoch < s.scope.MediaEpoch {
		return EpochMismatch
	}
	if scope.MediaEpoch == s.scope.MediaEpoch && scope.Graph == s.scope.Graph {
		return InvalidContract
	}
	// A process-owned allocator must monotonically advance changed epochs.
	// Retired stream watermarks remain bounded by the source's 16-name catalog;
	// detach/reattach must not recreate a delivery namespace from zero.
	watermarks := make(map[string]deliveryWatermark, len(s.deliveryWatermarks))
	for stream, mark := range s.deliveryWatermarks {
		watermarks[stream] = mark
	}
	for stream, epoch := range scope.Streams {
		mark, existed := watermarks[stream]
		if epoch < mark.epoch {
			return EpochMismatch
		}
		if !existed && len(watermarks) >= 16 {
			return CapacityRejected
		}
		if epoch > mark.epoch {
			watermarks[stream] = deliveryWatermark{epoch: epoch}
		}
	}
	if scope.Graph == s.scope.Graph && (scope.Geometry != s.scope.Geometry || scope.Surface != s.scope.Surface || scope.DecodedWidth != s.scope.DecodedWidth || scope.DecodedHeight != s.scope.DecodedHeight || scope.SurfaceRotation != s.scope.SurfaceRotation) {
		return GraphMismatch
	}
	s.clearLocked()
	if scope.MediaEpoch != s.scope.MediaEpoch {
		s.origins = sequenceWindow{}
	}
	s.scope = cloneScope(scope)
	s.deliveryWatermarks = watermarks
	s.admission = admission
	s.generation++
	s.lastNow = s.clock.NowNS()
	s.poisoned = false
	return nil
}

// Resolved is issuer-bound and cannot be manufactured by RPC arguments. A
// comparison rechecks both current stores, so an old resolution cannot survive
// metadata eviction, source restart or graph rebinding. Pixel availability is
// separate: knowing that two outputs share a capture does not promise a crop.
type Resolved struct {
	s *Store
	b *binding
}

func (s *Store) Resolve(ref DeliveryRef) (Resolved, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.tickLocked(); err != nil {
		return Resolved{}, err
	}
	if err := s.validateDeliveryLocked(ref); err != nil {
		return Resolved{}, err
	}
	b := s.bindings[ref]
	if b == nil {
		return Resolved{}, Expired
	}
	return Resolved{s, b}, nil
}
func Correlate(a, b Resolved) Code {
	if a.s == nil || a.b == nil || b.s == nil || b.b == nil {
		return InvalidContract
	}
	first, second := a.s, b.s
	if first.id > second.id {
		first, second = second, first
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	if second != first {
		second.mu.Lock()
		defer second.mu.Unlock()
	}
	for _, r := range []Resolved{a, b} {
		if _, err := r.s.tickLocked(); err != nil {
			return ProviderUnavailable
		}
		if r.s.bindings[r.b.ref] != r.b {
			return Expired
		}
	}
	if a.s.scope.Source != b.s.scope.Source {
		return DifferentSource
	}
	if a.s != b.s {
		return CorrelationUnavailable
	}
	if a.b.unavailable || b.b.unavailable {
		return CorrelationUnavailable
	}
	if a.b.origin == b.b.origin {
		return ExactSameOrigin
	}
	return DifferentOrigin
}

type Request struct {
	ID         [16]byte
	Frame      DeliveryRef
	Source     string
	Surface    string
	Regions    []Region
	DeadlineNS uint64
}
type Artifact struct {
	Region string
	State  Code
	Proof  Proof
}
type resultItem struct {
	artifact Artifact
	pixels   *block
}

// Result retains only output arena allocations, never source/camera ownership.
// Close is mandatory. A stalled holder consumes bounded output quota; it cannot
// grow the arena or prevent frame eviction. Visit's payload is read-only borrowed
// memory and MUST NOT escape the callback (trusted in-process transport adapter).
type Result struct {
	mu     sync.Mutex
	owner  *Store
	items  []resultItem
	closed bool
	bytes  int
}

func (r *Result) Artifacts() []Artifact {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	out := make([]Artifact, len(r.items))
	for i, item := range r.items {
		out[i] = item.artifact
	}
	return out
}

// Describe exposes immutable per-region outcomes, including those without any
// pixels. It neither borrows payloads nor changes Visit's borrowing contract.
func (r *Result) Describe(index int) (Artifact, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || index < 0 || index >= len(r.items) {
		return Artifact{}, InvalidContract
	}
	return r.items[index].artifact, nil
}

func (r *Result) Visit(index int, visit func(Artifact, []byte) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || index < 0 || index >= len(r.items) || visit == nil {
		return InvalidContract
	}
	item := r.items[index]
	if item.pixels == nil {
		return item.artifact.State
	}
	return visit(item.artifact, r.owner.bytes(item.pixels))
}
func (r *Result) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	r.owner.mu.Lock()
	defer r.owner.mu.Unlock()
	for _, item := range r.items {
		if item.pixels != nil {
			r.owner.releaseLocked(item.pixels)
		}
	}
	r.owner.artifactBytes -= r.bytes
	r.owner.openResults--
	r.items = nil
}

func (s *Store) Extract(ctx context.Context, q Request) (*Result, error) {
	if ctx == nil || q.ID == ([16]byte{}) || !token(q.Source) || len(q.Regions) < 1 || len(q.Regions) > s.limits.MaxRegions {
		return nil, InvalidContract
	}
	if ctx.Err() != nil {
		return nil, Canceled
	}
	if !s.copyBusy.CompareAndSwap(false, true) {
		return nil, CapacityRejected
	}
	defer s.copyBusy.Store(false)
	s.mu.Lock()
	now, err := s.tickLocked()
	if err == nil {
		err = s.deadlineLocked(now, q.DeadlineNS)
	}
	if err == nil {
		err = s.validateDeliveryLocked(q.Frame)
	}
	if err == nil && q.Source != s.scope.Source {
		err = SourceMismatch
	}
	if err == nil && q.Surface != s.scope.Surface {
		err = InvalidContract
	}
	var binding *binding
	if err == nil {
		binding = s.bindings[q.Frame]
	}
	if err == nil && binding == nil {
		err = Expired
	}
	if err == nil && binding.unavailable {
		err = CorrelationUnavailable
	}
	var f *storedFrame
	if err == nil {
		f = s.frameLocked(binding.origin)
		if f == nil {
			err = Expired
		}
	}
	if err == nil && s.openResults >= s.limits.MaxResults {
		err = CapacityRejected
	}
	if err == nil && f.integrityFailed {
		err = InvalidContract
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	items := make([]resultItem, len(q.Regions))
	boxes := make([]Rect, len(q.Regions))
	sizes := make([]int, len(q.Regions))
	seen := make(map[string]bool, len(q.Regions))
	total := 0
	for i, region := range q.Regions {
		if !token(region.ID) {
			err = InvalidContract
			break
		}
		if seen[region.ID] {
			err = InvalidContract
			break
		}
		seen[region.ID] = true
		items[i].artifact = Artifact{Region: region.ID, State: RegionInvalid}
		box, mapErr := MapRegion(binding.transform, region)
		if mapErr != nil {
			continue
		}
		g := Geometry{box.X2 - box.X1, box.Y2 - box.Y1, box.X2 - box.X1}
		n, _ := g.Bytes()
		if region.FullFrame && n > s.limits.MaxFullFrameBytes {
			err = CapacityRejected
			break
		}
		if n > s.limits.MaxOutputPerRequest-total {
			err = CapacityRejected
			break
		}
		total += n
		boxes[i] = box
		sizes[i] = n
		items[i].artifact.State = Complete
	}
	if err == nil && total > s.limits.MaxArtifactBytes-s.artifactBytes {
		err = CapacityRejected
	}
	if err == nil {
		for i, n := range sizes {
			if n == 0 {
				continue
			}
			items[i].pixels = s.allocateLocked(n, true)
			if items[i].pixels == nil {
				err = CapacityRejected
				break
			}
		}
	}
	if err != nil {
		for _, item := range items {
			if item.pixels != nil {
				s.releaseLocked(item.pixels)
			}
		}
		s.mu.Unlock()
		return nil, err
	}
	f.pixels.refs++
	gen := s.generation
	copyAdmission := s.admission
	s.artifactBytes += total
	s.openResults++
	s.mu.Unlock()
	budget := CopyBudget{total, q.DeadlineNS, s.clock}
	err = budget.Check(ctx)
	// copyBusy serializes all digest/copy calls and their admitted workspace.
	// The input block is pinned across cancellation/rebind and never writable
	// by the producer. Hash the ENTIRE source, including pixels outside the ROI;
	// a checksum of the crop alone cannot validate the producer's source SHA.
	checked, verified, rejected := false, false, false
	if err == nil && total != 0 && f.pendingIntegrity {
		actual := Digest(s.integrity.Sum256(s.bytes(f.pixels)))
		checked, verified = true, actual == f.expectedDigest
		err = budget.Check(ctx)
		if actual != f.expectedDigest {
			rejected = true
			err = InvalidContract
		}
	}
	for i, item := range items {
		if err != nil {
			break
		}
		if item.pixels == nil {
			continue
		}
		budget.MaxBytes = sizes[i]
		err = s.copier.CopyNV12(ctx, budget, s.bytes(item.pixels), s.bytes(f.pixels), f.geometry, boxes[i])
		if err == nil {
			err = budget.Check(ctx)
		}
		if err != nil {
			break
		}
		box := boxes[i]
		p := Proof{Version: 1, RequestID: q.ID, Delivery: q.Frame, Origin: f.origin, Surface: q.Surface, Transform: binding.transform.Digest(), Region: q.Regions[i].ID, BBox: box, Geometry: Geometry{box.X2 - box.X1, box.Y2 - box.Y1, box.X2 - box.X1}, PayloadSHA256: Digest(s.integrity.Sum256(s.bytes(item.pixels))), CopyProvider: copyAdmission.Provider, CopyEvidence: copyAdmission.Evidence}
		p.Digest = proofDigest(p)
		items[i].artifact.Proof = p
	}
	if err == nil {
		err = budget.Check(ctx)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, tickErr := s.tickLocked()
	if err == nil {
		err = tickErr
	}
	if err == nil && gen != s.generation {
		err = s.generationErrorLocked(f.origin)
	}
	if err == nil && s.bindings[q.Frame] != binding {
		err = Expired
	}
	if err == nil && s.frameLocked(f.origin) != f {
		err = Expired // Retained input was evicted during the bounded copy.
	}
	if checked {
		s.integrityChecks++
		if rejected {
			s.integrityFailures++
			f.integrityFailed = true
		}
	}
	// Cancellation/deadline invalidates this request, not a completed digest
	// of immutable bytes. Cache only if the exact frame is still current.
	if verified && gen == s.generation && s.frameLocked(f.origin) == f {
		f.pendingIntegrity = false
	}
	s.releaseLocked(f.pixels)
	if err != nil {
		for _, item := range items {
			if item.pixels != nil {
				s.releaseLocked(item.pixels)
			}
		}
		s.artifactBytes -= total
		s.openResults--
		return nil, normalizedCopyError(err)
	}
	return &Result{owner: s, items: items, bytes: total}, nil
}

type Stats struct {
	SurfaceIntegrityChecks, SurfaceIntegrityFailures                                                            uint64
	PendingIntegrityFrames, RejectedIntegrityFrames                                                             int
	IntegrityWorkspaceBytes                                                                                     int
	PoolBytes, UsedBytes, PeakUsedBytes, FrameBytes, ArtifactBytes, WorkingBytes, Frames, Bindings, OpenResults int
	CopyBusy, Poisoned                                                                                          bool
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.tickLocked()
	st := Stats{PoolBytes: len(s.arena), IntegrityWorkspaceBytes: s.integrity.WorkspaceBytes, UsedBytes: s.usedLocked(), PeakUsedBytes: s.peakBytes, ArtifactBytes: s.artifactBytes, Frames: len(s.frames), Bindings: len(s.bindings), OpenResults: s.openResults, CopyBusy: s.copyBusy.Load(), Poisoned: s.poisoned}
	for _, f := range s.frames {
		st.FrameBytes += f.pixels.size
		if f.integrityFailed {
			st.RejectedIntegrityFrames++
		} else if f.pendingIntegrity {
			st.PendingIntegrityFrames++
		}
	}
	st.SurfaceIntegrityChecks, st.SurfaceIntegrityFailures = s.integrityChecks, s.integrityFailures
	st.WorkingBytes = st.UsedBytes - st.FrameBytes - st.ArtifactBytes
	return st
}
