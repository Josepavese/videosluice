package framedomain_test

import (
	"context"
	"errors"
	"fmt"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	"github.com/Josepavese/videosluice/framedomain/entropy"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type authority struct{ value atomic.Value }

func (a *authority) CurrentGeneration() (string, error) {
	v := a.value.Load().(string)
	if v == "unavailable" {
		return "", errors.New("not attested")
	}
	return v, nil
}
func auth() *authority { a := &authority{}; a.value.Store("authenticated-process-1"); return a }

type testEntropy struct{ fail, zero bool }

func (e testEntropy) Fill(b []byte) error {
	if e.fail {
		return errors.New("entropy unavailable")
	}
	if !e.zero {
		for i := range b {
			b[i] = byte(i + 1)
		}
	}
	return nil
}

type clock struct{}

func (clock) NowNS() uint64 { return 100 }
func hash(x byte) fa.Digest { var d fa.Digest; d[0] = x; return d }

type admission struct {
	fail    bool
	entered chan struct{}
	release chan struct{}
}

func (a admission) Attest(ctx context.Context, s fa.Scope) (fa.CopyAdmission, error) {
	if a.entered != nil {
		a.entered <- struct{}{}
		<-a.release
	}
	if a.fail {
		return fa.CopyAdmission{}, errors.New("real provider did not attest")
	}
	return fa.CopyAdmission{Provider: "cpu_nv12_copy", MediaEpoch: s.MediaEpoch, Graph: s.Graph, Evidence: hash(9), Attested: true}, nil
}
func runtimeLimits() fd.Limits {
	return fd.Limits{MaxSources: 4, MaxStreams: 8, MaxSourceGenerations: 4, MaxMemoryBytes: 16384, MaxSourceBytes: 16384, MaxBuffers: 16, MaxBufferBytes: 4096, MaxResources: 16, MaxResourceOperations: 4}
}
func coreLimits() fa.Limits {
	return fa.Limits{MaxFrames: 2, MaxBindings: 8, PoolBytes: 4096, MaxFrameBytes: 96, MaxArtifactBytes: 2048, MaxOutputPerRequest: 512, MaxFullFrameBytes: 96, MaxRegions: 4, MaxResults: 4, MaxAgeNS: 1000, MaxCopyNS: 100}
}
func layout(source, stream string) fd.Layout {
	return fd.Layout{Source: source, Graph: hash(1), Streams: []string{stream}, Surface: "processed_full", Geometry: fa.Geometry{Width: 8, Height: 8, Stride: 8}, DecodedWidth: 8, DecodedHeight: 8}
}
func fixture(t testing.TB) (*fd.Runtime, *authority) {
	t.Helper()
	a := auth()
	r, err := fd.New(a, testEntropy{}, clock{}, runtimeLimits())
	if err != nil {
		t.Fatal(err)
	}
	return r, a
}
func open(t testing.TB, r *fd.Runtime, source, stream string, copier fa.Copier) *fd.Binding {
	t.Helper()
	b, err := r.Open(context.Background(), layout(source, stream), coreLimits(), copier, admission{}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func transform(b *fd.Binding) fa.Transform {
	g := b.Scope().Geometry
	return fa.Transform{Full: g, Crop: fa.Rect{X2: 8, Y2: 8}, ResizedWidth: 8, ResizedHeight: 8, DeliveryWidth: 8, DeliveryHeight: 8}
}
func observed(b *fd.Binding, seq uint64) fd.OriginRef {
	s := b.Scope()
	return fd.OriginRef{Fence: b.Fence(), Ref: fa.OriginRef{Source: s.Source, MediaEpoch: s.MediaEpoch, Sequence: seq, Graph: s.Graph}}
}
func ticket(t testing.TB, b *fd.Binding, stream string) fd.Ticket {
	t.Helper()
	tk, err := b.AllocateDelivery(stream)
	if err != nil {
		t.Fatal(err)
	}
	return tk
}
func publish(t testing.TB, b *fd.Binding, stream string, seq uint64) fd.Ticket {
	t.Helper()
	tk := ticket(t, b, stream)
	pixels := make([]byte, 96)
	for i := range pixels {
		pixels[i] = byte(seq)
	}
	if err := b.Publish(context.Background(), observed(b, seq), []fd.Publication{{Ticket: tk, Transform: transform(b)}}, pixels, 100, 200); err != nil {
		t.Fatal(err)
	}
	return tk
}
func query(b *fd.Binding, tk fd.Ticket) fd.Request {
	return fd.Request{ID: [16]byte{1}, Frame: tk.Ref(), Source: b.Scope().Source, Surface: b.Scope().Surface, Regions: []fa.Region{{ID: "full", FullFrame: true}}, DeadlineNS: 200}
}
func code(t testing.TB, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("err=%v want=%v", err, want)
	}
}
func wait(t testing.TB, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("operation stalled")
	}
}

func TestNamespaceRequiresEntropyAndCanonicalAuthority(t *testing.T) {
	a := auth()
	for _, e := range []testEntropy{{fail: true}, {zero: true}} {
		_, err := fd.New(a, e, clock{}, runtimeLimits())
		code(t, err, fd.ErrInvalid)
	}
	a.value.Store("pid only invalid generation")
	_, err := fd.New(a, testEntropy{}, clock{}, runtimeLimits())
	code(t, err, fd.ErrProcessChanged)
	a.value.Store("unavailable")
	_, err = fd.New(a, testEntropy{}, clock{}, runtimeLimits())
	code(t, err, fd.ErrProcessChanged)
	a.value.Store("authenticated-process-1")
	r1, err := fd.New(a, testEntropy{}, clock{}, runtimeLimits())
	if err != nil {
		t.Fatal(err)
	}
	r2, err := fd.New(a, testEntropy{}, clock{}, runtimeLimits())
	if err != nil {
		t.Fatal(err)
	}
	if r1.Fence() == r2.Fence() {
		t.Fatal("repeated entropy reused process-local runtime domain")
	}
	real, err := fd.New(a, entropy.System{}, clock{}, runtimeLimits())
	if err != nil || real.Fence().Namespace == ([16]byte{}) {
		t.Fatalf("system CSPRNG failed %v", err)
	}
}

func TestRecreatedCoreNeverReusesSourceOrPublicationIdentity(t *testing.T) {
	r, _ := fixture(t)
	b1 := open(t, r, "front", "analysis", copyport.CPU{})
	tk1 := publish(t, b1, "analysis", 1)
	oldQuery := query(b1, tk1)
	s1 := b1.Scope()
	b1.Close()
	b1.Close()
	b2 := open(t, r, "front", "analysis", copyport.CPU{})
	s2 := b2.Scope()
	if s2.MediaEpoch <= s1.MediaEpoch || s2.Streams["analysis"] <= s1.Streams["analysis"] {
		t.Fatal("NewStore reset identity allocator")
	}
	tk2 := publish(t, b2, "analysis", 1)
	if tk1.Ref() == tk2.Ref() {
		t.Fatal("delivery tuple reused")
	}
	_, err := r.Extract(context.Background(), oldQuery)
	code(t, err, fa.EpochMismatch)
	err = b2.Publish(context.Background(), observed(b1, 2), []fd.Publication{{Ticket: tk2, Transform: transform(b2)}}, make([]byte, 96), 100, 200)
	code(t, err, fd.ErrRetired)
	_, err = b1.AllocateDelivery("analysis")
	code(t, err, fd.ErrRetired)
	code(t, b2.BindUnavailable(context.Background(), tk1), fd.ErrDomainMismatch)
}

func TestRestartDomainRejectsStaleRefsEvenWithMatchingCoreCounters(t *testing.T) {
	r1, _ := fixture(t)
	b1 := open(t, r1, "front", "analysis", copyport.CPU{})
	old := publish(t, b1, "analysis", 1)
	q := query(b1, old)
	a2 := auth()
	a2.value.Store("authenticated-process-2")
	r2, err := fd.New(a2, testEntropy{}, clock{}, runtimeLimits())
	if err != nil {
		t.Fatal(err)
	}
	b2 := open(t, r2, "front", "analysis", copyport.CPU{})
	fresh := publish(t, b2, "analysis", 1)
	if old.Ref().Ref != fresh.Ref().Ref {
		t.Fatal("test must overlap all inner core counters")
	}
	_, err = r2.Extract(context.Background(), q)
	code(t, err, fd.ErrDomainMismatch)
	q.Frame.Fence = r2.Fence()
	q.Frame.Fence.ProcessGeneration = r1.Fence().ProcessGeneration
	_, err = r2.Extract(context.Background(), q)
	code(t, err, fd.ErrDomainMismatch)
}

func TestDomainProofBindsFenceAndInnerIntegrity(t *testing.T) {
	r, _ := fixture(t)
	b := open(t, r, "front", "analysis", copyport.CPU{})
	tk := publish(t, b, "analysis", 1)
	q := query(b, tk)
	result, err := r.Extract(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	err = result.Visit(0, func(a fd.Artifact, p []byte) error {
		if err := fd.VerifyDomainProof(a.Proof, r.Fence()); err != nil {
			t.Fatal(err)
		}
		s := b.Scope()
		adm, _ := admission{}.Attest(context.Background(), s)
		if err := fa.VerifyProof(a.Proof.Core, p, q.ID, q.Frame.Ref, observed(b, 1).Ref, transform(b), q.Regions[0], q.Surface, adm, 512); err != nil {
			t.Fatal(err)
		}
		bad := a.Proof
		bad.Fence.Namespace[0] ^= 1
		code(t, fd.VerifyDomainProof(bad, r.Fence()), fd.ErrDomainMismatch)
		bad = a.Proof
		bad.Core.Digest[0] ^= 1
		code(t, fd.VerifyDomainProof(bad, r.Fence()), fd.ErrInvalid)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTicketsSourcesAndUnknownOriginsCannotBeForged(t *testing.T) {
	r, _ := fixture(t)
	a := open(t, r, "front", "analysis", copyport.CPU{})
	b := open(t, r, "overview", "analysis-overview", copyport.CPU{})
	tk := publish(t, a, "analysis", 1)
	code(t, b.BindUnavailable(context.Background(), tk), fd.ErrDomainMismatch)
	code(t, a.BindUnavailable(context.Background(), fd.Ticket{}), fd.ErrDomainMismatch)
	_, err := a.AllocateDelivery("analysis-overview")
	code(t, err, fa.SourceMismatch)
	q := query(a, tk)
	q.Source = "overview"
	_, err = r.Extract(context.Background(), q)
	code(t, err, fa.SourceMismatch)
	u := ticket(t, a, "analysis")
	if err = a.BindUnavailable(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	_, err = r.Extract(context.Background(), query(a, u))
	code(t, err, fa.CorrelationUnavailable)
	later := ticket(t, a, "analysis")
	if err = a.BindExact(context.Background(), observed(a, 1), later, transform(a)); err != nil {
		t.Fatal(err)
	}
	result, err := r.Extract(context.Background(), query(a, later))
	if err != nil {
		t.Fatal(err)
	}
	result.Close()
}

func TestRetiredStoreArenaRemainsBudgetedUntilResultsClose(t *testing.T) {
	a := auth()
	l := runtimeLimits()
	l.MaxMemoryBytes = 4096
	l.MaxSourceBytes = 4096
	r, err := fd.New(a, testEntropy{}, clock{}, l)
	if err != nil {
		t.Fatal(err)
	}
	b := open(t, r, "front", "analysis", copyport.CPU{})
	tk := publish(t, b, "analysis", 1)
	result, err := r.Extract(context.Background(), query(b, tk))
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	stats := r.Stats()
	if stats.ReservedStoreBytes != 4096 || stats.LiveGenerations != 1 || stats.ActiveSources != 0 {
		t.Fatalf("unbudgeted retired arena %+v", stats)
	}
	_, err = r.Open(context.Background(), layout("front", "analysis"), coreLimits(), copyport.CPU{}, admission{}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	code(t, err, fa.CapacityRejected)
	if err = result.Visit(0, func(_ fd.Artifact, p []byte) error {
		if p[0] != 1 {
			t.Fatal("retired result changed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result.Close()
	result.Close()
	if stats = r.Stats(); stats.ReservedStoreBytes != 0 || stats.LiveGenerations != 0 {
		t.Fatal(stats)
	}
	open(t, r, "front", "analysis", copyport.CPU{})
}

type blockCopy struct {
	active           atomic.Bool
	entered, release chan struct{}
}

func (b *blockCopy) Capability() fa.CopyCapability { return copyport.CPU{}.Capability() }
func (b *blockCopy) CopyNV12(ctx context.Context, q fa.CopyBudget, dst, src []byte, g fa.Geometry, box fa.Rect) error {
	if b.active.Swap(false) {
		b.entered <- struct{}{}
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return copyport.CPU{}.CopyNV12(ctx, q, dst, src, g, box)
}
func TestAuthorityChangeDuringCopyRevokesBeforePublishingResult(t *testing.T) {
	r, a := fixture(t)
	copier := &blockCopy{entered: make(chan struct{}, 1), release: make(chan struct{})}
	b := open(t, r, "front", "analysis", copier)
	tk := publish(t, b, "analysis", 1)
	copier.active.Store(true)
	done := make(chan error, 1)
	go func() {
		result, err := r.Extract(context.Background(), query(b, tk))
		if result != nil {
			result.Close()
			done <- errors.New("stale result returned")
			return
		}
		done <- err
	}()
	wait(t, copier.entered)
	a.value.Store("authenticated-process-2")
	_, err := b.AllocateDelivery("analysis")
	code(t, err, fd.ErrProcessChanged)
	close(copier.release)
	if err = <-done; err == nil {
		t.Fatal("accepted revoked result")
	}
	stats := r.Stats()
	if !stats.Revoked || stats.ReservedStoreBytes != 0 || stats.LiveGenerations != 0 {
		t.Fatal(stats)
	}
}

func TestAdmissionFailureConsumesCountersAndPendingArenaIsBounded(t *testing.T) {
	r, _ := fixture(t)
	_, err := r.Open(context.Background(), layout("front", "analysis"), coreLimits(), copyport.CPU{}, admission{fail: true}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	if err == nil {
		t.Fatal("missing actual admission accepted")
	}
	if r.Stats().ReservedStoreBytes != 0 {
		t.Fatal("failed admission leaked arena")
	}
	b := open(t, r, "front", "analysis", copyport.CPU{})
	if b.Scope().MediaEpoch != 2 || b.Scope().Streams["analysis"] != 2 {
		t.Fatal("failed admission reused reserved identifiers")
	}
	b.Close()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := r.Open(context.Background(), layout("front", "analysis"), coreLimits(), copyport.CPU{}, admission{entered: entered, release: release}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
		done <- err
	}()
	wait(t, entered)
	r.Revoke()
	if r.Stats().ReservedStoreBytes != 4096 {
		t.Fatal("pending admission reservation was released before it returned")
	}
	close(release)
	if <-done == nil {
		t.Fatal("revoked opening was accepted")
	}
	if r.Stats().ReservedStoreBytes != 0 {
		t.Fatal("pending reservation leaked")
	}
}

func TestBoundedRegistryAndConcurrentAllocator(t *testing.T) {
	r, _ := fixture(t)
	b := open(t, r, "front", "analysis", copyport.CPU{})
	ids := make(chan uint64, 400)
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				tk, err := b.AllocateDelivery("analysis")
				if err != nil {
					errs <- err
					return
				}
				ids <- tk.Ref().Ref.FrameID
			}
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := map[uint64]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatal("duplicate delivery ID", id)
		}
		seen[id] = true
	}
	if len(seen) != 400 {
		t.Fatal(len(seen))
	}
	b.Close()
	for i := 0; i < 3; i++ {
		source := fmt.Sprintf("source%d", i)
		binding := open(t, r, source, source, copyport.CPU{})
		binding.Close()
	}
	_, err := r.Open(context.Background(), layout("overflow", "overflow"), coreLimits(), copyport.CPU{}, admission{}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	code(t, err, fa.CapacityRejected)
}
