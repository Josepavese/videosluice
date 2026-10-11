package ingress_test

import (
	"bytes"
	"context"
	in "github.com/Josepavese/videosluice/frameingress"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	peer "github.com/Josepavese/videosluice/framepeer"
	"sync/atomic"
	"testing"
	"time"
)

func accessFixture(t *testing.T) (*fixture, *in.Access, *authority, fd.Request) {
	t.Helper()
	f := setup(t)
	consumer := &authority{}
	consumer.generation.Store("consumer-start-1")
	tk := f.ticket(t)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
	policy := in.AccessPolicy{Generation: "consumer-start-1", Streams: []string{"analysis"}, MaxRegions: 1, MaxRequests: 1, MaxResults: 1}
	a, err := in.NewAccess(f.r, f.b, consumer, policy)
	must(t, err)
	t.Cleanup(a.Revoke)
	policy.Streams[0] = "forged-after-admission"
	q := fd.Request{ID: [16]byte{1}, Frame: tk.Ref(), Source: "front", Surface: "processed_full", Regions: []fa.Region{{ID: "roi", BBox: [4]float64{1, 1, 3, 3}}}, DeadlineNS: 200}
	return f, a, consumer, q
}

type accessBlockingCopy struct {
	active  atomic.Bool
	entered chan struct{}
}

func (b *accessBlockingCopy) Capability() fa.CopyCapability { return copyport.CPU{}.Capability() }
func (b *accessBlockingCopy) CopyNV12(ctx context.Context, budget fa.CopyBudget, dst, src []byte, g fa.Geometry, box fa.Rect) error {
	if b.active.Swap(false) {
		close(b.entered)
		<-ctx.Done()
		return ctx.Err()
	}
	return copyport.CPU{}.CopyNV12(ctx, budget, dst, src, g, box)
}
func TestAccessRevocationCancelsRealInFlightCopyAndReleasesAdmission(t *testing.T) {
	copier := &accessBlockingCopy{entered: make(chan struct{})}
	f := setupCopier(t, copier)
	tk := f.ticket(t)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
	consumer := &authority{}
	consumer.generation.Store("consumer-start-1")
	a, err := in.NewAccess(f.r, f.b, consumer, in.AccessPolicy{Generation: "consumer-start-1", Streams: []string{"analysis"}, MaxRegions: 1, MaxRequests: 1, MaxResults: 2})
	must(t, err)
	defer a.Revoke()
	q := fd.Request{ID: [16]byte{1}, Frame: tk.Ref(), Source: "front", Surface: "processed_full", Regions: []fa.Region{{ID: "roi", BBox: [4]float64{1, 1, 3, 3}}}, DeadlineNS: 200}
	copier.active.Store(true)
	done := make(chan error, 1)
	go func() {
		r, e := a.Extract(context.Background(), principal(), q)
		if r != nil {
			r.Close()
		}
		done <- e
	}()
	select {
	case <-copier.entered:
	case <-time.After(time.Second):
		a.Revoke()
		t.Fatal("copy not entered")
	}
	_, err = a.Extract(context.Background(), principal(), q)
	want(t, err, fa.CapacityRejected)
	a.Revoke()
	select {
	case err = <-done:
		want(t, err, peer.ErrRetired)
	case <-time.After(time.Second):
		t.Fatal("copy did not cancel")
	}
	r, err := f.r.Extract(context.Background(), q)
	must(t, err)
	r.Close()
}
func principal() peer.Principal { return peer.Principal{Generation: "consumer-start-1"} }

func TestAccessExtractsRealNativeROIAndEnforcesConsumerResultQuota(t *testing.T) {
	f, a, _, q := accessFixture(t)
	r, err := a.Extract(context.Background(), principal(), q)
	must(t, err)
	defer r.Close()
	must(t, r.Visit(0, func(artifact fd.Artifact, pixels []byte) error {
		if len(pixels) != 24 || artifact.Proof.Core.Geometry != (fa.Geometry{Width: 4, Height: 4, Stride: 4}) {
			t.Fatal("not native ROI", len(pixels))
		}
		return fd.VerifyDomainProof(artifact.Proof, f.b.Fence())
	}))
	_, err = a.Extract(context.Background(), principal(), q)
	want(t, err, fa.CapacityRejected)
	r.Close()
	r.Close()
	r, err = a.Extract(context.Background(), principal(), q)
	must(t, err)
	r.Close()
}

func TestAccessDeniesForeignPrincipalScopeAndFullFrameBeforeAllocation(t *testing.T) {
	f, a, _, q := accessFixture(t)
	for _, change := range []func(*fd.Request){
		func(q *fd.Request) { q.Source = "rear" }, func(q *fd.Request) { q.Surface = "source_decoded" },
		func(q *fd.Request) { q.Frame.Fence.Namespace[0] ^= 1 }, func(q *fd.Request) { q.Frame.Ref.Epoch++ },
		func(q *fd.Request) { q.Frame.Ref.Graph[0] ^= 1 }, func(q *fd.Request) { q.Frame.Ref.Stream = "live" },
		func(q *fd.Request) { q.Regions = []fa.Region{{ID: "full", FullFrame: true}} },
		func(q *fd.Request) { q.Regions = append(q.Regions, q.Regions[0]) },
	} {
		bad := q
		change(&bad)
		result, err := a.Extract(context.Background(), principal(), bad)
		want(t, err, peer.ErrUnauthorized)
		if result != nil {
			t.Fatal("unauthorized result")
		}
	}
	_, err := a.Extract(context.Background(), peer.Principal{Generation: "different-consumer"}, q)
	want(t, err, peer.ErrUnauthorized)
	// Repeated rejected requests must not consume the sole result slot.
	r, err := a.Extract(context.Background(), principal(), q)
	must(t, err)
	r.Close()
	if f.r.Stats().LiveGenerations != 1 {
		t.Fatal(f.r.Stats())
	}
}

func TestOldGrantDoesNotFollowSwitchButExtractedArtifactStaysExact(t *testing.T) {
	f, a, _, q := accessFixture(t)
	r, err := a.Extract(context.Background(), principal(), q)
	must(t, err)
	defer r.Close()
	var before []byte
	var proof fd.Proof
	must(t, r.Visit(0, func(v fd.Artifact, p []byte) error { proof = v.Proof; before = append([]byte(nil), p...); return nil }))
	f.b.Close()
	b, err := f.r.Open(context.Background(), f.layout, fa.Limits{MaxFrames: 2, MaxBindings: 8, PoolBytes: 8192, MaxFrameBytes: 384, MaxArtifactBytes: 4096, MaxOutputPerRequest: 1024, MaxFullFrameBytes: 384, MaxRegions: 4, MaxResults: 4, MaxAgeNS: 1000, MaxCopyNS: 100}, copyport.CPU{}, admission{}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	must(t, err)
	defer b.Close()
	tk, err := b.AllocateDelivery("analysis")
	must(t, err)
	newRequest := q
	newRequest.Frame = tk.Ref()
	_, err = a.Extract(context.Background(), principal(), newRequest)
	want(t, err, peer.ErrUnauthorized)
	must(t, r.Visit(0, func(v fd.Artifact, p []byte) error {
		if !bytes.Equal(before, p) || v.Proof != proof {
			t.Fatal("retained artifact changed identity/pixels")
		}
		return nil
	}))
	if f.r.Stats().LiveGenerations != 2 {
		t.Fatal("old arena released before result", f.r.Stats())
	}
	r.Close()
	if f.r.Stats().LiveGenerations != 1 {
		t.Fatal(f.r.Stats())
	}
}

func TestConsumerRevocationBlocksFutureDeliveryWithoutDeletingOwnedData(t *testing.T) {
	for _, mode := range []string{"explicit", "generation", "during-visit"} {
		t.Run(mode, func(t *testing.T) {
			_, a, consumer, q := accessFixture(t)
			r, err := a.Extract(context.Background(), principal(), q)
			must(t, err)
			defer r.Close()
			var owned []byte
			must(t, r.Visit(0, func(_ fd.Artifact, p []byte) error { owned = append([]byte(nil), p...); return nil }))
			if mode == "explicit" {
				a.Revoke()
			} else if mode == "generation" {
				consumer.generation.Store("consumer-start-2")
			}
			called := false
			err = r.Visit(0, func(fd.Artifact, []byte) error { called = true; a.Revoke(); return nil })
			want(t, err, peer.ErrRetired)
			if called != (mode == "during-visit") {
				t.Fatal("unexpected borrowed delivery")
			}
			_, err = a.Extract(context.Background(), principal(), q)
			want(t, err, peer.ErrRetired)
			if len(owned) != 24 {
				t.Fatal("consumer owned data altered")
			}
			consumer.generation.Store("consumer-start-1")
			_, err = a.Extract(context.Background(), principal(), q)
			want(t, err, peer.ErrRetired)
		})
	}
}

func TestExplicitFullFrameGrantAndInvalidPolicies(t *testing.T) {
	f, _, consumer, q := accessFixture(t)
	policy := in.AccessPolicy{Generation: "consumer-start-1", Streams: []string{"analysis"}, MaxRegions: 1, MaxRequests: 1, MaxResults: 1, AllowFullFrame: true}
	a, err := in.NewAccess(f.r, f.b, consumer, policy)
	must(t, err)
	defer a.Revoke()
	q.Regions = []fa.Region{{ID: "native-full", FullFrame: true}}
	r, err := a.Extract(context.Background(), principal(), q)
	must(t, err)
	defer r.Close()
	must(t, r.Visit(0, func(_ fd.Artifact, p []byte) error {
		if !bytes.Equal(p, pixels(1)) {
			t.Fatal("native full frame differs")
		}
		return nil
	}))
	for _, change := range []func(*in.AccessPolicy){func(p *in.AccessPolicy) { p.MaxRequests = 0 }, func(p *in.AccessPolicy) { p.MaxResults = 65 }, func(p *in.AccessPolicy) { p.MaxRegions = 0 }, func(p *in.AccessPolicy) { p.Streams = []string{"analysis", "analysis"} }, func(p *in.AccessPolicy) { p.Streams = []string{"live"} }} {
		bad := policy
		change(&bad)
		_, err = in.NewAccess(f.r, f.b, consumer, bad)
		want(t, err, peer.ErrInvalid)
	}
	_, err = a.Extract(nil, principal(), q)
	want(t, err, peer.ErrInvalid)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = a.Extract(ctx, principal(), q)
	want(t, err, context.Canceled)
}
