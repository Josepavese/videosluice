package frameaccess_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ n atomic.Uint64 }

func (c *fakeClock) NowNS() uint64 { return c.n.Load() }
func (c *fakeClock) set(n uint64)  { c.n.Store(n) }
func hash(x byte) fa.Digest        { var d fa.Digest; d[0] = x; return d }
func requestID(x byte) [16]byte    { var id [16]byte; id[0] = x; return id }
func scope(source string) fa.Scope {
	return fa.Scope{Source: source, MediaEpoch: 1, Graph: hash(1), Streams: map[string]uint64{"analysis": 11, "live": 12}, Surface: "processed_full", Geometry: fa.Geometry{Width: 8, Height: 8, Stride: 8}, DecodedWidth: 8, DecodedHeight: 8}
}
func limits() fa.Limits {
	return fa.Limits{MaxFrames: 2, MaxBindings: 8, PoolBytes: 4096, MaxFrameBytes: 256, MaxArtifactBytes: 2048, MaxOutputPerRequest: 512, MaxFullFrameBytes: 256, MaxRegions: 4, MaxResults: 4, MaxAgeNS: 1000, MaxCopyNS: 100}
}
func admission(s fa.Scope) fa.CopyAdmission {
	return fa.CopyAdmission{Provider: "cpu_nv12_copy", MediaEpoch: s.MediaEpoch, Graph: s.Graph, Evidence: hash(9), Attested: true}
}
func identity(g fa.Geometry) fa.Transform {
	return fa.Transform{Full: g, Crop: fa.Rect{X2: g.Width, Y2: g.Height}, ResizedWidth: g.Width, ResizedHeight: g.Height, DeliveryWidth: g.Width, DeliveryHeight: g.Height}
}
func fixture(t testing.TB, copier fa.Copier) (*fa.Store, *fakeClock, fa.Scope) {
	t.Helper()
	c := &fakeClock{}
	c.set(100)
	s := scope("front")
	st, err := fa.NewStore(s, limits(), c, copier, admission(s), fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	if err != nil {
		t.Fatal(err)
	}
	return st, c, s
}
func sourcePixels(g fa.Geometry, seed byte) []byte {
	n, _ := g.Bytes()
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i) + seed
	}
	return b
}
func origin(s fa.Scope, seq uint64) fa.OriginRef {
	return fa.OriginRef{Source: s.Source, MediaEpoch: s.MediaEpoch, Sequence: seq, Graph: s.Graph}
}
func delivery(s fa.Scope, stream string, id uint64) fa.DeliveryRef {
	return fa.DeliveryRef{Stream: stream, Epoch: s.Streams[stream], FrameID: id, Graph: s.Graph}
}
func publish(t testing.TB, st *fa.Store, c *fakeClock, s fa.Scope, seq uint64) fa.DeliveryRef {
	t.Helper()
	d := delivery(s, "analysis", seq)
	err := st.TryPublish(context.Background(), origin(s, seq), []fa.DeliveryBinding{{Ref: d, Transform: identity(s.Geometry)}}, sourcePixels(s.Geometry, byte(seq)), c.NowNS(), c.NowNS()+100)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func request(s fa.Scope, d fa.DeliveryRef, c *fakeClock) fa.Request {
	return fa.Request{ID: requestID(1), Frame: d, Source: s.Source, Surface: s.Surface, DeadlineNS: c.NowNS() + 100, Regions: []fa.Region{{ID: "roi", BBox: [4]float64{2, 2, 6, 6}}}}
}
func requireCode(t testing.TB, err error, want fa.Code) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error=%v want=%s", err, want)
	}
}
func resolve(t testing.TB, s *fa.Store, d fa.DeliveryRef) fa.Resolved {
	t.Helper()
	r, err := s.Resolve(d)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestGeometryAndScopeRejectInventedFullResolution(t *testing.T) {
	for _, g := range []fa.Geometry{{Width: 3, Height: 2, Stride: 4}, {Width: 8, Height: 7, Stride: 8}, {Width: 8, Height: 8, Stride: 6}, {Width: 8194, Height: 2, Stride: 8194}, {Width: 2, Height: 2, Stride: math.MaxInt}} {
		if _, err := g.Bytes(); err == nil {
			t.Errorf("accepted %+v", g)
		}
	}
	st, c, s := fixture(t, copyport.CPU{})
	_ = st
	for _, mutate := range []func(*fa.Scope){func(s *fa.Scope) { s.DecodedWidth = 4 }, func(s *fa.Scope) { s.Surface = "delivery:analysis" }, func(s *fa.Scope) { s.SurfaceRotation = 45 }, func(s *fa.Scope) { s.Streams = map[string]uint64{"analysis": 0} }} {
		candidate := s
		candidate.Streams = map[string]uint64{"analysis": 11}
		mutate(&candidate)
		_, err := fa.NewStore(candidate, limits(), c, copyport.CPU{}, admission(candidate), fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
		requireCode(t, err, fa.InvalidContract)
	}
}

func TestMapRegionKnownTransformsAndChroma(t *testing.T) {
	g := fa.Geometry{Width: 16, Height: 12, Stride: 20}
	base := identity(g)
	tests := []struct {
		name      string
		transform fa.Transform
		region    fa.Region
		want      fa.Rect
	}{
		{"identity-outward", base, fa.Region{ID: "r", BBox: [4]float64{1, 3, 7, 9}}, fa.Rect{X1: 0, Y1: 2, X2: 8, Y2: 10}},
		{"edges", base, fa.Region{ID: "r", BBox: [4]float64{-1, -1, 17, 13}}, fa.Rect{X2: 16, Y2: 12}},
		{"padding", base, fa.Region{ID: "r", BBox: [4]float64{4, 4, 8, 8}, PaddingRatio: .5}, fa.Rect{X1: 2, Y1: 2, X2: 10, Y2: 10}},
		{"full", base, fa.Region{ID: "r", FullFrame: true}, fa.Rect{X2: 16, Y2: 12}},
	}
	crop := base
	crop.Crop = fa.Rect{X1: 4, Y1: 2, X2: 12, Y2: 10}
	crop.ResizedWidth = 4
	crop.ResizedHeight = 4
	crop.DeliveryWidth = 8
	crop.DeliveryHeight = 8
	crop.PadLeft = 2
	crop.PadTop = 2
	tests = append(tests, struct {
		name      string
		transform fa.Transform
		region    fa.Region
		want      fa.Rect
	}{"crop-letterbox", crop, fa.Region{ID: "r", BBox: [4]float64{2, 2, 4, 4}}, fa.Rect{X1: 4, Y1: 2, X2: 8, Y2: 6}})
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := fa.MapRegion(tc.transform, tc.region)
			if err != nil || got != tc.want {
				t.Fatalf("got=%+v err=%v want=%+v", got, err, tc.want)
			}
		})
	}
	for _, rotation := range []int{0, 90, 180, 270} {
		for _, mirror := range []bool{false, true} {
			tr := base
			tr.Rotation = rotation
			tr.Mirror = mirror
			if rotation == 90 || rotation == 270 {
				tr.ResizedWidth = 12
				tr.ResizedHeight = 16
				tr.DeliveryWidth = 12
				tr.DeliveryHeight = 16
			}
			got, err := fa.MapRegion(tr, fa.Region{ID: "r", BBox: [4]float64{0, 0, 2, 4}})
			if err != nil {
				t.Fatal(err)
			}
			// Independent pixel-grid inverse: enumerate a 2x4 delivery rectangle.
			minx, miny, maxx, maxy := 16, 12, 0, 0
			for yy := 0; yy < 4; yy++ {
				for xx := 0; xx < 2; xx++ {
					x, y := xx, yy
					if mirror {
						x = tr.DeliveryWidth - 1 - x
					}
					switch rotation {
					case 90:
						x, y = y, 11-x
					case 180:
						x, y = 15-x, 11-y
					case 270:
						x, y = 15-y, x
					}
					minx = min(minx, x)
					miny = min(miny, y)
					maxx = max(maxx, x+1)
					maxy = max(maxy, y+1)
				}
			}
			want := fa.Rect{X1: minx / 2 * 2, Y1: miny / 2 * 2, X2: (maxx + 1) / 2 * 2, Y2: (maxy + 1) / 2 * 2}
			if got != want {
				t.Fatalf("rotation=%d mirror=%v got=%+v want=%+v", rotation, mirror, got, want)
			}
		}
	}
	for _, r := range []fa.Region{{ID: "r", BBox: [4]float64{math.NaN(), 0, 2, 2}}, {ID: "r", BBox: [4]float64{0, 0, math.Inf(1), 2}}, {ID: "r", BBox: [4]float64{-math.MaxFloat64, 0, math.MaxFloat64, 2}}, {ID: "r", BBox: [4]float64{2, 2, 2, 4}}, {ID: "r", BBox: [4]float64{0, 0, 2, 2}, PaddingRatio: math.NaN()}, {ID: "r", FullFrame: true, PaddingRatio: .1}} {
		_, err := fa.MapRegion(base, r)
		requireCode(t, err, fa.RegionInvalid)
	}
	_, err := fa.MapRegion(crop, fa.Region{ID: "padding", BBox: [4]float64{0, 0, 1, 1}})
	requireCode(t, err, fa.RegionInvalid)
}

func TestExplicitCopyAdmissionHasNoSoftwareFallback(t *testing.T) {
	c := &fakeClock{}
	s := scope("front")
	a := admission(s)
	for _, policy := range []fa.CopyPolicy{{RequireHardware: true, AllowSoftware: true}, {AllowSoftware: false}} {
		_, err := fa.NewStore(s, limits(), c, copyport.CPU{}, a, policy, portableIntegrity())
		requireCode(t, err, fa.ProviderUnavailable)
	}
	for _, mutate := range []func(*fa.CopyAdmission){func(a *fa.CopyAdmission) { a.Attested = false }, func(a *fa.CopyAdmission) { a.Provider = "fake" }, func(a *fa.CopyAdmission) { a.MediaEpoch = 2 }, func(a *fa.CopyAdmission) { a.Graph = hash(2) }, func(a *fa.CopyAdmission) { a.Evidence = fa.Digest{} }} {
		candidate := a
		mutate(&candidate)
		_, err := fa.NewStore(s, limits(), c, copyport.CPU{}, candidate, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
		requireCode(t, err, fa.ProviderUnavailable)
	}
}

func TestExtractPixelsProofAndTampering(t *testing.T) {
	st, c, s := fixture(t, copyport.CPU{})
	d := publish(t, st, c, s, 1)
	q := request(s, d, c)
	result, err := st.Extract(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	err = result.Visit(0, func(a fa.Artifact, b []byte) error {
		source := sourcePixels(s.Geometry, 1)
		want := []byte{}
		for y := 2; y < 6; y++ {
			want = append(want, source[y*8+2:y*8+6]...)
		}
		for y := 1; y < 3; y++ {
			want = append(want, source[64+y*8+2:64+y*8+6]...)
		}
		if !bytes.Equal(b, want) {
			t.Fatalf("ROI=%v want=%v", b, want)
		}
		verify := func(p fa.Proof, payload []byte) error {
			return fa.VerifyProof(p, payload, q.ID, q.Frame, origin(s, 1), identity(s.Geometry), q.Regions[0], s.Surface, admission(s), 512)
		}
		if err := verify(a.Proof, b); err != nil {
			t.Fatal(err)
		}
		for _, mutate := range []func(*fa.Proof){func(p *fa.Proof) { p.RequestID = requestID(2) }, func(p *fa.Proof) { p.Origin.Source = "other" }, func(p *fa.Proof) { p.Origin.MediaEpoch++ }, func(p *fa.Proof) { p.Delivery.FrameID++ }, func(p *fa.Proof) { p.Delivery.Graph = hash(2) }, func(p *fa.Proof) { p.Transform = hash(3) }, func(p *fa.Proof) { p.Geometry.Stride += 2 }, func(p *fa.Proof) { p.BBox.X2 += 2 }, func(p *fa.Proof) { p.PayloadSHA256 = hash(2) }, func(p *fa.Proof) { p.Digest = hash(2) }, func(p *fa.Proof) { p.Version = 2 }} {
			p := a.Proof
			mutate(&p)
			if verify(p, b) == nil {
				t.Fatalf("accepted tampered proof %+v", p)
			}
		}
		copyBytes := append([]byte(nil), b...)
		copyBytes[0] ^= 1
		if verify(a.Proof, copyBytes) == nil {
			t.Fatal("accepted changed pixels")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPaddedNV12FullFrameAndROI(t *testing.T) {
	c := &fakeClock{}
	c.set(100)
	s := scope("front")
	s.Geometry.Stride = 12
	st, err := fa.NewStore(s, limits(), c, copyport.CPU{}, admission(s), fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	if err != nil {
		t.Fatal(err)
	}
	d := publish(t, st, c, s, 1)
	q := request(s, d, c)
	q.Regions = []fa.Region{{ID: "full", FullFrame: true}, {ID: "roi", BBox: [4]float64{2, 2, 6, 6}}}
	r, err := st.Extract(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	source := sourcePixels(s.Geometry, 1)
	if err = r.Visit(0, func(a fa.Artifact, p []byte) error {
		if len(p) != 96 || a.Proof.Geometry.Stride != 8 {
			t.Fatal("padded source was not packed")
		}
		for row := 0; row < 12; row++ {
			if !bytes.Equal(p[row*8:row*8+8], source[row*12:row*12+8]) {
				t.Fatal("padding leaked into output")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCorrelationRequiresBoundOriginsNotCountersOrTimestamps(t *testing.T) {
	a, c, s := fixture(t, copyport.CPU{})
	d1 := publish(t, a, c, s, 1)
	live := delivery(s, "live", 99)
	if err := a.Bind(origin(s, 1), fa.DeliveryBinding{Ref: live, Transform: identity(s.Geometry)}); err != nil {
		t.Fatal(err)
	}
	r1, rLive := resolve(t, a, d1), resolve(t, a, live)
	if got := fa.Correlate(r1, rLive); got != fa.ExactSameOrigin {
		t.Fatal(got)
	}
	d2 := publish(t, a, c, s, 2)
	if got := fa.Correlate(r1, resolve(t, a, d2)); got != fa.DifferentOrigin {
		t.Fatal("equal timestamps must not correlate", got)
	}
	u := delivery(s, "live", 100)
	if err := a.BindUnavailable(u); err != nil {
		t.Fatal(err)
	}
	if got := fa.Correlate(r1, resolve(t, a, u)); got != fa.CorrelationUnavailable {
		t.Fatal(got)
	}
	q := request(s, u, c)
	_, err := a.Extract(context.Background(), q)
	requireCode(t, err, fa.CorrelationUnavailable)
	otherScope := scope("overview")
	otherScope.Graph = hash(2)
	b, err := fa.NewStore(otherScope, limits(), c, copyport.CPU{}, admission(otherScope), fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	if err != nil {
		t.Fatal(err)
	}
	other := publish(t, b, c, otherScope, 1)
	if got := fa.Correlate(r1, resolve(t, b, other)); got != fa.DifferentSource {
		t.Fatal("source-local graphs/counters were conflated", got)
	}
	if got := fa.Correlate(fa.Resolved{}, r1); got != fa.InvalidContract {
		t.Fatal(got)
	}
	publish(t, a, c, s, 3)
	if got := fa.Correlate(r1, rLive); got != fa.ExactSameOrigin {
		t.Fatal("pixel eviction erased still-current causal metadata", got)
	}
	_, err = a.Extract(context.Background(), request(s, d1, c))
	requireCode(t, err, fa.Expired)
	c.set(1100)
	if got := fa.Correlate(r1, rLive); got != fa.Expired {
		t.Fatal("cached resolution survived metadata expiration", got)
	}
}

func TestExactValidationAndPerRegionErrors(t *testing.T) {
	st, c, s := fixture(t, copyport.CPU{})
	d := publish(t, st, c, s, 1)
	q := request(s, d, c)
	for _, tc := range []struct {
		code   fa.Code
		mutate func(*fa.Request)
	}{{fa.SourceMismatch, func(q *fa.Request) { q.Source = "overview" }}, {fa.SourceMismatch, func(q *fa.Request) { q.Frame.Stream = "unknown" }}, {fa.EpochMismatch, func(q *fa.Request) { q.Frame.Epoch++ }}, {fa.GraphMismatch, func(q *fa.Request) { q.Frame.Graph = hash(2) }}, {fa.InvalidContract, func(q *fa.Request) { q.ID = [16]byte{} }}, {fa.DeadlineExceeded, func(q *fa.Request) { q.DeadlineNS = 100 }}, {fa.InvalidContract, func(q *fa.Request) { q.DeadlineNS = 201 }}, {fa.Expired, func(q *fa.Request) { q.Frame.FrameID = 99 }}, {fa.InvalidContract, func(q *fa.Request) { q.Regions = []fa.Region{q.Regions[0], q.Regions[0]} }}} {
		candidate := q
		tc.mutate(&candidate)
		r, err := st.Extract(context.Background(), candidate)
		if r != nil {
			r.Close()
			t.Fatal("invalid request produced result")
		}
		requireCode(t, err, tc.code)
	}
	q.Regions = append(q.Regions, fa.Region{ID: "bad", BBox: [4]float64{0, 0, math.NaN(), 1}})
	r, err := st.Extract(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	arts := r.Artifacts()
	if len(arts) != 2 || arts[0].State != fa.Complete || arts[1].State != fa.RegionInvalid {
		t.Fatalf("artifacts=%+v", arts)
	}
	requireCode(t, r.Visit(1, func(fa.Artifact, []byte) error { t.Fatal("invalid region yielded pixels"); return nil }), fa.RegionInvalid)
}

func TestFixedArenaIncludesOutstandingResultsAndRebind(t *testing.T) {
	c := &fakeClock{}
	c.set(100)
	s := scope("front")
	l := limits()
	l.PoolBytes = 384
	l.MaxFrameBytes = 96
	l.MaxArtifactBytes = 192
	l.MaxOutputPerRequest = 192
	l.MaxFullFrameBytes = 96
	l.MaxResults = 2
	st, err := fa.NewStore(s, l, c, copyport.CPU{}, admission(s), fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	if err != nil {
		t.Fatal(err)
	}
	d := publish(t, st, c, s, 1)
	q := request(s, d, c)
	q.Regions = []fa.Region{{ID: "full", FullFrame: true}}
	r1, err := st.Extract(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := st.Extract(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.Extract(context.Background(), q)
	requireCode(t, err, fa.CapacityRejected)
	for seq := uint64(2); seq < 500; seq++ {
		publish(t, st, c, s, seq)
		stats := st.Stats()
		if stats.PoolBytes != 384 || stats.UsedBytes > 384 || stats.ArtifactBytes != 192 || stats.PeakUsedBytes > 384 || stats.Frames > 2 {
			t.Fatalf("unbounded stats %+v", stats)
		}
	}
	s.MediaEpoch = 2
	s.Graph = hash(2)
	s.Streams = map[string]uint64{"analysis": 21, "live": 22}
	if err = st.Rebind(s, admission(s)); err != nil {
		t.Fatal(err)
	}
	publish(t, st, c, s, 1)
	if err = r1.Visit(0, func(_ fa.Artifact, p []byte) error {
		if !bytes.Equal(p, sourcePixels(s.Geometry, 1)) {
			t.Fatal("output was overwritten by ring eviction/rebind")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r1.Close()
	r1.Close()
	r2.Close()
	stats := st.Stats()
	if stats.ArtifactBytes != 0 || stats.OpenResults != 0 || stats.WorkingBytes != 0 || stats.UsedBytes != 96 {
		t.Fatalf("leaked %+v", stats)
	}
	requireCode(t, r1.Visit(0, func(fa.Artifact, []byte) error { return nil }), fa.InvalidContract)
}

type blockingCopier struct {
	active       atomic.Bool
	entered      chan struct{}
	release      chan struct{}
	ignoreCancel bool
}

func (b *blockingCopier) Capability() fa.CopyCapability { return copyport.CPU{}.Capability() }
func (b *blockingCopier) CopyNV12(ctx context.Context, q fa.CopyBudget, dst, src []byte, g fa.Geometry, r fa.Rect) error {
	if b.active.Swap(false) {
		b.entered <- struct{}{}
		if b.ignoreCancel {
			<-b.release
		} else {
			select {
			case <-b.release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return copyport.CPU{}.CopyNV12(ctx, q, dst, src, g, r)
}
func blocker() *blockingCopier {
	return &blockingCopier{entered: make(chan struct{}, 1), release: make(chan struct{})}
}
func wait(t testing.TB, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded operation stalled")
	}
}

func TestSlowExtractDoesNotBlockPublishRebindOrAnotherSource(t *testing.T) {
	b := blocker()
	b.ignoreCancel = true
	st, c, s := fixture(t, b)
	d := publish(t, st, c, s, 1)
	q := request(s, d, c)
	b.active.Store(true)
	done := make(chan error, 1)
	go func() {
		r, err := st.Extract(context.Background(), q)
		if r != nil {
			r.Close()
		}
		done <- err
	}()
	wait(t, b.entered)
	returned := make(chan struct{})
	go func() {
		err := st.TryPublish(context.Background(), origin(s, 2), []fa.DeliveryBinding{{Ref: delivery(s, "analysis", 2), Transform: identity(s.Geometry)}}, sourcePixels(s.Geometry, 2), 100, 200)
		if !errors.Is(err, fa.CapacityRejected) {
			done <- fmt.Errorf("publish err=%v", err)
		}
		close(returned)
	}()
	wait(t, returned)
	other, otherClock, otherScope := fixture(t, copyport.CPU{})
	publish(t, other, otherClock, otherScope, 1)
	s.Graph = hash(2)
	if err := st.Rebind(s, admission(s)); err != nil {
		t.Fatal(err)
	}
	stats := st.Stats()
	if stats.Frames != 0 || stats.WorkingBytes != 96 || stats.ArtifactBytes != 24 || stats.UsedBytes != 120 {
		t.Fatalf("old input or output disappeared from peak accounting: %+v", stats)
	}
	close(b.release)
	requireCode(t, <-done, fa.GraphMismatch)
	stats = st.Stats()
	if stats.UsedBytes != 0 || stats.OpenResults != 0 || stats.CopyBusy {
		t.Fatalf("leaked after invalidation %+v", stats)
	}
}

func TestCancelDeadlineAndEvictionDuringCopy(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline", "age", "provider"} {
		t.Run(mode, func(t *testing.T) {
			b := blocker()
			st, c, s := fixture(t, b)
			d := publish(t, st, c, s, 1)
			q := request(s, d, c)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.active.Store(true)
			done := make(chan error, 1)
			go func() {
				r, err := st.Extract(ctx, q)
				if r != nil {
					r.Close()
				}
				done <- err
			}()
			wait(t, b.entered)
			want := fa.Canceled
			switch mode {
			case "cancel":
				cancel()
			case "deadline":
				c.set(200)
				want = fa.DeadlineExceeded
				close(b.release)
			case "age":
				c.set(1100)
				want = fa.DeadlineExceeded
				close(b.release)
			case "provider":
				c.set(99)
				want = fa.ProviderUnavailable
				close(b.release)
			}
			requireCode(t, <-done, want)
			stats := st.Stats()
			if stats.ArtifactBytes != 0 || stats.OpenResults != 0 || stats.WorkingBytes != 0 {
				t.Fatalf("leak %+v", stats)
			}
		})
	}
}

func TestInvalidationDuringPublishAndObservedAge(t *testing.T) {
	b := blocker()
	st, c, s := fixture(t, b)
	b.active.Store(true)
	done := make(chan error, 1)
	o := origin(s, 1)
	d := delivery(s, "analysis", 1)
	go func() {
		done <- st.TryPublish(context.Background(), o, []fa.DeliveryBinding{{Ref: d, Transform: identity(s.Geometry)}}, sourcePixels(s.Geometry, 1), 100, 200)
	}()
	wait(t, b.entered)
	s.MediaEpoch = 2
	s.Graph = hash(2)
	if err := st.Rebind(s, admission(s)); err != nil {
		t.Fatal(err)
	}
	if st.Stats().WorkingBytes != 96 {
		t.Fatal("staging bytes not counted")
	}
	close(b.release)
	requireCode(t, <-done, fa.EpochMismatch)
	if st.Stats().UsedBytes != 0 {
		t.Fatal("staging leak")
	}
	c.set(1100)
	err := st.TryPublish(context.Background(), origin(s, 1), []fa.DeliveryBinding{{Ref: delivery(s, "analysis", 1), Transform: identity(s.Geometry)}}, sourcePixels(s.Geometry, 1), 100, 1200)
	requireCode(t, err, fa.Expired)
	err = st.TryPublish(context.Background(), origin(s, 1), []fa.DeliveryBinding{{Ref: delivery(s, "analysis", 1), Transform: identity(s.Geometry)}}, sourcePixels(s.Geometry, 1), 1101, 1200)
	requireCode(t, err, fa.InvalidContract)
}

func TestExpiryBindingQuotaAndClockFailure(t *testing.T) {
	st, c, s := fixture(t, copyport.CPU{})
	d := publish(t, st, c, s, 1)
	for id := uint64(2); id < 30; id++ {
		if err := st.BindUnavailable(delivery(s, "live", id)); err != nil {
			t.Fatal(err)
		}
	}
	if st.Stats().Bindings != 8 {
		t.Fatal("metadata quota not enforced")
	}
	_, err := st.Resolve(d)
	requireCode(t, err, fa.Expired)
	c.set(1100)
	if st.Stats().UsedBytes != 0 || st.Stats().Bindings != 0 {
		t.Fatal("age did not evict")
	}
	c.set(1099)
	if !st.Stats().Poisoned {
		t.Fatal("clock regression not fail-closed")
	}
	_, err = st.Resolve(delivery(s, "live", 29))
	requireCode(t, err, fa.ProviderUnavailable)
	s.MediaEpoch = 2
	s.Streams = map[string]uint64{"analysis": 21, "live": 22}
	if err = st.Rebind(s, admission(s)); err != nil {
		t.Fatal(err)
	}
	publish(t, st, c, s, 1)
}

func TestRacePublishExtractResolveAndRebind(t *testing.T) {
	st, c, s := fixture(t, copyport.CPU{})
	publish(t, st, c, s, 1)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 250; i++ {
				switch worker {
				case 0:
					seq := uint64(i + 2)
					_ = st.TryPublish(context.Background(), origin(s, seq), []fa.DeliveryBinding{{Ref: delivery(s, "analysis", seq), Transform: identity(s.Geometry)}}, sourcePixels(s.Geometry, byte(i)), 100, 200)
				case 1:
					r, err := st.Extract(context.Background(), request(s, delivery(s, "analysis", uint64(i+1)), c))
					if err == nil {
						_ = r.Visit(0, func(fa.Artifact, []byte) error { return nil })
						r.Close()
					}
				case 2:
					a, err := st.Resolve(delivery(s, "analysis", uint64(i+1)))
					if err == nil {
						_ = fa.Correlate(a, a)
					}
				case 3:
					_ = st.Stats()
					if i == 125 {
						next := scope("front")
						next.MediaEpoch = 2
						next.Graph = hash(2)
						_ = st.Rebind(next, admission(next))
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	stats := st.Stats()
	if stats.UsedBytes > stats.PoolBytes || stats.ArtifactBytes != 0 || stats.WorkingBytes != 0 || stats.OpenResults != 0 {
		t.Fatalf("bad post-race state %+v", stats)
	}
}

func FuzzMapRegionNeverEscapesNV12(f *testing.F) {
	f.Add(float64(1), float64(3), float64(77), float64(51), float64(.2), uint8(1), true)
	f.Add(math.MaxFloat64, math.NaN(), math.Inf(1), float64(0), float64(0), uint8(2), false)
	f.Fuzz(func(t *testing.T, x1, y1, x2, y2, padding float64, rotation uint8, mirror bool) {
		g := fa.Geometry{Width: 128, Height: 96, Stride: 144}
		tr := identity(g)
		tr.Rotation = int(rotation%4) * 90
		tr.Mirror = mirror
		out, err := fa.MapRegion(tr, fa.Region{ID: "r", BBox: [4]float64{x1, y1, x2, y2}, PaddingRatio: padding})
		if err == nil && (out.X1 < 0 || out.Y1 < 0 || out.X2 > 128 || out.Y2 > 96 || out.X1 >= out.X2 || out.Y1 >= out.Y2 || out.X1%2 != 0 || out.Y1%2 != 0 || out.X2%2 != 0 || out.Y2%2 != 0) {
			t.Fatalf("escaped surface %+v", out)
		}
	})
}
