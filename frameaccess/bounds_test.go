package frameaccess_test

import (
	"context"
	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	"go/parser"
	"go/token"
	"runtime"
	"strconv"
	"testing"
)

func TestCoreHasNoSyscallPlatformOrTimeProviders(t *testing.T) {
	allowed := map[string]bool{"context": true, "crypto/sha256": true, "encoding/binary": true, "fmt": true, "math": true, "errors": true, "sort": true, "sync": true, "sync/atomic": true}
	for _, name := range []string{"types.go", "store.go", "publication_diagnostics.go"} {
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range f.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil || !allowed[path] {
				t.Fatalf("logic/provider boundary violation %s import=%s", name, spec.Path.Value)
			}
		}
	}
}

func TestCPUProviderHonorsEveryCopyBudget(t *testing.T) {
	c := &fakeClock{}
	c.set(100)
	g := fa.Geometry{Width: 8, Height: 8, Stride: 8}
	src := sourcePixels(g, 0)
	cpu := copyport.CPU{}
	full := fa.Rect{X2: 8, Y2: 8}
	q := fa.CopyBudget{MaxBytes: 96, DeadlineNS: 200, Clock: c}
	requireCode(t, cpu.CopyNV12(context.Background(), fa.CopyBudget{MaxBytes: 95, DeadlineNS: 200, Clock: c}, make([]byte, 96), src, g, full), fa.CapacityRejected)
	requireCode(t, cpu.CopyNV12(context.Background(), q, make([]byte, 96), src[:95], g, full), fa.InvalidContract)
	requireCode(t, cpu.CopyNV12(context.Background(), q, make([]byte, 24), src, g, fa.Rect{X1: 1, Y1: 2, X2: 6, Y2: 6}), fa.InvalidContract)
	requireCode(t, cpu.CopyNV12(context.Background(), q, make([]byte, 23), src, g, fa.Rect{X1: 2, Y1: 2, X2: 6, Y2: 6}), fa.CapacityRejected)
	requireCode(t, cpu.CopyNV12(nil, q, make([]byte, 96), src, g, full), fa.InvalidContract)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	requireCode(t, cpu.CopyNV12(ctx, q, make([]byte, 96), src, g, full), fa.Canceled)
	c.set(200)
	requireCode(t, cpu.CopyNV12(context.Background(), q, make([]byte, 96), src, g, full), fa.DeadlineExceeded)
}

func TestRequestAndPoolQuotasDoNotProducePartialSuccess(t *testing.T) {
	c := &fakeClock{}
	c.set(100)
	s := scope("front")
	l := limits()
	l.MaxOutputPerRequest = 96
	l.MaxFullFrameBytes = 24
	st, err := fa.NewStore(s, l, c, copyport.CPU{}, admission(s), fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	if err != nil {
		t.Fatal(err)
	}
	d := publish(t, st, c, s, 1)
	q := request(s, d, c)
	q.Regions = []fa.Region{{ID: "full", FullFrame: true}}
	_, err = st.Extract(context.Background(), q)
	requireCode(t, err, fa.CapacityRejected)
	q.Regions = []fa.Region{{ID: "a", BBox: [4]float64{0, 0, 8, 8}}, {ID: "b", BBox: [4]float64{0, 0, 2, 2}}}
	_, err = st.Extract(context.Background(), q)
	requireCode(t, err, fa.CapacityRejected)
	if stats := st.Stats(); stats.ArtifactBytes != 0 || stats.OpenResults != 0 {
		t.Fatalf("partial output allocation leaked %+v", stats)
	}
	for _, mutate := range []func(*fa.Limits){func(l *fa.Limits) { l.PoolBytes = 1 << 30 }, func(l *fa.Limits) { l.MaxFrames = 65 }, func(l *fa.Limits) { l.MaxRegions = 17 }, func(l *fa.Limits) { l.MaxBindings = 4097 }, func(l *fa.Limits) { l.MaxCopyNS = 0 }, func(l *fa.Limits) { l.MaxAgeNS = 0 }} {
		bad := l
		mutate(&bad)
		_, err = fa.NewStore(s, bad, c, copyport.CPU{}, admission(s), fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
		requireCode(t, err, fa.InvalidContract)
	}
}

func TestFailedBindingsNeverReplaceAnExactAssociation(t *testing.T) {
	st, c, s := fixture(t, copyport.CPU{})
	d := publish(t, st, c, s, 1)
	o := origin(s, 1)
	bind := fa.DeliveryBinding{Ref: d, Transform: identity(s.Geometry)}
	requireCode(t, st.Bind(o, bind), fa.InvalidContract)
	requireCode(t, st.BindUnavailable(d), fa.InvalidContract)
	bad := o
	bad.Source = "other"
	requireCode(t, st.Bind(bad, bind), fa.SourceMismatch)
	bad = o
	bad.MediaEpoch = 2
	requireCode(t, st.Bind(bad, bind), fa.EpochMismatch)
	bad = o
	bad.Graph = hash(2)
	requireCode(t, st.Bind(bad, bind), fa.GraphMismatch)
	bind.Ref = delivery(s, "live", 22)
	bad = o
	bad.Sequence = 999
	if err := st.Bind(bad, bind); err != nil {
		t.Fatal("exact metadata required retained pixels", err)
	}
	_, err := st.Extract(context.Background(), request(s, bind.Ref, c))
	requireCode(t, err, fa.Expired)
	bind.Ref.FrameID++
	bind.Transform.Rotation = 1
	requireCode(t, st.Bind(o, bind), fa.TransformMismatch)
	if got := fa.Correlate(resolve(t, st, d), resolve(t, st, d)); got != fa.ExactSameOrigin {
		t.Fatal(got)
	}
	s.Streams["analysis"] = 999 // The caller's config map must not mutate the store.
	if _, err := st.Resolve(d); err != nil {
		t.Fatal("caller mutated source binding", err)
	}
}

func TestExpiryDuringExtractEvenWhenDeadlineStillValid(t *testing.T) {
	b := blocker()
	c := &fakeClock{}
	c.set(100)
	s := scope("front")
	l := limits()
	l.MaxAgeNS = 50
	st, err := fa.NewStore(s, l, c, b, admission(s), fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	if err != nil {
		t.Fatal(err)
	}
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
	c.set(150)
	close(b.release)
	requireCode(t, <-done, fa.Expired)
	if st.Stats().UsedBytes != 0 {
		t.Fatal("expired copy retained memory")
	}
}

func TestConcurrentBindingCannotBeReboundByPublishCommit(t *testing.T) {
	b := blocker()
	st, c, s := fixture(t, b)
	d := delivery(s, "analysis", 1)
	b.active.Store(true)
	done := make(chan error, 1)
	go func() {
		done <- st.TryPublish(context.Background(), origin(s, 1), []fa.DeliveryBinding{{Ref: d, Transform: identity(s.Geometry)}}, sourcePixels(s.Geometry, 1), 100, 200)
	}()
	wait(t, b.entered)
	if err := st.BindUnavailable(d); err != nil {
		t.Fatal(err)
	}
	resolved := resolve(t, st, d)
	close(b.release)
	requireCode(t, <-done, fa.InvalidContract)
	if got := fa.Correlate(resolved, resolve(t, st, d)); got != fa.CorrelationUnavailable {
		t.Fatalf("already visible binding changed: %s", got)
	}
	stats := st.Stats()
	if stats.Bindings != 1 || stats.Frames != 0 || stats.UsedBytes != 0 {
		t.Fatalf("duplicate order/staging leaked: %+v", stats)
	}
	publish(t, st, c, s, 2)
	if stats = st.Stats(); stats.Bindings != 2 || stats.Frames != 1 {
		t.Fatalf("subsequent publication was damaged: %+v", stats)
	}
}

func TestDeliveryNamespacesCannotReplayAfterEvictionRestartOrGraphCycle(t *testing.T) {
	st, c, s := fixture(t, copyport.CPU{})
	old := publish(t, st, c, s, 1)
	publish(t, st, c, s, 2)
	publish(t, st, c, s, 3)
	requireCode(t, st.Bind(origin(s, 3), fa.DeliveryBinding{Ref: old, Transform: identity(s.Geometry)}), fa.InvalidContract)
	requireCode(t, st.BindUnavailable(old), fa.InvalidContract)
	next := s
	next.MediaEpoch = 2
	if err := st.Rebind(next, admission(next)); err != nil {
		t.Fatal(err)
	}
	// A continuous publication is legal, but its counter may not restart.
	err := st.TryPublish(context.Background(), origin(next, 1), []fa.DeliveryBinding{{Ref: old, Transform: identity(s.Geometry)}}, sourcePixels(s.Geometry, 99), 100, 200)
	requireCode(t, err, fa.InvalidContract)
	requireCode(t, st.Rebind(s, admission(s)), fa.EpochMismatch)
	d := delivery(next, "analysis", 4)
	if err = st.TryPublish(context.Background(), origin(next, 1), []fa.DeliveryBinding{{Ref: d, Transform: identity(s.Geometry)}}, sourcePixels(s.Geometry, 99), 100, 200); err != nil {
		t.Fatal(err)
	}
	_, err = st.Extract(context.Background(), request(s, old, c))
	requireCode(t, err, fa.Expired)
	changed := next
	changed.Graph = hash(2)
	if err = st.Rebind(changed, admission(changed)); err != nil {
		t.Fatal(err)
	}
	// Revisiting an earlier graph hash must not resurrect its old frame IDs.
	if err = st.Rebind(next, admission(next)); err != nil {
		t.Fatal(err)
	}
	requireCode(t, st.BindUnavailable(old), fa.InvalidContract)
	changed.Streams = map[string]uint64{"analysis": 10, "live": 12}
	requireCode(t, st.Rebind(changed, admission(changed)), fa.EpochMismatch)
}

func TestDetachReattachPreservesBoundedRetiredStreamWatermark(t *testing.T) {
	st, _, s := fixture(t, copyport.CPU{})
	d := delivery(s, "live", 10)
	if err := st.BindUnavailable(d); err != nil {
		t.Fatal(err)
	}
	detached := s
	detached.Graph = hash(2)
	detached.Streams = map[string]uint64{"analysis": 11}
	if err := st.Rebind(detached, admission(detached)); err != nil {
		t.Fatal(err)
	}
	s.Graph = hash(3)
	if err := st.Rebind(s, admission(s)); err != nil {
		t.Fatal(err)
	}
	d.Graph = s.Graph
	requireCode(t, st.BindUnavailable(d), fa.InvalidContract)
	d.FrameID = 11
	if err := st.BindUnavailable(d); err != nil {
		t.Fatal(err)
	}
}

func TestOversizedPublisherMetadataRejectedBeforeAllocation(t *testing.T) {
	st, _, s := fixture(t, copyport.CPU{})
	deliveries := make([]fa.DeliveryBinding, 100000)
	pixels := sourcePixels(s.Geometry, 0)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := st.TryPublish(context.Background(), origin(s, 1), deliveries, pixels, 100, 200)
	runtime.ReadMemStats(&after)
	requireCode(t, err, fa.InvalidContract)
	if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
		t.Fatalf("invalid metadata batch allocated %d bytes", n)
	}
}

func BenchmarkBoundedPublishExtract720p(b *testing.B) {
	c := &fakeClock{}
	c.set(100)
	s := scope("front")
	s.Geometry = fa.Geometry{Width: 1280, Height: 720, Stride: 1280}
	s.DecodedWidth = 1280
	s.DecodedHeight = 720
	l := limits()
	l.MaxFrames = 6
	l.PoolBytes = 16 << 20
	l.MaxFrameBytes = 2 << 20
	l.MaxArtifactBytes = 4 << 20
	l.MaxOutputPerRequest = 2 << 20
	l.MaxFullFrameBytes = 2 << 20
	st, err := fa.NewStore(s, l, c, copyport.CPU{}, admission(s), fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	if err != nil {
		b.Fatal(err)
	}
	pixels := sourcePixels(s.Geometry, 1)
	tr := identity(s.Geometry)
	b.SetBytes(int64(len(pixels)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		seq := uint64(i + 1)
		d := delivery(s, "analysis", seq)
		if err := st.TryPublish(context.Background(), origin(s, seq), []fa.DeliveryBinding{{Ref: d, Transform: tr}}, pixels, 100, 200); err != nil {
			b.Fatal(err)
		}
		q := request(s, d, c)
		q.Regions = []fa.Region{{ID: "r", BBox: [4]float64{120, 40, 376, 296}}}
		r, err := st.Extract(context.Background(), q)
		if err != nil {
			b.Fatal(err)
		}
		r.Close()
	}
	b.StopTimer()
	stats := st.Stats()
	b.ReportMetric(float64(stats.PeakUsedBytes), "peak_payload_B")
	b.ReportMetric(float64(stats.PoolBytes), "preallocated_B")
}
