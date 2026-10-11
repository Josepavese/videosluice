package ingress_test

import (
	"bytes"
	"context"
	"errors"
	"go/parser"
	"go/token"
	"strconv"
	"sync/atomic"
	"testing"

	fa "github.com/Josepavese/videosluice/frameaccess"
	"github.com/Josepavese/videosluice/frameaccess/copyport"
	fd "github.com/Josepavese/videosluice/framedomain"
	"github.com/Josepavese/videosluice/framedomain/entropy"
	in "github.com/Josepavese/videosluice/frameingress"
	wire "github.com/Josepavese/videosluice/framewire"
)

type authority struct{ generation atomic.Value }

func (a *authority) CurrentGeneration() (string, error) { return a.generation.Load().(string), nil }

type clock struct{ now atomic.Uint64 }

func (c *clock) NowNS() uint64 { return c.now.Load() }

type admission struct{}

func (admission) Attest(_ context.Context, s fa.Scope) (fa.CopyAdmission, error) {
	return admitted(s), nil
}
func admitted(s fa.Scope) fa.CopyAdmission {
	return fa.CopyAdmission{Provider: "cpu_nv12_copy", MediaEpoch: s.MediaEpoch, Graph: s.Graph, Evidence: fa.Digest{9}, Attested: true}
}

type fixture struct {
	r      *fd.Runtime
	b      *fd.Binding
	i      *in.Ingress
	c      *clock
	a      *authority
	tr     fa.Transform
	layout fd.Layout
}

func setup(t testing.TB) *fixture {
	return setupCopier(t, copyport.CPU{})
}
func setupCopier(t testing.TB, copier fa.Copier) *fixture {
	return setupCopierAuthority(t, copier, nil)
}
func setupCopierAuthority(t testing.TB, copier fa.Copier, supplied fd.ProcessAuthority) *fixture {
	return setupGeometryAuthority(t, copier, supplied, 16, 16)
}
func setupGeometryAuthority(t testing.TB, copier fa.Copier, supplied fd.ProcessAuthority, width, height int) *fixture {
	return setupGeometryAuthorityCopyBudget(t, copier, supplied, width, height, 100)
}

func setupGeometryAuthorityCopyBudget(t testing.TB, copier fa.Copier, supplied fd.ProcessAuthority, width, height int, copyBudget uint64) *fixture {
	t.Helper()
	f := &fixture{c: &clock{}, a: &authority{}}
	f.c.now.Store(100)
	f.a.generation.Store("authenticated-process-1")
	if supplied == nil {
		supplied = f.a
	}
	var err error
	domainLimits := fd.Limits{MaxSources: 4, MaxStreams: 8, MaxSourceGenerations: 4, MaxMemoryBytes: 1 << 20, MaxSourceBytes: 1 << 19, MaxBuffers: 16, MaxBufferBytes: 4096, MaxResources: 16, MaxResourceOperations: 4}
	storeLimits := fa.Limits{MaxFrames: 2, MaxBindings: 8, PoolBytes: 8192, MaxFrameBytes: 384, MaxArtifactBytes: 4096, MaxOutputPerRequest: 1024, MaxFullFrameBytes: 384, MaxRegions: 4, MaxResults: 4, MaxAgeNS: 1000, MaxCopyNS: 100}
	storeLimits.MaxCopyNS = copyBudget
	if width != 16 || height != 16 {
		size := width * height * 3 / 2
		// Test-only memory admission: four store frames, simultaneous source
		// and surface receiver workspaces, plus extraction/consumer resources.
		// The former fixed 16 MiB quote admits 720p but not this same topology
		// at 1080p. Production limits and capacity-rejection tests are unchanged.
		domainLimits.MaxSourceBytes = max(16<<20, 8*size)
		domainLimits.MaxMemoryBytes = max(32<<20, 2*domainLimits.MaxSourceBytes)
		domainLimits.MaxBufferBytes = size
		storeLimits.PoolBytes, storeLimits.MaxFrameBytes, storeLimits.MaxArtifactBytes = size*4, size, size
		storeLimits.MaxOutputPerRequest, storeLimits.MaxFullFrameBytes = size, size
	}
	f.r, err = fd.New(supplied, entropy.System{}, f.c, domainLimits)
	must(t, err)
	f.layout = fd.Layout{Source: "front", Graph: fa.Digest{1}, Streams: []string{"analysis"}, Surface: "processed_full", Geometry: fa.Geometry{Width: width, Height: height, Stride: width}, DecodedWidth: width, DecodedHeight: height}
	f.b, err = f.r.Open(context.Background(), f.layout, storeLimits, copier, admission{}, fa.CopyPolicy{AllowSoftware: true}, portableIntegrity())
	must(t, err)
	t.Cleanup(f.b.Close)
	f.tr = fa.Transform{Full: f.layout.Geometry, Crop: fa.Rect{X2: width, Y2: height}, ResizedWidth: width / 2, ResizedHeight: height / 2, DeliveryWidth: width / 2, DeliveryHeight: height / 2}
	f.i, err = in.New(f.b, fa.Digest{7}, map[string]in.DeliveryLayout{"analysis": {Codec: wire.NV12, StrideY: uint32(width / 2), StrideUV: uint32(width / 2), Transform: f.tr, MaxPayload: uint32(width * height * 3 / 8)}})
	must(t, err)
	return f
}
func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func want(t testing.TB, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("got %v want %v", err, target)
	}
}
func (f *fixture) ticket(t testing.TB) fd.Ticket {
	t.Helper()
	tk, err := f.b.AllocateDelivery("analysis")
	must(t, err)
	return tk
}
func (f *fixture) header(seq uint64) wire.Header {
	s := f.b.Scope()
	v := f.b.Fence()
	return wire.Header{Source: s.Source, SourceEpoch: s.MediaEpoch, OriginSequence: seq, Correlation: 1, Namespace: v.Namespace, Process: v.ProcessGeneration, Graph: s.Graph}
}
func pixels(seq uint64) []byte {
	p := make([]byte, 384)
	for j := range p {
		p[j] = byte(uint64(j) + seq)
	}
	return p
}
func (f *fixture) surface(t testing.TB, seq uint64) []byte {
	h := f.header(seq)
	h.Kind = wire.Surface
	h.Surface = wire.ProcessedFull
	h.Codec = wire.NV12
	h.Width = uint32(f.layout.Geometry.Width)
	h.Height = uint32(f.layout.Geometry.Height)
	h.StrideY = h.Width
	h.StrideUV = h.Width
	h.Transform = fa.Digest{7}
	p := make([]byte, int(h.Width)*int(h.Height)*3/2)
	for j := range p {
		p[j] = byte(uint64(j) + seq)
	}
	return encode(t, h, p)
}
func (f *fixture) delivery(t testing.TB, tk fd.Ticket, seq uint64) []byte {
	h := f.header(seq)
	h.Kind = wire.Delivery
	h.Surface = wire.Delivered
	h.Codec = wire.NV12
	h.Width = 8
	h.Height = 8
	h.StrideY = 8
	h.StrideUV = 8
	h.Transform = f.tr.Digest()
	h.Stream = tk.Ref().Ref.Stream
	h.PublicationEpoch = tk.Ref().Ref.Epoch
	h.DeliveryID = tk.Ref().Ref.FrameID
	if seq == 0 {
		h.Correlation = 0
	}
	return encode(t, h, make([]byte, 96))
}
func encode(t testing.TB, h wire.Header, p []byte) []byte {
	t.Helper()
	b, err := wire.Marshal(h, p)
	must(t, err)
	return b
}
func mutate(t testing.TB, p []byte, change func(*wire.Header)) []byte {
	t.Helper()
	h, b, err := wire.Parse(p)
	must(t, err)
	change(&h)
	return encode(t, h, b)
}
func (f *fixture) verify(t testing.TB, tk fd.Ticket, seq uint64) {
	t.Helper()
	region := fa.Region{ID: "roi", BBox: [4]float64{1, 1, 3, 3}}
	q := fd.Request{ID: [16]byte{1}, Frame: tk.Ref(), Source: "front", Surface: "processed_full", Regions: []fa.Region{region}, DeadlineNS: f.c.NowNS() + 100}
	r, err := f.r.Extract(context.Background(), q)
	must(t, err)
	defer r.Close()
	must(t, r.Visit(0, func(a fd.Artifact, p []byte) error {
		if a.State != fa.Complete {
			t.Fatalf("state=%v", a.State)
		}
		must(t, fd.VerifyDomainProof(a.Proof, f.b.Fence()))
		o := fa.OriginRef{Source: "front", MediaEpoch: f.b.Scope().MediaEpoch, Sequence: seq, Graph: f.b.Scope().Graph}
		must(t, fa.VerifyProof(a.Proof.Core, p, q.ID, tk.Ref().Ref, o, f.tr, region, q.Surface, admitted(f.b.Scope()), 1024))
		if a.Proof.Core.Geometry != (fa.Geometry{Width: 4, Height: 4, Stride: 4}) {
			t.Fatal("native ROI was resized")
		}
		var expected []byte
		src := pixels(seq)
		for y := 2; y < 6; y++ {
			expected = append(expected, src[y*16+2:y*16+6]...)
		}
		for y := 1; y < 3; y++ {
			expected = append(expected, src[256+y*16+2:256+y*16+6]...)
		}
		if !bytes.Equal(expected, p) {
			t.Fatal("ROI does not match exact native pixels")
		}
		return nil
	}))
}

func TestSurfaceIndependentThenDelayedExactDelivery(t *testing.T) {
	f := setup(t)
	tk := f.ticket(t)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
	f.verify(t, tk, 1)
}
func (f *fixture) expectExpiredSurface(t testing.TB, tk fd.Ticket) {
	t.Helper()
	result, err := f.r.Extract(context.Background(), fd.Request{ID: [16]byte{1}, Frame: tk.Ref(), Source: f.layout.Source, Surface: f.layout.Surface, Regions: []fa.Region{{ID: "full", FullFrame: true}}, DeadlineNS: f.c.NowNS() + 100})
	want(t, err, fa.Expired)
	if result != nil {
		t.Fatal("unavailable native pixels produced an artifact")
	}
}
func TestDeliveryBeforeSurfaceConsumesExactTicketOnce(t *testing.T) {
	f := setup(t)
	tk := f.ticket(t)
	d := f.delivery(t, tk, 1)
	must(t, f.i.AcceptDelivery(context.Background(), tk, d))
	f.expectExpiredSurface(t, tk)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	f.verify(t, tk, 1)
	want(t, f.i.AcceptDelivery(context.Background(), tk, d), fa.InvalidContract)
}
func TestAsynchronousReorderedSurfaceAndDelivery(t *testing.T) {
	f := setup(t)
	one, two := f.ticket(t), f.ticket(t)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 2), 100, 200))
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	must(t, f.i.AcceptDelivery(context.Background(), two, f.delivery(t, two, 2)))
	must(t, f.i.AcceptDelivery(context.Background(), one, f.delivery(t, one, 1)))
	f.verify(t, one, 1)
	f.verify(t, two, 2)
}
func TestMissingExpiredAndUnavailableNeverNearest(t *testing.T) {
	f := setup(t)
	tk := f.ticket(t)
	must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 2), 100, 200))
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 1)))
	f.expectExpiredSurface(t, tk)
	tk = f.ticket(t)
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 0)))
	_, err := f.r.Extract(context.Background(), fd.Request{ID: [16]byte{1}, Frame: tk.Ref(), Source: "front", Surface: "processed_full", Regions: []fa.Region{{ID: "full", FullFrame: true}}, DeadlineNS: 200})
	want(t, err, fa.CorrelationUnavailable)
	tk = f.ticket(t)
	f.c.now.Store(1100)
	must(t, f.i.AcceptDelivery(context.Background(), tk, f.delivery(t, tk, 2)))
	f.expectExpiredSurface(t, tk)
}
func TestWireIdentityMismatchesDoNotConsumeDelivery(t *testing.T) {
	cases := []struct {
		name   string
		change func(*wire.Header)
		code   error
	}{
		{"source", func(h *wire.Header) { h.Source = "rear" }, fa.SourceMismatch},
		{"epoch", func(h *wire.Header) { h.SourceEpoch++ }, fa.EpochMismatch},
		{"graph", func(h *wire.Header) { h.Graph[0]++ }, fa.GraphMismatch},
		{"namespace", func(h *wire.Header) { h.Namespace[0]++ }, fd.ErrDomainMismatch},
		{"process", func(h *wire.Header) { h.Process = "old-worker" }, fd.ErrDomainMismatch},
		{"publication", func(h *wire.Header) { h.PublicationEpoch++ }, fd.ErrDomainMismatch},
		{"delivery", func(h *wire.Header) { h.DeliveryID++ }, fd.ErrDomainMismatch},
		{"stream", func(h *wire.Header) { h.Stream = "live" }, fd.ErrDomainMismatch},
		{"transform", func(h *wire.Header) { h.Transform[0]++ }, fa.TransformMismatch},
		{"geometry", func(h *wire.Header) { h.Width = 6 }, fa.InvalidContract},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := setup(t)
			tk := f.ticket(t)
			must(t, f.i.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
			d := f.delivery(t, tk, 1)
			err := f.i.AcceptDelivery(context.Background(), tk, mutate(t, d, tc.change))
			want(t, err, tc.code)
			var diagnostic *in.Failure
			if !errors.As(err, &diagnostic) || diagnostic.Stage == "" {
				t.Fatal("missing bounded diagnostic")
			}
			must(t, f.i.AcceptDelivery(context.Background(), tk, d))
			f.verify(t, tk, 1)
		})
	}
}
func TestRetiredAndRestartCannotPublish(t *testing.T) {
	for _, restart := range []bool{false, true} {
		f := setup(t)
		tk := f.ticket(t)
		p, d := f.surface(t, 1), f.delivery(t, tk, 1)
		if restart {
			f.a.generation.Store("authenticated-process-2")
		} else {
			f.b.Close()
		}
		if f.i.AcceptSurface(context.Background(), p, 100, 200) == nil || f.i.AcceptDelivery(context.Background(), tk, d) == nil {
			t.Fatal("retired namespace accepted")
		}
	}
}
func TestBoundaryRejectsInvalidInputBeforeAllocation(t *testing.T) {
	f := setup(t)
	tk := f.ticket(t)
	p := f.surface(t, 1)
	want(t, f.i.AcceptSurface(nil, p, 100, 200), fa.InvalidContract)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	want(t, f.i.AcceptSurface(ctx, p, 100, 200), context.Canceled)
	want(t, f.i.AcceptSurface(context.Background(), append(p, 0), 100, 200), fa.CapacityRejected)
	p[len(p)-1] ^= 1
	want(t, f.i.AcceptSurface(context.Background(), p, 100, 200), wire.ErrInvalid)
	want(t, f.i.AcceptSurface(context.Background(), mutate(t, f.surface(t, 1), func(h *wire.Header) { h.Transform[0]++ }), 100, 200), fa.TransformMismatch)
	want(t, f.i.AcceptDelivery(context.Background(), fd.Ticket{}, f.delivery(t, tk, 1)), fa.SourceMismatch)
	_, err := in.New(nil, fa.Digest{1}, nil)
	want(t, err, fa.InvalidContract)
}
func TestPALIngressHasNoPlatformImports(t *testing.T) {
	for _, name := range []string{"ingress.go", "mapping.go", "access.go", "control.go", "control_codec.go", "control_dispatch.go", "control_reply.go", "frame_request.go", "frame_output.go", "frame_sender.go", "frame_session.go", "control_client.go", "source_receiver.go"} {
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		must(t, err)
		allowed := map[string]bool{"context": true, "fmt": true, "errors": true, "sort": true, "sync": true, "sync/atomic": true, "github.com/Josepavese/videosluice/framewire": true, "github.com/Josepavese/videosluice/framewire/surfaceport": true, "github.com/Josepavese/videosluice/frameaccess": true, "github.com/Josepavese/videosluice/framedomain": true, "github.com/Josepavese/videosluice/framepeer": true}
		for _, i := range f.Imports {
			p, err := strconv.Unquote(i.Path.Value)
			must(t, err)
			// Portable monotonic timing in ingress attributes commit failures;
			// it does not replace the store's clock or manufacture frame identity.
			if !allowed[p] && !((name == "control_codec.go" || name == "control_reply.go" || name == "frame_request.go" || name == "frame_output.go") && p == "encoding/binary") && !(name == "frame_request.go" && p == "math") && !((name == "frame_output.go" || name == "frame_sender.go" || name == "ingress.go") && p == "crypto/sha256") && !((name == "frame_sender.go" || name == "frame_session.go" || name == "control_client.go" || name == "source_receiver.go" || name == "ingress.go") && p == "time") {
				t.Fatalf("platform boundary import %s", p)
			}
		}
	}
}

func TestEncodedDeliveryUsesItsAdmittedCodecAndTransform(t *testing.T) {
	f := setup(t)
	adapter, err := in.New(f.b, fa.Digest{7}, map[string]in.DeliveryLayout{"analysis": {Codec: wire.H265, Transform: f.tr, MaxPayload: 1024}})
	must(t, err)
	tk := f.ticket(t)
	must(t, adapter.AcceptSurface(context.Background(), f.surface(t, 1), 100, 200))
	h, _, err := wire.Parse(f.delivery(t, tk, 1))
	must(t, err)
	h.Codec, h.StrideY, h.StrideUV = wire.H265, 0, 0
	payload := []byte{0, 0, 1, 0x26, 1, 0x80}
	packet := encode(t, h, payload)
	wrong := mutate(t, packet, func(h *wire.Header) { h.Codec = wire.H264 })
	want(t, adapter.AcceptDelivery(context.Background(), tk, wrong), fa.InvalidContract)
	must(t, adapter.AcceptDelivery(context.Background(), tk, packet))
	f.verify(t, tk, 1)
}

func TestAdmissionRejectsUnboundedOrForeignLayouts(t *testing.T) {
	f := setup(t)
	valid := in.DeliveryLayout{Codec: wire.NV12, StrideY: 8, StrideUV: 8, Transform: f.tr, MaxPayload: 96}
	for _, change := range []func(*in.DeliveryLayout){
		func(d *in.DeliveryLayout) { d.MaxPayload = wire.MaxPayload + 1 },
		func(d *in.DeliveryLayout) { d.MaxPayload = 0 },
		func(d *in.DeliveryLayout) { d.StrideUV = 7 },
		func(d *in.DeliveryLayout) { d.StrideY = 6 },
		func(d *in.DeliveryLayout) { d.MaxPayload = 94 },
		func(d *in.DeliveryLayout) { d.Codec = 255 },
		func(d *in.DeliveryLayout) { d.Codec = wire.H265 },
		func(d *in.DeliveryLayout) { d.Transform.Full.Width = 8 },
		func(d *in.DeliveryLayout) { d.Transform.Rotation = 45 },
	} {
		d := valid
		change(&d)
		_, err := in.New(f.b, fa.Digest{7}, map[string]in.DeliveryLayout{"analysis": d})
		want(t, err, fa.InvalidContract)
		if err.Error() == "" {
			t.Fatal("diagnostic missing")
		}
	}
	_, err := in.New(f.b, fa.Digest{7}, map[string]in.DeliveryLayout{"foreign": valid})
	want(t, err, fa.InvalidContract)
	_, err = in.New(f.b, fa.Digest{7}, nil)
	want(t, err, fa.InvalidContract)
}
