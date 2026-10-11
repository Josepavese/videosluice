//go:build linux && cgo && hardware

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	videobroker "github.com/Josepavese/videosluice"
	fa "github.com/Josepavese/videosluice/frameaccess"
	fd "github.com/Josepavese/videosluice/framedomain"
	"github.com/Josepavese/videosluice/framedomain/entropy"
	"github.com/Josepavese/videosluice/framedomain/monotime"
	in "github.com/Josepavese/videosluice/frameingress"
	pl "github.com/Josepavese/videosluice/framepeer/provider/linux"
	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/internal/source"
	"github.com/Josepavese/videosluice/routing"
	"golang.org/x/sys/unix"
)

var hardwarePixelMutatingUnits = []string{"nemesi-update-*.service", "nemesi-build-*.service"}

// Reconciled from the private 9 September physical pixel probe. Never run beside
// Nemesi: this test owns a camera, not the integrated Tracker/Registry pipeline.
// The operator must quiesce through Safe Deploy and arrange checked restoration.
// Catalog/plan must describe the enumerated camera and unchanged central policy.
// No pixels, tokens, Registry records or recordings are persisted or displayed.
func TestHardwareNativePixels(t *testing.T) {
	if os.Getenv("FAP_HW_CATALOG") == "" {
		t.Skip("explicit catalog and exclusive-camera qualification required")
	}
	if runtime.GOARCH != "arm64" || os.Getenv("FAP_HW_PLAN") == "" {
		t.Fatal("explicit ARM64 hardware plan required")
	}
	check, cancelCheck := context.WithTimeout(context.Background(), 4*time.Second)
	state, err := exec.CommandContext(check, "systemctl", "show", "nemesi-agent.service", "nemesi-release-trial.service", "--property=ActiveState", "--value").Output()
	cancelCheck()
	pixelMust(t, err)
	check, cancelCheck = context.WithTimeout(context.Background(), 4*time.Second)
	jobArgs := append([]string{"list-units", "--all", "--no-legend", "--no-pager", "--plain", "--state=active,activating,deactivating"}, hardwarePixelMutatingUnits...)
	jobs, err := exec.CommandContext(check, "systemctl", jobArgs...).Output()
	cancelCheck()
	pixelMust(t, err)
	pixelMust(t, validatePixelQuiescence(state, jobs))
	catalog, err := routing.LoadCatalog(os.Getenv("FAP_HW_CATALOG"))
	pixelMust(t, err)
	plan, err := routing.LoadPlan(os.Getenv("FAP_HW_PLAN"))
	pixelMust(t, err)
	cfg, err := routing.Materialize(catalog, plan)
	pixelMust(t, err)
	if len(cfg.Sources) != 1 || cfg.Runtime.FrameAccess == nil || !cfg.Runtime.Governor.Enabled {
		t.Fatal("explicit single-source FAP graph with original governor required")
	}
	base, err := filepath.EvalSymlinks(os.TempDir())
	pixelMust(t, err)
	if strings.HasPrefix(base, "/opt/nemesi") || len(base) > 48 {
		t.Fatal("use a short private TMPDIR outside the measured release")
	}
	root, err := os.MkdirTemp(base, "fappx-")
	pixelMust(t, err)
	defer os.RemoveAll(root)
	if len(root) > 65 {
		t.Fatal("use a short private TMPDIR outside the measured release")
	}
	cfg.Runtime.RuntimeDirectory, cfg.Runtime.StateDirectory = root, root
	cfg.Runtime.Status.Socket = ""
	cfg.Runtime.Governor.StatePath = filepath.Join(root, "governor.json")
	for id, selected := range cfg.Sources {
		if !selected.Acceleration.RequireHardwareCodec || selected.Acceleration.AllowSoftwareFallback {
			t.Fatal("hardware codec and no software fallback required")
		}
		selected.Processing.Quality.StatePath = filepath.Join(root, id+"-quality.json")
		selected.Processing.Quality.AdmissionPath = filepath.Join(root, id+"-admission.json")
		cfg.Sources[id] = selected
	}
	var analysis string
	for id, stream := range cfg.Streams {
		if stream.Type == "analysis" {
			if analysis != "" {
				t.Fatal("ambiguous analysis stream")
			}
			analysis = id
		}
		if stream.Socket != "" {
			stream.Socket = filepath.Join(root, id+".sock")
		}
		if stream.NotifySocket != "" {
			stream.NotifySocket = filepath.Join(root, id+"-notify.sock")
		}
		if stream.SharedMemory != "" {
			stream.SharedMemory = filepath.Join(root, id+".nv12")
		}
		cfg.Streams[id] = stream
	}
	if analysis == "" {
		t.Fatal("analysis stream required")
	}
	for i, reader := range cfg.Runtime.FrameAccess.Readers {
		reader.DeliverySocket = filepath.Join(root, fmt.Sprintf("delivery-%d.sock", i))
		reader.ROISocket = filepath.Join(root, fmt.Sprintf("roi-%d.sock", i))
		cfg.Runtime.FrameAccess.Readers[i] = reader
	}
	handle, err := unix.PidfdOpen(os.Getpid(), 0)
	pixelMust(t, err)
	var nonce [16]byte
	_, err = rand.Read(nonce[:])
	pixelMust(t, err)
	self, err := pl.ImportProcessCapability(os.NewFile(uintptr(handle), "pixel-test-self"), pl.ProcessIdentity{PID: os.Getpid(), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), Generation: hex.EncodeToString(nonce[:])})
	pixelMust(t, err)
	clock := monotime.New()
	domain, err := fd.New(self, entropy.System{}, clock, fd.Limits{MaxSources: 16, MaxStreams: 64, MaxSourceGenerations: 64, MaxMemoryBytes: 256 << 20, MaxSourceBytes: 96 << 20, MaxBuffers: 128, MaxBufferBytes: 16 << 20, MaxResources: 512, MaxResourceOperations: 16})
	pixelMust(t, err)
	owner, err := fd.OwnRuntimeResource(context.Background(), domain, fd.ResourcePlan[*pl.AcceptedLaunch]{Bytes: 1 << 20,
		Create: func(context.Context) (fd.Created[*pl.AcceptedLaunch], error) {
			return fd.Created[*pl.AcceptedLaunch]{Value: &pl.AcceptedLaunch{Self: self}, Owned: true}, nil
		},
		Release: func(value *pl.AcceptedLaunch) error { return value.Close() }})
	pixelMust(t, err)
	bootstrap := &nativeFrameBootstrap{runtime: domain, resource: owner, clock: clock}
	defer func() { pixelMust(t, bootstrap.Close()) }()
	pixelMust(t, bootstrap.Configure(cfg))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var mu sync.Mutex
	var capture *nativeFrameCapture
	captures := 0
	factory := bootstrap.FrameCaptures()
	t.Log("hardware_capture_starting=true")
	service, err := videobroker.Start(ctx, cfg, videobroker.StartOptions{FrameCapture: func(ctx context.Context, p source.BrokerMuxPlan) (source.FrameCapture, error) {
		c, err := factory(ctx, p)
		if err == nil {
			mu.Lock()
			capture = c.(*nativeFrameCapture)
			captures++
			mu.Unlock()
		}
		return c, err
	}})
	pixelMust(t, err)
	defer func() {
		cancel()
		// Service.Wait joins cooperative workers even after its context expires.
		// Bound the observer separately; timeout is FAILED/unconfirmed cleanup,
		// never permission to kill a hardware owner or claim SDK release.
		joined := make(chan error, 1)
		go func() { joined <- service.Wait(context.Background()) }()
		select {
		case err := <-joined:
			pixelMust(t, err)
			t.Log("hardware_cleanup_confirmed=true")
		case <-time.After(10 * time.Second):
			t.Error("cooperative hardware cleanup unconfirmed after 10s")
		}
	}()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	samples, attempts, expiredPixels, last := 0, 0, 0, uint64(0)
	var lastHeader wire.Header
	for samples < 3 {
		select {
		case <-ctx.Done():
			t.Fatal("hardware pixel deadline", ctx.Err())
		case <-tick.C:
		}
		mu.Lock()
		c, count := capture, captures
		mu.Unlock()
		if count > 1 {
			t.Fatal("capture restarted during pixel qualification")
		}
		if c == nil {
			continue
		}
		var h wire.Header
		err = c.Snapshot(ctx, analysis, last, func(_ context.Context, header wire.Header, _ []byte) error { h = header; return nil })
		if errors.Is(err, in.ErrDeliveryIdle) {
			continue
		}
		pixelMust(t, err)
		if h.DeliveryID == 0 {
			continue
		}
		status, exists := service.Snapshot(time.Now()).Sources[h.Source]
		if !exists || !status.CameraAdmitted {
			continue
		}
		if h.SourceEpoch != c.SourceEpoch() || h.OriginSequence == 0 {
			t.Fatal("capture identity mismatch")
		}
		if lastHeader.OriginSequence != 0 && h.OriginSequence <= lastHeader.OriginSequence {
			t.Fatal("pixel qualification reused or regressed a physical origin")
		}
		attempts++
		if attempts > 32 {
			t.Fatalf("physical pixel qualification exhausted 32 distinct origins: complete=%d expired=%d", samples, expiredPixels)
		}
		err = verifyHardwarePixels(ctx, bootstrap, c, h)
		if errors.Is(err, fa.Expired) {
			// Metadata existence does not promise native pixel retention. This
			// origin remains a FAILED request, not a successful pixel sample.
			// Never rebind it or renew its budget; test a distinct next origin.
			expiredPixels++
			last = h.DeliveryID
			t.Logf("hardware_pixel_origin_rejected=expired origin=%d: %v", h.OriginSequence, err)
			continue
		}
		pixelMust(t, err)
		last = h.DeliveryID
		lastHeader = h
		samples++
	}
	t.Logf("hardware_pixel_samples=%d captures=%d; no integrated/soak claim", samples, captures)
	t.Logf("hardware_pixel_distinct_origin_attempts=%d expired_requests=%d; no expired request counted as PASS", attempts, expiredPixels)
	pixelMust(t, verifyHardwareHeldResults(ctx, t, bootstrap, capture, lastHeader))
	// A real, cooperatively retired hardware owner must not serve its old refs.
	cancel()
	joined := make(chan error, 1)
	go func() { joined <- service.Wait(context.Background()) }()
	select {
	case err := <-joined:
		pixelMust(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("hardware cleanup unconfirmed; stale-ref qualification not attempted")
	}
	id := [16]byte{1}
	stale := fd.Request{ID: id, Frame: fd.DeliveryRef{Fence: capture.binding.Fence(), Ref: fa.DeliveryRef{Stream: analysis, Epoch: lastHeader.PublicationEpoch, FrameID: last, Graph: lastHeader.Graph}}, Source: lastHeader.Source, Surface: "source_decoded", Regions: []fa.Region{{ID: "retired", FullFrame: true}}, DeadlineNS: clock.NowNS() + 100_000_000}
	result, staleErr := bootstrap.runtime.Extract(context.Background(), stale)
	if result != nil {
		defer result.Close()
	}
	if !errors.Is(staleErr, fd.ErrRetired) {
		t.Fatalf("retired hardware source did not explicitly reject old ref: %v", staleErr)
	}
	t.Log("hardware_source_retired_old_ref_rejected=true")
}

func verifyHardwareHeldResults(ctx context.Context, t *testing.T, b *nativeFrameBootstrap, c *nativeFrameCapture, previous wire.Header) error {
	// Exercise the configured result quota, not a lowered test quota or a
	// stopped Tracker/BPU. Held client artifacts must not stall the real camera.
	count := hardwareHeldFullFrameCount(c.admission.Limits)
	if count < 1 {
		return errors.New("hardware admission has no room for held-result qualification")
	}
	var held []*fd.Result
	defer func() {
		for _, result := range held {
			result.Close()
		}
	}()
	// Each request owns a separate full-size artifact allocation. Reuse the
	// just-proved exact input, not an unready metadata-only future input.
	// The pressure is real RAM/byte quota, not a claim of distinct input frames.
	stream, last := previous.Stream, previous.DeliveryID
	var pressureErr error
	for len(held) < count {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return err
		}
		request := fd.Request{ID: id, Frame: fd.DeliveryRef{Fence: c.binding.Fence(), Ref: fa.DeliveryRef{Stream: stream, Epoch: previous.PublicationEpoch, FrameID: previous.DeliveryID, Graph: previous.Graph}}, Source: previous.Source, Surface: "source_decoded", Regions: []fa.Region{{ID: "held", FullFrame: true}}, DeadlineNS: b.clock.NowNS() + 100_000_000}
		started := b.clock.NowNS()
		result, err := b.runtime.Extract(ctx, request)
		for errors.Is(err, fa.CapacityRejected) && b.clock.NowNS() < request.DeadlineNS && ctx.Err() == nil {
			time.Sleep(2 * time.Millisecond)
			result, err = b.runtime.Extract(ctx, request)
		}
		if errors.Is(err, fa.CapacityRejected) && result == nil && len(held) > 0 {
			// Ceilings are not eager reservations. Contiguous arena space can
			// run out before the byte/result ceilings: explicit rejection must
			// remain bounded and must not pin or stall the producer.
			pressureErr = err
			t.Logf("hardware_arena_pressure_rejected=true held=%d configured_full_frame_ceiling=%d elapsed_us=%d", len(held), count, (b.clock.NowNS()-started)/1000)
			break
		}
		if err != nil {
			return fmt.Errorf("held=%d target=%d artifact_byte_limit=%d full_frame_byte_limit=%d origin=%d elapsed_us=%d: %w", len(held), count, c.admission.Limits.MaxArtifactBytes, c.admission.Limits.MaxFullFrameBytes, previous.OriginSequence, (b.clock.NowNS()-started)/1000, err)
		}
		held = append(held, result)
		if err := result.Visit(0, func(a fd.Artifact, _ []byte) error {
			if a.State != fa.Complete || a.Proof.Core.Origin.Sequence != previous.OriginSequence {
				return errors.New("held hardware artifact lacks exact origin")
			}
			return fd.VerifyDomainProof(a.Proof, c.binding.Fence())
		}); err != nil {
			return err
		}
		last = previous.DeliveryID
	}
	// Full frames exhaust the byte quota before the result-count quota on the
	// production profile. Use the just-proved retained origin, not a newer
	// metadata-only origin whose optional native pixels might not be present.
	if pressureErr == nil {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return err
		}
		q := fd.Request{ID: id, Frame: fd.DeliveryRef{Fence: c.binding.Fence(), Ref: fa.DeliveryRef{Stream: stream, Epoch: previous.PublicationEpoch, FrameID: previous.DeliveryID, Graph: previous.Graph}}, Source: previous.Source, Surface: "source_decoded", Regions: []fa.Region{{ID: "over_quota", FullFrame: true}}, DeadlineNS: b.clock.NowNS() + 100_000_000}
		extra, err := b.runtime.Extract(ctx, q)
		if extra != nil {
			extra.Close()
		}
		pressureErr = err
	}
	if !errors.Is(pressureErr, fa.CapacityRejected) {
		return fmt.Errorf("hardware arena/quota pressure did not explicitly reject: %w", pressureErr)
	}
	beforeSurfaces := c.surfaces.Stats().StagedFrames
	beforeResources := b.runtime.Stats()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(250 * time.Millisecond):
	}
	var next wire.Header
	if err := c.Snapshot(ctx, stream, last, func(_ context.Context, h wire.Header, _ []byte) error { next = h; return nil }); err != nil {
		return err
	}
	if next.OriginSequence <= previous.OriginSequence || next.SourceEpoch != previous.SourceEpoch || next.Source != previous.Source {
		return errors.New("physical capture did not advance under held-result pressure")
	}
	afterSurfaces := c.surfaces.Stats().StagedFrames
	afterResources := b.runtime.Stats()
	if afterSurfaces <= beforeSurfaces || afterResources.ReservedMemoryBytes > beforeResources.ReservedMemoryBytes {
		return fmt.Errorf("native FAP stopped or resources grew under pressure: staged=%d->%d reserved=%d->%d", beforeSurfaces, afterSurfaces, beforeResources.ReservedMemoryBytes, afterResources.ReservedMemoryBytes)
	}
	t.Logf("hardware_held_results=%d configured_result_limit=%d artifact_byte_limit=%d full_frame_byte_limit=%d retained_input_origin=%d source_advanced=%d native_surfaces_advanced=%d reserved_memory_bytes=%d pressure_error=%v; no claim of eager quota reservation, no driver fault injection", len(held), c.admission.Limits.MaxResults, c.admission.Limits.MaxArtifactBytes, c.admission.Limits.MaxFullFrameBytes, previous.OriginSequence, next.OriginSequence-previous.OriginSequence, afterSurfaces-beforeSurfaces, afterResources.ReservedMemoryBytes, pressureErr)
	return nil
}

func hardwareHeldFullFrameCount(l fa.Limits) int {
	if l.MaxFullFrameBytes <= 0 {
		return 0
	}
	return min(8, l.MaxResults-1, l.MaxArtifactBytes/l.MaxFullFrameBytes)
}

func TestHardwarePixelHeldCountHonorsBothConfiguredQuotas(t *testing.T) {
	for _, test := range []struct{ results, artifacts, frame, want int }{
		{16, 6 << 20, 3 << 20, 2},
		{16, 32 << 20, 1 << 20, 8},
		{4, 32 << 20, 1 << 20, 3},
		{16, 32 << 20, 0, 0},
	} {
		if got := hardwareHeldFullFrameCount(fa.Limits{MaxResults: test.results, MaxArtifactBytes: test.artifacts, MaxFullFrameBytes: test.frame}); got != test.want {
			t.Fatalf("configured quotas %+v: held=%d want=%d", test, got, test.want)
		}
	}
}

func pixelMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestHardwarePixelWorkPatternsExcludeWaitingTimers(t *testing.T) {
	for _, test := range []struct {
		unit string
		want bool
	}{
		{"nemesi-update-check.timer", false},
		{"nemesi-build-cleanup.timer", false},
		{"nemesi-update-check.service", true},
		{"nemesi-update-20261006-example.service", true},
		{"nemesi-build-123-example.service", true},
	} {
		matched := false
		for _, pattern := range hardwarePixelMutatingUnits {
			match, err := filepath.Match(pattern, test.unit)
			pixelMust(t, err)
			matched = matched || match
		}
		if matched != test.want {
			t.Errorf("unit=%s mutating-work=%t want=%t", test.unit, matched, test.want)
		}
	}
}

// A canonical runtime can be inactive while its updater owns a hardware trial.
// These checks fail closed, but are not an atomic maintenance lock: the caller
// still owns exclusive quiescence/restoration throughout the qualification.
func validatePixelQuiescence(states, jobs []byte) error {
	values := strings.Fields(string(states))
	if len(values) != 2 || values[0] != "inactive" || values[1] != "inactive" {
		return errors.New("canonical runtime and release trial must both be inactive; never open a second camera reader")
	}
	if len(bytes.TrimSpace(jobs)) != 0 {
		return errors.New("update/build unit still owns maintenance; never qualify during a deployment")
	}
	return nil
}

func TestHardwarePixelQuiescenceRejectsOtherOwners(t *testing.T) {
	for _, test := range []struct {
		name, states, jobs string
		wantError          bool
	}{
		{"quiesced", "inactive\n\ninactive\n", "", false},
		{"runtime-active", "active\ninactive\n", "", true},
		{"trial-active", "inactive\nactive\n", "", true},
		{"trial-stopping", "inactive\ndeactivating\n", "", true},
		{"missing-state", "inactive\n", "", true},
		{"unknown-state", "inactive\nunknown\n", "", true},
		{"updater-active", "inactive\ninactive\n", "nemesi-update-example.service loaded active running", true},
		{"build-active", "inactive\ninactive\n", "nemesi-build-example.service loaded active running", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validatePixelQuiescence([]byte(test.states), []byte(test.jobs)); (err != nil) != test.wantError {
				t.Fatalf("quiescence error=%v wantError=%t", err, test.wantError)
			}
		})
	}
}

func verifyHardwarePixels(ctx context.Context, b *nativeFrameBootstrap, c *nativeFrameCapture, h wire.Header) error {
	t := c.admission.Deliveries[h.Stream].Transform
	g := t.Full
	if t.Rotation != 0 || t.Mirror || t.Crop != (fa.Rect{X2: g.Width, Y2: g.Height}) || t.PadLeft != 0 || t.PadTop != 0 || t.ResizedWidth != int(h.Width) || t.ResizedHeight != int(h.Height) || g.Width <= int(h.Width) || g.Height <= int(h.Height) {
		return errors.New("requires exact unrotated native-detail profile; other transforms are not qualified")
	}
	ref := fd.DeliveryRef{Fence: c.binding.Fence(), Ref: fa.DeliveryRef{Stream: h.Stream, Epoch: h.PublicationEpoch, FrameID: h.DeliveryID, Graph: h.Graph}}
	extract := func(region fa.Region, consume func(fd.Artifact, []byte) error) error {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return err
		}
		deadline := b.clock.NowNS() + 100_000_000
		req := fd.Request{ID: id, Frame: ref, Source: h.Source, Surface: "source_decoded", Regions: []fa.Region{region}, DeadlineNS: deadline}
		result, err := b.runtime.Extract(ctx, req)
		// Retry only explicit contention for this capture, never reset its budget.
		for errors.Is(err, fa.CapacityRejected) && b.clock.NowNS() < deadline && ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(2 * time.Millisecond):
			}
			result, err = b.runtime.Extract(ctx, req)
		}
		if err != nil {
			return fmt.Errorf("region=%s origin=%d phase=extract: %w", region.ID, h.OriginSequence, err)
		}
		defer result.Close()
		return result.Visit(0, func(a fd.Artifact, pixels []byte) error {
			if a.State != fa.Complete {
				return fmt.Errorf("region=%s incomplete=%s", region.ID, a.State)
			}
			if err := fd.VerifyDomainProof(a.Proof, c.binding.Fence()); err != nil {
				return err
			}
			if a.Region != region.ID || a.Proof.Core.Version != 1 || a.Proof.Core.RequestID != id || a.Proof.Core.Region != region.ID || a.Proof.Core.Surface != req.Surface || a.Proof.Core.Delivery != ref.Ref || a.Proof.Core.Origin.Graph != h.Graph || a.Proof.Core.Origin.Source != h.Source || a.Proof.Core.Origin.MediaEpoch != h.SourceEpoch || a.Proof.Core.Origin.Sequence != h.OriginSequence || a.Proof.Core.Transform != h.Transform || a.Proof.Core.PayloadSHA256 != fa.Digest(sha256.Sum256(pixels)) {
				return errors.New("pixel proof identity/digest mismatch")
			}
			return consume(a, pixels)
		})
	}
	var full []byte
	if err := extract(fa.Region{ID: "full", FullFrame: true}, func(a fd.Artifact, p []byte) error {
		if a.Proof.Core.Geometry != g || len(p) > 4<<20 {
			return errors.New("full surface geometry/budget")
		}
		full = append([]byte(nil), p...)
		return nil
	}); err != nil {
		return err
	}
	w, height := float64(h.Width), float64(h.Height)
	for _, region := range []fa.Region{{ID: "top_left", BBox: [4]float64{0, 0, w / 4, height / 4}}, {ID: "center", BBox: [4]float64{w / 4, height / 4, 3 * w / 4, 3 * height / 4}}, {ID: "bottom_right", BBox: [4]float64{3 * w / 4, 3 * height / 4, w, height}}} {
		// Independent exact-scale oracle, not production MapRegion.
		r := fa.Rect{X1: 2 * int(math.Floor(region.BBox[0]*float64(g.Width)/w/2)), Y1: 2 * int(math.Floor(region.BBox[1]*float64(g.Height)/height/2)), X2: 2 * int(math.Ceil(region.BBox[2]*float64(g.Width)/w/2)), Y2: 2 * int(math.Ceil(region.BBox[3]*float64(g.Height)/height/2))}
		if err := extract(region, func(a fd.Artifact, p []byte) error {
			if a.Proof.Core.BBox != r || a.Proof.Core.Geometry != (fa.Geometry{Width: r.X2 - r.X1, Height: r.Y2 - r.Y1, Stride: r.X2 - r.X1}) {
				return errors.New("hardware crop coordinates differ")
			}
			return compareHardwareNV12(full, p, g, r)
		}); err != nil {
			return fmt.Errorf("region=%s origin=%d phase=pixels: %w", region.ID, h.OriginSequence, err)
		}
	}
	return nil
}

func compareHardwareNV12(full, crop []byte, g fa.Geometry, r fa.Rect) error {
	size, err := g.Bytes()
	w, h := r.X2-r.X1, r.Y2-r.Y1
	if err != nil || len(full) != size || r.X1 < 0 || r.Y1 < 0 || r.X2 > g.Width || r.Y2 > g.Height || w <= 0 || h <= 0 || r.X1%2 != 0 || r.Y1%2 != 0 || w%2 != 0 || h%2 != 0 || len(crop) != w*h*3/2 {
		return errors.New("NV12 oracle extent")
	}
	for y := 0; y < h; y++ {
		start := (r.Y1+y)*g.Stride + r.X1
		if !bytes.Equal(crop[y*w:(y+1)*w], full[start:start+w]) {
			return fmt.Errorf("Y pixel mismatch row=%d", y)
		}
	}
	for y := 0; y < h/2; y++ {
		start := g.Stride*g.Height + (r.Y1/2+y)*g.Stride + r.X1
		if !bytes.Equal(crop[w*h+y*w:w*h+(y+1)*w], full[start:start+w]) {
			return fmt.Errorf("UV pixel mismatch row=%d", y)
		}
	}
	return nil
}

func TestHardwarePixelOracleDetectsWrongPlanes(t *testing.T) {
	g := fa.Geometry{Width: 8, Height: 8, Stride: 10}
	full := make([]byte, 120)
	for i := range full {
		full[i] = byte(i)
	}
	r := fa.Rect{X1: 2, Y1: 2, X2: 6, Y2: 6}
	expected := []byte{22, 23, 24, 25, 32, 33, 34, 35, 42, 43, 44, 45, 52, 53, 54, 55, 92, 93, 94, 95, 102, 103, 104, 105}
	pixelMust(t, compareHardwareNV12(full, expected, g, r))
	for _, offset := range []int{0, 15, 16, 23} {
		bad := append([]byte(nil), expected...)
		bad[offset] ^= 1
		if compareHardwareNV12(full, bad, g, r) == nil {
			t.Fatalf("corruption accepted offset=%d", offset)
		}
	}
	for _, bad := range []fa.Rect{{X1: -2, Y1: 2, X2: 2, Y2: 6}, {X1: 2, Y1: 2, X2: 10, Y2: 6}, {X1: 1, Y1: 2, X2: 5, Y2: 6}, {X1: 2, Y1: 2, X2: 6, Y2: 5}} {
		if compareHardwareNV12(full, expected, g, bad) == nil {
			t.Fatalf("invalid rectangle accepted: %+v", bad)
		}
	}
}
