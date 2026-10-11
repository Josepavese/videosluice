//go:build linux && cgo

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Josepavese/videosluice/config"
	fa "github.com/Josepavese/videosluice/frameaccess"
	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/internal/source"
)

func productSourcePlan() source.BrokerMuxPlan {
	return source.BrokerMuxPlan{SourceID: "camera-any", Transport: source.TransportV4L2UVC,
		WorkerProvider: "native_sp", DecodeProvider: "native_sp", RequireHWCodec: true,
		V4L2BridgePath: "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge", CaptureFormat: "h265",
		H265DecodeHWAvailable: true, CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
		AnalysisEnabled: true, AnalysisStreamID: "vision-detail", SubWidth: 640, SubHeight: 360, SubFPS: 20,
		VideoOrientation: "normal", CaptureAspectPolicy: "exact"}
}

func TestProductNativeCompositionUsesActualGeometryAndPublicStream(t *testing.T) {
	for _, size := range [][2]int{{1280, 720}, {1920, 1080}} {
		p := productSourcePlan()
		p.CaptureWidth, p.CaptureHeight = size[0], size[1]
		a, err := composeRDKFrameSource(p)
		if err != nil {
			t.Fatal(err)
		}
		n := size[0] * size[1] * 3 / 2
		if a.Layout.Source != p.SourceID || a.Layout.Streams[0] != p.AnalysisStreamID || a.Limits.MaxFrameBytes != n || a.CaptureBytes != 4*n+2*(640*360*3/2)+(5<<20) {
			t.Fatalf("composition lost source/size: %+v", a.Layout)
		}
		if a.Deliveries[p.AnalysisStreamID].MaxPayload != 640*360*3/2 {
			t.Fatal("wrong delivery geometry")
		}
		if a.OutputCapacity != wire.HeaderSize+640*360*3/2 || a.OutputMailboxBytes < 2*a.OutputCapacity || a.MailboxBytes < 2*(wire.HeaderSize+n) {
			t.Fatal("independent native/source-output RAM slots are not admitted", a.OutputCapacity, a.OutputMailboxBytes, a.MailboxBytes)
		}
		if a.StartupNS != 5_000_000_000 || a.ReceiveNS != frameProductReceiveNS || a.CopyNS != 100_000_000 {
			t.Fatal("startup/frame/copy budgets are not independent", a.StartupNS, a.ReceiveNS, a.CopyNS)
		}
		if _, err := a.Execution(context.Background()); err != nil {
			t.Fatal(err)
		}
		changed := p
		changed.MediaEpoch, changed.MainActive = 912, true
		b, err := composeRDKFrameSource(changed)
		if err != nil || a.Layout.Graph != b.Layout.Graph {
			t.Fatal("ephemeral state changes graph", err)
		}
	}
}

func TestProductNativeCompositionRejectsUnsupportedProfile(t *testing.T) {
	for name, change := range map[string]func(*source.BrokerMuxPlan){
		"software":           func(p *source.BrokerMuxPlan) { p.AllowSWFallback = true },
		"decode-unavailable": func(p *source.BrokerMuxPlan) { p.H265DecodeHWAvailable = false },
		"provider":           func(p *source.BrokerMuxPlan) { p.DecodeProvider = "auto" },
		"no-analysis":        func(p *source.BrokerMuxPlan) { p.AnalysisEnabled = false },
		"bad-id":             func(p *source.BrokerMuxPlan) { p.AnalysisStreamID = "bad/name" },
		"oversized":          func(p *source.BrokerMuxPlan) { p.CaptureWidth, p.CaptureHeight = 3840, 2160 },
		"odd":                func(p *source.BrokerMuxPlan) { p.CaptureWidth = 1919 },
	} {
		t.Run(name, func(t *testing.T) {
			p := productSourcePlan()
			change(&p)
			if _, err := composeRDKFrameSource(p); err == nil {
				t.Fatal("unsupported source admitted")
			}
		})
	}
}

func TestProductToneMappingPreservesNativeSourceAndFencesProcessingGraph(t *testing.T) {
	for _, codec := range []string{"mjpeg", "h265", "h264"} {
		for _, accelerator := range []string{"opencl", "cpu", "auto"} {
			p := productSourcePlan()
			p.CaptureFormat = codec
			p.MJPEGDecodeHWAvailable, p.H264DecodeHWAvailable = true, true
			off, err := composeRDKFrameSource(p)
			if err != nil {
				t.Fatal(err)
			}
			p.ToneMappingEnabled, p.ToneMappingAccelerator = true, accelerator
			on, err := composeRDKFrameSource(p)
			if err != nil {
				t.Fatal(codec, accelerator, err)
			}
			if on.Layout.Graph == off.Layout.Graph || on.Layout.Geometry != off.Layout.Geometry ||
				on.Layout.Surface != "source_decoded" || on.SurfaceDigest != off.SurfaceDigest {
				t.Fatal("tone processing lost the native source or graph fence", codec, accelerator)
			}
			if on.Deliveries[p.AnalysisStreamID].Transform != off.Deliveries[p.AnalysisStreamID].Transform {
				t.Fatal("tone processing changed spatial correlation", codec, accelerator)
			}
		}
	}
}

func TestProductNV12Transforms(t *testing.T) {
	full := fa.Geometry{Width: 1920, Height: 1080, Stride: 1920}
	for _, orientation := range []string{"normal", "rotate_180", "mirror_horizontal", "mirror_vertical"} {
		for _, aspect := range []string{"exact", "crop", "letterbox"} {
			transform, err := nativeNV12Transform(full, 640, 480, orientation, aspect)
			if err != nil {
				t.Fatal(orientation, aspect, err)
			}
			if aspect == "crop" && transform.Crop != (fa.Rect{X1: 240, X2: 1680, Y2: 1080}) {
				t.Fatal("incorrect crop", transform)
			}
			if aspect == "letterbox" && (transform.ResizedHeight != 360 || transform.PadTop != 60) {
				t.Fatal("incorrect padding", transform)
			}
		}
	}
}

func TestProductConfigCheckDoesNotRequireLiveBootstrap(t *testing.T) {
	dir := t.TempDir()
	cfg, err := config.Normalize(config.Config{SchemaVersion: config.SchemaVersion,
		Sources: map[string]config.SourceConfig{"camera-any": {Identity: "camera-any", Transport: "v4l2_uvc", Device: "/dev/video0", Format: "h265", Width: 1920, Height: 1080, FPS: 30}},
		Streams: map[string]config.StreamConfig{"vision-detail": {Type: "analysis", SourceRef: "camera-any", PixelFormat: "nv12", Width: 640, Height: 360, FPS: 20, SharedMemory: filepath.Join(dir, "analysis.nv12")}},
		Runtime: config.RuntimeConfig{RuntimeDirectory: dir, StateDirectory: dir, FrameAccess: &config.FrameAccessConfig{CopyProvider: "cpu_nv12_copy", Readers: []config.FrameReaderConfig{{ID: "reader-any", Stream: "vision-detail", DeliverySocket: filepath.Join(dir, "frames.sock"), ROISocket: filepath.Join(dir, "roi.sock")}}}}})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if err := runDaemon(context.Background(), []string{"--config", path, "--check-config"}, io.Discard, nil); err != nil {
		t.Fatal(err)
	}
	if err := runDaemon(context.Background(), []string{"--config", path}, io.Discard, nil); err == nil {
		t.Fatal("unbound frame start accepted")
	}
}

func TestProductConfigurationSelectsReadersNotSourceNames(t *testing.T) {
	b, _ := captureFixture(t)
	cfg, err := config.Normalize(config.Config{SchemaVersion: config.SchemaVersion,
		Sources: map[string]config.SourceConfig{"camera-any": {Identity: "camera-any", Transport: "v4l2_uvc", Device: "/dev/video0", Format: "h265", Width: 1920, Height: 1080, FPS: 30}},
		Streams: map[string]config.StreamConfig{"vision-detail": {Type: "analysis", SourceRef: "camera-any", PixelFormat: "nv12", Width: 640, Height: 360, FPS: 20, SharedMemory: "/run/video-broker/analysis.nv12"}},
		Runtime: config.RuntimeConfig{RuntimeDirectory: "/run/video-broker", StateDirectory: "/var/lib/video-broker", FrameAccess: &config.FrameAccessConfig{CopyProvider: "cpu_nv12_copy", Readers: []config.FrameReaderConfig{{ID: "reader-any", Stream: "vision-detail", DeliverySocket: "/run/video-broker/frames.sock", ROISocket: "/run/video-broker/roi.sock"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Configure(cfg); err != nil {
		t.Fatal(err)
	}
	if len(b.admission.readers) != 2 || b.admission.readers[0].Grant != "reader-any" || b.admission.readers[0].Stream != "vision-detail" {
		t.Fatal("hardcoded reader")
	}
	if _, err := b.admission.source(context.Background(), source.BrokerMuxPlan{}); !errors.Is(err, source.ErrFrameCaptureNotRequested) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.admission.source(ctx, source.BrokerMuxPlan{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := b.Configure(cfg); err == nil {
		t.Fatal("configuration rebound")
	}
}
