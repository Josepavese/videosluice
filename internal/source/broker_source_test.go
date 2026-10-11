package source

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestBuildBrokerMuxCommandHBN(t *testing.T) {
	cmd, adapterID, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled:                   true,
		LiveCodec:                     "h265",
		AnalysisEnabled:               true,
		ArchiveEnabled:                true,
		Transport:                     "vendor_hbn",
		WorkerProvider:                "native_sp",
		HBNBridgePath:                 "/opt/video-broker/bin/rdkx5_hbn_mux_bridge",
		SensorPrefix:                  "imx219",
		CameraHostIndex:               1,
		CaptureWidth:                  1920,
		CaptureHeight:                 1080,
		CaptureFPS:                    30,
		ArchiveWidth:                  1920,
		ArchiveHeight:                 1080,
		ArchiveFPS:                    30,
		ArchiveContent:                "processed",
		ArchiveCodec:                  "h265",
		H264EncodeHWAvailable:         true,
		H265EncodeHWAvailable:         true,
		MainWidth:                     1920,
		MainHeight:                    1080,
		MainFPS:                       20,
		SubWidth:                      640,
		SubHeight:                     360,
		SubFPS:                        30,
		WarmupSec:                     5,
		LiveBitrateKbps:               8192,
		ArchiveBitrateKbps:            8192,
		MainActive:                    true,
		VideoOrientation:              "rotate_180",
		ToneMappingEnabled:            true,
		ToneMappingMode:               "auto",
		ToneMappingAccelerator:        "opencl",
		ToneMappingTarget:             96,
		ToneMappingHysteresis:         12,
		ToneMappingGridColumns:        16,
		ToneMappingGridRows:           9,
		ToneMappingStatisticsHz:       8,
		ToneMappingMaxShadowGain:      1.8,
		ToneMappingMaxHighlightReduce: 0.30,
		ToneMappingTemporalAlpha:      0.18,
		ToneMappingNoiseGuard:         true,
		QualityMonitorEnabled:         true,
		QualityMonitorReportEvery:     30,
		QualityMonitorSampleStride:    8,
		QualityMonitorStatePath:       "/run/video-broker/video-quality.json",
	})
	if err != nil {
		t.Fatalf("BuildBrokerMuxCommand failed: %v", err)
	}
	if adapterID != "vendor_hbn" {
		t.Fatalf("unexpected adapter id: %s", adapterID)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"/opt/video-broker/bin/rdkx5_hbn_mux_bridge",
		"--video-index 1",
		"--sensor-prefix imx219",
		"--live-codec h265",
		"--live-encode-bitrate-kbps 8192",
		"--archive-encode-bitrate-kbps 8192",
		"--archive-width 1920",
		"--archive-height 1080",
		"--archive-fps 30",
		"--archive-content processed",
		"--archive-codec h265",
		"--video-orientation rotate_180",
		"--tone-mapping-mode auto",
		"--tone-mapping-accelerator opencl",
		"--tone-mapping-grid-columns 16",
		"--tone-mapping-grid-rows 9",
		"--quality-monitor-enabled true",
		"--quality-monitor-report-every 30",
		"--quality-monitor-sample-stride 8",
		"--quality-monitor-state-path /run/video-broker/video-quality.json",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("expected args to contain %q, got %q", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandHBNPublishesCanonicalCatalogSurfaces(t *testing.T) {
	base := BrokerMuxPlan{
		LiveEnabled: true, LiveCodec: "h265", AnalysisEnabled: true,
		Transport: "vendor_hbn", WorkerProvider: "native_sp",
		HBNBridgePath: "/opt/video-broker/bin/rdkx5_hbn_mux_bridge",
		SensorPrefix:  "imx219", H265EncodeHWAvailable: true,
		CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
		MainWidth: 1280, MainHeight: 720, MainFPS: 20,
		SubWidth: 640, SubHeight: 360, SubFPS: 20,
		LiveBitrateKbps: 4096,
		AdmittedSurfaces: []BrokerMuxSurface{
			{Width: 1920, Height: 1080},
			{Width: 1280, Height: 720},
			{Width: 640, Height: 360},
			{Width: 1280, Height: 720},
		},
	}
	cmd, _, err := BuildBrokerMuxCommand(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(cmd.Args, " ")
	ordered := []string{
		"--admitted-surface 640x360",
		"--admitted-surface 1280x720",
		"--admitted-surface 1920x1080",
	}
	previous := -1
	for _, want := range ordered {
		index := strings.Index(args, want)
		if index <= previous {
			t.Fatalf("surface catalog is missing or not canonical: %q", args)
		}
		previous = index
	}
	if strings.Count(args, "--admitted-surface 1280x720") != 1 {
		t.Fatalf("surface catalog was not de-duplicated: %q", args)
	}

	tooMany := base
	tooMany.AdmittedSurfaces = []BrokerMuxSurface{
		{Width: 320, Height: 240}, {Width: 640, Height: 360},
		{Width: 800, Height: 600}, {Width: 960, Height: 540},
		{Width: 1280, Height: 720}, {Width: 1920, Height: 1080},
	}
	if _, _, err := BuildBrokerMuxCommand(context.Background(), tooMany); err == nil ||
		!strings.Contains(err.Error(), "hardware limit is 5") {
		t.Fatalf("HBN surface overflow accepted: %v", err)
	}

	missingCurrent := base
	missingCurrent.AdmittedSurfaces = []BrokerMuxSurface{{Width: 640, Height: 360}}
	if _, _, err := BuildBrokerMuxCommand(context.Background(), missingCurrent); err == nil ||
		!strings.Contains(err.Error(), "active live surface") {
		t.Fatalf("active surface outside catalog accepted: %v", err)
	}
}

func TestBuildBrokerMuxCommandHBNPublishesWorkerFence(t *testing.T) {
	plan := BrokerMuxPlan{
		SourceID: "front", MediaEpoch: 41,
		LiveEnabled: true, LiveCodec: "h265",
		Transport: "vendor_hbn", WorkerProvider: "native_sp",
		HBNBridgePath: "/opt/video-broker/bin/rdkx5_hbn_mux_bridge",
		SensorPrefix:  "imx219", H265EncodeHWAvailable: true,
		CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 20,
		MainWidth: 1280, MainHeight: 720, MainFPS: 20,
		LiveBitrateKbps:  4096,
		AdmittedSurfaces: []BrokerMuxSurface{{Width: 1280, Height: 720}},
	}
	cmd, _, err := BuildBrokerMuxCommand(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, expected := range []string{"--source-id front", "--media-epoch 41"} {
		if !strings.Contains(args, expected) {
			t.Fatalf("HBN worker fence missing %q from %q", expected, args)
		}
	}
	plan.MediaEpoch = 0
	if _, _, err := BuildBrokerMuxCommand(context.Background(), plan); err == nil ||
		!strings.Contains(err.Error(), "identity fence is incomplete") {
		t.Fatalf("incomplete HBN worker fence accepted: %v", err)
	}
	plan.MediaEpoch = 41
	plan.AdmittedSurfaces = nil
	if _, _, err := BuildBrokerMuxCommand(context.Background(), plan); err == nil ||
		!strings.Contains(err.Error(), "requires admitted catalog surfaces") {
		t.Fatalf("fenced HBN worker without routed surfaces accepted: %v", err)
	}
}

func TestBuildBrokerMuxCommandHBNRejectsFalseCaptureQualityGeometry(t *testing.T) {
	_, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true,
		Transport: "vendor_hbn", WorkerProvider: "native_sp", HBNBridgePath: "/opt/video-broker/bin/rdkx5_hbn_mux_bridge",
		SensorPrefix: "imx219", H264EncodeHWAvailable: true, H265EncodeHWAvailable: true,
		CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
		MainWidth: 1280, MainHeight: 720, MainFPS: 20,
		SubWidth: 640, SubHeight: 360, SubFPS: 20,
		ArchiveWidth: 1280, ArchiveHeight: 720, ArchiveFPS: 30,
		ArchiveCodec: "h265", QualityMonitorEnabled: true,
		QualityMonitorStatePath: "/run/video-broker/video-quality.json",
	})
	if err == nil || !strings.Contains(err.Error(), "capture-sized") {
		t.Fatalf("expected truthful HBN quality geometry rejection, got %v", err)
	}
}

func TestBuildBrokerMuxCommandHBNRejectsUnsupportedArchiveAVBR(t *testing.T) {
	_, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		ArchiveEnabled: true, ArchiveRateControl: "avbr", Transport: "vendor_hbn",
	})
	if err == nil || !strings.Contains(err.Error(), "does not expose configurable archive AVBR") {
		t.Fatalf("unsupported HBN AVBR was not rejected: %v", err)
	}
}

func TestBuildBrokerMuxCommandHBNPreservesQualityIntentAndRequiresCaptureSizedSamplingRole(t *testing.T) {
	base := BrokerMuxPlan{
		LiveEnabled: true, LiveCodec: "h264", Transport: "vendor_hbn", WorkerProvider: "native_sp",
		HBNBridgePath: "/opt/video-broker/bin/rdkx5_hbn_mux_bridge", SensorPrefix: "imx219",
		H264EncodeHWAvailable: true,
		CaptureWidth:          1920, CaptureHeight: 1080, CaptureFPS: 30,
		MainWidth: 1920, MainHeight: 1080, MainFPS: 20,
		SubWidth: 640, SubHeight: 360, SubFPS: 20,
		QualityMonitorEnabled: true, QualityMonitorStatePath: "/run/video-broker/video-quality.json",
	}
	if cmd, _, err := BuildBrokerMuxCommand(context.Background(), base); err != nil || !strings.Contains(strings.Join(cmd.Args, " "), "--quality-monitor-enabled true") {
		t.Fatalf("inactive quality intent was not passed to the capture owner: %v", err)
	}
	base.LiveEnabled = false
	base.AnalysisEnabled = true
	if _, _, err := BuildBrokerMuxCommand(context.Background(), base); err == nil || !strings.Contains(err.Error(), "capture-sized") {
		t.Fatalf("quality monitor used geometry from a disabled live role: %v", err)
	}
}

func TestBuildBrokerMuxCommandCarriesExactConfiguredRoleSet(t *testing.T) {
	tests := []struct {
		name string
		plan BrokerMuxPlan
		want string
	}{
		{
			name: "HBN analysis only",
			plan: BrokerMuxPlan{
				AnalysisEnabled: true, Transport: TransportVendorHBN, WorkerProvider: "native_sp",
				HBNBridgePath: "/opt/video-broker/bin/rdkx5_hbn_mux_bridge", SensorPrefix: "imx219",
				CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
				SubWidth: 640, SubHeight: 360, SubFPS: 15,
			},
			want: "--live-enabled false --live-codec h265 --analysis-enabled true --archive-enabled false",
		},
		{
			name: "native V4L2 archive only",
			plan: BrokerMuxPlan{
				ArchiveEnabled: true, Transport: TransportV4L2UVC, WorkerProvider: "native_sp",
				V4L2BridgePath: "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge", CaptureDevicePath: "/dev/video0",
				CaptureFormat: "yuyv", CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 30,
				ArchiveWidth: 1280, ArchiveHeight: 720, ArchiveFPS: 30,
				ArchiveCodec: "h265", ArchiveContent: "processed", H265EncodeHWAvailable: true,
			},
			want: "--live-enabled false --live-codec h265 --analysis-enabled false --archive-enabled true",
		},
		{
			name: "portable V4L2 live only",
			plan: BrokerMuxPlan{
				LiveEnabled: true, LiveCodec: "h265", Transport: TransportV4L2, WorkerProvider: "gstreamer",
				GStreamerBridgePath: "/opt/video-broker/bin/video-ingest-gst", CaptureDevicePath: "/dev/video2",
				CaptureFormat: "nv12", CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 30,
				MainWidth: 1280, MainHeight: 720, MainFPS: 20,
				EncodeProvider: "software", DecodeProvider: "software", AllowSWFallback: true,
			},
			want: "--live-enabled true --live-codec h265 --analysis-enabled false --archive-enabled false",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, _, err := BuildBrokerMuxCommand(context.Background(), test.plan)
			if err != nil {
				t.Fatal(err)
			}
			args := strings.Join(cmd.Args, " ")
			if !strings.Contains(args, test.want) {
				t.Fatalf("worker command lost exact role set %q: %s", test.want, args)
			}
		})
	}
}

func TestBuildBrokerMuxCommandCoversEveryRoleCombinationOnEveryWorker(t *testing.T) {
	transports := []struct {
		name string
		plan BrokerMuxPlan
	}{
		{
			name: "hbn-native",
			plan: BrokerMuxPlan{
				Transport: TransportVendorHBN, WorkerProvider: "native_sp",
				HBNBridgePath: "/opt/video-broker/bin/rdkx5_hbn_mux_bridge", SensorPrefix: "imx219",
				H265EncodeHWAvailable: true,
			},
		},
		{
			name: "uvc-native",
			plan: BrokerMuxPlan{
				Transport: TransportV4L2UVC, WorkerProvider: "native_sp",
				V4L2BridgePath:    "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
				CaptureDevicePath: "/dev/video0", CaptureFormat: "yuyv",
				H265EncodeHWAvailable: true,
			},
		},
		{
			name: "portable",
			plan: BrokerMuxPlan{
				Transport: TransportV4L2, WorkerProvider: "gstreamer",
				GStreamerBridgePath: "/opt/video-broker/bin/video-ingest-gst",
				CaptureDevicePath:   "/dev/video0", CaptureFormat: "nv12",
				EncodeProvider: "software", DecodeProvider: "software", AllowSWFallback: true,
			},
		},
	}
	for _, transport := range transports {
		for mask := 1; mask < 8; mask++ {
			t.Run(fmt.Sprintf("%s-mask-%d", transport.name, mask), func(t *testing.T) {
				plan := transport.plan
				plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS = 1280, 720, 30
				plan.LiveEnabled, plan.AnalysisEnabled, plan.ArchiveEnabled = mask&1 != 0, mask&2 != 0, mask&4 != 0
				plan.LiveCodec = "h265"
				plan.MainWidth, plan.MainHeight, plan.MainFPS, plan.LiveBitrateKbps = 1280, 720, 15, 4096
				plan.SubWidth, plan.SubHeight, plan.SubFPS = 640, 360, 15
				plan.ArchiveWidth, plan.ArchiveHeight, plan.ArchiveFPS = 1280, 720, 30
				plan.ArchiveCodec, plan.ArchiveContent, plan.ArchiveBitrateKbps = "h265", "adaptive", 6144
				cmd, _, err := BuildBrokerMuxCommand(context.Background(), plan)
				if err != nil {
					t.Fatal(err)
				}
				args := strings.Join(cmd.Args, " ")
				for _, role := range []struct {
					name    string
					enabled bool
				}{
					{"live", plan.LiveEnabled},
					{"analysis", plan.AnalysisEnabled},
					{"archive", plan.ArchiveEnabled},
				} {
					want := fmt.Sprintf("--%s-enabled %t", role.name, role.enabled)
					if !strings.Contains(args, want) {
						t.Fatalf("missing exact role flag %q in %s", want, args)
					}
				}
			})
		}
	}
}

func TestBuildBrokerMuxCommandRejectsImplicitV4L2Device(t *testing.T) {
	for _, test := range []struct {
		name string
		plan BrokerMuxPlan
	}{
		{
			name: "native",
			plan: BrokerMuxPlan{
				Transport: TransportV4L2UVC, WorkerProvider: "native_sp",
				V4L2BridgePath: "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
			},
		},
		{
			name: "portable",
			plan: BrokerMuxPlan{
				Transport: TransportV4L2, WorkerProvider: "gstreamer",
				GStreamerBridgePath: "/opt/video-broker/bin/video-ingest-gst",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := test.plan
			plan.AnalysisEnabled = true
			plan.CaptureFormat = "nv12"
			plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS = 1280, 720, 30
			plan.SubWidth, plan.SubHeight, plan.SubFPS = 640, 360, 15
			if _, _, err := BuildBrokerMuxCommand(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "explicit capture device") {
				t.Fatalf("implicit V4L2 device was not rejected: %v", err)
			}
		})
	}
}

func TestBuildBrokerMuxCommandLibcamera(t *testing.T) {
	cmd, adapterID, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled:                true,
		LiveCodec:                  "h265",
		AnalysisEnabled:            true,
		ArchiveEnabled:             true,
		Transport:                  TransportLibcamera,
		WorkerProvider:             "gstreamer",
		GStreamerBridgePath:        "/opt/video-broker/bin/video-ingest-gst",
		CaptureDevicePath:          "/base/soc/i2c0mux/i2c@1/imx708@1a",
		CaptureFormat:              "auto",
		EncodeProvider:             "gstreamer_mpp",
		DecodeProvider:             "gstreamer_mpp",
		H264EncodeHWAvailable:      true,
		H265EncodeHWAvailable:      true,
		CaptureWidth:               1920,
		CaptureHeight:              1080,
		CaptureFPS:                 30,
		MainWidth:                  1280,
		MainHeight:                 720,
		MainFPS:                    20,
		SubWidth:                   640,
		SubHeight:                  360,
		SubFPS:                     20,
		ArchiveWidth:               1920,
		ArchiveHeight:              1080,
		ArchiveFPS:                 30,
		ArchiveCodec:               "h265",
		ArchiveContent:             "adaptive",
		QualityMonitorEnabled:      true,
		QualityMonitorReportEvery:  300,
		QualityMonitorSampleStride: 8,
		QualityMonitorStatePath:    "/run/video-broker/quality.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if adapterID != TransportLibcamera || cmd.Path != "/opt/video-broker/bin/video-ingest-gst" {
		t.Fatalf("unexpected libcamera adapter command id=%q path=%q", adapterID, cmd.Path)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"--source-transport libcamera",
		"--device /base/soc/i2c0mux/i2c@1/imx708@1a",
		"--input-format nv12",
		"--archive-codec h265",
		"--encode-provider gstreamer_mpp",
		"--archive-encode-provider gstreamer_mpp",
		"--quality-monitor-state-path /run/video-broker/quality.json",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("libcamera command missing %q: %s", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandLibcameraFallsBackPerCodecRole(t *testing.T) {
	cmd, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true,
		LiveCodec: "h264",
		Transport: TransportLibcamera, WorkerProvider: "gstreamer", GStreamerBridgePath: "/opt/video-broker/bin/video-ingest-gst",
		CaptureFormat: "nv12", EncodeProvider: "gstreamer_mpp", DecodeProvider: "gstreamer_mpp",
		AllowSWFallback: true, H264EncodeHWAvailable: false, H265EncodeHWAvailable: true,
		CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
		MainWidth: 1280, MainHeight: 720, MainFPS: 20,
		SubWidth: 640, SubHeight: 360, SubFPS: 20,
		ArchiveWidth: 1920, ArchiveHeight: 1080, ArchiveFPS: 30,
		ArchiveCodec: "h265", ArchiveContent: "adaptive",
	})
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{"--encode-provider software", "--archive-encode-provider gstreamer_mpp"} {
		if !strings.Contains(args, want) {
			t.Fatalf("role-specific libcamera provider missing %q: %s", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandV4L2UVC(t *testing.T) {
	cmd, adapterID, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		SourceID:               "front",
		MediaEpoch:             41,
		LiveEnabled:            true,
		LiveCodec:              "h265",
		AnalysisEnabled:        true,
		ArchiveEnabled:         true,
		Transport:              "v4l2_uvc",
		WorkerProvider:         "native_sp",
		V4L2BridgePath:         "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
		CaptureDevicePath:      "/dev/video0",
		CaptureFormat:          "mjpeg",
		AllowSWFallback:        true,
		MJPEGDecodeHWAvailable: true,
		H264EncodeHWAvailable:  true,
		H265EncodeHWAvailable:  true,
		ArchiveCodec:           "h265",
		CaptureWidth:           1280,
		CaptureHeight:          720,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		MainFPS:                30,
		SubWidth:               640,
		SubHeight:              360,
		SubFPS:                 30,
		LiveBitrateKbps:        4096,
		ArchiveBitrateKbps:     8192,
		VideoOrientation:       "mirror_horizontal",
		CaptureAspectPolicy:    "letterbox",
	})
	if err != nil {
		t.Fatalf("BuildBrokerMuxCommand failed: %v", err)
	}
	if adapterID != "v4l2_uvc" {
		t.Fatalf("unexpected adapter id: %s", adapterID)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
		"--device /dev/video0",
		"--input-format mjpeg",
		"--capture-width 1280",
		"--capture-select-timeout-ms 2000",
		"--capture-max-consecutive-timeouts 3",
		"--video-orientation mirror_horizontal",
		"--aspect-policy letterbox",
		"--mjpeg-decode-accelerator auto",
		"--mjpeg-decode-initial-backend jpu",
		"--allow-sw-fallback true",
		"--source-id front",
		"--media-epoch 41",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("expected args to contain %q, got %q", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandGenericV4L2UsesPortableAdapterIdentity(t *testing.T) {
	cmd, adapterID, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled:           true,
		LiveCodec:             "h264",
		AnalysisEnabled:       true,
		ArchiveEnabled:        true,
		Transport:             "v4l2",
		WorkerProvider:        "gstreamer",
		GStreamerBridgePath:   "/opt/video-broker/bin/video-ingest-gst",
		CaptureDevicePath:     "/dev/video3",
		CaptureFormat:         "nv12",
		CaptureWidth:          1920,
		CaptureHeight:         1080,
		CaptureFPS:            30,
		MainWidth:             1280,
		MainHeight:            720,
		MainFPS:               20,
		SubWidth:              640,
		SubHeight:             360,
		SubFPS:                20,
		ArchiveWidth:          1920,
		ArchiveHeight:         1080,
		ArchiveFPS:            30,
		ArchiveContent:        "adaptive",
		ArchiveCodec:          "h264",
		EncodeProvider:        "gstreamer_mpp",
		DecodeProvider:        "gstreamer_mpp",
		H264EncodeHWAvailable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if adapterID != "v4l2" {
		t.Fatalf("adapter id=%q", adapterID)
	}
	if cmd.Path != "/opt/video-broker/bin/video-ingest-gst" {
		t.Fatalf("worker path=%q", cmd.Path)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"--archive-width 1920",
		"--archive-height 1080",
		"--archive-fps 30",
		"--archive-content adaptive",
		"--archive-codec h264",
		"--archive-encode-provider gstreamer_mpp",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("expected args to contain %q, got %q", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandV4L2UVCAddsQualityMonitorArgs(t *testing.T) {
	cmd, adapterID, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled:                true,
		AnalysisEnabled:            true,
		ArchiveEnabled:             true,
		Transport:                  "v4l2_uvc",
		WorkerProvider:             "native_sp",
		V4L2BridgePath:             "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
		CaptureDevicePath:          "/dev/video0",
		CaptureFormat:              "yuyv",
		H264EncodeHWAvailable:      true,
		H265EncodeHWAvailable:      true,
		ArchiveCodec:               "h265",
		CaptureWidth:               640,
		CaptureHeight:              360,
		CaptureFPS:                 30,
		MainWidth:                  1280,
		MainHeight:                 720,
		MainFPS:                    15,
		SubWidth:                   640,
		SubHeight:                  360,
		SubFPS:                     30,
		LiveBitrateKbps:            4096,
		ArchiveBitrateKbps:         8192,
		CaptureSelectTimeoutMs:     1500,
		CaptureMaxTimeouts:         4,
		QualityMonitorEnabled:      true,
		QualityMonitorReportEvery:  17,
		QualityMonitorSampleStride: 4,
		QualityMonitorStatePath:    "/run/video-broker/video-quality.json",
	})
	if err != nil {
		t.Fatalf("BuildBrokerMuxCommand failed: %v", err)
	}
	if adapterID != "v4l2_uvc" {
		t.Fatalf("unexpected adapter id: %s", adapterID)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"--capture-select-timeout-ms 1500",
		"--capture-max-consecutive-timeouts 4",
		"--quality-monitor-enabled true",
		"--quality-monitor-report-every 17",
		"--quality-monitor-sample-stride 4",
		"--quality-monitor-state-path /run/video-broker/video-quality.json",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("expected args to contain %q, got %q", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandV4L2PreservesQualityIntentWithoutAnalysis(t *testing.T) {
	cmd, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled: true, LiveCodec: "h265",
		Transport: "v4l2_uvc", WorkerProvider: "native_sp",
		V4L2BridgePath:    "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
		CaptureDevicePath: "/dev/video0", CaptureFormat: "h265",
		H265DecodeHWAvailable: true,
		CaptureWidth:          1280, CaptureHeight: 720, CaptureFPS: 30,
		MainWidth: 1280, MainHeight: 720, MainFPS: 30,
		QualityMonitorEnabled:   true,
		QualityMonitorStatePath: "/run/video-broker/video-quality.json",
	})
	if err != nil || !strings.Contains(strings.Join(cmd.Args, " "), "--quality-monitor-enabled true") {
		t.Fatalf("inactive sampling intent not passed to the capture owner: %v", err)
	}
}

func TestBuildBrokerMuxCommandV4L2UVCAddsSpatialToneMappingArgs(t *testing.T) {
	cmd, adapterID, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled:                   true,
		AnalysisEnabled:               true,
		ArchiveEnabled:                true,
		Transport:                     "v4l2_uvc",
		WorkerProvider:                "native_sp",
		V4L2BridgePath:                "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
		CaptureDevicePath:             "/dev/video0",
		CaptureFormat:                 "h265",
		H264EncodeHWAvailable:         true,
		H265DecodeHWAvailable:         true,
		H265EncodeHWAvailable:         true,
		CaptureWidth:                  1280,
		CaptureHeight:                 720,
		CaptureFPS:                    30,
		ArchiveWidth:                  1920,
		ArchiveHeight:                 1080,
		MainWidth:                     1280,
		MainHeight:                    720,
		MainFPS:                       20,
		SubWidth:                      640,
		SubHeight:                     360,
		SubFPS:                        20,
		LiveBitrateKbps:               6144,
		ArchiveBitrateKbps:            8192,
		ToneMappingEnabled:            true,
		ToneMappingMode:               "local",
		ToneMappingAccelerator:        "cpu",
		ToneMappingTarget:             96,
		ToneMappingHysteresis:         12,
		ToneMappingGridColumns:        16,
		ToneMappingGridRows:           9,
		ToneMappingStatisticsHz:       8,
		ToneMappingMaxShadowGain:      1.8,
		ToneMappingMaxHighlightReduce: 0.30,
		ToneMappingTemporalAlpha:      0.18,
		ToneMappingNoiseGuard:         true,
		ArchiveCodec:                  "h265",
	})
	if err != nil || adapterID != "v4l2_uvc" {
		t.Fatalf("unexpected provider command resolution adapter=%s err=%v", adapterID, err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"--archive-width 1280",
		"--archive-height 720",
		"--tone-mapping-mode local",
		"--tone-mapping-accelerator cpu",
		"--tone-mapping-target 96",
		"--tone-mapping-hysteresis 12",
		"--tone-mapping-grid-columns 16",
		"--tone-mapping-grid-rows 9",
		"--tone-mapping-statistics-hz 8",
		"--tone-mapping-max-shadow-gain 1.8",
		"--tone-mapping-max-highlight-reduction 0.3",
		"--tone-mapping-temporal-alpha 0.18",
		"--tone-mapping-noise-guard true",
		"--archive-content adaptive",
		"--archive-codec h265",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("expected args to contain %q, got %q", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandNormalizesArchiveForNativeEncoder(t *testing.T) {
	cmd, adapterID, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true,
		LiveCodec: "h264",
		Transport: "v4l2_uvc", WorkerProvider: "native_sp", V4L2BridgePath: "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
		CaptureDevicePath: "/dev/video0", CaptureFormat: "mjpeg",
		EncodeProvider: "native_sp", ArchiveEncodeProvider: "native_sp", DecodeProvider: "native_sp",
		H264EncodeHWAvailable: true, H265EncodeHWAvailable: true, MJPEGDecodeHWAvailable: true,
		CaptureWidth: 960, CaptureHeight: 540, CaptureFPS: 30,
		ArchiveWidth: 960, ArchiveHeight: 540, ArchiveFPS: 30,
		ArchiveWidthAlignment: 8, ArchiveHeightAlignment: 8,
		ArchiveContent: "processed", ArchiveCodec: "h265",
		MainWidth: 960, MainHeight: 540, MainFPS: 30,
		MainWidthAlignment: 8, MainHeightAlignment: 8,
		SubWidth: 640, SubHeight: 360, SubFPS: 30,
		LiveBitrateKbps: 6144, ArchiveBitrateKbps: 4096,
	})
	if err != nil || adapterID != "v4l2_uvc" {
		t.Fatalf("unexpected provider command resolution adapter=%s err=%v", adapterID, err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"--archive-width 952", "--archive-height 536",
		"--output-width 952", "--output-height 536",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("expected args to contain %q, got %q", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandUsesGenericHardwareProvider(t *testing.T) {
	cmd, adapterID, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		SourceID:              "front",
		MediaEpoch:            41,
		LiveEnabled:           true,
		AnalysisEnabled:       true,
		ArchiveEnabled:        true,
		Transport:             "v4l2_uvc",
		WorkerProvider:        "gstreamer",
		V4L2BridgePath:        "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
		GStreamerBridgePath:   "/opt/video-broker/bin/video-ingest-gst",
		CaptureDevicePath:     "/dev/video0",
		CaptureFormat:         "h265",
		EncodeProvider:        "gstreamer_mpp",
		DecodeProvider:        "gstreamer_mpp",
		H265EncodeHWAvailable: true,
		H265DecodeHWAvailable: true,
		CaptureWidth:          1920,
		CaptureHeight:         1080,
		CaptureFPS:            30,
		MainWidth:             1280,
		MainHeight:            720,
		MainFPS:               20,
		SubWidth:              640,
		SubHeight:             360,
		SubFPS:                15,
		LiveBitrateKbps:       6000,
		ArchiveBitrateKbps:    9000,
	})
	if err != nil {
		t.Fatalf("BuildBrokerMuxCommand failed: %v", err)
	}
	if adapterID != "v4l2_uvc" || cmd.Path != "/opt/video-broker/bin/video-ingest-gst" {
		t.Fatalf("unexpected generic provider command: adapter=%s path=%s", adapterID, cmd.Path)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"--input-format h265",
		"--encode-provider gstreamer_mpp",
		"--archive-encode-provider gstreamer_mpp",
		"--decode-provider gstreamer_mpp",
		"--allow-sw-fallback false",
		"--source-id front",
		"--media-epoch 41",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("expected args to contain %q, got %q", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandKeepsH265HardwareWhenLiveNeedsSoftware(t *testing.T) {
	cmd, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true,
		LiveCodec: "h264",
		Transport: "v4l2", WorkerProvider: "gstreamer", GStreamerBridgePath: "/opt/video-broker/bin/video-ingest-gst",
		CaptureDevicePath: "/dev/video0", CaptureFormat: "nv12",
		EncodeProvider: "gstreamer_mpp", DecodeProvider: "gstreamer_mpp",
		AllowSWFallback: true, H264EncodeHWAvailable: false, H265EncodeHWAvailable: true,
		CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
		MainWidth: 1280, MainHeight: 720, MainFPS: 20,
		SubWidth: 640, SubHeight: 360, SubFPS: 20,
		ArchiveWidth: 1920, ArchiveHeight: 1080, ArchiveFPS: 30,
		ArchiveContent: "adaptive", ArchiveCodec: "h265",
	})
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{"--encode-provider software", "--archive-encode-provider gstreamer_mpp"} {
		if !strings.Contains(args, want) {
			t.Fatalf("mixed codec provider selection does not contain %q: %s", want, args)
		}
	}
}

func TestGStreamerAdaptiveArchiveCannotBypassEnabledPAL(t *testing.T) {
	plan := BrokerMuxPlan{ArchiveEnabled: true, ArchiveContent: "adaptive", ToneMappingEnabled: true}
	if !gstreamerArchiveEncodeRequired(plan, "h265") {
		t.Fatal("adaptive compressed archive bypassed enabled PAL")
	}
	plan.ToneMappingEnabled = false
	if gstreamerArchiveEncodeRequired(plan, "h265") {
		t.Fatal("adaptive compressed archive lost valid native path with PAL disabled")
	}
	plan.ToneMappingEnabled = true
	plan.ArchiveContent = "source"
	if gstreamerArchiveEncodeRequired(plan, "h265") {
		t.Fatal("explicit source policy unexpectedly requested a processed encode")
	}
}

func TestBuildBrokerMuxCommandUsesSoftwareOnlyForMissingH265Archive(t *testing.T) {
	cmd, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true,
		LiveCodec: "h264",
		Transport: "v4l2", WorkerProvider: "gstreamer", GStreamerBridgePath: "/opt/video-broker/bin/video-ingest-gst",
		CaptureDevicePath: "/dev/video0", CaptureFormat: "nv12",
		EncodeProvider: "gstreamer_v4l2", DecodeProvider: "gstreamer_v4l2",
		AllowSWFallback: true, H264EncodeHWAvailable: true, H265EncodeHWAvailable: false,
		CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 20,
		MainWidth: 1280, MainHeight: 720, MainFPS: 20,
		SubWidth: 640, SubHeight: 360, SubFPS: 15,
		ArchiveWidth: 1280, ArchiveHeight: 720, ArchiveFPS: 20,
		ArchiveContent: "adaptive", ArchiveCodec: "h265",
	})
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{"--encode-provider gstreamer_v4l2", "--archive-encode-provider software"} {
		if !strings.Contains(args, want) {
			t.Fatalf("mixed codec provider selection does not contain %q: %s", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandSelectsSoftwareOnlyForMissingCapabilities(t *testing.T) {
	cmd, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled: true, AnalysisEnabled: true,
		LiveCodec:             "h264",
		Transport:             "v4l2_uvc",
		WorkerProvider:        "gstreamer",
		GStreamerBridgePath:   "/opt/video-broker/bin/video-ingest-gst",
		CaptureDevicePath:     "/dev/video0",
		CaptureFormat:         "h264",
		EncodeProvider:        "gstreamer_v4l2",
		DecodeProvider:        "gstreamer_v4l2",
		AllowSWFallback:       true,
		H264EncodeHWAvailable: false,
		H264DecodeHWAvailable: false,
		CaptureWidth:          1280,
		CaptureHeight:         720,
		CaptureFPS:            30,
		MainWidth:             1280,
		MainHeight:            720,
		MainFPS:               20,
		SubWidth:              640,
		SubHeight:             360,
		SubFPS:                15,
	})
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"--encode-provider software",
		"--decode-provider software",
		"--allow-sw-fallback true",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("expected args to contain %q, got %q", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandRejectsMJPEGWhenSWFallbackDisabled(t *testing.T) {
	_, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		AnalysisEnabled:    true,
		Transport:          "v4l2_uvc",
		WorkerProvider:     "native_sp",
		V4L2BridgePath:     "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
		CaptureDevicePath:  "/dev/video0",
		CaptureFormat:      "mjpeg",
		CaptureWidth:       1280,
		CaptureHeight:      720,
		CaptureFPS:         30,
		MainWidth:          1280,
		MainHeight:         720,
		MainFPS:            30,
		SubWidth:           640,
		SubHeight:          360,
		SubFPS:             30,
		LiveBitrateKbps:    4096,
		ArchiveBitrateKbps: 8192,
	})
	if err == nil {
		t.Fatalf("expected mjpeg to be rejected when software fallback is disabled")
	}
}

func TestBuildBrokerMuxCommandUsesStrictJPUWhenSoftwareFallbackDisabled(t *testing.T) {
	cmd, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true,
		Transport: "v4l2_uvc", WorkerProvider: "native_sp", V4L2BridgePath: "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
		CaptureDevicePath: "/dev/video0", CaptureFormat: "mjpeg",
		MJPEGDecodeHWAvailable: true, H264EncodeHWAvailable: true, H265EncodeHWAvailable: true,
		CaptureWidth: 640, CaptureHeight: 360, CaptureFPS: 30,
		MainWidth: 1280, MainHeight: 720, MainFPS: 20,
		SubWidth: 640, SubHeight: 360, SubFPS: 20,
		ArchiveWidth: 640, ArchiveHeight: 360, ArchiveFPS: 15,
		ArchiveCodec: "h265", ArchiveContent: "processed", ArchiveRateControl: "avbr",
		ArchiveBitrateKbps: 750,
	})
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(cmd.Args, " ")
	if !strings.Contains(args, "--mjpeg-decode-accelerator jpu") ||
		!strings.Contains(args, "--mjpeg-decode-initial-backend jpu") ||
		!strings.Contains(args, "--allow-sw-fallback false") ||
		!strings.Contains(args, "--archive-fps 15") ||
		!strings.Contains(args, "--archive-encode-bitrate-kbps 750") ||
		!strings.Contains(args, "--archive-rate-control avbr") {
		t.Fatalf("strict hardware MJPEG contract missing: %s", args)
	}
}

func TestBuildBrokerMuxCommandKeepsAutoRecoveryDuringJPUCooldown(t *testing.T) {
	cmd, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{
		LiveEnabled: true, AnalysisEnabled: true, ArchiveEnabled: true,
		Transport: "v4l2_uvc", WorkerProvider: "native_sp", V4L2BridgePath: "/opt/video-broker/bin/rdkx5_v4l2_mux_bridge",
		CaptureDevicePath: "/dev/video0", CaptureFormat: "mjpeg", AllowSWFallback: true,
		MJPEGDecodeHWAvailable: false, MJPEGDecodeInPlaceRetrySupported: true,
		H264EncodeHWAvailable: true, H265EncodeHWAvailable: true,
		CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 30,
		MainWidth: 1280, MainHeight: 720, MainFPS: 20,
		SubWidth: 640, SubHeight: 360, SubFPS: 20,
		ArchiveWidth: 1280, ArchiveHeight: 720, ArchiveFPS: 30,
		ArchiveCodec: "h265", ArchiveContent: "processed",
	})
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Join(cmd.Args, " ")
	for _, want := range []string{
		"--mjpeg-decode-accelerator auto",
		"--mjpeg-decode-initial-backend cpu",
		"--allow-sw-fallback true",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("cooldown command lost recoverable JPU policy %q: %s", want, args)
		}
	}
}

func TestBuildBrokerMuxCommandRejectsUnknownTransport(t *testing.T) {
	_, _, err := BuildBrokerMuxCommand(context.Background(), BrokerMuxPlan{Transport: "unknown"})
	if err == nil {
		t.Fatal("expected unknown transport to fail")
	}
}

func TestDescribeBrokerMuxPlanFlagsUVCMJPEGCost(t *testing.T) {
	report, err := DescribeBrokerMuxPlan(BrokerMuxPlan{
		Transport:      "v4l2_uvc",
		WorkerProvider: "native_sp",
		CaptureFormat:  "mjpeg",
		CaptureWidth:   1280,
		CaptureHeight:  720,
		MainWidth:      1280,
		MainHeight:     720,
		SubWidth:       640,
		SubHeight:      360,
	})
	if err != nil {
		t.Fatalf("DescribeBrokerMuxPlan failed: %v", err)
	}
	if report.EstimatedCost != "high" || !report.RequiresSoftwareDecode || !report.RequiresSoftwareResize {
		t.Fatalf("unexpected UVC MJPEG report: %+v", report)
	}
}

func TestDescribeBrokerMuxPlanRecognizesHardwareMJPEGDecode(t *testing.T) {
	report, err := DescribeBrokerMuxPlan(BrokerMuxPlan{
		Transport: "v4l2_uvc", WorkerProvider: "native_sp", CaptureFormat: "mjpeg", MJPEGDecodeHWAvailable: true,
		CaptureWidth: 1280, CaptureHeight: 720,
		MainWidth: 1280, MainHeight: 720, SubWidth: 640, SubHeight: 360,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.EstimatedCost != "medium" || report.RequiresSoftwareDecode ||
		report.ComponentRetryScope("native.jpu") != ComponentRetryInPlace || len(report.ComponentRetries) != 1 {
		t.Fatalf("hardware MJPEG capability was not reflected: %+v", report)
	}

	portable, err := DescribeBrokerMuxPlan(BrokerMuxPlan{
		Transport: "v4l2_uvc", WorkerProvider: "gstreamer", CaptureFormat: "mjpeg", MJPEGDecodeHWAvailable: true,
		EncodeProvider: "gstreamer_mpp", DecodeProvider: "gstreamer_mpp",
		CaptureWidth: 1280, CaptureHeight: 720,
		MainWidth: 1280, MainHeight: 720, SubWidth: 640, SubHeight: 360,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(portable.ComponentRetries) != 0 || portable.ComponentRetryScope("native.jpu") != ComponentRetryNextNaturalEpoch {
		t.Fatalf("static adapter falsely advertised in-place component retry: %+v", portable)
	}
	cooldown, err := DescribeBrokerMuxPlan(BrokerMuxPlan{
		Transport: "v4l2_uvc", WorkerProvider: "native_sp", CaptureFormat: "mjpeg", DecodeProvider: "native_sp",
		MJPEGDecodeHWAvailable: false, MJPEGDecodeInPlaceRetrySupported: true,
		CaptureWidth: 1280, CaptureHeight: 720,
		MainWidth: 1280, MainHeight: 720, SubWidth: 640, SubHeight: 360,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cooldown.RequiresSoftwareDecode || cooldown.ComponentRetryScope("native.jpu") != ComponentRetryInPlace {
		t.Fatalf("CPU continuity epoch forgot its native in-place JPU operation: %+v", cooldown)
	}
}

func TestDescribeBrokerMuxPlanFlagsHBNLowCost(t *testing.T) {
	report, err := DescribeBrokerMuxPlan(BrokerMuxPlan{
		Transport:      "vendor_hbn",
		WorkerProvider: "native_sp",
		CaptureFormat:  "nv12",
		CaptureWidth:   1920,
		CaptureHeight:  1080,
		MainWidth:      1920,
		MainHeight:     1080,
		SubWidth:       640,
		SubHeight:      640,
	})
	if err != nil {
		t.Fatalf("DescribeBrokerMuxPlan failed: %v", err)
	}
	if report.EstimatedCost != "low" || !report.UsesHardwareSensorStack || report.RequiresSoftwareDecode ||
		report.AcceptsRuntimeControlProtocol || report.SupportsRuntimeLiveControl || report.LiveControlMode != "backend_static" {
		t.Fatalf("unexpected HBN report: %+v", report)
	}
}

func TestDescribeBrokerMuxPlanEnablesHBNFanoutControlOnlyWithFence(t *testing.T) {
	plan := BrokerMuxPlan{
		SourceID: "front", MediaEpoch: 41,
		Transport: "vendor_hbn", WorkerProvider: "native_sp",
		CaptureFormat: "nv12", CaptureWidth: 1280, CaptureHeight: 720,
		MainWidth: 1280, MainHeight: 720, SubWidth: 640, SubHeight: 360,
		AdmittedSurfaces: []BrokerMuxSurface{{Width: 1280, Height: 720}},
	}
	report, err := DescribeBrokerMuxPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if !report.AcceptsRuntimeControlProtocol {
		t.Fatalf("fenced HBN plan did not expose worker control: %+v", report)
	}
	plan.MediaEpoch = 0
	report, err = DescribeBrokerMuxPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if report.AcceptsRuntimeControlProtocol {
		t.Fatalf("incomplete HBN fence exposed worker control: %+v", report)
	}
}

func TestDescribeBrokerMuxPlanReportsLibcameraBoundary(t *testing.T) {
	report, err := DescribeBrokerMuxPlan(BrokerMuxPlan{
		Transport:      TransportLibcamera,
		WorkerProvider: "gstreamer",
		CaptureWidth:   1920, CaptureHeight: 1080,
		MainWidth: 1280, MainHeight: 720,
		SubWidth: 640, SubHeight: 360,
		H264EncodeHWAvailable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.AdapterID != TransportLibcamera || !report.UsesHardwareSensorStack ||
		!report.UsesHardwareEncode || !report.SupportsToneMapping || report.ToneMappingBackend != "portable_nv12" || report.LiveControlMode != "backend_static" {
		t.Fatalf("unexpected libcamera capabilities: %+v", report)
	}
}

func TestDescribeBrokerMuxPlanEnablesEpochControlForIndependentH265Archive(t *testing.T) {
	report, err := DescribeBrokerMuxPlan(BrokerMuxPlan{
		Transport: "v4l2_uvc", WorkerProvider: "native_sp", CaptureFormat: "h265", EncodeProvider: "native_sp", DecodeProvider: "native_sp",
		CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
		MainWidth: 1280, MainHeight: 720, MainFPS: 20,
		SubWidth: 640, SubHeight: 360, SubFPS: 20,
		ToneMappingEnabled: true, ArchiveCodec: "h265",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.AcceptsRuntimeControlProtocol || !report.SupportsRuntimeLiveControl || report.LiveControlMode != "persistent_encoder_epoch" {
		t.Fatalf("independent H265 archive did not enable live encoder epoch control: %+v", report)
	}
}

func TestDescribeBrokerMuxPlanKeepsGenericBackendStatic(t *testing.T) {
	report, err := DescribeBrokerMuxPlan(BrokerMuxPlan{
		Transport: "v4l2_uvc", WorkerProvider: "gstreamer", CaptureFormat: "h265",
		EncodeProvider: "gstreamer_mpp", DecodeProvider: "gstreamer_mpp",
		CaptureWidth: 1920, CaptureHeight: 1080, CaptureFPS: 30,
		MainWidth: 1280, MainHeight: 720, MainFPS: 20,
		SubWidth: 640, SubHeight: 360, SubFPS: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.AcceptsRuntimeControlProtocol || report.SupportsRuntimeLiveControl || report.LiveControlMode != "backend_static" {
		t.Fatalf("generic static backend misreported runtime control: %+v", report)
	}
}

func TestDescribeBrokerMuxPlanOpensPortableControlOnlyWithIdentityFence(t *testing.T) {
	report, err := DescribeBrokerMuxPlan(BrokerMuxPlan{
		Transport: "v4l2_uvc", WorkerProvider: "gstreamer", CaptureFormat: "nv12",
		SourceID: "front", MediaEpoch: 41,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.AcceptsRuntimeControlProtocol || report.SupportsRuntimeLiveControl ||
		report.LiveControlMode != "backend_static" {
		t.Fatalf("portable fan-out control boundary differs: %+v", report)
	}
	for _, plan := range []BrokerMuxPlan{
		{SourceID: "front"},
		{MediaEpoch: 41},
		{SourceID: "/dev/video0", MediaEpoch: 41},
	} {
		if _, err := workerIdentityArgs(plan); err == nil {
			t.Fatalf("partial or non-canonical worker identity was accepted: %+v", plan)
		}
	}
}

func TestParseV4L2FormatCapabilities(t *testing.T) {
	out := []byte(`
ioctl: VIDIOC_ENUM_FMT
	Type: Video Capture

	[0]: 'MJPG' (Motion-JPEG)
		Size: Discrete 1280x720
			Interval: Discrete 0.033s (30.000 fps)
	[1]: 'YUYV' (YUYV 4:2:2)
		Size: Discrete 1280x720
			Interval: Discrete 0.100s (10.000 fps)
		Size: Discrete 640x360
			Interval: Discrete 0.033s (30.000 fps)
`)
	caps := ParseV4L2FormatCapabilities(out)
	if len(caps) != 3 {
		t.Fatalf("unexpected caps len=%d caps=%+v", len(caps), caps)
	}
	if !formatSupported(caps, "mjpeg", 1280, 720, 30) {
		t.Fatalf("expected mjpeg 1280x720@30 supported: %+v", caps)
	}
	if formatSupported(caps, "yuyv", 1280, 720, 30) {
		t.Fatalf("expected yuyv 1280x720@30 unsupported: %+v", caps)
	}
	if !formatSupported(caps, "yuyv", 640, 360, 30) {
		t.Fatalf("expected yuyv 640x360@30 supported: %+v", caps)
	}
}

func TestResolveV4L2UVCInputFormatAutoPrefersYUYVWhenFPSMatches(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 640, Height: 360, FPS: 30},
			{PixelFormat: "yuyv", Width: 640, Height: 360, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCInputFormat(BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0",
		CaptureWidth:      640,
		CaptureHeight:     360,
		CaptureFPS:        30,
		PreferExactYUYV:   true,
	}, query)
	if got != "yuyv" {
		t.Fatalf("expected yuyv, got %q", got)
	}
}

func TestResolveV4L2UVCCaptureDecisionRejectsImplicitDeviceWithoutProbe(t *testing.T) {
	queried := false
	decision := ResolveV4L2UVCCaptureDecision(BrokerMuxPlan{
		CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 30,
	}, func(context.Context, string) ([]V4L2FormatCapability, error) {
		queried = true
		return nil, nil
	})
	if queried {
		t.Fatal("capability probe ran without an explicit V4L2 device")
	}
	if decision.Reason != "missing_device" || decision.Format != "" {
		t.Fatalf("unexpected missing-device decision: %+v", decision)
	}
}

func TestQueryV4L2FormatCapabilitiesRejectsImplicitDevice(t *testing.T) {
	if _, err := QueryV4L2FormatCapabilities(context.Background(), ""); err == nil || !strings.Contains(err.Error(), "device") {
		t.Fatalf("empty V4L2 device was not rejected: %v", err)
	}
}

func TestResolveV4L2UVCInputFormatAcceptsFractionalThirtyFPS(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 640, Height: 360, FPS: 30},
			{PixelFormat: "yuyv", Width: 640, Height: 360, FPS: 29.97},
		}, nil
	}
	got := ResolveV4L2UVCInputFormat(BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0",
		CaptureWidth:      640,
		CaptureHeight:     360,
		CaptureFPS:        30,
		PreferExactYUYV:   true,
	}, query)
	if got != "yuyv" {
		t.Fatalf("expected yuyv for 29.97fps nominal 30fps camera mode, got %q", got)
	}
}

func TestResolveV4L2UVCInputFormatAutoCanDisableExactYUYVPreference(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30},
			{PixelFormat: "yuyv", Width: 1280, Height: 720, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCInputFormatDecision(BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0",
		CaptureWidth:      1280,
		CaptureHeight:     720,
		CaptureFPS:        30,
		PreferExactYUYV:   false,
	}, query)
	if got.Format != "mjpeg" || got.Reason != "mjpeg_exact_match" {
		t.Fatalf("expected mjpeg when exact YUYV preference is disabled, got %+v", got)
	}
	if len(got.Notes) != 1 || got.Notes[0] != "exact_yuyv_candidate_disabled_by_policy" {
		t.Fatalf("expected disabled exact YUYV note, got %+v", got.Notes)
	}
}

func TestResolveV4L2UVCInputFormatUsesExactYUYVWhenMJPEGUnavailable(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "yuyv", Width: 1280, Height: 720, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCInputFormatDecision(BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0",
		CaptureWidth:      1280,
		CaptureHeight:     720,
		CaptureFPS:        30,
		PreferExactYUYV:   false,
	}, query)
	if got.Format != "yuyv" || got.Reason != "yuyv_exact_only_available" {
		t.Fatalf("expected exact yuyv when exact mjpeg is unavailable, got %+v", got)
	}
}

func TestResolveV4L2UVCInputFormatAutoFallsBackToMJPEGWhenYUYVTooSlow(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30},
			{PixelFormat: "yuyv", Width: 1280, Height: 720, FPS: 10},
		}, nil
	}
	got := ResolveV4L2UVCInputFormat(BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0",
		CaptureWidth:      1280,
		CaptureHeight:     720,
		CaptureFPS:        30,
	}, query)
	if got != "mjpeg" {
		t.Fatalf("expected mjpeg, got %q", got)
	}
}

func TestResolveV4L2UVCCaptureDecisionReportsScaledYUYVCandidateWithoutSwitch(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30},
			{PixelFormat: "yuyv", Width: 1280, Height: 720, FPS: 10},
			{PixelFormat: "yuyv", Width: 640, Height: 360, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCCaptureDecision(BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0",
		CapturePolicy:     "analysis_quality",
		AllowScaledYUYV:   false,
		CaptureWidth:      1280,
		CaptureHeight:     720,
		CaptureFPS:        30,
		SubWidth:          640,
		SubHeight:         360,
	}, query)
	if got.Format != "mjpeg" || got.Reason != "mjpeg_exact_match" || got.ResizedCapture {
		t.Fatalf("expected exact mjpeg decision with candidate note, got %+v", got)
	}
	if len(got.Notes) != 1 || !strings.Contains(got.Notes[0], "scaled_yuyv_candidate=640x360@30 disabled_by_policy") {
		t.Fatalf("expected disabled scaled candidate note, got %+v", got.Notes)
	}
}

func TestResolveV4L2UVCCaptureDecisionSwitchesToScaledYUYVWhenAllowed(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30},
			{PixelFormat: "yuyv", Width: 1280, Height: 720, FPS: 10},
			{PixelFormat: "yuyv", Width: 640, Height: 360, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCCaptureDecision(BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0",
		CapturePolicy:     "analysis_quality",
		AllowScaledYUYV:   true,
		CaptureWidth:      1280,
		CaptureHeight:     720,
		CaptureFPS:        30,
		SubWidth:          640,
		SubHeight:         360,
	}, query)
	if got.Format != "yuyv" || got.Reason != "analysis_quality_scaled_yuyv" {
		t.Fatalf("expected scaled yuyv decision, got %+v", got)
	}
	if got.CaptureWidth != 640 || got.CaptureHeight != 360 || got.CaptureFPS != 30 || !got.ResizedCapture {
		t.Fatalf("unexpected scaled capture decision: %+v", got)
	}
}

func TestResolveV4L2UVCInputFormatDecisionFailsClosedWhenProbeFails(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return nil, context.DeadlineExceeded
	}
	got := ResolveV4L2UVCInputFormatDecision(BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0",
		CaptureWidth:      1280,
		CaptureHeight:     720,
		CaptureFPS:        30,
	}, query)
	if got.Format != "" || got.Reason != "probe_failed" || got.UsedFallback || got.RequiresSoftwareDecode {
		t.Fatalf("unexpected decision: %+v", got)
	}
}

func TestResolveV4L2UVCInputFormatDecisionFailsClosedWithoutExactMode(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 640, Height: 480, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCInputFormatDecision(BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0",
		CaptureWidth:      1920,
		CaptureHeight:     1080,
		CaptureFPS:        30,
	}, query)
	if got.Format != "" || got.Reason != "no_exact_match" || got.UsedFallback || got.RequiresSoftwareDecode {
		t.Fatalf("unsupported capture mode did not fail closed: %+v", got)
	}
}

func TestResolveV4L2UVCInputFormatPrefersNativeH265(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "H264", Width: 1920, Height: 1080, FPS: 30},
			{PixelFormat: "HEVC", Width: 1920, Height: 1080, FPS: 30},
			{PixelFormat: "MJPG", Width: 1920, Height: 1080, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCInputFormatDecision(BrokerMuxPlan{
		CaptureDevicePath:     "/dev/video0",
		CaptureWidth:          1920,
		CaptureHeight:         1080,
		CaptureFPS:            30,
		H264DecodeHWAvailable: true,
		H265DecodeHWAvailable: true,
	}, query)
	if got.Format != "h265" || got.Reason != "h265_exact_native_compressed" || got.RequiresSoftwareDecode {
		t.Fatalf("unexpected native compressed decision: %+v", got)
	}
}

func TestResolveV4L2UVCInputFormatPrefersNative4MPH265Downscale(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "H264", Width: 1280, Height: 720, FPS: 30},
			{PixelFormat: "HEVC", Width: 2560, Height: 1440, FPS: 30},
			{PixelFormat: "MJPG", Width: 1280, Height: 720, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCInputFormatDecision(BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureCodecPreference: "h265",
		CapturePolicy:          "analysis_quality",
		CaptureWidth:           1280,
		CaptureHeight:          720,
		CaptureFPS:             30,
		H264DecodeHWAvailable:  true,
		H265DecodeHWAvailable:  true,
	}, query)
	if got.Format != "h265" || got.Reason != "h265_native_compressed_downscale" || got.RequiresSoftwareDecode {
		t.Fatalf("unexpected preferred native 4MP H265 decision: %+v", got)
	}
	if got.CaptureWidth != 2560 || got.CaptureHeight != 1440 || got.CaptureFPS != 30 || !got.ResizedCapture {
		t.Fatalf("unexpected native 4MP capture contract: %+v", got)
	}
	if len(got.Notes) < 3 || !strings.Contains(strings.Join(got.Notes, " "), "camera_native_mode=2560x1440@30.00") {
		t.Fatalf("missing native camera mode evidence: %+v", got.Notes)
	}
}

func TestResolveV4L2UVCInputFormatPerceptionQualitySupersamplesExactHardwareMode(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "HEVC", Width: 1920, Height: 1080, FPS: 30},
			{PixelFormat: "HEVC", Width: 2560, Height: 1440, FPS: 30},
			{PixelFormat: "HEVC", Width: 3840, Height: 2160, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCInputFormatDecision(BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "required",
		CapturePolicy:          "analysis_quality",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		RequireHWCodec:         true,
		H264EncodeHWAvailable:  true,
		H265DecodeHWAvailable:  true,
	}, query)
	if got.Format != "h265" || got.Reason != "h265_native_compressed_supersample" ||
		got.CaptureWidth != 2560 || got.CaptureHeight != 1440 || got.CaptureFPS != 30 || !got.ResizedCapture {
		t.Fatalf("unexpected bounded supersample decision: %+v", got)
	}
	if !strings.Contains(strings.Join(got.Notes, " "), "selection=analysis_quality_supersample") {
		t.Fatalf("missing supersample evidence: %+v", got.Notes)
	}
}

func TestResolveV4L2UVCInputFormatUsesRuntimeQualifiedFallback(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "HEVC", Width: 1280, Height: 720, FPS: 30},
			{PixelFormat: "HEVC", Width: 1920, Height: 1080, FPS: 30},
			{PixelFormat: "HEVC", Width: 2560, Height: 1440, FPS: 30},
		}, nil
	}
	plan := BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "required",
		CapturePolicy:          "analysis_quality",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		SubWidth:               640,
		SubHeight:              360,
		RequireHWCodec:         true,
		H264EncodeHWAvailable:  true,
		H265DecodeHWAvailable:  true,
		CaptureModeExclusions: []V4L2CaptureModeExclusion{{
			PixelFormat: "h265", Width: 2560, Height: 1440, FPS: 30,
		}},
	}
	got := ResolveV4L2UVCInputFormatDecision(plan, query)
	if got.Format != "h265" || got.Reason != "h265_exact_native_compressed" ||
		got.CaptureWidth != 1920 || got.CaptureHeight != 1080 || got.CaptureFPS != 30 || got.ResizedCapture {
		t.Fatalf("rejected supersample displaced the requested exact mode: %+v", got)
	}
	if !strings.Contains(strings.Join(got.Notes, " "), "selection=requested_exact_after_runtime_exclusion") {
		t.Fatalf("missing requested-mode recovery evidence: %+v", got.Notes)
	}

	plan.CaptureModeExclusions = append(plan.CaptureModeExclusions, V4L2CaptureModeExclusion{
		PixelFormat: "h265", Width: 1920, Height: 1080, FPS: 30,
	})
	got = ResolveV4L2UVCInputFormatDecision(plan, query)
	if got.Format != "h265" || got.Reason != "h265_native_compressed_runtime_fallback" ||
		got.CaptureWidth != 1280 || got.CaptureHeight != 720 || got.CaptureFPS != 30 || !got.ResizedCapture {
		t.Fatalf("runtime-qualified fallback did not use the remaining compatible mode: %+v", got)
	}
	if !strings.Contains(strings.Join(got.Notes, " "), "selection=runtime_qualified_fallback") {
		t.Fatalf("missing runtime fallback evidence: %+v", got.Notes)
	}

	exact := plan
	exact.CapturePolicy = "exact"
	exact.CaptureModeExclusions = []V4L2CaptureModeExclusion{{
		PixelFormat: "h265", Width: 1920, Height: 1080, FPS: 30,
	}}
	got = ResolveV4L2UVCInputFormatDecision(exact, query)
	if got.Format != "" || got.Reason != "required_h265_mode_unavailable" {
		t.Fatalf("exact policy silently changed resolution after runtime rejection: %+v", got)
	}
}

func TestResolveV4L2UVCInputFormatUsesMJPEGRuntimeFallbackAbovePerceptionFloor(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30},
			{PixelFormat: "mjpeg", Width: 960, Height: 540, FPS: 30},
			{PixelFormat: "mjpeg", Width: 640, Height: 360, FPS: 30},
		}, nil
	}
	plan := BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "prefer",
		CapturePolicy:          "analysis_quality",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		SubWidth:               640,
		SubHeight:              360,
		AllowSWFallback:        true,
		H264EncodeHWAvailable:  true,
		H265EncodeHWAvailable:  true,
		CaptureModeExclusions: []V4L2CaptureModeExclusion{{
			PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30,
		}},
	}
	got := ResolveV4L2UVCInputFormatDecision(plan, query)
	if got.Format != "mjpeg" || got.Reason != "mjpeg_native_runtime_fallback" ||
		got.CaptureWidth != 960 || got.CaptureHeight != 540 || got.CaptureFPS != 30 ||
		!got.RequiresSoftwareDecode || !got.ResizedCapture {
		t.Fatalf("unexpected MJPEG runtime fallback: %+v", got)
	}
	if !strings.Contains(strings.Join(got.Notes, " "), "selection=runtime_qualified_fallback") {
		t.Fatalf("missing runtime fallback evidence: %+v", got.Notes)
	}

	// An explicit codec constrains the encoded source type, not permanent use
	// of a quarantined geometry. Keep the stream alive on another native mode
	// while the exact mode circuit breaker is open.
	plan.CaptureFormat = "mjpeg"
	got = ResolveV4L2UVCInputFormatDecision(plan, query)
	if got.Format != "mjpeg" || got.Reason != "mjpeg_native_runtime_fallback" ||
		got.CaptureWidth != 960 || got.CaptureHeight != 540 || got.CaptureFPS != 30 || !got.ResizedCapture {
		t.Fatalf("explicit MJPEG contract did not use its bounded native fallback: %+v", got)
	}

	plan.CaptureWidth = 1280
	plan.CaptureHeight = 720
	got = ResolveV4L2UVCInputFormatDecision(plan, query)
	if got.Format != "mjpeg" || got.Reason != "mjpeg_native_runtime_fallback" ||
		got.CaptureWidth != 960 || got.CaptureHeight != 540 || got.CaptureFPS != 30 {
		t.Fatalf("quarantined exact explicit MJPEG mode deadlocked instead of adapting: %+v", got)
	}
}

func TestResolveV4L2UVCInputFormatRuntimeFallbackRebindsArchiveCadence(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30},
			{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 20},
			{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 15},
			{PixelFormat: "mjpeg", Width: 960, Height: 540, FPS: 30},
		}, nil
	}
	plan := BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "prefer",
		CapturePolicy:          "analysis_quality",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		MainFPS:                20,
		SubWidth:               640,
		SubHeight:              360,
		SubFPS:                 20,
		ArchiveFPS:             30,
		AllowSWFallback:        true,
		H264EncodeHWAvailable:  true,
		H265EncodeHWAvailable:  true,
		CaptureModeExclusions: []V4L2CaptureModeExclusion{{
			PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30,
		}},
	}
	got := ResolveV4L2UVCInputFormatDecision(plan, query)
	if got.Format != "mjpeg" || got.Reason != "mjpeg_native_runtime_fallback" ||
		got.CaptureWidth != 1280 || got.CaptureHeight != 720 || got.CaptureFPS != 20 {
		t.Fatalf("runtime fallback discarded native lower-cadence capture: %+v", got)
	}

	// Once that lower-cadence mode is also rejected, choose the next geometry
	// that still sustains every configured consumer.
	plan.CaptureModeExclusions = append(plan.CaptureModeExclusions, V4L2CaptureModeExclusion{
		PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 20,
	})
	got = ResolveV4L2UVCInputFormatDecision(plan, query)
	if got.CaptureWidth != 960 || got.CaptureHeight != 540 || got.CaptureFPS != 30 {
		t.Fatalf("runtime fallback undersupplied consumer cadence: %+v", got)
	}
}

func TestResolveV4L2UVCH265FallbackKeepsGeometryAndSelectsNative25FPS(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "h265", Width: 1920, Height: 1080, FPS: 30},
			{PixelFormat: "h265", Width: 1920, Height: 1080, FPS: 25},
			{PixelFormat: "h265", Width: 1920, Height: 1080, FPS: 15},
			{PixelFormat: "h265", Width: 1280, Height: 720, FPS: 30},
		}, nil
	}
	plan := BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "required",
		CapturePolicy:          "analysis_quality",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		ArchiveFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		MainFPS:                20,
		SubWidth:               640,
		SubHeight:              360,
		SubFPS:                 20,
		H265DecodeHWAvailable:  true,
		H264EncodeHWAvailable:  true,
		CaptureModeExclusions: []V4L2CaptureModeExclusion{{
			PixelFormat: "h265", Width: 1920, Height: 1080, FPS: 30,
		}},
	}
	got := ResolveV4L2UVCInputFormatDecision(plan, query)
	if got.Format != "h265" || got.Reason != "h265_native_compressed_runtime_fallback" ||
		got.CaptureWidth != 1920 || got.CaptureHeight != 1080 || got.CaptureFPS != 25 {
		t.Fatalf("runtime-rejected 30fps mode did not select native 25fps fallback: %+v", got)
	}
}

func TestResolveV4L2UVCInputFormatSupersamplingRequiresBoundedHardwarePath(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "HEVC", Width: 1920, Height: 1080, FPS: 30},
			{PixelFormat: "HEVC", Width: 2560, Height: 1440, FPS: 30},
		}, nil
	}
	base := BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureCodecPreference: "h265",
		CapturePolicy:          "analysis_quality",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		H264EncodeHWAvailable:  true,
		H265DecodeHWAvailable:  true,
	}
	for name, mutate := range map[string]func(*BrokerMuxPlan){
		"software policy": func(plan *BrokerMuxPlan) { plan.RequireHWCodec = false },
		"missing encoder": func(plan *BrokerMuxPlan) { plan.RequireHWCodec = true; plan.H264EncodeHWAvailable = false },
	} {
		plan := base
		mutate(&plan)
		got := ResolveV4L2UVCInputFormatDecision(plan, query)
		if got.CaptureWidth != 1920 || got.CaptureHeight != 1080 || got.Reason != "h265_exact_native_compressed" {
			t.Fatalf("%s unexpectedly supersampled: %+v", name, got)
		}
	}
}

func TestResolveV4L2UVCInputFormatExactPolicyDoesNotChangeResolution(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "HEVC", Width: 2560, Height: 1440, FPS: 30},
			{PixelFormat: "H264", Width: 1280, Height: 720, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCInputFormatDecision(BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureCodecPreference: "h265",
		CapturePolicy:          "exact",
		CaptureWidth:           1280,
		CaptureHeight:          720,
		CaptureFPS:             30,
		H264DecodeHWAvailable:  true,
		H265DecodeHWAvailable:  true,
	}, query)
	if got.Format != "h264" || got.Reason != "h264_exact_native_compressed" || got.ResizedCapture {
		t.Fatalf("exact capture policy unexpectedly changed resolution: %+v", got)
	}
}

func TestResolveV4L2UVCInputFormatHonorsH264Preference(t *testing.T) {
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "h264", Width: 1920, Height: 1080, FPS: 30},
			{PixelFormat: "h265", Width: 1920, Height: 1080, FPS: 30},
		}, nil
	}
	got := ResolveV4L2UVCInputFormatDecision(BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureCodecPreference: "h264",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		H264DecodeHWAvailable:  true,
		H265DecodeHWAvailable:  true,
	}, query)
	if got.Format != "h264" || got.Reason != "h264_exact_native_compressed" {
		t.Fatalf("unexpected preferred compressed decision: %+v", got)
	}
}
