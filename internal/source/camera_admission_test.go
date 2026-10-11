package source

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestCameraAdmissionChoosesPreferredCodecAcrossRenumberedNodes(t *testing.T) {
	plan := BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureFormat:          "auto",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "prefer",
		CaptureAspectPolicy:    "crop",
		CapturePolicy:          "exact",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		SubWidth:               640,
		SubHeight:              360,
		AllowSWFallback:        true,
		H265DecodeHWAvailable:  true,
	}
	query := func(_ context.Context, device string) ([]V4L2FormatCapability, error) {
		switch device {
		case "/dev/video0":
			return []V4L2FormatCapability{{PixelFormat: "mjpeg", Width: 1920, Height: 1080, FPS: 30}}, nil
		case "/dev/video2":
			return []V4L2FormatCapability{{PixelFormat: "hevc", Width: 1920, Height: 1080, FPS: 30}}, nil
		default:
			return nil, errors.New("not a capture interface")
		}
	}
	identity := func(string) (string, error) { return "usb=renumbered-camera", nil }
	got, err := ResolveV4L2UVCCameraAdmission(
		context.Background(), plan, []string{"/dev/video0", "/dev/video1", "/dev/video2"}, query, identity,
	)
	if err != nil {
		t.Fatalf("admission failed: %v", err)
	}
	if got.DevicePath != "/dev/video2" || got.Format != "h265" || got.CaptureFPS != 30 {
		t.Fatalf("unexpected admission: %+v", got)
	}
	if !reflect.DeepEqual(got.ProbedDevices, []string{"/dev/video0", "/dev/video1", "/dev/video2"}) {
		t.Fatalf("unexpected probes: %v", got.ProbedDevices)
	}
}

func TestCameraAdmissionAdaptsToMJPEGOnlyUVCModule(t *testing.T) {
	plan := BrokerMuxPlan{
		CaptureDevicePath:      "/dev/video0",
		CaptureFormat:          "auto",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "prefer",
		CaptureAspectPolicy:    "crop",
		CapturePolicy:          "analysis_quality",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		SubWidth:               640,
		SubHeight:              360,
		AllowSWFallback:        true,
		AllowScaledYUYV:        true,
		H264EncodeHWAvailable:  true,
		H264DecodeHWAvailable:  true,
		H265EncodeHWAvailable:  true,
		H265DecodeHWAvailable:  true,
	}
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30},
			{PixelFormat: "yuyv", Width: 1280, Height: 720, FPS: 10},
			{PixelFormat: "yuyv", Width: 640, Height: 360, FPS: 30},
		}, nil
	}
	got, err := ResolveV4L2UVCCameraAdmission(
		context.Background(), plan, []string{"/dev/video0"}, query, nil,
	)
	if err != nil {
		t.Fatalf("capability-driven MJPEG admission failed: %v", err)
	}
	if got.Format != "mjpeg" || got.CaptureWidth != 1280 || got.CaptureHeight != 720 || got.CaptureFPS != 30 ||
		!got.RequiresSoftwareDecode || !got.ResizedCapture || got.Reason != "mjpeg_native_compatible_adapted" {
		t.Fatalf("unexpected MJPEG-only adaptation: %+v", got)
	}
}

func TestCameraAdmissionAcceptsMJPEGJPUUnderHardwareOnlyPolicy(t *testing.T) {
	plan := BrokerMuxPlan{
		CaptureFormat: "mjpeg", CaptureAspectPolicy: "crop",
		CaptureWidth: 640, CaptureHeight: 360, CaptureFPS: 30,
		MainWidth: 1280, MainHeight: 720, SubWidth: 640, SubHeight: 360,
		RequireHWCodec: true, MJPEGDecodeHWAvailable: true,
		H264EncodeHWAvailable: true, H265EncodeHWAvailable: true,
		ArchiveCodec: "h265", ArchiveContent: "processed",
	}
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{{PixelFormat: "mjpeg", Width: 640, Height: 360, FPS: 30}}, nil
	}
	got, err := ResolveV4L2UVCCameraAdmission(context.Background(), plan, []string{"/dev/video0"}, query, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Format != "mjpeg" || got.RequiresSoftwareDecode {
		t.Fatalf("MJPEG JPU path was not admitted as hardware: %+v", got)
	}
}

func TestCameraAdmissionSelectsMaximumMJPEGModeWithHardwareDecode(t *testing.T) {
	plan := BrokerMuxPlan{
		CaptureFormat:          "auto",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "prefer",
		CapturePolicy:          "analysis_quality",
		CaptureAspectPolicy:    "crop",
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
		RequireHWCodec:         true,
		MJPEGDecodeHWAvailable: true,
		H264EncodeHWAvailable:  true,
		H265EncodeHWAvailable:  true,
	}
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{
			{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30},
			{PixelFormat: "mjpeg", Width: 960, Height: 540, FPS: 30},
			{PixelFormat: "mjpeg", Width: 640, Height: 360, FPS: 30},
			{PixelFormat: "yuyv", Width: 1280, Height: 720, FPS: 10},
		}, nil
	}
	got, err := ResolveV4L2UVCCameraAdmission(context.Background(), plan, []string{"/dev/video0"}, query, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Format != "mjpeg" || got.CaptureWidth != 1280 || got.CaptureHeight != 720 || got.CaptureFPS != 30 ||
		got.RequiresSoftwareDecode || !got.ResizedCapture || got.Reason != "mjpeg_native_compatible_adapted" {
		t.Fatalf("maximum hardware MJPEG mode was not selected: %+v", got)
	}
}

func TestCameraAdmissionHasCanonicalFallbackIdentityAndPreferredNode(t *testing.T) {
	plan := BrokerMuxPlan{
		CaptureFormat:         "mjpeg",
		CaptureWidth:          640,
		CaptureHeight:         480,
		CaptureFPS:            30,
		MainWidth:             640,
		MainHeight:            480,
		SubWidth:              320,
		SubHeight:             240,
		AllowSWFallback:       true,
		H264EncodeHWAvailable: true,
	}
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{{PixelFormat: "mjpeg", Width: 640, Height: 480, FPS: 30}}, nil
	}
	got, err := ResolveV4L2UVCCameraAdmission(
		context.Background(), plan, []string{"/dev/video4"}, query, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceTransport != TransportV4L2UVC || got.PreferredDevicePath != "/dev/video4" ||
		got.DeviceIdentity != "v4l2_uvc:/dev/video4" {
		t.Fatalf("admission did not publish canonical fallback identity: %+v", got)
	}
}

func TestCameraAdmissionPreferredSourceRejectsForeignIdentityBeforeProbe(t *testing.T) {
	frontIdentity := "name=Front Camera;idVendor=2bc5;idProduct=0529;serial=00000000;port=1-1.3"
	overviewIdentity := "name=Overview Camera;idVendor=2ab8;idProduct=a101;port=1-1.4"
	frontCaps := []V4L2FormatCapability{
		{PixelFormat: "h265", Width: 1280, Height: 720, FPS: 30},
		{PixelFormat: "mjpeg", Width: 1280, Height: 720, FPS: 30},
	}
	plan := BrokerMuxPlan{
		Transport:              TransportV4L2UVC,
		CaptureDevicePath:      "/dev/camera/by-id/front",
		CaptureFormat:          "auto",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "prefer",
		CapturePolicy:          "analysis_quality",
		CaptureAspectPolicy:    "crop",
		CaptureWidth:           1280,
		CaptureHeight:          720,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		SubWidth:               640,
		SubHeight:              360,
		AllowSWFallback:        true,
		RequireHWCodec:         true,
		H264EncodeHWAvailable:  true,
		H265DecodeHWAvailable:  true,
		MJPEGDecodeHWAvailable: true,
		H265EncodeHWAvailable:  true,
		CaptureModeExclusions: []V4L2CaptureModeExclusion{{
			CapabilityFingerprint: V4L2CapabilityFingerprint(frontIdentity, frontCaps),
			PixelFormat:           "h265",
			Width:                 1280,
			Height:                720,
			FPS:                   30,
		}},
	}
	identities := map[string]string{
		"/dev/camera/by-id/front": frontIdentity,
		"/dev/video0":             frontIdentity,
		"/dev/video2":             overviewIdentity,
	}
	identity := func(device string) (string, error) {
		value, ok := identities[device]
		if !ok {
			return "", errors.New("identity unavailable")
		}
		return value, nil
	}
	queries := map[string]int{}
	query := func(_ context.Context, device string) ([]V4L2FormatCapability, error) {
		queries[device]++
		if device == "/dev/video2" {
			t.Fatal("foreign overview camera reached capability probing")
		}
		return frontCaps, nil
	}
	got, err := ResolveV4L2UVCCameraAdmission(
		context.Background(), plan,
		[]string{"/dev/camera/by-id/front", "/dev/video0", "/dev/video2"},
		query, identity,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.DevicePath != "/dev/camera/by-id/front" || got.DeviceIdentity != frontIdentity ||
		got.Format != "mjpeg" || got.CaptureWidth != 1280 || got.CaptureHeight != 720 || got.CaptureFPS != 30 {
		t.Fatalf("identity-bound fallback did not stay on front camera: %+v", got)
	}
	if !reflect.DeepEqual(got.ProbedDevices, []string{
		"/dev/camera/by-id/front", "/dev/video0",
	}) || !strings.Contains(strings.Join(got.RejectedDevices, ","), "/dev/video2:identity_mismatch") ||
		queries["/dev/video2"] != 0 {
		t.Fatalf("foreign source was not rejected before probing: got=%+v queries=%v", got, queries)
	}
}

func TestLinuxDeviceMajorMinorRoundTripsV4L2DeviceNumbers(t *testing.T) {
	makeDevice := func(major, minor uint64) uint64 {
		return (minor & 0xff) |
			((major & 0xfff) << 8) |
			((minor & ^uint64(0xff)) << 12) |
			((major & ^uint64(0xfff)) << 32)
	}
	for _, want := range [][2]uint64{{81, 0}, {81, 259}, {0x12345, 0xabcde}} {
		major, minor := linuxDeviceMajorMinor(makeDevice(want[0], want[1]))
		if major != want[0] || minor != want[1] {
			t.Fatalf("device number round trip got=%d:%d want=%d:%d", major, minor, want[0], want[1])
		}
	}
}

func TestCameraAdmissionPreferredSourceFailsBeforeProbeWithoutIdentity(t *testing.T) {
	for _, selector := range []string{"/dev/video0", "/dev/v4l/by-id/arbitrary-camera", "/dev/custom/source-a"} {
		t.Run(selector, func(t *testing.T) {
			plan := BrokerMuxPlan{
				Transport:         TransportV4L2UVC,
				CaptureDevicePath: selector,
				CaptureWidth:      1280,
				CaptureHeight:     720,
				CaptureFPS:        30,
			}
			queries := 0
			query := func(context.Context, string) ([]V4L2FormatCapability, error) {
				queries++
				return nil, errors.New("unexpected capability query")
			}
			identity := func(string) (string, error) {
				return "", errors.New("missing sysfs identity")
			}
			_, err := ResolveV4L2UVCCameraAdmission(
				context.Background(), plan,
				[]string{selector, "/dev/video0"},
				query, identity,
			)
			if err == nil || !strings.Contains(err.Error(), "cannot attest preferred source identity") || queries != 0 {
				t.Fatalf("preferred source identity failure was not fail-closed err=%v queries=%d", err, queries)
			}
		})
	}
}

func TestDeclaredCameraAdmissionBindsHBNSourceAndRequiresRuntimeProof(t *testing.T) {
	plan := BrokerMuxPlan{
		Transport:           TransportVendorHBN,
		SensorPrefix:        "imx219",
		CameraHostIndex:     1,
		CaptureFormat:       "auto",
		CaptureCodecPolicy:  "prefer",
		CaptureAspectPolicy: "crop",
		CaptureWidth:        1920,
		CaptureHeight:       1080,
		CaptureFPS:          30,
		ArchiveWidth:        2560,
		ArchiveHeight:       1440,
	}
	prepared, got, err := PrepareDeclaredCameraAdmission(plan)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceTransport != TransportVendorHBN ||
		got.DevicePath != "vendor_hbn:imx219:1" ||
		got.DeviceIdentity != got.DevicePath ||
		got.Format != "nv12" || got.Reason != "declared_sensor_stack" ||
		got.CapabilityCount != 1 ||
		!reflect.DeepEqual(got.ProbedDevices, []string{"vendor_hbn:imx219:1"}) ||
		!strings.HasPrefix(got.CapabilityFingerprint, "cap-v1:") {
		t.Fatalf("unexpected HBN admission: %+v", got)
	}
	if prepared.CaptureFormat != "nv12" || prepared.ArchiveWidth != 1920 || prepared.ArchiveHeight != 1080 {
		t.Fatalf("declared admission did not normalize graph: %+v", prepared)
	}
}

func TestDeclaredCameraAdmissionUsesOpaqueLibcameraSelector(t *testing.T) {
	plan := BrokerMuxPlan{
		Transport:         TransportLibcamera,
		CaptureDevicePath: "/base/soc/i2c0mux/i2c@1/imx708@1a",
		CaptureWidth:      1536,
		CaptureHeight:     864,
		CaptureFPS:        30,
	}
	_, got, err := PrepareDeclaredCameraAdmission(plan)
	if err != nil {
		t.Fatal(err)
	}
	want := "libcamera:/base/soc/i2c0mux/i2c@1/imx708@1a"
	if got.SourceTransport != TransportLibcamera || got.DevicePath != want || got.Format != "nv12" {
		t.Fatalf("unexpected libcamera admission: %+v", got)
	}
	autoPlan := plan
	autoPlan.CaptureDevicePath = ""
	_, automatic, err := PrepareDeclaredCameraAdmission(autoPlan)
	if err != nil || automatic.DevicePath != "libcamera:auto" {
		t.Fatalf("automatic libcamera selector was not canonical got=%+v err=%v", automatic, err)
	}
	if automatic.CapabilityFingerprint == got.CapabilityFingerprint {
		t.Fatal("selector change did not change declared capability fingerprint")
	}
}

func TestDeclaredCameraAdmissionRejectsUnknownOrIncompleteSource(t *testing.T) {
	if _, _, err := PrepareDeclaredCameraAdmission(BrokerMuxPlan{Transport: "rtsp", CaptureWidth: 1, CaptureHeight: 1, CaptureFPS: 1}); err == nil {
		t.Fatal("unknown declared source transport was admitted")
	}
	if _, _, err := PrepareDeclaredCameraAdmission(BrokerMuxPlan{Transport: TransportVendorHBN, SensorPrefix: "imx219"}); err == nil {
		t.Fatal("invalid declared capture geometry was admitted")
	}
}

func TestApplyV4L2CameraAdmissionRebindsArchiveToAdaptiveCadence(t *testing.T) {
	plan := BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0",
		CaptureWidth:      1920,
		CaptureHeight:     1080,
		CaptureFPS:        30,
		ArchiveWidth:      1920,
		ArchiveHeight:     1080,
		ArchiveFPS:        30,
		MainFPS:           20,
		SubFPS:            20,
	}
	decision := CameraAdmissionDecision{
		DevicePath:    "/dev/video0",
		Format:        "h265",
		CaptureWidth:  1920,
		CaptureHeight: 1080,
		CaptureFPS:    25,
	}
	got := applyV4L2UVCCameraAdmissionDecision(plan, decision)
	if got.CaptureFPS != 25 || got.ArchiveFPS != 25 {
		t.Fatalf("adaptive native cadence was not bound to archive: capture=%d archive=%d", got.CaptureFPS, got.ArchiveFPS)
	}
	if got.MainFPS != 20 || got.SubFPS != 20 {
		t.Fatalf("independent consumer cadence changed: main=%d sub=%d", got.MainFPS, got.SubFPS)
	}
}

func TestApplyV4L2CameraAdmissionPreservesExplicitLowerArchiveCadence(t *testing.T) {
	plan := BrokerMuxPlan{
		CaptureDevicePath: "/dev/video0", CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 30,
		ArchiveWidth: 640, ArchiveHeight: 360, ArchiveFPS: 15, MainFPS: 30, SubFPS: 20,
	}
	decision := CameraAdmissionDecision{
		DevicePath: "/dev/video0", Format: "mjpeg", CaptureWidth: 1280, CaptureHeight: 720, CaptureFPS: 30,
	}
	got := applyV4L2UVCCameraAdmissionDecision(plan, decision)
	if got.CaptureFPS != 30 || got.ArchiveFPS != 15 {
		t.Fatalf("lower archive cadence was overwritten: capture=%d archive=%d", got.CaptureFPS, got.ArchiveFPS)
	}
}

func TestFitArchiveGeometryNeverUpscalesNegotiatedCapture(t *testing.T) {
	tests := []struct {
		name                                               string
		requestedWidth, requestedHeight                    int
		captureWidth, captureHeight, wantWidth, wantHeight int
	}{
		{name: "native request", requestedWidth: 1920, requestedHeight: 1080, captureWidth: 1920, captureHeight: 1080, wantWidth: 1920, wantHeight: 1080},
		{name: "smaller native source", requestedWidth: 1920, requestedHeight: 1080, captureWidth: 1280, captureHeight: 720, wantWidth: 1280, wantHeight: 720},
		{name: "crop aspect inside source", requestedWidth: 1920, requestedHeight: 1080, captureWidth: 1600, captureHeight: 1200, wantWidth: 1600, wantHeight: 900},
		{name: "requested archive already smaller", requestedWidth: 1280, requestedHeight: 720, captureWidth: 1920, captureHeight: 1080, wantWidth: 1280, wantHeight: 720},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			width, height := FitArchiveGeometry(
				test.requestedWidth, test.requestedHeight,
				test.captureWidth, test.captureHeight,
			)
			if width != test.wantWidth || height != test.wantHeight {
				t.Fatalf("geometry=%dx%d want=%dx%d", width, height, test.wantWidth, test.wantHeight)
			}
		})
	}
}

func TestFitArchiveGeometryHonorsEncoderAlignmentWithoutDiscardingFallbackMode(t *testing.T) {
	width, height := FitArchiveGeometryWithAlignment(1280, 720, 960, 540, 8, 8)
	if width != 952 || height != 536 {
		t.Fatalf("aligned fallback geometry=%dx%d want=952x536", width, height)
	}
	if width%8 != 0 || height%8 != 0 {
		t.Fatalf("aligned fallback geometry violates 8x8 contract: %dx%d", width, height)
	}
	if width > 960 || height > 540 {
		t.Fatalf("aligned fallback geometry upscaled capture: %dx%d", width, height)
	}
}

func TestFitArchiveGeometryKeepsRDKH265FallbackInsideMacroblockContract(t *testing.T) {
	width, height := FitArchiveGeometryWithAlignment(960, 540, 960, 540, 16, 8)
	if width != 960 || height != 536 {
		t.Fatalf("field-profile H265 fallback geometry=%dx%d want=960x536", width, height)
	}

	width, height = FitArchiveGeometryWithAlignment(1280, 720, 848, 480, 16, 8)
	if width != 832 || height != 472 {
		t.Fatalf("aligned H265 fallback geometry=%dx%d want=832x472", width, height)
	}
	if width%16 != 0 || height%8 != 0 {
		t.Fatalf("H265 fallback geometry violates 16x8 contract: %dx%d", width, height)
	}
}

func TestFitArchiveGeometryUsesExactAspectComparator(t *testing.T) {
	width, height := FitArchiveGeometryWithAlignment(2392, 1200, 3337, 1668, 16, 8)
	if width != 2384 || height != 1200 {
		t.Fatalf("exact aspect election geometry=%dx%d want=2384x1200", width, height)
	}
}

func TestFitArchiveGeometryCombinesNV12AndProviderAlignment(t *testing.T) {
	width, height := FitArchiveGeometryWithAlignment(641, 361, 641, 361, 3, 5)
	if width%6 != 0 || height%10 != 0 || width > 641 || height > 361 {
		t.Fatalf("combined alignment was not enforced: %dx%d", width, height)
	}
}

func TestCameraAdmissionRequiredCodecFailsClosed(t *testing.T) {
	plan := BrokerMuxPlan{
		CaptureFormat:          "auto",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "required",
		CaptureAspectPolicy:    "crop",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		SubWidth:               640,
		SubHeight:              360,
		H264DecodeHWAvailable:  true,
		H265DecodeHWAvailable:  true,
	}
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{{PixelFormat: "h264", Width: 1920, Height: 1080, FPS: 30}}, nil
	}
	got, err := ResolveV4L2UVCCameraAdmission(
		context.Background(), plan, []string{"/dev/video4"}, query, nil,
	)
	if err == nil || got.DevicePath != "" || !strings.Contains(err.Error(), "required") {
		t.Fatalf("required codec was not enforced got=%+v err=%v", got, err)
	}
}

func TestCameraAdmissionAspectPolicyIsExplicit(t *testing.T) {
	caps := []V4L2FormatCapability{{PixelFormat: "h265", Width: 1280, Height: 1024, FPS: 30}}
	base := BrokerMuxPlan{
		CaptureFormat:          "auto",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "required",
		CapturePolicy:          "exact",
		CaptureWidth:           1280,
		CaptureHeight:          1024,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		SubWidth:               640,
		SubHeight:              360,
		H265DecodeHWAvailable:  true,
	}
	query := func(context.Context, string) ([]V4L2FormatCapability, error) { return caps, nil }

	exact := base
	exact.CaptureAspectPolicy = "exact"
	if _, err := ResolveV4L2UVCCameraAdmission(context.Background(), exact, []string{"/dev/video3"}, query, nil); err == nil {
		t.Fatal("5:4 input was admitted into 16:9 outputs with exact aspect policy")
	}

	crop := base
	crop.CaptureAspectPolicy = "crop"
	got, err := ResolveV4L2UVCCameraAdmission(context.Background(), crop, []string{"/dev/video3"}, query, nil)
	if err != nil || got.DevicePath != "/dev/video3" {
		t.Fatalf("crop policy did not admit normalizable input got=%+v err=%v", got, err)
	}
}

func TestCapabilityFingerprintIsOrderIndependent(t *testing.T) {
	left := []V4L2FormatCapability{
		{PixelFormat: "HEVC", Width: 1920, Height: 1080, FPS: 30},
		{PixelFormat: "H264", Width: 1280, Height: 720, FPS: 30},
	}
	right := []V4L2FormatCapability{left[1], left[0]}
	if got, want := V4L2CapabilityFingerprint("usb=0c45:6366", left), V4L2CapabilityFingerprint("usb=0c45:6366", right); got != want {
		t.Fatalf("fingerprint changed with capability order got=%s want=%s", got, want)
	}
}

func TestCameraAdmissionBindsRuntimeModeExclusionToCapabilityFingerprint(t *testing.T) {
	caps := []V4L2FormatCapability{
		{PixelFormat: "h265", Width: 1280, Height: 720, FPS: 30},
		{PixelFormat: "h265", Width: 1920, Height: 1080, FPS: 30},
		{PixelFormat: "h265", Width: 2560, Height: 1440, FPS: 30},
	}
	rejectedFingerprint := V4L2CapabilityFingerprint("usb=camera-a", caps)
	plan := BrokerMuxPlan{
		CaptureFormat:          "auto",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "required",
		CaptureAspectPolicy:    "crop",
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
			CapabilityFingerprint: rejectedFingerprint,
			PixelFormat:           "h265",
			Width:                 2560,
			Height:                1440,
			FPS:                   30,
		}},
	}
	query := func(context.Context, string) ([]V4L2FormatCapability, error) { return caps, nil }
	identity := func(string) (string, error) { return "usb=camera-b", nil }
	got, err := ResolveV4L2UVCCameraAdmission(
		context.Background(), plan, []string{"/dev/video3"}, query, identity,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.CapabilityFingerprint == rejectedFingerprint || got.CaptureWidth != 2560 || got.CaptureHeight != 1440 {
		t.Fatalf("one camera's runtime exclusion leaked into another module: %+v", got)
	}
}

func TestCameraAdmissionEnforcesHardwareCodecContract(t *testing.T) {
	plan := BrokerMuxPlan{
		CaptureFormat:          "auto",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "required",
		CaptureAspectPolicy:    "crop",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		SubWidth:               640,
		SubHeight:              360,
		RequireHWCodec:         true,
		H264EncodeHWAvailable:  true,
		H265DecodeHWAvailable:  false,
	}
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{{PixelFormat: "h265", Width: 1920, Height: 1080, FPS: 30}}, nil
	}
	if _, err := ResolveV4L2UVCCameraAdmission(context.Background(), plan, []string{"/dev/video7"}, query, nil); err == nil {
		t.Fatal("required H265 was admitted without a hardware decoder")
	}
}

func TestCameraAdmissionAdaptsNativeCompressedModeToDownstreamContract(t *testing.T) {
	plan := BrokerMuxPlan{
		CaptureFormat:          "auto",
		CaptureCodecPreference: "h265",
		CaptureCodecPolicy:     "required",
		CaptureAspectPolicy:    "crop",
		CapturePolicy:          "analysis_quality",
		CaptureWidth:           1920,
		CaptureHeight:          1080,
		CaptureFPS:             30,
		MainWidth:              1280,
		MainHeight:             720,
		SubWidth:               640,
		SubHeight:              360,
		H265DecodeHWAvailable:  true,
	}
	query := func(context.Context, string) ([]V4L2FormatCapability, error) {
		return []V4L2FormatCapability{{PixelFormat: "h265", Width: 1280, Height: 1024, FPS: 30}}, nil
	}
	got, err := ResolveV4L2UVCCameraAdmission(context.Background(), plan, []string{"/dev/video5"}, query, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.CaptureWidth != 1280 || got.CaptureHeight != 1024 || got.Reason != "h265_native_compressed_adapted" || !got.ResizedCapture {
		t.Fatalf("unexpected adapted admission: %+v", got)
	}
}
