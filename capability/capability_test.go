package capability

import (
	"context"
	"reflect"
	"testing"
	"time"
)

type fakeProbe struct {
	executables map[string]bool
	paths       map[string]bool
	commands    map[string]bool
	elements    map[string]bool
	features    map[string]bool
	arch        string
}

func (probe fakeProbe) Executable(path string) bool { return probe.executables[path] }
func (probe fakeProbe) Exists(path string) bool     { return probe.paths[path] }
func (probe fakeProbe) LookPath(name string) bool   { return probe.commands[name] }
func (probe fakeProbe) InspectGStreamer(_ context.Context, element, _ string) bool {
	return probe.elements[element]
}
func (probe fakeProbe) CPUFeature(name string) bool { return probe.features[name] }
func (probe fakeProbe) Architecture() string        { return probe.arch }

func TestDiscoveryFindsComponentsWithoutBoardIdentity(t *testing.T) {
	probe := fakeProbe{
		executables: map[string]bool{"/opt/video/bin/rdkx5_v4l2_mux_bridge": true},
		paths: map[string]bool{
			"/dev/vpu": true, "/dev/jpu": true, "/dev/dri/renderD128": true,
			"/usr/lib/aarch64-linux-gnu/libOpenCL.so": true,
		},
		commands: map[string]bool{"gst-inspect-1.0": true, "gst-launch-1.0": true},
		elements: map[string]bool{"videobrokertone": true, "videoscale": true, "mpph265enc": true, "x264enc": true, "x265enc": true, "avdec_h264": true, "avdec_h265": true, "jpegdec": true},
		features: map[string]bool{"asimd": true},
		arch:     "arm64",
	}
	inventory := DiscoverWithProbe(context.Background(), DiscoverOptions{EngineDirectory: "/opt/video/bin", ProbeTimeout: time.Second}, probe)
	for _, expectation := range []struct{ backend, stage, format string }{
		{"native_sp", StageEncode, "h265"},
		{"native_sp", StageDecode, "mjpeg"},
		{"native_opencl", StageToneMap, "nv12"},
		{"native_neon", StageResize, "nv12"},
		{"gstreamer_cpu", StageToneMap, "nv12"},
	} {
		if !inventory.Supports(expectation.backend, expectation.stage, expectation.format, false) {
			t.Fatalf("missing discovered capability %+v in %#v", expectation, inventory.Components)
		}
	}
	_, encoder, ok := inventory.FindOperation("native_sp", StageEncode, "h265", true)
	if !ok || encoder.WidthAlignment != 16 || encoder.HeightAlignment != 8 {
		t.Fatalf("native VPU geometry contract was not discovered: ok=%t operation=%+v", ok, encoder)
	}
	components := map[string]Component{}
	for _, discovered := range inventory.Components {
		components[discovered.ID] = discovered
		switch discovered.ConcurrencyMode {
		case ConcurrencyExclusive:
			if discovered.ConcurrencyGroup == "" || discovered.MaxConcurrentOperations != 1 {
				t.Fatalf("exclusive component has invalid capacity: %+v", discovered)
			}
		case ConcurrencyFinite:
			if discovered.ConcurrencyGroup == "" || discovered.MaxConcurrentOperations < 1 {
				t.Fatalf("finite component has invalid capacity: %+v", discovered)
			}
		case ConcurrencyShareable:
			if discovered.ConcurrencyGroup != "" || discovered.MaxConcurrentOperations != 0 {
				t.Fatalf("shareable component exposes a finite capacity: %+v", discovered)
			}
		default:
			t.Fatalf("component has no explicit concurrency contract: %+v", discovered)
		}
	}
	for _, componentID := range []string{"native.sp-vpu", "native.jpu"} {
		if component := components[componentID]; component.ConcurrencyMode != ConcurrencyFinite || component.MaxConcurrentOperations != 32 {
			t.Fatalf("%s lost its vendor codec capacity contract: %+v", componentID, component)
		}
	}
	for _, componentID := range []string{"native.sp-vpu", "gstreamer.mpp-codec", "gstreamer.vendor-codec", "gstreamer.v4l2-codec"} {
		component := components[componentID]
		if component.ConcurrencyGroup != "video-codec" || component.ConcurrencyMode != ConcurrencyFinite || component.MaxConcurrentOperations != 32 {
			t.Fatalf("%s does not share the attested physical codec contract: %+v", componentID, component)
		}
	}
	if component := components["native.jpu"]; component.ConcurrencyGroup != "jpeg-codec" {
		t.Fatalf("JPU is not isolated as its own physical resource: %+v", component)
	}
	if component := components["cpu.neon-nv12"]; component.ConcurrencyMode != ConcurrencyShareable {
		t.Fatalf("NEON implementation acquired an artificial exclusive lease: %+v", component)
	}
}

func TestDiscoveryRequiresObservedASIMDForNEON(t *testing.T) {
	probe := fakeProbe{
		executables: map[string]bool{"/opt/video/bin/rdkx5_v4l2_mux_bridge": true},
		paths:       map[string]bool{}, commands: map[string]bool{}, elements: map[string]bool{},
		features: map[string]bool{}, arch: "arm64",
	}
	inventory := DiscoverWithProbe(context.Background(), DiscoverOptions{EngineDirectory: "/opt/video/bin", ProbeTimeout: time.Second}, probe)
	if inventory.Supports("native_neon", StageResize, "nv12", false) {
		t.Fatal("architecture name alone admitted NEON without an ASIMD feature observation")
	}
	if !inventory.Supports("native_cpu", StageResize, "nv12", false) {
		t.Fatal("bounded scalar fallback disappeared when ASIMD was unavailable")
	}
}

func TestPortableHardwareUsesUniversalExclusiveDefaultWithoutCapacityEvidence(t *testing.T) {
	probe := fakeProbe{
		executables: map[string]bool{"/opt/video/bin/video-ingest-gst": true},
		paths:       map[string]bool{"/dev/vpu": true},
		commands:    map[string]bool{"gst-inspect-1.0": true, "gst-launch-1.0": true},
		elements:    map[string]bool{"mpph265enc": true},
		features:    map[string]bool{},
		arch:        "arm64",
	}
	inventory := DiscoverWithProbe(context.Background(), DiscoverOptions{EngineDirectory: "/opt/video/bin", ProbeTimeout: time.Second}, probe)
	foundPortableCodec := false
	for _, component := range inventory.Components {
		if component.ConcurrencyGroup != "video-codec" || !component.Hardware {
			continue
		}
		if component.Backend == "gstreamer_mpp" && component.Available {
			foundPortableCodec = true
		}
		if component.ConcurrencyMode != ConcurrencyExclusive || component.MaxConcurrentOperations != 1 {
			t.Fatalf("portable hardware gained an unattested finite capacity: %+v", component)
		}
		for _, item := range component.Evidence {
			if item.Kind == "vendor_codec_capacity" {
				t.Fatalf("portable hardware published fabricated capacity evidence: %+v", component)
			}
		}
	}
	if !foundPortableCodec {
		t.Fatal("portable hardware codec fixture was not discovered")
	}
}

func TestPlannerPrefersNativeHardwareAndOpenCL(t *testing.T) {
	inventory := Inventory{Components: []Component{
		workerComponent("native.v4l2-worker", "native_sp", false, "v4l2", "v4l2_uvc"),
		component("vpu", "native_sp", true, true, "", []Operation{
			{Stage: StageEncode, Format: "h264"}, {Stage: StageEncode, Format: "h265"},
			{Stage: StageDecode, Format: "h265"},
		}),
		component("tone", "native_opencl", true, true, "", []Operation{{Stage: StageToneMap, Format: "nv12"}}),
		component("resize", "native_neon", true, true, "", []Operation{{Stage: StageResize, Format: "nv12"}}),
	}}
	plan, err := Resolve(inventory, ProviderPolicy{
		EncodeProvider: "auto", DecodeProvider: "auto", ToneMapAccelerator: "auto",
		RequireHardwareCodec: true,
	}, Requirements{
		Transport: "v4l2_uvc", CaptureFormat: "h265", LiveCodec: "h264", LiveEncode: true, ArchiveCodec: "h265",
		ArchiveEncode: true, ToneMap: true, ResizeRoles: []string{"analysis"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.EncodeProvider != "native_sp" || plan.DecodeProvider != "native_sp" || plan.ToneMapBackend != "native_opencl" {
		t.Fatalf("unexpected hardware plan: %+v", plan)
	}
}

func TestPlannerCarriesEncoderGeometryContractPerOutput(t *testing.T) {
	inventory := Inventory{Components: []Component{
		workerComponent("native.v4l2-worker", "native_sp", false, "v4l2", "v4l2_uvc"),
		component("vpu", "native_sp", true, true, "", []Operation{
			{Stage: StageEncode, Format: "h264", WidthAlignment: 8, HeightAlignment: 8},
			{Stage: StageEncode, Format: "h265", WidthAlignment: 16, HeightAlignment: 8},
			{Stage: StageDecode, Format: "mjpeg"},
		}),
	}}
	plan, err := Resolve(inventory, ProviderPolicy{
		EncodeProvider: "native_sp", DecodeProvider: "native_sp", RequireHardwareCodec: true,
	}, Requirements{
		Transport: "v4l2_uvc", CaptureFormat: "mjpeg",
		LiveCodec: "h264", LiveEncode: true, ArchiveCodec: "h265", ArchiveEncode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]StageSelection{}
	for _, stage := range plan.Stages {
		if stage.Stage == StageEncode {
			found[stage.Role] = stage
		}
	}
	if found["live"].WidthAlignment != 8 || found["live"].HeightAlignment != 8 {
		t.Fatalf("live geometry contract lost: %+v", found["live"])
	}
	if found["archive"].WidthAlignment != 16 || found["archive"].HeightAlignment != 8 {
		t.Fatalf("archive geometry contract lost: %+v", found["archive"])
	}
}

func TestPlannerDoesNotAdmitInstalledHBNSensorTransformIntoV4L2Graph(t *testing.T) {
	inventory := Inventory{Components: []Component{
		workerComponent("native.v4l2-worker", "native_sp", false, "v4l2", "v4l2_uvc"),
		workerComponent("native.hbn-worker", "native_sp", true, "vendor_hbn"),
		component("vpu", "native_sp", true, true, "", []Operation{
			{Stage: StageEncode, Format: "h264"}, {Stage: StageEncode, Format: "h265"},
			{Stage: StageDecode, Format: "h265"},
		}),
		component("vse", "native_sp_resize", true, true, "", []Operation{{Stage: StageResize, Format: "nv12"}}),
		component("neon", "native_neon", true, true, "", []Operation{{Stage: StageResize, Format: "nv12"}}),
		component("cpu", "native_cpu", false, true, "", []Operation{{Stage: StageResize, Format: "nv12"}}),
	}}
	requirements := Requirements{
		Transport: "v4l2_uvc", CaptureFormat: "h265", LiveCodec: "h264", LiveEncode: true,
		ArchiveCodec: "h265", ArchiveEncode: true, ResizeRoles: []string{"analysis"},
	}
	plan, err := Resolve(inventory, ProviderPolicy{
		EncodeProvider: "native_sp", DecodeProvider: "native_sp", AllowSoftwareFallback: true,
	}, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ResizeBackend != "native_neon" {
		t.Fatalf("V4L2 graph selected unrelated installed HBN transform: %+v", plan)
	}
	requirements.Transport = "vendor_hbn"
	plan, err = Resolve(inventory, ProviderPolicy{
		EncodeProvider: "native_sp", DecodeProvider: "native_sp", AllowSoftwareFallback: true,
	}, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ResizeBackend != "native_sp_resize" {
		t.Fatalf("HBN graph did not select its sensor transform: %+v", plan)
	}
}

func TestPlannerUsesPortableSoftwareOnlyWhenPolicyAllows(t *testing.T) {
	inventory := Inventory{Components: []Component{
		workerComponent("gstreamer.worker", "gstreamer", false, "v4l2", "v4l2_uvc"),
		component("software", "software", false, true, "", []Operation{
			{Stage: StageEncode, Format: "h264"}, {Stage: StageEncode, Format: "h265"},
			{Stage: StageDecode, Format: "h265"},
		}),
		component("tone", "gstreamer_cpu", false, true, "", []Operation{{Stage: StageToneMap, Format: "nv12"}}),
		component("resize", "gstreamer_video", false, true, "", []Operation{{Stage: StageResize, Format: "nv12"}}),
	}}
	requirements := Requirements{Transport: "v4l2_uvc", CaptureFormat: "h265", LiveCodec: "h264", LiveEncode: true, ArchiveCodec: "h265", ArchiveEncode: true, ToneMap: true, ResizeRoles: []string{"analysis"}}
	if _, err := Resolve(inventory, ProviderPolicy{EncodeProvider: "auto", DecodeProvider: "auto", AllowSoftwareFallback: false}, requirements); err == nil {
		t.Fatal("software plan admitted while fallback was forbidden")
	}
	plan, err := Resolve(inventory, ProviderPolicy{EncodeProvider: "auto", DecodeProvider: "auto", AllowSoftwareFallback: true}, requirements)
	if err != nil {
		t.Fatal(err)
	}
	if plan.EncodeProvider != "software" || plan.DecodeProvider != "software" || plan.ToneMapBackend != "gstreamer_cpu" {
		t.Fatalf("unexpected portable plan: %+v", plan)
	}
}

func TestPlannerSelectsBestProviderPerEncodedOutput(t *testing.T) {
	inventory := Inventory{Components: []Component{
		workerComponent("gstreamer.worker", "gstreamer", false, "libcamera"),
		component("v4l2-h264", "gstreamer_v4l2", true, true, "", []Operation{{Stage: StageEncode, Format: "h264"}}),
		component("software", "software", false, true, "", []Operation{
			{Stage: StageEncode, Format: "h264"}, {Stage: StageEncode, Format: "h265"},
		}),
		component("tone", "gstreamer_cpu", false, true, "", []Operation{{Stage: StageToneMap, Format: "nv12"}}),
		component("resize", "gstreamer_video", false, true, "", []Operation{{Stage: StageResize, Format: "nv12"}}),
	}}
	plan, err := Resolve(inventory, ProviderPolicy{
		EncodeProvider: "auto", DecodeProvider: "auto", AllowSoftwareFallback: true,
	}, Requirements{
		Transport: "libcamera", CaptureFormat: "nv12", LiveCodec: "h264", LiveEncode: true,
		ArchiveCodec: "h265", ArchiveEncode: true, ToneMap: true, ResizeRoles: []string{"analysis"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.EncodeProvider != "gstreamer_v4l2" || plan.ArchiveEncodeProvider != "software" {
		t.Fatalf("planner did not preserve per-output acceleration: %+v", plan)
	}
}

func TestPlannerSupportsNativeAnalysisOnlyWithoutInventingCodecStages(t *testing.T) {
	inventory := Inventory{Components: []Component{
		workerComponent("native.v4l2-worker", "native_sp", false, "v4l2_uvc"),
		component("native.tone-cpu", "native_cpu", false, true, "", []Operation{{Stage: StageToneMap, Format: "nv12"}}),
		component("native.cpu-transform", "native_cpu", false, true, "", []Operation{{Stage: StageResize, Format: "nv12"}}),
	}}
	plan, err := Resolve(inventory, ProviderPolicy{
		EncodeProvider: "auto", DecodeProvider: "auto", ToneMapAccelerator: "cpu", AllowSoftwareFallback: true,
	}, Requirements{Transport: "v4l2_uvc", CaptureFormat: "nv12", ToneMap: true, ResizeRoles: []string{"analysis"}})
	if err != nil {
		t.Fatal(err)
	}
	if plan.WorkerProvider != "native_sp" || plan.EncodeProvider != "native_sp" || plan.DecodeProvider != "native_sp" {
		t.Fatalf("analysis-only plan lost its native worker boundary: %+v", plan)
	}
	stages := map[string]int{}
	for _, stage := range plan.Stages {
		stages[stage.Stage]++
		if stage.Stage == StageEncode || stage.Stage == StageDecode {
			t.Fatalf("analysis-only raw graph invented a codec stage: %+v", stage)
		}
	}
	if stages[StageCapture] != 1 || stages[StageToneMap] != 1 || stages[StageResize] != 1 {
		t.Fatalf("analysis-only graph stages=%+v plan=%+v", stages, plan)
	}
}

func TestPlannerSupportsPortableAnalysisOnlyPassthrough(t *testing.T) {
	inventory := Inventory{Components: []Component{
		workerComponent("gstreamer.worker", "gstreamer", false, "libcamera"),
	}}
	plan, err := Resolve(inventory, ProviderPolicy{
		EncodeProvider: "auto", DecodeProvider: "auto", AllowSoftwareFallback: true,
	}, Requirements{Transport: "libcamera", CaptureFormat: "nv12"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.WorkerProvider != "gstreamer" || plan.EncodeProvider != "software" || plan.DecodeProvider != "software" || len(plan.Stages) != 1 || plan.Stages[0].Stage != StageCapture {
		t.Fatalf("portable analysis passthrough plan=%+v", plan)
	}
}

func TestPlannerAccountsResizeCapacityPerStreamRole(t *testing.T) {
	inventory := Inventory{Components: []Component{
		workerComponent("native.v4l2-worker", "native_sp", false, "v4l2_uvc"),
		component("native.resize", "native_neon", true, true, "", []Operation{{Stage: StageResize, Format: "nv12"}}),
	}}
	plan, err := Resolve(inventory, ProviderPolicy{
		EncodeProvider: "auto", DecodeProvider: "auto", AllowSoftwareFallback: true,
	}, Requirements{
		Transport: "v4l2_uvc", CaptureFormat: "nv12",
		ResizeRoles: []string{"analysis", "archive", "live"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var roles []string
	for _, stage := range plan.Stages {
		if stage.Stage == StageResize {
			roles = append(roles, stage.Role)
		}
	}
	if !reflect.DeepEqual(roles, []string{"analysis", "archive", "live"}) {
		t.Fatalf("resize capacity was not represented per role: %+v", plan.Stages)
	}
}

func TestPlannerRejectsNonCanonicalResizeRoles(t *testing.T) {
	inventory := Inventory{Components: []Component{
		workerComponent("native.v4l2-worker", "native_sp", false, "v4l2_uvc"),
		component("native.resize", "native_neon", true, true, "", []Operation{{Stage: StageResize, Format: "nv12"}}),
	}}
	for _, roles := range [][]string{
		{"live", "analysis"},
		{"analysis", "analysis"},
		{"source"},
	} {
		_, err := Resolve(inventory, ProviderPolicy{
			EncodeProvider: "auto", DecodeProvider: "auto", AllowSoftwareFallback: true,
		}, Requirements{Transport: "v4l2_uvc", CaptureFormat: "nv12", ResizeRoles: roles})
		if err == nil {
			t.Fatalf("non-canonical resize roles were accepted: %v", roles)
		}
	}
}

func workerComponent(id, backend string, hardware bool, transports ...string) Component {
	operations := make([]Operation, 0, len(transports))
	for _, transport := range transports {
		operations = append(operations, Operation{Stage: StageCapture, Format: transport})
	}
	return component(id, backend, hardware, true, "", operations)
}
