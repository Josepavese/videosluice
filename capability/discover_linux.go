package capability

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type DiscoverOptions struct {
	EngineDirectory string
	ProbeTimeout    time.Duration
}

type Probe interface {
	Executable(string) bool
	Exists(string) bool
	LookPath(string) bool
	InspectGStreamer(context.Context, string, string) bool
	CPUFeature(string) bool
	Architecture() string
}

type OSProbe struct{}

func (OSProbe) Executable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o111 != 0
}

func (OSProbe) Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (OSProbe) LookPath(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

func (OSProbe) CPUFeature(name string) bool {
	content, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return false
	}
	name = strings.ToLower(strings.TrimSpace(name))
	for _, line := range strings.Split(strings.ToLower(string(content)), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 || (strings.TrimSpace(parts[0]) != "features" && strings.TrimSpace(parts[0]) != "flags") {
			continue
		}
		for _, feature := range strings.Fields(parts[1]) {
			if feature == name {
				return true
			}
		}
	}
	return false
}

func (OSProbe) InspectGStreamer(ctx context.Context, element, pluginDirectory string) bool {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// The probe is already isolated in an owned subprocess. Scan inside it:
	// killing gst-inspect on timeout must not orphan gst-plugin-scanner in
	// the broker's process group. Plugin failures stay inside this probe.
	cmd := exec.CommandContext(probeCtx, "gst-inspect-1.0", "--gst-disable-registry-fork", element)
	if directory := strings.TrimSpace(pluginDirectory); directory != "" {
		environment := os.Environ()
		current := strings.TrimSpace(os.Getenv("GST_PLUGIN_PATH"))
		if current != "" {
			directory += string(os.PathListSeparator) + current
		}
		cmd.Env = append(environment, "GST_PLUGIN_PATH="+directory)
	}
	return cmd.Run() == nil
}

func (OSProbe) Architecture() string { return runtime.GOARCH }

func Discover(ctx context.Context, options DiscoverOptions) Inventory {
	return DiscoverWithProbe(ctx, options, OSProbe{})
}

func DiscoverWithProbe(ctx context.Context, options DiscoverOptions, probe Probe) Inventory {
	if options.ProbeTimeout <= 0 {
		options.ProbeTimeout = 8 * time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, options.ProbeTimeout)
	defer cancel()
	engineDirectory := filepath.Clean(strings.TrimSpace(options.EngineDirectory))
	if engineDirectory == "." || engineDirectory == string(filepath.Separator) {
		engineDirectory = ""
	}
	inventory := Inventory{Schema: Schema, DiscoveredAt: time.Now().UTC()}

	nativeV4L2 := productWorker(probe, engineDirectory, "rdkx5_v4l2_mux_bridge")
	nativeHBN := productWorker(probe, engineDirectory, "rdkx5_hbn_mux_bridge")
	nativeWorker := nativeV4L2 || nativeHBN
	vpu := probe.Exists("/dev/vpu")
	jpu := probe.Exists("/dev/jpu")
	opencl := nativeWorker && hasOpenCLRuntime(probe)

	inventory.Components = append(inventory.Components,
		component(
			"native.v4l2-worker", "native_sp", false, nativeV4L2,
			boolReason(nativeV4L2, "native V4L2 product worker unavailable"),
			[]Operation{{Stage: StageCapture, Format: "v4l2"}, {Stage: StageCapture, Format: "v4l2_uvc"}},
			evidence(nativeV4L2, "product_worker", "rdkx5_v4l2_mux_bridge"),
		),
		withConcurrencyGroup(component(
			"native.hbn-worker", "native_sp", true, nativeHBN,
			boolReason(nativeHBN, "native HBN product worker unavailable"),
			[]Operation{{Stage: StageCapture, Format: "vendor_hbn"}},
			evidence(nativeHBN, "product_worker", "rdkx5_hbn_mux_bridge"),
		), "hbn-capture"),
		withConcurrentCapacity(withConcurrencyGroup(component(
			"native.sp-vpu", "native_sp", true, nativeWorker && vpu,
			availabilityReason(nativeWorker, vpu, "native worker", "/dev/vpu"),
			[]Operation{
				{Stage: StageEncode, Format: "h264", WidthAlignment: 8, HeightAlignment: 8},
				{Stage: StageEncode, Format: "h265", WidthAlignment: 16, HeightAlignment: 8},
				{Stage: StageDecode, Format: "h264"},
				{Stage: StageDecode, Format: "h265"},
			},
			evidence(nativeWorker, "product_worker", engineDirectory), evidence(vpu, "device", "/dev/vpu"),
		), "video-codec"), 32, evidence(nativeWorker && vpu, "vendor_codec_capacity", "32")),
		withConcurrentCapacity(withConcurrencyGroup(component(
			"native.jpu", "native_sp", true, nativeV4L2 && jpu,
			availabilityReason(nativeV4L2, jpu, "native V4L2 worker", "/dev/jpu"),
			[]Operation{{Stage: StageDecode, Format: "mjpeg"}},
			evidence(nativeV4L2, "product_worker", "rdkx5_v4l2_mux_bridge"), evidence(jpu, "device", "/dev/jpu"),
		), "jpeg-codec"), 32, evidence(nativeV4L2 && jpu, "vendor_codec_capacity", "32")),
		component(
			"native.mjpeg-cpu", "native_sp", false, nativeV4L2,
			boolReason(nativeV4L2, "native V4L2 worker unavailable"),
			[]Operation{{Stage: StageDecode, Format: "mjpeg"}},
			evidence(nativeV4L2, "product_worker", "rdkx5_v4l2_mux_bridge"),
		),
		withConcurrencyGroup(component(
			"native.tone-opencl", "native_opencl", true, opencl,
			boolReason(opencl, "OpenCL runtime or render device unavailable"),
			[]Operation{{Stage: StageToneMap, Format: "nv12"}},
			evidence(nativeWorker, "product_worker", "native_mux"), evidence(opencl, "runtime", "opencl"),
		), "gpu-opencl"),
		component(
			"native.tone-cpu", "native_cpu", false, nativeWorker,
			boolReason(nativeWorker, "native product worker unavailable"),
			[]Operation{{Stage: StageToneMap, Format: "nv12"}},
			evidence(nativeWorker, "product_worker", "native_mux"),
		),
	)
	if nativeHBN {
		inventory.Components = append(inventory.Components, withConcurrencyGroup(component(
			"native.sensor-transform", "native_sp_resize", true, true, "",
			[]Operation{{Stage: StageResize, Format: "nv12"}, {Stage: StageConvert, Format: "nv12"}},
			Evidence{Kind: "product_worker", Value: "rdkx5_hbn_mux_bridge"},
		), "image-transform-vse"))
	}
	if nativeV4L2 && probe.Architecture() == "arm64" && probe.CPUFeature("asimd") {
		inventory.Components = append(inventory.Components, shareableComponent(component(
			"cpu.neon-nv12", "native_neon", true, true, "",
			[]Operation{{Stage: StageResize, Format: "nv12"}, {Stage: StageConvert, Format: "nv12"}},
			Evidence{Kind: "cpu_feature", Value: "asimd"},
		)))
	}
	if nativeV4L2 {
		inventory.Components = append(inventory.Components, component(
			"native.cpu-transform", "native_cpu", false, true, "",
			[]Operation{{Stage: StageResize, Format: "nv12"}, {Stage: StageConvert, Format: "nv12"}},
			Evidence{Kind: "product_worker", Value: "rdkx5_v4l2_mux_bridge"},
		))
	}

	gstAvailable := probe.LookPath("gst-inspect-1.0") && probe.LookPath("gst-launch-1.0")
	gstreamerWorker := gstAvailable && productWorker(probe, engineDirectory, "video-ingest-gst")
	inventory.Components = append(inventory.Components, component(
		"gstreamer.worker", "gstreamer", false, gstreamerWorker,
		boolReason(gstreamerWorker, "portable GStreamer product worker unavailable"),
		[]Operation{
			{Stage: StageCapture, Format: "v4l2"},
			{Stage: StageCapture, Format: "v4l2_uvc"},
			{Stage: StageCapture, Format: "libcamera"},
		},
		evidence(gstreamerWorker, "product_worker", "video-ingest-gst"),
	))
	inspect := func(element string) bool {
		return gstAvailable && probe.InspectGStreamer(probeCtx, element, engineDirectory)
	}
	addGStreamerCodecComponents(&inventory, inspect, nativeWorker && vpu)

	portableTone := inspect("videobrokertone")
	portableOpenCL := portableTone && hasOpenCLRuntime(probe)
	inventory.Components = append(inventory.Components,
		withConcurrencyGroup(component(
			"gstreamer.tone-opencl", "gstreamer_opencl", true, portableOpenCL,
			boolReason(portableOpenCL, "videobrokertone or OpenCL runtime unavailable"),
			[]Operation{{Stage: StageToneMap, Format: "nv12"}},
			evidence(portableTone, "gstreamer_element", "videobrokertone"), evidence(portableOpenCL, "runtime", "opencl"),
		), "gpu-opencl"),
		component(
			"gstreamer.tone-cpu", "gstreamer_cpu", false, portableTone,
			boolReason(portableTone, "videobrokertone element unavailable"),
			[]Operation{{Stage: StageToneMap, Format: "nv12"}},
			evidence(portableTone, "gstreamer_element", "videobrokertone"),
		),
		component(
			"gstreamer.videoscale", "gstreamer_video", false, inspect("videoscale"),
			boolReason(inspect("videoscale"), "videoscale element unavailable"),
			[]Operation{{Stage: StageResize, Format: "nv12"}},
			evidence(gstAvailable, "command", "gst-launch-1.0"),
		),
	)
	if rgaElement := firstAvailable(inspect, "rgaconvert", "rga", "mppvideoconvert"); rgaElement != "" {
		inventory.Components = append(inventory.Components, withConcurrencyGroup(component(
			"gstreamer.rga-transform", "gstreamer_mpp_resize", true, true, "",
			[]Operation{{Stage: StageResize, Format: "nv12"}, {Stage: StageConvert, Format: "nv12"}},
			Evidence{Kind: "gstreamer_element", Value: rgaElement},
		), "image-transform-rga"))
	}

	sort.Slice(inventory.Components, func(left, right int) bool {
		return inventory.Components[left].ID < inventory.Components[right].ID
	})
	return inventory
}

func addGStreamerCodecComponents(inventory *Inventory, inspect func(string) bool, vendorCodecCapacityAttested bool) {
	type codecSpec struct {
		id       string
		backend  string
		hardware bool
		entries  []struct {
			element    string
			operations []Operation
		}
	}
	specs := []codecSpec{
		{id: "gstreamer.mpp-codec", backend: "gstreamer_mpp", hardware: true, entries: []struct {
			element    string
			operations []Operation
		}{
			{element: "mpph264enc", operations: []Operation{{Stage: StageEncode, Format: "h264"}}},
			{element: "mpph265enc", operations: []Operation{{Stage: StageEncode, Format: "h265"}}},
			{element: "mppvideodec", operations: []Operation{{Stage: StageDecode, Format: "h264"}, {Stage: StageDecode, Format: "h265"}, {Stage: StageDecode, Format: "mjpeg"}}},
		}},
		{id: "gstreamer.v4l2-codec", backend: "gstreamer_v4l2", hardware: true, entries: []struct {
			element    string
			operations []Operation
		}{
			{element: firstAvailable(inspect, "v4l2h264enc", "v4l2slh264enc"), operations: []Operation{{Stage: StageEncode, Format: "h264"}}},
			{element: firstAvailable(inspect, "v4l2h265enc", "v4l2slh265enc"), operations: []Operation{{Stage: StageEncode, Format: "h265"}}},
			{element: firstAvailable(inspect, "v4l2h264dec", "v4l2slh264dec"), operations: []Operation{{Stage: StageDecode, Format: "h264"}}},
			{element: firstAvailable(inspect, "v4l2h265dec", "v4l2slh265dec"), operations: []Operation{{Stage: StageDecode, Format: "h265"}}},
		}},
		{id: "gstreamer.vendor-codec", backend: "gstreamer_vendor", hardware: true, entries: []struct {
			element    string
			operations []Operation
		}{
			{element: firstAvailable(inspect, "x5h264enc", "mpph264enc", "v4l2h264enc", "v4l2slh264enc"), operations: []Operation{{Stage: StageEncode, Format: "h264"}}},
			{element: firstAvailable(inspect, "x5h265enc", "mpph265enc", "v4l2h265enc", "v4l2slh265enc"), operations: []Operation{{Stage: StageEncode, Format: "h265"}}},
			{element: firstAvailable(inspect, "x5h264dec", "mppvideodec", "v4l2h264dec", "v4l2slh264dec"), operations: []Operation{{Stage: StageDecode, Format: "h264"}}},
			{element: firstAvailable(inspect, "x5h265dec", "mppvideodec", "v4l2h265dec", "v4l2slh265dec"), operations: []Operation{{Stage: StageDecode, Format: "h265"}}},
			{element: firstAvailable(inspect, "mppvideodec"), operations: []Operation{{Stage: StageDecode, Format: "mjpeg"}}},
		}},
		{id: "gstreamer.software-codec", backend: "software", hardware: false, entries: []struct {
			element    string
			operations []Operation
		}{
			{element: firstAvailable(inspect, "x264enc", "openh264enc"), operations: []Operation{{Stage: StageEncode, Format: "h264"}}},
			{element: "x265enc", operations: []Operation{{Stage: StageEncode, Format: "h265"}}},
			{element: "avdec_h264", operations: []Operation{{Stage: StageDecode, Format: "h264"}}},
			{element: "avdec_h265", operations: []Operation{{Stage: StageDecode, Format: "h265"}}},
			{element: "jpegdec", operations: []Operation{{Stage: StageDecode, Format: "mjpeg"}}},
		}},
	}
	for _, spec := range specs {
		operations := []Operation{}
		evidenceList := []Evidence{}
		for _, entry := range spec.entries {
			if entry.element == "" || !inspect(entry.element) {
				continue
			}
			operations = append(operations, entry.operations...)
			evidenceList = append(evidenceList, Evidence{Kind: "gstreamer_element", Value: entry.element})
		}
		available := len(operations) > 0
		value := component(
			spec.id, spec.backend, spec.hardware, available,
			boolReason(available, "no compatible GStreamer element was found"),
			operations, evidenceList...,
		)
		if spec.hardware {
			value = withConcurrencyGroup(value, "video-codec")
			value = withConcurrentCapacity(
				value, 32,
				evidence(vendorCodecCapacityAttested, "vendor_codec_capacity", "32"),
			)
		}
		*inventory = appendComponent(*inventory, value)
	}
}

func appendComponent(inventory Inventory, value Component) Inventory {
	inventory.Components = append(inventory.Components, value)
	return inventory
}

func productWorker(probe Probe, directory, name string) bool {
	if directory == "" {
		return false
	}
	return probe.Executable(filepath.Join(directory, name))
}

func hasOpenCLRuntime(probe Probe) bool {
	if !probe.Exists("/dev/dri/renderD128") && !probe.Exists("/dev/mali0") {
		return false
	}
	for _, path := range []string{
		"/usr/lib/libOpenCL.so", "/usr/lib/aarch64-linux-gnu/libOpenCL.so",
		"/lib/libOpenCL.so", "/lib/aarch64-linux-gnu/libOpenCL.so",
		"/usr/hobot/lib/libOpenCL.so", "/opt/hobot/lib/libOpenCL.so",
		"/usr/lib/libOpenCL.so.1", "/usr/lib/aarch64-linux-gnu/libOpenCL.so.1",
		"/lib/libOpenCL.so.1", "/lib/aarch64-linux-gnu/libOpenCL.so.1",
	} {
		if probe.Exists(path) {
			return true
		}
	}
	return false
}

func firstAvailable(inspect func(string) bool, elements ...string) string {
	for _, element := range elements {
		if element != "" && inspect(element) {
			return element
		}
	}
	return ""
}

func component(id, backend string, hardware, available bool, reason string, operations []Operation, evidenceValues ...Evidence) Component {
	evidenceValues = compactEvidence(evidenceValues)
	state := StateRejected
	if available {
		state = StateCandidate
		reason = ""
	}
	return Component{
		ID: id, Backend: backend, Hardware: hardware, Available: available,
		State: state, Reason: reason, ConcurrencyMode: defaultConcurrencyMode(hardware),
		ConcurrencyGroup:        defaultConcurrencyGroup(id, hardware),
		MaxConcurrentOperations: defaultConcurrentOperations(hardware),
		Operations:              operations, Evidence: evidenceValues,
	}
}

func defaultConcurrencyGroup(id string, hardware bool) string {
	if hardware {
		return id
	}
	return ""
}

func defaultConcurrencyMode(hardware bool) string {
	if hardware {
		return ConcurrencyExclusive
	}
	return ConcurrencyShareable
}

func defaultConcurrentOperations(hardware bool) int {
	if hardware {
		return 1
	}
	return 0
}

func withConcurrentCapacity(value Component, capacity int, capacityEvidence Evidence) Component {
	if capacity > 0 && capacityEvidence.Kind != "" && capacityEvidence.Value != "" {
		value.ConcurrencyMode = ConcurrencyFinite
		value.MaxConcurrentOperations = capacity
		value.Evidence = append(value.Evidence, capacityEvidence)
	}
	return value
}

func withConcurrencyGroup(value Component, group string) Component {
	if value.Hardware && strings.TrimSpace(group) != "" {
		value.ConcurrencyGroup = strings.TrimSpace(group)
	}
	return value
}

func shareableComponent(value Component) Component {
	value.ConcurrencyMode = ConcurrencyShareable
	value.ConcurrencyGroup = ""
	value.MaxConcurrentOperations = 0
	return value
}

func evidence(present bool, kind, value string) Evidence {
	if !present || strings.TrimSpace(value) == "" {
		return Evidence{}
	}
	return Evidence{Kind: kind, Value: value}
}

func compactEvidence(values []Evidence) []Evidence {
	out := make([]Evidence, 0, len(values))
	for _, value := range values {
		if value.Kind != "" && value.Value != "" {
			out = append(out, value)
		}
	}
	return out
}

func boolReason(available bool, reason string) string {
	if available {
		return ""
	}
	return reason
}

func availabilityReason(first, second bool, firstName, secondName string) string {
	if !first {
		return firstName + " unavailable"
	}
	if !second {
		return secondName + " unavailable"
	}
	return ""
}
