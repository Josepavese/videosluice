// Package capability defines the standalone hardware discovery and graph
// planning contract of video-broker. It intentionally knows nothing about an
// embedding application, machine inventory, installer profile or host name.
package capability

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const Schema = "video-broker.capabilities.v2"

const (
	StageCapture = "capture"
	StageDecode  = "decode"
	StageEncode  = "encode"
	StageToneMap = "tone_map"
	StageResize  = "resize"
	StageConvert = "convert"
)

const (
	StateCandidate = "candidate"
	StateAttested  = "attested"
	StateRejected  = "rejected"
)

const (
	ConcurrencyExclusive = "exclusive"
	ConcurrencyFinite    = "finite"
	ConcurrencyShareable = "shareable"
)

// Operation is one media operation exported by a component. An empty format
// means that the operation is format-independent.
type Operation struct {
	Stage           string `json:"stage"`
	Format          string `json:"format,omitempty"`
	WidthAlignment  int    `json:"width_alignment,omitempty"`
	HeightAlignment int    `json:"height_alignment,omitempty"`
}

// Evidence is a bounded, non-secret observation used to admit a component as
// a candidate. Runtime media telemetry is deliberately recorded separately.
type Evidence struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

// Component describes a physical engine, SIMD implementation, system media
// API or product-owned worker/plugin. Presence proves only candidacy: it does
// not prove that a requested stream can actually be processed.
type Component struct {
	ID                      string      `json:"id"`
	Backend                 string      `json:"backend"`
	Hardware                bool        `json:"hardware"`
	Available               bool        `json:"available"`
	State                   string      `json:"state"`
	Reason                  string      `json:"reason,omitempty"`
	ConcurrencyMode         string      `json:"concurrency_mode"`
	ConcurrencyGroup        string      `json:"concurrency_group"`
	MaxConcurrentOperations int         `json:"max_concurrent_operations"`
	Operations              []Operation `json:"operations,omitempty"`
	Evidence                []Evidence  `json:"evidence,omitempty"`
}

// Inventory is the deterministic result of one local component discovery.
// Board identity may be inferred for diagnostics by an embedding application,
// but it is never an input to this graph.
type Inventory struct {
	Schema       string      `json:"schema"`
	DiscoveredAt time.Time   `json:"discovered_at"`
	Components   []Component `json:"components"`
}

type ProviderPolicy struct {
	EncodeProvider        string
	DecodeProvider        string
	ToneMapAccelerator    string
	RequireHardwareCodec  bool
	AllowSoftwareFallback bool
}

type Requirements struct {
	Transport     string
	CaptureFormat string
	LiveCodec     string
	LiveEncode    bool
	ArchiveCodec  string
	ArchiveEncode bool
	ToneMap       bool
	ResizeRoles   []string
}

type StageSelection struct {
	Stage           string `json:"stage"`
	Role            string `json:"role,omitempty"`
	Format          string `json:"format,omitempty"`
	Component       string `json:"component"`
	Backend         string `json:"backend"`
	Hardware        bool   `json:"hardware"`
	State           string `json:"state"`
	Fallback        string `json:"fallback,omitempty"`
	WidthAlignment  int    `json:"width_alignment,omitempty"`
	HeightAlignment int    `json:"height_alignment,omitempty"`
}

type Plan struct {
	WorkerProvider        string           `json:"worker_provider"`
	EncodeProvider        string           `json:"encode_provider"`
	ArchiveEncodeProvider string           `json:"archive_encode_provider,omitempty"`
	DecodeProvider        string           `json:"decode_provider"`
	ToneMapBackend        string           `json:"tone_map_backend,omitempty"`
	ResizeBackend         string           `json:"resize_backend,omitempty"`
	Stages                []StageSelection `json:"stages"`
}

func NormalizeProvider(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return "auto"
	case "native_sp", "gstreamer_mpp", "gstreamer_v4l2", "gstreamer_vendor", "software":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return ""
	}
}

func NormalizeFormat(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "hevc":
		return "h265"
	case "mjpg", "jpeg":
		return "mjpeg"
	case "yuy2":
		return "yuyv"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func (inventory Inventory) Supports(backend, stage, format string, hardwareOnly bool) bool {
	_, ok := inventory.Find(backend, stage, format, hardwareOnly)
	return ok
}

func (inventory Inventory) Find(backend, stage, format string, hardwareOnly bool) (Component, bool) {
	component, _, ok := inventory.FindOperation(backend, stage, format, hardwareOnly)
	return component, ok
}

// FindOperation resolves both the component and the exact operation contract.
// Geometry constraints belong to a codec operation rather than to a host,
// board or camera model, so downstream graph adapters can remain agnostic.
func (inventory Inventory) FindOperation(backend, stage, format string, hardwareOnly bool) (Component, Operation, bool) {
	backend = strings.ToLower(strings.TrimSpace(backend))
	stage = strings.ToLower(strings.TrimSpace(stage))
	format = NormalizeFormat(format)
	for _, component := range inventory.Components {
		if !component.Available || strings.ToLower(component.Backend) != backend || hardwareOnly && !component.Hardware {
			continue
		}
		for _, operation := range component.Operations {
			if strings.ToLower(operation.Stage) != stage {
				continue
			}
			operationFormat := NormalizeFormat(operation.Format)
			if operationFormat == "" || format == "" || operationFormat == format {
				return component, operation, true
			}
		}
	}
	return Component{}, Operation{}, false
}

func (inventory Inventory) HardwareAvailable(stage, format string) bool {
	for _, backend := range []string{"native_sp", "gstreamer_vendor", "gstreamer_mpp", "gstreamer_v4l2"} {
		if inventory.Supports(backend, stage, format, true) {
			return true
		}
	}
	return false
}

func Resolve(inventory Inventory, policy ProviderPolicy, requirements Requirements) (Plan, error) {
	policy.EncodeProvider = NormalizeProvider(policy.EncodeProvider)
	policy.DecodeProvider = NormalizeProvider(policy.DecodeProvider)
	if policy.EncodeProvider == "" {
		return Plan{}, errors.New("unsupported encode provider policy")
	}
	if policy.DecodeProvider == "" {
		return Plan{}, errors.New("unsupported decode provider policy")
	}
	requirements.Transport = strings.ToLower(strings.TrimSpace(requirements.Transport))
	requirements.CaptureFormat = NormalizeFormat(requirements.CaptureFormat)
	requirements.LiveCodec = NormalizeFormat(requirements.LiveCodec)
	requirements.ArchiveCodec = NormalizeFormat(requirements.ArchiveCodec)
	if err := validateResizeRoles(requirements.ResizeRoles); err != nil {
		return Plan{}, err
	}
	workers, err := chooseWorkers(inventory, policy, requirements.Transport)
	if err != nil {
		return Plan{}, err
	}
	failures := make([]string, 0, len(workers))
	for _, worker := range workers {
		plan, resolveErr := resolveWithWorker(inventory, policy, requirements, worker.component, worker.provider)
		if resolveErr == nil {
			return plan, nil
		}
		failures = append(failures, worker.provider+": "+resolveErr.Error())
	}
	return Plan{}, fmt.Errorf("no compatible product worker plan: %s", strings.Join(failures, "; "))
}

func resolveWithWorker(
	inventory Inventory,
	policy ProviderPolicy,
	requirements Requirements,
	worker Component,
	workerProvider string,
) (Plan, error) {
	codecInventory := inventoryForWorker(inventory, workerProvider)
	var err error
	if workerProvider == "native_sp" {
		if policy.EncodeProvider == "auto" {
			policy.EncodeProvider = "native_sp"
		}
		if policy.DecodeProvider == "auto" {
			policy.DecodeProvider = "native_sp"
		}
	} else {
		if policy.EncodeProvider == "native_sp" || policy.DecodeProvider == "native_sp" {
			return Plan{}, errors.New("portable worker cannot host native_sp codec stages")
		}
	}

	liveEncodeProvider := ""
	liveEncodeFallback := ""
	archiveEncodeProvider := ""
	archiveEncodeFallback := ""
	if requirements.LiveEncode {
		liveEncodeProvider, liveEncodeFallback, err = chooseProvider(
			codecInventory, policy.EncodeProvider, StageEncode, []string{requirements.LiveCodec},
			policy.RequireHardwareCodec, policy.AllowSoftwareFallback, requirements.Transport,
		)
		if err != nil {
			return Plan{}, fmt.Errorf("live encode plan: %w", err)
		}
	}
	if requirements.ArchiveEncode {
		archiveEncodeProvider, archiveEncodeFallback, err = chooseProvider(
			codecInventory, policy.EncodeProvider, StageEncode, []string{requirements.ArchiveCodec},
			policy.RequireHardwareCodec, policy.AllowSoftwareFallback, requirements.Transport,
		)
		if err != nil {
			return Plan{}, fmt.Errorf("archive encode plan: %w", err)
		}
	}

	decodeFormats := []string{}
	if compressedCapture(requirements.CaptureFormat) {
		decodeFormats = []string{requirements.CaptureFormat}
	}
	decodeProvider := ""
	decodeFallback := ""
	if len(decodeFormats) > 0 {
		decodeProvider, decodeFallback, err = chooseProvider(
			codecInventory,
			policy.DecodeProvider,
			StageDecode,
			decodeFormats,
			policy.RequireHardwareCodec,
			policy.AllowSoftwareFallback,
			requirements.Transport,
		)
		if err != nil {
			return Plan{}, fmt.Errorf("decode plan: %w", err)
		}
	}
	encodeProvider := liveEncodeProvider
	if encodeProvider == "" {
		encodeProvider = archiveEncodeProvider
	}
	if encodeProvider == "" {
		encodeProvider = decodeProvider
	}
	if decodeProvider == "" {
		decodeProvider = encodeProvider
	}
	if archiveEncodeProvider == "" {
		archiveEncodeProvider = encodeProvider
	}
	if encodeProvider == "" {
		if workerProvider == "native_sp" {
			encodeProvider = "native_sp"
		} else {
			encodeProvider = firstPortableProvider(policy.EncodeProvider, policy.DecodeProvider)
		}
		decodeProvider = encodeProvider
		archiveEncodeProvider = encodeProvider
	}
	if requirements.Transport == "vendor_hbn" && (encodeProvider != "native_sp" || archiveEncodeProvider != "native_sp" || decodeProvider != "native_sp") {
		return Plan{}, errors.New("vendor_hbn requires the product native_sp worker")
	}
	nativeBoundary := encodeProvider == "native_sp" || archiveEncodeProvider == "native_sp" || decodeProvider == "native_sp"
	if nativeBoundary && (encodeProvider != "native_sp" || archiveEncodeProvider != "native_sp" || decodeProvider != "native_sp") {
		return Plan{}, errors.New("native_sp codec stages must belong to one native worker graph")
	}

	plan := Plan{WorkerProvider: workerProvider, EncodeProvider: encodeProvider, ArchiveEncodeProvider: archiveEncodeProvider, DecodeProvider: decodeProvider}
	plan.Stages = append(plan.Stages, StageSelection{
		Stage: StageCapture, Role: "source", Format: requirements.Transport,
		Component: worker.ID, Backend: workerProvider, Hardware: worker.Hardware, State: StateCandidate,
	})
	if requirements.LiveEncode {
		appendCodecStage(&plan, codecInventory, StageEncode, "live", requirements.LiveCodec, liveEncodeProvider, liveEncodeFallback)
	}
	if requirements.ArchiveEncode {
		appendCodecStage(&plan, codecInventory, StageEncode, "archive", requirements.ArchiveCodec, archiveEncodeProvider, archiveEncodeFallback)
	}
	for _, format := range decodeFormats {
		component, operation, _ := codecInventory.FindOperation(decodeProvider, StageDecode, format, false)
		plan.Stages = append(plan.Stages, StageSelection{
			Stage: StageDecode, Role: "capture", Format: format, Component: component.ID,
			Backend: decodeProvider, Hardware: component.Hardware,
			State: StateCandidate, Fallback: decodeFallback,
			WidthAlignment: operation.WidthAlignment, HeightAlignment: operation.HeightAlignment,
		})
	}

	if requirements.ToneMap {
		component, fallback, err := chooseToneMapper(inventory, workerProvider, policy.ToneMapAccelerator)
		if err != nil {
			return Plan{}, err
		}
		plan.ToneMapBackend = component.Backend
		plan.Stages = append(plan.Stages, StageSelection{
			Stage: StageToneMap, Format: "nv12", Component: component.ID,
			Backend: component.Backend, Hardware: component.Hardware,
			State: StateCandidate, Fallback: fallback,
		})
	}
	for _, role := range requirements.ResizeRoles {
		component, ok := chooseResize(inventory, workerProvider, requirements.Transport)
		if !ok {
			return Plan{}, fmt.Errorf("no compatible NV12 resize component is available for role %q", role)
		}
		plan.ResizeBackend = component.Backend
		plan.Stages = append(plan.Stages, StageSelection{
			Stage: StageResize, Role: role, Format: "nv12", Component: component.ID,
			Backend: component.Backend, Hardware: component.Hardware, State: StateCandidate,
		})
	}
	sort.SliceStable(plan.Stages, func(left, right int) bool {
		if plan.Stages[left].Stage == plan.Stages[right].Stage {
			if plan.Stages[left].Role == plan.Stages[right].Role {
				return plan.Stages[left].Format < plan.Stages[right].Format
			}
			return plan.Stages[left].Role < plan.Stages[right].Role
		}
		return plan.Stages[left].Stage < plan.Stages[right].Stage
	})
	return plan, nil
}

func validateResizeRoles(roles []string) error {
	previous := ""
	for _, role := range roles {
		if role != "analysis" && role != "archive" && role != "live" {
			return fmt.Errorf("unsupported resize role %q", role)
		}
		if role <= previous {
			return errors.New("resize roles must be unique and canonically sorted")
		}
		previous = role
	}
	return nil
}

type workerSelection struct {
	component Component
	provider  string
}

func chooseWorkers(inventory Inventory, policy ProviderPolicy, transport string) ([]workerSelection, error) {
	transport = strings.ToLower(strings.TrimSpace(transport))
	candidates := []string{"native_sp", "gstreamer"}
	switch transport {
	case "vendor_hbn":
		candidates = []string{"native_sp"}
	case "libcamera":
		candidates = []string{"gstreamer"}
	}
	providers := []string{policy.EncodeProvider, policy.DecodeProvider}
	for _, provider := range providers {
		if provider == "auto" {
			continue
		}
		if provider == "native_sp" {
			candidates = []string{"native_sp"}
		} else {
			candidates = []string{"gstreamer"}
		}
		break
	}
	workers := make([]workerSelection, 0, len(candidates))
	for _, backend := range candidates {
		if component, ok := inventory.Find(backend, StageCapture, transport, false); ok {
			workers = append(workers, workerSelection{component: component, provider: backend})
		}
	}
	if len(workers) == 0 {
		return nil, fmt.Errorf("no product worker supports %s capture", transport)
	}
	return workers, nil
}

func inventoryForWorker(inventory Inventory, workerProvider string) Inventory {
	filtered := inventory
	filtered.Components = append([]Component(nil), inventory.Components...)
	for index := range filtered.Components {
		component := &filtered.Components[index]
		codecBackend := NormalizeProvider(component.Backend)
		if codecBackend == "" {
			continue
		}
		if workerProvider == "native_sp" && codecBackend != "native_sp" ||
			workerProvider == "gstreamer" && codecBackend == "native_sp" {
			component.Available = false
			component.State = StateRejected
			component.Reason = "outside_" + workerProvider + "_worker_boundary"
		}
	}
	return filtered
}

func firstPortableProvider(values ...string) string {
	for _, value := range values {
		value = NormalizeProvider(value)
		if value != "" && value != "auto" && value != "native_sp" {
			return value
		}
	}
	return "software"
}

func appendCodecStage(plan *Plan, inventory Inventory, stage, role, format, backend, fallback string) {
	format = NormalizeFormat(format)
	for index := range plan.Stages {
		selection := &plan.Stages[index]
		if selection.Stage == stage && selection.Role == role && NormalizeFormat(selection.Format) == format && selection.Backend == backend {
			if selection.Fallback == "" {
				selection.Fallback = fallback
			}
			return
		}
	}
	component, operation, _ := inventory.FindOperation(backend, stage, format, false)
	plan.Stages = append(plan.Stages, StageSelection{
		Stage: stage, Role: role, Format: format, Component: component.ID,
		Backend: backend, Hardware: component.Hardware,
		State: StateCandidate, Fallback: fallback,
		WidthAlignment: operation.WidthAlignment, HeightAlignment: operation.HeightAlignment,
	})
}

func chooseProvider(
	inventory Inventory,
	requested string,
	stage string,
	formats []string,
	requireHardware bool,
	allowSoftware bool,
	transport string,
) (string, string, error) {
	candidates := []string{requested}
	if requested == "auto" {
		candidates = providerOrder(transport)
	}
	for _, backend := range candidates {
		if backend == "software" && (!allowSoftware || requireHardware) {
			continue
		}
		if providerSupportsAll(inventory, backend, stage, formats, requireHardware) {
			return backend, "", nil
		}
	}
	if requested != "software" && allowSoftware && !requireHardware && providerSupportsAll(inventory, "software", stage, formats, false) {
		return "software", requested + "_unavailable", nil
	}
	return "", "", fmt.Errorf("no permitted provider supports %s %s", stage, strings.Join(formats, ","))
}

func providerOrder(transport string) []string {
	if strings.EqualFold(strings.TrimSpace(transport), "vendor_hbn") {
		return []string{"native_sp"}
	}
	if strings.EqualFold(strings.TrimSpace(transport), "libcamera") {
		return []string{"gstreamer_vendor", "gstreamer_mpp", "gstreamer_v4l2", "software"}
	}
	return []string{"native_sp", "gstreamer_vendor", "gstreamer_mpp", "gstreamer_v4l2", "software"}
}

func providerSupportsAll(inventory Inventory, backend, stage string, formats []string, hardwareOnly bool) bool {
	if len(formats) == 0 {
		return true
	}
	for _, format := range formats {
		if !inventory.Supports(backend, stage, format, hardwareOnly) {
			return false
		}
	}
	return true
}

func chooseToneMapper(inventory Inventory, codecProvider, requested string) (Component, string, error) {
	requested = strings.ToLower(strings.TrimSpace(requested))
	if requested == "" {
		requested = "auto"
	}
	backendPrefix := "gstreamer"
	if codecProvider == "native_sp" {
		backendPrefix = "native"
	}
	if requested == "auto" || requested == "opencl" {
		if component, ok := inventory.Find(backendPrefix+"_opencl", StageToneMap, "nv12", false); ok {
			return component, "", nil
		}
		if requested == "opencl" {
			return Component{}, "", errors.New("explicit OpenCL tone mapper is unavailable")
		}
	}
	if requested != "auto" && requested != "cpu" {
		return Component{}, "", fmt.Errorf("unsupported tone-map accelerator %q", requested)
	}
	component, ok := inventory.Find(backendPrefix+"_cpu", StageToneMap, "nv12", false)
	if !ok {
		return Component{}, "", errors.New("no compatible tone mapper is available")
	}
	fallback := ""
	if requested == "auto" {
		fallback = "opencl_unavailable"
	}
	return component, fallback, nil
}

func chooseResize(inventory Inventory, codecProvider, transport string) (Component, bool) {
	transport = strings.ToLower(strings.TrimSpace(transport))
	backends := []string{}
	// The HBN worker receives already-scaled surfaces from the sensor/VSE graph.
	// Merely installing that worker does not make the component applicable to a
	// V4L2 graph, which owns a separate CPU/NEON transform boundary.
	if codecProvider == "native_sp" && transport == "vendor_hbn" {
		backends = append(backends, "native_sp_resize")
	}
	if codecProvider == "native_sp" {
		backends = append(backends, "native_neon", "native_cpu")
	}
	backends = append(backends, codecProvider+"_resize", "gstreamer_mpp_resize", "gstreamer_video", "cpu")
	for _, backend := range backends {
		if component, ok := inventory.Find(backend, StageResize, "nv12", false); ok {
			return component, true
		}
	}
	return Component{}, false
}

func compressedCapture(format string) bool {
	switch NormalizeFormat(format) {
	case "h264", "h265", "mjpeg":
		return true
	default:
		return false
	}
}

func uniqueNonEmpty(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = NormalizeFormat(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
