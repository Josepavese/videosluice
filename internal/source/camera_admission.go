package source

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const cameraAdmissionProbeTimeout = 2 * time.Second

// CameraAdmissionDecision binds one physical or logical camera source to the
// broker capture contract. V4L2 sources are discovered from their advertised
// mode catalog; sensor-stack sources are bound to the selector and mode that
// their adapter declares, then every source is attested from frames emitted by
// the running worker. The fingerprint is diagnostic, not an authentication
// primitive.
type CameraAdmissionDecision struct {
	SourceTransport        string
	PreferredDevicePath    string
	DevicePath             string
	DeviceIdentity         string
	CapabilityFingerprint  string
	Format                 string
	Reason                 string
	CodecPolicy            string
	AspectPolicy           string
	CaptureWidth           int
	CaptureHeight          int
	CaptureFPS             int
	RequiresSoftwareDecode bool
	ResizedCapture         bool
	CapabilityCount        int
	ProbedDevices          []string
	RejectedDevices        []string
}

// PrepareDeclaredCameraAdmission creates the discovery half of admission for
// source stacks whose camera negotiation is owned by the backend itself (HBN
// and libcamera). It does not mark the source as admitted: the broker still
// requires the worker capability line, analysis cadence, archive cadence and
// codec bootstrap before admission completes.
func PrepareDeclaredCameraAdmission(plan BrokerMuxPlan) (BrokerMuxPlan, CameraAdmissionDecision, error) {
	transport := strings.ToLower(strings.TrimSpace(plan.Transport))
	format := "nv12"
	var sourceReference string
	switch transport {
	case TransportVendorHBN:
		sensor := strings.TrimSpace(plan.SensorPrefix)
		if sensor == "" {
			return plan, CameraAdmissionDecision{}, errors.New("vendor HBN admission requires an explicit sensor identifier")
		}
		sourceReference = fmt.Sprintf("%s:%s:%d", transport, sensor, plan.CameraHostIndex)
	case TransportLibcamera:
		selector := strings.TrimSpace(plan.CaptureDevicePath)
		if selector == "" {
			selector = "auto"
		}
		sourceReference = transport + ":" + selector
	default:
		return plan, CameraAdmissionDecision{}, fmt.Errorf("declared camera admission is unsupported for transport %q", plan.Transport)
	}
	if plan.CaptureWidth <= 0 || plan.CaptureHeight <= 0 || plan.CaptureFPS <= 0 {
		return plan, CameraAdmissionDecision{}, fmt.Errorf(
			"declared camera admission has invalid capture mode %dx%d@%d",
			plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS,
		)
	}
	decision := CameraAdmissionDecision{
		SourceTransport:       transport,
		PreferredDevicePath:   sourceReference,
		DevicePath:            sourceReference,
		DeviceIdentity:        sourceReference,
		CapabilityFingerprint: DeclaredCameraCapabilityFingerprint(plan, sourceReference, format),
		Format:                format,
		Reason:                "declared_sensor_stack",
		CodecPolicy:           normalizeV4L2CaptureCodecPolicy(plan.CaptureCodecPolicy),
		AspectPolicy:          normalizeV4L2CaptureAspectPolicy(plan.CaptureAspectPolicy),
		CaptureWidth:          plan.CaptureWidth,
		CaptureHeight:         plan.CaptureHeight,
		CaptureFPS:            plan.CaptureFPS,
		CapabilityCount:       1,
		ProbedDevices:         []string{sourceReference},
	}
	plan.CaptureFormat = format
	plan.ArchiveWidth, plan.ArchiveHeight = FitArchiveGeometryWithAlignment(
		plan.ArchiveWidth,
		plan.ArchiveHeight,
		decision.CaptureWidth,
		decision.CaptureHeight,
		plan.ArchiveWidthAlignment,
		plan.ArchiveHeightAlignment,
	)
	return plan, decision, nil
}

// DeclaredCameraCapabilityFingerprint changes whenever the backend selector or
// its declared capture contract changes. It deliberately uses the same
// cap-v1 namespace as a V4L2 catalog fingerprint because both identify the
// capability contract that was subsequently proven at runtime.
func DeclaredCameraCapabilityFingerprint(plan BrokerMuxPlan, sourceReference, format string) string {
	fields := []string{
		strings.ToLower(strings.TrimSpace(plan.Transport)),
		strings.TrimSpace(sourceReference),
		normalizeV4L2Format(format),
		fmt.Sprintf("%dx%d@%d", plan.CaptureWidth, plan.CaptureHeight, plan.CaptureFPS),
		normalizeV4L2CaptureCodecPolicy(plan.CaptureCodecPolicy),
		normalizeV4L2CaptureAspectPolicy(plan.CaptureAspectPolicy),
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(strings.Join(fields, "\x00")))
	return fmt.Sprintf("cap-v1:%016x", hash.Sum64())
}

type V4L2CapabilityQuery func(context.Context, string) ([]V4L2FormatCapability, error)
type V4L2IdentityQuery func(string) (string, error)

// PrepareV4L2UVCCameraAdmission discovers and qualifies a physical UVC node.
// It is intentionally repeated on each broker-child start so hotplug and
// /dev/videoN renumbering cannot retain a stale binding.
func PrepareV4L2UVCCameraAdmission(
	ctx context.Context,
	plan BrokerMuxPlan,
) (BrokerMuxPlan, CameraAdmissionDecision, error) {
	candidates, err := ListV4L2CaptureDevices(plan.CaptureDevicePath)
	if err != nil {
		return plan, CameraAdmissionDecision{}, err
	}
	decision, err := ResolveV4L2UVCCameraAdmission(
		ctx,
		plan,
		candidates,
		QueryV4L2FormatCapabilities,
		QueryV4L2DeviceIdentity,
	)
	if err != nil {
		return plan, decision, err
	}
	plan = applyV4L2UVCCameraAdmissionDecision(plan, decision)
	return plan, decision, nil
}

func applyV4L2UVCCameraAdmissionDecision(
	plan BrokerMuxPlan,
	decision CameraAdmissionDecision,
) BrokerMuxPlan {
	plan.CaptureDevicePath = decision.DevicePath
	plan.CaptureFormat = decision.Format
	plan.CaptureWidth = decision.CaptureWidth
	plan.CaptureHeight = decision.CaptureHeight
	plan.CaptureFPS = decision.CaptureFPS
	// Preserve an explicitly lower archive cadence while capping it to the
	// admitted physical source. Archive cadence is an independent output policy,
	// not an alias for capture cadence.
	if plan.ArchiveFPS <= 0 || plan.ArchiveFPS > decision.CaptureFPS {
		plan.ArchiveFPS = decision.CaptureFPS
	}
	plan.ArchiveWidth, plan.ArchiveHeight = FitArchiveGeometryWithAlignment(
		plan.ArchiveWidth,
		plan.ArchiveHeight,
		decision.CaptureWidth,
		decision.CaptureHeight,
		plan.ArchiveWidthAlignment,
		plan.ArchiveHeightAlignment,
	)
	return plan
}

// FitArchiveGeometry preserves the requested archive aspect ratio while
// preventing a generated archive from being larger than the negotiated
// capture. Upscaling cannot recover sensor detail and consumes decode/resize/
// encode capacity that must remain available to the analysis stream.
func FitArchiveGeometry(requestedWidth, requestedHeight, captureWidth, captureHeight int) (int, int) {
	return FitArchiveGeometryWithAlignment(
		requestedWidth,
		requestedHeight,
		captureWidth,
		captureHeight,
		2,
		2,
	)
}

// FitArchiveGeometryWithAlignment additionally enforces the geometry contract
// exported by the selected encoder operation. NV12 itself requires even
// dimensions, so provider alignment is combined with a two-pixel baseline.
func FitArchiveGeometryWithAlignment(
	requestedWidth,
	requestedHeight,
	captureWidth,
	captureHeight,
	widthAlignment,
	heightAlignment int,
) (int, int) {
	if captureWidth <= 0 || captureHeight <= 0 {
		return requestedWidth, requestedHeight
	}
	if requestedWidth <= 0 || requestedHeight <= 0 {
		requestedWidth, requestedHeight = captureWidth, captureHeight
	}

	widthAlignment = EffectiveNV12Alignment(widthAlignment)
	heightAlignment = EffectiveNV12Alignment(heightAlignment)
	width := requestedWidth
	height := requestedHeight
	if width > captureWidth || height > captureHeight {
		width = captureWidth
		height = requestedHeight * width / requestedWidth
		if height > captureHeight {
			height = captureHeight
			width = requestedWidth * height / requestedHeight
		}
	}

	type geometry struct{ width, height int }
	candidates := make([]geometry, 0, 2)
	alignedWidth := alignDown(width, widthAlignment)
	if alignedWidth > 0 {
		alignedHeight := alignDown(requestedHeight*alignedWidth/requestedWidth, heightAlignment)
		if alignedHeight > 0 && alignedHeight <= height {
			candidates = append(candidates, geometry{alignedWidth, alignedHeight})
		}
	}
	alignedHeight := alignDown(height, heightAlignment)
	if alignedHeight > 0 {
		alignedWidth = alignDown(requestedWidth*alignedHeight/requestedHeight, widthAlignment)
		if alignedWidth > 0 && alignedWidth <= width {
			candidates = append(candidates, geometry{alignedWidth, alignedHeight})
		}
	}
	if len(candidates) == 0 {
		return captureWidth, captureHeight
	}
	best := candidates[0]
	for _, candidate := range candidates[1:] {
		if geometryAspectBetter(candidate.width, candidate.height, best.width, best.height, requestedWidth, requestedHeight) {
			best = candidate
		}
	}
	return best.width, best.height
}

// EffectiveNV12Alignment returns the exact alignment applied by the graph.
// Exporting the normalized value lets the signed runtime admission contract
// bind postflight validation to the same provider constraint used here.
func EffectiveNV12Alignment(providerAlignment int) int {
	if providerAlignment <= 0 {
		return 2
	}
	a, b := providerAlignment, 2
	for b != 0 {
		a, b = b, a%b
	}
	return providerAlignment / a * 2
}

func alignDown(value, alignment int) int {
	if value <= 0 || alignment <= 0 {
		return 0
	}
	return value - value%alignment
}

func geometryAspectBetter(
	width, height, bestWidth, bestHeight, requestedWidth, requestedHeight int,
) bool {
	errorNumerator := absInt64(int64(width)*int64(requestedHeight) - int64(requestedWidth)*int64(height))
	bestErrorNumerator := absInt64(int64(bestWidth)*int64(requestedHeight) - int64(requestedWidth)*int64(bestHeight))
	left := errorNumerator * int64(bestHeight)
	right := bestErrorNumerator * int64(height)
	if left != right {
		return left < right
	}
	return int64(width)*int64(height) > int64(bestWidth)*int64(bestHeight)
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func ListV4L2CaptureDevices(preferred string) ([]string, error) {
	paths, err := filepath.Glob("/dev/video*")
	if err != nil {
		return nil, fmt.Errorf("list V4L2 capture devices: %w", err)
	}
	sort.Slice(paths, func(i, j int) bool {
		return v4l2DeviceSortKey(paths[i]) < v4l2DeviceSortKey(paths[j])
	})
	ordered := make([]string, 0, len(paths)+1)
	seen := map[string]bool{}
	appendPath := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" || seen[path] {
			return
		}
		seen[path] = true
		ordered = append(ordered, path)
	}
	appendPath(preferred)
	for _, path := range paths {
		appendPath(path)
	}
	if len(ordered) == 0 {
		return nil, fmt.Errorf("no V4L2 capture device candidates found")
	}
	return ordered, nil
}

func v4l2DeviceSortKey(path string) string {
	base := filepath.Base(path)
	raw := strings.TrimPrefix(base, "video")
	index, err := strconv.Atoi(raw)
	if err != nil || index < 0 {
		return "999999:" + base
	}
	return fmt.Sprintf("%06d:%s", index, base)
}

func ResolveV4L2UVCCameraAdmission(
	ctx context.Context,
	plan BrokerMuxPlan,
	candidates []string,
	query V4L2CapabilityQuery,
	identityQuery V4L2IdentityQuery,
) (CameraAdmissionDecision, error) {
	preferred := strings.TrimSpace(plan.CaptureDevicePath)
	if preferred == "" && len(candidates) > 0 {
		preferred = strings.TrimSpace(candidates[0])
	}
	codecPolicy := normalizeV4L2CaptureCodecPolicy(plan.CaptureCodecPolicy)
	aspectPolicy := normalizeV4L2CaptureAspectPolicy(plan.CaptureAspectPolicy)
	transport := strings.ToLower(strings.TrimSpace(plan.Transport))
	if transport == "" {
		transport = TransportV4L2UVC
	}
	bestScore := int64(1<<63 - 1)
	best := CameraAdmissionDecision{
		SourceTransport:     transport,
		PreferredDevicePath: preferred,
		CodecPolicy:         codecPolicy,
		AspectPolicy:        aspectPolicy,
	}
	identityBoundPreferred := strings.HasPrefix(preferred, "/dev/nemesi-video-sources/")
	preferredIdentity := ""
	if identityQuery != nil && preferred != "" {
		identity, identityErr := identityQuery(preferred)
		preferredIdentity = strings.TrimSpace(identity)
		if identityErr != nil || preferredIdentity == "" {
			identityScope := "preferred"
			if identityBoundPreferred {
				identityScope = "canonical"
			}
			return best, fmt.Errorf(
				"camera admission cannot attest %s source identity device=%s",
				identityScope,
				preferred,
			)
		}
	}
	seen := map[string]bool{}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || seen[candidate] {
			continue
		}
		seen[candidate] = true
		identity := ""
		var identityErr error
		if identityQuery != nil {
			identity, identityErr = identityQuery(candidate)
			identity = strings.TrimSpace(identity)
		}
		if preferredIdentity != "" {
			if identityErr != nil || identity == "" {
				best.RejectedDevices = append(best.RejectedDevices, candidate+":identity_unavailable")
				continue
			}
			if identity != preferredIdentity {
				best.RejectedDevices = append(best.RejectedDevices, candidate+":identity_mismatch")
				continue
			}
		}
		best.ProbedDevices = append(best.ProbedDevices, candidate)
		probeCtx, cancel := context.WithTimeout(ctx, cameraAdmissionProbeTimeout)
		caps, err := query(probeCtx, candidate)
		cancel()
		if err != nil || len(caps) == 0 {
			reason := "no_capabilities"
			if err != nil {
				reason = "probe_failed"
			}
			best.RejectedDevices = append(best.RejectedDevices, candidate+":"+reason)
			continue
		}
		if identity == "" {
			identity = transport + ":" + candidate
		}
		capabilityFingerprint := V4L2CapabilityFingerprint(identity, caps)
		candidatePlan := plan
		candidatePlan.CaptureDevicePath = candidate
		candidatePlan.CaptureModeExclusions = captureModeExclusionsForFingerprint(
			plan.CaptureModeExclusions,
			capabilityFingerprint,
		)
		capture := ResolveV4L2UVCCaptureDecisionFromCapabilities(candidatePlan, caps)
		if strings.TrimSpace(capture.Format) == "" {
			best.RejectedDevices = append(best.RejectedDevices, candidate+":"+capture.Reason)
			continue
		}
		if reason := rejectV4L2Admission(candidatePlan, capture); reason != "" {
			best.RejectedDevices = append(best.RejectedDevices, candidate+":"+reason)
			continue
		}
		if aspectPolicy == V4L2CaptureAspectPolicyExact &&
			(!sameAspect(capture.CaptureWidth, capture.CaptureHeight, plan.MainWidth, plan.MainHeight) ||
				!sameAspect(capture.CaptureWidth, capture.CaptureHeight, plan.SubWidth, plan.SubHeight)) {
			best.RejectedDevices = append(best.RejectedDevices, candidate+":aspect_mismatch")
			continue
		}
		score := v4l2AdmissionScore(plan, capture, candidate == preferred)
		if score >= bestScore {
			continue
		}
		bestScore = score
		best.DevicePath = candidate
		best.DeviceIdentity = identity
		best.CapabilityFingerprint = capabilityFingerprint
		best.Format = capture.Format
		best.Reason = capture.Reason
		best.CaptureWidth = capture.CaptureWidth
		best.CaptureHeight = capture.CaptureHeight
		best.CaptureFPS = capture.CaptureFPS
		best.RequiresSoftwareDecode = capture.RequiresSoftwareDecode
		best.ResizedCapture = capture.ResizedCapture
		best.CapabilityCount = len(caps)
	}
	if best.DevicePath == "" {
		return best, fmt.Errorf(
			"camera admission failed codec_policy=%s preferred_codec=%s requested=%dx%d@%d rejected=%s",
			codecPolicy,
			normalizeV4L2Format(plan.CaptureCodecPreference),
			plan.CaptureWidth,
			plan.CaptureHeight,
			plan.CaptureFPS,
			strings.Join(best.RejectedDevices, ","),
		)
	}
	return best, nil
}

func captureModeExclusionsForFingerprint(
	exclusions []V4L2CaptureModeExclusion,
	fingerprint string,
) []V4L2CaptureModeExclusion {
	matched := make([]V4L2CaptureModeExclusion, 0, len(exclusions))
	for _, exclusion := range exclusions {
		bound := strings.TrimSpace(exclusion.CapabilityFingerprint)
		if bound != "" && bound != fingerprint {
			continue
		}
		matched = append(matched, exclusion)
	}
	return matched
}

func rejectV4L2Admission(plan BrokerMuxPlan, decision V4L2InputFormatDecision) string {
	format := normalizeV4L2Format(decision.Format)
	preference := normalizeV4L2Format(plan.CaptureCodecPreference)
	if preference == "" {
		preference = "h265"
	}
	if normalizeV4L2CaptureCodecPolicy(plan.CaptureCodecPolicy) == V4L2CaptureCodecPolicyRequired && format != preference {
		return "required_codec_not_selected"
	}
	if decision.RequiresSoftwareDecode && !plan.AllowSWFallback {
		return "software_decode_forbidden"
	}
	if plan.RequireHWCodec {
		switch format {
		case "h264":
			if !plan.H264DecodeHWAvailable {
				return "h264_hardware_decoder_unavailable"
			}
		case "h265":
			if !plan.H265DecodeHWAvailable {
				return "h265_hardware_decoder_unavailable"
			}
		case "mjpeg":
			if !plan.MJPEGDecodeHWAvailable {
				return "mjpeg_hardware_decoder_unavailable"
			}
		}
		if !plan.H264EncodeHWAvailable {
			return "h264_hardware_encoder_unavailable"
		}
		if gstreamerArchiveEncodeRequired(plan, format) {
			archiveCodec := normalizedArchiveCodec(plan.ArchiveCodec)
			if !declaredHardwareEncoderAvailable(plan, archiveCodec) {
				return archiveCodec + "_hardware_encoder_unavailable"
			}
		}
	}
	return ""
}

func v4l2AdmissionScore(plan BrokerMuxPlan, decision V4L2InputFormatDecision, preferredDevice bool) int64 {
	format := normalizeV4L2Format(decision.Format)
	preferredCodec := normalizeV4L2Format(plan.CaptureCodecPreference)
	if preferredCodec == "" {
		preferredCodec = "h265"
	}
	score := int64(0)
	if format != preferredCodec {
		score += 1_000_000_000
	}
	if format != "h264" && format != "h265" {
		score += 1_000_000_000
	}
	if decision.RequiresSoftwareDecode {
		score += 500_000_000
	}
	if decision.ResizedCapture {
		score += 10_000_000
	}
	requestedArea := int64(maxInt(plan.CaptureWidth, 1) * maxInt(plan.CaptureHeight, 1))
	selectedArea := int64(maxInt(decision.CaptureWidth, 1) * maxInt(decision.CaptureHeight, 1))
	if selectedArea > requestedArea {
		score += selectedArea - requestedArea
	}
	if !preferredDevice {
		score++
	}
	return score
}

func sameAspect(leftWidth, leftHeight, rightWidth, rightHeight int) bool {
	if leftWidth <= 0 || leftHeight <= 0 || rightWidth <= 0 || rightHeight <= 0 {
		return false
	}
	return int64(leftWidth)*int64(rightHeight) == int64(rightWidth)*int64(leftHeight)
}

func V4L2CapabilityFingerprint(identity string, caps []V4L2FormatCapability) string {
	items := make([]string, 0, len(caps))
	for _, cap := range caps {
		items = append(items, fmt.Sprintf(
			"%s:%dx%d@%.3f",
			normalizeV4L2Format(cap.PixelFormat),
			cap.Width,
			cap.Height,
			cap.FPS,
		))
	}
	sort.Strings(items)
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(strings.TrimSpace(identity)))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(strings.Join(items, "|")))
	return fmt.Sprintf("cap-v1:%016x", hash.Sum64())
}

// QueryV4L2DeviceIdentity returns a compact sysfs identity. Missing optional
// USB fields are tolerated because capability admission remains authoritative.
func QueryV4L2DeviceIdentity(device string) (string, error) {
	base := filepath.Base(strings.TrimSpace(device))
	classPath := ""
	if strings.HasPrefix(base, "video") {
		classPath = filepath.Join("/sys/class/video4linux", base)
	}
	if classPath == "" {
		info, err := os.Stat(device)
		if err != nil {
			return "", err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || info.Mode()&os.ModeCharDevice == 0 {
			return "", fmt.Errorf("V4L2 source is not a character device %q", device)
		}
		major, minor := linuxDeviceMajorMinor(uint64(stat.Rdev))
		classPath = fmt.Sprintf("/sys/dev/char/%d:%d", major, minor)
	}
	sysDevice, err := filepath.EvalSymlinks(filepath.Join(classPath, "device"))
	if err != nil {
		return "", err
	}
	values := make([]string, 0, 7)
	if name, err := os.ReadFile(filepath.Join(classPath, "name")); err == nil {
		values = append(values, "name="+strings.TrimSpace(string(name)))
	}
	current := sysDevice
	for depth := 0; depth < 8 && current != "/" && current != "."; depth++ {
		foundUSB := false
		for _, field := range []string{"idVendor", "idProduct", "bcdDevice", "serial", "product"} {
			body, readErr := os.ReadFile(filepath.Join(current, field))
			if readErr != nil {
				continue
			}
			value := strings.TrimSpace(string(body))
			if value != "" {
				values = append(values, field+"="+value)
				foundUSB = true
			}
		}
		if foundUSB {
			values = append(values, "port="+filepath.Base(current))
			break
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	if len(values) == 0 {
		return "", fmt.Errorf("no sysfs identity for %s", device)
	}
	return strings.Join(values, ";"), nil
}

func linuxDeviceMajorMinor(device uint64) (uint64, uint64) {
	major := (device >> 8) & 0xfff
	major |= (device >> 32) & 0xfffff000
	minor := device & 0xff
	minor |= (device >> 12) & 0xffffff00
	return major, minor
}
