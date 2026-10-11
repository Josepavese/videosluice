package broker

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Josepavese/videosluice/capability"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
)

const (
	cameraAdmissionSchema              = "video-broker.camera-admission.v2"
	cameraModeHealthSchema             = "video-broker.camera-mode-health.v10"
	cameraModeRuntimeFingerprintPrefix = "video-graph-v1:"
	cameraModeHealthReadOnlyEnv        = "VIDEO_BROKER_CAMERA_MODE_HEALTH_READ_ONLY"
	cameraModeFailureThreshold         = 2
	cameraModeHealthMaximumExclusions  = 16
	cameraModeHealthEvidenceTTL        = 24 * time.Hour
	cameraModeQuarantineBase           = 30 * time.Second
	cameraModeQuarantineMaximum        = 15 * time.Minute
	cameraModeProbationHealthyWindow   = 5 * time.Minute
	cameraStateDirectoryName           = "camera"
	cameraModeHealthStateFilename      = "mode-health.json"
)

type cameraModeHealthState struct {
	Schema             string                  `json:"schema"`
	RuntimeFingerprint string                  `json:"runtime_fingerprint"`
	UpdatedAt          time.Time               `json:"updated_at"`
	Entries            []cameraModeHealthEntry `json:"entries"`
}

// cameraModeHealthEntry is a persistent circuit-breaker record. RetryAfter
// makes a runtime rejection a bounded quarantine rather than a permanent
// exclusion; StableAfter keeps a recovered mode on probation until sustained
// healthy cadence has disproved an intermittent failure. Failures preserves
// exponential backoff across broker restarts.
type cameraModeHealthEntry struct {
	Mode        videoproviders.V4L2CaptureModeExclusion `json:"mode"`
	Failures    int                                     `json:"failures"`
	RetryAfter  time.Time                               `json:"retry_after,omitempty"`
	StableAfter time.Time                               `json:"stable_after"`
}

type cameraAdmissionRuntimeTracker struct {
	FirstLiveFrameAt    time.Time
	LastLiveFrameAt     time.Time
	LiveFrames          uint64
	LiveSeenVPS         bool
	LiveSeenSPS         bool
	LiveSeenPPS         bool
	LiveSeenKeyframe    bool
	FirstSubFrameAt     time.Time
	LastSubFrameAt      time.Time
	SubFrames           uint64
	FirstArchiveFrameAt time.Time
	LastArchiveFrameAt  time.Time
	ArchiveFrames       uint64
	SeenVPS             bool
	SeenSPS             bool
	SeenPPS             bool
	SeenKeyframe        bool
	ArchiveCodec        string
}

type cameraAdmissionState struct {
	Schema    string                        `json:"schema"`
	UpdatedAt time.Time                     `json:"updated_at"`
	Status    string                        `json:"status"`
	Reason    string                        `json:"reason"`
	Discovery cameraAdmissionDiscoveryState `json:"discovery"`
	Contract  cameraAdmissionContractState  `json:"contract"`
	Runtime   cameraAdmissionRuntimeState   `json:"runtime"`
}

type cameraAdmissionDiscoveryState struct {
	SourceTransport       string   `json:"source_transport"`
	PreferredDevice       string   `json:"preferred_device,omitempty"`
	SelectedDevice        string   `json:"selected_device,omitempty"`
	DeviceIdentity        string   `json:"device_identity,omitempty"`
	CapabilityFingerprint string   `json:"capability_fingerprint,omitempty"`
	SelectionReason       string   `json:"selection_reason,omitempty"`
	CapabilityCount       int      `json:"capability_count"`
	ProbedDevices         []string `json:"probed_devices,omitempty"`
	RejectedDevices       []string `json:"rejected_devices,omitempty"`
	RuntimeRejectedModes  []string `json:"runtime_rejected_modes,omitempty"`
}

type cameraAdmissionContractState struct {
	CodecPreference        string `json:"codec_preference"`
	CodecPolicy            string `json:"codec_policy"`
	AspectPolicy           string `json:"aspect_policy"`
	CaptureCodec           string `json:"capture_codec,omitempty"`
	CaptureWidth           int    `json:"capture_width"`
	CaptureHeight          int    `json:"capture_height"`
	CaptureFPS             int    `json:"capture_fps"`
	ArchiveWidth           int    `json:"archive_width"`
	ArchiveHeight          int    `json:"archive_height"`
	ArchiveWidthAlignment  int    `json:"archive_width_alignment"`
	ArchiveHeightAlignment int    `json:"archive_height_alignment"`
	HardwareOnly           bool   `json:"hardware_only"`
}

type cameraAdmissionRuntimeState struct {
	CaptureCodec       string  `json:"capture_codec,omitempty"`
	CaptureWidth       int     `json:"capture_width,omitempty"`
	CaptureHeight      int     `json:"capture_height,omitempty"`
	CaptureFPS         int     `json:"capture_fps,omitempty"`
	ArchiveCodec       string  `json:"archive_codec,omitempty"`
	ArchiveDelivery    string  `json:"archive_delivery,omitempty"`
	SubFrames          uint64  `json:"sub_frames"`
	ObservedSubFPS     float64 `json:"observed_sub_fps,omitempty"`
	ArchiveFrames      uint64  `json:"archive_frames"`
	ObservedArchiveFPS float64 `json:"observed_archive_fps,omitempty"`
	SeenVPS            bool    `json:"seen_vps"`
	SeenSPS            bool    `json:"seen_sps"`
	SeenPPS            bool    `json:"seen_pps"`
	SeenKeyframe       bool    `json:"seen_keyframe"`
}

func (rt *videoBrokerRuntime) observeCameraAdmissionFrameLocked(streamID byte, payload []byte, now time.Time) {
	if rt.cameraAdmission.DevicePath == "" || rt.cameraAdmitted {
		return
	}
	tracker := &rt.cameraAdmissionTracker
	watchdog := time.Duration(rt.plan.FrameWatchdogSec) * time.Second
	if watchdog <= 0 {
		watchdog = brokerDefaultFrameWatchdog
	}
	// Evidence from before a stall cannot complete admission of the current
	// stream. Keep each role's bootstrap and frame window contiguous.
	if streamID == brokerStreamH264 && !tracker.LastLiveFrameAt.IsZero() && now.Sub(tracker.LastLiveFrameAt) >= watchdog {
		tracker.FirstLiveFrameAt, tracker.LiveFrames = time.Time{}, 0
		tracker.LiveSeenVPS, tracker.LiveSeenSPS, tracker.LiveSeenPPS, tracker.LiveSeenKeyframe = false, false, false, false
	}
	if streamID == brokerStreamNV12 && !tracker.LastSubFrameAt.IsZero() && now.Sub(tracker.LastSubFrameAt) >= watchdog {
		tracker.FirstSubFrameAt, tracker.SubFrames = time.Time{}, 0
	}
	if (streamID == brokerStreamH265 || streamID == brokerMuxStreamArchiveH264Native || streamID == brokerMuxStreamArchiveH264Generated) &&
		!tracker.LastArchiveFrameAt.IsZero() && now.Sub(tracker.LastArchiveFrameAt) >= watchdog {
		tracker.FirstArchiveFrameAt, tracker.ArchiveFrames = time.Time{}, 0
		tracker.SeenVPS, tracker.SeenSPS, tracker.SeenPPS, tracker.SeenKeyframe = false, false, false, false
	}
	switch streamID {
	case brokerStreamH264:
		if tracker.FirstLiveFrameAt.IsZero() {
			tracker.FirstLiveFrameAt = now
		}
		tracker.LastLiveFrameAt = now
		tracker.LiveFrames++
		observeH26xNALs(
			payload,
			brokerLiveCodec(rt.plan.MainFormat),
			&tracker.LiveSeenVPS,
			&tracker.LiveSeenSPS,
			&tracker.LiveSeenPPS,
			&tracker.LiveSeenKeyframe,
		)
	case brokerStreamNV12:
		if tracker.FirstSubFrameAt.IsZero() {
			tracker.FirstSubFrameAt = now
		}
		tracker.LastSubFrameAt = now
		tracker.SubFrames++
	case brokerStreamH265:
		tracker.ArchiveCodec = "h265"
		observeCameraArchiveFrame(tracker, now)
		observeCameraH26xNALs(tracker, "h265", payload)
	case brokerMuxStreamArchiveH264Native, brokerMuxStreamArchiveH264Generated:
		tracker.ArchiveCodec = "h264"
		observeCameraArchiveFrame(tracker, now)
		observeCameraH26xNALs(tracker, "h264", payload)
	}
	rt.maybeCompleteCameraAdmissionLocked()
}

func observeCameraArchiveFrame(tracker *cameraAdmissionRuntimeTracker, now time.Time) {
	if tracker.FirstArchiveFrameAt.IsZero() {
		tracker.FirstArchiveFrameAt = now
	}
	tracker.LastArchiveFrameAt = now
	tracker.ArchiveFrames++
}

func observeCameraH26xNALs(tracker *cameraAdmissionRuntimeTracker, codec string, payload []byte) {
	observeH26xNALs(payload, codec, &tracker.SeenVPS, &tracker.SeenSPS, &tracker.SeenPPS, &tracker.SeenKeyframe)
}

func observeH26xNALs(payload []byte, codec string, seenVPS, seenSPS, seenPPS, seenKeyframe *bool) {
	for i := 0; i+4 < len(payload); i++ {
		startCodeLen := 0
		if payload[i] == 0 && payload[i+1] == 0 && payload[i+2] == 1 {
			startCodeLen = 3
		} else if i+5 < len(payload) && payload[i] == 0 && payload[i+1] == 0 && payload[i+2] == 0 && payload[i+3] == 1 {
			startCodeLen = 4
		}
		if startCodeLen == 0 || i+startCodeLen >= len(payload) {
			continue
		}
		if codec == "h265" {
			nalType := (payload[i+startCodeLen] >> 1) & 0x3f
			switch nalType {
			case 32:
				if seenVPS != nil {
					*seenVPS = true
				}
			case 33:
				*seenSPS = true
			case 34:
				*seenPPS = true
			}
			if nalType >= 16 && nalType <= 23 {
				*seenKeyframe = true
			}
			continue
		}
		nalType := payload[i+startCodeLen] & 0x1f
		if nalType == 7 {
			*seenSPS = true
		}
		if nalType == 8 {
			*seenPPS = true
		}
		if nalType == 5 {
			*seenKeyframe = true
		}
	}
}

func (rt *videoBrokerRuntime) maybeCompleteCameraAdmissionLocked() {
	if rt.cameraAdmission.DevicePath == "" || rt.cameraAdmitted {
		return
	}
	selected := rt.cameraAdmission
	if rt.observedCaptureCodec == "" || !strings.EqualFold(rt.observedCaptureCodec, selected.Format) ||
		rt.observedCaptureWidth != selected.CaptureWidth || rt.observedCaptureHeight != selected.CaptureHeight ||
		rt.observedCaptureFPS+1 < selected.CaptureFPS {
		return
	}
	tracker := &rt.cameraAdmissionTracker
	now := time.Now()
	freshness := time.Duration(rt.plan.FrameWatchdogSec) * time.Second
	if freshness <= 0 {
		freshness = brokerDefaultFrameWatchdog
	}
	liveFPS := observedCameraAdmissionFPS(tracker.LiveFrames, tracker.FirstLiveFrameAt, tracker.LastLiveFrameAt)
	if rt.cameraAdmissionLiveRequiredLocked() {
		if !cameraAdmissionFramesReady(tracker.LiveFrames, tracker.FirstLiveFrameAt, tracker.LastLiveFrameAt, now, freshness) ||
			!liveCodecBootstrapReady(rt.plan.MainFormat, tracker) {
			return
		}
	}
	observedFPS := rt.observedAdmissionSubFPSLocked()
	if rt.plan.AnalysisEnabled && !cameraAdmissionFramesReady(
		tracker.SubFrames, tracker.FirstSubFrameAt, tracker.LastSubFrameAt, now, freshness) {
		return
	}
	archiveFPS := rt.observedAdmissionArchiveFPSLocked()
	if rt.plan.ArchiveEnabled {
		// Every required role proves fresh frames and its own codec bootstrap.
		// Performance relative to its target is reported independently.
		if !cameraAdmissionFramesReady(
			tracker.ArchiveFrames,
			tracker.FirstArchiveFrameAt,
			tracker.LastArchiveFrameAt,
			now, freshness,
		) {
			return
		}
		switch tracker.ArchiveCodec {
		case "h265":
			if !tracker.SeenVPS || !tracker.SeenSPS || !tracker.SeenPPS || !tracker.SeenKeyframe {
				return
			}
		case "h264":
			if !tracker.SeenSPS || !tracker.SeenPPS || !tracker.SeenKeyframe {
				return
			}
		default:
			return
		}
	}
	rt.cameraAdmitted = true
	rt.attestNativeCompressedCameraDecoderLocked(selected.Format)
	// A successful exec only proves that the worker was created. Reset process
	// restart backoff after the running camera has proved codec, cadence,
	// bootstrap and healthy frames, so repeated post-Start S_FMT/USB failures
	// remain exponentially bounded.
	rt.resetChildStartFailureLocked()
	if rt.uvcAdmittedModes == nil {
		rt.uvcAdmittedModes = make(map[string]struct{})
	}
	rt.uvcAdmittedModes[cameraAdmissionModeKey(selected)] = struct{}{}
	rt.resetUVCHandoffFailuresLocked()
	rt.noteCameraModeRuntimeAdmissionLocked(selected, now)
	if !cameraModeHealthReadOnly() && cameraModeHealthStatePath(rt.plan.StateDirectory) != "" &&
		validCameraModeRuntimeFingerprint(rt.modeHealthFingerprint) {
		rt.persistCameraModeHealthLocked(now)
	}
	rt.writeCameraAdmissionStateLocked("admitted", "runtime_contract_verified")
	log.Printf(
		"[video-broker] event=capture_epoch_admitted incident_id=%s media_epoch=%d child_pid=%d admission_latency_ms=%d device=%s codec=%s capture=%dx%d@%d live_fps=%.2f archive_codec=%s archive_fps=%.2f analysis_fps=%.2f fingerprint=%s",
		captureIncidentID(rt.mediaEpoch),
		rt.mediaEpoch,
		brokerChildPID(rt.childCmd),
		timestampAgeMilliseconds(now, rt.childStartedAt),
		selected.DevicePath,
		rt.observedCaptureCodec,
		rt.observedCaptureWidth,
		rt.observedCaptureHeight,
		rt.observedCaptureFPS,
		liveFPS,
		tracker.ArchiveCodec,
		archiveFPS,
		observedFPS,
		selected.CapabilityFingerprint,
	)
}

// attestNativeCompressedCameraDecoderLocked closes the runtime capability
// proof for the native V4L2 H26x path.  That worker admits an H264/H265 camera
// only after decoded NV12 cadence has been observed inside the bridge, even
// when the sole externally selected role is zero-copy live passthrough.  In
// that topology no NV12 packet crosses the Go broker, so waiting for an
// analysis packet would leave a healthy source-move candidate permanently in
// admission.  MJPEG deliberately stays on its explicit capture-decoder
// telemetry path because that evidence distinguishes JPU from CPU fallback.
func (rt *videoBrokerRuntime) attestNativeCompressedCameraDecoderLocked(format string) {
	if rt == nil || rt.capabilityPlan.WorkerProvider != "native_sp" ||
		!strings.EqualFold(strings.TrimSpace(rt.plan.Transport), videoproviders.TransportV4L2UVC) {
		return
	}
	format = capability.NormalizeFormat(format)
	if format != "h264" && format != "h265" {
		return
	}
	rt.attestCapabilityStageRoleLocked(capability.StageDecode, "capture", format)
}

func (rt *videoBrokerRuntime) cameraAdmissionLiveRequiredLocked() bool {
	if rt == nil || !rt.plan.LiveEnabled {
		return false
	}
	// Runtime-controlled producers keep a configured live encoder preheated but
	// intentionally publish no live frames until a client requests the stream.
	// Admit the source from the roles that are producing at this boundary.
	// Static producers cannot suppress live, and a connected client makes its
	// codec bootstrap and cadence mandatory again.
	return !rt.childControlSupported || len(rt.mainClients) > 0
}

func liveCodecBootstrapReady(codec string, tracker *cameraAdmissionRuntimeTracker) bool {
	if tracker == nil || !tracker.LiveSeenSPS || !tracker.LiveSeenPPS || !tracker.LiveSeenKeyframe {
		return false
	}
	return brokerLiveCodec(codec) != "h265" || tracker.LiveSeenVPS
}

func (rt *videoBrokerRuntime) noteCameraModeRuntimeAdmissionLocked(
	selected videoproviders.CameraAdmissionDecision,
	now time.Time,
) {
	if rt == nil || selected.DevicePath == "" || !cameraTransportHasDiscoverableModes(rt.plan.Transport) {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	modeKey := cameraAdmissionModeKey(selected)
	if rt.captureModeHealthEntries == nil {
		rt.captureModeHealthEntries = make(map[string]cameraModeHealthEntry)
	}
	if rt.captureModeFailures == nil {
		rt.captureModeFailures = make(map[string]int)
	}
	if entry, exists := rt.captureModeHealthEntries[modeKey]; exists {
		// A one-second admission proves that the graph can start, but it does not
		// prove that an intermittently corrupt UVC mode is stable. Preserve one
		// strike until a full healthy window has elapsed. A successful half-open
		// trial collapses exponential backoff to probation instead of erasing all
		// evidence immediately.
		if entry.Failures >= cameraModeFailureThreshold {
			entry.Failures = cameraModeFailureThreshold - 1
			entry.RetryAfter = time.Time{}
			log.Printf(
				"[video-broker] event=capture_mode_half_open incident_id=%s media_epoch=%d child_pid=%d fingerprint=%s mode=%s/%dx%d@%d failures=%d probation_until=%s",
				captureIncidentID(rt.mediaEpoch), rt.mediaEpoch, brokerChildPID(rt.childCmd),
				selected.CapabilityFingerprint, selected.Format, selected.CaptureWidth, selected.CaptureHeight,
				selected.CaptureFPS, entry.Failures, now.Add(cameraModeProbationHealthyWindow).UTC().Format(time.RFC3339),
			)
		}
		entry.StableAfter = now.Add(cameraModeProbationHealthyWindow).UTC()
		rt.captureModeHealthEntries[modeKey] = entry
		rt.captureModeFailures[modeKey] = entry.Failures
	} else if failures := rt.captureModeFailures[modeKey]; failures > 0 {
		if failures >= cameraModeFailureThreshold {
			failures = cameraModeFailureThreshold - 1
		}
		entry := cameraModeHealthEntry{
			Mode:        cameraModeExclusionForDecision(selected),
			Failures:    failures,
			StableAfter: now.Add(cameraModeProbationHealthyWindow).UTC(),
		}
		rt.captureModeHealthEntries[modeKey] = entry
		rt.captureModeFailures[modeKey] = entry.Failures
	}
	rt.refreshCameraModeHealthLocked(now)
}

func cameraAdmissionModeKey(decision videoproviders.CameraAdmissionDecision) string {
	return fmt.Sprintf(
		"%s|%s|%dx%d@%d",
		strings.TrimSpace(decision.CapabilityFingerprint),
		strings.ToLower(strings.TrimSpace(decision.Format)),
		decision.CaptureWidth,
		decision.CaptureHeight,
		decision.CaptureFPS,
	)
}

func cameraModeExclusionKey(exclusion videoproviders.V4L2CaptureModeExclusion) string {
	return fmt.Sprintf(
		"%s|%s|%dx%d@%d",
		strings.TrimSpace(exclusion.CapabilityFingerprint),
		strings.ToLower(strings.TrimSpace(exclusion.PixelFormat)),
		exclusion.Width,
		exclusion.Height,
		exclusion.FPS,
	)
}

func cameraModeQuarantineDuration(failures int) time.Duration {
	duration := cameraModeQuarantineBase
	steps := failures - cameraModeFailureThreshold
	for steps > 0 && duration < cameraModeQuarantineMaximum {
		duration *= 2
		steps--
	}
	if duration > cameraModeQuarantineMaximum {
		return cameraModeQuarantineMaximum
	}
	return duration
}

func (rt *videoBrokerRuntime) refreshCameraModeHealthLocked(now time.Time) bool {
	if rt == nil {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	if rt.captureModeHealthEntries == nil {
		rt.captureModeHealthEntries = make(map[string]cameraModeHealthEntry)
	}
	if rt.captureModeFailures == nil {
		rt.captureModeFailures = make(map[string]int)
	}
	changed := false
	admittedModeKey := ""
	if rt.cameraAdmitted && rt.cameraAdmission.DevicePath != "" {
		admittedModeKey = cameraAdmissionModeKey(rt.cameraAdmission)
	}
	active := make([]videoproviders.V4L2CaptureModeExclusion, 0, len(rt.captureModeHealthEntries))
	for key, entry := range rt.captureModeHealthEntries {
		if key == admittedModeKey && entry.Failures < cameraModeFailureThreshold &&
			!entry.StableAfter.IsZero() && !now.Before(entry.StableAfter) {
			log.Printf(
				"[video-broker] event=capture_mode_recovered incident_id=%s media_epoch=%d child_pid=%d fingerprint=%s mode=%s/%dx%d@%d previous_failures=%d stable_after=%s action=remove_mode_health_entry",
				captureIncidentID(rt.mediaEpoch), rt.mediaEpoch, brokerChildPID(rt.childCmd), entry.Mode.CapabilityFingerprint,
				entry.Mode.PixelFormat, entry.Mode.Width, entry.Mode.Height, entry.Mode.FPS,
				entry.Failures, entry.StableAfter.UTC().Format(time.RFC3339),
			)
			delete(rt.captureModeHealthEntries, key)
			delete(rt.captureModeFailures, key)
			changed = true
			continue
		}
		if entry.Failures > rt.captureModeFailures[key] {
			rt.captureModeFailures[key] = entry.Failures
		}
		if entry.Failures >= cameraModeFailureThreshold && now.Before(entry.RetryAfter) {
			active = append(active, entry.Mode)
		}
	}
	sort.Slice(active, func(i, j int) bool {
		return cameraModeExclusionKey(active[i]) < cameraModeExclusionKey(active[j])
	})
	rt.captureModeExclusions = active
	return changed
}

func isCameraModeRuntimeFailure(reason string) bool {
	reason = strings.TrimSpace(reason)
	return strings.HasPrefix(reason, "runtime_attestation_timeout_after_") ||
		reason == "uvc_streamon_failed" ||
		reason == "uvc_capture_stalled"
}

func (rt *videoBrokerRuntime) noteCameraModeRuntimeFailureLocked(reason string) {
	if rt == nil || !isCameraModeRuntimeFailure(reason) || rt.cameraAdmission.DevicePath == "" ||
		!cameraTransportHasDiscoverableModes(rt.plan.Transport) {
		return
	}
	selected := rt.cameraAdmission
	key := cameraAdmissionModeKey(selected)
	if rt.captureModeFailures == nil {
		rt.captureModeFailures = make(map[string]int)
	}
	now := time.Now()
	rt.refreshCameraModeHealthLocked(now)
	rt.captureModeFailures[key]++
	failures := rt.captureModeFailures[key]
	exclusion := cameraModeExclusionForDecision(selected)
	if rt.captureModeHealthEntries == nil {
		rt.captureModeHealthEntries = make(map[string]cameraModeHealthEntry)
	}
	rt.ensureCameraModeHealthCapacityLocked(key)
	if failures < cameraModeFailureThreshold {
		entry := cameraModeHealthEntry{
			Mode:        exclusion,
			Failures:    failures,
			StableAfter: now.Add(cameraModeProbationHealthyWindow).UTC(),
		}
		rt.captureModeHealthEntries[key] = entry
		queued := rt.persistCameraModeHealthLocked(now)
		log.Printf(
			"[video-broker] event=capture_mode_probation incident_id=%s media_epoch=%d child_pid=%d fingerprint=%s mode=%s/%dx%d@%d failures=%d threshold=%d reason=%s stable_after=%s persistence_queued=%t",
			captureIncidentID(rt.mediaEpoch), rt.mediaEpoch, brokerChildPID(rt.childCmd),
			selected.CapabilityFingerprint, selected.Format, selected.CaptureWidth, selected.CaptureHeight,
			selected.CaptureFPS, failures, cameraModeFailureThreshold, reason,
			entry.StableAfter.Format(time.RFC3339), queued,
		)
		return
	}
	retryAfter := now.Add(cameraModeQuarantineDuration(failures))
	rt.captureModeHealthEntries[key] = cameraModeHealthEntry{
		Mode: exclusion, Failures: failures, RetryAfter: retryAfter.UTC(),
	}
	rt.refreshCameraModeHealthLocked(now)
	queued := rt.persistCameraModeHealthLocked(now)
	log.Printf(
		"[video-broker] event=capture_mode_quarantined incident_id=%s media_epoch=%d child_pid=%d fingerprint=%s mode=%s/%dx%d@%d failures=%d reason=%s fallback=next_native_mode retry_after=%s persistence_queued=%t",
		captureIncidentID(rt.mediaEpoch), rt.mediaEpoch, brokerChildPID(rt.childCmd),
		selected.CapabilityFingerprint, selected.Format, selected.CaptureWidth, selected.CaptureHeight,
		selected.CaptureFPS, failures, reason, retryAfter.UTC().Format(time.RFC3339), queued,
	)
}

func cameraModeExclusionForDecision(selected videoproviders.CameraAdmissionDecision) videoproviders.V4L2CaptureModeExclusion {
	return videoproviders.V4L2CaptureModeExclusion{
		CapabilityFingerprint: selected.CapabilityFingerprint,
		PixelFormat:           strings.ToLower(strings.TrimSpace(selected.Format)),
		Width:                 selected.CaptureWidth,
		Height:                selected.CaptureHeight,
		FPS:                   selected.CaptureFPS,
	}
}

func (rt *videoBrokerRuntime) ensureCameraModeHealthCapacityLocked(key string) {
	if rt == nil || len(rt.captureModeHealthEntries) < cameraModeHealthMaximumExclusions {
		return
	}
	if _, exists := rt.captureModeHealthEntries[key]; exists {
		return
	}
	oldestKey := ""
	var oldest time.Time
	for candidateKey, candidate := range rt.captureModeHealthEntries {
		deadline := candidate.StableAfter
		if deadline.IsZero() {
			deadline = candidate.RetryAfter
		}
		if oldestKey == "" || deadline.Before(oldest) ||
			(deadline.Equal(oldest) && candidateKey < oldestKey) {
			oldestKey = candidateKey
			oldest = deadline
		}
	}
	delete(rt.captureModeHealthEntries, oldestKey)
	delete(rt.captureModeFailures, oldestKey)
}

func (rt *videoBrokerRuntime) persistCameraModeHealthLocked(now time.Time) bool {
	if rt == nil || cameraModeHealthReadOnly() {
		return false
	}
	if !validCameraModeRuntimeFingerprint(rt.modeHealthFingerprint) {
		rt.modeHealthFingerprint = cameraModeRuntimeFingerprint(rt.plan)
	}
	body, err := encodeCameraModeHealthEntries(
		rt.plan.StateDirectory,
		rt.modeHealthFingerprint,
		rt.captureModeHealthEntries,
		now,
	)
	if err != nil {
		log.Printf("[video-broker] camera mode health snapshot failed path=%s err=%v", cameraModeHealthStatePath(rt.plan.StateDirectory), err)
		return false
	}
	return rt.queueCameraStateLocked(cameraModeHealthDocument, cameraModeHealthStatePath(rt.plan.StateDirectory), body)
}

func cameraTransportHasDiscoverableModes(transport string) bool {
	switch strings.ToLower(strings.TrimSpace(transport)) {
	case videoproviders.TransportV4L2, videoproviders.TransportV4L2UVC:
		return true
	default:
		return false
	}
}

func cameraModeHealthReadOnly() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(cameraModeHealthReadOnlyEnv)))
	return value == "1" || value == "true"
}

func cameraModeExclusionMatchesDecision(
	exclusion videoproviders.V4L2CaptureModeExclusion,
	decision videoproviders.CameraAdmissionDecision,
) bool {
	return strings.TrimSpace(exclusion.CapabilityFingerprint) == strings.TrimSpace(decision.CapabilityFingerprint) &&
		strings.EqualFold(strings.TrimSpace(exclusion.PixelFormat), strings.TrimSpace(decision.Format)) &&
		exclusion.Width == decision.CaptureWidth && exclusion.Height == decision.CaptureHeight &&
		exclusion.FPS == decision.CaptureFPS
}

func cameraModeHealthStatePath(configuredStateRoot string) string {
	stateRoot := strings.TrimSpace(configuredStateRoot)
	if stateRoot == "" || !filepath.IsAbs(stateRoot) {
		return ""
	}
	return filepath.Join(stateRoot, cameraStateDirectoryName, cameraModeHealthStateFilename)
}

func loadCameraModeHealthEntries(
	stateRoot string,
	runtimeFingerprint string,
	now time.Time,
) map[string]cameraModeHealthEntry {
	path := cameraModeHealthStatePath(stateRoot)
	if path == "" || !validCameraModeRuntimeFingerprint(runtimeFingerprint) {
		return nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[video-broker] camera mode health load failed path=%s err=%v", path, err)
		}
		return nil
	}
	var state cameraModeHealthState
	if err := json.Unmarshal(body, &state); err != nil || state.Schema != cameraModeHealthSchema ||
		!validCameraModeRuntimeFingerprint(state.RuntimeFingerprint) ||
		state.UpdatedAt.IsZero() || now.Sub(state.UpdatedAt) < 0 || now.Sub(state.UpdatedAt) > cameraModeHealthEvidenceTTL ||
		len(state.Entries) > cameraModeHealthMaximumExclusions {
		log.Printf("[video-broker] camera mode health ignored path=%s reason=invalid_or_expired", path)
		return nil
	}
	if state.RuntimeFingerprint != runtimeFingerprint {
		log.Printf("[video-broker] camera mode health ignored path=%s reason=video_graph_changed", path)
		return nil
	}
	valid := make(map[string]cameraModeHealthEntry, len(state.Entries))
	for _, entry := range state.Entries {
		exclusion := entry.Mode
		if !strings.HasPrefix(exclusion.CapabilityFingerprint, "cap-v1:") ||
			strings.TrimSpace(exclusion.PixelFormat) == "" || exclusion.Width <= 0 || exclusion.Height <= 0 || exclusion.FPS <= 0 ||
			entry.Failures < 1 ||
			(entry.Failures < cameraModeFailureThreshold && (entry.StableAfter.IsZero() ||
				entry.StableAfter.Before(state.UpdatedAt) ||
				entry.StableAfter.Sub(state.UpdatedAt) > cameraModeProbationHealthyWindow ||
				!entry.RetryAfter.IsZero())) ||
			(entry.Failures >= cameraModeFailureThreshold && (entry.RetryAfter.IsZero() ||
				entry.RetryAfter.Before(state.UpdatedAt) || entry.RetryAfter.Sub(state.UpdatedAt) > cameraModeQuarantineMaximum ||
				!entry.StableAfter.IsZero())) {
			continue
		}
		key := cameraModeExclusionKey(exclusion)
		if previous, exists := valid[key]; !exists || entry.RetryAfter.After(previous.RetryAfter) {
			entry.RetryAfter = entry.RetryAfter.UTC()
			valid[key] = entry
		}
	}
	return valid
}

func encodeCameraModeHealthEntries(
	stateRoot string, runtimeFingerprint string,
	entries map[string]cameraModeHealthEntry, now time.Time,
) ([]byte, error) {
	path := cameraModeHealthStatePath(stateRoot)
	if path == "" {
		return nil, fmt.Errorf("video-broker state directory is not an absolute path")
	}
	if !validCameraModeRuntimeFingerprint(runtimeFingerprint) {
		return nil, fmt.Errorf("video-broker runtime fingerprint is invalid")
	}
	ordered := make([]cameraModeHealthEntry, 0, len(entries))
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ordered = append(ordered, entries[key])
	}
	state := cameraModeHealthState{
		Schema: cameraModeHealthSchema, RuntimeFingerprint: runtimeFingerprint, UpdatedAt: now.UTC(),
		Entries: ordered,
	}
	return json.Marshal(state)
}

func cameraModeRuntimeFingerprint(plan videoBrokerPlan) string {
	identity := plan
	identity.MainSocketPath = ""
	identity.ArchiveSocketPath = ""
	identity.SubSocketPath = ""
	identity.SubShmPath = ""
	identity.SubNotifySocketPath = ""
	identity.CaptureDevice = ""
	identity.ResourceGovernorStatePath = ""
	identity.StateDirectory = ""
	identity.EngineDirectory = ""
	identity.QualityMonitorStatePath = ""
	identity.CameraAdmissionStatePath = ""

	hash := sha256.New()
	writeFingerprintPart(hash, "contract", identity)
	executable, err := os.Executable()
	if err != nil {
		writeFingerprintPart(hash, "broker", "unavailable")
	} else {
		writeFingerprintPart(hash, "broker", cameraModeRuntimeFileDigest(executable))
	}
	engineDirectory := resolveEngineDirectory(plan.EngineDirectory)
	workers := []struct {
		name     string
		env      string
		filename string
	}{
		{name: "vendor_hbn", env: rdkBrokerMuxBridgeEnv, filename: "rdkx5_hbn_mux_bridge"},
		{name: "v4l2_uvc", env: rdkBrokerV4L2MuxBridgeEnv, filename: "rdkx5_v4l2_mux_bridge"},
		{name: "portable", env: gstVideoIngestBridgeEnv, filename: "video-ingest-gst"},
	}
	for _, worker := range workers {
		path := strings.TrimSpace(os.Getenv(worker.env))
		if path == "" {
			path = filepath.Join(engineDirectory, worker.filename)
		}
		writeFingerprintPart(hash, "worker:"+worker.name, cameraModeRuntimeFileDigest(path))
	}
	return cameraModeRuntimeFingerprintPrefix + fmt.Sprintf("%x", hash.Sum(nil))
}

func writeFingerprintPart(writer io.Writer, name string, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		body = []byte(`"unavailable"`)
	}
	_, _ = fmt.Fprintf(writer, "%d:%s:%d:", len(name), name, len(body))
	_, _ = writer.Write(body)
}

func cameraModeRuntimeFileDigest(path string) string {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return "missing"
		}
		return "unreadable"
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "unreadable"
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil))
}

func validCameraModeRuntimeFingerprint(value string) bool {
	if !strings.HasPrefix(value, cameraModeRuntimeFingerprintPrefix) ||
		len(value) != len(cameraModeRuntimeFingerprintPrefix)+sha256.Size*2 {
		return false
	}
	for _, character := range value[len(cameraModeRuntimeFingerprintPrefix):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func (rt *videoBrokerRuntime) observedAdmissionArchiveFPSLocked() float64 {
	tracker := rt.cameraAdmissionTracker
	return observedCameraAdmissionFPS(tracker.ArchiveFrames, tracker.FirstArchiveFrameAt, tracker.LastArchiveFrameAt)
}

func (rt *videoBrokerRuntime) observedAdmissionSubFPSLocked() float64 {
	tracker := rt.cameraAdmissionTracker
	return observedCameraAdmissionFPS(tracker.SubFrames, tracker.FirstSubFrameAt, tracker.LastSubFrameAt)
}

func observedCameraAdmissionFPS(frames uint64, first, last time.Time) float64 {
	if frames < 2 || first.IsZero() || last.IsZero() {
		return 0
	}
	elapsed := last.Sub(first)
	if elapsed <= 0 {
		return 0
	}
	return float64(frames-1) / elapsed.Seconds()
}

func cameraAdmissionFramesReady(frames uint64, first, last, now time.Time, freshness time.Duration) bool {
	return frames >= 3 && !first.IsZero() && last.After(first) &&
		freshness > 0 && now.Sub(last) < freshness
}

func (rt *videoBrokerRuntime) writeCameraAdmissionStateLocked(status, reason string) {
	path := strings.TrimSpace(rt.plan.CameraAdmissionStatePath)
	if path == "" || rt.cameraAdmission.PreferredDevicePath == "" && rt.cameraAdmission.DevicePath == "" {
		return
	}
	selected := rt.cameraAdmission
	tracker := rt.cameraAdmissionTracker
	archiveWidth, archiveHeight := rt.activeArchiveGeometryLocked()
	sourceTransport := strings.ToLower(strings.TrimSpace(selected.SourceTransport))
	if sourceTransport == "" {
		sourceTransport = strings.ToLower(strings.TrimSpace(rt.plan.Transport))
	}
	state := cameraAdmissionState{
		Schema: cameraAdmissionSchema, UpdatedAt: time.Now().UTC(), Status: status, Reason: reason,
		Discovery: cameraAdmissionDiscoveryState{
			SourceTransport: sourceTransport,
			PreferredDevice: selected.PreferredDevicePath, SelectedDevice: selected.DevicePath,
			DeviceIdentity: selected.DeviceIdentity, CapabilityFingerprint: selected.CapabilityFingerprint,
			SelectionReason: selected.Reason,
			CapabilityCount: selected.CapabilityCount,
			ProbedDevices:   append([]string(nil), selected.ProbedDevices...), RejectedDevices: append([]string(nil), selected.RejectedDevices...),
			RuntimeRejectedModes: cameraModeExclusionLabels(rt.captureModeExclusions, selected.CapabilityFingerprint),
		},
		Contract: cameraAdmissionContractState{
			CodecPreference: rt.plan.CaptureCodecPreference, CodecPolicy: selected.CodecPolicy,
			AspectPolicy: selected.AspectPolicy, CaptureCodec: selected.Format,
			CaptureWidth: selected.CaptureWidth, CaptureHeight: selected.CaptureHeight, CaptureFPS: selected.CaptureFPS,
			ArchiveWidth:           archiveWidth,
			ArchiveHeight:          archiveHeight,
			ArchiveWidthAlignment:  videoproviders.EffectiveNV12Alignment(rt.plan.ArchiveWidthAlignment),
			ArchiveHeightAlignment: videoproviders.EffectiveNV12Alignment(rt.plan.ArchiveHeightAlignment),
			HardwareOnly:           rt.plan.RequireHWCodec,
		},
		Runtime: cameraAdmissionRuntimeState{
			CaptureCodec: rt.observedCaptureCodec, CaptureWidth: rt.observedCaptureWidth,
			CaptureHeight: rt.observedCaptureHeight, CaptureFPS: rt.observedCaptureFPS,
			ArchiveCodec: rt.observedArchiveCodec, ArchiveDelivery: rt.observedArchiveDelivery,
			SubFrames: tracker.SubFrames, ObservedSubFPS: rt.observedAdmissionSubFPSLocked(),
			ArchiveFrames: tracker.ArchiveFrames, ObservedArchiveFPS: rt.observedAdmissionArchiveFPSLocked(),
			SeenVPS: tracker.SeenVPS, SeenSPS: tracker.SeenSPS, SeenPPS: tracker.SeenPPS, SeenKeyframe: tracker.SeenKeyframe,
		},
	}
	body, err := json.Marshal(state)
	if err != nil {
		log.Printf("[video-broker] camera admission state snapshot failed path=%s err=%v", path, err)
		return
	}
	rt.queueCameraStateLocked(cameraAdmissionDocument, path, body)
}

func cameraModeExclusionLabels(
	exclusions []videoproviders.V4L2CaptureModeExclusion,
	fingerprint string,
) []string {
	labels := make([]string, 0, len(exclusions))
	for _, exclusion := range exclusions {
		if strings.TrimSpace(exclusion.CapabilityFingerprint) != strings.TrimSpace(fingerprint) {
			continue
		}
		labels = append(labels, fmt.Sprintf(
			"%s/%dx%d@%d",
			strings.ToLower(strings.TrimSpace(exclusion.PixelFormat)),
			exclusion.Width,
			exclusion.Height,
			exclusion.FPS,
		))
	}
	return labels
}

func ensureCameraStateDirectory(dir string) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	// MkdirAll applies the process umask to newly created directories. Release
	// packaging deliberately runs with umask 0077, while this shared runtime
	// state directory has an exact 0750 contract. Reconcile the final mode for
	// both newly created and pre-existing directories.
	return os.Chmod(dir, 0o750)
}
