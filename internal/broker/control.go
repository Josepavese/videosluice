package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	brokerControlProtocol                = "NMC1"
	brokerBridgeControlProtocol          = "NMC2"
	brokerBridgeRetryActionNone          = "none"
	brokerBridgeRetryActionTrial         = "trial"
	brokerBridgeRetryActionCommit        = "commit"
	brokerBridgeRetryActionCancel        = "cancel"
	brokerControlMaxLineBytes            = 384
	brokerResourceGovernorSchema         = "video-broker.resource-governor.v1"
	brokerResourceGovernorBPUPollEvery   = 15 * time.Second
	brokerResourceGovernorCommandTimeout = 3 * time.Second
	// Defaults stay conservative across hardware. Higher values are available
	// only to an explicit board-owned profile and remain capped below the
	// supported shutdown range.
	brokerResourceGovernorSoftDefaultC = 85
	brokerResourceGovernorHardDefaultC = 90
	brokerResourceGovernorSoftMaxC     = 90
	brokerResourceGovernorHardMaxC     = 95
)

var (
	brokerExecCommandContext          = exec.CommandContext
	brokerBPUStatusPattern            = regexp.MustCompile(`(?m)^\s*bpu[0-9]+:\s+\d+\s+\d+\s+\d+\s+(\d+)\s*$`)
	brokerBridgeRetryComponentPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	brokerCapabilityPattern           = regexp.MustCompile(`^\[(?:rdkx5_v4l2_mux_bridge|rdkx5_hbn_mux_bridge|video-ingest-gst)\]\[capability\] live_enabled=(true|false) analysis_enabled=(true|false) archive_enabled=(true|false) capture_codec=(h264|h265|mjpeg|yuyv|nv12) capture=([0-9]+)x([0-9]+) capture_fps=([0-9]+) live_codec=(h264|h265|none) archive_codec=(h264|h265|none) archive_delivery=(native_passthrough|source_hardware_encode|processed_hardware_encode|software_encode|disabled) archive_fps=([0-9]+) live_control_mode=(persistent_encoder_epoch|native_passthrough_static|backend_static|disabled) fanout_control_mode=(branch_transaction_v1|disabled)$`)
	brokerCaptureDecoderPattern       = regexp.MustCompile(`^\[rdkx5_v4l2_mux_bridge\]\[capture-decoder\] codec=(mjpeg) requested=(auto|jpu|cpu) selected=(jpu|cpu) fallback=([a-z0-9_]+) output=(nv12) retry_token=([0-9]+)$`)
	brokerCaptureDecoderTrialPattern  = regexp.MustCompile(`^\[rdkx5_v4l2_mux_bridge\]\[capture-decoder-trial\] event=(candidate_ready|candidate_canceled) component=([a-z0-9][a-z0-9._-]{0,63}) retry_token=([0-9]+)(?: completed_unix_ms=([0-9]+))?$`)
	brokerBridgeControlReadyPattern   = regexp.MustCompile(`^\[rdkx5_v4l2_mux_bridge\]\[control\] ready seq=([0-9]+) main_active=true main_fps=([0-9]+) main_bitrate_kbps=([0-9]+) sub_fps=([0-9]+) encoder_epoch=([0-9]+) scope=live_persistent_rc$`)
	brokerToneMappingPattern          = regexp.MustCompile(`^\[(?:rdkx5_v4l2_mux_bridge|rdkx5_hbn_mux_bridge|video-ingest-gst)\]\[tone-mapper\] event=tone_mapping_summary requested_mode=(off|global|local|auto) active_mode=(off|global|local|auto)(?: accelerator_requested=(auto|cpu|opencl) accelerator_selected=(cpu|opencl))?.* gain_min=([0-9]+(?:\.[0-9]+)?) gain_max=([0-9]+(?:\.[0-9]+)?) (?:statistics_hz=[0-9]+ effective_statistics_hz=[0-9]+ )?statistics_updates=([0-9]+) active=(true|false)$`)
	brokerStageCapabilityPattern      = regexp.MustCompile(`^\[(?:rdkx5_v4l2_mux_bridge|rdkx5_hbn_mux_bridge|video-ingest-gst)\]\[stage-capability\] stage=(decode|encode|resize|convert) role=(capture|live|archive|analysis) format=(h264|h265|mjpeg|nv12) requested_backend=([a-z0-9_]+) selected_backend=(native_sp|native_sp_resize|native_neon|native_cpu|gstreamer_vendor|gstreamer_mpp|gstreamer_v4l2|gstreamer_mpp_resize|gstreamer_video|software) element=([a-zA-Z0-9_]+) fallback=([a-z0-9_]+)$`)
)

type brokerRuntimeCapability struct {
	LiveEnabled       bool
	AnalysisEnabled   bool
	ArchiveEnabled    bool
	CaptureCodec      string
	CaptureWidth      int
	CaptureHeight     int
	CaptureFPS        int
	LiveCodec         string
	ArchiveCodec      string
	ArchiveDelivery   string
	ArchiveFPS        int
	LiveControlMode   string
	FanoutControlMode string
}

type brokerToneMappingTelemetry struct {
	RequestedMode string
	ActiveMode    string
	Accelerator   string
	GainMin       float64
	GainMax       float64
	Updates       uint64
	Active        bool
}

type brokerCaptureDecoderTelemetry struct {
	Codec      string
	Requested  string
	Selected   string
	Fallback   string
	Output     string
	RetryToken uint64
}

type brokerCaptureDecoderTrialTelemetry struct {
	Event             string
	ComponentID       string
	RetryToken        uint64
	CompletedAtUnixMS int64
}

type brokerBridgeControlReadyTelemetry struct {
	Sequence        uint64
	MainActive      bool
	MainFPS         int
	MainBitrateKbps int
	SubFPS          int
	EncoderEpoch    uint64
}

type brokerBridgeControlReadyObservation struct {
	brokerBridgeControlReadyTelemetry
	MediaEpoch uint64
	ChildPID   int
	ObservedAt time.Time
}

type brokerStageCapabilityTelemetry struct {
	Stage     string
	Role      string
	Format    string
	Requested string
	Selected  string
	Element   string
	Fallback  string
}

type videoMainClientControl struct {
	TargetFPS            int
	TargetBitrateKbps    int
	RequestKeyframeToken uint64
}

type bridgeRuntimeControl struct {
	Sequence                uint64
	MainActive              bool
	MainFPS                 int
	MainBitrateKbps         int
	SubFPS                  int
	OptionalComputeAdmitted bool
	RequestKeyframeToken    uint64
	RetryAction             string
	RetryComponent          string
	RetryToken              uint64
	RetryDeadlineMS         int64
	Reason                  string
}

func liveEncoderActivationChanged(before, after bridgeRuntimeControl) bool {
	return !before.MainActive && after.MainActive
}

type resourceGovernorDecision struct {
	Mode                    string             `json:"mode"`
	Reason                  string             `json:"reason"`
	ObservedAt              time.Time          `json:"observed_at"`
	MaxTemperatureCelsius   float64            `json:"max_temperature_celsius,omitempty"`
	ThermalZonesCelsius     map[string]float64 `json:"thermal_zones_celsius,omitempty"`
	BPUUtilizationPercent   *int               `json:"bpu_utilization_percent,omitempty"`
	MainFPSCap              int                `json:"live_fps_cap"`
	MainBitrateKbpsCap      int                `json:"live_bitrate_kbps_cap"`
	SubFPS                  int                `json:"analysis_fps_cap"`
	OptionalComputeAdmitted bool               `json:"optional_compute_admitted"`
}

type resourceGovernorState struct {
	Schema           string                       `json:"schema"`
	UpdatedAt        time.Time                    `json:"updated_at"`
	Mode             string                       `json:"mode"`
	Reason           string                       `json:"reason"`
	Inputs           resourceGovernorStateInputs  `json:"inputs"`
	Media            resourceGovernorStateMedia   `json:"media"`
	Effective        resourceGovernorStateOutputs `json:"effective"`
	Priorities       []string                     `json:"priorities"`
	ArchiveProtected bool                         `json:"archive_protected"`
}

type resourceGovernorStateInputs struct {
	ThermalZonesCelsius   map[string]float64 `json:"thermal_zones_celsius"`
	MaxTemperatureCelsius float64            `json:"max_temperature_celsius"`
	BPUUtilizationPercent *int               `json:"bpu_utilization_percent"`
	LiveClients           int                `json:"live_clients"`
}

type resourceGovernorStateMedia struct {
	CaptureCodec       string `json:"capture_codec"`
	CaptureWidth       int    `json:"capture_width"`
	CaptureHeight      int    `json:"capture_height"`
	CaptureFPS         int    `json:"capture_fps"`
	ArchiveCodec       string `json:"archive_codec"`
	ArchiveDelivery    string `json:"archive_delivery"`
	ArchiveFPS         int    `json:"archive_fps"`
	LiveControlMode    string `json:"live_control_mode"`
	LiveRuntimeControl bool   `json:"live_runtime_control"`
	LiveEncoderEpoch   uint64 `json:"live_encoder_epoch"`
}

type resourceGovernorStateOutputs struct {
	CaptureFPS              int  `json:"capture_fps"`
	ArchiveFPS              int  `json:"archive_fps"`
	LiveActive              bool `json:"live_active"`
	LiveFPS                 int  `json:"live_fps"`
	LiveBitrateKbps         int  `json:"live_bitrate_kbps"`
	AnalysisFPS             int  `json:"analysis_fps"`
	OptionalComputeAdmitted bool `json:"optional_compute_admitted"`
}

func nominalResourceGovernorDecision(plan videoBrokerPlan) resourceGovernorDecision {
	return resourceGovernorDecision{
		Mode:                    "nominal",
		Reason:                  "within_budget",
		ObservedAt:              time.Now().UTC(),
		MainFPSCap:              positiveOr(plan.MainFPS, 15),
		MainBitrateKbpsCap:      positiveOr(plan.LiveBitrateKbps, 4096),
		SubFPS:                  positiveOr(plan.SubFPS, 15),
		OptionalComputeAdmitted: true,
	}
}

func positiveOr(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func parseBrokerRuntimeCapability(line string) (brokerRuntimeCapability, bool) {
	matches := brokerCapabilityPattern.FindStringSubmatch(strings.TrimSpace(line))
	if len(matches) != 14 {
		return brokerRuntimeCapability{}, false
	}
	values := make([]int, 0, 4)
	for index, raw := range []string{matches[5], matches[6], matches[7], matches[11]} {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 || (index < 3 && value == 0) {
			return brokerRuntimeCapability{}, false
		}
		values = append(values, value)
	}
	if values[0] > 16384 || values[1] > 16384 || values[2] > 240 || values[3] > 240 {
		return brokerRuntimeCapability{}, false
	}
	liveEnabled := matches[1] == "true"
	analysisEnabled := matches[2] == "true"
	archiveEnabled := matches[3] == "true"
	if !liveEnabled && !analysisEnabled && !archiveEnabled {
		return brokerRuntimeCapability{}, false
	}
	if archiveEnabled {
		if matches[9] == "none" || matches[10] == "disabled" || values[3] == 0 {
			return brokerRuntimeCapability{}, false
		}
	} else if matches[9] != "none" || matches[10] != "disabled" || values[3] != 0 {
		return brokerRuntimeCapability{}, false
	}
	if liveEnabled != (matches[8] != "none") || liveEnabled != (matches[12] != "disabled") {
		return brokerRuntimeCapability{}, false
	}
	return brokerRuntimeCapability{
		LiveEnabled:       liveEnabled,
		AnalysisEnabled:   analysisEnabled,
		ArchiveEnabled:    archiveEnabled,
		CaptureCodec:      matches[4],
		CaptureWidth:      values[0],
		CaptureHeight:     values[1],
		CaptureFPS:        values[2],
		LiveCodec:         matches[8],
		ArchiveCodec:      matches[9],
		ArchiveDelivery:   matches[10],
		ArchiveFPS:        values[3],
		LiveControlMode:   matches[12],
		FanoutControlMode: matches[13],
	}, true
}

func parseBrokerToneMappingTelemetry(line string) (brokerToneMappingTelemetry, bool) {
	matches := brokerToneMappingPattern.FindStringSubmatch(strings.TrimSpace(line))
	if len(matches) != 9 {
		return brokerToneMappingTelemetry{}, false
	}
	minimum, err := strconv.ParseFloat(matches[5], 64)
	if err != nil || minimum < 0.1 || minimum > 4.0 {
		return brokerToneMappingTelemetry{}, false
	}
	maximum, err := strconv.ParseFloat(matches[6], 64)
	if err != nil || maximum < 0.1 || maximum > 4.0 || maximum < minimum {
		return brokerToneMappingTelemetry{}, false
	}
	updates, err := strconv.ParseUint(matches[7], 10, 64)
	if err != nil {
		return brokerToneMappingTelemetry{}, false
	}
	return brokerToneMappingTelemetry{
		RequestedMode: matches[1],
		ActiveMode:    matches[2],
		Accelerator:   matches[4],
		GainMin:       minimum,
		GainMax:       maximum,
		Updates:       updates,
		Active:        matches[8] == "true",
	}, true
}

func parseBrokerCaptureDecoderTelemetry(line string) (brokerCaptureDecoderTelemetry, bool) {
	matches := brokerCaptureDecoderPattern.FindStringSubmatch(strings.TrimSpace(line))
	if len(matches) != 7 {
		return brokerCaptureDecoderTelemetry{}, false
	}
	retryToken, err := strconv.ParseUint(matches[6], 10, 64)
	if err != nil {
		return brokerCaptureDecoderTelemetry{}, false
	}
	return brokerCaptureDecoderTelemetry{
		Codec: matches[1], Requested: matches[2], Selected: matches[3],
		Fallback: matches[4], Output: matches[5], RetryToken: retryToken,
	}, true
}

func parseBrokerCaptureDecoderTrialTelemetry(line string) (brokerCaptureDecoderTrialTelemetry, bool) {
	matches := brokerCaptureDecoderTrialPattern.FindStringSubmatch(strings.TrimSpace(line))
	if len(matches) != 5 {
		return brokerCaptureDecoderTrialTelemetry{}, false
	}
	retryToken, err := strconv.ParseUint(matches[3], 10, 64)
	if err != nil || retryToken == 0 {
		return brokerCaptureDecoderTrialTelemetry{}, false
	}
	completedAtUnixMS := int64(0)
	if matches[1] == "candidate_ready" {
		if matches[4] == "" {
			return brokerCaptureDecoderTrialTelemetry{}, false
		}
		completedAtUnixMS, err = strconv.ParseInt(matches[4], 10, 64)
		if err != nil || completedAtUnixMS <= 0 {
			return brokerCaptureDecoderTrialTelemetry{}, false
		}
	} else if matches[4] != "" {
		return brokerCaptureDecoderTrialTelemetry{}, false
	}
	return brokerCaptureDecoderTrialTelemetry{
		Event: matches[1], ComponentID: matches[2], RetryToken: retryToken,
		CompletedAtUnixMS: completedAtUnixMS,
	}, true
}

func parseBrokerBridgeControlReadyTelemetry(line string) (brokerBridgeControlReadyTelemetry, bool) {
	matches := brokerBridgeControlReadyPattern.FindStringSubmatch(strings.TrimSpace(line))
	if len(matches) != 6 {
		return brokerBridgeControlReadyTelemetry{}, false
	}
	sequence, err := strconv.ParseUint(matches[1], 10, 64)
	if err != nil || sequence == 0 {
		return brokerBridgeControlReadyTelemetry{}, false
	}
	mainFPS, err := strconv.Atoi(matches[2])
	if err != nil || mainFPS < 1 || mainFPS > 240 {
		return brokerBridgeControlReadyTelemetry{}, false
	}
	mainBitrateKbps, err := strconv.Atoi(matches[3])
	if err != nil || mainBitrateKbps < 1 || mainBitrateKbps > 1<<20 {
		return brokerBridgeControlReadyTelemetry{}, false
	}
	subFPS, err := strconv.Atoi(matches[4])
	if err != nil || subFPS < 1 || subFPS > 240 {
		return brokerBridgeControlReadyTelemetry{}, false
	}
	encoderEpoch, err := strconv.ParseUint(matches[5], 10, 64)
	if err != nil || encoderEpoch == 0 {
		return brokerBridgeControlReadyTelemetry{}, false
	}
	return brokerBridgeControlReadyTelemetry{
		Sequence: sequence, MainActive: true, MainFPS: mainFPS,
		MainBitrateKbps: mainBitrateKbps, SubFPS: subFPS, EncoderEpoch: encoderEpoch,
	}, true
}

func parseBrokerStageCapabilityTelemetry(line string) (brokerStageCapabilityTelemetry, bool) {
	matches := brokerStageCapabilityPattern.FindStringSubmatch(strings.TrimSpace(line))
	if len(matches) != 8 {
		return brokerStageCapabilityTelemetry{}, false
	}
	return brokerStageCapabilityTelemetry{
		Stage: matches[1], Role: matches[2], Format: matches[3], Requested: matches[4],
		Selected: matches[5], Element: matches[6], Fallback: matches[7],
	}, true
}

func parseVideoMainClientControl(line string, plan videoBrokerPlan) (videoMainClientControl, error) {
	fields := strings.Fields(strings.TrimSpace(line))
	if len(fields) != 4 || fields[0] != brokerControlProtocol {
		return videoMainClientControl{}, errors.New("invalid control envelope")
	}
	values := map[string]uint64{}
	for _, field := range fields[1:] {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 || (parts[0] != "target_fps" && parts[0] != "target_bitrate_kbps" && parts[0] != "request_keyframe_token") {
			return videoMainClientControl{}, errors.New("invalid control field")
		}
		if _, exists := values[parts[0]]; exists {
			return videoMainClientControl{}, errors.New("duplicate control field")
		}
		value, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil || (parts[0] != "request_keyframe_token" && value == 0) {
			return videoMainClientControl{}, errors.New("invalid control value")
		}
		values[parts[0]] = value
	}
	control := videoMainClientControl{
		TargetFPS:            int(values["target_fps"]),
		TargetBitrateKbps:    int(values["target_bitrate_kbps"]),
		RequestKeyframeToken: values["request_keyframe_token"],
	}
	if control.TargetFPS > plan.MainFPS || control.TargetBitrateKbps > plan.LiveBitrateKbps ||
		control.TargetFPS < 1 || control.TargetBitrateKbps < 128 {
		return videoMainClientControl{}, errors.New("control exceeds negotiated profile")
	}
	return control, nil
}

func (rt *videoBrokerRuntime) bridgeControlTargetsLocked() bridgeRuntimeControl {
	fps := positiveOr(rt.plan.MainFPS, 15)
	bitrate := positiveOr(rt.plan.LiveBitrateKbps, 4096)
	for _, client := range rt.mainClients {
		if client.mainControl.TargetFPS > 0 && client.mainControl.TargetFPS < fps {
			fps = client.mainControl.TargetFPS
		}
		if client.mainControl.TargetBitrateKbps > 0 && client.mainControl.TargetBitrateKbps < bitrate {
			bitrate = client.mainControl.TargetBitrateKbps
		}
	}
	subFPS := positiveOr(rt.plan.SubFPS, 15)
	mainActive := rt.plan.LiveEnabled && (len(rt.mainClients) > 0 || rt.routingLiveDemand)
	optionalComputeAdmitted := true
	if !rt.childControlSupported {
		// Static adapters cannot truthfully apply runtime caps. Their fixed live
		// path remains active because it may also be shared with archive.
		mainActive = rt.plan.LiveEnabled
	} else {
		governor := rt.governor
		if governor.MainFPSCap > 0 && governor.MainFPSCap < fps {
			fps = governor.MainFPSCap
		}
		if governor.MainBitrateKbpsCap > 0 && governor.MainBitrateKbpsCap < bitrate {
			bitrate = governor.MainBitrateKbpsCap
		}
		if governor.SubFPS > 0 && governor.SubFPS < subFPS {
			subFPS = governor.SubFPS
		}
		optionalComputeAdmitted = governor.OptionalComputeAdmitted
	}
	return bridgeRuntimeControl{
		MainActive:              mainActive,
		MainFPS:                 fps,
		MainBitrateKbps:         bitrate,
		SubFPS:                  subFPS,
		OptionalComputeAdmitted: optionalComputeAdmitted,
		RequestKeyframeToken:    rt.mainKeyframeRequestToken,
	}
}

func (rt *videoBrokerRuntime) effectiveBridgeControlLocked(reason string) bridgeRuntimeControl {
	control := rt.bridgeControlTargetsLocked()
	for _, recovery := range rt.componentRecovery {
		if !rt.hardwareComponentRecoveryActiveLocked(recovery) {
			continue
		}
		action := ""
		token := uint64(0)
		deadlineMS := int64(0)
		childPID := brokerChildPID(rt.childCmd)
		if recovery.CancelToken != 0 && recovery.CancelMediaEpoch == rt.mediaEpoch &&
			recovery.CancelChildPID == childPID {
			action = brokerBridgeRetryActionCancel
			token = recovery.CancelToken
		} else if recovery.TrialToken != 0 && recovery.StableAfter.IsZero() &&
			recovery.TrialMediaEpoch == rt.mediaEpoch && recovery.TrialChildPID == childPID {
			action = brokerBridgeRetryActionTrial
			if recovery.TrialCommitRequested {
				action = brokerBridgeRetryActionCommit
			}
			token = recovery.TrialToken
			deadlineMS = recovery.TrialDeadline.UnixMilli()
		}
		if action == "" || !brokerBridgeRetryComponentPattern.MatchString(recovery.ComponentID) {
			continue
		}
		selectedPriority := 0
		candidatePriority := 1
		if control.RetryAction == brokerBridgeRetryActionCancel {
			selectedPriority = 2
		}
		if action == brokerBridgeRetryActionCancel {
			candidatePriority = 2
		}
		if control.RetryComponent == "" || candidatePriority > selectedPriority ||
			(candidatePriority == selectedPriority && recovery.ComponentID < control.RetryComponent) {
			control.RetryAction = action
			control.RetryComponent = recovery.ComponentID
			control.RetryToken = token
			control.RetryDeadlineMS = deadlineMS
		}
	}
	if control.RetryAction == "" {
		control.RetryAction = brokerBridgeRetryActionNone
	}
	rt.controlSequence++
	control.Sequence = rt.controlSequence
	control.Reason = reason
	return control
}

func (rt *videoBrokerRuntime) queueBridgeControlLocked(reason string) (bridgeRuntimeControl, bool) {
	if rt.childControl == nil || !rt.childControlSupported {
		return bridgeRuntimeControl{}, false
	}
	return rt.queueBridgeControlForceLocked(reason)
}

func (rt *videoBrokerRuntime) queueBridgeControlForceLocked(reason string) (bridgeRuntimeControl, bool) {
	if rt.childControl == nil {
		return bridgeRuntimeControl{}, false
	}
	control := rt.effectiveBridgeControlLocked(reason)
	select {
	case rt.childControl <- control:
		return control, true
	default:
		select {
		case <-rt.childControl:
		default:
		}
		select {
		case rt.childControl <- control:
			return control, true
		default:
			return control, false
		}
	}
}

func (rt *videoBrokerRuntime) resetBridgeControlReadyLocked() {
	rt.bridgeControlReady = brokerBridgeControlReadyObservation{}
	rt.signalBridgeControlReadyChangedLocked()
}

func (rt *videoBrokerRuntime) signalBridgeControlReadyChangedLocked() {
	if rt.bridgeControlReadyChanged != nil {
		close(rt.bridgeControlReadyChanged)
	}
	rt.bridgeControlReadyChanged = make(chan struct{})
}

func (rt *videoBrokerRuntime) noteBridgeControlReadyLocked(
	mediaEpoch uint64,
	childPID int,
	ready brokerBridgeControlReadyTelemetry,
	now time.Time,
) bool {
	if mediaEpoch == 0 || childPID <= 0 || ready.Sequence == 0 || ready.EncoderEpoch == 0 ||
		rt.mediaEpoch != mediaEpoch || !rt.childRunning || brokerChildPID(rt.childCmd) != childPID ||
		ready.Sequence > rt.controlSequence {
		return false
	}
	previous := rt.bridgeControlReady
	if previous.MediaEpoch == mediaEpoch && previous.ChildPID == childPID &&
		previous.Sequence >= ready.Sequence {
		return false
	}
	rt.bridgeControlReady = brokerBridgeControlReadyObservation{
		brokerBridgeControlReadyTelemetry: ready,
		MediaEpoch:                        mediaEpoch,
		ChildPID:                          childPID,
		ObservedAt:                        now.UTC(),
	}
	rt.signalBridgeControlReadyChangedLocked()
	return true
}

func (rt *videoBrokerRuntime) waitBridgeControlReady(
	ctx context.Context,
	mediaEpoch uint64,
	childPID int,
	expected bridgeRuntimeControl,
) (brokerBridgeControlReadyObservation, error) {
	if expected.Sequence == 0 || mediaEpoch == 0 || childPID <= 0 {
		return brokerBridgeControlReadyObservation{}, errors.New("invalid bridge control readiness identity")
	}
	for {
		rt.mu.Lock()
		if rt.mediaEpoch != mediaEpoch || !rt.childRunning || brokerChildPID(rt.childCmd) != childPID {
			rt.mu.Unlock()
			return brokerBridgeControlReadyObservation{}, errors.New("capture child changed before bridge control became ready")
		}
		observed := rt.bridgeControlReady
		if observed.MediaEpoch == mediaEpoch && observed.ChildPID == childPID {
			if observed.Sequence > expected.Sequence {
				rt.mu.Unlock()
				return brokerBridgeControlReadyObservation{}, fmt.Errorf(
					"bridge control sequence %d was superseded by %d", expected.Sequence, observed.Sequence)
			}
			if observed.Sequence == expected.Sequence {
				rt.mu.Unlock()
				if observed.MainActive != expected.MainActive || observed.MainFPS != expected.MainFPS ||
					observed.MainBitrateKbps != expected.MainBitrateKbps || observed.SubFPS != expected.SubFPS {
					return brokerBridgeControlReadyObservation{}, fmt.Errorf(
						"bridge control ready mismatch sequence=%d expected_active=%t expected_fps=%d expected_kbps=%d expected_sub_fps=%d observed_active=%t observed_fps=%d observed_kbps=%d observed_sub_fps=%d",
						expected.Sequence, expected.MainActive, expected.MainFPS, expected.MainBitrateKbps, expected.SubFPS,
						observed.MainActive, observed.MainFPS, observed.MainBitrateKbps, observed.SubFPS)
				}
				return observed, nil
			}
		}
		changed := rt.bridgeControlReadyChanged
		if changed == nil {
			changed = make(chan struct{})
			rt.bridgeControlReadyChanged = changed
		}
		rt.mu.Unlock()

		select {
		case <-ctx.Done():
			return brokerBridgeControlReadyObservation{}, ctx.Err()
		case <-changed:
		}
	}
}

func (rt *videoBrokerRuntime) writeBridgeControls(
	ctx context.Context,
	input io.WriteCloser,
	controls <-chan bridgeRuntimeControl,
	fanout <-chan workerFanoutWrite,
) {
	defer input.Close()
	for {
		select {
		case <-ctx.Done():
			return
		case control, ok := <-controls:
			if !ok {
				return
			}
			retryComponent := control.RetryComponent
			if retryComponent == "" {
				retryComponent = "none"
			}
			retryAction := control.RetryAction
			if retryAction == "" {
				retryAction = brokerBridgeRetryActionNone
			}
			line := fmt.Sprintf(
				"%s seq=%d main_active=%d main_fps=%d main_bitrate_kbps=%d sub_fps=%d optional_compute_admitted=%d request_keyframe_token=%d retry_action=%s retry_component=%s retry_token=%d retry_deadline_unix_ms=%d\n",
				brokerBridgeControlProtocol,
				control.Sequence,
				boolToInt(control.MainActive),
				control.MainFPS,
				control.MainBitrateKbps,
				control.SubFPS,
				boolToInt(control.OptionalComputeAdmitted),
				control.RequestKeyframeToken,
				retryAction,
				retryComponent,
				control.RetryToken,
				control.RetryDeadlineMS,
			)
			if _, err := io.WriteString(input, line); err != nil && ctx.Err() == nil {
				log.Printf("[video-broker] bridge control write failed seq=%d reason=%s err=%v", control.Sequence, control.Reason, err)
				return
			}
		case request, ok := <-fanout:
			if !ok {
				fanout = nil
				continue
			}
			err := writeWorkerFanoutCommand(input, request.command)
			request.written <- err
			if err != nil && ctx.Err() == nil {
				log.Printf(
					"[video-broker] worker fan-out write failed transaction_id=%s sequence=%d phase=%s err=%v",
					request.command.TransactionID, request.command.Sequence, request.command.Phase, err)
				return
			}
		}
	}
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (rt *videoBrokerRuntime) watchMainClientControl(clientID int, conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 128), brokerControlMaxLineBytes)
	for scanner.Scan() {
		rt.mu.Lock()
		control, err := parseVideoMainClientControl(scanner.Text(), rt.plan)
		if err != nil {
			rt.mu.Unlock()
			log.Printf("[video-broker] rejected live client control client_id=%d err=%v", clientID, err)
			continue
		}
		client, ok := rt.mainClients[clientID]
		if ok && client.conn == conn {
			previousControl := rt.bridgeControlTargetsLocked()
			keyframeRequested := control.RequestKeyframeToken > client.mainControl.RequestKeyframeToken
			changed := client.mainControl.TargetFPS != control.TargetFPS ||
				client.mainControl.TargetBitrateKbps != control.TargetBitrateKbps
			client.mainControl = control
			client.mainControlUser = true
			if keyframeRequested {
				rt.mainKeyframeRequestToken++
				if rt.mainKeyframeRequestToken == 0 {
					rt.mainKeyframeRequestToken++
				}
			}
			if changed || keyframeRequested {
				currentControl := rt.bridgeControlTargetsLocked()
				if rt.childControlSupported && liveEncoderActivationChanged(previousControl, currentControl) {
					rt.mainAwaitingEncoderEpoch = true
					rt.clearMainBootstrapLocked()
				}
				reason := "live_consumer_adaptation"
				if keyframeRequested && !changed {
					reason = "live_consumer_keyframe"
				}
				rt.queueBridgeControlLocked(reason)
			}
		}
		rt.mu.Unlock()
	}
	rt.mu.Lock()
	rt.detachClientLocked(brokerStreamH264, clientID)
	rt.mu.Unlock()
}

func (rt *videoBrokerRuntime) runResourceGovernor(ctx context.Context) {
	rt.mu.Lock()
	poll := time.Duration(positiveOr(rt.plan.ResourceGovernorPollSec, 5)) * time.Second
	rt.mu.Unlock()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	var lastBPUAt time.Time
	var lastBPU *int
	observe := func() {
		zones := readThermalZonesCelsius()
		now := time.Now().UTC()
		if lastBPUAt.IsZero() || now.Sub(lastBPUAt) >= brokerResourceGovernorBPUPollEvery {
			lastBPU = readBPUUtilizationPercent(ctx)
			lastBPUAt = now
		}
		rt.mu.Lock()
		plan := rt.plan
		previous := rt.governor
		previousControl := rt.bridgeControlTargetsLocked()
		decision := decideResourceGovernor(plan, previous, zones, lastBPU, now)
		changed := decision.Mode != previous.Mode || decision.MainFPSCap != previous.MainFPSCap ||
			decision.MainBitrateKbpsCap != previous.MainBitrateKbpsCap || decision.SubFPS != previous.SubFPS ||
			decision.OptionalComputeAdmitted != previous.OptionalComputeAdmitted
		rt.governor = decision
		currentControl := rt.bridgeControlTargetsLocked()
		if rt.childControlSupported && liveEncoderActivationChanged(previousControl, currentControl) {
			rt.mainAwaitingEncoderEpoch = len(rt.mainClients) > 0
			rt.clearMainBootstrapLocked()
		}
		if changed {
			rt.queueBridgeControlLocked("resource_governor_" + decision.Mode)
			softC, hardC := resourceGovernorThermalLimits(plan)
			log.Printf(
				"[video-broker][resource-governor] mode=%s reason=%s max_temp_c=%.1f thermal_soft_c=%d thermal_hard_c=%d bpu_percent=%s live_fps_cap=%d live_kbps_cap=%d analysis_fps_cap=%d optional_compute_admitted=%t archive_protected=true",
				decision.Mode,
				decision.Reason,
				decision.MaxTemperatureCelsius,
				softC,
				hardC,
				optionalIntText(decision.BPUUtilizationPercent),
				decision.MainFPSCap,
				decision.MainBitrateKbpsCap,
				decision.SubFPS,
				decision.OptionalComputeAdmitted,
			)
		}
		state := rt.resourceGovernorStateLocked()
		rt.mu.Unlock()
		if err := writeResourceGovernorState(plan.ResourceGovernorStatePath, state); err != nil {
			log.Printf("[video-broker][resource-governor] state publish failed path=%s err=%v", plan.ResourceGovernorStatePath, err)
		}
	}
	observe()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			observe()
		}
	}
}

func decideResourceGovernor(plan videoBrokerPlan, previous resourceGovernorDecision, zones map[string]float64, bpu *int, now time.Time) resourceGovernorDecision {
	decision := nominalResourceGovernorDecision(plan)
	decision.ObservedAt = now
	decision.ThermalZonesCelsius = zones
	decision.BPUUtilizationPercent = cloneOptionalInt(bpu)
	for _, value := range zones {
		if value > decision.MaxTemperatureCelsius {
			decision.MaxTemperatureCelsius = value
		}
	}
	softC, hardC := resourceGovernorThermalLimits(plan)
	soft := float64(softC)
	hard := float64(hardC)
	bpuThreshold := positiveOr(plan.ResourceGovernorBPU, 90)
	hardThermal := decision.MaxTemperatureCelsius >= hard
	softThermal := decision.MaxTemperatureCelsius >= soft
	if previous.Mode == "thermal_hard" && decision.MaxTemperatureCelsius >= hard-3 {
		hardThermal = true
	}
	if strings.HasPrefix(previous.Mode, "thermal_") && decision.MaxTemperatureCelsius >= soft-3 {
		softThermal = true
	}
	bpuPressure := bpu != nil && *bpu >= bpuThreshold
	if previous.Mode == "bpu_pressure" && bpu != nil && *bpu >= maxIntDevices(1, bpuThreshold-15) {
		bpuPressure = true
	}
	baseFPS := positiveOr(plan.MainFPS, 15)
	baseBitrate := positiveOr(plan.LiveBitrateKbps, 4096)
	baseSubFPS := positiveOr(plan.SubFPS, 15)
	switch {
	case hardThermal:
		decision.Mode = "thermal_hard"
		decision.Reason = "thermal_hard_limit"
		decision.MainFPSCap = maxIntDevices(8, baseFPS/2)
		decision.MainBitrateKbpsCap = maxIntDevices(500, baseBitrate*45/100)
		decision.SubFPS = maxIntDevices(10, baseSubFPS/2)
		decision.OptionalComputeAdmitted = false
	case softThermal:
		decision.Mode = "thermal_soft"
		decision.Reason = "thermal_soft_limit"
		decision.MainFPSCap = maxIntDevices(10, baseFPS*2/3)
		decision.MainBitrateKbpsCap = maxIntDevices(700, baseBitrate*60/100)
		decision.SubFPS = maxIntDevices(10, baseSubFPS*2/3)
		decision.OptionalComputeAdmitted = false
	case bpuPressure:
		decision.Mode = "bpu_pressure"
		decision.Reason = "bpu_capacity_limit"
		decision.SubFPS = maxIntDevices(10, baseSubFPS*3/4)
		decision.OptionalComputeAdmitted = false
	}
	return decision
}

func resourceGovernorThermalLimits(plan videoBrokerPlan) (int, int) {
	softC := positiveOr(plan.ResourceGovernorSoftC, brokerResourceGovernorSoftDefaultC)
	if softC > brokerResourceGovernorSoftMaxC {
		softC = brokerResourceGovernorSoftMaxC
	}
	hardC := positiveOr(plan.ResourceGovernorHardC, brokerResourceGovernorHardDefaultC)
	if hardC > brokerResourceGovernorHardMaxC {
		hardC = brokerResourceGovernorHardMaxC
	}
	soft := float64(softC)
	hard := float64(hardC)
	if hard <= soft {
		hardC = softC + 2
	}
	return softC, hardC
}

func maxIntDevices(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func readThermalZonesCelsius() map[string]float64 {
	paths, _ := filepath.Glob("/sys/class/thermal/thermal_zone*/temp")
	sort.Strings(paths)
	zones := make(map[string]float64)
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(string(raw)), 64)
		if err != nil || value <= 0 {
			continue
		}
		if value > 1000 {
			value /= 1000
		}
		name := filepath.Base(filepath.Dir(path))
		if typeRaw, err := os.ReadFile(filepath.Join(filepath.Dir(path), "type")); err == nil {
			if candidate := strings.TrimSpace(string(typeRaw)); candidate != "" {
				name = candidate
			}
		}
		zones[name] = value
	}
	return zones
}

func readBPUUtilizationPercent(parent context.Context) *int {
	path, err := exec.LookPath("hrut_somstatus")
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, brokerResourceGovernorCommandTimeout)
	defer cancel()
	output, err := brokerExecCommandContext(ctx, path).CombinedOutput()
	if err != nil && len(output) == 0 {
		return nil
	}
	return parseBPUUtilizationPercent(output)
}

func parseBPUUtilizationPercent(output []byte) *int {
	matches := brokerBPUStatusPattern.FindAllSubmatch(output, -1)
	if len(matches) == 0 {
		return nil
	}
	maximum := 0
	valid := false
	for _, match := range matches {
		value, err := strconv.Atoi(string(match[1]))
		if err != nil || value < 0 || value > 100 {
			continue
		}
		valid = true
		if value > maximum {
			maximum = value
		}
	}
	if !valid {
		return nil
	}
	return &maximum
}

func (rt *videoBrokerRuntime) resourceGovernorStateLocked() resourceGovernorState {
	control := rt.bridgeControlTargetsLocked()
	decision := rt.governor
	captureCodec := fallbackBrokerTelemetry(rt.observedCaptureCodec)
	archiveCodec := fallbackBrokerTelemetry(rt.observedArchiveCodec)
	archiveDelivery := fallbackBrokerTelemetry(rt.observedArchiveDelivery)
	liveControlMode := fallbackBrokerTelemetry(rt.childLiveControlMode)
	captureFPS := rt.observedCaptureFPS
	archiveFPS := rt.observedArchiveFPS
	capabilityAttested := rt.resourceGovernorCapabilityAttestedLocked()
	liveFPS := control.MainFPS
	liveBitrate := control.MainBitrateKbps
	effectiveCaptureFPS := captureFPS
	effectiveArchiveFPS := archiveFPS
	effectiveAnalysisFPS := 0
	if rt.plan.AnalysisEnabled {
		effectiveAnalysisFPS = control.SubFPS
	}
	effectiveLiveActive := control.MainActive && capabilityAttested &&
		(!rt.childControlSupported || (rt.observedLiveEpoch > 0 && !rt.mainAwaitingEncoderEpoch))
	effectiveOptionalComputeAdmitted := control.OptionalComputeAdmitted && capabilityAttested
	stateMode := decision.Mode
	stateReason := decision.Reason
	if !capabilityAttested {
		effectiveCaptureFPS = 0
		effectiveArchiveFPS = 0
		effectiveAnalysisFPS = 0
		stateMode = "initializing"
		stateReason = "media_capability_unattested"
	}
	if !effectiveLiveActive {
		liveFPS = 0
		liveBitrate = 0
	}
	thermalZones := decision.ThermalZonesCelsius
	if thermalZones == nil {
		thermalZones = map[string]float64{}
	}
	return resourceGovernorState{
		Schema:           brokerResourceGovernorSchema,
		UpdatedAt:        time.Now().UTC(),
		Mode:             stateMode,
		Reason:           stateReason,
		ArchiveProtected: true,
		Priorities:       []string{"capture", "archive", "analysis", "live", "optional_compute"},
		Inputs: resourceGovernorStateInputs{
			ThermalZonesCelsius:   thermalZones,
			MaxTemperatureCelsius: decision.MaxTemperatureCelsius,
			BPUUtilizationPercent: cloneOptionalInt(decision.BPUUtilizationPercent),
			LiveClients:           len(rt.mainClients),
		},
		Media: resourceGovernorStateMedia{
			CaptureCodec:       captureCodec,
			CaptureWidth:       rt.observedCaptureWidth,
			CaptureHeight:      rt.observedCaptureHeight,
			CaptureFPS:         captureFPS,
			ArchiveCodec:       archiveCodec,
			ArchiveDelivery:    archiveDelivery,
			ArchiveFPS:         archiveFPS,
			LiveControlMode:    liveControlMode,
			LiveRuntimeControl: rt.childControlSupported,
			LiveEncoderEpoch:   rt.observedLiveEpoch,
		},
		Effective: resourceGovernorStateOutputs{
			CaptureFPS:              effectiveCaptureFPS,
			ArchiveFPS:              effectiveArchiveFPS,
			LiveActive:              effectiveLiveActive,
			LiveFPS:                 liveFPS,
			LiveBitrateKbps:         liveBitrate,
			AnalysisFPS:             effectiveAnalysisFPS,
			OptionalComputeAdmitted: effectiveOptionalComputeAdmitted,
		},
	}
}

// resourceGovernorCapabilityAttestedLocked validates only the telemetry owned
// by the roles enabled on this source graph. Requiring archive or live evidence
// from an analysis-only source leaves a healthy independent graph permanently
// in "initializing" and disables its governed analysis budget.
func (rt *videoBrokerRuntime) resourceGovernorCapabilityAttestedLocked() bool {
	if !rt.childRunning || rt.observedCaptureFPS <= 0 || strings.TrimSpace(rt.observedCaptureCodec) == "" {
		return false
	}
	if rt.plan.ArchiveEnabled && (rt.observedArchiveFPS <= 0 ||
		strings.TrimSpace(rt.observedArchiveCodec) == "" ||
		strings.TrimSpace(rt.observedArchiveDelivery) == "") {
		return false
	}
	if rt.plan.LiveEnabled && strings.TrimSpace(rt.childLiveControlMode) == "" {
		return false
	}
	return true
}

func resetResourceGovernorStateFile(plan videoBrokerPlan) {
	if !plan.ResourceGovernorEnabled {
		return
	}
	path := strings.TrimSpace(plan.ResourceGovernorStatePath)
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("[video-broker][resource-governor] state reset failed path=%s err=%v", path, err)
	}
}

func fallbackBrokerTelemetry(value string) string {
	if strings.TrimSpace(value) == "" {
		return "unknown"
	}
	return value
}

func writeResourceGovernorState(path string, state resourceGovernorState) error {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("invalid governor state path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), ".resource-governor-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o640); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func cloneOptionalInt(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func optionalIntText(value *int) string {
	if value == nil {
		return "unavailable"
	}
	return strconv.Itoa(*value)
}
