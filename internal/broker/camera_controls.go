package broker

import (
	"context"
	"fmt"
	"github.com/Josepavese/videosluice/internal/cameracontrol"
	videoproviders "github.com/Josepavese/videosluice/internal/source"
	"log"
	"os/exec"
	"sort"
	"strings"
	"time"
)

type videoCaptureControl struct {
	name  string
	value int
}

type videoCaptureControlCommand func(context.Context, ...string) ([]byte, error)

func runVideoCaptureControlCommand(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "v4l2-ctl", args...).CombinedOutput()
}

func videoCaptureControlProfileNames() []string {
	return []string{"auto", "configured"}
}

func isAllowedVideoCaptureControlProfile(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, candidate := range videoCaptureControlProfileNames() {
		if value == candidate {
			return true
		}
	}
	return false
}

func isAllowedVideoCaptureControl(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "brightness",
		"contrast",
		"saturation",
		"hue",
		"white_balance_automatic",
		"white_balance_temperature",
		"gamma",
		"gain",
		"power_line_frequency",
		"sharpness",
		"backlight_compensation",
		"auto_exposure",
		"exposure_time_absolute",
		"exposure_dynamic_framerate":
		return true
	default:
		return false
	}
}

func resolveVideoCaptureControlProfile(ctx context.Context, opt engineConfig) engineConfig {
	return resolveVideoCaptureControlProfileWithBackend(ctx, opt, newV4L2CameraControlBackend())
}

func resolveVideoCaptureControlProfileWithCommand(
	ctx context.Context,
	opt engineConfig,
	run videoCaptureControlCommand,
) engineConfig {
	if run == nil {
		return resolveVideoCaptureControlProfileWithBackend(ctx, opt, nil)
	}
	return resolveVideoCaptureControlProfileWithBackend(ctx, opt, cameracontrol.NewV4L2Backend(cameracontrol.Command(run)))
}

func resolveVideoCaptureControlProfileWithBackend(
	ctx context.Context,
	opt engineConfig,
	backend cameracontrol.Backend,
) engineConfig {
	_ = ctx
	_ = backend
	requested := strings.ToLower(strings.TrimSpace(opt.VideoCaptureControlProfile))
	if requested == "" {
		requested = "auto"
	}
	if requested == "configured" {
		log.Printf("[video-broker][camera-controls] using explicitly configured raw control values")
		return opt
	}
	transport := strings.ToLower(strings.TrimSpace(opt.VideoTransport))
	if transport != videoproviders.TransportV4L2UVC && transport != videoproviders.TransportV4L2 {
		return opt
	}
	// UVC control transactions are not a portable ISP contract. Several USB
	// modules keep streaming after attachment but wedge their control endpoint
	// (and eventually STREAMON) after otherwise valid GET_CUR/SET_CUR requests.
	// Auto therefore delegates exposure/white-balance entirely to the module and
	// performs no control-endpoint I/O. Image improvement remains in the broker's
	// post-ISP tone pipeline, which is independent from UVC firmware controls.
	opt.VideoCaptureControlProfile = "auto"
	opt.VideoCaptureControls = nil
	opt.CameraControlOptimizerControlMin = nil
	opt.CameraControlOptimizerControlMax = nil
	opt.CameraControlOptimizerControlSteps = nil
	log.Printf("[video-broker][camera-controls] selected profile=auto ownership=camera action=no_uvc_control_io")
	return opt
}

func startVideoCaptureControlApplier(ctx context.Context, opt engineConfig) error {
	controls := normalizeVideoCaptureControls(opt.VideoCaptureControls)
	if len(controls) == 0 {
		return nil
	}
	device := strings.TrimSpace(opt.VideoCaptureDevice)
	if device == "" {
		return fmt.Errorf("configured camera controls require video_capture_device")
	}
	return applyVideoCaptureControls(ctx, device, controls, "pre_stream")
}

func normalizeVideoCaptureControls(values map[string]int) []videoCaptureControl {
	if len(values) == 0 {
		return nil
	}
	controls := make([]videoCaptureControl, 0, len(values))
	for name, value := range values {
		name = strings.ToLower(strings.TrimSpace(name))
		if name == "" || !isAllowedVideoCaptureControl(name) {
			continue
		}
		controls = append(controls, videoCaptureControl{name: name, value: value})
	}
	sort.Slice(controls, func(i, j int) bool {
		return controls[i].name < controls[j].name
	})
	return controls
}

func applyVideoCaptureControls(ctx context.Context, device string, controls []videoCaptureControl, reason string) error {
	return applyVideoCaptureControlsWithBackend(ctx, device, controls, reason, newV4L2CameraControlBackend())
}

func applyVideoCaptureControlsWithCommand(
	ctx context.Context,
	device string,
	controls []videoCaptureControl,
	reason string,
	run videoCaptureControlCommand,
) error {
	var backend cameracontrol.Backend
	if run != nil {
		backend = cameracontrol.NewV4L2Backend(cameracontrol.Command(run))
	}
	return applyVideoCaptureControlsWithBackend(ctx, device, controls, reason, backend)
}

func applyVideoCaptureControlsWithBackend(
	ctx context.Context,
	device string,
	controls []videoCaptureControl,
	reason string,
	backend cameracontrol.Backend,
) error {
	if len(controls) == 0 {
		return nil
	}
	if backend == nil {
		return fmt.Errorf("camera control apply device=%s reason=%s: missing backend", device, reason)
	}

	probeCtx, probeCancel := context.WithTimeout(ctx, 2*time.Second)
	supported, probeErr := backend.Capabilities(probeCtx, device)
	probeCancel()
	if probeErr != nil {
		return fmt.Errorf("camera control capability probe device=%s reason=%s backend=%s: %w", device, reason, backend.Name(), probeErr)
	}
	if len(supported) == 0 {
		return fmt.Errorf("camera control capability probe device=%s reason=%s backend=%s: no controls returned", device, reason, backend.Name())
	}

	// Validate the whole explicit profile before the first SET_CUR. A camera
	// replacement must never receive a partially compatible calibration.
	for _, control := range controls {
		capability, ok := supported[control.name]
		if !ok {
			return fmt.Errorf("camera control %s is unsupported by %s", control.name, device)
		}
		if !capability.Writable() {
			return fmt.Errorf("camera control %s is not writable on %s", control.name, device)
		}
		if capability.HasMin && control.value < capability.Min || capability.HasMax && control.value > capability.Max {
			return fmt.Errorf("camera control %s value=%d is outside device range", control.name, control.value)
		}
	}

	applied := make([]string, 0, len(controls))
	for _, control := range controls {
		cmdCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := backend.Set(cmdCtx, device, control.name, control.value)
		cancel()
		if err != nil {
			return fmt.Errorf("camera control apply device=%s reason=%s backend=%s control=%s: %w", device, reason, backend.Name(), control.name, err)
		}
		applied = append(applied, control.name)
	}
	verifyCtx, verifyCancel := context.WithTimeout(ctx, 2*time.Second)
	observed, verifyErr := backend.Capabilities(verifyCtx, device)
	verifyCancel()
	if verifyErr != nil {
		return fmt.Errorf("camera control verification device=%s reason=%s backend=%s: %w", device, reason, backend.Name(), verifyErr)
	}
	for _, control := range controls {
		capability, ok := observed[control.name]
		if !ok || !capability.HasCurrent {
			return fmt.Errorf("camera control verification unavailable control=%s device=%s", control.name, device)
		}
		if capability.Current != control.value {
			return fmt.Errorf("camera control verification drift control=%s requested=%d observed=%d", control.name, control.value, capability.Current)
		}
	}
	log.Printf("[video-broker][camera-controls] apply complete device=%s reason=%s applied=%s verified=true", device, reason, strings.Join(applied, ","))
	return nil
}

func newV4L2CameraControlBackend() cameracontrol.Backend {
	return cameracontrol.NewV4L2Backend(cameracontrol.Command(runVideoCaptureControlCommand))
}
