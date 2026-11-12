package main

import (
	"context"
	"log"
	"os/exec"
	"regexp"
	"time"

	"github.com/Josepavese/videosluice/internal/cameracontrol"
)

const autofocusSetupTimeout = 2 * time.Second
const libcameraContinuousFocusProperty = "af-mode=2" // libcamera AfModeContinuous

func initializeV4L2Autofocus(ctx context.Context, device string) {
	ctx, cancel := context.WithTimeout(ctx, autofocusSetupTimeout)
	defer cancel()
	backend := cameracontrol.NewV4L2Backend(func(ctx context.Context, args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, "v4l2-ctl", args...).CombinedOutput()
	})
	state, err := backend.EnableContinuousFocus(ctx, device)
	log.Printf("[camera-autofocus] backend=v4l2 state=%s error=%v", state, err)
}

func discoverLibcameraAutofocus(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, autofocusSetupTimeout)
	defer cancel()
	output, err := exec.CommandContext(ctx, "gst-inspect-1.0", "libcamerasrc").Output()
	if err != nil || !hasLibcameraAutofocus(output) {
		log.Printf("[camera-autofocus] backend=libcamera state=unavailable reason=af_mode_property_missing error=%v", err)
		return ""
	}
	// libcamerasrc filters generic controls against the selected camera's
	// capabilities at acquisition. No-AF cameras ignore AfMode, without failing
	// capture. This is a request; only camera metadata can attest actual focus.
	log.Printf("[camera-autofocus] backend=libcamera state=requested mode=continuous")
	return libcameraContinuousFocusProperty
}

func hasLibcameraAutofocus(output []byte) bool {
	return regexp.MustCompile(`(?m)^\s+af-mode\s+:`).Match(output)
}
