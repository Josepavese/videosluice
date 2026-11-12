package cameracontrol

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// Linux's V4L2_CID_FOCUS_AUTO. Use its stable ID so tool/driver display-name
// changes cannot select a different control. No unrelated GET_CUR is issued.
const v4l2ContinuousFocusID = "0x009a090c"
const continuousFocusEnabled = 1

// EnableContinuousFocus performs one bounded pre-stream transaction. It is
// called only by the source owner; an already enabled camera is never written.
// Unsupported is distinct from failed and readback proves mode, not optical focus.
func (backend *V4L2Backend) EnableContinuousFocus(ctx context.Context, device string) (string, error) {
	if backend == nil || backend.run == nil || strings.TrimSpace(device) == "" {
		return "failed", fmt.Errorf("autofocus backend or device missing")
	}
	read := func() (int, bool, error) {
		output, err := backend.run(ctx, "-d", device, "--get-ctrl="+v4l2ContinuousFocusID)
		if strings.TrimSpace(string(output)) == "unknown control '"+v4l2ContinuousFocusID+"'" {
			return 0, false, nil
		}
		if err != nil {
			return 0, true, commandError("read autofocus", err, output)
		}
		_, raw, ok := strings.Cut(strings.TrimSpace(string(output)), ":")
		value, parseErr := strconv.Atoi(strings.TrimSpace(raw))
		if !ok || parseErr != nil || value < 0 || value > continuousFocusEnabled {
			return 0, true, fmt.Errorf("invalid autofocus readback")
		}
		return value, true, nil
	}
	value, supported, err := read()
	if err != nil {
		return "failed", err
	}
	if !supported {
		return "unsupported", nil
	}
	if value == continuousFocusEnabled {
		return "already_enabled", nil
	}
	output, err := backend.run(ctx, "-d", device, "--set-ctrl="+v4l2ContinuousFocusID+"="+strconv.Itoa(continuousFocusEnabled))
	if err != nil {
		return "failed", commandError("enable autofocus", err, output)
	}
	value, supported, err = read()
	if err != nil {
		return "failed", err
	}
	if !supported || value != continuousFocusEnabled {
		return "failed", fmt.Errorf("autofocus readback did not confirm continuous mode")
	}
	return "enabled", nil
}
