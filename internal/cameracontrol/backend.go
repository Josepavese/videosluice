package cameracontrol

import (
	"context"
	"fmt"
	"strings"
)

// Backend is the hardware-facing camera-control boundary. Policy code consumes
// normalized capabilities and never depends on a board, a device inventory or
// a command-line tool.
type Backend interface {
	Name() string
	Capabilities(context.Context, string) (map[string]Capability, error)
	Set(context.Context, string, string, int) error
}

// Command is the narrow process adapter used by the V4L2 implementation. It is
// injectable so capability discovery and control policy remain unit-testable
// without camera hardware.
type Command func(context.Context, ...string) ([]byte, error)

type V4L2Backend struct {
	run Command
}

func NewV4L2Backend(run Command) *V4L2Backend {
	return &V4L2Backend{run: run}
}

func (backend *V4L2Backend) Name() string { return "v4l2" }

func (backend *V4L2Backend) Capabilities(ctx context.Context, device string) (map[string]Capability, error) {
	if backend == nil || backend.run == nil {
		return nil, fmt.Errorf("v4l2 camera-control backend has no command runner")
	}
	device = strings.TrimSpace(device)
	if device == "" {
		return nil, fmt.Errorf("v4l2 camera-control backend received an empty device")
	}
	output, err := backend.run(ctx, "-d", device, "--list-ctrls")
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
		return nil, commandError("discover capabilities", err, output)
	}
	capabilities := ParseV4L2Capabilities(output)
	if len(capabilities) == 0 {
		return nil, fmt.Errorf("v4l2 camera-control discovery returned no capabilities")
	}
	return capabilities, nil
}

func (backend *V4L2Backend) Set(ctx context.Context, device string, name string, value int) error {
	if backend == nil || backend.run == nil {
		return fmt.Errorf("v4l2 camera-control backend has no command runner")
	}
	device = strings.TrimSpace(device)
	name = strings.ToLower(strings.TrimSpace(name))
	if device == "" {
		return fmt.Errorf("v4l2 camera-control backend received an empty device")
	}
	if !validControlName(name) {
		return fmt.Errorf("invalid camera-control name %q", name)
	}
	output, err := backend.run(ctx, "-d", device, fmt.Sprintf("--set-ctrl=%s=%d", name, value))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		}
		return commandError("set "+name, err, output)
	}
	return nil
}

func validControlName(name string) bool {
	if name == "" {
		return false
	}
	for _, char := range name {
		if char != '_' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func commandError(action string, err error, output []byte) error {
	detail := strings.TrimSpace(string(output))
	if detail == "" {
		return fmt.Errorf("v4l2 camera-control %s failed: %w", action, err)
	}
	return fmt.Errorf("v4l2 camera-control %s failed: %w: %s", action, err, detail)
}
