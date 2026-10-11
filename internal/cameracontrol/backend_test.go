package cameracontrol

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestV4L2BackendNormalizesDiscoveryAndWritesControls(t *testing.T) {
	var calls [][]string
	backend := NewV4L2Backend(func(_ context.Context, args ...string) ([]byte, error) {
		calls = append(calls, append([]string(nil), args...))
		if args[len(args)-1] == "--list-ctrls" {
			return []byte(`brightness 0x00980900 (int) : min=0 max=255 step=1 default=128 value=129`), nil
		}
		return nil, nil
	})

	capabilities, err := backend.Capabilities(context.Background(), "/dev/video7")
	if err != nil {
		t.Fatal(err)
	}
	if got := capabilities["brightness"].Current; got != 129 {
		t.Fatalf("brightness current=%d want=129", got)
	}
	if err := backend.Set(context.Background(), "/dev/video7", "Brightness", 128); err != nil {
		t.Fatal(err)
	}
	want := [][]string{
		{"-d", "/dev/video7", "--list-ctrls"},
		{"-d", "/dev/video7", "--set-ctrl=brightness=128"},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls=%#v want=%#v", calls, want)
	}
}

func TestV4L2BackendPreservesDeviceErrorWithoutLeakingThroughAShell(t *testing.T) {
	backend := NewV4L2Backend(func(_ context.Context, args ...string) ([]byte, error) {
		return []byte("control rejected"), errors.New("exit status 1")
	})
	if err := backend.Set(context.Background(), "/dev/video0", "brightness;reboot", 1); err == nil || !strings.Contains(err.Error(), "invalid camera-control name") {
		t.Fatalf("invalid name error=%v", err)
	}
	err := backend.Set(context.Background(), "/dev/video0", "brightness", 1)
	if err == nil || !strings.Contains(err.Error(), "control rejected") {
		t.Fatalf("device error=%v", err)
	}
}

func TestV4L2BackendRejectsMissingAndEmptyDiscovery(t *testing.T) {
	if _, err := NewV4L2Backend(nil).Capabilities(context.Background(), "/dev/video0"); err == nil {
		t.Fatal("missing runner accepted")
	}
	backend := NewV4L2Backend(func(context.Context, ...string) ([]byte, error) { return []byte("noise"), nil })
	if _, err := backend.Capabilities(context.Background(), "/dev/video0"); err == nil {
		t.Fatal("empty normalized discovery accepted")
	}
}
