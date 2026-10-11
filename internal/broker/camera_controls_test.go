package broker

import (
	"context"
	"errors"
	"fmt"
	"github.com/Josepavese/videosluice/internal/cameracontrol"
	"reflect"
	"strings"
	"testing"
)

func TestParseVideoCaptureControlCapabilities(t *testing.T) {
	output := []byte(`
User Controls

                     brightness 0x00980900 (int)    : min=0 max=255 step=1 default=128 value=128
        white_balance_automatic 0x0098090c (bool)   : default=1 value=1
      white_balance_temperature 0x0098091a (int)    : min=0 max=255 step=1 default=128 value=128 flags=inactive, volatile

Camera Controls

                  auto_exposure 0x009a0901 (menu)   : min=0 max=3 default=0 value=0 (Auto Mode)
				0: Auto Mode
				1: Manual Mode
`)

	got := cameracontrol.ParseV4L2Capabilities(output)
	for _, want := range []string{"auto_exposure", "brightness", "white_balance_automatic"} {
		if _, ok := got[want]; !ok {
			t.Fatalf("missing parsed control %q in %#v", want, got)
		}
	}
	brightness := got["brightness"]
	if !brightness.HasMin || brightness.Min != 0 || !brightness.HasMax || brightness.Max != 255 || !brightness.HasDefault || brightness.Default != 128 || !brightness.HasCurrent || brightness.Current != 128 {
		t.Fatalf("brightness capability metadata not parsed: %#v", brightness)
	}
	if _, ok := got["0:"]; ok {
		t.Fatalf("menu entry parsed as a control: %#v", got)
	}
	if got["white_balance_temperature"].Writable() {
		t.Fatalf("inactive control parsed as writable: %#v", got["white_balance_temperature"])
	}
}

func TestResolveVideoCaptureControlProfileAutoPerformsNoUVCControlIO(t *testing.T) {
	opt := engineConfig{
		VideoCaptureDevice:           "/dev/video0",
		VideoCaptureControlProfile:   "auto",
		CameraControlOptimizerPolicy: "auto",
		VideoTransport:               "v4l2_uvc",
		VideoCaptureControls: map[string]int{
			"brightness": 0,
			"contrast":   32,
			"saturation": 72,
			"sharpness":  4,
		},
		CameraControlOptimizerControlMax:   map[string]int{"brightness": 99, "gain": 10},
		CameraControlOptimizerControlSteps: map[string]int{"brightness": 8, "gain": 5},
	}
	called := false
	run := func(_ context.Context, args ...string) ([]byte, error) {
		called = true
		return nil, fmt.Errorf("unexpected UVC control I/O: %v", args)
	}

	got := resolveVideoCaptureControlProfileWithCommand(context.Background(), opt, run)
	if called || got.VideoCaptureControlProfile != "auto" || len(got.VideoCaptureControls) != 0 ||
		len(got.CameraControlOptimizerControlMin) != 0 || len(got.CameraControlOptimizerControlMax) != 0 ||
		len(got.CameraControlOptimizerControlSteps) != 0 {
		t.Fatalf("auto profile must be control-I/O free and clear device-specific state: called=%t got=%+v", called, got)
	}
}

func TestResolveVideoCaptureControlProfileAutoLeavesCameraOptimizerOff(t *testing.T) {
	opt := engineConfig{
		VideoCaptureDevice:           "/dev/video0",
		VideoCaptureControlProfile:   "auto",
		CameraControlOptimizerPolicy: "auto",
		VideoTransport:               "v4l2_uvc",
		Enabled:                      true,
	}
	got := resolveCameraControlOptimizerRuntime(resolveVideoCaptureControlProfileWithCommand(context.Background(), opt, func(context.Context, ...string) ([]byte, error) {
		t.Fatal("auto profile attempted UVC control I/O")
		return nil, nil
	}))
	if got.CameraControlOptimizerPolicy != "off" || len(got.VideoCaptureControls) != 0 {
		t.Fatalf("UVC auto profile must leave ISP ownership to the camera: %+v", got)
	}
}

func TestDeriveSemanticVideoCaptureControlsSelectsFixedApertureAutomaticMode(t *testing.T) {
	capabilities := cameracontrol.ParseV4L2Capabilities([]byte(`
auto_exposure 0x009a0901 (menu) : min=0 max=3 value=2
`))
	got := cameracontrol.DeriveNeutralControls(capabilities)
	if got["auto_exposure"] != 3 {
		t.Fatalf("standard fixed-aperture menu should select aperture-priority automatic mode: %#v", got)
	}
}

func TestDeriveSemanticVideoCaptureControlsPreservesDefaultWithWritableIris(t *testing.T) {
	capabilities := cameracontrol.ParseV4L2Capabilities([]byte(`
auto_exposure 0x009a0901 (menu) : min=0 max=3 default=0 value=0
iris_absolute 0x009a0911 (int) : min=0 max=255 step=1 default=128 value=128
`))
	got := cameracontrol.DeriveNeutralControls(capabilities)
	if got["auto_exposure"] != 0 {
		t.Fatalf("camera with a real writable iris should preserve its declared full-auto default: %#v", got)
	}
}

func TestDeriveSemanticVideoCaptureControlsIsRangeIndependent(t *testing.T) {
	capabilities := cameracontrol.ParseV4L2Capabilities([]byte(`
brightness 0x00980900 (int) : min=-64 max=64 step=1 default=0 value=-40
contrast 0x00980901 (int) : min=0 max=100 step=1 default=50 value=10
white_balance_automatic 0x0098090c (bool) : default=0 value=0
auto_exposure 0x009a0901 (menu) : min=1 max=8 default=4 value=1
`))
	got := cameracontrol.DeriveNeutralControls(capabilities)
	want := map[string]int{
		"brightness": 0, "contrast": 50, "white_balance_automatic": 1, "auto_exposure": 4,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("semantic defaults should follow device capability scale: got=%#v want=%#v", got, want)
	}
}

func TestDeriveSemanticVideoCaptureControlsSkipsUnavailableControls(t *testing.T) {
	capabilities := cameracontrol.ParseV4L2Capabilities([]byte(`
brightness 0x00980900 (int) : min=0 max=255 step=1 default=128 value=128
white_balance_temperature 0x0098091a (int) : min=0 max=255 step=1 default=128 value=128 flags=inactive
auto_exposure 0x009a0901 (menu) : min=0 max=3 default=0 value=0 flags=read-only
`))
	got := cameracontrol.DeriveNeutralControls(capabilities)
	want := map[string]int{"brightness": 128}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unavailable controls must not be selected: got=%#v want=%#v", got, want)
	}
}

func TestDeriveCameraControlOptimizerEnvelopeUsesCurrentCapabilityScale(t *testing.T) {
	capabilities := cameracontrol.ParseV4L2Capabilities([]byte(`
brightness 0x00980900 (int) : min=-64 max=64 step=2 default=0 value=0
gain 0x00980913 (int) : min=0 max=4095 step=32 default=64 value=64
gamma 0x00980910 (int) : min=72 max=500 step=1 default=100 value=100 flags=inactive
auto_exposure 0x009a0901 (menu) : min=0 max=3 step=1 default=3 value=3
`))
	envelopes := cameracontrol.DeriveResidualEnvelopes(capabilities)
	wantSteps := map[string]int{"brightness": 2, "gain": 64}
	gotSteps := map[string]int{}
	for name, envelope := range envelopes {
		gotSteps[name] = envelope.Step
	}
	if !reflect.DeepEqual(gotSteps, wantSteps) {
		t.Fatalf("unexpected device-derived CameraControl steps: got=%#v want=%#v", gotSteps, wantSteps)
	}
	if !cameracontrol.HasWritableResidual(capabilities) {
		t.Fatal("expected writable residual luma capability")
	}
}

func TestResolveVideoCaptureControlProfileConfiguredBypassesAutodetection(t *testing.T) {
	opt := engineConfig{
		VideoCaptureDevice:         "/dev/video0",
		VideoCaptureControlProfile: "configured",
		VideoCaptureControls:       map[string]int{"contrast": 77},
	}
	called := false
	got := resolveVideoCaptureControlProfileWithCommand(context.Background(), opt, func(context.Context, ...string) ([]byte, error) {
		called = true
		return nil, nil
	})
	if called || !reflect.DeepEqual(got.VideoCaptureControls, opt.VideoCaptureControls) {
		t.Fatalf("configured profile should bypass detection: called=%t got=%#v", called, got.VideoCaptureControls)
	}
}

func TestApplyVideoCaptureControlsAbortsAtFirstSetFailure(t *testing.T) {
	controls := normalizeVideoCaptureControls(map[string]int{
		"auto_exposure": 3,
		"brightness":    0,
		"saturation":    72,
	})
	var calls []string
	run := func(_ context.Context, args ...string) ([]byte, error) {
		call := strings.Join(args, " ")
		calls = append(calls, call)
		if strings.Contains(call, "--list-ctrls") {
			return []byte(`
brightness 0x00980900 (int) : min=0 max=255 step=1 default=128 value=128
saturation 0x00980902 (int) : min=0 max=255 step=1 default=128 value=128
white_balance_temperature 0x0098091a (int) : min=0 max=255 step=1 default=128 value=128 flags=inactive
auto_exposure 0x009a0901 (menu) : min=0 max=3 default=0 value=0
`), nil
		}
		if strings.Contains(call, "brightness=0") {
			return []byte("temporary control failure"), errors.New("exit status 1")
		}
		return nil, nil
	}

	err := applyVideoCaptureControlsWithCommand(context.Background(), "/dev/video0", controls, "test", run)
	if err == nil || !strings.Contains(err.Error(), "control=brightness") {
		t.Fatalf("expected fail-closed first SET error, got %v", err)
	}

	want := []string{
		"-d /dev/video0 --list-ctrls",
		"-d /dev/video0 --set-ctrl=auto_exposure=3",
		"-d /dev/video0 --set-ctrl=brightness=0",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("unexpected capability-aware calls:\n got: %#v\nwant: %#v", calls, want)
	}
}

func TestApplyVideoCaptureControlsRejectsUnavailableProfileBeforeWriting(t *testing.T) {
	controls := normalizeVideoCaptureControls(map[string]int{
		"brightness":                128,
		"white_balance_temperature": 128,
	})
	var calls []string
	run := func(_ context.Context, args ...string) ([]byte, error) {
		call := strings.Join(args, " ")
		calls = append(calls, call)
		if strings.Contains(call, "--list-ctrls") {
			return []byte(`
brightness 0x00980900 (int) : min=0 max=255 step=1 default=128 value=128
white_balance_temperature 0x0098091a (int) : min=0 max=255 step=1 default=128 value=128 flags=inactive
`), nil
		}
		return nil, nil
	}

	err := applyVideoCaptureControlsWithCommand(context.Background(), "/dev/video0", controls, "test", run)
	if err == nil || !strings.Contains(err.Error(), "not writable") {
		t.Fatalf("expected unavailable control rejection, got %v", err)
	}
	want := []string{
		"-d /dev/video0 --list-ctrls",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("invalid explicit profile must not write any control: got=%#v want=%#v", calls, want)
	}
}

func TestApplyVideoCaptureControlsVerifiesConfiguredProfileOnce(t *testing.T) {
	controls := normalizeVideoCaptureControls(map[string]int{
		"auto_exposure": 3,
		"brightness":    0,
		"contrast":      32,
	})
	values := map[string]int{"auto_exposure": 0, "brightness": 128, "contrast": 32}
	var calls []string
	run := func(_ context.Context, args ...string) ([]byte, error) {
		call := strings.Join(args, " ")
		calls = append(calls, call)
		if strings.Contains(call, "--set-ctrl=") {
			assignment := strings.TrimPrefix(args[len(args)-1], "--set-ctrl=")
			name, raw, _ := strings.Cut(assignment, "=")
			var value int
			if _, err := fmt.Sscanf(raw, "%d", &value); err != nil {
				t.Fatalf("parse assignment %q: %v", assignment, err)
			}
			values[name] = value
			return nil, nil
		}
		return []byte(fmt.Sprintf(`
auto_exposure 0x009a0901 (menu) : min=0 max=3 default=0 value=%d
brightness 0x00980900 (int) : min=0 max=255 step=1 default=128 value=%d
contrast 0x00980901 (int) : min=0 max=255 step=1 default=128 value=%d
`, values["auto_exposure"], values["brightness"], values["contrast"])), nil
	}

	if err := applyVideoCaptureControlsWithCommand(context.Background(), "/dev/video0", controls, "pre_stream", run); err != nil {
		t.Fatalf("configured pre-stream profile failed: %v", err)
	}
	want := []string{
		"-d /dev/video0 --list-ctrls",
		"-d /dev/video0 --set-ctrl=auto_exposure=3",
		"-d /dev/video0 --set-ctrl=brightness=0",
		"-d /dev/video0 --set-ctrl=contrast=32",
		"-d /dev/video0 --list-ctrls",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("configured controls must be applied and verified exactly once before streaming: got=%#v want=%#v", calls, want)
	}
}

func TestStartVideoCaptureControlApplierIsNoopWithoutExplicitControls(t *testing.T) {
	if err := startVideoCaptureControlApplier(context.Background(), engineConfig{
		Enabled:                    true,
		VideoCaptureDevice:         "/dev/video0",
		VideoCaptureControlProfile: "auto",
	}); err != nil {
		t.Fatalf("control-free auto profile failed: %v", err)
	}
}
