package cameracontrol

import (
	"reflect"
	"testing"
)

func TestParseAndDeriveModuleIndependentProfile(t *testing.T) {
	capabilities := ParseV4L2Capabilities([]byte(`
brightness 0x00980900 (int) : min=0 max=255 step=1 default=128 value=0
gain 0x00980913 (int) : min=0 max=100 step=1 default=5 value=0
white_balance_automatic 0x0098090c (bool) : default=1 value=1
auto_exposure 0x009a0901 (menu) : min=0 max=3 default=0 value=3
white_balance_temperature 0x0098091a (int) : min=0 max=255 step=1 default=128 value=128 flags=inactive
`))
	wantControls := map[string]int{
		"auto_exposure": 3, "brightness": 128, "gain": 5, "white_balance_automatic": 1,
	}
	if got := DeriveNeutralControls(capabilities); !reflect.DeepEqual(got, wantControls) {
		t.Fatalf("neutral profile=%#v want=%#v", got, wantControls)
	}
	envelopes := DeriveResidualEnvelopes(capabilities)
	if got := envelopes["brightness"]; got != (Envelope{Minimum: 102, Baseline: 128, Maximum: 154, Step: 3}) {
		t.Fatalf("brightness envelope=%+v", got)
	}
	if got := envelopes["gain"]; got != (Envelope{Minimum: 0, Baseline: 5, Maximum: 15, Step: 1}) {
		t.Fatalf("gain envelope=%+v", got)
	}
	if _, ok := envelopes["white_balance_temperature"]; ok {
		t.Fatal("inactive control received an envelope")
	}
}

func TestEnvelopeFollowsLargeSteppedRange(t *testing.T) {
	capabilities := ParseV4L2Capabilities([]byte(`
gain 0x00980913 (int) : min=0 max=4095 step=32 default=64 value=64
`))
	if got := DeriveResidualEnvelopes(capabilities)["gain"]; got != (Envelope{Minimum: 0, Baseline: 64, Maximum: 480, Step: 64}) {
		t.Fatalf("stepped envelope=%+v", got)
	}
}
