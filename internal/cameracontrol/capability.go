// Package cameracontrol models camera control capabilities independently from
// the capture backend and from any device inventory.
package cameracontrol

import (
	"sort"
	"strconv"
	"strings"
)

type Capability struct {
	Type       string
	Flags      map[string]bool
	Min        int
	Max        int
	Step       int
	Default    int
	Current    int
	HasMin     bool
	HasMax     bool
	HasStep    bool
	HasDefault bool
	HasCurrent bool
}

func (capability Capability) Writable() bool {
	for _, flag := range []string{"inactive", "disabled", "grabbed", "read-only"} {
		if capability.Flags[flag] {
			return false
		}
	}
	return true
}

// ParseV4L2Capabilities is the Linux UVC/V4L2 adapter. All policy above this
// function operates on Capability and remains independent from v4l2-ctl text.
func ParseV4L2Capabilities(output []byte) map[string]Capability {
	controls := make(map[string]Capability)
	for _, line := range strings.Split(string(output), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || !strings.HasPrefix(fields[1], "0x") {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(fields[0]))
		if name == "" {
			continue
		}
		capability := Capability{Flags: make(map[string]bool)}
		capability.Type = strings.Trim(fields[2], "()")
		if flagOffset := strings.Index(line, "flags="); flagOffset >= 0 {
			flagText := strings.ReplaceAll(line[flagOffset+len("flags="):], ",", " ")
			for _, flag := range strings.Fields(flagText) {
				capability.Flags[strings.ToLower(strings.TrimSpace(flag))] = true
			}
		}
		for _, field := range fields[2:] {
			key, raw, ok := strings.Cut(field, "=")
			if !ok {
				continue
			}
			value, err := strconv.Atoi(strings.TrimRight(raw, ",)"))
			if err != nil {
				continue
			}
			switch key {
			case "min":
				capability.Min, capability.HasMin = value, true
			case "max":
				capability.Max, capability.HasMax = value, true
			case "step":
				capability.Step, capability.HasStep = value, true
			case "default":
				capability.Default, capability.HasDefault = value, true
			case "value":
				capability.Current, capability.HasCurrent = value, true
			}
		}
		controls[name] = capability
	}
	return controls
}

var residualNames = []string{"gamma", "brightness", "backlight_compensation", "gain"}

func ResidualNames() []string {
	return append([]string(nil), residualNames...)
}

func HasWritableResidual(capabilities map[string]Capability) bool {
	for _, name := range residualNames {
		capability, ok := capabilities[name]
		if ok && residualEligible(capability) {
			return true
		}
	}
	return false
}

type Envelope struct {
	Minimum  int
	Baseline int
	Maximum  int
	Step     int
}

// DeriveResidualEnvelopes maps device-specific integer ranges into a bounded
// residual domain. Automatic exposure remains camera-owned; the host may move
// at most ten percent of the device range around its own neutral/default value
// and must be able to unwind back to that value after a lighting transition.
func DeriveResidualEnvelopes(capabilities map[string]Capability) map[string]Envelope {
	envelopes := make(map[string]Envelope)
	for _, name := range residualNames {
		capability, ok := capabilities[name]
		if !ok || !residualEligible(capability) {
			continue
		}
		baseline := capability.Current
		if capability.HasDefault {
			baseline = capability.Default
		}
		baseline = clamp(baseline, capability.Min, capability.Max)
		unit := 1
		if capability.HasStep && capability.Step > 0 {
			unit = capability.Step
		}
		rangeSize := capability.Max - capability.Min
		step := alignUp(max(1, (rangeSize+99)/100), unit)
		headroom := alignUp(max(unit, (rangeSize+9)/10), unit)
		envelopes[name] = Envelope{
			Minimum: clamp(baseline-headroom, capability.Min, baseline), Baseline: baseline,
			Maximum: clamp(baseline+headroom, baseline, capability.Max),
			Step:    step,
		}
	}
	return envelopes
}

func DeriveNeutralControls(capabilities map[string]Capability) map[string]int {
	controls := make(map[string]int)
	for _, name := range []string{
		"brightness", "contrast", "saturation", "sharpness", "gain", "gamma",
		"backlight_compensation", "power_line_frequency", "white_balance_temperature",
	} {
		capability, ok := capabilities[name]
		if !ok || !capability.Writable() {
			continue
		}
		if capability.HasDefault {
			controls[name] = capability.Default
		} else if capability.HasCurrent {
			controls[name] = capability.Current
		}
	}
	for _, name := range []string{"white_balance_automatic", "exposure_dynamic_framerate"} {
		capability, ok := capabilities[name]
		if !ok || !capability.Writable() {
			continue
		}
		if capability.Type == "bool" || capability.HasMax && capability.Max >= 1 {
			controls[name] = 1
		} else if capability.HasDefault {
			controls[name] = capability.Default
		}
	}
	if capability, ok := capabilities["auto_exposure"]; ok && capability.Writable() {
		if preferred, ok := preferredAutomaticExposureValue(capabilities); ok {
			controls["auto_exposure"] = preferred
		} else if capability.HasDefault {
			controls["auto_exposure"] = capability.Default
		} else if capability.HasCurrent {
			controls["auto_exposure"] = capability.Current
		}
	}
	return controls
}

func Names(values map[string]int) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func preferredAutomaticExposureValue(capabilities map[string]Capability) (int, bool) {
	exposure, ok := capabilities["auto_exposure"]
	if !ok || exposure.Type != "menu" || !exposure.HasMin || !exposure.HasMax || exposure.Min != 0 || exposure.Max < 3 {
		return 0, false
	}
	for _, irisName := range []string{"iris_absolute", "iris_relative"} {
		if iris, exists := capabilities[irisName]; exists && iris.Writable() {
			return 0, false
		}
	}
	return 3, true
}

func residualEligible(capability Capability) bool {
	return capability.Writable() && capability.HasMin && capability.HasMax && capability.Max > capability.Min &&
		(capability.HasDefault || capability.HasCurrent)
}

func alignUp(value, unit int) int {
	if unit <= 1 {
		return value
	}
	if remainder := value % unit; remainder != 0 {
		value += unit - remainder
	}
	return value
}

func clamp(value, minimum, maximum int) int {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}
