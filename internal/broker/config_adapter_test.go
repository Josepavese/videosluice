package broker

import (
	"reflect"
	"testing"

	productconfig "github.com/Josepavese/videosluice/config"
)

func TestNormalizeQualityLifecyclePreservesSourcePolicyAndHardwareControls(t *testing.T) {
	for _, transport := range []string{"v4l2_uvc", "v4l2", "hbn", "libcamera"} {
		t.Run(transport, func(t *testing.T) {
			graph := productconfig.SourceGraph{
				SourceID: "overview", SourceCount: 2,
				Source: productconfig.SourceConfig{
					Identity: "overview-camera", Transport: transport,
					Controls:     map[string]int{"exposure_auto": 3},
					Acceleration: productconfig.AccelerationConfig{RequireHardwareCodec: true, DecodeProvider: "native_sp", EncodeProvider: "native_sp"},
					Processing: productconfig.ProcessingConfig{
						Quality:       productconfig.QualityConfig{Enabled: true, StatePath: "/run/video/overview/quality.json", AdmissionPath: "/run/video/overview/admission.json"},
						CameraControl: productconfig.CameraControlOptimizerConfig{Policy: "auto", Mode: "auto"},
					},
				},
				Analysis: &productconfig.NamedStream{ID: "analysis", Config: productconfig.StreamConfig{Type: "analysis", Width: 640, Height: 360, FPS: 20}},
				Runtime:  productconfig.RuntimeConfig{Governor: productconfig.GovernorConfig{Enabled: true, ThermalSoftC: 90, ThermalHardC: 95}},
			}
			savedSource, savedAnalysis := graph.Source, graph.Analysis
			bound := normalizeConfig(graph)
			if !bound.VideoQualityMonitorEnabled || bound.CameraControlOptimizerPolicy != "auto" {
				t.Fatal("bound analysis did not activate the configured sampling policy")
			}
			graph.Analysis = nil
			unbound := resolveCameraControlOptimizerRuntime(normalizeConfig(graph))
			if !unbound.VideoQualityMonitorEnabled || unbound.AnalysisEnabled {
				t.Fatalf("unbound analysis lost sampling intent or retained a hidden branch: quality=%t analysis=%t", unbound.VideoQualityMonitorEnabled, unbound.AnalysisEnabled)
			}
			if !reflect.DeepEqual(unbound.VideoCaptureControls, savedSource.Controls) || !unbound.VideoRequireHWCodec || unbound.VideoAllowSWFallback || unbound.VideoDecodeProvider != bound.VideoDecodeProvider || unbound.VideoEncodeProvider != bound.VideoEncodeProvider || !unbound.ResourceGovernorEnabled || unbound.ResourceGovernorThermalSoftC != 90 || unbound.ResourceGovernorThermalHardC != 95 {
				t.Fatal("switch changed explicit camera controls, hardware policy or thermal limits")
			}
			if !reflect.DeepEqual(graph.Source, savedSource) {
				t.Fatal("effective state mutated the saved source policy")
			}
			graph.Analysis = savedAnalysis
			if restored := normalizeConfig(graph); !reflect.DeepEqual(restored, bound) {
				t.Fatal("rebinding analysis did not restore the same effective source settings")
			}
			graph.Source.Processing.Quality.Enabled = false
			graph.Source.Processing.CameraControl.Policy = "off"
			if got := resolveCameraControlOptimizerRuntime(normalizeConfig(graph)); got.VideoQualityMonitorEnabled {
				t.Fatal("rebinding analysis enabled an explicitly disabled quality policy")
			}
		})
	}
}

func TestNormalizeConfigScopesManagerGovernorStateForMultipleSources(t *testing.T) {
	graph := productconfig.SourceGraph{
		SourceID: "side", SourceCount: 2,
		Source: productconfig.SourceConfig{Identity: "side-camera"},
		Runtime: productconfig.RuntimeConfig{
			StateDirectory: "/var/lib/video-broker",
			Governor: productconfig.GovernorConfig{
				Enabled: true, StatePath: "/run/video-broker/resource-governor.json",
			},
		},
	}
	got := normalizeConfig(graph)
	if got.StateDirectory != "/var/lib/video-broker/side" {
		t.Fatalf("source state root=%q", got.StateDirectory)
	}
	if got.SourceIdentity != "side-camera" {
		t.Fatalf("source identity=%q", got.SourceIdentity)
	}
	if got.ResourceGovernorStatePath != "/run/video-broker/sources/side/resource-governor.json" {
		t.Fatalf("governor state path=%q", got.ResourceGovernorStatePath)
	}

	graph.SourceCount = 1
	got = normalizeConfig(graph)
	if got.ResourceGovernorStatePath != "/run/video-broker/resource-governor.json" {
		t.Fatalf("single-source integration path changed: %q", got.ResourceGovernorStatePath)
	}
}
