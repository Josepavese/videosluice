package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfig() Config {
	return Config{
		SchemaVersion: SchemaVersion,
		Sources: map[string]SourceConfig{
			"front": {
				Identity: "front-camera", Transport: "v4l2_uvc", Device: "/dev/video0", Format: "h265",
				Width: 1920, Height: 1080, FPS: 30,
				Acceleration: AccelerationConfig{EncodeProvider: "native_sp", DecodeProvider: "native_sp"},
			},
		},
		Streams: map[string]StreamConfig{
			"live": {
				Type: "live", SourceRef: "front", Socket: "/run/video-broker/live.sock",
				Codec: "h265", Width: 1920, Height: 1080, FPS: 20,
			},
			"analysis": {
				Type: "analysis", SourceRef: "front", SharedMemory: "/run/video-broker/analysis.nv12",
				PixelFormat: "nv12", Width: 640, Height: 360, FPS: 20,
			},
			"archive": {
				Type: "archive", SourceRef: "front", Socket: "/run/video-broker/archive.sock",
				Codec: "h265", Width: 1920, Height: 1080, FPS: 30,
			},
		},
		Runtime: RuntimeConfig{
			StateDirectory: "/var/lib/video-broker", RuntimeDirectory: "/run/video-broker",
		},
	}
}

func TestNormalizeAppliesStableDefaultsPerSourceAndStream(t *testing.T) {
	input := validConfig()
	live := input.Streams["live"]
	live.Codec = ""
	input.Streams["live"] = live
	cfg, err := Normalize(input)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Streams["live"].Codec != "h265" || cfg.Streams["live"].Content != "processed" || cfg.Streams["archive"].Content != "adaptive" {
		t.Fatalf("unexpected stream defaults: %+v", cfg.Streams)
	}
	if cfg.Streams["live"].RateControl != "cbr" || cfg.Streams["archive"].RateControl != "cbr" {
		t.Fatalf("unexpected rate-control defaults: %+v", cfg.Streams)
	}
	source := cfg.Sources["front"]
	if source.Processing.ToneMapping.Mode != "auto" || source.Processing.ToneMapping.Accelerator != "auto" || !source.NoiseGuardEnabled() {
		t.Fatalf("unexpected tone mapping defaults: %+v", source.Processing.ToneMapping)
	}
	if source.Processing.CameraControl.Policy != "auto" || source.Processing.CameraControl.Mode != "off" {
		t.Fatalf("unexpected camera-control defaults: %+v", source.Processing.CameraControl)
	}
}

func TestCloneDetachesEveryMutableConfigField(t *testing.T) {
	original := validConfig()
	noiseGuard := true
	source := original.Sources["front"]
	source.Controls = map[string]int{"exposure_absolute": 17}
	source.Processing.ToneMapping.NoiseGuard = &noiseGuard
	source.Processing.CameraControl.ControlMin = map[string]int{"gain": 1}
	source.Processing.CameraControl.ControlMax = map[string]int{"gain": 9}
	source.Processing.CameraControl.ControlSteps = map[string]int{"gain": 2}
	original.Sources["front"] = source

	cloned := Clone(original)
	delete(cloned.Streams, "archive")
	clonedSource := cloned.Sources["front"]
	clonedSource.Controls["exposure_absolute"] = 1
	clonedSource.Processing.CameraControl.ControlMin["gain"] = 2
	clonedSource.Processing.CameraControl.ControlMax["gain"] = 8
	clonedSource.Processing.CameraControl.ControlSteps["gain"] = 3
	*clonedSource.Processing.ToneMapping.NoiseGuard = false
	cloned.Sources["front"] = clonedSource

	originalSource := original.Sources["front"]
	if _, exists := original.Streams["archive"]; !exists ||
		originalSource.Controls["exposure_absolute"] != 17 ||
		originalSource.Processing.CameraControl.ControlMin["gain"] != 1 ||
		originalSource.Processing.CameraControl.ControlMax["gain"] != 9 ||
		originalSource.Processing.CameraControl.ControlSteps["gain"] != 2 ||
		!*originalSource.Processing.ToneMapping.NoiseGuard {
		t.Fatalf("config clone retained mutable aliases: %+v", original)
	}
}

func TestNormalizeAcceptsIndependentLowerAVBRArchiveCadence(t *testing.T) {
	cfg := validConfig()
	archive := cfg.Streams["archive"]
	archive.Width = 640
	archive.Height = 360
	archive.FPS = 15
	archive.BitrateKbps = 750
	archive.RateControl = "AVBR"
	cfg.Streams["archive"] = archive
	normalized, err := Normalize(cfg)
	if err != nil {
		t.Fatal(err)
	}
	got := normalized.Streams["archive"]
	if got.FPS != 15 || got.RateControl != "avbr" || got.BitrateKbps != 750 {
		t.Fatalf("archive target was not preserved: %+v", got)
	}
}

func TestAdaptiveToneProfileReplacesStaleMaterializedTuningPerSource(t *testing.T) {
	cfg := validConfig()
	source := cfg.Sources["front"]
	source.Processing.ToneMapping = ToneMappingConfig{
		Profile: "adaptive", TargetLuma: 96, Hysteresis: 4,
		StatisticsHz: 2, MaxShadowGain: 1.8,
	}
	cfg.Sources["front"] = source
	normalized, err := Normalize(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tone := normalized.Sources["front"].Processing.ToneMapping
	if tone.TargetLuma != 128 || tone.Hysteresis != 12 || tone.StatisticsHz != 8 || tone.MaxShadowGain != 2.4 {
		t.Fatalf("adaptive profile retained stale values: %+v", tone)
	}
}

func TestSourceGraphsBindEveryRoleExplicitlyAndDeterministically(t *testing.T) {
	cfg := validConfig()
	side := cfg.Sources["front"]
	side.Identity = "side-camera"
	side.Device = "/dev/video2"
	cfg.Sources["side"] = side
	analysis := cfg.Streams["analysis"]
	analysis.SourceRef = "side"
	cfg.Streams["analysis"] = analysis

	normalized, err := Normalize(cfg)
	if err != nil {
		t.Fatal(err)
	}
	graphs := normalized.SourceGraphs()
	if len(graphs) != 2 || graphs[0].SourceID != "front" || graphs[1].SourceID != "side" {
		t.Fatalf("graphs are not deterministic: %+v", graphs)
	}
	if graphs[0].SourceCount != 2 || graphs[1].SourceCount != 2 {
		t.Fatalf("source count was not propagated to graph adapters: %+v", graphs)
	}
	if graphs[0].Live == nil || graphs[0].Archive == nil || graphs[0].Analysis != nil {
		t.Fatalf("front graph roles are incorrect: %+v", graphs[0])
	}
	if graphs[1].Analysis == nil || graphs[1].Live != nil || graphs[1].Archive != nil {
		t.Fatalf("side graph roles are incorrect: %+v", graphs[1])
	}
	if graphs[1].Analysis.Config.SourceRef != "side" {
		t.Fatalf("analysis source binding was lost: %+v", graphs[1].Analysis)
	}
}

func TestNormalizeAcceptsLibcameraSource(t *testing.T) {
	cfg := validConfig()
	source := cfg.Sources["front"]
	source.Transport = "libcamera"
	source.Device = "/base/soc/i2c0mux/i2c@1/imx708@1a"
	source.Format = "nv12"
	source.Acceleration.EncodeProvider = "gstreamer_v4l2"
	source.Acceleration.DecodeProvider = "gstreamer_v4l2"
	cfg.Sources["front"] = source
	archive := cfg.Streams["archive"]
	archive.Codec = "h264"
	cfg.Streams["archive"] = archive
	if _, err := Normalize(cfg); err != nil {
		t.Fatalf("libcamera adapter rejected by product schema: %v", err)
	}
}

func TestValidateRejectsUnboundAmbiguousOrRoleInvalidGraphs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"missing source ref", func(cfg *Config) { s := cfg.Streams["analysis"]; s.SourceRef = ""; cfg.Streams["analysis"] = s }, "unknown source"},
		{"unknown source ref", func(cfg *Config) { s := cfg.Streams["analysis"]; s.SourceRef = "side"; cfg.Streams["analysis"] = s }, "unknown source"},
		{"duplicate physical source", func(cfg *Config) { s := cfg.Sources["front"]; s.Identity = "side-camera"; cfg.Sources["side"] = s }, "same physical source"},
		{"unreferenced source", func(cfg *Config) {
			s := cfg.Sources["front"]
			s.Identity = "side-camera"
			s.Device = "/dev/video2"
			cfg.Sources["side"] = s
		}, "has no bound streams"},
		{"duplicate source identity", func(cfg *Config) { s := cfg.Sources["front"]; s.Device = "/dev/video2"; cfg.Sources["side"] = s }, "same source identity"},
		{"duplicate role", func(cfg *Config) {
			s := cfg.Streams["live"]
			s.Socket = "/run/video-broker/live-two.sock"
			cfg.Streams["live_two"] = s
		}, "duplicate live role"},
		{"shared endpoint", func(cfg *Config) {
			s := cfg.Streams["archive"]
			s.Socket = cfg.Streams["live"].Socket
			cfg.Streams["archive"] = s
		}, "share endpoint"},
		{"invalid source id", func(cfg *Config) {
			cfg.Sources["Front Camera"] = cfg.Sources["front"]
			delete(cfg.Sources, "front")
			for id, s := range cfg.Streams {
				s.SourceRef = "Front Camera"
				cfg.Streams[id] = s
			}
		}, "invalid source id"},
		{"invalid stream id", func(cfg *Config) {
			cfg.Streams["Analysis Stream"] = cfg.Streams["analysis"]
			delete(cfg.Streams, "analysis")
		}, "invalid stream id"},
		{"unsupported role", func(cfg *Config) { s := cfg.Streams["analysis"]; s.Type = "preview"; cfg.Streams["analysis"] = s }, "unsupported type"},
		{"analysis encoded field", func(cfg *Config) { s := cfg.Streams["analysis"]; s.Codec = "h265"; cfg.Streams["analysis"] = s }, "encoded-only"},
		{"live codec alias", func(cfg *Config) { s := cfg.Streams["live"]; s.Codec = "hevc"; cfg.Streams["live"] = s }, "requires h265"},
		{"archive codec alias", func(cfg *Config) { s := cfg.Streams["archive"]; s.Codec = "hevc"; cfg.Streams["archive"] = s }, "unsupported codec"},
		{"source format alias", func(cfg *Config) { s := cfg.Sources["front"]; s.Format = "mjpg"; cfg.Sources["front"] = s }, "unsupported source"},
		{"source codec preference alias", func(cfg *Config) { s := cfg.Sources["front"]; s.CodecPreference = "hevc"; cfg.Sources["front"] = s }, "unsupported source"},
		{"live analysis field", func(cfg *Config) { s := cfg.Streams["live"]; s.PixelFormat = "nv12"; cfg.Streams["live"] = s }, "analysis-only"},
		{"live avbr", func(cfg *Config) { s := cfg.Streams["live"]; s.RateControl = "avbr"; cfg.Streams["live"] = s }, "rate_control must be cbr"},
		{"unknown archive rate control", func(cfg *Config) { s := cfg.Streams["archive"]; s.RateControl = "vbr"; cfg.Streams["archive"] = s }, "unsupported rate_control"},
		{"transport", func(cfg *Config) { s := cfg.Sources["front"]; s.Transport = "unknown"; cfg.Sources["front"] = s }, "unsupported source"},
		{"odd geometry", func(cfg *Config) { s := cfg.Streams["analysis"]; s.Width = 641; cfg.Streams["analysis"] = s }, "must be even"},
		{"software forbidden", func(cfg *Config) {
			s := cfg.Sources["front"]
			s.Acceleration.EncodeProvider = "software"
			s.Acceleration.DecodeProvider = "software"
			cfg.Sources["front"] = s
		}, "requires allow_software_fallback"},
		{"quality state missing", func(cfg *Config) {
			s := cfg.Sources["front"]
			s.Processing.Quality.Enabled = true
			cfg.Sources["front"] = s
		}, "quality state path"},
		{"governor state missing", func(cfg *Config) {
			cfg.Runtime.Governor.Enabled = true
			cfg.Runtime.Governor.StatePath = ""
		}, "resource governor state path"},
		{"shared source state", func(cfg *Config) {
			front := cfg.Sources["front"]
			front.Processing.Quality = QualityConfig{Enabled: true, StatePath: "/run/video-broker/quality.json", AdmissionPath: "/run/video-broker/admission.json"}
			cfg.Sources["front"] = front
			side := front
			side.Identity = "side-camera"
			side.Device = "/dev/video2"
			cfg.Sources["side"] = side
			analysis := cfg.Streams["analysis"]
			analysis.SourceRef = "side"
			cfg.Streams["analysis"] = analysis
		}, "share "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.mutate(&cfg)
			_, err := Normalize(cfg)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want substring %q", err, test.want)
			}
		})
	}
}

func TestNormalizePreservesQualityIntentWhenAnalysisMoves(t *testing.T) {
	cfg := validConfig()
	front := cfg.Sources["front"]
	front.Processing.Quality = QualityConfig{
		Enabled: true, StatePath: "/run/video-broker/front-quality.json",
		AdmissionPath: "/run/video-broker/front-admission.json",
	}
	cfg.Sources["front"] = front
	side := front
	side.Identity, side.Device = "side-camera", "/dev/video2"
	side.Processing.Quality = QualityConfig{}
	cfg.Sources["side"] = side
	analysis := cfg.Streams["analysis"]
	analysis.SourceRef = "side"
	cfg.Streams["analysis"] = analysis
	got, err := Normalize(cfg)
	if err != nil {
		t.Fatalf("quality intent must not pin the analysis route: %v", err)
	}
	if !got.Sources["front"].Processing.Quality.Enabled || got.Streams["analysis"].SourceRef != "side" {
		t.Fatal("normalization changed saved source intent or stream selection")
	}
	for _, graph := range got.SourceGraphs() {
		if graph.SourceID == "front" && graph.Analysis != nil {
			t.Fatal("normalization inserted a hidden analysis branch")
		}
	}
}

func TestLoadRejectsSchemaV2AndLegacyFieldsWithoutFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	for _, body := range []string{
		`{"sources":{},"streams":{},"runtime":{}}`,
		`{"schema_version":2,"sources":{},"streams":{},"runtime":{}}`,
		`{"schema_version":3,"enabled":false,"sources":{},"streams":{},"runtime":{}}`,
		`{"schema_version":3,"source":{},"streams":{},"runtime":{}}`,
		`{"schema_version":3,"sources":{},"streams":{},"runtime":{}} {"schema_version":3}`,
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Fatalf("strict current-only loader accepted %q", body)
		}
	}
}

func TestStreamGovernorStatePathIsStablePerNamedStream(t *testing.T) {
	path, err := StreamGovernorStatePath(
		"/run/video-broker/resource-governor.json", "vision.primary",
	)
	if err != nil {
		t.Fatal(err)
	}
	if path != "/run/video-broker/streams/vision.primary/resource-governor.json" {
		t.Fatalf("stream governor path=%q", path)
	}
	for _, test := range []struct {
		base, stream string
	}{
		{"resource-governor.json", "analysis"},
		{"/run/video-broker/../governor.json", "analysis"},
		{"/run/video-broker/governor.json", "Analysis Stream"},
	} {
		if _, err := StreamGovernorStatePath(test.base, test.stream); err == nil {
			t.Fatalf("unsafe stream governor path accepted base=%q stream=%q", test.base, test.stream)
		}
	}
}

func TestPublishedExamplesValidate(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "examples", "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("published examples unavailable paths=%v err=%v", paths, err)
	}
	for _, relative := range paths {
		path, err := filepath.Abs(relative)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(filepath.Base(path), func(t *testing.T) {
			if _, err := Load(path); err != nil {
				t.Fatalf("example does not satisfy product schema: %v", err)
			}
		})
	}
}
