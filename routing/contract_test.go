package routing

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Josepavese/videosluice/config"
)

const testRevision = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func testSource(id string) config.SourceConfig {
	return config.SourceConfig{
		Identity: "sha256:" + strings.Repeat(id[len(id)-1:], 64), Transport: "v4l2_uvc",
		Device: "/dev/camera/by-id/" + id, Format: "h265",
		CodecPreference: "h265", CodecPolicy: "required", ModePolicy: "exact",
		AspectPolicy: "crop", Width: 1920, Height: 1080, FPS: 30,
		Processing: config.ProcessingConfig{
			Orientation:   "normal",
			ToneMapping:   config.ToneMappingConfig{Profile: "adaptive", Mode: "off"},
			CameraControl: config.CameraControlOptimizerConfig{Policy: "off", Mode: "off"},
		},
		Acceleration: config.AccelerationConfig{
			EncodeProvider: "native_sp", DecodeProvider: "native_sp", RequireHardwareCodec: true,
			Discovery: config.CapabilityDiscoveryConfig{Mode: "auto", ProbeTimeoutMS: 8000},
		},
	}
}

func testCatalog(sourceCount int) Catalog {
	sources := make(map[string]config.SourceConfig, sourceCount)
	liveOptions := make(map[string]StreamOption, sourceCount)
	analysisOptions := make(map[string]StreamOption, sourceCount)
	archiveOptions := make(map[string]StreamOption, sourceCount)
	for index := 0; index < sourceCount; index++ {
		id := string(rune('a' + index))
		sourceID := "camera_" + id
		sources[sourceID] = testSource(sourceID)
		liveOptions["live_"+id] = StreamOption{SourceRef: sourceID, Codec: "h265", Width: 1280, Height: 720, FPS: 20, BitrateKbps: 4096}
		analysisOptions["analysis_"+id] = StreamOption{SourceRef: sourceID, PixelFormat: "nv12", Width: 640, Height: 360, FPS: 20}
		archiveOptions["archive_"+id] = StreamOption{SourceRef: sourceID, Codec: "h265", Width: 640, Height: 360, FPS: 15, BitrateKbps: 750, Content: "adaptive", RateControl: "avbr"}
	}
	return Catalog{
		Schema: CatalogSchema, Revision: testRevision, Sources: sources,
		Streams: map[string]StreamCatalog{
			"live":     {Type: "live", Socket: "/run/video/live.sock", Options: liveOptions},
			"analysis": {Type: "analysis", Socket: "/run/video/analysis.sock", SharedMemory: "/run/video/analysis.nv12", NotifySocket: "/run/video/analysis.notify.sock", Options: analysisOptions},
			"archive":  {Type: "archive", Socket: "/run/video/archive.sock", Options: archiveOptions},
		},
		DefaultSelections: map[string]string{"live": "live_a", "analysis": "analysis_a", "archive": "archive_a"},
		Runtime:           config.RuntimeConfig{RuntimeDirectory: "/run/video", StateDirectory: "/var/lib/video", Status: config.StatusConfig{Socket: "/run/video/status.sock"}},
	}
}

func testPlan(catalog Catalog, revision uint64, selections map[string]string) Plan {
	return Plan{
		Schema: PlanSchema, CatalogRevision: catalog.Revision, Revision: revision,
		Selections:        cloneSelections(selections),
		StreamGenerations: initialStreamGenerations(catalog.Streams),
	}
}

func TestCatalogMaterializesIndependentPlansForOneTwoThreeAndFiveSources(t *testing.T) {
	for _, count := range []int{1, 2, 3, 5} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			catalog := testCatalog(count)
			selected := map[string]string{"live": "live_a", "analysis": "analysis_a", "archive": "archive_a"}
			if count >= 2 {
				selected["analysis"] = "analysis_b"
			}
			if count >= 3 {
				selected["archive"] = "archive_c"
			}
			if count == 5 {
				selected["live"] = "live_e"
			}
			cfg, err := Materialize(catalog, testPlan(catalog, 7, selected))
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Streams["live"].SourceRef != catalog.Streams["live"].Options[selected["live"]].SourceRef ||
				cfg.Streams["analysis"].SourceRef != catalog.Streams["analysis"].Options[selected["analysis"]].SourceRef ||
				cfg.Streams["archive"].SourceRef != catalog.Streams["archive"].Options[selected["archive"]].SourceRef {
				t.Fatalf("materialized bindings differ from opaque selections: %+v", cfg.Streams)
			}
			for _, stream := range cfg.Streams {
				if _, exists := cfg.Sources[stream.SourceRef]; !exists {
					t.Fatalf("stream references source outside active graph: %+v", stream)
				}
			}
		})
	}
}

func TestAdmissionProfilesRetainInactiveSourcesAndAllCatalogAlternatives(t *testing.T) {
	catalog := testCatalog(2)
	catalog.Streams["live"].Options["live_a_high"] = StreamOption{
		SourceRef: "camera_a", Codec: "h265", Width: 1920, Height: 1080,
		FPS: 20, BitrateKbps: 6144,
	}
	profiles, err := AdmissionProfiles(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) != 2 || len(profiles["camera_a"]) != 4 || len(profiles["camera_b"]) != 3 {
		t.Fatalf("catalog admission profiles=%+v", profiles)
	}
	// camera_b is absent from the default materialized graph but must remain in
	// the immutable admission universe for a later source move.
	plan, err := DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Materialize(catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, active := cfg.Sources["camera_b"]; active {
		t.Fatal("inactive catalog source unexpectedly materialized into the active graph")
	}
	if profiles["camera_a"][0].Role != "analysis" ||
		profiles["camera_a"][1].Role != "archive" ||
		profiles["camera_a"][2].Role != "live" ||
		profiles["camera_a"][3].Role != "live" {
		t.Fatalf("profiles are not canonically sorted: %+v", profiles["camera_a"])
	}
	profiles["camera_a"][0].Width = 2
	fresh, err := AdmissionProfiles(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if fresh["camera_a"][0].Width == 2 {
		t.Fatal("admission profile result aliases catalog state")
	}
}

func TestRoutingPlanRejectsArbitraryValuesAndStalePolicy(t *testing.T) {
	catalog := testCatalog(2)
	valid := testPlan(catalog, 2, map[string]string{"live": "live_a", "analysis": "analysis_b", "archive": "archive_a"})
	for name, mutate := range map[string]func(*Plan){
		"legacy schema":      func(plan *Plan) { plan.Schema = "video-broker.routing-plan.v0" },
		"stale catalog":      func(plan *Plan) { plan.CatalogRevision = "sha256:" + strings.Repeat("b", 64) },
		"zero revision":      func(plan *Plan) { plan.Revision = 0 },
		"missing stream":     func(plan *Plan) { delete(plan.Selections, "analysis") },
		"unknown stream":     func(plan *Plan) { plan.Selections["preview"] = "live_a" },
		"unknown option":     func(plan *Plan) { plan.Selections["live"] = "raw_device_override" },
		"missing generation": func(plan *Plan) { delete(plan.StreamGenerations, "analysis") },
		"unknown generation": func(plan *Plan) { plan.StreamGenerations["preview"] = 1 },
		"zero generation":    func(plan *Plan) { plan.StreamGenerations["live"] = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			plan := valid
			plan.Selections = cloneSelections(valid.Selections)
			plan.StreamGenerations = cloneStreamGenerations(valid.StreamGenerations)
			mutate(&plan)
			if _, err := Materialize(catalog, plan); err == nil {
				t.Fatal("invalid routing plan accepted")
			}
		})
	}
}

func TestCatalogRejectsDuplicatePhysicalSourcesAndInvalidUnusedOptions(t *testing.T) {
	catalog := testCatalog(2)
	duplicate := catalog.Sources["camera_a"]
	duplicate.Identity = "sha256:" + strings.Repeat("b", 64)
	catalog.Sources["camera_b"] = duplicate
	if _, err := NormalizeCatalog(catalog); err == nil || !strings.Contains(err.Error(), "share a physical source") {
		t.Fatalf("duplicate physical source accepted: %v", err)
	}

	catalog = testCatalog(2)
	option := catalog.Streams["live"].Options["live_b"]
	option.Codec = "vp9"
	catalog.Streams["live"].Options["live_b"] = option
	if _, err := NormalizeCatalog(catalog); err == nil || !strings.Contains(err.Error(), "live_b") {
		t.Fatalf("invalid unused option accepted: %v", err)
	}
}

func TestCatalogRejectsDuplicateMaterializedOptions(t *testing.T) {
	catalog := testCatalog(1)
	live := catalog.Streams["live"]
	live.Options["same_graph"] = live.Options["live_a"]
	catalog.Streams["live"] = live
	if _, err := NormalizeCatalog(catalog); err == nil ||
		!strings.Contains(err.Error(), `options "live_a" and "same_graph" materialize the same graph`) {
		t.Fatalf("duplicate materialized option accepted: %v", err)
	}
}

func TestQualityPolicyDoesNotPinAnalysisToSource(t *testing.T) {
	catalog := testCatalog(2)
	catalog.Sources["camera_b"] = func() config.SourceConfig {
		source := catalog.Sources["camera_b"]
		source.Processing.Quality = config.QualityConfig{Enabled: true, StatePath: "/run/video/camera_b/quality.json", AdmissionPath: "/run/video/camera_b/admission.json"}
		return source
	}()
	plan := testPlan(catalog, 3, map[string]string{
		"live": "live_b", "analysis": "analysis_a", "archive": "archive_a",
	})
	materialized, err := Materialize(catalog, plan)
	if err != nil {
		t.Fatalf("saved sampling intent pinned the analysis route: %v", err)
	}
	if !materialized.Sources["camera_b"].Processing.Quality.Enabled || materialized.Streams["analysis"].SourceRef != "camera_a" {
		t.Fatal("materialization changed sampling intent or analysis selection")
	}
	broken, err := NormalizeCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	source := broken.Sources["camera_b"]
	source.Processing.Quality.StatePath = ""
	broken.Sources["camera_b"] = source
	if _, err := Materialize(broken, plan); err == nil {
		t.Fatal("inactive sampling intent bypassed source policy validation")
	}
}

func TestStrictLoadRejectsUnknownFieldsAndTrailingDocuments(t *testing.T) {
	directory := t.TempDir()
	catalog := testCatalog(1)
	body, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "catalog.json")
	if err := os.WriteFile(path, append(body, []byte(`{"extra":true}`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCatalog(path); err == nil {
		t.Fatal("multiple catalog documents accepted")
	}
	planPath := filepath.Join(directory, "plan.json")
	if err := os.WriteFile(planPath, []byte(`{"schema":"video-broker.routing-plan.v1","catalog_revision":"`+testRevision+`","revision":1,"selections":{"live":"live_a"},"device":"/dev/video0"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPlan(planPath); err == nil {
		t.Fatal("unknown operator field accepted")
	}
}
