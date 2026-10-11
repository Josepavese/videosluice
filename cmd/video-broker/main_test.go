package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	videobroker "github.com/Josepavese/videosluice"
	"github.com/Josepavese/videosluice/config"
	in "github.com/Josepavese/videosluice/frameingress"
	"github.com/Josepavese/videosluice/routing"
)

func TestStatusSocketPublishesProductState(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "without_frames"
		if enabled {
			name = "with_frames"
		}
		t.Run(name, func(t *testing.T) { testStatusSocket(t, enabled) })
	}
}

type statusFrameFixture struct{ metadata in.DeliveryMetadata }

func (f statusFrameFixture) FrameMetadata(context.Context) map[string]map[string]in.DeliveryMetadata {
	return map[string]map[string]in.DeliveryMetadata{"overview": {"analysis": f.metadata}}
}

func testStatusSocket(t *testing.T, enabled bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var service *videobroker.Service
	metadata := in.DeliveryMetadata{Source: "overview", Stream: "analysis", DeliveryID: (1 << 53) + 19,
		Namespace: strings.Repeat("ab", 16), Process: "broker-process", SourceEpoch: 2, PublicationEpoch: 3,
		OriginSequence: 4, Graph: strings.Repeat("cd", 32), Transform: strings.Repeat("ef", 32),
		Width: 640, Height: 360, CommittedAt: time.Now().UTC()}
	var frames frameMetadataProvider
	if enabled {
		frames = statusFrameFixture{metadata: metadata}
	}
	path := filepath.Join(t.TempDir(), "status.sock")
	done := make(chan error, 1)
	go func() { done <- serveStatus(ctx, path, service, frames) }()

	var connection net.Conn
	var err error
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		connection, err = net.DialTimeout("unix", path, 100*time.Millisecond)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if connection == nil {
		cancel()
		t.Fatalf("status socket did not become available: %v", err)
	}
	defer connection.Close()
	info, err := os.Lstat(path)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	socketMetadata, ok := info.Sys().(*syscall.Stat_t)
	if info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 ||
		!ok || socketMetadata.Nlink != 1 {
		cancel()
		t.Fatalf("status socket is not single-owner: mode=%v metadata=%+v", info.Mode(), socketMetadata)
	}
	var status videobroker.Status
	if err := json.NewDecoder(connection).Decode(&status); err != nil {
		cancel()
		t.Fatal(err)
	}
	if status.Schema != "video-broker.status.v5" || status.State != "stopped" || status.Sources == nil || status.Streams == nil {
		cancel()
		t.Fatalf("unexpected status: %+v", status)
	}
	if enabled {
		if status.FrameAccess == nil || status.FrameAccess.Schema != "video-broker.frame-metadata.v1" ||
			status.FrameAccess.Sources["overview"]["analysis"] != metadata {
			t.Fatal("status socket lost committed frame metadata", status.FrameAccess)
		}
	} else if status.FrameAccess != nil {
		t.Fatal("status invented a frame plane", status.FrameAccess)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("status server did not stop with its context")
	}
}

func TestLoadRuntimeConfigRequiresOneExactInputMode(t *testing.T) {
	for _, args := range [][3]string{
		{},
		{"/tmp/config.json", "/tmp/catalog.json", "/tmp/plan.json"},
		{"", "/tmp/catalog.json", ""},
		{"", "", "/tmp/plan.json"},
	} {
		if _, err := loadRuntimeConfig(args[0], args[1], args[2]); err == nil {
			t.Fatalf("ambiguous/incomplete inputs accepted: %q", args)
		}
	}
}

func TestLoadRuntimeConfigMaterializesPersistedRoutingPlan(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "routing")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	revision := "sha256:" + strings.Repeat("a", 64)
	source := config.SourceConfig{
		Identity: "sha256:" + strings.Repeat("b", 64), Transport: "v4l2_uvc",
		Device: "/dev/camera-front", Format: "h265", CodecPreference: "h265",
		CodecPolicy: "required", ModePolicy: "exact", AspectPolicy: "crop",
		Width: 1280, Height: 720, FPS: 30,
		Processing: config.ProcessingConfig{
			Orientation: "normal", ToneMapping: config.ToneMappingConfig{Profile: "adaptive", Mode: "off"},
			CameraControl: config.CameraControlOptimizerConfig{Policy: "off", Mode: "off"},
		},
		Acceleration: config.AccelerationConfig{
			EncodeProvider: "native_sp", DecodeProvider: "native_sp", RequireHardwareCodec: true,
			Discovery: config.CapabilityDiscoveryConfig{Mode: "auto", ProbeTimeoutMS: 8000},
		},
	}
	catalog := routing.Catalog{
		Schema: routing.CatalogSchema, Revision: revision,
		Sources: map[string]config.SourceConfig{"front": source},
		Streams: map[string]routing.StreamCatalog{
			"live": {Type: "live", Socket: "/run/video/live.sock", Options: map[string]routing.StreamOption{
				"balanced": {SourceRef: "front", Codec: "h265", Width: 1280, Height: 720, FPS: 30, BitrateKbps: 4096},
			}},
		},
		DefaultSelections: map[string]string{"live": "balanced"},
		Runtime:           config.RuntimeConfig{RuntimeDirectory: "/run/video", StateDirectory: "/var/lib/video"},
	}
	plan := routing.Plan{
		Schema: routing.PlanSchema, CatalogRevision: revision, Revision: 4,
		Selections:        map[string]string{"live": "balanced"},
		StreamGenerations: map[string]uint64{"live": 4},
	}
	catalogPath := filepath.Join(directory, "catalog.json")
	planPath := filepath.Join(directory, "routing-plan.json")
	for path, value := range map[string]any{catalogPath: catalog, planPath: plan} {
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	input, err := loadRuntimeInput("", catalogPath, planPath)
	if err != nil {
		t.Fatal(err)
	}
	loaded := input.Config
	if loaded.Streams["live"].SourceRef != "front" || loaded.Streams["live"].Codec != "h265" || loaded.SchemaVersion != config.SchemaVersion {
		t.Fatalf("unexpected routed config: %+v", loaded)
	}
	if input.Catalog == nil || input.Plan == nil || input.Catalog.Revision != revision || input.Plan.Revision != 4 ||
		input.Plan.StreamGenerations["live"] != 4 {
		t.Fatalf("daemon discarded routed runtime identity: %+v", input)
	}
}

func TestDaemonStartupReconcilesTrustedCatalogWhileValidationStaysReadOnly(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "routing")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	oldRevision := "sha256:" + strings.Repeat("a", 64)
	newRevision := "sha256:" + strings.Repeat("c", 64)
	source := config.SourceConfig{
		Identity: "sha256:" + strings.Repeat("b", 64), Transport: "v4l2_uvc",
		Device: "/dev/camera-front", Format: "h265", CodecPreference: "h265",
		CodecPolicy: "required", ModePolicy: "exact", AspectPolicy: "crop",
		Width: 1280, Height: 720, FPS: 30,
		Processing: config.ProcessingConfig{
			Orientation: "normal", ToneMapping: config.ToneMappingConfig{Profile: "adaptive", Mode: "off"},
			CameraControl: config.CameraControlOptimizerConfig{Policy: "off", Mode: "off"},
		},
		Acceleration: config.AccelerationConfig{
			EncodeProvider: "native_sp", DecodeProvider: "native_sp", RequireHardwareCodec: true,
			Discovery: config.CapabilityDiscoveryConfig{Mode: "auto", ProbeTimeoutMS: 8000},
		},
	}
	oldCatalog := routing.Catalog{
		Schema: routing.CatalogSchema, Revision: oldRevision,
		Sources: map[string]config.SourceConfig{"front": source},
		Streams: map[string]routing.StreamCatalog{
			"live": {Type: "live", Socket: "/run/video/live.sock", Options: map[string]routing.StreamOption{
				"balanced": {SourceRef: "front", Codec: "h265", Width: 1280, Height: 720, FPS: 30, BitrateKbps: 4096},
			}},
		},
		DefaultSelections: map[string]string{"live": "balanced"},
		Runtime:           config.RuntimeConfig{RuntimeDirectory: "/run/video", StateDirectory: "/var/lib/video"},
	}
	newCatalog := oldCatalog
	newCatalog.Revision = newRevision
	newCatalog.Sources = map[string]config.SourceConfig{"front": source}
	front := newCatalog.Sources["front"]
	front.FPS = 25
	newCatalog.Sources["front"] = front
	newCatalog.Streams = map[string]routing.StreamCatalog{
		"live": {Type: "live", Socket: "/run/video/live.sock", Options: map[string]routing.StreamOption{
			"balanced": {SourceRef: "front", Codec: "h265", Width: 1280, Height: 720, FPS: 25, BitrateKbps: 4096},
		}},
	}
	plan := routing.Plan{
		Schema: routing.PlanSchema, CatalogRevision: oldRevision, Revision: 4,
		Selections:        map[string]string{"live": "balanced"},
		StreamGenerations: map[string]uint64{"live": 7},
	}
	catalogPath := filepath.Join(directory, "catalog.json")
	planPath := filepath.Join(directory, "routing-plan.json")
	writeJSON := func(path string, value any) {
		t.Helper()
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeJSON(catalogPath, newCatalog)
	writeJSON(planPath, plan)
	before, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadRuntimeInput("", catalogPath, planPath); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("read-only validation accepted a stale plan: %v", err)
	}
	afterValidation, err := os.ReadFile(planPath)
	if err != nil || !stringSlicesEqual(before, afterValidation) {
		t.Fatalf("read-only validation mutated the plan: err=%v", err)
	}

	input, err := loadRuntimeInputForStart("", catalogPath, planPath)
	if err != nil {
		t.Fatal(err)
	}
	if !input.CatalogReconciled || input.Plan == nil || input.Plan.Revision != 5 ||
		input.Plan.CatalogRevision != newRevision || input.Plan.StreamGenerations["live"] != 8 ||
		input.Config.Sources["front"].FPS != 25 || input.Config.Streams["live"].FPS != 25 {
		t.Fatalf("startup did not reconcile the trusted catalog: %+v", input)
	}
	second, err := loadRuntimeInputForStart("", catalogPath, planPath)
	if err != nil {
		t.Fatal(err)
	}
	if second.CatalogReconciled || second.Plan == nil || second.Plan.Revision != input.Plan.Revision ||
		second.Plan.StreamGenerations["live"] != input.Plan.StreamGenerations["live"] {
		t.Fatalf("startup reconciled the same catalog twice: first=%+v second=%+v", input, second)
	}
}

func stringSlicesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
