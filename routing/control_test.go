package routing

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Josepavese/videosluice/config"
)

func TestValidateControlRequestAcceptsOnlyCurrentOpaqueOperations(t *testing.T) {
	valid := []ControlRequest{
		{Schema: ControlSchema, RequestID: "read-1", Action: ControlActionRead},
		{Schema: ControlSchema, RequestID: "preview-1", Action: ControlActionPreview, ExpectedRevision: 2, Selections: map[string]string{"live": "balanced"}},
		{Schema: ControlSchema, RequestID: "apply-1", Action: ControlActionApply, ExpectedRevision: 2, Selections: map[string]string{"live": "balanced"}},
		{Schema: ControlSchema, RequestID: "rollback-1", Action: ControlActionRollback, ExpectedRevision: 2},
		{Schema: ControlSchema, RequestID: "progress-1", Action: ControlActionProgress, OperationID: "apply-1"},
	}
	for _, request := range valid {
		if err := ValidateControlRequest(request); err != nil {
			t.Fatalf("valid request %+v rejected: %v", request, err)
		}
	}

	invalid := []ControlRequest{
		{},
		{Schema: ControlSchema, RequestID: "Uppercase", Action: ControlActionRead},
		{Schema: ControlSchema, RequestID: "read-1", Action: " read"},
		{Schema: ControlSchema, RequestID: "read-1", Action: ControlActionRead, ExpectedRevision: 1},
		{Schema: ControlSchema, RequestID: "preview-1", Action: ControlActionPreview, ExpectedRevision: 1},
		{Schema: ControlSchema, RequestID: "apply-1", Action: ControlActionApply, Selections: map[string]string{"live": "balanced"}},
		{Schema: ControlSchema, RequestID: "rollback-1", Action: ControlActionRollback, ExpectedRevision: 1, Selections: map[string]string{}},
		{Schema: ControlSchema, RequestID: "progress-1", Action: ControlActionProgress},
		{Schema: ControlSchema, RequestID: "progress-1", Action: ControlActionProgress, OperationID: "Uppercase"},
		{Schema: ControlSchema, RequestID: "progress-1", Action: ControlActionProgress, OperationID: "apply-1", ExpectedRevision: 1},
		{Schema: ControlSchema, RequestID: "unknown-1", Action: "replace", ExpectedRevision: 1},
	}
	for _, request := range invalid {
		if err := ValidateControlRequest(request); err == nil {
			t.Fatalf("invalid request accepted: %+v", request)
		}
	}
}

func TestProjectControlCatalogExposesOnlyBoundedMediaChoices(t *testing.T) {
	catalog := Catalog{
		Schema: CatalogSchema, Revision: "sha256:" + strings.Repeat("a", 64),
		Sources: map[string]config.SourceConfig{
			"rear":  {Identity: "rear-id", Transport: "v4l2_uvc", Device: "/dev/camera-rear", Format: "mjpeg", Width: 1280, Height: 720, FPS: 30},
			"front": {Identity: "front-id", Transport: "v4l2_uvc", Device: "/dev/camera-front", Format: "mjpeg", Width: 1280, Height: 720, FPS: 30},
		},
		Streams: map[string]StreamCatalog{
			"live": {
				Type: "live", Socket: "/run/video/live.sock",
				Options: map[string]StreamOption{
					"balanced": {SourceRef: "front", Codec: "h265", Width: 1280, Height: 720, FPS: 20, BitrateKbps: 4096},
					"rear":     {SourceRef: "rear", Codec: "h265", Width: 1280, Height: 720, FPS: 20, BitrateKbps: 4096},
				},
			},
		},
		DefaultSelections: map[string]string{"live": "balanced"},
		Runtime: config.RuntimeConfig{
			RuntimeDirectory: "/run/video", StateDirectory: "/var/lib/video",
			Status: config.StatusConfig{Socket: "/run/video/status.sock"},
		},
	}
	view, err := ProjectControlCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Sources) != 2 || view.Sources[0] != "front" || view.Sources[1] != "rear" ||
		view.Streams["live"].Options["balanced"].Codec != "h265" {
		t.Fatalf("unexpected catalog projection: %+v", view)
	}
	body, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"/dev/", "/run/", "identity", "acceler", "provider"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("safe catalog contains %q: %s", forbidden, body)
		}
	}
}
