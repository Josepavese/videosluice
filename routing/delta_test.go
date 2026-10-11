package routing

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Josepavese/videosluice/config"
)

func TestFrameAccessRestartPreviewFencesEveryAffectedPublication(t *testing.T) {
	catalog := testCatalog(3)
	catalog.Runtime.FrameAccess = &config.FrameAccessConfig{CopyProvider: "cpu_nv12_copy", Readers: []config.FrameReaderConfig{
		{ID: "reader", Stream: "analysis", DeliverySocket: "/run/video/frames.sock", ROISocket: "/run/video/roi.sock"},
	}}
	catalog.DefaultSelections = map[string]string{"live": "live_b", "analysis": "analysis_a", "archive": "archive_a"}
	// A third active capture is outside this transaction and must not be fenced.
	catalog.Streams["peer_live"] = catalog.Streams["live"]
	peer := catalog.Streams["peer_live"]
	peer.Socket = "/run/video/peer.sock"
	catalog.Streams["peer_live"] = peer
	catalog.DefaultSelections["peer_live"] = "live_c"
	store, err := NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	selections := cloneSelections(catalog.DefaultSelections)
	selections["analysis"] = "analysis_b"
	preview, err := PreviewGraph(catalog, store, 1, selections)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(preview.ChangedSourceIDs, []string{"camera_a", "camera_b"}) ||
		!reflect.DeepEqual(preview.ChangedStreamIDs, []string{"analysis", "archive", "live"}) {
		t.Fatalf("preview hid the capture restart impact: %+v", preview)
	}
	for _, id := range []string{"camera_a", "camera_b"} {
		if preview.Sources[id].PreserveCaptureOwner {
			t.Fatalf("preview promised to preserve the frame capture %q", id)
		}
	}
	for _, id := range []string{"analysis", "archive", "live"} {
		if preview.Candidate.StreamGenerations[id] != 2 {
			t.Fatalf("restarted publication %q was not fenced: %+v", id, preview.Candidate)
		}
	}
	if preview.Candidate.StreamGenerations["peer_live"] != 1 || !preview.Sources["camera_c"].PreserveCaptureOwner {
		t.Fatal("unrelated capture or publication was changed")
	}
	if preview.Streams["archive"].ProfileChanged || preview.Streams["live"].ProfileChanged {
		t.Fatal("capture replacement was misreported as an output profile edit")
	}
	// Preview is not persistence; commit and rollback must use identical fencing.
	current, err := store.Read(catalog)
	if err != nil || current.Revision != 1 {
		t.Fatalf("preview mutated the store: %+v %v", current, err)
	}
	tx, err := store.Begin(catalog, 1, selections)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tx.Plan(), preview.Candidate) {
		t.Fatal("preview and prepared plan disagree")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rollback, err := store.BeginRollback(catalog, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback.Abort()
	for _, id := range []string{"analysis", "archive", "live"} {
		if rollback.Plan().StreamGenerations[id] != 3 {
			t.Fatalf("rollback reused publication %q", id)
		}
	}
	if rollback.Plan().StreamGenerations["peer_live"] != 1 {
		t.Fatal("rollback fenced an unrelated capture")
	}
}

func TestDiffConfigsPreservesOwnersForSharedSourceSwitch(t *testing.T) {
	catalog := testCatalog(2)
	current, err := Materialize(catalog, testPlan(catalog, 1, map[string]string{
		"live": "live_a", "analysis": "analysis_b", "archive": "archive_a",
	}))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := Materialize(catalog, testPlan(catalog, 2, map[string]string{
		"live": "live_b", "analysis": "analysis_b", "archive": "archive_a",
	}))
	if err != nil {
		t.Fatal(err)
	}
	delta, err := DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !delta.Changed || !reflect.DeepEqual(delta.SourceIDs, []string{"camera_a", "camera_b"}) ||
		!reflect.DeepEqual(delta.ChangedSourceIDs, []string{"camera_a", "camera_b"}) ||
		!reflect.DeepEqual(delta.ChangedStreamIDs, []string{"live"}) {
		t.Fatalf("unexpected deterministic delta: %+v", delta)
	}
	for _, sourceID := range []string{"camera_a", "camera_b"} {
		change := delta.Sources[sourceID]
		if change.State != SourceChangeReconfigure || !change.PreserveCaptureOwner {
			t.Fatalf("active source %q would lose its capture owner: %+v", sourceID, change)
		}
	}
	if live := delta.Streams["live"]; live.State != StreamChangeMove || live.CurrentSource != "camera_a" ||
		live.CandidateSource != "camera_b" || live.ProfileChanged {
		t.Fatalf("live move was misclassified: %+v", live)
	}
	for _, streamID := range []string{"analysis", "archive"} {
		if delta.Streams[streamID].State != StreamChangeUnchanged {
			t.Fatalf("unaffected stream %q changed: %+v", streamID, delta.Streams[streamID])
		}
	}
}

func TestDiffConfigsClassifiesActivationAcrossFiveSourceCatalog(t *testing.T) {
	catalog := testCatalog(5)
	current, err := Materialize(catalog, testPlan(catalog, 4, map[string]string{
		"live": "live_a", "analysis": "analysis_a", "archive": "archive_a",
	}))
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := Materialize(catalog, testPlan(catalog, 5, map[string]string{
		"live": "live_e", "analysis": "analysis_e", "archive": "archive_e",
	}))
	if err != nil {
		t.Fatal(err)
	}
	delta, err := DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if delta.Sources["camera_a"].State != SourceChangeDeactivate || delta.Sources["camera_a"].PreserveCaptureOwner ||
		delta.Sources["camera_e"].State != SourceChangeActivate || delta.Sources["camera_e"].PreserveCaptureOwner {
		t.Fatalf("activation lifecycle was misclassified: %+v", delta.Sources)
	}
	if !reflect.DeepEqual(delta.SourceIDs, []string{"camera_a", "camera_e"}) || len(delta.ChangedStreamIDs) != 3 {
		t.Fatalf("five-source catalog produced nondeterministic active delta: %+v", delta)
	}
}

func TestDiffConfigsClassifiesProfileChangeWithoutReplacingCapture(t *testing.T) {
	catalog := testCatalog(1)
	live := catalog.Streams["live"]
	option := live.Options["live_a"]
	option.FPS = 15
	option.BitrateKbps = 2048
	live.Options["live_detail"] = option
	catalog.Streams["live"] = live
	current, err := Materialize(catalog, testPlan(catalog, 1, catalog.DefaultSelections))
	if err != nil {
		t.Fatal(err)
	}
	selections := cloneSelections(catalog.DefaultSelections)
	selections["live"] = "live_detail"
	candidate, err := Materialize(catalog, testPlan(catalog, 2, selections))
	if err != nil {
		t.Fatal(err)
	}
	delta, err := DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if delta.Streams["live"].State != StreamChangeReconfigure || !delta.Streams["live"].ProfileChanged ||
		delta.Sources["camera_a"].State != SourceChangeReconfigure || !delta.Sources["camera_a"].PreserveCaptureOwner {
		t.Fatalf("in-place profile change was misclassified: %+v", delta)
	}
}

func TestDiffConfigsRejectsChangesOutsideRoutingAuthority(t *testing.T) {
	catalog := testCatalog(2)
	current, err := Materialize(catalog, testPlan(catalog, 1, catalog.DefaultSelections))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*config.Config){
		"runtime": func(candidate *config.Config) { candidate.Runtime.StateDirectory = "/var/lib/other" },
		"source": func(candidate *config.Config) {
			source := candidate.Sources["camera_a"]
			source.Device = "/dev/nemesi-video-sources/replaced"
			candidate.Sources["camera_a"] = source
		},
		"stream": func(candidate *config.Config) { delete(candidate.Streams, "archive") },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := cloneConfig(current)
			mutate(&candidate)
			_, err := DiffConfigs(current, candidate)
			if err == nil || (!strings.Contains(err.Error(), name) && name != "stream") {
				t.Fatalf("out-of-authority %s change accepted: %v", name, err)
			}
		})
	}
}

func TestDiffConfigsReportsNoChangeDeterministically(t *testing.T) {
	catalog := testCatalog(3)
	cfg, err := Materialize(catalog, testPlan(catalog, 3, map[string]string{
		"live": "live_c", "analysis": "analysis_b", "archive": "archive_a",
	}))
	if err != nil {
		t.Fatal(err)
	}
	delta, err := DiffConfigs(cfg, cloneConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if delta.Changed || len(delta.ChangedSourceIDs) != 0 || len(delta.ChangedStreamIDs) != 0 {
		t.Fatalf("equal graphs reported a change: %+v", delta)
	}
	if !reflect.DeepEqual(delta.SourceIDs, []string{"camera_a", "camera_b", "camera_c"}) ||
		!reflect.DeepEqual(delta.StreamIDs, []string{"analysis", "archive", "live"}) {
		t.Fatalf("equal graph ordering is unstable: %+v", delta)
	}
}
