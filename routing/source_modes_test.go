package routing

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestSourceModesAreQualifiedSharedPersistentAndRevocable(t *testing.T) {
	catalog := testCatalog(5)
	catalog.SourceModes = map[string]map[string]SourceMode{"camera_a": {"hd": {Format: "mjpeg", Width: 1280, Height: 720, FPS: 20}}, "camera_e": {"hd": {Format: "mjpeg", Width: 1280, Height: 720, FPS: 20}}}
	store, err := NewStore(filepath.Join(t.TempDir(), "routing"))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []map[string]string{{"camera_a": "unknown"}, {"missing": "hd"}, {"camera_a": "default"}} {
		if _, err := store.Begin(catalog, plan.Revision, plan.Selections, invalid); err == nil {
			t.Fatal("unqualified mode accepted")
		}
	}
	tx, err := store.Begin(catalog, plan.Revision, plan.Selections, map[string]string{"camera_a": "hd", "camera_e": "hd"})
	if err != nil {
		t.Fatal(err)
	}
	delta, err := DiffConfigs(tx.CurrentConfig(), tx.Config())
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.ChangedStreamIDs) != 3 || delta.Sources["camera_a"].PreserveCaptureOwner || !delta.Sources["camera_a"].CaptureModeChanged {
		t.Fatalf("shared capture delta=%+v", delta)
	}
	if tx.Config().Sources["camera_a"].Format != "mjpeg" || tx.Config().Sources["camera_a"].Width != 1280 {
		t.Fatal("mode not materialized")
	}
	applied := tx.Plan()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	fresh, _ := NewStore(filepath.Join(store.directory))
	persisted, err := fresh.Read(catalog)
	if err != nil || !reflect.DeepEqual(applied, persisted) {
		t.Fatalf("mode not persisted: %v", err)
	}
	if persisted.SourceSelections["camera_e"] != "hd" {
		t.Fatal("inactive source choice lost")
	}
	rollback, err := store.BeginRollback(catalog, applied.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if rollback.Config().Sources["camera_a"].Width != 1920 || len(rollback.Plan().SourceSelections) != 0 {
		t.Fatal("rollback did not restore base acquisition")
	}
	_ = rollback.Abort()
	delete(catalog.SourceModes["camera_a"], "hd")
	if _, err := fresh.Read(catalog); err == nil {
		t.Fatal("revoked mode silently replaced")
	}
}

func TestSourceModeCannotLowerCaptureBelowConsumerFPS(t *testing.T) {
	catalog := testCatalog(1)
	catalog.SourceModes = map[string]map[string]SourceMode{"camera_a": {"slow": {Format: "mjpeg", Width: 1280, Height: 720, FPS: 10}}}
	plan, err := DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	plan.SourceSelections = map[string]string{"camera_a": "slow"}
	if _, err := Materialize(catalog, plan); err == nil {
		t.Fatal("shared consumer conflict accepted")
	}
	view, err := ProjectControlCatalog(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if len(view.SourceModes["camera_a"]) != 2 || !reflect.DeepEqual(view.Streams["live"].Options["live_a"].SourceModes, []string{"default"}) {
		t.Fatal("public mode compatibility differs")
	}
}
