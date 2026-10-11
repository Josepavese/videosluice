package routing

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Josepavese/videosluice/config"
)

type previewGraphValidatorStub struct {
	calls int
	err   error
	delta GraphDelta
}

func (validator *previewGraphValidatorStub) ValidateGraph(
	_ context.Context,
	current config.Config,
	candidate config.Config,
	currentPlan Plan,
	candidatePlan Plan,
	delta GraphDelta,
) error {
	validator.calls++
	validator.delta = delta
	// The validator receives detached inputs and cannot mutate Store authority.
	current.Streams["live"] = config.StreamConfig{}
	candidate.Streams["live"] = config.StreamConfig{}
	currentPlan.Selections["live"] = "tampered"
	candidatePlan.Selections["live"] = "tampered"
	delta.ChangedStreamIDs[0] = "tampered"
	for sourceID, change := range delta.Sources {
		if len(change.CurrentStreams) > 0 {
			change.CurrentStreams[0] = "tampered"
			delta.Sources[sourceID] = change
			break
		}
	}
	return validator.err
}

func TestPreviewRuntimeGraphRequiresDataPlaneValidationWithoutMutation(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	validator := &previewGraphValidatorStub{}
	preview, err := PreviewRuntimeGraph(
		context.Background(), catalog, store, 1, applySelection("camera_b"), validator)
	if err != nil {
		t.Fatal(err)
	}
	if validator.calls != 1 || !validator.delta.Changed || preview.Candidate.Selections["live"] != "live_b" ||
		preview.ChangedStreamIDs[0] == "tampered" {
		t.Fatalf("runtime preview did not preserve validated projection: preview=%+v validator=%+v", preview, validator)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Revision != 1 || persisted.Selections["live"] != "live_a" {
		t.Fatalf("runtime preview changed persistent authority: %+v", persisted)
	}
}

func TestPreviewRuntimeGraphRejectsUnsupportedTransitionAndReleasesStore(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	cause := errors.New("backend cannot switch this graph")
	validator := &previewGraphValidatorStub{err: cause}
	if _, err := PreviewRuntimeGraph(
		context.Background(), catalog, store, 1, applySelection("camera_b"), validator,
	); err == nil || !errors.Is(err, cause) {
		t.Fatalf("unsupported runtime transition returned: %v", err)
	}
	if validator.calls != 1 {
		t.Fatalf("runtime validator calls=%d want=1", validator.calls)
	}
	if _, err := PreviewGraph(catalog, store, 1, applySelection("camera_b")); err != nil {
		t.Fatalf("failed runtime preview retained the store lock: %v", err)
	}
}

func TestPreviewRuntimeGraphRequiresLiveContextAndValidator(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	if _, err := PreviewRuntimeGraph(
		nil, catalog, store, 1, applySelection("camera_b"), &previewGraphValidatorStub{},
	); err == nil {
		t.Fatal("nil preview context was accepted")
	}
	if _, err := PreviewRuntimeGraph(
		context.Background(), catalog, store, 1, applySelection("camera_b"), nil,
	); err == nil {
		t.Fatal("nil runtime validator was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := PreviewRuntimeGraph(
		ctx, catalog, store, 1, applySelection("camera_b"), &previewGraphValidatorStub{},
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preview context returned: %v", err)
	}
}

func TestPreviewGraphUsesApplyValidationWithoutPersisting(t *testing.T) {
	catalog := testCatalog(3)
	store := newApplyStore(t)
	selections := map[string]string{
		"live": "live_c", "analysis": "analysis_b", "archive": "archive_a",
	}
	preview, err := PreviewGraph(catalog, store, 1, selections)
	if err != nil {
		t.Fatal(err)
	}
	if preview.CurrentRevision != 1 || preview.Candidate.Revision != 2 ||
		!reflect.DeepEqual(preview.ChangedSourceIDs, []string{"camera_a", "camera_b", "camera_c"}) ||
		!reflect.DeepEqual(preview.ChangedStreamIDs, []string{"analysis", "live"}) {
		t.Fatalf("unexpected preview: %+v", preview)
	}
	if preview.Candidate.StreamGenerations["live"] != 2 ||
		preview.Candidate.StreamGenerations["analysis"] != 2 ||
		preview.Candidate.StreamGenerations["archive"] != 1 {
		t.Fatalf("preview generations do not match changed streams: %+v", preview.Candidate)
	}
	if preview.Streams["live"].CurrentSource != "camera_a" ||
		preview.Streams["live"].CandidateSource != "camera_c" ||
		preview.Sources["camera_a"].State != SourceChangeReconfigure ||
		preview.Sources["camera_b"].State != SourceChangeActivate {
		t.Fatalf("preview lost exact graph delta: %+v", preview)
	}
	encoded, err := json.Marshal(preview)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "/dev/") || strings.Contains(string(encoded), "native_sp") {
		t.Fatalf("operator preview leaked physical graph details: %s", encoded)
	}
	persisted, err := store.Read(catalog)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Revision != 1 || !reflect.DeepEqual(persisted.Selections, catalog.DefaultSelections) {
		t.Fatalf("preview persisted a mutation: %+v", persisted)
	}
}

func TestPreviewGraphReturnsDetachedOperatorProjection(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	preview, err := PreviewGraph(catalog, store, 1, applySelection("camera_b"))
	if err != nil {
		t.Fatal(err)
	}
	preview.Candidate.Selections["live"] = "tampered"
	change := preview.Sources["camera_a"]
	change.CurrentStreams[0] = "tampered"
	preview.Sources["camera_a"] = change

	second, err := PreviewGraph(catalog, store, 1, applySelection("camera_b"))
	if err != nil {
		t.Fatal(err)
	}
	if second.Candidate.Selections["live"] != "live_b" || second.Sources["camera_a"].CurrentStreams[0] == "tampered" {
		t.Fatalf("caller mutated preview authority: %+v", second)
	}
}

func TestPreviewGraphRejectsStaleInvalidAndUnchangedRequests(t *testing.T) {
	catalog := testCatalog(2)
	store := newApplyStore(t)
	for name, test := range map[string]struct {
		revision   uint64
		selections map[string]string
		want       error
	}{
		"stale":     {revision: 2, selections: applySelection("camera_b"), want: ErrStaleRevision},
		"unchanged": {revision: 1, selections: cloneSelections(catalog.DefaultSelections), want: ErrNoChange},
		"invalid":   {revision: 1, selections: map[string]string{"live": "raw"}, want: nil},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := PreviewGraph(catalog, store, test.revision, test.selections)
			if err == nil || (test.want != nil && !errors.Is(err, test.want)) {
				t.Fatalf("unexpected preview error: %v", err)
			}
		})
	}
}
