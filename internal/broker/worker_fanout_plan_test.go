package broker

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	productconfig "github.com/Josepavese/videosluice/config"
	"github.com/Josepavese/videosluice/routing"
)

func TestWorkerFanoutChangesCompileRealFiveSourceRoutingMove(t *testing.T) {
	catalog := fanoutRoutingCatalog(t, 5)
	initial, err := routing.DefaultPlan(catalog)
	if err != nil {
		t.Fatal(err)
	}
	store, err := routingTestStore(t)
	if err != nil {
		t.Fatal(err)
	}
	selections := cloneSelections(initial.Selections)
	selections["live"] = "camera-2"
	preview, err := routing.PreviewGraph(catalog, store, initial.Revision, selections)
	if err != nil {
		t.Fatal(err)
	}
	current, err := routing.Materialize(catalog, initial)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := routing.Materialize(catalog, preview.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := routing.DiffConfigs(current, candidate)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := classifyPreservedOwnerFanout(current, candidate, delta)
	if err != nil {
		t.Fatal(err)
	}
	from, err := workerFanoutChangesForSource(transition, "camera-1", "")
	if err != nil {
		t.Fatal(err)
	}
	to, err := workerFanoutChangesForSource(transition, "camera-2", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(from) != 1 || from[0].Role != "live" || from[0].Action != "detach" || from[0].Profile != nil ||
		len(to) != 1 || to[0].Role != "live" || to[0].Action != "attach" || to[0].Profile == nil {
		t.Fatalf("unexpected source-local move plans: from=%+v to=%+v", from, to)
	}
	if !reflect.DeepEqual(transition.SourceIDs, []string{"camera-1", "camera-2"}) {
		t.Fatalf("unrelated five-source peers entered worker plan: %v", transition.SourceIDs)
	}
}

func TestWorkerFanoutChangesCompileCanonicalAttachDetachReplace(t *testing.T) {
	source := productconfig.SourceConfig{
		Identity: "camera-a", Transport: "v4l2_uvc", Device: "/dev/nemesi-video-sources/camera-a",
		Format: "mjpeg", Width: 1280, Height: 720, FPS: 30,
	}
	current := productconfig.SourceGraph{
		SourceID: "camera-a", Source: source,
		Analysis: workerFanoutTestStream("analysis-old", "analysis", "camera-a", "/run/old-analysis.sock"),
		Live:     workerFanoutTestStream("live", "live", "camera-a", "/run/live.sock"),
	}
	candidate := current
	candidate.Analysis = nil
	candidate.Archive = workerFanoutTestStream("archive", "archive", "camera-a", "/run/archive.sock")
	candidate.Archive.Config.Codec = "auto"
	candidate.Live = workerFanoutTestStream("live", "live", "camera-a", "/run/live.sock")
	candidate.Live.Config.FPS = 20
	transition := preservedOwnerFanout{
		SourceIDs:       []string{"camera-a"},
		CurrentGraphs:   map[string]productconfig.SourceGraph{"camera-a": current},
		CandidateGraphs: map[string]productconfig.SourceGraph{"camera-a": candidate},
	}

	changes, err := workerFanoutChangesForSource(transition, "camera-a", "h265")
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{
		changes[0].Role + ":" + changes[0].Action,
		changes[1].Role + ":" + changes[1].Action,
		changes[2].Role + ":" + changes[2].Action,
	}; !reflect.DeepEqual(got, []string{"analysis:detach", "archive:attach", "live:replace"}) {
		t.Fatalf("branch changes are not canonical: %v", got)
	}
	if changes[0].Profile != nil || changes[1].Profile == nil || changes[1].Profile.Codec != "h265" ||
		changes[2].Profile == nil || changes[2].Profile.FPS != 20 {
		t.Fatalf("branch profiles are incomplete: %+v", changes)
	}
	payload, err := json.Marshal(changes)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"/dev/", "/run/", "native_sp", "gstreamer", "camera-a"} {
		if strings.Contains(string(payload), forbidden) {
			t.Fatalf("worker branch plan leaked %q: %s", forbidden, payload)
		}
	}
}

func TestWorkerFanoutChangesRejectCaptureMutationAndUnresolvedArchiveCodec(t *testing.T) {
	stream := workerFanoutTestStream("archive", "archive", "camera-a", "/run/archive.sock")
	stream.Config.Codec = "auto"
	current := productconfig.SourceGraph{
		SourceID: "camera-a",
		Source:   productconfig.SourceConfig{Identity: "camera-a", Device: "/dev/camera-a"},
		Live:     workerFanoutTestStream("live", "live", "camera-a", "/run/live.sock"),
	}
	candidate := current
	candidate.Archive = stream
	transition := preservedOwnerFanout{
		CurrentGraphs:   map[string]productconfig.SourceGraph{"camera-a": current},
		CandidateGraphs: map[string]productconfig.SourceGraph{"camera-a": candidate},
	}
	if _, err := workerFanoutChangesForSource(transition, "camera-a", ""); err == nil ||
		!strings.Contains(err.Error(), "concrete encoded contract") {
		t.Fatalf("unresolved archive codec was accepted: %v", err)
	}
	candidate.Source.Width++
	transition.CandidateGraphs["camera-a"] = candidate
	if _, err := workerFanoutChangesForSource(transition, "camera-a", "h265"); err == nil ||
		!strings.Contains(err.Error(), "capture contract") {
		t.Fatalf("capture mutation was accepted: %v", err)
	}
}

func workerFanoutTestStream(id, role, sourceID, socket string) *productconfig.NamedStream {
	stream := productconfig.StreamConfig{
		Type: role, SourceRef: sourceID, Socket: socket,
		Width: 640, Height: 360, FPS: 15,
	}
	switch role {
	case "analysis":
		stream.PixelFormat = "nv12"
	case "live":
		stream.Codec = "h265"
		stream.BitrateKbps = 2048
		stream.Content = "processed"
		stream.RateControl = "cbr"
	case "archive":
		stream.Codec = "h265"
		stream.BitrateKbps = 750
		stream.Content = "processed"
		stream.RateControl = "avbr"
	}
	return &productconfig.NamedStream{ID: id, Config: stream}
}
