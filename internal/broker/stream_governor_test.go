package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	productconfig "github.com/Josepavese/videosluice/config"
)

func TestStreamGovernorProjectionFollowsNamedStreamBinding(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "resource-governor.json")
	service := governorProjectionService(basePath, map[string]string{
		"camera-a": "thermal_soft",
		"camera-b": "nominal",
	}, map[string]productconfig.StreamConfig{
		"analysis": {Type: "analysis", SourceRef: "camera-a"},
	})

	service.publishStreamGovernorStates()
	path := streamGovernorStatePath(basePath, "analysis")
	if got := readProjectedGovernorMode(t, path); got != "thermal_soft" {
		t.Fatalf("initial analysis governor mode=%q", got)
	}

	service.mu.Lock()
	stream := service.config.Streams["analysis"]
	stream.SourceRef = "camera-b"
	service.config.Streams["analysis"] = stream
	service.mu.Unlock()
	service.publishStreamGovernorStates()
	if got := readProjectedGovernorMode(t, path); got != "nominal" {
		t.Fatalf("rebound analysis governor mode=%q", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o640 || info.Sys() == nil {
		t.Fatalf("projected governor file mode=%v", info.Mode())
	}
}

func TestStreamGovernorProjectionHasNoCameraCountLimit(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "resource-governor.json")
	modes := map[string]string{}
	streams := map[string]productconfig.StreamConfig{}
	for index := 0; index < 5; index++ {
		sourceID := "camera-" + string(rune('a'+index))
		streamID := "analysis-" + string(rune('a'+index))
		modes[sourceID] = "mode-" + sourceID
		streams[streamID] = productconfig.StreamConfig{Type: "analysis", SourceRef: sourceID}
	}
	service := governorProjectionService(basePath, modes, streams)
	service.publishStreamGovernorStates()
	for streamID, stream := range streams {
		want := modes[stream.SourceRef]
		if got := readProjectedGovernorMode(t, streamGovernorStatePath(basePath, streamID)); got != want {
			t.Fatalf("stream=%s mode=%q want=%q", streamID, got, want)
		}
	}
}

func TestStreamGovernorProjectionDoesNotBlockOnRoutingOwnedSource(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "resource-governor.json")
	service := governorProjectionService(basePath, map[string]string{
		"camera-a": "nominal",
	}, map[string]productconfig.StreamConfig{
		"analysis": {Type: "analysis", SourceRef: "camera-a"},
	})
	service.publishStreamGovernorStates()
	path := streamGovernorStatePath(basePath, "analysis")
	supervisor := service.sources["camera-a"]
	supervisor.mu.Lock()
	done := make(chan struct{})
	go func() {
		service.publishStreamGovernorStates()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		supervisor.mu.Unlock()
		t.Fatal("stream governor publisher blocked on routing-owned source")
	}
	supervisor.mu.Unlock()
	if got := readProjectedGovernorMode(t, path); got != "nominal" {
		t.Fatalf("locked source invalidated last committed state: %q", got)
	}
}

func governorProjectionService(
	basePath string,
	modes map[string]string,
	streams map[string]productconfig.StreamConfig,
) *Service {
	sources := make(map[string]*sourceSupervisor, len(modes))
	configSources := make(map[string]productconfig.SourceConfig, len(modes))
	for sourceID, mode := range modes {
		runtime := &videoBrokerRuntime{
			plan: videoBrokerPlan{
				ResourceGovernorEnabled: true, AnalysisEnabled: true,
				SubFPS: 20,
			},
			childRunning: true, observedCaptureCodec: "mjpeg", observedCaptureFPS: 20,
			mainClients: map[int]*brokerClient{},
			governor: resourceGovernorDecision{
				Mode: mode, Reason: "test", ObservedAt: time.Now().UTC(),
				SubFPS: 20, OptionalComputeAdmitted: true,
			},
		}
		sources[sourceID] = &sourceSupervisor{
			state: "running", graph: &graphService{sourceID: sourceID, runtime: runtime},
		}
		configSources[sourceID] = productconfig.SourceConfig{}
	}
	return &Service{
		sources: sources,
		config: productconfig.Config{
			Sources: configSources, Streams: streams,
			Runtime: productconfig.RuntimeConfig{Governor: productconfig.GovernorConfig{
				Enabled: true, PollSeconds: 5, StatePath: basePath,
			}},
		},
	}
}

func readProjectedGovernorMode(t *testing.T, path string) string {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state resourceGovernorState
	if err := json.Unmarshal(payload, &state); err != nil {
		t.Fatal(err)
	}
	return state.Mode
}
