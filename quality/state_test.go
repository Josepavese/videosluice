package quality

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func validState() State {
	metrics := Metrics{
		Width: 1920, Height: 1080, Samples: 32400, Luma: 118.4, ContentLuma: 122.1,
		LaplacianVariance: 312.7, Edge: 9.2, BlackRatio: 0.01, WhiteRatio: 0.02,
		SignalInformative: true,
	}
	metrics.Spatial = uniformSpatial(metrics)
	analysis := metrics
	analysis.Width, analysis.Height, analysis.Samples = 640, 360, 3600
	analysis.Spatial = uniformSpatial(analysis)
	return State{
		Schema: Schema, TimestampUnixMS: time.Now().UnixMilli(), Frame: 42, SampleStride: 8,
		Source: metrics, Processed: metrics, Analysis: analysis,
		Capture: Capture{Format: "h265", Orientation: "normal"},
	}
}

func uniformSpatial(metrics Metrics) Spatial {
	regions := make([]Region, 0, SpatialRegionCount)
	base := metrics.Samples / SpatialRegionCount
	remainder := metrics.Samples % SpatialRegionCount
	for index := 0; index < SpatialRegionCount; index++ {
		samples := base
		if uint64(index) < remainder {
			samples++
		}
		region := Region{X: index % SpatialColumns, Y: index / SpatialColumns, Samples: samples}
		if samples > 0 {
			region.Luma = metrics.Luma
			region.ContentLuma = metrics.ContentLuma
			region.LaplacianVariance = metrics.LaplacianVariance
			region.Edge = metrics.Edge
			region.BlackRatio = metrics.BlackRatio
			region.WhiteRatio = metrics.WhiteRatio
			region.SignalInformative = metrics.SignalInformative
		}
		regions = append(regions, region)
	}
	return Spatial{Columns: SpatialColumns, Rows: SpatialRows, Regions: regions}
}

func TestStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quality.json")
	state := validState()
	state.Processed.Luma = 136.5
	if err := WriteFile(path, state); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Source.Luma != 118.4 || got.Processed.Luma != 136.5 || got.Analysis.Width != 640 {
		t.Fatalf("quality domains were not preserved: %+v", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o640 {
		t.Fatalf("quality state mode=%o", info.Mode().Perm())
	}
}

func TestDecodeRejectsLegacyOrIncompleteState(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`{"schema":"video-broker.quality.v1","timestamp_unix_ms":1}`),
		[]byte(`{"schema":"video-broker.quality.v2","timestamp_unix_ms":1}`),
		[]byte(`{"schema":"video-broker.quality.v3","timestamp_unix_ms":1}`),
	} {
		if _, err := Decode(payload); err == nil {
			t.Fatalf("invalid state accepted: %s", payload)
		}
	}
}

func TestStateRejectsMalformedSpatialContract(t *testing.T) {
	state := validState()
	state.Source.Spatial.Regions[1].X = 3
	if err := state.Validate(); err == nil {
		t.Fatal("non-row-major spatial region accepted")
	}

	state = validState()
	state.Source.Spatial.Regions[0].Samples++
	if err := state.Validate(); err == nil {
		t.Fatal("spatial/global sample mismatch accepted")
	}
}

func TestDecodeRejectsUnknownFieldsAndInconsistentSignal(t *testing.T) {
	state := validState()
	payload, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	payload = bytes.Replace(payload, []byte(`"frame":42`), []byte(`"frame":42,"legacy_luma":12`), 1)
	if _, err := Decode(payload); err == nil {
		t.Fatal("unknown legacy field accepted")
	}

	state.Source.BlackRatio = 1
	state.Source.WhiteRatio = 0
	state.Source.LaplacianVariance = 0
	state.Source.Edge = 0
	state.Source.SignalInformative = true
	if err := state.Validate(); err == nil {
		t.Fatal("inconsistent informative state accepted")
	}
}

func TestStateRejectsNonFiniteMetricsBeforeWriting(t *testing.T) {
	state := validState()
	state.Processed.Luma = math.NaN()
	if err := WriteFile(filepath.Join(t.TempDir(), "quality.json"), state); err == nil {
		t.Fatal("non-finite metric accepted")
	}
}
