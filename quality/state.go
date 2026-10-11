// Package quality defines the backend-independent observation contract shared
// by capture engines, image processors, health checks, and camera controllers.
package quality

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	Schema             = "video-broker.quality.v3"
	SpatialColumns     = 4
	SpatialRows        = 3
	SpatialRegionCount = SpatialColumns * SpatialRows
)

// Region describes one cell of the fixed coarse observation grid. The grid is
// deliberately smaller than the tone mapper's implementation grid: it is a
// stable control-plane contract, not an accelerator-specific detail.
type Region struct {
	X                 int     `json:"x"`
	Y                 int     `json:"y"`
	Samples           uint64  `json:"samples"`
	Luma              float64 `json:"luma"`
	ContentLuma       float64 `json:"content_luma"`
	LaplacianVariance float64 `json:"laplacian_variance"`
	Edge              float64 `json:"edge"`
	BlackRatio        float64 `json:"black_ratio"`
	WhiteRatio        float64 `json:"white_ratio"`
	SignalInformative bool    `json:"signal_informative"`
}

// Spatial is row-major and has a fixed geometry so every backend and consumer
// interprets clipping in the same regions without knowing camera or board IDs.
type Spatial struct {
	Columns int      `json:"columns"`
	Rows    int      `json:"rows"`
	Regions []Region `json:"regions"`
}

// Metrics describes one immutable NV12 observation surface.
type Metrics struct {
	Width             int     `json:"width"`
	Height            int     `json:"height"`
	Samples           uint64  `json:"samples"`
	Luma              float64 `json:"luma"`
	ContentLuma       float64 `json:"content_luma"`
	LaplacianVariance float64 `json:"laplacian_variance"`
	Edge              float64 `json:"edge"`
	BlackRatio        float64 `json:"black_ratio"`
	WhiteRatio        float64 `json:"white_ratio"`
	SignalInformative bool    `json:"signal_informative"`
	Spatial           Spatial `json:"spatial"`
}

// Capture identifies the transport that produced Source. It deliberately
// carries no board, camera model, or application identity.
type Capture struct {
	Format      string `json:"format"`
	Orientation string `json:"orientation"`
}

// State keeps three domains distinct:
//   - Source is measured before any broker image processing.
//   - Processed is the corrected full-resolution surface used by consumers.
//   - Analysis is the corrected, resized surface used by vision consumers.
type State struct {
	Schema          string  `json:"schema"`
	TimestampUnixMS int64   `json:"timestamp_unix_ms"`
	Frame           uint64  `json:"frame"`
	SampleStride    int     `json:"sample_stride"`
	Source          Metrics `json:"source"`
	Processed       Metrics `json:"processed"`
	Analysis        Metrics `json:"analysis"`
	Capture         Capture `json:"capture"`
}

func (state State) Validate() error {
	if state.Schema != Schema {
		return fmt.Errorf("unsupported quality schema %q", state.Schema)
	}
	if state.TimestampUnixMS <= 0 {
		return errors.New("quality state missing timestamp")
	}
	if state.Frame == 0 || state.SampleStride <= 0 || state.SampleStride > 64 {
		return errors.New("quality state missing sampling identity")
	}
	for name, metrics := range map[string]Metrics{
		"source": state.Source, "processed": state.Processed, "analysis": state.Analysis,
	} {
		if err := metrics.validate(); err != nil {
			return fmt.Errorf("invalid %s metrics: %w", name, err)
		}
	}
	if state.Capture.Format == "" || state.Capture.Orientation == "" {
		return errors.New("quality state missing capture identity")
	}
	return nil
}

func (metrics Metrics) validate() error {
	if metrics.Width < 3 || metrics.Height < 3 || metrics.Samples == 0 {
		return errors.New("missing dimensions or samples")
	}
	for name, value := range map[string]float64{
		"luma": metrics.Luma, "content_luma": metrics.ContentLuma,
		"laplacian_variance": metrics.LaplacianVariance, "edge": metrics.Edge,
		"black_ratio": metrics.BlackRatio, "white_ratio": metrics.WhiteRatio,
	} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("%s is not finite", name)
		}
	}
	if metrics.Luma < 0 || metrics.Luma > 255 || metrics.ContentLuma < 0 || metrics.ContentLuma > 255 {
		return errors.New("luma is outside the NV12 range")
	}
	if metrics.LaplacianVariance < 0 || metrics.Edge < 0 {
		return errors.New("detail metrics are negative")
	}
	if metrics.BlackRatio < 0 || metrics.BlackRatio > 1 || metrics.WhiteRatio < 0 || metrics.WhiteRatio > 1 {
		return errors.New("clipping ratio is outside [0,1]")
	}
	if metrics.SignalInformative != signalInformative(metrics.BlackRatio, metrics.WhiteRatio, metrics.LaplacianVariance, metrics.Edge) {
		return errors.New("signal_informative is inconsistent with clipping and detail metrics")
	}
	if metrics.Spatial.Columns != SpatialColumns || metrics.Spatial.Rows != SpatialRows || len(metrics.Spatial.Regions) != SpatialRegionCount {
		return fmt.Errorf("spatial grid must be exactly %dx%d", SpatialColumns, SpatialRows)
	}
	var regionSamples uint64
	for index, region := range metrics.Spatial.Regions {
		expectedX := index % SpatialColumns
		expectedY := index / SpatialColumns
		if region.X != expectedX || region.Y != expectedY {
			return fmt.Errorf("spatial region %d is not row-major: got=(%d,%d) want=(%d,%d)", index, region.X, region.Y, expectedX, expectedY)
		}
		if err := region.validate(); err != nil {
			return fmt.Errorf("invalid spatial region (%d,%d): %w", region.X, region.Y, err)
		}
		regionSamples += region.Samples
	}
	if regionSamples != metrics.Samples {
		return fmt.Errorf("spatial samples=%d differ from global samples=%d", regionSamples, metrics.Samples)
	}
	return nil
}

func (region Region) validate() error {
	values := map[string]float64{
		"luma": region.Luma, "content_luma": region.ContentLuma,
		"laplacian_variance": region.LaplacianVariance, "edge": region.Edge,
		"black_ratio": region.BlackRatio, "white_ratio": region.WhiteRatio,
	}
	for name, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("%s is not finite", name)
		}
	}
	if region.Samples == 0 {
		for name, value := range values {
			if value != 0 {
				return fmt.Errorf("empty region has non-zero %s", name)
			}
		}
		if region.SignalInformative {
			return errors.New("empty region is informative")
		}
		return nil
	}
	if region.Luma < 0 || region.Luma > 255 || region.ContentLuma < 0 || region.ContentLuma > 255 {
		return errors.New("luma is outside the NV12 range")
	}
	if region.LaplacianVariance < 0 || region.Edge < 0 {
		return errors.New("detail metrics are negative")
	}
	if region.BlackRatio < 0 || region.BlackRatio > 1 || region.WhiteRatio < 0 || region.WhiteRatio > 1 {
		return errors.New("clipping ratio is outside [0,1]")
	}
	if region.SignalInformative != signalInformative(region.BlackRatio, region.WhiteRatio, region.LaplacianVariance, region.Edge) {
		return errors.New("signal_informative is inconsistent with clipping and detail metrics")
	}
	return nil
}

func signalInformative(blackRatio, whiteRatio, laplacianVariance, edge float64) bool {
	return (blackRatio < 0.98 && whiteRatio < 0.98) || laplacianVariance >= 1 || edge >= 0.25
}

func Decode(payload []byte) (State, error) {
	var state State
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return State{}, fmt.Errorf("decode quality state: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return State{}, errors.New("decode quality state: multiple JSON values")
		}
		return State{}, fmt.Errorf("decode quality state trailer: %w", err)
	}
	if err := state.Validate(); err != nil {
		return State{}, err
	}
	return state, nil
}

func ReadFile(path string) (State, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return State{}, err
	}
	return Decode(payload)
}

// WriteFile atomically publishes ephemeral control-plane state. The file lives
// under a runtime directory; storage writeback would add capture latency without
// strengthening the atomic-reader contract.
func WriteFile(path string, state State) error {
	if err := state.Validate(); err != nil {
		return err
	}
	directory := filepath.Dir(path)
	tmp, err := os.CreateTemp(directory, ".video-quality-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o640); err != nil {
		_ = tmp.Close()
		return err
	}
	encoder := json.NewEncoder(tmp)
	if err := encoder.Encode(state); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
