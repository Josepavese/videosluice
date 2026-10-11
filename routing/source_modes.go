package routing

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/Josepavese/videosluice/config"
)

// SourceMode is one qualified combination, never a Cartesian product of
// enumerated camera capabilities. Device identity, controls, provider policy
// and resource/thermal limits cannot be changed through a mode selection.
type SourceMode struct {
	Format string `json:"format"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	FPS    int    `json:"fps"`
}

func normalizeSourceModes(catalog *Catalog) error {
	result := make(map[string]map[string]SourceMode, len(catalog.SourceModes))
	for id, modes := range catalog.SourceModes {
		if _, exists := catalog.Sources[id]; !exists {
			return fmt.Errorf("modes reference unknown source %q", id)
		}
		result[id] = make(map[string]SourceMode, len(modes))
		base := catalog.Sources[id]
		seen := map[SourceMode]bool{{Format: base.Format, Width: base.Width, Height: base.Height, FPS: base.FPS}: true}
		for modeID, mode := range modes {
			if !idPattern.MatchString(modeID) || modeID == "default" {
				return errors.New("invalid or reserved source mode id")
			}
			if mode.Format != strings.ToLower(strings.TrimSpace(mode.Format)) || mode.Format == "" || mode.Format == "auto" || mode.Width <= 0 || mode.Height <= 0 || mode.FPS <= 0 {
				return fmt.Errorf("source %q mode %q must be exact", id, modeID)
			}
			if seen[mode] {
				return fmt.Errorf("source %q has duplicate acquisition modes", id)
			}
			seen[mode] = true
			if _, err := config.NormalizeSource(id, applySourceMode(base, mode)); err != nil {
				return fmt.Errorf("source %q mode %q: %w", id, modeID, err)
			}
			result[id][modeID] = mode
		}
	}
	catalog.SourceModes = result
	return nil
}

func applySourceMode(source config.SourceConfig, mode SourceMode) config.SourceConfig {
	source.Format, source.Width, source.Height, source.FPS = mode.Format, mode.Width, mode.Height, mode.FPS
	source.ModePolicy = "exact"
	return source
}

func sourceForMode(catalog Catalog, id, modeID string) (config.SourceConfig, error) {
	source, exists := catalog.Sources[id]
	if !exists {
		return config.SourceConfig{}, fmt.Errorf("unknown acquisition source %q", id)
	}
	if modeID == "" || modeID == "default" {
		return source, nil
	}
	mode, exists := catalog.SourceModes[id][modeID]
	if !exists {
		return config.SourceConfig{}, fmt.Errorf("source %q selects unqualified mode %q", id, modeID)
	}
	return config.NormalizeSource(id, applySourceMode(source, mode))
}

func sourceModeIDs(catalog Catalog, id string) []string {
	ids := []string{"default"}
	for modeID := range catalog.SourceModes[id] {
		ids = append(ids, modeID)
	}
	sort.Strings(ids)
	return ids
}

// SourceModeChangeOnly is shared by delta validation and runtime admission.
// The materialized catalog is the authority for these four media fields;
// identity, controls, providers, safety policy and paths stay immutable.
func SourceModeChangeOnly(before, after config.SourceConfig) bool {
	before.Format, before.Width, before.Height, before.FPS, before.ModePolicy = after.Format, after.Width, after.Height, after.FPS, after.ModePolicy
	return reflect.DeepEqual(before, after)
}
