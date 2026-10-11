// Package routing defines the current-only runtime routing contract for the
// standalone video broker. Operators select opaque option IDs; physical
// devices, codecs, accelerators and arbitrary media values remain owned by the
// prevalidated catalog.
package routing

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/Josepavese/videosluice/config"
)

const (
	CatalogSchema = "video-broker.routing-catalog.v1"
	PlanSchema    = "video-broker.routing-plan.v2"
	maxFileSize   = 2 * 1024 * 1024
)

var (
	idPattern       = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,63}$`)
	revisionPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// Catalog is central/device-owned and integrity-bound to its revision. The
// deployment boundary authenticates the document; its source definitions and
// stream options are the complete authority boundary for runtime changes.
type Catalog struct {
	Schema   string                         `json:"schema"`
	Revision string                         `json:"revision"`
	Sources  map[string]config.SourceConfig `json:"sources"`
	// Additional qualified acquisition modes. The source definition itself
	// is the authoritative "default" mode, even on fixed-mode sources.
	SourceModes       map[string]map[string]SourceMode `json:"source_modes,omitempty"`
	Streams           map[string]StreamCatalog         `json:"streams"`
	DefaultSelections map[string]string                `json:"default_selections"`
	Runtime           config.RuntimeConfig             `json:"runtime"`
}

// StreamCatalog fixes the logical endpoint while offering bounded media/source
// combinations. Option IDs are intentionally opaque to the core; an integrator
// may attach localized labels outside this standalone contract.
type StreamCatalog struct {
	Type         string                  `json:"type"`
	Socket       string                  `json:"socket,omitempty"`
	SharedMemory string                  `json:"shared_memory,omitempty"`
	NotifySocket string                  `json:"notify_socket,omitempty"`
	Options      map[string]StreamOption `json:"options"`
}

type StreamOption struct {
	SourceRef   string `json:"source_ref"`
	Codec       string `json:"codec,omitempty"`
	PixelFormat string `json:"pixel_format,omitempty"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	FPS         int    `json:"fps"`
	BitrateKbps int    `json:"bitrate_kbps,omitempty"`
	Content     string `json:"content,omitempty"`
	RateControl string `json:"rate_control,omitempty"`
}

// Plan is the only operator-persisted document. Revision is a monotonic CAS
// generation; CatalogRevision prevents applying a choice after central policy
// has revoked or changed an option. StreamGenerations are derived and advanced
// by Store from the materialized graph delta, never accepted as apply input.
// A daemon startup may explicitly reconcile a trusted newer catalog when all
// opaque active and rollback choices still exist; that atomic boundary advances
// the plan revision and every stream generation before workers are started.
type Plan struct {
	Schema             string            `json:"schema"`
	CatalogRevision    string            `json:"catalog_revision"`
	Revision           uint64            `json:"revision"`
	Selections         map[string]string `json:"selections"`
	StreamGenerations  map[string]uint64 `json:"stream_generations"`
	RollbackSelections map[string]string `json:"rollback_selections,omitempty"`
	// Only explicit alternatives are persisted; an absent entry selects the
	// catalog's base source definition. These are operator choices, not raw
	// capture parameters, and remain saved while a source has no consumers.
	SourceSelections         map[string]string `json:"source_selections,omitempty"`
	RollbackSourceSelections map[string]string `json:"rollback_source_selections,omitempty"`
}

func LoadCatalog(path string) (Catalog, error) {
	var catalog Catalog
	if err := loadStrict(path, &catalog); err != nil {
		return Catalog{}, fmt.Errorf("load routing catalog: %w", err)
	}
	return NormalizeCatalog(catalog)
}

func LoadPlan(path string) (Plan, error) {
	var plan Plan
	if err := loadStrict(path, &plan); err != nil {
		return Plan{}, fmt.Errorf("load routing plan: %w", err)
	}
	if err := ValidatePlanEnvelope(plan); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func loadStrict(path string, target any) error {
	path = strings.TrimSpace(path)
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) == "/" {
		return errors.New("path must be absolute and non-root")
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return decodeStrict(file, target)
}

func decodeStrict(reader io.Reader, target any) error {
	body, err := io.ReadAll(io.LimitReader(reader, maxFileSize+1))
	if err != nil {
		return err
	}
	if len(body) > maxFileSize {
		return fmt.Errorf("document exceeds %d bytes", maxFileSize)
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("document contains multiple JSON values")
		}
		return err
	}
	return nil
}

func NormalizeCatalog(catalog Catalog) (Catalog, error) {
	if catalog.Schema != CatalogSchema {
		return Catalog{}, fmt.Errorf("unsupported routing catalog schema=%q", catalog.Schema)
	}
	if !revisionPattern.MatchString(catalog.Revision) {
		return Catalog{}, errors.New("routing catalog revision must be a canonical sha256 reference")
	}
	if len(catalog.Sources) == 0 || len(catalog.Streams) == 0 {
		return Catalog{}, errors.New("routing catalog requires sources and streams")
	}
	runtime, err := config.NormalizeRuntime(catalog.Runtime)
	if err != nil {
		return Catalog{}, fmt.Errorf("routing catalog runtime: %w", err)
	}
	catalog.Runtime = runtime

	normalizedSources := make(map[string]config.SourceConfig, len(catalog.Sources))
	identityOwners := make(map[string]string, len(catalog.Sources))
	physicalOwners := make(map[string]string, len(catalog.Sources))
	statePathOwners := make(map[string]string, len(catalog.Sources)*2)
	for sourceID, source := range catalog.Sources {
		normalized, err := config.NormalizeSource(sourceID, source)
		if err != nil {
			return Catalog{}, fmt.Errorf("routing catalog source %q: %w", sourceID, err)
		}
		if owner, exists := identityOwners[normalized.Identity]; exists {
			return Catalog{}, fmt.Errorf("routing catalog sources %q and %q share identity", owner, sourceID)
		}
		identityOwners[normalized.Identity] = sourceID
		physical := physicalSourceKey(normalized)
		if owner, exists := physicalOwners[physical]; exists {
			return Catalog{}, fmt.Errorf("routing catalog sources %q and %q share a physical source", owner, sourceID)
		}
		physicalOwners[physical] = sourceID
		for _, path := range []string{normalized.Processing.Quality.StatePath, normalized.Processing.Quality.AdmissionPath} {
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}
			path = filepath.Clean(path)
			if owner, exists := statePathOwners[path]; exists {
				return Catalog{}, fmt.Errorf("routing catalog sources %q and %q share state path %s", owner, sourceID, path)
			}
			statePathOwners[path] = sourceID
		}
		normalizedSources[sourceID] = normalized
	}
	catalog.Sources = normalizedSources
	if err := normalizeSourceModes(&catalog); err != nil {
		return Catalog{}, err
	}

	normalizedStreams := make(map[string]StreamCatalog, len(catalog.Streams))
	endpointOwners := make(map[string]string, len(catalog.Streams)*3)
	referencedSources := make(map[string]bool, len(catalog.Sources))
	for streamID, stream := range catalog.Streams {
		if !idPattern.MatchString(streamID) {
			return Catalog{}, fmt.Errorf("invalid routing stream id=%q", streamID)
		}
		stream.Type = strings.ToLower(strings.TrimSpace(stream.Type))
		if len(stream.Options) == 0 {
			return Catalog{}, fmt.Errorf("routing stream %q requires options", streamID)
		}
		for label, path := range map[string]string{
			"socket": stream.Socket, "shared_memory": stream.SharedMemory, "notify_socket": stream.NotifySocket,
		} {
			path = strings.TrimSpace(path)
			if path == "" {
				continue
			}
			if !filepath.IsAbs(path) || filepath.Clean(path) == "/" {
				return Catalog{}, fmt.Errorf("routing stream %q %s must be absolute", streamID, label)
			}
			path = filepath.Clean(path)
			if owner, exists := endpointOwners[path]; exists {
				return Catalog{}, fmt.Errorf("routing streams %q and %q share endpoint %s", owner, streamID, path)
			}
			endpointOwners[path] = streamID
		}
		normalizedOptions := make(map[string]StreamOption, len(stream.Options))
		optionOwners := make(map[StreamOption]string, len(stream.Options))
		optionIDs := make([]string, 0, len(stream.Options))
		for optionID := range stream.Options {
			optionIDs = append(optionIDs, optionID)
		}
		sort.Strings(optionIDs)
		for _, optionID := range optionIDs {
			option := stream.Options[optionID]
			if !idPattern.MatchString(optionID) {
				return Catalog{}, fmt.Errorf("routing stream %q has invalid option id=%q", streamID, optionID)
			}
			source, exists := catalog.Sources[option.SourceRef]
			if !exists || strings.TrimSpace(option.SourceRef) != option.SourceRef {
				return Catalog{}, fmt.Errorf("routing stream %q option %q references unknown source %q", streamID, optionID, option.SourceRef)
			}
			candidate, err := config.NormalizeStream(streamID, option.streamConfig(stream), source)
			if err != nil {
				for _, modeID := range sourceModeIDs(catalog, option.SourceRef) {
					modeSource, modeErr := sourceForMode(catalog, option.SourceRef, modeID)
					if modeErr != nil {
						continue
					}
					candidate, err = config.NormalizeStream(streamID, option.streamConfig(stream), modeSource)
					if err == nil {
						break
					}
				}
			}
			if err != nil {
				return Catalog{}, fmt.Errorf("routing stream %q option %q: %w", streamID, optionID, err)
			}
			normalizedOption := optionFromStream(candidate)
			if owner, exists := optionOwners[normalizedOption]; exists {
				return Catalog{}, fmt.Errorf(
					"routing stream %q options %q and %q materialize the same graph",
					streamID, owner, optionID,
				)
			}
			optionOwners[normalizedOption] = optionID
			normalizedOptions[optionID] = normalizedOption
			referencedSources[option.SourceRef] = true
		}
		stream.Options = normalizedOptions
		normalizedStreams[streamID] = stream
	}
	catalog.Streams = normalizedStreams
	for sourceID := range catalog.Sources {
		if !referencedSources[sourceID] {
			return Catalog{}, fmt.Errorf("routing catalog source %q has no stream option", sourceID)
		}
	}
	defaultPlan := Plan{
		Schema: PlanSchema, CatalogRevision: catalog.Revision, Revision: 1,
		Selections: catalog.DefaultSelections, StreamGenerations: initialStreamGenerations(catalog.Streams),
	}
	if _, err := materializeNormalized(catalog, defaultPlan); err != nil {
		return Catalog{}, fmt.Errorf("routing catalog default selections: %w", err)
	}
	return catalog, nil
}

func ValidatePlanEnvelope(plan Plan) error {
	if plan.Schema != PlanSchema {
		return fmt.Errorf("unsupported routing plan schema=%q", plan.Schema)
	}
	if !revisionPattern.MatchString(plan.CatalogRevision) {
		return errors.New("routing plan catalog_revision must be a canonical sha256 reference")
	}
	if plan.Revision == 0 {
		return errors.New("routing plan revision must be positive")
	}
	if len(plan.Selections) == 0 {
		return errors.New("routing plan requires selections")
	}
	if len(plan.StreamGenerations) == 0 {
		return errors.New("routing plan requires stream generations")
	}
	for streamID, generation := range plan.StreamGenerations {
		if !idPattern.MatchString(streamID) || generation == 0 {
			return fmt.Errorf("routing plan has invalid generation for stream %q", streamID)
		}
	}
	if plan.RollbackSelections != nil && len(plan.RollbackSelections) == 0 {
		return errors.New("routing plan rollback selections cannot be empty")
	}
	return nil
}

// DefaultPlan returns the catalog-owned boot plan. The first persisted user
// change advances from revision 1 to revision 2.
func DefaultPlan(catalog Catalog) (Plan, error) {
	normalized, err := NormalizeCatalog(catalog)
	if err != nil {
		return Plan{}, err
	}
	return Plan{
		Schema: PlanSchema, CatalogRevision: normalized.Revision, Revision: 1,
		Selections:        cloneSelections(normalized.DefaultSelections),
		StreamGenerations: initialStreamGenerations(normalized.Streams),
	}, nil
}

// Materialize converts only catalog-owned choices into a strict schema-v3
// broker configuration. It is a pure prepare step and performs no mutation.
func Materialize(catalog Catalog, plan Plan) (config.Config, error) {
	normalized, err := NormalizeCatalog(catalog)
	if err != nil {
		return config.Config{}, err
	}
	return materializeNormalized(normalized, plan)
}

// AdmissionProfiles returns the complete catalog-authorized media universe per
// physical source. The active Config intentionally contains only the selected
// graph; routed workers need this separate, immutable view to preconfigure
// bounded hardware surfaces without learning endpoint paths or application
// identities. Profiles are de-duplicated and sorted canonically.
func AdmissionProfiles(catalog Catalog) (map[string][]config.SourceRoleProfile, error) {
	normalized, err := NormalizeCatalog(catalog)
	if err != nil {
		return nil, err
	}
	profiles := make(map[string][]config.SourceRoleProfile, len(normalized.Sources))
	seen := make(map[string]map[config.SourceRoleProfile]bool, len(normalized.Sources))
	streamIDs := make([]string, 0, len(normalized.Streams))
	for streamID := range normalized.Streams {
		streamIDs = append(streamIDs, streamID)
	}
	sort.Strings(streamIDs)
	for _, streamID := range streamIDs {
		stream := normalized.Streams[streamID]
		optionIDs := make([]string, 0, len(stream.Options))
		for optionID := range stream.Options {
			optionIDs = append(optionIDs, optionID)
		}
		sort.Strings(optionIDs)
		for _, optionID := range optionIDs {
			option := stream.Options[optionID]
			profile := config.SourceRoleProfile{
				Role: stream.Type, Codec: option.Codec, PixelFormat: option.PixelFormat,
				Width: option.Width, Height: option.Height, FPS: option.FPS,
				BitrateKbps: option.BitrateKbps, Content: option.Content,
				RateControl: option.RateControl,
			}
			if seen[option.SourceRef] == nil {
				seen[option.SourceRef] = make(map[config.SourceRoleProfile]bool)
			}
			if seen[option.SourceRef][profile] {
				continue
			}
			seen[option.SourceRef][profile] = true
			profiles[option.SourceRef] = append(profiles[option.SourceRef], profile)
		}
	}
	for sourceID := range normalized.Sources {
		sort.Slice(profiles[sourceID], func(i, j int) bool {
			return sourceRoleProfileKey(profiles[sourceID][i]) <
				sourceRoleProfileKey(profiles[sourceID][j])
		})
	}
	return profiles, nil
}

func sourceRoleProfileKey(profile config.SourceRoleProfile) string {
	return fmt.Sprintf(
		"%s\x00%s\x00%s\x00%08d\x00%08d\x00%04d\x00%08d\x00%s\x00%s",
		profile.Role, profile.Codec, profile.PixelFormat, profile.Width,
		profile.Height, profile.FPS, profile.BitrateKbps, profile.Content,
		profile.RateControl,
	)
}

func materializeNormalized(catalog Catalog, plan Plan) (config.Config, error) {
	if err := ValidatePlanEnvelope(plan); err != nil {
		return config.Config{}, err
	}
	if plan.CatalogRevision != catalog.Revision {
		return config.Config{}, errors.New("routing plan catalog revision is stale")
	}
	if len(plan.Selections) != len(catalog.Streams) {
		return config.Config{}, errors.New("routing plan must select every catalog stream exactly once")
	}
	if len(plan.StreamGenerations) != len(catalog.Streams) {
		return config.Config{}, errors.New("routing plan must carry one generation for every catalog stream")
	}
	for streamID := range catalog.Streams {
		if plan.StreamGenerations[streamID] == 0 {
			return config.Config{}, fmt.Errorf("routing plan has no valid generation for stream %q", streamID)
		}
	}
	for streamID := range plan.StreamGenerations {
		if _, exists := catalog.Streams[streamID]; !exists {
			return config.Config{}, fmt.Errorf("routing plan carries generation for unknown stream %q", streamID)
		}
	}
	if plan.RollbackSelections != nil {
		if _, err := materializeSelections(catalog, plan.RollbackSelections, plan.RollbackSourceSelections); err != nil {
			return config.Config{}, fmt.Errorf("routing plan rollback selections: %w", err)
		}
	}
	return materializeSelections(catalog, plan.Selections, plan.SourceSelections)
}

func materializeSelections(catalog Catalog, selections map[string]string, sourceSelections ...map[string]string) (config.Config, error) {
	modes := map[string]string(nil)
	if len(sourceSelections) > 0 {
		modes = sourceSelections[0]
	}
	for sourceID, modeID := range modes {
		if modeID == "default" {
			return config.Config{}, errors.New("default source mode must be represented by an absent override")
		}
		if _, err := sourceForMode(catalog, sourceID, modeID); err != nil {
			return config.Config{}, err
		}
	}
	if len(selections) != len(catalog.Streams) {
		return config.Config{}, errors.New("must select every catalog stream exactly once")
	}
	streams := make(map[string]config.StreamConfig, len(catalog.Streams))
	sources := make(map[string]config.SourceConfig)
	streamIDs := make([]string, 0, len(catalog.Streams))
	for streamID := range catalog.Streams {
		streamIDs = append(streamIDs, streamID)
	}
	sort.Strings(streamIDs)
	for _, streamID := range streamIDs {
		stream := catalog.Streams[streamID]
		optionID, exists := selections[streamID]
		if !exists || strings.TrimSpace(optionID) != optionID {
			return config.Config{}, fmt.Errorf("routing plan has no valid selection for stream %q", streamID)
		}
		option, exists := stream.Options[optionID]
		if !exists {
			return config.Config{}, fmt.Errorf("routing plan stream %q selects unknown option %q", streamID, optionID)
		}
		streams[streamID] = option.streamConfig(stream)
		source, err := sourceForMode(catalog, option.SourceRef, modes[option.SourceRef])
		if err != nil {
			return config.Config{}, err
		}
		sources[option.SourceRef] = source
	}
	for streamID := range selections {
		if _, exists := catalog.Streams[streamID]; !exists {
			return config.Config{}, fmt.Errorf("routing plan selects unknown stream %q", streamID)
		}
	}
	return config.Normalize(config.Config{
		SchemaVersion: config.SchemaVersion,
		Sources:       sources,
		Streams:       streams,
		Runtime:       catalog.Runtime,
	})
}

func (option StreamOption) streamConfig(stream StreamCatalog) config.StreamConfig {
	return config.StreamConfig{
		Type: stream.Type, SourceRef: option.SourceRef, Socket: stream.Socket,
		SharedMemory: stream.SharedMemory, NotifySocket: stream.NotifySocket,
		Codec: option.Codec, PixelFormat: option.PixelFormat, Width: option.Width,
		Height: option.Height, FPS: option.FPS, BitrateKbps: option.BitrateKbps,
		Content: option.Content, RateControl: option.RateControl,
	}
}

func optionFromStream(stream config.StreamConfig) StreamOption {
	return StreamOption{
		SourceRef: stream.SourceRef, Codec: stream.Codec, PixelFormat: stream.PixelFormat,
		Width: stream.Width, Height: stream.Height, FPS: stream.FPS,
		BitrateKbps: stream.BitrateKbps, Content: stream.Content, RateControl: stream.RateControl,
	}
}

func physicalSourceKey(source config.SourceConfig) string {
	device := strings.TrimSpace(source.Device)
	if device != "" {
		device = filepath.Clean(device)
	}
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d", strings.ToLower(strings.TrimSpace(source.Transport)), device, strings.TrimSpace(source.Sensor), source.HostIndex)
}

func cloneSelections(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneStreamGenerations(input map[string]uint64) map[string]uint64 {
	output := make(map[string]uint64, len(input))
	for streamID, generation := range input {
		output[streamID] = generation
	}
	return output
}

func initialStreamGenerations(streams map[string]StreamCatalog) map[string]uint64 {
	generations := make(map[string]uint64, len(streams))
	for streamID := range streams {
		generations[streamID] = 1
	}
	return generations
}
