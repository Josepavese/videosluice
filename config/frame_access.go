package config

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Integrators name consumers and public stream IDs. They cannot supply native
// byte quotes, process identities, source epochs or hardware attestations.
// Source selection is always inherited from the stream's current routing.
type FrameAccessConfig struct {
	CopyProvider string              `json:"copy_provider"`
	Readers      []FrameReaderConfig `json:"readers"`
}

type FrameReaderConfig struct {
	ID             string `json:"id"`
	Stream         string `json:"stream"`
	DeliverySocket string `json:"delivery_socket"`
	ROISocket      string `json:"roi_socket"`
}

func cloneFrameAccess(access *FrameAccessConfig) *FrameAccessConfig {
	if access == nil {
		return nil
	}
	cloned := *access
	cloned.Readers = append([]FrameReaderConfig(nil), access.Readers...)
	return &cloned
}

func validateFrameAccess(access *FrameAccessConfig, streams map[string]StreamConfig, reserved ...string) error {
	if access == nil {
		return nil
	}
	// Explicit portable copying is independent of decode/encode acceleration;
	// an unsupported requested copy provider is not silently replaced by CPU.
	if access.CopyProvider != "cpu_nv12_copy" || len(access.Readers) == 0 || len(access.Readers) > 32 {
		return fmt.Errorf("frame_access: unsupported copy provider or reader count")
	}
	ids, paths := map[string]bool{}, map[string]bool{}
	for _, path := range reserved {
		if path != "" {
			paths[path] = true
		}
	}
	for _, stream := range streams {
		for _, p := range []string{stream.Socket, stream.SharedMemory, stream.NotifySocket} {
			if p != "" {
				paths[p] = true
			}
		}
	}
	for _, reader := range access.Readers {
		if !graphIDPattern.MatchString(reader.ID) || !graphIDPattern.MatchString(reader.Stream) || ids[reader.ID] {
			return fmt.Errorf("frame_access: invalid or duplicate reader id=%q", reader.ID)
		}
		ids[reader.ID] = true
		if streams != nil {
			stream, ok := streams[reader.Stream]
			if !ok || stream.Type != "analysis" || stream.PixelFormat != "nv12" {
				return fmt.Errorf("frame_access reader %q requires a named NV12 analysis stream", reader.ID)
			}
		}
		for _, p := range []string{reader.DeliverySocket, reader.ROISocket} {
			if !filepath.IsAbs(p) || filepath.Clean(p) != p || p == "/" || len(p) > 107 || strings.IndexByte(p, 0) >= 0 || paths[p] {
				return fmt.Errorf("frame_access reader %q has an invalid or shared endpoint", reader.ID)
			}
			paths[p] = true
		}
	}
	return nil
}
