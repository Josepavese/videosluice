//go:build linux

package main

import (
	"context"
	in "github.com/Josepavese/videosluice/frameingress"
)

// FrameMetadata is bounded by the already admitted source/publication set.
// Copy owners first: Retire takes capture.mu before unregistering its owner.
func (b *nativeFrameBootstrap) FrameMetadata(ctx context.Context) map[string]map[string]in.DeliveryMetadata {
	b.capturesMu.Lock()
	captures := make([]*nativeFrameCapture, 0, len(b.captures))
	for _, capture := range b.captures {
		captures = append(captures, capture)
	}
	b.capturesMu.Unlock()
	result := make(map[string]map[string]in.DeliveryMetadata, len(captures))
	for _, capture := range captures {
		capture.mu.Lock()
		if !capture.retired {
			streams := make(map[string]in.DeliveryMetadata, len(capture.publications))
			for name, publication := range capture.publications {
				if metadata, err := publication.Metadata(ctx); err == nil {
					streams[name] = metadata
				}
			}
			result[capture.binding.Reference().Source] = streams
		}
		capture.mu.Unlock()
	}
	return result
}
