package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/quality"
)

func TestQualityCaptureTracksAnalysisAttachmentWithoutIdleTimeout(t *testing.T) {
	for _, mode := range []string{"detached", "attached", "detached_during_sample"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			cfg := ingestConfig{captureWidth: 8, captureHeight: 8, qualityEvery: 1, qualityStride: 1,
				qualityEnabled: true, qualityState: filepath.Join(t.TempDir(), "quality.json"), inputFormat: "nv12", orientation: "normal"}
			// The owner starts without analysis; a later role carries its own geometry.
			role := &isolatedGStreamerRole{role: "analysis", cfg: ingestConfig{subWidth: 4, subHeight: 4}}
			var current atomic.Pointer[isolatedGStreamerRole]
			if mode != "detached" {
				current.Store(role)
			}
			requests := make(chan qualityRequest)
			done := make(chan struct{})
			go func() {
				defer close(done)
				select {
				case request := <-requests:
					if mode == "detached_during_sample" {
						current.Store(nil)
					}
					request.response <- measureNV12Luma(bytes.Repeat([]byte{64}, 4*4*3/2), 4, 4, 1)
				case <-ctx.Done():
				}
			}()
			frame := bytes.Repeat([]byte{64}, 8*8*3/2)
			err := forwardQualityCapture(ctx, bytes.NewReader(frame), bytes.NewReader(frame), cfg, time.Time{}, requests, current.Load)
			cancel()
			<-done
			if !errors.Is(err, io.EOF) {
				t.Fatalf("quality loop treated an intentional detach as failure: %v", err)
			}
			if mode != "attached" {
				if _, err := os.Stat(cfg.qualityState); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("detached/retired analysis published quality: %v", err)
				}
				return
			}
			state, err := quality.ReadFile(cfg.qualityState)
			if err != nil || state.Analysis.Width != 4 || state.Analysis.Height != 4 {
				t.Fatalf("attached role did not publish its real geometry: state=%+v error=%v", state, err)
			}
		})
	}
}
