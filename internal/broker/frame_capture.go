package broker

import (
	"context"
	"errors"
	"fmt"
	"github.com/Josepavese/videosluice/internal/source"
	"time"
)

// The admitted frame domain supplies capture identity BEFORE command building.
// Broker delivery counters are not an authority for source capture epochs.
func prepareCaptureFrames(ctx context.Context, factory source.FrameCaptureFactory, plan *source.BrokerMuxPlan) (source.FrameCapture, error) {
	if factory == nil {
		return nil, nil
	}
	frames, err := factory(ctx, *plan)
	if err == source.ErrFrameCaptureNotRequested && frames == nil {
		return nil, nil // explicit encoded-only plan; no frame domain was acquired
	}
	if err == nil && (frames == nil || frames.SourceEpoch() == 0) {
		err = errors.New("frame source returned no admitted generation")
	}
	if err != nil {
		if frames != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err = errors.Join(err, frames.Retire(cleanupCtx))
		}
		return nil, fmt.Errorf("frame source prepare: %w", err)
	}
	plan.MediaEpoch = frames.SourceEpoch()
	return frames, nil
}
