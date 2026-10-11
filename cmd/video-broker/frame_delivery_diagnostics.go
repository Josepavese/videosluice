package main

import (
	"context"
	"fmt"
	"time"

	wire "github.com/Josepavese/videosluice/framewire"
	"github.com/Josepavese/videosluice/internal/source"
)

// A verified frame still crosses two independent local operations. Attribute a
// failure to the operation which consumed the budget, without another copy,
// hash, goroutine, retry or log on the healthy per-frame path.
func publishObservedDelivery(ctx context.Context, h wire.Header, pixels []byte,
	publish, observe source.FrameDeliveryConsumer) (err error) {
	started := time.Now()
	phase := "before_publication"
	publishUS, observerUS := int64(-1), int64(-1)
	defer func() {
		if err == nil {
			return
		}
		budgetUS := int64(-1)
		if deadline, ok := ctx.Deadline(); ok {
			budgetUS = deadline.Sub(started).Microseconds()
		}
		err = fmt.Errorf("delivery_commit_progress phase=%s source_epoch=%d delivery_id=%d origin_sequence=%d publish_us=%d observer_us=%d elapsed_us=%d budget_us=%d: %w",
			phase, h.SourceEpoch, h.DeliveryID, h.OriginSequence, publishUS, observerUS,
			time.Since(started).Microseconds(), budgetUS, err)
	}()
	if err = ctx.Err(); err != nil {
		return err
	}
	phase = "atomic_publication"
	err = publish(ctx, h, pixels)
	publishUS = time.Since(started).Microseconds()
	if err != nil {
		return err
	}
	// A late successful atomic commit cannot be undone/retried. Do not invoke
	// another consumer with its expired budget or pretend this frame was timely.
	if err = ctx.Err(); err != nil {
		return err
	}
	phase = "runtime_observer"
	observerStarted := time.Now()
	err = observe(ctx, h, pixels)
	observerUS = time.Since(observerStarted).Microseconds()
	if err == nil {
		err = ctx.Err()
	}
	return err
}
