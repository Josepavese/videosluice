#include <assert.h>
#include <stdint.h>

#include "stable_cadence.h"
#include "frame_observation.h"

static void observe_steady_frames(
	video_broker_stable_cadence_t *cadence,
	uint64_t *now_us,
	int frame_count,
	uint64_t interval_us
) {
	for (int frame = 0; frame < frame_count; ++frame) {
		*now_us += interval_us;
		(void)video_broker_stable_cadence_observe(cadence, *now_us);
	}
}

int main(void) {
	video_broker_stable_cadence_t cadence;
	uint64_t now_us = 1000000U;
	int analysis_accumulator = 0;
	int archive_accumulator = 0;
	int analysis_frames = 0;
	int archive_frames = 0;
	assert(video_broker_frame_observation_due(1, 30, 30, 1000000U, 0));
	assert(!video_broker_frame_observation_due(2, 30, 30, 1033333U, 1000000U));
	assert(video_broker_frame_observation_due(2, 30, 30, 2000000U, 1000000U));
	assert(!video_broker_frame_observation_due(0, 30, 30, 2000000U, 1000000U));
	assert(!video_broker_frame_observation_due(2, 900, 30, 2000000U, 1000000U));
	assert(video_broker_frame_observation_due(2, 900, 30, 31000000U, 1000000U));

	/* Some UVC firmware publishes a few bootstrap access units, pauses while
	 * its encoder settles, then produces a stable stream. The discontinuity is
	 * excluded from a new frame window after the configured stall deadline. */
	video_broker_stable_cadence_init(&cadence, 30, 30, 2000000U);
	observe_steady_frames(&cadence, &now_us, 4, 40000U);
	now_us += 2080000U;
	assert(!video_broker_stable_cadence_observe(&cadence, now_us));
	assert(cadence.settling_resets == 1U);
	/* A second, non-fatal firmware rearm interval can immediately follow the
	 * large pause. It remains part of the conservative admission average, but
	 * must not bias the steady scheduling clock for the lifetime of the epoch. */
	observe_steady_frames(&cadence, &now_us, 1, 80000U);
	observe_steady_frames(&cadence, &now_us, 28, 40000U);
	assert(cadence.frames == 30U);
	assert(cadence.observed_fps > 24.16L && cadence.observed_fps < 24.18L);
	assert(cadence.effective_fps == 25);
	assert(!video_broker_stable_cadence_meets_expected(&cadence));

	/* A 25 fps admitted source supplies independent 20 and 15 fps consumers
	 * exactly; neither inherits the camera's inaccurate declared 30 fps. */
	for (int frame = 0; frame < cadence.effective_fps; ++frame) {
		analysis_frames += video_broker_cadence_due(
			&analysis_accumulator, 20, cadence.effective_fps) ? 1 : 0;
		archive_frames += video_broker_cadence_due(
			&archive_accumulator, 15, cadence.effective_fps) ? 1 : 0;
	}
	assert(analysis_frames == 20);
	assert(archive_frames == 15);

	/* A slow source is available but has not met its performance objective. */
	now_us = 1000000U;
	video_broker_stable_cadence_init(&cadence, 30, 30, 2000000U);
	observe_steady_frames(&cadence, &now_us, 30, 50000U);
	assert(cadence.observed_fps > 19.99L && cadence.observed_fps < 20.01L);
	assert(!video_broker_stable_cadence_meets_expected(&cadence));

	/* A later discontinuity invalidates the partial admission window and must
	 * be followed by a complete new stable sample. */
	now_us += cadence.maximum_gap_us;
	assert(!video_broker_stable_cadence_observe(&cadence, now_us));
	assert(cadence.frames == 1U);
	assert(cadence.effective_fps == 0);
	assert(!video_broker_stable_cadence_meets_expected(&cadence));

	/* Admission needs three fresh decoded frames, independent of the target.
	 * Even one frame per second is valid within the actual capture deadline. */
	for (int requested = 25; requested <= 30; requested += 5) {
		for (int actual = 1; actual <= 10; actual += 9) {
			now_us = 1000000U;
			video_broker_stable_cadence_init(&cadence, requested, 3, 6000000U);
			observe_steady_frames(&cadence, &now_us, 2, 1000000U / (uint64_t)actual);
			assert(cadence.effective_fps == 0);
			now_us += 1000000U / (uint64_t)actual;
			assert(video_broker_stable_cadence_observe(&cadence, now_us));
			assert(cadence.effective_fps == actual);
			assert(!video_broker_stable_cadence_meets_expected(&cadence));
			/* Recovery updates the scheduling clock in the same epoch. */
			observe_steady_frames(&cadence, &now_us, 40, 1000000U / (uint64_t)requested);
			assert(cadence.effective_fps == requested);
			assert(video_broker_stable_cadence_meets_expected(&cadence));
			observe_steady_frames(&cadence, &now_us, 40, 100000U);
			assert(cadence.effective_fps == 10);
			assert(!video_broker_stable_cadence_meets_expected(&cadence));
		}
	}

	return 0;
}
