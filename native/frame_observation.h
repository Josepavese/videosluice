#ifndef VIDEO_BROKER_FRAME_OBSERVATION_H
#define VIDEO_BROKER_FRAME_OBSERVATION_H

#include <stdbool.h>
#include <stdint.h>

/* Frame-count sampling must also refresh on elapsed time when a valid source
 * delivers below its nominal FPS. It never manufactures observations without
 * a real frame. The first usable frame supplies startup evidence immediately. */
static inline bool video_broker_frame_observation_due(
	uint64_t frame, int report_every, int nominal_fps,
	uint64_t now_us, uint64_t last_sample_us
) {
	if (frame == 0 || now_us == 0 || nominal_fps <= 0 || report_every <= 0) return false;
	if (last_sample_us == 0 || frame % (uint64_t)report_every == 0) return true;
	uint64_t interval_us = (uint64_t)report_every * 1000000U / (uint64_t)nominal_fps;
	if (interval_us < 1000000U) interval_us = 1000000U;
	return now_us >= last_sample_us && now_us - last_sample_us >= interval_us;
}

#endif
