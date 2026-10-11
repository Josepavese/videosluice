#ifndef VIDEO_BROKER_STABLE_CADENCE_H
#define VIDEO_BROKER_STABLE_CADENCE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define VIDEO_BROKER_STABLE_CADENCE_MAX_INTERVALS 31U

typedef struct {
	int expected_fps;
	int minimum_frames;
	uint64_t first_frame_us;
	uint64_t last_frame_us;
	uint64_t frames;
	uint64_t settling_resets;
	uint64_t maximum_gap_us;
	uint64_t intervals_us[VIDEO_BROKER_STABLE_CADENCE_MAX_INTERVALS];
	size_t interval_count;
	size_t next_interval;
	long double observed_fps;
	int effective_fps;
} video_broker_stable_cadence_t;

static inline uint64_t video_broker_stable_cadence_median_interval_us(
	const video_broker_stable_cadence_t *cadence
) {
	uint64_t sorted[VIDEO_BROKER_STABLE_CADENCE_MAX_INTERVALS];
	size_t count;

	if (cadence == NULL || cadence->interval_count == 0) {
		return 0;
	}
	count = cadence->interval_count;
	if (count > VIDEO_BROKER_STABLE_CADENCE_MAX_INTERVALS) {
		count = VIDEO_BROKER_STABLE_CADENCE_MAX_INTERVALS;
	}
	for (size_t i = 0; i < count; ++i) {
		size_t j = i;
		sorted[i] = cadence->intervals_us[i];
		while (j > 0 && sorted[j - 1] > sorted[j]) {
			uint64_t swap = sorted[j - 1];
			sorted[j - 1] = sorted[j];
			sorted[j] = swap;
			--j;
		}
	}
	return sorted[count / 2U];
}

static inline void video_broker_stable_cadence_init(
	video_broker_stable_cadence_t *cadence,
	int expected_fps,
	int minimum_frames,
	uint64_t maximum_gap_us
) {
	if (cadence == NULL) {
		return;
	}
	*cadence = (video_broker_stable_cadence_t){0};
	cadence->expected_fps = expected_fps > 0 ? expected_fps : 30;
	cadence->minimum_frames = minimum_frames > 1 ? minimum_frames : 2;
	cadence->maximum_gap_us = maximum_gap_us;
}

static inline bool video_broker_stable_cadence_observe(
	video_broker_stable_cadence_t *cadence,
	uint64_t now_us
) {
	uint64_t gap_us;
	uint64_t median_interval_us;
	long double observed_fps;
	uint64_t window_us = 0;
	int effective_fps;

	if (cadence == NULL || now_us == 0) {
		return false;
	}
	if (cadence->last_frame_us > 0) {
		if (now_us <= cadence->last_frame_us) {
			return false;
		}
		gap_us = now_us - cadence->last_frame_us;
		/* Availability uses the capture stall deadline, never nominal FPS. */
		if (gap_us >= cadence->maximum_gap_us) {
			cadence->first_frame_us = 0;
			cadence->frames = 0;
			cadence->interval_count = 0;
			cadence->next_interval = 0;
			cadence->observed_fps = 0.0L;
			cadence->effective_fps = 0;
			cadence->settling_resets++;
		} else {
			cadence->intervals_us[cadence->next_interval] = gap_us;
			cadence->next_interval = (cadence->next_interval + 1U) % VIDEO_BROKER_STABLE_CADENCE_MAX_INTERVALS;
			if (cadence->interval_count < VIDEO_BROKER_STABLE_CADENCE_MAX_INTERVALS) {
				cadence->interval_count++;
			}
		}
	}
	if (cadence->first_frame_us == 0) {
		cadence->first_frame_us = now_us;
	}
	cadence->last_frame_us = now_us;
	cadence->frames++;
	if (cadence->frames < (uint64_t)cadence->minimum_frames ||
	    cadence->last_frame_us <= cadence->first_frame_us) {
		return false;
	}
	for (size_t i = 0; i < cadence->interval_count; ++i) {
		window_us += cadence->intervals_us[i];
	}
	observed_fps = ((long double)cadence->interval_count * 1000000.0L) / (long double)window_us;
	cadence->observed_fps = observed_fps;
	/* Scheduling follows a bounded rolling median. It can recover when a slow
	 * source speeds up, without accumulating or duplicating any frames. */
	median_interval_us = video_broker_stable_cadence_median_interval_us(cadence);
	if (median_interval_us > 0) {
		effective_fps = (int)((1000000U + median_interval_us / 2U) / median_interval_us);
	} else {
		effective_fps = (int)(observed_fps + 0.5L);
	}
	if (effective_fps < 1) {
		effective_fps = 1;
	}
	if (effective_fps > cadence->expected_fps) {
		effective_fps = cadence->expected_fps;
	}
	cadence->effective_fps = effective_fps;
	return true;
}

static inline bool video_broker_stable_cadence_meets_expected(
	const video_broker_stable_cadence_t *cadence
) {
	return cadence != NULL && cadence->effective_fps > 0 &&
		cadence->observed_fps >= (long double)cadence->expected_fps * 0.90L;
}

static inline bool video_broker_cadence_due(
	int *accumulator,
	int target_fps,
	int source_fps
) {
	if (accumulator == NULL || target_fps <= 0 || source_fps <= 0) {
		return false;
	}
	if (target_fps >= source_fps) {
		return true;
	}
	*accumulator += target_fps;
	if (*accumulator < source_fps) {
		return false;
	}
	*accumulator -= source_fps;
	return true;
}

#endif
