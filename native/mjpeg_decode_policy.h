#ifndef VIDEO_BROKER_MJPEG_DECODE_POLICY_H
#define VIDEO_BROKER_MJPEG_DECODE_POLICY_H

#include <stdbool.h>
#include <stdint.h>
#include <string.h>

typedef enum {
	VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO = 0,
	VIDEO_BROKER_MJPEG_ACCELERATOR_JPU = 1,
	VIDEO_BROKER_MJPEG_ACCELERATOR_CPU = 2,
} video_broker_mjpeg_accelerator_t;

typedef enum {
	VIDEO_BROKER_MJPEG_ASYNC_FAILURE_REJECT = 0,
	VIDEO_BROKER_MJPEG_ASYNC_FAILURE_FALLBACK_CPU = 1,
} video_broker_mjpeg_async_failure_action_t;

static inline bool video_broker_parse_mjpeg_accelerator(
	const char *value,
	video_broker_mjpeg_accelerator_t *parsed
) {
	if (value == NULL || parsed == NULL) {
		return false;
	}
	if (strcmp(value, "auto") == 0) {
		*parsed = VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO;
		return true;
	}
	if (strcmp(value, "jpu") == 0) {
		*parsed = VIDEO_BROKER_MJPEG_ACCELERATOR_JPU;
		return true;
	}
	if (strcmp(value, "cpu") == 0) {
		*parsed = VIDEO_BROKER_MJPEG_ACCELERATOR_CPU;
		return true;
	}
	return false;
}

static inline bool video_broker_parse_required_bool(
	const char *value,
	bool *parsed
) {
	if (value == NULL || parsed == NULL) {
		return false;
	}
	if (strcmp(value, "true") == 0) {
		*parsed = true;
		return true;
	}
	if (strcmp(value, "false") == 0) {
		*parsed = false;
		return true;
	}
	return false;
}

static inline bool video_broker_mjpeg_decode_policy_valid(
	video_broker_mjpeg_accelerator_t requested,
	bool requested_explicit,
	video_broker_mjpeg_accelerator_t initial_backend,
	bool initial_backend_explicit,
	bool allow_software_fallback
) {
	if (!requested_explicit || !initial_backend_explicit ||
	    initial_backend == VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO) {
		return false;
	}
	if (requested == VIDEO_BROKER_MJPEG_ACCELERATOR_CPU) {
		return initial_backend == VIDEO_BROKER_MJPEG_ACCELERATOR_CPU;
	}
	if (requested == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU) {
		return initial_backend == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU;
	}
	if (requested != VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO) {
		return false;
	}
	return initial_backend == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU ||
		(initial_backend == VIDEO_BROKER_MJPEG_ACCELERATOR_CPU && allow_software_fallback);
}

static inline video_broker_mjpeg_async_failure_action_t
video_broker_mjpeg_async_failure_action(
	video_broker_mjpeg_accelerator_t requested,
	bool allow_software_fallback
) {
	return requested == VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO && allow_software_fallback ?
		VIDEO_BROKER_MJPEG_ASYNC_FAILURE_FALLBACK_CPU :
		VIDEO_BROKER_MJPEG_ASYNC_FAILURE_REJECT;
}

static inline bool video_broker_mjpeg_decoder_output_stalled(
	bool camera_admitted,
	uint64_t first_unacknowledged_submission_us,
	uint64_t now_us,
	uint64_t timeout_us
) {
	return camera_admitted && first_unacknowledged_submission_us > 0 &&
		now_us >= first_unacknowledged_submission_us && timeout_us > 0 &&
		now_us - first_unacknowledged_submission_us >= timeout_us;
}

/*
 * The asynchronous decoder owns its own completion cadence.  Rate limiting on
 * capture ticks would first discard input access units and then discard a
 * second fraction of completed frames, multiplying the requested analysis
 * ratio.  Instead, submit every access unit and cap only completed outputs on
 * a monotonic timeline.  When the decoder is slower than the requested rate,
 * every completed frame remains due; when it is faster, the phase-preserving
 * deadline keeps the long-term rate bounded without accumulating bursts.
 */
static inline bool video_broker_mjpeg_async_output_due(
	uint64_t *next_due_us,
	int target_fps,
	uint64_t now_us
) {
	uint64_t period_us;
	uint64_t intervals;
	if (next_due_us == NULL || target_fps <= 0 || now_us == 0) {
		return false;
	}
	period_us = 1000000U / (uint64_t)target_fps;
	if (period_us == 0) {
		period_us = 1;
	}
	if (*next_due_us == 0) {
		*next_due_us = now_us > UINT64_MAX - period_us ? UINT64_MAX : now_us + period_us;
		return true;
	}
	if (now_us < *next_due_us) {
		return false;
	}
	intervals = (now_us - *next_due_us) / period_us + 1;
	if (intervals > (UINT64_MAX - *next_due_us) / period_us) {
		*next_due_us = now_us > UINT64_MAX - period_us ? UINT64_MAX : now_us + period_us;
	} else {
		*next_due_us += intervals * period_us;
	}
	return true;
}

#endif
