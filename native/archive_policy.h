#ifndef VIDEO_BROKER_ARCHIVE_POLICY_H
#define VIDEO_BROKER_ARCHIVE_POLICY_H

#include <stdbool.h>
#include <stddef.h>
#include <strings.h>

typedef enum {
	VIDEO_BROKER_ARCHIVE_ADAPTIVE = 0,
	VIDEO_BROKER_ARCHIVE_SOURCE = 1,
	VIDEO_BROKER_ARCHIVE_PROCESSED = 2,
} video_broker_archive_content_t;

static inline video_broker_archive_content_t video_broker_parse_archive_content(const char *value) {
	if (value != NULL && strcasecmp(value, "source") == 0) {
		return VIDEO_BROKER_ARCHIVE_SOURCE;
	}
	if (value != NULL && strcasecmp(value, "processed") == 0) {
		return VIDEO_BROKER_ARCHIVE_PROCESSED;
	}
	return VIDEO_BROKER_ARCHIVE_ADAPTIVE;
}

static inline const char *video_broker_archive_content_name(video_broker_archive_content_t content) {
	switch (content) {
	case VIDEO_BROKER_ARCHIVE_SOURCE:
		return "source";
	case VIDEO_BROKER_ARCHIVE_PROCESSED:
		return "processed";
	default:
		return "adaptive";
	}
}

// Source explicitly preserves camera pixels. Adaptive may preserve an encoded
// camera stream only when no broker-owned presentation processing is enabled.
// Once PAL can alter the image, ordinary consumers must observe the same
// processed pixels; a hidden native bypass would violate the stream contract.
static inline bool video_broker_archive_uses_native(
	video_broker_archive_content_t content,
	bool compressed_capture,
	bool presentation_processing_enabled
) {
	if (!compressed_capture || content == VIDEO_BROKER_ARCHIVE_PROCESSED) {
		return false;
	}
	if (content == VIDEO_BROKER_ARCHIVE_SOURCE) {
		return true;
	}
	return !presentation_processing_enabled;
}

#endif
