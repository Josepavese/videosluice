#ifndef VIDEO_BROKER_H26X_BOOTSTRAP_H
#define VIDEO_BROKER_H26X_BOOTSTRAP_H

#include <stdbool.h>
#include <stddef.h>
#include <limits.h>
#include <string.h>

/* A codec instance owns this cache; zero it on creation/destruction, retain
 * it only while the same immutable encoder survives warmup or role cutover. */
typedef struct {
	unsigned char data[65536];
	size_t size;
	bool codec_bound;
	bool h265;
} video_broker_h26x_bootstrap_t;

static inline size_t video_broker_h26x_nal_start(
	const unsigned char *data, size_t size, size_t offset, size_t *prefix
) {
	for (; offset + 3 <= size; ++offset) {
		if (data[offset] != 0 || data[offset + 1] != 0) continue;
		if (data[offset + 2] == 1) {
			*prefix = 3;
			return offset;
		}
		if (offset + 4 <= size && data[offset + 2] == 0 && data[offset + 3] == 1) {
			*prefix = 4;
			return offset;
		}
	}
	*prefix = 0;
	return size;
}

static inline unsigned int video_broker_h26x_parameter_bit(
	bool h265, unsigned char header
) {
	unsigned int type = h265 ?
		(header >> 1) & 0x3fU : header & 0x1fU;
	if (h265) {
		return type >= 32 && type <= 34 ? 1U << (type - 32) : 0;
	}
	return type == 7 ? 2U : type == 8 ? 4U : 0;
}

/* Classify the first coded picture for loss recovery. CRA/open-GOP pictures
 * are deliberately NOT IDR: they can retain leading-picture dependencies.
 * Parameter-only access units may still feed the decoder while waiting for a
 * new IDR, but may never lift the output gate. No platform/driver assumptions. */
static inline int video_broker_h26x_first_picture(
	bool h265, const unsigned char *data, size_t size
) {
	size_t prefix, offset;
	if (data == NULL) return -1;
	offset = video_broker_h26x_nal_start(data, size, 0, &prefix);
	if (offset == size) return -1;
	while (offset < size) {
		size_t next_prefix, next = video_broker_h26x_nal_start(data, size, offset + prefix, &next_prefix);
		size_t minimum = h265 ? 2U : 1U;
		if (next - offset <= prefix + minimum || (data[offset + prefix] & 0x80U)) return -1;
		if (h265) {
			unsigned int type = (data[offset + prefix] >> 1) & 0x3fU;
			if (!(data[offset + prefix + 1] & 7U)) return -1;
			if (type <= 31) return type == 19 || type == 20 ? 2 : 1;
		} else {
			unsigned int type = data[offset + prefix] & 0x1fU;
			if (type >= 1 && type <= 5) return type == 5 ? 2 : 1;
		}
		offset = next;
		prefix = next_prefix;
	}
	return 0;
}

/* Preserve a complete family, not a GOP or a warmup picture. All callers
 * (V4L2/HBN, live/archive, H264/H265) use the same encoder boundary. No heap
 * allocation, hardware restart or extra frame is needed on a delivery switch.
 * Partial new families invalidate the cache rather than mixing generations. */
static inline int video_broker_h26x_bootstrap_copy(
	video_broker_h26x_bootstrap_t *cache, bool h265,
	const unsigned char *data, size_t size,
	unsigned char *stream, size_t capacity
) {
	size_t prefix, offset, config_size = 0;
	unsigned int seen = 0;
	unsigned int required = h265 ? 7U : 6U;
	bool idr = false;
	if (cache == NULL || data == NULL || stream == NULL ||
	    cache->size > sizeof(cache->data) || size > INT_MAX || size > capacity) return -1;
	if (!cache->codec_bound || cache->h265 != h265) {
		cache->size = 0;
		cache->codec_bound = true;
		cache->h265 = h265;
	}
	offset = video_broker_h26x_nal_start(data, size, 0, &prefix);
	while (offset < size) {
		size_t next_prefix, next = video_broker_h26x_nal_start(data, size, offset + prefix, &next_prefix);
		size_t minimum = h265 ? 2U : 1U;
		unsigned int bit, type;
		if (next - offset < prefix + minimum) {
			cache->size = 0;
			return -1;
		}
		type = h265 ?
			(data[offset + prefix] >> 1) & 0x3fU : data[offset + prefix] & 0x1fU;
		idr |= h265 ?
			type == 19 || type == 20 : type == 5;
		bit = video_broker_h26x_parameter_bit(h265, data[offset + prefix]);
		if (bit != 0) {
			if (next - offset > sizeof(cache->data) - config_size) {
				cache->size = 0;
				return -1;
			}
			config_size += next - offset;
			seen |= bit;
		}
		offset = next;
		prefix = next_prefix;
	}
	if (seen == required) {
		cache->size = 0;
		offset = video_broker_h26x_nal_start(data, size, 0, &prefix);
		while (offset < size) {
			size_t next_prefix, next = video_broker_h26x_nal_start(data, size, offset + prefix, &next_prefix);
			if (video_broker_h26x_parameter_bit(h265, data[offset + prefix]) != 0) {
				memcpy(cache->data + cache->size, data + offset, next - offset);
				cache->size += next - offset;
			}
			offset = next;
			prefix = next_prefix;
		}
	} else if (seen != 0) {
		cache->size = 0;
	}
	config_size = idr && seen == 0 ? cache->size : 0;
	if (config_size > capacity - size || config_size > (size_t)INT_MAX - size) return -1;
	/* HBN supplies the same input/output buffer. Shift before prefixing. */
	if (size != 0) memmove(stream + config_size, data, size);
	if (config_size != 0) memcpy(stream, cache->data, config_size);
	return (int)(config_size + size);
}

#endif
