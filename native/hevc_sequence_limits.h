#ifndef VIDEO_BROKER_HEVC_SEQUENCE_LIMITS_H
#define VIDEO_BROKER_HEVC_SEQUENCE_LIMITS_H

#include <stdint.h>
#include "h26x_bootstrap.h"

/* Allocation-relevant HEVC sequence prefix, not a software decoder. The
 * native SDK still validates the full bitstream. Never let an SPS request a
 * larger/different surface, chroma depth or DPB than the admitted capture.
 * No heap, retained access unit, or timestamp matching is involved. */
typedef struct {
	const unsigned char *data;
	size_t size, position;
	unsigned int zeros, remaining, byte;
	bool invalid;
} vb_hevc_bits;

static inline uint32_t vb_hevc_read(vb_hevc_bits *bits, unsigned int count) {
	uint32_t result = 0;
	if (count > 32) { bits->invalid = true; return 0; }
	while (count-- && !bits->invalid) {
		if (bits->remaining == 0) {
			if (bits->position == bits->size) { bits->invalid = true; break; }
			unsigned int byte = bits->data[bits->position++];
			if (bits->zeros == 2 && byte == 3) {
				if (bits->position == bits->size || bits->data[bits->position] > 3) {
					bits->invalid = true; break;
				}
				bits->zeros = 0;
				byte = bits->data[bits->position++];
			}
			bits->zeros = byte == 0 ? (bits->zeros < 2 ? bits->zeros + 1 : 2) : 0;
			bits->byte = byte;
			bits->remaining = 8;
		}
		result = (result << 1) | ((bits->byte >> --bits->remaining) & 1U);
	}
	return result;
}

static inline uint32_t vb_hevc_ue(vb_hevc_bits *bits) {
	unsigned int zeros = 0;
	while (!bits->invalid && vb_hevc_read(bits, 1) == 0) {
		if (++zeros >= 32) { bits->invalid = true; return 0; }
	}
	if (bits->invalid) return 0;
	return ((UINT32_C(1) << zeros) - 1U) + vb_hevc_read(bits, zeros);
}

static inline void vb_hevc_skip(vb_hevc_bits *bits, unsigned int count) {
	while (count && !bits->invalid) {
		unsigned int part = count > 32 ? 32 : count;
		(void)vb_hevc_read(bits, part);
		count -= part;
	}
}

typedef struct {
	uint32_t coded_width, coded_height, dpb_frames, reorder_frames;
} vb_hevc_sequence;

static inline bool vb_hevc_sequence_prefix(const unsigned char *data, size_t size,
	uint32_t width, uint32_t height, vb_hevc_sequence *out) {
	if (data == NULL || out == NULL || size < 2 || width < 2 || height < 2 ||
	    width > 8192 || height > 8192 || (width & 1U) || (height & 1U)) return false;
	vb_hevc_bits bits = {.data = data, .size = size};
	(void)vb_hevc_read(&bits, 4); /* sps_video_parameter_set_id */
	unsigned int sublayers = vb_hevc_read(&bits, 3);
	(void)vb_hevc_read(&bits, 1);
	if (sublayers > 6) return false;
	/* profile_tier_level: general profile (88 bits) + level (8 bits),
	 * then each explicitly present sublayer profile/level. */
	vb_hevc_skip(&bits, 96);
	unsigned int profile[6] = {0}, level[6] = {0};
	for (unsigned int i = 0; i < sublayers; ++i) {
		profile[i] = vb_hevc_read(&bits, 1);
		level[i] = vb_hevc_read(&bits, 1);
	}
	if (sublayers) vb_hevc_skip(&bits, 2 * (8 - sublayers));
	for (unsigned int i = 0; i < sublayers; ++i) {
		if (profile[i]) vb_hevc_skip(&bits, 88);
		if (level[i]) vb_hevc_skip(&bits, 8);
	}
	if (vb_hevc_ue(&bits) > 15 || vb_hevc_ue(&bits) != 1) return false;
	vb_hevc_sequence sequence = {0};
	sequence.coded_width = vb_hevc_ue(&bits);
	sequence.coded_height = vb_hevc_ue(&bits);
	if (bits.invalid || sequence.coded_width < width || sequence.coded_height < height ||
	    sequence.coded_width > ((width + 63U) & ~63U) ||
	    sequence.coded_height > ((height + 63U) & ~63U)) return false;
	uint32_t left = 0, right = 0, top = 0, bottom = 0;
	if (vb_hevc_read(&bits, 1)) {
		left = vb_hevc_ue(&bits); right = vb_hevc_ue(&bits);
		top = vb_hevc_ue(&bits); bottom = vb_hevc_ue(&bits);
	}
	if (bits.invalid || (uint64_t)width + 2ULL * (left + (uint64_t)right) != sequence.coded_width ||
	    (uint64_t)height + 2ULL * (top + (uint64_t)bottom) != sequence.coded_height) return false;
	if (vb_hevc_ue(&bits) != 0 || vb_hevc_ue(&bits) != 0 || vb_hevc_ue(&bits) > 12) return false;
	unsigned int first = vb_hevc_read(&bits, 1) ? 0 : sublayers;
	for (unsigned int i = first; i <= sublayers; ++i) {
		uint32_t dpb_minus_one = vb_hevc_ue(&bits);
		uint32_t reorder = vb_hevc_ue(&bits);
		(void)vb_hevc_ue(&bits); /* max_latency_increase_plus1 */
		if (bits.invalid || dpb_minus_one > 15 || reorder > dpb_minus_one) return false;
		if (dpb_minus_one + 1 > sequence.dpb_frames) sequence.dpb_frames = dpb_minus_one + 1;
		if (reorder > sequence.reorder_frames) sequence.reorder_frames = reorder;
	}
	if (bits.invalid) return false;
	*out = sequence;
	return true;
}

typedef struct {
	bool seen_sequence;
	vb_hevc_sequence sequence;
} vb_hevc_sequence_guard;

/* 1 = bounded sequence/headers accepted; 0 = VCL before any valid SPS;
 * -1 = malformed or outside the admitted geometry/depth/DPB. Inspect EVERY
 * SPS, including replacement headers in an already running camera stream.
 * Publish guard state only after the entire access unit passed inspection. */
static inline int vb_hevc_guard_access_unit(vb_hevc_sequence_guard *guard,
	const unsigned char *data, size_t size, uint32_t width, uint32_t height) {
	if (guard == NULL || data == NULL || size == 0) return -1;
	vb_hevc_sequence_guard next_guard = *guard;
	size_t prefix = 0;
	size_t offset = video_broker_h26x_nal_start(data, size, 0, &prefix);
	if (offset == size) return -1;
	for (size_t i = 0; i < offset; ++i) if (data[i] != 0) return -1;
	bool vcl_without_sequence = false;
	while (offset < size) {
		size_t next_prefix;
		size_t next = video_broker_h26x_nal_start(data, size, offset + prefix, &next_prefix);
		if (next - offset < prefix + 2) return -1;
		const unsigned char *nal = data + offset + prefix;
		size_t nal_size = next - offset - prefix;
		unsigned int type = (nal[0] >> 1) & 63U;
		/* Only the base layer/8-bit 4:2:0 provider is qualified here. */
		if ((nal[0] & 0x81U) || (nal[1] >> 3) != 0 || (nal[1] & 7U) == 0) return -1;
		if (type == 32) {
			vb_hevc_bits vps = {.data = nal + 2, .size = nal_size - 2};
			vb_hevc_skip(&vps, 6);
			if (vb_hevc_read(&vps, 6) != 0 || vb_hevc_read(&vps, 3) > 6 || vps.invalid) return -1;
		} else if (type == 33) {
			if (!vb_hevc_sequence_prefix(nal + 2, nal_size - 2, width, height, &next_guard.sequence)) return -1;
			next_guard.seen_sequence = true;
		} else if (type <= 31 && !next_guard.seen_sequence) {
			vcl_without_sequence = true;
		}
		offset = next;
		prefix = next_prefix;
	}
	if (vcl_without_sequence) return 0;
	*guard = next_guard;
	return 1;
}

#endif
