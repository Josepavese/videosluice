/* Allocation-free JPEG frame envelope validation. No SDK/platform dependency.
 * This is a geometry boundary, NOT entropy validation or a DMA size receipt.
 * JPEG table-only streams, deferred dimensions and hierarchical images cannot
 * satisfy a complete frame-sized capture contract. */
#ifndef VIDEO_BROKER_JPEG_FRAME_HEADER_H
#define VIDEO_BROKER_JPEG_FRAME_HEADER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
	uint16_t width, height;
	uint8_t precision, components, coding_marker;
	uint8_t horizontal[4], vertical[4];
} video_broker_jpeg_frame_header;

static inline unsigned vb_jpeg_u16(const unsigned char *p) {
	return ((unsigned)p[0] << 8) | p[1];
}

static inline bool video_broker_jpeg_frame_inspect(const unsigned char *data,
	size_t size, video_broker_jpeg_frame_header *result) {
	video_broker_jpeg_frame_header frame = {0};
	uint8_t ids[4] = {0};
	size_t at = 2;
	bool saw_frame = false, saw_scan = false, entropy = false;
	if (result == NULL) return false;
	*result = frame;
	if (data == NULL || size < 4 || data[0] != 0xff || data[1] != 0xd8) return false;
	while (at < size) {
		if (entropy) {
			while (at < size && data[at] != 0xff) ++at;
			if (at == size) return false;
		}
		if (data[at++] != 0xff) return false;
		while (at < size && data[at] == 0xff) ++at;
		if (at == size) return false;
		unsigned marker = data[at++];
		if (entropy && (marker == 0 || (marker >= 0xd0 && marker <= 0xd7))) continue;
		entropy = false;
		if (marker == 0xd9) {
			if (!saw_frame || !saw_scan) return false;
			/* Allow capture-buffer padding, never a second encoded image. */
			while (at < size) {
				if (data[at] != 0 && data[at] != 0xff) return false;
				++at;
			}
			*result = frame;
			return true;
		}
		if (marker == 0 || marker == 0xd8 || (marker >= 0xd0 && marker <= 0xd7) ||
		    marker == 0xdc || marker == 0xde || marker == 0xdf) return false;
		if (marker == 1) continue; /* TEM is the other standalone marker. */
		if (size - at < 2) return false;
		size_t length = vb_jpeg_u16(data + at);
		if (length < 2 || length > size - at) return false;
		const unsigned char *p = data + at;
		if (marker >= 0xc0 && marker <= 0xcf && marker != 0xc4 &&
		    marker != 0xc8 && marker != 0xcc) {
			if (saw_frame || length < 8) return false;
			frame.precision = p[2];
			frame.height = (uint16_t)vb_jpeg_u16(p + 3);
			frame.width = (uint16_t)vb_jpeg_u16(p + 5);
			frame.components = p[7];
			frame.coding_marker = (uint8_t)marker;
			if (frame.width == 0 || frame.height == 0 || frame.precision == 0 ||
			    frame.precision > 16 || frame.components == 0 || frame.components > 4 ||
			    length != 8U + 3U * frame.components) return false;
			for (unsigned i = 0; i < frame.components; ++i) {
				ids[i] = p[8 + 3 * i];
				for (unsigned j = 0; j < i; ++j) if (ids[i] == ids[j]) return false;
				frame.horizontal[i] = p[9 + 3 * i] >> 4;
				frame.vertical[i] = p[9 + 3 * i] & 15;
				if (frame.horizontal[i] == 0 || frame.horizontal[i] > 4 ||
				    frame.vertical[i] == 0 || frame.vertical[i] > 4 || p[10 + 3 * i] > 3)
					return false;
			}
			saw_frame = true;
		} else if (marker == 0xda) {
			if (!saw_frame || length < 6 || p[2] == 0 || p[2] > frame.components ||
			    length != 6U + 2U * p[2]) return false;
			unsigned used = 0;
			for (unsigned i = 0; i < p[2]; ++i) {
				unsigned j;
				for (j = 0; j < frame.components && ids[j] != p[3 + 2 * i]; ++j) {}
				if (j == frame.components || (used & (1U << j)) != 0) return false;
				used |= 1U << j;
			}
			saw_scan = true;
			entropy = true;
		}
		at += length;
	}
	return false;
}

#endif
