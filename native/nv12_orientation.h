#ifndef VIDEO_BROKER_NV12_ORIENTATION_H
#define VIDEO_BROKER_NV12_ORIENTATION_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>

#include "nv12_resize.h"

typedef enum {
	VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL = 0,
	VIDEO_BROKER_VIDEO_ORIENTATION_ROTATE_180 = 1,
	VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_HORIZONTAL = 2,
	VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_VERTICAL = 3,
} video_broker_video_orientation_t;

static inline video_broker_video_orientation_t video_broker_parse_video_orientation(const char *value) {
	if (value == NULL || *value == '\0' || strcasecmp(value, "normal") == 0 || strcasecmp(value, "none") == 0 || strcasecmp(value, "identity") == 0) {
		return VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL;
	}
	if (strcasecmp(value, "rotate_180") == 0 || strcasecmp(value, "rotate-180") == 0 || strcasecmp(value, "180") == 0 ||
	    strcasecmp(value, "upside_down") == 0 || strcasecmp(value, "upside-down") == 0) {
		return VIDEO_BROKER_VIDEO_ORIENTATION_ROTATE_180;
	}
	if (strcasecmp(value, "mirror_horizontal") == 0 || strcasecmp(value, "mirror-horizontal") == 0 ||
	    strcasecmp(value, "hflip") == 0 || strcasecmp(value, "horizontal") == 0 || strcasecmp(value, "horiz") == 0) {
		return VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_HORIZONTAL;
	}
	if (strcasecmp(value, "mirror_vertical") == 0 || strcasecmp(value, "mirror-vertical") == 0 ||
	    strcasecmp(value, "vflip") == 0 || strcasecmp(value, "vertical") == 0 || strcasecmp(value, "vert") == 0) {
		return VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_VERTICAL;
	}
	return VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL;
}

static inline const char *video_broker_video_orientation_name(video_broker_video_orientation_t orientation) {
	switch (orientation) {
	case VIDEO_BROKER_VIDEO_ORIENTATION_ROTATE_180:
		return "rotate_180";
	case VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_HORIZONTAL:
		return "mirror_horizontal";
	case VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_VERTICAL:
		return "mirror_vertical";
	default:
		return "normal";
	}
}

static inline int video_broker_video_orientation_is_transform(video_broker_video_orientation_t orientation) {
	return orientation != VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL;
}

static inline size_t video_broker_nv12_frame_size(int width, int height) {
	return ((size_t)width * (size_t)height * 3) / 2;
}

static inline int video_broker_clamp_int(int value, int min_value, int max_value) {
	if (value < min_value) {
		return min_value;
	}
	if (value > max_value) {
		return max_value;
	}
	return value;
}

static inline void video_broker_orient_xy(
	int x,
	int y,
	int width,
	int height,
	video_broker_video_orientation_t orientation,
	int *out_x,
	int *out_y
) {
	switch (orientation) {
	case VIDEO_BROKER_VIDEO_ORIENTATION_ROTATE_180:
		*out_x = width - 1 - x;
		*out_y = height - 1 - y;
		break;
	case VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_HORIZONTAL:
		*out_x = width - 1 - x;
		*out_y = y;
		break;
	case VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_VERTICAL:
		*out_x = x;
		*out_y = height - 1 - y;
		break;
	default:
		*out_x = x;
		*out_y = y;
		break;
	}
	*out_x = video_broker_clamp_int(*out_x, 0, width - 1);
	*out_y = video_broker_clamp_int(*out_y, 0, height - 1);
}

static inline void video_broker_nv12_resize_nearest_oriented(
	const unsigned char *src,
	int src_w,
	int src_h,
	unsigned char *dst,
	int dst_w,
	int dst_h,
	video_broker_video_orientation_t orientation
) {
	const unsigned char *src_y = src;
	const unsigned char *src_uv = src + ((size_t)src_w * (size_t)src_h);
	unsigned char *dst_y = dst;
	unsigned char *dst_uv = dst + ((size_t)dst_w * (size_t)dst_h);

	if (orientation == VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL && src_w == dst_w && src_h == dst_h) {
		memcpy(dst, src, video_broker_nv12_frame_size(src_w, src_h));
		return;
	}

	for (int y = 0; y < dst_h; ++y) {
		unsigned char *dst_row = dst_y + (size_t)y * (size_t)dst_w;
		int base_sy = (int)(((int64_t)y * src_h) / dst_h);
		for (int x = 0; x < dst_w; ++x) {
			int base_sx = (int)(((int64_t)x * src_w) / dst_w);
			int sx = base_sx;
			int sy = base_sy;
			video_broker_orient_xy(base_sx, base_sy, src_w, src_h, orientation, &sx, &sy);
			dst_row[x] = src_y[(size_t)sy * (size_t)src_w + (size_t)sx];
		}
	}

	for (int y = 0; y < dst_h / 2; ++y) {
		unsigned char *dst_row = dst_uv + (size_t)y * (size_t)dst_w;
		int base_luma_y = (int)(((int64_t)(y * 2) * src_h) / dst_h);
		for (int x = 0; x < dst_w; x += 2) {
			int base_luma_x = (int)(((int64_t)x * src_w) / dst_w);
			int sx = base_luma_x;
			int sy = base_luma_y;
			video_broker_orient_xy(base_luma_x, base_luma_y, src_w, src_h, orientation, &sx, &sy);
			sx &= ~1;
			sx = video_broker_clamp_int(sx, 0, src_w - 2);
			sy = video_broker_clamp_int(sy / 2, 0, (src_h / 2) - 1);
			dst_row[x] = src_uv[(size_t)sy * (size_t)src_w + (size_t)sx];
			dst_row[x + 1] = src_uv[(size_t)sy * (size_t)src_w + (size_t)sx + 1];
		}
	}
}

static inline void video_broker_nv12_resize_nearest_oriented_aspect(
	const unsigned char *src,
	int src_w,
	int src_h,
	unsigned char *dst,
	int dst_w,
	int dst_h,
	video_broker_video_orientation_t orientation,
	video_broker_nv12_aspect_policy_t aspect_policy
) {
	video_broker_nv12_aspect_layout_t layout;
	const unsigned char *src_uv;
	unsigned char *dst_uv;
	if (orientation == VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL) {
		video_broker_nv12_resize_nearest_aspect(src, src_w, src_h, dst, dst_w, dst_h, aspect_policy);
		return;
	}
	layout = video_broker_nv12_aspect_layout(src_w, src_h, dst_w, dst_h, aspect_policy);
	if (aspect_policy == VIDEO_BROKER_NV12_ASPECT_LETTERBOX) {
		video_broker_nv12_fill_black(dst, dst_w, dst_h);
	}
	src_uv = src + ((size_t)src_w * (size_t)src_h);
	dst_uv = dst + ((size_t)dst_w * (size_t)dst_h);
	for (int y = 0; y < layout.dst_h; ++y) {
		unsigned char *dst_row = dst + (size_t)(layout.dst_y + y) * (size_t)dst_w + (size_t)layout.dst_x;
		int base_y = (int)(((int64_t)y * layout.src_h) / layout.dst_h);
		for (int x = 0; x < layout.dst_w; ++x) {
			int base_x = (int)(((int64_t)x * layout.src_w) / layout.dst_w);
			int sx = base_x;
			int sy = base_y;
			video_broker_orient_xy(base_x, base_y, layout.src_w, layout.src_h, orientation, &sx, &sy);
			dst_row[x] = src[(size_t)(layout.src_y + sy) * (size_t)src_w + (size_t)(layout.src_x + sx)];
		}
	}
	for (int y = 0; y < layout.dst_h / 2; ++y) {
		unsigned char *dst_row = dst_uv + (size_t)(layout.dst_y / 2 + y) * (size_t)dst_w + (size_t)layout.dst_x;
		int base_y = (int)(((int64_t)(y * 2) * layout.src_h) / layout.dst_h);
		for (int x = 0; x < layout.dst_w; x += 2) {
			int base_x = (int)(((int64_t)x * layout.src_w) / layout.dst_w);
			int sx = base_x;
			int sy = base_y;
			video_broker_orient_xy(base_x, base_y, layout.src_w, layout.src_h, orientation, &sx, &sy);
			sx = (layout.src_x + sx) & ~1;
			sx = video_broker_clamp_int(sx, layout.src_x, layout.src_x + layout.src_w - 2);
			sy = video_broker_clamp_int((layout.src_y + sy) / 2, layout.src_y / 2, (layout.src_y + layout.src_h) / 2 - 1);
			dst_row[x] = src_uv[(size_t)sy * (size_t)src_w + (size_t)sx];
			dst_row[x + 1] = src_uv[(size_t)sy * (size_t)src_w + (size_t)sx + 1];
		}
	}
}

#endif
