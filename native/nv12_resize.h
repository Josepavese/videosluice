#ifndef VIDEO_BROKER_NV12_RESIZE_H
#define VIDEO_BROKER_NV12_RESIZE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

typedef enum {
	VIDEO_BROKER_NV12_ASPECT_EXACT = 0,
	VIDEO_BROKER_NV12_ASPECT_CROP = 1,
	VIDEO_BROKER_NV12_ASPECT_LETTERBOX = 2,
} video_broker_nv12_aspect_policy_t;

typedef struct {
	int src_x;
	int src_y;
	int src_w;
	int src_h;
	int dst_x;
	int dst_y;
	int dst_w;
	int dst_h;
} video_broker_nv12_aspect_layout_t;

/* Allocation-free nearest-neighbour NV12 resize. The source coordinate is
 * advanced as quotient+remainder instead of dividing for every output pixel.
 * This preserves floor(dst * source / destination) exactly while keeping
 * large non-integral ratios suitable for an embedded real-time loop. */
static inline void video_broker_nv12_downscale_half(
	const unsigned char *src, int src_w, int src_h, unsigned char *dst
) {
	int dst_w = src_w / 2;
	int dst_h = src_h / 2;
	const unsigned char *src_y = src;
	const unsigned char *src_uv = src + ((size_t)src_w * (size_t)src_h);
	unsigned char *dst_y = dst;
	unsigned char *dst_uv = dst + ((size_t)dst_w * (size_t)dst_h);

	for (int y = 0; y < dst_h; ++y) {
		const unsigned char *src_row = src_y + (size_t)(y * 2) * (size_t)src_w;
		unsigned char *dst_row = dst_y + (size_t)y * (size_t)dst_w;
		int x = 0;
#if defined(__aarch64__)
		for (; x <= dst_w - 16; x += 16) {
			uint8x16x2_t pixels = vld2q_u8(src_row + (size_t)x * 2U);
			vst1q_u8(dst_row + x, pixels.val[0]);
		}
#endif
		const unsigned char *src_pixel = src_row + (size_t)x * 2U;
		for (; x < dst_w; ++x, src_pixel += 2) {
			dst_row[x] = *src_pixel;
		}
	}
	for (int y = 0; y < dst_h / 2; ++y) {
		const unsigned char *src_row = src_uv + (size_t)(y * 2) * (size_t)src_w;
		unsigned char *dst_row = dst_uv + (size_t)y * (size_t)dst_w;
		int x = 0;
#if defined(__aarch64__)
		for (; x <= dst_w - 16; x += 16) {
			uint8x16_t first = vld1q_u8(src_row + (size_t)x * 2U);
			uint8x16_t second = vld1q_u8(src_row + (size_t)x * 2U + 16U);
			uint16x8_t selected = vuzp1q_u16(
				vreinterpretq_u16_u8(first), vreinterpretq_u16_u8(second));
			vst1q_u8(dst_row + x, vreinterpretq_u8_u16(selected));
		}
#endif
		const unsigned char *src_pair = src_row + (size_t)x * 2U;
		for (; x < dst_w; x += 2, src_pair += 4) {
			dst_row[x] = src_pair[0];
			dst_row[x + 1] = src_pair[1];
		}
	}
}

static inline void video_broker_nv12_downscale_third(
	const unsigned char *src, int src_w, int src_h, unsigned char *dst
) {
	int dst_w = src_w / 3;
	int dst_h = src_h / 3;
	const unsigned char *src_y = src;
	const unsigned char *src_uv = src + ((size_t)src_w * (size_t)src_h);
	unsigned char *dst_y = dst;
	unsigned char *dst_uv = dst + ((size_t)dst_w * (size_t)dst_h);

	for (int y = 0; y < dst_h; ++y) {
		const unsigned char *src_row = src_y + (size_t)(y * 3) * (size_t)src_w;
		unsigned char *dst_row = dst_y + (size_t)y * (size_t)dst_w;
		int x = 0;
#if defined(__aarch64__)
		static const uint8_t indices_bytes[16] = {
			0, 3, 6, 9, 12, 15, 18, 21,
			24, 27, 30, 33, 36, 39, 42, 45,
		};
		uint8x16_t indices = vld1q_u8(indices_bytes);
		for (; x <= dst_w - 16; x += 16) {
			const unsigned char *source = src_row + (size_t)x * 3U;
			uint8x16x3_t table;
			table.val[0] = vld1q_u8(source);
			table.val[1] = vld1q_u8(source + 16U);
			table.val[2] = vld1q_u8(source + 32U);
			vst1q_u8(dst_row + x, vqtbl3q_u8(table, indices));
		}
#endif
		for (; x < dst_w; ++x) {
			dst_row[x] = src_row[(size_t)x * 3U];
		}
	}
	for (int y = 0; y < dst_h / 2; ++y) {
		const unsigned char *src_row = src_uv + (size_t)(y * 3) * (size_t)src_w;
		unsigned char *dst_row = dst_uv + (size_t)y * (size_t)dst_w;
		int x = 0;
#if defined(__aarch64__)
		static const uint8_t uv_indices_bytes[16] = {
			0, 1, 6, 7, 12, 13, 18, 19,
			24, 25, 30, 31, 36, 37, 42, 43,
		};
		uint8x16_t indices = vld1q_u8(uv_indices_bytes);
		for (; x <= dst_w - 16; x += 16) {
			const unsigned char *source = src_row + (size_t)(x / 2) * 6U;
			uint8x16x3_t table;
			table.val[0] = vld1q_u8(source);
			table.val[1] = vld1q_u8(source + 16U);
			table.val[2] = vld1q_u8(source + 32U);
			vst1q_u8(dst_row + x, vqtbl3q_u8(table, indices));
		}
#endif
		for (; x < dst_w; x += 2) {
			const unsigned char *source = src_row + (size_t)(x / 2) * 6U;
			dst_row[x] = source[0];
			dst_row[x + 1] = source[1];
		}
	}
}

static inline bool video_broker_nv12_resize_is_accelerated(
	int src_w,
	int src_h,
	int dst_w,
	int dst_h
) {
#if defined(__aarch64__)
	return (
		((int64_t)src_w == (int64_t)dst_w * 2 &&
		 (int64_t)src_h == (int64_t)dst_h * 2) ||
		((int64_t)src_w == (int64_t)dst_w * 3 &&
		 (int64_t)src_h == (int64_t)dst_h * 3) ||
		((int64_t)src_w * 2 == (int64_t)dst_w * 3 &&
		 (int64_t)src_h * 2 == (int64_t)dst_h * 3)
	);
#else
	(void)src_w;
	(void)src_h;
	(void)dst_w;
	(void)dst_h;
	return false;
#endif
}

static inline const char *video_broker_nv12_resize_backend_name(
	int src_w,
	int src_h,
	int dst_w,
	int dst_h
) {
	return video_broker_nv12_resize_is_accelerated(src_w, src_h, dst_w, dst_h) ?
		"neon" : "cpu";
}

static inline void video_broker_nv12_downscale_three_to_two(
	const unsigned char *src, int src_w, int src_h, unsigned char *dst
) {
	int dst_w = (src_w * 2) / 3;
	int dst_h = (src_h * 2) / 3;
	const unsigned char *src_y = src;
	const unsigned char *src_uv = src + ((size_t)src_w * (size_t)src_h);
	unsigned char *dst_y = dst;
	unsigned char *dst_uv = dst + ((size_t)dst_w * (size_t)dst_h);

	for (int y = 0; y < dst_h; ++y) {
		int sy = ((y / 2) * 3) + (y & 1);
		const unsigned char *src_row = src_y + (size_t)sy * (size_t)src_w;
		unsigned char *dst_row = dst_y + (size_t)y * (size_t)dst_w;
		int x = 0;
#if defined(__aarch64__)
		static const uint8_t indices_bytes[16] = {
			0, 1, 3, 4, 6, 7, 9, 10,
			12, 13, 15, 16, 18, 19, 21, 22,
		};
		uint8x16_t indices = vld1q_u8(indices_bytes);
		for (; x <= dst_w - 16 && ((x * 3) / 2) + 32 <= src_w; x += 16) {
			const unsigned char *source = src_row + (size_t)(x * 3) / 2U;
			uint8x16x2_t table;
			table.val[0] = vld1q_u8(source);
			table.val[1] = vld1q_u8(source + 16U);
			vst1q_u8(dst_row + x, vqtbl2q_u8(table, indices));
		}
#endif
		int sx = (x * 3) / 2;
		for (; x < dst_w; x += 2, sx += 3) {
			dst_row[x] = src_row[sx];
			dst_row[x + 1] = src_row[sx + 1];
		}
	}
	for (int y = 0; y < dst_h / 2; ++y) {
		int sy = (y * 3) / 2;
		const unsigned char *src_row = src_uv + (size_t)sy * (size_t)src_w;
		unsigned char *dst_row = dst_uv + (size_t)y * (size_t)dst_w;
		int x = 0;
#if defined(__aarch64__)
		static const uint8_t uv_indices_bytes[16] = {
			0, 1, 2, 3, 6, 7, 8, 9,
			12, 13, 14, 15, 18, 19, 20, 21,
		};
		uint8x16_t indices = vld1q_u8(uv_indices_bytes);
		for (; x <= dst_w - 16 && ((x * 3) / 2) + 32 <= src_w; x += 16) {
			const unsigned char *source = src_row + (size_t)(x * 3) / 2U;
			uint8x16x2_t table;
			table.val[0] = vld1q_u8(source);
			table.val[1] = vld1q_u8(source + 16U);
			vst1q_u8(dst_row + x, vqtbl2q_u8(table, indices));
		}
#endif
		int source_luma_x = (x * 3) / 2;
		for (; x < dst_w; x += 2, source_luma_x += 3) {
			int sx = source_luma_x & ~1;
			dst_row[x] = src_row[sx];
			dst_row[x + 1] = src_row[sx + 1];
		}
	}
}

static inline void video_broker_nv12_resize_nearest(
	const unsigned char *src, int src_w, int src_h,
	unsigned char *dst, int dst_w, int dst_h
) {
	const unsigned char *src_y = src;
	const unsigned char *src_uv = src + ((size_t)src_w * (size_t)src_h);
	unsigned char *dst_y = dst;
	unsigned char *dst_uv = dst + ((size_t)dst_w * (size_t)dst_h);
	int x_step;
	int x_remainder_step;
	int uv_x_step;
	int uv_x_remainder_step;

	if (src_w == dst_w && src_h == dst_h) {
		memcpy(dst, src, ((size_t)src_w * (size_t)src_h * 3U) / 2U);
		return;
	}
	if (src_w == dst_w * 2 && src_h == dst_h * 2) {
		video_broker_nv12_downscale_half(src, src_w, src_h, dst);
		return;
	}
	if (src_w == dst_w * 3 && src_h == dst_h * 3) {
		video_broker_nv12_downscale_third(src, src_w, src_h, dst);
		return;
	}
	if ((int64_t)src_w * 2 == (int64_t)dst_w * 3 &&
	    (int64_t)src_h * 2 == (int64_t)dst_h * 3) {
		video_broker_nv12_downscale_three_to_two(src, src_w, src_h, dst);
		return;
	}

	x_step = src_w / dst_w;
	x_remainder_step = src_w % dst_w;
	for (int y = 0; y < dst_h; ++y) {
		int sy = (int)(((int64_t)y * src_h) / dst_h);
		const unsigned char *src_row = src_y + (size_t)sy * (size_t)src_w;
		unsigned char *dst_row = dst_y + (size_t)y * (size_t)dst_w;
		int sx = 0;
		int remainder = 0;
		for (int x = 0; x < dst_w; ++x) {
			dst_row[x] = src_row[sx];
			sx += x_step;
			remainder += x_remainder_step;
			if (remainder >= dst_w) {
				sx++;
				remainder -= dst_w;
			}
		}
	}

	uv_x_step = (src_w * 2) / dst_w;
	uv_x_remainder_step = (src_w * 2) % dst_w;
	for (int y = 0; y < dst_h / 2; ++y) {
		int src_luma_y = (int)(((int64_t)(y * 2) * src_h) / dst_h);
		int sy = src_luma_y / 2;
		const unsigned char *src_row;
		unsigned char *dst_row = dst_uv + (size_t)y * (size_t)dst_w;
		int source_luma_x = 0;
		int remainder = 0;
		if (sy >= src_h / 2) {
			sy = (src_h / 2) - 1;
		}
		src_row = src_uv + (size_t)sy * (size_t)src_w;
		for (int x = 0; x < dst_w; x += 2) {
			int sx = source_luma_x & ~1;
			if (sx >= src_w - 1) {
				sx = src_w - 2;
			}
			dst_row[x] = src_row[sx];
			dst_row[x + 1] = src_row[sx + 1];
			source_luma_x += uv_x_step;
			remainder += uv_x_remainder_step;
			if (remainder >= dst_w) {
				source_luma_x++;
				remainder -= dst_w;
			}
		}
	}
}

static inline int video_broker_nv12_even_floor(int value) {
	return value > 1 ? (value & ~1) : 2;
}

static inline video_broker_nv12_aspect_layout_t video_broker_nv12_aspect_layout(
	int src_w,
	int src_h,
	int dst_w,
	int dst_h,
	video_broker_nv12_aspect_policy_t policy
) {
	video_broker_nv12_aspect_layout_t layout = {0, 0, src_w, src_h, 0, 0, dst_w, dst_h};
	int64_t source_ratio = (int64_t)src_w * (int64_t)dst_h;
	int64_t target_ratio = (int64_t)dst_w * (int64_t)src_h;
	if (source_ratio == target_ratio || policy == VIDEO_BROKER_NV12_ASPECT_EXACT) {
		return layout;
	}
	if (policy == VIDEO_BROKER_NV12_ASPECT_CROP) {
		if (source_ratio > target_ratio) {
			layout.src_w = video_broker_nv12_even_floor((int)(((int64_t)src_h * dst_w) / dst_h));
			layout.src_x = ((src_w - layout.src_w) / 2) & ~1;
		} else {
			layout.src_h = video_broker_nv12_even_floor((int)(((int64_t)src_w * dst_h) / dst_w));
			layout.src_y = ((src_h - layout.src_h) / 2) & ~1;
		}
		return layout;
	}
	if (policy == VIDEO_BROKER_NV12_ASPECT_LETTERBOX) {
		if (source_ratio > target_ratio) {
			layout.dst_h = video_broker_nv12_even_floor((int)(((int64_t)dst_w * src_h) / src_w));
			layout.dst_y = ((dst_h - layout.dst_h) / 2) & ~1;
		} else {
			layout.dst_w = video_broker_nv12_even_floor((int)(((int64_t)dst_h * src_w) / src_h));
			layout.dst_x = ((dst_w - layout.dst_w) / 2) & ~1;
		}
	}
	return layout;
}

static inline void video_broker_nv12_fill_black(unsigned char *dst, int width, int height) {
	size_t y_size = (size_t)width * (size_t)height;
	memset(dst, 16, y_size);
	memset(dst + y_size, 128, y_size / 2U);
}

static inline void video_broker_nv12_resize_nearest_region(
	const unsigned char *src,
	int src_stride,
	int src_frame_h,
	int src_x,
	int src_y,
	int src_w,
	int src_h,
	unsigned char *dst,
	int dst_stride,
	int dst_frame_h,
	int dst_x,
	int dst_y,
	int dst_w,
	int dst_h
) {
	const unsigned char *src_uv = src + ((size_t)src_stride * (size_t)src_frame_h);
	unsigned char *dst_uv = dst + ((size_t)dst_stride * (size_t)dst_frame_h);
	for (int y = 0; y < dst_h; ++y) {
		int sy = src_y + (int)(((int64_t)y * src_h) / dst_h);
		const unsigned char *src_row = src + (size_t)sy * (size_t)src_stride;
		unsigned char *dst_row = dst + (size_t)(dst_y + y) * (size_t)dst_stride + (size_t)dst_x;
		for (int x = 0; x < dst_w; ++x) {
			int sx = src_x + (int)(((int64_t)x * src_w) / dst_w);
			dst_row[x] = src_row[sx];
		}
	}
	for (int y = 0; y < dst_h / 2; ++y) {
		int luma_y = src_y + (int)(((int64_t)(y * 2) * src_h) / dst_h);
		int sy = luma_y / 2;
		const unsigned char *src_row = src_uv + (size_t)sy * (size_t)src_stride;
		unsigned char *dst_row = dst_uv + (size_t)(dst_y / 2 + y) * (size_t)dst_stride + (size_t)dst_x;
		for (int x = 0; x < dst_w; x += 2) {
			int sx = src_x + (int)(((int64_t)x * src_w) / dst_w);
			sx &= ~1;
			if (sx >= src_x + src_w - 1) {
				sx = src_x + src_w - 2;
			}
			dst_row[x] = src_row[sx];
			dst_row[x + 1] = src_row[sx + 1];
		}
	}
}

static inline void video_broker_nv12_resize_nearest_aspect(
	const unsigned char *src,
	int src_w,
	int src_h,
	unsigned char *dst,
	int dst_w,
	int dst_h,
	video_broker_nv12_aspect_policy_t policy
) {
	video_broker_nv12_aspect_layout_t layout;
	if ((int64_t)src_w * dst_h == (int64_t)dst_w * src_h) {
		video_broker_nv12_resize_nearest(src, src_w, src_h, dst, dst_w, dst_h);
		return;
	}
	layout = video_broker_nv12_aspect_layout(src_w, src_h, dst_w, dst_h, policy);
	if (policy == VIDEO_BROKER_NV12_ASPECT_LETTERBOX) {
		video_broker_nv12_fill_black(dst, dst_w, dst_h);
	}
	video_broker_nv12_resize_nearest_region(
		src, src_w, src_h,
		layout.src_x, layout.src_y, layout.src_w, layout.src_h,
		dst, dst_w, dst_h,
		layout.dst_x, layout.dst_y, layout.dst_w, layout.dst_h);
}

#endif
