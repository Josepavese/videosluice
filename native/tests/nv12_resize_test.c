/* Video-broker NV12 resize contract test. */
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../nv12_resize.h"

static size_t frame_size(int width, int height) {
	return ((size_t)width * (size_t)height * 3U) / 2U;
}

static void reference_resize(
	const unsigned char *src, int src_w, int src_h,
	unsigned char *dst, int dst_w, int dst_h
) {
	const unsigned char *src_uv = src + ((size_t)src_w * (size_t)src_h);
	unsigned char *dst_uv = dst + ((size_t)dst_w * (size_t)dst_h);
	for (int y = 0; y < dst_h; ++y) {
		int sy = (int)(((int64_t)y * src_h) / dst_h);
		for (int x = 0; x < dst_w; ++x) {
			int sx = (int)(((int64_t)x * src_w) / dst_w);
			dst[(size_t)y * (size_t)dst_w + (size_t)x] =
				src[(size_t)sy * (size_t)src_w + (size_t)sx];
		}
	}
	for (int y = 0; y < dst_h / 2; ++y) {
		int sy = (int)(((int64_t)(y * 2) * src_h) / dst_h) / 2;
		if (sy >= src_h / 2) {
			sy = (src_h / 2) - 1;
		}
		for (int x = 0; x < dst_w; x += 2) {
			int sx = (int)(((int64_t)x * src_w) / dst_w) & ~1;
			if (sx >= src_w - 1) {
				sx = src_w - 2;
			}
			dst_uv[(size_t)y * (size_t)dst_w + (size_t)x] =
				src_uv[(size_t)sy * (size_t)src_w + (size_t)sx];
			dst_uv[(size_t)y * (size_t)dst_w + (size_t)x + 1] =
				src_uv[(size_t)sy * (size_t)src_w + (size_t)sx + 1];
		}
	}
}

static void check_case(int src_w, int src_h, int dst_w, int dst_h) {
	size_t src_size = frame_size(src_w, src_h);
	size_t dst_size = frame_size(dst_w, dst_h);
	unsigned char *src = malloc(src_size);
	unsigned char *expected = malloc(dst_size);
	unsigned char *actual = malloc(dst_size);
	assert(src && expected && actual);
	for (size_t i = 0; i < src_size; ++i) {
		src[i] = (unsigned char)((i * 131U + i / 17U) & 0xffU);
	}
	reference_resize(src, src_w, src_h, expected, dst_w, dst_h);
	video_broker_nv12_resize_nearest(src, src_w, src_h, actual, dst_w, dst_h);
	assert(memcmp(expected, actual, dst_size) == 0);
	free(actual);
	free(expected);
	free(src);
}

static void check_center_crop(void) {
	const int src_w = 8;
	const int src_h = 4;
	const int dst_w = 4;
	const int dst_h = 4;
	unsigned char src[48];
	unsigned char dst[24];
	memset(src, 128, sizeof(src));
	for (int y = 0; y < src_h; ++y) {
		for (int x = 0; x < src_w; ++x) {
			src[y * src_w + x] = (unsigned char)(y * 10 + x);
		}
	}
	video_broker_nv12_resize_nearest_aspect(
		src, src_w, src_h, dst, dst_w, dst_h, VIDEO_BROKER_NV12_ASPECT_CROP);
	assert(dst[0] == 2);
	assert(dst[3] == 5);
	assert(dst[3 * dst_w] == 32);
	assert(dst[3 * dst_w + 3] == 35);
}

static void check_letterbox(void) {
	const int src_w = 8;
	const int src_h = 4;
	const int dst_w = 8;
	const int dst_h = 8;
	unsigned char src[48];
	unsigned char dst[96];
	memset(src, 90, 32);
	memset(src + 32, 140, 16);
	video_broker_nv12_resize_nearest_aspect(
		src, src_w, src_h, dst, dst_w, dst_h, VIDEO_BROKER_NV12_ASPECT_LETTERBOX);
	for (int x = 0; x < dst_w; ++x) {
		assert(dst[x] == 16);
		assert(dst[dst_w + x] == 16);
		assert(dst[2 * dst_w + x] == 90);
		assert(dst[5 * dst_w + x] == 90);
		assert(dst[6 * dst_w + x] == 16);
		assert(dst[7 * dst_w + x] == 16);
	}
	assert(dst[dst_w * dst_h] == 128);
	assert(dst[dst_w * dst_h + dst_w] == 140);
}

int main(void) {
#if defined(__aarch64__)
	assert(video_broker_nv12_resize_is_accelerated(1920, 1080, 1280, 720));
	assert(video_broker_nv12_resize_is_accelerated(1920, 1080, 640, 360));
	assert(video_broker_nv12_resize_is_accelerated(1280, 720, 640, 360));
	assert(strcmp(video_broker_nv12_resize_backend_name(1920, 1080, 640, 360), "neon") == 0);
	assert(strcmp(video_broker_nv12_resize_backend_name(1280, 720, 640, 360), "neon") == 0);
#else
	assert(!video_broker_nv12_resize_is_accelerated(1920, 1080, 1280, 720));
	assert(!video_broker_nv12_resize_is_accelerated(1920, 1080, 640, 360));
	assert(!video_broker_nv12_resize_is_accelerated(1280, 720, 640, 360));
	assert(strcmp(video_broker_nv12_resize_backend_name(1920, 1080, 640, 360), "cpu") == 0);
	assert(strcmp(video_broker_nv12_resize_backend_name(1280, 720, 640, 360), "cpu") == 0);
#endif
	check_case(1920, 1080, 1280, 720);
	check_case(1920, 1080, 640, 360);
	check_case(1280, 720, 640, 360);
	check_case(800, 600, 640, 480);
	check_case(640, 480, 640, 480);
	check_center_crop();
	check_letterbox();
	puts("OK_NATIVE_NV12_RESIZE");
	return 0;
}
