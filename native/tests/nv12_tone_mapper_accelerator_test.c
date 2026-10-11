#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "../nv12_tone_mapper_accelerator.h"

#if defined(VIDEO_BROKER_HAVE_OPENCL)

static void fill_scene(unsigned char *frame, int width, int height) {
	size_t y_size = (size_t)width * (size_t)height;
	for (int y = 0; y < height; ++y) {
		for (int x = 0; x < width; ++x) {
			frame[(size_t)y * (size_t)width + (size_t)x] =
				(unsigned char)(x < width / 2 ? 36 + (y % 12) : 218 + (y % 12));
		}
	}
	memset(frame + y_size, 128, y_size / 2);
}

static void assert_backend_matches_cpu(video_broker_tone_mode_t mode, int width, int height) {
	const size_t size = (size_t)width * (size_t)height * 3 / 2;
	unsigned char *reference = (unsigned char *)malloc(size);
	unsigned char *accelerated = (unsigned char *)malloc(size);
	video_broker_nv12_tone_mapper_t mapper;
	video_broker_tone_accelerator_t accelerator;
	assert(reference != NULL && accelerated != NULL);
	fill_scene(reference, width, height);
	memcpy(accelerated, reference, size);
	video_broker_nv12_tone_mapper_init(
		&mapper, mode, 128, 8, 16, 9, 8, 30, 1.8, 0.30, 0.35, true);
	video_broker_nv12_tone_mapper_set_signal_informative(&mapper, true);
	assert(video_broker_nv12_tone_mapper_update(&mapper, reference, width, height, 4));
	assert(video_broker_nv12_tone_mapper_apply(&mapper, reference, width, height));
	assert(video_broker_tone_accelerator_init(
		&accelerator, VIDEO_BROKER_TONE_ACCELERATOR_OPENCL) == 0);
	assert(accelerator.selected == VIDEO_BROKER_TONE_ACCELERATOR_OPENCL);
	assert(video_broker_tone_accelerator_apply(
		&accelerator, &mapper, accelerated, width, height) == 1);
	assert(memcmp(reference, accelerated, size) == 0);
	assert(video_broker_tone_accelerator_destroy(&accelerator) == 0);
	assert(!video_broker_tone_accelerator_resources_quarantined());
	video_broker_nv12_tone_mapper_destroy(&mapper);
	free(accelerated);
	free(reference);
}

int main(void) {
	assert_backend_matches_cpu(VIDEO_BROKER_TONE_GLOBAL, 640, 360);
	assert_backend_matches_cpu(VIDEO_BROKER_TONE_LOCAL, 640, 360);
	assert_backend_matches_cpu(VIDEO_BROKER_TONE_LOCAL, 1920, 1080);
	return 0;
}

#else

int main(void) {
	return 0;
}

#endif
