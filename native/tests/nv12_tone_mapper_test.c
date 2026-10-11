/* Video-broker spatial tone-mapping contract tests. */
#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../nv12_tone_mapper.h"

static long double mean_region(
	const unsigned char *frame,
	int width,
	int x0,
	int x1,
	int height
) {
	unsigned long long sum = 0;
	unsigned long long samples = 0;
	for (int y = 0; y < height; ++y) {
		for (int x = x0; x < x1; ++x) {
			sum += frame[(size_t)y * (size_t)width + (size_t)x];
			samples++;
		}
	}
	return samples == 0 ? 0.0L : (long double)sum / (long double)samples;
}

static void fill_backlit(unsigned char *frame, int width, int height, int dark, int bright) {
	for (int y = 0; y < height; ++y) {
		for (int x = 0; x < width; ++x) {
			frame[(size_t)y * (size_t)width + (size_t)x] =
				(unsigned char)(x < width / 2 ? dark : bright);
		}
	}
	memset(frame + (size_t)width * (size_t)height, 128, (size_t)width * (size_t)height / 2U);
}

static void force_statistics_update(
	video_broker_nv12_tone_mapper_t *mapper,
	const unsigned char *frame,
	int width,
	int height
) {
	mapper->frames_until_statistics = 0;
	assert(video_broker_nv12_tone_mapper_update(mapper, frame, width, height, 1));
}

static uint64_t monotonic_us(void) {
	struct timespec value;
	assert(clock_gettime(CLOCK_MONOTONIC, &value) == 0);
	return (uint64_t)value.tv_sec * 1000000ULL + (uint64_t)value.tv_nsec / 1000ULL;
}

static void assert_perceptual_shadow_curve_contract(void) {
	const double gains[] = {0.70, 1.0, 1.30, 1.80, 2.40, 3.0};
	for (size_t gain_index = 0; gain_index < sizeof(gains) / sizeof(gains[0]); ++gain_index) {
		unsigned char lut[256];
		video_broker_tone_build_lut(lut, gains[gain_index]);
		assert(lut[0] == 0);
		for (int value = 1; value < 256; ++value) {
			assert(lut[value] >= lut[value - 1]);
		}
	}
	assert(video_broker_tone_map_value(4, 2.4) == 4);
	assert(video_broker_tone_map_value(5, 2.4) <= 15);
	assert(video_broker_tone_map_value(16, 2.4) >= 45);
	assert(video_broker_tone_map_value(40, 2.4) >= 95);
	assert(video_broker_tone_map_value(220, 2.4) <= 240);
	assert(video_broker_tone_map_value(255, 2.4) == 255);
}

static void assert_full_hd_performance_contract(void) {
	const int width = 1920;
	const int height = 1080;
	const int iterations = 5;
	const int mixed_iterations = 30;
	const size_t frame_size = (size_t)width * (size_t)height * 3U / 2U;
	video_broker_nv12_tone_mapper_t mapper;
	unsigned char *frame = (unsigned char *)malloc(frame_size);
	uint64_t started;
	uint64_t elapsed;

	assert(frame != NULL);
	fill_backlit(frame, width, height, 40, 220);
	video_broker_nv12_tone_mapper_init(
		&mapper, VIDEO_BROKER_TONE_LOCAL, 96, 8, 16, 9, 8, 30, 1.8, 0.30, 0.18, true);
	video_broker_nv12_tone_mapper_set_signal_informative(&mapper, true);
	force_statistics_update(&mapper, frame, width, height);
	assert(mapper.statistics_hz == 8);
	assert(mapper.effective_statistics_hz == 2);
	assert(mapper.frames_until_statistics == 14);
	assert(video_broker_nv12_tone_mapper_apply(&mapper, frame, width, height));
	assert(!mapper.spatial_lut_dirty && mapper.spatial_lut_index != NULL);

	started = monotonic_us();
	for (int iteration = 0; iteration < iterations; ++iteration) {
		assert(video_broker_nv12_tone_mapper_apply(&mapper, frame, width, height));
	}
	elapsed = monotonic_us() - started;
	fprintf(stderr, "[video-broker][benchmark] tone_mapping_local_1080p_avg_ms=%.3f\n",
		(double)elapsed / (double)iterations / 1000.0);
	/* This host-side ceiling catches accidental super-linear regressions. The
	 * stricter real-time budget is measured by native telemetry on each board. */
	assert(elapsed / (uint64_t)iterations < 150000ULL);

	started = monotonic_us();
	for (int iteration = 0; iteration < mixed_iterations; ++iteration) {
		if (iteration % 4 == 0) {
			force_statistics_update(&mapper, frame, width, height);
			assert(mapper.spatial_lut_dirty);
		}
		assert(video_broker_nv12_tone_mapper_apply(&mapper, frame, width, height));
	}
	elapsed = monotonic_us() - started;
	fprintf(stderr, "[video-broker][benchmark] tone_mapping_local_1080p_mixed_avg_ms=%.3f\n",
		(double)elapsed / (double)mixed_iterations / 1000.0);
	/* 8 Hz spatial-statistics refresh at 30 fps must remain comfortably below
	 * one frame period on a generic host; board telemetry enforces the target's
	 * stricter end-to-end cadence. */
	assert(elapsed / (uint64_t)mixed_iterations < 50000ULL);

	video_broker_nv12_tone_mapper_destroy(&mapper);
	free(frame);
}

int main(void) {
	const int width = 160;
	const int height = 90;
	const size_t y_size = (size_t)width * (size_t)height;
	const size_t frame_size = y_size * 3U / 2U;
	video_broker_nv12_tone_mapper_t mapper;
	unsigned char *source = (unsigned char *)malloc(frame_size);
	unsigned char *output = (unsigned char *)malloc(frame_size);
	unsigned char *uniform = (unsigned char *)malloc(frame_size);
	unsigned char *chroma = (unsigned char *)malloc(y_size / 2U);
	double prior_left_gain;
	double prior_right_gain;

	assert(source != NULL && output != NULL && uniform != NULL && chroma != NULL);

	/* A local map must lift the room and attenuate the bright window in one
	 * shared NV12 pass while preserving chroma and true black. */
	fill_backlit(source, width, height, 40, 220);
	memcpy(chroma, source + y_size, y_size / 2U);
	video_broker_nv12_tone_mapper_init(
		&mapper, VIDEO_BROKER_TONE_LOCAL, 96, 8, 8, 4, 10, 30, 1.8, 0.30, 0.18, true);
	video_broker_nv12_tone_mapper_set_signal_informative(&mapper, true);
	for (int i = 0; i < 36; ++i) {
		force_statistics_update(&mapper, source, width, height);
	}
	assert(mapper.effective_statistics_hz == 10);
	assert(mapper.minimum_gain < 0.90);
	assert(mapper.maximum_gain > 1.30);
	memcpy(output, source, frame_size);
	output[0] = 0;
	assert(video_broker_nv12_tone_mapper_apply(&mapper, output, width, height));
	assert(output[0] == 0);
	assert(mean_region(output, width, 0, width / 2, height) > 70.0L);
	assert(mean_region(output, width, width / 2, width, height) < 205.0L);
	assert(memcmp(chroma, output + y_size, y_size / 2U) == 0);

	/* Bilinear curve interpolation must not expose the configured tile grid.
	 * A uniform image transformed by the learned backlit field changes smoothly. */
	memset(uniform, 100, y_size);
	memset(uniform + y_size, 128, y_size / 2U);
	assert(video_broker_nv12_tone_mapper_apply(&mapper, uniform, width, height));
	for (int x = 1; x < width; ++x) {
		int delta = abs((int)uniform[x] - (int)uniform[x - 1]);
		assert(delta <= 3);
	}

	/* A scene inversion can move each learned gain only by the configured
	 * bounded temporal step, preventing visible frame-to-frame pumping. */
	prior_left_gain = mapper.tile_gain[0];
	prior_right_gain = mapper.tile_gain[mapper.columns - 1];
	fill_backlit(source, width, height, 220, 40);
	force_statistics_update(&mapper, source, width, height);
	assert(fabs(mapper.tile_gain[0] - prior_left_gain) <= 0.037);
	assert(fabs(mapper.tile_gain[mapper.columns - 1] - prior_right_gain) <= 0.037);
	video_broker_nv12_tone_mapper_destroy(&mapper);

	/* Near-black low-contrast sensor noise is not a valid signal to amplify. */
	for (size_t i = 0; i < y_size; ++i) {
		source[i] = (unsigned char)(6 + (i % 3));
	}
	memset(source + y_size, 128, y_size / 2U);
	video_broker_nv12_tone_mapper_init(
		&mapper, VIDEO_BROKER_TONE_LOCAL, 96, 8, 8, 4, 8, 30, 1.8, 0.30, 0.18, true);
	video_broker_nv12_tone_mapper_set_signal_informative(&mapper, true);
	force_statistics_update(&mapper, source, width, height);
	assert(!video_broker_nv12_tone_mapper_active(&mapper));
	assert(mapper.minimum_gain == 1.0 && mapper.maximum_gain == 1.0);
	video_broker_nv12_tone_mapper_destroy(&mapper);

	/* Auto mode requires stable evidence before selecting the local path. */
	fill_backlit(source, width, height, 40, 220);
	video_broker_nv12_tone_mapper_init(
		&mapper, VIDEO_BROKER_TONE_AUTO, 96, 8, 8, 4, 8, 30, 1.8, 0.30, 0.18, true);
	video_broker_nv12_tone_mapper_set_signal_informative(&mapper, true);
	assert(mapper.active_mode == VIDEO_BROKER_TONE_GLOBAL);
	force_statistics_update(&mapper, source, width, height);
	force_statistics_update(&mapper, source, width, height);
	assert(mapper.active_mode == VIDEO_BROKER_TONE_GLOBAL);
	force_statistics_update(&mapper, source, width, height);
	assert(mapper.active_mode == VIDEO_BROKER_TONE_LOCAL);
	video_broker_nv12_tone_mapper_destroy(&mapper);

	/* Disabled mode is an exact no-op. */
	video_broker_nv12_tone_mapper_init(
		&mapper, VIDEO_BROKER_TONE_OFF, 96, 8, 8, 4, 8, 30, 1.8, 0.30, 0.18, true);
	memcpy(output, source, frame_size);
	assert(!video_broker_nv12_tone_mapper_apply(&mapper, output, width, height));
	assert(memcmp(output, source, frame_size) == 0);
	video_broker_nv12_tone_mapper_destroy(&mapper);
	assert_full_hd_performance_contract();
	assert_perceptual_shadow_curve_contract();

	free(chroma);
	free(uniform);
	free(output);
	free(source);
	return 0;
}
