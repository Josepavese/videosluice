/* Video-broker quality measurement contract test. */
#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../nv12_quality_measure.h"

static video_broker_nv12_quality_measure_t reference_measure(
	const unsigned char *nv12,
	int width,
	int height,
	int stride
) {
	video_broker_nv12_quality_measure_t measure;
	uint64_t n = 0;
	uint64_t y_sum = 0;
	long double lap_sum = 0.0L;
	long double lap_sq_sum = 0.0L;
	long double edge_sum = 0.0L;
	long double lap_mean;

	memset(&measure, 0, sizeof(measure));
	for (int y = 1; y < height - 1; y += stride) {
		for (int x = 1; x < width - 1; x += stride) {
			int c = nv12[(size_t)y * (size_t)width + (size_t)x];
			int left = nv12[(size_t)y * (size_t)width + (size_t)x - 1];
			int right = nv12[(size_t)y * (size_t)width + (size_t)x + 1];
			int up = nv12[((size_t)y - 1) * (size_t)width + (size_t)x];
			int down = nv12[((size_t)y + 1) * (size_t)width + (size_t)x];
			int lap = (4 * c) - left - right - up - down;

			y_sum += (uint64_t)c;
			measure.black_samples += c <= 4;
			measure.white_samples += c >= 251;
			if (c > 4) {
				measure.content_samples++;
				measure.content_luma_sum += (uint64_t)c;
			}
			lap_sum += (long double)lap;
			lap_sq_sum += (long double)lap * (long double)lap;
			edge_sum += (long double)(abs(c - left) + abs(c - up));
			n++;
		}
	}
	lap_mean = lap_sum / (long double)n;
	measure.samples = n;
	measure.luma = (long double)y_sum / (long double)n;
	measure.lap_var = (lap_sq_sum / (long double)n) - (lap_mean * lap_mean);
	measure.edge = edge_sum / (long double)n;
	measure.black_ratio = (long double)measure.black_samples / (long double)n;
	measure.white_ratio = (long double)measure.white_samples / (long double)n;
	measure.content_luma = measure.content_samples > 0 ?
		(long double)measure.content_luma_sum / (long double)measure.content_samples : 0.0L;
	return measure;
}

static void assert_close(long double actual, long double expected) {
	assert(fabsl(actual - expected) <= 1e-15L);
}

static void assert_equivalent(const unsigned char *frame, int width, int height, int stride) {
	video_broker_nv12_quality_measure_t fast = video_broker_nv12_quality_measure(frame, width, height, stride);
	video_broker_nv12_quality_measure_t reference = reference_measure(frame, width, height, stride);
	uint64_t region_samples = 0;
	uint64_t region_black_samples = 0;
	uint64_t region_white_samples = 0;
	uint64_t region_content_samples = 0;

	assert(fast.samples == reference.samples);
	assert(fast.black_samples == reference.black_samples);
	assert(fast.white_samples == reference.white_samples);
	assert(fast.content_samples == reference.content_samples);
	assert(fast.content_luma_sum == reference.content_luma_sum);
	assert_close(fast.luma, reference.luma);
	assert_close(fast.lap_var, reference.lap_var);
	assert_close(fast.edge, reference.edge);
	assert_close(fast.black_ratio, reference.black_ratio);
	assert_close(fast.white_ratio, reference.white_ratio);
	assert_close(fast.content_luma, reference.content_luma);
	for (int index = 0; index < VIDEO_BROKER_QUALITY_GRID_REGIONS; ++index) {
		const video_broker_nv12_quality_region_t *region = &fast.regions[index];
		region_samples += region->samples;
		region_black_samples += region->black_samples;
		region_white_samples += region->white_samples;
		region_content_samples += region->content_samples;
		if (region->samples > 0) {
			assert(region->luma >= 0.0L && region->luma <= 255.0L);
			assert(region->black_ratio >= 0.0L && region->black_ratio <= 1.0L);
			assert(region->white_ratio >= 0.0L && region->white_ratio <= 1.0L);
		}
	}
	assert(region_samples == fast.samples);
	assert(region_black_samples == fast.black_samples);
	assert(region_white_samples == fast.white_samples);
	assert(region_content_samples == fast.content_samples);
}

int main(void) {
	unsigned char uniform[8 * 8 * 3 / 2];
	unsigned char black[8 * 8 * 3 / 2];
	unsigned char white[8 * 8 * 3 / 2];
	const int width = 321;
	const int height = 181;
	const size_t frame_size = (size_t)width * (size_t)height * 3U / 2U;
	unsigned char *pattern = malloc(frame_size);

	assert(pattern != NULL);
	memset(uniform, 96, sizeof(uniform));
	memset(black, 0, sizeof(black));
	memset(white, 255, sizeof(white));
	assert_equivalent(uniform, 8, 8, 1);
	assert_equivalent(black, 8, 8, 1);
	assert_equivalent(white, 8, 8, 1);
	{
		video_broker_nv12_quality_measure_t blank =
			video_broker_nv12_quality_measure(black, 8, 8, 1);
		video_broker_nv12_quality_measure_t saturated =
			video_broker_nv12_quality_measure(white, 8, 8, 1);
		assert(!video_broker_nv12_quality_is_informative(&blank));
		assert(!video_broker_nv12_quality_is_informative(&saturated));
	}

	for (size_t i = 0; i < frame_size; ++i) {
		pattern[i] = (unsigned char)((i * 73U + (i / (size_t)width) * 19U) & 0xffU);
	}
	assert_equivalent(pattern, width, height, 1);
	assert_equivalent(pattern, width, height, 8);
	{
		video_broker_nv12_quality_measure_t detailed =
			video_broker_nv12_quality_measure(pattern, width, height, 8);
		assert(video_broker_nv12_quality_is_informative(&detailed));
	}
	free(pattern);
	return 0;
}
