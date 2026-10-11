#ifndef VIDEO_BROKER_NV12_QUALITY_MEASURE_H
#define VIDEO_BROKER_NV12_QUALITY_MEASURE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define VIDEO_BROKER_QUALITY_GRID_COLUMNS 4
#define VIDEO_BROKER_QUALITY_GRID_ROWS 3
#define VIDEO_BROKER_QUALITY_GRID_REGIONS \
	(VIDEO_BROKER_QUALITY_GRID_COLUMNS * VIDEO_BROKER_QUALITY_GRID_ROWS)

typedef struct {
	uint64_t samples;
	uint64_t black_samples;
	uint64_t white_samples;
	uint64_t content_samples;
	uint64_t content_luma_sum;
	uint64_t luma_sum;
	uint64_t lap_sq_sum;
	uint64_t edge_sum;
	int64_t lap_sum;
	long double luma;
	long double lap_var;
	long double edge;
	long double black_ratio;
	long double white_ratio;
	long double content_luma;
} video_broker_nv12_quality_region_t;

typedef struct {
	uint64_t samples;
	uint64_t black_samples;
	uint64_t white_samples;
	uint64_t content_samples;
	uint64_t content_luma_sum;
	long double luma;
	long double lap_var;
	long double edge;
	long double black_ratio;
	long double white_ratio;
	long double content_luma;
	video_broker_nv12_quality_region_t regions[VIDEO_BROKER_QUALITY_GRID_REGIONS];
} video_broker_nv12_quality_measure_t;

static inline void video_broker_nv12_quality_finalize_region(
	video_broker_nv12_quality_region_t *region
) {
	long double lap_mean;

	if (region == NULL || region->samples == 0) {
		return;
	}
	lap_mean = (long double)region->lap_sum / (long double)region->samples;
	region->luma = (long double)region->luma_sum / (long double)region->samples;
	region->lap_var = ((long double)region->lap_sq_sum / (long double)region->samples) -
		(lap_mean * lap_mean);
	if (region->lap_var < 0.0L) {
		region->lap_var = 0.0L;
	}
	region->edge = (long double)region->edge_sum / (long double)region->samples;
	region->black_ratio = (long double)region->black_samples / (long double)region->samples;
	region->white_ratio = (long double)region->white_samples / (long double)region->samples;
	region->content_luma = region->content_samples > 0 ?
		(long double)region->content_luma_sum / (long double)region->content_samples : 0.0L;
}

/*
 * Keep the sampled pixel loop integer-only.  AArch64 implements 128-bit
 * long-double additions and multiplications through libgcc helpers; doing
 * those operations per sample consumed enough CPU to throttle a native 30 fps
 * camera.  The source values and the Laplacian are integers, so accumulating
 * them exactly and converting once after the loop preserves the result while
 * avoiding the software floating-point hot path.
 */
static inline video_broker_nv12_quality_measure_t video_broker_nv12_quality_measure(
	const unsigned char *nv12,
	int width,
	int height,
	int stride
) {
	video_broker_nv12_quality_measure_t measure;
	const unsigned char *y_plane = nv12;
	uint64_t n = 0;
	uint64_t y_sum = 0;
	uint64_t lap_sq_sum = 0;
	uint64_t edge_sum = 0;
	int64_t lap_sum = 0;
	long double lap_mean;

	memset(&measure, 0, sizeof(measure));
	if (nv12 == NULL || width < 3 || height < 3) {
		return measure;
	}
	if (stride <= 0) {
		stride = 1;
	}
	for (int y = 1; y < height - 1; y += stride) {
		for (int x = 1; x < width - 1; x += stride) {
			int region_x = (x * VIDEO_BROKER_QUALITY_GRID_COLUMNS) / width;
			int region_y = (y * VIDEO_BROKER_QUALITY_GRID_ROWS) / height;
			int region_index;
			video_broker_nv12_quality_region_t *region;
			int c = y_plane[(size_t)y * (size_t)width + (size_t)x];
			int left = y_plane[(size_t)y * (size_t)width + (size_t)x - 1];
			int right = y_plane[(size_t)y * (size_t)width + (size_t)x + 1];
			int up = y_plane[((size_t)y - 1) * (size_t)width + (size_t)x];
			int down = y_plane[((size_t)y + 1) * (size_t)width + (size_t)x];
			int lap = (4 * c) - left - right - up - down;
			int horizontal_edge = c - left;
			int vertical_edge = c - up;

			if (horizontal_edge < 0) {
				horizontal_edge = -horizontal_edge;
			}
			if (vertical_edge < 0) {
				vertical_edge = -vertical_edge;
			}
			if (region_x >= VIDEO_BROKER_QUALITY_GRID_COLUMNS) {
				region_x = VIDEO_BROKER_QUALITY_GRID_COLUMNS - 1;
			}
			if (region_y >= VIDEO_BROKER_QUALITY_GRID_ROWS) {
				region_y = VIDEO_BROKER_QUALITY_GRID_ROWS - 1;
			}
			region_index = region_y * VIDEO_BROKER_QUALITY_GRID_COLUMNS + region_x;
			region = &measure.regions[region_index];
			y_sum += (uint64_t)c;
			region->luma_sum += (uint64_t)c;
			if (c <= 4) {
				measure.black_samples++;
				region->black_samples++;
			}
			if (c >= 251) {
				measure.white_samples++;
				region->white_samples++;
			}
			if (c > 4) {
				measure.content_samples++;
				measure.content_luma_sum += (uint64_t)c;
				region->content_samples++;
				region->content_luma_sum += (uint64_t)c;
			}
			lap_sum += (int64_t)lap;
			lap_sq_sum += (uint64_t)((int64_t)lap * (int64_t)lap);
			edge_sum += (uint64_t)(horizontal_edge + vertical_edge);
			region->lap_sum += (int64_t)lap;
			region->lap_sq_sum += (uint64_t)((int64_t)lap * (int64_t)lap);
			region->edge_sum += (uint64_t)(horizontal_edge + vertical_edge);
			region->samples++;
			n++;
		}
	}
	if (n == 0) {
		return measure;
	}
	lap_mean = (long double)lap_sum / (long double)n;
	measure.samples = n;
	measure.luma = (long double)y_sum / (long double)n;
	measure.lap_var = ((long double)lap_sq_sum / (long double)n) - (lap_mean * lap_mean);
	if (measure.lap_var < 0.0L) {
		measure.lap_var = 0.0L;
	}
	measure.edge = (long double)edge_sum / (long double)n;
	measure.black_ratio = (long double)measure.black_samples / (long double)n;
	measure.white_ratio = (long double)measure.white_samples / (long double)n;
	measure.content_luma = measure.content_samples > 0 ?
		(long double)measure.content_luma_sum / (long double)measure.content_samples : 0.0L;
	for (int index = 0; index < VIDEO_BROKER_QUALITY_GRID_REGIONS; ++index) {
		video_broker_nv12_quality_finalize_region(&measure.regions[index]);
	}
	return measure;
}

static inline bool video_broker_nv12_quality_is_informative(
	const video_broker_nv12_quality_measure_t *measure
) {
	if (measure == NULL || measure->samples == 0) {
		return false;
	}
	/* A dark scene still carries sensor noise/edges. This only rejects frames
	 * that are overwhelmingly code values 0..4 and contain no useful detail. */
	return (measure->black_ratio < 0.98L && measure->white_ratio < 0.98L) ||
		measure->lap_var >= 1.0L || measure->edge >= 0.25L;
}

static inline bool video_broker_nv12_quality_region_is_informative(
	const video_broker_nv12_quality_region_t *region
) {
	if (region == NULL || region->samples == 0) {
		return false;
	}
	return (region->black_ratio < 0.98L && region->white_ratio < 0.98L) ||
		region->lap_var >= 1.0L || region->edge >= 0.25L;
}

#endif
