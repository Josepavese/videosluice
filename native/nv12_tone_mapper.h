#ifndef VIDEO_BROKER_NV12_TONE_MAPPER_H
#define VIDEO_BROKER_NV12_TONE_MAPPER_H

#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/*
 * Spatially adaptive, camera-independent tone mapper for decoded NV12 frames.
 *
 * The mapper operates only on the Y plane. It learns bounded monotonic curves
 * from a sparse grid of robust luma histograms, spatially smooths the curves,
 * and applies temporal inertia before touching a frame. Chroma is never
 * modified and code zero remains zero, preventing the grey veil that a plain
 * brightness lift creates. No UVC control, VID/PID or ISP-specific scale is
 * assumed.
 */

enum {
	VIDEO_BROKER_TONE_MAX_COLUMNS = 32,
	VIDEO_BROKER_TONE_MAX_ROWS = 24,
	VIDEO_BROKER_TONE_MAX_TILES = VIDEO_BROKER_TONE_MAX_COLUMNS * VIDEO_BROKER_TONE_MAX_ROWS,
	VIDEO_BROKER_TONE_HISTOGRAM_BINS = 64,
	VIDEO_BROKER_TONE_GAIN_LUT_LEVELS = 1024,
	/* Rebuilding a capture-sized spatial LUT is the expensive, bursty part of
	 * local tone mapping. Bound that work to two 1080p surfaces per second;
	 * smaller role surfaces retain the configured response rate. */
	VIDEO_BROKER_TONE_SPATIAL_LUT_PIXELS_PER_SECOND = 1920 * 1080 * 2,
};

/*
 * Local tone mapping is a bandwidth-sensitive hot path on embedded ARM SoCs.
 * Curves are linear in their bounded gain parameter, so one process-wide bank
 * of quantized monotonic curves can serve every mapper. The local renderer
 * interpolates compact gain indexes and performs one curve lookup per luma
 * sample instead of fetching and blending four large tile LUTs.
 */
static unsigned char video_broker_tone_gain_lut[VIDEO_BROKER_TONE_GAIN_LUT_LEVELS][256];
static bool video_broker_tone_gain_lut_ready = false;

typedef enum {
	VIDEO_BROKER_TONE_OFF = 0,
	VIDEO_BROKER_TONE_GLOBAL = 1,
	VIDEO_BROKER_TONE_LOCAL = 2,
	VIDEO_BROKER_TONE_AUTO = 3,
} video_broker_tone_mode_t;

typedef struct {
	bool enabled;
	bool initialized;
	bool signal_informative;
	bool noise_guard;
	bool auto_local_active;
	bool spatial_lut_dirty;
	video_broker_tone_mode_t requested_mode;
	video_broker_tone_mode_t active_mode;
	int target_luma;
	int hysteresis;
	int columns;
	int rows;
	int statistics_hz;
	int effective_statistics_hz;
	int source_fps;
	int frames_until_statistics;
	int auto_local_votes;
	int auto_global_votes;
	uint64_t statistics_updates;
	double max_shadow_gain;
	double max_highlight_reduction;
	double temporal_alpha;
	double minimum_gain;
	double maximum_gain;
	double tile_gain[VIDEO_BROKER_TONE_MAX_TILES];
	uint16_t tile_gain_lut_index[VIDEO_BROKER_TONE_MAX_TILES];
	unsigned char global_lut[256];
	int geometry_width;
	int geometry_height;
	uint16_t *x_left;
	uint16_t *x_right;
	uint16_t *x_weight;
	uint16_t *y_top;
	uint16_t *y_bottom;
	uint16_t *y_weight;
	uint16_t *spatial_lut_index;
} video_broker_nv12_tone_mapper_t;

static inline int video_broker_tone_clamp_int(int value, int minimum, int maximum) {
	if (value < minimum) {
		return minimum;
	}
	if (value > maximum) {
		return maximum;
	}
	return value;
}

static inline double video_broker_tone_clamp_double(double value, double minimum, double maximum) {
	if (value < minimum) {
		return minimum;
	}
	if (value > maximum) {
		return maximum;
	}
	return value;
}

static inline video_broker_tone_mode_t video_broker_tone_parse_mode(const char *value) {
	if (value == NULL || *value == '\0' || strcmp(value, "auto") == 0) {
		return VIDEO_BROKER_TONE_AUTO;
	}
	if (strcmp(value, "off") == 0) {
		return VIDEO_BROKER_TONE_OFF;
	}
	if (strcmp(value, "global") == 0) {
		return VIDEO_BROKER_TONE_GLOBAL;
	}
	if (strcmp(value, "local") == 0) {
		return VIDEO_BROKER_TONE_LOCAL;
	}
	return VIDEO_BROKER_TONE_AUTO;
}

static inline const char *video_broker_tone_mode_name(video_broker_tone_mode_t mode) {
	switch (mode) {
	case VIDEO_BROKER_TONE_OFF:
		return "off";
	case VIDEO_BROKER_TONE_GLOBAL:
		return "global";
	case VIDEO_BROKER_TONE_LOCAL:
		return "local";
	case VIDEO_BROKER_TONE_AUTO:
		return "auto";
	default:
		return "unknown";
	}
}

static inline unsigned char video_broker_tone_map_value(int value, double gain) {
	double x;
	double corrected;
	int rounded;
	if (value <= 0) {
		return 0;
	}
	/* Preserve the sensor's near-black floor instead of amplifying read noise. */
	if (value <= 4) {
		return (unsigned char)value;
	}
	x = (double)value / 255.0;
	if (gain >= 1.0) {
		/* The low-order exposure curve preserves contrast but cannot make retained
		 * shadow texture intelligible in a strongly backlit scene: a configured
		 * 2.4 gain maps code 20 only to about 44. Blend it into a bounded
		 * perceptual midtone curve after a protected toe. Codes immediately above
		 * the sensor floor therefore remain quiet, while useful shadow samples are
		 * lifted without moving black or the white endpoint. LUT construction is
		 * the monotonicity backstop for quantisation at the blend boundary. */
		double exposure = (double)value +
			(gain - 1.0) * (double)value * (1.0 - x) * (1.0 - x);
		double perceptual_gamma = 1.0 + 0.75 * (gain - 1.0);
		double perceptual = 255.0 * pow(x, 1.0 / perceptual_gamma);
		double toe = video_broker_tone_clamp_double(((double)value - 4.0) / 28.0, 0.0, 1.0);
		toe = toe * toe * (3.0 - 2.0 * toe);
		corrected = exposure + (perceptual - exposure) * toe;
	} else {
		/* Move the highlight endpoint down without destroying texture near it.
		 * Smoothstep has zero slope at black and white, so the resulting curve
		 * keeps local edge contrast at both ends instead of halving detail near a
		 * bright window as the former quadratic attenuation did. The LUT builder
		 * below remains the monotonicity backstop for extreme configured bounds. */
		double smooth = x * x * (3.0 - 2.0 * x);
		corrected = (double)value - (1.0 - gain) * 255.0 * smooth;
	}
	rounded = (int)(corrected + 0.5);
	return (unsigned char)video_broker_tone_clamp_int(rounded, 0, 255);
}

static inline void video_broker_tone_build_lut(unsigned char lut[256], double gain) {
	int value;
	lut[0] = 0;
	for (value = 1; value < 256; ++value) {
		lut[value] = video_broker_tone_map_value(value, gain);
		if (lut[value] < lut[value - 1]) {
			lut[value] = lut[value - 1];
		}
	}
}

static inline double video_broker_tone_gain_for_lut_index(int index) {
	const double minimum = 0.25;
	const double maximum = 3.0;
	index = video_broker_tone_clamp_int(index, 0, VIDEO_BROKER_TONE_GAIN_LUT_LEVELS - 1);
	return minimum + (maximum - minimum) * (double)index /
		(double)(VIDEO_BROKER_TONE_GAIN_LUT_LEVELS - 1);
}

static inline int video_broker_tone_gain_lut_index(double gain) {
	const double minimum = 0.25;
	const double maximum = 3.0;
	double normalized = (video_broker_tone_clamp_double(gain, minimum, maximum) - minimum) /
		(maximum - minimum);
	return video_broker_tone_clamp_int(
		(int)(normalized * (double)(VIDEO_BROKER_TONE_GAIN_LUT_LEVELS - 1) + 0.5),
		0,
		VIDEO_BROKER_TONE_GAIN_LUT_LEVELS - 1);
}

static inline void video_broker_tone_prepare_gain_luts(void) {
	int index;
	if (video_broker_tone_gain_lut_ready) {
		return;
	}
	for (index = 0; index < VIDEO_BROKER_TONE_GAIN_LUT_LEVELS; ++index) {
		video_broker_tone_build_lut(
			video_broker_tone_gain_lut[index],
			video_broker_tone_gain_for_lut_index(index));
	}
	video_broker_tone_gain_lut_ready = true;
}

static inline void video_broker_nv12_tone_mapper_release_geometry(
	video_broker_nv12_tone_mapper_t *mapper
) {
	if (mapper == NULL) {
		return;
	}
	free(mapper->x_left);
	free(mapper->x_right);
	free(mapper->x_weight);
	free(mapper->y_top);
	free(mapper->y_bottom);
	free(mapper->y_weight);
	free(mapper->spatial_lut_index);
	mapper->x_left = NULL;
	mapper->x_right = NULL;
	mapper->x_weight = NULL;
	mapper->y_top = NULL;
	mapper->y_bottom = NULL;
	mapper->y_weight = NULL;
	mapper->spatial_lut_index = NULL;
	mapper->geometry_width = 0;
	mapper->geometry_height = 0;
	mapper->spatial_lut_dirty = true;
}

static inline void video_broker_nv12_tone_mapper_destroy(
	video_broker_nv12_tone_mapper_t *mapper
) {
	video_broker_nv12_tone_mapper_release_geometry(mapper);
}

static inline void video_broker_nv12_tone_mapper_init(
	video_broker_nv12_tone_mapper_t *mapper,
	video_broker_tone_mode_t mode,
	int target_luma,
	int hysteresis,
	int columns,
	int rows,
	int statistics_hz,
	int source_fps,
	double max_shadow_gain,
	double max_highlight_reduction,
	double temporal_alpha,
	bool noise_guard
) {
	int tile;
	if (mapper == NULL) {
		return;
	}
	memset(mapper, 0, sizeof(*mapper));
	video_broker_tone_prepare_gain_luts();
	mapper->requested_mode = mode;
	mapper->spatial_lut_dirty = true;
	mapper->active_mode = mode == VIDEO_BROKER_TONE_AUTO ? VIDEO_BROKER_TONE_GLOBAL : mode;
	mapper->enabled = mode != VIDEO_BROKER_TONE_OFF;
	mapper->signal_informative = false;
	mapper->noise_guard = noise_guard;
	mapper->target_luma = video_broker_tone_clamp_int(target_luma, 16, 235);
	mapper->hysteresis = video_broker_tone_clamp_int(hysteresis, 1, 64);
	mapper->columns = video_broker_tone_clamp_int(columns, 1, VIDEO_BROKER_TONE_MAX_COLUMNS);
	mapper->rows = video_broker_tone_clamp_int(rows, 1, VIDEO_BROKER_TONE_MAX_ROWS);
	mapper->statistics_hz = video_broker_tone_clamp_int(statistics_hz, 1, 30);
	mapper->effective_statistics_hz = mapper->statistics_hz;
	mapper->source_fps = video_broker_tone_clamp_int(source_fps, 1, 240);
	mapper->max_shadow_gain = video_broker_tone_clamp_double(max_shadow_gain, 1.0, 3.0);
	mapper->max_highlight_reduction = video_broker_tone_clamp_double(max_highlight_reduction, 0.0, 0.75);
	mapper->temporal_alpha = video_broker_tone_clamp_double(temporal_alpha, 0.01, 1.0);
	mapper->minimum_gain = 1.0;
	mapper->maximum_gain = 1.0;
	for (tile = 0; tile < VIDEO_BROKER_TONE_MAX_TILES; ++tile) {
		mapper->tile_gain[tile] = 1.0;
		mapper->tile_gain_lut_index[tile] = (uint16_t)video_broker_tone_gain_lut_index(1.0);
	}
	video_broker_tone_build_lut(mapper->global_lut, 1.0);
}

static inline void video_broker_nv12_tone_mapper_set_signal_informative(
	video_broker_nv12_tone_mapper_t *mapper,
	bool informative
) {
	if (mapper == NULL) {
		return;
	}
	mapper->signal_informative = informative;
	if (!informative) {
		mapper->frames_until_statistics = 0;
	}
}

static inline int video_broker_tone_histogram_percentile(
	const uint32_t histogram[VIDEO_BROKER_TONE_HISTOGRAM_BINS],
	uint32_t samples,
	int percentile
) {
	uint32_t threshold;
	uint32_t accumulated = 0;
	int bin;
	if (samples == 0) {
		return 0;
	}
	threshold = (samples * (uint32_t)percentile + 99U) / 100U;
	if (threshold == 0) {
		threshold = 1;
	}
	for (bin = 0; bin < VIDEO_BROKER_TONE_HISTOGRAM_BINS; ++bin) {
		accumulated += histogram[bin];
		if (accumulated >= threshold) {
			return video_broker_tone_clamp_int(bin * 4 + 2, 0, 255);
		}
	}
	return 255;
}

static inline double video_broker_tone_desired_gain(
	const video_broker_nv12_tone_mapper_t *mapper,
	int p10,
	int median,
	int p90
) {
	double gain;
	if (mapper == NULL || median <= 4) {
		return 1.0;
	}
	if (mapper->noise_guard && median < 16 && p90 - p10 < 10) {
		return 1.0;
	}
	if (abs(median - mapper->target_luma) <= mapper->hysteresis) {
		return 1.0;
	}
	gain = (double)mapper->target_luma / (double)median;
	if (gain > 1.0) {
		/* A tile already containing highlights may lift its shadows, but must not
		 * use the full gain and wash the highlight edge into a halo. */
		if (p90 >= 235) {
			gain = 1.0 + (gain - 1.0) * 0.35;
		}
		gain = video_broker_tone_clamp_double(gain, 1.0, mapper->max_shadow_gain);
	} else {
		gain = video_broker_tone_clamp_double(
			gain, 1.0 - mapper->max_highlight_reduction, 1.0);
	}
	return gain;
}

static inline void video_broker_tone_update_auto_mode(
	video_broker_nv12_tone_mapper_t *mapper,
	int minimum_median,
	int maximum_median
) {
	int spread;
	if (mapper == NULL || mapper->requested_mode != VIDEO_BROKER_TONE_AUTO) {
		return;
	}
	spread = maximum_median - minimum_median;
	if (spread >= 36) {
		mapper->auto_local_votes++;
		mapper->auto_global_votes = 0;
	} else if (spread <= 22) {
		mapper->auto_global_votes++;
		mapper->auto_local_votes = 0;
	} else {
		mapper->auto_local_votes = 0;
		mapper->auto_global_votes = 0;
	}
	if (!mapper->auto_local_active && mapper->auto_local_votes >= 3) {
		mapper->auto_local_active = true;
		mapper->auto_local_votes = 0;
	} else if (mapper->auto_local_active && mapper->auto_global_votes >= 8) {
		mapper->auto_local_active = false;
		mapper->auto_global_votes = 0;
	}
	mapper->active_mode = mapper->auto_local_active ? VIDEO_BROKER_TONE_LOCAL : VIDEO_BROKER_TONE_GLOBAL;
}

static inline bool video_broker_nv12_tone_mapper_update(
	video_broker_nv12_tone_mapper_t *mapper,
	const unsigned char *nv12,
	int width,
	int height,
	int sample_stride
) {
	uint32_t histogram[VIDEO_BROKER_TONE_MAX_TILES][VIDEO_BROKER_TONE_HISTOGRAM_BINS];
	uint32_t samples[VIDEO_BROKER_TONE_MAX_TILES];
	uint32_t global_histogram[VIDEO_BROKER_TONE_HISTOGRAM_BINS];
	uint32_t global_samples = 0;
	double desired[VIDEO_BROKER_TONE_MAX_TILES];
	double smoothed[VIDEO_BROKER_TONE_MAX_TILES] = {0};
	int medians[VIDEO_BROKER_TONE_MAX_TILES];
	int tile_count;
	int minimum_median = 255;
	int maximum_median = 0;
	int global_p10;
	int global_median;
	int global_p90;
	int x;
	int y;
	int tile;
	int effective_statistics_hz;
	int interval;

	if (mapper == NULL || !mapper->enabled || !mapper->signal_informative ||
	    nv12 == NULL || width <= 0 || height <= 0) {
		return false;
	}
	if (mapper->frames_until_statistics > 0) {
		mapper->frames_until_statistics--;
		return false;
	}
	{
		uint64_t pixels = (uint64_t)(unsigned int)width * (uint64_t)(unsigned int)height;
		uint64_t pixel_bound = pixels == 0 ? 1 :
			(uint64_t)VIDEO_BROKER_TONE_SPATIAL_LUT_PIXELS_PER_SECOND / pixels;
		if (pixel_bound < 1) {
			pixel_bound = 1;
		}
		effective_statistics_hz = mapper->statistics_hz;
		if ((uint64_t)effective_statistics_hz > pixel_bound) {
			effective_statistics_hz = (int)pixel_bound;
		}
		if (effective_statistics_hz > mapper->source_fps) {
			effective_statistics_hz = mapper->source_fps;
		}
		mapper->effective_statistics_hz = effective_statistics_hz;
	}
	/* Ceiling division guarantees the learned-map cadence never exceeds the
	 * configured or resolution-derived budget. */
	interval = (mapper->source_fps + effective_statistics_hz - 1) /
		effective_statistics_hz;
	mapper->frames_until_statistics = interval > 1 ? interval - 1 : 0;
	if (sample_stride <= 0) {
		sample_stride = 1;
	}
	/* Keep statistics bounded at high resolutions independently of frame rate. */
	while ((width / sample_stride) * (height / sample_stride) > 160000) {
		sample_stride++;
	}
	tile_count = mapper->columns * mapper->rows;
	memset(histogram, 0, sizeof(histogram));
	memset(samples, 0, sizeof(samples));
	memset(global_histogram, 0, sizeof(global_histogram));
	for (y = 0; y < height; y += sample_stride) {
		int tile_y = video_broker_tone_clamp_int((y * mapper->rows) / height, 0, mapper->rows - 1);
		for (x = 0; x < width; x += sample_stride) {
			unsigned int value = nv12[(size_t)y * (size_t)width + (size_t)x];
			int tile_x = video_broker_tone_clamp_int((x * mapper->columns) / width, 0, mapper->columns - 1);
			int tile_index = tile_y * mapper->columns + tile_x;
			int bin = (int)(value >> 2);
			/* Codes 0..4 represent true black/no-signal and must not pull the
			 * content median down into an aggressive gain. */
			if (value <= 4) {
				continue;
			}
			histogram[tile_index][bin]++;
			samples[tile_index]++;
			global_histogram[bin]++;
			global_samples++;
		}
	}
	if (global_samples < 32) {
		return false;
	}
	global_p10 = video_broker_tone_histogram_percentile(global_histogram, global_samples, 10);
	global_median = video_broker_tone_histogram_percentile(global_histogram, global_samples, 50);
	global_p90 = video_broker_tone_histogram_percentile(global_histogram, global_samples, 90);
	for (tile = 0; tile < tile_count; ++tile) {
		int p10 = global_p10;
		int median = global_median;
		int p90 = global_p90;
		if (samples[tile] >= 8) {
			p10 = video_broker_tone_histogram_percentile(histogram[tile], samples[tile], 10);
			median = video_broker_tone_histogram_percentile(histogram[tile], samples[tile], 50);
			p90 = video_broker_tone_histogram_percentile(histogram[tile], samples[tile], 90);
		}
		medians[tile] = median;
		if (median < minimum_median) {
			minimum_median = median;
		}
		if (median > maximum_median) {
			maximum_median = median;
		}
		desired[tile] = video_broker_tone_desired_gain(mapper, p10, median, p90);
	}
	video_broker_tone_update_auto_mode(mapper, minimum_median, maximum_median);
	if (mapper->active_mode == VIDEO_BROKER_TONE_GLOBAL) {
		double global_gain = video_broker_tone_desired_gain(mapper, global_p10, global_median, global_p90);
		for (tile = 0; tile < tile_count; ++tile) {
			desired[tile] = global_gain;
		}
	} else {
		/* One compact 3x3 spatial pass suppresses tile seams and halos while
		 * retaining meaningful room/window separation. */
		for (tile = 0; tile < tile_count; ++tile) {
			int tile_x = tile % mapper->columns;
			int tile_y = tile / mapper->columns;
			double sum = desired[tile] * 4.0;
			double weight = 4.0;
			int oy;
			int ox;
			for (oy = -1; oy <= 1; ++oy) {
				for (ox = -1; ox <= 1; ++ox) {
					int neighbor_x;
					int neighbor_y;
					if (ox == 0 && oy == 0) {
						continue;
					}
					neighbor_x = tile_x + ox;
					neighbor_y = tile_y + oy;
					if (neighbor_x < 0 || neighbor_x >= mapper->columns ||
					    neighbor_y < 0 || neighbor_y >= mapper->rows) {
						continue;
					}
					sum += desired[neighbor_y * mapper->columns + neighbor_x];
					weight += 1.0;
				}
			}
			smoothed[tile] = sum / weight;
		}
		memcpy(desired, smoothed, (size_t)tile_count * sizeof(desired[0]));
	}
	mapper->minimum_gain = 10.0;
	mapper->maximum_gain = 0.0;
	for (tile = 0; tile < tile_count; ++tile) {
		double next_gain;
		if (!mapper->initialized) {
			next_gain = desired[tile];
		} else {
			double delta = video_broker_tone_clamp_double(
				desired[tile] - mapper->tile_gain[tile], -0.20, 0.20);
			next_gain = mapper->tile_gain[tile] + delta * mapper->temporal_alpha;
		}
		if (fabs(next_gain - 1.0) < 0.01) {
			next_gain = 1.0;
		}
		mapper->tile_gain[tile] = next_gain;
		mapper->tile_gain_lut_index[tile] = (uint16_t)video_broker_tone_gain_lut_index(next_gain);
		if (next_gain < mapper->minimum_gain) {
			mapper->minimum_gain = next_gain;
		}
		if (next_gain > mapper->maximum_gain) {
			mapper->maximum_gain = next_gain;
		}
	}
	video_broker_tone_build_lut(mapper->global_lut, mapper->tile_gain[0]);
	mapper->spatial_lut_dirty = true;
	mapper->initialized = true;
	mapper->statistics_updates++;
	(void)medians;
	return true;
}

static inline bool video_broker_nv12_tone_mapper_prepare_geometry_axes(
	video_broker_nv12_tone_mapper_t *mapper,
	int width,
	int height
) {
	int x;
	int y;
	if (mapper == NULL || width <= 0 || height <= 0 || width > 65535 || height > 65535) {
		return false;
	}
	if (mapper->geometry_width == width && mapper->geometry_height == height &&
	    mapper->x_left != NULL && mapper->x_right != NULL && mapper->x_weight != NULL &&
	    mapper->y_top != NULL && mapper->y_bottom != NULL && mapper->y_weight != NULL) {
		return true;
	}
	video_broker_nv12_tone_mapper_release_geometry(mapper);
	mapper->x_left = (uint16_t *)calloc((size_t)width, sizeof(uint16_t));
	mapper->x_right = (uint16_t *)calloc((size_t)width, sizeof(uint16_t));
	mapper->x_weight = (uint16_t *)calloc((size_t)width, sizeof(uint16_t));
	mapper->y_top = (uint16_t *)calloc((size_t)height, sizeof(uint16_t));
	mapper->y_bottom = (uint16_t *)calloc((size_t)height, sizeof(uint16_t));
	mapper->y_weight = (uint16_t *)calloc((size_t)height, sizeof(uint16_t));
	if (mapper->x_left == NULL || mapper->x_right == NULL || mapper->x_weight == NULL ||
	    mapper->y_top == NULL || mapper->y_bottom == NULL || mapper->y_weight == NULL) {
		video_broker_nv12_tone_mapper_release_geometry(mapper);
		return false;
	}
	for (x = 0; x < width; ++x) {
		double grid = ((double)x + 0.5) * (double)mapper->columns / (double)width - 0.5;
		int left = (int)floor(grid);
		double fraction = grid - (double)left;
		int right = left + 1;
		if (left < 0) {
			left = 0;
			fraction = 0.0;
		}
		if (right >= mapper->columns) {
			right = mapper->columns - 1;
			fraction = 0.0;
		}
		mapper->x_left[x] = (uint16_t)left;
		mapper->x_right[x] = (uint16_t)right;
		mapper->x_weight[x] = (uint16_t)video_broker_tone_clamp_int((int)(fraction * 256.0 + 0.5), 0, 256);
	}
	for (y = 0; y < height; ++y) {
		double grid = ((double)y + 0.5) * (double)mapper->rows / (double)height - 0.5;
		int top = (int)floor(grid);
		double fraction = grid - (double)top;
		int bottom = top + 1;
		if (top < 0) {
			top = 0;
			fraction = 0.0;
		}
		if (bottom >= mapper->rows) {
			bottom = mapper->rows - 1;
			fraction = 0.0;
		}
		mapper->y_top[y] = (uint16_t)top;
		mapper->y_bottom[y] = (uint16_t)bottom;
		mapper->y_weight[y] = (uint16_t)video_broker_tone_clamp_int((int)(fraction * 256.0 + 0.5), 0, 256);
	}
	mapper->geometry_width = width;
	mapper->geometry_height = height;
	mapper->spatial_lut_dirty = true;
	return true;
}

static inline bool video_broker_nv12_tone_mapper_prepare_geometry(
	video_broker_nv12_tone_mapper_t *mapper,
	int width,
	int height
) {
	if (!video_broker_nv12_tone_mapper_prepare_geometry_axes(mapper, width, height)) {
		return false;
	}
	if (mapper->spatial_lut_index != NULL) {
		return true;
	}
	if ((size_t)width > SIZE_MAX / (size_t)height / sizeof(uint16_t)) {
		return false;
	}
	mapper->spatial_lut_index = (uint16_t *)calloc(
		(size_t)width * (size_t)height, sizeof(uint16_t));
	if (mapper->spatial_lut_index == NULL) {
		return false;
	}
	mapper->spatial_lut_dirty = true;
	return true;
}

static inline bool video_broker_nv12_tone_mapper_prepare_spatial_lut(
	video_broker_nv12_tone_mapper_t *mapper,
	int width,
	int height
) {
	int x;
	int y;
	if (!video_broker_nv12_tone_mapper_prepare_geometry(mapper, width, height)) {
		return false;
	}
	if (!mapper->spatial_lut_dirty) {
		return true;
	}
	for (y = 0; y < height; ++y) {
		int top = mapper->y_top[y];
		int bottom = mapper->y_bottom[y];
		int wy = mapper->y_weight[y];
		uint16_t *row = mapper->spatial_lut_index + (size_t)y * (size_t)width;
		for (x = 0; x < width; ++x) {
			int left = mapper->x_left[x];
			int right = mapper->x_right[x];
			int wx = mapper->x_weight[x];
			int top_left = mapper->tile_gain_lut_index[top * mapper->columns + left];
			int top_right = mapper->tile_gain_lut_index[top * mapper->columns + right];
			int bottom_left = mapper->tile_gain_lut_index[bottom * mapper->columns + left];
			int bottom_right = mapper->tile_gain_lut_index[bottom * mapper->columns + right];
			int top_index = top_left + ((top_right - top_left) * wx + 128) / 256;
			int bottom_index = bottom_left + ((bottom_right - bottom_left) * wx + 128) / 256;
			row[x] = (uint16_t)(top_index + ((bottom_index - top_index) * wy + 128) / 256);
		}
	}
	mapper->spatial_lut_dirty = false;
	return true;
}

static inline bool video_broker_nv12_tone_mapper_active(
	const video_broker_nv12_tone_mapper_t *mapper
) {
	return mapper != NULL && mapper->enabled && mapper->signal_informative && mapper->initialized &&
		(mapper->minimum_gain < 0.99 || mapper->maximum_gain > 1.01);
}

static inline bool video_broker_nv12_tone_mapper_apply(
	video_broker_nv12_tone_mapper_t *mapper,
	unsigned char *nv12,
	int width,
	int height
) {
	size_t offset;
	size_t y_size;
	if (!video_broker_nv12_tone_mapper_active(mapper) || nv12 == NULL || width <= 0 || height <= 0) {
		return false;
	}
	if (mapper->active_mode == VIDEO_BROKER_TONE_GLOBAL || mapper->columns == 1 || mapper->rows == 1) {
		size_t y_size = (size_t)width * (size_t)height;
		size_t offset;
		for (offset = 0; offset < y_size; ++offset) {
			nv12[offset] = mapper->global_lut[nv12[offset]];
		}
		return true;
	}
	if (!video_broker_nv12_tone_mapper_prepare_spatial_lut(mapper, width, height)) {
		return false;
	}
	y_size = (size_t)width * (size_t)height;
	for (offset = 0; offset < y_size; ++offset) {
		nv12[offset] = video_broker_tone_gain_lut[mapper->spatial_lut_index[offset]][nv12[offset]];
	}
	return true;
}

#endif
