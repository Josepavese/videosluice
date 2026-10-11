/* RDK X5 mux worker owned by the standalone video-broker data plane. */
#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <signal.h>
#include "worker_signal.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "sp_codec.h"
#include "sp_vio.h"
#include "archive_policy.h"
#include "h26x_bootstrap.h"
#include "nv12_orientation.h"
#include "nv12_tone_mapper_accelerator.h"
#include "nv12_quality_measure.h"
#include "nv12_quality_state.h"
#include "worker_fanout_protocol.h"
#include "frame_observation.h"
#include "worker_release.h"

static volatile sig_atomic_t g_running = 1;
static volatile sig_atomic_t g_stop_signal = 0;

enum {
	kDefaultSensorWidth = 1920,
	kDefaultSensorHeight = 1080,
	kDefaultOutputWidth = 1920,
	kDefaultOutputHeight = 1088,
	kDefaultSubWidth = 512,
	kDefaultSubHeight = 512,
	kDefaultFPS = 30,
	kDefaultWarmupSec = 5,
	kDefaultBitrateKbps = 8192,
	kDefaultTimeoutMs = 2000,
	kDefaultPipeID = 0,
	kDefaultVideoIndex = -1,
	kMaxOutputChannels = 5,
	kMainStreamID = 1,
	kSubStreamID = 2,
	kArchiveH265StreamID = 3,
	kArchiveH264StreamID = 6,
	kStreamBufferFloor = 1 << 20,
	kStreamBufferCeiling = 64 << 20,
	kStreamBufferGuard = 4096,
	kDefaultQualityReportEvery = 900,
	kDefaultQualitySampleStride = 8,
	kFanoutFrameTimeoutMs = 8000,
};

typedef struct {
	const char *sensor_prefix;
	int sensor_width;
	int sensor_height;
	int video_index;
	int output_width;
	int output_height;
	int sub_width;
	int sub_height;
	int sub_fps;
	int main_fps;
	int archive_width;
	int archive_height;
	int archive_fps;
	int live_codec;
	int archive_codec;
	int fps;
	int warmup_sec;
	int live_encode_bitrate_kbps;
	int archive_encode_bitrate_kbps;
	int timeout_ms;
	bool live_enabled;
	bool analysis_enabled;
	bool archive_enabled;
	bool live_enabled_explicit;
	bool analysis_enabled_explicit;
	bool archive_enabled_explicit;
	bool live_codec_explicit;
	video_broker_video_orientation_t video_orientation;
	video_broker_tone_mode_t tone_mapping_mode;
	video_broker_tone_accelerator_kind_t tone_mapping_accelerator;
	int tone_mapping_target;
	int tone_mapping_hysteresis;
	int tone_mapping_grid_columns;
	int tone_mapping_grid_rows;
	int tone_mapping_statistics_hz;
	double tone_mapping_max_shadow_gain;
	double tone_mapping_max_highlight_reduction;
	double tone_mapping_temporal_alpha;
	bool tone_mapping_noise_guard;
	bool quality_monitor_enabled;
	int quality_monitor_report_every;
	int quality_monitor_sample_stride;
	const char *quality_monitor_state_path;
	const char *source_id;
	uint64_t media_epoch;
	bool source_id_explicit;
	bool media_epoch_explicit;
	video_broker_archive_content_t archive_content;
	int admitted_widths[kMaxOutputChannels];
	int admitted_heights[kMaxOutputChannels];
	int admitted_surface_count;
} bridge_config_t;

// A role runtime owns every mutable resource that must later move together at
// cutover. Capture/VSE stays process-owned; encoders, buffers, cadence, tone
// state and frame identity are deliberately never shared across roles.
typedef struct {
	video_broker_worker_role_t role;
	video_broker_worker_branch_profile_t profile;
	uint64_t branch_epoch;
	void *encoder;
	unsigned char *frame;
	unsigned char *oriented;
	unsigned char *stream;
	size_t frame_size;
	size_t stream_capacity;
	video_broker_h26x_bootstrap_t bootstrap;
	int emit_accumulator;
	uint64_t frames;
	uint64_t last_quality_sample_us;
	bool seen_vps;
	bool seen_sps;
	bool seen_pps;
	bool seen_keyframe;
	bool logged_first_frame;
	bool logged_first_stream;
	video_broker_nv12_tone_mapper_t tone_mapper;
	video_broker_tone_accelerator_t tone_accelerator;
} hbn_role_runtime_t;

typedef struct {
	bool active_present;
	bool candidate_present;
	bool incumbent_present;
	hbn_role_runtime_t active;
	hbn_role_runtime_t candidate;
	hbn_role_runtime_t incumbent;
	uint64_t next_epoch;
	uint64_t cutover_frame_floor;
} hbn_role_slot_t;

typedef struct {
	const unsigned char *analysis;
	int analysis_width;
	int analysis_height;
	bool analysis_ready;
} hbn_quality_context_t;

typedef struct {
	const bridge_config_t *cfg;
	void *vio;
	FILE *output;
	const int *widths;
	const int *heights;
	int channel_count;
	hbn_role_slot_t *slots;
	hbn_quality_context_t *quality;
	video_broker_worker_branch_change_t changes[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	size_t change_count;
	bool cutover_completed;
} hbn_fanout_backend_t;

typedef struct {
	char buffer[VIDEO_BROKER_WORKER_FANOUT_MAX_LINE_BYTES];
	size_t used;
	video_broker_worker_fanout_controller_t *controller;
} hbn_fanout_input_t;

static void handle_signal(int signum) {
	g_running = 0;
	g_stop_signal = signum;
}

static bool parse_int_arg(const char *value, int minimum, int maximum, int *out) {
	char *end = NULL;
	long parsed;
	if (value == NULL || *value == '\0' || out == NULL) {
		return false;
	}
	errno = 0;
	parsed = strtol(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0' || parsed < minimum || parsed > maximum) {
		return false;
	}
	*out = (int)parsed;
	return true;
}

static bool parse_uint64_arg(const char *value, uint64_t *out) {
	char *end = NULL;
	unsigned long long parsed;
	if (value == NULL || *value == '\0' || out == NULL || *value == '-') {
		return false;
	}
	errno = 0;
	parsed = strtoull(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0' || parsed == 0) {
		return false;
	}
	*out = (uint64_t)parsed;
	return true;
}

static bool parse_double_arg(const char *value, double *out) {
	char *end = NULL;
	double parsed;
	if (value == NULL || *value == '\0' || out == NULL) {
		return false;
	}
	errno = 0;
	parsed = strtod(value, &end);
	if (errno != 0 || end == value || *end != '\0' || !isfinite(parsed)) {
		return false;
	}
	*out = parsed;
	return true;
}

static bool parse_surface_arg(const char *value, int *width, int *height) {
	char tail = '\0';
	int parsed_width = 0;
	int parsed_height = 0;
	if (value == NULL || width == NULL || height == NULL ||
	    sscanf(value, "%dx%d%c", &parsed_width, &parsed_height, &tail) != 2 ||
	    parsed_width <= 0 || parsed_height <= 0 ||
	    (parsed_width & 1) != 0 || (parsed_height & 1) != 0) {
		return false;
	}
	*width = parsed_width;
	*height = parsed_height;
	return true;
}

static bool parse_required_bool(const char *value, bool *out) {
	if (value == NULL || out == NULL) {
		return false;
	}
	if (strcmp(value, "true") == 0) {
		*out = true;
		return true;
	}
	if (strcmp(value, "false") == 0) {
		*out = false;
		return true;
	}
	return false;
}

static bool parse_video_orientation_arg(const char *value, video_broker_video_orientation_t *out) {
	if (value == NULL || out == NULL) {
		return false;
	}
	if (strcmp(value, "normal") == 0) {
		*out = VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL;
	} else if (strcmp(value, "rotate_180") == 0) {
		*out = VIDEO_BROKER_VIDEO_ORIENTATION_ROTATE_180;
	} else if (strcmp(value, "mirror_horizontal") == 0) {
		*out = VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_HORIZONTAL;
	} else if (strcmp(value, "mirror_vertical") == 0) {
		*out = VIDEO_BROKER_VIDEO_ORIENTATION_MIRROR_VERTICAL;
	} else {
		return false;
	}
	return true;
}

static bool parse_tone_mode_arg(const char *value, video_broker_tone_mode_t *out) {
	if (value == NULL || out == NULL) {
		return false;
	}
	if (strcmp(value, "auto") == 0) {
		*out = VIDEO_BROKER_TONE_AUTO;
	} else if (strcmp(value, "off") == 0) {
		*out = VIDEO_BROKER_TONE_OFF;
	} else if (strcmp(value, "global") == 0) {
		*out = VIDEO_BROKER_TONE_GLOBAL;
	} else if (strcmp(value, "local") == 0) {
		*out = VIDEO_BROKER_TONE_LOCAL;
	} else {
		return false;
	}
	return true;
}

static bool parse_tone_accelerator_arg(const char *value, video_broker_tone_accelerator_kind_t *out) {
	if (value == NULL || out == NULL) {
		return false;
	}
	if (strcmp(value, "auto") == 0) {
		*out = VIDEO_BROKER_TONE_ACCELERATOR_AUTO;
	} else if (strcmp(value, "cpu") == 0) {
		*out = VIDEO_BROKER_TONE_ACCELERATOR_CPU;
	} else if (strcmp(value, "opencl") == 0) {
		*out = VIDEO_BROKER_TONE_ACCELERATOR_OPENCL;
	} else {
		return false;
	}
	return true;
}

static bool parse_archive_content_arg(const char *value, video_broker_archive_content_t *out) {
	if (value == NULL || out == NULL) {
		return false;
	}
	if (strcmp(value, "adaptive") == 0) {
		*out = VIDEO_BROKER_ARCHIVE_ADAPTIVE;
	} else if (strcmp(value, "source") == 0) {
		*out = VIDEO_BROKER_ARCHIVE_SOURCE;
	} else if (strcmp(value, "processed") == 0) {
		*out = VIDEO_BROKER_ARCHIVE_PROCESSED;
	} else {
		return false;
	}
	return true;
}

static void default_config(bridge_config_t *cfg) {
	memset(cfg, 0, sizeof(*cfg));
	cfg->sensor_prefix = "imx219";
	cfg->sensor_width = kDefaultSensorWidth;
	cfg->sensor_height = kDefaultSensorHeight;
	cfg->video_index = kDefaultVideoIndex;
	cfg->output_width = kDefaultOutputWidth;
	cfg->output_height = kDefaultOutputHeight;
	cfg->sub_width = kDefaultSubWidth;
	cfg->sub_height = kDefaultSubHeight;
	cfg->sub_fps = kDefaultFPS;
	cfg->main_fps = kDefaultFPS;
	cfg->archive_width = kDefaultSensorWidth;
	cfg->archive_height = kDefaultSensorHeight;
	cfg->archive_fps = kDefaultFPS;
	cfg->live_codec = SP_ENCODER_H265;
	cfg->archive_codec = SP_ENCODER_H265;
	cfg->fps = kDefaultFPS;
	cfg->warmup_sec = kDefaultWarmupSec;
	cfg->live_encode_bitrate_kbps = kDefaultBitrateKbps;
	cfg->archive_encode_bitrate_kbps = kDefaultBitrateKbps;
	cfg->timeout_ms = kDefaultTimeoutMs;
	cfg->live_enabled = false;
	cfg->analysis_enabled = false;
	cfg->archive_enabled = false;
	cfg->video_orientation = VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL;
	cfg->tone_mapping_mode = VIDEO_BROKER_TONE_OFF;
	cfg->tone_mapping_accelerator = VIDEO_BROKER_TONE_ACCELERATOR_AUTO;
	cfg->tone_mapping_target = 128;
	cfg->tone_mapping_hysteresis = 12;
	cfg->tone_mapping_grid_columns = 16;
	cfg->tone_mapping_grid_rows = 9;
	cfg->tone_mapping_statistics_hz = 8;
	cfg->tone_mapping_max_shadow_gain = 2.4;
	cfg->tone_mapping_max_highlight_reduction = 0.20;
	cfg->tone_mapping_temporal_alpha = 0.35;
	cfg->tone_mapping_noise_guard = true;
	cfg->quality_monitor_enabled = false;
	cfg->quality_monitor_report_every = kDefaultQualityReportEvery;
	cfg->quality_monitor_sample_stride = kDefaultQualitySampleStride;
	cfg->archive_content = VIDEO_BROKER_ARCHIVE_ADAPTIVE;
}

static int parse_args(int argc, char **argv, bridge_config_t *cfg) {
#define REQUIRE_INT(ARG_NAME, FIELD, MINIMUM) \
	do { \
		if (++i >= argc || !parse_int_arg(argv[i], (MINIMUM), 1 << 20, &(FIELD))) { \
			fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid %s\n", (ARG_NAME)); \
			return -1; \
		} \
	} while (0)
#define REQUIRE_DOUBLE(ARG_NAME, FIELD) \
	do { \
		if (++i >= argc || !parse_double_arg(argv[i], &(FIELD))) { \
			fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid %s\n", (ARG_NAME)); \
			return -1; \
		} \
	} while (0)
	for (int i = 1; i < argc; ++i) {
		if (strcmp(argv[i], "--sensor-prefix") == 0 && i + 1 < argc) {
			cfg->sensor_prefix = argv[++i];
		} else if (strcmp(argv[i], "--sensor-width") == 0) {
			REQUIRE_INT("--sensor-width", cfg->sensor_width, 1);
		} else if (strcmp(argv[i], "--sensor-height") == 0) {
			REQUIRE_INT("--sensor-height", cfg->sensor_height, 1);
		} else if (strcmp(argv[i], "--video-index") == 0) {
			REQUIRE_INT("--video-index", cfg->video_index, -1);
		} else if (strcmp(argv[i], "--output-width") == 0) {
			REQUIRE_INT("--output-width", cfg->output_width, 1);
		} else if (strcmp(argv[i], "--output-height") == 0) {
			REQUIRE_INT("--output-height", cfg->output_height, 1);
		} else if (strcmp(argv[i], "--sub-width") == 0) {
			REQUIRE_INT("--sub-width", cfg->sub_width, 1);
		} else if (strcmp(argv[i], "--sub-height") == 0) {
			REQUIRE_INT("--sub-height", cfg->sub_height, 1);
		} else if (strcmp(argv[i], "--sub-fps") == 0) {
			REQUIRE_INT("--sub-fps", cfg->sub_fps, 1);
		} else if (strcmp(argv[i], "--main-fps") == 0) {
			REQUIRE_INT("--main-fps", cfg->main_fps, 1);
		} else if (strcmp(argv[i], "--archive-width") == 0) {
			REQUIRE_INT("--archive-width", cfg->archive_width, 1);
		} else if (strcmp(argv[i], "--archive-height") == 0) {
			REQUIRE_INT("--archive-height", cfg->archive_height, 1);
		} else if (strcmp(argv[i], "--admitted-surface") == 0) {
			int width = 0;
			int height = 0;
			if (++i >= argc || cfg->admitted_surface_count >= kMaxOutputChannels ||
			    !parse_surface_arg(argv[i], &width, &height)) {
				fprintf(stderr,
					"[rdkx5_hbn_mux_bridge] invalid --admitted-surface; expected even WIDTHxHEIGHT, max=%d\n",
					kMaxOutputChannels);
				return -1;
			}
			cfg->admitted_widths[cfg->admitted_surface_count] = width;
			cfg->admitted_heights[cfg->admitted_surface_count] = height;
			cfg->admitted_surface_count++;
		} else if (strcmp(argv[i], "--archive-fps") == 0) {
			REQUIRE_INT("--archive-fps", cfg->archive_fps, 1);
		} else if (strcmp(argv[i], "--live-codec") == 0 && i + 1 < argc) {
			const char *codec = argv[++i];
			if (strcmp(codec, "h265") == 0) {
				cfg->live_codec = SP_ENCODER_H265;
			} else if (strcmp(codec, "h264") == 0) {
				cfg->live_codec = SP_ENCODER_H264;
			} else {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --live-codec; expected h265|h264\n");
				return -1;
			}
			cfg->live_codec_explicit = true;
		} else if (strcmp(argv[i], "--archive-codec") == 0 && i + 1 < argc) {
			const char *codec = argv[++i];
			if (strcmp(codec, "h265") == 0) {
				cfg->archive_codec = SP_ENCODER_H265;
			} else if (strcmp(codec, "h264") == 0) {
				cfg->archive_codec = SP_ENCODER_H264;
			} else {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --archive-codec; expected h265|h264\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--archive-content") == 0) {
			if (++i >= argc || !parse_archive_content_arg(argv[i], &cfg->archive_content)) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --archive-content; expected adaptive|source|processed\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--fps") == 0) {
			REQUIRE_INT("--fps", cfg->fps, 1);
		} else if (strcmp(argv[i], "--warmup-sec") == 0) {
			REQUIRE_INT("--warmup-sec", cfg->warmup_sec, 0);
		} else if (strcmp(argv[i], "--live-encode-bitrate-kbps") == 0) {
			REQUIRE_INT("--live-encode-bitrate-kbps", cfg->live_encode_bitrate_kbps, 1);
		} else if (strcmp(argv[i], "--archive-encode-bitrate-kbps") == 0) {
			REQUIRE_INT("--archive-encode-bitrate-kbps", cfg->archive_encode_bitrate_kbps, 1);
		} else if (strcmp(argv[i], "--live-enabled") == 0) {
			if (++i >= argc || !parse_required_bool(argv[i], &cfg->live_enabled)) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --live-enabled; expected true|false\n");
				return -1;
			}
			cfg->live_enabled_explicit = true;
		} else if (strcmp(argv[i], "--analysis-enabled") == 0) {
			if (++i >= argc || !parse_required_bool(argv[i], &cfg->analysis_enabled)) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --analysis-enabled; expected true|false\n");
				return -1;
			}
			cfg->analysis_enabled_explicit = true;
		} else if (strcmp(argv[i], "--archive-enabled") == 0) {
			if (++i >= argc || !parse_required_bool(argv[i], &cfg->archive_enabled)) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --archive-enabled; expected true|false\n");
				return -1;
			}
			cfg->archive_enabled_explicit = true;
		} else if (strcmp(argv[i], "--timeout-ms") == 0) {
			REQUIRE_INT("--timeout-ms", cfg->timeout_ms, 1);
		} else if (strcmp(argv[i], "--video-orientation") == 0) {
			if (++i >= argc || !parse_video_orientation_arg(argv[i], &cfg->video_orientation)) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --video-orientation; expected normal|rotate_180|mirror_horizontal|mirror_vertical\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--tone-mapping-mode") == 0) {
			if (++i >= argc || !parse_tone_mode_arg(argv[i], &cfg->tone_mapping_mode)) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --tone-mapping-mode; expected auto|off|global|local\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--tone-mapping-accelerator") == 0) {
			if (++i >= argc || !parse_tone_accelerator_arg(argv[i], &cfg->tone_mapping_accelerator)) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --tone-mapping-accelerator; expected auto|cpu|opencl\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--tone-mapping-target") == 0) {
			REQUIRE_INT("--tone-mapping-target", cfg->tone_mapping_target, 1);
		} else if (strcmp(argv[i], "--tone-mapping-hysteresis") == 0) {
			REQUIRE_INT("--tone-mapping-hysteresis", cfg->tone_mapping_hysteresis, 1);
		} else if (strcmp(argv[i], "--tone-mapping-grid-columns") == 0) {
			REQUIRE_INT("--tone-mapping-grid-columns", cfg->tone_mapping_grid_columns, 1);
		} else if (strcmp(argv[i], "--tone-mapping-grid-rows") == 0) {
			REQUIRE_INT("--tone-mapping-grid-rows", cfg->tone_mapping_grid_rows, 1);
		} else if (strcmp(argv[i], "--tone-mapping-statistics-hz") == 0) {
			REQUIRE_INT("--tone-mapping-statistics-hz", cfg->tone_mapping_statistics_hz, 1);
		} else if (strcmp(argv[i], "--tone-mapping-max-shadow-gain") == 0) {
			REQUIRE_DOUBLE("--tone-mapping-max-shadow-gain", cfg->tone_mapping_max_shadow_gain);
		} else if (strcmp(argv[i], "--tone-mapping-max-highlight-reduction") == 0) {
			REQUIRE_DOUBLE("--tone-mapping-max-highlight-reduction", cfg->tone_mapping_max_highlight_reduction);
		} else if (strcmp(argv[i], "--tone-mapping-temporal-alpha") == 0) {
			REQUIRE_DOUBLE("--tone-mapping-temporal-alpha", cfg->tone_mapping_temporal_alpha);
		} else if (strcmp(argv[i], "--tone-mapping-noise-guard") == 0) {
			if (++i >= argc || !parse_required_bool(argv[i], &cfg->tone_mapping_noise_guard)) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --tone-mapping-noise-guard; expected true|false\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--quality-monitor-enabled") == 0) {
			if (++i >= argc || !parse_required_bool(argv[i], &cfg->quality_monitor_enabled)) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --quality-monitor-enabled; expected true|false\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--quality-monitor-report-every") == 0) {
			REQUIRE_INT("--quality-monitor-report-every", cfg->quality_monitor_report_every, 1);
		} else if (strcmp(argv[i], "--quality-monitor-sample-stride") == 0) {
			REQUIRE_INT("--quality-monitor-sample-stride", cfg->quality_monitor_sample_stride, 1);
		} else if (strcmp(argv[i], "--quality-monitor-state-path") == 0 && i + 1 < argc) {
			cfg->quality_monitor_state_path = argv[++i];
		} else if (strcmp(argv[i], "--source-id") == 0 && i + 1 < argc) {
			cfg->source_id = argv[++i];
			cfg->source_id_explicit = true;
		} else if (strcmp(argv[i], "--media-epoch") == 0) {
			if (++i >= argc || !parse_uint64_arg(argv[i], &cfg->media_epoch)) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] invalid --media-epoch\n");
				return -1;
			}
			cfg->media_epoch_explicit = true;
		} else {
			fprintf(stderr, "[rdkx5_hbn_mux_bridge] unknown or incomplete argument: %s\n", argv[i]);
			return -1;
		}
	}
#undef REQUIRE_DOUBLE
#undef REQUIRE_INT
	if (!cfg->live_enabled_explicit || !cfg->analysis_enabled_explicit || !cfg->archive_enabled_explicit ||
	    !cfg->live_codec_explicit ||
	    (!cfg->live_enabled && !cfg->analysis_enabled && !cfg->archive_enabled)) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] explicit output roles and live codec are required\n");
		return -1;
	}
	if (cfg->source_id_explicit != cfg->media_epoch_explicit) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] source identity fence is incomplete\n");
		return -1;
	}
	return 0;
}

static bool surface_list_contains(
	const int *widths,
	const int *heights,
	int count,
	int width,
	int height
) {
	for (int index = 0; index < count; ++index) {
		if (widths[index] == width && heights[index] == height) {
			return true;
		}
	}
	return false;
}

static int add_surface(
	int *widths,
	int *heights,
	int *count,
	int width,
	int height
) {
	if (surface_list_contains(widths, heights, *count, width, height)) {
		return 0;
	}
	if (*count >= kMaxOutputChannels) {
		return -1;
	}
	widths[*count] = width;
	heights[*count] = height;
	(*count)++;
	return 0;
}

static int write_mux_frame(FILE *out, uint8_t stream_id, const unsigned char *data, uint32_t size) {
	uint32_t be_len;
	unsigned char header[5];
	if (out == NULL || data == NULL || size == 0) {
		return -1;
	}
	header[0] = stream_id;
	be_len = htonl(size);
	memcpy(header + 1, &be_len, sizeof(be_len));
	if (fwrite(header, 1, sizeof(header), out) != sizeof(header)) {
		return -1;
	}
	if (fwrite(data, 1, size, out) != size) {
		return -1;
	}
	fflush(out);
	return ferror(out) ? -1 : 0;
}

static size_t stream_buffer_capacity(size_t frame_buffer_size) {
	size_t size = frame_buffer_size * 2;
	if (size < kStreamBufferFloor) {
		size = kStreamBufferFloor;
	}
	if (size > kStreamBufferCeiling) {
		size = kStreamBufferCeiling;
	}
	return size;
}

static void arm_stream_guard(unsigned char *buffer, size_t capacity) {
	memset(buffer + capacity, 0xA5, kStreamBufferGuard);
}

static int stream_guard_ok(const unsigned char *buffer, size_t capacity) {
	for (size_t i = 0; i < kStreamBufferGuard; ++i) {
		if (buffer[capacity + i] != 0xA5) {
			return 0;
		}
	}
	return 1;
}

static bool cadence_due(int *accumulator, int target_fps, int source_fps) {
	if (accumulator == NULL || target_fps <= 0 || source_fps <= 0) {
		return false;
	}
	if (target_fps >= source_fps) {
		return true;
	}
	*accumulator += target_fps;
	if (*accumulator < source_fps) {
		return false;
	}
	*accumulator -= source_fps;
	return true;
}

static void refresh_tone_signal(
	video_broker_nv12_tone_mapper_t *mapper,
	const unsigned char *frame,
	int width,
	int height,
	int stride
) {
	video_broker_nv12_quality_measure_t quality;
	if (mapper == NULL || !mapper->enabled || frame == NULL ||
	    (mapper->initialized && mapper->frames_until_statistics > 0)) {
		return;
	}
	quality = video_broker_nv12_quality_measure(frame, width, height, stride);
	video_broker_nv12_tone_mapper_set_signal_informative(
		mapper,
		video_broker_nv12_quality_is_informative(&quality));
}

static int apply_tone_accelerator(
	const char *role,
	video_broker_tone_accelerator_t *accelerator,
	video_broker_nv12_tone_mapper_t *mapper,
	unsigned char *frame,
	int width,
	int height
) {
	video_broker_tone_accelerator_kind_t before = accelerator->selected;
	int rc = video_broker_tone_accelerator_apply(accelerator, mapper, frame, width, height);
	if (rc >= 0 && before != accelerator->selected) {
		fprintf(stderr,
			"[rdkx5_hbn_mux_bridge] %s tone accelerator runtime fallback requested=%s selected=%s reason=%s\n",
			role,
			video_broker_tone_accelerator_name(accelerator->requested),
			video_broker_tone_accelerator_name(accelerator->selected),
			video_broker_tone_accelerator_error(accelerator));
	}
	return rc;
}

static void quality_monitor_observe(
	const bridge_config_t *cfg,
	uint64_t frame,
	const video_broker_nv12_quality_measure_t *source_quality,
	int source_width,
	int source_height,
	const unsigned char *processed,
	int processed_width,
	int processed_height,
	const unsigned char *analysis,
	int analysis_width,
	int analysis_height
) {
	video_broker_nv12_quality_measure_t processed_quality;
	video_broker_nv12_quality_measure_t analysis_quality;
	int stride;

	if (cfg == NULL || !cfg->quality_monitor_enabled || frame == 0 || source_quality == NULL ||
	    processed == NULL || analysis == NULL) {
		return;
	}
	stride = cfg->quality_monitor_sample_stride > 0 ?
		cfg->quality_monitor_sample_stride : kDefaultQualitySampleStride;
	processed_quality = video_broker_nv12_quality_measure(
		processed, processed_width, processed_height, stride);
	analysis_quality = video_broker_nv12_quality_measure(
		analysis, analysis_width, analysis_height, stride);
	if (source_quality->samples == 0 || processed_quality.samples == 0 || analysis_quality.samples == 0) {
		return;
	}
	if (video_broker_nv12_quality_write_state(
		cfg->quality_monitor_state_path,
		frame,
		stride,
		source_quality,
		source_width,
		source_height,
		&processed_quality,
		processed_width,
		processed_height,
		&analysis_quality,
		analysis_width,
		analysis_height,
		"nv12",
		video_broker_video_orientation_name(cfg->video_orientation)) != 0) {
		fprintf(stderr,
			"[rdkx5_hbn_mux_bridge][quality] state commit failed path=%s err=%s\n",
			cfg->quality_monitor_state_path == NULL ? "" : cfg->quality_monitor_state_path,
			strerror(errno));
	}
}

static void *allocate_frame_buffer(size_t size) {
	void *buffer = NULL;
	if (size == 0 || posix_memalign(&buffer, 4096, size) != 0) {
		return NULL;
	}
	return buffer;
}

static uint64_t monotonic_milliseconds(void) {
	struct timespec now;
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return 0;
	return (uint64_t)now.tv_sec * 1000U + (uint64_t)now.tv_nsec / 1000000U;
}

static void hbn_role_move(hbn_role_runtime_t *target, hbn_role_runtime_t *source) {
	*target = *source;
	memset(source, 0, sizeof(*source));
}

static int hbn_role_destroy(hbn_role_runtime_t *role) {
	if (role == NULL) return 0;
	if (video_broker_tone_accelerator_destroy(&role->tone_accelerator) != 0) return -1;
	video_broker_nv12_tone_mapper_destroy(&role->tone_mapper);
	if (role->encoder != NULL) {
		sp_stop_encode(role->encoder);
		sp_release_encoder_module(role->encoder);
	}
	free(role->stream);
	free(role->oriented);
	free(role->frame);
	memset(role, 0, sizeof(*role));
	return 0;
}

static int hbn_profile_codec(const video_broker_worker_branch_profile_t *profile) {
	if (strcmp(profile->codec, "h265") == 0) return SP_ENCODER_H265;
	if (strcmp(profile->codec, "h264") == 0) return SP_ENCODER_H264;
	return -1;
}

static bool hbn_profile_processed(
	video_broker_worker_role_t role,
	const video_broker_worker_branch_profile_t *profile) {
	return role != VIDEO_BROKER_WORKER_ROLE_ARCHIVE ||
		strcmp(profile->content, "source") != 0;
}

static int hbn_role_init(
	hbn_role_runtime_t *role,
	video_broker_worker_role_t role_id,
	const video_broker_worker_branch_profile_t *profile,
	uint64_t branch_epoch,
	const bridge_config_t *cfg) {
	video_broker_tone_mode_t tone_mode;
	int codec;
	if (role == NULL || profile == NULL || cfg == NULL || branch_epoch == 0) return -1;
	memset(role, 0, sizeof(*role));
	role->role = role_id;
	role->profile = *profile;
	role->branch_epoch = branch_epoch;
	role->frame_size = FRAME_BUFFER_SIZE(profile->width, profile->height);
	role->frame = allocate_frame_buffer(role->frame_size);
	if (role->frame == NULL) goto fail;
	if (video_broker_video_orientation_is_transform(cfg->video_orientation)) {
		role->oriented = allocate_frame_buffer(role->frame_size);
		if (role->oriented == NULL) goto fail;
	}
	tone_mode = hbn_profile_processed(role_id, profile) ?
		cfg->tone_mapping_mode : VIDEO_BROKER_TONE_OFF;
	video_broker_nv12_tone_mapper_init(
		&role->tone_mapper,
		tone_mode,
		cfg->tone_mapping_target,
		cfg->tone_mapping_hysteresis,
		cfg->tone_mapping_grid_columns,
		cfg->tone_mapping_grid_rows,
		cfg->tone_mapping_statistics_hz,
		profile->fps,
		cfg->tone_mapping_max_shadow_gain,
		cfg->tone_mapping_max_highlight_reduction,
		cfg->tone_mapping_temporal_alpha,
		cfg->tone_mapping_noise_guard);
	if (video_broker_tone_accelerator_init(
			&role->tone_accelerator,
			tone_mode == VIDEO_BROKER_TONE_OFF ?
				VIDEO_BROKER_TONE_ACCELERATOR_CPU : cfg->tone_mapping_accelerator) != 0) {
		fprintf(stderr,
			"[rdkx5_hbn_mux_bridge] %s tone accelerator unavailable requested=%s reason=%s\n",
			video_broker_worker_role_name(role_id),
			video_broker_tone_accelerator_name(cfg->tone_mapping_accelerator),
			video_broker_tone_accelerator_error(&role->tone_accelerator));
		goto fail;
	}
	if (role_id == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) return 0;
	codec = hbn_profile_codec(profile);
	if (codec < 0) goto fail;
	role->stream_capacity = stream_buffer_capacity(role->frame_size);
	role->stream = malloc(role->stream_capacity + kStreamBufferGuard);
	if (role->stream == NULL) goto fail;
	role->encoder = sp_init_encoder_module();
	if (role->encoder == NULL || sp_start_encode(
			role->encoder, 0, codec, profile->width, profile->height,
			profile->bitrate_kbps) != 0) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] %s hardware encoder warmup failed\n",
			video_broker_worker_role_name(role_id));
		goto fail;
	}
	return 0;

fail:
	hbn_role_destroy(role);
	return -1;
}

static void hbn_profile_from_config(
	video_broker_worker_branch_profile_t *profile,
	video_broker_worker_role_t role,
	const bridge_config_t *cfg) {
	memset(profile, 0, sizeof(*profile));
	if (role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) {
		memcpy(profile->pixel_format, "nv12", sizeof("nv12"));
		profile->width = cfg->sub_width;
		profile->height = cfg->sub_height;
		profile->fps = cfg->sub_fps;
		return;
	}
	if (role == VIDEO_BROKER_WORKER_ROLE_LIVE) {
		memcpy(profile->codec,
			cfg->live_codec == SP_ENCODER_H265 ? "h265" : "h264", sizeof("h265"));
		memcpy(profile->content, "processed", sizeof("processed"));
		memcpy(profile->rate_control, "cbr", sizeof("cbr"));
		profile->width = cfg->output_width;
		profile->height = cfg->output_height;
		profile->fps = cfg->main_fps;
		profile->bitrate_kbps = cfg->live_encode_bitrate_kbps;
		return;
	}
	memcpy(profile->codec,
		cfg->archive_codec == SP_ENCODER_H265 ? "h265" : "h264", sizeof("h265"));
	memcpy(profile->content,
		video_broker_archive_content_name(cfg->archive_content),
		strlen(video_broker_archive_content_name(cfg->archive_content)) + 1);
	memcpy(profile->rate_control, "cbr", sizeof("cbr"));
	profile->width = cfg->archive_width;
	profile->height = cfg->archive_height;
	profile->fps = cfg->archive_fps;
	profile->bitrate_kbps = cfg->archive_encode_bitrate_kbps;
}

static void hbn_role_observe_bootstrap(
	hbn_role_runtime_t *role,
	const unsigned char *data,
	size_t size) {
	if (role == NULL || data == NULL || role->role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) return;
	for (size_t index = 0; index + 4 < size; ++index) {
		size_t prefix = 0;
		if (data[index] == 0 && data[index + 1] == 0 && data[index + 2] == 1) {
			prefix = 3;
		} else if (index + 5 < size && data[index] == 0 && data[index + 1] == 0 &&
		           data[index + 2] == 0 && data[index + 3] == 1) {
			prefix = 4;
		}
		if (prefix == 0 || index + prefix >= size) continue;
		if (strcmp(role->profile.codec, "h265") == 0) {
			unsigned int type = (data[index + prefix] >> 1) & 0x3fU;
			role->seen_vps |= type == 32;
			role->seen_sps |= type == 33;
			role->seen_pps |= type == 34;
			role->seen_keyframe |= type >= 16 && type <= 23;
		} else {
			unsigned int type = data[index + prefix] & 0x1fU;
			role->seen_sps |= type == 7;
			role->seen_pps |= type == 8;
			role->seen_keyframe |= type == 5;
		}
	}
}

static bool hbn_role_evidence_ready(const hbn_role_runtime_t *role) {
	if (role == NULL || role->frames == 0) return false;
	if (role->role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) return true;
	if (strcmp(role->profile.codec, "h265") == 0) {
		return role->seen_vps && role->seen_sps && role->seen_pps && role->seen_keyframe;
	}
	return role->seen_sps && role->seen_pps && role->seen_keyframe;
}

static int hbn_role_process(
	hbn_role_runtime_t *role,
	void *vio,
	FILE *output,
	const bridge_config_t *cfg,
	bool publish,
	bool quality_source,
	hbn_quality_context_t *quality) {
	unsigned char *processed;
	video_broker_nv12_quality_measure_t source_quality;
	bool source_quality_ready = false;
	int frame_rc;
	int stream_size;
	uint8_t stream_id;
	if (role == NULL || vio == NULL || cfg == NULL) return -1;
	frame_rc = sp_vio_get_frame(
		vio, (char *)role->frame,
		role->profile.width, role->profile.height, cfg->timeout_ms);
	if (frame_rc != 0) return 1;
	if (!cadence_due(&role->emit_accumulator, role->profile.fps, cfg->fps)) return 1;
	if (!role->logged_first_frame) {
		fprintf(stderr,
			"[rdkx5_hbn_mux_bridge] first %s frame ready bytes=%zu branch_epoch=%llu\n",
			video_broker_worker_role_name(role->role), role->frame_size,
			(unsigned long long)role->branch_epoch);
		role->logged_first_frame = true;
	}
	processed = role->frame;
	uint64_t sample_now_us = monotonic_milliseconds() * 1000U;
	if (quality_source && quality != NULL && quality->analysis_ready && cfg->quality_monitor_enabled &&
	    video_broker_frame_observation_due(role->frames + 1U, cfg->quality_monitor_report_every,
		cfg->fps, sample_now_us, role->last_quality_sample_us)) {
		source_quality = video_broker_nv12_quality_measure(
			role->frame, role->profile.width, role->profile.height,
			cfg->quality_monitor_sample_stride);
		source_quality_ready = source_quality.samples > 0;
		if (source_quality_ready) role->last_quality_sample_us = sample_now_us;
	}
	if (role->oriented != NULL) {
		video_broker_nv12_resize_nearest_oriented(
			role->frame, role->profile.width, role->profile.height,
			role->oriented, role->profile.width, role->profile.height,
			cfg->video_orientation);
		processed = role->oriented;
	}
	refresh_tone_signal(
		&role->tone_mapper, processed,
		role->profile.width, role->profile.height, kDefaultQualitySampleStride);
	(void)video_broker_nv12_tone_mapper_update(
		&role->tone_mapper, processed,
		role->profile.width, role->profile.height, 8);
	if (apply_tone_accelerator(
			video_broker_worker_role_name(role->role),
			&role->tone_accelerator, &role->tone_mapper, processed,
			role->profile.width, role->profile.height) < 0) {
		return -1;
	}
	if (role->role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) {
		role->frames++;
		if (publish && write_mux_frame(
				output, kSubStreamID, processed, (uint32_t)role->frame_size) != 0) {
			return -1;
		}
		if (publish && quality != NULL) {
			quality->analysis = processed;
			quality->analysis_width = role->profile.width;
			quality->analysis_height = role->profile.height;
			quality->analysis_ready = true;
		}
		return 0;
	}
	if (sp_encoder_set_frame(role->encoder, (char *)processed, (int32_t)role->frame_size) != 0) {
		return -1;
	}
	arm_stream_guard(role->stream, role->stream_capacity);
	stream_size = sp_encoder_get_stream(role->encoder, (char *)role->stream);
	if (!stream_guard_ok(role->stream, role->stream_capacity) || stream_size < 0 ||
	    (size_t)stream_size > role->stream_capacity) {
		return -1;
	}
	if (stream_size == 0) return 1;
	stream_size = video_broker_h26x_bootstrap_copy(&role->bootstrap,
		strcmp(role->profile.codec, "h265") == 0,
		role->stream, (size_t)stream_size, role->stream, role->stream_capacity);
	if (stream_size < 0) return -1;
	hbn_role_observe_bootstrap(role, role->stream, (size_t)stream_size);
	role->frames++;
	if (!role->logged_first_stream) {
		fprintf(stderr,
			"[rdkx5_hbn_mux_bridge] first %s stream size=%d codec=%s branch_epoch=%llu\n",
			video_broker_worker_role_name(role->role), stream_size, role->profile.codec,
			(unsigned long long)role->branch_epoch);
		role->logged_first_stream = true;
	}
	if (source_quality_ready) {
		quality_monitor_observe(
			cfg, role->frames, &source_quality,
			role->profile.width, role->profile.height,
			processed, role->profile.width, role->profile.height,
			quality->analysis, quality->analysis_width, quality->analysis_height);
	}
	if (publish && role->role == VIDEO_BROKER_WORKER_ROLE_LIVE &&
	    cfg->tone_mapping_mode != VIDEO_BROKER_TONE_OFF &&
	    (role->frames == 1 || role->frames % (uint64_t)(role->profile.fps * 10) == 0)) {
		fprintf(stderr,
			"[rdkx5_hbn_mux_bridge][tone-mapper] event=tone_mapping_summary requested_mode=%s active_mode=%s accelerator_requested=%s accelerator_selected=%s target=%d gain_min=%.3f gain_max=%.3f statistics_hz=%d effective_statistics_hz=%d statistics_updates=%llu active=%s\n",
			video_broker_tone_mode_name(role->tone_mapper.requested_mode),
			video_broker_tone_mode_name(role->tone_mapper.active_mode),
			video_broker_tone_accelerator_name(role->tone_accelerator.requested),
			video_broker_tone_accelerator_name(role->tone_accelerator.selected),
			role->tone_mapper.target_luma, role->tone_mapper.minimum_gain,
			role->tone_mapper.maximum_gain, role->tone_mapper.statistics_hz,
			role->tone_mapper.effective_statistics_hz,
			(unsigned long long)role->tone_mapper.statistics_updates,
			video_broker_nv12_tone_mapper_active(&role->tone_mapper) ? "true" : "false");
	}
	stream_id = role->role == VIDEO_BROKER_WORKER_ROLE_LIVE ? kMainStreamID :
		(strcmp(role->profile.codec, "h265") == 0 ? kArchiveH265StreamID : kArchiveH264StreamID);
	if (publish && write_mux_frame(output, stream_id, role->stream, (uint32_t)stream_size) != 0) {
		return -1;
	}
	return 0;
}

static void hbn_fanout_reset(hbn_fanout_backend_t *backend) {
	backend->change_count = 0;
	backend->cutover_completed = false;
	memset(backend->changes, 0, sizeof(backend->changes));
}

static int hbn_process_active_roles(hbn_fanout_backend_t *backend, int *produced) {
	bool live_is_quality_source;
	if (backend == NULL || backend->slots == NULL || backend->quality == NULL) return -1;
	if (produced != NULL) *produced = 0;
	live_is_quality_source = backend->slots[VIDEO_BROKER_WORKER_ROLE_LIVE].active_present &&
		backend->slots[VIDEO_BROKER_WORKER_ROLE_LIVE].active.profile.width == backend->cfg->sensor_width &&
		backend->slots[VIDEO_BROKER_WORKER_ROLE_LIVE].active.profile.height == backend->cfg->sensor_height;
	for (int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	     role <= VIDEO_BROKER_WORKER_ROLE_LIVE; ++role) {
		hbn_role_slot_t *slot = &backend->slots[role];
		bool quality_source = backend->cfg->quality_monitor_enabled &&
			backend->slots[VIDEO_BROKER_WORKER_ROLE_ANALYSIS].active_present &&
			((role == VIDEO_BROKER_WORKER_ROLE_LIVE && live_is_quality_source) ||
			 (role == VIDEO_BROKER_WORKER_ROLE_ARCHIVE && !live_is_quality_source &&
			  slot->active_present && slot->active.profile.width == backend->cfg->sensor_width &&
			  slot->active.profile.height == backend->cfg->sensor_height));
		int result;
		if (!slot->active_present) continue;
		result = hbn_role_process(
			&slot->active, backend->vio, backend->output, backend->cfg,
			true, quality_source, backend->quality);
		if (result < 0) return -1;
		if (result == 0 && produced != NULL) (*produced)++;
	}
	return 0;
}

static int hbn_fanout_prepare(
	void *opaque,
	const video_broker_worker_branch_change_t *changes,
	size_t change_count) {
	hbn_fanout_backend_t *backend = opaque;
	bool resulting[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	video_broker_worker_branch_profile_t profiles[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	if (backend == NULL || changes == NULL || change_count == 0 ||
	    change_count > VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES || backend->change_count != 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	     role <= VIDEO_BROKER_WORKER_ROLE_LIVE; ++role) {
		resulting[role] = backend->slots[role].active_present;
		if (resulting[role]) profiles[role] = backend->slots[role].active.profile;
		else memset(&profiles[role], 0, sizeof(profiles[role]));
		if (backend->slots[role].candidate_present || backend->slots[role].incumbent_present) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
	}
	for (size_t index = 0; index < change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &changes[index];
		hbn_role_slot_t *slot = &backend->slots[change->role];
		if ((change->action == VIDEO_BROKER_WORKER_ACTION_ATTACH && slot->active_present) ||
		    ((change->action == VIDEO_BROKER_WORKER_ACTION_DETACH ||
		      change->action == VIDEO_BROKER_WORKER_ACTION_REPLACE) && !slot->active_present)) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
		if (change->action == VIDEO_BROKER_WORKER_ACTION_DETACH) {
			resulting[change->role] = false;
			continue;
		}
		if (change->profile.fps > backend->cfg->fps ||
		    !surface_list_contains(
			backend->widths, backend->heights, backend->channel_count,
			change->profile.width, change->profile.height) ||
		    (change->role != VIDEO_BROKER_WORKER_ROLE_ANALYSIS &&
		     strcmp(change->profile.rate_control, "cbr") != 0)) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
		resulting[change->role] = true;
		profiles[change->role] = change->profile;
	}
	if (backend->cfg->quality_monitor_enabled && resulting[VIDEO_BROKER_WORKER_ROLE_ANALYSIS] &&
	    !((resulting[VIDEO_BROKER_WORKER_ROLE_LIVE] &&
	        profiles[VIDEO_BROKER_WORKER_ROLE_LIVE].width == backend->cfg->sensor_width &&
	        profiles[VIDEO_BROKER_WORKER_ROLE_LIVE].height == backend->cfg->sensor_height) ||
	       (resulting[VIDEO_BROKER_WORKER_ROLE_ARCHIVE] &&
	        profiles[VIDEO_BROKER_WORKER_ROLE_ARCHIVE].width == backend->cfg->sensor_width &&
	        profiles[VIDEO_BROKER_WORKER_ROLE_ARCHIVE].height == backend->cfg->sensor_height))) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	memcpy(backend->changes, changes, change_count * sizeof(changes[0]));
	backend->change_count = change_count;
	backend->cutover_completed = false;
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int hbn_fanout_warmup(void *opaque) {
	hbn_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		hbn_role_slot_t *slot = &backend->slots[change->role];
		uint64_t epoch;
		if (change->action == VIDEO_BROKER_WORKER_ACTION_DETACH || slot->candidate_present) continue;
		epoch = slot->next_epoch + 1;
		if (epoch == 0 || hbn_role_init(
				&slot->candidate, change->role, &change->profile, epoch, backend->cfg) != 0) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
		slot->candidate_present = true;
	}
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int hbn_fanout_admit(void *opaque) {
	hbn_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		if (change->action != VIDEO_BROKER_WORKER_ACTION_DETACH &&
		    !backend->slots[change->role].candidate_present) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
	}
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static bool hbn_fanout_candidates_ready(const hbn_fanout_backend_t *backend) {
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		if (change->action != VIDEO_BROKER_WORKER_ACTION_DETACH &&
		    (!backend->slots[change->role].candidate_present ||
		     !hbn_role_evidence_ready(&backend->slots[change->role].candidate))) {
			return false;
		}
	}
	return true;
}

static int hbn_fanout_first_frame(
	void *opaque,
	video_broker_worker_branch_receipt_t *branches,
	size_t *branch_count) {
	hbn_fanout_backend_t *backend = opaque;
	uint64_t started = monotonic_milliseconds();
	size_t output_count = 0;
	if (backend == NULL || branches == NULL || branch_count == NULL ||
	    *branch_count < backend->change_count || started == 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	while (!hbn_fanout_candidates_ready(backend)) {
		if (hbn_process_active_roles(backend, NULL) != 0) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
		for (size_t index = 0; index < backend->change_count; ++index) {
			const video_broker_worker_branch_change_t *change = &backend->changes[index];
			hbn_role_slot_t *slot = &backend->slots[change->role];
			if (change->action == VIDEO_BROKER_WORKER_ACTION_DETACH ||
			    hbn_role_evidence_ready(&slot->candidate)) continue;
			if (hbn_role_process(
					&slot->candidate, backend->vio, backend->output, backend->cfg,
					false, false, NULL) < 0) {
				return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
			}
		}
		if (monotonic_milliseconds() - started >= kFanoutFrameTimeoutMs) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_TIMEOUT;
		}
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		const hbn_role_runtime_t *candidate;
		if (change->action == VIDEO_BROKER_WORKER_ACTION_DETACH) continue;
		candidate = &backend->slots[change->role].candidate;
		branches[output_count++] = (video_broker_worker_branch_receipt_t){
			.role = change->role,
			.branch_epoch = candidate->branch_epoch,
			.frame_id = candidate->frames,
			.codec_bootstrap_complete = change->role != VIDEO_BROKER_WORKER_ROLE_ANALYSIS,
		};
		video_broker_worker_branch_receipt_t *branch = &branches[output_count - 1];
		if (change->role == VIDEO_BROKER_WORKER_ROLE_LIVE) {
			memcpy(branch->runtime_mode, "backend_static", sizeof("backend_static"));
		} else if (change->role == VIDEO_BROKER_WORKER_ROLE_ARCHIVE) {
			const char *mode = strcmp(candidate->profile.content, "source") == 0 ?
				"source_hardware_encode" : "processed_hardware_encode";
			memcpy(branch->runtime_mode, mode, strlen(mode) + 1);
		}
	}
	*branch_count = output_count;
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int hbn_fanout_cutover(void *opaque) {
	hbn_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0 || backend->cutover_completed) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		hbn_role_slot_t *slot = &backend->slots[change->role];
		if (slot->active_present) {
			hbn_role_move(&slot->incumbent, &slot->active);
			slot->incumbent_present = true;
			slot->active_present = false;
		}
		if (change->action != VIDEO_BROKER_WORKER_ACTION_DETACH) {
			hbn_role_move(&slot->active, &slot->candidate);
			slot->active_present = true;
			slot->candidate_present = false;
			slot->cutover_frame_floor = slot->active.frames;
		}
	}
	backend->cutover_completed = true;
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static bool hbn_fanout_verified(const hbn_fanout_backend_t *backend) {
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		const hbn_role_slot_t *slot = &backend->slots[change->role];
		if (change->action != VIDEO_BROKER_WORKER_ACTION_DETACH &&
		    (!slot->active_present || slot->active.frames <= slot->cutover_frame_floor)) {
			return false;
		}
	}
	return true;
}

static int hbn_fanout_verify(void *opaque) {
	hbn_fanout_backend_t *backend = opaque;
	uint64_t started = monotonic_milliseconds();
	if (backend == NULL || !backend->cutover_completed || started == 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	while (!hbn_fanout_verified(backend)) {
		if (hbn_process_active_roles(backend, NULL) != 0) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
		if (monotonic_milliseconds() - started >= kFanoutFrameTimeoutMs) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_TIMEOUT;
		}
	}
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int hbn_fanout_rollback(void *opaque) {
	hbn_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		hbn_role_slot_t *slot = &backend->slots[change->role];
		if (!backend->cutover_completed) {
			if (slot->candidate_present && hbn_role_destroy(&slot->candidate) != 0)
				return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
			slot->candidate_present = false;
			continue;
		}
		if (slot->active_present && hbn_role_destroy(&slot->active) != 0)
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		slot->active_present = false;
		if (slot->incumbent_present) {
			hbn_role_move(&slot->active, &slot->incumbent);
			slot->active_present = true;
			slot->incumbent_present = false;
		}
	}
	hbn_fanout_reset(backend);
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int hbn_fanout_abort(void *opaque) {
	hbn_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0 || backend->cutover_completed) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		hbn_role_slot_t *slot = &backend->slots[backend->changes[index].role];
		if (slot->candidate_present && hbn_role_destroy(&slot->candidate) != 0)
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		slot->candidate_present = false;
	}
	hbn_fanout_reset(backend);
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int hbn_fanout_retire(void *opaque) {
	hbn_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0 || !backend->cutover_completed) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		hbn_role_slot_t *slot = &backend->slots[change->role];
		if (slot->incumbent_present && hbn_role_destroy(&slot->incumbent) != 0)
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		slot->incumbent_present = false;
		if (change->action != VIDEO_BROKER_WORKER_ACTION_DETACH) {
			slot->next_epoch = slot->active.branch_epoch;
		}
	}
	hbn_fanout_reset(backend);
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static video_broker_worker_fanout_callbacks_t hbn_fanout_callbacks(void) {
	return (video_broker_worker_fanout_callbacks_t){
		.prepare = hbn_fanout_prepare,
		.warmup = hbn_fanout_warmup,
		.admit = hbn_fanout_admit,
		.first_frame = hbn_fanout_first_frame,
		.cutover = hbn_fanout_cutover,
		.verify = hbn_fanout_verify,
		.rollback = hbn_fanout_rollback,
		.abort = hbn_fanout_abort,
		.retire = hbn_fanout_retire,
	};
}

static int hbn_fanout_input_init(
	hbn_fanout_input_t *input,
	video_broker_worker_fanout_controller_t *controller) {
	int flags;
	if (input == NULL || controller == NULL) return -1;
	memset(input, 0, sizeof(*input));
	input->controller = controller;
	flags = fcntl(STDIN_FILENO, F_GETFL, 0);
	if (flags < 0 || fcntl(STDIN_FILENO, F_SETFL, flags | O_NONBLOCK) != 0) return -1;
	return 0;
}

static int hbn_fanout_input_poll(hbn_fanout_input_t *input) {
	char incoming[1024];
	ssize_t count;
	if (input == NULL || input->controller == NULL) return -1;
	for (;;) {
		count = read(STDIN_FILENO, incoming, sizeof(incoming));
		if (count < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
			if (errno == EINTR) continue;
			return -1;
		}
		if (count == 0) return -1;
		for (ssize_t index = 0; index < count; ++index) {
			video_broker_worker_fanout_command_t command;
			video_broker_worker_fanout_receipt_t receipt;
			char value = incoming[index];
			if (input->used >= sizeof(input->buffer) || value == '\r') return -1;
			input->buffer[input->used++] = value;
			if (value != '\n') continue;
			if (video_broker_worker_fanout_decode_command(
					input->buffer, input->used, &command) != 0) {
				return -1;
			}
			video_broker_worker_fanout_controller_handle(
				input->controller, &command, &receipt);
			if (video_broker_tone_accelerator_resources_quarantined()) return -1;
			if (video_broker_worker_fanout_write_receipt(stderr, &receipt) != 0) return -1;
			input->used = 0;
		}
	}
}

int main(int argc, char **argv) {
	bridge_config_t cfg;
	sp_sensors_parameters sensor = {0};
	int widths[kMaxOutputChannels];
	int heights[kMaxOutputChannels];
	int channel_count = 0;
	int binary_fd = -1;
	void *vio = NULL;
	hbn_role_slot_t slots[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	hbn_fanout_backend_t fanout_backend;
	video_broker_worker_fanout_controller_t fanout_controller;
	hbn_fanout_input_t fanout_input;
	hbn_quality_context_t quality;
	int rc = 1;
	bool archive_processed;
	bool fanout_enabled;
	FILE *out = NULL;

	default_config(&cfg);
	if (parse_args(argc, argv, &cfg) != 0) {
		return 2;
	}
	memset(slots, 0, sizeof(slots));
	memset(&fanout_backend, 0, sizeof(fanout_backend));
	memset(&fanout_controller, 0, sizeof(fanout_controller));
	memset(&fanout_input, 0, sizeof(fanout_input));
	memset(&quality, 0, sizeof(quality));
	fanout_enabled = cfg.source_id_explicit && cfg.media_epoch_explicit;
	if (cfg.sub_fps <= 0 || cfg.sub_fps > cfg.fps) {
		cfg.sub_fps = cfg.fps;
	}
	if (cfg.main_fps <= 0 || cfg.main_fps > cfg.fps) {
		cfg.main_fps = cfg.fps;
	}
	if (cfg.archive_fps <= 0 || cfg.archive_fps > cfg.fps) {
		cfg.archive_fps = cfg.fps;
	}
	if (cfg.quality_monitor_enabled &&
	    (cfg.quality_monitor_state_path == NULL || cfg.quality_monitor_state_path[0] == '\0')) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] configured quality monitor requires a state path\n");
		return 2;
	}
	if (cfg.archive_enabled && ((cfg.archive_width & 1) != 0 || (cfg.archive_height & 1) != 0)) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] archive geometry must be even\n");
		return 2;
	}
	if (cfg.archive_enabled && cfg.archive_content == VIDEO_BROKER_ARCHIVE_SOURCE &&
	    cfg.tone_mapping_mode != VIDEO_BROKER_TONE_OFF) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] archive content source requires tone mapping off\n");
		return 2;
	}
	archive_processed = cfg.archive_enabled && cfg.archive_content != VIDEO_BROKER_ARCHIVE_SOURCE;
	if (cfg.quality_monitor_enabled && cfg.analysis_enabled &&
	    (!cfg.live_enabled || cfg.output_width != cfg.sensor_width || cfg.output_height != cfg.sensor_height) &&
	    (!cfg.archive_enabled || cfg.archive_width != cfg.sensor_width || cfg.archive_height != cfg.sensor_height)) {
		fprintf(stderr,
			"[rdkx5_hbn_mux_bridge] quality monitor requires a capture-sized live or archive channel\n");
		return 2;
	}
	if (video_broker_worker_install_stop_signals(handle_signal) != 0) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] stop signal setup failed err=%s\n", strerror(errno));
		return 2;
	}
	setvbuf(stderr, NULL, _IOLBF, 0);
	binary_fd = dup(STDOUT_FILENO);
	if (binary_fd < 0) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] dup stdout failed\n");
		goto cleanup;
	}
	if (dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] redirect stdout failed\n");
		goto cleanup;
	}
	out = fdopen(binary_fd, "wb");
	if (out == NULL) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] fdopen failed\n");
		goto cleanup;
	}

	if (cfg.admitted_surface_count > 0) {
		for (int index = 0; index < cfg.admitted_surface_count; ++index) {
			if (add_surface(
					widths, heights, &channel_count,
					cfg.admitted_widths[index], cfg.admitted_heights[index]) != 0) {
				fprintf(stderr, "[rdkx5_hbn_mux_bridge] routed surface catalog exceeds hardware channels\n");
				goto cleanup;
			}
		}
		if ((cfg.analysis_enabled && !surface_list_contains(
				widths, heights, channel_count, cfg.sub_width, cfg.sub_height)) ||
		    (cfg.live_enabled && !surface_list_contains(
				widths, heights, channel_count, cfg.output_width, cfg.output_height)) ||
		    (cfg.archive_enabled && !surface_list_contains(
				widths, heights, channel_count, cfg.archive_width, cfg.archive_height))) {
			fprintf(stderr, "[rdkx5_hbn_mux_bridge] active output is outside routed surface catalog\n");
			goto cleanup;
		}
	} else {
		if ((cfg.analysis_enabled && add_surface(
				widths, heights, &channel_count, cfg.sub_width, cfg.sub_height) != 0) ||
		    (cfg.live_enabled && add_surface(
				widths, heights, &channel_count, cfg.output_width, cfg.output_height) != 0) ||
		    (cfg.archive_enabled && add_surface(
				widths, heights, &channel_count, cfg.archive_width, cfg.archive_height) != 0)) {
			fprintf(stderr, "[rdkx5_hbn_mux_bridge] active output surfaces exceed hardware channels\n");
			goto cleanup;
		}
	}
	sensor.raw_width = cfg.sensor_width;
	sensor.raw_height = cfg.sensor_height;
	sensor.fps = cfg.fps;

	fprintf(stderr,
		"[rdkx5_hbn_mux_bridge] open camera prefix=%s video_index=%d roles=%s/%s/%s sensor=%dx%d output=%dx%d sub=%dx%d archive=%dx%d fps=%d sub_fps=%d main_fps=%d archive_fps=%d live_codec=%s live_bitrate_kbps=%d archive_bitrate_kbps=%d archive_codec=%s archive_content=%s archive_encoder_shared=false orientation=%s tone_mapping=%s tone_accelerator=%s grid=%dx%d statistics_hz=%d\n",
		cfg.sensor_prefix, cfg.video_index,
		cfg.live_enabled ? "live" : "-", cfg.analysis_enabled ? "analysis" : "-", cfg.archive_enabled ? "archive" : "-",
		cfg.sensor_width, cfg.sensor_height,
		cfg.output_width, cfg.output_height, cfg.sub_width, cfg.sub_height,
		cfg.archive_width, cfg.archive_height, cfg.fps, cfg.sub_fps, cfg.main_fps, cfg.archive_fps,
		cfg.live_codec == SP_ENCODER_H265 ? "h265" : "h264",
		cfg.live_encode_bitrate_kbps, cfg.archive_encode_bitrate_kbps,
		cfg.archive_codec == SP_ENCODER_H265 ? "h265" : "h264",
		video_broker_archive_content_name(cfg.archive_content),
		video_broker_video_orientation_name(cfg.video_orientation),
		video_broker_tone_mode_name(cfg.tone_mapping_mode),
		video_broker_tone_accelerator_name(cfg.tone_mapping_accelerator),
		cfg.tone_mapping_grid_columns, cfg.tone_mapping_grid_rows, cfg.tone_mapping_statistics_hz);

	vio = sp_init_vio_module();
	if (vio == NULL) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] sp_init_vio_module failed\n");
		goto cleanup;
	}
	if (sp_open_camera_v2(vio, kDefaultPipeID, cfg.video_index, channel_count, &sensor, widths, heights) != 0) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] sp_open_camera_v2 failed\n");
		goto cleanup;
	}
	if (cfg.warmup_sec > 0) {
		sleep((unsigned int)cfg.warmup_sec);
	}
	for (int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	     role <= VIDEO_BROKER_WORKER_ROLE_LIVE; ++role) {
		bool enabled = role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS ? cfg.analysis_enabled :
			(role == VIDEO_BROKER_WORKER_ROLE_ARCHIVE ? cfg.archive_enabled : cfg.live_enabled);
		video_broker_worker_branch_profile_t profile;
		if (!enabled) continue;
		hbn_profile_from_config(&profile, (video_broker_worker_role_t)role, &cfg);
		if (hbn_role_init(
				&slots[role].active, (video_broker_worker_role_t)role,
				&profile, 1, &cfg) != 0) {
			fprintf(stderr, "[rdkx5_hbn_mux_bridge] initial %s branch failed\n",
				video_broker_worker_role_name((video_broker_worker_role_t)role));
			goto cleanup;
		}
		slots[role].active_present = true;
		slots[role].next_epoch = 1;
	}
	for (int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	     role <= VIDEO_BROKER_WORKER_ROLE_LIVE; ++role) {
		bool enabled = role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS ? cfg.analysis_enabled :
			(role == VIDEO_BROKER_WORKER_ROLE_ARCHIVE ? cfg.archive_enabled : cfg.live_enabled);
		if (!enabled) continue;
		fprintf(stderr,
			"[rdkx5_hbn_mux_bridge][stage-capability] stage=resize role=%s format=nv12 requested_backend=native_sp_resize selected_backend=native_sp_resize element=sp_vio_get_frame fallback=none\n",
			video_broker_worker_role_name((video_broker_worker_role_t)role));
	}
	if (fanout_enabled && cfg.admitted_surface_count == 0) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge] routed fan-out requires an admitted surface catalog\n");
		goto cleanup;
	}
	/* Keep one backend view for both the steady capture loop and transactional
	 * candidate work. This guarantees that first-frame/verify advance the same
	 * incumbent roles, quality state and output as the normal loop. */
	fanout_backend = (hbn_fanout_backend_t){
		.cfg = &cfg,
		.vio = vio,
		.output = out,
		.widths = widths,
		.heights = heights,
		.channel_count = channel_count,
		.slots = slots,
		.quality = &quality,
	};
	if (fanout_enabled) {
		if (video_broker_worker_fanout_controller_init(
				&fanout_controller, cfg.source_id, (int)getpid(), cfg.media_epoch,
				hbn_fanout_callbacks(), &fanout_backend) != 0 ||
		    hbn_fanout_input_init(&fanout_input, &fanout_controller) != 0) {
			fprintf(stderr, "[rdkx5_hbn_mux_bridge] fan-out control initialization failed\n");
			goto cleanup;
		}
	}
	fprintf(stderr,
		"[rdkx5_hbn_mux_bridge][capability] live_enabled=%s analysis_enabled=%s archive_enabled=%s capture_codec=nv12 capture=%dx%d capture_fps=%d live_codec=%s archive_codec=%s archive_delivery=%s archive_fps=%d live_control_mode=%s fanout_control_mode=%s\n",
		cfg.live_enabled ? "true" : "false",
		cfg.analysis_enabled ? "true" : "false",
		cfg.archive_enabled ? "true" : "false",
		cfg.sensor_width, cfg.sensor_height, cfg.fps,
		cfg.live_enabled ? (cfg.live_codec == SP_ENCODER_H265 ? "h265" : "h264") : "none",
		cfg.archive_enabled ? (cfg.archive_codec == SP_ENCODER_H265 ? "h265" : "h264") : "none",
		cfg.archive_enabled ? (archive_processed ? "processed_hardware_encode" : "source_hardware_encode") : "disabled",
		cfg.archive_enabled ? cfg.archive_fps : 0,
		cfg.live_enabled ? "backend_static" : "disabled",
		fanout_enabled ? "branch_transaction_v1" : "disabled");
	fprintf(stderr,
		"[rdkx5_hbn_mux_bridge][surface-catalog] admitted_surfaces=%d hardware_limit=%d\n",
		channel_count, kMaxOutputChannels);


	while (g_running) {
		bool any_active = false;
		int produced = 0;
		if (fanout_enabled && hbn_fanout_input_poll(&fanout_input) != 0) {
			fprintf(stderr, "[rdkx5_hbn_mux_bridge] fan-out control channel closed or invalid\n");
			goto cleanup;
		}
		if (hbn_process_active_roles(&fanout_backend, &produced) != 0) {
			fprintf(stderr, "[rdkx5_hbn_mux_bridge] active branch processing failed\n");
			goto cleanup;
		}
		for (int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
		     role <= VIDEO_BROKER_WORKER_ROLE_LIVE; ++role) {
			any_active |= slots[role].active_present;
		}
		if (produced > 0) rc = 0;
		if (!any_active) {
			struct timespec idle = {.tv_sec = 0, .tv_nsec = 10000000L};
			(void)nanosleep(&idle, NULL);
		}
	}

cleanup:
	if (g_stop_signal != 0) {
		fprintf(stderr,
			"[rdkx5_hbn_mux_bridge][lifecycle] event=cooperative_shutdown_started signal=%d action=release_pipeline\n",
			(int)g_stop_signal);
	}
	for (int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	     role <= VIDEO_BROKER_WORKER_ROLE_LIVE; ++role) {
		if (slots[role].candidate_present) hbn_role_destroy(&slots[role].candidate);
		if (slots[role].incumbent_present) hbn_role_destroy(&slots[role].incumbent);
		if (slots[role].active_present) hbn_role_destroy(&slots[role].active);
	}
	if (video_broker_tone_accelerator_resources_quarantined()) {
		fprintf(stderr, "[rdkx5_hbn_mux_bridge][worker-release] owner=tone"
			" sdk_release=unconfirmed action=retain_buffers_and_exit\n");
		_Exit(VIDEO_BROKER_WORKER_RELEASE_UNCONFIRMED);
	}
	if (vio != NULL) {
		sp_vio_close(vio);
		sp_release_vio_module(vio);
	}
	if (out != NULL) {
		fclose(out);
	} else if (binary_fd >= 0) {
		close(binary_fd);
	}
	return rc;
}
