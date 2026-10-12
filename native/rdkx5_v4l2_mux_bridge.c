/* V4L2 media graph worker owned by the standalone video-broker data plane. */
#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/videodev2.h>
#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "sp_codec.h"
#include "sp_vio.h"
#include "archive_policy.h"
#include "mjpeg_decode_policy.h"
#include "jpeg_frame_header.h"
#include "stable_cadence.h"
#include "frame_observation.h"
#include "nv12_resize.h"
#include "nv12_orientation.h"
#include "nv12_tone_mapper.h"
#include "nv12_tone_mapper_accelerator.h"
#include "nv12_quality_measure.h"
#include "nv12_quality_state.h"
#include "rdkx5_decoder_runtime.h"
#include "rdkx5_persistent_encoder.h"
#include "turbojpeg.h"
#include "worker_fanout_protocol.h"
#include "stream_epoch.h"
#include "frame_source_linux.h"
#include "worker_release.h"
#include "worker_signal.h"
#include "diagnostic_pipe.h"
#include "pipeline_latency.h"
#include "hevc_sequence_limits.h"

#ifndef V4L2_PIX_FMT_HEVC
#define V4L2_PIX_FMT_HEVC v4l2_fourcc('H', 'E', 'V', 'C')
#endif

/* A physical interframe discontinuity after admission is a camera-side hiccup,
 * not by itself proof of a missing codec reference: rebuild the reference gate
 * (drop the affected frames until a fresh IDR) and keep the capture process
 * alive, bounded. A jump larger than VB_DISCONTINUITY_MAX_DELTA, more than
 * VB_DISCONTINUITY_MAX_TOLERATED tolerated events in one capture process, or a
 * resync that does not reach an IDR within VB_DISCONTINUITY_RESYNC_BUDGET_US
 * still retires the source, so a persistently corrupt stream fails closed. */
#define VB_DISCONTINUITY_MAX_DELTA 64u
#define VB_DISCONTINUITY_MAX_TOLERATED 32u
#define VB_DISCONTINUITY_RESYNC_BUDGET_US 2000000ull

/* Deadline for delivering one analysis frame to the broker. The broker admits
 * the same class of stall at three seconds (frameProductWorkBudgetNS and the
 * receive/publication ages in cmd/video-broker/frame_product_linux.go); a
 * shorter deadline here retired a healthy source on a multi-second storage or
 * scheduling hiccup, which cost the capture epoch and the tracker's binding.
 * The send is abandoned and the source retired when the deadline expires, so a
 * truly stuck peer still fails closed. */
#define VB_ANALYSIS_SEND_BUDGET_MS 3000

static volatile sig_atomic_t g_running = 1;
static volatile sig_atomic_t g_stop_signal = 0;
static vb_diagnostic_pipe g_telemetry = {.fd = -1};

enum {
	kDefaultCaptureWidth = 1280,
	kDefaultCaptureHeight = 720,
	kDefaultOutputWidth = 1280,
	kDefaultOutputHeight = 720,
	kDefaultSubWidth = 640,
	kDefaultSubHeight = 360,
	kDefaultFPS = 30,
	kDefaultWarmupSec = 1,
	kDefaultLiveBitrateKbps = 4096,
	kDefaultArchiveBitrateKbps = 8192,
	kDefaultBufferCount = 4,
	kDefaultCaptureSelectTimeoutMs = 2000,
	kDefaultCaptureMaxConsecutiveTimeouts = 3,
	kMaxConsecutiveCorruptFrames = 8,
	kDecoderOutputPollTimeoutMs = 2,
	kMJPEGTrialFrameTimeoutMs = 250,
	kMainStreamID = 1,
	kSubStreamID = 2,
	kArchiveH265StreamID = 3,
	kArchiveH264StreamID = 4,
	kGeneratedArchiveH264StreamID = 6,
	kLiveEncoderEpochStreamID = 7,
	kStreamBufferFloor = 1 << 20,
	kStreamBufferCeiling = 64 << 20,
	kStreamBufferGuard = 4096,
	kDefaultQualityReportEvery = 900,
	kDefaultQualitySampleStride = 8,
	/* State transitions and faults are emitted immediately. Periodic optical
	 * and quality snapshots are deliberately slower than the data-plane
	 * summary so journald retains signal during long USB incidents. */
	kDiagnosticLogIntervalUs = 120000000,
	kRuntimeMetricsReportEveryUs = 60000000,
	kCameraAdmissionTimeoutUs = 10000000,
	kMJPEGDecoderOutputStallTimeoutUs = 2000000,
	kUVCStartReprepareQuietMs = 1000,
	kControlLineMax = VIDEO_BROKER_WORKER_FANOUT_MAX_LINE_BYTES + 1,
	kRetryComponentMax = 64,
	kRetryActionMax = 16,
	kFanoutFrameTimeoutMs = 8000,
};

typedef struct {
	void *start;
	size_t length;
} mmap_buffer_t;

typedef struct {
	const char *device;
	const char *input_format_name;
	uint32_t input_format;
	int capture_width;
	int capture_height;
	int archive_width;
	int archive_height;
	int output_width;
	int output_height;
	int sub_width;
	int sub_height;
	int sub_fps;
	int main_fps;
	int archive_fps;
	int live_codec;
	int fps;
	int warmup_sec;
	int live_encode_bitrate_kbps;
	int archive_encode_bitrate_kbps;
	bool live_enabled;
	bool analysis_enabled;
	bool archive_enabled;
	bool live_enabled_explicit;
	bool analysis_enabled_explicit;
	bool archive_enabled_explicit;
	bool live_codec_explicit;
	bool main_active;
	video_broker_mjpeg_accelerator_t mjpeg_decode_accelerator;
	bool mjpeg_decode_accelerator_explicit;
	video_broker_mjpeg_accelerator_t mjpeg_decode_initial_backend;
	bool mjpeg_decode_initial_backend_explicit;
	bool allow_software_fallback;
	int capture_select_timeout_ms;
	int capture_max_consecutive_timeouts;
	video_broker_video_orientation_t video_orientation;
	video_broker_nv12_aspect_policy_t aspect_policy;
	bool quality_monitor_enabled;
	int quality_monitor_report_every;
	int quality_monitor_sample_stride;
	const char *quality_monitor_state_path;
	bool tone_mapping_enabled;
	video_broker_tone_mode_t tone_mapping_mode;
	int tone_mapping_target;
	int tone_mapping_hysteresis;
	int tone_mapping_grid_columns;
	int tone_mapping_grid_rows;
	int tone_mapping_statistics_hz;
	double tone_mapping_max_shadow_gain;
	double tone_mapping_max_highlight_reduction;
	double tone_mapping_temporal_alpha;
	bool tone_mapping_noise_guard;
	video_broker_tone_accelerator_kind_t tone_mapping_accelerator;
	video_broker_archive_content_t archive_content;
	int archive_codec;
	video_broker_rdkx5_rate_control_profile_t archive_rate_control;
	const char *source_id;
	uint64_t media_epoch;
	bool source_id_explicit;
	bool media_epoch_explicit;
	video_broker_frame_launch_args frame_launch;
} bridge_config_t;

typedef struct {
	int fd;
	mmap_buffer_t *buffers;
	unsigned int buffer_count;
	int width;
	int height;
	int fps;
	uint32_t pixel_format;
	bool streaming;
} v4l2_capture_t;

typedef struct {
	uint64_t frames;
	uint64_t last_log_us;
	uint64_t last_sample_us;
} quality_monitor_t;

typedef video_broker_nv12_quality_measure_t quality_measure_t;

typedef struct {
	bool active;
	uint64_t frame;
	int stride;
	int source_width;
	int source_height;
	quality_measure_t source;
} quality_observation_t;

typedef struct {
	uint64_t last_log_us;
	uint64_t last_capture_us;
	uint64_t frames;
	uint64_t main_frames;
	uint64_t sub_frames;
	uint64_t capture_timeouts;
	uint64_t capture_corrupt_frames;
	uint64_t capture_corrupt_run_max;
	uint64_t wait_us;
	uint64_t wait_max_us;
	uint64_t capture_gap_max_us;
	uint64_t frame_work_us;
	uint64_t frame_work_max_us;
	uint64_t dqbuf_us;
	uint64_t decode_us;
	uint64_t decode_max_us;
	uint64_t decode_no_output;
	uint64_t decode_no_output_run;
	uint64_t decode_no_output_run_max;
	uint64_t mjpeg_decode_queue_replacements;
	uint64_t mjpeg_decode_post_wait_frames;
	uint64_t main_resize_ops;
	uint64_t main_resize_us;
	uint64_t main_capture_surface_frames;
	uint64_t sub_resize_ops;
	uint64_t sub_resize_us;
	uint64_t sub_resize_accelerated_frames;
	uint64_t sub_resize_cpu_frames;
	uint64_t archive_resize_ops;
	uint64_t archive_resize_us;
	uint64_t archive_capture_surface_frames;
	uint64_t tone_mapping_ops;
	uint64_t tone_mapping_us;
	uint64_t tone_mapping_max_us;
	uint64_t quality_ops;
	uint64_t quality_us;
	uint64_t quality_max_us;
	uint64_t encode_ops;
	uint64_t encode_us;
	uint64_t encode_max_us;
	uint64_t archive_frames;
	uint64_t archive_encode_us;
	uint64_t archive_encode_max_us;
	uint64_t write_main_us;
	uint64_t write_main_max_us;
	uint64_t write_sub_us;
	uint64_t write_sub_max_us;
	uint64_t write_archive_us;
	uint64_t write_archive_max_us;
} runtime_metrics_t;

typedef struct {
	char pending[kControlLineMax];
	size_t pending_size;
	uint64_t sequence;
	bool main_active;
	int main_fps;
	int main_bitrate_kbps;
	int sub_fps;
	bool optional_compute_admitted;
	uint64_t request_keyframe_token;
	char retry_action[kRetryActionMax + 1];
	char retry_component[kRetryComponentMax + 1];
	uint64_t retry_token;
	int64_t retry_deadline_unix_ms;
	uint64_t last_retry_token;
	bool trial_pending;
} runtime_control_t;

/* Every mutable output resource moves as one role-owned unit during a future
 * branch transaction. Capture and decode remain source-owned and are never
 * duplicated by a role replacement. */
typedef struct {
	video_broker_worker_role_t role;
	video_broker_worker_branch_profile_t profile;
	uint64_t branch_epoch;
	uint64_t frames;
	unsigned char *frame;
	size_t frame_size;
	unsigned char *stream;
	size_t stream_capacity;
	/* The vendor codec context is address-bound after initialization. Keep it
	 * heap-owned so a branch cutover moves only this pointer, never the live
	 * media_codec_context_t object itself. */
	video_broker_rdkx5_persistent_encoder_t *encoder;
	bool encoder_initialized;
	int emit_accumulator;
	uint64_t async_next_due_us;
	int active_bitrate_kbps;
	uint64_t encoder_local_epoch;
	uint64_t pending_control_sequence;
	bool pending_control_ready;
	bool delivery_active;
	bool native_passthrough;
	bool uses_capture_surface;
	bool seen_vps;
	bool seen_sps;
	bool seen_pps;
	bool seen_keyframe;
	bool candidate_tone_initialized;
	video_broker_nv12_tone_mapper_t candidate_tone_mapper;
	video_broker_tone_accelerator_t candidate_tone_accelerator;
} v4l2_role_runtime_t;

typedef struct {
	bridge_config_t *cfg;
	v4l2_capture_t *capture;
	runtime_control_t *control;
	v4l2_role_runtime_t *active[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	bool active_present[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	v4l2_role_runtime_t candidate[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	bool candidate_present[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	bool candidate_failed;
	v4l2_role_runtime_t incumbent[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	bool incumbent_present[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	bool active_failed[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	uint64_t next_epoch[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	video_broker_stream_epoch_t live_epoch;
	uint64_t cutover_frame_floor[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	video_broker_worker_branch_change_t changes[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	size_t change_count;
	bool cutover_completed;
	uint64_t wait_started_ms;
	const unsigned char *capture_access_unit;
	size_t capture_access_unit_size;
	const unsigned char *decoded_frame;
	int scheduling_fps;
	bool decoded_frame_ready;
	bool *direct_main_native;
	bool *archive_native_passthrough;
	bool *archive_encode_required;
	bool *main_uses_capture_surface;
	bool *archive_uses_capture_surface;
	bool *tone_map_capture_frame;
	video_broker_tone_accelerator_t *shared_tone_accelerator;
	bool *persistent_main_control;
} v4l2_fanout_backend_t;

typedef struct {
	uint32_t codec;
	uint64_t started_us;
	video_broker_stable_cadence_t decoded_cadence;
	bool seen_vps;
	bool seen_sps;
	bool seen_pps;
	bool seen_keyframe;
	bool admitted;
} camera_admission_t;

static const char *format_name(uint32_t fmt);
static quality_measure_t quality_measure_nv12_luma(
	const unsigned char *nv12,
	int width,
	int height,
	int stride);
static bool quality_measure_is_informative(const quality_measure_t *measure);

static const char *mjpeg_accelerator_name(video_broker_mjpeg_accelerator_t accelerator) {
	switch (accelerator) {
	case VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO:
		return "auto";
	case VIDEO_BROKER_MJPEG_ACCELERATOR_JPU:
		return "jpu";
	case VIDEO_BROKER_MJPEG_ACCELERATOR_CPU:
		return "cpu";
	default:
		return "invalid";
	}
}

static void handle_signal(int signum) {
	g_running = 0;
	g_stop_signal = signum;
}

static uint64_t monotonic_us(void) {
	struct timespec ts;
	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
		return 0;
	}
	return ((uint64_t)ts.tv_sec * 1000000ULL) + ((uint64_t)ts.tv_nsec / 1000ULL);
}

static int64_t realtime_ms(void) {
	struct timespec ts;
	if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
		return -1;
	}
	return (int64_t)ts.tv_sec * 1000 + (int64_t)ts.tv_nsec / 1000000;
}

static void metrics_add_stage(uint64_t *total, uint64_t *max_value, uint64_t start_us, uint64_t end_us) {
	uint64_t elapsed;
	if (total == NULL || start_us == 0 || end_us < start_us) {
		return;
	}
	elapsed = end_us - start_us;
	*total += elapsed;
	if (max_value != NULL && elapsed > *max_value) {
		*max_value = elapsed;
	}
}

static void metrics_log_if_due(runtime_metrics_t *metrics, video_broker_frame_source *source, uint64_t now_us, bool force) {
	uint64_t elapsed_us;
	long double elapsed_sec;
	if (metrics == NULL || now_us == 0) {
		return;
	}
	if (metrics->last_log_us == 0) {
		metrics->last_log_us = now_us;
		return;
	}
	elapsed_us = now_us - metrics->last_log_us;
	if (!force && elapsed_us < kRuntimeMetricsReportEveryUs) {
		return;
	}
	elapsed_sec = (long double)elapsed_us / 1000000.0L;
	if (elapsed_sec <= 0) {
		elapsed_sec = 1;
	}
	if (metrics->mjpeg_decode_post_wait_frames > 0) {
		vb_diagnostic_pipe_printf(&g_telemetry, "[rdkx5_v4l2_mux_bridge][metrics] event=mjpeg_completion_cadence"
			" elapsed_sec=%.1Lf post_wait_frames=%llu polling=bounded_twice_per_capture\n",
			elapsed_sec, (unsigned long long)metrics->mjpeg_decode_post_wait_frames);
	}
	vb_diagnostic_pipe_printf(&g_telemetry,
		"[rdkx5_v4l2_mux_bridge][metrics] event=native_pipeline_summary elapsed_sec=%.1Lf frames=%llu fps=%.2Lf main_frames=%llu sub_frames=%llu archive_frames=%llu capture_timeouts=%llu capture_corrupt_frames=%llu capture_corrupt_run_max=%llu wait_avg_ms=%.2Lf wait_max_ms=%.2Lf capture_gap_max_ms=%.2Lf frame_work_avg_ms=%.2Lf frame_work_max_ms=%.2Lf dqbuf_avg_ms=%.2Lf decode_avg_ms=%.2Lf decode_max_ms=%.2Lf decode_no_output=%llu decode_no_output_run_max=%llu mjpeg_decode_queue_replacements=%llu main_resize_avg_ms=%.2Lf main_capture_surface_frames=%llu sub_resize_avg_ms=%.2Lf sub_resize_accelerated_frames=%llu sub_resize_cpu_frames=%llu archive_resize_avg_ms=%.2Lf archive_capture_surface_frames=%llu tone_mapping_avg_ms=%.2Lf tone_mapping_max_ms=%.2Lf quality_avg_ms=%.2Lf quality_max_ms=%.2Lf encode_avg_ms=%.2Lf encode_max_ms=%.2Lf archive_encode_avg_ms=%.2Lf archive_encode_max_ms=%.2Lf write_main_avg_ms=%.2Lf write_main_max_ms=%.2Lf write_sub_avg_ms=%.2Lf write_sub_max_ms=%.2Lf write_archive_avg_ms=%.2Lf write_archive_max_ms=%.2Lf\n",
		elapsed_sec,
		(unsigned long long)metrics->frames,
		(long double)metrics->frames / elapsed_sec,
		(unsigned long long)metrics->main_frames,
		(unsigned long long)metrics->sub_frames,
		(unsigned long long)metrics->archive_frames,
		(unsigned long long)metrics->capture_timeouts,
		(unsigned long long)metrics->capture_corrupt_frames,
		(unsigned long long)metrics->capture_corrupt_run_max,
		metrics->frames ? ((long double)metrics->wait_us / (long double)metrics->frames / 1000.0L) : 0.0L,
		(long double)metrics->wait_max_us / 1000.0L,
		(long double)metrics->capture_gap_max_us / 1000.0L,
		metrics->frames ? ((long double)metrics->frame_work_us / (long double)metrics->frames / 1000.0L) : 0.0L,
		(long double)metrics->frame_work_max_us / 1000.0L,
		metrics->frames ? ((long double)metrics->dqbuf_us / (long double)metrics->frames / 1000.0L) : 0.0L,
		metrics->frames ? ((long double)metrics->decode_us / (long double)metrics->frames / 1000.0L) : 0.0L,
		(long double)metrics->decode_max_us / 1000.0L,
		(unsigned long long)metrics->decode_no_output,
		(unsigned long long)metrics->decode_no_output_run_max,
		(unsigned long long)metrics->mjpeg_decode_queue_replacements,
		metrics->main_resize_ops ? ((long double)metrics->main_resize_us / (long double)metrics->main_resize_ops / 1000.0L) : 0.0L,
		(unsigned long long)metrics->main_capture_surface_frames,
		metrics->sub_resize_ops ? ((long double)metrics->sub_resize_us / (long double)metrics->sub_resize_ops / 1000.0L) : 0.0L,
		(unsigned long long)metrics->sub_resize_accelerated_frames,
		(unsigned long long)metrics->sub_resize_cpu_frames,
		metrics->archive_resize_ops ? ((long double)metrics->archive_resize_us / (long double)metrics->archive_resize_ops / 1000.0L) : 0.0L,
		(unsigned long long)metrics->archive_capture_surface_frames,
		metrics->tone_mapping_ops ? ((long double)metrics->tone_mapping_us / (long double)metrics->tone_mapping_ops / 1000.0L) : 0.0L,
		(long double)metrics->tone_mapping_max_us / 1000.0L,
		metrics->quality_ops ? ((long double)metrics->quality_us / (long double)metrics->quality_ops / 1000.0L) : 0.0L,
		(long double)metrics->quality_max_us / 1000.0L,
		metrics->encode_ops ? ((long double)metrics->encode_us / (long double)metrics->encode_ops / 1000.0L) : 0.0L,
		(long double)metrics->encode_max_us / 1000.0L,
		metrics->archive_frames ? ((long double)metrics->archive_encode_us / (long double)metrics->archive_frames / 1000.0L) : 0.0L,
		(long double)metrics->archive_encode_max_us / 1000.0L,
		metrics->main_frames ? ((long double)metrics->write_main_us / (long double)metrics->main_frames / 1000.0L) : 0.0L,
		(long double)metrics->write_main_max_us / 1000.0L,
		metrics->sub_frames ? ((long double)metrics->write_sub_us / (long double)metrics->sub_frames / 1000.0L) : 0.0L,
		(long double)metrics->write_sub_max_us / 1000.0L,
		metrics->archive_frames ? ((long double)metrics->write_archive_us / (long double)metrics->archive_frames / 1000.0L) : 0.0L,
		(long double)metrics->write_archive_max_us / 1000.0L);
	vbsp_stats publication;
	if (source != NULL && source->enabled && video_broker_frame_source_stats(source, &publication) == 0) {
		vb_diagnostic_pipe_printf(&g_telemetry, "[rdkx5_v4l2_mux_bridge][metrics] event=native_source_publication_summary"
			" source=%s scope=lifetime mode=detached_bounded submitted=%llu attempted=%llu published=%llu"
			" replaced=%llu busy=%llu expired=%llu canceled=%llu pending=%u inflight=%u capacity=%u failure=%d"
			" submit_copy_wall_avg_ms=%.2Lf submit_copy_wall_max_ms=%.2Lf"
			" worker_wall_avg_ms=%.2Lf worker_wall_max_ms=%.2Lf"
			" publish_attempts=%llu publish_health_wall_max_us=%llu publish_health_cpu_us=%llu"
			" publish_authority_wall_max_us=%llu publish_authority_cpu_us=%llu"
			" publish_submit_wall_max_us=%llu publish_submit_cpu_us=%llu"
			" integrity=sha256 atomic_order=seq_cst\n",
			source->definition.source,
			(unsigned long long)publication.submitted, (unsigned long long)publication.attempted,
			(unsigned long long)publication.published, (unsigned long long)publication.replaced,
			(unsigned long long)publication.busy, (unsigned long long)publication.expired,
			(unsigned long long)publication.canceled, publication.pending, publication.inflight,
			publication.capacity, publication.failure,
			publication.submitted ? (long double)publication.copy_ns / publication.submitted / 1000000.0L : 0.0L,
			(long double)publication.copy_max_ns / 1000000.0L,
			publication.attempted ? (long double)publication.work_ns / publication.attempted / 1000000.0L : 0.0L,
			(long double)publication.work_max_ns / 1000000.0L,
			(unsigned long long)source->publish_attempts,
			(unsigned long long)(source->publish_phase_wall_max_ns[0]/1000),
			(unsigned long long)(source->publish_phase_cpu_at_wall_max_ns[0]/1000),
			(unsigned long long)(source->publish_phase_wall_max_ns[1]/1000),
			(unsigned long long)(source->publish_phase_cpu_at_wall_max_ns[1]/1000),
			(unsigned long long)(source->publish_phase_wall_max_ns[2]/1000),
			(unsigned long long)(source->publish_phase_cpu_at_wall_max_ns[2]/1000));
		vb_diagnostic_pipe_printf(&g_telemetry, "[rdkx5_v4l2_mux_bridge][metrics] event=native_output_submission_summary"
			" source=%s scope=lifetime transform_wall_max_us=%llu transform_cpu_us=%llu"
			" authority_wall_max_us=%llu authority_cpu_us=%llu"
			" layout_wall_max_us=%llu layout_cpu_us=%llu"
			" submit_wall_max_us=%llu submit_cpu_us=%llu\n", source->definition.source,
			(unsigned long long)(source->output_phase_wall_max_ns[0]/1000),
			(unsigned long long)(source->output_phase_cpu_at_wall_max_ns[0]/1000),
			(unsigned long long)(source->output_phase_wall_max_ns[1]/1000),
			(unsigned long long)(source->output_phase_cpu_at_wall_max_ns[1]/1000),
			(unsigned long long)(source->output_phase_wall_max_ns[2]/1000),
			(unsigned long long)(source->output_phase_cpu_at_wall_max_ns[2]/1000),
			(unsigned long long)(source->output_phase_wall_max_ns[3]/1000),
			(unsigned long long)(source->output_phase_cpu_at_wall_max_ns[3]/1000));
	}
	metrics->last_log_us = now_us;
	metrics->frames = 0;
	metrics->main_frames = 0;
	metrics->sub_frames = 0;
	metrics->archive_frames = 0;
	metrics->capture_timeouts = 0;
	metrics->capture_corrupt_frames = 0;
	metrics->capture_corrupt_run_max = 0;
	metrics->wait_us = 0;
	metrics->wait_max_us = 0;
	metrics->capture_gap_max_us = 0;
	metrics->frame_work_us = 0;
	metrics->frame_work_max_us = 0;
	metrics->dqbuf_us = 0;
	metrics->decode_us = 0;
	metrics->decode_max_us = 0;
	metrics->decode_no_output = 0;
	metrics->decode_no_output_run_max = 0;
	metrics->main_resize_ops = 0;
	metrics->main_resize_us = 0;
	metrics->main_capture_surface_frames = 0;
	metrics->sub_resize_ops = 0;
	metrics->sub_resize_us = 0;
	metrics->sub_resize_accelerated_frames = 0;
	metrics->sub_resize_cpu_frames = 0;
	metrics->archive_resize_ops = 0;
	metrics->archive_resize_us = 0;
	metrics->archive_capture_surface_frames = 0;
	metrics->tone_mapping_ops = 0;
	metrics->tone_mapping_us = 0;
	metrics->tone_mapping_max_us = 0;
	metrics->quality_ops = 0;
	metrics->quality_us = 0;
	metrics->quality_max_us = 0;
	metrics->encode_ops = 0;
	metrics->encode_us = 0;
	metrics->encode_max_us = 0;
	metrics->archive_encode_us = 0;
	metrics->archive_encode_max_us = 0;
	metrics->write_main_us = 0;
	metrics->write_main_max_us = 0;
	metrics->write_sub_us = 0;
	metrics->write_sub_max_us = 0;
	metrics->write_archive_us = 0;
	metrics->write_archive_max_us = 0;
}

static int tone_map_processed_frame(
	video_broker_nv12_tone_mapper_t *tone_mapper,
	video_broker_tone_accelerator_t *tone_accelerator,
	const bridge_config_t *cfg,
	runtime_metrics_t *metrics,
	unsigned char *frame,
	int width,
	int height,
	uint64_t *last_log_us
) {
	quality_measure_t raw_quality;
	quality_measure_t output_quality;
	uint64_t stage_start_us;
	uint64_t stage_end_us;
	uint64_t now_us;
	bool informative;
	bool report_due;
	video_broker_tone_accelerator_kind_t accelerator_before;
	int accelerator_rc;
	if (tone_mapper == NULL || cfg == NULL || metrics == NULL || frame == NULL ||
	    width <= 0 || height <= 0 || last_log_us == NULL) {
		return -1;
	}
	stage_start_us = monotonic_us();
	raw_quality = quality_measure_nv12_luma(frame, width, height, cfg->quality_monitor_sample_stride);
	if (raw_quality.samples == 0) {
		return 0;
	}
	informative = quality_measure_is_informative(&raw_quality);
	video_broker_nv12_tone_mapper_set_signal_informative(tone_mapper, informative);
	(void)video_broker_nv12_tone_mapper_update(
		tone_mapper, frame, width, height, cfg->quality_monitor_sample_stride);
	accelerator_before = tone_accelerator->selected;
	accelerator_rc = video_broker_tone_accelerator_apply(
		tone_accelerator, tone_mapper, frame, width, height);
	if (accelerator_rc < 0) {
		return -1;
	}
	if (accelerator_before != tone_accelerator->selected) {
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge] tone accelerator runtime fallback requested=%s selected=%s reason=%s\n",
			video_broker_tone_accelerator_name(tone_accelerator->requested),
			video_broker_tone_accelerator_name(tone_accelerator->selected),
			video_broker_tone_accelerator_error(tone_accelerator));
	}
	now_us = monotonic_us();
	report_due = *last_log_us == 0 || now_us - *last_log_us >= kDiagnosticLogIntervalUs;
	/* This observation is only used by the diagnostic below. The independent
	 * quality monitor still samples source/processed/analysis on its own cadence. */
	if (report_due) {
		output_quality = quality_measure_nv12_luma(frame, width, height, cfg->quality_monitor_sample_stride);
	}
	stage_end_us = monotonic_us();
	metrics->tone_mapping_ops++;
	metrics_add_stage(
		&metrics->tone_mapping_us,
		&metrics->tone_mapping_max_us,
		stage_start_us,
		stage_end_us);
	if (!report_due) {
		return 0;
	}
	*last_log_us = now_us;
	vb_diagnostic_pipe_printf(&g_telemetry,
		"[rdkx5_v4l2_mux_bridge][tone-mapper] event=tone_mapping_summary requested_mode=%s active_mode=%s accelerator_requested=%s accelerator_selected=%s raw_luma=%.1Lf raw_content_luma=%.1Lf output_luma=%.1Lf output_content_luma=%.1Lf raw_black_ratio=%.5Lf output_black_ratio=%.5Lf raw_white_ratio=%.5Lf output_white_ratio=%.5Lf signal_informative=%s target=%d gain_min=%.3f gain_max=%.3f statistics_hz=%d effective_statistics_hz=%d statistics_updates=%llu active=%s\n",
		video_broker_tone_mode_name(tone_mapper->requested_mode),
		video_broker_tone_mode_name(tone_mapper->active_mode),
		video_broker_tone_accelerator_name(tone_accelerator->requested),
		video_broker_tone_accelerator_name(tone_accelerator->selected),
		raw_quality.luma, raw_quality.content_luma, output_quality.luma, output_quality.content_luma,
		raw_quality.black_ratio, output_quality.black_ratio, raw_quality.white_ratio, output_quality.white_ratio,
		informative ? "true" : "false", tone_mapper->target_luma,
		tone_mapper->minimum_gain, tone_mapper->maximum_gain,
		tone_mapper->statistics_hz, tone_mapper->effective_statistics_hz,
		(unsigned long long)tone_mapper->statistics_updates,
		video_broker_nv12_tone_mapper_active(tone_mapper) ? "true" : "false");
	return 0;
}

static bool capture_corrupt_frame_should_restart(
	runtime_metrics_t *metrics,
	int *consecutive,
	uint32_t pixel_format,
	const char *reason
) {
	if (metrics == NULL || consecutive == NULL) {
		return true;
	}
	(*consecutive)++;
	metrics->capture_corrupt_frames++;
	if ((uint64_t)*consecutive > metrics->capture_corrupt_run_max) {
		metrics->capture_corrupt_run_max = (uint64_t)*consecutive;
	}
	fprintf(stderr,
		"[rdkx5_v4l2_mux_bridge] dropping corrupt capture frame format=%s reason=%s consecutive=%d limit=%d\n",
		format_name(pixel_format),
		reason == NULL ? "unknown" : reason,
		*consecutive,
		kMaxConsecutiveCorruptFrames);
	return *consecutive >= kMaxConsecutiveCorruptFrames;
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

static uint32_t parse_v4l2_format(const char *value) {
	if (value == NULL) {
		return 0;
	}
	if (strcmp(value, "mjpeg") == 0) {
		return V4L2_PIX_FMT_MJPEG;
	}
	if (strcmp(value, "yuyv") == 0) {
		return V4L2_PIX_FMT_YUYV;
	}
	if (strcmp(value, "nv12") == 0) {
		return V4L2_PIX_FMT_NV12;
	}
	if (strcmp(value, "h264") == 0) {
		return V4L2_PIX_FMT_H264;
	}
	if (strcmp(value, "h265") == 0) {
		return V4L2_PIX_FMT_HEVC;
	}
	return 0;
}

static const char *format_name(uint32_t fmt) {
	switch (fmt) {
	case V4L2_PIX_FMT_MJPEG:
		return "mjpeg";
	case V4L2_PIX_FMT_YUYV:
		return "yuyv";
	case V4L2_PIX_FMT_NV12:
		return "nv12";
	case V4L2_PIX_FMT_H264:
		return "h264";
	case V4L2_PIX_FMT_HEVC:
		return "h265";
	default:
		return "unknown";
	}
}

static const char *aspect_policy_name(video_broker_nv12_aspect_policy_t policy) {
	switch (policy) {
	case VIDEO_BROKER_NV12_ASPECT_EXACT:
		return "exact";
	case VIDEO_BROKER_NV12_ASPECT_LETTERBOX:
		return "letterbox";
	default:
		return "crop";
	}
}

static bool video_aspect_matches(int left_w, int left_h, int right_w, int right_h) {
	return left_w > 0 && left_h > 0 && right_w > 0 && right_h > 0 &&
		(int64_t)left_w * right_h == (int64_t)right_w * left_h;
}

static int even_or_fallback(int value, int fallback) {
	if (value <= 0) {
		value = fallback;
	}
	if (value % 2 != 0) {
		value -= 1;
	}
	return value > 0 ? value : fallback;
}

static void *allocate_frame_buffer(size_t size) {
	void *buffer = NULL;
	if (size == 0 || posix_memalign(&buffer, 4096, size) != 0) {
		return NULL;
	}
	return buffer;
}

static void default_config(bridge_config_t *cfg) {
	memset(cfg, 0, sizeof(*cfg));
	cfg->device = NULL;
	cfg->input_format_name = "mjpeg";
	cfg->input_format = V4L2_PIX_FMT_MJPEG;
	cfg->capture_width = kDefaultCaptureWidth;
	cfg->capture_height = kDefaultCaptureHeight;
	cfg->archive_width = 0;
	cfg->archive_height = 0;
	cfg->output_width = kDefaultOutputWidth;
	cfg->output_height = kDefaultOutputHeight;
	cfg->sub_width = kDefaultSubWidth;
	cfg->sub_height = kDefaultSubHeight;
	cfg->sub_fps = kDefaultFPS;
	cfg->main_fps = kDefaultFPS;
	cfg->archive_fps = kDefaultFPS;
	cfg->fps = kDefaultFPS;
	cfg->warmup_sec = kDefaultWarmupSec;
	cfg->live_codec = SP_ENCODER_H265;
	cfg->live_encode_bitrate_kbps = kDefaultLiveBitrateKbps;
	cfg->archive_encode_bitrate_kbps = kDefaultArchiveBitrateKbps;
	cfg->live_enabled = false;
	cfg->analysis_enabled = false;
	cfg->archive_enabled = false;
	cfg->live_enabled_explicit = false;
	cfg->analysis_enabled_explicit = false;
	cfg->archive_enabled_explicit = false;
	cfg->main_active = false;
	cfg->mjpeg_decode_accelerator = VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO;
	cfg->mjpeg_decode_initial_backend = VIDEO_BROKER_MJPEG_ACCELERATOR_JPU;
	cfg->allow_software_fallback = true;
	cfg->capture_select_timeout_ms = kDefaultCaptureSelectTimeoutMs;
	cfg->capture_max_consecutive_timeouts = kDefaultCaptureMaxConsecutiveTimeouts;
	cfg->video_orientation = VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL;
	cfg->aspect_policy = VIDEO_BROKER_NV12_ASPECT_CROP;
	cfg->quality_monitor_enabled = false;
	cfg->quality_monitor_report_every = kDefaultQualityReportEvery;
	cfg->quality_monitor_sample_stride = kDefaultQualitySampleStride;
	cfg->tone_mapping_enabled = false;
	cfg->tone_mapping_mode = VIDEO_BROKER_TONE_OFF;
	cfg->tone_mapping_target = 128;
	cfg->tone_mapping_hysteresis = 12;
	cfg->tone_mapping_grid_columns = 16;
	cfg->tone_mapping_grid_rows = 9;
	cfg->tone_mapping_statistics_hz = 8;
	cfg->tone_mapping_max_shadow_gain = 2.4;
	cfg->tone_mapping_max_highlight_reduction = 0.20;
	cfg->tone_mapping_temporal_alpha = 0.35;
	cfg->tone_mapping_noise_guard = true;
	cfg->tone_mapping_accelerator = VIDEO_BROKER_TONE_ACCELERATOR_AUTO;
	cfg->archive_content = VIDEO_BROKER_ARCHIVE_ADAPTIVE;
	cfg->archive_codec = SP_ENCODER_H265;
	cfg->archive_rate_control = VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_CBR;
}

static const char *archive_rate_control_name(video_broker_rdkx5_rate_control_profile_t profile) {
	return profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR ? "avbr" : "cbr";
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

static int parse_args(int argc, char **argv, bridge_config_t *cfg) {
#define REQUIRE_INT(ARG_NAME, FIELD, MINIMUM) \
	do { \
		if (++i >= argc || !parse_int_arg(argv[i], (MINIMUM), 1 << 20, &(FIELD))) { \
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid %s\n", (ARG_NAME)); \
			return -1; \
		} \
	} while (0)
#define REQUIRE_DOUBLE(ARG_NAME, FIELD) \
	do { \
		if (++i >= argc || !parse_double_arg(argv[i], &(FIELD))) { \
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid %s\n", (ARG_NAME)); \
			return -1; \
		} \
	} while (0)
	for (int i = 1; i < argc; ++i) {
		if (strcmp(argv[i], "--device") == 0 && i + 1 < argc) {
			cfg->device = argv[++i];
		} else if (strcmp(argv[i], "--input-format") == 0 && i + 1 < argc) {
			cfg->input_format_name = argv[++i];
			cfg->input_format = parse_v4l2_format(cfg->input_format_name);
		} else if (strcmp(argv[i], "--capture-width") == 0) {
			REQUIRE_INT("--capture-width", cfg->capture_width, 0);
		} else if (strcmp(argv[i], "--capture-height") == 0) {
			REQUIRE_INT("--capture-height", cfg->capture_height, 0);
		} else if (strcmp(argv[i], "--archive-width") == 0) {
			REQUIRE_INT("--archive-width", cfg->archive_width, 0);
		} else if (strcmp(argv[i], "--archive-height") == 0) {
			REQUIRE_INT("--archive-height", cfg->archive_height, 0);
		} else if (strcmp(argv[i], "--output-width") == 0) {
			REQUIRE_INT("--output-width", cfg->output_width, 0);
		} else if (strcmp(argv[i], "--output-height") == 0) {
			REQUIRE_INT("--output-height", cfg->output_height, 0);
		} else if (strcmp(argv[i], "--sub-width") == 0) {
			REQUIRE_INT("--sub-width", cfg->sub_width, 0);
		} else if (strcmp(argv[i], "--sub-height") == 0) {
			REQUIRE_INT("--sub-height", cfg->sub_height, 0);
		} else if (strcmp(argv[i], "--sub-fps") == 0) {
			REQUIRE_INT("--sub-fps", cfg->sub_fps, 0);
		} else if (strcmp(argv[i], "--main-fps") == 0) {
			REQUIRE_INT("--main-fps", cfg->main_fps, 0);
		} else if (strcmp(argv[i], "--archive-fps") == 0) {
			REQUIRE_INT("--archive-fps", cfg->archive_fps, 0);
		} else if (strcmp(argv[i], "--live-codec") == 0 && i + 1 < argc) {
			const char *codec = argv[++i];
			if (strcmp(codec, "h265") == 0) {
				cfg->live_codec = SP_ENCODER_H265;
			} else if (strcmp(codec, "h264") == 0) {
				cfg->live_codec = SP_ENCODER_H264;
			} else {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --live-codec; expected h265|h264\n");
				return -1;
			}
			cfg->live_codec_explicit = true;
		} else if (strcmp(argv[i], "--fps") == 0) {
			REQUIRE_INT("--fps", cfg->fps, 0);
		} else if (strcmp(argv[i], "--warmup-sec") == 0) {
			REQUIRE_INT("--warmup-sec", cfg->warmup_sec, 0);
		} else if (strcmp(argv[i], "--live-encode-bitrate-kbps") == 0) {
			REQUIRE_INT("--live-encode-bitrate-kbps", cfg->live_encode_bitrate_kbps, 0);
		} else if (strcmp(argv[i], "--archive-encode-bitrate-kbps") == 0) {
			REQUIRE_INT("--archive-encode-bitrate-kbps", cfg->archive_encode_bitrate_kbps, 0);
		} else if (strcmp(argv[i], "--live-enabled") == 0) {
			if (++i >= argc || !video_broker_parse_required_bool(argv[i], &cfg->live_enabled)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --live-enabled; expected true|false\n");
				return -1;
			}
			cfg->live_enabled_explicit = true;
		} else if (strcmp(argv[i], "--analysis-enabled") == 0) {
			if (++i >= argc || !video_broker_parse_required_bool(argv[i], &cfg->analysis_enabled)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --analysis-enabled; expected true|false\n");
				return -1;
			}
			cfg->analysis_enabled_explicit = true;
		} else if (strcmp(argv[i], "--archive-enabled") == 0) {
			if (++i >= argc || !video_broker_parse_required_bool(argv[i], &cfg->archive_enabled)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --archive-enabled; expected true|false\n");
				return -1;
			}
			cfg->archive_enabled_explicit = true;
		} else if (strcmp(argv[i], "--main-active") == 0) {
			if (++i >= argc || !video_broker_parse_required_bool(argv[i], &cfg->main_active)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --main-active; expected true|false\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--mjpeg-decode-accelerator") == 0) {
			if (++i >= argc || !video_broker_parse_mjpeg_accelerator(
					argv[i], &cfg->mjpeg_decode_accelerator)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --mjpeg-decode-accelerator; expected auto|jpu|cpu\n");
				return -1;
			}
			cfg->mjpeg_decode_accelerator_explicit = true;
		} else if (strcmp(argv[i], "--mjpeg-decode-initial-backend") == 0) {
			if (++i >= argc || !video_broker_parse_mjpeg_accelerator(
					argv[i], &cfg->mjpeg_decode_initial_backend)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --mjpeg-decode-initial-backend; expected auto|jpu|cpu\n");
				return -1;
			}
			cfg->mjpeg_decode_initial_backend_explicit = true;
		} else if (strcmp(argv[i], "--allow-sw-fallback") == 0) {
			if (++i >= argc || !video_broker_parse_required_bool(
					argv[i], &cfg->allow_software_fallback)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --allow-sw-fallback; expected true|false\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--capture-select-timeout-ms") == 0) {
			REQUIRE_INT("--capture-select-timeout-ms", cfg->capture_select_timeout_ms, 0);
		} else if (strcmp(argv[i], "--capture-max-consecutive-timeouts") == 0) {
			REQUIRE_INT("--capture-max-consecutive-timeouts", cfg->capture_max_consecutive_timeouts, 0);
		} else if (strcmp(argv[i], "--video-orientation") == 0) {
			if (++i >= argc || !parse_video_orientation_arg(argv[i], &cfg->video_orientation)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --video-orientation; expected normal|rotate_180|mirror_horizontal|mirror_vertical\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--aspect-policy") == 0 && i + 1 < argc) {
			const char *policy = argv[++i];
			if (strcmp(policy, "exact") == 0) {
				cfg->aspect_policy = VIDEO_BROKER_NV12_ASPECT_EXACT;
			} else if (strcmp(policy, "crop") == 0) {
				cfg->aspect_policy = VIDEO_BROKER_NV12_ASPECT_CROP;
			} else if (strcmp(policy, "letterbox") == 0) {
				cfg->aspect_policy = VIDEO_BROKER_NV12_ASPECT_LETTERBOX;
			} else {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --aspect-policy; expected exact|crop|letterbox\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--quality-monitor-enabled") == 0) {
			if (++i >= argc || !video_broker_parse_required_bool(argv[i], &cfg->quality_monitor_enabled)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --quality-monitor-enabled; expected true|false\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--quality-monitor-report-every") == 0) {
			REQUIRE_INT("--quality-monitor-report-every", cfg->quality_monitor_report_every, 0);
		} else if (strcmp(argv[i], "--quality-monitor-sample-stride") == 0) {
			REQUIRE_INT("--quality-monitor-sample-stride", cfg->quality_monitor_sample_stride, 0);
		} else if (strcmp(argv[i], "--quality-monitor-state-path") == 0 && i + 1 < argc) {
			cfg->quality_monitor_state_path = argv[++i];
		} else if (strcmp(argv[i], "--tone-mapping-mode") == 0) {
			if (++i >= argc || !parse_tone_mode_arg(argv[i], &cfg->tone_mapping_mode)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --tone-mapping-mode; expected auto|off|global|local\n");
				return -1;
			}
			cfg->tone_mapping_enabled = cfg->tone_mapping_mode != VIDEO_BROKER_TONE_OFF;
		} else if (strcmp(argv[i], "--tone-mapping-target") == 0) {
			REQUIRE_INT("--tone-mapping-target", cfg->tone_mapping_target, 0);
		} else if (strcmp(argv[i], "--tone-mapping-hysteresis") == 0) {
			REQUIRE_INT("--tone-mapping-hysteresis", cfg->tone_mapping_hysteresis, 0);
		} else if (strcmp(argv[i], "--tone-mapping-grid-columns") == 0) {
			REQUIRE_INT("--tone-mapping-grid-columns", cfg->tone_mapping_grid_columns, 0);
		} else if (strcmp(argv[i], "--tone-mapping-grid-rows") == 0) {
			REQUIRE_INT("--tone-mapping-grid-rows", cfg->tone_mapping_grid_rows, 0);
		} else if (strcmp(argv[i], "--tone-mapping-statistics-hz") == 0) {
			REQUIRE_INT("--tone-mapping-statistics-hz", cfg->tone_mapping_statistics_hz, 0);
		} else if (strcmp(argv[i], "--tone-mapping-max-shadow-gain") == 0) {
			REQUIRE_DOUBLE("--tone-mapping-max-shadow-gain", cfg->tone_mapping_max_shadow_gain);
		} else if (strcmp(argv[i], "--tone-mapping-max-highlight-reduction") == 0) {
			REQUIRE_DOUBLE("--tone-mapping-max-highlight-reduction", cfg->tone_mapping_max_highlight_reduction);
		} else if (strcmp(argv[i], "--tone-mapping-temporal-alpha") == 0) {
			REQUIRE_DOUBLE("--tone-mapping-temporal-alpha", cfg->tone_mapping_temporal_alpha);
		} else if (strcmp(argv[i], "--tone-mapping-noise-guard") == 0) {
			if (++i >= argc || !video_broker_parse_required_bool(argv[i], &cfg->tone_mapping_noise_guard)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --tone-mapping-noise-guard; expected true|false\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--tone-mapping-accelerator") == 0) {
			if (++i >= argc || !parse_tone_accelerator_arg(argv[i], &cfg->tone_mapping_accelerator)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --tone-mapping-accelerator; expected auto|cpu|opencl\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--archive-content") == 0) {
			if (++i >= argc || !parse_archive_content_arg(argv[i], &cfg->archive_content)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --archive-content; expected adaptive|source|processed\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--archive-codec") == 0 && i + 1 < argc) {
			const char *codec = argv[++i];
			if (strcmp(codec, "h265") == 0) {
				cfg->archive_codec = SP_ENCODER_H265;
			} else if (strcmp(codec, "h264") == 0) {
				cfg->archive_codec = SP_ENCODER_H264;
			} else {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --archive-codec; expected h265|h264\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--archive-rate-control") == 0 && i + 1 < argc) {
			const char *mode = argv[++i];
			if (strcmp(mode, "cbr") == 0) {
				cfg->archive_rate_control = VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_CBR;
			} else if (strcmp(mode, "avbr") == 0) {
				cfg->archive_rate_control = VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR;
			} else {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --archive-rate-control; expected cbr|avbr\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--source-id") == 0 && i + 1 < argc) {
			cfg->source_id = argv[++i];
			cfg->source_id_explicit = true;
		} else if (strcmp(argv[i], "--media-epoch") == 0) {
			if (++i >= argc || !parse_uint64_arg(argv[i], &cfg->media_epoch)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid --media-epoch\n");
				return -1;
			}
			cfg->media_epoch_explicit = true;
		} else if ((strncmp(argv[i], "--frame-launch-", 15) == 0 ||
		            strncmp(argv[i], "--frame-native-mailbox-", 23) == 0 ||
		            strncmp(argv[i], "--frame-output-mailbox-", 23) == 0 ||
		            strcmp(argv[i], "--frame-output-stream") == 0) && i + 1 < argc) {
			if (video_broker_frame_launch_arg(&cfg->frame_launch, argv[i], argv[i+1]) != 1) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] frame_launch stage=argument_invalid option=%s\n", argv[i]);
				return -1;
			}
			i++;
		} else {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] unknown or incomplete argument: %s\n", argv[i]);
			return -1;
		}
	}
#undef REQUIRE_DOUBLE
#undef REQUIRE_INT
	if (cfg->device == NULL || cfg->device[0] == '\0') {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] explicit --device is required\n");
		return -1;
	}
	if (!cfg->live_enabled_explicit || !cfg->analysis_enabled_explicit || !cfg->archive_enabled_explicit ||
	    !cfg->live_codec_explicit ||
	    (!cfg->live_enabled && !cfg->analysis_enabled && !cfg->archive_enabled)) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] explicit output roles and live codec are required\n");
		return -1;
	}
	if (cfg->source_id_explicit != cfg->media_epoch_explicit) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] source identity fence is incomplete\n");
		return -1;
	}
	if (video_broker_frame_launch_validate(&cfg->frame_launch) != 0 ||
	    (cfg->frame_launch.seen && !cfg->source_id_explicit)) return -1;
	cfg->main_active = cfg->live_enabled && cfg->main_active;
	if (!video_broker_mjpeg_decode_policy_valid(
			cfg->mjpeg_decode_accelerator,
			cfg->mjpeg_decode_accelerator_explicit,
			cfg->mjpeg_decode_initial_backend,
			cfg->mjpeg_decode_initial_backend_explicit,
			cfg->allow_software_fallback)) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid MJPEG policy/initial backend combination\n");
		return -1;
	}
	cfg->capture_width = even_or_fallback(cfg->capture_width, kDefaultCaptureWidth);
	cfg->capture_height = even_or_fallback(cfg->capture_height, kDefaultCaptureHeight);
	cfg->archive_width = even_or_fallback(cfg->archive_width, cfg->capture_width);
	cfg->archive_height = even_or_fallback(cfg->archive_height, cfg->capture_height);
	cfg->output_width = even_or_fallback(cfg->output_width, kDefaultOutputWidth);
	cfg->output_height = even_or_fallback(cfg->output_height, kDefaultOutputHeight);
	cfg->sub_width = even_or_fallback(cfg->sub_width, kDefaultSubWidth);
	cfg->sub_height = even_or_fallback(cfg->sub_height, kDefaultSubHeight);
	if (cfg->fps <= 0) {
		cfg->fps = kDefaultFPS;
	}
	if (cfg->sub_fps <= 0 || cfg->sub_fps > cfg->fps) {
		cfg->sub_fps = cfg->fps;
	}
	if (cfg->main_fps <= 0 || cfg->main_fps > cfg->fps) {
		cfg->main_fps = cfg->fps;
	}
	if (cfg->archive_fps <= 0) {
		cfg->archive_fps = cfg->fps;
	}
	if (cfg->archive_fps > cfg->fps) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] archive fps must not exceed capture fps\n");
		return -1;
	}
	if (cfg->live_encode_bitrate_kbps <= 0) {
		cfg->live_encode_bitrate_kbps = kDefaultLiveBitrateKbps;
	}
	if (cfg->archive_encode_bitrate_kbps <= 0) {
		cfg->archive_encode_bitrate_kbps = kDefaultArchiveBitrateKbps;
	}
	if (cfg->capture_select_timeout_ms <= 0) {
		cfg->capture_select_timeout_ms = kDefaultCaptureSelectTimeoutMs;
	}
	if (cfg->capture_max_consecutive_timeouts <= 0) {
		cfg->capture_max_consecutive_timeouts = kDefaultCaptureMaxConsecutiveTimeouts;
	}
	if (cfg->quality_monitor_report_every <= 0) {
		cfg->quality_monitor_report_every = kDefaultQualityReportEvery;
	}
	if (cfg->quality_monitor_sample_stride <= 0) {
		cfg->quality_monitor_sample_stride = kDefaultQualitySampleStride;
	}
	if (cfg->quality_monitor_sample_stride > 64) {
		cfg->quality_monitor_sample_stride = 64;
	}
	cfg->tone_mapping_target = video_broker_tone_clamp_int(cfg->tone_mapping_target, 16, 235);
	cfg->tone_mapping_hysteresis = video_broker_tone_clamp_int(cfg->tone_mapping_hysteresis, 1, 64);
	cfg->tone_mapping_grid_columns = video_broker_tone_clamp_int(
		cfg->tone_mapping_grid_columns, 1, VIDEO_BROKER_TONE_MAX_COLUMNS);
	cfg->tone_mapping_grid_rows = video_broker_tone_clamp_int(
		cfg->tone_mapping_grid_rows, 1, VIDEO_BROKER_TONE_MAX_ROWS);
	cfg->tone_mapping_statistics_hz = video_broker_tone_clamp_int(cfg->tone_mapping_statistics_hz, 1, 30);
	cfg->tone_mapping_max_shadow_gain = video_broker_tone_clamp_double(
		cfg->tone_mapping_max_shadow_gain, 1.0, 3.0);
	cfg->tone_mapping_max_highlight_reduction = video_broker_tone_clamp_double(
		cfg->tone_mapping_max_highlight_reduction, 0.0, 0.75);
	cfg->tone_mapping_temporal_alpha = video_broker_tone_clamp_double(
		cfg->tone_mapping_temporal_alpha, 0.01, 1.0);
	if (cfg->tone_mapping_enabled && cfg->archive_content == VIDEO_BROKER_ARCHIVE_SOURCE) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] archive content source requires tone mapping off\n");
		return -1;
	}
	return 0;
}

static bool capture_sequence_contiguous(uint32_t previous, uint32_t current) {
	return (uint32_t)(current - previous) == 1U;
}

static bool capture_recovery_output_ready(
	bool waiting_idr, vb_capture_origin clean, vb_capture_origin decoded) {
	return !waiting_idr && (clean.sequence == 0 ||
		(decoded.epoch == clean.epoch && decoded.sequence >= clean.sequence));
}

static int xioctl(int fd, unsigned long request, void *arg) {
	int rc;
	do {
		/* QBUF is in/out: drivers may clear bytesused, sequence and other
		 * captured metadata. Preserve the dequeue snapshot after returning its
		 * lease, including when EINTR requires a fresh queue attempt. All input
		 * formats share this rule; DQBUF and other result ioctls stay in/out. */
		struct v4l2_buffer queue_snapshot;
		void *ioctl_arg = arg;
		if (request == VIDIOC_QBUF && arg != NULL) {
			queue_snapshot = *(const struct v4l2_buffer *)arg;
			ioctl_arg = &queue_snapshot;
		}
		rc = ioctl(fd, request, ioctl_arg);
	} while (rc == -1 && errno == EINTR);
	return rc;
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

static int write_live_encoder_epoch(FILE *out, uint64_t epoch) {
	unsigned char payload[8];
	for (size_t index = 0; index < sizeof(payload); ++index) {
		payload[sizeof(payload) - 1U - index] = (unsigned char)(epoch & 0xffU);
		epoch >>= 8U;
	}
	return write_mux_frame(out, kLiveEncoderEpochStreamID, payload, (uint32_t)sizeof(payload));
}

static int v4l2_publish_live_encoder_epoch(
	v4l2_fanout_backend_t *backend, FILE *out,
	uint64_t encoded_epoch, int encoded_bitrate_kbps) {
	v4l2_role_runtime_t *live;
	int changed;
	if (backend == NULL || out == NULL || encoded_bitrate_kbps <= 0) return -1;
	live = backend->active[VIDEO_BROKER_WORKER_ROLE_LIVE];
	if (live == NULL) return -1;
	changed = video_broker_stream_epoch_observe(
		&backend->live_epoch, live->branch_epoch, encoded_epoch);
	if (changed < 0) return -1;
	live->encoder_local_epoch = encoded_epoch;
	live->active_bitrate_kbps = encoded_bitrate_kbps;
	return changed ? write_live_encoder_epoch(out, backend->live_epoch.value) : 0;
}

static void runtime_control_init(runtime_control_t *control, const bridge_config_t *cfg) {
	memset(control, 0, sizeof(*control));
	control->main_active = cfg->main_active;
	control->main_fps = cfg->main_fps;
	control->main_bitrate_kbps = cfg->live_encode_bitrate_kbps;
	control->sub_fps = cfg->sub_fps;
	control->optional_compute_admitted = true;
	(void)snprintf(control->retry_action, sizeof(control->retry_action), "none");
	(void)snprintf(control->retry_component, sizeof(control->retry_component), "none");
}

static int runtime_control_parse_line(runtime_control_t *control, const char *line, int capture_fps) {
	unsigned long long sequence = 0;
	unsigned long long retry_token = 0;
	long long retry_deadline_unix_ms = 0;
	int main_active = 0;
	int main_fps = 0;
	int main_bitrate_kbps = 0;
	int sub_fps = 0;
	int optional_compute_admitted = 0;
	unsigned long long request_keyframe_token = 0;
	char retry_action[kRetryActionMax + 1];
	char retry_component[kRetryComponentMax + 1];
	bool retry_none;
	bool retry_trial;
	bool retry_commit;
	bool retry_cancel;
	int consumed = 0;
	int matched;

	memset(retry_action, 0, sizeof(retry_action));
	memset(retry_component, 0, sizeof(retry_component));
	matched = sscanf(
		line,
		"NMC2 seq=%llu main_active=%d main_fps=%d main_bitrate_kbps=%d sub_fps=%d optional_compute_admitted=%d request_keyframe_token=%llu retry_action=%16[a-z_] retry_component=%64[a-z0-9._-] retry_token=%llu retry_deadline_unix_ms=%lld%n",
		&sequence,
		&main_active,
		&main_fps,
		&main_bitrate_kbps,
		&sub_fps,
		&optional_compute_admitted,
		&request_keyframe_token,
		retry_action,
		retry_component,
		&retry_token,
		&retry_deadline_unix_ms,
		&consumed);
	retry_none = strcmp(retry_action, "none") == 0;
	retry_trial = strcmp(retry_action, "trial") == 0;
	retry_commit = strcmp(retry_action, "commit") == 0;
	retry_cancel = strcmp(retry_action, "cancel") == 0;
	if (matched != 11 || line[consumed] != '\0' || (main_active != 0 && main_active != 1) ||
	    (optional_compute_admitted != 0 && optional_compute_admitted != 1) ||
	    main_fps <= 0 || main_fps > capture_fps || sub_fps <= 0 || sub_fps > capture_fps ||
	    main_bitrate_kbps <= 0 || main_bitrate_kbps > (1 << 20) ||
	    (!retry_none && !retry_trial && !retry_commit && !retry_cancel) ||
	    (retry_none && (strcmp(retry_component, "none") != 0 || retry_token != 0 || retry_deadline_unix_ms != 0)) ||
	    ((retry_trial || retry_commit) &&
	     (strcmp(retry_component, "none") == 0 || retry_token == 0 || retry_deadline_unix_ms <= 0)) ||
	    (retry_cancel &&
	     (strcmp(retry_component, "none") == 0 || retry_token == 0 || retry_deadline_unix_ms != 0))) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge][control] rejected malformed command\n");
		return 0;
	}
	if ((uint64_t)sequence <= control->sequence) {
		return 0;
	}
	control->sequence = (uint64_t)sequence;
	control->main_active = main_active != 0;
	control->main_fps = main_fps;
	control->main_bitrate_kbps = main_bitrate_kbps;
	control->sub_fps = sub_fps;
	control->optional_compute_admitted = optional_compute_admitted != 0;
	control->request_keyframe_token = (uint64_t)request_keyframe_token;
	(void)snprintf(control->retry_action, sizeof(control->retry_action), "%s", retry_action);
	(void)snprintf(control->retry_component, sizeof(control->retry_component), "%s", retry_component);
	control->retry_token = (uint64_t)retry_token;
	control->retry_deadline_unix_ms = (int64_t)retry_deadline_unix_ms;
	control->trial_pending = retry_trial && retry_token > control->last_retry_token;
	if (retry_none) {
		control->trial_pending = false;
	}
	return 1;
}

static int runtime_control_poll(
	runtime_control_t *control,
	int capture_fps,
	video_broker_worker_fanout_controller_t *fanout_controller) {
	unsigned char input[256];
	ssize_t count;
	int changed = 0;

	for (;;) {
		count = read(STDIN_FILENO, input, sizeof(input));
		if (count < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				break;
			}
			if (errno == EINTR) {
				continue;
			}
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge][control] read failed err=%s\n", strerror(errno));
			return -1;
		}
		if (count == 0) {
			if (fanout_controller != NULL) return -1;
			break;
		}
		for (ssize_t i = 0; i < count; ++i) {
			unsigned char value = input[i];
			if (value == '\n') {
				if (control->pending_size > 0 && control->pending[0] == '{') {
					video_broker_worker_fanout_command_t command;
					video_broker_worker_fanout_receipt_t receipt;
					int controller_state;
					if (fanout_controller == NULL ||
					    control->pending_size + 1 > VIDEO_BROKER_WORKER_FANOUT_MAX_LINE_BYTES) {
						return -1;
					}
					control->pending[control->pending_size] = '\n';
					if (video_broker_worker_fanout_decode_command(
							control->pending, control->pending_size + 1, &command) != 0) {
						return -1;
					}
					controller_state = video_broker_worker_fanout_controller_handle(
						fanout_controller, &command, &receipt);
					if (video_broker_rdkx5_encoder_resources_quarantined()) return -1;
					if (controller_state == VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY &&
					    video_broker_worker_fanout_write_receipt(stderr, &receipt) != 0) {
						return -1;
					}
				} else {
					control->pending[control->pending_size] = '\0';
					changed |= runtime_control_parse_line(control, control->pending, capture_fps);
				}
				control->pending_size = 0;
				continue;
			}
			if (value == '\r') {
				if (control->pending_size > 0 && control->pending[0] == '{') return -1;
				continue;
			}
			if (control->pending_size + 1 >= sizeof(control->pending)) {
				if (control->pending_size > 0 && control->pending[0] == '{') return -1;
				control->pending_size = 0;
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge][control] rejected oversized command\n");
				continue;
			}
			control->pending[control->pending_size++] = (char)value;
		}
	}
	return changed;
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

static void nv12_resize_oriented(
	const unsigned char *src,
	int src_w,
	int src_h,
	unsigned char *dst,
	int dst_w,
	int dst_h,
	video_broker_video_orientation_t orientation,
	video_broker_nv12_aspect_policy_t aspect_policy
) {
	video_broker_nv12_resize_nearest_oriented_aspect(
		src, src_w, src_h, dst, dst_w, dst_h, orientation, aspect_policy);
}

static int yuyv_to_nv12(const unsigned char *src, size_t src_size, unsigned char *dst, int width, int height) {
	size_t expected = (size_t)width * (size_t)height * 2;
	unsigned char *y_plane = dst;
	unsigned char *uv_plane = dst + (width * height);

	if (src_size < expected) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] short yuyv frame size=%zu expected=%zu\n", src_size, expected);
		return -1;
	}

	for (int y = 0; y < height; ++y) {
		const unsigned char *in = src + (size_t)y * (size_t)width * 2;
		unsigned char *out_y = y_plane + (size_t)y * (size_t)width;
		for (int x = 0; x < width; x += 2) {
			out_y[x] = in[0];
			out_y[x + 1] = in[2];
			in += 4;
		}
	}

	for (int y = 0; y < height; y += 2) {
		const unsigned char *in = src + (size_t)y * (size_t)width * 2;
		unsigned char *out_uv = uv_plane + (size_t)(y / 2) * (size_t)width;
		for (int x = 0; x < width; x += 2) {
			out_uv[x] = in[1];
			out_uv[x + 1] = in[3];
			in += 4;
		}
	}

	return 0;
}

static void planar_yuv_to_nv12(
	const unsigned char *y_plane,
	const unsigned char *u_plane,
	const unsigned char *v_plane,
	int y_stride,
	int u_stride,
	int v_stride,
	int u_width,
	int u_height,
	int v_width,
	int v_height,
	unsigned char *dst,
	int width,
	int height
) {
	unsigned char *dst_y = dst;
	unsigned char *dst_uv = dst + (width * height);

	for (int y = 0; y < height; ++y) {
		memcpy(dst_y + (size_t)y * (size_t)width, y_plane + (size_t)y * (size_t)y_stride, (size_t)width);
	}

	for (int y = 0; y < height / 2; ++y) {
		int uy = (int)(((int64_t)y * u_height) / (height / 2));
		int vy = (int)(((int64_t)y * v_height) / (height / 2));
		const unsigned char *u_row = u_plane + (size_t)uy * (size_t)u_stride;
		const unsigned char *v_row = v_plane + (size_t)vy * (size_t)v_stride;
		unsigned char *uv_row = dst_uv + (size_t)y * (size_t)width;
		for (int x = 0; x < width / 2; ++x) {
			int ux = (int)(((int64_t)x * u_width) / (width / 2));
			int vx = (int)(((int64_t)x * v_width) / (width / 2));
			uv_row[x * 2] = u_row[ux];
			uv_row[x * 2 + 1] = v_row[vx];
		}
	}
}

static int mjpeg_to_nv12(tjhandle jpeg, const unsigned char *src, size_t src_size, unsigned char *dst, unsigned char *scratch, int width, int height) {
	int jpeg_w = 0;
	int jpeg_h = 0;
	int subsamp = 0;
	int colorspace = 0;
	int y_stride = width;
	int u_width;
	int u_height;
	int v_width;
	int v_height;
	int strides[3];
	unsigned char *planes[3];

	if (tjDecompressHeader3(jpeg, src, (unsigned long)src_size, &jpeg_w, &jpeg_h, &subsamp, &colorspace) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] jpeg header failed: %s\n", tjGetErrorStr2(jpeg));
		return -1;
	}
	if (jpeg_w != width || jpeg_h != height) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] jpeg size mismatch got=%dx%d expected=%dx%d\n", jpeg_w, jpeg_h, width, height);
		return -1;
	}
	if (subsamp == TJSAMP_GRAY) {
		unsigned char *gray_planes[3] = { dst, NULL, NULL };
		int gray_strides[3] = { width, 0, 0 };
		if (tjDecompressToYUVPlanes(jpeg, src, (unsigned long)src_size, gray_planes, width, gray_strides, height, TJFLAG_FASTDCT) != 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] jpeg yuv decode failed: %s\n", tjGetErrorStr2(jpeg));
			return -1;
		}
		memset(dst + (width * height), 128, (size_t)width * (size_t)height / 2);
		return 0;
	}

	u_width = tjPlaneWidth(1, width, subsamp);
	u_height = tjPlaneHeight(1, height, subsamp);
	v_width = tjPlaneWidth(2, width, subsamp);
	v_height = tjPlaneHeight(2, height, subsamp);
	if (u_width <= 0 || u_height <= 0 || v_width <= 0 || v_height <= 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] unsupported jpeg subsampling=%d\n", subsamp);
		return -1;
	}

	planes[0] = scratch;
	planes[1] = scratch + (size_t)width * (size_t)height;
	planes[2] = planes[1] + (size_t)u_width * (size_t)u_height;
	strides[0] = y_stride;
	strides[1] = u_width;
	strides[2] = v_width;
	if (tjDecompressToYUVPlanes(jpeg, src, (unsigned long)src_size, planes, width, strides, height, TJFLAG_FASTDCT) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] jpeg yuv decode failed: %s\n", tjGetErrorStr2(jpeg));
		return -1;
	}

	planar_yuv_to_nv12(
		planes[0], planes[1], planes[2],
		strides[0], strides[1], strides[2],
		u_width, u_height, v_width, v_height,
		dst, width, height);
	return 0;
}

static int decode_capture_frame_to_nv12(
	tjhandle jpeg,
	video_broker_rdkx5_decoder_t *decoder,
	const unsigned char *src,
	size_t src_size,
	uint32_t fmt,
	unsigned char *nv12,
	unsigned char *scratch,
	int width,
	int height,
	vb_capture_origin input_origin,
	vb_capture_origin *decoded_origin
) {
	*decoded_origin = (vb_capture_origin){0,0};
	if (fmt == V4L2_PIX_FMT_YUYV) {
		int rc = yuyv_to_nv12(src, src_size, nv12, width, height);
		if (!rc) *decoded_origin = input_origin;
		return rc;
	}
	if (fmt == V4L2_PIX_FMT_NV12) {
		size_t expected = FRAME_BUFFER_SIZE(width, height);
		if (src_size < expected) {
			return -1;
		}
		memcpy(nv12, src, expected);
		*decoded_origin = input_origin;
		return 0;
	}
	if (fmt == V4L2_PIX_FMT_MJPEG) {
		if (jpeg != NULL) {
			int rc = mjpeg_to_nv12(jpeg, src, src_size, nv12, scratch, width, height);
			if (!rc) *decoded_origin = input_origin;
			return rc;
		}
		{
			int decode_rc = video_broker_rdkx5_decoder_decode(
				decoder, src, src_size, nv12, FRAME_BUFFER_SIZE(width, height),
				kDecoderOutputPollTimeoutMs, input_origin, decoded_origin);
			if (decode_rc > 0) {
				return 0;
			}
			return decode_rc == 0 ? 1 : -1;
		}
	}
	if (fmt == V4L2_PIX_FMT_H264 || fmt == V4L2_PIX_FMT_HEVC) {
		int decode_rc = video_broker_rdkx5_decoder_decode(
			decoder, src, src_size, nv12, FRAME_BUFFER_SIZE(width, height),
			kDecoderOutputPollTimeoutMs, input_origin, decoded_origin);
		if (decode_rc > 0) {
			return 0;
		}
		return decode_rc == 0 ? 1 : -1;
	}
	fprintf(stderr, "[rdkx5_v4l2_mux_bridge] unsupported capture format=%s\n", format_name(fmt));
	return -1;
}

static bool is_compressed_h26x(uint32_t fmt) {
	return fmt == V4L2_PIX_FMT_H264 || fmt == V4L2_PIX_FMT_HEVC;
}

static bool is_annexb_access_unit(const unsigned char *data, size_t size) {
	if (data == NULL || size < 4) {
		return false;
	}
	for (size_t i = 0; i + 3 < size && i < 64; ++i) {
		if (data[i] == 0 && data[i + 1] == 0 &&
			(data[i + 2] == 1 || (data[i + 2] == 0 && data[i + 3] == 1))) {
			return true;
		}
	}
	return false;
}

static void camera_admission_init(camera_admission_t *admission, uint32_t codec, int expected_fps, uint64_t maximum_gap_us) {
	memset(admission, 0, sizeof(*admission));
	admission->codec = codec;
	expected_fps = expected_fps > 0 ? expected_fps : kDefaultFPS;
	video_broker_stable_cadence_init(
		&admission->decoded_cadence, expected_fps, 3, maximum_gap_us);
	admission->started_us = monotonic_us();
}

static void camera_admission_observe_access_unit(
	camera_admission_t *admission,
	const unsigned char *data,
	size_t size
) {
	if (admission == NULL || data == NULL || admission->admitted) {
		return;
	}
	for (size_t i = 0; i + 4 < size; ++i) {
		size_t start_code_len = 0;
		if (data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1) {
			start_code_len = 3;
		} else if (i + 5 < size && data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 0 && data[i + 3] == 1) {
			start_code_len = 4;
		}
		if (start_code_len == 0 || i + start_code_len >= size) {
			continue;
		}
		if (admission->codec == V4L2_PIX_FMT_HEVC) {
			unsigned int nal_type = (data[i + start_code_len] >> 1) & 0x3f;
			admission->seen_vps |= nal_type == 32;
			admission->seen_sps |= nal_type == 33;
			admission->seen_pps |= nal_type == 34;
			admission->seen_keyframe |= nal_type >= 16 && nal_type <= 23;
		} else if (admission->codec == V4L2_PIX_FMT_H264) {
			unsigned int nal_type = data[i + start_code_len] & 0x1f;
			admission->seen_sps |= nal_type == 7;
			admission->seen_pps |= nal_type == 8;
			admission->seen_keyframe |= nal_type == 5;
		}
	}
}

static bool camera_admission_codec_ready(const camera_admission_t *admission) {
	if (admission->codec == V4L2_PIX_FMT_HEVC) {
		return admission->seen_vps && admission->seen_sps && admission->seen_pps && admission->seen_keyframe;
	}
	if (admission->codec == V4L2_PIX_FMT_H264) {
		return admission->seen_sps && admission->seen_pps && admission->seen_keyframe;
	}
	return true;
}

static bool camera_admission_observe_decoded_frame(camera_admission_t *admission, uint64_t now_us) {
	if (admission == NULL || now_us == 0) {
		return false;
	}
	bool available = video_broker_stable_cadence_observe(&admission->decoded_cadence, now_us);
	if (admission->admitted) {
		return true;
	}
	if (!available ||
	    !camera_admission_codec_ready(admission)) {
		return false;
	}
	admission->admitted = true;
	fprintf(stderr,
		"[rdkx5_v4l2_mux_bridge][camera-admission] status=admitted codec=%s framing=%s config=%s keyframe=%s frames=%llu observed_fps=%.2Lf effective_fps=%d settling_resets=%llu requested_fps=%d performance=%s\n",
		format_name(admission->codec),
		is_compressed_h26x(admission->codec) ? "annexb" : "frame",
		admission->codec == V4L2_PIX_FMT_HEVC ? "vps+sps+pps" :
			(admission->codec == V4L2_PIX_FMT_H264 ? "sps+pps" : "not_applicable"),
		is_compressed_h26x(admission->codec) ? "present" : "not_applicable",
		(unsigned long long)admission->decoded_cadence.frames,
		admission->decoded_cadence.observed_fps,
		admission->decoded_cadence.effective_fps,
		(unsigned long long)admission->decoded_cadence.settling_resets,
		admission->decoded_cadence.expected_fps,
		video_broker_stable_cadence_meets_expected(&admission->decoded_cadence) ? "on_target" : "reduced");
	return true;
}

static bool camera_admission_timed_out(const camera_admission_t *admission, uint64_t now_us) {
	return admission != NULL && !admission->admitted && admission->started_us > 0 &&
		now_us > admission->started_us && now_us - admission->started_us >= kCameraAdmissionTimeoutUs;
}

static quality_measure_t quality_measure_nv12_luma(
	const unsigned char *nv12,
	int width,
	int height,
	int stride
) {
	if (stride <= 0) {
		stride = kDefaultQualitySampleStride;
	}
	return video_broker_nv12_quality_measure(nv12, width, height, stride);
}

static bool quality_measure_is_informative(const quality_measure_t *measure) {
	return video_broker_nv12_quality_is_informative(measure);
}

static void quality_monitor_write_state(
	const bridge_config_t *cfg,
	uint64_t frame,
	int stride,
	const quality_measure_t *source,
	int source_width,
	int source_height,
	const quality_measure_t *processed,
	int processed_width,
	int processed_height,
	const quality_measure_t *analysis,
	int analysis_width,
	int analysis_height
) {
	if (cfg == NULL || source == NULL || processed == NULL || analysis == NULL) {
		return;
	}
	if (cfg->quality_monitor_state_path == NULL || cfg->quality_monitor_state_path[0] == '\0') {
		return;
	}
	if (video_broker_nv12_quality_write_state(
		cfg->quality_monitor_state_path,
		frame,
		stride,
		source,
		source_width,
		source_height,
		processed,
		processed_width,
		processed_height,
		analysis,
		analysis_width,
		analysis_height,
		format_name(cfg->input_format),
		video_broker_video_orientation_name(cfg->video_orientation)) != 0) {
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge][quality] state commit failed path=%s err=%s\n",
			cfg->quality_monitor_state_path,
			strerror(errno));
	}
}

static bool quality_monitor_begin(
	quality_monitor_t *monitor,
	const bridge_config_t *cfg,
	const unsigned char *source_nv12,
	int source_width,
	int source_height,
	quality_observation_t *observation
) {
	int stride;

	if (observation != NULL) {
		memset(observation, 0, sizeof(*observation));
	}
	if (monitor == NULL || cfg == NULL || observation == NULL || source_nv12 == NULL ||
	    !cfg->quality_monitor_enabled) {
		return false;
	}
	monitor->frames++;
	uint64_t now_us = monotonic_us();
	if (!video_broker_frame_observation_due(monitor->frames, cfg->quality_monitor_report_every,
		cfg->fps, now_us, monitor->last_sample_us)) {
		return false;
	}
	stride = cfg->quality_monitor_sample_stride;
	if (stride <= 0) {
		stride = kDefaultQualitySampleStride;
	}
	observation->source = quality_measure_nv12_luma(source_nv12, source_width, source_height, stride);
	if (observation->source.samples == 0) {
		return false;
	}
	observation->active = true;
	observation->frame = monitor->frames;
	observation->stride = stride;
	observation->source_width = source_width;
	observation->source_height = source_height;
	monitor->last_sample_us = now_us;
	return true;
}

static bool quality_monitor_finish(
	quality_monitor_t *monitor,
	const bridge_config_t *cfg,
	const quality_observation_t *observation,
	const unsigned char *processed_nv12,
	int processed_width,
	int processed_height,
	const unsigned char *analysis_nv12,
	int analysis_width,
	int analysis_height
) {
	quality_measure_t processed;
	quality_measure_t analysis;
	uint64_t now_us;

	if (monitor == NULL || cfg == NULL || observation == NULL || !observation->active ||
	    processed_nv12 == NULL || analysis_nv12 == NULL) {
		return false;
	}
	processed = quality_measure_nv12_luma(
		processed_nv12, processed_width, processed_height, observation->stride);
	analysis = quality_measure_nv12_luma(
		analysis_nv12, analysis_width, analysis_height, observation->stride);
	if (processed.samples == 0 || analysis.samples == 0) {
		return false;
	}
	now_us = monotonic_us();
	if (monitor->last_log_us == 0 || now_us - monitor->last_log_us >= kDiagnosticLogIntervalUs) {
		monitor->last_log_us = now_us;
		vb_diagnostic_pipe_printf(&g_telemetry,
			"[rdkx5_v4l2_mux_bridge][quality] event=optical_quality_summary frame=%llu sample_stride=%d source=%dx%d source_luma=%.1Lf source_black_ratio=%.5Lf source_white_ratio=%.5Lf source_signal_informative=%s processed=%dx%d processed_luma=%.1Lf processed_black_ratio=%.5Lf processed_white_ratio=%.5Lf processed_signal_informative=%s analysis=%dx%d analysis_luma=%.1Lf analysis_lap_var=%.1Lf analysis_edge=%.1Lf analysis_black_ratio=%.5Lf analysis_white_ratio=%.5Lf analysis_signal_informative=%s format=%s orientation=%s\n",
			(unsigned long long)observation->frame,
			observation->stride,
			observation->source_width,
			observation->source_height,
			observation->source.luma,
			observation->source.black_ratio,
			observation->source.white_ratio,
			quality_measure_is_informative(&observation->source) ? "true" : "false",
			processed_width,
			processed_height,
			processed.luma,
			processed.black_ratio,
			processed.white_ratio,
			quality_measure_is_informative(&processed) ? "true" : "false",
			analysis_width,
			analysis_height,
			analysis.luma,
			analysis.lap_var,
			analysis.edge,
			analysis.black_ratio,
			analysis.white_ratio,
			quality_measure_is_informative(&analysis) ? "true" : "false",
			format_name(cfg->input_format),
			video_broker_video_orientation_name(cfg->video_orientation));
	}
	quality_monitor_write_state(
		cfg,
		observation->frame,
		observation->stride,
		&observation->source,
		observation->source_width,
		observation->source_height,
		&processed,
		processed_width,
		processed_height,
		&analysis,
		analysis_width,
		analysis_height);
	return true;
}

static int prepare_v4l2_capture(const bridge_config_t *cfg, v4l2_capture_t *cap) {
	struct v4l2_capability capability;
	struct v4l2_format format;
	struct v4l2_streamparm parm;
	struct v4l2_requestbuffers req;

	memset(cap, 0, sizeof(*cap));
	cap->fd = -1;
	cap->fd = open(cfg->device, O_RDWR | O_NONBLOCK, 0);
	if (cap->fd < 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] open failed device=%s err=%s\n", cfg->device, strerror(errno));
		return -1;
	}

	memset(&capability, 0, sizeof(capability));
	if (xioctl(cap->fd, VIDIOC_QUERYCAP, &capability) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] VIDIOC_QUERYCAP failed err=%s\n", strerror(errno));
		return -1;
	}
	if ((capability.capabilities & V4L2_CAP_VIDEO_CAPTURE) == 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] device does not support video capture\n");
		return -1;
	}
	if ((capability.capabilities & V4L2_CAP_STREAMING) == 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] device does not support streaming I/O\n");
		return -1;
	}

	memset(&format, 0, sizeof(format));
	format.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
	format.fmt.pix.width = (uint32_t)cfg->capture_width;
	format.fmt.pix.height = (uint32_t)cfg->capture_height;
	format.fmt.pix.pixelformat = cfg->input_format;
	format.fmt.pix.field = V4L2_FIELD_NONE;
	if (xioctl(cap->fd, VIDIOC_S_FMT, &format) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] VIDIOC_S_FMT failed format=%s %dx%d err=%s\n",
			format_name(cfg->input_format), cfg->capture_width, cfg->capture_height, strerror(errno));
		return -1;
	}
	cap->width = (int)format.fmt.pix.width;
	cap->height = (int)format.fmt.pix.height;
	cap->pixel_format = format.fmt.pix.pixelformat;
	if (cap->pixel_format != cfg->input_format) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] requested format=%s got_fourcc=0x%08x\n",
			format_name(cfg->input_format), cap->pixel_format);
		return -1;
	}

	memset(&parm, 0, sizeof(parm));
	parm.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
	parm.parm.capture.timeperframe.numerator = 1;
	parm.parm.capture.timeperframe.denominator = (uint32_t)cfg->fps;
	(void)xioctl(cap->fd, VIDIOC_S_PARM, &parm);
	cap->fps = cfg->fps;
	memset(&parm, 0, sizeof(parm));
	parm.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
	if (xioctl(cap->fd, VIDIOC_G_PARM, &parm) == 0 &&
	    parm.parm.capture.timeperframe.numerator > 0 &&
	    parm.parm.capture.timeperframe.denominator > 0) {
		uint32_t numerator = parm.parm.capture.timeperframe.numerator;
		uint32_t denominator = parm.parm.capture.timeperframe.denominator;
		int actual_fps = (int)((denominator + (numerator / 2U)) / numerator);
		if (actual_fps > 0) {
			cap->fps = actual_fps;
		}
	}

	memset(&req, 0, sizeof(req));
	req.count = kDefaultBufferCount;
	req.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
	req.memory = V4L2_MEMORY_MMAP;
	if (xioctl(cap->fd, VIDIOC_REQBUFS, &req) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] VIDIOC_REQBUFS failed err=%s\n", strerror(errno));
		return -1;
	}
	if (req.count < 2) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] insufficient mmap buffers count=%u\n", req.count);
		return -1;
	}
	cap->buffers = (mmap_buffer_t *)calloc(req.count, sizeof(*cap->buffers));
	if (cap->buffers == NULL) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] buffer metadata allocation failed\n");
		return -1;
	}
	cap->buffer_count = req.count;

	for (unsigned int i = 0; i < cap->buffer_count; ++i) {
		struct v4l2_buffer buf;
		memset(&buf, 0, sizeof(buf));
		buf.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
		buf.memory = V4L2_MEMORY_MMAP;
		buf.index = i;
		if (xioctl(cap->fd, VIDIOC_QUERYBUF, &buf) != 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] VIDIOC_QUERYBUF failed index=%u err=%s\n", i, strerror(errno));
			return -1;
		}
		cap->buffers[i].length = buf.length;
		cap->buffers[i].start = mmap(NULL, buf.length, PROT_READ | PROT_WRITE, MAP_SHARED, cap->fd, buf.m.offset);
		if (cap->buffers[i].start == MAP_FAILED) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] mmap failed index=%u err=%s\n", i, strerror(errno));
			return -1;
		}
	}
	return 0;
}

static int start_v4l2_capture(v4l2_capture_t *cap) {
	if (!g_running || cap == NULL || cap->fd < 0 || cap->buffers == NULL ||
	    cap->buffer_count < 2 || cap->streaming) {
		return -1;
	}
	for (unsigned int i = 0; i < cap->buffer_count; ++i) {
		struct v4l2_buffer buf;
		memset(&buf, 0, sizeof(buf));
		buf.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
		buf.memory = V4L2_MEMORY_MMAP;
		buf.index = i;
		if (xioctl(cap->fd, VIDIOC_QBUF, &buf) != 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] VIDIOC_QBUF failed index=%u err=%s\n", i, strerror(errno));
			return -1;
		}
	}
	if (!g_running) {
		return -1;
	}

	enum v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
	if (xioctl(cap->fd, VIDIOC_STREAMON, &type) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] VIDIOC_STREAMON failed err=%s\n", strerror(errno));
		return -1;
	}
	cap->streaming = true;
	return 0;
}

static bool uvc_start_reprepare_allowed(int error_code) {
	return error_code == EIO || error_code == EPROTO;
}

static int wait_uvc_start_reprepare_quiet(void) {
	struct timespec request;
	struct timespec remaining;

	request.tv_sec = kUVCStartReprepareQuietMs / 1000;
	request.tv_nsec = (long)(kUVCStartReprepareQuietMs % 1000) * 1000000L;
	while (g_running && nanosleep(&request, &remaining) != 0) {
		if (errno != EINTR) {
			return -1;
		}
		request = remaining;
	}
	return g_running ? 0 : -1;
}

static bool capture_layout_matches(
	const v4l2_capture_t *cap,
	int width,
	int height,
	uint32_t pixel_format,
	int fps,
	size_t mjpeg_copy_capacity
) {
	if (cap == NULL || cap->width != width || cap->height != height ||
	    cap->pixel_format != pixel_format || cap->fps != fps) {
		return false;
	}
	if (pixel_format != V4L2_PIX_FMT_MJPEG) {
		return true;
	}
	for (unsigned int index = 0; index < cap->buffer_count; ++index) {
		if (cap->buffers[index].length > mjpeg_copy_capacity) {
			return false;
		}
	}
	return true;
}

static int wait_for_capture_frame(int fd, int timeout_ms) {
	for (;;) {
		fd_set fds;
		struct timeval tv;
		int rc;
		FD_ZERO(&fds);
		FD_SET(fd, &fds);
		if (timeout_ms <= 0) {
			timeout_ms = kDefaultCaptureSelectTimeoutMs;
		}
		tv.tv_sec = timeout_ms / 1000;
		tv.tv_usec = (timeout_ms % 1000) * 1000;
		rc = select(fd + 1, &fds, NULL, NULL, &tv);
		if (rc > 0) {
			return 1;
		}
		if (rc == 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] capture select timeout\n");
			return 0;
		}
		if (errno != EINTR) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] capture select failed err=%s\n", strerror(errno));
			return -1;
		}
		if (!g_running) {
			return -1;
		}
	}
}

static int close_v4l2_capture(v4l2_capture_t *cap) {
	int streamoff_attempted = 0;
	int streamoff_rc = 0;
	int streamoff_errno = 0;
	int close_rc = 0;
	int close_errno = 0;
	int capture_fd;
	unsigned int buffer_count;
	unsigned int unmapped = 0;
	unsigned int unmap_failed = 0;

	if (cap == NULL) {
		return 0;
	}
	capture_fd = cap->fd;
	buffer_count = cap->buffer_count;
	if (cap->fd >= 0 && cap->streaming) {
		enum v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
		streamoff_attempted = 1;
		streamoff_rc = xioctl(cap->fd, VIDIOC_STREAMOFF, &type);
		if (streamoff_rc != 0) {
			streamoff_errno = errno;
		}
	}
	if (cap->buffers != NULL) {
		for (unsigned int i = 0; i < cap->buffer_count; ++i) {
			if (cap->buffers[i].start != NULL && cap->buffers[i].start != MAP_FAILED) {
				if (munmap(cap->buffers[i].start, cap->buffers[i].length) == 0) {
					unmapped++;
				} else {
					unmap_failed++;
				}
			}
		}
		free(cap->buffers);
	}
	if (cap->fd >= 0) {
		close_rc = close(cap->fd);
		if (close_rc != 0) {
			close_errno = errno;
		}
	}
	/* ENODEV is the V4L2 removed-device outcome, not a failed DMA release.
	 * The disconnected descriptor still has to be closed and every mapped
	 * buffer unmapped successfully. Do not excuse EIO or other ioctl errors,
	 * and do not change the independent decoder/encoder release checks. */
	const bool streamoff_released = streamoff_rc == 0 || streamoff_errno == ENODEV;
	const bool release_confirmed = streamoff_released && close_rc == 0 && unmap_failed == 0;
	const char *streamoff_outcome = !streamoff_attempted ? "not_streaming" :
		streamoff_rc == 0 ? "stopped" : streamoff_errno == ENODEV ? "device_removed" : "failed";
	fprintf(
		stderr,
		"[rdkx5_v4l2_mux_bridge][capture-lifecycle] event=uvc_capture_closed signal=%d fd=%d streamoff_attempted=%d streamoff_rc=%d streamoff_errno=%d buffers=%u unmapped=%u close_rc=%d close_errno=%d unmap_failed=%u release=%s streamoff_outcome=%s\n",
		(int)g_stop_signal,
		capture_fd,
		streamoff_attempted,
		streamoff_rc,
		streamoff_errno,
		buffer_count,
		unmapped,
		close_rc,
		close_errno,
		unmap_failed,
		release_confirmed ? "confirmed" : "unconfirmed",
		streamoff_outcome);
	memset(cap, 0, sizeof(*cap));
	cap->fd = -1;
	return release_confirmed ? 0 : -1;
}

static void v4l2_role_move(v4l2_role_runtime_t *target, v4l2_role_runtime_t *source) {
	*target = *source;
	memset(source, 0, sizeof(*source));
}

static int v4l2_role_release_candidate_tone(v4l2_role_runtime_t *role) {
	if (role == NULL) return 0;
	if (video_broker_tone_accelerator_destroy(&role->candidate_tone_accelerator) != 0) return -1;
	video_broker_nv12_tone_mapper_destroy(&role->candidate_tone_mapper);
	role->candidate_tone_initialized = false;
	return 0;
}

static int v4l2_role_destroy(v4l2_role_runtime_t *role) {
	if (role == NULL) return 0;
	/* init may fail after acquiring SDK resources but before the role's
	 * initialized flag is set. The object itself owns that partial state. */
	if (role->encoder != NULL &&
	    video_broker_rdkx5_persistent_encoder_destroy(role->encoder) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge][fanout] event=release_failed role=%s"
			" branch_epoch=%llu resources_released=false action=retire_process\n",
			video_broker_worker_role_name(role->role), (unsigned long long)role->branch_epoch);
		return -1;
	}
	if (v4l2_role_release_candidate_tone(role) != 0) return -1;
	free(role->encoder);
	free(role->stream);
	free(role->frame);
	memset(role, 0, sizeof(*role));
	return 0;
}

static video_broker_archive_content_t v4l2_profile_archive_content(
	const video_broker_worker_branch_profile_t *profile) {
	if (strcmp(profile->content, "source") == 0) return VIDEO_BROKER_ARCHIVE_SOURCE;
	if (strcmp(profile->content, "processed") == 0) return VIDEO_BROKER_ARCHIVE_PROCESSED;
	return VIDEO_BROKER_ARCHIVE_ADAPTIVE;
}

static video_broker_rdkx5_rate_control_profile_t v4l2_profile_rate_control(
	video_broker_worker_role_t role,
	const video_broker_worker_branch_profile_t *profile) {
	if (role == VIDEO_BROKER_WORKER_ROLE_LIVE) {
		return VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR;
	}
	return strcmp(profile->rate_control, "avbr") == 0 ?
		VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR :
		VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_CBR;
}

static video_broker_rdkx5_encoder_codec_t v4l2_profile_encoder_codec(
	const video_broker_worker_branch_profile_t *profile) {
	return strcmp(profile->codec, "h264") == 0 ?
		VIDEO_BROKER_RDKX5_ENCODER_H264 : VIDEO_BROKER_RDKX5_ENCODER_H265;
}

static bool v4l2_profile_native_passthrough(
	video_broker_worker_role_t role,
	const video_broker_worker_branch_profile_t *profile,
	const bridge_config_t *cfg,
	const v4l2_capture_t *capture) {
	bool codec_matches;
	int capture_fps;
	if (role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS || profile == NULL || cfg == NULL ||
	    capture == NULL || !is_compressed_h26x(capture->pixel_format)) {
		return false;
	}
	codec_matches = (capture->pixel_format == V4L2_PIX_FMT_HEVC &&
		strcmp(profile->codec, "h265") == 0) ||
		(capture->pixel_format == V4L2_PIX_FMT_H264 && strcmp(profile->codec, "h264") == 0);
	capture_fps = capture->fps > 0 ? capture->fps : cfg->fps;
	if (!codec_matches || profile->width != capture->width || profile->height != capture->height ||
	    profile->fps != capture_fps || cfg->video_orientation != VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL) {
		return false;
	}
	if (role == VIDEO_BROKER_WORKER_ROLE_LIVE) return !cfg->tone_mapping_enabled;
	return video_broker_archive_uses_native(
		v4l2_profile_archive_content(profile), true, cfg->tone_mapping_enabled) &&
		strcmp(profile->rate_control, "avbr") != 0;
}

static void v4l2_role_profile_from_config(
	v4l2_role_runtime_t *role,
	video_broker_worker_role_t role_id,
	const bridge_config_t *cfg,
	const v4l2_capture_t *capture) {
	video_broker_worker_branch_profile_t *profile;
	memset(role, 0, sizeof(*role));
	role->role = role_id;
	role->branch_epoch = 1;
	profile = &role->profile;
	if (role_id == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) {
		memcpy(profile->pixel_format, "nv12", sizeof("nv12"));
		profile->width = cfg->sub_width;
		profile->height = cfg->sub_height;
		profile->fps = cfg->sub_fps;
	} else if (role_id == VIDEO_BROKER_WORKER_ROLE_LIVE) {
		memcpy(profile->codec, cfg->live_codec == SP_ENCODER_H264 ? "h264" : "h265", sizeof("h265"));
		memcpy(profile->content, "processed", sizeof("processed"));
		memcpy(profile->rate_control, "cbr", sizeof("cbr"));
		profile->width = cfg->output_width;
		profile->height = cfg->output_height;
		profile->fps = cfg->main_fps;
		profile->bitrate_kbps = cfg->live_encode_bitrate_kbps;
		role->delivery_active = cfg->main_active;
	} else {
		memcpy(profile->codec, cfg->archive_codec == SP_ENCODER_H264 ? "h264" : "h265", sizeof("h265"));
		memcpy(profile->content, video_broker_archive_content_name(cfg->archive_content),
			strlen(video_broker_archive_content_name(cfg->archive_content)) + 1);
		memcpy(profile->rate_control, archive_rate_control_name(cfg->archive_rate_control),
			strlen(archive_rate_control_name(cfg->archive_rate_control)) + 1);
		profile->width = cfg->archive_width;
		profile->height = cfg->archive_height;
		profile->fps = cfg->archive_fps;
		profile->bitrate_kbps = cfg->archive_encode_bitrate_kbps;
		role->delivery_active = true;
	}
	role->native_passthrough = v4l2_profile_native_passthrough(role_id, profile, cfg, capture);
}

static void v4l2_role_observe_bootstrap(
	v4l2_role_runtime_t *role,
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

static bool v4l2_role_evidence_ready(const v4l2_role_runtime_t *role) {
	if (role == NULL || role->frames == 0) return false;
	if (role->role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) return true;
	if (strcmp(role->profile.codec, "h265") == 0) {
		return role->seen_vps && role->seen_sps && role->seen_pps && role->seen_keyframe;
	}
	return role->seen_sps && role->seen_pps && role->seen_keyframe;
}

static int v4l2_candidate_encoder_index(
	const v4l2_fanout_backend_t *backend,
	video_broker_worker_role_t role) {
	int primary = role == VIDEO_BROKER_WORKER_ROLE_LIVE ? 0 : 1;
	const v4l2_role_runtime_t *active = backend->active[role];
	if (backend->active_present[role] && active->encoder_initialized &&
	    active->encoder != NULL && active->encoder->instance_index == primary) {
		return primary + 2;
	}
	return primary;
}

static bool v4l2_role_uses_tone(
	video_broker_worker_role_t role,
	const video_broker_worker_branch_profile_t *profile,
	const bridge_config_t *cfg) {
	return cfg->tone_mapping_enabled &&
		(role != VIDEO_BROKER_WORKER_ROLE_ARCHIVE || strcmp(profile->content, "source") != 0);
}

static int v4l2_candidate_init(
	v4l2_fanout_backend_t *backend,
	v4l2_role_runtime_t *role,
	video_broker_worker_role_t role_id,
	const video_broker_worker_branch_profile_t *profile,
	uint64_t branch_epoch) {
	bool uses_tone;
	if (backend == NULL || role == NULL || profile == NULL || branch_epoch == 0 ||
	    video_broker_rdkx5_encoder_resources_quarantined() ||
	    video_broker_tone_accelerator_resources_quarantined()) return -1;
	if (role->encoder != NULL || role->frame != NULL || role->stream != NULL ||
	    role->candidate_tone_initialized) return -1;
	memset(role, 0, sizeof(*role));
	role->role = role_id;
	role->profile = *profile;
	role->branch_epoch = branch_epoch;
	role->delivery_active = true;
	role->native_passthrough = v4l2_profile_native_passthrough(
		role_id, profile, backend->cfg, backend->capture);
	if (role->native_passthrough) return 0;
	role->frame_size = FRAME_BUFFER_SIZE(profile->width, profile->height);
	role->frame = allocate_frame_buffer(role->frame_size);
	if (role->frame == NULL) goto fail;
	uses_tone = v4l2_role_uses_tone(role_id, profile, backend->cfg);
	if (uses_tone) {
		if (!backend->control->optional_compute_admitted) goto fail;
		video_broker_nv12_tone_mapper_init(
			&role->candidate_tone_mapper,
			backend->cfg->tone_mapping_mode,
			backend->cfg->tone_mapping_target,
			backend->cfg->tone_mapping_hysteresis,
			backend->cfg->tone_mapping_grid_columns,
			backend->cfg->tone_mapping_grid_rows,
			backend->cfg->tone_mapping_statistics_hz,
			profile->fps,
			backend->cfg->tone_mapping_max_shadow_gain,
			backend->cfg->tone_mapping_max_highlight_reduction,
			backend->cfg->tone_mapping_temporal_alpha,
			backend->cfg->tone_mapping_noise_guard);
		role->candidate_tone_initialized = true;
		if (video_broker_tone_accelerator_init(
				&role->candidate_tone_accelerator,
				backend->cfg->tone_mapping_accelerator) != 0) {
			goto fail;
		}
	}
	if (role_id == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) return 0;
	role->stream_capacity = stream_buffer_capacity(role->frame_size);
	role->stream = malloc(role->stream_capacity + kStreamBufferGuard);
	role->encoder = calloc(1, sizeof(*role->encoder));
	if (role->stream == NULL || role->encoder == NULL ||
	    video_broker_rdkx5_persistent_encoder_init(
			role->encoder,
			v4l2_profile_encoder_codec(profile),
			v4l2_profile_rate_control(role_id, profile),
			v4l2_candidate_encoder_index(backend, role_id),
			profile->width,
			profile->height,
			profile->fps,
			profile->bitrate_kbps) != 0) {
		goto fail;
	}
	role->encoder_initialized = true;
	if (video_broker_rdkx5_persistent_encoder_set_control(
			role->encoder, true, profile->fps, profile->bitrate_kbps) < 0) {
		goto fail;
	}
	role->active_bitrate_kbps = profile->bitrate_kbps;
	return 0;

fail:
	v4l2_role_destroy(role);
	return -1;
}

static int v4l2_candidate_apply_tone(v4l2_role_runtime_t *role) {
	quality_measure_t measure;
	if (!role->candidate_tone_initialized) return 0;
	measure = quality_measure_nv12_luma(
		role->frame, role->profile.width, role->profile.height, kDefaultQualitySampleStride);
	video_broker_nv12_tone_mapper_set_signal_informative(
		&role->candidate_tone_mapper, quality_measure_is_informative(&measure));
	(void)video_broker_nv12_tone_mapper_update(
		&role->candidate_tone_mapper,
		role->frame, role->profile.width, role->profile.height, kDefaultQualitySampleStride);
	return video_broker_tone_accelerator_apply(
		&role->candidate_tone_accelerator,
		&role->candidate_tone_mapper,
		role->frame, role->profile.width, role->profile.height) < 0 ? -1 : 0;
}

static int v4l2_candidate_process(
	v4l2_fanout_backend_t *backend,
	v4l2_role_runtime_t *role) {
	video_broker_video_orientation_t orientation;
	uint64_t encoder_epoch = 0;
	int encoded_bitrate = 0;
	int encoded;
	if (backend == NULL || role == NULL || backend->scheduling_fps <= 0 ||
	    !video_broker_cadence_due(
		&role->emit_accumulator, role->profile.fps, backend->scheduling_fps)) {
		return 0;
	}
	if (role->native_passthrough) {
		if (backend->capture_access_unit == NULL || backend->capture_access_unit_size == 0 ||
		    !is_annexb_access_unit(
			backend->capture_access_unit, backend->capture_access_unit_size)) return 0;
		v4l2_role_observe_bootstrap(
			role, backend->capture_access_unit, backend->capture_access_unit_size);
		role->frames++;
		return 0;
	}
	if (!backend->decoded_frame_ready || backend->decoded_frame == NULL || role->frame == NULL) return 0;
	orientation = role->role == VIDEO_BROKER_WORKER_ROLE_ARCHIVE ?
		VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL : backend->cfg->video_orientation;
	nv12_resize_oriented(
		backend->decoded_frame,
		backend->capture->width,
		backend->capture->height,
		role->frame,
		role->profile.width,
		role->profile.height,
		orientation,
		backend->cfg->aspect_policy);
	if (v4l2_candidate_apply_tone(role) != 0) return -1;
	if (role->role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) {
		role->frames++;
		return 0;
	}
	arm_stream_guard(role->stream, role->stream_capacity);
	encoded = video_broker_rdkx5_persistent_encoder_encode(
		role->encoder,
		role->frame,
		role->frame_size,
		role->stream,
		role->stream_capacity,
		&encoder_epoch,
		&encoded_bitrate);
	if (encoded < 0 || (size_t)encoded > role->stream_capacity ||
	    !stream_guard_ok(role->stream, role->stream_capacity)) return -1;
	if (encoded > 0) {
		v4l2_role_observe_bootstrap(role, role->stream, (size_t)encoded);
		role->frames++;
	}
	return 0;
}

static void v4l2_fanout_reset(v4l2_fanout_backend_t *backend) {
	backend->change_count = 0;
	backend->cutover_completed = false;
	backend->candidate_failed = false;
	backend->wait_started_ms = 0;
	memset(backend->changes, 0, sizeof(backend->changes));
	memset(backend->active_failed, 0, sizeof(backend->active_failed));
}

static int v4l2_fanout_prepare(
	void *opaque,
	const video_broker_worker_branch_change_t *changes,
	size_t change_count) {
	v4l2_fanout_backend_t *backend = opaque;
	if (backend == NULL || changes == NULL || change_count == 0 ||
	    change_count > VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES || backend->change_count != 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	     role <= VIDEO_BROKER_WORKER_ROLE_LIVE; ++role) {
		if (backend->candidate_present[role] || backend->incumbent_present[role]) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
	}
	for (size_t index = 0; index < change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &changes[index];
		if ((change->action == VIDEO_BROKER_WORKER_ACTION_ATTACH &&
		     backend->active_present[change->role]) ||
		    ((change->action == VIDEO_BROKER_WORKER_ACTION_DETACH ||
		      change->action == VIDEO_BROKER_WORKER_ACTION_REPLACE) &&
		     !backend->active_present[change->role])) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
		if (change->action != VIDEO_BROKER_WORKER_ACTION_DETACH &&
		    (change->profile.fps > (backend->capture->fps > 0 ?
			backend->capture->fps : backend->cfg->fps) ||
		     (backend->cfg->aspect_policy == VIDEO_BROKER_NV12_ASPECT_EXACT &&
		      !video_aspect_matches(
			backend->capture->width, backend->capture->height,
			change->profile.width, change->profile.height)))) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
	}
	/* Sampling runs only in emit_sub. Keep its configured intent when analysis
	 * is detached, so a later attach resumes it without reopening capture. */
	memcpy(backend->changes, changes, change_count * sizeof(changes[0]));
	backend->change_count = change_count;
	backend->cutover_completed = false;
	backend->candidate_failed = false;
	backend->wait_started_ms = 0;
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int v4l2_fanout_warmup(void *opaque) {
	v4l2_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		uint64_t epoch;
		if (change->action == VIDEO_BROKER_WORKER_ACTION_DETACH ||
		    backend->candidate_present[change->role]) continue;
		epoch = backend->next_epoch[change->role] + 1;
		if (epoch == 0 || v4l2_candidate_init(
				backend, &backend->candidate[change->role], change->role,
				&change->profile, epoch) != 0) {
			/* A failed candidate can still own an initialized/failed SDK object.
			 * Keep it discoverable by abort/cleanup instead of reporting absence. */
			v4l2_role_runtime_t *candidate = &backend->candidate[change->role];
			backend->candidate_present[change->role] = candidate->encoder != NULL ||
				candidate->frame != NULL || candidate->stream != NULL || candidate->candidate_tone_initialized;
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
		backend->candidate_present[change->role] = true;
	}
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int v4l2_fanout_admit(void *opaque) {
	v4l2_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		if (change->action != VIDEO_BROKER_WORKER_ACTION_DETACH &&
		    !backend->candidate_present[change->role]) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
	}
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static bool v4l2_fanout_candidates_ready(const v4l2_fanout_backend_t *backend) {
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		if (change->action != VIDEO_BROKER_WORKER_ACTION_DETACH &&
		    (!backend->candidate_present[change->role] ||
		     !v4l2_role_evidence_ready(&backend->candidate[change->role]))) return false;
	}
	return true;
}

static int v4l2_fanout_first_frame(
	void *opaque,
	video_broker_worker_branch_receipt_t *branches,
	size_t *branch_count) {
	v4l2_fanout_backend_t *backend = opaque;
	uint64_t now_ms = monotonic_us() / 1000U;
	size_t output_count = 0;
	if (backend == NULL || branches == NULL || branch_count == NULL || now_ms == 0 ||
	    *branch_count < backend->change_count) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	if (backend->candidate_failed) return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	if (!v4l2_fanout_candidates_ready(backend)) {
		if (backend->wait_started_ms == 0) backend->wait_started_ms = now_ms;
		if (now_ms - backend->wait_started_ms >= kFanoutFrameTimeoutMs) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_TIMEOUT;
		}
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_PENDING;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		const v4l2_role_runtime_t *candidate;
		if (change->action == VIDEO_BROKER_WORKER_ACTION_DETACH) continue;
		candidate = &backend->candidate[change->role];
		branches[output_count++] = (video_broker_worker_branch_receipt_t){
			.role = change->role,
			.branch_epoch = candidate->branch_epoch,
			.frame_id = candidate->frames,
			.codec_bootstrap_complete = change->role != VIDEO_BROKER_WORKER_ROLE_ANALYSIS,
		};
		video_broker_worker_branch_receipt_t *branch = &branches[output_count - 1];
		if (change->role == VIDEO_BROKER_WORKER_ROLE_LIVE) {
			const char *mode = candidate->native_passthrough ?
				"native_passthrough_static" : "persistent_encoder_epoch";
			memcpy(branch->runtime_mode, mode, strlen(mode) + 1);
		} else if (change->role == VIDEO_BROKER_WORKER_ROLE_ARCHIVE) {
			const char *mode;
			if (candidate->native_passthrough) {
				mode = "native_passthrough";
			} else {
				mode = strcmp(candidate->profile.content, "source") == 0 ?
					"source_hardware_encode" : "processed_hardware_encode";
			}
			memcpy(branch->runtime_mode, mode, strlen(mode) + 1);
		}
	}
	*branch_count = output_count;
	backend->wait_started_ms = 0;
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static void v4l2_fanout_apply_active(v4l2_fanout_backend_t *backend) {
	bridge_config_t *cfg = backend->cfg;
	v4l2_role_runtime_t *analysis = backend->active[VIDEO_BROKER_WORKER_ROLE_ANALYSIS];
	v4l2_role_runtime_t *archive = backend->active[VIDEO_BROKER_WORKER_ROLE_ARCHIVE];
	v4l2_role_runtime_t *live = backend->active[VIDEO_BROKER_WORKER_ROLE_LIVE];
	cfg->analysis_enabled = backend->active_present[VIDEO_BROKER_WORKER_ROLE_ANALYSIS];
	if (cfg->analysis_enabled) {
		cfg->sub_width = analysis->profile.width;
		cfg->sub_height = analysis->profile.height;
		cfg->sub_fps = analysis->profile.fps;
		backend->control->sub_fps = analysis->profile.fps;
	}
	cfg->live_enabled = backend->active_present[VIDEO_BROKER_WORKER_ROLE_LIVE];
	if (cfg->live_enabled) {
		cfg->live_codec = strcmp(live->profile.codec, "h264") == 0 ? SP_ENCODER_H264 : SP_ENCODER_H265;
		cfg->output_width = live->profile.width;
		cfg->output_height = live->profile.height;
		cfg->main_fps = live->profile.fps;
		cfg->live_encode_bitrate_kbps = live->profile.bitrate_kbps;
		backend->control->main_active = live->delivery_active;
		backend->control->main_fps = live->profile.fps;
		backend->control->main_bitrate_kbps = live->profile.bitrate_kbps;
	} else {
		backend->control->main_active = false;
	}
	cfg->archive_enabled = backend->active_present[VIDEO_BROKER_WORKER_ROLE_ARCHIVE];
	if (cfg->archive_enabled) {
		cfg->archive_codec = strcmp(archive->profile.codec, "h264") == 0 ? SP_ENCODER_H264 : SP_ENCODER_H265;
		cfg->archive_content = v4l2_profile_archive_content(&archive->profile);
		cfg->archive_rate_control = v4l2_profile_rate_control(
			VIDEO_BROKER_WORKER_ROLE_ARCHIVE, &archive->profile);
		cfg->archive_width = archive->profile.width;
		cfg->archive_height = archive->profile.height;
		cfg->archive_fps = archive->profile.fps;
		cfg->archive_encode_bitrate_kbps = archive->profile.bitrate_kbps;
	}
	*backend->direct_main_native = cfg->live_enabled && live->native_passthrough;
	*backend->archive_native_passthrough = cfg->archive_enabled && archive->native_passthrough;
	*backend->archive_encode_required = cfg->archive_enabled && !archive->native_passthrough;
	*backend->tone_map_capture_frame = cfg->tone_mapping_enabled &&
		*backend->archive_encode_required && cfg->archive_content != VIDEO_BROKER_ARCHIVE_SOURCE;
	*backend->main_uses_capture_surface = cfg->live_enabled && !live->native_passthrough &&
		cfg->output_width == backend->capture->width &&
		cfg->output_height == backend->capture->height &&
		cfg->video_orientation == VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL &&
		(*backend->tone_map_capture_frame || !cfg->tone_mapping_enabled);
	*backend->archive_uses_capture_surface = *backend->archive_encode_required &&
		cfg->archive_width == backend->capture->width &&
		cfg->archive_height == backend->capture->height;
	*backend->persistent_main_control = cfg->live_enabled && !live->native_passthrough;
}

/* The shared accelerator may still own a CL_MEM_USE_HOST_PTR reference to
 * the last active analysis/live surface, even after its queue is idle. Release
 * that reference before moving/freeing role storage at a transaction boundary.
 * This adds no wait/allocation to the steady per-frame path. */
static int v4l2_fanout_release_shared_surface(v4l2_fanout_backend_t *backend, const char *phase) {
	if (video_broker_tone_accelerator_release_surface(backend->shared_tone_accelerator) == 0) return 0;
	fprintf(stderr, "[rdkx5_v4l2_mux_bridge][fanout] phase=%s"
		" shared_tone_release=unconfirmed role_storage=retained\n", phase);
	return -1;
}

static int v4l2_fanout_cutover(void *opaque) {
	v4l2_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0 || backend->cutover_completed) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	if (v4l2_fanout_release_shared_surface(backend, "cutover") != 0)
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		v4l2_role_runtime_t *active = backend->active[change->role];
		if (change->role == VIDEO_BROKER_WORKER_ROLE_LIVE) {
			video_broker_stream_epoch_boundary(&backend->live_epoch);
		}
		if (backend->active_present[change->role]) {
			v4l2_role_move(&backend->incumbent[change->role], active);
			backend->incumbent_present[change->role] = true;
		} else {
			if (v4l2_role_destroy(active) != 0) return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
		backend->active_present[change->role] = false;
		backend->active_failed[change->role] = false;
		if (change->action != VIDEO_BROKER_WORKER_ACTION_DETACH) {
			if (v4l2_role_release_candidate_tone(&backend->candidate[change->role]) != 0)
				return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
			v4l2_role_move(active, &backend->candidate[change->role]);
			backend->candidate_present[change->role] = false;
			backend->active_present[change->role] = true;
			backend->cutover_frame_floor[change->role] = active->frames;
		}
	}
	v4l2_fanout_apply_active(backend);
	backend->cutover_completed = true;
	backend->wait_started_ms = 0;
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static bool v4l2_fanout_verified(const v4l2_fanout_backend_t *backend) {
	for (size_t index = 0; index < backend->change_count; ++index) {
		const video_broker_worker_branch_change_t *change = &backend->changes[index];
		if (change->action != VIDEO_BROKER_WORKER_ACTION_DETACH &&
		    (!backend->active_present[change->role] ||
		     backend->active[change->role]->frames <=
			backend->cutover_frame_floor[change->role])) return false;
	}
	return true;
}

static int v4l2_fanout_verify(void *opaque) {
	v4l2_fanout_backend_t *backend = opaque;
	uint64_t now_ms = monotonic_us() / 1000U;
	if (backend == NULL || !backend->cutover_completed || now_ms == 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		if (backend->active_failed[backend->changes[index].role]) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
	}
	if (!v4l2_fanout_verified(backend)) {
		if (backend->wait_started_ms == 0) backend->wait_started_ms = now_ms;
		if (now_ms - backend->wait_started_ms >= kFanoutFrameTimeoutMs) {
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_TIMEOUT;
		}
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_PENDING;
	}
	backend->wait_started_ms = 0;
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int v4l2_fanout_rollback(void *opaque) {
	v4l2_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	if (backend->cutover_completed && v4l2_fanout_release_shared_surface(backend, "rollback") != 0)
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	for (size_t index = 0; index < backend->change_count; ++index) {
		video_broker_worker_role_t role = backend->changes[index].role;
		if (!backend->cutover_completed) {
			if (backend->candidate_present[role] && v4l2_role_destroy(&backend->candidate[role]) != 0)
				return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
			backend->candidate_present[role] = false;
			continue;
		}
		if (role == VIDEO_BROKER_WORKER_ROLE_LIVE) {
			video_broker_stream_epoch_boundary(&backend->live_epoch);
		}
		if (backend->active_present[role] && v4l2_role_destroy(backend->active[role]) != 0)
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		backend->active_present[role] = false;
		if (backend->incumbent_present[role]) {
			v4l2_role_move(backend->active[role], &backend->incumbent[role]);
			backend->active_present[role] = true;
			backend->incumbent_present[role] = false;
		}
	}
	if (backend->cutover_completed) v4l2_fanout_apply_active(backend);
	v4l2_fanout_reset(backend);
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int v4l2_fanout_abort(void *opaque) {
	v4l2_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0 || backend->cutover_completed) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	for (size_t index = 0; index < backend->change_count; ++index) {
		video_broker_worker_role_t role = backend->changes[index].role;
		if (backend->candidate_present[role] && v4l2_role_destroy(&backend->candidate[role]) != 0)
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		backend->candidate_present[role] = false;
	}
	v4l2_fanout_reset(backend);
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static int v4l2_fanout_retire(void *opaque) {
	v4l2_fanout_backend_t *backend = opaque;
	if (backend == NULL || backend->change_count == 0 || !backend->cutover_completed) {
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	}
	if (v4l2_fanout_release_shared_surface(backend, "retire") != 0)
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	for (size_t index = 0; index < backend->change_count; ++index) {
		video_broker_worker_role_t role = backend->changes[index].role;
		if (backend->incumbent_present[role] && v4l2_role_destroy(&backend->incumbent[role]) != 0)
			return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		backend->incumbent_present[role] = false;
		if (backend->active_present[role]) {
			backend->next_epoch[role] = backend->active[role]->branch_epoch;
		}
	}
	v4l2_fanout_reset(backend);
	return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK;
}

static video_broker_worker_fanout_callbacks_t v4l2_fanout_callbacks(void) {
	return (video_broker_worker_fanout_callbacks_t){
		.prepare = v4l2_fanout_prepare,
		.warmup = v4l2_fanout_warmup,
		.admit = v4l2_fanout_admit,
		.first_frame = v4l2_fanout_first_frame,
		.cutover = v4l2_fanout_cutover,
		.verify = v4l2_fanout_verify,
		.rollback = v4l2_fanout_rollback,
		.abort = v4l2_fanout_abort,
		.retire = v4l2_fanout_retire,
	};
}

static int v4l2_fanout_process_candidates(v4l2_fanout_backend_t *backend) {
	if (backend == NULL || backend->change_count == 0 || backend->cutover_completed) return 0;
	if (backend->candidate_failed) return 0;
	for (size_t index = 0; index < backend->change_count; ++index) {
		video_broker_worker_role_t role = backend->changes[index].role;
		if (backend->candidate_present[role] &&
		    v4l2_candidate_process(backend, &backend->candidate[role]) != 0) {
			backend->candidate_failed = true;
			fprintf(stderr,
				"[rdkx5_v4l2_mux_bridge][fanout] candidate failed role=%s action=retain_incumbent\n",
				video_broker_worker_role_name(role));
			break;
		}
	}
	return 0;
}

static bool v4l2_fanout_mark_active_failure(
	v4l2_fanout_backend_t *backend,
	video_broker_worker_role_t role) {
	if (backend == NULL || !backend->cutover_completed ||
	    !backend->active_present[role] ||
	    !video_broker_worker_fanout_role_activated(
		backend->changes, backend->change_count, role)) return false;
	backend->active_failed[role] = true;
	fprintf(stderr,
		"[rdkx5_v4l2_mux_bridge][fanout] verification failed role=%s action=request_rollback\n",
		video_broker_worker_role_name(role));
	return true;
}

static int v4l2_fanout_resume_and_publish(
	video_broker_worker_fanout_controller_t *controller) {
	video_broker_worker_fanout_receipt_t receipt;
	int controller_state;
	if (controller == NULL || !controller->phase_pending) return 0;
	controller_state = video_broker_worker_fanout_controller_resume(controller, &receipt);
	if (video_broker_rdkx5_encoder_resources_quarantined() ||
	    video_broker_tone_accelerator_resources_quarantined()) return -1;
	if (controller_state == VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_PENDING) return 0;
	return video_broker_worker_fanout_write_receipt(stderr, &receipt);
}

static int v4l2_fanout_destroy(v4l2_fanout_backend_t *backend) {
	int result = 0;
	if (backend == NULL) return 0;
	for (int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	     role <= VIDEO_BROKER_WORKER_ROLE_LIVE; ++role) {
		if (backend->candidate_present[role]) {
			if (v4l2_role_destroy(&backend->candidate[role]) == 0) backend->candidate_present[role] = false;
			else result = -1;
		}
		if (backend->incumbent_present[role]) {
			if (v4l2_role_destroy(&backend->incumbent[role]) == 0) backend->incumbent_present[role] = false;
			else result = -1;
		}
	}
	return result;
}

static void pipeline_checkpoint(vb_pipeline_latency *probe, const char *next) {
	struct timespec cpu;
	vb_pipeline_delay delay;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu) != 0) return;
	uint64_t cpu_us = (uint64_t)cpu.tv_sec*1000000ULL + (uint64_t)cpu.tv_nsec/1000ULL;
	if (vb_pipeline_checkpoint(probe, next, monotonic_us(), cpu_us, &delay)) {
		vb_diagnostic_pipe_printf(&g_telemetry, "[video-broker][pipeline-latency] event=native_phase_slow phase=%s wall_us=%llu thread_cpu_us=%llu suppressed=%llu next=%s\n",
			delay.phase, (unsigned long long)delay.wall_us, (unsigned long long)delay.cpu_us,
			(unsigned long long)delay.suppressed, next);
	}
}

int main(int argc, char **argv) {
	bridge_config_t cfg;
	video_broker_frame_source frame_source = {0};
	v4l2_capture_t cap;
	tjhandle jpeg = NULL;
	video_broker_rdkx5_decoder_t decoder;
	video_broker_rdkx5_decoder_t *capture_decoder = &decoder;
	video_broker_rdkx5_mjpeg_trial_t mjpeg_trial;
	video_broker_rdkx5_async_decoder_t mjpeg_async_decoder;
	int binary_fd = -1;
	FILE *out = NULL;
	unsigned char *capture_frame_buffer = NULL;
	unsigned char *capture_input_copy = NULL;
	unsigned char *decode_scratch_buffer = NULL;
	v4l2_role_runtime_t analysis_role;
	v4l2_role_runtime_t archive_role;
	v4l2_role_runtime_t live_role;
	size_t capture_frame_buffer_size;
	size_t capture_input_copy_capacity = 0;
	bool capture_detach_reported = false;
	bool capture_sequence_seen = false;
	uint32_t capture_previous_sequence = 0;
	bool capture_waiting_idr = false;
	unsigned capture_discontinuity_tolerated = 0;
	uint64_t capture_resync_deadline_us = 0;
	vb_capture_origin capture_clean_origin = {0,0};
	size_t decode_scratch_buffer_size;
	size_t mjpeg_trial_access_unit_capacity = 0;
	time_t warmup_until = 0;
	uint64_t tone_mapper_last_log_us = 0;
	int capture_timeout_count = 0;
	int corrupt_frame_run = 0;
	int rc = 1;
	int last_operation_result = 1;
	quality_monitor_t quality_monitor;
	runtime_metrics_t runtime_metrics;
	vb_pipeline_latency phase_probe = {0};
	bool direct_main_native = false;
	bool archive_native_passthrough = false;
	bool archive_encode_required = false;
	bool tone_map_capture_frame = false;
	bool main_uses_capture_surface = false;
	bool archive_uses_capture_surface = false;
	bool metrics_steady_window_started = false;
	bool persistent_main_control = false;
	video_broker_mjpeg_accelerator_t mjpeg_decode_selected = VIDEO_BROKER_MJPEG_ACCELERATOR_CPU;
	bool mjpeg_decoder_attested = false;
	bool mjpeg_trial_decoder_active = false;
	bool mjpeg_trial_decoder_reset_pending = false;
	bool mjpeg_base_decoder_close_pending = false;
	bool mjpeg_candidate_ready = false;
	bool mjpeg_switch_ready = false;
	uint64_t mjpeg_candidate_token = 0;
	uint64_t mjpeg_worker_token = 0;
	uint64_t mjpeg_active_trial_token = 0;
	uint64_t mjpeg_last_cancel_ack_token = 0;
	uint64_t mjpeg_cancel_ack_pending_token = 0;
	uint64_t mjpeg_async_replacements_observed = 0;
	uint64_t mjpeg_first_unacknowledged_submission_us = 0;
	int64_t mjpeg_candidate_completed_unix_ms = 0;
	int64_t mjpeg_commit_deadline_unix_ms = 0;
	runtime_control_t runtime_control;
	video_broker_nv12_tone_mapper_t tone_mapper;
	video_broker_tone_accelerator_t tone_accelerator;
	camera_admission_t camera_admission;
	v4l2_fanout_backend_t fanout_backend;
	video_broker_worker_fanout_controller_t fanout_controller;
	bool fanout_enabled = false;
	bool cleanup_confirmed = true;
	bool operation_loop_completed = false;
	vb_hevc_sequence_guard hevc_sequence = {0};

	default_config(&cfg);
	if (parse_args(argc, argv, &cfg) != 0) {
		return 2;
	}
	memset(&cap, 0, sizeof(cap));
	cap.fd = -1;
	memset(&quality_monitor, 0, sizeof(quality_monitor));
	memset(&runtime_metrics, 0, sizeof(runtime_metrics));
	memset(&decoder, 0, sizeof(decoder));
	memset(&mjpeg_trial, 0, sizeof(mjpeg_trial));
	memset(&mjpeg_async_decoder, 0, sizeof(mjpeg_async_decoder));
	memset(&analysis_role, 0, sizeof(analysis_role));
	memset(&archive_role, 0, sizeof(archive_role));
	memset(&live_role, 0, sizeof(live_role));
	memset(&camera_admission, 0, sizeof(camera_admission));
	memset(&tone_accelerator, 0, sizeof(tone_accelerator));
	memset(&tone_mapper, 0, sizeof(tone_mapper));
	memset(&fanout_backend, 0, sizeof(fanout_backend));
	memset(&fanout_controller, 0, sizeof(fanout_controller));
	runtime_control_init(&runtime_control, &cfg);
	if (cfg.input_format == 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] unsupported input format=%s\n",
			cfg.input_format_name == NULL ? "" : cfg.input_format_name);
		return 2;
	}
	if (cfg.quality_monitor_enabled &&
	    (cfg.quality_monitor_state_path == NULL ||
	     cfg.quality_monitor_state_path[0] == '\0')) {
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge] configured quality monitor requires a state path\n");
		return 2;
	}
	if (cfg.aspect_policy == VIDEO_BROKER_NV12_ASPECT_EXACT &&
	    ((cfg.live_enabled && !video_aspect_matches(cfg.capture_width, cfg.capture_height, cfg.output_width, cfg.output_height)) ||
	     (cfg.analysis_enabled && !video_aspect_matches(cfg.capture_width, cfg.capture_height, cfg.sub_width, cfg.sub_height)) ||
	     (cfg.archive_enabled && !video_aspect_matches(cfg.capture_width, cfg.capture_height, cfg.archive_width, cfg.archive_height)))) {
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge] exact aspect policy rejects capture=%dx%d output=%dx%d sub=%dx%d archive=%dx%d\n",
			cfg.capture_width, cfg.capture_height, cfg.output_width, cfg.output_height, cfg.sub_width, cfg.sub_height,
			cfg.archive_width, cfg.archive_height);
		return 2;
	}

	if (video_broker_worker_install_stop_signals(handle_signal) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] stop signal setup failed err=%s\n", strerror(errno));
		return 2;
	}
	setvbuf(stderr, NULL, _IOLBF, 0);
	if (vb_diagnostic_pipe_open(&g_telemetry, STDERR_FILENO) != 0) {
		fprintf(stderr, "[video-broker][diagnostics] event=telemetry_disabled"
			" reason=non_pipe_or_unavailable action=preserve_media_and_critical_receipts\n");
	}
	{
		int stdin_flags = fcntl(STDIN_FILENO, F_GETFL, 0);
		if (stdin_flags >= 0) {
			(void)fcntl(STDIN_FILENO, F_SETFL, stdin_flags | O_NONBLOCK);
		}
	}

	if (video_broker_frame_source_open(&frame_source, &cfg.frame_launch, cfg.source_id, cfg.media_epoch) != 0) {
		fprintf(stderr,"[rdkx5_v4l2_mux_bridge][frame-source] stage=%s action=reject_before_hardware_init\n",frame_source.stage);
		goto cleanup;
	}
	/* Private source bootstrap must complete before ANY accelerator allocation,
	 * including tone-mapping OpenCL/GPU resources, not only camera/JPU/VPU. */
	video_broker_nv12_tone_mapper_init(
		&tone_mapper, cfg.tone_mapping_mode, cfg.tone_mapping_target,
		cfg.tone_mapping_hysteresis, cfg.tone_mapping_grid_columns,
		cfg.tone_mapping_grid_rows, cfg.tone_mapping_statistics_hz, cfg.fps,
		cfg.tone_mapping_max_shadow_gain, cfg.tone_mapping_max_highlight_reduction,
		cfg.tone_mapping_temporal_alpha, cfg.tone_mapping_noise_guard);
	if (video_broker_tone_accelerator_init(
			&tone_accelerator,
			cfg.tone_mapping_enabled ? cfg.tone_mapping_accelerator : VIDEO_BROKER_TONE_ACCELERATOR_CPU) != 0) {
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge] tone accelerator unavailable requested=%s error=%s\n",
			video_broker_tone_accelerator_name(cfg.tone_mapping_accelerator),
			video_broker_tone_accelerator_error(&tone_accelerator));
		goto cleanup;
	}
	fprintf(stderr,
		"[rdkx5_v4l2_mux_bridge] tone accelerator requested=%s selected=%s fallback=%s\n",
		video_broker_tone_accelerator_name(cfg.tone_mapping_accelerator),
		video_broker_tone_accelerator_name(tone_accelerator.selected),
		video_broker_tone_accelerator_error(&tone_accelerator));
	binary_fd = dup(STDOUT_FILENO);
	if (binary_fd < 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] dup stdout failed\n");
		goto cleanup;
	}
	if (dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] redirect stdout failed\n");
		goto cleanup;
	}
	out = fdopen(binary_fd, "wb");
	if (out == NULL) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] fdopen failed\n");
		goto cleanup;
	}

	fprintf(stderr,
		"[rdkx5_v4l2_mux_bridge] open device=%s format=%s roles=%s/%s/%s capture=%dx%d archive=%dx%d output=%dx%d sub=%dx%d capture_fps=%d analysis_fps=%d live_fps=%d archive_fps=%d live_bitrate_kbps=%d archive_bitrate_kbps=%d live_active=%s mjpeg_decode_accelerator=%s mjpeg_decode_initial_backend=%s allow_sw_fallback=%s orientation=%s aspect_policy=%s capture_timeout_ms=%d capture_max_timeouts=%d quality_monitor=%s/%d/%d quality_state=%s tone_mapping=%s tone_accelerator=%s target=%d hysteresis=%d grid=%dx%d statistics_hz=%d max_shadow_gain=%.2f max_highlight_reduction=%.2f temporal_alpha=%.2f noise_guard=%s archive_content=%s archive_codec=%s archive_rate_control=%s\n",
		cfg.device, format_name(cfg.input_format),
		cfg.live_enabled ? "live" : "-", cfg.analysis_enabled ? "analysis" : "-", cfg.archive_enabled ? "archive" : "-",
		cfg.capture_width, cfg.capture_height,
		cfg.archive_width, cfg.archive_height,
		cfg.output_width, cfg.output_height, cfg.sub_width, cfg.sub_height, cfg.fps, cfg.sub_fps, cfg.main_fps, cfg.archive_fps,
		cfg.live_encode_bitrate_kbps, cfg.archive_encode_bitrate_kbps, cfg.main_active ? "true" : "false",
		mjpeg_accelerator_name(cfg.mjpeg_decode_accelerator),
		mjpeg_accelerator_name(cfg.mjpeg_decode_initial_backend),
		cfg.allow_software_fallback ? "true" : "false",
		video_broker_video_orientation_name(cfg.video_orientation),
		aspect_policy_name(cfg.aspect_policy),
		cfg.capture_select_timeout_ms,
		cfg.capture_max_consecutive_timeouts,
		cfg.quality_monitor_enabled ? "on" : "off",
		cfg.quality_monitor_report_every,
		cfg.quality_monitor_sample_stride,
		cfg.quality_monitor_state_path == NULL ? "" : cfg.quality_monitor_state_path,
		video_broker_tone_mode_name(cfg.tone_mapping_mode),
		video_broker_tone_accelerator_name(tone_accelerator.selected),
		cfg.tone_mapping_target,
		cfg.tone_mapping_hysteresis,
		cfg.tone_mapping_grid_columns,
		cfg.tone_mapping_grid_rows,
		cfg.tone_mapping_statistics_hz,
		cfg.tone_mapping_max_shadow_gain,
		cfg.tone_mapping_max_highlight_reduction,
		cfg.tone_mapping_temporal_alpha,
		cfg.tone_mapping_noise_guard ? "true" : "false",
		video_broker_archive_content_name(cfg.archive_content),
		cfg.archive_codec == SP_ENCODER_H264 ? "h264" : "h265",
		archive_rate_control_name(cfg.archive_rate_control));

	if (prepare_v4l2_capture(&cfg, &cap) != 0) {
		goto cleanup;
	}
	camera_admission_init(&camera_admission, cap.pixel_format, cap.fps > 0 ? cap.fps : cfg.fps,
		(uint64_t)cfg.capture_select_timeout_ms * (uint64_t)cfg.capture_max_consecutive_timeouts * 1000U);
	fprintf(stderr,
		"[rdkx5_v4l2_mux_bridge] capture ready actual=%dx%d format=%s fourcc=0x%08x actual_fps=%d requested_fps=%d\n",
		cap.width, cap.height, format_name(cap.pixel_format), cap.pixel_format, cap.fps, cfg.fps);
	direct_main_native = cfg.live_enabled && !cfg.tone_mapping_enabled &&
		((cfg.live_codec == SP_ENCODER_H264 && cap.pixel_format == V4L2_PIX_FMT_H264) ||
		 (cfg.live_codec == SP_ENCODER_H265 && cap.pixel_format == V4L2_PIX_FMT_HEVC)) &&
		cfg.output_width == cap.width &&
		cfg.output_height == cap.height &&
		cfg.main_fps == (cap.fps > 0 ? cap.fps : cfg.fps) &&
		cfg.video_orientation == VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL;
	archive_native_passthrough = cfg.archive_enabled && video_broker_archive_uses_native(
		cfg.archive_content,
		is_compressed_h26x(cap.pixel_format),
		cfg.tone_mapping_enabled) &&
		cfg.archive_rate_control != VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR &&
		cfg.archive_fps == (cap.fps > 0 ? cap.fps : cfg.fps);
	archive_encode_required = cfg.archive_enabled && !archive_native_passthrough;
	/* Native/source archives must never pay for presentation-only tone mapping.
	 * Keep their capture path independent and render PAL on the bounded derived
	 * geometry. A processed archive still needs the corrected capture surface. */
	tone_map_capture_frame = cfg.tone_mapping_enabled && archive_encode_required &&
		cfg.archive_content != VIDEO_BROKER_ARCHIVE_SOURCE;
	main_uses_capture_surface = cfg.output_width == cap.width && cfg.output_height == cap.height &&
		cfg.video_orientation == VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL &&
		(tone_map_capture_frame || !cfg.tone_mapping_enabled);
	archive_uses_capture_surface = archive_encode_required &&
		cfg.archive_width == cap.width && cfg.archive_height == cap.height;
	v4l2_role_profile_from_config(
		&analysis_role, VIDEO_BROKER_WORKER_ROLE_ANALYSIS, &cfg, &cap);
	v4l2_role_profile_from_config(
		&archive_role, VIDEO_BROKER_WORKER_ROLE_ARCHIVE, &cfg, &cap);
	v4l2_role_profile_from_config(
		&live_role, VIDEO_BROKER_WORKER_ROLE_LIVE, &cfg, &cap);
	analysis_role.uses_capture_surface = cfg.sub_width == cap.width &&
		cfg.sub_height == cap.height &&
		cfg.video_orientation == VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL;
	archive_role.uses_capture_surface = archive_uses_capture_surface;
	live_role.uses_capture_surface = main_uses_capture_surface;
	fprintf(stderr,
		"[rdkx5_v4l2_mux_bridge][surface-plan] corrected_surface=%s main_surface=%s archive_surface=%s analysis_surface=%s\n",
		cfg.tone_mapping_enabled ? "capture_nv12" : "capture_native",
		main_uses_capture_surface ? "capture_zero_copy" : "derived_nv12",
		archive_uses_capture_surface ? "capture_zero_copy" : "derived_nv12",
		cfg.sub_width == cap.width && cfg.sub_height == cap.height &&
			cfg.video_orientation == VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL ?
			"capture_copy" : "derived_nv12");
	persistent_main_control = cfg.live_enabled && !direct_main_native;

	if (video_broker_frame_source_geometry(&frame_source, (uint32_t)cap.width, (uint32_t)cap.height, (uint32_t)cap.width, 1) != 0) {
		fprintf(stderr,"[rdkx5_v4l2_mux_bridge][frame-source] stage=%s action=reject_camera_geometry\n",frame_source.stage);
		goto cleanup;
	}
	capture_frame_buffer_size = FRAME_BUFFER_SIZE(cap.width, cap.height);
	decode_scratch_buffer_size = (size_t)cap.width * (size_t)cap.height * 3;
	live_role.frame_size = FRAME_BUFFER_SIZE(cfg.output_width, cfg.output_height);
	analysis_role.frame_size = FRAME_BUFFER_SIZE(cfg.sub_width, cfg.sub_height);
	archive_role.frame_size = FRAME_BUFFER_SIZE(cfg.archive_width, cfg.archive_height);
	live_role.stream_capacity = cfg.live_enabled && !direct_main_native ?
		stream_buffer_capacity(live_role.frame_size) : 0;
	archive_role.stream_capacity = archive_encode_required ? stream_buffer_capacity(
		capture_frame_buffer_size > archive_role.frame_size
			? capture_frame_buffer_size
			: archive_role.frame_size) : 0;
	capture_frame_buffer = (unsigned char *)allocate_frame_buffer(capture_frame_buffer_size);
	if (cap.pixel_format == V4L2_PIX_FMT_MJPEG || frame_source.enabled) {
		for (unsigned int index = 0; index < cap.buffer_count; ++index) {
			if (cap.buffers[index].length > capture_input_copy_capacity) {
				capture_input_copy_capacity = cap.buffers[index].length;
			}
		}
		/* Frame Access owns only a bounded detached copy, never the camera's
		 * lease. Its admission reserves at most two NV12 surfaces for this
		 * input; a driver advertising more is rejected BEFORE allocation. */
		if (frame_source.enabled &&
		    (capture_input_copy_capacity == 0 ||
		     capture_input_copy_capacity > 2U * capture_frame_buffer_size)) {
			fprintf(stderr,"[rdkx5_v4l2_mux_bridge][frame-source] stage=input_copy_limit capacity=%zu limit=%zu action=reject_before_allocation\n",
				capture_input_copy_capacity, 2U * capture_frame_buffer_size);
			goto cleanup;
		}
		capture_input_copy = (unsigned char *)malloc(capture_input_copy_capacity);
	}
	decode_scratch_buffer = (unsigned char *)malloc(decode_scratch_buffer_size);
	if (cfg.live_enabled && !direct_main_native) {
		live_role.frame = (unsigned char *)allocate_frame_buffer(live_role.frame_size);
		live_role.stream = (unsigned char *)malloc(live_role.stream_capacity + kStreamBufferGuard);
	}
	if (cfg.analysis_enabled) {
		analysis_role.frame = (unsigned char *)allocate_frame_buffer(analysis_role.frame_size);
	}
	if (archive_encode_required) {
		archive_role.frame = (unsigned char *)allocate_frame_buffer(archive_role.frame_size);
		archive_role.stream = (unsigned char *)malloc(
			archive_role.stream_capacity + kStreamBufferGuard);
	}
	if (capture_frame_buffer == NULL ||
	    ((cap.pixel_format == V4L2_PIX_FMT_MJPEG || frame_source.enabled) && capture_input_copy == NULL) ||
	    decode_scratch_buffer == NULL ||
	    (cfg.live_enabled && !direct_main_native &&
	     (live_role.frame == NULL || live_role.stream == NULL)) ||
	    (cfg.analysis_enabled && analysis_role.frame == NULL) ||
	    (archive_encode_required &&
	     (archive_role.frame == NULL || archive_role.stream == NULL))) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] buffer allocation failed\n");
		goto cleanup;
	}
	if (cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
	    video_broker_rdkx5_async_decoder_init(
		    &mjpeg_async_decoder,
		    capture_input_copy_capacity,
		    capture_frame_buffer_size,
		    kDecoderOutputPollTimeoutMs) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] MJPEG async decoder workspace allocation failed\n");
		goto cleanup;
	}
	if (cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
	    cfg.mjpeg_decode_accelerator == VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO &&
	    cfg.allow_software_fallback) {
		mjpeg_trial_access_unit_capacity = capture_input_copy_capacity;
		if (video_broker_rdkx5_mjpeg_trial_init(
				&mjpeg_trial,
				mjpeg_trial_access_unit_capacity,
				cap.width,
				cap.height) != 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] MJPEG async trial workspace allocation failed\n");
			goto cleanup;
		}
	}

	if (cap.pixel_format == V4L2_PIX_FMT_MJPEG) {
		bool try_jpu = cfg.mjpeg_decode_accelerator != VIDEO_BROKER_MJPEG_ACCELERATOR_CPU &&
			cfg.mjpeg_decode_initial_backend == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU;
		bool jpu_ready = false;
		if (try_jpu && video_broker_rdkx5_decoder_open(
				&decoder, VIDEO_BROKER_RDKX5_CODEC_MJPEG, cap.width, cap.height, video_broker_frame_source_epoch(&frame_source)) == 0) {
			jpu_ready = video_broker_rdkx5_async_decoder_start(
				&mjpeg_async_decoder, &decoder) == 0;
			if (!jpu_ready) {
				if (video_broker_rdkx5_decoder_close(&decoder) != 0) goto cleanup;
			}
		}
		if (video_broker_rdkx5_decoder_resources_quarantined()) goto cleanup;
		if (jpu_ready) {
			mjpeg_decode_selected = VIDEO_BROKER_MJPEG_ACCELERATOR_JPU;
			capture_decoder = &decoder;
		} else {
			if (cfg.mjpeg_decode_accelerator == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU ||
			    !cfg.allow_software_fallback) {
				fprintf(stderr,
					"[rdkx5_v4l2_mux_bridge] MJPEG JPU decoder unavailable requested=%s allow_sw_fallback=%s\n",
					mjpeg_accelerator_name(cfg.mjpeg_decode_accelerator),
					cfg.allow_software_fallback ? "true" : "false");
				goto cleanup;
			}
			jpeg = tjInitDecompress();
			if (jpeg == NULL) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] turbojpeg init failed\n");
				goto cleanup;
			}
			mjpeg_decode_selected = VIDEO_BROKER_MJPEG_ACCELERATOR_CPU;
			mjpeg_decoder_attested = true;
			fprintf(stderr,
				"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=%s selected=cpu fallback=%s output=nv12 retry_token=0\n",
				mjpeg_accelerator_name(cfg.mjpeg_decode_accelerator),
				try_jpu ? "jpu_init_failed" : "none");
		}
	}
	if (is_compressed_h26x(cap.pixel_format)) {
		video_broker_rdkx5_codec_t codec = cap.pixel_format == V4L2_PIX_FMT_HEVC ?
			VIDEO_BROKER_RDKX5_CODEC_H265 : VIDEO_BROKER_RDKX5_CODEC_H264;
		if (video_broker_rdkx5_decoder_open(&decoder, codec, cap.width, cap.height, video_broker_frame_source_epoch(&frame_source)) != 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] hardware decoder init failed codec=%s\n", format_name(cap.pixel_format));
			goto cleanup;
		}
		capture_decoder = &decoder;
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge] hardware decoder configured codec=%s feed_mode=frame_size reorder=false output_wait_ms=%d scope=shared_capture_low_latency\n",
			format_name(cap.pixel_format), kDecoderOutputPollTimeoutMs);
	}

	if (cfg.live_enabled && !direct_main_native) {
		int control_rc;
		int encoder_fps = runtime_control.main_fps;
		live_role.encoder = calloc(1, sizeof(*live_role.encoder));
		if (live_role.encoder == NULL || video_broker_rdkx5_persistent_encoder_init(
			    live_role.encoder,
			    cfg.live_codec == SP_ENCODER_H264 ?
				    VIDEO_BROKER_RDKX5_ENCODER_H264 : VIDEO_BROKER_RDKX5_ENCODER_H265,
			    VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,
			    0,
			    cfg.output_width,
			    cfg.output_height,
			    encoder_fps,
			    runtime_control.main_bitrate_kbps) != 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] persistent %s live encoder preheat failed\n",
				cfg.live_codec == SP_ENCODER_H264 ? "H264" : "H265");
			goto cleanup;
		}
		live_role.encoder_initialized = true;
		control_rc = video_broker_rdkx5_persistent_encoder_set_control(
			live_role.encoder,
			runtime_control.main_active,
			encoder_fps,
			runtime_control.main_bitrate_kbps);
		if (control_rc < 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] persistent live encoder initial control failed\n");
			goto cleanup;
		}
		if (control_rc == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED) {
			live_role.active_bitrate_kbps = live_role.encoder->bitrate_kbps;
			live_role.pending_control_sequence = runtime_control.sequence;
			live_role.pending_control_ready = runtime_control.sequence > 0;
		}
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge] persistent %s live encoder preheated width=%d height=%d fps=%d bitrate_kbps=%d active=%s rc=strict_realtime_cbr vbv_ms=%d max_qp_i=%d max_qp_p=%d max_qp_b=%d\n",
			cfg.live_codec == SP_ENCODER_H264 ? "H264" : "H265",
			cfg.output_width,
			cfg.output_height,
			encoder_fps,
			runtime_control.main_bitrate_kbps,
			runtime_control.main_active ? "true" : "false",
			live_role.encoder->vbv_buffer_ms,
			live_role.encoder->max_qp_i,
			live_role.encoder->max_qp_p,
			live_role.encoder->max_qp_b);
	}
	if (archive_encode_required) {
		int archive_fps = cfg.archive_fps;
		int archive_bitrate_kbps = cfg.archive_encode_bitrate_kbps;
		video_broker_rdkx5_encoder_codec_t archive_encoder_codec =
			cfg.archive_codec == SP_ENCODER_H264 ?
				VIDEO_BROKER_RDKX5_ENCODER_H264 : VIDEO_BROKER_RDKX5_ENCODER_H265;
		archive_role.encoder = calloc(1, sizeof(*archive_role.encoder));
		if (archive_role.encoder == NULL || video_broker_rdkx5_persistent_encoder_init(
			    archive_role.encoder,
			    archive_encoder_codec,
			    cfg.archive_rate_control,
			    1,
			    cfg.archive_width,
			    cfg.archive_height,
			    archive_fps,
			    archive_bitrate_kbps) != 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] %s archive encoder init failed\n",
				cfg.archive_codec == SP_ENCODER_H264 ? "H264" : "H265");
			goto cleanup;
		}
		archive_role.encoder_initialized = true;
		if (video_broker_rdkx5_persistent_encoder_set_control(
			    archive_role.encoder,
			    true,
			    archive_fps,
			    archive_bitrate_kbps) < 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] archive encoder activation failed\n");
			goto cleanup;
		}
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge] persistent %s archive encoder preheated width=%d height=%d fps=%d bitrate_kbps=%d content=%s active=true rc=quality_bounded_%s vbv_ms=%d max_qp_i=%d max_qp_p=%d max_qp_b=%d\n",
			cfg.archive_codec == SP_ENCODER_H264 ? "H264" : "H265",
			cfg.archive_width,
			cfg.archive_height,
			archive_fps,
			archive_bitrate_kbps,
			video_broker_archive_content_name(cfg.archive_content),
			archive_rate_control_name(cfg.archive_rate_control),
			archive_role.encoder->vbv_buffer_ms,
			archive_role.encoder->max_qp_i,
			archive_role.encoder->max_qp_p,
			archive_role.encoder->max_qp_b);
	}
	fanout_backend.cfg = &cfg;
	fanout_backend.capture = &cap;
	fanout_backend.control = &runtime_control;
	fanout_backend.active[VIDEO_BROKER_WORKER_ROLE_ANALYSIS] = &analysis_role;
	fanout_backend.active[VIDEO_BROKER_WORKER_ROLE_ARCHIVE] = &archive_role;
	fanout_backend.active[VIDEO_BROKER_WORKER_ROLE_LIVE] = &live_role;
	fanout_backend.active_present[VIDEO_BROKER_WORKER_ROLE_ANALYSIS] = cfg.analysis_enabled;
	fanout_backend.active_present[VIDEO_BROKER_WORKER_ROLE_ARCHIVE] = cfg.archive_enabled;
	fanout_backend.active_present[VIDEO_BROKER_WORKER_ROLE_LIVE] = cfg.live_enabled;
	fanout_backend.direct_main_native = &direct_main_native;
	fanout_backend.archive_native_passthrough = &archive_native_passthrough;
	fanout_backend.archive_encode_required = &archive_encode_required;
	fanout_backend.main_uses_capture_surface = &main_uses_capture_surface;
	fanout_backend.archive_uses_capture_surface = &archive_uses_capture_surface;
	fanout_backend.tone_map_capture_frame = &tone_map_capture_frame;
	fanout_backend.shared_tone_accelerator = &tone_accelerator;
	fanout_backend.persistent_main_control = &persistent_main_control;
	for (int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	     role <= VIDEO_BROKER_WORKER_ROLE_LIVE; ++role) {
		fanout_backend.next_epoch[role] = fanout_backend.active_present[role] ?
			fanout_backend.active[role]->branch_epoch : 0;
	}
	fanout_enabled = cfg.source_id_explicit && cfg.media_epoch_explicit;
	if (fanout_enabled && video_broker_worker_fanout_controller_init(
			&fanout_controller,
			cfg.source_id,
			(int)getpid(),
			cfg.media_epoch,
			v4l2_fanout_callbacks(),
			&fanout_backend) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge] fanout controller init failed\n");
		goto cleanup;
	}
	{
		const char *archive_codec;
		const char *archive_delivery;
		const char *live_control_mode;
		int actual_fps = cap.fps > 0 ? cap.fps : cfg.fps;
		if (!cfg.archive_enabled) {
			archive_codec = "none";
			archive_delivery = "disabled";
		} else if (archive_native_passthrough && cap.pixel_format == V4L2_PIX_FMT_HEVC) {
			archive_codec = "h265";
			archive_delivery = "native_passthrough";
		} else if (archive_native_passthrough && cap.pixel_format == V4L2_PIX_FMT_H264) {
			archive_codec = "h264";
			archive_delivery = "native_passthrough";
		} else {
			archive_codec = cfg.archive_codec == SP_ENCODER_H264 ? "h264" : "h265";
			archive_delivery = cfg.archive_content == VIDEO_BROKER_ARCHIVE_SOURCE ?
				"source_hardware_encode" : "processed_hardware_encode";
		}
		if (!cfg.live_enabled) {
			live_control_mode = "disabled";
		} else if (direct_main_native) {
			live_control_mode = "native_passthrough_static";
		} else {
			live_control_mode = "persistent_encoder_epoch";
		}
		/* Archive cadence is an independent output contract. Reporting capture
		 * cadence here makes the broker wait forever when archive_fps is lower. */
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge][capability] live_enabled=%s analysis_enabled=%s archive_enabled=%s capture_codec=%s capture=%dx%d capture_fps=%d live_codec=%s archive_codec=%s archive_delivery=%s archive_fps=%d live_control_mode=%s fanout_control_mode=%s\n",
			cfg.live_enabled ? "true" : "false",
			cfg.analysis_enabled ? "true" : "false",
			cfg.archive_enabled ? "true" : "false",
			format_name(cap.pixel_format), cap.width, cap.height, actual_fps,
			cfg.live_enabled ? (cfg.live_codec == SP_ENCODER_H264 ? "h264" : "h265") : "none",
			archive_codec, archive_delivery, cfg.archive_enabled ? cfg.archive_fps : 0,
			live_control_mode, fanout_enabled ? "branch_transaction_v1" : "disabled");
	}
	if (cfg.warmup_sec > 0) {
		warmup_until = time(NULL) + cfg.warmup_sec;
	}
	/* No data-plane codec operation may run after STREAMON and before the first
	 * DQBUF. Queue the mapped ring only after decoder/encoder initialization and
	 * preheat have completed, then enter capture polling immediately. */
	if (start_v4l2_capture(&cap) != 0) {
		int start_errno = errno;
		int prepared_width = cap.width;
		int prepared_height = cap.height;
		uint32_t prepared_pixel_format = cap.pixel_format;
		int prepared_fps = cap.fps;

		if (!g_running) {
			rc = 0;
			goto cleanup;
		}
		if (!uvc_start_reprepare_allowed(start_errno)) {
			goto cleanup;
		}
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge][capture-lifecycle] event=uvc_start_reprepare_started first_errno=%d quiet_ms=%d action=retain_codec_state\n",
			start_errno,
			kUVCStartReprepareQuietMs);
		if (close_v4l2_capture(&cap) != 0) {
			cleanup_confirmed = false;
			goto cleanup;
		}
		if (wait_uvc_start_reprepare_quiet() != 0) {
			if (!g_running) {
				rc = 0;
			}
			goto cleanup;
		}
		if (prepare_v4l2_capture(&cfg, &cap) != 0) {
			goto cleanup;
		}
		if (!capture_layout_matches(
				&cap,
				prepared_width,
				prepared_height,
				prepared_pixel_format,
				prepared_fps,
				capture_input_copy_capacity)) {
			fprintf(stderr,
				"[rdkx5_v4l2_mux_bridge][capture-lifecycle] event=uvc_start_reprepare_rejected reason=capture_layout_changed expected=%dx%d/%s@%d actual=%dx%d/%s@%d\n",
				prepared_width,
				prepared_height,
				format_name(prepared_pixel_format),
				prepared_fps,
				cap.width,
				cap.height,
				format_name(cap.pixel_format),
				cap.fps);
			goto cleanup;
		}
		if (start_v4l2_capture(&cap) != 0) {
			fprintf(stderr,
				"[rdkx5_v4l2_mux_bridge][capture-lifecycle] event=uvc_start_reprepare_failed second_errno=%d action=fail_closed\n",
				errno);
			goto cleanup;
		}
		fprintf(stderr,
			"[rdkx5_v4l2_mux_bridge][capture-lifecycle] event=uvc_start_reprepare_completed first_errno=%d action=capture_poll\n",
			start_errno);
	}
	/* Codec/encoder initialization is not camera-stream time. Start the bounded
	 * admission window only once capture polling is ready. */
	camera_admission.started_us = monotonic_us();

	while (g_running) {
		pipeline_checkpoint(&phase_probe, "control_and_decoder_poll");
		if (video_broker_rdkx5_encoder_resources_quarantined()) { rc = 1; goto cleanup; }
		if (video_broker_rdkx5_decoder_resources_quarantined()) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge][capture-lifecycle]"
				" event=decoder_resources_quarantined action=retire_process\n");
			rc = 1;
			goto cleanup;
		}
		struct v4l2_buffer buf;
		const unsigned char *capture_source = NULL;
		size_t capture_source_size = 0;
		bool capture_buffer_requeued = false;
		bool mjpeg_async_frame_ready = false;
		vb_capture_origin capture_origin = {0,0}, decoded_frame_origin = {0,0};
		bool compressed_capture = is_compressed_h26x(cap.pixel_format);
		bool emit_sub = cfg.analysis_enabled;
		bool emit_main = runtime_control.main_active;
		bool emit_archive = false;
		bool defer_archive_cadence = false;
		bool publish_main = runtime_control.main_active;
		bool in_warmup;
		bool main_frame_ready = false;
		unsigned char *main_surface = live_role.frame;
		unsigned char *archive_surface = archive_role.frame;
		const unsigned char *quality_processed_surface = capture_frame_buffer;
		int quality_processed_width = cap.width;
		int quality_processed_height = cap.height;
		quality_observation_t quality_observation;
		int stream_size = 0;
		int wait_rc;
		int declared_capture_fps;
		int scheduling_fps;
		static int logged_first_sub = 0;
		static int logged_first_main = 0;
		static int logged_first_stream = 0;
		static int logged_first_main_resize = 0;
		static int logged_first_archive_resize = 0;
		uint64_t stage_start_us;
		uint64_t stage_end_us;
		uint64_t frame_work_start_us = 0;

		memset(&quality_observation, 0, sizeof(quality_observation));
		fanout_backend.capture_access_unit = NULL;
		fanout_backend.capture_access_unit_size = 0;
		fanout_backend.decoded_frame = NULL;
		fanout_backend.decoded_frame_ready = false;

		declared_capture_fps = cap.fps > 0 ? cap.fps : cfg.fps;
		if (declared_capture_fps <= 0) {
			declared_capture_fps = kDefaultFPS;
		}
		scheduling_fps = camera_admission.admitted &&
			camera_admission.decoded_cadence.effective_fps > 0 ?
			camera_admission.decoded_cadence.effective_fps : declared_capture_fps;
		{
			bool previous_optional_compute_admitted = runtime_control.optional_compute_admitted;
			uint64_t previous_keyframe_token = runtime_control.request_keyframe_token;
			int control_poll_rc = runtime_control_poll(
				&runtime_control,
				declared_capture_fps,
				fanout_enabled ? &fanout_controller : NULL);
			if (control_poll_rc < 0) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge][control] input channel closed or invalid\n");
				goto cleanup;
			}
			if (control_poll_rc > 0) {
				if (cfg.tone_mapping_enabled &&
				    previous_optional_compute_admitted != runtime_control.optional_compute_admitted) {
					if (!runtime_control.optional_compute_admitted) {
						if (video_broker_tone_accelerator_destroy(&tone_accelerator) != 0) goto cleanup;
						fprintf(stderr,
							"[rdkx5_v4l2_mux_bridge][control] optional_compute_admitted=false tone_accelerator=released scope=resource_governor\n");
					} else {
						if (video_broker_tone_accelerator_init(
								&tone_accelerator, cfg.tone_mapping_accelerator) != 0) {
							fprintf(stderr,
								"[rdkx5_v4l2_mux_bridge][control] optional compute resume failed requested=%s error=%s\n",
								video_broker_tone_accelerator_name(cfg.tone_mapping_accelerator),
								video_broker_tone_accelerator_error(&tone_accelerator));
							goto cleanup;
						}
						fprintf(stderr,
							"[rdkx5_v4l2_mux_bridge][control] optional_compute_admitted=true tone_accelerator=%s scope=resource_governor\n",
							video_broker_tone_accelerator_name(tone_accelerator.selected));
					}
				}
				if (persistent_main_control) {
					int control_rc = video_broker_rdkx5_persistent_encoder_set_control(
						live_role.encoder,
						runtime_control.main_active,
						runtime_control.main_fps,
						runtime_control.main_bitrate_kbps);
					if (control_rc < 0) {
						fprintf(stderr, "[rdkx5_v4l2_mux_bridge][control] persistent live rate-control failed\n");
						goto cleanup;
					}
					if (runtime_control.main_active) {
						live_role.pending_control_sequence = runtime_control.sequence;
						live_role.pending_control_ready = runtime_control.sequence > 0;
					} else {
						live_role.pending_control_ready = false;
					}
					if (control_rc == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED) {
						live_role.emit_accumulator = 0;
					} else if (control_rc == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_RATE_CHANGED) {
						live_role.active_bitrate_kbps = live_role.encoder->bitrate_kbps;
						live_role.emit_accumulator = 0;
					}
					if (runtime_control.request_keyframe_token > previous_keyframe_token &&
					    runtime_control.main_active && control_rc == 0) {
						if (video_broker_rdkx5_persistent_encoder_request_idr(live_role.encoder) != 0) {
							fprintf(stderr, "[rdkx5_v4l2_mux_bridge][control] live IDR request failed\n");
							goto cleanup;
						}
						fprintf(stderr,
							"[rdkx5_v4l2_mux_bridge][control] keyframe_requested token=%llu action=hardware_idr encoder_epoch=%llu\n",
							(unsigned long long)runtime_control.request_keyframe_token,
							(unsigned long long)fanout_backend.live_epoch.value);
					}
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][control] applied seq=%llu main_active=%s main_fps=%d main_bitrate_kbps=%d sub_fps=%d optional_compute_admitted=%s activation_changed=%s rate_control_changed=%s scope=live_persistent_rc\n",
						(unsigned long long)runtime_control.sequence,
						runtime_control.main_active ? "true" : "false",
						runtime_control.main_fps,
						runtime_control.main_bitrate_kbps,
						runtime_control.sub_fps,
						runtime_control.optional_compute_admitted ? "true" : "false",
						control_rc == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED ? "true" : "false",
						control_rc == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_RATE_CHANGED ? "true" : "false");
				} else {
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][control] applied seq=%llu main_active=%s main_fps=%d main_bitrate_kbps=%d sub_fps=%d optional_compute_admitted=%s encoder_epoch=%llu scope=%s\n",
						(unsigned long long)runtime_control.sequence,
						runtime_control.main_active ? "true" : "false",
						runtime_control.main_fps,
						runtime_control.main_bitrate_kbps,
						runtime_control.sub_fps,
						runtime_control.optional_compute_admitted ? "true" : "false",
						(unsigned long long)fanout_backend.live_epoch.value,
							direct_main_native ? "native_passthrough_static" : "live_persistent_rc");
				}
				if (fanout_backend.active_present[VIDEO_BROKER_WORKER_ROLE_LIVE]) {
					live_role.profile.fps = runtime_control.main_fps;
					live_role.profile.bitrate_kbps = runtime_control.main_bitrate_kbps;
					live_role.delivery_active = runtime_control.main_active;
				}
				if (fanout_backend.active_present[VIDEO_BROKER_WORKER_ROLE_ANALYSIS]) {
					analysis_role.profile.fps = runtime_control.sub_fps;
				}
			}
		}
		if (mjpeg_trial_decoder_reset_pending || mjpeg_base_decoder_close_pending) {
			int reap_rc = video_broker_rdkx5_async_decoder_reap(&mjpeg_async_decoder);
			if (reap_rc < 0) {
				goto cleanup;
			}
			if (reap_rc > 0) {
				if (mjpeg_trial_decoder_reset_pending) {
					if (video_broker_rdkx5_mjpeg_trial_recycle(&mjpeg_trial) != 0) {
						goto cleanup;
					}
					mjpeg_trial_decoder_reset_pending = false;
				}
				if (mjpeg_base_decoder_close_pending) {
					mjpeg_base_decoder_close_pending = false;
				}
				if (mjpeg_cancel_ack_pending_token != 0) {
					mjpeg_last_cancel_ack_token = mjpeg_cancel_ack_pending_token;
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_canceled component=native.jpu retry_token=%llu\n",
						(unsigned long long)mjpeg_cancel_ack_pending_token);
					mjpeg_cancel_ack_pending_token = 0;
				}
				mjpeg_async_replacements_observed = 0;
			}
		}
		if (cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
		    mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU) {
			uint64_t replacements = 0;
			int async_rc = video_broker_rdkx5_async_decoder_poll(
				&mjpeg_async_decoder,
				capture_frame_buffer,
				capture_frame_buffer_size,
				&replacements, &decoded_frame_origin);
			if (replacements >= mjpeg_async_replacements_observed) {
				runtime_metrics.mjpeg_decode_queue_replacements +=
					replacements - mjpeg_async_replacements_observed;
			} else {
				runtime_metrics.mjpeg_decode_queue_replacements += replacements;
			}
			mjpeg_async_replacements_observed = replacements;
			if (async_rc < 0) {
				uint64_t failed_trial_token = mjpeg_active_trial_token;
				video_broker_rdkx5_async_decoder_request_stop(&mjpeg_async_decoder);
				if (video_broker_mjpeg_async_failure_action(
						cfg.mjpeg_decode_accelerator,
						cfg.allow_software_fallback) == VIDEO_BROKER_MJPEG_ASYNC_FAILURE_REJECT) {
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][camera-admission] status=rejected codec=mjpeg reason=decoder_runtime_failed backend=jpu requested=%s frames=%llu\n",
						mjpeg_accelerator_name(cfg.mjpeg_decode_accelerator),
						(unsigned long long)camera_admission.decoded_cadence.frames);
					goto cleanup;
				}
				if (mjpeg_trial_decoder_active) {
					mjpeg_trial_decoder_reset_pending = true;
					mjpeg_trial_decoder_active = false;
				} else {
					mjpeg_base_decoder_close_pending = true;
				}
				capture_decoder = &decoder;
				mjpeg_active_trial_token = 0;
				if (failed_trial_token != 0) {
					mjpeg_cancel_ack_pending_token = failed_trial_token;
				}
				if (jpeg == NULL) {
					jpeg = tjInitDecompress();
				}
				if (jpeg == NULL) {
					fprintf(stderr, "[rdkx5_v4l2_mux_bridge] turbojpeg runtime fallback init failed\n");
					goto cleanup;
				}
				mjpeg_decode_selected = VIDEO_BROKER_MJPEG_ACCELERATOR_CPU;
				mjpeg_decoder_attested = true;
				mjpeg_async_replacements_observed = 0;
				mjpeg_first_unacknowledged_submission_us = 0;
				if (failed_trial_token != 0) {
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_retry_frame_failed output=nv12 retry_token=%llu\n",
						(unsigned long long)failed_trial_token);
				} else {
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_runtime_failed output=nv12 retry_token=0\n");
				}
			} else {
				mjpeg_async_frame_ready = async_rc > 0;
				if (mjpeg_async_frame_ready) {
					mjpeg_first_unacknowledged_submission_us = 0;
					/* Completed output is detached from camera/SDK buffers. Publish
					 * BEFORE waiting for another capture, including late last output. */
					if (camera_admission.admitted && !(warmup_until > 0 && time(NULL) < warmup_until) &&
					    video_broker_frame_source_publish(&frame_source, decoded_frame_origin,
					        capture_frame_buffer, capture_frame_buffer_size, 1000) < 0) goto cleanup;
				}
			}
		}
		/*
		 * Accelerator trial lifecycle is serviced before capture wait/DQBUF.
		 * Joining a completed worker or closing a rejected candidate may enter
		 * vendor teardown, so neither operation may own a UVC buffer. The worker
		 * itself owns a copied access unit and cannot delay CPU capture/QBUF.
		 */
		if (cap.pixel_format == V4L2_PIX_FMT_MJPEG && mjpeg_trial.initialized) {
			video_broker_rdkx5_mjpeg_trial_result_t trial_result;
			uint64_t trial_token = 0;
			int64_t trial_deadline_ms = 0;
			int64_t trial_completed_ms = 0;
			int poll_rc;
			int64_t now_ms = realtime_ms();

			if (mjpeg_active_trial_token != 0) {
				bool committed_current =
					strcmp(runtime_control.retry_action, "commit") == 0 &&
					strcmp(runtime_control.retry_component, "native.jpu") == 0 &&
					runtime_control.retry_token == mjpeg_active_trial_token &&
					now_ms >= 0 && now_ms < runtime_control.retry_deadline_unix_ms;
				bool broker_accepted = strcmp(runtime_control.retry_action, "none") == 0;
				if (broker_accepted) {
					/* The broker consumed the exact committed-frame ACK. CPU is no
					 * longer needed as a rollback surface for this handshake. */
					if (jpeg != NULL) {
						tjDestroy(jpeg);
						jpeg = NULL;
					}
					mjpeg_active_trial_token = 0;
				} else if (!committed_current) {
					uint64_t canceled_token = mjpeg_active_trial_token;
					if (jpeg == NULL) {
						jpeg = tjInitDecompress();
						if (jpeg == NULL) {
							fprintf(stderr, "[rdkx5_v4l2_mux_bridge] turbojpeg rollback init failed\n");
							goto cleanup;
						}
					}
					video_broker_rdkx5_async_decoder_request_stop(&mjpeg_async_decoder);
					mjpeg_trial_decoder_reset_pending = true;
					capture_decoder = &decoder;
					mjpeg_decode_selected = VIDEO_BROKER_MJPEG_ACCELERATOR_CPU;
					mjpeg_decoder_attested = true;
					mjpeg_trial_decoder_active = false;
					mjpeg_active_trial_token = 0;
					mjpeg_cancel_ack_pending_token = canceled_token;
				}
			}

			if (mjpeg_active_trial_token == 0 && !mjpeg_candidate_ready) {
				poll_rc = video_broker_rdkx5_mjpeg_trial_poll(
					&mjpeg_trial,
					&trial_result,
					&trial_token,
					&trial_deadline_ms,
					&trial_completed_ms);
				if (poll_rc < 0) {
					fprintf(stderr, "[rdkx5_v4l2_mux_bridge] MJPEG trial poll failed\n");
					goto cleanup;
				}
				if (poll_rc > 0) {
					bool current_trial =
						strcmp(runtime_control.retry_action, "trial") == 0 &&
						strcmp(runtime_control.retry_component, "native.jpu") == 0 &&
						runtime_control.retry_token == trial_token &&
						runtime_control.retry_deadline_unix_ms == trial_deadline_ms;
					mjpeg_worker_token = 0;
					if (trial_result == VIDEO_BROKER_RDKX5_MJPEG_TRIAL_SUCCESS &&
					    current_trial && trial_completed_ms > 0 &&
					    trial_completed_ms < trial_deadline_ms && now_ms >= 0 &&
					    now_ms < trial_deadline_ms) {
						mjpeg_candidate_ready = true;
						mjpeg_candidate_token = trial_token;
						mjpeg_candidate_completed_unix_ms = trial_completed_ms;
						fprintf(stderr,
							"[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_ready component=native.jpu retry_token=%llu completed_unix_ms=%lld\n",
							(unsigned long long)trial_token,
							(long long)trial_completed_ms);
					} else {
						if (current_trial) {
							const char *fallback =
								trial_result == VIDEO_BROKER_RDKX5_MJPEG_TRIAL_OPEN_FAILED ?
								"jpu_retry_open_failed" :
								trial_result == VIDEO_BROKER_RDKX5_MJPEG_TRIAL_DEADLINE_FAILED ?
								"jpu_retry_deadline_failed" : "jpu_retry_frame_failed";
							fprintf(stderr,
								"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=%s output=nv12 retry_token=%llu\n",
								fallback, (unsigned long long)trial_token);
						}
						if (trial_result == VIDEO_BROKER_RDKX5_MJPEG_TRIAL_SUCCESS) {
							if (video_broker_rdkx5_mjpeg_trial_begin_async_reset(
									&mjpeg_trial, &mjpeg_async_decoder) != 0) {
								fprintf(stderr, "[rdkx5_v4l2_mux_bridge] completed JPU candidate async reset failed\n");
								goto cleanup;
							}
							mjpeg_trial_decoder_reset_pending = true;
							mjpeg_cancel_ack_pending_token = trial_token;
						} else if (video_broker_rdkx5_mjpeg_trial_recycle(&mjpeg_trial) != 0) {
							fprintf(stderr, "[rdkx5_v4l2_mux_bridge] completed JPU trial reset failed\n");
							goto cleanup;
						}
					}
				}
			}

			if (mjpeg_candidate_ready) {
				bool commit_current =
					strcmp(runtime_control.retry_action, "commit") == 0 &&
					strcmp(runtime_control.retry_component, "native.jpu") == 0 &&
					runtime_control.retry_token == mjpeg_candidate_token &&
					now_ms >= 0 && now_ms < runtime_control.retry_deadline_unix_ms;
				bool trial_still_current =
					strcmp(runtime_control.retry_action, "trial") == 0 &&
					strcmp(runtime_control.retry_component, "native.jpu") == 0 &&
					runtime_control.retry_token == mjpeg_candidate_token;
				if (commit_current) {
					mjpeg_switch_ready = true;
					mjpeg_commit_deadline_unix_ms = runtime_control.retry_deadline_unix_ms;
				} else if (!trial_still_current ||
				           now_ms < 0 || now_ms >= runtime_control.retry_deadline_unix_ms) {
					uint64_t canceled_token = mjpeg_candidate_token;
					if (video_broker_rdkx5_mjpeg_trial_begin_async_reset(
							&mjpeg_trial, &mjpeg_async_decoder) != 0) {
						fprintf(stderr, "[rdkx5_v4l2_mux_bridge] stale JPU candidate async reset failed\n");
						goto cleanup;
					}
					mjpeg_trial_decoder_reset_pending = true;
					mjpeg_candidate_ready = false;
					mjpeg_switch_ready = false;
					mjpeg_candidate_token = 0;
					mjpeg_candidate_completed_unix_ms = 0;
					mjpeg_commit_deadline_unix_ms = 0;
					mjpeg_cancel_ack_pending_token = canceled_token;
				}
			}

			if (strcmp(runtime_control.retry_action, "cancel") == 0 &&
			    runtime_control.retry_token != 0 &&
			    runtime_control.retry_token != mjpeg_worker_token &&
			    runtime_control.retry_token != mjpeg_candidate_token &&
			    runtime_control.retry_token != mjpeg_active_trial_token &&
			    runtime_control.retry_token != mjpeg_cancel_ack_pending_token &&
			    runtime_control.retry_token != mjpeg_last_cancel_ack_token) {
				mjpeg_last_cancel_ack_token = runtime_control.retry_token;
				fprintf(stderr,
					"[rdkx5_v4l2_mux_bridge][capture-decoder-trial] event=candidate_canceled component=native.jpu retry_token=%llu\n",
					(unsigned long long)runtime_control.retry_token);
			}
		}
		defer_archive_cadence = cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
			mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU;
		emit_sub = cfg.analysis_enabled;
		emit_main = runtime_control.main_active;
		publish_main = runtime_control.main_active;
		main_surface = live_role.frame;
		archive_surface = archive_role.frame;
		fanout_backend.scheduling_fps = scheduling_fps;

		pipeline_checkpoint(&phase_probe, "capture_wait");
		stage_start_us = monotonic_us();
		wait_rc = wait_for_capture_frame(cap.fd, cfg.capture_select_timeout_ms);
		pipeline_checkpoint(&phase_probe, "capture_dequeue");
		stage_end_us = monotonic_us();
		metrics_add_stage(&runtime_metrics.wait_us, &runtime_metrics.wait_max_us, stage_start_us, stage_end_us);
		if (wait_rc == 0) {
			capture_timeout_count++;
			runtime_metrics.capture_timeouts++;
			metrics_log_if_due(&runtime_metrics, &frame_source, stage_end_us, false);
			if (cfg.capture_max_consecutive_timeouts > 0 && capture_timeout_count >= cfg.capture_max_consecutive_timeouts) {
				fprintf(stderr,
					"[rdkx5_v4l2_mux_bridge] capture stalled after %d consecutive timeouts timeout_ms=%d; exiting for supervisor recovery\n",
					capture_timeout_count,
					cfg.capture_select_timeout_ms);
				goto cleanup;
			}
			if (fanout_enabled &&
			    v4l2_fanout_resume_and_publish(&fanout_controller) != 0) {
				goto cleanup;
			}
			continue;
		}
		if (wait_rc < 0) {
			goto cleanup;
		}
		capture_timeout_count = 0;
		if (!g_running) {
			continue;
		}
		if (runtime_metrics.last_capture_us > 0 && stage_end_us > runtime_metrics.last_capture_us) {
			uint64_t gap_us = stage_end_us - runtime_metrics.last_capture_us;
			if (gap_us > runtime_metrics.capture_gap_max_us) {
				runtime_metrics.capture_gap_max_us = gap_us;
			}
		}
		runtime_metrics.last_capture_us = stage_end_us;
		frame_work_start_us = stage_end_us;

		memset(&buf, 0, sizeof(buf));
		buf.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
		buf.memory = V4L2_MEMORY_MMAP;
		stage_start_us = monotonic_us();
		if (xioctl(cap.fd, VIDIOC_DQBUF, &buf) != 0) {
			if (errno == EAGAIN) {
				continue;
			}
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] VIDIOC_DQBUF failed err=%s\n", strerror(errno));
			goto cleanup;
		}
		stage_end_us = monotonic_us();
		metrics_add_stage(&runtime_metrics.dqbuf_us, NULL, stage_start_us, stage_end_us);
		if (buf.index >= cap.buffer_count) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] invalid buffer index=%u count=%u\n", buf.index, cap.buffer_count);
			goto cleanup;
		}
		capture_source = (const unsigned char *)cap.buffers[buf.index].start;
		capture_source_size = buf.bytesused;
		/* Physical discontinuity is not by itself proof of a missing codec
		 * reference (startup buffers may contain no coded picture). Before
		 * admission, gate output until a fresh IDR and rebuild stable cadence.
		 * After admission, retire the source before any downstream publication.
		 * Unsigned subtraction permits sequence wrap without reuse. */
		if (compressed_capture && capture_sequence_seen &&
		    !capture_sequence_contiguous(capture_previous_sequence, buf.sequence)) {
			uint32_t discontinuity_delta = (uint32_t)(buf.sequence - capture_previous_sequence);
			bool discontinuity_tolerable = camera_admission.admitted &&
				discontinuity_delta <= VB_DISCONTINUITY_MAX_DELTA &&
				capture_discontinuity_tolerated < VB_DISCONTINUITY_MAX_TOLERATED;
			const char *discontinuity_action = "await_fresh_idr_no_publication";
			if (discontinuity_tolerable) {
				discontinuity_action = "rebuild_reference_gate";
			} else if (camera_admission.admitted) {
				discontinuity_action = "retire_source_before_publication";
			}
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge][capture-integrity]"
				" event=interframe_capture_discontinuity format=%s previous=%u current=%u"
				" delta=%u bytes=%u flags=%u admitted=%s tolerated=%u action=%s\n",
				format_name(cap.pixel_format), capture_previous_sequence, buf.sequence,
				discontinuity_delta, buf.bytesused, buf.flags,
				camera_admission.admitted ? "true" : "false",
				capture_discontinuity_tolerated, discontinuity_action);
			if (!discontinuity_tolerable && camera_admission.admitted) {
				metrics_log_if_due(&runtime_metrics, &frame_source, monotonic_us(), true);
				rc = 1;
				goto cleanup;
			}
			if (discontinuity_tolerable) {
				capture_discontinuity_tolerated++;
				capture_resync_deadline_us = monotonic_us() + VB_DISCONTINUITY_RESYNC_BUDGET_US;
			}
			capture_waiting_idr = true;
			capture_clean_origin = (vb_capture_origin){0,0};
			camera_admission.seen_keyframe = false;
			video_broker_stable_cadence_init(&camera_admission.decoded_cadence,
				camera_admission.decoded_cadence.expected_fps, 3,
				camera_admission.decoded_cadence.maximum_gap_us);
		}
		capture_previous_sequence = buf.sequence;
		capture_sequence_seen = true;
		pipeline_checkpoint(&phase_probe, "source_authority");
		if (video_broker_frame_source_next(&frame_source, &capture_origin) < 0) goto cleanup;
		pipeline_checkpoint(&phase_probe, "capture_detach_requeue");
		if (cap.pixel_format == V4L2_PIX_FMT_MJPEG || frame_source.enabled) {
			if (capture_source_size == 0 ||
			    capture_source_size > cap.buffers[buf.index].length ||
			    capture_source_size > capture_input_copy_capacity) {
				if (capture_corrupt_frame_should_restart(
					    &runtime_metrics,
					    &corrupt_frame_run,
					    cap.pixel_format,
					    "invalid_capture_payload_extent")) {
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge] corrupt capture limit reached format=%s bytes=%zu capacity=%zu; exiting for supervisor recovery\n",
						format_name(cap.pixel_format), capture_source_size, capture_input_copy_capacity);
					goto cleanup;
				}
				goto requeue;
			}
			memcpy(capture_input_copy, capture_source, capture_source_size);
			capture_source = capture_input_copy;
			/* Return the driver lease before decoder or source transport I/O.
			 * FAP extends this existing detached MJPEG path to all input formats. */
			if (xioctl(cap.fd, VIDIOC_QBUF, &buf) != 0) {
				fprintf(stderr,
					"[rdkx5_v4l2_mux_bridge] VIDIOC_QBUF early detached capture requeue failed err=%s\n",
					strerror(errno));
				goto cleanup;
			}
			capture_buffer_requeued = true;
			if (!capture_detach_reported) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge][capture-detach] stage=ready"
					" format=%s bytes=%zu index=%u sequence=%u driver_lease=returned"
					" payload=owned_copy metadata=dequeue_snapshot\n",
					format_name(cap.pixel_format), capture_source_size, buf.index, buf.sequence);
				capture_detach_reported = true;
			}
		}

		pipeline_checkpoint(&phase_probe, "decode_and_admission");
		/* JPU can finish while select/DQBUF waits for the next camera frame.
		 * A single pre-wait poll misses that completion for a whole capture tick;
		 * alternating misses then alias the independent analysis cadence. Check
		 * once more after returning the UVC lease, only if no output was taken.
		 * Never replace an already selected origin or wait/spin for the decoder. */
		if (cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
		    mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU &&
		    !mjpeg_async_frame_ready) {
			uint64_t replacements = 0;
			int async_rc = video_broker_rdkx5_async_decoder_poll(
				&mjpeg_async_decoder, capture_frame_buffer, capture_frame_buffer_size,
				&replacements, &decoded_frame_origin);
			/* The next pre-wait poll owns existing strict/fallback fault policy. */
			if (async_rc < 0) goto requeue;
			if (async_rc > 0) {
				mjpeg_async_frame_ready = true;
				mjpeg_first_unacknowledged_submission_us = 0;
				runtime_metrics.mjpeg_decode_post_wait_frames++;
				if (camera_admission.admitted && !(warmup_until > 0 && time(NULL) < warmup_until) &&
				    video_broker_frame_source_publish(&frame_source, decoded_frame_origin,
				        capture_frame_buffer, capture_frame_buffer_size, 1000) < 0) goto cleanup;
			}
		}

		in_warmup = (warmup_until > 0 && time(NULL) < warmup_until) || !camera_admission.admitted;
		if (!in_warmup && !metrics_steady_window_started) {
			/* Admission and configured warmup are startup work, not delivered
			 * throughput. Start the first telemetry window at the first eligible
			 * frame so its FPS cannot be diluted by codec admission or ISP settling. */
			memset(&runtime_metrics, 0, sizeof(runtime_metrics));
			runtime_metrics.last_log_us = stage_end_us;
			runtime_metrics.last_capture_us = stage_end_us;
			metrics_steady_window_started = true;
		}
		if (in_warmup) {
			/*
			 * A native H.26x camera publishes its codec configuration at stream
			 * startup.  Feed those access units to the persistent hardware decoder
			 * while suppressing external output; dropping them would make the first
			 * post-warmup P-frame look like an invalid sequence to the decoder.
			 */
			emit_sub = false;
			emit_main = false;
		} else {
			runtime_metrics.frames++;
			if (!defer_archive_cadence) {
				emit_archive = cfg.archive_enabled && video_broker_cadence_due(
					&archive_role.emit_accumulator, cfg.archive_fps, scheduling_fps);
			}
			if (cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
			    mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU) {
				/* The async worker must receive every access unit.  Its completed
				 * frames are cadence-capped after poll/decode instead of dropping
				 * input and output independently. */
				emit_sub = false;
			} else if (runtime_control.sub_fps < scheduling_fps) {
				analysis_role.emit_accumulator += runtime_control.sub_fps;
				if (analysis_role.emit_accumulator < scheduling_fps) {
					emit_sub = false;
				} else {
					analysis_role.emit_accumulator -= scheduling_fps;
				}
			}
			if (runtime_control.main_active && runtime_control.main_fps < scheduling_fps) {
				live_role.emit_accumulator += runtime_control.main_fps;
				if (live_role.emit_accumulator < scheduling_fps) {
					emit_main = false;
				} else {
					live_role.emit_accumulator -= scheduling_fps;
				}
			}
			if (!runtime_control.main_active) {
				emit_main = false;
				publish_main = false;
			}
		}
		if (compressed_capture) {
			const unsigned char *access_unit = capture_source;
			uint8_t archive_stream_id = cap.pixel_format == V4L2_PIX_FMT_HEVC ?
				kArchiveH265StreamID : kArchiveH264StreamID;
			if (buf.bytesused == 0 || buf.bytesused > cap.buffers[buf.index].length ||
				!is_annexb_access_unit(access_unit, buf.bytesused)) {
				if (capture_corrupt_frame_should_restart(
					    &runtime_metrics,
					    &corrupt_frame_run,
					    cap.pixel_format,
					    "invalid_native_access_unit")) {
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge] corrupt capture limit reached format=%s bytes=%u capacity=%zu; exiting for supervisor recovery\n",
						format_name(cap.pixel_format), buf.bytesused, cap.buffers[buf.index].length);
					goto cleanup;
				}
				goto requeue;
			}
			if (capture_waiting_idr) {
				int picture = video_broker_h26x_first_picture(
					cap.pixel_format == V4L2_PIX_FMT_HEVC, access_unit, buf.bytesused);
				if (picture < 0) { rc = 1; goto cleanup; }
				if (picture == 1) {
					uint64_t resync_now_us = monotonic_us();
					bool resync_expired = capture_resync_deadline_us != 0 &&
						resync_now_us > capture_resync_deadline_us;
					if (resync_expired ||
					    camera_admission_timed_out(&camera_admission, resync_now_us)) {
						fprintf(stderr, "[rdkx5_v4l2_mux_bridge][capture-integrity]"
							" event=%s format=%s action=retire_source_before_publication\n",
							resync_expired ? "interframe_resync_timeout" : "startup_resync_timeout",
							format_name(cap.pixel_format));
						rc = 1;
						goto cleanup;
					}
					goto requeue;
				}
				if (picture == 2) {
					capture_waiting_idr = false;
					capture_resync_deadline_us = 0;
					capture_clean_origin = capture_origin;
					fprintf(stderr, "[rdkx5_v4l2_mux_bridge][capture-integrity]"
						" event=startup_resync_idr source_epoch=%llu origin=%llu"
						" action=await_exact_decoded_origin_and_stable_cadence\n",
						(unsigned long long)capture_origin.epoch,
						(unsigned long long)capture_origin.sequence);
				}
			}
			if (cap.pixel_format == V4L2_PIX_FMT_HEVC && decoder.started) {
				bool had_sequence = hevc_sequence.seen_sequence;
				int sequence_result = vb_hevc_guard_access_unit(&hevc_sequence,
					access_unit, buf.bytesused, (uint32_t)cap.width, (uint32_t)cap.height);
				if (sequence_result < 0) {
					fprintf(stderr, "[rdkx5_v4l2_mux_bridge][decoder-admission] event=sequence_rejected"
						" codec=h265 expected=%dx%d bytes=%u reason=invalid_or_outside_surface_dpb_limits"
						" action=stop_before_native_submission\n", cap.width, cap.height, buf.bytesused);
					goto cleanup;
				}
				if (sequence_result == 0) goto requeue; /* wait for current execution's SPS */
				if (!had_sequence && hevc_sequence.seen_sequence) {
					fprintf(stderr, "[rdkx5_v4l2_mux_bridge][decoder-admission] event=sequence_admitted"
						" codec=h265 visible=%dx%d coded=%ux%u bit_depth=8 chroma=420"
						" dpb_frames=%u reorder_frames=%u scope=sequence_not_total_sdk_memory\n",
						cap.width, cap.height, hevc_sequence.sequence.coded_width,
						hevc_sequence.sequence.coded_height, hevc_sequence.sequence.dpb_frames,
						hevc_sequence.sequence.reorder_frames);
				}
			}
			camera_admission_observe_access_unit(&camera_admission, access_unit, buf.bytesused);
			if (!in_warmup) {
				if (archive_native_passthrough && emit_archive) {
					stage_start_us = monotonic_us();
					if (write_mux_frame(out, archive_stream_id, access_unit, buf.bytesused) != 0) {
						rc = 0;
						goto cleanup;
					}
					stage_end_us = monotonic_us();
					runtime_metrics.archive_frames++;
					v4l2_role_observe_bootstrap(&archive_role, access_unit, buf.bytesused);
					archive_role.frames++;
					metrics_add_stage(
						&runtime_metrics.write_archive_us,
						&runtime_metrics.write_archive_max_us,
						stage_start_us,
						stage_end_us);
				}
				if (direct_main_native && publish_main) {
					stage_start_us = monotonic_us();
					if (write_mux_frame(out, kMainStreamID, access_unit, buf.bytesused) != 0) {
						rc = 0;
						goto cleanup;
					}
					stage_end_us = monotonic_us();
					runtime_metrics.main_frames++;
					v4l2_role_observe_bootstrap(&live_role, access_unit, buf.bytesused);
					live_role.frames++;
					metrics_add_stage(
						&runtime_metrics.write_main_us,
						&runtime_metrics.write_main_max_us,
						stage_start_us,
						stage_end_us);
					emit_main = false;
				}
			}
		}
		/*
		 * A compressed inter-frame stream must be submitted in full even when
		 * neither derived output is due on this capture tick.  Output cadence is
		 * applied after decode; dropping an input AU here breaks reference chains
		 * and compounds the requested 20/30 sampling ratio.
		 */
		/*
		 * The archive is an independent always-on output. Do not let the lower
		 * analysis cadence short-circuit decode/process/archive work for raw or
		 * intra-frame capture formats (notably MJPEG).  Doing so silently capped
		 * the archive at sub_fps while advertising capture_fps, which made the
		 * runtime attestation reject an otherwise healthy camera and restart it.
		 */
		if (!emit_main && !emit_sub && !compressed_capture &&
		    (!cfg.tone_mapping_enabled || !runtime_control.optional_compute_admitted) &&
		    !(archive_encode_required && emit_archive) &&
		    !(fanout_enabled && fanout_backend.change_count > 0 &&
		      !fanout_backend.cutover_completed) &&
		    !(cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
		      mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU) &&
		    camera_admission.admitted) {
			goto requeue;
		}

		/* Check encoded geometry before either JPU submission or a retry trial
		 * can allocate sequence-dependent output. CPU/JPU take the same gate;
		 * matching V4L2 negotiation alone is not proof of JPEG dimensions. */
		if (cap.pixel_format == V4L2_PIX_FMT_MJPEG) {
			video_broker_jpeg_frame_header header;
			bool valid = video_broker_jpeg_frame_inspect(capture_source, capture_source_size, &header);
			if (!valid || header.width != cap.width || header.height != cap.height) {
				if (capture_corrupt_frame_should_restart(&runtime_metrics, &corrupt_frame_run,
					cap.pixel_format, valid ? "jpeg_geometry_mismatch_before_decode" : "jpeg_envelope_invalid_before_decode")) {
					fprintf(stderr, "[rdkx5_v4l2_mux_bridge][capture-decoder] event=input_rejected"
						" expected=%dx%d observed=%ux%u bytes=%zu sdk_submission=false action=retire_source\n",
						cap.width, cap.height, (unsigned)header.width, (unsigned)header.height, capture_source_size);
					goto cleanup;
				}
				goto requeue;
			}
		}
		stage_start_us = monotonic_us();
		{
			int decode_rc;
			if (cap.pixel_format == V4L2_PIX_FMT_MJPEG && runtime_control.trial_pending &&
			    mjpeg_worker_token == 0 && !mjpeg_candidate_ready &&
			    mjpeg_active_trial_token == 0 && !mjpeg_trial_decoder_reset_pending &&
			    !mjpeg_base_decoder_close_pending) {
				uint64_t retry_token = runtime_control.retry_token;
				int64_t deadline_ms = runtime_control.retry_deadline_unix_ms;
				runtime_control.trial_pending = false;
				runtime_control.last_retry_token = retry_token;
				if (mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_CPU &&
				    strcmp(runtime_control.retry_action, "trial") == 0 &&
				    strcmp(runtime_control.retry_component, "native.jpu") == 0) {
					int64_t now_ms = realtime_ms();
					if (now_ms < 0 || now_ms >= deadline_ms) {
						fprintf(stderr,
							"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_retry_deadline_failed output=nv12 retry_token=%llu\n",
							(unsigned long long)retry_token);
					} else if (video_broker_rdkx5_mjpeg_trial_start(
							&mjpeg_trial,
							capture_source,
							capture_source_size,
							retry_token,
							kMJPEGTrialFrameTimeoutMs,
							deadline_ms) == 0) {
						mjpeg_worker_token = retry_token;
					} else {
						fprintf(stderr,
							"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_retry_open_failed output=nv12 retry_token=%llu\n",
							(unsigned long long)retry_token);
					}
				}
			}
			if (cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
			    mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU) {
				int submit_rc = video_broker_rdkx5_async_decoder_submit(
					&mjpeg_async_decoder, capture_source, capture_source_size, capture_origin);
				if (submit_rc > 0 && mjpeg_first_unacknowledged_submission_us == 0) {
					mjpeg_first_unacknowledged_submission_us = monotonic_us();
				}
				decode_rc = submit_rc < 0 ? -1 : (mjpeg_async_frame_ready ? 0 : 1);
			} else {
				decode_rc = decode_capture_frame_to_nv12(
				    jpeg,
				    capture_decoder,
				    capture_source,
				    capture_source_size,
				    cap.pixel_format,
				    capture_frame_buffer,
				    decode_scratch_buffer,
				    cap.width,
				    cap.height,
				    capture_origin,
				    &decoded_frame_origin);
			}
			if (decode_rc < 0 && cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
			    mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU &&
			    cfg.mjpeg_decode_accelerator == VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO &&
			    cfg.allow_software_fallback) {
				uint64_t failed_trial_token = mjpeg_active_trial_token;
				video_broker_rdkx5_async_decoder_request_stop(&mjpeg_async_decoder);
				if (mjpeg_trial_decoder_active) {
					mjpeg_trial_decoder_reset_pending = true;
					mjpeg_active_trial_token = 0;
					mjpeg_trial_decoder_active = false;
				} else {
					mjpeg_base_decoder_close_pending = true;
				}
				if (failed_trial_token != 0) {
					mjpeg_cancel_ack_pending_token = failed_trial_token;
				}
				capture_decoder = &decoder;
				if (jpeg == NULL) {
					jpeg = tjInitDecompress();
				}
				if (jpeg != NULL) {
					mjpeg_decode_selected = VIDEO_BROKER_MJPEG_ACCELERATOR_CPU;
					mjpeg_first_unacknowledged_submission_us = 0;
					if (failed_trial_token != 0) {
						fprintf(stderr,
							"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_retry_frame_failed output=nv12 retry_token=%llu\n",
							(unsigned long long)failed_trial_token);
					} else {
						fprintf(stderr,
							"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_runtime_failed output=nv12 retry_token=0\n");
					}
					decode_rc = mjpeg_to_nv12(
						jpeg,
						capture_source,
						capture_source_size,
						capture_frame_buffer,
						decode_scratch_buffer,
						cap.width,
						cap.height);
					if (decode_rc == 0) decoded_frame_origin = capture_origin;
				}
			}
			if (decode_rc < 0) {
				if (capture_corrupt_frame_should_restart(
					    &runtime_metrics,
					    &corrupt_frame_run,
					    cap.pixel_format,
					    "decode_failed")) {
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge] corrupt capture limit reached format=%s; exiting for supervisor recovery\n",
						format_name(cap.pixel_format));
					goto cleanup;
				}
				goto requeue;
			}
			if (decode_rc == 0 && cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
			    mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU && !mjpeg_decoder_attested) {
				mjpeg_decoder_attested = true;
				fprintf(stderr,
					"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=%s selected=jpu fallback=none output=nv12 retry_token=0\n",
					mjpeg_accelerator_name(cfg.mjpeg_decode_accelerator));
			}
			corrupt_frame_run = 0;
			if (decode_rc > 0) {
				uint64_t no_output_now_us = monotonic_us();
				bool runtime_output_stalled = video_broker_mjpeg_decoder_output_stalled(
					camera_admission.admitted,
					mjpeg_first_unacknowledged_submission_us,
					no_output_now_us,
					kMJPEGDecoderOutputStallTimeoutUs);
				bool admission_output_timed_out =
					camera_admission_timed_out(&camera_admission, no_output_now_us);
				runtime_metrics.decode_no_output++;
				runtime_metrics.decode_no_output_run++;
				if (runtime_metrics.decode_no_output_run > runtime_metrics.decode_no_output_run_max) {
					runtime_metrics.decode_no_output_run_max = runtime_metrics.decode_no_output_run;
				}
				if ((admission_output_timed_out || runtime_output_stalled) &&
				    cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
				    mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU &&
				    cfg.mjpeg_decode_accelerator == VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO &&
				    cfg.allow_software_fallback) {
					uint64_t failed_trial_token = mjpeg_active_trial_token;
					video_broker_rdkx5_async_decoder_request_stop(&mjpeg_async_decoder);
					if (mjpeg_trial_decoder_active) {
						mjpeg_trial_decoder_reset_pending = true;
						mjpeg_trial_decoder_active = false;
					} else {
						mjpeg_base_decoder_close_pending = true;
					}
					if (failed_trial_token != 0) {
						mjpeg_cancel_ack_pending_token = failed_trial_token;
					}
					capture_decoder = &decoder;
					mjpeg_active_trial_token = 0;
					if (jpeg == NULL) {
						jpeg = tjInitDecompress();
					}
					if (jpeg == NULL) {
						fprintf(stderr, "[rdkx5_v4l2_mux_bridge] turbojpeg no-output fallback init failed\n");
						goto cleanup;
					}
					mjpeg_decode_selected = VIDEO_BROKER_MJPEG_ACCELERATOR_CPU;
					mjpeg_decoder_attested = true;
					mjpeg_async_replacements_observed = 0;
					mjpeg_first_unacknowledged_submission_us = 0;
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=%s output=nv12 retry_token=%llu\n",
						failed_trial_token != 0 ? "jpu_retry_frame_failed" : "jpu_runtime_failed",
						(unsigned long long)failed_trial_token);
					camera_admission_init(&camera_admission, cap.pixel_format, declared_capture_fps,
						(uint64_t)cfg.capture_select_timeout_ms * (uint64_t)cfg.capture_max_consecutive_timeouts * 1000U);
					runtime_metrics.decode_no_output_run = 0;
					corrupt_frame_run = 0;
					goto requeue;
				}
				if (admission_output_timed_out || runtime_output_stalled) {
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][camera-admission] status=rejected codec=%s reason=decoder_no_output_timeout frames=%llu\n",
						format_name(camera_admission.codec),
						(unsigned long long)camera_admission.decoded_cadence.frames);
					goto cleanup;
				}
				goto requeue;
			}
			runtime_metrics.decode_no_output_run = 0;
		}
		stage_end_us = monotonic_us();
		/* Late decoded surfaces from before the recovery IDR remain unusable,
		 * even if their exact provider tag is valid. Never relabel old output. */
		if (!capture_recovery_output_ready(capture_waiting_idr,
		    capture_clean_origin, decoded_frame_origin)) goto requeue;
		(void)camera_admission_observe_decoded_frame(&camera_admission, stage_end_us);
		pipeline_checkpoint(&phase_probe, "native_surface_publish");
		if (!(cap.pixel_format == V4L2_PIX_FMT_MJPEG && mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU) &&
		    camera_admission.admitted && !in_warmup) {
			if (video_broker_frame_source_publish(&frame_source, decoded_frame_origin,
			    capture_frame_buffer, capture_frame_buffer_size, 1000) < 0) goto cleanup;
		}
		pipeline_checkpoint(&phase_probe, "fanout_and_main_transform");
		if (camera_admission_timed_out(&camera_admission, stage_end_us)) {
			fprintf(stderr,
				"[rdkx5_v4l2_mux_bridge][camera-admission] status=rejected codec=%s reason=runtime_attestation_timeout frames=%llu observed_fps=%.2Lf expected_fps=%d effective_fps=%d settling_resets=%llu vps=%s sps=%s pps=%s keyframe=%s\n",
				format_name(camera_admission.codec),
				(unsigned long long)camera_admission.decoded_cadence.frames,
				camera_admission.decoded_cadence.observed_fps,
				camera_admission.decoded_cadence.expected_fps,
				camera_admission.decoded_cadence.effective_fps,
				(unsigned long long)camera_admission.decoded_cadence.settling_resets,
				camera_admission.seen_vps ? "true" : "false",
				camera_admission.seen_sps ? "true" : "false",
				camera_admission.seen_pps ? "true" : "false",
				camera_admission.seen_keyframe ? "true" : "false");
			goto cleanup;
		}
		fanout_backend.capture_access_unit = capture_source;
		fanout_backend.capture_access_unit_size = capture_source_size;
		fanout_backend.decoded_frame = capture_frame_buffer;
		fanout_backend.decoded_frame_ready = true;
		if (fanout_enabled && !in_warmup) {
			(void)v4l2_fanout_process_candidates(&fanout_backend);
			if (v4l2_fanout_resume_and_publish(&fanout_controller) != 0) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge][fanout] receipt write failed\n");
				goto cleanup;
			}
		}
		if (in_warmup) {
			/* Learn and prewarm the shared corrected capture surface before it is
			 * externally visible. This hides OpenCL allocation/JIT latency and lets
			 * temporal/local mode selection settle inside the declared warmup. */
			if (tone_map_capture_frame && runtime_control.optional_compute_admitted && tone_map_processed_frame(
					&tone_mapper, &tone_accelerator, &cfg, &runtime_metrics,
					capture_frame_buffer, cap.width, cap.height,
					&tone_mapper_last_log_us) != 0) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] tone accelerator warmup failed error=%s\n",
					video_broker_tone_accelerator_error(&tone_accelerator));
				goto cleanup;
			}
			goto requeue;
		}
		if (defer_archive_cadence) {
			emit_archive = cfg.archive_enabled && video_broker_cadence_due(
				&archive_role.emit_accumulator, cfg.archive_fps, scheduling_fps);
		}
		if (cap.pixel_format == V4L2_PIX_FMT_MJPEG &&
		    mjpeg_decode_selected == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU &&
		    cfg.analysis_enabled && mjpeg_async_frame_ready) {
			emit_sub = video_broker_mjpeg_async_output_due(
				&analysis_role.async_next_due_us,
				runtime_control.sub_fps,
				stage_end_us);
		}
		if (!emit_main && !emit_sub && !(archive_encode_required && emit_archive) &&
		    !(fanout_enabled && fanout_backend.change_count > 0 &&
		      !fanout_backend.cutover_completed)) {
			goto requeue;
		}
		if (emit_sub) {
			(void)quality_monitor_begin(
				&quality_monitor,
				&cfg,
				capture_frame_buffer,
				cap.width,
				cap.height,
				&quality_observation);
		}
		metrics_add_stage(&runtime_metrics.decode_us, &runtime_metrics.decode_max_us, stage_start_us, stage_end_us);
		if (archive_encode_required && emit_archive && cfg.archive_content == VIDEO_BROKER_ARCHIVE_SOURCE) {
			if (archive_uses_capture_surface) {
				archive_surface = capture_frame_buffer;
				runtime_metrics.archive_capture_surface_frames++;
			} else {
				stage_start_us = monotonic_us();
				nv12_resize_oriented(
					capture_frame_buffer, cap.width, cap.height,
					archive_role.frame, cfg.archive_width, cfg.archive_height,
					VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL, cfg.aspect_policy);
				stage_end_us = monotonic_us();
				runtime_metrics.archive_resize_ops++;
				metrics_add_stage(&runtime_metrics.archive_resize_us, NULL, stage_start_us, stage_end_us);
				if (!logged_first_archive_resize) {
					bool accelerated = video_aspect_matches(
						cap.width, cap.height, cfg.archive_width, cfg.archive_height) &&
						video_broker_nv12_resize_is_accelerated(
							cap.width, cap.height, cfg.archive_width, cfg.archive_height);
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][stage-capability] stage=resize role=archive format=nv12 requested_backend=native_neon selected_backend=%s element=%s fallback=%s\n",
						accelerated ? "native_neon" : "native_cpu",
						accelerated ? "nv12_neon" : "nv12_cpu",
						accelerated ? "none" : "geometry_or_orientation_not_accelerated");
					logged_first_archive_resize = 1;
				}
			}
		}
		if (tone_map_capture_frame && runtime_control.optional_compute_admitted) {
			if (tone_map_processed_frame(
				&tone_mapper, &tone_accelerator, &cfg, &runtime_metrics,
				capture_frame_buffer, cap.width, cap.height,
				&tone_mapper_last_log_us) != 0) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] tone accelerator failed error=%s\n",
					video_broker_tone_accelerator_error(&tone_accelerator));
				goto cleanup;
			}
		}

		if (emit_main) {
			if (main_uses_capture_surface) {
				main_surface = capture_frame_buffer;
				runtime_metrics.main_capture_surface_frames++;
			} else {
				stage_start_us = monotonic_us();
				nv12_resize_oriented(capture_frame_buffer, cap.width, cap.height, live_role.frame, cfg.output_width, cfg.output_height, cfg.video_orientation, cfg.aspect_policy);
				stage_end_us = monotonic_us();
				runtime_metrics.main_resize_ops++;
				metrics_add_stage(&runtime_metrics.main_resize_us, NULL, stage_start_us, stage_end_us);
				if (!logged_first_main_resize) {
					bool accelerated =
						cfg.video_orientation == VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL &&
						video_aspect_matches(
							cap.width, cap.height, cfg.output_width, cfg.output_height) &&
						video_broker_nv12_resize_is_accelerated(
							cap.width, cap.height, cfg.output_width, cfg.output_height);
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][stage-capability] stage=resize role=live format=nv12 requested_backend=native_neon selected_backend=%s element=%s fallback=%s\n",
						accelerated ? "native_neon" : "native_cpu",
						accelerated ? "nv12_neon" : "nv12_cpu",
						accelerated ? "none" : "geometry_or_orientation_not_accelerated");
					logged_first_main_resize = 1;
				}
			}
			main_frame_ready = true;
			if (!logged_first_main) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] first main frame ready bytes=%zu\n", live_role.frame_size);
				logged_first_main = 1;
			}
		}
		if (cfg.tone_mapping_enabled && runtime_control.optional_compute_admitted &&
		    !tone_map_capture_frame && main_frame_ready) {
			if (tone_map_processed_frame(
				&tone_mapper, &tone_accelerator, &cfg, &runtime_metrics,
				main_surface, cfg.output_width, cfg.output_height,
				&tone_mapper_last_log_us) != 0) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] tone accelerator failed error=%s\n",
					video_broker_tone_accelerator_error(&tone_accelerator));
				goto cleanup;
			}
		}
		if (!tone_map_capture_frame && main_frame_ready) {
			quality_processed_surface = main_surface;
			quality_processed_width = cfg.output_width;
			quality_processed_height = cfg.output_height;
		}

		pipeline_checkpoint(&phase_probe, "analysis_transform");
		if (emit_sub) {
			/* Causal analysis uses exactly one source-space transform. Reusing an
			 * already-resized live surface would hide a second sampling/crop step
			 * and couple analysis geometry to live demand/profile changes. */
			bool sub_from_main = !frame_source.enabled && main_frame_ready &&
				video_aspect_matches(cfg.output_width, cfg.output_height, cfg.sub_width, cfg.sub_height);
			int sub_source_width = sub_from_main ? cfg.output_width : cap.width;
			int sub_source_height = sub_from_main ? cfg.output_height : cap.height;
			bool sub_resize_accelerated =
				(sub_from_main || cfg.video_orientation == VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL) &&
				video_aspect_matches(
					sub_source_width, sub_source_height,
					cfg.sub_width, cfg.sub_height) &&
				video_broker_nv12_resize_is_accelerated(
					sub_source_width, sub_source_height,
					cfg.sub_width, cfg.sub_height);
			stage_start_us = monotonic_us();
			if (sub_from_main) {
				video_broker_nv12_resize_nearest_aspect(
					main_surface, cfg.output_width, cfg.output_height,
					analysis_role.frame, cfg.sub_width, cfg.sub_height,
					cfg.aspect_policy);
			} else {
				nv12_resize_oriented(capture_frame_buffer, cap.width, cap.height, analysis_role.frame, cfg.sub_width, cfg.sub_height, cfg.video_orientation, cfg.aspect_policy);
			}
			stage_end_us = monotonic_us();
			runtime_metrics.sub_resize_ops++;
			if (sub_resize_accelerated) {
				runtime_metrics.sub_resize_accelerated_frames++;
			} else {
				runtime_metrics.sub_resize_cpu_frames++;
			}
			metrics_add_stage(&runtime_metrics.sub_resize_us, NULL, stage_start_us, stage_end_us);
			if (cfg.tone_mapping_enabled && runtime_control.optional_compute_admitted &&
			    !tone_map_capture_frame && !sub_from_main) {
				stage_start_us = monotonic_us();
				if (video_broker_tone_accelerator_apply(
						&tone_accelerator, &tone_mapper,
						analysis_role.frame, cfg.sub_width, cfg.sub_height) < 0) {
					fprintf(stderr, "[rdkx5_v4l2_mux_bridge] tone accelerator failed error=%s\n",
						video_broker_tone_accelerator_error(&tone_accelerator));
					goto cleanup;
				}
				stage_end_us = monotonic_us();
				runtime_metrics.tone_mapping_ops++;
				metrics_add_stage(
					&runtime_metrics.tone_mapping_us,
					&runtime_metrics.tone_mapping_max_us,
					stage_start_us,
					stage_end_us);
			}
			if (!logged_first_sub) {
				fprintf(stderr,
					"[rdkx5_v4l2_mux_bridge] first sub frame ready bytes=%zu resize_backend=%s\n",
					analysis_role.frame_size,
					sub_resize_accelerated ?
						video_broker_nv12_resize_backend_name(
							sub_source_width, sub_source_height,
							cfg.sub_width, cfg.sub_height) : "cpu");
				fprintf(stderr,
					"[rdkx5_v4l2_mux_bridge][stage-capability] stage=resize role=analysis format=nv12 requested_backend=native_neon selected_backend=%s element=%s fallback=%s\n",
					sub_resize_accelerated ? "native_neon" : "native_cpu",
					sub_resize_accelerated ? "nv12_neon" : "nv12_cpu",
					sub_resize_accelerated ? "none" : "geometry_or_orientation_not_accelerated");
				logged_first_sub = 1;
			}
			stage_start_us = monotonic_us();
			if (quality_monitor_finish(
				&quality_monitor,
				&cfg,
				&quality_observation,
				quality_processed_surface,
				quality_processed_width,
				quality_processed_height,
				analysis_role.frame,
				cfg.sub_width,
				cfg.sub_height)) {
				stage_end_us = monotonic_us();
				runtime_metrics.quality_ops++;
				metrics_add_stage(
					&runtime_metrics.quality_us,
					&runtime_metrics.quality_max_us,
					stage_start_us,
					stage_end_us);
			}
			stage_start_us = monotonic_us();
			pipeline_checkpoint(&phase_probe, "analysis_send");
			if (frame_source.enabled) {
				if (video_broker_frame_source_nv12_output(&frame_source,frame_source.output_stream,
					decoded_frame_origin,analysis_role.frame,analysis_role.frame_size,
					cfg.sub_width,cfg.sub_height,cfg.video_orientation,cfg.aspect_policy,VB_ANALYSIS_SEND_BUDGET_MS) < 0) {
					fprintf(stderr,"[rdkx5_v4l2_mux_bridge][frame-output] stage=%s role=analysis origin_epoch=%llu origin_sequence=%llu geometry=%dx%d action=retire_source\n",
						frame_source.stage,(unsigned long long)decoded_frame_origin.epoch,
						(unsigned long long)decoded_frame_origin.sequence,cfg.sub_width,cfg.sub_height);
					goto cleanup;
				}
			} else if (write_mux_frame(out, kSubStreamID, analysis_role.frame, (uint32_t)analysis_role.frame_size) != 0) {
				rc = 0;
				goto cleanup;
			}
			stage_end_us = monotonic_us();
			runtime_metrics.sub_frames++;
			analysis_role.frames++;
			metrics_add_stage(&runtime_metrics.write_sub_us, &runtime_metrics.write_sub_max_us, stage_start_us, stage_end_us);
		}

		pipeline_checkpoint(&phase_probe, "live_encode_send");
		if (emit_main && live_role.encoder_initialized) {
			uint64_t encoded_epoch = live_role.encoder_local_epoch;
			int encoded_bitrate_kbps = live_role.active_bitrate_kbps;
			stage_start_us = monotonic_us();
			arm_stream_guard(live_role.stream, live_role.stream_capacity);
			stream_size = video_broker_rdkx5_persistent_encoder_encode(
				live_role.encoder,
				main_surface,
				live_role.frame_size,
				live_role.stream,
				live_role.stream_capacity,
				&encoded_epoch,
				&encoded_bitrate_kbps);
			if (stream_size < 0) {
				if (v4l2_fanout_mark_active_failure(
						&fanout_backend, VIDEO_BROKER_WORKER_ROLE_LIVE)) {
					goto archive_output;
				}
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] persistent live encoder failed\n");
				goto cleanup;
			}
			stage_end_us = monotonic_us();
			if (encoded_epoch > 0) {
				runtime_metrics.encode_ops++;
				metrics_add_stage(&runtime_metrics.encode_us, &runtime_metrics.encode_max_us, stage_start_us, stage_end_us);
			}
			if (!stream_guard_ok(live_role.stream, live_role.stream_capacity)) {
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] encoder stream guard corrupted buffer=%zu\n", live_role.stream_capacity);
				goto cleanup;
			}
			if (stream_size > 0) {
				if (!logged_first_stream) {
					fprintf(stderr, "[rdkx5_v4l2_mux_bridge] first stream size=%d\n", stream_size);
					logged_first_stream = 1;
				}
				if ((size_t)stream_size > live_role.stream_capacity) {
					fprintf(stderr, "[rdkx5_v4l2_mux_bridge] encoder stream overflow size=%d buffer=%zu\n", stream_size, live_role.stream_capacity);
					goto cleanup;
				}
				stage_start_us = monotonic_us();
					if (publish_main) {
						if (persistent_main_control && v4l2_publish_live_encoder_epoch(
								&fanout_backend, out, encoded_epoch, encoded_bitrate_kbps) != 0) {
							goto cleanup;
						}
						if (write_mux_frame(out, kMainStreamID, live_role.stream, (uint32_t)stream_size) != 0) {
						rc = 0;
						goto cleanup;
					}
						runtime_metrics.main_frames++;
						v4l2_role_observe_bootstrap(
							&live_role, live_role.stream, (size_t)stream_size);
						live_role.frames++;
					if (persistent_main_control && live_role.pending_control_ready) {
						fprintf(stderr,
							"[rdkx5_v4l2_mux_bridge][control] ready seq=%llu main_active=true main_fps=%d main_bitrate_kbps=%d sub_fps=%d encoder_epoch=%llu scope=live_persistent_rc\n",
							(unsigned long long)live_role.pending_control_sequence,
							runtime_control.main_fps,
							live_role.active_bitrate_kbps,
							runtime_control.sub_fps,
							(unsigned long long)fanout_backend.live_epoch.value);
						live_role.pending_control_ready = false;
					}
				}
				stage_end_us = monotonic_us();
				metrics_add_stage(&runtime_metrics.write_main_us, &runtime_metrics.write_main_max_us, stage_start_us, stage_end_us);
			}
		}
	archive_output:
		pipeline_checkpoint(&phase_probe, "archive_transform");
		if (archive_encode_required && emit_archive) {
			int archive_stream_size;
			uint8_t archive_stream_id = cfg.archive_codec == SP_ENCODER_H264 ?
				kGeneratedArchiveH264StreamID : kArchiveH265StreamID;
			uint64_t archive_epoch = 0;
			int archive_bitrate_kbps = 0;
			if (cfg.archive_content != VIDEO_BROKER_ARCHIVE_SOURCE) {
				if (archive_uses_capture_surface) {
					archive_surface = capture_frame_buffer;
					runtime_metrics.archive_capture_surface_frames++;
				} else {
					stage_start_us = monotonic_us();
					nv12_resize_oriented(
						capture_frame_buffer, cap.width, cap.height,
						archive_role.frame, cfg.archive_width, cfg.archive_height,
						VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL, cfg.aspect_policy);
					stage_end_us = monotonic_us();
					runtime_metrics.archive_resize_ops++;
					metrics_add_stage(&runtime_metrics.archive_resize_us, NULL, stage_start_us, stage_end_us);
					if (!logged_first_archive_resize) {
						bool accelerated = video_aspect_matches(
							cap.width, cap.height, cfg.archive_width, cfg.archive_height) &&
							video_broker_nv12_resize_is_accelerated(
								cap.width, cap.height, cfg.archive_width, cfg.archive_height);
						fprintf(stderr,
							"[rdkx5_v4l2_mux_bridge][stage-capability] stage=resize role=archive format=nv12 requested_backend=native_neon selected_backend=%s element=%s fallback=%s\n",
							accelerated ? "native_neon" : "native_cpu",
							accelerated ? "nv12_neon" : "nv12_cpu",
							accelerated ? "none" : "geometry_or_orientation_not_accelerated");
						logged_first_archive_resize = 1;
					}
				}
			}
			pipeline_checkpoint(&phase_probe, "archive_encode");
			stage_start_us = monotonic_us();
			arm_stream_guard(archive_role.stream, archive_role.stream_capacity);
			archive_stream_size = video_broker_rdkx5_persistent_encoder_encode(
				archive_role.encoder,
				archive_surface,
				archive_role.frame_size,
				archive_role.stream,
				archive_role.stream_capacity,
				&archive_epoch,
				&archive_bitrate_kbps);
			stage_end_us = monotonic_us();
			if (archive_role.encoder->encode_delay_valid) {
				vb_pipeline_delay delay = archive_role.encoder->encode_delay;
				vb_diagnostic_pipe_printf(&g_telemetry,
					"[video-broker][encoder-latency] event=native_phase_slow role=archive phase=%s"
					" wall_us=%llu thread_cpu_us=%llu suppressed=%llu epoch=%llu"
					" instance=%d queue_faulted=%s sdk_timeout_ms=1000\n",
					delay.phase, (unsigned long long)delay.wall_us, (unsigned long long)delay.cpu_us,
					(unsigned long long)delay.suppressed, (unsigned long long)archive_role.encoder->epoch,
					archive_role.encoder->instance_index, archive_role.encoder->queue_faulted ? "true" : "false");
			}
			if (!stream_guard_ok(archive_role.stream, archive_role.stream_capacity) ||
			    archive_stream_size < 0 || (size_t)archive_stream_size > archive_role.stream_capacity) {
				if (archive_stream_size < 0 && v4l2_fanout_mark_active_failure(
						&fanout_backend, VIDEO_BROKER_WORKER_ROLE_ARCHIVE)) {
					goto requeue;
				}
				fprintf(stderr, "[rdkx5_v4l2_mux_bridge] generated archive stream invalid codec=%s size=%d buffer=%zu\n",
					cfg.archive_codec == SP_ENCODER_H264 ? "h264" : "h265",
					archive_stream_size, archive_role.stream_capacity);
				goto cleanup;
			}
			if (archive_stream_size > 0) {
				uint64_t write_start_us = monotonic_us();
				uint64_t write_end_us;
				pipeline_checkpoint(&phase_probe, "archive_mux_write");
				if (write_mux_frame(
					out, archive_stream_id, archive_role.stream,
					(uint32_t)archive_stream_size) != 0) {
					rc = 0;
					goto cleanup;
				}
				write_end_us = monotonic_us();
				metrics_add_stage(
					&runtime_metrics.write_archive_us,
					&runtime_metrics.write_archive_max_us,
					write_start_us,
					write_end_us);
			}
			if (archive_stream_size > 0) {
				runtime_metrics.archive_frames++;
				v4l2_role_observe_bootstrap(
					&archive_role, archive_role.stream, (size_t)archive_stream_size);
				archive_role.frames++;
				metrics_add_stage(
					&runtime_metrics.archive_encode_us,
					&runtime_metrics.archive_encode_max_us,
					stage_start_us,
					stage_end_us);
			}
		}
		rc = 0;

requeue:
		pipeline_checkpoint(&phase_probe, "requeue_and_fanout_resume");
		if (!capture_buffer_requeued && xioctl(cap.fd, VIDIOC_QBUF, &buf) != 0) {
			fprintf(stderr, "[rdkx5_v4l2_mux_bridge] VIDIOC_QBUF requeue failed err=%s\n", strerror(errno));
			goto cleanup;
		}
		/* Commit only after the current CPU-decoded frame has been returned to
		 * V4L2. The broker's commit envelope is causal and is revalidated at this
		 * exact frame boundary; a late/canceled candidate never becomes active. */
		if (mjpeg_switch_ready) {
			int64_t now_ms = realtime_ms();
			bool commit_current =
				strcmp(runtime_control.retry_action, "commit") == 0 &&
				strcmp(runtime_control.retry_component, "native.jpu") == 0 &&
				runtime_control.retry_token == mjpeg_candidate_token &&
				runtime_control.retry_deadline_unix_ms == mjpeg_commit_deadline_unix_ms &&
				mjpeg_candidate_completed_unix_ms > 0 && now_ms >= 0 &&
				now_ms < mjpeg_commit_deadline_unix_ms;
			if (commit_current && video_broker_rdkx5_mjpeg_trial_accept(&mjpeg_trial) == 0) {
				capture_decoder = video_broker_rdkx5_mjpeg_trial_decoder(&mjpeg_trial);
				if (capture_decoder != NULL && video_broker_rdkx5_async_decoder_start(
						&mjpeg_async_decoder, capture_decoder) == 0) {
						mjpeg_decode_selected = VIDEO_BROKER_MJPEG_ACCELERATOR_JPU;
						mjpeg_decoder_attested = true;
						mjpeg_trial_decoder_active = true;
						mjpeg_active_trial_token = mjpeg_candidate_token;
						mjpeg_async_replacements_observed = 0;
						mjpeg_first_unacknowledged_submission_us = 0;
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=jpu fallback=none output=nv12 retry_token=%llu\n",
						(unsigned long long)mjpeg_active_trial_token);
				} else {
					uint64_t canceled_token = mjpeg_candidate_token;
					capture_decoder = &decoder;
					if (video_broker_rdkx5_mjpeg_trial_begin_async_reset(
							&mjpeg_trial, &mjpeg_async_decoder) != 0) {
						fprintf(stderr, "[rdkx5_v4l2_mux_bridge] accepted JPU candidate async reset failed\n");
						goto cleanup;
					}
					mjpeg_trial_decoder_reset_pending = true;
					mjpeg_cancel_ack_pending_token = canceled_token;
					fprintf(stderr,
						"[rdkx5_v4l2_mux_bridge][capture-decoder] codec=mjpeg requested=auto selected=cpu fallback=jpu_retry_open_failed output=nv12 retry_token=%llu\n",
						(unsigned long long)canceled_token);
				}
			} else {
				uint64_t canceled_token = mjpeg_candidate_token;
				if (video_broker_rdkx5_mjpeg_trial_begin_async_reset(
						&mjpeg_trial, &mjpeg_async_decoder) != 0) {
					fprintf(stderr, "[rdkx5_v4l2_mux_bridge] uncommitted JPU candidate async reset failed\n");
					goto cleanup;
				}
				mjpeg_trial_decoder_reset_pending = true;
				mjpeg_cancel_ack_pending_token = canceled_token;
			}
			mjpeg_candidate_ready = false;
			mjpeg_switch_ready = false;
			mjpeg_candidate_token = 0;
			mjpeg_candidate_completed_unix_ms = 0;
			mjpeg_commit_deadline_unix_ms = 0;
		}
		if (fanout_enabled &&
		    v4l2_fanout_resume_and_publish(&fanout_controller) != 0) {
			goto cleanup;
		}
		if (metrics_steady_window_started) {
			metrics_add_stage(
				&runtime_metrics.frame_work_us,
				&runtime_metrics.frame_work_max_us,
				frame_work_start_us,
				monotonic_us());
		}
		pipeline_checkpoint(&phase_probe, "metrics_log");
		metrics_log_if_due(&runtime_metrics, &frame_source, monotonic_us(), false);
	}
	operation_loop_completed = true;

cleanup:
	last_operation_result = rc;
	rc = video_broker_worker_operation_status(rc, operation_loop_completed);
	if (rc != 0) {
		fprintf(stderr, "[video-broker][worker-operation] event=operation_failed adapter=rdkx5_v4l2"
			" phase=%s last_result=%d loop_completed=%s stop_signal=%d"
			" action=checked_cleanup_before_recovery\n",
			phase_probe.phase == NULL ? "startup" : phase_probe.phase, last_operation_result,
			operation_loop_completed ? "true" : "false", (int)g_stop_signal);
	}
	pipeline_checkpoint(&phase_probe, "cleanup");
	/* Short failed admissions/probes can end before the periodic report. Retain
	 * the final steady window so capture, decoder and consumer cadence remain
	 * distinguishable without per-frame logging or extending a failed run. */
	if (metrics_steady_window_started) {
		metrics_log_if_due(&runtime_metrics, &frame_source, monotonic_us(), true);
	}
	/* A previous successful operation may have set rc=0. Terminal frame
	 * failure remains an operation error even with verified SDK teardown. */
	if (frame_source.failed && rc == 0) rc = -1;
	if (frame_source.enabled || frame_source.failed) {
		vbsp_stats output_stats={0};
		(void)video_broker_frame_source_output_stats(&frame_source,&output_stats);
		fprintf(stderr,"[rdkx5_v4l2_mux_bridge][frame-source] stage=%s failure_code=%d published=%llu unavailable=%llu output_submitted=%llu output_published=%llu output_replaced=%llu output_busy=%llu output_expired=%llu output_failure=%d action=close\n",
			frame_source.stage,frame_source.failure_code,(unsigned long long)frame_source.published,(unsigned long long)frame_source.unavailable,
			(unsigned long long)output_stats.submitted,(unsigned long long)output_stats.published,
			(unsigned long long)output_stats.replaced,(unsigned long long)output_stats.busy,
			(unsigned long long)output_stats.expired,output_stats.failure);
	}
	if (video_broker_frame_source_close(&frame_source) != 0) cleanup_confirmed = false;
	if (g_stop_signal != 0) {
		fprintf(
			stderr,
			"[rdkx5_v4l2_mux_bridge][capture-lifecycle] event=cooperative_shutdown_started signal=%d action=streamoff_unmap_close_then_codec_release\n",
			(int)g_stop_signal);
	}
	if (close_v4l2_capture(&cap) != 0) cleanup_confirmed = false;
	/* The shared accelerator can reference active role/capture storage. Drain
	 * it before freeing any of those buffers. Uncertain OpenCL ownership must
	 * retain host memory through process termination, not normal free/atexit. */
	if (video_broker_tone_accelerator_destroy(&tone_accelerator) != 0) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge][worker-release] sdk_release=unconfirmed"
			" owner=tone_shared exit_status=%d action=retain_buffers_and_exit\n",
			VIDEO_BROKER_WORKER_RELEASE_UNCONFIRMED);
		_Exit(VIDEO_BROKER_WORKER_RELEASE_UNCONFIRMED);
	}
	if (v4l2_fanout_destroy(&fanout_backend) != 0) cleanup_confirmed = false;
	video_broker_nv12_tone_mapper_destroy(&tone_mapper);
	if (v4l2_role_destroy(&live_role) != 0) cleanup_confirmed = false;
	if (v4l2_role_destroy(&archive_role) != 0) cleanup_confirmed = false;
	if (v4l2_role_destroy(&analysis_role) != 0) cleanup_confirmed = false;
	if (video_broker_tone_accelerator_resources_quarantined()) {
		fprintf(stderr, "[rdkx5_v4l2_mux_bridge][worker-release] sdk_release=unconfirmed"
			" owner=tone_role exit_status=%d action=retain_buffers_and_exit\n",
			VIDEO_BROKER_WORKER_RELEASE_UNCONFIRMED);
		_Exit(VIDEO_BROKER_WORKER_RELEASE_UNCONFIRMED);
	}
	if (video_broker_rdkx5_async_decoder_destroy(&mjpeg_async_decoder) != 0) {
		/* Its decoder may still be in use if joining failed. Never close it
		 * from this second owner; retain storage until this process exits. */
		cleanup_confirmed = false;
	} else {
		if (video_broker_rdkx5_mjpeg_trial_destroy(&mjpeg_trial) != 0) cleanup_confirmed = false;
		if (video_broker_rdkx5_decoder_close(&decoder) != 0) cleanup_confirmed = false;
	}
	if (video_broker_rdkx5_decoder_resources_quarantined() ||
	    video_broker_rdkx5_encoder_resources_quarantined()) cleanup_confirmed = false;
	if (jpeg != NULL) {
		tjDestroy(jpeg);
	}
	free(decode_scratch_buffer);
	free(capture_input_copy);
	free(capture_frame_buffer);
	if (out != NULL) {
		fclose(out);
	} else if (binary_fd >= 0) {
		close(binary_fd);
	}
	/* Failed joins may leave SDK workers referring to main's stack objects.
	 * Keep those lifetimes valid until process termination and do not invoke
	 * unknown vendor atexit handlers after an uncertain release. */
	const int final_status = video_broker_worker_exit_status(rc, cleanup_confirmed);
	vb_diagnostic_pipe_close(&g_telemetry);
	fprintf(stderr, "[rdkx5_v4l2_mux_bridge][worker-release] operation_status=%d"
		" sdk_release=%s exit_status=%d tone_path=%s\n", rc,
		cleanup_confirmed ? "confirmed" : "unconfirmed", final_status,
		cfg.tone_mapping_enabled ? "checked" : "disabled");
	/* Every owned camera, codec and tone resource above has an explicit checked
	 * release. Returning from main runs unrelated vendor atexit handlers; a field board
	 * emitted the confirmed receipt and then remained alive until SIGKILL.
	 * Flush this final receipt and exit without a second hidden SDK teardown. */
	(void)fflush(stderr);
	_Exit(final_status);
}
