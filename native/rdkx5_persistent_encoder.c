/* Persistent encoder owned by the standalone video-broker data plane. */
#define _POSIX_C_SOURCE 200809L

#include "rdkx5_persistent_encoder.h"

#include <limits.h>
#include <errno.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static atomic_bool encoder_resources_quarantined = ATOMIC_VAR_INIT(false);

static void encoder_checkpoint(video_broker_rdkx5_persistent_encoder_t *encoder, const char *next) {
	int saved_errno = errno;
	struct timespec wall, cpu;
	if (clock_gettime(CLOCK_MONOTONIC, &wall) == 0 &&
	    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu) == 0) {
		vb_pipeline_delay delay;
		if (vb_pipeline_checkpoint(&encoder->encode_latency, next,
		    (uint64_t)wall.tv_sec*1000000+(uint64_t)wall.tv_nsec/1000,
		    (uint64_t)cpu.tv_sec*1000000+(uint64_t)cpu.tv_nsec/1000, &delay)) {
			encoder->encode_delay = delay;
			encoder->encode_delay_valid = true;
		}
	} else encoder->encode_latency.phase = NULL;
	errno = saved_errno;
}

bool video_broker_rdkx5_encoder_resources_quarantined(void) {
	return atomic_load_explicit(&encoder_resources_quarantined, memory_order_relaxed);
}

static int encoder_release_failure(video_broker_rdkx5_persistent_encoder_t *encoder,
	const char *stage, int code) {
	encoder->close_error = code;
	encoder->active = false;
	atomic_store_explicit(&encoder_resources_quarantined, true, memory_order_relaxed);
	fprintf(stderr, "[video-broker][encoder-release] event=release_failed stage=%s"
		" sdk_code=%d codec=%d instance=%d width=%d height=%d epoch=%" PRIu64
		" initialized=%s started=%s resources_released=false action=retire_process\n",
		stage, code, encoder->codec, encoder->instance_index, encoder->width, encoder->height,
		encoder->epoch, encoder->initialized ? "true" : "false", encoder->started ? "true" : "false");
	return -1;
}

enum {
	kCodecTimeoutMs = 1000,
	kCodecBufferCount = 3,
	/* The vendor documents 10 ms as the most precise CBR VBV setting. */
	kRealtimeVBVBufferMs = 10,
	kQualityVBVBufferMs = 1000,
	kMinQP = 8,
	kRealtimeMaxQP = 51,
	kRealtimeHVSScale = 2,
	kRealtimeHVSMaxDeltaQP = 10,
	kQualityHVSScale = 1,
	kQualityHVSMaxDeltaQP = 4,
	kCodecDimensionMin = 128,
	kCodecDimensionMax = 8192,
	kCodecHeightAlignment = 8,
	kH264WidthAlignment = 8,
	kH265WidthAlignment = 16,
};

/* Error-only diagnostics: keep successful frames free of clocks, allocations
 * and log I/O. Preserve the vendor status before collapsing the public result
 * to -1; that distinction is essential when a worker retires after a stall. */
static int encoder_encode_failure(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	const char *stage, int sdk_code, size_t frame_size, size_t stream_capacity,
	bool queue_faulted
) {
	/* After dequeue/queue failure we cannot bind a later output to a new input.
	 * Retain SDK ownership for checked teardown, but never enqueue again into
	 * this generation (including while fan-out rollback is pending). */
	if (queue_faulted) encoder->queue_faulted = true;
	fprintf(stderr, "[video-broker][encoder] event=encode_failed stage=%s"
		" sdk_code=%d codec=%d instance=%d width=%d height=%d epoch=%" PRIu64
		" frame_bytes=%zu stream_capacity=%zu sdk_timeout_ms=%d action=return_failure"
		" queue_faulted=%s recovery=%s\n",
		stage, sdk_code, encoder->codec, encoder->instance_index,
		encoder->width, encoder->height, encoder->epoch,
		frame_size, stream_capacity, kCodecTimeoutMs,
		encoder->queue_faulted ? "true" : "false",
		encoder->queue_faulted ? "checked_release_required" : "returned_output_buffer");
	return -1;
}

static bool valid_encoder_geometry(
	video_broker_rdkx5_encoder_codec_t codec,
	int width,
	int height
) {
	int width_alignment = codec == VIDEO_BROKER_RDKX5_ENCODER_H265 ?
		kH265WidthAlignment : kH264WidthAlignment;
	return width >= kCodecDimensionMin && width <= kCodecDimensionMax &&
		height >= kCodecDimensionMin && height <= kCodecDimensionMax &&
		width % width_alignment == 0 &&
		height % kCodecHeightAlignment == 0;
}

static int checked_frame_size(int width, int height, size_t *frame_size) {
	size_t pixels;
	if (width <= 0 || height <= 0 || frame_size == NULL) {
		return -1;
	}
	pixels = (size_t)width * (size_t)height;
	if (pixels > (size_t)UINT32_MAX / 3U * 2U) {
		return -1;
	}
	*frame_size = pixels + pixels / 2U;
	return (*frame_size <= UINT32_MAX) ? 0 : -1;
}

static bool supported_codec(video_broker_rdkx5_encoder_codec_t codec) {
	return codec == VIDEO_BROKER_RDKX5_ENCODER_H264 || codec == VIDEO_BROKER_RDKX5_ENCODER_H265;
}

static bool supported_rate_control_profile(
	video_broker_rdkx5_rate_control_profile_t profile
) {
	return profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR ||
		profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_CBR ||
		profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR;
}

static int request_decoder_refresh_idr(
	video_broker_rdkx5_persistent_encoder_t *encoder
) {
	if (encoder == NULL || !encoder->started || encoder->close_error != 0 ||
	    video_broker_rdkx5_encoder_resources_quarantined()) {
		return -1;
	}
	/* Ask for headers plus IDR, but do not treat command acceptance as proof of
	 * output shape. RDK X5 can emit an IDR without repeating VPS/SPS/PPS after
	 * dynamic rate control. The encoder retains its own complete parameter sets
	 * even when warmup output is private, and prefixes subsequent bare IDRs. */
	if (hb_mm_mc_request_idr_header(&encoder->context, 1) != 0) {
		return -1;
	}
	return hb_mm_mc_request_idr_frame(&encoder->context) == 0 ? 0 : -1;
}

static media_codec_id_t media_codec_id(video_broker_rdkx5_encoder_codec_t codec) {
	return codec == VIDEO_BROKER_RDKX5_ENCODER_H265 ? MEDIA_CODEC_ID_H265 : MEDIA_CODEC_ID_H264;
}

static mc_video_rate_control_mode_t rate_control_mode(
	const video_broker_rdkx5_persistent_encoder_t *encoder) {
	bool avbr = encoder->rate_control_profile ==
		VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR;
	if (encoder->codec == VIDEO_BROKER_RDKX5_ENCODER_H265) {
		return avbr ? MC_AV_RC_MODE_H265AVBR : MC_AV_RC_MODE_H265CBR;
	}
	return avbr ? MC_AV_RC_MODE_H264AVBR : MC_AV_RC_MODE_H264CBR;
}

static int quality_max_qp(
	video_broker_rdkx5_encoder_codec_t codec,
	int width,
	int height,
	int frame_rate,
	int bitrate_kbps
) {
	uint64_t pixel_rate = (uint64_t)width * (uint64_t)height * (uint64_t)frame_rate;
	uint64_t milli_bits_per_pixel;
	if (pixel_rate == 0) {
		return 44;
	}
	/* kbps * 1000 gives bits/s; multiply by another 1000 for milli-bpp. */
	milli_bits_per_pixel = ((uint64_t)bitrate_kbps * 1000000ULL) / pixel_rate;
	if (codec == VIDEO_BROKER_RDKX5_ENCODER_H265) {
		if (milli_bits_per_pixel >= 120) {
			return 36;
		}
		if (milli_bits_per_pixel >= 80) {
			return 38;
		}
		if (milli_bits_per_pixel >= 50) {
			return 41;
		}
		return 44;
	}
	if (milli_bits_per_pixel >= 180) {
		return 36;
	}
	if (milli_bits_per_pixel >= 120) {
		return 38;
	}
	if (milli_bits_per_pixel >= 75) {
		return 41;
	}
	return 44;
}

static void quality_qp_bounds(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	int frame_rate,
	int bitrate_kbps,
	int *max_qp_i,
	int *max_qp_p,
	int *max_qp_b
) {
	int p = quality_max_qp(
		encoder->codec, encoder->width, encoder->height, frame_rate, bitrate_kbps);
	*max_qp_i = p > 2 ? p - 2 : p;
	*max_qp_p = p;
	*max_qp_b = p < 50 ? p + 2 : 51;
}

static void rate_control_qp_bounds(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	int frame_rate,
	int bitrate_kbps,
	int *max_qp_i,
	int *max_qp_p,
	int *max_qp_b
) {
	if (encoder->rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR) {
		*max_qp_i = kRealtimeMaxQP;
		*max_qp_p = kRealtimeMaxQP;
		*max_qp_b = kRealtimeMaxQP;
		return;
	}
	quality_qp_bounds(
		encoder, frame_rate, bitrate_kbps, max_qp_i, max_qp_p, max_qp_b);
}

static int rate_control_vbv_buffer_ms(
	const video_broker_rdkx5_persistent_encoder_t *encoder
) {
	return encoder->rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR ?
		kRealtimeVBVBufferMs : kQualityVBVBufferMs;
}

static int rate_control_seed_qp(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	int frame_rate,
	int bitrate_kbps,
	int max_qp_i
) {
	if (encoder->rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR) {
		/*
		 * The RDK X5 firmware-selected initial QP and intra_qp=0 can emit a
		 * multi-second burst far above the requested bitrate when a dormant
		 * live encoder is activated. Seed both from the target bits-per-pixel
		 * budget so the first GOP is bounded as tightly as later GOPs.
		 */
		return quality_max_qp(
			encoder->codec, encoder->width, encoder->height, frame_rate, bitrate_kbps);
	}
	return max_qp_i < 30 ? max_qp_i : 30;
}

static void configure_h264_cbr(
	mc_h264_cbr_params_t *params,
	int frame_rate,
	int bitrate_kbps,
	int max_qp_i,
	int max_qp_p,
	int max_qp_b,
	int seed_qp,
	int vbv_buffer_ms,
	bool block_level_rate_control,
	bool startup
) {
	params->intra_period = (hb_u32)frame_rate;
	params->intra_qp = (hb_u32)seed_qp;
	params->bit_rate = (hb_u32)bitrate_kbps;
	params->frame_rate = (hb_u32)frame_rate;
	params->vbv_buffer_size = vbv_buffer_ms;
	params->min_qp_I = kMinQP;
	params->max_qp_I = (hb_u32)max_qp_i;
	params->min_qp_P = kMinQP;
	params->max_qp_P = (hb_u32)max_qp_p;
	params->min_qp_B = kMinQP;
	params->max_qp_B = (hb_u32)max_qp_b;
	params->hvs_qp_enable = 1U;
	params->hvs_qp_scale = block_level_rate_control ? kRealtimeHVSScale : kQualityHVSScale;
	params->max_delta_qp = block_level_rate_control ?
		kRealtimeHVSMaxDeltaQP : kQualityHVSMaxDeltaQP;
	if (startup) {
		params->initial_rc_qp = (hb_u32)seed_qp;
		params->mb_level_rc_enalbe = block_level_rate_control;
		params->qp_map_enable = false;
	}
}

static void configure_h265_cbr(
	mc_h265_cbr_params_t *params,
	int frame_rate,
	int bitrate_kbps,
	int max_qp_i,
	int max_qp_p,
	int max_qp_b,
	int seed_qp,
	int vbv_buffer_ms,
	bool block_level_rate_control,
	bool startup
) {
	params->intra_period = (hb_u32)frame_rate;
	params->intra_qp = (hb_u32)seed_qp;
	params->bit_rate = (hb_u32)bitrate_kbps;
	params->frame_rate = (hb_u32)frame_rate;
	params->vbv_buffer_size = vbv_buffer_ms;
	params->min_qp_I = kMinQP;
	params->max_qp_I = (hb_u32)max_qp_i;
	params->min_qp_P = kMinQP;
	params->max_qp_P = (hb_u32)max_qp_p;
	params->min_qp_B = kMinQP;
	params->max_qp_B = (hb_u32)max_qp_b;
	params->hvs_qp_enable = 1U;
	params->hvs_qp_scale = block_level_rate_control ? kRealtimeHVSScale : kQualityHVSScale;
	params->max_delta_qp = block_level_rate_control ?
		kRealtimeHVSMaxDeltaQP : kQualityHVSMaxDeltaQP;
	if (startup) {
		params->initial_rc_qp = (hb_u32)seed_qp;
		params->ctu_level_rc_enalbe = block_level_rate_control;
		params->qp_map_enable = false;
	}
}

static void configure_h264_avbr(
	mc_h264_avbr_params_t *params,
	int frame_rate,
	int bitrate_kbps,
	int max_qp_i,
	int max_qp_p,
	int max_qp_b,
	int seed_qp,
	int vbv_buffer_ms,
	bool startup
) {
	params->intra_period = (hb_u32)frame_rate;
	params->intra_qp = (hb_u32)seed_qp;
	params->bit_rate = (hb_u32)bitrate_kbps;
	params->frame_rate = (hb_u32)frame_rate;
	params->vbv_buffer_size = vbv_buffer_ms;
	params->min_qp_I = kMinQP;
	params->max_qp_I = (hb_u32)max_qp_i;
	params->min_qp_P = kMinQP;
	params->max_qp_P = (hb_u32)max_qp_p;
	params->min_qp_B = kMinQP;
	params->max_qp_B = (hb_u32)max_qp_b;
	params->hvs_qp_enable = 1U;
	params->hvs_qp_scale = kQualityHVSScale;
	params->max_delta_qp = kQualityHVSMaxDeltaQP;
	if (startup) {
		params->initial_rc_qp = (hb_u32)seed_qp;
		params->mb_level_rc_enalbe = false;
		params->qp_map_enable = false;
	}
}

static void configure_h265_avbr(
	mc_h265_avbr_params_t *params,
	int frame_rate,
	int bitrate_kbps,
	int max_qp_i,
	int max_qp_p,
	int max_qp_b,
	int seed_qp,
	int vbv_buffer_ms,
	bool startup
) {
	params->intra_period = (hb_u32)frame_rate;
	params->intra_qp = (hb_u32)seed_qp;
	params->bit_rate = (hb_u32)bitrate_kbps;
	params->frame_rate = (hb_u32)frame_rate;
	params->vbv_buffer_size = vbv_buffer_ms;
	params->min_qp_I = kMinQP;
	params->max_qp_I = (hb_u32)max_qp_i;
	params->min_qp_P = kMinQP;
	params->max_qp_P = (hb_u32)max_qp_p;
	params->min_qp_B = kMinQP;
	params->max_qp_B = (hb_u32)max_qp_b;
	params->hvs_qp_enable = 1U;
	params->hvs_qp_scale = kQualityHVSScale;
	params->max_delta_qp = kQualityHVSMaxDeltaQP;
	if (startup) {
		params->initial_rc_qp = (hb_u32)seed_qp;
		params->ctu_level_rc_enalbe = false;
		params->qp_map_enable = false;
	}
}

static int configure_rate_control_values(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	mc_rate_control_params_t *params,
	int frame_rate,
	int bitrate_kbps,
	bool startup
) {
	int max_qp_i;
	int max_qp_p;
	int max_qp_b;
	int vbv_buffer_ms;
	int seed_qp;
	bool block_level_rate_control;
	if (encoder == NULL || params == NULL || frame_rate <= 0 || frame_rate > 240 ||
	    bitrate_kbps <= 0 || bitrate_kbps > 700000) {
		return -1;
	}
	if (params->mode != rate_control_mode(encoder)) {
		return -1;
	}
	rate_control_qp_bounds(
		encoder, frame_rate, bitrate_kbps, &max_qp_i, &max_qp_p, &max_qp_b);
	vbv_buffer_ms = rate_control_vbv_buffer_ms(encoder);
	seed_qp = rate_control_seed_qp(
		encoder, frame_rate, bitrate_kbps, max_qp_i);
	block_level_rate_control =
		encoder->rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR;
	if (encoder->rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR &&
	    encoder->codec == VIDEO_BROKER_RDKX5_ENCODER_H265) {
		configure_h265_avbr(
			&params->h265_avbr_params, frame_rate, bitrate_kbps,
			max_qp_i, max_qp_p, max_qp_b, seed_qp, vbv_buffer_ms, startup);
	} else if (encoder->rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR) {
		configure_h264_avbr(
			&params->h264_avbr_params, frame_rate, bitrate_kbps,
			max_qp_i, max_qp_p, max_qp_b, seed_qp, vbv_buffer_ms, startup);
	} else if (encoder->codec == VIDEO_BROKER_RDKX5_ENCODER_H265) {
		configure_h265_cbr(
			&params->h265_cbr_params, frame_rate, bitrate_kbps,
			max_qp_i, max_qp_p, max_qp_b, seed_qp, vbv_buffer_ms,
			block_level_rate_control, startup);
	} else {
		configure_h264_cbr(
			&params->h264_cbr_params, frame_rate, bitrate_kbps,
			max_qp_i, max_qp_p, max_qp_b, seed_qp, vbv_buffer_ms,
			block_level_rate_control, startup);
	}
	return 0;
}

static bool rate_control_values_match(
	const video_broker_rdkx5_persistent_encoder_t *encoder,
	const mc_rate_control_params_t *expected,
	const mc_rate_control_params_t *actual
) {
	if (encoder == NULL || expected == NULL || actual == NULL ||
	    actual->mode != expected->mode) {
		return false;
	}
	if (encoder->rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR &&
	    encoder->codec == VIDEO_BROKER_RDKX5_ENCODER_H265) {
		const mc_h265_avbr_params_t *want = &expected->h265_avbr_params;
		const mc_h265_avbr_params_t *got = &actual->h265_avbr_params;
		return got->intra_period == want->intra_period && got->intra_qp == want->intra_qp &&
			got->bit_rate == want->bit_rate && got->frame_rate == want->frame_rate &&
			got->initial_rc_qp == want->initial_rc_qp &&
			got->vbv_buffer_size == want->vbv_buffer_size &&
			got->ctu_level_rc_enalbe == want->ctu_level_rc_enalbe &&
			got->min_qp_I == want->min_qp_I && got->max_qp_I == want->max_qp_I &&
			got->min_qp_P == want->min_qp_P && got->max_qp_P == want->max_qp_P &&
			got->min_qp_B == want->min_qp_B && got->max_qp_B == want->max_qp_B &&
			got->hvs_qp_enable == want->hvs_qp_enable &&
			got->hvs_qp_scale == want->hvs_qp_scale &&
			got->max_delta_qp == want->max_delta_qp &&
			got->qp_map_enable == want->qp_map_enable;
	}
	if (encoder->rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR) {
		const mc_h264_avbr_params_t *want = &expected->h264_avbr_params;
		const mc_h264_avbr_params_t *got = &actual->h264_avbr_params;
		return got->intra_period == want->intra_period && got->intra_qp == want->intra_qp &&
			got->bit_rate == want->bit_rate && got->frame_rate == want->frame_rate &&
			got->initial_rc_qp == want->initial_rc_qp &&
			got->vbv_buffer_size == want->vbv_buffer_size &&
			got->mb_level_rc_enalbe == want->mb_level_rc_enalbe &&
			got->min_qp_I == want->min_qp_I && got->max_qp_I == want->max_qp_I &&
			got->min_qp_P == want->min_qp_P && got->max_qp_P == want->max_qp_P &&
			got->min_qp_B == want->min_qp_B && got->max_qp_B == want->max_qp_B &&
			got->hvs_qp_enable == want->hvs_qp_enable &&
			got->hvs_qp_scale == want->hvs_qp_scale &&
			got->max_delta_qp == want->max_delta_qp &&
			got->qp_map_enable == want->qp_map_enable;
	}
	if (encoder->codec == VIDEO_BROKER_RDKX5_ENCODER_H265) {
		const mc_h265_cbr_params_t *want = &expected->h265_cbr_params;
		const mc_h265_cbr_params_t *got = &actual->h265_cbr_params;
		return got->intra_period == want->intra_period && got->intra_qp == want->intra_qp &&
			got->bit_rate == want->bit_rate && got->frame_rate == want->frame_rate &&
			got->initial_rc_qp == want->initial_rc_qp &&
			got->vbv_buffer_size == want->vbv_buffer_size &&
			got->ctu_level_rc_enalbe == want->ctu_level_rc_enalbe &&
			got->min_qp_I == want->min_qp_I && got->max_qp_I == want->max_qp_I &&
			got->min_qp_P == want->min_qp_P && got->max_qp_P == want->max_qp_P &&
			got->min_qp_B == want->min_qp_B && got->max_qp_B == want->max_qp_B &&
			got->hvs_qp_enable == want->hvs_qp_enable &&
			got->hvs_qp_scale == want->hvs_qp_scale &&
			got->max_delta_qp == want->max_delta_qp &&
			got->qp_map_enable == want->qp_map_enable;
	}
	{
		const mc_h264_cbr_params_t *want = &expected->h264_cbr_params;
		const mc_h264_cbr_params_t *got = &actual->h264_cbr_params;
		return got->intra_period == want->intra_period && got->intra_qp == want->intra_qp &&
			got->bit_rate == want->bit_rate && got->frame_rate == want->frame_rate &&
			got->initial_rc_qp == want->initial_rc_qp &&
			got->vbv_buffer_size == want->vbv_buffer_size &&
			got->mb_level_rc_enalbe == want->mb_level_rc_enalbe &&
			got->min_qp_I == want->min_qp_I && got->max_qp_I == want->max_qp_I &&
			got->min_qp_P == want->min_qp_P && got->max_qp_P == want->max_qp_P &&
			got->min_qp_B == want->min_qp_B && got->max_qp_B == want->max_qp_B &&
			got->hvs_qp_enable == want->hvs_qp_enable &&
			got->hvs_qp_scale == want->hvs_qp_scale &&
			got->max_delta_qp == want->max_delta_qp &&
			got->qp_map_enable == want->qp_map_enable;
	}
}

static void record_rate_control_state(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	int frame_rate,
	int bitrate_kbps
) {
	encoder->frame_rate = frame_rate;
	encoder->bitrate_kbps = bitrate_kbps;
	encoder->vbv_buffer_ms = rate_control_vbv_buffer_ms(encoder);
	rate_control_qp_bounds(
		encoder, frame_rate, bitrate_kbps,
		&encoder->max_qp_i, &encoder->max_qp_p, &encoder->max_qp_b);
}

static int configure_rate_control(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	int frame_rate,
	int bitrate_kbps
) {
	mc_rate_control_params_t params;
	mc_rate_control_params_t applied;
	if (encoder == NULL) {
		return -1;
	}
	memset(&params, 0, sizeof(params));
	params.mode = rate_control_mode(encoder);
	if (hb_mm_mc_get_rate_control_config(&encoder->context, &params) != 0 ||
	    configure_rate_control_values(encoder, &params, frame_rate, bitrate_kbps, false) != 0 ||
	    hb_mm_mc_set_rate_control_config(&encoder->context, &params) != 0) {
		return -1;
	}
	memset(&applied, 0, sizeof(applied));
	applied.mode = rate_control_mode(encoder);
	if (hb_mm_mc_get_rate_control_config(&encoder->context, &applied) != 0 ||
	    !rate_control_values_match(encoder, &params, &applied)) {
		return -1;
	}
	encoder->context.video_enc_params.rc_params = applied;
	record_rate_control_state(encoder, frame_rate, bitrate_kbps);
	return 0;
}

int video_broker_rdkx5_persistent_encoder_init(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	video_broker_rdkx5_encoder_codec_t codec,
	video_broker_rdkx5_rate_control_profile_t rate_control_profile,
	int instance_index,
	int width,
	int height,
	int frame_rate,
	int bitrate_kbps
) {
	mc_video_codec_enc_params_t *params;
	mc_av_codec_startup_params_t startup_params;
	mc_rate_control_params_t applied_rate_control;
	size_t frame_size;
	if (encoder == NULL || !supported_codec(codec) ||
	    !supported_rate_control_profile(rate_control_profile) || instance_index < 0 ||
	    frame_rate <= 0 || frame_rate > 240 || bitrate_kbps <= 0 || bitrate_kbps > 700000 ||
	    !valid_encoder_geometry(codec, width, height) ||
	    checked_frame_size(width, height, &frame_size) != 0) {
		return -1;
	}
	if (encoder->initialized || encoder->started || encoder->close_error != 0 ||
	    video_broker_rdkx5_encoder_resources_quarantined()) {
		fprintf(stderr, "[video-broker][encoder-release] event=open_rejected"
			" reason=ownership_not_released action=no_sdk_allocation\n");
		return -1;
	}
	memset(encoder, 0, sizeof(*encoder));
	encoder->codec = codec;
	encoder->rate_control_profile = rate_control_profile;
	encoder->instance_index = instance_index;
	encoder->width = width;
	encoder->height = height;
	if (hb_mm_mc_get_default_context(media_codec_id(codec), true, &encoder->context) != 0) {
		return -1;
	}
	encoder->context.instance_index = instance_index;
	params = &encoder->context.video_enc_params;
	params->width = width;
	params->height = height;
	params->pix_fmt = MC_PIXEL_FORMAT_NV12;
	params->frame_buf_count = kCodecBufferCount;
	params->external_frame_buf = false;
	params->bitstream_buf_count = kCodecBufferCount;
	params->bitstream_buf_size = (hb_u32)((frame_size + 0x3ffU) & ~0x3ffU);
	params->gop_params.gop_preset_idx = 1;
	params->gop_params.decoding_refresh_type = 2;
	params->rot_degree = MC_CCW_0;
	params->mir_direction = MC_DIRECTION_NONE;
	params->frame_cropping_flag = false;
	params->enable_user_pts = 0;
	params->rc_params.mode = rate_control_mode(encoder);
	if (hb_mm_mc_get_rate_control_config(&encoder->context, &params->rc_params) != 0 ||
	    configure_rate_control_values(
		encoder, &params->rc_params, frame_rate, bitrate_kbps, true) != 0) {
		return -1;
	}
	int code = hb_mm_mc_initialize(&encoder->context);
	if (code != 0) return encoder_release_failure(encoder, "sdk_initialize", code);
	encoder->initialized = true;
	{
		hb_s32 explicit_header = 0;
		/*
		 * This is a static codec property and therefore must be applied after
		 * initialize and before configure.  Do not rely on the SDK default:
		 * X5 images have shipped with both behaviours.  A combined
		 * VPS/SPS/PPS+IDR (or SPS/PPS+IDR) access unit is the only output that
		 * can safely cross the one-input/one-output realtime boundary below.
		 */
		if (hb_mm_mc_set_explicit_header_config(&encoder->context, 1) != 0 ||
		    hb_mm_mc_get_explicit_header_config(&encoder->context, &explicit_header) != 0 ||
		    explicit_header != 1) {
			video_broker_rdkx5_persistent_encoder_destroy(encoder);
			return -1;
		}
	}
	if (hb_mm_mc_configure(&encoder->context) != 0) {
		video_broker_rdkx5_persistent_encoder_destroy(encoder);
		return -1;
	}
	memset(&startup_params, 0, sizeof(startup_params));
	if (hb_mm_mc_start(&encoder->context, &startup_params) != 0) {
		video_broker_rdkx5_persistent_encoder_destroy(encoder);
		return -1;
	}
	encoder->started = true;
	memset(&applied_rate_control, 0, sizeof(applied_rate_control));
	applied_rate_control.mode = rate_control_mode(encoder);
	if (hb_mm_mc_get_rate_control_config(&encoder->context, &applied_rate_control) != 0 ||
	    !rate_control_values_match(encoder, &params->rc_params, &applied_rate_control)) {
		video_broker_rdkx5_persistent_encoder_destroy(encoder);
		return -1;
	}
	params->rc_params = applied_rate_control;
	record_rate_control_state(encoder, frame_rate, bitrate_kbps);
	return 0;
}

int video_broker_rdkx5_persistent_encoder_set_control(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	bool active,
	int frame_rate,
	int bitrate_kbps
) {
	bool activating;
	bool rate_control_changed;
	if (encoder == NULL || !encoder->started || encoder->close_error != 0 ||
	    encoder->queue_faulted || video_broker_rdkx5_encoder_resources_quarantined() || frame_rate <= 0 || bitrate_kbps <= 0) {
		return VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ERROR;
	}
	if (!active) {
		encoder->active = false;
		return VIDEO_BROKER_RDKX5_ENCODER_CONTROL_UNCHANGED;
	}
	activating = !encoder->active;
	rate_control_changed = encoder->frame_rate != frame_rate ||
		encoder->bitrate_kbps != bitrate_kbps;
	if (!activating && !rate_control_changed) {
		return VIDEO_BROKER_RDKX5_ENCODER_CONTROL_UNCHANGED;
	}
	if (rate_control_changed &&
	    configure_rate_control(encoder, frame_rate, bitrate_kbps) != 0) {
		return VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ERROR;
	}
	if (request_decoder_refresh_idr(encoder) != 0) {
		return VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ERROR;
	}
	if (!activating) {
		return VIDEO_BROKER_RDKX5_ENCODER_CONTROL_RATE_CHANGED;
	}
	encoder->active = true;
	encoder->epoch++;
	if (encoder->epoch == 0) {
		encoder->epoch++;
	}
	return VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED;
}

int video_broker_rdkx5_persistent_encoder_request_idr(
	video_broker_rdkx5_persistent_encoder_t *encoder
) {
	if (encoder == NULL || !encoder->started || !encoder->active || encoder->close_error != 0 || encoder->queue_faulted ||
	    video_broker_rdkx5_encoder_resources_quarantined()) {
		return -1;
	}
	return request_decoder_refresh_idr(encoder);
}


int video_broker_rdkx5_persistent_encoder_encode(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	const unsigned char *frame,
	size_t frame_size,
	unsigned char *stream,
	size_t stream_capacity,
	uint64_t *epoch,
	int *bitrate_kbps
) {
	media_codec_buffer_t input_buffer;
	media_codec_buffer_t output_buffer;
	media_codec_output_buffer_info_t output_info;
	size_t expected_size;
	size_t luma_size;
	int luma_stride;
	int chroma_stride;
	int result = -1;
	int sdk_code;
	const char *output_failure_stage = "output_layout";
	if (epoch != NULL) *epoch = 0;
	if (bitrate_kbps != NULL) *bitrate_kbps = 0;
	if (encoder != NULL) {
		encoder->encode_delay_valid = false;
		encoder->encode_latency.phase = NULL;
	}
	if (encoder == NULL || frame == NULL || stream == NULL || epoch == NULL ||
	    bitrate_kbps == NULL || !encoder->started || encoder->close_error != 0 || encoder->queue_faulted ||
	    video_broker_rdkx5_encoder_resources_quarantined() ||
	    checked_frame_size(encoder->width, encoder->height, &expected_size) != 0 ||
	    frame_size != expected_size || stream_capacity == 0) {
		return -1;
	}
	*epoch = 0;
	*bitrate_kbps = 0;
	if (!encoder->active) {
		return 0;
	}
	memset(&input_buffer, 0, sizeof(input_buffer));
	encoder_checkpoint(encoder, "dequeue_input_buffer");
	sdk_code = hb_mm_mc_dequeue_input_buffer(
		&encoder->context, &input_buffer, kCodecTimeoutMs);
	encoder_checkpoint(encoder, "input_copy");
	if (sdk_code != 0) {
		return encoder_encode_failure(encoder, "dequeue_input", sdk_code, frame_size, stream_capacity, true);
	}
	luma_size = (size_t)encoder->width * (size_t)encoder->height;
	if (input_buffer.vframe_buf.vir_ptr[0] == NULL ||
	    input_buffer.vframe_buf.vir_ptr[1] == NULL) {
		return encoder_encode_failure(encoder, "input_planes", 0, frame_size, stream_capacity, true);
	}
	luma_stride = input_buffer.vframe_buf.stride > 0 ?
		input_buffer.vframe_buf.stride : encoder->width;
	chroma_stride = input_buffer.vframe_buf.vstride > 0 ?
		input_buffer.vframe_buf.vstride : encoder->width;
	if (luma_stride < encoder->width || chroma_stride < encoder->width) {
		return encoder_encode_failure(encoder, "input_strides", 0, frame_size, stream_capacity, true);
	}
	input_buffer.type = MC_VIDEO_FRAME_BUFFER;
	input_buffer.vframe_buf.width = encoder->width;
	input_buffer.vframe_buf.height = encoder->height;
	input_buffer.vframe_buf.pix_fmt = MC_PIXEL_FORMAT_NV12;
	input_buffer.vframe_buf.size = (hb_u32)expected_size;
	for (int row = 0; row < encoder->height; ++row) {
		unsigned char *destination = input_buffer.vframe_buf.vir_ptr[0] +
			(size_t)row * (size_t)luma_stride;
		memcpy(destination, frame + (size_t)row * (size_t)encoder->width,
			(size_t)encoder->width);
		if (luma_stride > encoder->width) {
			memset(destination + encoder->width, 0, (size_t)(luma_stride - encoder->width));
		}
	}
	for (int row = 0; row < encoder->height / 2; ++row) {
		unsigned char *destination = input_buffer.vframe_buf.vir_ptr[1] +
			(size_t)row * (size_t)chroma_stride;
		memcpy(destination, frame + luma_size + (size_t)row * (size_t)encoder->width,
			(size_t)encoder->width);
		if (chroma_stride > encoder->width) {
			memset(destination + encoder->width, 128,
				(size_t)(chroma_stride - encoder->width));
		}
	}
	encoder_checkpoint(encoder, "queue_input_buffer");
	sdk_code = hb_mm_mc_queue_input_buffer(
		&encoder->context, &input_buffer, kCodecTimeoutMs);
	encoder_checkpoint(encoder, "output_prepare");
	if (sdk_code != 0) {
		return encoder_encode_failure(encoder, "queue_input", sdk_code, frame_size, stream_capacity, true);
	}
	memset(&output_buffer, 0, sizeof(output_buffer));
	memset(&output_info, 0, sizeof(output_info));
	encoder_checkpoint(encoder, "dequeue_output_buffer");
	sdk_code = hb_mm_mc_dequeue_output_buffer(
		&encoder->context, &output_buffer, &output_info, kCodecTimeoutMs);
	encoder_checkpoint(encoder, "output_copy");
	if (sdk_code != 0) {
		return encoder_encode_failure(encoder, "dequeue_output", sdk_code, frame_size, stream_capacity, true);
	}
	if (output_buffer.type == MC_VIDEO_STREAM_BUFFER &&
	    output_buffer.vstream_buf.vir_ptr != NULL &&
	    output_buffer.vstream_buf.size <= stream_capacity) {
		output_failure_stage = "bootstrap_copy";
		result = video_broker_h26x_bootstrap_copy(&encoder->bootstrap,
			encoder->codec == VIDEO_BROKER_RDKX5_ENCODER_H265,
			output_buffer.vstream_buf.vir_ptr, output_buffer.vstream_buf.size,
			stream, stream_capacity);
	}
	encoder_checkpoint(encoder, "queue_output_buffer");
	sdk_code = hb_mm_mc_queue_output_buffer(
		&encoder->context, &output_buffer, kCodecTimeoutMs);
	encoder_checkpoint(encoder, "encode_complete");
	if (sdk_code != 0) {
		return encoder_encode_failure(encoder, "queue_output", sdk_code, frame_size, stream_capacity, true);
	}
	if (result < 0) {
		return encoder_encode_failure(encoder, output_failure_stage, 0, frame_size, stream_capacity, false);
	}
	if (result >= 0) {
		*epoch = encoder->epoch;
		*bitrate_kbps = encoder->bitrate_kbps;
	}
	return result;
}

int video_broker_rdkx5_persistent_encoder_destroy(
	video_broker_rdkx5_persistent_encoder_t *encoder) {
	if (encoder == NULL) {
		return 0;
	}
	if (encoder->close_error != 0) return -1;
	encoder->active = false;
	if (encoder->started) {
		int code = hb_mm_mc_pause(&encoder->context);
		if (code != 0) return encoder_release_failure(encoder, "sdk_pause", code);
		encoder->started = false;
	}
	if (encoder->initialized) {
		int code = hb_mm_mc_release(&encoder->context);
		if (code != 0) return encoder_release_failure(encoder, "sdk_release", code);
		encoder->initialized = false;
	}
	memset(encoder, 0, sizeof(*encoder));
	return 0;
}
