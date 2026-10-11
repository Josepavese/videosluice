/* Video-broker persistent encoder contract test. */
#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdio.h>
#include <sys/wait.h>
#include <unistd.h>

#include "../rdkx5_persistent_encoder.h"

typedef struct {
	int initialize_calls;
	int configure_calls;
	int start_calls;
	int pause_calls;
	int release_calls;
	int initialize_error, configure_error, start_error, pause_error, release_error;
	int set_rate_calls;
	bool corrupt_rate_readback;
	int request_idr_calls;
	int request_idr_header_calls;
	hb_u32 last_idr_header_mode;
	bool fail_idr_header;
	int get_explicit_header_calls;
	int set_explicit_header_calls;
	hb_s32 explicit_header_status;
	bool corrupt_explicit_header_readback;
	int input_queue_calls;
	int output_queue_calls;
	int dequeue_input_error, queue_input_error, dequeue_output_error, queue_output_error;
	int dequeue_input_calls, dequeue_output_calls;
	int output_delay_ms;
	int input_stride;
	int input_chroma_stride;
	unsigned char y[1280 * 720];
	unsigned char uv[1280 * 720 / 2];
	unsigned char stream[64];
	const unsigned char *output_override;
	size_t output_size;
} fake_codec_t;

static fake_codec_t fake;

static void pause_ms(long milliseconds) {
	struct timespec delay = {
		.tv_sec = milliseconds / 1000,
		.tv_nsec = (milliseconds % 1000) * 1000000L,
	};
	while (nanosleep(&delay, &delay) != 0) {
	}
}

static uint64_t monotonic_ms(void) {
	struct timespec value;
	assert(clock_gettime(CLOCK_MONOTONIC, &value) == 0);
	return (uint64_t)value.tv_sec * 1000ULL + (uint64_t)value.tv_nsec / 1000000ULL;
}

hb_s32 hb_mm_mc_get_default_context(
	media_codec_id_t codec_id,
	hb_bool encoder,
	media_codec_context_t *context
) {
	memset(context, 0, sizeof(*context));
	context->codec_id = codec_id;
	context->encoder = encoder;
	context->test_private = &fake;
	context->video_enc_params.rc_params.mode = codec_id == MEDIA_CODEC_ID_H265 ?
		MC_AV_RC_MODE_H265CBR : MC_AV_RC_MODE_H264CBR;
	return 0;
}

hb_s32 hb_mm_mc_initialize(media_codec_context_t *context) {
	(void)context;
	fake.initialize_calls++;
	return fake.initialize_error;
}

hb_s32 hb_mm_mc_configure(media_codec_context_t *context) {
	(void)context;
	fake.configure_calls++;
	return fake.configure_error;
}

hb_s32 hb_mm_mc_start(
	media_codec_context_t *context,
	mc_av_codec_startup_params_t *params
) {
	(void)context;
	(void)params;
	fake.start_calls++;
	pause_ms(180);
	return fake.start_error;
}

hb_s32 hb_mm_mc_pause(media_codec_context_t *context) {
	(void)context;
	fake.pause_calls++;
	return fake.pause_error;
}

hb_s32 hb_mm_mc_release(media_codec_context_t *context) {
	(void)context;
	fake.release_calls++;
	return fake.release_error;
}

hb_s32 hb_mm_mc_get_rate_control_config(
	media_codec_context_t *context,
	mc_rate_control_params_t *params
) {
	*params = context->video_enc_params.rc_params;
	if (fake.corrupt_rate_readback && fake.set_rate_calls > 0) {
		if (params->mode == MC_AV_RC_MODE_H265CBR) {
			params->h265_cbr_params.bit_rate++;
		} else if (params->mode == MC_AV_RC_MODE_H265AVBR) {
			params->h265_avbr_params.bit_rate++;
		} else if (params->mode == MC_AV_RC_MODE_H264AVBR) {
			params->h264_avbr_params.bit_rate++;
		} else {
			params->h264_cbr_params.bit_rate++;
		}
	}
	return 0;
}

hb_s32 hb_mm_mc_set_rate_control_config(
	media_codec_context_t *context,
	const mc_rate_control_params_t *params
) {
	context->video_enc_params.rc_params = *params;
	fake.set_rate_calls++;
	return 0;
}

hb_s32 hb_mm_mc_request_idr_frame(media_codec_context_t *context) {
	(void)context;
	fake.request_idr_calls++;
	return 0;
}

hb_s32 hb_mm_mc_request_idr_header(
	media_codec_context_t *context,
	hb_u32 force_header
) {
	(void)context;
	fake.request_idr_header_calls++;
	fake.last_idr_header_mode = force_header;
	return fake.fail_idr_header ? -1 : 0;
}

hb_s32 hb_mm_mc_get_explicit_header_config(
	media_codec_context_t *context,
	hb_s32 *status
) {
	(void)context;
	fake.get_explicit_header_calls++;
	*status = fake.corrupt_explicit_header_readback ? 0 : fake.explicit_header_status;
	return 0;
}

hb_s32 hb_mm_mc_set_explicit_header_config(
	media_codec_context_t *context,
	hb_s32 status
) {
	(void)context;
	fake.set_explicit_header_calls++;
	fake.explicit_header_status = status;
	return 0;
}

hb_s32 hb_mm_mc_dequeue_input_buffer(
	media_codec_context_t *context,
	media_codec_buffer_t *buffer,
	hb_s32 timeout
) {
	(void)context;
	assert(timeout == 1000);
	fake.dequeue_input_calls++;
	if (fake.dequeue_input_error) return fake.dequeue_input_error;
	buffer->type = MC_VIDEO_FRAME_BUFFER;
	buffer->vframe_buf.vir_ptr[0] = fake.y;
	buffer->vframe_buf.vir_ptr[1] = fake.uv;
	buffer->vframe_buf.stride = fake.input_stride;
	buffer->vframe_buf.vstride = fake.input_chroma_stride;
	return 0;
}

hb_s32 hb_mm_mc_queue_input_buffer(
	media_codec_context_t *context,
	media_codec_buffer_t *buffer,
	hb_s32 timeout
) {
	(void)context;
	assert(timeout == 1000 && buffer->type == MC_VIDEO_FRAME_BUFFER);
	fake.input_queue_calls++;
	return fake.queue_input_error;
}

hb_s32 hb_mm_mc_dequeue_output_buffer(
	media_codec_context_t *context,
	media_codec_buffer_t *buffer,
	media_codec_output_buffer_info_t *info,
	hb_s32 timeout
) {
	(void)context;
	(void)info;
	assert(timeout == 1000);
	fake.dequeue_output_calls++;
	if (fake.output_delay_ms) pause_ms(fake.output_delay_ms);
	if (fake.dequeue_output_error) return fake.dequeue_output_error;
	memset(fake.stream, 0x5a, sizeof(fake.stream));
	buffer->type = MC_VIDEO_STREAM_BUFFER;
	buffer->vstream_buf.vir_ptr = fake.stream;
	buffer->vstream_buf.size = sizeof(fake.stream);
	if (fake.output_override != NULL) {
		buffer->vstream_buf.vir_ptr = (unsigned char *)fake.output_override;
		buffer->vstream_buf.size = (hb_u32)fake.output_size;
	}
	return 0;
}

hb_s32 hb_mm_mc_queue_output_buffer(
	media_codec_context_t *context,
	media_codec_buffer_t *buffer,
	hb_s32 timeout
) {
	(void)context;
	assert(timeout == 1000 && buffer->type == MC_VIDEO_STREAM_BUFFER);
	fake.output_queue_calls++;
	return fake.queue_output_error;
}

static void assert_h264_realtime_contract(
	const video_broker_rdkx5_persistent_encoder_t *encoder,
	hb_u32 expected_intra_qp,
	hb_u32 expected_initial_qp
) {
	const mc_h264_cbr_params_t *params =
		&encoder->context.video_enc_params.rc_params.h264_cbr_params;
	assert(encoder->codec == VIDEO_BROKER_RDKX5_ENCODER_H264);
	assert(encoder->rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR);
	assert(encoder->instance_index == 0 && encoder->context.instance_index == 0);
	assert(encoder->vbv_buffer_ms == 10);
	assert(encoder->max_qp_i == 51 && encoder->max_qp_p == 51 && encoder->max_qp_b == 51);
	assert(params->intra_period == (hb_u32)encoder->frame_rate);
	assert(params->intra_qp == expected_intra_qp && params->initial_rc_qp == expected_initial_qp);
	assert(params->vbv_buffer_size == 10);
	assert(params->min_qp_I == 8 && params->max_qp_I == 51);
	assert(params->min_qp_P == 8 && params->max_qp_P == 51);
	assert(params->min_qp_B == 8 && params->max_qp_B == 51);
	assert(params->hvs_qp_enable && params->hvs_qp_scale == 2 && params->max_delta_qp == 10);
	assert(params->mb_level_rc_enalbe && !params->qp_map_enable);
}

static void test_live_runtime_control(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	unsigned char *frame = calloc(1, 1280 * 720 * 3 / 2);
	unsigned char stream[128] = {0};
	uint64_t epoch = 0;
	uint64_t before;
	int bitrate = 0;
	assert(frame != NULL);
	memset(frame, 0x11, 1280 * 720);
	memset(frame + 1280 * 720, 0x22, 1280 * 720 / 2);

	before = monotonic_ms();
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H264,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,
		0, 1280, 720, 20, 6144) == 0);
	assert(monotonic_ms() - before >= 150);
	assert(fake.initialize_calls == 1 && fake.configure_calls == 1 && fake.start_calls == 1);
	assert(fake.set_explicit_header_calls == 1 && fake.get_explicit_header_calls == 1 &&
		fake.explicit_header_status == 1);
	assert(!encoder.active && encoder.epoch == 0);
	assert_h264_realtime_contract(&encoder, 36, 36);

	before = monotonic_ms();
	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, true, 10, 3072) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED);
	assert(monotonic_ms() - before < 30);
	assert(encoder.active && encoder.epoch == 1);
	assert(fake.start_calls == 1 && fake.set_rate_calls == 1 && fake.request_idr_calls == 1);
	assert(fake.request_idr_header_calls == 1 && fake.last_idr_header_mode == 1);
	assert(encoder.context.video_enc_params.rc_params.h264_cbr_params.frame_rate == 10);
	assert(encoder.context.video_enc_params.rc_params.h264_cbr_params.bit_rate == 3072);
	assert_h264_realtime_contract(&encoder, 36, 36);

	assert(video_broker_rdkx5_persistent_encoder_encode(
		&encoder, frame, 1280 * 720 * 3 / 2, stream, sizeof(stream), &epoch, &bitrate) == 64);
	assert(epoch == 1 && bitrate == 3072 && stream[0] == 0x5a);
	assert(fake.y[0] == 0x11 && fake.uv[0] == 0x22);
	assert(fake.input_queue_calls == 1 && fake.output_queue_calls == 1);

	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, true, 10, 3072) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_UNCHANGED);
	assert(fake.set_rate_calls == 1 && fake.request_idr_calls == 1 &&
		fake.request_idr_header_calls == 1 && fake.start_calls == 1);
	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, true, 8, 2048) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_RATE_CHANGED);
	assert(encoder.epoch == 1 && fake.set_rate_calls == 2 &&
		fake.request_idr_calls == 2 && fake.request_idr_header_calls == 2);
	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, true, 20, 1024) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_RATE_CHANGED);
	assert(encoder.epoch == 1 && encoder.max_qp_i == 51 &&
		encoder.max_qp_p == 51 && encoder.max_qp_b == 51);

	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, false, 20, 1024) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_UNCHANGED);
	assert(video_broker_rdkx5_persistent_encoder_encode(
		&encoder, frame, 1280 * 720 * 3 / 2, stream, sizeof(stream), &epoch, &bitrate) == 0);
	assert(fake.pause_calls == 0 && fake.release_calls == 0 && fake.start_calls == 1);
	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, true, 20, 1024) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED);
	assert(encoder.epoch == 2 && fake.request_idr_calls == 4 &&
		fake.request_idr_header_calls == 4 && fake.start_calls == 1);

	video_broker_rdkx5_persistent_encoder_destroy(&encoder);
	assert(fake.pause_calls == 1 && fake.release_calls == 1 && fake.start_calls == 1);
	free(frame);
}

static void test_h265_quality_contract(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	const mc_h265_cbr_params_t *params;
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_CBR,
		1, 1920, 1080, 30, 8192) == 0);
	params = &encoder.context.video_enc_params.rc_params.h265_cbr_params;
	assert(encoder.codec == VIDEO_BROKER_RDKX5_ENCODER_H265);
	assert(encoder.rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_CBR);
	assert(encoder.instance_index == 1 && encoder.context.instance_index == 1);
	assert(encoder.vbv_buffer_ms == 1000);
	assert(encoder.max_qp_i == 34 && encoder.max_qp_p == 36 && encoder.max_qp_b == 38);
	assert(params->intra_period == 30 && params->bit_rate == 8192 && params->frame_rate == 30);
	assert(params->vbv_buffer_size == 1000);
	assert(params->min_qp_I == 8 && params->max_qp_I == 34);
	assert(params->min_qp_P == 8 && params->max_qp_P == 36);
	assert(params->min_qp_B == 8 && params->max_qp_B == 38);
	assert(params->hvs_qp_enable && params->hvs_qp_scale == 1 && params->max_delta_qp == 4);
	assert(!params->ctu_level_rc_enalbe && !params->qp_map_enable);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 30, 8192) ==
		VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED);
	assert(encoder.epoch == 1 && fake.request_idr_calls == 1 &&
		fake.request_idr_header_calls == 1 && fake.last_idr_header_mode == 1);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 30, 4096) ==
		VIDEO_BROKER_RDKX5_ENCODER_CONTROL_RATE_CHANGED);
	assert(encoder.epoch == 1 && encoder.max_qp_i == 39 &&
		encoder.max_qp_p == 41 && encoder.max_qp_b == 43);
	assert(params->bit_rate == 4096 && params->max_qp_P == 41);
	video_broker_rdkx5_persistent_encoder_destroy(&encoder);
	assert(fake.pause_calls == 1 && fake.release_calls == 1);
}

static void test_h265_realtime_contract(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	const mc_h265_cbr_params_t *params;
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,
		0, 1280, 720, 30, 4096) == 0);
	params = &encoder.context.video_enc_params.rc_params.h265_cbr_params;
	assert(encoder.rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR);
	assert(encoder.vbv_buffer_ms == 10);
	assert(encoder.max_qp_i == 51 && encoder.max_qp_p == 51 && encoder.max_qp_b == 51);
	assert(params->intra_qp == 36 && params->initial_rc_qp == 36);
	assert(params->vbv_buffer_size == 10);
	assert(params->min_qp_I == 8 && params->max_qp_I == 51);
	assert(params->min_qp_P == 8 && params->max_qp_P == 51);
	assert(params->min_qp_B == 8 && params->max_qp_B == 51);
	assert(params->ctu_level_rc_enalbe && !params->qp_map_enable);
	assert(params->hvs_qp_enable && params->hvs_qp_scale == 2 && params->max_delta_qp == 10);
	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, true, 20, 3072) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED);
	assert(params->frame_rate == 20 && params->bit_rate == 3072);
	assert(params->intra_qp == 36 && params->initial_rc_qp == 36);
	assert(params->vbv_buffer_size == 10 && params->ctu_level_rc_enalbe);
	video_broker_rdkx5_persistent_encoder_destroy(&encoder);
	assert(fake.pause_calls == 1 && fake.release_calls == 1);
}

static void test_h265_avbr_recorder_contract(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	const mc_h265_avbr_params_t *params;
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR,
		1, 640, 360, 15, 750) == 0);
	params = &encoder.context.video_enc_params.rc_params.h265_avbr_params;
	assert(encoder.context.video_enc_params.rc_params.mode == MC_AV_RC_MODE_H265AVBR);
	assert(encoder.rate_control_profile == VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR);
	assert(encoder.vbv_buffer_ms == 1000);
	assert(encoder.max_qp_i == 34 && encoder.max_qp_p == 36 && encoder.max_qp_b == 38);
	assert(params->intra_period == 15 && params->bit_rate == 750 && params->frame_rate == 15);
	assert(params->vbv_buffer_size == 1000);
	assert(params->min_qp_I == 8 && params->max_qp_I == 34);
	assert(params->min_qp_P == 8 && params->max_qp_P == 36);
	assert(params->min_qp_B == 8 && params->max_qp_B == 38);
	assert(params->hvs_qp_enable && params->hvs_qp_scale == 1 && params->max_delta_qp == 4);
	assert(!params->ctu_level_rc_enalbe && !params->qp_map_enable);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 15, 750) ==
		VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED);
	assert(encoder.epoch == 1 && fake.request_idr_calls == 1 &&
		fake.request_idr_header_calls == 1 && fake.last_idr_header_mode == 1);
	video_broker_rdkx5_persistent_encoder_destroy(&encoder);
}

static void test_runtime_rate_control_readback_is_fail_closed(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,
		0, 1280, 720, 30, 4096) == 0);
	fake.corrupt_rate_readback = true;
	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, true, 20, 3072) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ERROR);
	assert(!encoder.active && encoder.epoch == 0);
	assert(encoder.frame_rate == 30 && encoder.bitrate_kbps == 4096);
	assert(fake.request_idr_calls == 0);
	video_broker_rdkx5_persistent_encoder_destroy(&encoder);
}

static void test_explicit_header_readback_is_fail_closed(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	memset(&fake, 0, sizeof(fake));
	fake.corrupt_explicit_header_readback = true;
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,
		0, 1280, 720, 30, 4096) == -1);
	assert(fake.initialize_calls == 1 && fake.configure_calls == 0 && fake.start_calls == 0);
	assert(fake.set_explicit_header_calls == 1 && fake.get_explicit_header_calls == 1);
	assert(fake.release_calls == 1);
}

static void test_invalid_geometry_fails_before_vendor_initialization(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_CBR,
		1, 960, 540, 30, 4096) == -1);
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_CBR,
		1, 840, 472, 30, 4096) == -1);
	assert(fake.initialize_calls == 0 && fake.configure_calls == 0 && fake.start_calls == 0);
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H264,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,
		0, 120, 128, 30, 4096) == -1);
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H264,
		(video_broker_rdkx5_rate_control_profile_t)0,
		0, 1280, 720, 30, 4096) == -1);
	assert(fake.initialize_calls == 0 && fake.configure_calls == 0 && fake.start_calls == 0);
}

static void test_encoder_input_honors_vendor_plane_strides(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	const int width = 848;
	const int height = 472;
	const int luma_stride = 896;
	const int chroma_stride = 864;
	const size_t luma_size = (size_t)width * (size_t)height;
	const size_t frame_size = luma_size + luma_size / 2U;
	unsigned char *frame = malloc(frame_size);
	unsigned char stream[128] = {0};
	uint64_t epoch = 0;
	int bitrate = 0;
	assert(frame != NULL);
	memset(&fake, 0, sizeof(fake));
	fake.input_stride = luma_stride;
	fake.input_chroma_stride = chroma_stride;
	memset(fake.y, 0xee, sizeof(fake.y));
	memset(fake.uv, 0xee, sizeof(fake.uv));
	for (int row = 0; row < height; ++row) {
		memset(frame + (size_t)row * (size_t)width, row & 0xff, (size_t)width);
	}
	for (int row = 0; row < height / 2; ++row) {
		memset(frame + luma_size + (size_t)row * (size_t)width,
			(128 + row) & 0xff, (size_t)width);
	}
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_CBR, 1,
		width, height, 30, 6144) == 0);
	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, true, 30, 6144) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED);
	assert(video_broker_rdkx5_persistent_encoder_encode(
		&encoder, frame, frame_size, stream, sizeof(stream), &epoch, &bitrate) == 64);
	for (int row = 0; row < height; ++row) {
		const unsigned char *encoded = fake.y + (size_t)row * (size_t)luma_stride;
		assert(encoded[0] == (unsigned char)(row & 0xff));
		assert(encoded[width - 1] == (unsigned char)(row & 0xff));
		for (int column = width; column < luma_stride; ++column) {
			assert(encoded[column] == 0);
		}
	}
	for (int row = 0; row < height / 2; ++row) {
		const unsigned char *encoded = fake.uv + (size_t)row * (size_t)chroma_stride;
		assert(encoded[0] == (unsigned char)((128 + row) & 0xff));
		assert(encoded[width - 1] == (unsigned char)((128 + row) & 0xff));
		for (int column = width; column < chroma_stride; ++column) {
			assert(encoded[column] == 128);
		}
	}
	video_broker_rdkx5_persistent_encoder_destroy(&encoder);
	free(frame);
}

static void test_explicit_idr_does_not_advance_encoder_epoch(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR, 1,
		1280, 720, 30, 4096) == 0);
	assert(video_broker_rdkx5_persistent_encoder_request_idr(&encoder) == -1);
	assert(fake.request_idr_calls == 0 && fake.request_idr_header_calls == 0 && encoder.epoch == 0);
	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, true, 30, 4096) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED);
	assert(fake.request_idr_calls == 1 && fake.request_idr_header_calls == 1 &&
		fake.last_idr_header_mode == 1 && encoder.epoch == 1);
	assert(video_broker_rdkx5_persistent_encoder_request_idr(&encoder) == 0);
	assert(fake.request_idr_calls == 2 && fake.request_idr_header_calls == 2 && encoder.epoch == 1);
	video_broker_rdkx5_persistent_encoder_destroy(&encoder);
}

static void test_missing_idr_headers_fail_closed(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(
		&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR, 1,
		1280, 720, 30, 4096) == 0);
	fake.fail_idr_header = true;
	assert(video_broker_rdkx5_persistent_encoder_set_control(
		&encoder, true, 30, 4096) == VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ERROR);
	assert(!encoder.active && encoder.epoch == 0);
	assert(fake.request_idr_header_calls == 1 && fake.request_idr_calls == 0);
	video_broker_rdkx5_persistent_encoder_destroy(&encoder);
}

static void test_warmup_parameter_sets_survive_publication_cutover(
	video_broker_rdkx5_encoder_codec_t codec
) {
	static const unsigned char h265_headers[] = {
		0,0,0,1,0x40,1,0x11, 0,0,1,0x42,1,0x22, 0,0,0,1,0x44,1,0x33,
	};
	static const unsigned char h264_headers[] = {
		0,0,1,0x67,0x11, 0,0,0,1,0x68,0x22,
	};
	static const unsigned char h265_idr[] = {0,0,0,1,0x26,1,0x44};
	static const unsigned char h264_idr[] = {0,0,1,0x65,0x44};
	const unsigned char *headers = codec == VIDEO_BROKER_RDKX5_ENCODER_H265 ? h265_headers : h264_headers;
	const unsigned char *idr = codec == VIDEO_BROKER_RDKX5_ENCODER_H265 ? h265_idr : h264_idr;
	size_t header_size = codec == VIDEO_BROKER_RDKX5_ENCODER_H265 ? sizeof(h265_headers) : sizeof(h264_headers);
	size_t idr_size = codec == VIDEO_BROKER_RDKX5_ENCODER_H265 ? sizeof(h265_idr) : sizeof(h264_idr);
	unsigned char bootstrap[128] = {0}, stream[128] = {0};
	size_t frame_size = 128U * 128U * 3U / 2U;
	unsigned char *frame = calloc(1, frame_size);
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	uint64_t epoch = 0;
	int bitrate = 0, output_queues;
	assert(frame != NULL);
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(&encoder, codec,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR, 0, 128, 128, 30, 4096) == 0);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 30, 4096) == 1);
	memcpy(bootstrap, headers, header_size);
	memcpy(bootstrap + header_size, idr, idr_size);
	{
		video_broker_h26x_bootstrap_t cache = {0};
		bool h265 = codec == VIDEO_BROKER_RDKX5_ENCODER_H265;
		const unsigned char *other_idr = h265 ? h264_idr : h265_idr;
		size_t other_size = h265 ? sizeof(h264_idr) : sizeof(h265_idr);
		memcpy(stream, bootstrap, header_size + idr_size);
		assert(video_broker_h26x_bootstrap_copy(&cache, h265, stream,
			header_size + idr_size, stream, sizeof(stream)) == (int)(header_size + idr_size));
		memcpy(stream, idr, idr_size);
		assert(video_broker_h26x_bootstrap_copy(&cache, h265, stream,
			idr_size, stream, sizeof(stream)) == (int)(header_size + idr_size));
		assert(memcmp(stream, bootstrap, header_size + idr_size) == 0);
		memcpy(stream, other_idr, other_size);
		assert(video_broker_h26x_bootstrap_copy(&cache, !h265, stream,
			other_size, stream, sizeof(stream)) == (int)other_size);
		assert(cache.size == 0 && memcmp(stream, other_idr, other_size) == 0);
	}
	fake.output_override = bootstrap;
	fake.output_size = header_size + idr_size;
	/* Private warmup consumes this output before the publication exists. */
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
		stream, sizeof(stream), &epoch, &bitrate) == (int)(header_size + idr_size));
	assert(memcmp(stream, bootstrap, header_size + idr_size) == 0);
	fake.output_override = idr;
	fake.output_size = idr_size;
	assert(video_broker_rdkx5_persistent_encoder_request_idr(&encoder) == 0);
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
		stream, sizeof(stream), &epoch, &bitrate) == (int)(header_size + idr_size));
	assert(memcmp(stream, bootstrap, header_size + idr_size) == 0);
	assert(epoch == 1 && fake.initialize_calls == 1 && fake.start_calls == 1);
	{
		unsigned char inter[] = {0,0,0,1,0x02,1,0x55};
		if (codec == VIDEO_BROKER_RDKX5_ENCODER_H264) inter[4] = 0x41;
		fake.output_override = inter;
		fake.output_size = sizeof(inter);
		assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
			stream, sizeof(stream), &epoch, &bitrate) == (int)sizeof(inter));
		assert(memcmp(stream, inter, sizeof(inter)) == 0);
	}
	/* An incomplete new family invalidates the old one; never mix parameter
	 * generations. A later complete hardware family replaces it atomically. */
	fake.output_override = headers;
	fake.output_size = codec == VIDEO_BROKER_RDKX5_ENCODER_H265 ? 7U : 5U;
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
		stream, sizeof(stream), &epoch, &bitrate) == (int)fake.output_size);
	fake.output_override = idr;
	fake.output_size = idr_size;
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
		stream, sizeof(stream), &epoch, &bitrate) == (int)idr_size);
	bootstrap[codec == VIDEO_BROKER_RDKX5_ENCODER_H265 ? 6 : 4] ^= 0x10;
	fake.output_override = bootstrap;
	fake.output_size = header_size; /* Complete parameter-only hardware output. */
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
		stream, sizeof(stream), &epoch, &bitrate) == (int)header_size);
	fake.output_override = idr;
	fake.output_size = idr_size;
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
		stream, sizeof(stream), &epoch, &bitrate) == (int)(header_size + idr_size));
	assert(memcmp(stream, bootstrap, header_size + idr_size) == 0);
	/* Rate changes and demand epochs must not lose the immutable codec config. */
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 15, 1843) == 2);
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
		stream, sizeof(stream), &epoch, &bitrate) == (int)(header_size + idr_size));
	assert(epoch == 1 && bitrate == 1843);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, false, 15, 1843) == 0);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 15, 1843) == 1);
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
		stream, sizeof(stream), &epoch, &bitrate) == (int)(header_size + idr_size));
	assert(epoch == 2);
	/* A too-small caller buffer must fail and still return the vendor buffer. */
	output_queues = fake.output_queue_calls;
	memset(stream, 0xee, sizeof(stream));
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
		stream, idr_size, &epoch, &bitrate) == -1);
	assert(fake.output_queue_calls == output_queues + 1 && stream[idr_size] == 0xee);
	{
		size_t huge_size = 65544;
		unsigned char *huge = malloc(huge_size), *destination = malloc(huge_size);
		assert(huge != NULL && destination != NULL);
		memset(huge, 0x55, huge_size);
		memcpy(huge, headers, codec == VIDEO_BROKER_RDKX5_ENCODER_H265 ? 7U : 5U);
		fake.output_override = huge;
		fake.output_size = huge_size;
		output_queues = fake.output_queue_calls;
		assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
			destination, huge_size, &epoch, &bitrate) == -1);
		assert(fake.output_queue_calls == output_queues + 1);
		free(huge);
		free(destination);
		fake.output_override = idr;
		fake.output_size = idr_size;
		assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
			stream, sizeof(stream), &epoch, &bitrate) == (int)idr_size);
	}
	video_broker_rdkx5_persistent_encoder_destroy(&encoder);
	/* A new physical encoder may never borrow the previous one's headers. */
	assert(video_broker_rdkx5_persistent_encoder_init(&encoder, codec,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR, 0, 128, 128, 30, 4096) == 0);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 30, 4096) == 1);
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, frame_size,
		stream, sizeof(stream), &epoch, &bitrate) == (int)idr_size);
	video_broker_rdkx5_persistent_encoder_destroy(&encoder);
	free(frame);
}

static void test_encode_failure_diagnostics(int codec) {
	const char *stages[] = {"", "dequeue_input", "queue_input", "dequeue_output", "queue_output", "output_layout"};
	for (int stage=0; stage<=5; stage++) {
		video_broker_rdkx5_persistent_encoder_t encoder = {0};
		unsigned char frame[128*128*3/2] = {0}, stream[128] = {0};
		uint64_t epoch = 123;
		int bitrate = 123, pipes[2], saved_stderr;
		char log[1024] = {0}, expected[80];
		memset(&fake, 0, sizeof(fake));
		assert(video_broker_rdkx5_persistent_encoder_init(&encoder, codec,
			VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR, 0, 128, 128, 20, 4096) == 0);
		assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 20, 4096) == 1);
		int *failures[] = {NULL, &fake.dequeue_input_error, &fake.queue_input_error,
			&fake.dequeue_output_error, &fake.queue_output_error};
		if (stage>=1 && stage<=4) *failures[stage] = -7300-stage;
		fflush(stderr);
		assert(pipe(pipes) == 0);
		saved_stderr = dup(STDERR_FILENO);
		assert(saved_stderr >= 0 && dup2(pipes[1], STDERR_FILENO) >= 0);
		close(pipes[1]);
		int result = video_broker_rdkx5_persistent_encoder_encode(&encoder,
			frame, sizeof(frame), stream, stage==5 ? 8 : sizeof(stream), &epoch, &bitrate);
		fflush(stderr);
		assert(dup2(saved_stderr, STDERR_FILENO) >= 0);
		close(saved_stderr);
		ssize_t length = read(pipes[0], log, sizeof(log)-1);
		assert(length >= 0);
		close(pipes[0]);
		if (stage == 0) {
			assert(result > 0 && length == 0 && epoch == 1 && bitrate == 4096);
		} else {
			assert(result == -1 && epoch == 0 && bitrate == 0);
			snprintf(expected, sizeof(expected), "event=encode_failed stage=%s sdk_code=%d",
				stages[stage], stage<=4 ? -7300-stage : 0);
			assert(strstr(log, expected) != NULL);
			assert(strstr(log, "sdk_timeout_ms=1000 action=return_failure") != NULL);
			assert(strchr(log, '\n') == log+length-1); /* Exactly one bounded failure record. */
		}
		assert(fake.dequeue_input_calls == 1);
		assert(fake.input_queue_calls == (stage==1 ? 0 : 1));
		assert(fake.dequeue_output_calls == (stage==1 || stage==2 ? 0 : 1));
		assert(fake.output_queue_calls == (stage>=1 && stage<=3 ? 0 : 1));
		assert(video_broker_rdkx5_persistent_encoder_destroy(&encoder) == 0);
	}
}

static void test_failed_queue_generation_cannot_accept_another_frame(int codec, int stage) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	unsigned char frame[128*128*3/2] = {0}, stream[128] = {0};
	uint64_t epoch = 0;
	int bitrate = 0;
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(&encoder, codec,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR, 0, 128, 128, 20, 4096) == 0);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 20, 4096) == 1);
	int *faults[] = {NULL, &fake.dequeue_input_error, &fake.queue_input_error,
		&fake.dequeue_output_error, &fake.queue_output_error};
	*faults[stage] = -268435443; /* Observed hardware WAIT_TIMEOUT. */
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder,
		frame, sizeof(frame), stream, sizeof(stream), &epoch, &bitrate) == -1);
	assert(encoder.queue_faulted && encoder.initialized && encoder.started);
	*faults[stage] = 0; /* Even a transient SDK fault cannot unpoison ownership. */
	fake_codec_t before = fake;
	for (int attempt = 0; attempt < 10; ++attempt) {
		epoch = 99; bitrate = 99;
		assert(video_broker_rdkx5_persistent_encoder_encode(&encoder,
			frame, sizeof(frame), stream, sizeof(stream), &epoch, &bitrate) == -1);
		assert(epoch == 0 && bitrate == 0);
		assert(video_broker_rdkx5_persistent_encoder_request_idr(&encoder) == -1);
		assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 30, 2048) == -1);
	}
	assert(memcmp(&fake, &before, sizeof(fake)) == 0); /* No SDK work or buffer reuse. */
	assert(video_broker_rdkx5_persistent_encoder_destroy(&encoder) == 0);
	assert(!encoder.queue_faulted && !encoder.initialized && !encoder.started);
	assert(video_broker_rdkx5_persistent_encoder_init(&encoder, codec,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR, 0, 128, 128, 20, 4096) == 0);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 20, 4096) == 1);
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder,
		frame, sizeof(frame), stream, sizeof(stream), &epoch, &bitrate) > 0);
	assert(video_broker_rdkx5_persistent_encoder_destroy(&encoder) == 0);
}

static void release_failure(int codec, int stage) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(&encoder, codec,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR, 0, 128, 128, 30, 1024) == 0);
	fake.pause_error = stage == 1 ? -71 : 0;
	fake.release_error = stage == 2 ? -72 : 0;
	assert(video_broker_rdkx5_persistent_encoder_destroy(&encoder) < 0);
	assert(encoder.initialized); /* Old destroy discarded failed SDK ownership. */
	assert(encoder.started == (stage == 1));
	assert(encoder.close_error == (stage == 1 ? -71 : -72));
	assert(fake.pause_calls == 1 && fake.release_calls == (stage == 2));
	assert(video_broker_rdkx5_persistent_encoder_destroy(&encoder) < 0);
	assert(fake.pause_calls == 1 && fake.release_calls == (stage == 2));
	assert(video_broker_rdkx5_persistent_encoder_init(&encoder, codec,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR, 0, 128, 128, 30, 1024) < 0);
	assert(fake.initialize_calls == 1);
	assert(video_broker_rdkx5_encoder_resources_quarantined());
	video_broker_rdkx5_persistent_encoder_t other = {0};
	assert(video_broker_rdkx5_persistent_encoder_init(&other, codec,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR, 1, 128, 128, 30, 1024) < 0);
	assert(fake.initialize_calls == 1);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, false, 30, 1024) < 0);
	assert(video_broker_rdkx5_persistent_encoder_request_idr(&encoder) < 0);
	unsigned char frame = 0, stream = 0;
	uint64_t epoch = 19; int bitrate = 99;
	assert(video_broker_rdkx5_persistent_encoder_encode(&encoder,&frame,128*128*3/2,
		&stream,1,&epoch,&bitrate)<0);
	assert(epoch == 0 && bitrate == 0 && fake.input_queue_calls == 0 && fake.output_queue_calls == 0);
}

static void partial_open_failure(int codec, int stage) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	memset(&fake, 0, sizeof(fake));
	fake.initialize_error = stage == 1 ? -81 : 0;
	fake.configure_error = stage == 2 ? -82 : 0;
	fake.start_error = stage == 3 ? -83 : 0;
	fake.corrupt_explicit_header_readback = stage == 4;
	fake.release_error = -84;
	assert(video_broker_rdkx5_persistent_encoder_init(&encoder,codec,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,0,128,128,30,1024)<0);
	assert(encoder.initialized == (stage != 1) && !encoder.started);
	assert(encoder.close_error == (stage == 1 ? -81 : -84));
	assert(video_broker_rdkx5_encoder_resources_quarantined());
	assert(video_broker_rdkx5_persistent_encoder_destroy(&encoder)<0);
	assert(fake.initialize_calls == 1 && fake.pause_calls == 0 && fake.release_calls == (stage != 1));
}

static void successful_release_and_reuse(int codec, int unused) {
	(void)unused;
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	memset(&fake, 0, sizeof(fake));
	fake.configure_error = -82;
	assert(video_broker_rdkx5_persistent_encoder_init(&encoder,codec,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,0,128,128,30,1024)<0);
	assert(!encoder.initialized && encoder.close_error == 0 && fake.release_calls == 1);
	fake.configure_error = 0;
	for (int cycle=0;cycle<3;cycle++) {
		assert(video_broker_rdkx5_persistent_encoder_init(&encoder,codec,
			VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,0,128,128,30,1024)==0);
		video_broker_rdkx5_persistent_encoder_t snapshot = encoder;
		assert(video_broker_rdkx5_persistent_encoder_init(&encoder,codec,
			VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,0,256,256,30,2048)<0);
		assert(memcmp(&snapshot,&encoder,sizeof encoder)==0);
		assert(video_broker_rdkx5_persistent_encoder_destroy(&encoder)==0);
		assert(video_broker_rdkx5_persistent_encoder_destroy(&encoder)==0);
	}
	assert(!video_broker_rdkx5_encoder_resources_quarantined());
	assert(fake.initialize_calls==4 && fake.pause_calls==3 && fake.release_calls==4);
}

static void run_failure(void (*test)(int, int), int codec, int stage) {
	pid_t child = fork(); assert(child >= 0);
	if (child == 0) { test(codec, stage); _exit(0); }
	int status; assert(waitpid(child, &status, 0) == child);
	assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

static void test_encoder_output_delay_is_localized_without_hot_log_io(void) {
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	memset(&fake, 0, sizeof(fake));
	assert(video_broker_rdkx5_persistent_encoder_init(&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
		VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR, 1, 128, 128, 15, 750) == 0);
	assert(video_broker_rdkx5_persistent_encoder_set_control(&encoder, true, 15, 750) == 1);
	fake.output_delay_ms = 150;
	unsigned char frame[128*128*3/2] = {0}, stream[128];
	uint64_t epoch; int bitrate;
	(void)video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, sizeof(frame), stream, sizeof(stream), &epoch, &bitrate);
	assert(encoder.encode_delay_valid);
	assert(strcmp(encoder.encode_delay.phase, "dequeue_output_buffer") == 0);
	assert(encoder.encode_delay.wall_us >= 100000);
	assert(fake.output_queue_calls == 1 && !encoder.queue_faulted);
	fake.output_delay_ms = 0;
	(void)video_broker_rdkx5_persistent_encoder_encode(&encoder, frame, sizeof(frame), stream, sizeof(stream), &epoch, &bitrate);
	assert(!encoder.encode_delay_valid); /* No stale delay attributed to this frame. */
	assert(video_broker_rdkx5_persistent_encoder_destroy(&encoder) == 0);
}

int main(void) {
	test_encoder_output_delay_is_localized_without_hot_log_io();
	memset(&fake, 0, sizeof(fake));
	test_live_runtime_control();
	test_h265_quality_contract();
	test_h265_realtime_contract();
	test_h265_avbr_recorder_contract();
	test_runtime_rate_control_readback_is_fail_closed();
	test_explicit_header_readback_is_fail_closed();
	test_invalid_geometry_fails_before_vendor_initialization();
	test_encoder_input_honors_vendor_plane_strides();
	test_explicit_idr_does_not_advance_encoder_epoch();
	test_missing_idr_headers_fail_closed();
	test_warmup_parameter_sets_survive_publication_cutover(VIDEO_BROKER_RDKX5_ENCODER_H265);
	test_warmup_parameter_sets_survive_publication_cutover(VIDEO_BROKER_RDKX5_ENCODER_H264);
	for (int codec=1;codec<=2;codec++) {
		test_encode_failure_diagnostics(codec);
		for (int stage=1;stage<=4;stage++) test_failed_queue_generation_cannot_accept_another_frame(codec, stage);
		for(int stage=1;stage<=2;stage++) run_failure(release_failure,codec,stage);
		for(int stage=1;stage<=4;stage++) run_failure(partial_open_failure,codec,stage);
		run_failure(successful_release_and_reuse,codec,0);
	}
	puts("PERSISTENT_ENCODER_RELEASE_PASS");
	return 0;
}
