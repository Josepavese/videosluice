/* RDK X5 codec adapter owned by the standalone video-broker data plane. */
#define _POSIX_C_SOURCE 200809L

#include "rdkx5_decoder_runtime.h"

#include <errno.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

extern int32_t vp_codec_deinit(media_codec_context_t *context);
extern int32_t vp_codec_start(media_codec_context_t *context);
extern int32_t vp_codec_stop(media_codec_context_t *context);

enum {
	kDecoderOutputThreadTimeoutMs = 100,
	/* Preserve the six-buffer policy of the installed libhbspdev adapter,
	 * but own these parameters through the public, declared MediaCodec API. */
	kDecoderInputBuffers = 6,
	kDecoderInitialOutputBuffers = 6,
	kDecoderMaximumOutputBuffers = 31,
};

/* A failed vendor teardown can retain process-wide SDK/driver resources.
 * Switching object or codec is not evidence that those resources disappeared. */
static atomic_bool decoder_resources_quarantined = ATOMIC_VAR_INIT(false);

static void decoder_input_checkpoint(video_broker_rdkx5_decoder_t *decoder,
	const char *next) {
	const int saved_errno = errno;
	struct timespec wall, cpu;
	vb_pipeline_delay delay;
	if (clock_gettime(CLOCK_MONOTONIC, &wall) == 0 &&
	    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &cpu) == 0 &&
	    wall.tv_sec >= 0 && cpu.tv_sec >= 0) {
		if (vb_pipeline_checkpoint(&decoder->input_latency, next,
		    (uint64_t)wall.tv_sec*1000000 + (uint64_t)wall.tv_nsec/1000,
		    (uint64_t)cpu.tv_sec*1000000 + (uint64_t)cpu.tv_nsec/1000, &delay)) {
			fprintf(stderr, "[video-broker][decoder-input] event=native_phase_slow"
				" phase=%s wall_us=%" PRIu64 " thread_cpu_us=%" PRIu64
				" suppressed=%" PRIu64 " codec=%d source_epoch=%" PRIu64 "\n",
				delay.phase, delay.wall_us, delay.cpu_us, delay.suppressed,
				decoder->codec, decoder->captures.epoch);
		}
	} else {
		decoder->input_latency.phase = NULL;
	}
	errno = saved_errno;
}

bool video_broker_rdkx5_decoder_resources_quarantined(void) {
	return atomic_load_explicit(&decoder_resources_quarantined, memory_order_relaxed);
}

static int decoder_close_failure(video_broker_rdkx5_decoder_t *decoder,
	const char *stage, int code) {
	decoder->close_error = code;
	atomic_store_explicit(&decoder_resources_quarantined, true, memory_order_relaxed);
	fprintf(stderr, "[video-broker][decoder-release] event=release_failed stage=%s"
		" sdk_code=%d codec=%d width=%d height=%d source_epoch=%" PRIu64
		" initialized=%s started=%s resources_released=false action=retire_process\n",
		stage, code, decoder->codec, decoder->width, decoder->height,
		decoder->captures.epoch, decoder->initialized ? "true" : "false",
		decoder->started ? "true" : "false");
	return -1;
}

/* A failed join is NOT permission to inspect/mutate the worker's decoder:
 * it may still be opening or releasing it. Preserve all of its storage. */
static int decoder_join_failure(const char *stage, int code) {
	atomic_store_explicit(&decoder_resources_quarantined, true, memory_order_relaxed);
	fprintf(stderr, "[video-broker][decoder-release] event=release_failed stage=%s"
		" thread_code=%d resources_released=false action=retire_process\n", stage, code);
	return -1;
}

static const char *capture_admission_reason(int result) {
	switch (result) {
	case VB_CAPTURE_CAPACITY: return "correlation_capacity";
	case VB_CAPTURE_RETIRED: return "execution_retired_or_failed";
	case VB_CAPTURE_INVALID: return "invalid_origin_or_sequence";
	default: return "correlation_unavailable";
	}
}

static int64_t decoder_realtime_ms(void) {
	struct timespec now;
	if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
		return -1;
	}
	return (int64_t)now.tv_sec * 1000 + (int64_t)now.tv_nsec / 1000000;
}

static void decoder_record_output_error(video_broker_rdkx5_decoder_t *decoder) {
	(void)pthread_mutex_lock(&decoder->output_mutex);
	decoder->output_error = -1;
	if (decoder->output_condition_initialized) {
		(void)pthread_cond_broadcast(&decoder->output_condition);
	}
	(void)pthread_mutex_unlock(&decoder->output_mutex);
}

static bool decoder_output_stop_requested(video_broker_rdkx5_decoder_t *decoder) {
	bool stop;
	(void)pthread_mutex_lock(&decoder->output_mutex);
	stop = decoder->output_thread_stop;
	(void)pthread_mutex_unlock(&decoder->output_mutex);
	return stop;
}

static bool decoder_copy_nv12_frame(
	const video_broker_rdkx5_decoder_t *decoder,
	const media_codec_buffer_t *output,
	unsigned char *destination
) {
	const mc_video_frame_buffer_info_t *frame;
	int y_stride;
	int uv_stride;
	if (decoder == NULL || output == NULL || destination == NULL) {
		return false;
	}
	frame = &output->vframe_buf;
	if (frame->vir_ptr[0] == NULL || frame->vir_ptr[1] == NULL ||
	    decoder->width <= 0 || decoder->height <= 0 ||
	    frame->width != decoder->width || frame->height != decoder->height) {
		return false;
	}
	y_stride = frame->stride > 0 ? frame->stride : decoder->width;
	uv_stride = frame->vstride > 0 ? frame->vstride : y_stride;
	if (y_stride < decoder->width) {
		return false;
	}
	/* Validate the entire layout BEFORE modifying the published cache. A later
	 * rejection must not replace its Y pixels while keeping the old origin/UV.
	 * Driver return may block before output_error is published to readers. */
	if (frame->pix_fmt == MC_PIXEL_FORMAT_YUV420P) {
		if (frame->vir_ptr[2] == NULL || uv_stride < decoder->width / 2) return false;
	} else if (frame->pix_fmt == MC_PIXEL_FORMAT_NV12 || frame->pix_fmt == MC_PIXEL_FORMAT_NV21) {
		if (uv_stride < decoder->width) return false;
	} else {
		return false;
	}
	for (int row = 0; row < decoder->height; ++row) {
		memcpy(
			destination + (size_t)row * (size_t)decoder->width,
			frame->vir_ptr[0] + (size_t)row * (size_t)y_stride,
			(size_t)decoder->width);
	}
	destination += (size_t)decoder->width * (size_t)decoder->height;
	if (frame->pix_fmt == MC_PIXEL_FORMAT_YUV420P) {
		for (int row = 0; row < decoder->height / 2; ++row) {
			const unsigned char *u = frame->vir_ptr[1] + (size_t)row * (size_t)uv_stride;
			const unsigned char *v = frame->vir_ptr[2] + (size_t)row * (size_t)uv_stride;
			unsigned char *uv = destination + (size_t)row * (size_t)decoder->width;
			for (int column = 0; column < decoder->width / 2; ++column) {
				uv[column * 2] = u[column];
				uv[column * 2 + 1] = v[column];
			}
		}
	} else if (frame->pix_fmt == MC_PIXEL_FORMAT_NV12) {
		for (int row = 0; row < decoder->height / 2; ++row) {
			memcpy(
				destination + (size_t)row * (size_t)decoder->width,
				frame->vir_ptr[1] + (size_t)row * (size_t)uv_stride,
				(size_t)decoder->width);
		}
	} else { /* NV21, already validated above. */
		for (int row = 0; row < decoder->height / 2; ++row) {
			const unsigned char *vu = frame->vir_ptr[1] + (size_t)row * (size_t)uv_stride;
			unsigned char *uv = destination + (size_t)row * (size_t)decoder->width;
			for (int column = 0; column < decoder->width; column += 2) {
				uv[column] = vu[column + 1];
				uv[column + 1] = vu[column];
			}
		}
	}
	return true;
}

static void *decoder_output_loop(void *opaque) {
	video_broker_rdkx5_decoder_t *decoder = (video_broker_rdkx5_decoder_t *)opaque;

	while (!decoder_output_stop_requested(decoder)) {
		media_codec_buffer_t output;
		media_codec_output_buffer_info_t info;
		bool valid_frame;
		bool unmatched_capture = false;
		vb_capture_ticket unmatched_ticket = {0, 0};
		size_t unmatched_pending = 0;
		int32_t rc;

		memset(&output, 0, sizeof(output));
		memset(&info, 0, sizeof(info));
		rc = hb_mm_mc_dequeue_output_buffer(
			&decoder->context, &output, &info, kDecoderOutputThreadTimeoutMs);
		if (rc != 0) {
			continue;
		}
		valid_frame = output.type == MC_VIDEO_FRAME_BUFFER &&
			(decoder->codec == VIDEO_BROKER_RDKX5_CODEC_MJPEG
				? info.jpeg_frame_info.decode_result != 0
				: info.video_frame_info.decode_result != 0) &&
			output.vframe_buf.size >= decoder->frame_size &&
			output.vframe_buf.vir_ptr[0] != NULL &&
			output.vframe_buf.vir_ptr[1] != NULL;
		if (valid_frame) {
			(void)pthread_mutex_lock(&decoder->output_mutex);
			valid_frame = decoder_copy_nv12_frame(decoder, &output, decoder->latest_frame);
			if (valid_frame) {
				decoder->latest_origin = (vb_capture_origin){0, 0};
				if (decoder->captures.initialized && !decoder->captures.retired) {
					vb_capture_ticket ticket = {decoder->captures.epoch, output.vframe_buf.pts};
					int correlation = vb_capture_resolve(
						&decoder->captures, ticket, &decoder->latest_origin);
					if (correlation != VB_CAPTURE_OK) {
						unmatched_capture = true;
						unmatched_ticket = ticket;
						unmatched_pending = decoder->captures.used;
						vb_capture_retire(&decoder->captures);
					}
				}
				decoder->produced_generation++;
				if (decoder->output_condition_initialized) {
					(void)pthread_cond_signal(&decoder->output_condition);
				}
			}
			(void)pthread_mutex_unlock(&decoder->output_mutex);
		}
		if (hb_mm_mc_queue_output_buffer(&decoder->context, &output, 0) != 0) {
			decoder_record_output_error(decoder);
			break;
		}
		/* Diagnostic I/O must not hold the correlation mutex or vendor surface. */
		if (unmatched_capture) {
			fprintf(stderr, "[capture-correlation] event=unavailable codec=%d epoch=%" PRIu64
				" tag=%" PRIu64 " pending=%zu reason=unmatched_output action=retire\n",
				decoder->codec, unmatched_ticket.epoch, unmatched_ticket.tag, unmatched_pending);
		}
		if (!valid_frame) {
			decoder_record_output_error(decoder);
			fprintf(stderr, "[capture-correlation] event=surface_rejected reason=invalid_decoded_surface"
				" expected=%dx%d action=stop_decoder\n", decoder->width, decoder->height);
			break;
		}
	}
	return NULL;
}

static int decoder_take_latest(
	video_broker_rdkx5_decoder_t *decoder,
	unsigned char *nv12,
	int timeout_ms,
	vb_capture_origin *output_origin
) {
	int rc = 0;
	*output_origin = (vb_capture_origin){0, 0};
	(void)pthread_mutex_lock(&decoder->output_mutex);
	if (decoder->output_error == 0 &&
	    decoder->produced_generation == decoder->consumed_generation &&
	    timeout_ms > 0 && decoder->output_condition_initialized) {
		struct timespec deadline;
		if (clock_gettime(CLOCK_MONOTONIC, &deadline) == 0) {
			deadline.tv_sec += timeout_ms / 1000;
			deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
			if (deadline.tv_nsec >= 1000000000L) {
				deadline.tv_sec++;
				deadline.tv_nsec -= 1000000000L;
			}
			while (decoder->output_error == 0 && !decoder->output_thread_stop &&
			       decoder->produced_generation == decoder->consumed_generation) {
				int wait_rc = pthread_cond_timedwait(
					&decoder->output_condition, &decoder->output_mutex, &deadline);
				if (wait_rc == ETIMEDOUT) {
					break;
				}
				if (wait_rc != 0) {
					decoder->output_error = -1;
					break;
				}
			}
		}
	}
	if (decoder->output_error != 0) {
		rc = -1;
	} else if (decoder->produced_generation != decoder->consumed_generation) {
		memcpy(nv12, decoder->latest_frame, decoder->frame_size);
		*output_origin = decoder->latest_origin;
		decoder->consumed_generation = decoder->produced_generation;
		rc = 1;
	}
	(void)pthread_mutex_unlock(&decoder->output_mutex);
	return rc;
}

int video_broker_rdkx5_decoder_open(
	video_broker_rdkx5_decoder_t *decoder,
	video_broker_rdkx5_codec_t codec,
	int width,
	int height,
	uint64_t qualified_capture_epoch
) {
	media_codec_id_t codec_id;
	pthread_condattr_t condition_attributes;
	int32_t code;
	if (decoder == NULL || width <= 0 || height <= 0 || width > 8192 || height > 8192 || (width & 1) != 0 || (height & 1) != 0) {
		return -1;
	}
	if (codec != VIDEO_BROKER_RDKX5_CODEC_H264 && codec != VIDEO_BROKER_RDKX5_CODEC_H265 &&
	    codec != VIDEO_BROKER_RDKX5_CODEC_MJPEG) {
		return -1;
	}
	if (decoder->initialized || decoder->started || decoder->close_error != 0 ||
	    video_broker_rdkx5_decoder_resources_quarantined()) {
		fprintf(stderr, "[video-broker][decoder-release] event=open_rejected"
			" reason=ownership_not_released action=no_sdk_allocation\n");
		return -1;
	}
	memset(decoder, 0, sizeof(*decoder));
	decoder->codec = codec;
	decoder->width = width;
	decoder->height = height;
	if (qualified_capture_epoch != 0 && vb_capture_init(
		&decoder->captures, qualified_capture_epoch, VB_CAPTURE_CORRELATION_MAX) != VB_CAPTURE_OK) {
		return -1;
	}
	if (codec == VIDEO_BROKER_RDKX5_CODEC_H265) {
		codec_id = MEDIA_CODEC_ID_H265;
	} else if (codec == VIDEO_BROKER_RDKX5_CODEC_MJPEG) {
		codec_id = MEDIA_CODEC_ID_MJPEG;
	} else {
		codec_id = MEDIA_CODEC_ID_H264;
	}
	if (hb_mm_mc_get_default_context(codec_id, false, &decoder->context) != 0) {
		return -1;
	}
	decoder->frame_size = (size_t)width * (size_t)height * 3U / 2U;
	mc_video_codec_dec_params_t *params = &decoder->context.video_dec_params;
	params->pix_fmt = MC_PIXEL_FORMAT_NV12;
	params->bitstream_buf_size = (hb_u32)((decoder->frame_size + 1023U) & ~(size_t)1023U);
	params->bitstream_buf_count = kDecoderInputBuffers;
	params->external_bitstream_buf = false;
	params->frame_buf_count = kDecoderInitialOutputBuffers;
	/*
	 * UVC gives this bridge one complete Annex-B access unit per V4L2 buffer.
	 * The vendor default enables presentation-order buffering, which lets a
	 * malformed or over-conservative SPS hold an entire GOP before publishing
	 * decoded pictures. Real-time consumers require the newest live picture, not B
	 * frame presentation order, so make the low-latency contract explicit.
	 */
	decoder->context.video_dec_params.feed_mode = MC_FEEDING_MODE_FRAME_SIZE;
	if (codec == VIDEO_BROKER_RDKX5_CODEC_H265) {
		decoder->context.video_dec_params.h265_dec_config.reorder_enable = false;
	} else if (codec == VIDEO_BROKER_RDKX5_CODEC_H264) {
		decoder->context.video_dec_params.h264_dec_config.reorder_enable = false;
	}
	/* The convenience vp_codec_init wrapper hides an initialize/configure/
	 * release sequence and discards a failed release after configure failure.
	 * Own each SDK stage so partial native resources cannot become a false
	 * successful close. An initialize error has no proven cleanup receipt. */
	code = hb_mm_mc_initialize(&decoder->context);
	if (code != 0) return decoder_close_failure(decoder, "sdk_initialize", code);
	decoder->initialized = true;
	code = hb_mm_mc_configure(&decoder->context);
	if (code != 0) {
		fprintf(stderr, "[video-broker][decoder-open] event=open_failed stage=sdk_configure"
			" sdk_code=%d codec=%d width=%d height=%d action=release_initialized_context\n",
			code, codec, width, height);
		video_broker_rdkx5_decoder_close(decoder);
		return -1;
	}
	if (params->bitstream_buf_count != kDecoderInputBuffers ||
	    params->bitstream_buf_size != ((decoder->frame_size + 1023U) & ~(size_t)1023U) ||
	    params->external_bitstream_buf || params->frame_buf_count < kDecoderInitialOutputBuffers ||
	    params->frame_buf_count > kDecoderMaximumOutputBuffers || params->pix_fmt != MC_PIXEL_FORMAT_NV12) {
		fprintf(stderr, "[video-broker][decoder-open] event=open_failed stage=buffer_contract"
			" codec=%d input_buffers=%u input_bytes=%u output_buffers=%u action=release_before_start\n",
			codec, params->bitstream_buf_count, params->bitstream_buf_size, params->frame_buf_count);
		video_broker_rdkx5_decoder_close(decoder);
		return -1;
	}
	fprintf(stderr, "[video-broker][decoder-open] event=buffer_contract_configured"
		" codec=%d geometry=%dx%d input_buffers=%u input_bytes=%u output_buffers=%u"
		" output_count_ceiling=%d scope=codec_buffers_not_total_sdk_memory\n",
		codec, width, height, params->bitstream_buf_count, params->bitstream_buf_size,
		params->frame_buf_count, kDecoderMaximumOutputBuffers);
	if (vp_codec_start(&decoder->context) != 0) {
		video_broker_rdkx5_decoder_close(decoder);
		return -1;
	}
	decoder->started = true;
	decoder->width = width;
	decoder->height = height;
	decoder->codec = codec;
	decoder->latest_frame = (unsigned char *)malloc(decoder->frame_size);
	if (decoder->latest_frame == NULL) {
		video_broker_rdkx5_decoder_close(decoder);
		return -1;
	}
	if (pthread_mutex_init(&decoder->output_mutex, NULL) != 0) {
		video_broker_rdkx5_decoder_close(decoder);
		return -1;
	}
	decoder->output_mutex_initialized = true;
	if (pthread_condattr_init(&condition_attributes) != 0) {
		video_broker_rdkx5_decoder_close(decoder);
		return -1;
	}
	if (pthread_condattr_setclock(&condition_attributes, CLOCK_MONOTONIC) != 0 ||
	    pthread_cond_init(&decoder->output_condition, &condition_attributes) != 0) {
		(void)pthread_condattr_destroy(&condition_attributes);
		video_broker_rdkx5_decoder_close(decoder);
		return -1;
	}
	(void)pthread_condattr_destroy(&condition_attributes);
	decoder->output_condition_initialized = true;
	if (pthread_create(&decoder->output_thread, NULL, decoder_output_loop, decoder) != 0) {
		video_broker_rdkx5_decoder_close(decoder);
		return -1;
	}
	decoder->output_thread_started = true;
	return 0;
}

int video_broker_rdkx5_decoder_poll(
	video_broker_rdkx5_decoder_t *decoder, unsigned char *nv12,
	size_t nv12_capacity, vb_capture_origin *output_origin
) {
	if (output_origin != NULL) *output_origin = (vb_capture_origin){0, 0};
	if (decoder == NULL || !decoder->started || decoder->close_error != 0 || nv12 == NULL ||
	    nv12_capacity < decoder->frame_size || output_origin == NULL) return -1;
	return decoder_take_latest(decoder, nv12, 0, output_origin);
}

int video_broker_rdkx5_decoder_decode(
	video_broker_rdkx5_decoder_t *decoder,
	const unsigned char *access_unit,
	size_t access_unit_size,
	unsigned char *nv12,
	size_t nv12_capacity,
	int timeout_ms,
	vb_capture_origin input_origin,
	vb_capture_origin *output_origin
) {
	media_codec_buffer_t input;
	vb_capture_ticket ticket = {0, 0};
	int ready_rc;
	int32_t rc;

	if (output_origin != NULL) *output_origin = (vb_capture_origin){0, 0};
	if (decoder == NULL || !decoder->started || decoder->close_error != 0 || access_unit == NULL || access_unit_size == 0 ||
		nv12 == NULL || nv12_capacity < decoder->frame_size || access_unit_size > UINT32_MAX ||
		output_origin == NULL) {
		return -1;
	}
	/* Reserve before any native input acquisition. Never recycle a ticket after
	 * an ambiguous queue failure. Output correlation shares this same mutex. */
	/* Do not charge time between calls to a decoder operation. */
	decoder->input_latency.phase = NULL;
	decoder_input_checkpoint(decoder, "admission_mutex");
	(void)pthread_mutex_lock(&decoder->output_mutex);
	int admission = VB_CAPTURE_OK;
	if (decoder->output_error != 0 || decoder->output_thread_stop) {
		admission = VB_CAPTURE_RETIRED;
	} else if (decoder->captures.initialized) {
		admission = input_origin.epoch == decoder->captures.epoch
			? vb_capture_admit(&decoder->captures, input_origin.sequence, &ticket)
			: VB_CAPTURE_INVALID;
	} else if (input_origin.epoch != 0 || input_origin.sequence != 0) {
		admission = VB_CAPTURE_INVALID;
	}
	(void)pthread_mutex_unlock(&decoder->output_mutex);
	decoder_input_checkpoint(decoder, "latest_before_input");
	if (admission != VB_CAPTURE_OK) {
		fprintf(stderr, "[capture-correlation] event=input_rejected reason=%s epoch=%" PRIu64
			" sequence=%" PRIu64 " action=no_native_submission\n",
			capture_admission_reason(admission), input_origin.epoch, input_origin.sequence);
		return -1;
	}
	ready_rc = decoder_take_latest(decoder, nv12, 0, output_origin);
	decoder_input_checkpoint(decoder, "dequeue_input_buffer");
	if (ready_rc < 0) {
		goto not_submitted;
	}
	memset(&input, 0, sizeof(input));
	input.type = MC_VIDEO_STREAM_BUFFER;
	rc = hb_mm_mc_dequeue_input_buffer(&decoder->context, &input, 2000);
	decoder_input_checkpoint(decoder, "input_validation");
	if (rc != 0) {
		goto not_submitted;
	}
	if (input.vstream_buf.vir_ptr == NULL || (size_t)input.vstream_buf.size < access_unit_size) {
		const uint32_t available = input.vstream_buf.size;
		input.vstream_buf.size = 0;
		input.vstream_buf.stream_end = 0;
		decoder_input_checkpoint(decoder, "return_unusable_input_buffer");
		rc = hb_mm_mc_queue_input_buffer(&decoder->context, &input, 2000);
		decoder_input_checkpoint(decoder, NULL);
		if (rc != 0) {
			/* Acquisition succeeded, but returning this unusable native buffer
			 * did not. Do not claim non-submission/release or keep feeding the
			 * decoder: SDK ownership is uncertain until checked teardown. */
			fprintf(stderr, "[video-broker][decoder-input] event=return_failed"
				" sdk_code=%d codec=%d available=%u requested=%zu tag=%" PRIu64
				" ownership=uncertain action=retain_ticket_stop_decoder\n",
				rc, decoder->codec, available, access_unit_size, ticket.tag);
			decoder_record_output_error(decoder);
			*output_origin = (vb_capture_origin){0, 0};
			return -1;
		}
		goto not_submitted;
	}
	decoder_input_checkpoint(decoder, "copy_access_unit");
	memcpy(input.vstream_buf.vir_ptr, access_unit, access_unit_size);
	input.vstream_buf.size = (uint32_t)access_unit_size;
	input.vstream_buf.stream_end = 0;
	input.vstream_buf.pts = ticket.tag;
	decoder_input_checkpoint(decoder, "queue_input_buffer");
	rc = hb_mm_mc_queue_input_buffer(&decoder->context, &input, 2000);
	decoder_input_checkpoint(decoder, "latest_after_input");
	if (rc != 0) {
		fprintf(stderr, "[capture-correlation] event=submit_failed ownership=uncertain tag=%" PRIu64
			" action=retain_ticket_stop_decoder\n", ticket.tag);
		decoder_record_output_error(decoder);
		*output_origin = (vb_capture_origin){0, 0};
		return -1;
	}
	if (ready_rc > 0) {
		return ready_rc;
	}
	ready_rc = decoder_take_latest(decoder, nv12, timeout_ms, output_origin);
	decoder_input_checkpoint(decoder, NULL);
	return ready_rc;
not_submitted:
	decoder_input_checkpoint(decoder, "cancel_ticket_mutex");
	(void)pthread_mutex_lock(&decoder->output_mutex);
	if (ticket.tag != 0) (void)vb_capture_cancel(&decoder->captures, ticket);
	(void)pthread_mutex_unlock(&decoder->output_mutex);
	decoder_input_checkpoint(decoder, NULL);
	*output_origin = (vb_capture_origin){0, 0};
	return -1;
}

static video_broker_rdkx5_mjpeg_trial_result_t decoder_try_mjpeg(
	video_broker_rdkx5_decoder_t *decoder,
	const unsigned char *access_unit,
	size_t access_unit_size,
	unsigned char *nv12,
	size_t nv12_capacity,
	int width,
	int height,
	int timeout_ms,
	int64_t deadline_unix_ms,
	int64_t *completed_unix_ms
) {
	int decode_rc;
	vb_capture_origin trial_origin;
	int64_t completed_at_ms;
	if (decoder == NULL || decoder->initialized || decoder->started ||
	    access_unit == NULL || access_unit_size == 0 || nv12 == NULL || timeout_ms <= 0 ||
	    deadline_unix_ms <= 0 || completed_unix_ms == NULL) {
		return VIDEO_BROKER_RDKX5_MJPEG_TRIAL_OPEN_FAILED;
	}
	*completed_unix_ms = 0;
	if (video_broker_rdkx5_decoder_open(
			decoder, VIDEO_BROKER_RDKX5_CODEC_MJPEG, width, height, 0) != 0) {
		return VIDEO_BROKER_RDKX5_MJPEG_TRIAL_OPEN_FAILED;
	}
	decode_rc = video_broker_rdkx5_decoder_decode(
		decoder, access_unit, access_unit_size, nv12, nv12_capacity, timeout_ms,
		(vb_capture_origin){0, 0}, &trial_origin);
	if (decode_rc == 1) {
		completed_at_ms = decoder_realtime_ms();
		*completed_unix_ms = completed_at_ms;
		if (completed_at_ms >= 0 && completed_at_ms < deadline_unix_ms) {
			return VIDEO_BROKER_RDKX5_MJPEG_TRIAL_SUCCESS;
		}
		video_broker_rdkx5_decoder_close(decoder);
		return VIDEO_BROKER_RDKX5_MJPEG_TRIAL_DEADLINE_FAILED;
	}
	video_broker_rdkx5_decoder_close(decoder);
	return VIDEO_BROKER_RDKX5_MJPEG_TRIAL_FRAME_FAILED;
}

static void *mjpeg_trial_worker(void *opaque) {
	video_broker_rdkx5_mjpeg_trial_t *trial =
		(video_broker_rdkx5_mjpeg_trial_t *)opaque;
	video_broker_rdkx5_mjpeg_trial_result_t result = decoder_try_mjpeg(
		&trial->decoder,
		trial->access_unit,
		trial->access_unit_size,
		trial->nv12,
		trial->nv12_capacity,
		trial->width,
		trial->height,
		trial->timeout_ms,
		trial->deadline_unix_ms,
		&trial->completed_unix_ms);
	(void)pthread_mutex_lock(&trial->mutex);
	trial->result = result;
	trial->completed = true;
	(void)pthread_mutex_unlock(&trial->mutex);
	return NULL;
}

int video_broker_rdkx5_mjpeg_trial_init(
	video_broker_rdkx5_mjpeg_trial_t *trial,
	size_t access_unit_capacity,
	int width,
	int height
) {
	if (video_broker_rdkx5_decoder_resources_quarantined()) return -1;
	size_t frame_size;
	size_t pixel_count;
	if (trial == NULL || access_unit_capacity == 0 || width <= 0 || height <= 0 ||
	    (width & 1) != 0 || (height & 1) != 0) {
		return -1;
	}
	if ((size_t)width > SIZE_MAX / (size_t)height) {
		return -1;
	}
	pixel_count = (size_t)width * (size_t)height;
	if (pixel_count > SIZE_MAX - pixel_count / 2U) {
		return -1;
	}
	frame_size = pixel_count + pixel_count / 2U;
	memset(trial, 0, sizeof(*trial));
	trial->access_unit = (unsigned char *)malloc(access_unit_capacity);
	trial->nv12 = (unsigned char *)malloc(frame_size);
	if (trial->access_unit == NULL || trial->nv12 == NULL) {
		video_broker_rdkx5_mjpeg_trial_destroy(trial);
		return -1;
	}
	if (pthread_mutex_init(&trial->mutex, NULL) != 0) {
		video_broker_rdkx5_mjpeg_trial_destroy(trial);
		return -1;
	}
	trial->mutex_initialized = true;
	trial->access_unit_capacity = access_unit_capacity;
	trial->nv12_capacity = frame_size;
	trial->width = width;
	trial->height = height;
	trial->initialized = true;
	return 0;
}

int video_broker_rdkx5_mjpeg_trial_start(
	video_broker_rdkx5_mjpeg_trial_t *trial,
	const unsigned char *access_unit,
	size_t access_unit_size,
	uint64_t token,
	int timeout_ms,
	int64_t deadline_unix_ms
) {
	if (trial == NULL || !trial->initialized || access_unit == NULL ||
	    access_unit_size == 0 || access_unit_size > trial->access_unit_capacity ||
	    token == 0 || timeout_ms <= 0 || deadline_unix_ms <= 0) {
		return -1;
	}
	(void)pthread_mutex_lock(&trial->mutex);
	if (trial->thread_started || trial->completed || trial->result_observed || trial->accepted ||
	    trial->decoder.initialized || trial->decoder.started) {
		(void)pthread_mutex_unlock(&trial->mutex);
		return -1;
	}
	memcpy(trial->access_unit, access_unit, access_unit_size);
	trial->access_unit_size = access_unit_size;
	trial->token = token;
	trial->timeout_ms = timeout_ms;
	trial->deadline_unix_ms = deadline_unix_ms;
	trial->completed_unix_ms = 0;
	trial->result = VIDEO_BROKER_RDKX5_MJPEG_TRIAL_OPEN_FAILED;
	trial->thread_started = true;
	(void)pthread_mutex_unlock(&trial->mutex);
	if (pthread_create(&trial->thread, NULL, mjpeg_trial_worker, trial) != 0) {
		(void)pthread_mutex_lock(&trial->mutex);
		trial->thread_started = false;
		trial->access_unit_size = 0;
		trial->token = 0;
		trial->timeout_ms = 0;
		trial->deadline_unix_ms = 0;
		trial->completed_unix_ms = 0;
		(void)pthread_mutex_unlock(&trial->mutex);
		return -1;
	}
	return 0;
}

int video_broker_rdkx5_mjpeg_trial_poll(
	video_broker_rdkx5_mjpeg_trial_t *trial,
	video_broker_rdkx5_mjpeg_trial_result_t *result,
	uint64_t *token,
	int64_t *deadline_unix_ms,
	int64_t *completed_unix_ms
) {
	pthread_t worker;
	if (trial == NULL || !trial->initialized || result == NULL || token == NULL ||
	    deadline_unix_ms == NULL || completed_unix_ms == NULL) {
		return -1;
	}
	(void)pthread_mutex_lock(&trial->mutex);
	if (!trial->completed || trial->result_observed) {
		(void)pthread_mutex_unlock(&trial->mutex);
		return 0;
	}
	trial->result_observed = true;
	worker = trial->thread;
	*result = trial->result;
	*token = trial->token;
	*deadline_unix_ms = trial->deadline_unix_ms;
	*completed_unix_ms = trial->completed_unix_ms;
	(void)pthread_mutex_unlock(&trial->mutex);
	/* completed is the worker's final publication; this join only reaps an
	 * already-completed thread and never waits on a codec operation. */
	(void)pthread_join(worker, NULL);
	(void)pthread_mutex_lock(&trial->mutex);
	trial->thread_started = false;
	(void)pthread_mutex_unlock(&trial->mutex);
	return 1;
}

int video_broker_rdkx5_mjpeg_trial_accept(video_broker_rdkx5_mjpeg_trial_t *trial) {
	if (trial == NULL || !trial->initialized) {
		return -1;
	}
	(void)pthread_mutex_lock(&trial->mutex);
	if (trial->thread_started || !trial->completed || !trial->result_observed ||
	    trial->result != VIDEO_BROKER_RDKX5_MJPEG_TRIAL_SUCCESS ||
	    !trial->decoder.started) {
		(void)pthread_mutex_unlock(&trial->mutex);
		return -1;
	}
	trial->accepted = true;
	(void)pthread_mutex_unlock(&trial->mutex);
	return 0;
}

video_broker_rdkx5_decoder_t *video_broker_rdkx5_mjpeg_trial_decoder(
	video_broker_rdkx5_mjpeg_trial_t *trial
) {
	video_broker_rdkx5_decoder_t *decoder = NULL;
	if (trial == NULL || !trial->initialized) {
		return NULL;
	}
	(void)pthread_mutex_lock(&trial->mutex);
	if (trial->accepted && trial->decoder.started) {
		decoder = &trial->decoder;
	}
	(void)pthread_mutex_unlock(&trial->mutex);
	return decoder;
}

int video_broker_rdkx5_mjpeg_trial_recycle(video_broker_rdkx5_mjpeg_trial_t *trial) {
	if (trial == NULL || !trial->initialized) {
		return -1;
	}
	(void)pthread_mutex_lock(&trial->mutex);
	if (trial->thread_started || trial->join_error || trial->decoder.initialized || trial->decoder.started ||
	    trial->decoder.close_error != 0) {
		(void)pthread_mutex_unlock(&trial->mutex);
		return -1;
	}
	trial->access_unit_size = 0;
	trial->token = 0;
	trial->timeout_ms = 0;
	trial->deadline_unix_ms = 0;
	trial->completed_unix_ms = 0;
	trial->completed = false;
	trial->result_observed = false;
	trial->accepted = false;
	(void)pthread_mutex_unlock(&trial->mutex);
	return 0;
}

int video_broker_rdkx5_mjpeg_trial_destroy(video_broker_rdkx5_mjpeg_trial_t *trial) {
	pthread_t worker;
	bool join_worker = false;
	if (trial == NULL) {
		return 0;
	}
	if (trial->join_error) return -1;
	if (trial->mutex_initialized) {
		(void)pthread_mutex_lock(&trial->mutex);
		if (trial->thread_started) {
			worker = trial->thread;
			join_worker = true;
		}
		(void)pthread_mutex_unlock(&trial->mutex);
	}
	if (join_worker) {
		int joined = pthread_join(worker, NULL);
		if (joined != 0) {
			trial->join_error = joined;
			return decoder_join_failure("trial_join", joined);
		}
		trial->thread_started = false;
	}
	if (video_broker_rdkx5_decoder_close(&trial->decoder) != 0) return -1;
	if (trial->mutex_initialized) {
		(void)pthread_mutex_destroy(&trial->mutex);
	}
	free(trial->nv12);
	free(trial->access_unit);
	memset(trial, 0, sizeof(*trial));
	return 0;
}

static void *async_decoder_worker(void *opaque) {
	video_broker_rdkx5_async_decoder_t *worker =
		(video_broker_rdkx5_async_decoder_t *)opaque;
	for (;;) {
		unsigned char *access_unit;
		size_t access_unit_size;
		vb_capture_origin input_origin, output_origin;
		video_broker_rdkx5_decoder_t *decoder;
		int decode_rc;
		bool take_output = false;

		(void)pthread_mutex_lock(&worker->mutex);
		decoder = worker->decoder;
		while (!worker->stop_requested && !worker->failed) {
			/* Lock order is worker -> output. Keeping output locked until wait
			 * prevents a lost wakeup between checking both predicates and sleep.
			 * After wait, release output before reacquiring worker. */
			(void)pthread_mutex_lock(&decoder->output_mutex);
			take_output = decoder->output_error != 0 ||
				decoder->produced_generation != decoder->consumed_generation;
			if (worker->pending || take_output) {
				(void)pthread_mutex_unlock(&decoder->output_mutex);
				break;
			}
			(void)pthread_mutex_unlock(&worker->mutex);
			int wait_rc = pthread_cond_wait(&decoder->output_condition, &decoder->output_mutex);
			(void)pthread_mutex_unlock(&decoder->output_mutex);
			(void)pthread_mutex_lock(&worker->mutex);
			if (wait_rc != 0) worker->failed = true;
		}
		if (worker->stop_requested || worker->failed) {
			decoder = worker->decoder;
			(void)pthread_mutex_unlock(&worker->mutex);
			int close_rc = video_broker_rdkx5_decoder_close(decoder);
			(void)pthread_mutex_lock(&worker->mutex);
			worker->cleanup_failed = close_rc != 0;
			worker->failed = worker->failed || worker->cleanup_failed;
			worker->exited = true;
			(void)pthread_mutex_unlock(&worker->mutex);
			return NULL;
		}
		if (take_output) {
			(void)pthread_mutex_unlock(&worker->mutex);
			decode_rc = video_broker_rdkx5_decoder_poll(
				decoder, worker->working_nv12, worker->nv12_capacity, &output_origin);
		} else {
			access_unit = worker->working_access_unit;
			worker->working_access_unit = worker->pending_access_unit;
			worker->pending_access_unit = access_unit;
			access_unit = worker->working_access_unit;
			access_unit_size = worker->pending_access_unit_size;
			input_origin = worker->pending_origin;
			worker->pending_origin = (vb_capture_origin){0, 0};
			worker->pending_access_unit_size = 0;
			worker->pending = false;
			(void)pthread_mutex_unlock(&worker->mutex);
			decode_rc = video_broker_rdkx5_decoder_decode(
				decoder, access_unit, access_unit_size, worker->working_nv12,
				worker->nv12_capacity, worker->timeout_ms, input_origin, &output_origin);
		}
		(void)pthread_mutex_lock(&worker->mutex);
		if (decode_rc < 0) {
			worker->failed = true;
			decoder = worker->decoder;
			(void)pthread_mutex_unlock(&worker->mutex);
			int close_rc = video_broker_rdkx5_decoder_close(decoder);
			(void)pthread_mutex_lock(&worker->mutex);
			worker->cleanup_failed = close_rc != 0;
			worker->exited = true;
			(void)pthread_mutex_unlock(&worker->mutex);
			return NULL;
		}
		if (decode_rc > 0) {
			memcpy(worker->latest_nv12, worker->working_nv12, worker->nv12_capacity);
			worker->latest_origin = output_origin;
			worker->output_generation++;
		}
		(void)pthread_mutex_unlock(&worker->mutex);
	}
}

int video_broker_rdkx5_async_decoder_init(
	video_broker_rdkx5_async_decoder_t *worker,
	size_t access_unit_capacity,
	size_t nv12_capacity,
	int timeout_ms
) {
	if (video_broker_rdkx5_decoder_resources_quarantined()) return -1;
	if (worker == NULL || access_unit_capacity == 0 || nv12_capacity == 0 || timeout_ms <= 0) {
		return -1;
	}
	memset(worker, 0, sizeof(*worker));
	worker->pending_access_unit = (unsigned char *)malloc(access_unit_capacity);
	worker->working_access_unit = (unsigned char *)malloc(access_unit_capacity);
	worker->working_nv12 = (unsigned char *)malloc(nv12_capacity);
	worker->latest_nv12 = (unsigned char *)malloc(nv12_capacity);
	if (worker->pending_access_unit == NULL || worker->working_access_unit == NULL ||
	    worker->working_nv12 == NULL || worker->latest_nv12 == NULL) {
		video_broker_rdkx5_async_decoder_destroy(worker);
		return -1;
	}
	if (pthread_mutex_init(&worker->mutex, NULL) != 0) {
		video_broker_rdkx5_async_decoder_destroy(worker);
		return -1;
	}
	worker->mutex_initialized = true;
	worker->access_unit_capacity = access_unit_capacity;
	worker->nv12_capacity = nv12_capacity;
	worker->timeout_ms = timeout_ms;
	worker->initialized = true;
	return 0;
}

int video_broker_rdkx5_async_decoder_start(
	video_broker_rdkx5_async_decoder_t *worker,
	video_broker_rdkx5_decoder_t *decoder
) {
	if (worker == NULL || !worker->initialized || decoder == NULL || !decoder->started ||
	    worker->nv12_capacity != decoder->frame_size) {
		return -1;
	}
	(void)pthread_mutex_lock(&worker->mutex);
	if (worker->thread_started || worker->cleanup_failed || worker->join_error) {
		(void)pthread_mutex_unlock(&worker->mutex);
		return -1;
	}
	(void)pthread_mutex_lock(&decoder->output_mutex);
	if (decoder->async_owned || decoder->output_thread_stop) {
		(void)pthread_mutex_unlock(&decoder->output_mutex);
		(void)pthread_mutex_unlock(&worker->mutex);
		return -1;
	}
	worker->decoder = decoder;
	worker->pending_access_unit_size = 0;
	worker->output_generation = 0;
	worker->consumed_generation = 0;
	worker->replacements = 0;
	worker->pending_origin = (vb_capture_origin){0, 0};
	worker->latest_origin = (vb_capture_origin){0, 0};
	worker->pending = false;
	worker->stop_requested = false;
	worker->exited = false;
	worker->failed = false;
	/* Publish thread_started only after pthread_create filled the join handle;
	 * a concurrent request_stop must not observe a partly started execution. */
	if (pthread_create(&worker->thread, NULL, async_decoder_worker, worker) != 0) {
		worker->decoder = NULL;
		worker->thread_started = false;
		worker->exited = true;
		(void)pthread_mutex_unlock(&decoder->output_mutex);
		(void)pthread_mutex_unlock(&worker->mutex);
		return -1;
	}
	worker->thread_started = true;
	decoder->async_owned = true;
	(void)pthread_mutex_unlock(&decoder->output_mutex);
	(void)pthread_mutex_unlock(&worker->mutex);
	return 0;
}

/* Called with worker->mutex held, before publishing stop_requested/failed.
 * Those flags fence decoder close, so its condition/mutex are still alive.
 * No caller may touch decoder synchronization once either flag is set. */
static void async_decoder_wake_locked(video_broker_rdkx5_async_decoder_t *worker) {
	if (!worker->thread_started || worker->decoder == NULL || worker->stop_requested ||
	    worker->failed || worker->exited) return;
	(void)pthread_mutex_lock(&worker->decoder->output_mutex);
	(void)pthread_cond_broadcast(&worker->decoder->output_condition);
	(void)pthread_mutex_unlock(&worker->decoder->output_mutex);
}

int video_broker_rdkx5_mjpeg_trial_begin_async_reset(
	video_broker_rdkx5_mjpeg_trial_t *trial,
	video_broker_rdkx5_async_decoder_t *worker
) {
	video_broker_rdkx5_decoder_t *decoder;
	if (trial == NULL || !trial->initialized || worker == NULL || !worker->initialized) {
		return -1;
	}
	(void)pthread_mutex_lock(&trial->mutex);
	if (trial->thread_started || !trial->completed || !trial->result_observed ||
	    !trial->decoder.started) {
		(void)pthread_mutex_unlock(&trial->mutex);
		return -1;
	}
	decoder = &trial->decoder;
	(void)pthread_mutex_unlock(&trial->mutex);
	if (video_broker_rdkx5_async_decoder_start(worker, decoder) != 0) {
		return -1;
	}
	video_broker_rdkx5_async_decoder_request_stop(worker);
	return 0;
}

int video_broker_rdkx5_async_decoder_submit(
	video_broker_rdkx5_async_decoder_t *worker,
	const unsigned char *access_unit,
	size_t access_unit_size,
	vb_capture_origin input_origin
) {
	int result = 1;
	if (worker == NULL || !worker->initialized || access_unit == NULL ||
	    access_unit_size == 0 || access_unit_size > worker->access_unit_capacity) {
		return -1;
	}
	(void)pthread_mutex_lock(&worker->mutex);
	if (!worker->thread_started || worker->stop_requested || worker->exited || worker->failed) {
		(void)pthread_mutex_unlock(&worker->mutex);
		return -1;
	}
	if (worker->pending) {
		worker->replacements++;
		result = 2;
	}
	memcpy(worker->pending_access_unit, access_unit, access_unit_size);
	worker->pending_access_unit_size = access_unit_size;
	worker->pending_origin = input_origin;
	worker->pending = true;
	async_decoder_wake_locked(worker);
	(void)pthread_mutex_unlock(&worker->mutex);
	return result;
}

int video_broker_rdkx5_async_decoder_poll(
	video_broker_rdkx5_async_decoder_t *worker,
	unsigned char *nv12,
	size_t nv12_capacity,
	uint64_t *replacements,
	vb_capture_origin *output_origin
) {
	int result = 0;
	if (output_origin != NULL) *output_origin = (vb_capture_origin){0, 0};
	if (worker == NULL || !worker->initialized || nv12 == NULL ||
	    nv12_capacity < worker->nv12_capacity || replacements == NULL || output_origin == NULL) {
		return -1;
	}
	(void)pthread_mutex_lock(&worker->mutex);
	*replacements = worker->replacements;
	if (worker->failed || worker->stop_requested || worker->exited) {
		result = -1;
	} else if (worker->output_generation != worker->consumed_generation) {
		memcpy(nv12, worker->latest_nv12, worker->nv12_capacity);
		*output_origin = worker->latest_origin;
		worker->consumed_generation = worker->output_generation;
		result = 1;
	}
	(void)pthread_mutex_unlock(&worker->mutex);
	return result;
}

void video_broker_rdkx5_async_decoder_request_stop(
	video_broker_rdkx5_async_decoder_t *worker
) {
	if (worker == NULL || !worker->initialized) {
		return;
	}
	(void)pthread_mutex_lock(&worker->mutex);
	async_decoder_wake_locked(worker);
	worker->stop_requested = true;
	worker->pending = false;
	worker->pending_access_unit_size = 0;
	worker->pending_origin = (vb_capture_origin){0, 0};
	(void)pthread_mutex_unlock(&worker->mutex);
}

int video_broker_rdkx5_async_decoder_reap(video_broker_rdkx5_async_decoder_t *worker) {
	pthread_t thread;
	if (worker == NULL || !worker->initialized) {
		return -1;
	}
	(void)pthread_mutex_lock(&worker->mutex);
	if (worker->join_error) {
		(void)pthread_mutex_unlock(&worker->mutex);
		return -1;
	}
	if (!worker->thread_started) {
		int result = worker->cleanup_failed ? -1 : 1;
		(void)pthread_mutex_unlock(&worker->mutex);
		return result;
	}
	if (!worker->exited) {
		(void)pthread_mutex_unlock(&worker->mutex);
		return 0;
	}
	thread = worker->thread;
	(void)pthread_mutex_unlock(&worker->mutex);
	int joined = pthread_join(thread, NULL);
	if (joined != 0) {
		/* The only lifecycle owner invokes reap/destroy, never concurrently. */
		(void)pthread_mutex_lock(&worker->mutex);
		worker->join_error = joined;
		(void)pthread_mutex_unlock(&worker->mutex);
		return decoder_join_failure("async_join", joined);
	}
	(void)pthread_mutex_lock(&worker->mutex);
	worker->thread_started = false;
	int result = worker->cleanup_failed ? -1 : 1;
	if (!worker->cleanup_failed) worker->decoder = NULL;
	(void)pthread_mutex_unlock(&worker->mutex);
	return result;
}

int video_broker_rdkx5_async_decoder_destroy(video_broker_rdkx5_async_decoder_t *worker) {
	pthread_t thread;
	bool join_worker = false;
	if (worker == NULL) {
		return 0;
	}
	if (worker->join_error) return -1;
	if (worker->mutex_initialized) {
		(void)pthread_mutex_lock(&worker->mutex);
		if (worker->thread_started) {
			async_decoder_wake_locked(worker);
			worker->stop_requested = true;
			worker->pending = false;
			thread = worker->thread;
			join_worker = true;
		}
		(void)pthread_mutex_unlock(&worker->mutex);
	}
	if (join_worker) {
		int joined = pthread_join(thread, NULL);
		if (joined != 0) {
			worker->join_error = joined;
			return decoder_join_failure("async_join", joined);
		}
		worker->thread_started = false;
	}
	if (worker->cleanup_failed) return -1;
	if (worker->mutex_initialized) {
		(void)pthread_mutex_destroy(&worker->mutex);
	}
	free(worker->latest_nv12);
	free(worker->working_nv12);
	free(worker->working_access_unit);
	free(worker->pending_access_unit);
	memset(worker, 0, sizeof(*worker));
	return 0;
}

int video_broker_rdkx5_decoder_close(video_broker_rdkx5_decoder_t *decoder) {
	if (decoder == NULL) {
		return 0;
	}
	if (decoder->close_error != 0) return -1;
	if (decoder->output_thread_started) {
		(void)pthread_mutex_lock(&decoder->output_mutex);
		decoder->output_thread_stop = true;
		vb_capture_retire(&decoder->captures);
		decoder->latest_origin = (vb_capture_origin){0, 0};
		if (decoder->output_condition_initialized) {
			(void)pthread_cond_broadcast(&decoder->output_condition);
		}
		(void)pthread_mutex_unlock(&decoder->output_mutex);
		int joined = pthread_join(decoder->output_thread, NULL);
		if (joined != 0) return decoder_close_failure(decoder, "output_join", joined);
		decoder->output_thread_started = false;
	}
	vb_capture_retire(&decoder->captures);
	decoder->latest_origin = (vb_capture_origin){0, 0};
	if (decoder->started) {
		int code = vp_codec_stop(&decoder->context);
		if (code != 0) return decoder_close_failure(decoder, "sdk_stop", code);
		decoder->started = false;
	}
	if (decoder->initialized) {
		int code = vp_codec_deinit(&decoder->context);
		if (code != 0) return decoder_close_failure(decoder, "sdk_deinit", code);
		decoder->initialized = false;
	}
	if (decoder->output_condition_initialized) {
		(void)pthread_cond_destroy(&decoder->output_condition);
	}
	if (decoder->output_mutex_initialized) {
		(void)pthread_mutex_destroy(&decoder->output_mutex);
	}
	free(decoder->latest_frame);
	memset(decoder, 0, sizeof(*decoder));
	return 0;
}
