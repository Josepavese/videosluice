/* Deterministic video-broker decoder test runtime. */
#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "hb_media_codec.h"
#include "sp_codec.h"

static unsigned char input_data[1024];
/* Bounded synthetic SDK storage; not a native hardware memory admission. */
enum { TEST_MAX_WIDTH = 1920, TEST_MAX_HEIGHT = 1080 };
static unsigned char y_data[(TEST_MAX_WIDTH + 4) * TEST_MAX_HEIGHT];
static unsigned char uv_data[(TEST_MAX_WIDTH + 4) * (TEST_MAX_HEIGHT / 2)];
static uint64_t tokens[64];
static int results[64];
static unsigned char pixels[64];
static int capture_pixels;
static int output_budget = -1;
static int queue_failure;
static int invalid_geometry;
static int invalid_layout;
static int output_return_blocked;
static int output_return_entered;
static int output_returns;
static int head;
static int tail;
static int output_width = 4;
static int output_height = 2;
static int decoder_chunks;
static int delayed_output_mode;
static int delayed_output_ready;
static int output_absent;
static int low_latency_configured;
static int input_delay_ms;
static int input_failure;
static int input_blocked;
static int input_block_entered;
static int teardown_blocked;
static int teardown_block_entered;
static int stop_failure;
static int deinit_failure;
static int start_failure;
static int initialize_failure;
static int configure_failure;
static int buffer_contract_fault;
static int initialize_calls;
static int configure_calls;
static int stop_calls;
static int deinit_calls;
static pthread_mutex_t runtime_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t runtime_condition = PTHREAD_COND_INITIALIZER;

void decoder_runtime_buffer_contract_fault(int fault) { buffer_contract_fault = fault; }

void decoder_runtime_open_failures(int initialize, int configure) {
	initialize_failure = initialize;
	configure_failure = configure;
}

int decoder_runtime_open_calls(int configure) {
	return configure ? configure_calls : initialize_calls;
}

void decoder_runtime_teardown_failures(int stop, int deinit, int start) {
	(void)pthread_mutex_lock(&runtime_mutex);
	stop_failure = stop;
	deinit_failure = deinit;
	start_failure = start;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

int decoder_runtime_teardown_calls(int deinit) {
	(void)pthread_mutex_lock(&runtime_mutex);
	int count = deinit ? deinit_calls : stop_calls;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return count;
}

void decoder_runtime_capture_pixels(int enabled) {
	(void)pthread_mutex_lock(&runtime_mutex);
	capture_pixels = enabled;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

void decoder_runtime_output_budget(int count) {
	(void)pthread_mutex_lock(&runtime_mutex);
	output_budget = count;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

void decoder_runtime_queue_failure(int enabled) {
	(void)pthread_mutex_lock(&runtime_mutex);
	queue_failure = enabled;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

void decoder_runtime_invalid_geometry(int enabled) {
	(void)pthread_mutex_lock(&runtime_mutex);
	invalid_geometry = enabled;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

void decoder_runtime_invalid_layout(int mode) {
	(void)pthread_mutex_lock(&runtime_mutex);
	invalid_layout = mode;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

void decoder_runtime_block_output_return(int enabled) {
	(void)pthread_mutex_lock(&runtime_mutex);
	output_return_blocked = enabled;
	if (enabled) output_return_entered = 0;
	else (void)pthread_cond_broadcast(&runtime_condition);
	(void)pthread_mutex_unlock(&runtime_mutex);
}

int decoder_runtime_output_return_entered(void) {
	(void)pthread_mutex_lock(&runtime_mutex);
	int result = output_return_entered;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return result;
}

int decoder_runtime_output_returns(void) {
	(void)pthread_mutex_lock(&runtime_mutex);
	int result = output_returns;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return result;
}

int decoder_runtime_submissions(void) {
	(void)pthread_mutex_lock(&runtime_mutex);
	int result = tail;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return result;
}

/* Called with output_budget zero. Reorder pixels and tags as one picture. */
void decoder_runtime_reverse_pending(void) {
	(void)pthread_mutex_lock(&runtime_mutex);
	for (int i = head, j = tail - 1; i < j; ++i, --j) {
		uint64_t token = tokens[i % 64];
		int result = results[i % 64];
		unsigned char pixel = pixels[i % 64];
		tokens[i % 64] = tokens[j % 64]; tokens[j % 64] = token;
		results[i % 64] = results[j % 64]; results[j % 64] = result;
		pixels[i % 64] = pixels[j % 64]; pixels[j % 64] = pixel;
	}
	(void)pthread_mutex_unlock(&runtime_mutex);
}

void decoder_runtime_override_next_tag(uint64_t tag) {
	(void)pthread_mutex_lock(&runtime_mutex);
	if (head < tail) tokens[head % 64] = tag;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

static void runtime_delay_ms(long milliseconds) {
	struct timespec delay;
	delay.tv_sec = milliseconds / 1000L;
	delay.tv_nsec = (milliseconds % 1000L) * 1000000L;
	(void)nanosleep(&delay, NULL);
}

void decoder_runtime_enable_delayed_output(int enabled) {
	(void)pthread_mutex_lock(&runtime_mutex);
	delayed_output_mode = enabled;
	delayed_output_ready = 0;
	head = 0;
	tail = 0;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

void decoder_runtime_set_output_absent(int enabled) {
	(void)pthread_mutex_lock(&runtime_mutex);
	output_absent = enabled;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

int decoder_runtime_low_latency_configured(void) {
	int configured;
	(void)pthread_mutex_lock(&runtime_mutex);
	configured = low_latency_configured;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return configured;
}

void decoder_runtime_set_input_delay_ms(int milliseconds) {
	(void)pthread_mutex_lock(&runtime_mutex);
	input_delay_ms = milliseconds;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

void decoder_runtime_set_input_failure(int enabled) {
	(void)pthread_mutex_lock(&runtime_mutex);
	input_failure = enabled;
	(void)pthread_mutex_unlock(&runtime_mutex);
}

void decoder_runtime_set_input_blocked(int enabled) {
	(void)pthread_mutex_lock(&runtime_mutex);
	input_blocked = enabled;
	if (enabled) {
		input_block_entered = 0;
	} else {
		(void)pthread_cond_broadcast(&runtime_condition);
	}
	(void)pthread_mutex_unlock(&runtime_mutex);
}

int decoder_runtime_input_block_entered(void) {
	int entered;
	(void)pthread_mutex_lock(&runtime_mutex);
	entered = input_block_entered;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return entered;
}

void decoder_runtime_set_teardown_blocked(int enabled) {
	(void)pthread_mutex_lock(&runtime_mutex);
	teardown_blocked = enabled;
	if (enabled) {
		teardown_block_entered = 0;
	} else {
		(void)pthread_cond_broadcast(&runtime_condition);
	}
	(void)pthread_mutex_unlock(&runtime_mutex);
}

int decoder_runtime_teardown_block_entered(void) {
	int entered;
	(void)pthread_mutex_lock(&runtime_mutex);
	entered = teardown_block_entered;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return entered;
}

void decoder_runtime_output_geometry(int width, int height) {
	assert(width >= 2 && width <= TEST_MAX_WIDTH && height >= 2 && height <= TEST_MAX_HEIGHT && width % 2 == 0 && height % 2 == 0);
	output_width = width;
	output_height = height;
}

hb_s32 hb_mm_mc_get_default_context(media_codec_id_t codec, hb_bool encoder, media_codec_context_t *context) {
	assert(!encoder);
	memset(context, 0, sizeof(*context));
	context->codec_id = codec;
	context->encoder = false;
	context->video_dec_params.feed_mode = MC_FEEDING_MODE_NONE;
	/* Match the RDK SDK default so the decoder test proves an explicit override. */
	context->video_dec_params.h264_dec_config.reorder_enable = true;
	context->video_dec_params.bitstream_buf_size = 10U * 1024U * 1024U;
	context->video_dec_params.bitstream_buf_count = 5;
	context->video_dec_params.frame_buf_count = 5;
	return 0;
}

/* The separate NHD2 bridge still calls this SDK convenience API. Keep its
 * stub only for that live caller; the mux decoder uses the public API above. */
int32_t vp_decode_config_param(media_codec_context_t *context, media_codec_id_t codec,
	int32_t width, int32_t height) {
	if (width < 2 || width > TEST_MAX_WIDTH || height < 2 || height > TEST_MAX_HEIGHT || width % 2 || height % 2) return -1;
	decoder_runtime_output_geometry(width, height);
	return hb_mm_mc_get_default_context(codec, false, context);
}

int32_t vp_codec_init(media_codec_context_t *context) {
	/* Match the upstream convenience wrapper, including the hidden failed
	 * release. Production must observe both phases directly instead. */
	int result = hb_mm_mc_initialize(context);
	if (result != 0) return -1;
	result = hb_mm_mc_configure(context);
	if (result != 0) {
		(void)hb_mm_mc_release(context);
		return -1;
	}
	return 0;
}

int32_t hb_mm_mc_initialize(media_codec_context_t *context) {
	(void)context;
	initialize_calls++;
	return initialize_failure;
}

int32_t hb_mm_mc_configure(media_codec_context_t *context) {
	configure_calls++;
	if (configure_failure != 0) return configure_failure;
	(void)pthread_mutex_lock(&runtime_mutex);
	/* One active native instance is modelled by this fixture. Reinitializing
	 * after a joined close cannot inherit buffers from the previous context. */
	head = 0;
	tail = 0;
	delayed_output_ready = 0;
	output_returns = 0;
	low_latency_configured = context->video_dec_params.feed_mode == MC_FEEDING_MODE_FRAME_SIZE &&
		((context->codec_id == MEDIA_CODEC_ID_H265 &&
		  !context->video_dec_params.h265_dec_config.reorder_enable) ||
		 (context->codec_id == MEDIA_CODEC_ID_H264 &&
		  !context->video_dec_params.h264_dec_config.reorder_enable) ||
		 context->codec_id == MEDIA_CODEC_ID_MJPEG);
	if (buffer_contract_fault == 1) context->video_dec_params.bitstream_buf_count++;
	if (buffer_contract_fault == 2) context->video_dec_params.frame_buf_count = 32;
	if (buffer_contract_fault == 3) context->video_dec_params.bitstream_buf_size++;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return 0;
}

int32_t vp_codec_deinit(media_codec_context_t *context) {
	return hb_mm_mc_release(context);
}

int32_t hb_mm_mc_release(media_codec_context_t *context) {
	(void)context;
	(void)pthread_mutex_lock(&runtime_mutex);
	deinit_calls++;
	int result = deinit_failure;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return result;
}

int32_t vp_codec_start(media_codec_context_t *context) {
	(void)context;
	(void)pthread_mutex_lock(&runtime_mutex);
	int result = start_failure;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return result;
}

int32_t vp_codec_stop(media_codec_context_t *context) {
	(void)context;
	(void)pthread_mutex_lock(&runtime_mutex);
	stop_calls++;
	if (teardown_blocked) {
		teardown_block_entered = 1;
		(void)pthread_cond_broadcast(&runtime_condition);
	}
	while (teardown_blocked) {
		(void)pthread_cond_wait(&runtime_condition, &runtime_mutex);
	}
	int result = stop_failure;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return result;
}

int32_t hb_mm_mc_dequeue_input_buffer(
	media_codec_context_t *context, media_codec_buffer_t *buffer, int32_t timeout) {
	int delay;
	int fail;
	(void)context;
	(void)timeout;
	(void)pthread_mutex_lock(&runtime_mutex);
	if (input_blocked) {
		input_block_entered = 1;
		(void)pthread_cond_broadcast(&runtime_condition);
	}
	while (input_blocked) {
		(void)pthread_cond_wait(&runtime_condition, &runtime_mutex);
	}
	delay = input_delay_ms;
	fail = input_failure;
	(void)pthread_mutex_unlock(&runtime_mutex);
	if (delay > 0) {
		runtime_delay_ms(delay);
	}
	if (fail) {
		return -1;
	}
	buffer->vstream_buf.vir_ptr = input_data;
	buffer->vstream_buf.size = sizeof(input_data);
	return 0;
}

int32_t hb_mm_mc_queue_input_buffer(
	media_codec_context_t *context, media_codec_buffer_t *buffer, int32_t timeout) {
	(void)context;
	(void)timeout;
	(void)pthread_mutex_lock(&runtime_mutex);
	if (buffer->vstream_buf.size > 0) {
		tokens[tail % 64] = input_data[0] == 'u' ? 0 : buffer->vstream_buf.pts;
		results[tail % 64] = input_data[0] == 'x' ? 0 : 1;
		pixels[tail % 64] = input_data[0];
		tail++;
	}
	int failure = queue_failure;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return failure ? -1 : 0;
}

int32_t hb_mm_mc_dequeue_output_buffer(media_codec_context_t *context,
	media_codec_buffer_t *buffer, media_codec_output_buffer_info_t *info, int32_t timeout) {
	(void)context;
	(void)pthread_mutex_lock(&runtime_mutex);
	if (output_absent || output_budget == 0) {
		(void)pthread_mutex_unlock(&runtime_mutex);
		if (timeout > 0) {
			runtime_delay_ms(timeout);
		}
		return -1;
	}
	if (head == tail) {
		(void)pthread_mutex_unlock(&runtime_mutex);
		if (timeout > 0) {
			runtime_delay_ms(timeout);
		}
		return -1;
	}
	if (delayed_output_mode && !delayed_output_ready) {
		delayed_output_ready = 1;
		(void)pthread_mutex_unlock(&runtime_mutex);
		runtime_delay_ms(50);
		return -1;
	}
	size_t y_size = (size_t)(output_width + 4) * (size_t)output_height;
	memset(y_data, capture_pixels ? pixels[head % 64] : 80, y_size);
	memset(uv_data, 128, y_size / 2);
	if (capture_pixels == 2) {
		/* Spatial fixture uses the vendor's padded stride, not packed output.
		 * It distinguishes wrong capture, row, column and chroma in ROI tests. */
		int stride = output_width + 4;
		memset(y_data, 0xee, y_size);
		memset(uv_data, 0xdd, y_size / 2);
		for (int row = 0; row < output_height; row++) {
			for (int col = 0; col < output_width; col++) {
				y_data[row * stride + col] = (unsigned char)(pixels[head % 64] + row * 7 + col * 3);
			}
		}
		for (int row = 0; row < output_height / 2; row++) {
			for (int col = 0; col < output_width; col++) {
				uv_data[row * stride + col] = (unsigned char)(128 + row * 5 + col);
			}
		}
	}
	buffer->type = MC_VIDEO_FRAME_BUFFER;
	buffer->vframe_buf.pts = tokens[head % 64];
	buffer->vframe_buf.size = (uint32_t)((output_width + 4) * output_height * 3 / 2);
	buffer->vframe_buf.width = output_width + (invalid_geometry ? 2 : 0);
	buffer->vframe_buf.height = output_height;
	buffer->vframe_buf.stride = output_width + 4;
	buffer->vframe_buf.vstride = output_width + 4;
	buffer->vframe_buf.pix_fmt = MC_PIXEL_FORMAT_NV12;
	if (invalid_layout == 1) buffer->vframe_buf.pix_fmt = (mc_pixel_format_t)999;
	if (invalid_layout == 2) buffer->vframe_buf.vstride = 1;
	if (invalid_layout == 3) buffer->vframe_buf.pix_fmt = MC_PIXEL_FORMAT_YUV420P;
	buffer->vframe_buf.vir_ptr[0] = y_data;
	buffer->vframe_buf.vir_ptr[1] = uv_data;
	if (context->codec_id == MEDIA_CODEC_ID_MJPEG) {
		info->jpeg_frame_info.decode_result = results[head % 64];
	} else {
		info->video_frame_info.decode_result = results[head % 64];
	}
	head++;
	if (output_budget > 0) output_budget--;
	if (delayed_output_mode) {
		delayed_output_ready = 0;
	}
	(void)pthread_mutex_unlock(&runtime_mutex);
	return 0;
}

int32_t hb_mm_mc_queue_output_buffer(
	media_codec_context_t *context, media_codec_buffer_t *buffer, int32_t timeout) {
	(void)context;
	(void)buffer;
	(void)timeout;
	(void)pthread_mutex_lock(&runtime_mutex);
	if (output_return_blocked) output_return_entered = 1;
	while (output_return_blocked) (void)pthread_cond_wait(&runtime_condition, &runtime_mutex);
	output_returns++;
	(void)pthread_mutex_unlock(&runtime_mutex);
	return 0;
}

void *sp_init_decoder_module(void) {
	return (void *)1;
}

void sp_release_decoder_module(void *object) {
	(void)object;
}

int32_t sp_start_decode(
	void *object, const char *path, int32_t channel, int32_t codec, int32_t width, int32_t height) {
	(void)object;
	(void)path;
	(void)channel;
	(void)codec;
	output_width = width;
	output_height = height;
	return 0;
}

int32_t sp_decoder_get_image(void *object, char *buffer) {
	(void)object;
	if (decoder_chunks < 4) {
		return -1;
	}
	memset(buffer, 80, (size_t)(output_width * output_height));
	memset(buffer + output_width * output_height, 128,
		(size_t)(output_width * output_height / 2));
	decoder_chunks = 0;
	return 0;
}

int32_t sp_decoder_set_image(
	void *object, char *buffer, int32_t channel, int32_t size, int32_t eos) {
	(void)object;
	(void)buffer;
	(void)channel;
	(void)size;
	(void)eos;
	decoder_chunks++;
	return 0;
}

int32_t sp_stop_decode(void *object) {
	(void)object;
	return 0;
}
