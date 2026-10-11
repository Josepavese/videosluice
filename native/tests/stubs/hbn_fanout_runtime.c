#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "sp_codec.h"
#include "sp_vio.h"

typedef struct {
	int codec;
	int width;
	unsigned int frames;
} fake_encoder_t;

void *sp_init_vio_module(void) {
	return calloc(1, 1);
}

void sp_release_vio_module(void *module) {
	free(module);
}

int sp_open_camera_v2(
	void *module,
	int pipe_id,
	int video_index,
	int channel_count,
	sp_sensors_parameters *sensor,
	int *widths,
	int *heights) {
	(void)pipe_id;
	(void)video_index;
	(void)widths;
	(void)heights;
	return module != NULL && sensor != NULL && channel_count > 0 ? 0 : -1;
}

int sp_vio_get_frame(void *module, char *frame, int width, int height, int timeout_ms) {
	if (module == NULL || frame == NULL || width <= 0 || height <= 0 || timeout_ms <= 0) return -1;
	frame[0] = 0x60;
	return 0;
}

int sp_vio_close(void *module) {
	return module == NULL ? -1 : 0;
}

void *sp_init_encoder_module(void) {
	return calloc(1, sizeof(fake_encoder_t));
}

void sp_release_encoder_module(void *module) {
	free(module);
}

int32_t sp_start_encode(
	void *module,
	int32_t channel,
	int32_t codec,
	int32_t width,
	int32_t height,
	int32_t bitrate_kbps) {
	fake_encoder_t *encoder = module;
	(void)channel;
	if (encoder == NULL || (codec != SP_ENCODER_H264 && codec != SP_ENCODER_H265) ||
	    width <= 0 || height <= 0 || bitrate_kbps <= 0) return -1;
	encoder->codec = codec;
	encoder->width = width;
	return 0;
}

int32_t sp_stop_encode(void *module) {
	return module == NULL ? -1 : 0;
}

int32_t sp_encoder_set_frame(void *module, char *frame, int32_t size) {
	return module != NULL && frame != NULL && size > 0 ? 0 : -1;
}

int32_t sp_encoder_get_stream(void *module, char *stream) {
	static const unsigned char h265[] = {
		0, 0, 0, 1, 32U << 1, 1,
		0, 0, 0, 1, 33U << 1, 1,
		0, 0, 0, 1, 34U << 1, 1,
		0, 0, 0, 1, 19U << 1, 1,
	};
	static const unsigned char h264[] = {
		0, 0, 0, 1, 0x67, 1,
		0, 0, 0, 1, 0x68, 1,
		0, 0, 0, 1, 0x65, 1,
	};
	fake_encoder_t *encoder = module;
	if (encoder == NULL || stream == NULL) return -1;
	/* The private warmup gets the sole complete hardware bootstrap. Later
	 * access units are bare IDRs, including after the role is published. */
	if (encoder->frames++ > 0) {
		const unsigned char idr[] = {0, 0, 0, 1,
			encoder->codec == SP_ENCODER_H265 ? 19U << 1 : 0x65,
			(unsigned char)(encoder->width / 10)};
		memcpy(stream, idr, sizeof(idr));
		return (int32_t)sizeof(idr);
	}
	if (encoder->codec == SP_ENCODER_H265) {
		memcpy(stream, h265, sizeof(h265));
		stream[sizeof(h265) - 1] = (char)(encoder->width / 10);
		return (int32_t)sizeof(h265);
	}
	memcpy(stream, h264, sizeof(h264));
	stream[sizeof(h264) - 1] = (char)(encoder->width / 10);
	return (int32_t)sizeof(h264);
}

void *sp_init_decoder_module(void) { return NULL; }
void sp_release_decoder_module(void *module) { (void)module; }
int32_t sp_start_decode(void *a, const char *b, int32_t c, int32_t d, int32_t e, int32_t f) {
	(void)a; (void)b; (void)c; (void)d; (void)e; (void)f; return -1;
}
int32_t sp_decoder_get_image(void *a, char *b) { (void)a; (void)b; return -1; }
int32_t sp_decoder_set_image(void *a, char *b, int32_t c, int32_t d, int32_t e) {
	(void)a; (void)b; (void)c; (void)d; (void)e; return -1;
}
int32_t sp_stop_decode(void *module) { (void)module; return -1; }
