#ifndef VIDEO_BROKER_TEST_SP_CODEC_H
#define VIDEO_BROKER_TEST_SP_CODEC_H

#include <stdint.h>

#define SP_ENCODER_H264 1
#define SP_ENCODER_H265 2

void *sp_init_decoder_module(void);
void sp_release_decoder_module(void *);
int32_t sp_start_decode(void *, const char *, int32_t, int32_t, int32_t, int32_t);
int32_t sp_decoder_get_image(void *, char *);
int32_t sp_decoder_set_image(void *, char *, int32_t, int32_t, int32_t);
int32_t sp_stop_decode(void *);

void *sp_init_encoder_module(void);
void sp_release_encoder_module(void *);
int32_t sp_start_encode(void *, int32_t, int32_t, int32_t, int32_t, int32_t);
int32_t sp_stop_encode(void *);
int32_t sp_encoder_set_frame(void *, char *, int32_t);
int32_t sp_encoder_get_stream(void *, char *);

#endif
