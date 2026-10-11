#ifndef VIDEO_BROKER_RDKX5_PERSISTENT_ENCODER_H
#define VIDEO_BROKER_RDKX5_PERSISTENT_ENCODER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <hb_media_codec.h>
#include "h26x_bootstrap.h"
#include "pipeline_latency.h"

typedef enum {
	VIDEO_BROKER_RDKX5_ENCODER_H264 = 1,
	VIDEO_BROKER_RDKX5_ENCODER_H265 = 2,
} video_broker_rdkx5_encoder_codec_t;

typedef enum {
	VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR = 1,
	VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_CBR = 2,
	VIDEO_BROKER_RDKX5_RATE_CONTROL_QUALITY_BOUNDED_AVBR = 3,
} video_broker_rdkx5_rate_control_profile_t;

typedef enum {
	VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ERROR = -1,
	VIDEO_BROKER_RDKX5_ENCODER_CONTROL_UNCHANGED = 0,
	VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED = 1,
	VIDEO_BROKER_RDKX5_ENCODER_CONTROL_RATE_CHANGED = 2,
} video_broker_rdkx5_encoder_control_result_t;

typedef struct {
	media_codec_context_t context;
	bool initialized;
	bool started;
	bool active;
	/* A failed SDK buffer operation poisons this queue generation. Only
	 * checked destroy may clear it; no subsequent frame/control may enter SDK. */
	bool queue_faulted;
	int close_error;
	video_broker_rdkx5_encoder_codec_t codec;
	video_broker_rdkx5_rate_control_profile_t rate_control_profile;
	int instance_index;
	int width;
	int height;
	int frame_rate;
	int bitrate_kbps;
	int vbv_buffer_ms;
	int max_qp_i;
	int max_qp_p;
	int max_qp_b;
	uint64_t epoch;
	/* Bounded, encoder-owned configuration observed from actual hardware output.
	 * It survives private warmup and delivery epochs, never codec recreation. */
	video_broker_h26x_bootstrap_t bootstrap;
	/* Last rate-limited SDK subphase delay. Caller may report it through its
	 * bounded diagnostic sink; encoder never performs hot-path log I/O here. */
	vb_pipeline_latency encode_latency;
	vb_pipeline_delay encode_delay;
	bool encode_delay_valid;
} video_broker_rdkx5_persistent_encoder_t;

/* Objects MUST be zero-initialized before first init. Failed release retains
 * ownership: callers must not free, overwrite or reuse the object. A process
 * with uncertain native release must retire, not open a replacement encoder.
 * An initialized encoder is address-bound to its vendor media context. Owners
 * may move a pointer to this object, but must not copy or relocate the object
 * before video_broker_rdkx5_persistent_encoder_destroy(). */

/*
 * Initializes one hardware encoder during bridge warm-up. Live delivery uses
 * a strict realtime CBR budget and short VBV; archive delivery selects either
 * quality-bounded CBR or hardware AVBR with codec-aware QP ceilings.
 */
int video_broker_rdkx5_persistent_encoder_init(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	video_broker_rdkx5_encoder_codec_t codec,
	video_broker_rdkx5_rate_control_profile_t rate_control_profile,
	int instance_index,
	int width,
	int height,
	int frame_rate,
	int bitrate_kbps);

/*
 * Returns ACTIVATED when an inactive encoder starts a new media epoch,
 * RATE_CHANGED when an already-active encoder applies new FPS/bitrate without
 * changing epoch, UNCHANGED for a no-op (including deactivation), and ERROR on
 * failure. Activation and rate changes request a decoder refresh without
 * recreating the codec instance. The vendor may emit an IDR without repeating
 * parameter sets after warmup or a runtime rate change. This encoder retains
 * the complete hardware configuration and prefixes header-free IDRs so a
 * newly published consumer need not have observed the private warmup.
 */
int video_broker_rdkx5_persistent_encoder_set_control(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	bool active,
	int frame_rate,
	int bitrate_kbps);

/* Requests one hardware IDR without changing rate control or encoder epoch. */
int video_broker_rdkx5_persistent_encoder_request_idr(
	video_broker_rdkx5_persistent_encoder_t *encoder);

/*
 * Returns encoded access-unit size, zero while inactive/no output, and -1 on
 * failure. Stream capacity is an enforced boundary.
 */
int video_broker_rdkx5_persistent_encoder_encode(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	const unsigned char *frame,
	size_t frame_size,
	unsigned char *stream,
	size_t stream_capacity,
	uint64_t *epoch,
	int *bitrate_kbps);

/* Zero is a completed SDK release; negative retains ownership. Caller must
 * serialize with encode/control. No in-process reset of failed ownership. */
int video_broker_rdkx5_persistent_encoder_destroy(
	video_broker_rdkx5_persistent_encoder_t *encoder);
bool video_broker_rdkx5_encoder_resources_quarantined(void);

#endif
