#ifndef VIDEO_BROKER_RDKX5_DECODER_RUNTIME_H
#define VIDEO_BROKER_RDKX5_DECODER_RUNTIME_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#include "hb_media_codec.h"
#include "capture_correlation.h"
#include "pipeline_latency.h"

typedef enum {
	VIDEO_BROKER_RDKX5_CODEC_H264 = 1,
	VIDEO_BROKER_RDKX5_CODEC_H265 = 2,
	VIDEO_BROKER_RDKX5_CODEC_MJPEG = 3,
} video_broker_rdkx5_codec_t;

typedef enum {
	VIDEO_BROKER_RDKX5_MJPEG_TRIAL_SUCCESS = 0,
	VIDEO_BROKER_RDKX5_MJPEG_TRIAL_OPEN_FAILED = 1,
	VIDEO_BROKER_RDKX5_MJPEG_TRIAL_FRAME_FAILED = 2,
	VIDEO_BROKER_RDKX5_MJPEG_TRIAL_DEADLINE_FAILED = 3,
} video_broker_rdkx5_mjpeg_trial_result_t;

typedef struct {
	media_codec_context_t context;
	size_t frame_size;
	int width;
	int height;
	video_broker_rdkx5_codec_t codec;
	unsigned char *latest_frame;
	pthread_t output_thread;
	pthread_mutex_t output_mutex;
	pthread_cond_t output_condition;
	uint64_t produced_generation;
	uint64_t consumed_generation;
	vb_capture_correlation captures;
	vb_capture_origin latest_origin;
	/* Single submitter owns this diagnostic state, never the output thread. */
	vb_pipeline_latency input_latency;
	int output_error;
	int close_error;
	bool initialized;
	bool started;
	bool output_mutex_initialized;
	bool output_condition_initialized;
	bool output_thread_started;
	bool output_thread_stop;
	bool async_owned;
} video_broker_rdkx5_decoder_t;

/*
 * One asynchronous MJPEG accelerator trial. The access unit and proof surface
 * are owned by this object, so the capture thread can return its V4L2 buffer
 * immediately and continue through the software decoder while the candidate
 * is opening or blocked inside the vendor codec API.
 */
typedef struct {
	video_broker_rdkx5_decoder_t decoder;
	unsigned char *access_unit;
	unsigned char *nv12;
	size_t access_unit_capacity;
	size_t access_unit_size;
	size_t nv12_capacity;
	int width;
	int height;
	int timeout_ms;
	int64_t deadline_unix_ms;
	int64_t completed_unix_ms;
	uint64_t token;
	video_broker_rdkx5_mjpeg_trial_result_t result;
	pthread_t thread;
	pthread_mutex_t mutex;
	bool initialized;
	bool mutex_initialized;
	bool thread_started;
	bool completed;
	bool result_observed;
	bool accepted;
	int join_error;
} video_broker_rdkx5_mjpeg_trial_t;

/* Bounded latest-frame input worker for an admitted hardware decoder. The
 * capture thread owns neither vendor waits nor an unbounded queue: one pending
 * access unit may be replaced while one unit is in flight. */
typedef struct {
	video_broker_rdkx5_decoder_t *decoder;
	unsigned char *pending_access_unit;
	unsigned char *working_access_unit;
	unsigned char *working_nv12;
	unsigned char *latest_nv12;
	size_t access_unit_capacity;
	size_t pending_access_unit_size;
	size_t nv12_capacity;
	int timeout_ms;
	pthread_t thread;
	pthread_mutex_t mutex;
	uint64_t output_generation;
	uint64_t consumed_generation;
	uint64_t replacements;
	vb_capture_origin pending_origin;
	vb_capture_origin latest_origin;
	bool initialized;
	bool mutex_initialized;
	bool thread_started;
	bool pending;
	bool stop_requested;
	bool exited;
	bool failed;
	bool cleanup_failed;
	int join_error;
} video_broker_rdkx5_async_decoder_t;

int video_broker_rdkx5_decoder_open(
	video_broker_rdkx5_decoder_t *decoder,
	video_broker_rdkx5_codec_t codec,
	int width,
	int height,
	uint64_t qualified_capture_epoch
);

/* A nonzero capture epoch is accepted only from the trusted composition after
 * exact opaque-tag echo has been qualified for this provider/codec. Zero means
 * explicitly unavailable provenance, not successful capture qualification.
 * The caller must admit sizeof(decoder), including the fixed correlation table.
 * Epoch/source/process authentication belongs to the enclosing source domain.
 * Objects MUST be zero-initialized before their first open. A live or failed
 * object cannot be overwritten to reopen it. One submitter owns decode;
 * close is allowed only after it has joined. */

/*
 * Submits one frame-sized access unit while a dedicated output thread drains
 * decoded frames concurrently, matching the producer/consumer contract of the
 * RDK X5 codec SDK. The bounded timeout is used only when no output is already
 * available; it closes the submit/drain scheduler race without serializing the
 * input side behind the hardware decoder. Returns 1 when the most recent NV12
 * frame was written, 0 when the access unit was accepted but no new output is
 * available by the deadline, and -1 on a structural codec error.
 */
int video_broker_rdkx5_decoder_decode(
	video_broker_rdkx5_decoder_t *decoder,
	const unsigned char *access_unit,
	size_t access_unit_size,
	unsigned char *nv12,
	size_t nv12_capacity,
	int timeout_ms,
	vb_capture_origin input_origin,
	vb_capture_origin *output_origin
);

/* Copies pixel and origin atomically; never submits a replacement input.
 * {0,0} means provenance unavailable. The reference is cleared on 0/-1 too.
 * No vendor wait; caller serializes with decode and joins before close. */
int video_broker_rdkx5_decoder_poll(
	video_broker_rdkx5_decoder_t *decoder, unsigned char *nv12,
	size_t nv12_capacity, vb_capture_origin *output_origin);

int video_broker_rdkx5_mjpeg_trial_init(
	video_broker_rdkx5_mjpeg_trial_t *trial,
	size_t access_unit_capacity,
	int width,
	int height
);

/* Copies the access unit before starting the worker and returns without
 * waiting for any vendor codec operation. */
int video_broker_rdkx5_mjpeg_trial_start(
	video_broker_rdkx5_mjpeg_trial_t *trial,
	const unsigned char *access_unit,
	size_t access_unit_size,
	uint64_t token,
	int timeout_ms,
	int64_t deadline_unix_ms
);

/* Non-blocking until the worker has published completion. Returns 1 exactly
 * once for a completed result, 0 while pending/already observed and -1 for an
 * invalid object. */
int video_broker_rdkx5_mjpeg_trial_poll(
	video_broker_rdkx5_mjpeg_trial_t *trial,
	video_broker_rdkx5_mjpeg_trial_result_t *result,
	uint64_t *token,
	int64_t *deadline_unix_ms,
	int64_t *completed_unix_ms
);

/* Commits a successfully observed candidate. Its decoder remains owned by the
 * trial object and may then be used by the capture thread. */
int video_broker_rdkx5_mjpeg_trial_accept(video_broker_rdkx5_mjpeg_trial_t *trial);
video_broker_rdkx5_decoder_t *video_broker_rdkx5_mjpeg_trial_decoder(
	video_broker_rdkx5_mjpeg_trial_t *trial);

/* Recycles metadata only after the candidate worker has already closed the
 * decoder. It fails closed if codec resources are still live. */
int video_broker_rdkx5_mjpeg_trial_recycle(video_broker_rdkx5_mjpeg_trial_t *trial);
int video_broker_rdkx5_mjpeg_trial_destroy(video_broker_rdkx5_mjpeg_trial_t *trial);

/* 0 means all owned SDK/thread resources were released. -1 means ownership is
 * uncertain: retain the object, never retry vendor release or reuse it. The
 * source process must retire; its owner must verify native resource cleanup
 * before releasing the corresponding reservation. No in-process reset API. */
int video_broker_rdkx5_decoder_close(video_broker_rdkx5_decoder_t *decoder);
bool video_broker_rdkx5_decoder_resources_quarantined(void);

int video_broker_rdkx5_async_decoder_init(
	video_broker_rdkx5_async_decoder_t *worker,
	size_t access_unit_capacity,
	size_t nv12_capacity,
	int timeout_ms
);
int video_broker_rdkx5_async_decoder_start(
	video_broker_rdkx5_async_decoder_t *worker,
	video_broker_rdkx5_decoder_t *decoder
);
/* start/reap/destroy are serialized by the lifecycle owner. Once start succeeds,
 * the worker owns decoder teardown. On start failure the caller retains it.
 * submit/poll/request_stop may run concurrently with the active worker.
 * The decoder output condition is the only wakeup source for input, output,
 * errors and stop: no timeout polling or borrowed callback target is needed. */
/* Transfers a completed MJPEG candidate to the async worker for codec teardown.
 * After this returns, the caller only polls/reaps the worker and never closes
 * the decoder on the capture thread. */
int video_broker_rdkx5_mjpeg_trial_begin_async_reset(
	video_broker_rdkx5_mjpeg_trial_t *trial,
	video_broker_rdkx5_async_decoder_t *worker
);
/* Returns 1 for a new pending unit and 2 when the previous pending unit was
 * replaced by the latest frame. */
int video_broker_rdkx5_async_decoder_submit(
	video_broker_rdkx5_async_decoder_t *worker,
	const unsigned char *access_unit,
	size_t access_unit_size,
	vb_capture_origin input_origin
);
/* Returns 1 with a new frame, 0 without output and -1 after decoder failure. */
int video_broker_rdkx5_async_decoder_poll(
	video_broker_rdkx5_async_decoder_t *worker,
	unsigned char *nv12,
	size_t nv12_capacity,
	uint64_t *replacements,
	vb_capture_origin *output_origin
);
void video_broker_rdkx5_async_decoder_request_stop(
	video_broker_rdkx5_async_decoder_t *worker);
/* Non-blocking: 1 only after join AND successful SDK release, 0 pending,
 * -1 on release failure. Failure remains sticky after the join. */
int video_broker_rdkx5_async_decoder_reap(video_broker_rdkx5_async_decoder_t *worker);
int video_broker_rdkx5_async_decoder_destroy(video_broker_rdkx5_async_decoder_t *worker);

#endif
