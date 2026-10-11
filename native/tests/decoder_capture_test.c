/* Native adapter tests with controlled driver completion; not HW qualification. */
#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include "rdkx5_decoder_runtime.h"

extern void decoder_runtime_enable_delayed_output(int);
extern void decoder_runtime_capture_pixels(int);
extern void decoder_runtime_output_budget(int);
extern void decoder_runtime_reverse_pending(void);
extern void decoder_runtime_override_next_tag(uint64_t);
extern int decoder_runtime_submissions(void);
extern void decoder_runtime_queue_failure(int);
extern void decoder_runtime_set_input_failure(int);
extern void decoder_runtime_set_input_blocked(int);
extern int decoder_runtime_input_block_entered(void);
extern void decoder_runtime_invalid_geometry(int);
extern void decoder_runtime_invalid_layout(int);
extern void decoder_runtime_block_output_return(int);
extern int decoder_runtime_output_return_entered(void);
extern int decoder_runtime_output_returns(void);

static uint64_t now_ms(void) {
	struct timespec t;
	assert(clock_gettime(CLOCK_MONOTONIC, &t) == 0);
	return (uint64_t)t.tv_sec * 1000 + (uint64_t)t.tv_nsec / 1000000;
}
static void tick(void) {
	struct timespec t = {0, 1000000};
	(void)nanosleep(&t, NULL);
}
static void open_decoder(video_broker_rdkx5_decoder_t *d, video_broker_rdkx5_codec_t codec, uint64_t epoch) {
	decoder_runtime_enable_delayed_output(0);
	decoder_runtime_capture_pixels(1);
	decoder_runtime_output_budget(0);
	assert(video_broker_rdkx5_decoder_open(d, codec, 4, 2, epoch) == 0);
}
static int submit(video_broker_rdkx5_decoder_t *d, uint64_t epoch, uint64_t seq,
                  unsigned char value, unsigned char *pixels, vb_capture_origin *origin) {
	return video_broker_rdkx5_decoder_decode(d, &value, 1, pixels, 12, 0,
		(vb_capture_origin){epoch, seq}, origin);
}
static vb_capture_origin take(video_broker_rdkx5_decoder_t *d, unsigned char *pixels) {
	vb_capture_origin origin = {999, 999};
	uint64_t deadline = now_ms() + 1000;
	int rc = 0;
	decoder_runtime_output_budget(1);
	while (!rc && now_ms() < deadline) {
		rc = video_broker_rdkx5_decoder_poll(d, pixels, 12, &origin);
		if (!rc) { assert(origin.epoch == 0 && origin.sequence == 0); tick(); }
	}
	assert(rc == 1);
	return origin;
}
static void image_is(const unsigned char *pixels, unsigned char value) {
	for (int i = 0; i < 8; ++i) assert(pixels[i] == value);
	for (int i = 8; i < 12; ++i) assert(pixels[i] == 128);
}
static void reordered(video_broker_rdkx5_codec_t codec) {
	video_broker_rdkx5_decoder_t d = {0};
	unsigned char pixels[12];
	vb_capture_origin origin;
	const uint64_t epoch = UINT64_MAX - 3, base = (UINT64_C(1) << 53) + 10;
	open_decoder(&d, codec, epoch);
	for (uint64_t i = 1; i <= 3; ++i) {
		assert(submit(&d, epoch, base + i, (unsigned char)('a' + i), pixels, &origin) == 0);
		assert(origin.epoch == 0 && origin.sequence == 0);
	}
	decoder_runtime_reverse_pending();
	for (uint64_t i = 3; i > 0; --i) {
		origin = take(&d, pixels);
		assert(origin.epoch == epoch && origin.sequence == base + i);
		image_is(pixels, (unsigned char)('a' + i));
	}
	assert(d.captures.used == 0);
	video_broker_rdkx5_decoder_close(&d);
}
static void ready_before_submit(void) {
	video_broker_rdkx5_decoder_t d = {0};
	unsigned char pixels[12];
	vb_capture_origin origin;
	open_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_H265, 17);
	assert(submit(&d, 17, 101, 'a', pixels, &origin) == 0);
	decoder_runtime_output_budget(1);
	uint64_t deadline = now_ms() + 1000;
	bool ready = false;
	while (!ready && now_ms() < deadline) {
		(void)pthread_mutex_lock(&d.output_mutex);
		ready = d.produced_generation == 1;
		(void)pthread_mutex_unlock(&d.output_mutex);
		if (!ready) tick();
	}
	assert(ready);
	assert(submit(&d, 17, 102, 'b', pixels, &origin) == 1);
	assert(origin.epoch == 17 && origin.sequence == 101);
	image_is(pixels, 'a');
	origin = take(&d, pixels);
	assert(origin.epoch == 17 && origin.sequence == 102);
	image_is(pixels, 'b');
	video_broker_rdkx5_decoder_close(&d);
}
static void unavailable(uint64_t tag) {
	video_broker_rdkx5_decoder_t d = {0};
	unsigned char pixels[12];
	vb_capture_origin origin;
	open_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 21);
	assert(submit(&d, 21, 1, 'a', pixels, &origin) == 0);
	origin = take(&d, pixels);
	assert(origin.sequence == 1);
	assert(submit(&d, 21, 2, 'b', pixels, &origin) == 0);
	decoder_runtime_override_next_tag(tag); /* zero, duplicate or foreign tag */
	origin = take(&d, pixels);
	assert(origin.epoch == 0 && origin.sequence == 0);
	image_is(pixels, 'b');
	assert(submit(&d, 21, 3, 'c', pixels, &origin) == -1);
	assert(decoder_runtime_submissions() == 2);
	video_broker_rdkx5_decoder_close(&d);
}
static void admission_and_failure(void) {
	video_broker_rdkx5_decoder_t d = {0};
	unsigned char pixels[12];
	vb_capture_origin origin = {9, 9};
	open_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 31);
	assert(submit(&d, 32, 1, 'a', pixels, &origin) == -1);
	assert(origin.epoch == 0 && origin.sequence == 0);
	assert(decoder_runtime_submissions() == 0);
	decoder_runtime_set_input_failure(1);
	assert(submit(&d, 31, 1, 'a', pixels, &origin) == -1);
	assert(d.captures.used == 0); /* proven non-submission only */
	decoder_runtime_set_input_failure(0);
	for (uint64_t i = 2; i <= 65; ++i) assert(submit(&d, 31, i, 'a', pixels, &origin) == 0);
	assert(submit(&d, 31, 66, 'a', pixels, &origin) == -1);
	assert(decoder_runtime_submissions() == 64 && d.captures.used == 64);
	video_broker_rdkx5_decoder_close(&d);
	open_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 32);
	decoder_runtime_queue_failure(1);
	assert(submit(&d, 32, 1, 'a', pixels, &origin) == -1);
	assert(d.captures.used == 1 && decoder_runtime_submissions() == 1);
	assert(origin.epoch == 0 && origin.sequence == 0);
	decoder_runtime_queue_failure(0);
	assert(submit(&d, 32, 2, 'b', pixels, &origin) == -1);
	assert(decoder_runtime_submissions() == 1);
	video_broker_rdkx5_decoder_close(&d);
	open_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 0);
	assert(submit(&d, 0, 0, 'a', pixels, &origin) == 0);
	origin = take(&d, pixels);
	assert(origin.epoch == 0 && origin.sequence == 0);
	assert(submit(&d, 32, 2, 'b', pixels, &origin) == -1);
	video_broker_rdkx5_decoder_close(&d);
}
static void rejected_input_release(video_broker_rdkx5_codec_t codec, int failed_return) {
	video_broker_rdkx5_decoder_t d = {0};
	unsigned char pixels[12], oversized[1025] = {0}; /* stub input capacity=1024 */
	vb_capture_origin origin = {9, 9};
	open_decoder(&d, codec, 41);
	decoder_runtime_queue_failure(failed_return);
	assert(video_broker_rdkx5_decoder_decode(&d, oversized, sizeof(oversized),
		pixels, sizeof(pixels), 0, (vb_capture_origin){41, 1}, &origin) == -1);
	assert(origin.epoch == 0 && origin.sequence == 0);
	assert(decoder_runtime_submissions() == 0);
	assert(d.captures.used == (failed_return ? 1U : 0U));
	decoder_runtime_queue_failure(0);
	assert(submit(&d, 41, 2, 'a', pixels, &origin) == (failed_return ? -1 : 0));
	assert(decoder_runtime_submissions() == (failed_return ? 0 : 1));
	assert(video_broker_rdkx5_decoder_close(&d) == 0);
}
static void async_replacement(void) {
	video_broker_rdkx5_decoder_t d = {0};
	video_broker_rdkx5_async_decoder_t worker;
	unsigned char pixels[12], value = 'a';
	vb_capture_origin origin;
	uint64_t replacements = 0;
	open_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 43);
	assert(video_broker_rdkx5_async_decoder_init(&worker, 1, 12, 500) == 0);
	decoder_runtime_set_input_blocked(1);
	assert(video_broker_rdkx5_async_decoder_start(&worker, &d) == 0);
	assert(video_broker_rdkx5_async_decoder_submit(&worker, &value, 1, (vb_capture_origin){43, 1}) == 1);
	uint64_t deadline = now_ms() + 1000;
	while (!decoder_runtime_input_block_entered() && now_ms() < deadline) tick();
	assert(decoder_runtime_input_block_entered());
	for (uint64_t i = 2; i <= 9; ++i) {
		value = (unsigned char)('a' + i);
		assert(video_broker_rdkx5_async_decoder_submit(&worker, &value, 1, (vb_capture_origin){43, i}) > 0);
	}
	value = 'z'; /* copied input must not reference this reused capture storage */
	decoder_runtime_output_budget(1);
	decoder_runtime_set_input_blocked(0);
	int rc = 0;
	deadline = now_ms() + 1000;
	while (!rc && now_ms() < deadline) {
		rc = video_broker_rdkx5_async_decoder_poll(&worker, pixels, 12, &replacements, &origin);
		if (!rc) tick();
	}
	assert(rc == 1 && origin.epoch == 43 && origin.sequence == 1 && replacements == 7);
	image_is(pixels, 'a');
	decoder_runtime_output_budget(1);
	rc = 0; deadline = now_ms() + 1000;
	while (!rc && now_ms() < deadline) {
		rc = video_broker_rdkx5_async_decoder_poll(&worker, pixels, 12, &replacements, &origin);
		if (!rc) tick();
	}
	assert(rc == 1 && origin.epoch == 43 && origin.sequence == 9);
	image_is(pixels, (unsigned char)('a' + 9));
	assert(decoder_runtime_submissions() == 2);
	video_broker_rdkx5_async_decoder_request_stop(&worker);
	origin = (vb_capture_origin){43, 9};
	assert(video_broker_rdkx5_async_decoder_poll(&worker, pixels, 12, &replacements, &origin) == -1);
	assert(origin.epoch == 0 && origin.sequence == 0);
	video_broker_rdkx5_async_decoder_destroy(&worker);
}

static void geometry_and_capacity(void) {
	video_broker_rdkx5_decoder_t d = {0};
	video_broker_rdkx5_async_decoder_t worker;
	unsigned char pixels[12];
	vb_capture_origin origin = {1, 2};
	open_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_H265, 61);
	assert(video_broker_rdkx5_async_decoder_init(&worker, 1, 14, 2) == 0);
	assert(video_broker_rdkx5_async_decoder_start(&worker, &d) == -1);
	video_broker_rdkx5_async_decoder_destroy(&worker);
	assert(submit(&d, 61, 1, 'a', pixels, &origin) == 0);
	decoder_runtime_invalid_geometry(1);
	decoder_runtime_output_budget(1);
	uint64_t deadline = now_ms() + 1000;
	int rc = 0;
	while (!rc && now_ms() < deadline) {
		rc = video_broker_rdkx5_decoder_poll(&d, pixels, 12, &origin);
		if (!rc) tick();
	}
	assert(rc == -1 && origin.epoch == 0 && origin.sequence == 0);
	video_broker_rdkx5_decoder_close(&d);
	decoder_runtime_invalid_geometry(0);
}
static void rejected_surface_preserves_previous(int mode) {
	video_broker_rdkx5_decoder_t d = {0};
	unsigned char pixels[12];
	vb_capture_origin origin;
	open_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_H265, 91);
	assert(submit(&d, 91, 1, 'a', pixels, &origin) == 0);
	assert(submit(&d, 91, 2, 'b', pixels, &origin) == 0);
	decoder_runtime_output_budget(1);
	bool ready = false;
	uint64_t end = now_ms() + 1000;
	while (!ready && now_ms() < end) {
		(void)pthread_mutex_lock(&d.output_mutex);
		ready = d.produced_generation == 1;
		(void)pthread_mutex_unlock(&d.output_mutex);
		if (!ready) tick();
	}
	assert(ready);
	decoder_runtime_invalid_layout(mode);
	end = now_ms() + 1000;
	while (decoder_runtime_output_returns() != 1 && now_ms() < end) tick();
	assert(decoder_runtime_output_returns() == 1);
	decoder_runtime_block_output_return(1);
	decoder_runtime_output_budget(1);
	end = now_ms() + 1000;
	while (!decoder_runtime_output_return_entered() && now_ms() < end) tick();
	assert(decoder_runtime_output_return_entered());
	/* Bad B has been inspected but native return/error publication is blocked.
	 * The still-unconsumed valid A must not contain any pixels from B. */
	assert(video_broker_rdkx5_decoder_poll(&d, pixels, 12, &origin) == 1);
	assert(origin.epoch == 91 && origin.sequence == 1);
	image_is(pixels, 'a');
	decoder_runtime_block_output_return(0);
	video_broker_rdkx5_decoder_close(&d);
	decoder_runtime_invalid_layout(0);
}
int main(void) {
	for (int codec = VIDEO_BROKER_RDKX5_CODEC_H264; codec <= VIDEO_BROKER_RDKX5_CODEC_MJPEG; ++codec) {
		rejected_input_release((video_broker_rdkx5_codec_t)codec, 0);
		rejected_input_release((video_broker_rdkx5_codec_t)codec, 1);
	}
	rejected_surface_preserves_previous(1);
	rejected_surface_preserves_previous(2);
	rejected_surface_preserves_previous(3);
	reordered(VIDEO_BROKER_RDKX5_CODEC_H264);
	reordered(VIDEO_BROKER_RDKX5_CODEC_H265);
	reordered(VIDEO_BROKER_RDKX5_CODEC_MJPEG);
	ready_before_submit();
	unavailable(0); unavailable(1); unavailable(UINT64_MAX);
	admission_and_failure();
	async_replacement();
	geometry_and_capacity();
	puts("decoder capture exact/reordered/async/unknown/admission PASS");
	return 0;
}
