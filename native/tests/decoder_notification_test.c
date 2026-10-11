/* Event delivery tests: no replacement input, timer polling or callback target. */
#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <time.h>
#include "rdkx5_decoder_runtime.h"

extern void decoder_runtime_enable_delayed_output(int);
extern void decoder_runtime_capture_pixels(int);
extern void decoder_runtime_output_budget(int);
extern int decoder_runtime_submissions(void);
extern int __real_pthread_create(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *);
extern int __real_pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *);

static _Atomic(pthread_cond_t *) observed_condition;
static atomic_bool parked;
static atomic_bool fail_create;
static atomic_int spurious_waits;
static atomic_uint waits;

int __wrap_pthread_create(pthread_t *thread, const pthread_attr_t *attrs,
                         void *(*fn)(void *), void *arg) {
	if (atomic_exchange(&fail_create, false)) return EAGAIN;
	return __real_pthread_create(thread, attrs, fn, arg);
}
int __wrap_pthread_cond_wait(pthread_cond_t *condition, pthread_mutex_t *mutex) {
	bool observed = condition == atomic_load(&observed_condition);
	if (observed) {
		atomic_fetch_add(&waits, 1);
		int remaining = atomic_load(&spurious_waits);
		if (remaining > 0 && atomic_compare_exchange_strong(&spurious_waits, &remaining, remaining - 1)) {
			return 0; /* Legal spurious wake: caller still owns the mutex. */
		}
		atomic_store(&parked, true);
	}
	int rc = __real_pthread_cond_wait(condition, mutex);
	if (observed) atomic_store(&parked, false);
	return rc;
}
static uint64_t clock_ns(clockid_t clock) {
	struct timespec t;
	assert(clock_gettime(clock, &t) == 0);
	return (uint64_t)t.tv_sec * UINT64_C(1000000000) + (uint64_t)t.tv_nsec;
}
static uint64_t deadline(void) { return clock_ns(CLOCK_MONOTONIC) + UINT64_C(1000000000); }
static void tick(void) {
	struct timespec t = {0, 1000000};
	(void)nanosleep(&t, NULL);
}
static void await_parked(int submissions) {
	uint64_t end = deadline();
	while ((!atomic_load(&parked) || decoder_runtime_submissions() != submissions) &&
	       clock_ns(CLOCK_MONOTONIC) < end) tick();
	assert(atomic_load(&parked) && decoder_runtime_submissions() == submissions);
}
static void setup_decoder(video_broker_rdkx5_decoder_t *d, video_broker_rdkx5_codec_t codec, uint64_t epoch) {
	decoder_runtime_enable_delayed_output(0);
	decoder_runtime_capture_pixels(1);
	decoder_runtime_output_budget(0);
	assert(video_broker_rdkx5_decoder_open(d, codec, 4, 2, epoch) == 0);
	atomic_store(&observed_condition, &d->output_condition);
	atomic_store(&parked, false);
	atomic_store(&waits, 0);
}
static void setup_worker(video_broker_rdkx5_async_decoder_t *w, video_broker_rdkx5_decoder_t *d) {
	assert(video_broker_rdkx5_async_decoder_init(w, 1, 12, 2) == 0);
	assert(video_broker_rdkx5_async_decoder_start(w, d) == 0);
}
static int await_result(video_broker_rdkx5_async_decoder_t *w, unsigned char *pixels, vb_capture_origin *origin) {
	uint64_t end = deadline(), replacements = 0;
	int rc = 0;
	while (!rc && clock_ns(CLOCK_MONOTONIC) < end) {
		rc = video_broker_rdkx5_async_decoder_poll(w, pixels, 12, &replacements, origin);
		assert(replacements == 0);
		if (!rc) tick();
	}
	return rc;
}
static void late_frame(video_broker_rdkx5_codec_t codec, uint64_t epoch) {
	video_broker_rdkx5_decoder_t d = {0};
	video_broker_rdkx5_async_decoder_t w;
	unsigned char input = 'a', pixels[12];
	vb_capture_origin origin;
	setup_decoder(&d, codec, epoch);
	atomic_store(&spurious_waits, 3);
	setup_worker(&w, &d);
	await_parked(0);
	assert(atomic_load(&waits) >= 4); /* Spurious wake must recheck predicates. */
	assert(video_broker_rdkx5_async_decoder_submit(&w, &input, 1,
		(vb_capture_origin){epoch, epoch ? 101 : 0}) == 1);
	input = 'z';
	await_parked(1); /* decode timed out; worker is sleeping with no further input. */
	decoder_runtime_output_budget(1);
	assert(await_result(&w, pixels, &origin) == 1);
	assert(origin.epoch == epoch && origin.sequence == (epoch ? 101 : 0));
	for (int i = 0; i < 8; ++i) assert(pixels[i] == 'a');
	for (int i = 8; i < 12; ++i) assert(pixels[i] == 128);
	assert(decoder_runtime_submissions() == 1);
	await_parked(1);
	clockid_t cpu_clock;
	assert(pthread_getcpuclockid(w.thread, &cpu_clock) == 0);
	uint64_t cpu_before = clock_ns(cpu_clock);
	struct timespec pause = {0, 100000000};
	(void)nanosleep(&pause, NULL);
	uint64_t cpu_ns = clock_ns(cpu_clock) - cpu_before;
	assert(cpu_ns < UINT64_C(25000000)); /* Excludes a spinning idle worker. */
	printf("late_frame codec=%d epoch=%llu submissions=1 idle_worker_cpu_ns=%llu\n",
		codec, (unsigned long long)epoch, (unsigned long long)cpu_ns);
	video_broker_rdkx5_async_decoder_destroy(&w);
	atomic_store(&observed_condition, NULL);
}
static void late_failure(void) {
	video_broker_rdkx5_decoder_t d = {0};
	video_broker_rdkx5_async_decoder_t w;
	unsigned char input = 'x', pixels[12];
	vb_capture_origin origin = {8, 8};
	setup_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 71);
	setup_worker(&w, &d);
	await_parked(0);
	assert(video_broker_rdkx5_async_decoder_submit(&w, &input, 1, (vb_capture_origin){71, 1}) == 1);
	await_parked(1);
	decoder_runtime_output_budget(1);
	assert(await_result(&w, pixels, &origin) == -1);
	assert(origin.epoch == 0 && origin.sequence == 0);
	assert(decoder_runtime_submissions() == 1);
	video_broker_rdkx5_async_decoder_destroy(&w);
	atomic_store(&observed_condition, NULL);
}
static void cached_before_start(void) {
	video_broker_rdkx5_decoder_t d = {0};
	video_broker_rdkx5_async_decoder_t w;
	unsigned char input = 'b', pixels[12];
	vb_capture_origin origin;
	setup_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_H265, 72);
	assert(video_broker_rdkx5_decoder_decode(&d, &input, 1, pixels, 12, 0,
		(vb_capture_origin){72, 5}, &origin) == 0);
	decoder_runtime_output_budget(1);
	bool ready = false;
	uint64_t end = deadline();
	while (!ready && clock_ns(CLOCK_MONOTONIC) < end) {
		(void)pthread_mutex_lock(&d.output_mutex);
		ready = d.produced_generation == 1;
		(void)pthread_mutex_unlock(&d.output_mutex);
		if (!ready) tick();
	}
	assert(ready);
	setup_worker(&w, &d);
	assert(await_result(&w, pixels, &origin) == 1);
	assert(origin.epoch == 72 && origin.sequence == 5 && pixels[0] == 'b');
	assert(decoder_runtime_submissions() == 1);
	video_broker_rdkx5_async_decoder_destroy(&w);
	atomic_store(&observed_condition, NULL);
}
static void startup_failure_and_ownership(void) {
	video_broker_rdkx5_decoder_t d = {0};
	video_broker_rdkx5_async_decoder_t w, second;
	setup_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 73);
	assert(video_broker_rdkx5_async_decoder_init(&w, 1, 12, 2) == 0);
	atomic_store(&fail_create, true);
	assert(video_broker_rdkx5_async_decoder_start(&w, &d) == -1);
	assert(d.started && !d.async_owned && !w.thread_started && w.decoder == NULL);
	assert(video_broker_rdkx5_async_decoder_start(&w, &d) == 0);
	await_parked(0);
	assert(video_broker_rdkx5_async_decoder_init(&second, 1, 12, 2) == 0);
	assert(video_broker_rdkx5_async_decoder_start(&second, &d) == -1);
	video_broker_rdkx5_async_decoder_destroy(&second);
	video_broker_rdkx5_async_decoder_request_stop(&w);
	uint64_t end = deadline();
	while (video_broker_rdkx5_async_decoder_reap(&w) == 0 && clock_ns(CLOCK_MONOTONIC) < end) tick();
	assert(video_broker_rdkx5_async_decoder_reap(&w) == 1);
	video_broker_rdkx5_async_decoder_destroy(&w);
	atomic_store(&observed_condition, NULL);
}

static void failed_decoder_async_retirement(void) {
	video_broker_rdkx5_decoder_t d = {0};
	video_broker_rdkx5_async_decoder_t w;
	unsigned char input = 'x', pixels[12];
	vb_capture_origin origin;
	setup_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 74);
	assert(video_broker_rdkx5_decoder_decode(&d, &input, 1, pixels, 12, 0,
		(vb_capture_origin){74, 1}, &origin) == 0);
	decoder_runtime_output_budget(1);
	bool failed = false;
	uint64_t end = deadline();
	while (!failed && clock_ns(CLOCK_MONOTONIC) < end) {
		(void)pthread_mutex_lock(&d.output_mutex);
		failed = d.output_error != 0;
		(void)pthread_mutex_unlock(&d.output_mutex);
		if (!failed) tick();
	}
	assert(failed);
	/* A failed trial still needs off-capture-thread teardown. Start means
	 * ownership transfer, not a new successful runtime-health attestation. */
	setup_worker(&w, &d);
	assert(await_result(&w, pixels, &origin) == -1);
	assert(origin.epoch == 0 && origin.sequence == 0);
	video_broker_rdkx5_async_decoder_destroy(&w);
	atomic_store(&observed_condition, NULL);
}
static void rapid_stop(void) {
	for (uint64_t i = 0; i < 24; ++i) {
		video_broker_rdkx5_decoder_t d = {0};
		video_broker_rdkx5_async_decoder_t w;
		unsigned char input = 'a', pixels[12];
		vb_capture_origin origin = {1, 1};
		uint64_t replacements;
		setup_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 100 + i);
		setup_worker(&w, &d);
		assert(video_broker_rdkx5_async_decoder_submit(&w, &input, 1,
			(vb_capture_origin){100 + i, 1}) == 1);
		if (i & 1) decoder_runtime_output_budget(1);
		video_broker_rdkx5_async_decoder_request_stop(&w);
		assert(video_broker_rdkx5_async_decoder_poll(&w, pixels, 12, &replacements, &origin) == -1);
		assert(origin.epoch == 0 && origin.sequence == 0);
		video_broker_rdkx5_async_decoder_destroy(&w);
		atomic_store(&observed_condition, NULL);
	}
}

static void fixture_context_isolation(void) {
	video_broker_rdkx5_decoder_t d = {0};
	unsigned char input = 'a', pixels[12];
	vb_capture_origin origin;
	setup_decoder(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 80);
	assert(video_broker_rdkx5_decoder_decode(&d, &input, 1, pixels, 12, 0,
		(vb_capture_origin){80, 1}, &origin) == 0);
	assert(decoder_runtime_submissions() == 1);
	video_broker_rdkx5_decoder_close(&d);
	/* Do not call test reset: a newly initialized vendor context owns no queued
	 * buffers from the closed context. This stub models one active instance. */
	assert(video_broker_rdkx5_decoder_open(&d, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 4, 2, 81) == 0);
	assert(decoder_runtime_submissions() == 0);
	video_broker_rdkx5_decoder_close(&d);
	atomic_store(&observed_condition, NULL);
}
int main(void) {
	fixture_context_isolation();
	late_frame(VIDEO_BROKER_RDKX5_CODEC_H264, 41);
	late_frame(VIDEO_BROKER_RDKX5_CODEC_H265, 42);
	late_frame(VIDEO_BROKER_RDKX5_CODEC_MJPEG, 43);
	late_frame(VIDEO_BROKER_RDKX5_CODEC_MJPEG, 0);
	late_failure(); cached_before_start(); startup_failure_and_ownership();
	failed_decoder_async_retirement(); rapid_stop();
	puts("decoder notification late/error/cached/spurious/start-failure/stop PASS");
	return 0;
}
