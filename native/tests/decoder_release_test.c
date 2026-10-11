/* Faulted SDK release must retain ownership and prevent in-process reuse.
 * Each quarantined execution ends in its own child; no test clears poison or
 * frees memory still owned by the simulated SDK just to satisfy a leak check. */
#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <errno.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "rdkx5_decoder_runtime.h"

extern void decoder_runtime_teardown_failures(int, int, int);
extern int decoder_runtime_teardown_calls(int);
extern void decoder_runtime_open_failures(int, int);
extern int decoder_runtime_open_calls(int);

/* Fault only the chosen join, not the output thread's unrelated join. */
static pthread_t fault_join_target;
static atomic_bool fault_join_enabled;
static atomic_int fault_join_calls;
extern int __real_pthread_join(pthread_t, void **);
int __wrap_pthread_join(pthread_t thread, void **result) {
	if (atomic_load(&fault_join_enabled) && pthread_equal(thread, fault_join_target)) {
		atomic_fetch_add(&fault_join_calls, 1);
		return EINVAL;
	}
	return __real_pthread_join(thread, result);
}

static void fail_join(pthread_t target) {
	fault_join_target = target;
	atomic_store(&fault_join_enabled, true);
}

static void direct_failure(int codec, int stage) {
	video_broker_rdkx5_decoder_t decoder = {0};
	assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 17) == 0);
	decoder_runtime_teardown_failures(stage == 1 ? -71 : 0, stage == 2 ? -72 : 0, 0);
	assert(video_broker_rdkx5_decoder_close(&decoder) < 0);
	assert(decoder.initialized); /* The old close incorrectly clears this. */
	assert(decoder.started == (stage == 1));
	assert(decoder_runtime_teardown_calls(0) == 1);
	assert(decoder_runtime_teardown_calls(1) == (stage == 2));
	assert(video_broker_rdkx5_decoder_close(&decoder) < 0);
	assert(decoder_runtime_teardown_calls(0) == 1);
	assert(decoder_runtime_teardown_calls(1) == (stage == 2));
	assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 18) < 0);
	assert(video_broker_rdkx5_decoder_resources_quarantined());
	video_broker_rdkx5_decoder_t other = {0};
	assert(video_broker_rdkx5_decoder_open(&other, codec, 4, 2, 19) < 0);
	unsigned char nv12[12], input = 'a';
	vb_capture_origin origin = {17, 1};
	assert(video_broker_rdkx5_decoder_poll(&decoder, nv12, sizeof nv12, &origin) < 0);
	assert(origin.epoch == 0 && origin.sequence == 0);
	assert(video_broker_rdkx5_decoder_decode(&decoder, &input, 1, nv12, sizeof nv12, 1,
		(vb_capture_origin){17, 1}, &origin) < 0);
}

static void async_failure(int codec, int stage) {
	video_broker_rdkx5_decoder_t decoder = {0};
	video_broker_rdkx5_async_decoder_t worker = {0};
	assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 17) == 0);
	assert(video_broker_rdkx5_async_decoder_init(&worker, 1024, 12, 1) == 0);
	assert(video_broker_rdkx5_async_decoder_start(&worker, &decoder) == 0);
	decoder_runtime_teardown_failures(stage == 1 ? -71 : 0, stage == 2 ? -72 : 0, 0);
	video_broker_rdkx5_async_decoder_request_stop(&worker);
	int result = 0;
	for (int i = 0; i < 200 && result == 0; ++i) {
		struct timespec delay = {0, 10000000};
		nanosleep(&delay, NULL);
		result = video_broker_rdkx5_async_decoder_reap(&worker);
	}
	assert(result < 0 && !worker.thread_started && worker.cleanup_failed);
	assert(worker.decoder == &decoder && decoder.initialized);
	assert(video_broker_rdkx5_async_decoder_reap(&worker) < 0);
	assert(video_broker_rdkx5_async_decoder_destroy(&worker) < 0);
	assert(video_broker_rdkx5_async_decoder_start(&worker, &decoder) < 0);
	assert(video_broker_rdkx5_async_decoder_init(&worker, 1024, 12, 1) < 0);
	assert(worker.decoder == &decoder && worker.cleanup_failed);
	assert(decoder_runtime_teardown_calls(0) == 1);
	assert(decoder_runtime_teardown_calls(1) == (stage == 2));
}

static void partial_open_failure(int codec, int unused) {
	(void)unused;
	video_broker_rdkx5_decoder_t decoder = {0};
	decoder_runtime_teardown_failures(0, -72, -73);
	assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 17) < 0);
	assert(decoder.initialized && !decoder.started && decoder.close_error == -72);
	assert(video_broker_rdkx5_decoder_close(&decoder) < 0);
	assert(decoder_runtime_teardown_calls(0) == 0);
	assert(decoder_runtime_teardown_calls(1) == 1);
	assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 18) < 0);
}

static void initialization_failure(int codec, int stage) {
	video_broker_rdkx5_decoder_t decoder = {0};
	/* Failed initialize has no qualified partial-allocation receipt. Failed
	 * configure owns an initialized context and must report release failure. */
	decoder_runtime_open_failures(stage == 1 ? -81 : 0, stage == 2 ? -82 : 0);
	decoder_runtime_teardown_failures(0, -83, 0);
	assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 17) < 0);
	assert(decoder.close_error == (stage == 1 ? -81 : -83));
	assert(decoder.initialized == (stage == 2));
	assert(!decoder.started);
	assert(video_broker_rdkx5_decoder_resources_quarantined());
	assert(decoder_runtime_open_calls(0) == 1);
	assert(decoder_runtime_open_calls(1) == (stage == 2));
	assert(decoder_runtime_teardown_calls(0) == 0);
	assert(decoder_runtime_teardown_calls(1) == (stage == 2));
	assert(video_broker_rdkx5_decoder_close(&decoder) < 0);
	assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 18) < 0);
	assert(decoder_runtime_open_calls(0) == 1);
	assert(decoder_runtime_teardown_calls(1) == (stage == 2));
}

static void configure_failure_released(int codec, int unused) {
	(void)unused;
	video_broker_rdkx5_decoder_t decoder = {0};
	decoder_runtime_open_failures(0, -82);
	assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 17) < 0);
	assert(!decoder.initialized && !decoder.started && decoder.close_error == 0);
	assert(decoder_runtime_teardown_calls(0) == 0);
	assert(decoder_runtime_teardown_calls(1) == 1);
	assert(!video_broker_rdkx5_decoder_resources_quarantined());
	decoder_runtime_open_failures(0, 0);
	assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 18) == 0);
	assert(video_broker_rdkx5_decoder_close(&decoder) == 0);
	assert(decoder_runtime_open_calls(0) == 2);
	assert(decoder_runtime_open_calls(1) == 2);
	assert(decoder_runtime_teardown_calls(1) == 2);
}

static void trial_failure(int codec, int stage) {
	video_broker_rdkx5_mjpeg_trial_t trial = {0};
	assert(video_broker_rdkx5_mjpeg_trial_init(&trial, 1024, 4, 2) == 0);
	assert(video_broker_rdkx5_decoder_open(&trial.decoder, codec, 4, 2, 17) == 0);
	decoder_runtime_teardown_failures(stage == 1 ? -71 : 0, stage == 2 ? -72 : 0, 0);
	assert(video_broker_rdkx5_mjpeg_trial_destroy(&trial) < 0);
	assert(trial.initialized && trial.decoder.initialized);
	assert(video_broker_rdkx5_mjpeg_trial_recycle(&trial) < 0);
	assert(video_broker_rdkx5_mjpeg_trial_destroy(&trial) < 0);
	assert(video_broker_rdkx5_mjpeg_trial_init(&trial, 1024, 4, 2) < 0);
	assert(trial.decoder.initialized);
	assert(decoder_runtime_teardown_calls(0) == 1);
	assert(decoder_runtime_teardown_calls(1) == (stage == 2));
}

static void successful_reuse(int codec, int unused) {
	(void)unused;
	video_broker_rdkx5_decoder_t decoder = {0};
	for (int cycle = 0; cycle < 3; ++cycle) {
		assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 17 + cycle) == 0);
		unsigned char *owned = decoder.latest_frame;
		assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 99) < 0);
		assert(decoder.latest_frame == owned && decoder.captures.epoch == (uint64_t)(17 + cycle));
		assert(video_broker_rdkx5_decoder_close(&decoder) == 0);
		assert(!decoder.initialized && !decoder.started && decoder.latest_frame == NULL);
		assert(video_broker_rdkx5_decoder_close(&decoder) == 0);
	}
	assert(decoder_runtime_teardown_calls(0) == 3 && decoder_runtime_teardown_calls(1) == 3);
	assert(!video_broker_rdkx5_decoder_resources_quarantined());
}

static void join_failure(int codec, int stage) {
	video_broker_rdkx5_decoder_t decoder = {0};
	video_broker_rdkx5_async_decoder_t worker = {0};
	video_broker_rdkx5_mjpeg_trial_t trial = {0};
	if (stage == 3) {
		unsigned char input = 'a';
		struct timespec now;
		assert(clock_gettime(CLOCK_REALTIME, &now) == 0);
		assert(video_broker_rdkx5_mjpeg_trial_init(&trial, 1024, 4, 2) == 0);
		assert(video_broker_rdkx5_mjpeg_trial_start(&trial, &input, 1, 19, 5,
			(int64_t)now.tv_sec * 1000 + now.tv_nsec / 1000000 + 5000) == 0);
		fail_join(trial.thread);
		assert(video_broker_rdkx5_mjpeg_trial_destroy(&trial) < 0);
		assert(trial.join_error == EINVAL && trial.initialized);
		assert(video_broker_rdkx5_mjpeg_trial_destroy(&trial) < 0);
	} else {
		assert(video_broker_rdkx5_decoder_open(&decoder, codec, 4, 2, 17) == 0);
		if (stage == 1) {
			fail_join(decoder.output_thread);
			assert(video_broker_rdkx5_decoder_close(&decoder) < 0);
			assert(video_broker_rdkx5_decoder_close(&decoder) < 0);
			assert(decoder_runtime_teardown_calls(0) == 0);
			assert(decoder_runtime_teardown_calls(1) == 0);
		} else {
			assert(video_broker_rdkx5_async_decoder_init(&worker, 1024, 12, 1) == 0);
			assert(video_broker_rdkx5_async_decoder_start(&worker, &decoder) == 0);
			fail_join(worker.thread);
			assert(video_broker_rdkx5_async_decoder_destroy(&worker) < 0);
			assert(worker.join_error == EINVAL && worker.initialized);
			assert(video_broker_rdkx5_async_decoder_destroy(&worker) < 0);
			assert(video_broker_rdkx5_async_decoder_reap(&worker) < 0);
		}
	}
	assert(atomic_load(&fault_join_calls) == 1);
	assert(video_broker_rdkx5_decoder_resources_quarantined());
	/* A deliberately unjoined thread can still use these stack objects. Exit
	 * while their lifetimes are valid, not after returning to run_case. */
	_exit(0);
}

static void run_case(void (*test)(int, int), int codec, int stage) {
	pid_t pid = fork();
	assert(pid >= 0);
	if (pid == 0) {
		test(codec, stage);
		_exit(0);
	}
	int status;
	assert(waitpid(pid, &status, 0) == pid);
	assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

int main(void) {
	for (int codec = 1; codec <= 3; ++codec) {
		run_case(successful_reuse, codec, 0);
		run_case(partial_open_failure, codec, 0);
		run_case(configure_failure_released, codec, 0);
		for (int stage = 1; stage <= 2; ++stage) run_case(initialization_failure, codec, stage);
		for (int stage = 1; stage <= 3; ++stage) run_case(join_failure, codec, stage);
		for (int stage = 1; stage <= 2; ++stage) {
			run_case(direct_failure, codec, stage);
			run_case(async_failure, codec, stage);
			run_case(trial_failure, codec, stage);
		}
	}
	puts("DECODER_RELEASE_FAILURES_PASS");
	return 0;
}
