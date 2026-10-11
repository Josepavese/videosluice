/* Video-broker codec adapter contract test. */
#define _POSIX_C_SOURCE 200809L

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "mjpeg_decode_policy.h"
#include "rdkx5_decoder_runtime.h"

extern void decoder_runtime_enable_delayed_output(int enabled);
extern void decoder_runtime_set_output_absent(int enabled);
extern int decoder_runtime_low_latency_configured(void);
extern void decoder_runtime_set_input_delay_ms(int milliseconds);
extern void decoder_runtime_set_input_failure(int enabled);
extern void decoder_runtime_set_input_blocked(int enabled);
extern int decoder_runtime_input_block_entered(void);
extern void decoder_runtime_set_teardown_blocked(int enabled);
extern int decoder_runtime_teardown_block_entered(void);
extern void decoder_runtime_buffer_contract_fault(int fault);

static uint64_t monotonic_ms(void) {
	struct timespec now;
	assert(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	return (uint64_t)now.tv_sec * 1000U + (uint64_t)now.tv_nsec / 1000000U;
}

static int64_t realtime_ms(void) {
	struct timespec now;
	assert(clock_gettime(CLOCK_REALTIME, &now) == 0);
	return (int64_t)now.tv_sec * 1000 + (int64_t)now.tv_nsec / 1000000;
}

static void delay_ms(long milliseconds) {
	struct timespec delay;
	delay.tv_sec = milliseconds / 1000L;
	delay.tv_nsec = (milliseconds % 1000L) * 1000000L;
	assert(nanosleep(&delay, NULL) == 0);
}

int main(void) {
	video_broker_rdkx5_decoder_t decoder = {0};
	video_broker_rdkx5_mjpeg_trial_t trial;
	video_broker_rdkx5_async_decoder_t async_decoder;
	unsigned char nv12[12];
	unsigned char access_unit[] = {'a'};
	vb_capture_origin output_origin;
	uint64_t started_ms;
	uint64_t elapsed_ms;
	uint64_t deadline_ms;
	int rc;
	video_broker_mjpeg_accelerator_t parsed_accelerator;
	bool parsed_bool;
	uint64_t next_output_due_us = 0;
	memset(&trial, 0, sizeof(trial));
	memset(&async_decoder, 0, sizeof(async_decoder));

	/* The current CLI contract is explicit: omitting either decoder selector is
	 * rejected, and an asynchronous JPU fault can use CPU only under AUTO with
	 * software fallback enabled. Strict JPU remains fail-closed. */
	assert(video_broker_parse_mjpeg_accelerator("auto", &parsed_accelerator));
	assert(parsed_accelerator == VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO);
	assert(video_broker_parse_mjpeg_accelerator("jpu", &parsed_accelerator));
	assert(parsed_accelerator == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU);
	assert(video_broker_parse_mjpeg_accelerator("cpu", &parsed_accelerator));
	assert(parsed_accelerator == VIDEO_BROKER_MJPEG_ACCELERATOR_CPU);
	parsed_accelerator = VIDEO_BROKER_MJPEG_ACCELERATOR_JPU;
	assert(!video_broker_parse_mjpeg_accelerator("software", &parsed_accelerator));
	assert(!video_broker_parse_mjpeg_accelerator("AUTO", &parsed_accelerator));
	assert(!video_broker_parse_mjpeg_accelerator("jup", &parsed_accelerator));
	assert(!video_broker_parse_mjpeg_accelerator("", &parsed_accelerator));
	assert(!video_broker_parse_mjpeg_accelerator(NULL, &parsed_accelerator));
	assert(parsed_accelerator == VIDEO_BROKER_MJPEG_ACCELERATOR_JPU);
	assert(video_broker_parse_required_bool("true", &parsed_bool) && parsed_bool);
	assert(video_broker_parse_required_bool("false", &parsed_bool) && !parsed_bool);
	assert(!video_broker_parse_required_bool("TRUE", &parsed_bool));
	assert(!video_broker_parse_required_bool("1", &parsed_bool));
	assert(!video_broker_parse_required_bool("yes", &parsed_bool));
	assert(!video_broker_parse_required_bool("truthy", &parsed_bool));
	assert(!video_broker_parse_required_bool("", &parsed_bool));
	assert(!video_broker_parse_required_bool(NULL, &parsed_bool));
	assert(!video_broker_mjpeg_decode_policy_valid(
		VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO,
		false,
		VIDEO_BROKER_MJPEG_ACCELERATOR_JPU,
		true,
		true));
	assert(!video_broker_mjpeg_decode_policy_valid(
		VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO,
		true,
		VIDEO_BROKER_MJPEG_ACCELERATOR_JPU,
		false,
		true));
	assert(video_broker_mjpeg_decode_policy_valid(
		VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO,
		true,
		VIDEO_BROKER_MJPEG_ACCELERATOR_CPU,
		true,
		true));
	assert(video_broker_mjpeg_async_failure_action(
		VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO,
		true) == VIDEO_BROKER_MJPEG_ASYNC_FAILURE_FALLBACK_CPU);
	assert(video_broker_mjpeg_async_failure_action(
		VIDEO_BROKER_MJPEG_ACCELERATOR_JPU,
		false) == VIDEO_BROKER_MJPEG_ASYNC_FAILURE_REJECT);
	assert(video_broker_mjpeg_async_failure_action(
		VIDEO_BROKER_MJPEG_ACCELERATOR_JPU,
		true) == VIDEO_BROKER_MJPEG_ASYNC_FAILURE_REJECT);
	assert(!video_broker_mjpeg_decoder_output_stalled(true, 100, 199, 100));
	assert(video_broker_mjpeg_decoder_output_stalled(true, 100, 200, 100));
	assert(!video_broker_mjpeg_decoder_output_stalled(false, 100, 1000, 100));
	/* A long camera input gap is not decoder starvation. The first returning
	 * access unit starts a new pending-output window at its submission time. */
	assert(!video_broker_mjpeg_decoder_output_stalled(true, 0, 10000, 100));
	assert(!video_broker_mjpeg_decoder_output_stalled(true, 10000, 10000, 100));
	assert(!video_broker_mjpeg_decoder_output_stalled(true, 10000, 10099, 100));
	/* Latest-frame replacements do not renew the first pending submission. */
	assert(video_broker_mjpeg_decoder_output_stalled(true, 10000, 10100, 100));

	/* Async completion cadence caps outputs without pre-dropping input. A
	 * decoder slower than the target publishes every completed frame; a faster
	 * decoder stays phase-bounded to the requested rate. */
	assert(!video_broker_mjpeg_async_output_due(NULL, 15, 1000000));
	assert(!video_broker_mjpeg_async_output_due(&next_output_due_us, 0, 1000000));
	assert(video_broker_mjpeg_async_output_due(&next_output_due_us, 15, 1000000));
	assert(!video_broker_mjpeg_async_output_due(&next_output_due_us, 15, 1040000));
	assert(video_broker_mjpeg_async_output_due(&next_output_due_us, 15, 1080000));
	assert(video_broker_mjpeg_async_output_due(&next_output_due_us, 15, 1160000));
	next_output_due_us = 0;
	assert(video_broker_mjpeg_async_output_due(&next_output_due_us, 15, 2000000));
	assert(video_broker_mjpeg_async_output_due(&next_output_due_us, 15, 2100000));
	assert(video_broker_mjpeg_async_output_due(&next_output_due_us, 15, 2200000));

	for (int codec = VIDEO_BROKER_RDKX5_CODEC_H264; codec <= VIDEO_BROKER_RDKX5_CODEC_MJPEG; ++codec) {
		for (int fault = 1; fault <= 3; ++fault) {
			decoder_runtime_buffer_contract_fault(fault);
			assert(video_broker_rdkx5_decoder_open(&decoder, (video_broker_rdkx5_codec_t)codec, 4, 2, 0) < 0);
			assert(!decoder.initialized && !decoder.started);
			assert(!video_broker_rdkx5_decoder_resources_quarantined());
		}
	}
	decoder_runtime_buffer_contract_fault(0);
	assert(video_broker_rdkx5_decoder_open(
		&decoder, VIDEO_BROKER_RDKX5_CODEC_H265, 4, 2, 0) == 0);
	assert(decoder.context.video_dec_params.bitstream_buf_count == 6);
	assert(decoder.context.video_dec_params.frame_buf_count == 6);
	assert(decoder.context.video_dec_params.bitstream_buf_size == 1024);
	assert(decoder.context.video_dec_params.pix_fmt == MC_PIXEL_FORMAT_NV12);
	assert(!decoder.context.video_dec_params.external_bitstream_buf);
	assert(decoder_runtime_low_latency_configured() == 1);
	decoder_runtime_enable_delayed_output(1);

	/*
	 * The output stub deliberately holds every decoded frame for 50 ms. Eight
	 * synchronous submit-and-receive calls would therefore require at least
	 * 400 ms. The decoder input side must remain independent of that output delay.
	 */
	started_ms = monotonic_ms();
	for (int i = 0; i < 8; ++i) {
		rc = video_broker_rdkx5_decoder_decode(
			&decoder, access_unit, sizeof(access_unit), nv12, sizeof(nv12), 2,
			(vb_capture_origin){0, 0}, &output_origin);
		assert(rc >= 0);
	}
	elapsed_ms = monotonic_ms() - started_ms;
	assert(elapsed_ms < 200);

	/*
	 * The dedicated output thread eventually publishes a decoded NV12 frame.
	 * Poll against a monotonic deadline instead of assuming a scheduler wake-up
	 * at one exact instant: under the full contract suite the output thread may
	 * legitimately spend its first 100 ms in the empty-queue timeout before the
	 * stub adds its deliberate 50 ms delay.
	 */
	deadline_ms = monotonic_ms() + 1000;
	rc = 0;
	while (rc == 0 && monotonic_ms() < deadline_ms) {
		delay_ms(10);
		rc = video_broker_rdkx5_decoder_decode(
			&decoder, access_unit, sizeof(access_unit), nv12, sizeof(nv12), 2,
			(vb_capture_origin){0, 0}, &output_origin);
	}
	assert(rc == 1);
	assert(output_origin.epoch == 0 && output_origin.sequence == 0);
	for (size_t i = 0; i < 8; ++i) {
		assert(nv12[i] == 80);
	}
	for (size_t i = 8; i < sizeof(nv12); ++i) {
		assert(nv12[i] == 128);
	}

	video_broker_rdkx5_decoder_close(&decoder);

	/* An accelerator that accepts input but publishes no output is stopped in the
	 * background at the admission boundary. CPU capture can continue in the same
	 * process/epoch while codec teardown is still blocked. */
	assert(video_broker_rdkx5_decoder_open(
		&decoder, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 4, 2, 0) == 0);
	assert(video_broker_rdkx5_async_decoder_init(
		&async_decoder, sizeof(access_unit), sizeof(nv12), 2) == 0);
	decoder_runtime_set_output_absent(1);
	assert(video_broker_rdkx5_async_decoder_start(&async_decoder, &decoder) == 0);
	{
		const int capture_pid = 7101;
		const uint64_t media_epoch = 91;
		uint64_t replacements = 0;
		int simulated_dqbuf_count = 0;
		int simulated_qbuf_count = 0;
		for (int frame = 0; frame < 16; ++frame) {
			simulated_dqbuf_count++;
			simulated_qbuf_count++;
			assert(video_broker_rdkx5_async_decoder_submit(
				&async_decoder, access_unit, sizeof(access_unit), (vb_capture_origin){0, 0}) > 0);
			assert(video_broker_rdkx5_async_decoder_poll(
				&async_decoder, nv12, sizeof(nv12), &replacements, &output_origin) == 0);
		}
		/* Once the camera is admitted, only an access unit that remains without
		 * output can trip the decoder watchdog. A camera input gap has no pending
		 * submission and is handled by the capture lifecycle instead. */
		assert(!video_broker_mjpeg_decoder_output_stalled(false, 100, 1000, 100));
		assert(!video_broker_mjpeg_decoder_output_stalled(true, 0, 1000, 100));
		assert(video_broker_mjpeg_decoder_output_stalled(true, 100, 200, 100));
		decoder_runtime_set_teardown_blocked(1);
		video_broker_rdkx5_async_decoder_request_stop(&async_decoder);
		deadline_ms = monotonic_ms() + 1000;
		while (!decoder_runtime_teardown_block_entered() && monotonic_ms() < deadline_ms) {
			delay_ms(1);
		}
		assert(decoder_runtime_teardown_block_entered());
		assert(video_broker_rdkx5_async_decoder_reap(&async_decoder) == 0);
		for (int frame = 0; frame < 12; ++frame) {
			simulated_dqbuf_count++;
			simulated_qbuf_count++;
		}
		assert(simulated_dqbuf_count == 28 && simulated_qbuf_count == 28);
		assert(capture_pid == 7101 && media_epoch == 91);
		decoder_runtime_set_teardown_blocked(0);
		deadline_ms = monotonic_ms() + 1000;
		while (video_broker_rdkx5_async_decoder_reap(&async_decoder) == 0 &&
		       monotonic_ms() < deadline_ms) {
			delay_ms(1);
		}
		assert(video_broker_rdkx5_async_decoder_reap(&async_decoder) == 1);
	}
	video_broker_rdkx5_async_decoder_destroy(&async_decoder);
	decoder_runtime_set_output_absent(0);
	video_broker_rdkx5_decoder_close(&decoder);

	/* Both compressed camera codecs use the same frame-sized low-latency contract. */
	assert(video_broker_rdkx5_decoder_open(
		&decoder, VIDEO_BROKER_RDKX5_CODEC_H264, 4, 2, 0) == 0);
	assert(decoder_runtime_low_latency_configured() == 1);
	video_broker_rdkx5_decoder_close(&decoder);

	/* RDK X5 exposes MJPEG through the same frame-sized decoder API (JPU). */
	assert(video_broker_rdkx5_decoder_open(
		&decoder, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 4, 2, 0) == 0);
	assert(decoder_runtime_low_latency_configured() == 1);
	video_broker_rdkx5_decoder_close(&decoder);

	/*
	 * A blocked active JPU runs behind a one-slot latest-frame worker. The
	 * capture side can DQBUF/copy/QBUF many frames while the vendor call owns one
	 * older copy; pending frames are replaced rather than accumulated.
	 */
	assert(video_broker_rdkx5_decoder_open(
		&decoder, VIDEO_BROKER_RDKX5_CODEC_MJPEG, 4, 2, 0) == 0);
	{
		unsigned char mmap_access_unit[] = {'a'};
		uint64_t replacements = 0;
		int simulated_dqbuf_count = 0;
		int simulated_qbuf_count = 0;
		assert(video_broker_rdkx5_async_decoder_init(
			&async_decoder, sizeof(mmap_access_unit), sizeof(nv12), 2) == 0);
		decoder_runtime_set_input_failure(1);
		decoder_runtime_set_input_blocked(1);
		assert(video_broker_rdkx5_async_decoder_start(&async_decoder, &decoder) == 0);
		assert(video_broker_rdkx5_async_decoder_submit(
			&async_decoder, mmap_access_unit, sizeof(mmap_access_unit), (vb_capture_origin){0, 0}) == 1);
		deadline_ms = monotonic_ms() + 1000;
		while (!decoder_runtime_input_block_entered() && monotonic_ms() < deadline_ms) {
			delay_ms(1);
		}
		assert(decoder_runtime_input_block_entered());
		for (int frame = 0; frame < 12; ++frame) {
			unsigned char owned_access_unit[sizeof(mmap_access_unit)];
			simulated_dqbuf_count++;
			memcpy(owned_access_unit, mmap_access_unit, sizeof(owned_access_unit));
			simulated_qbuf_count++;
			mmap_access_unit[0] = mmap_access_unit[0] == 'a' ? 'b' : 'a';
			assert(video_broker_rdkx5_async_decoder_submit(
				&async_decoder, owned_access_unit, sizeof(owned_access_unit), (vb_capture_origin){0, 0}) > 0);
		}
		assert(simulated_dqbuf_count == 12 && simulated_qbuf_count == 12);
		assert(video_broker_rdkx5_async_decoder_poll(
			&async_decoder, nv12, sizeof(nv12), &replacements, &output_origin) == 0);
		assert(replacements >= 11);
		decoder_runtime_set_teardown_blocked(1);
		decoder_runtime_set_input_blocked(0);
		deadline_ms = monotonic_ms() + 1000;
		rc = 0;
		while (rc == 0 && monotonic_ms() < deadline_ms) {
			rc = video_broker_rdkx5_async_decoder_poll(
				&async_decoder, nv12, sizeof(nv12), &replacements, &output_origin);
			delay_ms(1);
		}
		assert(rc == -1);
		assert(video_broker_mjpeg_async_failure_action(
			VIDEO_BROKER_MJPEG_ACCELERATOR_AUTO,
			true) == VIDEO_BROKER_MJPEG_ASYNC_FAILURE_FALLBACK_CPU);
		assert(video_broker_mjpeg_async_failure_action(
			VIDEO_BROKER_MJPEG_ACCELERATOR_JPU,
			false) == VIDEO_BROKER_MJPEG_ASYNC_FAILURE_REJECT);
		assert(replacements >= 8);
		deadline_ms = monotonic_ms() + 1000;
		while (!decoder_runtime_teardown_block_entered() && monotonic_ms() < deadline_ms) {
			delay_ms(1);
		}
		assert(decoder_runtime_teardown_block_entered());
		assert(video_broker_rdkx5_async_decoder_reap(&async_decoder) == 0);
		/* Once failure is published, the capture path has already selected CPU.
		 * Codec stop/deinit remains blocked on the worker while UVC keeps cycling. */
		for (int frame = 0; frame < 12; ++frame) {
			simulated_dqbuf_count++;
			simulated_qbuf_count++;
		}
		assert(simulated_dqbuf_count == 24 && simulated_qbuf_count == 24);
		decoder_runtime_set_teardown_blocked(0);
		deadline_ms = monotonic_ms() + 1000;
		while (video_broker_rdkx5_async_decoder_reap(&async_decoder) == 0 &&
		       monotonic_ms() < deadline_ms) {
			delay_ms(1);
		}
		assert(video_broker_rdkx5_async_decoder_reap(&async_decoder) == 1);
		video_broker_rdkx5_async_decoder_destroy(&async_decoder);
		decoder_runtime_set_input_failure(0);
	}
	video_broker_rdkx5_decoder_close(&decoder);

	/*
	 * A blocked vendor input call runs only on the copied-AU worker. Starting the
	 * trial must stay comfortably below one 30fps capture period, which lets the
	 * capture thread continue CPU decode and QBUF progress in the same PID/epoch.
	 * Mutating the caller buffer after start also proves the worker has no V4L2
	 * buffer alias.
	 */
	decoder_runtime_enable_delayed_output(0);
	assert(video_broker_rdkx5_mjpeg_trial_init(&trial, sizeof(access_unit), 4, 2) == 0);
	decoder_runtime_set_input_blocked(1);
	started_ms = monotonic_ms();
	assert(video_broker_rdkx5_mjpeg_trial_start(
		&trial, access_unit, sizeof(access_unit), 41, 250, realtime_ms() + 2000) == 0);
	elapsed_ms = monotonic_ms() - started_ms;
	assert(elapsed_ms < 25);
	deadline_ms = monotonic_ms() + 1000;
	while (!decoder_runtime_input_block_entered() && monotonic_ms() < deadline_ms) {
		delay_ms(1);
	}
	assert(decoder_runtime_input_block_entered());
	access_unit[0] = 'x';
	{
		video_broker_rdkx5_mjpeg_trial_result_t result;
		uint64_t token = 0;
		int64_t trial_deadline = 0;
		int64_t completed_at = 0;
		uint64_t simulated_cpu_frames = 0;
		assert(video_broker_rdkx5_mjpeg_trial_poll(
			&trial, &result, &token, &trial_deadline, &completed_at) == 0);
		for (int frame = 0; frame < 100; ++frame) {
			/* This is the capture thread's uninterrupted CPU/QBUF budget while
			 * the vendor input call is deterministically held. */
			simulated_cpu_frames++;
		}
		assert(simulated_cpu_frames >= 100);
		assert(video_broker_rdkx5_mjpeg_trial_poll(
			&trial, &result, &token, &trial_deadline, &completed_at) == 0);
		decoder_runtime_set_input_blocked(0);
		deadline_ms = monotonic_ms() + 1000;
		while (video_broker_rdkx5_mjpeg_trial_poll(
				&trial, &result, &token, &trial_deadline, &completed_at) == 0 &&
		       monotonic_ms() < deadline_ms) {
			delay_ms(1);
		}
		assert(result == VIDEO_BROKER_RDKX5_MJPEG_TRIAL_SUCCESS);
		assert(token == 41);
		assert(trial_deadline > realtime_ms());
		assert(completed_at > 0 && completed_at < trial_deadline);
		assert(video_broker_rdkx5_mjpeg_trial_accept(&trial) == 0);
		assert(video_broker_rdkx5_mjpeg_trial_decoder(&trial) != NULL);
		assert(video_broker_rdkx5_async_decoder_init(
			&async_decoder, sizeof(access_unit), sizeof(nv12), 2) == 0);
		decoder_runtime_set_teardown_blocked(1);
		assert(video_broker_rdkx5_mjpeg_trial_begin_async_reset(
			&trial, &async_decoder) == 0);
		deadline_ms = monotonic_ms() + 1000;
		while (!decoder_runtime_teardown_block_entered() && monotonic_ms() < deadline_ms) {
			delay_ms(1);
		}
		assert(decoder_runtime_teardown_block_entered());
		assert(video_broker_rdkx5_async_decoder_reap(&async_decoder) == 0);
		decoder_runtime_set_teardown_blocked(0);
		deadline_ms = monotonic_ms() + 1000;
		while (video_broker_rdkx5_async_decoder_reap(&async_decoder) == 0 &&
		       monotonic_ms() < deadline_ms) {
			delay_ms(1);
		}
		assert(video_broker_rdkx5_async_decoder_reap(&async_decoder) == 1);
		assert(video_broker_rdkx5_mjpeg_trial_recycle(&trial) == 0);
		assert(video_broker_rdkx5_mjpeg_trial_decoder(&trial) == NULL);
		video_broker_rdkx5_async_decoder_destroy(&async_decoder);
	}

	/* A real candidate frame completed after its absolute deadline is closed. */
	access_unit[0] = 'a';
	decoder_runtime_set_input_delay_ms(0);
	assert(video_broker_rdkx5_mjpeg_trial_start(
		&trial, access_unit, sizeof(access_unit), 42, 250, 1) == 0);
	{
		video_broker_rdkx5_mjpeg_trial_result_t result;
		uint64_t token = 0;
		int64_t trial_deadline = 0;
		int64_t completed_at = 0;
		deadline_ms = monotonic_ms() + 1000;
		while (video_broker_rdkx5_mjpeg_trial_poll(
				&trial, &result, &token, &trial_deadline, &completed_at) == 0 &&
		       monotonic_ms() < deadline_ms) {
			delay_ms(1);
		}
		assert(result == VIDEO_BROKER_RDKX5_MJPEG_TRIAL_DEADLINE_FAILED);
		assert(token == 42);
		assert(completed_at >= trial_deadline);
		assert(video_broker_rdkx5_mjpeg_trial_decoder(&trial) == NULL);
		assert(video_broker_rdkx5_mjpeg_trial_recycle(&trial) == 0);
	}
	video_broker_rdkx5_mjpeg_trial_destroy(&trial);
	return 0;
}
