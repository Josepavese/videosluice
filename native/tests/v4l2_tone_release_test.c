/* Compose the real worker with the deterministic OpenCL API boundary. */
#define _POSIX_C_SOURCE 200809L
#define main tone_owner_test_main
#include "nv12_tone_release_test.c"
#undef main
#include "../nv12_quality_measure.h"
static unsigned quality_measure_calls;
static video_broker_nv12_quality_measure_t counted_quality_measure(
	const unsigned char *frame, int width, int height, int stride
) {
	quality_measure_calls++;
	return video_broker_nv12_quality_measure(frame, width, height, stride);
}
#define video_broker_nv12_quality_measure counted_quality_measure
#define main video_broker_v4l2_worker_main
#include "../rdkx5_v4l2_mux_bridge.c"
#undef main
#undef video_broker_nv12_quality_measure

int video_broker_rdkx5_persistent_encoder_destroy(video_broker_rdkx5_persistent_encoder_t *encoder) {
	(void)encoder; abort(); /* These fixtures must never create vendor codecs. */
}
bool video_broker_rdkx5_encoder_resources_quarantined(void) { return false; }
/* AArch64 sanitizer instrumentation may retain address-taken fanout callbacks
 * even when the hardware main is discarded. Resolve unreachable encoder paths
 * with traps; accidentally reaching the vendor boundary must fail the test. */
int video_broker_rdkx5_persistent_encoder_init(video_broker_rdkx5_persistent_encoder_t *p,
	video_broker_rdkx5_encoder_codec_t codec, video_broker_rdkx5_rate_control_profile_t rate,
	int index, int width, int height, int fps, int bitrate) {
	(void)p; (void)codec; (void)rate; (void)index; (void)width; (void)height; (void)fps; (void)bitrate; abort();
}
int video_broker_rdkx5_persistent_encoder_set_control(video_broker_rdkx5_persistent_encoder_t *p,
	bool active, int fps, int bitrate) {
	(void)p; (void)active; (void)fps; (void)bitrate; abort();
}

static void retained_role(void) {
	for (int initialized = 0; initialized <= 1; initialized++) {
		reset(); fail_finish = 1;
		v4l2_role_runtime_t role = {0};
		unsigned char *frame = malloc(64);
		uint16_t *axis = malloc(64);
		assert(frame != NULL && axis != NULL);
		role.frame = frame;
		role.candidate_tone_initialized = initialized != 0;
		role.candidate_tone_mapper.x_left = axis;
		role.candidate_tone_accelerator = full_owner();
		role.candidate_tone_accelerator.host_frame = frame;
		role.candidate_tone_accelerator.commands_pending = true;
		assert(v4l2_role_destroy(&role) == -1);
		assert(role.frame == frame && role.candidate_tone_mapper.x_left == axis);
		assert(role.candidate_tone_initialized == (initialized != 0));
		frame[0] = 37; axis[0] = 9; /* ASan proves storage was not freed. */
		assert(releases == 0 && finishes == 1);
		assert(v4l2_role_destroy(&role) == -1 && finishes == 1);
		assert(video_broker_worker_exit_status(0, !video_broker_tone_accelerator_resources_quarantined()) == 65);
		/* No actual GPU references exist in this failure-injection fixture. */
		free(frame); free(axis);
	}
}

static void cutover_refuses_uncertain_release(void) {
	reset(); fail_release = 3;
	v4l2_fanout_backend_t backend = {0};
	v4l2_role_runtime_t active = {0};
	const int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	backend.change_count = 1;
	backend.changes[0].role = role;
	backend.changes[0].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	backend.active[role] = &active;
	backend.candidate_present[role] = true;
	backend.candidate[role].candidate_tone_initialized = true;
	backend.candidate[role].candidate_tone_accelerator = full_owner();
	assert(v4l2_fanout_cutover(&backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED);
	assert(!backend.cutover_completed && !backend.active_present[role]);
	assert(backend.candidate_present[role] && backend.candidate[role].candidate_tone_initialized);
	assert(releases == 3);
}

static void shared_surface_transaction_lifetime(void) {
	int (*operations[])(void *) = {v4l2_fanout_cutover, v4l2_fanout_rollback, v4l2_fanout_retire};
	for (size_t index = 0; index < sizeof(operations) / sizeof(operations[0]); ++index) {
		reset(); fail_finish = 1;
		v4l2_fanout_backend_t backend = {0};
		v4l2_role_runtime_t active = {0};
		unsigned char *frame = malloc(64);
		assert(frame != NULL);
		active.frame = frame;
		video_broker_tone_accelerator_t shared = full_owner();
		shared.host_frame = frame;
		shared.commands_pending = true;
		backend.shared_tone_accelerator = &shared;
		backend.change_count = 1;
		backend.cutover_completed = index != 0;
		backend.changes[0].role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
		backend.active[VIDEO_BROKER_WORKER_ROLE_ANALYSIS] = &active;
		backend.active_present[VIDEO_BROKER_WORKER_ROLE_ANALYSIS] = true;
		assert(operations[index](&backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED);
		assert(active.frame == frame && backend.active_present[VIDEO_BROKER_WORKER_ROLE_ANALYSIS]);
		assert(backend.cutover_completed == (index != 0) && shared.host_frame == frame);
		assert(finishes == 1 && releases == 0);
		frame[0] = 42; /* No role storage may be freed after uncertain drain. */
		free(frame); /* Fixture owns no actual driver resource. */
	}
	reset();
	video_broker_tone_accelerator_t shared = full_owner();
	v4l2_fanout_backend_t backend = {.shared_tone_accelerator = &shared};
	assert(v4l2_fanout_release_shared_surface(&backend, "fixture") == 0);
	assert(shared.host_frame == NULL && shared.frame_buffer == NULL && finishes == 0);
	assert(video_broker_tone_accelerator_destroy(&shared) == 0);
}

static void tone_telemetry_reads_only_when_reported(void) {
	const int width = 96, height = 64;
	unsigned char input[96 * 64 * 3 / 2];
	unsigned char frames[2][sizeof(input)];
	bridge_config_t cfg = {.quality_monitor_sample_stride = 8};
	reset();
	for (size_t i = 0; i < sizeof(input); i++) input[i] = (unsigned char)(i % 180 + 20);
	for (int report = 0; report <= 1; report++) {
		video_broker_nv12_tone_mapper_t mapper;
		video_broker_tone_accelerator_t accelerator = {
			.requested = VIDEO_BROKER_TONE_ACCELERATOR_CPU,
			.selected = VIDEO_BROKER_TONE_ACCELERATOR_CPU, .initialized = true};
		runtime_metrics_t metrics = {0};
		uint64_t last_log = report ? 0 : monotonic_us();
		video_broker_nv12_tone_mapper_init(&mapper, VIDEO_BROKER_TONE_AUTO,
			128, 12, 4, 3, 8, 20, 2.4, 0.2, 0.35, true);
		memcpy(frames[report], input, sizeof(input));
		quality_measure_calls = 0;
		assert(tone_map_processed_frame(&mapper, &accelerator, &cfg, &metrics,
			frames[report], width, height, &last_log) == 0);
		/* Raw noise protection is per-frame; post-tone quality is logging only. */
		assert(quality_measure_calls == (report ? 2U : 1U));
		assert(metrics.tone_mapping_ops == 1 && mapper.signal_informative);
		/* Do not reuse a previous informative decision when the scene goes dark. */
		unsigned char dark[sizeof(input)] = {0};
		quality_measure_calls = 0;
		assert(tone_map_processed_frame(&mapper, &accelerator, &cfg, &metrics,
			dark, width, height, &last_log) == 0);
		assert(quality_measure_calls == 1 && !mapper.signal_informative);
		assert(metrics.tone_mapping_ops == 2);
		for (size_t i = 0; i < sizeof(dark); i++) assert(dark[i] == 0);
		video_broker_nv12_tone_mapper_destroy(&mapper);
	}
	assert(memcmp(frames[0], frames[1], sizeof(input)) == 0);
	puts("OK_V4L2_TONE_TELEMETRY_NO_UNUSED_PIXEL_SCAN");
}

int main(void) {
	retained_role(); cutover_refuses_uncertain_release(); shared_surface_transaction_lifetime();
	tone_telemetry_reads_only_when_reported();
	puts("OK_V4L2_TONE_RELEASE_RETENTION_AND_CUTOVER_FAILURE");
	return 0;
}
