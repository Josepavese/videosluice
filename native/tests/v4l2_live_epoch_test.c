/* Exercise the real worker's publication boundary without opening hardware. */
#define main video_broker_v4l2_worker_main
#include "../rdkx5_v4l2_mux_bridge.c"
#undef main

#include <assert.h>
#include <sys/wait.h>

static uint64_t read_epoch(FILE *wire) {
	unsigned char packet[13];
	uint64_t epoch = 0;
	assert(fread(packet, 1, sizeof(packet), wire) == sizeof(packet));
	assert(packet[0] == kLiveEncoderEpochStreamID);
	assert(packet[1] == 0 && packet[2] == 0 && packet[3] == 0 && packet[4] == 8);
	for (size_t index = 5; index < sizeof(packet); ++index) epoch = (epoch << 8U) | packet[index];
	return epoch;
}

static void test_replacement_then_resume(void) {
	v4l2_fanout_backend_t backend = {0};
	v4l2_role_runtime_t first = {0}, replacement = {0};
	FILE *wire = tmpfile();
	assert(wire != NULL);
	first.branch_epoch = 1;
	replacement.branch_epoch = 2;
	backend.active[VIDEO_BROKER_WORKER_ROLE_LIVE] = &first;
	assert(v4l2_publish_live_encoder_epoch(&backend, wire, 1, 4096) == 0);
	assert(v4l2_publish_live_encoder_epoch(&backend, wire, 2, 4096) == 0);
	/* Rate changes and additional clients cannot manufacture an activation. */
	assert(v4l2_publish_live_encoder_epoch(&backend, wire, 2, 2457) == 0);
	assert(first.active_bitrate_kbps == 2457);
	backend.active[VIDEO_BROKER_WORKER_ROLE_LIVE] = &replacement;
	assert(v4l2_publish_live_encoder_epoch(&backend, wire, 1, 4096) == 0);
	assert(v4l2_publish_live_encoder_epoch(&backend, wire, 2, 4096) == 0);
	assert(first.encoder_local_epoch == 2 && replacement.encoder_local_epoch == 2);
	rewind(wire);
	assert(read_epoch(wire) == 1);
	assert(read_epoch(wire) == 2);
	assert(read_epoch(wire) == 3);
	assert(read_epoch(wire) == 4);
	assert(fgetc(wire) == EOF);
	fclose(wire);
}

/* Metadata-only fixtures must never allocate or destroy vendor hardware. */
static bool fault_release;
static bool encoder_quarantined;
static int destroy_calls;
int video_broker_rdkx5_persistent_encoder_destroy(video_broker_rdkx5_persistent_encoder_t *encoder) {
	if (fault_release) {
		assert(encoder != NULL);
		destroy_calls++;
		encoder->close_error = -77;
		encoder_quarantined = true;
		return -1;
	}
	abort();
}

bool video_broker_rdkx5_encoder_resources_quarantined(void) { return encoder_quarantined; }

typedef struct {
	v4l2_fanout_backend_t backend;
	bridge_config_t cfg;
	v4l2_capture_t capture;
	runtime_control_t control;
	v4l2_role_runtime_t roles[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	bool flags[7];
} fixture_t;

static void fixture_init(fixture_t *fixture, const char *codec) {
	v4l2_fanout_backend_t *backend = &fixture->backend;
	memset(fixture, 0, sizeof(*fixture));
	backend->cfg = &fixture->cfg;
	backend->capture = &fixture->capture;
	backend->control = &fixture->control;
	backend->direct_main_native = &fixture->flags[0];
	backend->archive_native_passthrough = &fixture->flags[1];
	backend->archive_encode_required = &fixture->flags[2];
	backend->main_uses_capture_surface = &fixture->flags[3];
	backend->archive_uses_capture_surface = &fixture->flags[4];
	backend->tone_map_capture_frame = &fixture->flags[5];
	backend->persistent_main_control = &fixture->flags[6];
	fixture->capture.width = 1920;
	fixture->capture.height = 1080;
	for (int role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS; role <= VIDEO_BROKER_WORKER_ROLE_LIVE; ++role) {
		backend->active[role] = &fixture->roles[role];
		fixture->roles[role].role = (video_broker_worker_role_t)role;
		fixture->roles[role].branch_epoch = 1;
	}
	v4l2_role_runtime_t *live = backend->active[VIDEO_BROKER_WORKER_ROLE_LIVE];
	strcpy(live->profile.codec, codec);
	live->profile.width = 1280;
	live->profile.height = 720;
	live->profile.fps = 30;
	live->profile.bitrate_kbps = 4096;
	live->delivery_active = true;
	backend->active_present[VIDEO_BROKER_WORKER_ROLE_LIVE] = true;
	v4l2_fanout_apply_active(backend);
}

static void stage_candidate(fixture_t *fixture, video_broker_worker_role_t role) {
	v4l2_fanout_backend_t *backend = &fixture->backend;
	assert(backend->change_count == 0);
	backend->change_count = 1;
	backend->changes[0].role = role;
	backend->changes[0].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	backend->candidate[role] = *backend->active[role];
	backend->candidate[role].branch_epoch = backend->active[role]->branch_epoch + 1;
	backend->candidate[role].encoder_local_epoch = 0;
	backend->candidate[role].frames = 1; /* Private warmup, not a public marker. */
	backend->candidate_present[role] = true;
}

static void test_real_cutover_rollback_hooks(const char *codec) {
	fixture_t fixture;
	fixture_init(&fixture, codec);
	v4l2_fanout_backend_t *backend = &fixture.backend;
	FILE *wire = tmpfile();
	assert(wire != NULL);
	assert(v4l2_publish_live_encoder_epoch(backend, wire, 1, 4096) == 0);
	stage_candidate(&fixture, VIDEO_BROKER_WORKER_ROLE_LIVE);
	assert(backend->live_epoch.value == 1 && !backend->live_epoch.boundary_pending);
	assert(v4l2_fanout_abort(backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	assert(v4l2_publish_live_encoder_epoch(backend, wire, 1, 4096) == 0);
	assert(backend->live_epoch.value == 1);
	stage_candidate(&fixture, VIDEO_BROKER_WORKER_ROLE_LIVE);
	assert(v4l2_fanout_cutover(backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	assert(backend->live_epoch.boundary_pending);
	assert(v4l2_publish_live_encoder_epoch(backend, wire, 1, 4096) == 0);
	assert(backend->live_epoch.value == 2);
	assert(v4l2_fanout_rollback(backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	assert(backend->live_epoch.boundary_pending);
	assert(v4l2_publish_live_encoder_epoch(backend, wire, 1, 4096) == 0);
	assert(backend->live_epoch.value == 3);
	stage_candidate(&fixture, VIDEO_BROKER_WORKER_ROLE_LIVE);
	assert(v4l2_fanout_cutover(backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	assert(v4l2_publish_live_encoder_epoch(backend, wire, 1, 4096) == 0);
	assert(backend->live_epoch.value == 4);
	/* Roll back and replace again before the incumbent publishes any frame.
	 * The new private branch/encoder counters equal the last candidate's. */
	assert(v4l2_fanout_rollback(backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	stage_candidate(&fixture, VIDEO_BROKER_WORKER_ROLE_LIVE);
	assert(v4l2_fanout_cutover(backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	assert(v4l2_publish_live_encoder_epoch(backend, wire, 1, 4096) == 0);
	assert(backend->live_epoch.value == 5);
	assert(v4l2_fanout_retire(backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	stage_candidate(&fixture, VIDEO_BROKER_WORKER_ROLE_ANALYSIS);
	assert(v4l2_fanout_cutover(backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	assert(!backend->live_epoch.boundary_pending);
	assert(v4l2_publish_live_encoder_epoch(backend, wire, 1, 4096) == 0);
	assert(v4l2_fanout_rollback(backend) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	assert(!backend->live_epoch.boundary_pending);
	assert(v4l2_publish_live_encoder_epoch(backend, wire, 1, 4096) == 0);
	assert(backend->live_epoch.value == 5);
	rewind(wire);
	for (uint64_t expected = 1; expected <= 5; ++expected) assert(read_epoch(wire) == expected);
	assert(fgetc(wire) == EOF);
	fclose(wire);
}

static void test_invalid_and_exhausted_clock(void) {
	video_broker_stream_epoch_t clock = {0}, before;
	assert(video_broker_stream_epoch_observe(NULL, 1, 1) == -1);
	assert(video_broker_stream_epoch_observe(&clock, 0, 1) == -1);
	assert(video_broker_stream_epoch_observe(&clock, 1, 0) == -1);
	assert(clock.value == 0);
	assert(video_broker_stream_epoch_observe(&clock, 1, 9) == 1);
	before = clock;
	assert(video_broker_stream_epoch_observe(&clock, 1, 8) == -1);
	assert(memcmp(&clock, &before, sizeof(clock)) == 0);
	clock.value = UINT64_MAX;
	before = clock;
	assert(video_broker_stream_epoch_observe(&clock, 1, 10) == -1);
	assert(memcmp(&clock, &before, sizeof(clock)) == 0);
	video_broker_stream_epoch_boundary(&clock);
	assert(video_broker_stream_epoch_observe(&clock, 2, 1) == -1);
	assert(clock.value == UINT64_MAX && clock.boundary_pending);
}

static void failed_release_keeps_role(int codec, int operation) {
	fixture_t fixture;
	fixture_init(&fixture, codec == 1 ? "h264" : "h265");
	stage_candidate(&fixture, VIDEO_BROKER_WORKER_ROLE_LIVE);
	v4l2_fanout_backend_t *backend = &fixture.backend;
	const int live = VIDEO_BROKER_WORKER_ROLE_LIVE;
	v4l2_role_runtime_t *target = &backend->candidate[live];
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	encoder.codec = (video_broker_rdkx5_encoder_codec_t)codec;
	if (operation == 3 || operation == 6) target = backend->active[live];
	if (operation == 4) {
		backend->incumbent_present[live] = true;
		target = &backend->incumbent[live];
	}
	target->encoder = &encoder; /* Must never be freed by failed role destroy. */
	target->encoder_initialized = operation != 0; /* Partial init is owned too. */
	target->role = VIDEO_BROKER_WORKER_ROLE_LIVE;
	target->branch_epoch = 23;
	target->frame = malloc(32); assert(target->frame);
	memset(target->frame,0xa5,32);
	unsigned char *frame = target->frame;
	fault_release = true;
	int result;
	switch (operation) {
	case 0: result = v4l2_role_destroy(target); break;
	case 1: result = v4l2_fanout_abort(backend); break;
	case 2: result = v4l2_fanout_rollback(backend); break;
	case 3: backend->cutover_completed = true; result = v4l2_fanout_rollback(backend); break;
	case 4: backend->cutover_completed = true; result = v4l2_fanout_retire(backend); break;
	case 5: result = v4l2_fanout_destroy(backend); break;
	default: backend->active_present[live] = false; result = v4l2_fanout_cutover(backend); break;
	}
	assert(result < 0 && destroy_calls == 1 && encoder_quarantined);
	assert(target->encoder == &encoder && target->frame == frame && target->branch_epoch == 23);
	assert(target->encoder_initialized == (operation != 0));
	for(int i=0;i<32;i++)assert(frame[i]==0xa5);
	assert(backend->change_count == 1);
	if(operation==3)assert(backend->active_present[live]);
	else if(operation==4)assert(backend->incumbent_present[live]);
	else assert(backend->candidate_present[live]);
	/* No SDK was loaded. Preserve the simulated uncertain owner until this
	 * isolated execution exits instead of manually clearing it for the test. */
	_exit(0);
}

static void test_quality_intent_does_not_pin_analysis(void) {
	bridge_config_t cfg = {0};
	v4l2_capture_t capture = {0};
	v4l2_fanout_backend_t backend = {0};
	video_broker_worker_branch_change_t change = {0};
	cfg.quality_monitor_enabled = true;
	cfg.fps = capture.fps = 30;
	capture.width = 1280;
	capture.height = 720;
	backend.cfg = &cfg;
	backend.capture = &capture;
	backend.active_present[VIDEO_BROKER_WORKER_ROLE_ANALYSIS] = true;
	backend.active_present[VIDEO_BROKER_WORKER_ROLE_ARCHIVE] = true;
	change.role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	change.action = VIDEO_BROKER_WORKER_ACTION_DETACH;
	assert(v4l2_fanout_prepare(&backend, &change, 1) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	assert(cfg.quality_monitor_enabled);
	v4l2_fanout_reset(&backend);
	backend.active_present[VIDEO_BROKER_WORKER_ROLE_ANALYSIS] = false;
	change.action = VIDEO_BROKER_WORKER_ACTION_ATTACH;
	change.profile.width = 640;
	change.profile.height = 360;
	change.profile.fps = 20;
	assert(v4l2_fanout_prepare(&backend, &change, 1) == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK);
	assert(cfg.quality_monitor_enabled);
}

int main(void) {
	test_quality_intent_does_not_pin_analysis();
	test_replacement_then_resume();
	test_real_cutover_rollback_hooks("h264");
	test_real_cutover_rollback_hooks("h265");
	test_invalid_and_exhausted_clock();
	for (int codec=1;codec<=2;codec++) for (int operation=0;operation<=6;operation++) {
		pid_t child=fork();assert(child>=0);
		if(child==0)failed_release_keeps_role(codec,operation);
		int status;assert(waitpid(child,&status,0)==child);
		assert(WIFEXITED(status)&&WEXITSTATUS(status)==0);
	}
	puts("OK_V4L2_LIVE_EPOCH_SURVIVES_ENCODER_REPLACEMENT");
	puts("OK_V4L2_ENCODER_FAILED_RELEASE_RETAINED");
	return 0;
}
