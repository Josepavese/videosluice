#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "../worker_fanout_protocol.h"

static int decode(const char *line, video_broker_worker_fanout_command_t *command) {
	return video_broker_worker_fanout_decode_command(line, strlen(line), command);
}

static void expect_rejected(const char *line) {
	video_broker_worker_fanout_command_t command;
	assert(decode(line, &command) != 0);
}

static void test_prepare_all_roles(void) {
	const char *line =
		"{\"phase\":\"prepare\",\"source_id\":\"camera-1\","
		"\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"media_epoch\":9,\"capture_pid\":77,\"sequence\":41,"
		"\"transaction_id\":\"switch-1\",\"changes\":["
		"{\"action\":\"replace\",\"role\":\"analysis\",\"profile\":{"
		"\"fps\":20,\"height\":360,\"pixel_format\":\"nv12\",\"width\":640}},"
		"{\"role\":\"archive\",\"action\":\"replace\",\"profile\":{"
		"\"codec\":\"h265\",\"width\":640,\"height\":360,\"fps\":15,"
		"\"bitrate_kbps\":750,\"content\":\"adaptive\",\"rate_control\":\"avbr\"}},"
		"{\"role\":\"live\",\"action\":\"replace\",\"profile\":{"
		"\"codec\":\"h265\",\"width\":1280,\"height\":720,\"fps\":20,"
		"\"bitrate_kbps\":4096,\"content\":\"processed\",\"rate_control\":\"cbr\"}}]}\n";
	video_broker_worker_fanout_command_t command;
	assert(decode(line, &command) == 0);
	assert(strcmp(command.transaction_id, "switch-1") == 0);
	assert(strcmp(command.source_id, "camera-1") == 0);
	assert(command.sequence == 41 && command.capture_pid == 77 && command.media_epoch == 9);
	assert(command.phase == VIDEO_BROKER_WORKER_PHASE_PREPARE && command.change_count == 3);
	assert(command.changes[0].role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS);
	assert(command.changes[1].role == VIDEO_BROKER_WORKER_ROLE_ARCHIVE);
	assert(command.changes[2].role == VIDEO_BROKER_WORKER_ROLE_LIVE);
	assert(command.changes[0].profile.width == 640);
	assert(strcmp(command.changes[1].profile.rate_control, "avbr") == 0);
	assert(strcmp(command.changes[2].profile.codec, "h265") == 0);
}

static void test_phase_without_changes(void) {
	const char *line =
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"switch-1\",\"sequence\":42,"
		"\"source_id\":\"camera-1\",\"capture_pid\":77,"
		"\"media_epoch\":9,\"phase\":\"warmup\"}\n";
	video_broker_worker_fanout_command_t command;
	assert(decode(line, &command) == 0);
	assert(command.phase == VIDEO_BROKER_WORKER_PHASE_WARMUP);
	assert(command.change_count == 0);
}

static void test_fail_closed_inputs(void) {
	const char *valid_detach =
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"tx\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"prepare\","
		"\"changes\":[{\"role\":\"analysis\",\"action\":\"detach\"}]}\n";
	video_broker_worker_fanout_command_t command;
	assert(decode(valid_detach, &command) == 0);

	expect_rejected(
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"tx\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"warmup\"}\n");
	expect_rejected(
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"tx\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"warmup\",\"legacy\":true}\n");
	expect_rejected(
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"tx\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"prepare\","
		"\"changes\":[{\"role\":\"live\",\"action\":\"detach\"},"
		"{\"role\":\"analysis\",\"action\":\"detach\"}]}\n");
	expect_rejected(
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"tx\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"prepare\","
		"\"changes\":[{\"role\":\"analysis\",\"action\":\"attach\",\"profile\":{"
		"\"pixel_format\":\"nv12\",\"width\":641,\"height\":360,\"fps\":20}}]}\n");
	expect_rejected(
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"tx\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"prepare\","
		"\"changes\":[{\"role\":\"live\",\"action\":\"attach\",\"profile\":{"
		"\"codec\":\"h265\",\"width\":1280,\"height\":720,\"fps\":20,"
		"\"bitrate_kbps\":4096,\"content\":\"processed\",\"rate_control\":\"avbr\"}}]}\n");
	expect_rejected(
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"tx\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"warmup\","
		"\"changes\":[{\"role\":\"analysis\",\"action\":\"detach\"}]}\n");
	expect_rejected(
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"tx\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"warmup\","
		"\"changes\":[]}\n");
	expect_rejected(
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"tx\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"warmup\"}");
	expect_rejected(
		"{\"schema\":\"video-broker.worker-fanout-command.v1\",\r"
		"\"transaction_id\":\"tx\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"warmup\"}\n");
	expect_rejected(
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"t\\u0078\",\"sequence\":1,\"source_id\":\"cam\","
		"\"capture_pid\":7,\"media_epoch\":1,\"phase\":\"warmup\"}\n");
}

static void test_receipts(void) {
	video_broker_worker_fanout_command_t command;
	video_broker_worker_fanout_receipt_t receipt;
	FILE *stream;
	char output[1024];
	size_t size;
	const char *line =
		"{\"schema\":\"video-broker.worker-fanout-command.v1\","
		"\"transaction_id\":\"switch-1\",\"sequence\":4,"
		"\"source_id\":\"camera-1\",\"capture_pid\":77,"
		"\"media_epoch\":9,\"phase\":\"first_frame\"}\n";
	assert(decode(line, &command) == 0);
	video_broker_worker_fanout_receipt_from_command(&receipt, &command);
	receipt.event = VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME;
	receipt.branch_count = 2;
	receipt.branches[0] = (video_broker_worker_branch_receipt_t){
		.role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS,
		.branch_epoch = 2,
		.frame_id = 8,
		.codec_bootstrap_complete = false,
	};
	strcpy(receipt.branches[0].runtime_mode, "");
	receipt.branches[1] = (video_broker_worker_branch_receipt_t){
		.role = VIDEO_BROKER_WORKER_ROLE_LIVE,
		.branch_epoch = 3,
		.frame_id = 9,
		.codec_bootstrap_complete = true,
	};
	strcpy(receipt.branches[1].runtime_mode, "persistent_encoder_epoch");
	stream = tmpfile();
	assert(stream != NULL);
	assert(video_broker_worker_fanout_write_receipt(stream, &receipt) == 0);
	rewind(stream);
	size = fread(output, 1, sizeof(output) - 1, stream);
	assert(!ferror(stream));
	output[size] = '\0';
	assert(strcmp(output,
		"{\"schema\":\"video-broker.worker-fanout-receipt.v1\","
		"\"transaction_id\":\"switch-1\",\"sequence\":4,"
		"\"source_id\":\"camera-1\",\"capture_pid\":77,\"media_epoch\":9,"
		"\"event\":\"first_frame\",\"branches\":["
		"{\"role\":\"analysis\",\"branch_epoch\":2,\"frame_id\":8,"
		"\"codec_bootstrap\":\"not_applicable\",\"runtime_mode\":\"\"},"
		"{\"role\":\"live\",\"branch_epoch\":3,\"frame_id\":9,"
		"\"codec_bootstrap\":\"complete\","
		"\"runtime_mode\":\"persistent_encoder_epoch\"}]}\n") == 0);
	fclose(stream);

	video_broker_worker_fanout_receipt_from_command(&receipt, &command);
	receipt.event = VIDEO_BROKER_WORKER_EVENT_REJECTED;
	strcpy(receipt.error_code, "invalid_fence");
	stream = tmpfile();
	assert(stream != NULL);
	assert(video_broker_worker_fanout_write_receipt(stream, &receipt) == 0);
	fclose(stream);

	receipt.error_code[0] = 'I';
	assert(video_broker_worker_fanout_write_receipt(stderr, &receipt) != 0);
	video_broker_worker_fanout_receipt_from_command(&receipt, &command);
	receipt.event = VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME;
	receipt.branch_count = 1;
	receipt.branches[0] = (video_broker_worker_branch_receipt_t){
		.role = (video_broker_worker_role_t)3,
		.branch_epoch = 1,
		.frame_id = 1,
		.codec_bootstrap_complete = true,
	};
	assert(video_broker_worker_fanout_write_receipt(stderr, &receipt) != 0);
}

typedef struct {
	int prepare_calls;
	int warmup_calls;
	int admit_calls;
	int first_frame_calls;
	int cutover_calls;
	int verify_calls;
	int rollback_calls;
	int abort_calls;
	int retire_calls;
	int pending_first_frame_calls;
	int result;
} fake_backend_t;

static int fake_prepare(
	void *opaque,
	const video_broker_worker_branch_change_t *changes,
	size_t change_count) {
	fake_backend_t *backend = opaque;
	backend->prepare_calls++;
	assert(changes != NULL && change_count == 2);
	return backend->result;
}

#define FAKE_CALLBACK(NAME, FIELD) \
	static int NAME(void *opaque) { \
		fake_backend_t *backend = opaque; \
		backend->FIELD++; \
		return backend->result; \
	}

FAKE_CALLBACK(fake_warmup, warmup_calls)
FAKE_CALLBACK(fake_admit, admit_calls)
FAKE_CALLBACK(fake_cutover, cutover_calls)
FAKE_CALLBACK(fake_verify, verify_calls)
FAKE_CALLBACK(fake_rollback, rollback_calls)
FAKE_CALLBACK(fake_abort, abort_calls)
FAKE_CALLBACK(fake_retire, retire_calls)

static int fake_first_frame(
	void *opaque,
	video_broker_worker_branch_receipt_t *branches,
	size_t *branch_count) {
	fake_backend_t *backend = opaque;
	backend->first_frame_calls++;
	if (backend->result != 0) return backend->result;
	if (backend->pending_first_frame_calls > 0) {
		backend->pending_first_frame_calls--;
		return VIDEO_BROKER_WORKER_FANOUT_CALLBACK_PENDING;
	}
	assert(*branch_count >= 2);
	branches[0] = (video_broker_worker_branch_receipt_t){
		.role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS,
		.branch_epoch = 2,
		.frame_id = 11,
		.codec_bootstrap_complete = false,
	};
	strcpy(branches[0].runtime_mode, "");
	branches[1] = (video_broker_worker_branch_receipt_t){
		.role = VIDEO_BROKER_WORKER_ROLE_LIVE,
		.branch_epoch = 3,
		.frame_id = 12,
		.codec_bootstrap_complete = true,
	};
	strcpy(branches[1].runtime_mode, "persistent_encoder_epoch");
	*branch_count = 2;
	return 0;
}

static video_broker_worker_fanout_callbacks_t fake_callbacks(void) {
	return (video_broker_worker_fanout_callbacks_t){
		.prepare = fake_prepare,
		.warmup = fake_warmup,
		.admit = fake_admit,
		.first_frame = fake_first_frame,
		.cutover = fake_cutover,
		.verify = fake_verify,
		.rollback = fake_rollback,
		.abort = fake_abort,
		.retire = fake_retire,
	};
}

static video_broker_worker_fanout_command_t controller_command(
	video_broker_worker_phase_t phase,
	uint64_t sequence) {
	video_broker_worker_fanout_command_t command;
	memset(&command, 0, sizeof(command));
	strcpy(command.transaction_id, "switch-1");
	strcpy(command.source_id, "camera-1");
	command.capture_pid = 77;
	command.media_epoch = 9;
	command.sequence = sequence;
	command.phase = phase;
	return command;
}

static void expect_event(
	video_broker_worker_fanout_controller_t *controller,
	video_broker_worker_fanout_command_t *command,
	video_broker_worker_event_t event) {
	video_broker_worker_fanout_receipt_t receipt;
	video_broker_worker_fanout_controller_handle(controller, command, &receipt);
	assert(receipt.event == event);
}

static void test_controller_lifecycle_and_fences(void) {
	fake_backend_t backend = {0};
	video_broker_worker_fanout_controller_t controller;
	video_broker_worker_fanout_command_t command;
	video_broker_worker_fanout_receipt_t receipt;
	assert(video_broker_worker_fanout_controller_init(
		&controller, "camera-1", 77, 9, fake_callbacks(), &backend) == 0);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_PREPARE, 1);
	command.change_count = 2;
	command.changes[0].role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	command.changes[0].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	command.changes[1].role = VIDEO_BROKER_WORKER_ROLE_LIVE;
	command.changes[1].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_PREPARED);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_WARMUP, 2);
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_WARMED);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_ADMIT, 3);
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_ADMITTED);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_FIRST_FRAME, 4);
	video_broker_worker_fanout_controller_handle(&controller, &command, &receipt);
	assert(receipt.event == VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME && receipt.branch_count == 2);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_CUTOVER, 5);
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_CUTOVER);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_VERIFY, 6);
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_VERIFIED);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_RETIRE, 7);
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_RETIRED);
	assert(!controller.transaction_active && backend.prepare_calls == 1 &&
		backend.warmup_calls == 1 && backend.admit_calls == 1 &&
		backend.first_frame_calls == 1 && backend.cutover_calls == 1 &&
		backend.verify_calls == 1 && backend.retire_calls == 1);

	command = controller_command(VIDEO_BROKER_WORKER_PHASE_PREPARE, 8);
	command.change_count = 2;
	command.changes[0].role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	command.changes[0].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	command.changes[1].role = VIDEO_BROKER_WORKER_ROLE_LIVE;
	command.changes[1].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_PREPARED);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_ROLLBACK, 9);
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_ROLLED_BACK);
	assert(backend.rollback_calls == 1 && !controller.transaction_active);

	command = controller_command(VIDEO_BROKER_WORKER_PHASE_PREPARE, 10);
	command.change_count = 2;
	command.changes[0].role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	command.changes[0].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	command.changes[1].role = VIDEO_BROKER_WORKER_ROLE_LIVE;
	command.changes[1].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	strcpy(command.source_id, "camera-2");
	video_broker_worker_fanout_controller_handle(&controller, &command, &receipt);
	assert(receipt.event == VIDEO_BROKER_WORKER_EVENT_REJECTED &&
		strcmp(receipt.error_code, "invalid_fence") == 0 && controller.last_sequence == 9);
	strcpy(command.source_id, "camera-1");
	command.sequence = 9;
	video_broker_worker_fanout_controller_handle(&controller, &command, &receipt);
	assert(receipt.event == VIDEO_BROKER_WORKER_EVENT_REJECTED &&
		strcmp(receipt.error_code, "stale_sequence") == 0);
}

static void test_controller_failures_are_canonical(void) {
	fake_backend_t backend = {.result = VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED};
	video_broker_worker_fanout_controller_t controller;
	video_broker_worker_fanout_command_t command =
		controller_command(VIDEO_BROKER_WORKER_PHASE_PREPARE, 1);
	video_broker_worker_fanout_receipt_t receipt;
	assert(video_broker_worker_fanout_controller_init(
		&controller, "camera-1", 77, 9, fake_callbacks(), &backend) == 0);
	command.change_count = 2;
	command.changes[0].role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	command.changes[0].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	command.changes[1].role = VIDEO_BROKER_WORKER_ROLE_LIVE;
	command.changes[1].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	video_broker_worker_fanout_controller_handle(&controller, &command, &receipt);
	assert(receipt.event == VIDEO_BROKER_WORKER_EVENT_REJECTED &&
		strcmp(receipt.error_code, "prepare_failed") == 0 &&
		!controller.transaction_active);
}

static void test_controller_pending_phase_resumes_without_new_sequence(void) {
	fake_backend_t backend = {.pending_first_frame_calls = 2};
	video_broker_worker_fanout_controller_t controller;
	video_broker_worker_fanout_command_t command;
	video_broker_worker_fanout_receipt_t receipt;
	assert(video_broker_worker_fanout_controller_init(
		&controller, "camera-1", 77, 9, fake_callbacks(), &backend) == 0);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_PREPARE, 1);
	command.change_count = 2;
	command.changes[0].role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	command.changes[0].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	command.changes[1].role = VIDEO_BROKER_WORKER_ROLE_LIVE;
	command.changes[1].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_PREPARED);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_WARMUP, 2);
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_WARMED);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_ADMIT, 3);
	expect_event(&controller, &command, VIDEO_BROKER_WORKER_EVENT_ADMITTED);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_FIRST_FRAME, 4);
	assert(video_broker_worker_fanout_controller_handle(
		&controller, &command, &receipt) == VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_PENDING);
	assert(controller.phase_pending && controller.last_sequence == 4 &&
		receipt.event == VIDEO_BROKER_WORKER_EVENT_INVALID);
	command = controller_command(VIDEO_BROKER_WORKER_PHASE_CUTOVER, 5);
	assert(video_broker_worker_fanout_controller_handle(
		&controller, &command, &receipt) == VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY);
	assert(receipt.event == VIDEO_BROKER_WORKER_EVENT_REJECTED &&
		strcmp(receipt.error_code, "phase_pending") == 0 && controller.last_sequence == 4);
	assert(video_broker_worker_fanout_controller_resume(
		&controller, &receipt) == VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_PENDING);
	assert(controller.phase_pending && controller.last_sequence == 4);
	assert(video_broker_worker_fanout_controller_resume(
		&controller, &receipt) == VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY);
	assert(!controller.phase_pending && receipt.event == VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME &&
		receipt.sequence == 4 && receipt.branch_count == 2 && backend.first_frame_calls == 3);
}

static void test_post_cutover_role_activation_policy(void) {
	video_broker_worker_branch_change_t changes[3] = {0};
	changes[0].role = VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	changes[0].action = VIDEO_BROKER_WORKER_ACTION_DETACH;
	changes[1].role = VIDEO_BROKER_WORKER_ROLE_ARCHIVE;
	changes[1].action = VIDEO_BROKER_WORKER_ACTION_REPLACE;
	changes[2].role = VIDEO_BROKER_WORKER_ROLE_LIVE;
	changes[2].action = VIDEO_BROKER_WORKER_ACTION_ATTACH;
	assert(!video_broker_worker_fanout_role_activated(
		changes, 3, VIDEO_BROKER_WORKER_ROLE_ANALYSIS));
	assert(video_broker_worker_fanout_role_activated(
		changes, 3, VIDEO_BROKER_WORKER_ROLE_ARCHIVE));
	assert(video_broker_worker_fanout_role_activated(
		changes, 3, VIDEO_BROKER_WORKER_ROLE_LIVE));
	assert(!video_broker_worker_fanout_role_activated(
		changes, 2, VIDEO_BROKER_WORKER_ROLE_LIVE));
	assert(!video_broker_worker_fanout_role_activated(
		NULL, 3, VIDEO_BROKER_WORKER_ROLE_LIVE));
}

int main(void) {
	test_prepare_all_roles();
	test_phase_without_changes();
	test_fail_closed_inputs();
	test_receipts();
	test_controller_lifecycle_and_fences();
	test_controller_failures_are_canonical();
	test_controller_pending_phase_resumes_without_new_sequence();
	test_post_cutover_role_activation_policy();
	puts("worker fanout protocol tests passed");
	return 0;
}
