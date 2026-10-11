#ifndef VIDEO_BROKER_WORKER_FANOUT_PROTOCOL_H
#define VIDEO_BROKER_WORKER_FANOUT_PROTOCOL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

enum {
	VIDEO_BROKER_WORKER_FANOUT_MAX_LINE_BYTES = 4096,
	VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES = 3,
	VIDEO_BROKER_WORKER_FANOUT_ID_BYTES = 129,
};

typedef enum {
	VIDEO_BROKER_WORKER_ROLE_INVALID = -1,
	VIDEO_BROKER_WORKER_ROLE_ANALYSIS = 0,
	VIDEO_BROKER_WORKER_ROLE_ARCHIVE = 1,
	VIDEO_BROKER_WORKER_ROLE_LIVE = 2,
} video_broker_worker_role_t;

typedef enum {
	VIDEO_BROKER_WORKER_ACTION_INVALID = 0,
	VIDEO_BROKER_WORKER_ACTION_ATTACH,
	VIDEO_BROKER_WORKER_ACTION_DETACH,
	VIDEO_BROKER_WORKER_ACTION_REPLACE,
} video_broker_worker_action_t;

typedef enum {
	VIDEO_BROKER_WORKER_PHASE_INVALID = 0,
	VIDEO_BROKER_WORKER_PHASE_PREPARE,
	VIDEO_BROKER_WORKER_PHASE_WARMUP,
	VIDEO_BROKER_WORKER_PHASE_ADMIT,
	VIDEO_BROKER_WORKER_PHASE_FIRST_FRAME,
	VIDEO_BROKER_WORKER_PHASE_CUTOVER,
	VIDEO_BROKER_WORKER_PHASE_VERIFY,
	VIDEO_BROKER_WORKER_PHASE_ROLLBACK,
	VIDEO_BROKER_WORKER_PHASE_ABORT,
	VIDEO_BROKER_WORKER_PHASE_RETIRE,
} video_broker_worker_phase_t;

typedef enum {
	VIDEO_BROKER_WORKER_EVENT_INVALID = 0,
	VIDEO_BROKER_WORKER_EVENT_PREPARED,
	VIDEO_BROKER_WORKER_EVENT_WARMED,
	VIDEO_BROKER_WORKER_EVENT_ADMITTED,
	VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME,
	VIDEO_BROKER_WORKER_EVENT_CUTOVER,
	VIDEO_BROKER_WORKER_EVENT_VERIFIED,
	VIDEO_BROKER_WORKER_EVENT_ROLLED_BACK,
	VIDEO_BROKER_WORKER_EVENT_ABORTED,
	VIDEO_BROKER_WORKER_EVENT_RETIRED,
	VIDEO_BROKER_WORKER_EVENT_REJECTED,
} video_broker_worker_event_t;

typedef struct {
	char codec[5];
	char pixel_format[5];
	int width;
	int height;
	int fps;
	int bitrate_kbps;
	char content[10];
	char rate_control[5];
} video_broker_worker_branch_profile_t;

typedef struct {
	video_broker_worker_role_t role;
	video_broker_worker_action_t action;
	bool profile_present;
	video_broker_worker_branch_profile_t profile;
} video_broker_worker_branch_change_t;

typedef struct {
	char transaction_id[VIDEO_BROKER_WORKER_FANOUT_ID_BYTES];
	uint64_t sequence;
	char source_id[VIDEO_BROKER_WORKER_FANOUT_ID_BYTES];
	int capture_pid;
	uint64_t media_epoch;
	video_broker_worker_phase_t phase;
	video_broker_worker_branch_change_t changes[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	size_t change_count;
} video_broker_worker_fanout_command_t;

typedef struct {
	video_broker_worker_role_t role;
	uint64_t branch_epoch;
	uint64_t frame_id;
	bool codec_bootstrap_complete;
	char runtime_mode[32];
} video_broker_worker_branch_receipt_t;

typedef struct {
	char transaction_id[VIDEO_BROKER_WORKER_FANOUT_ID_BYTES];
	uint64_t sequence;
	char source_id[VIDEO_BROKER_WORKER_FANOUT_ID_BYTES];
	int capture_pid;
	uint64_t media_epoch;
	video_broker_worker_event_t event;
	video_broker_worker_branch_receipt_t branches[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	size_t branch_count;
	char error_code[65];
} video_broker_worker_fanout_receipt_t;

enum {
	VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK = 0,
	VIDEO_BROKER_WORKER_FANOUT_CALLBACK_PENDING = 1,
	VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED = -1,
	VIDEO_BROKER_WORKER_FANOUT_CALLBACK_TIMEOUT = -2,
};

enum {
	VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY = 0,
	VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_PENDING = 1,
};

typedef struct {
	int (*prepare)(
		void *context,
		const video_broker_worker_branch_change_t *changes,
		size_t change_count);
	int (*warmup)(void *context);
	int (*admit)(void *context);
	int (*first_frame)(
		void *context,
		video_broker_worker_branch_receipt_t *branches,
		size_t *branch_count);
	int (*cutover)(void *context);
	int (*verify)(void *context);
	int (*rollback)(void *context);
	int (*abort)(void *context);
	int (*retire)(void *context);
} video_broker_worker_fanout_callbacks_t;

/* The generic controller owns protocol sequencing and process fences. Backend
 * callbacks own only their already-admitted branch resources and publication
 * swap, keeping the same transaction contract reusable by HBN and V4L2. */
typedef struct {
	char source_id[VIDEO_BROKER_WORKER_FANOUT_ID_BYTES];
	int capture_pid;
	uint64_t media_epoch;
	uint64_t last_sequence;
	bool transaction_active;
	char transaction_id[VIDEO_BROKER_WORKER_FANOUT_ID_BYTES];
	video_broker_worker_event_t stage;
	video_broker_worker_branch_change_t changes[VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES];
	size_t change_count;
	bool phase_pending;
	video_broker_worker_fanout_command_t pending_command;
	video_broker_worker_fanout_callbacks_t callbacks;
	void *callback_context;
} video_broker_worker_fanout_controller_t;

/* Decode one complete JSON line, including its trailing LF. The decoder is
 * intentionally dependency-free and accepts only the current broker command
 * schema; unknown/duplicate fields, escapes and trailing data fail closed. */
int video_broker_worker_fanout_decode_command(
	const char *line,
	size_t line_size,
	video_broker_worker_fanout_command_t *command);

void video_broker_worker_fanout_receipt_from_command(
	video_broker_worker_fanout_receipt_t *receipt,
	const video_broker_worker_fanout_command_t *command);

int video_broker_worker_fanout_write_receipt(
	FILE *output,
	const video_broker_worker_fanout_receipt_t *receipt);

/* True only when the current transaction activated a candidate generation for
 * the role at cutover. A post-cutover failure of such a role is recoverable by
 * transaction rollback even when that source had no local incumbent branch. */
bool video_broker_worker_fanout_role_activated(
	const video_broker_worker_branch_change_t *changes,
	size_t change_count,
	video_broker_worker_role_t role);

int video_broker_worker_fanout_controller_init(
	video_broker_worker_fanout_controller_t *controller,
	const char *source_id,
	int capture_pid,
	uint64_t media_epoch,
	video_broker_worker_fanout_callbacks_t callbacks,
	void *callback_context);

int video_broker_worker_fanout_controller_handle(
	video_broker_worker_fanout_controller_t *controller,
	const video_broker_worker_fanout_command_t *command,
	video_broker_worker_fanout_receipt_t *receipt);

/* Resume a backend phase that returned CALLBACK_PENDING after the capture loop
 * has advanced. No new command or sequence is consumed. */
int video_broker_worker_fanout_controller_resume(
	video_broker_worker_fanout_controller_t *controller,
	video_broker_worker_fanout_receipt_t *receipt);

const char *video_broker_worker_role_name(video_broker_worker_role_t role);
const char *video_broker_worker_phase_name(video_broker_worker_phase_t phase);
const char *video_broker_worker_event_name(video_broker_worker_event_t event);

#endif
