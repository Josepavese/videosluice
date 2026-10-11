#include "worker_fanout_protocol.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <string.h>

#define WORKER_FANOUT_COMMAND_SCHEMA "video-broker.worker-fanout-command.v1"
#define WORKER_FANOUT_RECEIPT_SCHEMA "video-broker.worker-fanout-receipt.v1"

bool video_broker_worker_fanout_role_activated(
	const video_broker_worker_branch_change_t *changes,
	size_t change_count,
	video_broker_worker_role_t role) {
	if (changes == NULL || change_count == 0 ||
	    change_count > VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES ||
	    role < VIDEO_BROKER_WORKER_ROLE_ANALYSIS ||
	    role > VIDEO_BROKER_WORKER_ROLE_LIVE) {
		return false;
	}
	for (size_t index = 0; index < change_count; ++index) {
		if (changes[index].role == role) {
			return changes[index].action == VIDEO_BROKER_WORKER_ACTION_ATTACH ||
			       changes[index].action == VIDEO_BROKER_WORKER_ACTION_REPLACE;
		}
	}
	return false;
}

typedef struct {
	const char *next;
	const char *end;
} json_cursor_t;

static void skip_space(json_cursor_t *cursor) {
	while (cursor->next < cursor->end &&
	       (*cursor->next == ' ' || *cursor->next == '\t')) {
		cursor->next++;
	}
}

static bool take(json_cursor_t *cursor, char expected) {
	skip_space(cursor);
	if (cursor->next >= cursor->end || *cursor->next != expected) {
		return false;
	}
	cursor->next++;
	return true;
}

static bool parse_string(json_cursor_t *cursor, char *output, size_t capacity) {
	size_t length = 0;
	if (output == NULL || capacity == 0 || !take(cursor, '"')) {
		return false;
	}
	while (cursor->next < cursor->end && *cursor->next != '"') {
		unsigned char value = (unsigned char)*cursor->next++;
		/* Broker-generated command values are canonical ASCII tokens. Refusing
		 * escapes also prevents multiple spellings of object keys and IDs. */
		if (value < 0x20 || value >= 0x7f || value == '\\' || length + 1 >= capacity) {
			return false;
		}
		output[length++] = (char)value;
	}
	if (cursor->next >= cursor->end || *cursor->next != '"') {
		return false;
	}
	cursor->next++;
	output[length] = '\0';
	return true;
}

static bool parse_uint64(json_cursor_t *cursor, uint64_t *output) {
	uint64_t value = 0;
	const char *start;
	if (output == NULL) {
		return false;
	}
	skip_space(cursor);
	start = cursor->next;
	if (start >= cursor->end || !isdigit((unsigned char)*start)) {
		return false;
	}
	if (*start == '0' && start + 1 < cursor->end && isdigit((unsigned char)start[1])) {
		return false;
	}
	while (cursor->next < cursor->end && isdigit((unsigned char)*cursor->next)) {
		unsigned int digit = (unsigned int)(*cursor->next - '0');
		if (value > (UINT64_MAX - digit) / 10U) {
			return false;
		}
		value = value * 10U + digit;
		cursor->next++;
	}
	*output = value;
	return cursor->next > start;
}

static bool parse_positive_int(json_cursor_t *cursor, int *output) {
	uint64_t value;
	if (!parse_uint64(cursor, &value) || value == 0 || value > INT_MAX) {
		return false;
	}
	*output = (int)value;
	return true;
}

static bool canonical_id(const char *value) {
	size_t length;
	if (value == NULL) {
		return false;
	}
	length = strlen(value);
	if (length == 0 || length >= VIDEO_BROKER_WORKER_FANOUT_ID_BYTES ||
	    !((value[0] >= 'a' && value[0] <= 'z') ||
	      (value[0] >= '0' && value[0] <= '9'))) {
		return false;
	}
	for (size_t index = 1; index < length; ++index) {
		char c = value[index];
		if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
		      c == '.' || c == '_' || c == ':' || c == '-')) {
			return false;
		}
	}
	return true;
}

static bool canonical_error_code(const char *value) {
	size_t length;
	if (value == NULL) {
		return false;
	}
	length = strlen(value);
	if (length == 0 || length > 64 || value[0] < 'a' || value[0] > 'z') {
		return false;
	}
	for (size_t index = 1; index < length; ++index) {
		char c = value[index];
		if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) {
			return false;
		}
	}
	return true;
}

static video_broker_worker_role_t parse_role_name(const char *value) {
	if (strcmp(value, "analysis") == 0) {
		return VIDEO_BROKER_WORKER_ROLE_ANALYSIS;
	}
	if (strcmp(value, "archive") == 0) {
		return VIDEO_BROKER_WORKER_ROLE_ARCHIVE;
	}
	if (strcmp(value, "live") == 0) {
		return VIDEO_BROKER_WORKER_ROLE_LIVE;
	}
	return VIDEO_BROKER_WORKER_ROLE_INVALID;
}

static video_broker_worker_action_t parse_action_name(const char *value) {
	if (strcmp(value, "attach") == 0) {
		return VIDEO_BROKER_WORKER_ACTION_ATTACH;
	}
	if (strcmp(value, "detach") == 0) {
		return VIDEO_BROKER_WORKER_ACTION_DETACH;
	}
	if (strcmp(value, "replace") == 0) {
		return VIDEO_BROKER_WORKER_ACTION_REPLACE;
	}
	return VIDEO_BROKER_WORKER_ACTION_INVALID;
}

static video_broker_worker_phase_t parse_phase_name(const char *value) {
	if (strcmp(value, "prepare") == 0) return VIDEO_BROKER_WORKER_PHASE_PREPARE;
	if (strcmp(value, "warmup") == 0) return VIDEO_BROKER_WORKER_PHASE_WARMUP;
	if (strcmp(value, "admit") == 0) return VIDEO_BROKER_WORKER_PHASE_ADMIT;
	if (strcmp(value, "first_frame") == 0) return VIDEO_BROKER_WORKER_PHASE_FIRST_FRAME;
	if (strcmp(value, "cutover") == 0) return VIDEO_BROKER_WORKER_PHASE_CUTOVER;
	if (strcmp(value, "verify") == 0) return VIDEO_BROKER_WORKER_PHASE_VERIFY;
	if (strcmp(value, "rollback") == 0) return VIDEO_BROKER_WORKER_PHASE_ROLLBACK;
	if (strcmp(value, "abort") == 0) return VIDEO_BROKER_WORKER_PHASE_ABORT;
	if (strcmp(value, "retire") == 0) return VIDEO_BROKER_WORKER_PHASE_RETIRE;
	return VIDEO_BROKER_WORKER_PHASE_INVALID;
}

const char *video_broker_worker_role_name(video_broker_worker_role_t role) {
	switch (role) {
	case VIDEO_BROKER_WORKER_ROLE_ANALYSIS: return "analysis";
	case VIDEO_BROKER_WORKER_ROLE_ARCHIVE: return "archive";
	case VIDEO_BROKER_WORKER_ROLE_LIVE: return "live";
	default: return "";
	}
}

const char *video_broker_worker_phase_name(video_broker_worker_phase_t phase) {
	switch (phase) {
	case VIDEO_BROKER_WORKER_PHASE_PREPARE: return "prepare";
	case VIDEO_BROKER_WORKER_PHASE_WARMUP: return "warmup";
	case VIDEO_BROKER_WORKER_PHASE_ADMIT: return "admit";
	case VIDEO_BROKER_WORKER_PHASE_FIRST_FRAME: return "first_frame";
	case VIDEO_BROKER_WORKER_PHASE_CUTOVER: return "cutover";
	case VIDEO_BROKER_WORKER_PHASE_VERIFY: return "verify";
	case VIDEO_BROKER_WORKER_PHASE_ROLLBACK: return "rollback";
	case VIDEO_BROKER_WORKER_PHASE_ABORT: return "abort";
	case VIDEO_BROKER_WORKER_PHASE_RETIRE: return "retire";
	default: return "";
	}
}

const char *video_broker_worker_event_name(video_broker_worker_event_t event) {
	switch (event) {
	case VIDEO_BROKER_WORKER_EVENT_PREPARED: return "prepared";
	case VIDEO_BROKER_WORKER_EVENT_WARMED: return "warmed";
	case VIDEO_BROKER_WORKER_EVENT_ADMITTED: return "admitted";
	case VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME: return "first_frame";
	case VIDEO_BROKER_WORKER_EVENT_CUTOVER: return "cutover";
	case VIDEO_BROKER_WORKER_EVENT_VERIFIED: return "verified";
	case VIDEO_BROKER_WORKER_EVENT_ROLLED_BACK: return "rolled_back";
	case VIDEO_BROKER_WORKER_EVENT_ABORTED: return "aborted";
	case VIDEO_BROKER_WORKER_EVENT_RETIRED: return "retired";
	case VIDEO_BROKER_WORKER_EVENT_REJECTED: return "rejected";
	default: return "";
	}
}

static bool parse_profile(
	json_cursor_t *cursor,
	video_broker_worker_branch_profile_t *profile) {
	unsigned int fields = 0;
	bool first = true;
	memset(profile, 0, sizeof(*profile));
	if (!take(cursor, '{')) {
		return false;
	}
	for (;;) {
		char key[32];
		unsigned int bit;
		skip_space(cursor);
		if (cursor->next < cursor->end && *cursor->next == '}') {
			cursor->next++;
			break;
		}
		if (!first && !take(cursor, ',')) {
			return false;
		}
		first = false;
		if (!parse_string(cursor, key, sizeof(key)) || !take(cursor, ':')) {
			return false;
		}
		if (strcmp(key, "codec") == 0) bit = 1U << 0;
		else if (strcmp(key, "pixel_format") == 0) bit = 1U << 1;
		else if (strcmp(key, "width") == 0) bit = 1U << 2;
		else if (strcmp(key, "height") == 0) bit = 1U << 3;
		else if (strcmp(key, "fps") == 0) bit = 1U << 4;
		else if (strcmp(key, "bitrate_kbps") == 0) bit = 1U << 5;
		else if (strcmp(key, "content") == 0) bit = 1U << 6;
		else if (strcmp(key, "rate_control") == 0) bit = 1U << 7;
		else return false;
		if ((fields & bit) != 0) {
			return false;
		}
		fields |= bit;
		switch (bit) {
		case 1U << 0:
			if (!parse_string(cursor, profile->codec, sizeof(profile->codec))) return false;
			break;
		case 1U << 1:
			if (!parse_string(cursor, profile->pixel_format, sizeof(profile->pixel_format))) return false;
			break;
		case 1U << 2:
			if (!parse_positive_int(cursor, &profile->width)) return false;
			break;
		case 1U << 3:
			if (!parse_positive_int(cursor, &profile->height)) return false;
			break;
		case 1U << 4:
			if (!parse_positive_int(cursor, &profile->fps)) return false;
			break;
		case 1U << 5:
			if (!parse_positive_int(cursor, &profile->bitrate_kbps)) return false;
			break;
		case 1U << 6:
			if (!parse_string(cursor, profile->content, sizeof(profile->content))) return false;
			break;
		case 1U << 7:
			if (!parse_string(cursor, profile->rate_control, sizeof(profile->rate_control))) return false;
			break;
		default:
			return false;
		}
	}
	return (fields & ((1U << 2) | (1U << 3) | (1U << 4))) ==
		((1U << 2) | (1U << 3) | (1U << 4));
}

static bool profile_valid(
	video_broker_worker_role_t role,
	const video_broker_worker_branch_profile_t *profile) {
	if (profile->width <= 0 || profile->width > 16384 ||
	    profile->height <= 0 || profile->height > 16384 ||
	    (profile->width & 1) != 0 || (profile->height & 1) != 0 ||
	    profile->fps <= 0 || profile->fps > 240) {
		return false;
	}
	if (role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) {
		return strcmp(profile->pixel_format, "nv12") == 0 &&
			profile->codec[0] == '\0' && profile->bitrate_kbps == 0 &&
			profile->content[0] == '\0' && profile->rate_control[0] == '\0';
	}
	if (role != VIDEO_BROKER_WORKER_ROLE_LIVE &&
	    role != VIDEO_BROKER_WORKER_ROLE_ARCHIVE) {
		return false;
	}
	if ((strcmp(profile->codec, "h264") != 0 && strcmp(profile->codec, "h265") != 0) ||
	    profile->pixel_format[0] != '\0' || profile->bitrate_kbps <= 0 ||
	    profile->bitrate_kbps > (1 << 20)) {
		return false;
	}
	if (role == VIDEO_BROKER_WORKER_ROLE_LIVE) {
		return strcmp(profile->content, "processed") == 0 &&
			strcmp(profile->rate_control, "cbr") == 0;
	}
	return (strcmp(profile->content, "source") == 0 ||
		strcmp(profile->content, "processed") == 0 ||
		strcmp(profile->content, "adaptive") == 0) &&
		(strcmp(profile->rate_control, "cbr") == 0 ||
		 strcmp(profile->rate_control, "avbr") == 0);
}

static bool parse_change(
	json_cursor_t *cursor,
	video_broker_worker_branch_change_t *change) {
	unsigned int fields = 0;
	bool first = true;
	memset(change, 0, sizeof(*change));
	change->role = VIDEO_BROKER_WORKER_ROLE_INVALID;
	change->action = VIDEO_BROKER_WORKER_ACTION_INVALID;
	if (!take(cursor, '{')) {
		return false;
	}
	for (;;) {
		char key[32];
		char value[32];
		unsigned int bit;
		skip_space(cursor);
		if (cursor->next < cursor->end && *cursor->next == '}') {
			cursor->next++;
			break;
		}
		if (!first && !take(cursor, ',')) return false;
		first = false;
		if (!parse_string(cursor, key, sizeof(key)) || !take(cursor, ':')) return false;
		if (strcmp(key, "role") == 0) bit = 1U << 0;
		else if (strcmp(key, "action") == 0) bit = 1U << 1;
		else if (strcmp(key, "profile") == 0) bit = 1U << 2;
		else return false;
		if ((fields & bit) != 0) return false;
		fields |= bit;
		if (bit == (1U << 0)) {
			if (!parse_string(cursor, value, sizeof(value))) return false;
			change->role = parse_role_name(value);
		} else if (bit == (1U << 1)) {
			if (!parse_string(cursor, value, sizeof(value))) return false;
			change->action = parse_action_name(value);
		} else {
			if (!parse_profile(cursor, &change->profile)) return false;
			change->profile_present = true;
		}
	}
	if ((fields & 3U) != 3U || change->role == VIDEO_BROKER_WORKER_ROLE_INVALID ||
	    change->action == VIDEO_BROKER_WORKER_ACTION_INVALID) {
		return false;
	}
	if (change->action == VIDEO_BROKER_WORKER_ACTION_DETACH) {
		return !change->profile_present;
	}
	return change->profile_present && profile_valid(change->role, &change->profile);
}

static bool parse_changes(
	json_cursor_t *cursor,
	video_broker_worker_fanout_command_t *command) {
	bool first = true;
	if (!take(cursor, '[')) return false;
	for (;;) {
		skip_space(cursor);
		if (cursor->next < cursor->end && *cursor->next == ']') {
			cursor->next++;
			break;
		}
		if (!first && !take(cursor, ',')) return false;
		first = false;
		if (command->change_count >= VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES ||
		    !parse_change(cursor, &command->changes[command->change_count])) {
			return false;
		}
		command->change_count++;
	}
	return true;
}

int video_broker_worker_fanout_decode_command(
	const char *line,
	size_t line_size,
	video_broker_worker_fanout_command_t *command) {
	json_cursor_t cursor;
	unsigned int fields = 0;
	bool first = true;
	char schema[64] = {0};
	if (line == NULL || command == NULL || line_size < 3 ||
	    line_size > VIDEO_BROKER_WORKER_FANOUT_MAX_LINE_BYTES ||
	    line[line_size - 1] != '\n' || memchr(line, '\r', line_size) != NULL ||
	    memchr(line, '\n', line_size - 1) != NULL) {
		return -1;
	}
	memset(command, 0, sizeof(*command));
	command->phase = VIDEO_BROKER_WORKER_PHASE_INVALID;
	cursor.next = line;
	cursor.end = line + line_size - 1;
	if (!take(&cursor, '{')) return -1;
	for (;;) {
		char key[32];
		char value[32];
		unsigned int bit;
		skip_space(&cursor);
		if (cursor.next < cursor.end && *cursor.next == '}') {
			cursor.next++;
			break;
		}
		if (!first && !take(&cursor, ',')) return -1;
		first = false;
		if (!parse_string(&cursor, key, sizeof(key)) || !take(&cursor, ':')) return -1;
		if (strcmp(key, "schema") == 0) bit = 1U << 0;
		else if (strcmp(key, "transaction_id") == 0) bit = 1U << 1;
		else if (strcmp(key, "sequence") == 0) bit = 1U << 2;
		else if (strcmp(key, "source_id") == 0) bit = 1U << 3;
		else if (strcmp(key, "capture_pid") == 0) bit = 1U << 4;
		else if (strcmp(key, "media_epoch") == 0) bit = 1U << 5;
		else if (strcmp(key, "phase") == 0) bit = 1U << 6;
		else if (strcmp(key, "changes") == 0) bit = 1U << 7;
		else return -1;
		if ((fields & bit) != 0) return -1;
		fields |= bit;
		switch (bit) {
		case 1U << 0:
			if (!parse_string(&cursor, schema, sizeof(schema))) return -1;
			break;
		case 1U << 1:
			if (!parse_string(&cursor, command->transaction_id, sizeof(command->transaction_id))) return -1;
			break;
		case 1U << 2:
			if (!parse_uint64(&cursor, &command->sequence)) return -1;
			break;
		case 1U << 3:
			if (!parse_string(&cursor, command->source_id, sizeof(command->source_id))) return -1;
			break;
		case 1U << 4:
			if (!parse_positive_int(&cursor, &command->capture_pid)) return -1;
			break;
		case 1U << 5:
			if (!parse_uint64(&cursor, &command->media_epoch)) return -1;
			break;
		case 1U << 6:
			if (!parse_string(&cursor, value, sizeof(value))) return -1;
			command->phase = parse_phase_name(value);
			break;
		case 1U << 7:
			if (!parse_changes(&cursor, command)) return -1;
			break;
		default:
			return -1;
		}
	}
	skip_space(&cursor);
	if (cursor.next != cursor.end || (fields & 0x7fU) != 0x7fU ||
	    strcmp(schema, WORKER_FANOUT_COMMAND_SCHEMA) != 0 ||
	    !canonical_id(command->transaction_id) || !canonical_id(command->source_id) ||
	    command->sequence == 0 || command->capture_pid <= 0 || command->media_epoch == 0 ||
	    command->phase == VIDEO_BROKER_WORKER_PHASE_INVALID) {
		return -1;
	}
	if (command->phase != VIDEO_BROKER_WORKER_PHASE_PREPARE) {
		return (fields & (1U << 7)) == 0 && command->change_count == 0 ? 0 : -1;
	}
	if ((fields & (1U << 7)) == 0 || command->change_count == 0) return -1;
	for (size_t index = 1; index < command->change_count; ++index) {
		if (command->changes[index - 1].role >= command->changes[index].role) {
			return -1;
		}
	}
	return 0;
}

void video_broker_worker_fanout_receipt_from_command(
	video_broker_worker_fanout_receipt_t *receipt,
	const video_broker_worker_fanout_command_t *command) {
	if (receipt == NULL || command == NULL) return;
	memset(receipt, 0, sizeof(*receipt));
	memcpy(receipt->transaction_id, command->transaction_id, sizeof(receipt->transaction_id));
	receipt->sequence = command->sequence;
	memcpy(receipt->source_id, command->source_id, sizeof(receipt->source_id));
	receipt->capture_pid = command->capture_pid;
	receipt->media_epoch = command->media_epoch;
}

static bool receipt_runtime_mode_valid(
	video_broker_worker_role_t role,
	const char *mode) {
	if (mode == NULL) return false;
	if (role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS) return mode[0] == '\0';
	if (role == VIDEO_BROKER_WORKER_ROLE_ARCHIVE) {
		return strcmp(mode, "native_passthrough") == 0 ||
			strcmp(mode, "source_hardware_encode") == 0 ||
			strcmp(mode, "processed_hardware_encode") == 0 ||
			strcmp(mode, "software_encode") == 0;
	}
	if (role == VIDEO_BROKER_WORKER_ROLE_LIVE) {
		return strcmp(mode, "persistent_encoder_epoch") == 0 ||
			strcmp(mode, "native_passthrough_static") == 0 ||
			strcmp(mode, "backend_static") == 0;
	}
	return false;
}

static bool receipt_valid(const video_broker_worker_fanout_receipt_t *receipt) {
	if (receipt == NULL || !canonical_id(receipt->transaction_id) ||
	    !canonical_id(receipt->source_id) || receipt->sequence == 0 ||
	    receipt->capture_pid <= 0 || receipt->media_epoch == 0 ||
	    video_broker_worker_event_name(receipt->event)[0] == '\0') {
		return false;
	}
	if (receipt->event == VIDEO_BROKER_WORKER_EVENT_REJECTED) {
		return receipt->branch_count == 0 && canonical_error_code(receipt->error_code);
	}
	if (receipt->error_code[0] != '\0') return false;
	if (receipt->event != VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME) {
		return receipt->branch_count == 0;
	}
	if (receipt->branch_count > VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES) return false;
	for (size_t index = 0; index < receipt->branch_count; ++index) {
		const video_broker_worker_branch_receipt_t *branch = &receipt->branches[index];
		if (video_broker_worker_role_name(branch->role)[0] == '\0' ||
		    branch->branch_epoch == 0 || branch->frame_id == 0 ||
		    (index > 0 && receipt->branches[index - 1].role >= branch->role) ||
		    (branch->role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS && branch->codec_bootstrap_complete) ||
		    (branch->role != VIDEO_BROKER_WORKER_ROLE_ANALYSIS && !branch->codec_bootstrap_complete) ||
		    !receipt_runtime_mode_valid(branch->role, branch->runtime_mode)) {
			return false;
		}
	}
	return true;
}

int video_broker_worker_fanout_write_receipt(
	FILE *output,
	const video_broker_worker_fanout_receipt_t *receipt) {
	if (output == NULL || !receipt_valid(receipt)) return -1;
	if (fprintf(output,
		"{\"schema\":\"%s\",\"transaction_id\":\"%s\",\"sequence\":%llu,"
		"\"source_id\":\"%s\",\"capture_pid\":%d,\"media_epoch\":%llu,\"event\":\"%s\"",
		WORKER_FANOUT_RECEIPT_SCHEMA,
		receipt->transaction_id, (unsigned long long)receipt->sequence,
		receipt->source_id, receipt->capture_pid,
		(unsigned long long)receipt->media_epoch,
		video_broker_worker_event_name(receipt->event)) < 0) {
		return -1;
	}
	if (receipt->event == VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME && receipt->branch_count > 0) {
		if (fputs(",\"branches\":[", output) == EOF) return -1;
		for (size_t index = 0; index < receipt->branch_count; ++index) {
			const video_broker_worker_branch_receipt_t *branch = &receipt->branches[index];
			if (index > 0 && fputc(',', output) == EOF) return -1;
			if (fprintf(output,
				"{\"role\":\"%s\",\"branch_epoch\":%llu,\"frame_id\":%llu,"
				"\"codec_bootstrap\":\"%s\",\"runtime_mode\":\"%s\"}",
				video_broker_worker_role_name(branch->role),
				(unsigned long long)branch->branch_epoch,
				(unsigned long long)branch->frame_id,
				branch->codec_bootstrap_complete ? "complete" : "not_applicable",
				branch->runtime_mode) < 0) {
				return -1;
			}
		}
		if (fputc(']', output) == EOF) return -1;
	}
	if (receipt->event == VIDEO_BROKER_WORKER_EVENT_REJECTED &&
	    fprintf(output, ",\"error_code\":\"%s\"", receipt->error_code) < 0) {
		return -1;
	}
	if (fputs("}\n", output) == EOF || fflush(output) != 0) return -1;
	return 0;
}

static bool callbacks_complete(video_broker_worker_fanout_callbacks_t callbacks) {
	return callbacks.prepare != NULL && callbacks.warmup != NULL &&
		callbacks.admit != NULL && callbacks.first_frame != NULL &&
		callbacks.cutover != NULL && callbacks.verify != NULL &&
		callbacks.rollback != NULL && callbacks.abort != NULL &&
		callbacks.retire != NULL;
}

int video_broker_worker_fanout_controller_init(
	video_broker_worker_fanout_controller_t *controller,
	const char *source_id,
	int capture_pid,
	uint64_t media_epoch,
	video_broker_worker_fanout_callbacks_t callbacks,
	void *callback_context) {
	if (controller == NULL || !canonical_id(source_id) || capture_pid <= 0 ||
	    media_epoch == 0 || !callbacks_complete(callbacks)) {
		return -1;
	}
	memset(controller, 0, sizeof(*controller));
	memcpy(controller->source_id, source_id, strlen(source_id) + 1);
	controller->capture_pid = capture_pid;
	controller->media_epoch = media_epoch;
	controller->callbacks = callbacks;
	controller->callback_context = callback_context;
	return 0;
}

static void reset_transaction(video_broker_worker_fanout_controller_t *controller) {
	controller->transaction_active = false;
	controller->transaction_id[0] = '\0';
	controller->stage = VIDEO_BROKER_WORKER_EVENT_INVALID;
	controller->change_count = 0;
	controller->phase_pending = false;
	memset(controller->changes, 0, sizeof(controller->changes));
	memset(&controller->pending_command, 0, sizeof(controller->pending_command));
}

static void reject_receipt(
	video_broker_worker_fanout_receipt_t *receipt,
	const char *error_code) {
	receipt->event = VIDEO_BROKER_WORKER_EVENT_REJECTED;
	receipt->branch_count = 0;
	memset(receipt->branches, 0, sizeof(receipt->branches));
	memset(receipt->error_code, 0, sizeof(receipt->error_code));
	if (canonical_error_code(error_code)) {
		memcpy(receipt->error_code, error_code, strlen(error_code) + 1);
	} else {
		memcpy(receipt->error_code, "worker_rejected", sizeof("worker_rejected"));
	}
}

static const char *callback_error_code(
	video_broker_worker_phase_t phase,
	int callback_result) {
	if (callback_result == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_TIMEOUT) {
		return "first_frame_timeout";
	}
	switch (phase) {
	case VIDEO_BROKER_WORKER_PHASE_PREPARE: return "prepare_failed";
	case VIDEO_BROKER_WORKER_PHASE_WARMUP: return "warmup_failed";
	case VIDEO_BROKER_WORKER_PHASE_ADMIT: return "admission_failed";
	case VIDEO_BROKER_WORKER_PHASE_FIRST_FRAME: return "first_frame_failed";
	case VIDEO_BROKER_WORKER_PHASE_CUTOVER: return "cutover_failed";
	case VIDEO_BROKER_WORKER_PHASE_VERIFY: return "verify_failed";
	case VIDEO_BROKER_WORKER_PHASE_ROLLBACK: return "rollback_failed";
	case VIDEO_BROKER_WORKER_PHASE_ABORT: return "abort_failed";
	case VIDEO_BROKER_WORKER_PHASE_RETIRE: return "retire_failed";
	default: return "worker_rejected";
	}
}

static bool first_frame_evidence_matches(
	const video_broker_worker_fanout_controller_t *controller,
	const video_broker_worker_fanout_receipt_t *receipt) {
	size_t branch_index = 0;
	for (size_t change_index = 0; change_index < controller->change_count; ++change_index) {
		const video_broker_worker_branch_change_t *change = &controller->changes[change_index];
		if (change->action == VIDEO_BROKER_WORKER_ACTION_DETACH) continue;
		if (branch_index >= receipt->branch_count ||
		    receipt->branches[branch_index].role != change->role ||
		    receipt->branches[branch_index].branch_epoch == 0 ||
		    receipt->branches[branch_index].frame_id == 0 ||
		    (change->role == VIDEO_BROKER_WORKER_ROLE_ANALYSIS &&
		     receipt->branches[branch_index].codec_bootstrap_complete) ||
		    (change->role != VIDEO_BROKER_WORKER_ROLE_ANALYSIS &&
		     !receipt->branches[branch_index].codec_bootstrap_complete) ||
		    !receipt_runtime_mode_valid(
			    change->role, receipt->branches[branch_index].runtime_mode)) {
			return false;
		}
		branch_index++;
	}
	return branch_index == receipt->branch_count;
}

static int execute_transaction_phase(
	video_broker_worker_fanout_controller_t *controller,
	const video_broker_worker_fanout_command_t *command,
	video_broker_worker_fanout_receipt_t *receipt) {
	int result = VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
	switch (command->phase) {
	case VIDEO_BROKER_WORKER_PHASE_WARMUP:
		if (controller->stage != VIDEO_BROKER_WORKER_EVENT_PREPARED) {
			reject_receipt(receipt, "invalid_phase");
			return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
		}
		result = controller->callbacks.warmup(controller->callback_context);
		if (result == 0) {
			controller->stage = VIDEO_BROKER_WORKER_EVENT_WARMED;
			receipt->event = VIDEO_BROKER_WORKER_EVENT_WARMED;
		}
		break;
	case VIDEO_BROKER_WORKER_PHASE_ADMIT:
		if (controller->stage != VIDEO_BROKER_WORKER_EVENT_WARMED) {
			reject_receipt(receipt, "invalid_phase");
			return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
		}
		result = controller->callbacks.admit(controller->callback_context);
		if (result == 0) {
			controller->stage = VIDEO_BROKER_WORKER_EVENT_ADMITTED;
			receipt->event = VIDEO_BROKER_WORKER_EVENT_ADMITTED;
		}
		break;
	case VIDEO_BROKER_WORKER_PHASE_FIRST_FRAME:
		if (controller->stage != VIDEO_BROKER_WORKER_EVENT_ADMITTED) {
			reject_receipt(receipt, "invalid_phase");
			return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
		}
		receipt->branch_count = VIDEO_BROKER_WORKER_FANOUT_MAX_CHANGES;
		result = controller->callbacks.first_frame(
			controller->callback_context, receipt->branches, &receipt->branch_count);
		if (result == 0 && !first_frame_evidence_matches(controller, receipt)) {
			result = VIDEO_BROKER_WORKER_FANOUT_CALLBACK_FAILED;
		}
		if (result == 0) {
			controller->stage = VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME;
			receipt->event = VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME;
		} else {
			receipt->branch_count = 0;
			memset(receipt->branches, 0, sizeof(receipt->branches));
		}
		break;
	case VIDEO_BROKER_WORKER_PHASE_CUTOVER:
		if (controller->stage != VIDEO_BROKER_WORKER_EVENT_FIRST_FRAME) {
			reject_receipt(receipt, "invalid_phase");
			return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
		}
		result = controller->callbacks.cutover(controller->callback_context);
		if (result == 0) {
			controller->stage = VIDEO_BROKER_WORKER_EVENT_CUTOVER;
			receipt->event = VIDEO_BROKER_WORKER_EVENT_CUTOVER;
		}
		break;
	case VIDEO_BROKER_WORKER_PHASE_VERIFY:
		if (controller->stage != VIDEO_BROKER_WORKER_EVENT_CUTOVER) {
			reject_receipt(receipt, "invalid_phase");
			return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
		}
		result = controller->callbacks.verify(controller->callback_context);
		if (result == 0) {
			controller->stage = VIDEO_BROKER_WORKER_EVENT_VERIFIED;
			receipt->event = VIDEO_BROKER_WORKER_EVENT_VERIFIED;
		}
		break;
	case VIDEO_BROKER_WORKER_PHASE_ROLLBACK:
		result = controller->callbacks.rollback(controller->callback_context);
		if (result == 0) {
			reset_transaction(controller);
			receipt->event = VIDEO_BROKER_WORKER_EVENT_ROLLED_BACK;
		}
		break;
	case VIDEO_BROKER_WORKER_PHASE_ABORT:
		if (controller->stage == VIDEO_BROKER_WORKER_EVENT_CUTOVER ||
		    controller->stage == VIDEO_BROKER_WORKER_EVENT_VERIFIED) {
			reject_receipt(receipt, "invalid_phase");
			return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
		}
		result = controller->callbacks.abort(controller->callback_context);
		if (result == 0) {
			reset_transaction(controller);
			receipt->event = VIDEO_BROKER_WORKER_EVENT_ABORTED;
		}
		break;
	case VIDEO_BROKER_WORKER_PHASE_RETIRE:
		if (controller->stage != VIDEO_BROKER_WORKER_EVENT_VERIFIED) {
			reject_receipt(receipt, "invalid_phase");
			return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
		}
		result = controller->callbacks.retire(controller->callback_context);
		if (result == 0) {
			reset_transaction(controller);
			receipt->event = VIDEO_BROKER_WORKER_EVENT_RETIRED;
		}
		break;
	default:
		reject_receipt(receipt, "invalid_phase");
		return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
	}
	if (result == VIDEO_BROKER_WORKER_FANOUT_CALLBACK_PENDING) {
		controller->phase_pending = true;
		controller->pending_command = *command;
		memset(receipt, 0, sizeof(*receipt));
		return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_PENDING;
	}
	if (result != VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK) {
		reject_receipt(receipt, callback_error_code(command->phase, result));
	}
	return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
}

int video_broker_worker_fanout_controller_handle(
	video_broker_worker_fanout_controller_t *controller,
	const video_broker_worker_fanout_command_t *command,
	video_broker_worker_fanout_receipt_t *receipt) {
	int result;
	if (receipt == NULL) return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
	memset(receipt, 0, sizeof(*receipt));
	if (controller == NULL || command == NULL) return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
	video_broker_worker_fanout_receipt_from_command(receipt, command);
	if (strcmp(command->source_id, controller->source_id) != 0 ||
	    command->capture_pid != controller->capture_pid ||
	    command->media_epoch != controller->media_epoch) {
		reject_receipt(receipt, "invalid_fence");
		return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
	}
	if (controller->phase_pending) {
		reject_receipt(receipt, "phase_pending");
		return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
	}
	if (command->sequence <= controller->last_sequence) {
		reject_receipt(receipt, "stale_sequence");
		return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
	}
	controller->last_sequence = command->sequence;
	if (command->phase == VIDEO_BROKER_WORKER_PHASE_PREPARE) {
		if (controller->transaction_active) {
			reject_receipt(receipt, "transaction_busy");
			return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
		}
		result = controller->callbacks.prepare(
			controller->callback_context, command->changes, command->change_count);
		if (result != VIDEO_BROKER_WORKER_FANOUT_CALLBACK_OK) {
			reject_receipt(receipt, callback_error_code(command->phase, result));
			return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
		}
		controller->transaction_active = true;
		memcpy(controller->transaction_id, command->transaction_id,
			strlen(command->transaction_id) + 1);
		memcpy(controller->changes, command->changes,
			command->change_count * sizeof(command->changes[0]));
		controller->change_count = command->change_count;
		controller->stage = VIDEO_BROKER_WORKER_EVENT_PREPARED;
		receipt->event = VIDEO_BROKER_WORKER_EVENT_PREPARED;
		return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
	}
	if (!controller->transaction_active ||
	    strcmp(command->transaction_id, controller->transaction_id) != 0) {
		reject_receipt(receipt, "invalid_transaction");
		return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
	}
	return execute_transaction_phase(controller, command, receipt);
}

int video_broker_worker_fanout_controller_resume(
	video_broker_worker_fanout_controller_t *controller,
	video_broker_worker_fanout_receipt_t *receipt) {
	video_broker_worker_fanout_command_t command;
	if (controller == NULL || receipt == NULL || !controller->phase_pending) {
		return VIDEO_BROKER_WORKER_FANOUT_CONTROLLER_READY;
	}
	command = controller->pending_command;
	controller->phase_pending = false;
	memset(&controller->pending_command, 0, sizeof(controller->pending_command));
	video_broker_worker_fanout_receipt_from_command(receipt, &command);
	return execute_transaction_phase(controller, &command, receipt);
}
