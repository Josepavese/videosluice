/* RDK X5 decode worker owned by the standalone video-broker data plane. */
#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include "worker_signal.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "hb_media_codec.h"
#include "sp_codec.h"

/* These C helpers are exported by libspcdev but are not part of sp_codec.h. */
extern int32_t vp_decode_config_param(media_codec_context_t *context,
	media_codec_id_t codec_type, int32_t width, int32_t height);
extern int32_t vp_codec_init(media_codec_context_t *context);
extern int32_t vp_codec_deinit(media_codec_context_t *context);
extern int32_t vp_codec_start(media_codec_context_t *context);
extern int32_t vp_codec_stop(media_codec_context_t *context);

static volatile sig_atomic_t g_running = 1;

enum {
	kDefaultDecodeWidth = 640,
	kDefaultDecodeHeight = 480,
	kDefaultDecodeCodec = SP_ENCODER_H265,
	kDefaultDecodeFrames = 1,
	kDecodeFramesInfinite = 0,
	kDecodeBufferFloor = 1 << 20,
	kMaxEncodedChunk = 64 << 20,
	kNHD2Version = 2,
	kNHD2HeaderSize = 32,
	kNHD2KindReady = 1,
	kNHD2KindEncodedAU = 2,
	kNHD2KindDecodedNV12 = 3,
	kNHD2KindDropped = 4,
	kNHD2KindCorrelationLost = 5,
	kNHD2FlagDecodeWarning = 1 << 8,
	kCorrelationSlots = 4096,
	kDecodeFirstPollTimeoutMs = 2,
	kDecodeDrainPollTimeoutMs = 0,
};

typedef struct {
	int codec_type;
	int width;
	int height;
	int frames;
	bool codec_explicit;
	bool pipe_protocol_explicit;
} decode_config_t;

typedef struct {
	uint8_t kind;
	uint16_t flags;
	uint32_t frame_id;
	uint64_t timestamp_ns;
	uint32_t frame_gap;
	uint32_t missed_frames;
	uint32_t payload_size;
} nhd2_record_t;

typedef struct {
	uint64_t token;
	bool valid;
	nhd2_record_t source;
} correlation_slot_t;

typedef struct {
	correlation_slot_t slots[kCorrelationSlots];
	uint64_t next_token;
} correlation_table_t;

static void handle_signal(int signum) {
	(void)signum;
	g_running = 0;
}

static bool parse_int_arg(const char *value, int minimum, int maximum, int *out) {
	char *end = NULL;
	long parsed;
	if (value == NULL || *value == '\0' || out == NULL) {
		return false;
	}
	errno = 0;
	parsed = strtol(value, &end, 10);
	if (errno != 0 || end == value || *end != '\0' || parsed < minimum || parsed > maximum) {
		return false;
	}
	*out = (int)parsed;
	return true;
}

static bool parse_codec_type(const char *value, int *out) {
	if (value == NULL || out == NULL) {
		return false;
	}
	if (strcmp(value, "h265") == 0) {
		*out = SP_ENCODER_H265;
		return true;
	}
	if (strcmp(value, "h264") == 0) {
		*out = SP_ENCODER_H264;
		return true;
	}
	return false;
}

static void default_config(decode_config_t *cfg) {
	memset(cfg, 0, sizeof(*cfg));
	cfg->codec_type = kDefaultDecodeCodec;
	cfg->width = kDefaultDecodeWidth;
	cfg->height = kDefaultDecodeHeight;
	cfg->frames = kDefaultDecodeFrames;
}

static int parse_args(int argc, char **argv, decode_config_t *cfg) {
	for (int i = 1; i < argc; ++i) {
		if (strcmp(argv[i], "--codec") == 0) {
			if (++i >= argc || !parse_codec_type(argv[i], &cfg->codec_type)) {
				fprintf(stderr, "[rdkx5_h26x_decode_bridge] invalid --codec; expected h265|h264\n");
				return -1;
			}
			cfg->codec_explicit = true;
		} else if (strcmp(argv[i], "--width") == 0) {
			if (++i >= argc || !parse_int_arg(argv[i], 1, 1 << 20, &cfg->width)) {
				fprintf(stderr, "[rdkx5_h26x_decode_bridge] invalid --width\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--height") == 0) {
			if (++i >= argc || !parse_int_arg(argv[i], 1, 1 << 20, &cfg->height)) {
				fprintf(stderr, "[rdkx5_h26x_decode_bridge] invalid --height\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--frames") == 0) {
			if (++i >= argc || !parse_int_arg(argv[i], 0, 1 << 20, &cfg->frames)) {
				fprintf(stderr, "[rdkx5_h26x_decode_bridge] invalid --frames\n");
				return -1;
			}
		} else if (strcmp(argv[i], "--pipe-protocol") == 0 && i + 1 < argc) {
			const char *protocol = argv[++i];
			if (strcmp(protocol, "nhd2") != 0) {
				fprintf(stderr, "[rdkx5_h26x_decode_bridge] unsupported pipe protocol: %s\n", protocol);
				return -1;
			}
			cfg->pipe_protocol_explicit = true;
		} else {
			fprintf(stderr, "[rdkx5_h26x_decode_bridge] unsupported or incomplete argument: %s\n", argv[i]);
			return -1;
		}
	}
	if (!cfg->codec_explicit || !cfg->pipe_protocol_explicit) {
		fprintf(stderr, "[rdkx5_h26x_decode_bridge] explicit --codec and --pipe-protocol nhd2 are required\n");
		return -1;
	}
	return 0;
}

static uint64_t host_to_be64(uint64_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
	return ((uint64_t)htonl((uint32_t)(value & 0xffffffffU)) << 32) |
		(uint64_t)htonl((uint32_t)(value >> 32));
#else
	return value;
#endif
}

static uint64_t be64_to_host(uint64_t value) {
	return host_to_be64(value);
}

static void put_u16(unsigned char *dst, uint16_t value) {
	value = htons(value);
	memcpy(dst, &value, sizeof(value));
}

static void put_u32(unsigned char *dst, uint32_t value) {
	value = htonl(value);
	memcpy(dst, &value, sizeof(value));
}

static void put_u64(unsigned char *dst, uint64_t value) {
	value = host_to_be64(value);
	memcpy(dst, &value, sizeof(value));
}

static uint16_t get_u16(const unsigned char *src) {
	uint16_t value;
	memcpy(&value, src, sizeof(value));
	return ntohs(value);
}

static uint32_t get_u32(const unsigned char *src) {
	uint32_t value;
	memcpy(&value, src, sizeof(value));
	return ntohl(value);
}

static uint64_t get_u64(const unsigned char *src) {
	uint64_t value;
	memcpy(&value, src, sizeof(value));
	return be64_to_host(value);
}

static int read_exact(FILE *in, unsigned char *data, size_t size) {
	size_t offset = 0;
	while (offset < size) {
		size_t count = fread(data + offset, 1, size - offset, in);
		if (count == 0) {
			/* EOF before a record starts is clean. EOF after any byte is a
			 * truncated protocol record and must never be reported as success. */
			return feof(in) && offset == 0 ? 0 : -1;
		}
		offset += count;
	}
	return 1;
}

static void encode_nhd2_header(unsigned char header[kNHD2HeaderSize], const nhd2_record_t *record) {
	memset(header, 0, kNHD2HeaderSize);
	memcpy(header, "NHD2", 4);
	header[4] = kNHD2Version;
	header[5] = record->kind;
	put_u16(header + 6, record->flags);
	put_u32(header + 8, record->frame_id);
	put_u64(header + 12, record->timestamp_ns);
	put_u32(header + 20, record->frame_gap);
	put_u32(header + 24, record->missed_frames);
	put_u32(header + 28, record->payload_size);
}

static int decode_nhd2_header(const unsigned char header[kNHD2HeaderSize], nhd2_record_t *record) {
	if (memcmp(header, "NHD2", 4) != 0 || header[4] != kNHD2Version) {
		return -1;
	}
	memset(record, 0, sizeof(*record));
	record->kind = header[5];
	record->flags = get_u16(header + 6);
	record->frame_id = get_u32(header + 8);
	record->timestamp_ns = get_u64(header + 12);
	record->frame_gap = get_u32(header + 20);
	record->missed_frames = get_u32(header + 24);
	record->payload_size = get_u32(header + 28);
	return 0;
}

static int write_nhd2_record(FILE *out, const nhd2_record_t *record,
	const unsigned char *payload) {
	unsigned char header[kNHD2HeaderSize];
	if (out == NULL || record == NULL || (record->payload_size > 0 && payload == NULL)) {
		return -1;
	}
	encode_nhd2_header(header, record);
	if (fwrite(header, 1, sizeof(header), out) != sizeof(header)) {
		return -1;
	}
	if (record->payload_size > 0 &&
		fwrite(payload, 1, record->payload_size, out) != record->payload_size) {
		return -1;
	}
	fflush(out);
	return ferror(out) ? -1 : 0;
}

static int ensure_stream_capacity(unsigned char **buffer, size_t *capacity, size_t required) {
	unsigned char *resized;
	if (required == 0 || required > kMaxEncodedChunk) {
		return -1;
	}
	if (*capacity >= required) {
		return 0;
	}
	resized = (unsigned char *)realloc(*buffer, required);
	if (resized == NULL) {
		return -1;
	}
	*buffer = resized;
	*capacity = required;
	return 0;
}

static uint64_t remember_correlation(correlation_table_t *table, const nhd2_record_t *source) {
	correlation_slot_t *slot;
	table->next_token++;
	if (table->next_token == 0) {
		table->next_token++;
	}
	slot = &table->slots[table->next_token % kCorrelationSlots];
	slot->token = table->next_token;
	slot->valid = true;
	slot->source = *source;
	return table->next_token;
}

static bool take_correlation(correlation_table_t *table, uint64_t token, nhd2_record_t *source) {
	correlation_slot_t *slot;
	if (token == 0) {
		return false;
	}
	slot = &table->slots[token % kCorrelationSlots];
	if (!slot->valid || slot->token != token) {
		return false;
	}
	*source = slot->source;
	slot->valid = false;
	return true;
}

static int queue_decoder_input(media_codec_context_t *context, const unsigned char *data,
	uint32_t size, uint64_t token) {
	media_codec_buffer_t buffer;
	int32_t rc;
	memset(&buffer, 0, sizeof(buffer));
	buffer.type = MC_VIDEO_STREAM_BUFFER;
	rc = hb_mm_mc_dequeue_input_buffer(context, &buffer, 2000);
	if (rc != 0) {
		return -1;
	}
	if (buffer.vstream_buf.vir_ptr == NULL || buffer.vstream_buf.size < size) {
		buffer.vstream_buf.size = 0;
		buffer.vstream_buf.stream_end = 0;
		(void)hb_mm_mc_queue_input_buffer(context, &buffer, 2000);
		return -1;
	}
	buffer.type = MC_VIDEO_STREAM_BUFFER;
	buffer.vstream_buf.size = size;
	buffer.vstream_buf.stream_end = 0;
	buffer.vstream_buf.pts = token;
	memcpy(buffer.vstream_buf.vir_ptr, data, size);
	return hb_mm_mc_queue_input_buffer(context, &buffer, 2000) == 0 ? 0 : -1;
}

static int emit_decoder_outputs(media_codec_context_t *context, FILE *out,
	correlation_table_t *correlations, unsigned char *image_buffer, size_t frame_size,
	int *frames_written, int frame_limit, int first_timeout_ms) {
	int timeout_ms = first_timeout_ms;
	while (g_running && (frame_limit == kDecodeFramesInfinite || *frames_written < frame_limit)) {
		media_codec_buffer_t buffer;
		media_codec_output_buffer_info_t info;
		nhd2_record_t source;
		nhd2_record_t output;
		uint64_t token;
		bool mapped;
		bool valid_frame;
		int32_t rc;

		memset(&buffer, 0, sizeof(buffer));
		memset(&info, 0, sizeof(info));
		rc = hb_mm_mc_dequeue_output_buffer(context, &buffer, &info, timeout_ms);
		if (rc != 0) {
			return 0;
		}
		timeout_ms = kDecodeDrainPollTimeoutMs;
		token = buffer.vframe_buf.pts;
		mapped = take_correlation(correlations, token, &source);
		valid_frame = buffer.type == MC_VIDEO_FRAME_BUFFER &&
			info.video_frame_info.decode_result != 0 &&
			buffer.vframe_buf.size >= frame_size &&
			buffer.vframe_buf.vir_ptr[0] != NULL &&
			buffer.vframe_buf.vir_ptr[1] != NULL;

		memset(&output, 0, sizeof(output));
		if (mapped) {
			output = source;
		}
		if (valid_frame) {
			memcpy(image_buffer, buffer.vframe_buf.vir_ptr[0], frame_size * 2U / 3U);
			memcpy(image_buffer + frame_size * 2U / 3U,
				buffer.vframe_buf.vir_ptr[1], frame_size / 3U);
			output.kind = mapped ? kNHD2KindDecodedNV12 : kNHD2KindCorrelationLost;
			output.payload_size = (uint32_t)frame_size;
			if (info.video_frame_info.decode_result != 1) {
				output.flags |= kNHD2FlagDecodeWarning;
			}
			rc = write_nhd2_record(out, &output, image_buffer);
			(*frames_written)++;
		} else {
			output.kind = mapped ? kNHD2KindDropped : kNHD2KindCorrelationLost;
			output.payload_size = 0;
			rc = write_nhd2_record(out, &output, NULL);
		}
		if (hb_mm_mc_queue_output_buffer(context, &buffer, 0) != 0) {
			return -1;
		}
		if (rc != 0) {
			return -1;
		}
	}
	return 0;
}

static int run_nhd2(const decode_config_t *cfg, FILE *out, size_t frame_size,
	unsigned char *image_buffer, unsigned char **stream_buffer, size_t *stream_capacity) {
	media_codec_context_t context;
	correlation_table_t correlations;
	media_codec_id_t codec_type;
	bool initialized = false;
	bool started = false;
	int frames_written = 0;
	int rc = 1;

	memset(&context, 0, sizeof(context));
	memset(&correlations, 0, sizeof(correlations));
	codec_type = cfg->codec_type == SP_ENCODER_H265 ? MEDIA_CODEC_ID_H265 : MEDIA_CODEC_ID_H264;
	if (vp_decode_config_param(&context, codec_type, cfg->width, cfg->height) != 0 ||
		vp_codec_init(&context) != 0) {
		fprintf(stderr, "[rdkx5_h26x_decode_bridge] NHD2 decoder init failed\n");
		goto cleanup;
	}
	initialized = true;
	if (vp_codec_start(&context) != 0) {
		fprintf(stderr, "[rdkx5_h26x_decode_bridge] NHD2 decoder start failed\n");
		goto cleanup;
	}
	started = true;

	{
		nhd2_record_t ready;
		memset(&ready, 0, sizeof(ready));
		ready.kind = kNHD2KindReady;
		if (write_nhd2_record(out, &ready, NULL) != 0) {
			goto cleanup;
		}
	}

	while (g_running && (cfg->frames == kDecodeFramesInfinite || frames_written < cfg->frames)) {
		unsigned char header[kNHD2HeaderSize];
		nhd2_record_t source;
		uint64_t token;
		int read_rc = read_exact(stdin, header, sizeof(header));
		if (read_rc == 0) {
			rc = 0;
			break;
		}
		if (read_rc < 0 || decode_nhd2_header(header, &source) != 0 ||
			source.kind != kNHD2KindEncodedAU || source.payload_size == 0 ||
			source.payload_size > kMaxEncodedChunk) {
			fprintf(stderr, "[rdkx5_h26x_decode_bridge] invalid NHD2 input record\n");
			goto cleanup;
		}
		if (ensure_stream_capacity(stream_buffer, stream_capacity, source.payload_size) != 0 ||
			read_exact(stdin, *stream_buffer, source.payload_size) != 1) {
			fprintf(stderr, "[rdkx5_h26x_decode_bridge] truncated or oversized NHD2 payload\n");
			goto cleanup;
		}
		token = remember_correlation(&correlations, &source);
		if (queue_decoder_input(&context, *stream_buffer, source.payload_size, token) != 0) {
			nhd2_record_t dropped = source;
			dropped.kind = kNHD2KindDropped;
			dropped.payload_size = 0;
			(void)take_correlation(&correlations, token, &source);
			if (write_nhd2_record(out, &dropped, NULL) != 0) {
				goto cleanup;
			}
			continue;
		}
		if (emit_decoder_outputs(&context, out, &correlations, image_buffer, frame_size,
			&frames_written, cfg->frames, kDecodeFirstPollTimeoutMs) != 0) {
			goto cleanup;
		}
		rc = 0;
	}
	if (g_running && (cfg->frames == kDecodeFramesInfinite || frames_written < cfg->frames)) {
		(void)emit_decoder_outputs(&context, out, &correlations, image_buffer, frame_size,
			&frames_written, cfg->frames, 2000);
	}

cleanup:
	if (started) {
		(void)vp_codec_stop(&context);
	}
	if (initialized) {
		(void)vp_codec_deinit(&context);
	}
	return rc;
}

int main(int argc, char **argv) {
	decode_config_t cfg;
	unsigned char *image_buffer = NULL;
	unsigned char *stream_buffer = NULL;
	size_t frame_size;
	size_t image_size;
	size_t stream_capacity;
	int binary_fd = -1;
	FILE *out = NULL;
	int rc = 1;

	default_config(&cfg);
	if (parse_args(argc, argv, &cfg) != 0) {
		return 2;
	}

	if (video_broker_worker_install_stop_signals(handle_signal) != 0) {
		fprintf(stderr, "[rdkx5_h26x_decode_bridge] stop signal setup failed err=%s\n", strerror(errno));
		return 2;
	}
	setvbuf(stderr, NULL, _IOLBF, 0);

	binary_fd = dup(STDOUT_FILENO);
	if (binary_fd < 0) {
		fprintf(stderr, "[rdkx5_h26x_decode_bridge] dup stdout failed\n");
		goto cleanup;
	}
	if (dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
		fprintf(stderr, "[rdkx5_h26x_decode_bridge] redirect stdout failed\n");
		goto cleanup;
	}
	out = fdopen(binary_fd, "wb");
	if (out == NULL) {
		fprintf(stderr, "[rdkx5_h26x_decode_bridge] fdopen failed\n");
		goto cleanup;
	}

	frame_size = (size_t)cfg.width * (size_t)cfg.height * 3U / 2U;
	image_size = frame_size < kDecodeBufferFloor ? kDecodeBufferFloor : frame_size;
	stream_capacity = image_size;
	image_buffer = (unsigned char *)malloc(image_size);
	stream_buffer = (unsigned char *)malloc(stream_capacity);
	if (image_buffer == NULL || stream_buffer == NULL) {
		fprintf(stderr, "[rdkx5_h26x_decode_bridge] buffer allocation failed\n");
		goto cleanup;
	}

	fprintf(stderr,
		"[rdkx5_h26x_decode_bridge] start input=stdin codec=%s output=%dx%d frames=%d protocol=nhd2\n",
		cfg.codec_type == SP_ENCODER_H265 ? "h265" : "h264",
		cfg.width, cfg.height, cfg.frames);

	rc = run_nhd2(&cfg, out, frame_size, image_buffer, &stream_buffer, &stream_capacity);

cleanup:
	free(stream_buffer);
	free(image_buffer);
	if (out != NULL) {
		fclose(out);
	} else if (binary_fd >= 0) {
		close(binary_fd);
	}
	return rc;
}
