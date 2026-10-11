/* Hardware-only bootstrap probe for the RDK X5 persistent encoder. */
#define _POSIX_C_SOURCE 200809L

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "../rdkx5_persistent_encoder.h"

enum {
	kWidth = 1280,
	kHeight = 720,
	kFPS = 30,
	kBitrateKbps = 4096,
	kMediumBitrateKbps = 3276,
	kLowFPS = 20,
	kLowBitrateKbps = 2457,
	kMaxProbeFrames = 8,
};

static size_t start_code_length(const unsigned char *data, size_t size, size_t offset) {
	if (offset + 3 <= size && data[offset] == 0 && data[offset + 1] == 0 &&
	    data[offset + 2] == 1) {
		return 3;
	}
	if (offset + 4 <= size && data[offset] == 0 && data[offset + 1] == 0 &&
	    data[offset + 2] == 0 && data[offset + 3] == 1) {
		return 4;
	}
	return 0;
}

static bool h265_access_unit_is_bootstrap(const unsigned char *data, size_t size) {
	bool vps = false;
	bool sps = false;
	bool pps = false;
	bool idr = false;
	printf("nal_types=");
	for (size_t offset = 0; offset + 4 < size; ++offset) {
		size_t prefix = start_code_length(data, size, offset);
		unsigned int nal_type;
		if (prefix == 0 || offset + prefix >= size) {
			continue;
		}
		nal_type = (data[offset + prefix] >> 1) & 0x3fU;
		printf("%u,", nal_type);
		vps |= nal_type == 32;
		sps |= nal_type == 33;
		pps |= nal_type == 34;
		idr |= nal_type == 19 || nal_type == 20;
		offset += prefix;
	}
	printf(" bootstrap=%s\n", vps && sps && pps && idr ? "true" : "false");
	return vps && sps && pps && idr;
}

static int await_bootstrap(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	const unsigned char *frame,
	size_t frame_size,
	unsigned char *stream,
	size_t stream_capacity,
	const char *phase
) {
	for (int index = 0; index < kMaxProbeFrames; ++index) {
		uint64_t epoch = 0;
		int bitrate_kbps = 0;
		int encoded = video_broker_rdkx5_persistent_encoder_encode(
			encoder, frame, frame_size, stream, stream_capacity, &epoch, &bitrate_kbps);
		if (encoded < 0) {
			fprintf(stderr, "phase=%s frame=%d encode=failed\n", phase, index + 1);
			return -1;
		}
		printf("phase=%s frame=%d bytes=%d epoch=%llu bitrate_kbps=%d ",
			phase, index + 1, encoded, (unsigned long long)epoch, bitrate_kbps);
		if (encoded > 0 && h265_access_unit_is_bootstrap(stream, (size_t)encoded)) {
			return 0;
		}
		if (encoded == 0) {
			printf("nal_types= bootstrap=false\n");
		}
	}
	fprintf(stderr, "phase=%s bootstrap=missing frames=%d\n", phase, kMaxProbeFrames);
	return -1;
}

static int await_output_at_epoch(
	video_broker_rdkx5_persistent_encoder_t *encoder,
	const unsigned char *frame,
	size_t frame_size,
	unsigned char *stream,
	size_t stream_capacity,
	uint64_t expected_epoch,
	int expected_bitrate_kbps,
	const char *phase,
	bool require_bootstrap
) {
	for (int index = 0; index < kMaxProbeFrames; ++index) {
		uint64_t epoch = 0;
		int bitrate_kbps = 0;
		int encoded = video_broker_rdkx5_persistent_encoder_encode(
			encoder, frame, frame_size, stream, stream_capacity, &epoch, &bitrate_kbps);
		printf("phase=%s frame=%d bytes=%d epoch=%llu bitrate_kbps=%d ",
			phase, index + 1, encoded, (unsigned long long)epoch, bitrate_kbps);
		if (encoded < 0 || epoch != expected_epoch || bitrate_kbps != expected_bitrate_kbps) {
			fprintf(stderr, "phase=%s random_access_contract=invalid\n", phase);
			return -1;
		}
		if (encoded > 0) {
			bool bootstrap = h265_access_unit_is_bootstrap(stream, (size_t)encoded);
			/* Continuous rate updates retain decoder history and need fresh
			 * media at the same epoch, not an immediate decoder reset. A new
			 * publication, explicit recovery and activation require full IDR. */
			if (bootstrap || !require_bootstrap) {
				return 0;
			}
		} else {
			printf("nal_types= bootstrap=false\n");
		}
	}
	fprintf(stderr, "phase=%s random_access=missing frames=%d\n", phase, kMaxProbeFrames);
	return -1;
}

int main(void) {
	const size_t frame_size = (size_t)kWidth * (size_t)kHeight * 3U / 2U;
	video_broker_rdkx5_persistent_encoder_t encoder = {0};
	unsigned char *frame = calloc(1, frame_size);
	unsigned char *stream = malloc(frame_size);
	int rc = 1;
	if (frame == NULL || stream == NULL) {
		fprintf(stderr, "allocation=failed\n");
		goto cleanup_buffers;
	}
	for (size_t index = (size_t)kWidth * (size_t)kHeight; index < frame_size; ++index) {
		frame[index] = 128;
	}
	if (video_broker_rdkx5_persistent_encoder_init(
			&encoder, VIDEO_BROKER_RDKX5_ENCODER_H265,
			VIDEO_BROKER_RDKX5_RATE_CONTROL_REALTIME_CBR,
			0, kWidth, kHeight, kFPS, kBitrateKbps) != 0) {
		fprintf(stderr, "encoder_init=failed\n");
		goto cleanup_buffers;
	}
	if (video_broker_rdkx5_persistent_encoder_set_control(
			&encoder, true, kFPS, kBitrateKbps) != VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED ||
	    await_bootstrap(&encoder, frame, frame_size, stream, frame_size, "activation") != 0 ||
	    video_broker_rdkx5_persistent_encoder_request_idr(&encoder) != 0 ||
	    await_output_at_epoch(
			&encoder, frame, frame_size, stream, frame_size, 1, kBitrateKbps,
			"publication-after-private-warmup", true) != 0 ||
	    video_broker_rdkx5_persistent_encoder_set_control(
			&encoder, true, kFPS, kMediumBitrateKbps) != VIDEO_BROKER_RDKX5_ENCODER_CONTROL_RATE_CHANGED ||
	    encoder.epoch != 1 ||
	    await_output_at_epoch(
			&encoder, frame, frame_size, stream, frame_size, 1, kMediumBitrateKbps,
			"rate-medium", false) != 0 ||
	    video_broker_rdkx5_persistent_encoder_set_control(
			&encoder, true, kLowFPS, kLowBitrateKbps) != VIDEO_BROKER_RDKX5_ENCODER_CONTROL_RATE_CHANGED ||
	    encoder.epoch != 1 ||
	    await_output_at_epoch(
			&encoder, frame, frame_size, stream, frame_size, 1, kLowBitrateKbps,
			"rate-low", false) != 0 ||
	    video_broker_rdkx5_persistent_encoder_request_idr(&encoder) != 0 ||
	    await_output_at_epoch(
			&encoder, frame, frame_size, stream, frame_size, 1, kLowBitrateKbps,
			"recovery", true) != 0 ||
	    video_broker_rdkx5_persistent_encoder_set_control(
			&encoder, false, kLowFPS, kLowBitrateKbps) != VIDEO_BROKER_RDKX5_ENCODER_CONTROL_UNCHANGED ||
	    video_broker_rdkx5_persistent_encoder_set_control(
			&encoder, true, kLowFPS, kLowBitrateKbps) != VIDEO_BROKER_RDKX5_ENCODER_CONTROL_ACTIVATED ||
	    await_output_at_epoch(
			&encoder, frame, frame_size, stream, frame_size, 2, kLowBitrateKbps,
			"demand-resume", true) != 0) {
		video_broker_rdkx5_persistent_encoder_destroy(&encoder);
		goto cleanup_buffers;
	}
	if (video_broker_rdkx5_persistent_encoder_destroy(&encoder) != 0) goto cleanup_buffers;
	printf("RDKX5_PERSISTENT_ENCODER_BOOTSTRAP_OK\n");
	rc = 0;

cleanup_buffers:
	free(stream);
	free(frame);
	if (video_broker_rdkx5_encoder_resources_quarantined()) _Exit(1);
	return rc;
}
