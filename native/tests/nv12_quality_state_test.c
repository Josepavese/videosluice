#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../nv12_quality_state.h"

static int fsync_calls;

int fsync(int fd) {
	(void)fd;
	fsync_calls++;
	errno = EIO;
	return -1;
}

int main(void) {
	char dir_template[] = "/tmp/video-broker-quality-state-XXXXXX";
	char path[512];
	char payload[32768];
	unsigned char source_frame[16 * 12 * 3 / 2];
	unsigned char processed_frame[16 * 12 * 3 / 2];
	unsigned char analysis_frame[8 * 8 * 3 / 2];
	video_broker_nv12_quality_measure_t source;
	video_broker_nv12_quality_measure_t processed;
	video_broker_nv12_quality_measure_t analysis;
	struct stat state_stat;
	FILE *fp;
	size_t bytes;
	char *dir = mkdtemp(dir_template);

	memset(source_frame, 118, sizeof(source_frame));
	memset(processed_frame, 136, sizeof(processed_frame));
	memset(analysis_frame, 112, sizeof(analysis_frame));
	source = video_broker_nv12_quality_measure(source_frame, 16, 12, 1);
	processed = video_broker_nv12_quality_measure(processed_frame, 16, 12, 1);
	analysis = video_broker_nv12_quality_measure(analysis_frame, 8, 8, 1);

	if (dir == NULL || snprintf(path, sizeof(path), "%s/quality.json", dir) >= (int)sizeof(path)) {
		perror("quality state test setup");
		return 1;
	}
	if (video_broker_nv12_quality_write_state(
			path, 42, 8,
			&source, 16, 12,
			&processed, 16, 12,
			&analysis, 8, 8,
			"mjpeg", "normal") != 0) {
		perror("quality state write");
		return 1;
	}
	if (fsync_calls != 0) {
		fprintf(stderr, "ephemeral quality state invoked fsync calls=%d\n", fsync_calls);
		return 1;
	}
	if (stat(path, &state_stat) != 0 || (state_stat.st_mode & 0777) != 0640) {
		perror("quality state mode");
		return 1;
	}
	fp = fopen(path, "r");
	if (fp == NULL) {
		perror("quality state read");
		return 1;
	}
	bytes = fread(payload, 1, sizeof(payload) - 1U, fp);
	payload[bytes] = '\0';
	if (fclose(fp) != 0 ||
	    strstr(payload, "\"schema\":\"video-broker.quality.v3\"") == NULL ||
	    strstr(payload, "\"frame\":42") == NULL ||
	    strstr(payload, "\"source\":{\"width\":16") == NULL ||
	    strstr(payload, "\"processed\":{\"width\":16") == NULL ||
	    strstr(payload, "\"analysis\":{\"width\":8") == NULL ||
	    strstr(payload, "\"spatial\":{\"columns\":4,\"rows\":3,\"regions\":[") == NULL ||
	    strstr(payload, "\"x\":3,\"y\":2") == NULL ||
	    strstr(payload, "\"capture\":{\"format\":\"mjpeg\"") == NULL) {
		fprintf(stderr, "quality state payload contract mismatch: %s\n", payload);
		return 1;
	}
	if (unlink(path) != 0 || rmdir(dir) != 0) {
		perror("quality state cleanup");
		return 1;
	}
	puts("OK_NATIVE_NV12_QUALITY_STATE_EPHEMERAL_ATOMIC");
	return 0;
}
