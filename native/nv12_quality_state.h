#ifndef VIDEO_BROKER_NV12_QUALITY_STATE_H
#define VIDEO_BROKER_NV12_QUALITY_STATE_H

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "nv12_quality_measure.h"

#define VIDEO_BROKER_QUALITY_STATE_SCHEMA "video-broker.quality.v3"

static inline int video_broker_nv12_quality_write_metrics(
	FILE *fp,
	const video_broker_nv12_quality_measure_t *measure,
	int width,
	int height
) {
	if (fp == NULL || measure == NULL) {
		errno = EINVAL;
		return -1;
	}
	if (fprintf(fp,
		"{\"width\":%d,\"height\":%d,\"samples\":%llu,\"luma\":%.6Lf,\"content_luma\":%.6Lf,"
		"\"laplacian_variance\":%.6Lf,\"edge\":%.6Lf,\"black_ratio\":%.9Lf,\"white_ratio\":%.9Lf,"
		"\"signal_informative\":%s,\"spatial\":{\"columns\":%d,\"rows\":%d,\"regions\":[",
		width,
		height,
		(unsigned long long)measure->samples,
		measure->luma,
		measure->content_luma,
		measure->lap_var,
		measure->edge,
		measure->black_ratio,
		measure->white_ratio,
		video_broker_nv12_quality_is_informative(measure) ? "true" : "false",
		VIDEO_BROKER_QUALITY_GRID_COLUMNS,
		VIDEO_BROKER_QUALITY_GRID_ROWS) < 0) {
		return -1;
	}
	for (int index = 0; index < VIDEO_BROKER_QUALITY_GRID_REGIONS; ++index) {
		const video_broker_nv12_quality_region_t *region = &measure->regions[index];
		if (index > 0 && fputc(',', fp) == EOF) {
			return -1;
		}
		if (fprintf(fp,
			"{\"x\":%d,\"y\":%d,\"samples\":%llu,\"luma\":%.6Lf,\"content_luma\":%.6Lf,"
			"\"laplacian_variance\":%.6Lf,\"edge\":%.6Lf,\"black_ratio\":%.9Lf,\"white_ratio\":%.9Lf,"
			"\"signal_informative\":%s}",
			index % VIDEO_BROKER_QUALITY_GRID_COLUMNS,
			index / VIDEO_BROKER_QUALITY_GRID_COLUMNS,
			(unsigned long long)region->samples,
			region->luma,
			region->content_luma,
			region->lap_var,
			region->edge,
			region->black_ratio,
			region->white_ratio,
			video_broker_nv12_quality_region_is_informative(region) ? "true" : "false") < 0) {
			return -1;
		}
	}
	return fputs("]}}", fp) == EOF ? -1 : 0;
}

/*
 * Publish one backend-independent quality snapshot. The caller owns sampling;
 * this helper owns only the strict schema and the atomic file commit. Quality
 * state is an ephemeral control-plane observation (canonically under /run),
 * not durable product data: forcing storage writeback here would stall the
 * capture thread without improving the atomic-reader contract.
 */
static inline int video_broker_nv12_quality_write_state(
	const char *path,
	uint64_t frame,
	int stride,
	const video_broker_nv12_quality_measure_t *source,
	int source_width,
	int source_height,
	const video_broker_nv12_quality_measure_t *processed,
	int processed_width,
	int processed_height,
	const video_broker_nv12_quality_measure_t *analysis,
	int analysis_width,
	int analysis_height,
	const char *format,
	const char *orientation
) {
	struct timespec ts;
	char tmp_path[PATH_MAX];
	FILE *fp = NULL;
	long long timestamp_ms;
	int saved_errno = 0;

	if (path == NULL || path[0] == '\0' || source == NULL || processed == NULL || analysis == NULL ||
	    format == NULL || orientation == NULL || stride <= 0) {
		errno = EINVAL;
		return -1;
	}
	if (snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.%ld", path, (long)getpid()) >= (int)sizeof(tmp_path)) {
		errno = ENAMETOOLONG;
		return -1;
	}
	if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
		return -1;
	}
	timestamp_ms = ((long long)ts.tv_sec * 1000LL) + ((long long)ts.tv_nsec / 1000000LL);
	fp = fopen(tmp_path, "w");
	if (fp == NULL) {
		return -1;
	}
	if (fchmod(fileno(fp), 0640) != 0 ||
	    fprintf(fp,
		"{\"schema\":\"%s\",\"timestamp_unix_ms\":%lld,\"frame\":%llu,\"sample_stride\":%d,\"source\":",
		VIDEO_BROKER_QUALITY_STATE_SCHEMA,
		timestamp_ms,
		(unsigned long long)frame,
		stride) < 0 ||
	    video_broker_nv12_quality_write_metrics(fp, source, source_width, source_height) != 0 ||
	    fputs(",\"processed\":", fp) == EOF ||
	    video_broker_nv12_quality_write_metrics(fp, processed, processed_width, processed_height) != 0 ||
	    fputs(",\"analysis\":", fp) == EOF ||
	    video_broker_nv12_quality_write_metrics(fp, analysis, analysis_width, analysis_height) != 0 ||
	    fprintf(fp, ",\"capture\":{\"format\":\"%s\",\"orientation\":\"%s\"}}\n", format, orientation) < 0 ||
	    fflush(fp) != 0) {
		saved_errno = errno != 0 ? errno : EIO;
	}
	if (fclose(fp) != 0 && saved_errno == 0) {
		saved_errno = errno != 0 ? errno : EIO;
	}
	if (saved_errno == 0 && rename(tmp_path, path) != 0) {
		saved_errno = errno;
	}
	if (saved_errno != 0) {
		(void)unlink(tmp_path);
		errno = saved_errno;
		return -1;
	}
	return 0;
}

#endif
