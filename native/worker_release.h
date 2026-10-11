#ifndef VIDEO_BROKER_WORKER_RELEASE_H
#define VIDEO_BROKER_WORKER_RELEASE_H

#include <stdbool.h>

/* Current native-worker exit contract, consumed only after the exact child is
 * reaped. These are NOT camera readiness or a memory-footprint attestation.
 * 64 preserves an operation failure while proving checked resource teardown;
 * 65, a signal, or any other status must never be treated as reclaimed SDK/DMA.
 */
enum {
	VIDEO_BROKER_WORKER_RELEASED = 0,
	VIDEO_BROKER_WORKER_FAILED_RELEASED = 64,
	VIDEO_BROKER_WORKER_RELEASE_UNCONFIRMED = 65,
};

static inline int video_broker_worker_exit_status(int operation_status, bool release_confirmed) {
	if (!release_confirmed) return VIDEO_BROKER_WORKER_RELEASE_UNCONFIRMED;
	return operation_status == 0 ? VIDEO_BROKER_WORKER_RELEASED : VIDEO_BROKER_WORKER_FAILED_RELEASED;
}

/* A successful intermediate call is not successful worker completion. Error
 * paths jump to cleanup; only the normal loop exit completes the operation. */
static inline int video_broker_worker_operation_status(int last_result, bool loop_completed) {
	return loop_completed ? 0 : (last_result == 0 ? -1 : last_result);
}

#endif
