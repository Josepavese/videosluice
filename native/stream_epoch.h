#ifndef VIDEO_BROKER_STREAM_EPOCH_H
#define VIDEO_BROKER_STREAM_EPOCH_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Owned by the publication, not by a replaceable encoder. Private warmup
 * never advances this clock; observe only an access unit being published. */
typedef struct {
	uint64_t value;
	uint64_t owner_epoch;
	uint64_t encoder_epoch;
	bool boundary_pending;
} video_broker_stream_epoch_t;

static inline void video_broker_stream_epoch_boundary(video_broker_stream_epoch_t *clock) {
	clock->boundary_pending = true;
}

/* Return 1 for a new public marker, 0 for continuous output, -1 fail-closed.
 * Role cutover/rollback explicitly invalidates the binding even if a failed
 * candidate's private owner/encoder counters are reused before another frame. */
static inline int video_broker_stream_epoch_observe(
	video_broker_stream_epoch_t *clock, uint64_t owner_epoch, uint64_t encoder_epoch) {
	bool same_owner;
	if (clock == NULL || owner_epoch == 0 || encoder_epoch == 0) return -1;
	same_owner = !clock->boundary_pending && owner_epoch == clock->owner_epoch;
	if (same_owner && encoder_epoch < clock->encoder_epoch) return -1;
	if (same_owner && encoder_epoch == clock->encoder_epoch) return 0;
	if (clock->value == UINT64_MAX) return -1;
	clock->value++;
	clock->owner_epoch = owner_epoch;
	clock->encoder_epoch = encoder_epoch;
	clock->boundary_pending = false;
	return 1;
}

#endif
