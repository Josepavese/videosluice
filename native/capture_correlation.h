#ifndef VIDEO_BROKER_CAPTURE_CORRELATION_H
#define VIDEO_BROKER_CAPTURE_CORRELATION_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>

/* Generic decoder/transform correlation, not an accelerator implementation.
 * Callers serialize access and admit sizeof(vb_capture_correlation) first.
 * epoch is a non-reusable generation supplied by the trusted source lifecycle.
 * Reserve BEFORE submitting input; carry ticket through an attested exact
 * provider tag channel. PTS proximity, output order and current input are NEVER
 * evidence. Missing/unknown tags explicitly return UNAVAILABLE.
 * One state belongs to one provider execution. Retire before teardown/switch;
 * never reinitialize it to rehabilitate outstanding output from that execution.
 * Retiring this metadata never releases driver buffers or native-resource quota;
 * those remain owned by the provider lifecycle until physical release completes.
 * No allocations, payload copies, clocks, OS APIs, locks or hidden queue.
 */
enum { VB_CAPTURE_CORRELATION_MAX = 64 };
enum {
 VB_CAPTURE_OK = 0,
 VB_CAPTURE_UNAVAILABLE = 1,
 VB_CAPTURE_CAPACITY = 2,
 VB_CAPTURE_RETIRED = 3,
 VB_CAPTURE_INVALID = 4
};
typedef struct { uint64_t epoch, tag; } vb_capture_ticket;
typedef struct { uint64_t epoch, sequence; } vb_capture_origin;
typedef struct { uint64_t tag, sequence; } vb_capture_slot;
typedef struct {
 uint64_t epoch, last_tag, last_sequence;
 size_t limit, used;
 int initialized, retired;
 vb_capture_slot slots[VB_CAPTURE_CORRELATION_MAX];
} vb_capture_correlation;

/* Requires a zero-initialized, newly admitted state. Epoch authenticity and
 * namespace/source binding are enforced by the enclosing bootstrap/domain. */
static inline int vb_capture_init(vb_capture_correlation *state, uint64_t epoch, size_t limit) {
 if (!state || state->initialized || !epoch || !limit || limit > VB_CAPTURE_CORRELATION_MAX) return VB_CAPTURE_INVALID;
 memset(state, 0, sizeof(*state));
 state->epoch = epoch; state->limit = limit; state->initialized = 1;
 return VB_CAPTURE_OK;
}

static inline void vb_capture_retire(vb_capture_correlation *state) {
 if (!state || !state->initialized) return;
 state->retired = 1; state->used = 0;
 memset(state->slots, 0, sizeof(state->slots));
}

static inline int vb_capture_admit(vb_capture_correlation *state, uint64_t sequence, vb_capture_ticket *ticket) {
 if (ticket) memset(ticket, 0, sizeof(*ticket));
 if (!state || !ticket || !state->initialized || !sequence) return VB_CAPTURE_INVALID;
 if (state->retired) return VB_CAPTURE_RETIRED;
 if (sequence <= state->last_sequence) return VB_CAPTURE_INVALID;
 if (state->last_tag == UINT64_MAX) { vb_capture_retire(state); return VB_CAPTURE_RETIRED; }
 if (state->used == state->limit) return VB_CAPTURE_CAPACITY;
 for (size_t i = 0; i < state->limit; ++i) {
  if (!state->slots[i].tag) {
   uint64_t tag = ++state->last_tag;
   state->last_sequence = sequence;
   state->slots[i].tag = tag; state->slots[i].sequence = sequence; state->used++;
   ticket->epoch = state->epoch; ticket->tag = tag;
   return VB_CAPTURE_OK;
  }
 }
 return VB_CAPTURE_INVALID;
}

/* Exactly one completed picture consumes its ticket. A duplicate, a zero tag,
 * an old generation or a dropped submission cannot steal another slot. */
static inline int vb_capture_resolve(vb_capture_correlation *state, vb_capture_ticket ticket, vb_capture_origin *origin) {
 if (origin) memset(origin, 0, sizeof(*origin));
 if (!state || !origin || !state->initialized) return VB_CAPTURE_INVALID;
 if (state->retired) return VB_CAPTURE_RETIRED;
 if (!ticket.tag || ticket.epoch != state->epoch) return VB_CAPTURE_UNAVAILABLE;
 for (size_t i = 0; i < state->limit; ++i) {
  if (state->slots[i].tag == ticket.tag) {
   origin->epoch = state->epoch; origin->sequence = state->slots[i].sequence;
   memset(&state->slots[i], 0, sizeof(state->slots[i])); state->used--;
   return VB_CAPTURE_OK;
  }
 }
 return VB_CAPTURE_UNAVAILABLE;
}

/* Use only after proven non-submission/cancellation. Ambiguous vendor send
 * must retain its slot until exact output or full execution retirement. */
static inline int vb_capture_cancel(vb_capture_correlation *state, vb_capture_ticket ticket) {
 vb_capture_origin ignored;
 return vb_capture_resolve(state, ticket, &ignored);
}
#endif
