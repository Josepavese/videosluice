#include "capture_correlation.h"
#include <assert.h>
#include <stdio.h>

int main(void) {
 vb_capture_correlation state = {0};
 vb_capture_ticket one, two, three, rejected;
 vb_capture_origin origin;
 assert(vb_capture_init(&state, 17, 2) == VB_CAPTURE_OK);
 assert(vb_capture_init(&state, 18, 2) == VB_CAPTURE_INVALID);
 assert(vb_capture_admit(&state, 100, &one) == VB_CAPTURE_OK);
 assert(vb_capture_admit(&state, 101, &two) == VB_CAPTURE_OK);
 /* Newest submitted input is deliberately NOT the next completed picture. */
 assert(vb_capture_resolve(&state, two, &origin) == VB_CAPTURE_OK && origin.sequence == 101);
 assert(vb_capture_admit(&state, 102, &three) == VB_CAPTURE_OK);
 assert(vb_capture_resolve(&state, one, &origin) == VB_CAPTURE_OK && origin.sequence == 100);
 assert(vb_capture_resolve(&state, one, &origin) == VB_CAPTURE_UNAVAILABLE && !origin.sequence);
 assert(vb_capture_resolve(&state, (vb_capture_ticket){17, 0}, &origin) == VB_CAPTURE_UNAVAILABLE);
 assert(vb_capture_resolve(&state, (vb_capture_ticket){18, three.tag}, &origin) == VB_CAPTURE_UNAVAILABLE);
 assert(state.used == 1);
 assert(vb_capture_admit(&state, 103, &one) == VB_CAPTURE_OK);
 assert(vb_capture_admit(&state, 104, &rejected) == VB_CAPTURE_CAPACITY && !rejected.tag);
 assert(state.used == 2 && state.last_sequence == 103);
 assert(vb_capture_cancel(&state, one) == VB_CAPTURE_OK);
 assert(vb_capture_resolve(&state, one, &origin) == VB_CAPTURE_UNAVAILABLE);
 assert(vb_capture_admit(&state, 104, &two) == VB_CAPTURE_OK && two.tag > one.tag);
 assert(vb_capture_admit(&state, 103, &rejected) == VB_CAPTURE_INVALID);
 vb_capture_retire(&state);
 assert(vb_capture_resolve(&state, three, &origin) == VB_CAPTURE_RETIRED && !origin.sequence);
 assert(vb_capture_admit(&state, 105, &rejected) == VB_CAPTURE_RETIRED);
 assert(vb_capture_init(&state, 18, 2) == VB_CAPTURE_INVALID);

 vb_capture_correlation boundary = {0};
 assert(vb_capture_init(&boundary, UINT64_MAX, VB_CAPTURE_CORRELATION_MAX) == VB_CAPTURE_OK);
 boundary.last_tag = UINT64_MAX - 1; /* deterministic boundary fixture */
 assert(vb_capture_admit(&boundary, UINT64_MAX-1, &one) == VB_CAPTURE_OK && one.tag == UINT64_MAX);
 assert(vb_capture_resolve(&boundary, one, &origin) == VB_CAPTURE_OK && origin.sequence == UINT64_MAX-1);
 assert(vb_capture_admit(&boundary, UINT64_MAX, &two) == VB_CAPTURE_RETIRED && !two.tag);

 vb_capture_correlation full = {0};
 vb_capture_ticket tickets[VB_CAPTURE_CORRELATION_MAX];
 assert(vb_capture_init(&full, 33, VB_CAPTURE_CORRELATION_MAX) == VB_CAPTURE_OK);
 for (size_t i=0;i<VB_CAPTURE_CORRELATION_MAX;i++) assert(vb_capture_admit(&full, (UINT64_C(1)<<53)+i+1, &tickets[i]) == VB_CAPTURE_OK);
 for (size_t i=VB_CAPTURE_CORRELATION_MAX;i>0;i--) {
  assert(vb_capture_resolve(&full, tickets[i-1], &origin) == VB_CAPTURE_OK);
  assert(origin.sequence == (UINT64_C(1)<<53)+i);
 }
 assert(full.used == 0);
 puts("capture correlation exact/reordered/stale/capacity/exhaustion PASS");
 return 0;
}
