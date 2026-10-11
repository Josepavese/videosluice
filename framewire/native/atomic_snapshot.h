#ifndef VBSC_ATOMIC_SNAPSHOT_H
#define VBSC_ATOMIC_SNAPSHOT_H
#include "causal_snapshot.h"
#include <stdatomic.h>

/* Native provider primitive, not wire ABI. Mapping lifetime is owned outside
 * this object; do not unmap concurrently. The caller admits lock-free64-bit
 * atomics and aligned allocation. All processes use this same atomic access
 * width: ordinary mmap slicing/memcpy is NOT a supported reader. */
typedef struct { _Atomic uint64_t *words;size_t bytes; } vbsc_atomic_region;
int vbsc_atomic_bind(void *memory,size_t bytes,vbsc_atomic_region *out);
/* Exclusive unpublished allocation only, before any concurrent access. */
int vbsc_atomic_initialize(vbsc_atomic_region *region);
/* Single writer. Sequence never wraps; expected mapping never reused.
 * Input packet is immutable private memory, not the shared region. */
int vbsc_atomic_publish(vbsc_atomic_region *region,const vbsc_mapping *expected,
    const uint8_t *packet,size_t packet_size,vbcf_sha256_fn hash,void *context);
/* Construct one envelope from source-owned immutable pixels. Hashes exactly
 * once and commits header+payload without an additional contiguous copy. */
int vbsc_atomic_produce(vbsc_atomic_region *region,const vbsc_mapping *expected,
    const vbcf_header *metadata,const uint8_t *pixels,size_t size,vbcf_sha256_fn hash,void *context);
typedef int (*vbsc_commit_guard_fn)(void *);
/* The guard runs after hashing and before the final even counter. Rejection
 * after copying leaves an odd, unreadable slot; its owner must retire it.
 * Guards must be bounded and must not access the shared payload ordinarily. */
int vbsc_atomic_produce_guarded(vbsc_atomic_region *,const vbsc_mapping *,
    const vbcf_header *,const uint8_t *,size_t,vbcf_sha256_fn,void *,vbsc_commit_guard_fn);
/* One bounded attempt, no spin/retry, allocation, callback under driver lease
 * or reader writes. Buffer private, preallocated and included in admission.
 * On error used=0; private destination content is undefined, not a frame. */
int vbsc_atomic_snapshot(const vbsc_atomic_region *region,const vbsc_mapping *expected,
    uint8_t *packet,size_t capacity,size_t *used,vbcf_header *header,
    vbcf_sha256_fn hash,void *context,uint64_t after_serial,uint64_t *serial);
/* Same bounded atomic snapshot, but payload integrity is pending. This port
 * is only for a private staging Store that enforces the complete expected
 * SHA before exposing any pixels. Never use it as a verified frame reader. */
int vbsc_atomic_stage(const vbsc_atomic_region *region,const vbsc_mapping *expected,
    uint8_t *packet,size_t capacity,size_t *used,vbcf_header *header,
    uint64_t after_serial,uint64_t *serial);
#endif
