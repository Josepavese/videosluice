#ifndef FAPD_CAUSAL_CHUNKS_H
#define FAPD_CAUSAL_CHUNKS_H
#include "causal_envelope.h"
enum { FAPD_HEADER_SIZE=44, FAPD_MAX_PACKET=16384, FAPD_MAX_EXTENT=1<<28 };

/* Generic FAPD data syntax, shared by source VBCF and consumer manifests.
 * The manifest SHA identifies bytes, not an authenticated sender or domain. */
int fapd_encode(uint8_t *out,size_t capacity,const uint8_t manifest[32],
    uint32_t offset,const uint8_t *payload,size_t size,size_t *written);

typedef int (*vbcf_send_record_fn)(void *context,const uint8_t *packet,size_t size);
enum {
    VBCF_SEND_CONTRACT=1,VBCF_SEND_MANIFEST_HASH=2,VBCF_SEND_HEADER=3,
    VBCF_SEND_CHUNK=4,VBCF_SEND_DATA=5,VBCF_SEND_COMPLETE=6
};
typedef struct {
    unsigned stage;
    uint32_t bytes_sent,records_sent,attempted_offset;
    int attempted; /* Any attempted send can have an uncertain outcome. */
} vbcf_send_progress;

/* Synchronously publish one VBCF source header followed by sequential FAPD
 * records. All buffers are caller-owned, admitted and non-overlapping; pixels
 * stay detached and immutable through return. No allocation, clock, queue or
 * retry. The send port MUST hold one total deadline for the entire operation
 * and enforce peer identity/cancellation. Failure after any send retires that
 * transfer/channel at the owning layer: never resume using guessed delivery.
 * Header payload hash was computed at capture publication; receiver verifies
 * final bytes. This function does NOT recompute full-frame SHA a second time. */
int vbcf_send_source_packet(const uint8_t header[VBCF_HEADER_SIZE],const uint8_t *pixels,size_t size,
    uint8_t *workspace,size_t workspace_size,vbcf_sha256_fn hash,void *hash_context,
    vbcf_send_record_fn send,void *send_context,vbcf_send_progress *progress);
#endif
