#ifndef VBCF_CAUSAL_SNAPSHOT_H
#define VBCF_CAUSAL_SNAPSHOT_H
#include "causal_envelope.h"

enum {VBSC_SIZE=64, VBSC_OK=0, VBSC_INVALID=-1, VBSC_BUSY=-2, VBSC_MAPPING=-3};
typedef struct {
    uint8_t namespace_id[16];
    uint64_t generation,capacity;
} vbsc_mapping;
typedef struct {
    uint64_t begin,capacity,used,generation;
    uint8_t namespace_id[16];
    uint64_t end;
} vbsc_control;

int vbsc_parse(const uint8_t *bytes,size_t size,vbsc_control *out);
int vbsc_marshal(const vbsc_control *control,uint8_t bytes[VBSC_SIZE]);
/* Structural validation ONLY, for a broker-owned staging arena. The returned
 * payload is NOT verified and MUST NOT be exposed as a frame or ROI until its
 * complete SHA matches header.payload_sha. Mapping/serial/header checks are
 * identical to validate_snapshot; the expected producer SHA is preserved. */
int vbsc_validate_staging(
    const uint8_t *before,size_t before_size,
    const uint8_t *packet,size_t packet_size,
    const uint8_t *after,size_t after_size,
    const vbsc_mapping *expected,vbcf_header *header,const uint8_t **payload);
/* Inputs are immutable COPIES acquired by a platform provider. No atomic
 * shared-memory loads, ordering, retries, mapping lifetime or authentication
 * are provided here. Expected allocation comes from trusted middleware.
 * Returned payload borrows packet lifetime. Outputs must not alias inputs. */
int vbsc_validate_snapshot(
    const uint8_t *before,size_t before_size,
    const uint8_t *packet,size_t packet_size,
    const uint8_t *after,size_t after_size,
    const vbsc_mapping *expected,vbcf_sha256_fn hash,void *context,
    vbcf_header *header,const uint8_t **payload);
#endif
