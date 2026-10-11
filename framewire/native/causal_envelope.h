#ifndef VBCF_CAUSAL_ENVELOPE_H
#define VBCF_CAUSAL_ENVELOPE_H
#include <stddef.h>
#include <stdint.h>

enum { VBCF_HEADER_SIZE=448, VBCF_MAX_PAYLOAD=16*1024*1024 };
typedef struct {
    uint8_t kind,codec,surface,correlation;
    uint32_t flags,payload_bytes,width,height,stride_y,stride_uv;
    uint64_t source_epoch,origin_sequence,publication_epoch,delivery_id,capture_ns;
    int64_t media_pts;
    uint8_t namespace_id[16],graph[32],transform[32],payload_sha[32];
    char source[65],stream[65],process[65];
    uint64_t output_sequence; /* kind5 only; never a broker DeliveryRef */
} vbcf_header;

/* A trusted provider computes SHA256. Failure rejects the packet; no fallback.
 * Integrity never authenticates the sender. All APIs return0 on success. */
typedef int (*vbcf_sha256_fn)(void *context,const uint8_t *data,size_t size,uint8_t digest[32]);
int vbcf_validate(const vbcf_header *header);
int vbcf_parse_header(const uint8_t *bytes,size_t size,vbcf_header *header);
int vbcf_marshal_header(const vbcf_header *header,uint8_t bytes[VBCF_HEADER_SIZE]);
int vbcf_validate_payload(const vbcf_header *header,const uint8_t *payload,size_t size,vbcf_sha256_fn hash,void *context);

enum {
    VBCF_SURFACE_INVALID = -1,
    VBCF_SURFACE_ORIGIN_UNAVAILABLE = -2,
    VBCF_SURFACE_EPOCH_MISMATCH = -3,
    VBCF_SURFACE_HASH_FAILED = -4
};

/* Construct a source/processed NV12 surface from an admitted immutable layout
 * and the origin RETURNED WITH THESE PIXELS by the capture/decoder provider.
 * layout is a validated SourceDefinition(kind4), not a fictitious captured
 * frame. It contains codec/surface/geometry/source epoch/namespace/graph/
 * transform/source/process. Per-frame fields zero, payload SHA is SHA256(empty).
 * Epoch/sequence are never inferred from a current input, arrival clock or PTS.
 * The caller authenticates/admit-charges the layout, provider and borrowed
 * detached pixel buffer; no camera/driver lease may be held by a slow sender.
 * Hashing and header construction are synchronous and allocate no storage.
 * The caller keeps pixels immutable through hashing AND subsequent publication.
 * The output buffer must not alias layout or pixels. Failure zeros the entire
 * output header, so a previously valid header cannot be accidentally reused.
 * This function does not authenticate, write, queue or transfer ownership. */
int vbcf_marshal_surface_header(const vbcf_header *layout,
    uint64_t origin_epoch,uint64_t origin_sequence,
    const uint8_t *pixels,size_t size,vbcf_sha256_fn hash,void *context,
    uint8_t bytes[VBCF_HEADER_SIZE]);
#endif
