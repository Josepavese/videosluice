#ifndef VBSC_OWNED_BRIDGE_H
#define VBSC_OWNED_BRIDGE_H
#include <stddef.h>
#include <stdint.h>
#include "../../native/causal_snapshot.h"
typedef struct vbsc_owned vbsc_owned;
enum {VBSC_CREATE_VALIDATION=1,VBSC_CREATE_HEAP,VBSC_CREATE_DESCRIPTOR,VBSC_CREATE_SIZE,
    VBSC_CREATE_MAP,VBSC_CREATE_ATOMIC,VBSC_CREATE_SEALS};
/* Failed creation returns a retained owner iff cleanup is unconfirmed. */
vbsc_owned *vbsc_owned_create(const uint8_t namespace_id[16],uint64_t generation,uint64_t capacity,int *stage,int *cleanup);
int vbsc_owned_destroy(vbsc_owned *owned);
int vbsc_owned_publish(vbsc_owned *owned,const uint8_t *packet,size_t size);
int vbsc_owned_snapshot(vbsc_owned *owned,uint8_t *packet,size_t size,size_t *used);
size_t vbsc_hash_workspace_bytes(size_t payload_bytes);
#endif
