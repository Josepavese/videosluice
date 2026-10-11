//go:build linux && cgo

#define _GNU_SOURCE
#include "bridge.h"
#include "../../native/causal_envelope.c"
#include "../../native/causal_snapshot.c"
#include "../../native/atomic_snapshot.c"
#include <fcntl.h>
#include "../../../internal/framehash/frame_sha256_linux.h"
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

struct vbsc_owned {
    vbsc_atomic_region region;vbsc_mapping mapping;
    void *memory;size_t bytes;int fd,release_error;
};
static int owned_hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]) {
    (void)ctx;return vb_frame_sha256_sum(p,n,out);
}

size_t vbsc_hash_workspace_bytes(size_t n) { return vb_frame_sha256_workspace_bytes(n); }
static vbsc_owned *rollback_create(vbsc_owned *o,int *cleanup) {
    *cleanup=vbsc_owned_destroy(o);
    return *cleanup?o:NULL;
}
vbsc_owned *vbsc_owned_create(const uint8_t ns[16],uint64_t generation,uint64_t capacity,int *stage,int *cleanup) {
    if(!stage||!cleanup)return NULL;
    *cleanup=0;
    *stage=VBSC_CREATE_VALIDATION;
    if(!ns||!generation||capacity<VBCF_HEADER_SIZE||capacity>VBCF_HEADER_SIZE+VBCF_MAX_PAYLOAD)return NULL;
    *stage=VBSC_CREATE_HEAP;
    vbsc_owned *o=calloc(1,sizeof(*o));if(!o)return NULL;
    *stage=VBSC_CREATE_DESCRIPTOR;
    o->fd=memfd_create("vbsc-surface",MFD_CLOEXEC|MFD_ALLOW_SEALING);
    if(o->fd<0){free(o);return NULL;}
    size_t bytes=VBSC_SIZE+(size_t)((capacity+7)/8)*8;
    *stage=VBSC_CREATE_SIZE;
    if(ftruncate(o->fd,(off_t)bytes))return rollback_create(o,cleanup);
    *stage=VBSC_CREATE_MAP;
    void *memory=mmap(NULL,bytes,PROT_READ|PROT_WRITE,MAP_SHARED,o->fd,0);
    if(memory==MAP_FAILED)return rollback_create(o,cleanup);
    /* Atomic bind may clear region on failure; raw ownership is separate. */
    o->memory=memory;o->bytes=bytes;
    *stage=VBSC_CREATE_ATOMIC;
    if(vbsc_atomic_bind(memory,bytes,&o->region)||vbsc_atomic_initialize(&o->region)) {
        return rollback_create(o,cleanup);
    }
    *stage=VBSC_CREATE_SEALS;
    if(fcntl(o->fd,F_ADD_SEALS,F_SEAL_SHRINK|F_SEAL_GROW|F_SEAL_FUTURE_WRITE|F_SEAL_SEAL)) {
        return rollback_create(o,cleanup);
    }
    memcpy(o->mapping.namespace_id,ns,16);o->mapping.generation=generation;o->mapping.capacity=capacity;
    *stage=0;return o;
}
int vbsc_owned_destroy(vbsc_owned *o) {
    if(!o)return 0;
    if(o->release_error)return o->release_error;
    if(o->memory) {
        if(munmap(o->memory,o->bytes)){o->release_error=1;return 1;}
        o->memory=NULL;o->bytes=0;memset(&o->region,0,sizeof(o->region));
    }
    /* Do not retry close: on Linux a reported error can follow FD release. */
    int fd=o->fd;o->fd=-1;
    if(fd>=0&&close(fd)){o->release_error=2;return 2;}
    free(o);return 0;
}
int vbsc_owned_publish(vbsc_owned *o,const uint8_t *p,size_t n) {
    return vbsc_atomic_publish(&o->region,&o->mapping,p,n,owned_hash,NULL);
}
int vbsc_owned_snapshot(vbsc_owned *o,uint8_t *p,size_t n,size_t *used) {
    vbcf_header header;
    return vbsc_atomic_snapshot(&o->region,&o->mapping,p,n,used,&header,owned_hash,NULL,0,NULL);
}
