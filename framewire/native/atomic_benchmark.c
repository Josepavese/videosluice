#define _POSIX_C_SOURCE 200809L
#include "atomic_snapshot.h"
#include <assert.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]){(void)ctx;return SHA256(p,n,out)?0:-1;}
static double clock_seconds(clockid_t clock) {struct timespec t;assert(!clock_gettime(clock,&t));return (double)t.tv_sec+(double)t.tv_nsec/1e9;}
int main(void) {
    const size_t payload=1280*720*3/2,n=VBCF_HEADER_SIZE+payload,size=VBSC_SIZE+n;
    void *memory=aligned_alloc(64,size);uint8_t *input=malloc(n),*output=malloc(n);assert(memory&&input&&output);
    vbsc_atomic_region r;vbsc_mapping m={{1},1,n};assert(!vbsc_atomic_bind(memory,size,&r));assert(!vbsc_atomic_initialize(&r));
    vbcf_header h={0};h.kind=2;h.codec=3;h.surface=3;h.correlation=1;h.width=1280;h.height=720;h.stride_y=1280;h.stride_uv=1280;h.payload_bytes=(uint32_t)payload;
    h.namespace_id[0]=1;h.graph[0]=1;h.transform[0]=1;h.source_epoch=1;h.publication_epoch=1;strcpy(h.source,"front");strcpy(h.stream,"analysis");strcpy(h.process,"test");
    double cpu=clock_seconds(CLOCK_PROCESS_CPUTIME_ID),wall=clock_seconds(CLOCK_MONOTONIC);
    for(uint64_t id=1;id<=100;id++){
        h.origin_sequence=id;h.delivery_id=id;memset(input+VBCF_HEADER_SIZE,(int)(id%251),payload);
        assert(!hash(NULL,input+VBCF_HEADER_SIZE,payload,h.payload_sha));assert(!vbcf_marshal_header(&h,input));
        assert(!vbsc_atomic_publish(&r,&m,input,n,hash,NULL));
        size_t used=0;vbcf_header received;assert(!vbsc_atomic_snapshot(&r,&m,output,n,&used,&received,hash,NULL,0,NULL));
        assert(used==n&&received.origin_sequence==id&&!memcmp(input,output,n));
    }
    cpu=clock_seconds(CLOCK_PROCESS_CPUTIME_ID)-cpu;wall=clock_seconds(CLOCK_MONOTONIC)-wall;
    printf("ATOMIC_LOCAL_BENCH frames=100 pixels=1280x720 bytes=%zu cpu_ms_per_frame=%.6f wall_ms_per_frame=%.6f includes_generation_hash_publish_read_and_compare=1\n",n,cpu*10,wall*10);
    free(output);free(input);free(memory);return 0;
}
