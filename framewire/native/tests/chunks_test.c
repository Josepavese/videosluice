/* Portable sender fault fixture. No camera, OS provider or HW qualification. */
#include "causal_chunks.h"
#include <assert.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <string.h>

static uint8_t pixels[1280*720*3/2],workspace[FAPD_MAX_PACKET],header[VBCF_HEADER_SIZE];
static uint8_t manifest[32];
static int hash_mode,hash_calls;
static int hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]) {
    (void)ctx;hash_calls++;
    if(hash_mode==1)return -1;
    if(hash_mode==2){memset(out,0,32);return 0;}
    return SHA256(p,n,out)?0:-1;
}
typedef struct { int calls,fail_at;size_t next; } sink;
static int send_record(void *ctx,const uint8_t *p,size_t size) {
    sink *s=ctx;int call=s->calls++;
    if(call==s->fail_at)return -71; /* Uncertain send, never retried here. */
    assert(size<=FAPD_MAX_PACKET);
    if(!call){assert(size==sizeof(header)&&!memcmp(p,header,size));return 0;}
    assert(size>FAPD_HEADER_SIZE&&!memcmp(p,"FAPD\x01\x00",6));
    assert((size_t)(p[6]*256+p[7])==size&&!memcmp(p+8,manifest,32));
    uint32_t off=(uint32_t)p[40]<<24|(uint32_t)p[41]<<16|(uint32_t)p[42]<<8|p[43];
    assert(off==s->next&&size-FAPD_HEADER_SIZE<=sizeof(pixels)-s->next);
    assert(!memcmp(p+FAPD_HEADER_SIZE,pixels+off,size-FAPD_HEADER_SIZE));
    s->next+=size-FAPD_HEADER_SIZE;return 0;
}
int main(void) {
    for(size_t j=0;j<sizeof(pixels);j++)pixels[j]=(uint8_t)(j*7);
    vbcf_header layout={.kind=4,.codec=3,.surface=2,.width=1280,.height=720,.stride_y=1280,.stride_uv=1280,.source_epoch=7};
    layout.namespace_id[0]=1;layout.graph[0]=1;layout.transform[0]=7;
    strcpy(layout.source,"front");strcpy(layout.process,"native.fixture");
    assert(SHA256(NULL,0,layout.payload_sha));
    assert(vbcf_marshal_surface_header(&layout,7,(UINT64_C(1)<<53)+11,pixels,sizeof(pixels),hash,NULL,header)==0);
    assert(SHA256(header,sizeof(header),manifest));
    vbcf_send_progress progress;
    const int failures[]={-1,0,1,2,85};
    for(size_t j=0;j<sizeof(failures)/sizeof(failures[0]);j++) {
        sink s={.fail_at=failures[j]};hash_calls=0;
        int rc=vbcf_send_source_packet(header,pixels,sizeof(pixels),workspace,sizeof(workspace),hash,NULL,send_record,&s,&progress);
        assert(hash_calls==1&&progress.attempted==1);
        if(s.fail_at<0) {
            assert(rc==0&&s.calls==86&&s.next==sizeof(pixels));
            assert(progress.stage==VBCF_SEND_COMPLETE&&progress.bytes_sent==sizeof(pixels)&&progress.records_sent==86);
        } else {
            assert(rc==-71&&s.calls==s.fail_at+1&&progress.records_sent==(uint32_t)s.fail_at);
            assert(progress.stage==(s.fail_at?VBCF_SEND_DATA:VBCF_SEND_HEADER));
            assert(progress.bytes_sent==s.next&&progress.attempted_offset==s.next);
        }
    }
    for(hash_mode=1;hash_mode<=2;hash_mode++) {
        sink s={.fail_at=-1};hash_calls=0;
        assert(vbcf_send_source_packet(header,pixels,sizeof(pixels),workspace,sizeof(workspace),hash,NULL,send_record,&s,&progress)!=0);
        assert(s.calls==0&&hash_calls==1&&progress.stage==VBCF_SEND_MANIFEST_HASH&&!progress.attempted);
    }
    hash_mode=0;hash_calls=0;sink s={.fail_at=-1};
#define REJECT(h,p,n,w,wn,fn) do { \
    assert(vbcf_send_source_packet(h,p,n,w,wn,hash,NULL,fn,&s,&progress)!=0); \
    assert(progress.stage==VBCF_SEND_CONTRACT&&!progress.attempted&&s.calls==0&&hash_calls==0); \
} while(0)
    REJECT(NULL,pixels,sizeof(pixels),workspace,sizeof(workspace),send_record);
    REJECT(header,NULL,sizeof(pixels),workspace,sizeof(workspace),send_record);
    REJECT(header,pixels,sizeof(pixels)-1,workspace,sizeof(workspace),send_record);
    REJECT(header,pixels,sizeof(pixels),workspace,sizeof(workspace)-1,send_record);
    REJECT(header,pixels,sizeof(pixels),workspace,sizeof(workspace),NULL);
    header[4]=2;REJECT(header,pixels,sizeof(pixels),workspace,sizeof(workspace),send_record);
#undef REJECT
    size_t written=99;uint8_t zero[32]={0};
    assert(fapd_encode(workspace,sizeof(workspace),zero,0,pixels,1,&written)<0&&written==0);
    written=99;assert(fapd_encode(workspace,sizeof(workspace),manifest,FAPD_MAX_EXTENT,pixels,1,&written)<0&&written==0);
    written=99;assert(fapd_encode(workspace,FAPD_HEADER_SIZE,manifest,0,pixels,1,&written)<0&&written==0);
    puts("NATIVE_CHUNKS_PASS chunks=85 errors_no_retry=4 bounded_workspace=16384");
    return 0;
}
