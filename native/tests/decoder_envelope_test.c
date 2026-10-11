/* Cross-component fixture: the real decoder adapter/correlator feeds the
 * portable VBCF writer. Vendor calls and hardware admission are fixtures, NOT
 * HW proof. The real launcher/native provider authenticate setup and records. */
#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "rdkx5_decoder_runtime.h"
#include "causal_envelope.h"
#include "causal_chunks.h"
#include "launch.h"
#include "frame_source_linux.h"
#include "nv12_causal_transform.h"

extern void decoder_runtime_enable_delayed_output(int);
extern void decoder_runtime_capture_pixels(int);
extern void decoder_runtime_output_budget(int);
extern void decoder_runtime_reverse_pending(void);
extern void decoder_runtime_override_next_tag(uint64_t);

static int hash_calls;
static int hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]) {
    (void)ctx;
    hash_calls++;
    return SHA256(p,n,out)?0:-1;
}
static int failed_hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]) {
    (void)ctx;(void)p;(void)n;
    memset(out,0xaa,32);
    return -1;
}
static uint64_t now_ms(void) {
    struct timespec t;
    assert(clock_gettime(CLOCK_MONOTONIC,&t)==0);
    return (uint64_t)t.tv_sec*1000+(uint64_t)t.tv_nsec/1000000;
}
static void tick(void) {
    struct timespec t={0,1000000};
    (void)nanosleep(&t,NULL);
}
static void zero_header(const uint8_t *b) {
    for(size_t j=0;j<VBCF_HEADER_SIZE;j++)assert(b[j]==0);
}
static void negative_contracts(const vbcf_header *layout,const uint8_t *pixels,size_t size) {
    uint8_t b[VBCF_HEADER_SIZE];
    memset(b,0xff,sizeof(b));
    assert(vbcf_marshal_surface_header(layout,0,0,pixels,size,hash,NULL,b)==VBCF_SURFACE_ORIGIN_UNAVAILABLE);
    zero_header(b);
    assert(vbcf_marshal_surface_header(layout,layout->source_epoch,0,pixels,size,hash,NULL,b)==VBCF_SURFACE_ORIGIN_UNAVAILABLE);
    zero_header(b);
    assert(vbcf_marshal_surface_header(layout,layout->source_epoch==UINT64_MAX?1:layout->source_epoch+1,1,pixels,size,hash,NULL,b)==VBCF_SURFACE_EPOCH_MISMATCH);
    zero_header(b);
    vbcf_header bad=*layout;
    bad.origin_sequence=77;
    assert(vbcf_marshal_surface_header(&bad,layout->source_epoch,1,pixels,size,hash,NULL,b)==VBCF_SURFACE_INVALID);
    bad=*layout;bad.stride_uv=14;
    assert(vbcf_marshal_surface_header(&bad,layout->source_epoch,1,pixels,size,hash,NULL,b)==VBCF_SURFACE_INVALID);
    assert(vbcf_marshal_surface_header(layout,layout->source_epoch,1,pixels,size-1,hash,NULL,b)==VBCF_SURFACE_INVALID);
    assert(vbcf_marshal_surface_header(layout,layout->source_epoch,1,pixels,SIZE_MAX,hash,NULL,b)==VBCF_SURFACE_INVALID);
#define BAD_LAYOUT(change) do { \
    bad=*layout; change; memset(b,0xff,sizeof(b)); \
    assert(vbcf_marshal_surface_header(&bad,layout->source_epoch,1,pixels,size,hash,NULL,b)==VBCF_SURFACE_INVALID); \
    zero_header(b); \
} while(0)
    BAD_LAYOUT(bad.kind=2);
    BAD_LAYOUT(bad.codec=2);
    BAD_LAYOUT(bad.surface=3);
    BAD_LAYOUT(bad.width=16385);
    BAD_LAYOUT(bad.height=15);
    BAD_LAYOUT(bad.stride_y=14);
    BAD_LAYOUT(bad.correlation=1);
    BAD_LAYOUT(bad.payload_bytes=384);
    BAD_LAYOUT(bad.flags=1);
    BAD_LAYOUT(bad.capture_ns=77);
    BAD_LAYOUT(bad.media_pts=77);
    BAD_LAYOUT(bad.publication_epoch=77);
    BAD_LAYOUT(bad.delivery_id=77);
    BAD_LAYOUT(bad.stream[0]='x');
    BAD_LAYOUT(bad.payload_sha[0]=1);
    BAD_LAYOUT(bad.source[0]='/');
    BAD_LAYOUT(bad.process[0]=0);
    BAD_LAYOUT(memset(bad.namespace_id,0,sizeof(bad.namespace_id)));
    BAD_LAYOUT(memset(bad.graph,0,sizeof(bad.graph)));
    BAD_LAYOUT(memset(bad.transform,0,sizeof(bad.transform)));
#undef BAD_LAYOUT
    assert(vbcf_marshal_surface_header(layout,layout->source_epoch,1,NULL,size,hash,NULL,b)==VBCF_SURFACE_INVALID);
    assert(vbcf_marshal_surface_header(layout,layout->source_epoch,1,pixels,size,NULL,NULL,b)==VBCF_SURFACE_INVALID);
    zero_header(b);
    assert(hash_calls==0); /* Invalid geometry/origin MUST NOT touch pixels. */
    memset(b,0xff,sizeof(b));
    assert(vbcf_marshal_surface_header(layout,layout->source_epoch,1,pixels,size,failed_hash,NULL,b)==VBCF_SURFACE_HASH_FAILED);
    zero_header(b);
}
static vb_capture_origin take(video_broker_rdkx5_decoder_t *decoder,uint8_t *pixels,size_t size) {
    decoder_runtime_output_budget(1);
    uint64_t until=now_ms()+2000;
    vb_capture_origin origin={0,0};
    int rc=0;
    while(!rc&&now_ms()<until) {
        rc=video_broker_rdkx5_decoder_poll(decoder,pixels,size,&origin);
        if(!rc)tick();
    }
    assert(rc==1);
    return origin;
}
int main(int argc,char **argv) {
    extern void decoder_runtime_output_geometry(int width, int height);
    assert(argc==8);
    video_broker_rdkx5_codec_t codec;
    if(!strcmp(argv[1],"mjpeg"))codec=VIDEO_BROKER_RDKX5_CODEC_MJPEG;
    else if(!strcmp(argv[1],"h265"))codec=VIDEO_BROKER_RDKX5_CODEC_H265;
    else {assert(!strcmp(argv[1],"h264"));codec=VIDEO_BROKER_RDKX5_CODEC_H264;}
    uint8_t input[VBCF_HEADER_SIZE],header[VBCF_HEADER_SIZE];
    video_broker_frame_launch_args args={0};
    assert(video_broker_frame_launch_arg(&args,"--frame-launch-socket-fd","3")==1);
    assert(video_broker_frame_launch_arg(&args,"--frame-launch-controller-fd","4")==1);
    assert(video_broker_frame_launch_arg(&args,"--frame-launch-controller-metadata",argv[2])==1);
    assert(video_broker_frame_launch_arg(&args,"--frame-native-mailbox-fd","5")==1);
    assert(video_broker_frame_launch_arg(&args,"--frame-native-mailbox-generation",argv[5])==1);
    assert(video_broker_frame_launch_arg(&args,"--frame-output-mailbox-fd","6")==1);
    assert(video_broker_frame_launch_arg(&args,"--frame-output-mailbox-generation",argv[6])==1);
    assert(video_broker_frame_launch_arg(&args,"--frame-output-mailbox-capacity",argv[7])==1);
    assert(video_broker_frame_launch_arg(&args,"--frame-output-stream","analysis")==1);
    char *end=NULL;uint64_t epoch=strtoull(argv[4],&end,10);assert(end&&!*end&&epoch);
    video_broker_frame_source source={0};
    assert(video_broker_frame_source_open(&source,&args,argv[3],epoch)==0);
    size_t received;
    vbcf_header layout=source.definition;
    assert(!strcmp(layout.process,source.launch.controller.generation));
    assert((layout.width==16&&layout.height==16)||(layout.width==1280&&layout.height==720)||
           (layout.width==1920&&layout.height==1080));
    size_t width=layout.width,height=layout.height,size=width*height*3/2;
    assert(layout.kind==4&&layout.payload_bytes==0);
    assert(video_broker_frame_source_geometry(&source,(uint32_t)width,(uint32_t)height,(uint32_t)width,layout.surface)==0);
    uint8_t *pixels=calloc(1,size);assert(pixels);
    /* An explicit admitted source definition, not an invented frame template. */
    negative_contracts(&layout,pixels,size);
    size_t output_size=size/4;
    uint8_t *output=calloc(1,output_size);assert(output);
    video_broker_rdkx5_decoder_t decoder = {0};
    decoder_runtime_enable_delayed_output(0);
    decoder_runtime_capture_pixels(2);
    decoder_runtime_output_budget(0);
    decoder_runtime_output_geometry((int)width, (int)height);
    assert(video_broker_rdkx5_decoder_open(&decoder,codec,(int)width,(int)height,video_broker_frame_source_epoch(&source))==0);
    const uint64_t base=(UINT64_C(1)<<53)+10;
    source.sequence=base; /* Test-only overflow/float boundary seed. */
    vb_capture_origin origin;
    for(uint64_t j=1;j<=2;j++) {
        uint8_t value=(uint8_t)(31+j);
        vb_capture_origin input_origin;
        assert(video_broker_frame_source_next(&source,&input_origin)==0);
        assert(input_origin.epoch==epoch&&input_origin.sequence==base+j);
        assert(video_broker_rdkx5_decoder_decode(&decoder,&value,1,pixels,size,0,
            input_origin,&origin)==0);
        assert(origin.epoch==0&&origin.sequence==0);
    }
    decoder_runtime_reverse_pending();
    for(uint64_t j=2;j>0;j--) {
        origin=take(&decoder,pixels,size);
        assert(origin.epoch==layout.source_epoch&&origin.sequence==base+j);
        for(size_t y=0;y<height;y++)for(size_t x=0;x<width;x++)
            assert(pixels[y*width+x]==(uint8_t)(31+j+y*7+x*3));
        for(size_t y=0;y<height/2;y++)for(size_t x=0;x<width;x++)
            assert(pixels[width*height+y*width+x]==(uint8_t)(128+y*5+x));
        int rc=video_broker_frame_source_publish(&source,origin,pixels,size,1000);
        if(rc)fprintf(stderr,"source_send_failed code=%d provider_stage=%s\n",rc,source.stage);
        assert(rc==0);
        video_broker_nv12_resize_nearest_oriented_aspect(pixels,(int)width,(int)height,
            output,(int)width/2,(int)height/2,VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL,VIDEO_BROKER_NV12_ASPECT_EXACT);
        assert(video_broker_frame_source_nv12_output(&source,"analysis",origin,output,output_size,
            (int)width/2,(int)height/2,VIDEO_BROKER_VIDEO_ORIENTATION_NORMAL,VIDEO_BROKER_NV12_ASPECT_EXACT,1000)==0);
        /* Exact two-frame oracle: production is free to replace pending
         * analysis frames when a reader falls behind. */
        uint64_t output_until=now_ms()+1000;
        for(;;) {
            vbsp_stats stats;assert(video_broker_frame_source_output_stats(&source,&stats)==0);
            assert(!stats.failure&&!stats.replaced);
            if(stats.published>=3-j)break;
            assert(now_ms()<output_until);tick();
        }
        assert(source.outputs[0].sequence==3-j);
        memset(output,0xfb,output_size);
        memset(pixels,0xfa,size); /* Next use cannot alias old output. */
    }
    uint8_t value=99;
    vb_capture_origin input_origin;
    assert(video_broker_frame_source_next(&source,&input_origin)==0);
    assert(video_broker_rdkx5_decoder_decode(&decoder,&value,1,pixels,size,0,
        input_origin,&origin)==0);
    decoder_runtime_override_next_tag(0);
    origin=take(&decoder,pixels,size);
    assert(vbcf_marshal_surface_header(&layout,origin.epoch,origin.sequence,
        pixels,size,hash,NULL,header)==VBCF_SURFACE_ORIGIN_UNAVAILABLE);
    zero_header(header); /* No third frame or renamed origin leaves the worker. */
    assert(video_broker_frame_source_publish(&source,origin,pixels,size,1000)==1);
    /* Submission is asynchronous. SOURCE_DONE is a test-only completion
     * barrier, not a production wait on the camera path. Keep the same 1s
     * budget and require both exact surfaces, with no replacement or failure. */
    uint64_t publication_until=now_ms()+1000;
    for(;;) {
        vbsp_stats stats;assert(video_broker_frame_source_stats(&source,&stats)==0);
        assert(!stats.failure&&!stats.replaced&&!stats.expired&&!stats.busy);
        if(stats.published==2&&!stats.pending&&!stats.inflight)break;
        assert(now_ms()<publication_until);tick();
    }
    assert(source.published==2&&source.unavailable==1);
    assert(video_broker_rdkx5_decoder_close(&decoder)==0);
    free(output);
    free(pixels);
    assert(framepeer_linux_send(&source.launch,(const uint8_t *)"SOURCE_DONE",11,1000)==0);
    assert(framepeer_linux_receive(&source.launch,input,sizeof(input),&received,1000)==0);
    assert(received==4&&!memcmp(input,"done",4));
    assert(video_broker_frame_source_close(&source)==0);
    fprintf(stderr,"decoder_envelope PASS codec=%s surfaces=2 unknown_origin_rejected=1\n",argv[1]);
    assert(getchar()!=EOF); /* Join final controller write before exiting. */
    return 0;
}
