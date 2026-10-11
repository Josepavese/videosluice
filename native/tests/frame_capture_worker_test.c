/* Real authenticated native source provider; synthetic pixels, not camera/HW
 * admission. Launched through the SAME production mux command builder. */
#include "frame_source_linux.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Spatially distinct Y/U/V values also vary across source, capture and epoch.
 * Constant images cannot expose wrong coordinates, row/plane offsets or a
 * same-sized crop accidentally taken from another part of the surface. */
static uint8_t pixel_at(unsigned seed, uint32_t x, uint32_t y, int chroma) {
    return (uint8_t)(chroma ? seed+73+(x/2)*11+y*29+(x%2)*101 : seed+x*3+y*17);
}

int main(int argc,char **argv) {
    fprintf(stderr,"frame_capture_test phase=main_enter\n");fflush(stderr);
    /* Reject malformed/manual launch structs before any descriptor use. */
    video_broker_frame_launch_args malformed={.socket_fd=3,.controller_fd=4,.controller_metadata="a",.seen=7};
    assert(video_broker_frame_launch_validate(&malformed)==FRAMEPEER_INVALID);
    malformed.controller_metadata="0000000000000000000000000000000000000z";
    assert(video_broker_frame_launch_validate(&malformed)==FRAMEPEER_INVALID);
    malformed.controller_metadata="00000000000000000000000000000000000000";
    malformed.socket_fd=1000000;
    assert(video_broker_frame_launch_validate(&malformed)==FRAMEPEER_INVALID);
    video_broker_frame_launch_args absent={0};
    malformed.socket_fd=3;
    malformed.output_stream="vision-detail";
    assert(video_broker_frame_launch_validate(&malformed)==FRAMEPEER_INVALID);
    malformed.seen=15;
    assert(video_broker_frame_launch_validate(&malformed)==FRAMEPEER_INVALID);
    malformed.seen=511;malformed.mailbox_fd=5;malformed.mailbox_generation=1;
    malformed.output_mailbox_fd=6;malformed.output_mailbox_generation=2;
    malformed.output_mailbox_capacity=VBCF_HEADER_SIZE+192;
    assert(video_broker_frame_launch_validate(&malformed)==0);
    malformed.output_stream="invalid/stream";
    assert(video_broker_frame_launch_validate(&malformed)==FRAMEPEER_INVALID);
    malformed.output_stream=NULL;
    assert(video_broker_frame_launch_validate(&malformed)==FRAMEPEER_INVALID);
    assert(video_broker_frame_launch_arg(&absent,"--frame-output-stream","")==-1);
    assert(video_broker_frame_launch_arg(&absent,"--frame-output-stream","invalid/stream")==-1);
    video_broker_frame_source disabled={0};vb_capture_origin missing={1,1};
    assert(video_broker_frame_source_open(&disabled,&absent,NULL,0)==0);
    assert(video_broker_frame_source_epoch(&disabled)==0);
    assert(video_broker_frame_source_next(&disabled,&missing)==0&&missing.epoch==0&&missing.sequence==0);
    assert(video_broker_frame_source_close(&disabled)==0);
    video_broker_frame_launch_args args={0};
    video_broker_frame_source source={0};
    const char *source_id=NULL;
    uint64_t epoch=0;
    int exec_ready=0,defer_bootstrap=0,step_frames=0,step_surface=0;
    for(int i=1;i<argc;i++) {
        if(strcmp(argv[i],"--fixture-exec-ready")==0){exec_ready=1;continue;}
        if(strcmp(argv[i],"--fixture-defer-bootstrap")==0){defer_bootstrap=1;continue;}
        if(strcmp(argv[i],"--fixture-step-frames")==0){step_frames=1;continue;}
        if(strcmp(argv[i],"--fixture-step-surface")==0){step_surface=1;continue;}
        if(i+1<argc) {
            int known=video_broker_frame_launch_arg(&args,argv[i],argv[i+1]);
            assert(known>=0);
            if(known){i++;continue;}
            if(strcmp(argv[i],"--source-id")==0){source_id=argv[++i];continue;}
            if(strcmp(argv[i],"--media-epoch")==0){epoch=strtoull(argv[++i],NULL,10);continue;}
        }
    }
    /* Test scheduling only, not frame authority: the real authenticated
     * bootstrap, mailbox seals and per-record deadlines remain unchanged. */
    if(exec_ready){puts("exec-ready");fflush(stdout);}
    if(defer_bootstrap)assert(getchar()==3);
    assert(video_broker_frame_source_open(&source,&args,source_id,epoch)==0);
    assert(source.enabled);
    uint32_t width=source.definition.width,height=source.definition.height;
    assert(video_broker_frame_source_geometry(&source,width,height,width,1)==0);
    puts("bound");fflush(stdout);
    assert(getchar()==1);
    size_t bytes=(size_t)width*height*3/2;
    uint8_t *pixels=malloc(bytes);assert(pixels);
    uint8_t *delivery=malloc(bytes/4);assert(delivery);
    unsigned source_seed=0;
    for(const unsigned char *p=(const unsigned char *)source_id;*p;p++) source_seed=(source_seed*33+*p)&255;
    int frame=1,command=4;
    while(command==4) {
    for(int count=0;count<2;count++,frame++) {
        unsigned seed=(source_seed+(unsigned)(epoch&255)*47+(unsigned)frame*29)&255;
        for(uint32_t y=0;y<height;y++) for(uint32_t x=0;x<width;x++)
            pixels[(size_t)y*width+x]=pixel_at(seed,x,y,0);
        for(uint32_t y=0;y<height/2;y++) for(uint32_t x=0;x<width;x++)
            pixels[(size_t)width*height+(size_t)y*width+x]=pixel_at(seed,x,y,1);
        vb_capture_origin origin;
        assert(video_broker_frame_source_next(&source,&origin)==0);
        assert(origin.epoch==epoch && origin.sequence==(uint64_t)frame);
        assert(video_broker_frame_source_publish(&source,origin,pixels,bytes,1000)==0);
        if(step_surface) {
            /* Exact finite-fixture oracle only: let the independent source
             * pump stage this surface before publication can contend for the
             * Store lock. No producer ACK exists in the production path. */
            puts("surface-published");fflush(stdout);assert(getchar()==6);
        }
        /* Explicit test-only 2:1 point sampling; not a camera/scaler claim. */
        for(uint32_t y=0;y<height/2;y++) for(uint32_t x=0;x<width/2;x++)
            delivery[(size_t)y*(width/2)+x]=pixels[(size_t)(2*y)*width+2*x];
        for(uint32_t y=0;y<height/4;y++) for(uint32_t x=0;x<width/2;x++)
            delivery[(size_t)(width/2)*(height/2)+(size_t)y*(width/2)+x]=pixels[(size_t)width*height+(size_t)(2*y)*width+2*(x&~1u)+(x&1u)];
        assert(video_broker_frame_source_nv12_output(&source,source.output_stream,origin,delivery,bytes/4,
            width/2,height/2,0,0,1000)==0);
        /* The production producer may replace pending frames. This exact
         * two-frame fixture waits for each completed delivery. */
        vbsp_stats delivery_stats={0};
        for(int wait=0;wait<200;wait++) {
            assert(video_broker_frame_source_output_stats(&source,&delivery_stats)==0);
            if(delivery_stats.published>=(uint64_t)frame)break;
            struct timespec pause={.tv_nsec=10000000};assert(nanosleep(&pause,NULL)==0);
        }
        assert(delivery_stats.published>=(uint64_t)frame);
        if(step_frames&&count==0) {
            /* Fixture-only barrier, not a camera ACK: retain both exact
             * surfaces for the unchanged two-frame ROI pixel oracle. Real
             * independent ingress intentionally drops on Store contention. */
            puts("first-published");fflush(stdout);assert(getchar()==5);
        }
        memset(pixels,0xff,bytes); /* sender released its input, broker owns copy */
        memset(delivery,0xff,bytes/4);
    }
    puts("published");fflush(stdout);
    fprintf(stderr,"frame_capture_test phase=await_release\n");fflush(stderr);
    command=getchar();assert(command==2||command==4);
    }
    free(delivery);
    free(pixels);
    assert(source.publish_attempts==(uint64_t)(frame-1));
    for(int phase=0;phase<3;phase++)
        assert(source.publish_phase_wall_max_ns[phase]>0);
    fprintf(stderr,"frame_capture_test phase=source_close_begin\n");fflush(stderr);
    assert(video_broker_frame_source_close(&source)==0);
    fprintf(stderr,"frame_capture_test phase=source_close_complete\n");fflush(stderr);
    puts("closed");fflush(stdout);
    fprintf(stderr,"frame_capture_test phase=main_return\n");fflush(stderr);
    return 0;
}
