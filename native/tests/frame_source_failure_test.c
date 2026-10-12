/* Actual bounded native output mailbox, synthetic pixels/authority only.
 * No camera, decoder or accelerator is attested by this standalone fixture. */
#define _GNU_SOURCE
#include "frame_source_linux.h"
#include "worker_release.h"
#include <assert.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static pthread_t capture_thread;
static int delay_capture_authority;
int __wrap_framepeer_linux_current(framepeer_linux_launch *launch) {
    assert(launch->accepted);
    if(pthread_equal(pthread_self(),capture_thread)&&delay_capture_authority) {
        delay_capture_authority=0;
        struct timespec pause={.tv_nsec=50000000};
        assert(nanosleep(&pause,NULL)==0);
    }
    return 0;
}
static void await_output(video_broker_frame_source *s,unsigned published) {
    for(int i=0;i<200;i++) {
        vbsp_stats stats={0};assert(video_broker_frame_source_output_stats(s,&stats)==0);
        assert(!stats.failure);
        if(stats.published>=published)return;
        struct timespec pause={.tv_nsec=10000000};assert(nanosleep(&pause,NULL)==0);
    }
    assert(!"native output did not publish within two seconds");
}
int main(void) {
    capture_thread=pthread_self();
    uint8_t pixels[96*64*3/2]={0};
    video_broker_frame_source source={.initialized=1,.enabled=1,.geometry_verified=1,.sequence=1};
    source.launch.accepted=true;
    assert(pthread_mutex_init(&source.authority_mutex,NULL)==0);
    source.definition=(vbcf_header){.kind=4,.codec=3,.surface=1,.width=192,.height=128,
        .stride_y=192,.stride_uv=192,.source_epoch=7};
    source.definition.namespace_id[0]=1;source.definition.graph[0]=1;source.definition.transform[0]=7;
    strcpy(source.definition.source,"source-a");strcpy(source.definition.process,"native.fixture");
    assert(SHA256(NULL,0,source.definition.payload_sha));
    vbsc_mapping mapping={.generation=9,.capacity=VBCF_HEADER_SIZE+sizeof(pixels)};
    mapping.namespace_id[0]=1;
    int error=0;
    vbsm_source_mailbox *reader=vbsm_source_mailbox_create(&mapping,&error);assert(reader&&!error);
    int fd=vbsm_source_mailbox_export(reader);assert(fd>=3);
    source.output_mailbox=vbsm_source_mailbox_attach(fd,&mapping,&error);assert(source.output_mailbox&&!error);
    assert(vbsm_source_mailbox_confirm(reader)==0);
    source.output_capacity=sizeof(pixels);
    delay_capture_authority=1;
    /* Twenty complete frames replace two RAM slots with no consumer reads. */
    for(unsigned frame=1;frame<=20;frame++) {
        memset(pixels,(int)frame,sizeof(pixels));source.sequence=frame;
        assert(video_broker_frame_source_nv12_output(&source,"analysis",(vb_capture_origin){7,frame},
            pixels,sizeof(pixels),96,64,0,0,1000)==0);
        await_output(&source,frame);
        memset(pixels,0xee,sizeof(pixels));
        assert(!source.failed&&!source.output_retired);
    }
    /* Injected authority wait must be distinguished from CPU work and from
     * the detached publisher. Delivery bytes/counters still use the oracle. */
    assert(source.output_phase_wall_max_ns[1]>=45000000);
    assert(source.output_phase_cpu_at_wall_max_ns[1]<source.output_phase_wall_max_ns[1]);
    for(unsigned phase=0;phase<4;phase++)assert(source.output_phase_wall_max_ns[phase]>0);
    uint8_t snapshot[VBCF_HEADER_SIZE+sizeof(pixels)];size_t used;uint64_t serial;
    for(unsigned slot=0;slot<2;slot++) {
        assert(vbsm_source_mailbox_stage(reader,slot,0,snapshot,sizeof(snapshot),&used,&serial)==0);
        vbcf_header h;assert(vbcf_parse_header(snapshot,VBCF_HEADER_SIZE,&h)==0);
        assert(h.kind==5&&h.output_sequence==19+slot&&h.origin_sequence==19+slot&&serial==20);
        for(size_t i=VBCF_HEADER_SIZE;i<used;i++)assert(snapshot[i]==19+slot);
    }
    assert(video_broker_worker_exit_status(source.failed?-1:0,true)==0);
    assert(vbsp_close(source.output_publisher,1000)==0);source.output_publisher=NULL;
    assert(vbsm_source_mailbox_close(source.output_mailbox)==0);source.output_mailbox=NULL;
    assert(vbsm_source_mailbox_close(reader)==0);
    assert(pthread_mutex_destroy(&source.authority_mutex)==0);
    puts("OK_NATIVE_OUTPUT_MAILBOX_NO_READER_NO_CAPTURE_FAILURE");return 0;
}
