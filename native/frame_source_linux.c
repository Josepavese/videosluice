#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "frame_source_linux.h"
#include "nv12_causal_transform.h"
#include "../internal/framehash/frame_sha256_linux.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

static int hex_digit(char c) {
    if(c>='0'&&c<='9')return c-'0';
    if(c>='a'&&c<='f')return c-'a'+10;
    return -1;
}
static int descriptor_number(const char *s) {
    if(!s||!*s||*s=='0')return -1;
    int n=0;size_t j;
    for(j=0;s[j]&&j<6;j++){if(s[j]<'0'||s[j]>'9')return -1;n=n*10+s[j]-'0';}
    return s[j]||n<3?-1:n;
}
static size_t metadata_length(const char *s) {
    if(!s)return 0;
    size_t n=0;
    while(n<=164&&s[n]){if(hex_digit(s[n])<0)return 0;n++;}
    return n>=38&&n<=164&&!(n%2)?n:0;
}
static uint64_t generation_number(const char *s) {
    if(!s||!*s||*s=='0')return 0;
    uint64_t n=0;
    for(size_t i=0;s[i];i++) {
        if(i>=20||s[i]<'0'||s[i]>'9'||n>(UINT64_MAX-(unsigned)(s[i]-'0'))/10)return 0;
        n=n*10+(unsigned)(s[i]-'0');
    }
    return n;
}
static size_t stream_length(const char *s) {
    if(!s)return 0;
    size_t n=0;
    for(;n<65&&s[n];n++) {
        char c=s[n];
        if(!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||
             (c>='0'&&c<='9')||c=='_'||c=='-'||c=='.'))return 0;
    }
    return n&&n<=64?n:0;
}
int video_broker_frame_launch_arg(video_broker_frame_launch_args *a,const char *key,const char *value) {
    if(!a||!key||!value)return -1;
    unsigned bit;
    if(!strcmp(key,"--frame-launch-socket-fd"))bit=1;
    else if(!strcmp(key,"--frame-launch-controller-fd"))bit=2;
    else if(!strcmp(key,"--frame-launch-controller-metadata"))bit=4;
    else if(!strcmp(key,"--frame-output-stream"))bit=8;
    else if(!strcmp(key,"--frame-native-mailbox-fd"))bit=16;
    else if(!strcmp(key,"--frame-native-mailbox-generation"))bit=32;
    else if(!strcmp(key,"--frame-output-mailbox-fd"))bit=64;
    else if(!strcmp(key,"--frame-output-mailbox-generation"))bit=128;
    else if(!strcmp(key,"--frame-output-mailbox-capacity"))bit=256;
    else return 0;
    if(a->seen&bit)return -1;
    if(bit==32||bit==128||bit==256) {
        uint64_t number=generation_number(value);if(!number)return -1;
        if(bit==32)a->mailbox_generation=number;
        else if(bit==128)a->output_mailbox_generation=number;
        else a->output_mailbox_capacity=number;
    } else if(bit==8) {
        if(!stream_length(value))return -1;
        a->output_stream=value;
    } else if(bit==4) {
        if(!metadata_length(value))return -1;
        a->controller_metadata=value;
    } else {
        int n=descriptor_number(value);if(n<0)return -1;
        if(bit==1)a->socket_fd=n;else if(bit==2)a->controller_fd=n;
        else if(bit==16)a->mailbox_fd=n;else a->output_mailbox_fd=n;
    }
    a->seen|=bit;return 1;
}
int video_broker_frame_launch_validate(const video_broker_frame_launch_args *a) {
    if(!a)return FRAMEPEER_INVALID;
    if(!a->seen)return (!a->socket_fd&&!a->controller_fd&&!a->controller_metadata&&!a->output_stream&&!a->mailbox_fd&&!a->mailbox_generation&&!a->output_mailbox_fd&&!a->output_mailbox_generation&&!a->output_mailbox_capacity)?0:FRAMEPEER_INVALID;
    return a->seen==511&&stream_length(a->output_stream)&&a->socket_fd>=3&&a->socket_fd<=999999&&a->controller_fd>=3&&a->controller_fd<=999999&&a->socket_fd!=a->controller_fd&&metadata_length(a->controller_metadata)&&a->mailbox_fd>=3&&a->mailbox_fd<=999999&&a->mailbox_fd!=a->socket_fd&&a->mailbox_fd!=a->controller_fd&&a->mailbox_generation&&a->output_mailbox_fd>=3&&a->output_mailbox_fd<=999999&&a->output_mailbox_fd!=a->socket_fd&&a->output_mailbox_fd!=a->controller_fd&&a->output_mailbox_fd!=a->mailbox_fd&&a->output_mailbox_generation&&a->output_mailbox_generation!=a->mailbox_generation&&a->output_mailbox_capacity>VBCF_HEADER_SIZE&&a->output_mailbox_capacity<=VBCF_HEADER_SIZE+VBCF_MAX_PAYLOAD?0:FRAMEPEER_INVALID;
}
static uint64_t diagnostic_now_ns(void) {
    struct timespec now;
    if(clock_gettime(CLOCK_MONOTONIC,&now)||now.tv_sec<0)return 0;
    return (uint64_t)now.tv_sec*UINT64_C(1000000000)+(uint64_t)now.tv_nsec;
}
static uint64_t diagnostic_cpu_ns(void) {
    struct timespec now;
    if(clock_gettime(CLOCK_THREAD_CPUTIME_ID,&now)||now.tv_sec<0)return 0;
    return (uint64_t)now.tv_sec*UINT64_C(1000000000)+(uint64_t)now.tv_nsec;
}
static uint64_t diagnostic_span(uint64_t start,uint64_t end) {
    return start&&end>=start?end-start:0;
}
enum {SOURCE_PUBLISH_HEALTH,SOURCE_PUBLISH_AUTHORITY,SOURCE_PUBLISH_SUBMIT};
enum {OUTPUT_TRANSFORM,OUTPUT_AUTHORITY,OUTPUT_LAYOUT,OUTPUT_SUBMIT};
static void observe_output_phase(video_broker_frame_source *s,int phase,
    uint64_t wall_start,uint64_t cpu_start) {
    uint64_t wall=diagnostic_span(wall_start,diagnostic_now_ns());
    uint64_t cpu=diagnostic_span(cpu_start,diagnostic_cpu_ns());
    if(wall>s->output_phase_wall_max_ns[phase]) {
        s->output_phase_wall_max_ns[phase]=wall;
        s->output_phase_cpu_at_wall_max_ns[phase]=cpu;
    }
}
static void observe_publish_phase(video_broker_frame_source *s,int phase,
    uint64_t wall_start,uint64_t cpu_start) {
    uint64_t wall=diagnostic_span(wall_start,diagnostic_now_ns());
    uint64_t cpu=diagnostic_span(cpu_start,diagnostic_cpu_ns());
    if(wall>s->publish_phase_wall_max_ns[phase]) {
        s->publish_phase_wall_max_ns[phase]=wall;
        s->publish_phase_cpu_at_wall_max_ns[phase]=cpu;
    }
}
static int current(video_broker_frame_source *s) {
    pthread_mutex_lock(&s->authority_mutex);
    int rc=framepeer_linux_current(&s->launch);
    pthread_mutex_unlock(&s->authority_mutex);return rc;
}
static int fail(video_broker_frame_source *s,const char *stage,int code) {
    if(!s->failed) {
        uint64_t now=diagnostic_now_ns(),start=s->attempt_started_ns;
        long long elapsed=start&&now>=start?(long long)((now-start)/1000000):-1;
        long long integrity=start&&s->integrity_finished_ns>=start?
            (long long)((s->integrity_finished_ns-start)/1000000):-1;
        s->stage=stage;s->failed=1;s->failure_code=code;
        /* One terminal record, never per-frame logging. Preserve the peer
         * code/stage BEFORE close replaces state. -1 means timing unavailable.
         * No timeout is changed and no ambiguous transmission is retried. */
        if(s->authority_mutex_ready)pthread_mutex_lock(&s->authority_mutex);
        const char *peer_stage=s->launch.stage?s->launch.stage:"unavailable";
        if(s->authority_mutex_ready)pthread_mutex_unlock(&s->authority_mutex);
        fprintf(stderr,"[video-broker][frame-source] event=frame_source_failure"
            " stage=%s code=%d peer_stage=%s source=%s source_epoch=%llu"
            " attempted_origin_sequence=%llu payload_bytes=%zu"
            " budget_ms=%d elapsed_ms=%lld payload_integrity_ms=%lld action=retire_source\n",
            stage,code,peer_stage,
            s->definition.source[0]?s->definition.source:"unbound",
            (unsigned long long)s->definition.source_epoch,
            (unsigned long long)s->attempt_origin_sequence,
            s->attempt_payload_bytes,s->attempt_timeout_ms,elapsed,integrity);
    }
    return code;
}
static void start_attempt(video_broker_frame_source *s,vb_capture_origin origin,size_t size,int timeout_ms) {
    s->attempt_started_ns=0;s->integrity_finished_ns=0;
    s->attempt_timeout_ms=timeout_ms;s->attempt_payload_bytes=size;
    s->attempt_origin_sequence=origin.sequence;
}
int video_broker_frame_source_open(video_broker_frame_source *s,const video_broker_frame_launch_args *a,const char *source,uint64_t epoch) {
    if(!s||s->initialized||video_broker_frame_launch_validate(a))return FRAMEPEER_INVALID;
    s->initialized=1;s->stage="disabled";
    if(!a->seen)return 0;
    if(pthread_mutex_init(&s->authority_mutex,NULL))return fail(s,"source_authority_mutex",FRAMEPEER_UNAVAILABLE);
    s->authority_mutex_ready=1;
    if(!source||!epoch)return fail(s,"source_identity_missing",FRAMEPEER_INVALID);
    uint8_t metadata[82];size_t n=metadata_length(a->controller_metadata)/2;
    if(n>sizeof(metadata))return fail(s,"source_metadata",FRAMEPEER_INVALID);
    for(size_t j=0;j<n;j++) {
        int hi=hex_digit(a->controller_metadata[2*j]),lo=hex_digit(a->controller_metadata[2*j+1]);
        if(hi<0||lo<0)return fail(s,"source_metadata",FRAMEPEER_INVALID);
        metadata[j]=(uint8_t)(hi*16+lo);
    }
    int rc=framepeer_linux_accept(&s->launch,a->socket_fd,a->controller_fd,metadata,n,1000);
    if(rc)return fail(s,"source_authentication",rc);
    uint8_t packet[VBCF_HEADER_SIZE];size_t got;
    rc=framepeer_linux_receive(&s->launch,packet,sizeof(packet),&got,1000);
    if(rc)return fail(s,"source_definition_receive",rc);
    if(vbcf_parse_header(packet,got,&s->definition)||s->definition.kind!=4)
        return fail(s,"source_definition_invalid",FRAMEPEER_RECORD);
    if(strcmp(s->definition.process,s->launch.controller.generation)||strcmp(s->definition.source,source)||s->definition.source_epoch!=epoch)
        return fail(s,"source_definition_identity",FRAMEPEER_UNAUTHORIZED);
    if(a->output_stream)memcpy(s->output_stream,a->output_stream,stream_length(a->output_stream)+1);
    uint64_t size=(uint64_t)s->definition.stride_y*s->definition.height*3/2;
    if(size>VBCF_MAX_PAYLOAD)return fail(s,"source_mailbox_capacity",FRAMEPEER_INVALID);
    vbsc_mapping mapping={.generation=a->mailbox_generation,.capacity=VBCF_HEADER_SIZE+size};
    memcpy(mapping.namespace_id,s->definition.namespace_id,16);
    s->mailbox=vbsm_source_mailbox_attach(a->mailbox_fd,&mapping,&rc);
    if(rc||!s->mailbox)return fail(s,"source_mailbox_attach",FRAMEPEER_INVALID);
    mapping.generation=a->output_mailbox_generation;mapping.capacity=a->output_mailbox_capacity;
    s->output_mailbox=vbsm_source_mailbox_attach(a->output_mailbox_fd,&mapping,&rc);
    if(rc||!s->output_mailbox)return fail(s,"output_mailbox_attach",FRAMEPEER_INVALID);
    s->output_capacity=(size_t)a->output_mailbox_capacity-VBCF_HEADER_SIZE;
    uint64_t integrity_start=diagnostic_now_ns();
    const char *integrity_backend=vb_frame_sha256_backend();
    uint64_t integrity_end=diagnostic_now_ns();
    if(!strcmp(integrity_backend,"unavailable"))
        return fail(s,"source_integrity_unavailable",FRAMEPEER_UNAVAILABLE);
    fprintf(stderr,"[video-broker][frame-integrity] event=integrity_ready backend=%s"
        " small_payload_backend=openssl startup_ns=%llu phase=before_source_ready\n",
        integrity_backend,(unsigned long long)(integrity_start&&integrity_end>=integrity_start?
            integrity_end-integrity_start:0));
    const uint8_t ready[8]={'V','B','N','S',2,VBSM_SLOTS,VBSM_SLOTS,0};
    rc=framepeer_linux_send(&s->launch,ready,sizeof(ready),1000);
    if(rc)return fail(s,"source_mailbox_ready",rc);
    s->enabled=1;s->stage="source_definition_bound";return 0;
}
typedef struct {video_broker_frame_source *source;vbsp_publisher *publisher;uint64_t deadline;int error;} publication_guard;
static int publish_guard(void *opaque) {
    publication_guard *g=opaque;
    if(vbsp_stopping(g->publisher))return g->error=FRAMEPEER_RETIRED;
    int rc=current(g->source);if(rc)g->error=rc;
    return rc;
}
static int publish_hash(void *opaque,const uint8_t *pixels,size_t size,uint8_t digest[32]) {
    publication_guard *g=opaque;
    if(publish_guard(g))return -1;
    uint64_t now=diagnostic_now_ns();
    if(!now||now>=g->deadline){g->error=FRAMEPEER_DEADLINE;return -1;}
    if(vb_frame_sha256_sum(pixels,size,digest)){g->error=FRAMEPEER_RECORD;return -1;}
    now=diagnostic_now_ns();
    if(!now||now>=g->deadline){g->error=FRAMEPEER_DEADLINE;return -1;}
    /* Budget expiry is checked before any atomic publication begins. Never
     * abandon a shared slot half-written just because a scheduler paused us.
     * Capture stays independent of this finite detached RAM copy. */
    return publish_guard(g);
}
static int publish_detached(void *opaque,const vbcf_header *h,const uint8_t *pixels,size_t size,uint64_t deadline) {
    video_broker_frame_source *s=opaque;
    publication_guard guard={.source=s,.publisher=s->publisher,.deadline=deadline};
    int rc=vbsm_source_mailbox_publish_guarded(s->mailbox,(unsigned)(s->publisher_sequence%VBSM_SLOTS),h,pixels,size,publish_hash,&guard,publish_guard);
    if(rc)return guard.error==FRAMEPEER_DEADLINE?VBSP_EXPIRED:guard.error?guard.error:FRAMEPEER_RECORD;
    s->publisher_sequence++;return VBSP_OK;
}
int video_broker_frame_source_stats(video_broker_frame_source *s,vbsp_stats *out) {
    if(!s||!out)return FRAMEPEER_INVALID;
    memset(out,0,sizeof(*out));
    if(!s->publisher)return 0;
    int rc=vbsp_snapshot(s->publisher,out);
    if(!rc)s->published=out->published;
    return rc;
}
int video_broker_frame_source_output_stats(video_broker_frame_source *s,vbsp_stats *out) {
    if(!s||!out)return FRAMEPEER_INVALID;
    memset(out,0,sizeof(*out));
    return s->output_publisher?vbsp_snapshot(s->output_publisher,out):0;
}
static int publisher_health(video_broker_frame_source *s) {
    int rc=s->publisher?vbsp_failure(s->publisher):0;
    return rc?fail(s,"source_publisher_failed",rc):0;
}
int video_broker_frame_source_geometry(video_broker_frame_source *s,uint32_t width,uint32_t height,uint32_t stride,uint8_t surface) {
    if(!s||!s->initialized||s->closed)return FRAMEPEER_INVALID;
    if(s->failed)return FRAMEPEER_RETIRED;
    if(!s->enabled)return 0;
    if(s->geometry_verified)return fail(s,"source_geometry_rebind",FRAMEPEER_RETIRED);
    if(s->definition.width!=width||s->definition.height!=height||s->definition.stride_y!=stride||s->definition.stride_uv!=stride||s->definition.surface!=surface)
        return fail(s,"source_geometry_changed",FRAMEPEER_UNAUTHORIZED);
    int rc=current(s);if(rc)return fail(s,"source_geometry_authority",rc);
    s->publisher=vbsp_create((size_t)stride*height*3/2,publish_detached,s,&rc);
    if(rc||!s->publisher)return fail(s,"source_publisher_allocation",FRAMEPEER_UNAVAILABLE);
    s->geometry_verified=1;s->stage="source_geometry_bound";
    fprintf(stderr,"[video-broker][frame-integrity] algorithm=sha256 backend=%s kernel_workspace_bytes=%zu wire_contract=unchanged\n",
        vb_frame_sha256_backend(),vb_frame_sha256_workspace_bytes((size_t)stride*height*3/2));
    return 0;
}
uint64_t video_broker_frame_source_epoch(const video_broker_frame_source *s) {
    return s&&s->enabled&&s->geometry_verified&&!s->closed&&!s->failed?s->definition.source_epoch:0;
}
int video_broker_frame_source_next(video_broker_frame_source *s,vb_capture_origin *origin) {
    if(origin)*origin=(vb_capture_origin){0,0};
    if(!s||!origin||!s->initialized||s->closed)return FRAMEPEER_INVALID;
    if(s->failed)return FRAMEPEER_RETIRED;
    if(!s->enabled)return 0;
    if(!s->geometry_verified)return fail(s,"source_geometry_unverified",FRAMEPEER_UNAUTHORIZED);
    int rc=publisher_health(s);if(rc)return rc;
    rc=current(s);if(rc)return fail(s,"source_capture_authority",rc);
    if(s->sequence==UINT64_MAX)return fail(s,"source_sequence_exhausted",FRAMEPEER_RETIRED);
    *origin=(vb_capture_origin){s->definition.source_epoch,++s->sequence};return 0;
}
static int sha(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]){(void)ctx;return vb_frame_sha256_sum(p,n,out);}
int video_broker_frame_source_publish(video_broker_frame_source *s,vb_capture_origin origin,const uint8_t *pixels,size_t size,int timeout_ms) {
    if(!s||!s->initialized||s->closed)return FRAMEPEER_INVALID;
    if(s->failed)return FRAMEPEER_RETIRED;
    if(!s->enabled)return 0;
    start_attempt(s,origin,size,timeout_ms);
    if(!s->geometry_verified)return fail(s,"source_geometry_unverified",FRAMEPEER_UNAUTHORIZED);
    s->publish_attempts++;
    uint64_t wall_start=diagnostic_now_ns(),cpu_start=diagnostic_cpu_ns();
    int rc=publisher_health(s);
    observe_publish_phase(s,SOURCE_PUBLISH_HEALTH,wall_start,cpu_start);
    if(rc)return rc;
    wall_start=diagnostic_now_ns();cpu_start=diagnostic_cpu_ns();
    rc=current(s);
    observe_publish_phase(s,SOURCE_PUBLISH_AUTHORITY,wall_start,cpu_start);
    if(rc)return fail(s,"source_publish_authority",rc);
    if(!origin.epoch&&!origin.sequence){s->unavailable++;s->stage="source_origin_unavailable";return 1;}
    if(origin.epoch!=s->definition.source_epoch||!origin.sequence||origin.sequence>s->sequence)
        return fail(s,"source_origin_foreign",FRAMEPEER_UNAUTHORIZED);
    s->attempt_started_ns=diagnostic_now_ns();
    if(!pixels||size>VBCF_MAX_PAYLOAD||size!=(size_t)s->definition.stride_y*s->definition.height*3/2||timeout_ms<1||timeout_ms>5000)
        return fail(s,"source_surface_invalid",FRAMEPEER_RECORD);
    vbcf_header h=s->definition;
    h.kind=1;h.correlation=1;h.origin_sequence=origin.sequence;h.payload_bytes=(uint32_t)size;
    wall_start=diagnostic_now_ns();cpu_start=diagnostic_cpu_ns();
    rc=vbsp_submit(s->publisher,&h,pixels,size,s->attempt_started_ns+(uint64_t)timeout_ms*1000000);
    observe_publish_phase(s,SOURCE_PUBLISH_SUBMIT,wall_start,cpu_start);
    if(rc<0)return fail(s,"source_publisher_submit",rc);
    if(rc==VBSP_BUSY||rc==VBSP_EXPIRED){s->unavailable++;s->stage="source_surface_unavailable";return 1;}
    s->stage="source_surface_staged";return rc==VBSP_REPLACED?1:0;
}
static int publish_output_detached(void *opaque,const vbcf_header *h,
    const uint8_t *pixels,size_t size,uint64_t deadline) {
    video_broker_frame_source *s=opaque;
    uint64_t start=diagnostic_now_ns(),cpu_start=diagnostic_cpu_ns();
    publication_guard guard={.source=s,.publisher=s->output_publisher,.deadline=deadline};
    int rc=vbsm_source_mailbox_publish_guarded(s->output_mailbox,
        (unsigned)(s->output_publisher_sequence%VBSM_SLOTS),h,pixels,size,
        publish_hash,&guard,publish_guard);
    uint64_t finished=diagnostic_now_ns(),cpu_finished=diagnostic_cpu_ns();
    uint64_t elapsed=diagnostic_span(start,finished);
    if(!rc)s->output_publisher_sequence++;
    if(elapsed>=UINT64_C(100000000)&&
       (!s->last_slow_output_ns||diagnostic_span(s->last_slow_output_ns,finished)>=UINT64_C(1000000000))) {
        fprintf(stderr,"[video-broker][frame-source] event=source_output_slow"
            " source=%s source_epoch=%llu stream=%s origin_sequence=%llu"
            " wall_us=%llu thread_cpu_us=%lld transport=immutable_ram_mailbox"
            " payload_bytes=%zu code=%d action=preserve_capture\n",
            s->definition.source,(unsigned long long)s->definition.source_epoch,h->stream,
            (unsigned long long)h->origin_sequence,(unsigned long long)(elapsed/1000),
            cpu_start&&cpu_finished>=cpu_start?(long long)((cpu_finished-cpu_start)/1000):-1,size,rc);
        s->last_slow_output_ns=finished;
    }
    if(rc&&guard.error==FRAMEPEER_DEADLINE)return VBSP_EXPIRED;
    return rc?(guard.error?guard.error:FRAMEPEER_RECORD):VBSP_OK;
}

static int retire_output(video_broker_frame_source *s,const char *stage,int code) {
    if(!s->output_retired) {
        s->output_retired=1;
        fprintf(stderr,"[video-broker][frame-source] event=frame_output_failed"
            " stage=%s code=%d source=%s source_epoch=%llu"
            " action=retire_output_keep_capture\n",stage,code,
            s->definition.source,(unsigned long long)s->definition.source_epoch);
    }
    s->unavailable++;s->stage=stage;return 1;
}

int video_broker_frame_source_output(video_broker_frame_source *s,
    const video_broker_frame_output_layout *layout,vb_capture_origin origin,
    const uint8_t *pixels,size_t size,int timeout_ms) {
    if(!s||!s->initialized||s->closed)return FRAMEPEER_INVALID;
    if(s->failed)return FRAMEPEER_RETIRED;
    if(!s->enabled)return 0;
    if(s->output_retired){s->unavailable++;return 1;}
    start_attempt(s,origin,size,timeout_ms);
    if(!s->geometry_verified)return fail(s,"output_source_unverified",FRAMEPEER_UNAUTHORIZED);
    if(!layout||!layout->stream||!layout->transform||!pixels||!size||size>VBCF_MAX_PAYLOAD||
       timeout_ms<1||timeout_ms>5000)return fail(s,"output_layout",FRAMEPEER_INVALID);
    size_t name_size=stream_length(layout->stream);
    if(!name_size)return fail(s,"output_stream",FRAMEPEER_INVALID);
    int rc=publisher_health(s);if(rc)return rc;
    uint64_t wall_start=diagnostic_now_ns(),cpu_start=diagnostic_cpu_ns();
    rc=current(s);
    observe_output_phase(s,OUTPUT_AUTHORITY,wall_start,cpu_start);
    if(rc)return fail(s,"output_authority",rc);
    wall_start=diagnostic_now_ns();cpu_start=diagnostic_cpu_ns();
    if((origin.epoch||origin.sequence)&&
       (origin.epoch!=s->definition.source_epoch||!origin.sequence||origin.sequence>s->sequence))
        return fail(s,"output_origin_foreign",FRAMEPEER_UNAUTHORIZED);
    size_t slot=0;
    while(slot<16&&s->outputs[slot].stream[0]&&strcmp(s->outputs[slot].stream,layout->stream))slot++;
    if(slot==16)return fail(s,"output_stream_capacity",FRAMEPEER_INVALID);
    if(s->outputs[slot].sequence==UINT64_MAX)return fail(s,"output_sequence_exhausted",FRAMEPEER_RETIRED);
    vbcf_header h=s->definition;
    h.kind=5;h.surface=3;h.codec=layout->codec;h.flags=layout->flags;
    h.width=layout->width;h.height=layout->height;h.stride_y=layout->stride_y;h.stride_uv=layout->stride_uv;
    h.correlation=origin.sequence!=0;h.origin_sequence=origin.sequence;
    h.payload_bytes=(uint32_t)size;h.output_sequence=s->outputs[slot].sequence+1;
    memcpy(h.stream,layout->stream,name_size+1);memcpy(h.transform,layout->transform,32);
    if(vbcf_validate(&h))return fail(s,"output_contract",FRAMEPEER_RECORD);
    if(s->output_publisher&&vbsp_failure(s->output_publisher))
        return retire_output(s,"output_worker_failed",vbsp_failure(s->output_publisher));
    if(!s->output_publisher) {
        if(!s->output_mailbox||size!=s->output_capacity)
            return fail(s,"output_mailbox_contract",FRAMEPEER_INVALID);
        s->output_publisher=vbsp_create(size,publish_output_detached,s,&rc);
        if(rc||!s->output_publisher)return retire_output(s,"output_publisher_allocation",FRAMEPEER_UNAVAILABLE);
    }
    if(size!=s->output_capacity)return retire_output(s,"output_geometry_changed",FRAMEPEER_UNAUTHORIZED);
    uint64_t now=diagnostic_now_ns();
    if(!now||now>UINT64_MAX-(uint64_t)timeout_ms*1000000)
        return retire_output(s,"output_budget",FRAMEPEER_UNAVAILABLE);
    memcpy(s->outputs[slot].stream,layout->stream,name_size+1);
    s->outputs[slot].sequence=h.output_sequence;
    observe_output_phase(s,OUTPUT_LAYOUT,wall_start,cpu_start);
    wall_start=diagnostic_now_ns();cpu_start=diagnostic_cpu_ns();
    rc=vbsp_submit(s->output_publisher,&h,pixels,size,now+(uint64_t)timeout_ms*1000000);
    observe_output_phase(s,OUTPUT_SUBMIT,wall_start,cpu_start);
    if(rc==VBSP_BUSY||rc==VBSP_EXPIRED||rc==VBSP_REPLACED||rc==VBSP_CLOSED) {
        s->unavailable++;s->stage="source_output_unavailable";return 1;
    }
    if(rc)return retire_output(s,"output_publisher_submit",rc);
    s->stage="source_output_staged";return 0;
}
int video_broker_frame_source_nv12_output(video_broker_frame_source *s,const char *stream,
    vb_capture_origin origin,const uint8_t *pixels,size_t size,int width,int height,
    int orientation,int aspect_policy,int timeout_ms) {
    if(!s||!s->initialized||s->closed)return FRAMEPEER_INVALID;
    if(s->failed)return FRAMEPEER_RETIRED;
    if(!s->enabled)return 0;
    uint8_t digest[32];
    uint64_t wall_start=diagnostic_now_ns(),cpu_start=diagnostic_cpu_ns();
    int rc=video_broker_nv12_transform_digest((int)s->definition.width,(int)s->definition.height,
        (int)s->definition.stride_y,width,height,(video_broker_video_orientation_t)orientation,
        (video_broker_nv12_aspect_policy_t)aspect_policy,sha,NULL,digest);
    observe_output_phase(s,OUTPUT_TRANSFORM,wall_start,cpu_start);
    if(rc)return fail(s,"output_transform",FRAMEPEER_RECORD);
    video_broker_frame_output_layout layout={.stream=stream,.codec=3,
        .width=(uint32_t)width,.height=(uint32_t)height,.stride_y=(uint32_t)width,.stride_uv=(uint32_t)width,.transform=digest};
    return video_broker_frame_source_output(s,&layout,origin,pixels,size,timeout_ms);
}
int video_broker_frame_source_close(video_broker_frame_source *s) {
    if(!s)return FRAMEPEER_INVALID;
    s->closed=1;
    if(s->output_publisher) {
        /* No consumer socket can hold this finite detached RAM publication. */
        if(vbsp_close(s->output_publisher,5000))return FRAMEPEER_UNAVAILABLE;
        s->output_publisher=NULL;
    }
    int output_mailbox=vbsm_source_mailbox_close(s->output_mailbox);
    if(!output_mailbox)s->output_mailbox=NULL;
    if(s->publisher) {
        vbsp_stats stats;
        (void)video_broker_frame_source_stats(s,&stats);
        if(vbsp_close(s->publisher,1000))return FRAMEPEER_UNAVAILABLE;
        s->publisher=NULL;
    }
    int mailbox=vbsm_source_mailbox_close(s->mailbox);
    if(!mailbox)s->mailbox=NULL;
    int peer=s->launch.initialized?framepeer_linux_close(&s->launch):0;
    if(s->authority_mutex_ready) {
        if(pthread_mutex_destroy(&s->authority_mutex))return FRAMEPEER_UNAVAILABLE;
        s->authority_mutex_ready=0;
    }
    return mailbox||output_mailbox?FRAMEPEER_INVALID:peer;
}
