#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdalign.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include "launch.h"
#include "../internal/framehash/frame_sha256_linux.h"

/* Opaque state for Python: no replicated ABI struct layout and no native heap.
 * This only exposes the existing Linux provider, not a second authenticator. */
_Static_assert(alignof(framepeer_linux_launch) <= alignof(uint64_t), "Python state alignment");
size_t video_broker_frame_peer_state_bytes(void) { return sizeof(framepeer_linux_launch); }
const char *video_broker_frame_peer_stage(const framepeer_linux_launch *state) {
    return state && state->stage ? state->stage : "none";
}
/* Conservative per-concurrent-receive quote, including two transient crypto
 * FDs and capped kernel buffers. Launch composition must reserve it in addition
 * to existing frame/transport storage, regardless of the selected backend. */
size_t video_broker_frame_data_workspace_bytes(void) { return VB_FRAME_SHA256_KERNEL_WORKSPACE; }
const char *video_broker_frame_data_integrity_backend(void) { return vb_frame_sha256_backend(); }

static int data_failure(framepeer_linux_launch *state, const char *stage, int code) {
    if(state) state->stage=stage;
    return code;
}

/* Per-thread, fixed-size diagnostics for the last delivery call. No frame
 * bytes, global lock or logging are retained. The Python caller reads these
 * immediately on the same worker thread, including after a native failure. */
static _Thread_local uint64_t delivery_timing_us[4];
static uint64_t monotonic_us(void) {
    struct timespec now;
    if(clock_gettime(CLOCK_MONOTONIC,&now) || now.tv_sec<0) return 0;
    return (uint64_t)now.tv_sec*UINT64_C(1000000)+(uint64_t)now.tv_nsec/1000;
}
static uint64_t elapsed_us(uint64_t started) {
    uint64_t finished=monotonic_us();
    return started && finished>=started ? finished-started : 0;
}
int video_broker_frame_delivery_last_timing(uint64_t *out, size_t count) {
    if(!out || count!=4) return -1;
    memcpy(out,delivery_timing_us,sizeof(delivery_timing_us));
    return 0;
}

/* FAPD is shared by frame delivery and ROI. Reuse the provider's admitted
 * record scratch and the caller's exclusive output; no heap, FD, thread or
 * additional frame queue. Every record still passes the existing credential,
 * pidfd and deadline checks. Compute full pixel SHA while owning the input;
 * the SDK compares this independently calculated digest to its trusted header.
 * The qualified crypto backend has its separately admitted temporary footprint.
 * progress = records, wire bytes, validated payload bytes; partial bytes are
 * diagnostic only and MUST be discarded on any error. */
static int data_receive_sha256(framepeer_linux_launch *state, uint8_t *scratch,
        size_t scratch_size, uint8_t *output, size_t size, const uint8_t *digest,
        int64_t until, uint64_t *progress, uint8_t *payload_sha, uint64_t *timing) {
    if(progress) memset(progress,0,3*sizeof(*progress));
    if(payload_sha) memset(payload_sha,0,32);
    if(!state || !scratch || scratch_size!=16384 || !output || !digest || !progress || !payload_sha ||
       !size || size>16*1024*1024)
        return data_failure(state,"data_contract",FRAMEPEER_INVALID);
    uint64_t started=monotonic_us();
    for(size_t offset=0; offset<size;) {
        size_t got=0;
        int rc=framepeer_linux_receive_until(state,scratch,scratch_size,&got,until);
        if(rc) return rc;
        progress[0]++;
        progress[1]+=got;
        if(got<=44 || got>scratch_size || memcmp(scratch,"FAPD\1\0",6) ||
           (((size_t)scratch[6]<<8)|scratch[7])!=got || memcmp(scratch+8,digest,32))
            return data_failure(state,"data_record_identity",FRAMEPEER_RECORD);
        uint32_t at=(uint32_t)scratch[40]<<24 | (uint32_t)scratch[41]<<16 |
            (uint32_t)scratch[42]<<8 | scratch[43];
        if(at!=offset || got-44>size-offset)
            return data_failure(state,"data_record_extent",FRAMEPEER_RECORD);
        memcpy(output+offset,scratch+44,got-44);
        offset+=got-44;
        progress[2]=offset;
    }
    if(timing) timing[2]=elapsed_us(started);
    started=monotonic_us();
    if(vb_frame_sha256_sum_until(output,size,payload_sha,until))
        return data_failure(state,errno==ETIMEDOUT ? "data_integrity_deadline" : "data_integrity",
                            errno==ETIMEDOUT ? FRAMEPEER_DEADLINE : FRAMEPEER_UNAVAILABLE);
    if(timing) timing[3]=elapsed_us(started);
    int rc=framepeer_linux_current(state);
    if(rc) memset(payload_sha,0,32);
    if(rc) return rc;
    struct timespec now;
    if(clock_gettime(CLOCK_MONOTONIC,&now) || now.tv_sec<0 ||
       now.tv_sec>(INT64_MAX-INT64_C(999999999))/INT64_C(1000000000)) {
        memset(payload_sha,0,32);
        return data_failure(state,"data_final_clock",FRAMEPEER_UNAVAILABLE);
    }
    if((int64_t)now.tv_sec*INT64_C(1000000000)+now.tv_nsec>=until) {
        memset(payload_sha,0,32);
        return data_failure(state,"data_final_deadline",FRAMEPEER_DEADLINE);
    }
    return 0;
}

int video_broker_frame_data_receive_sha256(framepeer_linux_launch *state, uint8_t *scratch,
        size_t scratch_size, uint8_t *output, size_t size, const uint8_t *digest,
        int64_t until, uint64_t *progress, uint8_t *payload_sha) {
    return data_receive_sha256(state,scratch,scratch_size,output,size,digest,until,
                               progress,payload_sha,NULL);
}

/* One native delivery transaction, using the SAME FAPN/FAPB/VBCF/FAPD wire
 * records and peer implementation. No interpreter resumption between request,
 * descriptor and payload. Python still validates the complete publication and
 * header before exposing the caller-owned bytes. Native admission bounds any
 * unvalidated descriptor to the already reserved output and total deadline.
 * counters: payload records/wire/validated bytes, metadata/header extents,
 * declared payload bytes. Failure leaves no usable digest or committed frame. */
int video_broker_frame_delivery_receive_sha256(framepeer_linux_launch *state,
        uint8_t *scratch,size_t scratch_size,const uint8_t *request,size_t request_size,
        uint8_t *metadata,size_t metadata_capacity,uint8_t *header,size_t header_capacity,
        uint8_t *output,size_t maximum,int64_t until,uint64_t *counters,uint8_t *payload_sha) {
    memset(delivery_timing_us,0,sizeof(delivery_timing_us));
    if(counters) memset(counters,0,6*sizeof(*counters));
    if(payload_sha) memset(payload_sha,0,32);
    if(!state || !scratch || scratch_size!=16384 || !request || request_size!=24 ||
       !metadata || metadata_capacity!=512 || !header || header_capacity!=448 ||
       !output || !maximum || maximum>16*1024*1024 || !counters || !payload_sha)
        return data_failure(state,"delivery_transaction_contract",FRAMEPEER_INVALID);
    if(memcmp(request,"FAPN\1\0\0\0",8))
        return data_failure(state,"delivery_request_identity",FRAMEPEER_INVALID);
    uint64_t epoch=0,last=0;
    for(size_t i=0;i<8;i++) { epoch=(epoch<<8)|request[8+i];last=(last<<8)|request[16+i]; }
    if(!epoch && last)
        return data_failure(state,"delivery_request_epoch",FRAMEPEER_INVALID);
    uint64_t started=monotonic_us();
    int rc=framepeer_linux_send_until(state,request,request_size,until);
    delivery_timing_us[0]=elapsed_us(started);
    if(rc) return rc;
    started=monotonic_us();
    size_t got=0;
    if(!epoch) {
        rc=framepeer_linux_receive_until(state,metadata,metadata_capacity,&got,until);
        if(rc) return rc;
        counters[3]=got;
        if(got<174 || memcmp(metadata,"FAPB\1\0",6) ||
           (((size_t)metadata[6]<<8)|metadata[7])!=got)
            return data_failure(state,"delivery_metadata_record",FRAMEPEER_RECORD);
    }
    rc=framepeer_linux_receive_until(state,header,header_capacity,&got,until);
    delivery_timing_us[1]=elapsed_us(started);
    if(rc) return rc;
    counters[4]=got;
    if(got==24 && !memcmp(header,"FAPI\1\0\0\0",8)) {
        /* Exact epoch/last comparison remains mandatory in DeliveryReader;
         * receipt already passed native identity and total-deadline checks. */
        return 0;
    }
    if(got!=448 || memcmp(header,"VBCF\0\1\1\300",8) || header[8]!=2 || header[9]!=3)
        return data_failure(state,"delivery_header_record",FRAMEPEER_RECORD);
    uint32_t size=(uint32_t)header[16]<<24 | (uint32_t)header[17]<<16 |
        (uint32_t)header[18]<<8 | header[19];
    counters[5]=size;
    if(!size || size>maximum)
        return data_failure(state,"delivery_payload_admission",FRAMEPEER_RECORD);
    uint8_t descriptor_sha[32];
    if(vb_frame_sha256_sum_until(header,448,descriptor_sha,until))
        return data_failure(state,errno==ETIMEDOUT?"delivery_descriptor_deadline":"delivery_descriptor_integrity",
                            errno==ETIMEDOUT?FRAMEPEER_DEADLINE:FRAMEPEER_UNAVAILABLE);
    rc=data_receive_sha256(state,scratch,scratch_size,output,size,
                           descriptor_sha,until,counters,payload_sha,delivery_timing_us);
    if(rc) { memset(payload_sha,0,32);return rc; }
    if(memcmp(payload_sha,header+168,32)) {
        memset(payload_sha,0,32);
        return data_failure(state,"delivery_payload_integrity",FRAMEPEER_RECORD);
    }
    return 0;
}
