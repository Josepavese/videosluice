#define _GNU_SOURCE
#include <assert.h>
#include <pthread.h>
#include "frame_peer_python.c"

static void record_invalid_delivery(uint64_t *pair) {
    uint64_t counters[6];
    uint8_t sha[32];
    int rc=video_broker_frame_delivery_receive_sha256(NULL,NULL,0,NULL,0,
        NULL,0,NULL,0,NULL,0,0,counters,sha);
    assert(rc==FRAMEPEER_INVALID);
    assert(video_broker_frame_delivery_last_boundary(&pair[0],&pair[1])==0);
    assert(pair[0]>0 && pair[1]>=pair[0]);
    for(size_t i=0;i<6;i++) assert(counters[i]==0);
    for(size_t i=0;i<32;i++) assert(sha[i]==0);
    uint64_t authority[4];
    assert(video_broker_frame_peer_current_timed(NULL,NULL)==FRAMEPEER_INVALID);
    assert(video_broker_frame_peer_current_timed(NULL,authority)==FRAMEPEER_INVALID);
    assert(authority[0]>0 && authority[1]>=authority[0]);
    assert(authority[2]>0 && authority[3]>=authority[2]);
}

static void *worker(void *argument) {
    record_invalid_delivery(argument);
    return NULL;
}

int main(void) {
    uint64_t owner[2], other[2], after[2];
    assert(video_broker_frame_delivery_last_boundary(NULL,&owner[1])==-1);
    assert(video_broker_frame_delivery_last_boundary(&owner[0],NULL)==-1);
    assert(video_broker_frame_delivery_last_boundary(&owner[0],&owner[1])==0);
    assert(owner[0]==0 && owner[1]==0);
    record_invalid_delivery(owner);
    pthread_t thread;
    assert(pthread_create(&thread,NULL,worker,other)==0);
    assert(pthread_join(thread,NULL)==0);
    assert(other[0]>=owner[1]);
    assert(video_broker_frame_delivery_last_boundary(&after[0],&after[1])==0);
    assert(after[0]==owner[0] && after[1]==owner[1]);
    return 0;
}
