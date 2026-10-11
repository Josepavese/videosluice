#define _GNU_SOURCE
#include <stdatomic.h>
#include "launch.c"
#define video_broker_frame_delivery_receive_sha256 real_delivery_exchange
#include "frame_peer_python.c"
#undef video_broker_frame_delivery_receive_sha256
extern int PyGILState_Check(void);
static _Atomic int receiving,producing,gate,finished;
static int64_t probe_now(void) {
    int64_t result;
    return now_ns(&result)?0:result;
}
static void pause_briefly(void) {
    struct timespec delay={.tv_nsec=100000};
    nanosleep(&delay,NULL);
}
int video_broker_frame_delivery_receive_sha256(framepeer_linux_launch *state,
        uint8_t *scratch,size_t scratch_size,const uint8_t *request,size_t request_size,
        uint8_t *metadata,size_t metadata_capacity,uint8_t *header,size_t header_capacity,
        uint8_t *output,size_t maximum,int64_t until,uint64_t *counters,uint8_t *payload_sha) {
    atomic_store(&receiving,1);
    int rc=real_delivery_exchange(state,scratch,scratch_size,request,request_size,
        metadata,metadata_capacity,header,header_capacity,output,maximum,until,counters,payload_sha);
    atomic_store(&finished,1);
    return rc;
}
int delivery_probe_started(void) {
    int64_t until=probe_now()+INT64_C(1000000000);
    while(!atomic_load(&receiving)||!atomic_load(&producing)) {
        if(probe_now()>=until)return -1;
        pause_briefly();
    }
    return 0;
}
int delivery_probe_complete_with_gil(void) {
    if(!PyGILState_Check())return -2;
    if(atomic_load(&finished))return -3;
    atomic_store(&gate,1);
    int64_t until=probe_now()+INT64_C(500000000);
    while(!atomic_load(&finished)) {
        if(probe_now()>=until)return -1;
        pause_briefly();
    }
    return PyGILState_Check()?0:-4;
}
static int fixture_ready(int fd,short event,int64_t until) {
    int64_t now=probe_now();
    if(!now||now>=until)return -1;
    struct pollfd item={.fd=fd,.events=event};
    return poll(&item,1,(int)((until-now+999999)/1000000))>0 && (item.revents&event)?0:-1;
}
int delivery_probe_produce(int fd,const uint8_t *records,size_t size) {
    if(!records||size>1048576)return -1;
    atomic_store(&producing,1);
    int64_t until=probe_now()+INT64_C(1000000000);
    while(!atomic_load(&gate)) {
        if(probe_now()>=until)return -2;
        pause_briefly();
    }
    uint8_t request[24];
    if(fixture_ready(fd,POLLIN,until)||recv(fd,request,sizeof(request),0)!=24||memcmp(request,"FAPN",4))return -3;
    for(size_t offset=0;offset<size;) {
        if(size-offset<2)return -4;
        size_t count=((size_t)records[offset]<<8)|records[offset+1];
        offset+=2;
        if(!count||count>16384||count>size-offset)return -5;
        if(fixture_ready(fd,POLLOUT,until)||send(fd,records+offset,count,MSG_NOSIGNAL)!=(ssize_t)count)return -6;
        offset+=count;
    }
    return 0;
}
