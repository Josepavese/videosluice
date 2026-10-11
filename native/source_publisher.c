#define _GNU_SOURCE
#include "source_publisher.h"
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <time.h>
#include <unistd.h>

enum {SLOT_FREE,SLOT_WRITING,SLOT_PENDING,SLOT_RUNNING};
typedef struct {
    _Atomic int state;
    _Atomic uint64_t order;
    uint8_t *pixels;
    vbcf_header header;
    size_t size;
    uint64_t deadline;
} vbsp_slot;
struct vbsp_publisher {
    /* Only shutdown joins use this mutex, never capture or publishing. */
    pthread_mutex_t join_mutex;
    pthread_cond_t exited_condition;
    pthread_t worker;
    int mutex_ready,cond_ready,thread_ready,joined,exited,release_error,wake_fd;
    _Atomic int stopping,failure;
    _Atomic uint64_t submitted,attempted,published,replaced,busy,expired,canceled;
    _Atomic uint64_t work_ns,work_max_ns,copy_ns,copy_max_ns;
    size_t capacity;
    uint8_t *pixels;
    vbsp_slot slots[VBSP_SLOTS];
    vbsp_publish_fn publish;
    void *context;
};
uint64_t vbsp_now_ns(void) {
    struct timespec t;
    if(clock_gettime(CLOCK_MONOTONIC,&t)||t.tv_sec<0)return 0;
    return (uint64_t)t.tv_sec*UINT64_C(1000000000)+(uint64_t)t.tv_nsec;
}
size_t vbsp_bytes(size_t capacity) {
    return capacity&&capacity<=VBCF_MAX_PAYLOAD?
        VBSP_SLOTS*capacity+sizeof(vbsp_publisher)+VBSP_THREAD_ALLOWANCE:0;
}
int vbsp_stopping(const vbsp_publisher *p) {
    return !p||atomic_load_explicit(&p->stopping,memory_order_acquire);
}
int vbsp_failure(const vbsp_publisher *p) {
    return p?atomic_load_explicit(&p->failure,memory_order_acquire):VBSP_INVALID;
}
static void notify(vbsp_publisher *p) {
    uint64_t one=1;
    ssize_t n=write(p->wake_fd,&one,sizeof(one));
    if(n<0&&errno==EINTR)n=write(p->wake_fd,&one,sizeof(one));
    if(n!=(ssize_t)sizeof(one)&&!(n<0&&errno==EAGAIN)) {
        atomic_store_explicit(&p->failure,VBSP_FAILED,memory_order_release);
        atomic_store_explicit(&p->stopping,1,memory_order_release);
    }
}
static int claim_pending(vbsp_publisher *p) {
    /* Order reads are atomic. Only a successful CAS permits pixel access. */
    for(int attempt=0;attempt<VBSP_SLOTS;attempt++) {
        int chosen=-1;uint64_t order=UINT64_MAX;
        for(int i=0;i<VBSP_SLOTS;i++)if(atomic_load_explicit(&p->slots[i].state,memory_order_acquire)==SLOT_PENDING) {
            uint64_t candidate=atomic_load_explicit(&p->slots[i].order,memory_order_relaxed);
            if(chosen<0||candidate<order){chosen=i;order=candidate;}
        }
        if(chosen<0)return -1;
        int state=SLOT_PENDING;
        if(atomic_compare_exchange_strong_explicit(&p->slots[chosen].state,&state,SLOT_RUNNING,memory_order_acquire,memory_order_relaxed)) {
            /* A submit may replace PENDING between the order read and CAS.
             * Do not publish its newer frame before an older pending slot. */
            if(atomic_load_explicit(&p->slots[chosen].order,memory_order_relaxed)==order)return chosen;
            atomic_store_explicit(&p->slots[chosen].state,SLOT_PENDING,memory_order_release);
        }
    }
    return -1;
}
static void *publish_loop(void *arg) {
    vbsp_publisher *p=arg;
    while(!vbsp_stopping(p)) {
        int index=claim_pending(p);
        if(index<0) {
            /* eventfd retains wakeups racing the empty-slot observation.
             * Finite poll also observes terminal notification failures. */
            struct pollfd wake={.fd=p->wake_fd,.events=POLLIN};
            int rc=poll(&wake,1,1000);
            if(rc<0&&errno==EINTR)continue;
            if(rc<0||(rc>0&&(wake.revents&(POLLERR|POLLHUP|POLLNVAL)))) {
                atomic_store(&p->failure,VBSP_FAILED);atomic_store(&p->stopping,1);break;
            }
            if(rc>0) {
                uint64_t count;ssize_t bytes=read(p->wake_fd,&count,sizeof(count));
                if(bytes!=(ssize_t)sizeof(count)&&!(bytes<0&&(errno==EAGAIN||errno==EINTR))) {
                    atomic_store(&p->failure,VBSP_FAILED);atomic_store(&p->stopping,1);
                }
            }
            continue;
        }
        vbsp_slot *slot=&p->slots[index];uint64_t begin=vbsp_now_ns();
        atomic_fetch_add(&p->attempted,1);
        int rc=!begin?VBSP_FAILED:begin>=slot->deadline?VBSP_EXPIRED:
            p->publish(p->context,&slot->header,slot->pixels,slot->size,slot->deadline);
        uint64_t end=vbsp_now_ns(),elapsed=end>=begin?end-begin:0;
        atomic_fetch_add(&p->work_ns,elapsed);
        if(elapsed>atomic_load(&p->work_max_ns))atomic_store(&p->work_max_ns,elapsed);
        if(rc==VBSP_OK)atomic_fetch_add(&p->published,1);
        else if(rc==VBSP_EXPIRED)atomic_fetch_add(&p->expired,1);
        else if(vbsp_stopping(p))atomic_fetch_add(&p->canceled,1);
        else {atomic_store(&p->failure,rc?rc:VBSP_FAILED);atomic_store(&p->stopping,1);}
        atomic_store_explicit(&slot->state,SLOT_FREE,memory_order_release);
    }
    for(int i=0;i<VBSP_SLOTS;i++) {
        int pending=SLOT_PENDING;
        if(atomic_compare_exchange_strong(&p->slots[i].state,&pending,SLOT_FREE))atomic_fetch_add(&p->canceled,1);
    }
    pthread_mutex_lock(&p->join_mutex);p->exited=1;
    pthread_cond_broadcast(&p->exited_condition);pthread_mutex_unlock(&p->join_mutex);
    return NULL;
}
vbsp_publisher *vbsp_create(size_t capacity,vbsp_publish_fn publish,void *context,int *error) {
    if(!error)return NULL;
    *error=VBSP_INVALID;
    if(!vbsp_bytes(capacity)||!publish)return NULL;
    vbsp_publisher *p=calloc(1,sizeof(*p));if(!p)return NULL;
    p->wake_fd=-1;p->capacity=capacity;p->publish=publish;p->context=context;
#define INIT(name) atomic_init(&p->name,0)
    INIT(stopping);INIT(failure);INIT(submitted);INIT(attempted);INIT(published);INIT(replaced);
    INIT(busy);INIT(expired);INIT(canceled);INIT(work_ns);INIT(work_max_ns);INIT(copy_ns);INIT(copy_max_ns);
#undef INIT
    if(!atomic_is_lock_free(&p->stopping)||!atomic_is_lock_free(&p->submitted))goto failed;
    p->pixels=malloc(VBSP_SLOTS*capacity);if(!p->pixels)goto failed;
    for(int i=0;i<VBSP_SLOTS;i++) {
        atomic_init(&p->slots[i].state,SLOT_FREE);atomic_init(&p->slots[i].order,0);
        p->slots[i].pixels=p->pixels+(size_t)i*capacity;
    }
    p->wake_fd=eventfd(0,EFD_CLOEXEC|EFD_NONBLOCK);if(p->wake_fd<0)goto failed;
    if(pthread_mutex_init(&p->join_mutex,NULL))goto failed;
    p->mutex_ready=1;
    pthread_condattr_t cond;
    if(pthread_condattr_init(&cond))goto failed;
    int rc=pthread_condattr_setclock(&cond,CLOCK_MONOTONIC);
    if(!rc)rc=pthread_cond_init(&p->exited_condition,&cond);
    pthread_condattr_destroy(&cond);
    if(rc)goto failed;
    p->cond_ready=1;
    pthread_attr_t attr;
    if(pthread_attr_init(&attr))goto failed;
    rc=pthread_attr_setstacksize(&attr,VBSP_STACK_BYTES);
    if(!rc)rc=pthread_create(&p->worker,&attr,publish_loop,p);
    pthread_attr_destroy(&attr);
    if(rc)goto failed;
    p->thread_ready=1;*error=VBSP_OK;return p;
failed:
    if(vbsp_close(p,1))return p;
    return NULL;
}
int vbsp_submit(vbsp_publisher *p,const vbcf_header *header,const uint8_t *pixels,size_t size,uint64_t deadline) {
    if(!p||!header||!pixels||!size||size>p->capacity||header->payload_bytes!=size)return VBSP_INVALID;
    if(vbsp_stopping(p))return VBSP_CLOSED;
    uint64_t begin=vbsp_now_ns();if(!begin)return VBSP_INVALID;
    if(begin>=deadline){atomic_fetch_add(&p->expired,1);return VBSP_EXPIRED;}
    if(atomic_load(&p->submitted)==UINT64_MAX)return VBSP_FAILED;
    int chosen=-1,replaced=0;
    /* Single submitter, one worker, two slots. Bounded scans cover worker
     * handoffs without spinning or waiting for scheduling/I/O. */
    for(int attempt=0;attempt<3&&chosen<0;attempt++) {
        for(int pass=0;pass<2&&chosen<0;pass++)for(int i=0;i<VBSP_SLOTS;i++) {
            int state=pass==0?SLOT_FREE:SLOT_PENDING,expected=state;
            if(atomic_compare_exchange_strong_explicit(&p->slots[i].state,&expected,SLOT_WRITING,memory_order_acquire,memory_order_relaxed)) {
                chosen=i;replaced=state==SLOT_PENDING;break;
            }
        }
    }
    if(chosen<0){atomic_fetch_add(&p->busy,1);return VBSP_BUSY;}
    vbsp_slot *slot=&p->slots[chosen];
    if(replaced)atomic_fetch_add(&p->replaced,1);
    slot->header=*header;memcpy(slot->pixels,pixels,size);slot->size=size;slot->deadline=deadline;
    uint64_t order=atomic_fetch_add(&p->submitted,1)+1;
    atomic_store_explicit(&slot->order,order,memory_order_relaxed);
    uint64_t end=vbsp_now_ns(),elapsed=end>=begin?end-begin:0;
    atomic_fetch_add(&p->copy_ns,elapsed);
    if(elapsed>atomic_load(&p->copy_max_ns))atomic_store(&p->copy_max_ns,elapsed);
    if(vbsp_stopping(p)) {
        atomic_fetch_add(&p->canceled,1);atomic_store_explicit(&slot->state,SLOT_FREE,memory_order_release);return VBSP_CLOSED;
    }
    atomic_store_explicit(&slot->state,SLOT_PENDING,memory_order_release);notify(p);
    return replaced?VBSP_REPLACED:VBSP_OK;
}
int vbsp_snapshot(vbsp_publisher *p,vbsp_stats *out) {
    if(!p||!out)return VBSP_INVALID;
    memset(out,0,sizeof(*out));
#define READ(name) out->name=atomic_load(&p->name)
    READ(submitted);READ(attempted);READ(published);READ(replaced);READ(busy);READ(expired);READ(canceled);
    READ(work_ns);READ(work_max_ns);READ(copy_ns);READ(copy_max_ns);READ(failure);
#undef READ
    for(int i=0;i<VBSP_SLOTS;i++) {
        int state=atomic_load_explicit(&p->slots[i].state,memory_order_acquire);
        out->pending+=state==SLOT_PENDING;out->inflight+=state==SLOT_RUNNING;
    }
    out->capacity=VBSP_SLOTS;
    return VBSP_OK;
}
int vbsp_close(vbsp_publisher *p,unsigned timeout_ms) {
    if(!p)return VBSP_OK;
    if(p->release_error)return VBSP_FAILED;
    if(!timeout_ms||timeout_ms>5000)return VBSP_INVALID;
    atomic_store_explicit(&p->stopping,1,memory_order_release);
    if(p->thread_ready&&!p->joined) {
        notify(p);
        uint64_t now=vbsp_now_ns();if(!now)return VBSP_FAILED;
        uint64_t limit=now+(uint64_t)timeout_ms*1000000;
        struct timespec until={(time_t)(limit/1000000000),(long)(limit%1000000000)};
        pthread_mutex_lock(&p->join_mutex);
        int rc=0;
        while(!p->exited&&!rc)rc=pthread_cond_timedwait(&p->exited_condition,&p->join_mutex,&until);
        int exited=p->exited;pthread_mutex_unlock(&p->join_mutex);
        if(!exited)return VBSP_BUSY;
        if(pthread_join(p->worker,NULL)){p->release_error=1;return VBSP_FAILED;}
        p->joined=1;
    }
    if(p->cond_ready&&pthread_cond_destroy(&p->exited_condition)){p->release_error=1;return VBSP_FAILED;}
    p->cond_ready=0;
    if(p->mutex_ready&&pthread_mutex_destroy(&p->join_mutex)){p->release_error=1;return VBSP_FAILED;}
    if(p->wake_fd>=0&&close(p->wake_fd)&&errno!=EINTR){p->release_error=1;return VBSP_FAILED;}
    free(p->pixels);free(p);return VBSP_OK;
}
