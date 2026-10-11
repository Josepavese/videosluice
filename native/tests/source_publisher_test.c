#define _GNU_SOURCE
#include "source_publisher.h"
#include <assert.h>
#include <dirent.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t changed;
    int hold,entered,release,fail;
    uint64_t seen[4],last;size_t count;
    unsigned source;
} fixture;
static void nap(unsigned ms) {
    struct timespec t={(time_t)(ms/1000),(long)(ms%1000)*1000000};
    nanosleep(&t,NULL);
}
static uint8_t pattern(unsigned source,uint64_t seq,size_t i) {
    return (uint8_t)(source*47u+(unsigned)seq*13u+(unsigned)i*7u);
}
static int publish(void *context,const vbcf_header *h,const uint8_t *pixels,size_t n,uint64_t deadline) {
    fixture *f=context;
    assert(vbsp_now_ns()<deadline);
    pthread_mutex_lock(&f->mutex);f->entered=1;pthread_cond_broadcast(&f->changed);
    while(f->hold&&!f->release)pthread_cond_wait(&f->changed,&f->mutex);
    assert(h->source_epoch==f->source&&h->payload_bytes==n);
    assert(h->origin_sequence>f->last);f->last=h->origin_sequence;
    for(size_t i=0;i<n;i++)assert(pixels[i]==pattern(f->source,h->origin_sequence,i));
    if(f->count<4)f->seen[f->count]=h->origin_sequence;
    f->count++;int rc=f->fail;
    pthread_mutex_unlock(&f->mutex);return rc;
}
static vbsp_publisher *open_fixture(fixture *f,unsigned source,int hold) {
    memset(f,0,sizeof(*f));f->source=source;f->hold=hold;
    assert(!pthread_mutex_init(&f->mutex,NULL));assert(!pthread_cond_init(&f->changed,NULL));
    int error=99;vbsp_publisher *p=vbsp_create(4096,publish,f,&error);
    assert(p&&error==VBSP_OK);return p;
}
static void release(fixture *f) {
    pthread_mutex_lock(&f->mutex);f->release=1;pthread_cond_broadcast(&f->changed);pthread_mutex_unlock(&f->mutex);
}
static void entered(fixture *f) {
    uint64_t until=vbsp_now_ns()+1000000000;
    for(;;) {
        pthread_mutex_lock(&f->mutex);int ready=f->entered;pthread_mutex_unlock(&f->mutex);
        if(ready)return;
        assert(vbsp_now_ns()<until);nap(1);
    }
}
static int submit(vbsp_publisher *p,unsigned source,uint64_t sequence,uint64_t deadline) {
    uint8_t pixels[4096];for(size_t i=0;i<sizeof(pixels);i++)pixels[i]=pattern(source,sequence,i);
    vbcf_header h={.source_epoch=source,.origin_sequence=sequence,.payload_bytes=sizeof(pixels)};
    int rc=vbsp_submit(p,&h,pixels,sizeof(pixels),deadline);
    memset(pixels,0xff,sizeof(pixels));memset(&h,0xff,sizeof(h));return rc;
}
static vbsp_stats completed(vbsp_publisher *p,uint64_t published,uint64_t expired) {
    uint64_t until=vbsp_now_ns()+1000000000;vbsp_stats s;
    do {
        assert(vbsp_snapshot(p,&s)==VBSP_OK);
        if(s.published>=published&&s.expired>=expired&&!s.pending&&!s.inflight)return s;
        assert(vbsp_now_ns()<until);nap(1);
    }while(1);
}
static void close_fixture(vbsp_publisher *p,fixture *f) {
    assert(vbsp_close(p,1000)==VBSP_OK);
    assert(!pthread_cond_destroy(&f->changed));assert(!pthread_mutex_destroy(&f->mutex));
}
static size_t fd_count(void) {
    DIR *d=opendir("/proc/self/fd");assert(d);size_t n=0;
    while(readdir(d))n++;
    assert(!closedir(d));return n;
}
static void saturated_sources(void) {
    fixture a,b;vbsp_publisher *p=open_fixture(&a,7,0),*q=open_fixture(&b,11,0);
    /* Both worker threads contend against their independent single producer.
     * Exercise replacement/claim ABA, immutable bytes and retained wakeups. */
    for(uint64_t i=1;i<=10000;i++) {
        int r=submit(p,7,i,vbsp_now_ns()+1000000000);
        int s=submit(q,11,i,vbsp_now_ns()+1000000000);
        assert(r==VBSP_OK||r==VBSP_REPLACED);
        assert(s==VBSP_OK||s==VBSP_REPLACED);
    }
    uint64_t until=vbsp_now_ns()+1000000000;
    for(;;) {
        vbsp_stats s,t;assert(!vbsp_snapshot(p,&s)&&!vbsp_snapshot(q,&t));
        assert(!s.failure&&!t.failure&&!s.expired&&!t.expired);
        if(s.submitted==s.published+s.replaced&&t.submitted==t.published+t.replaced&&
            !s.pending&&!s.inflight&&!t.pending&&!t.inflight)break;
        assert(vbsp_now_ns()<until);nap(1);
    }
    pthread_mutex_lock(&a.mutex);assert(a.last==10000);pthread_mutex_unlock(&a.mutex);
    pthread_mutex_lock(&b.mutex);assert(b.last==10000);pthread_mutex_unlock(&b.mutex);
    close_fixture(p,&a);close_fixture(q,&b);
}
int main(void) {
    assert(vbsp_bytes(0)==0&&vbsp_bytes(SIZE_MAX)==0);
    assert(vbsp_bytes(4096)>=2*4096+VBSP_STACK_BYTES);
    size_t descriptors=fd_count();
    fixture a,b;vbsp_publisher *p=open_fixture(&a,1,1),*other=open_fixture(&b,2,0);
    assert(submit(p,1,1,vbsp_now_ns()+1000000000)==VBSP_OK);entered(&a);
    assert(submit(p,1,2,vbsp_now_ns()+1000000000)==VBSP_OK);
    assert(submit(p,1,3,vbsp_now_ns()+1000000000)==VBSP_REPLACED);
    assert(submit(other,2,1,vbsp_now_ns()+1000000000)==VBSP_OK);
    vbsp_stats independent=completed(other,1,0);assert(independent.published==1);
    release(&a);vbsp_stats s=completed(p,2,0);
    assert(s.submitted==3&&s.replaced==1&&!s.busy&&!s.failure&&s.capacity==2);
    assert(a.seen[0]==1&&a.seen[1]==3&&b.seen[0]==1);
    close_fixture(p,&a);close_fixture(other,&b);
    p=open_fixture(&a,1,1);
    assert(submit(p,1,1,vbsp_now_ns()+1000000000)==VBSP_OK);entered(&a);
    assert(submit(p,1,2,vbsp_now_ns()+10000000)==VBSP_OK);
    nap(20);release(&a);s=completed(p,1,1);
    assert(s.published==1&&s.expired==1&&a.count==1);
    assert(submit(p,1,3,vbsp_now_ns()-1)==VBSP_EXPIRED);
    close_fixture(p,&a);
    p=open_fixture(&a,1,1);
    assert(submit(p,1,1,vbsp_now_ns()+1000000000)==VBSP_OK);entered(&a);
    assert(submit(p,1,2,vbsp_now_ns()+1000000000)==VBSP_OK);
    assert(vbsp_close(p,10)==VBSP_BUSY);
    assert(submit(p,1,3,vbsp_now_ns()+1000000000)==VBSP_CLOSED);
    release(&a);close_fixture(p,&a);
    p=open_fixture(&a,1,0);a.fail=-77;
    assert(submit(p,1,1,vbsp_now_ns()+1000000000)==VBSP_OK);
    uint64_t until=vbsp_now_ns()+1000000000;
    while(!vbsp_failure(p)){assert(vbsp_now_ns()<until);nap(1);}
    assert(vbsp_failure(p)==-77&&vbsp_stopping(p));close_fixture(p,&a);
    for(int i=0;i<100;i++){p=open_fixture(&a,1,0);close_fixture(p,&a);}
    saturated_sources();
    assert(fd_count()==descriptors);
    puts("SOURCE_PUBLISHER_PASS immutable_input bounded_replacement ordered_sources expiry fail_stop retained_close stress_20000");
    return 0;
}
