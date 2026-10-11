#define _GNU_SOURCE
#include "atomic_snapshot.h"
#include <assert.h>
#include <fcntl.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

static vbsc_mapping expected={{1},1,1024};
static int hash(void *ctx,const uint8_t *p,size_t n,uint8_t out[32]){(void)ctx;return SHA256(p,n,out)?0:-1;}
static void packet(uint8_t bytes[472],uint64_t id) {
    vbcf_header h={0};h.kind=2;h.codec=3;h.surface=3;h.correlation=1;h.width=4;h.height=2;h.stride_y=8;h.stride_uv=8;
    h.payload_bytes=24;h.source_epoch=1;h.origin_sequence=id;h.publication_epoch=1;h.delivery_id=id;h.namespace_id[0]=1;h.graph[0]=1;h.transform[0]=1;
    strcpy(h.source,"front");strcpy(h.stream,"analysis");strcpy(h.process,"broker-1");
    for(size_t i=0;i<24;i++)bytes[VBCF_HEADER_SIZE+i]=(uint8_t)((id+i)%251);
    assert(!hash(NULL,bytes+VBCF_HEADER_SIZE,24,h.payload_sha));assert(!vbcf_marshal_header(&h,bytes));
}
static uint64_t consume(vbsc_atomic_region *r) {
    uint8_t copy[1024];size_t used=123;vbcf_header h;
    int result=vbsc_atomic_snapshot(r,&expected,copy,sizeof(copy),&used,&h,hash,NULL,0,NULL);
    if(result){assert(!used);assert(result==VBSC_BUSY);return 0;}
    assert(used==472&&h.origin_sequence==h.delivery_id);
    for(size_t i=0;i<24;i++)assert(copy[VBCF_HEADER_SIZE+i]==(uint8_t)((h.origin_sequence+i)%251));
    return h.origin_sequence;
}
static void boundary_tests(vbsc_atomic_region *r) {
    uint8_t p[472],copy[1024];size_t used;vbcf_header h;packet(p,1);
    assert(vbsc_atomic_snapshot(r,&expected,copy,sizeof(copy),&used,&h,hash,NULL,0,NULL)==VBSC_BUSY);
    assert(!vbsc_atomic_publish(r,&expected,p,sizeof(p),hash,NULL));assert(consume(r)==1);
    vbsc_mapping wrong=expected;wrong.generation++;
    assert(vbsc_atomic_snapshot(r,&wrong,copy,sizeof(copy),&used,&h,hash,NULL,0,NULL)==VBSC_MAPPING);
    assert(vbsc_atomic_publish(r,&wrong,p,sizeof(p),hash,NULL)==VBSC_MAPPING);
    assert(consume(r)==1);
    assert(vbsc_atomic_snapshot(r,&expected,copy,471,&used,&h,hash,NULL,0,NULL)==VBSC_INVALID);
    assert(vbsc_atomic_publish(r,&expected,p,sizeof(p),NULL,NULL)==VBSC_INVALID);
    assert(consume(r)==1);
    /* Counter exhaustion cannot wrap to a reused publication identity. */
    const uint8_t exhausted[8]={255,255,255,255,255,255,255,254};uint64_t encoded;
    memcpy(&encoded,exhausted,8);atomic_store(r->words+1,encoded);
    assert(vbsc_atomic_publish(r,&expected,p,sizeof(p),hash,NULL)==VBSC_MAPPING);
    /* A stuck writer returns immediately; readers never spin internally. */
    const uint8_t odd[8]={0,0,0,0,0,0,0,3};memcpy(&encoded,odd,8);atomic_store(r->words+1,encoded);
    assert(vbsc_atomic_publish(r,&expected,p,sizeof(p),hash,NULL)==VBSC_BUSY);
    assert(vbsc_atomic_snapshot(r,&expected,copy,sizeof(copy),&used,&h,hash,NULL,0,NULL)==VBSC_BUSY);
    vbsc_atomic_region invalid;
    assert(vbsc_atomic_bind((uint8_t *)r->words+1,r->bytes,&invalid)==VBSC_INVALID);
    assert(vbsc_atomic_bind(r->words,r->bytes-1,&invalid)==VBSC_INVALID);
}
struct thread_state {vbsc_atomic_region *r;_Atomic int ready;uint64_t samples;};
struct guard_state {int calls,reject_at;};
static int commit_guard(void *arg) {
    struct guard_state *g=arg;return ++g->calls==g->reject_at;
}
static void guarded(vbsc_atomic_region *r) {
    uint8_t p[472];packet(p,1);vbcf_header h;assert(!vbcf_parse_header(p,VBCF_HEADER_SIZE,&h));
    for(int boundary=1;boundary<=2;boundary++) {
        assert(!vbsc_atomic_initialize(r));
        assert(!vbsc_atomic_publish(r,&expected,p,sizeof(p),hash,NULL));
        h.origin_sequence=h.delivery_id=2;struct guard_state g={0,boundary};
        assert(vbsc_atomic_produce_guarded(r,&expected,&h,p+VBCF_HEADER_SIZE,24,hash,&g,commit_guard)==VBSC_INVALID);
        assert(g.calls==boundary);
        if(boundary==1)assert(consume(r)==1);
        else assert(consume(r)==0); /*late revocation never commits even serial*/
    }
}
static void *reader(void *argument) {
    struct thread_state *state=argument;
    assert(consume(state->r)==1);atomic_store(&state->ready,1);
    uint64_t last=1;
    while(last<1000){uint64_t n=consume(state->r);if(n){assert(n>=last);last=n;state->samples++;}else sched_yield();}
    return NULL;
}
static void threaded(vbsc_atomic_region *r) {
    assert(!vbsc_atomic_initialize(r));uint8_t p[472];packet(p,1);assert(!vbsc_atomic_publish(r,&expected,p,sizeof(p),hash,NULL));
    struct thread_state state={r,0,0};pthread_t task;assert(!pthread_create(&task,NULL,reader,&state));
    while(!atomic_load(&state.ready))sched_yield();
    for(uint64_t id=2;id<=1000;id++){packet(p,id);assert(!vbsc_atomic_publish(r,&expected,p,sizeof(p),hash,NULL));sched_yield();}
    assert(!pthread_join(task,NULL));assert(state.samples>0);
    printf("ATOMIC_THREAD_PASS publications=1000 valid_snapshots=%llu\n",(unsigned long long)state.samples);
}
int main(void) {
    const size_t size=1088;
    int fd=memfd_create("vbsc-provider-test",MFD_CLOEXEC|MFD_ALLOW_SEALING);assert(fd>=0);
    assert(!ftruncate(fd,(off_t)size));assert(!fcntl(fd,F_ADD_SEALS,F_SEAL_SHRINK|F_SEAL_GROW|F_SEAL_SEAL));
    void *memory=mmap(NULL,size,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);assert(memory!=MAP_FAILED);
    vbsc_atomic_region r;assert(!vbsc_atomic_bind(memory,size,&r));assert(!vbsc_atomic_initialize(&r));boundary_tests(&r);
    guarded(&r);threaded(&r);assert(!vbsc_atomic_initialize(&r));uint8_t p[472];packet(p,1);assert(!vbsc_atomic_publish(&r,&expected,p,sizeof(p),hash,NULL));
    int ready[2];assert(!pipe2(ready,O_CLOEXEC));assert(!fflush(stdout));pid_t child=fork();assert(child>=0);
    if(!child){
        close(ready[0]);assert(!munmap(memory,size));
        void *read_only=mmap(NULL,size,PROT_READ,MAP_SHARED,fd,0);assert(read_only!=MAP_FAILED);
        vbsc_atomic_region consumer;assert(!vbsc_atomic_bind(read_only,size,&consumer));assert(consume(&consumer)==1);
        assert(write(ready[1],"R",1)==1);close(ready[1]);uint64_t last=1,samples=0;
        while(last<1000){uint64_t id=consume(&consumer);if(id){assert(id>=last);last=id;samples++;}else sched_yield();}
        assert(samples>0);assert(!munmap(read_only,size));close(fd);_exit(0);
    }
    close(ready[1]);char ready_byte;assert(read(ready[0],&ready_byte,1)==1&&ready_byte=='R');close(ready[0]);
    for(uint64_t id=2;id<=1000;id++){packet(p,id);assert(!vbsc_atomic_publish(&r,&expected,p,sizeof(p),hash,NULL));sched_yield();}
    int status;assert(waitpid(child,&status,0)==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0);
    assert(ftruncate(fd,0)==-1);assert(!munmap(memory,size));close(fd);
    puts("ATOMIC_PROCESS_PASS publications=1000 readonly_consumer=1 shrink_sealed=1");return 0;
}
