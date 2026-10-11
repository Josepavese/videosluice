//go:build ignore

/* Isolated fault probe: NEVER linked into a production provider. */
#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdint.h>
#include <stdatomic.h>
#include <sys/mman.h>
#include <unistd.h>
static int inject_unmap,inject_close,inject_size,inject_map,inject_atomic,unmap_calls,close_calls,last_closed_fd;
static int test_lock_free(void) {
    _Atomic uint64_t value=0;return atomic_is_lock_free(&value);
}
static int fail_seal(int fd,int command,int value) {
    (void)fd;(void)command;(void)value;errno=EPERM;return -1;
}
static int test_unmap(void *address,size_t bytes) {
    unmap_calls++;
    if(inject_unmap){errno=EBUSY;return -1;}
    return munmap(address,bytes);
}
static int test_close(int fd) {
    close_calls++;
    last_closed_fd=fd;
    int result=close(fd);
    if(inject_close){assert(!result);errno=EIO;return -1;}
    return result;
}
static int test_size(int fd,off_t size) {
    if(inject_size){errno=ENOSPC;return -1;}
    return ftruncate(fd,size);
}
static void *test_map(void *p,size_t n,int protection,int flags,int fd,off_t off) {
    if(inject_map){errno=ENOMEM;return MAP_FAILED;}
    return mmap(p,n,protection,flags,fd,off);
}
#define fcntl fail_seal
#define munmap test_unmap
#define close test_close
#define ftruncate test_size
#define mmap test_map
#undef atomic_is_lock_free
#define atomic_is_lock_free(p) ((void)(p),inject_atomic?0:test_lock_free())
#include "bridge.c"
#undef fcntl
#undef munmap
#undef close
#undef ftruncate
#undef mmap
#undef atomic_is_lock_free

int main(void) {
    uint8_t ns[16]={1};int stage=0,cleanup=0;
    for(int scenario=0;scenario<6;scenario++) {
        inject_unmap=scenario==0||scenario==5;inject_close=scenario>0&&scenario<4;
        inject_size=scenario==2;inject_map=scenario==3;
        inject_atomic=scenario==5;
        unmap_calls=close_calls=0;
        vbsc_owned *owned=vbsc_owned_create(ns,1,1024,&stage,&cleanup);
        int expected=inject_size?VBSC_CREATE_SIZE:inject_map?VBSC_CREATE_MAP:inject_atomic?VBSC_CREATE_ATOMIC:VBSC_CREATE_SEALS;
        assert(stage==expected);
        if(scenario==4){assert(!owned&&cleanup==0);continue;}
        assert(owned&&cleanup==(inject_unmap?1:2));
        int unmaps=unmap_calls,closes=close_calls;
        int sentinel=open("/dev/null",O_RDONLY|O_CLOEXEC);assert(sentinel>=0);
        if(inject_close)assert(sentinel==last_closed_fd);
        for(int i=0;i<10;i++)assert(vbsc_owned_destroy(owned)==cleanup);
        assert(unmap_calls==unmaps&&close_calls==closes);
        assert(fcntl(sentinel,F_GETFD)>=0);assert(!close(sentinel));
        if(inject_unmap) {
            unsigned char residency[4]={0};
            assert(owned->memory&&owned->bytes>0&&owned->fd>=0);
            assert(!mincore(owned->memory,owned->bytes,residency));
            /* Test-only cleanup of the exact fixture-owned mapping and FD. */
            assert(!munmap(owned->memory,owned->bytes));assert(!close(owned->fd));
        } else assert(!owned->memory&&owned->fd==-1);
        free(owned);
    }
    puts("CREATE_CLEANUP_PASS cases=6 partial_owner_retained=1 stages_preserved=1 no_retry=1 reused_fd_safe=1 failed_atomic_bind_owned=1 fixture_cleanup=1");
    return 0;
}
