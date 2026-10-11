//go:build ignore

/* Standalone native fixture: not linked into the production provider. */
#include "bridge.c"
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <sys/resource.h>
#include <sys/wait.h>

int main(void) {
    int stage=0,cleanup=0;uint8_t ns[16]={1};
    vbsc_owned *bad=vbsc_owned_create(ns,1,1024,&stage,&cleanup);assert(bad);
    size_t actual_bytes=bad->bytes;bad->bytes=0;
    assert(vbsc_owned_destroy(bad)==1); /* failed unmap must retain ownership */
    assert(fcntl(bad->fd,F_GETFD)>=0);
    bad->bytes=actual_bytes;assert(vbsc_owned_destroy(bad)==1);
    assert(!munmap(bad->memory,actual_bytes));assert(!close(bad->fd));free(bad);
    bad=vbsc_owned_create(ns,1,1024,&stage,&cleanup);assert(bad);
    assert(!close(bad->fd));assert(vbsc_owned_destroy(bad)==2);
    assert(bad->fd==-1&&!bad->memory&&vbsc_owned_destroy(bad)==2);free(bad);
    assert(!vbsc_owned_create(ns,0,1024,&stage,&cleanup)&&stage==VBSC_CREATE_VALIDATION);
    assert(!vbsc_owned_create(ns,1,1024,NULL,&cleanup));
    assert(!vbsc_owned_create(ns,1,1024,&stage,NULL));
    for(int attempt=0;attempt<100;attempt++) {
        vbsc_owned *o=vbsc_owned_create(ns,1,1024,&stage,&cleanup);assert(o&&stage==0&&cleanup==0);
        int fd=o->fd;
        int seals=fcntl(fd,F_GET_SEALS);
        int required=F_SEAL_SHRINK|F_SEAL_GROW|F_SEAL_FUTURE_WRITE|F_SEAL_SEAL;
        assert((seals&required)==required);
        assert(fcntl(fd,F_GETFD)&FD_CLOEXEC);
        assert(ftruncate(fd,0)==-1&&errno==EPERM);
        assert(ftruncate(fd,8192)==-1&&errno==EPERM);
        uint8_t byte=0;assert(pwrite(fd,&byte,1,0)==-1&&errno==EPERM);
        assert(mmap(NULL,o->region.bytes,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0)==MAP_FAILED&&errno==EPERM);
        void *reader=mmap(NULL,o->region.bytes,PROT_READ,MAP_SHARED,fd,0);assert(reader!=MAP_FAILED);
        assert(mprotect(reader,o->region.bytes,PROT_READ|PROT_WRITE)==-1);
        assert(!munmap(reader,o->region.bytes));vbsc_owned_destroy(o);
        assert(fcntl(fd,F_GETFD)==-1&&errno==EBADF);
    }
    vbsc_owned_destroy(NULL);
    /* Kernel resource denial in an isolated child; parent/workstation limits
     * remain unchanged. The explicit stage must survive failed creation. */
    pid_t child=fork();assert(child>=0);
    if(!child) {
        struct rlimit limit={0,0};assert(!setrlimit(RLIMIT_NOFILE,&limit));
        assert(!vbsc_owned_create(ns,1,1024,&stage,&cleanup)&&stage==VBSC_CREATE_DESCRIPTOR&&cleanup==0);
        _exit(0);
    }
    int status;assert(waitpid(child,&status,0)==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0);
    puts("OWNED_NATIVE_PASS cycles=100 fd_closed=1 size_sealed=1 future_write_denied=1 readonly_mapping=1 descriptor_denial_diagnostic=1");
    return 0;
}
