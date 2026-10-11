#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "source_mailbox.h"
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#define VBSM_INITIAL_SEALS (F_SEAL_SHRINK|F_SEAL_GROW)
#define VBSM_FINAL_SEALS (VBSM_INITIAL_SEALS|F_SEAL_FUTURE_WRITE|F_SEAL_SEAL)
struct vbsm_source_mailbox {
    int fd,writer,confirmed,release_error;
    void *memory;
    size_t bytes;
    vbsc_mapping mapping;
    vbsc_atomic_region slots[VBSM_SLOTS];
};
size_t vbsm_source_mailbox_bytes(uint64_t capacity) {
    if(capacity<VBCF_HEADER_SIZE||capacity>VBCF_HEADER_SIZE+VBCF_MAX_PAYLOAD)return 0;
    return VBSM_SLOTS*(VBSC_SIZE+(size_t)((capacity+7)/8)*8);
}
static int valid_mapping(const vbsc_mapping *m) {
    if(!m||!m->generation||!vbsm_source_mailbox_bytes(m->capacity))return 0;
    uint8_t ns=0;for(size_t i=0;i<16;i++)ns|=m->namespace_id[i];return ns!=0;
}
static int bind_slots(vbsm_source_mailbox *o) {
    size_t stride=o->bytes/VBSM_SLOTS;
    for(unsigned i=0;i<VBSM_SLOTS;i++)
        if(vbsc_atomic_bind((uint8_t *)o->memory+i*stride,stride,&o->slots[i]))return VBSC_INVALID;
    return 0;
}
int vbsm_source_mailbox_close(vbsm_source_mailbox *o) {
    if(!o)return 0;
    if(o->release_error)return o->release_error;
    if(o->memory) {
        if(munmap(o->memory,o->bytes)){o->release_error=1;return 1;}
        o->memory=NULL;
    }
    int fd=o->fd;o->fd=-1;
    if(fd>=0&&close(fd)&&errno!=EINTR){o->release_error=2;return 2;}
    free(o);return 0;
}
static vbsm_source_mailbox *failed(vbsm_source_mailbox *o,int *error) {
    *error=VBSC_INVALID;
    return vbsm_source_mailbox_close(o)?o:NULL;
}
vbsm_source_mailbox *vbsm_source_mailbox_create(const vbsc_mapping *m,int *error) {
    if(!error)return NULL;
    *error=VBSC_INVALID;
    if(!valid_mapping(m))return NULL;
    vbsm_source_mailbox *o=calloc(1,sizeof(*o));if(!o)return NULL;
    o->fd=-1;o->mapping=*m;o->bytes=vbsm_source_mailbox_bytes(m->capacity);
    o->fd=memfd_create("vb-native-source",MFD_CLOEXEC|MFD_ALLOW_SEALING);
    if(o->fd<0||ftruncate(o->fd,(off_t)o->bytes))return failed(o,error);
    void *p=mmap(NULL,o->bytes,PROT_READ|PROT_WRITE,MAP_SHARED,o->fd,0);
    if(p==MAP_FAILED)return failed(o,error);
    o->memory=p;
    if(bind_slots(o))return failed(o,error);
    for(unsigned i=0;i<VBSM_SLOTS;i++)
        if(vbsc_atomic_initialize(&o->slots[i]))return failed(o,error);
    /* Relinquish the initial writable mapping. The final read mapping is made
     * only AFTER FUTURE_WRITE is sealed, so VM_MAYWRITE cannot be retained. */
    if(munmap(o->memory,o->bytes)){o->release_error=1;return o;}
    o->memory=NULL;
    memset(o->slots,0,sizeof(o->slots));
    if(fcntl(o->fd,F_ADD_SEALS,VBSM_INITIAL_SEALS))return failed(o,error);
    *error=0;return o;
}
vbsm_source_mailbox *vbsm_source_mailbox_attach(int fd,const vbsc_mapping *m,int *error) {
    if(!error){if(fd>=3)close(fd);return NULL;}
    *error=VBSC_INVALID;
    if(fd<3||!valid_mapping(m)){if(fd>=3)close(fd);return NULL;}
    vbsm_source_mailbox *o=calloc(1,sizeof(*o));
    if(!o){close(fd);return NULL;}
    o->fd=fd;o->mapping=*m;o->bytes=vbsm_source_mailbox_bytes(m->capacity);
    struct stat info;
    int flags=fcntl(fd,F_GETFD);
    if(flags<0||fcntl(fd,F_SETFD,flags|FD_CLOEXEC)||fstat(fd,&info)||
       !S_ISREG(info.st_mode)||info.st_nlink!=0||info.st_size!=(off_t)o->bytes||
       fcntl(fd,F_GET_SEALS)!=VBSM_INITIAL_SEALS)return failed(o,error);
    void *p=mmap(NULL,o->bytes,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);
    if(p==MAP_FAILED)return failed(o,error);
    o->memory=p;
    if(bind_slots(o)||fcntl(fd,F_ADD_SEALS,F_SEAL_FUTURE_WRITE|F_SEAL_SEAL))return failed(o,error);
    o->writer=1;o->confirmed=1;*error=0;return o;
}
int vbsm_source_mailbox_export(vbsm_source_mailbox *o) {
    if(!o||o->writer||o->confirmed||o->release_error)return -1;
    return fcntl(o->fd,F_DUPFD_CLOEXEC,3);
}
int vbsm_source_mailbox_confirm(vbsm_source_mailbox *o) {
    if(!o||o->writer||o->release_error||
       fcntl(o->fd,F_GET_SEALS)!=VBSM_FINAL_SEALS)return VBSC_INVALID;
    if(o->confirmed)return 0;
    if(o->memory)return VBSC_INVALID;
    void *p=mmap(NULL,o->bytes,PROT_READ,MAP_SHARED,o->fd,0);
    if(p==MAP_FAILED)return VBSC_INVALID;
    o->memory=p;
    if(bind_slots(o))return VBSC_INVALID;
    o->confirmed=1;return 0;
}
int vbsm_source_mailbox_publish(vbsm_source_mailbox *o,unsigned slot,const vbcf_header *header,const uint8_t *p,size_t n,vbcf_sha256_fn hash,void *ctx) {
    return vbsm_source_mailbox_publish_guarded(o,slot,header,p,n,hash,ctx,NULL);
}
int vbsm_source_mailbox_publish_guarded(vbsm_source_mailbox *o,unsigned slot,const vbcf_header *header,const uint8_t *p,size_t n,vbcf_sha256_fn hash,void *ctx,vbsc_commit_guard_fn guard) {
    if(!o||!o->writer||!o->confirmed||o->release_error||slot>=VBSM_SLOTS)return VBSC_INVALID;
    return vbsc_atomic_produce_guarded(&o->slots[slot],&o->mapping,header,p,n,hash,ctx,guard);
}
int vbsm_source_mailbox_stage(vbsm_source_mailbox *o,unsigned slot,uint64_t after,uint8_t *p,size_t cap,size_t *used,uint64_t *serial) {
    if(used)*used=0;
    if(serial)*serial=0;
    if(!o||o->writer||!o->confirmed||o->release_error||slot>=VBSM_SLOTS)return VBSC_INVALID;
    vbcf_header header;
    return vbsc_atomic_stage(&o->slots[slot],&o->mapping,p,cap,used,&header,after,serial);
}
