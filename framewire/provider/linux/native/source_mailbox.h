#ifndef VBSM_SOURCE_MAILBOX_LINUX_H
#define VBSM_SOURCE_MAILBOX_LINUX_H
#include "../../../native/atomic_snapshot.h"

/* One private, process-bound source allocation, two independent atomic slots.
 * This provider does NOT authenticate a process or charge the resource ledger.
 * The trusted launcher must admit footprint + private buffers before create,
 * inherit the descriptor only to its pinned worker, and fence every snapshot.
 * No slot is pinned by a reader and no driver lease may span a call. */
enum {VBSM_SLOTS=2};
typedef struct vbsm_source_mailbox vbsm_source_mailbox;
size_t vbsm_source_mailbox_bytes(uint64_t capacity);
/* The parent mapping is read-only after initialization. The inherited writer
 * maps first, then seals all future writers. Parent must confirm those seals
 * after the authenticated one-shot worker-ready record, before any snapshot.
 * A non-NULL result on error retains failed cleanup ownership. */
vbsm_source_mailbox *vbsm_source_mailbox_create(const vbsc_mapping *,int *error);
/* Consumes fd, including failure. Never closes unrelated descriptors on retry. */
vbsm_source_mailbox *vbsm_source_mailbox_attach(int fd,const vbsc_mapping *,int *error);
int vbsm_source_mailbox_export(vbsm_source_mailbox *);
int vbsm_source_mailbox_confirm(vbsm_source_mailbox *);
int vbsm_source_mailbox_publish(vbsm_source_mailbox *,unsigned slot,const vbcf_header *,const uint8_t *,size_t,vbcf_sha256_fn,void *);
int vbsm_source_mailbox_publish_guarded(vbsm_source_mailbox *,unsigned slot,const vbcf_header *,const uint8_t *,size_t,vbcf_sha256_fn,void *,vbsc_commit_guard_fn);
/* Staging only: structure/serial validated, producer SHA retained unchanged.
 * A private Store must verify that SHA before releasing ANY pixel artifact. */
int vbsm_source_mailbox_stage(vbsm_source_mailbox *,unsigned slot,uint64_t after,uint8_t *,size_t,size_t *,uint64_t *);
int vbsm_source_mailbox_close(vbsm_source_mailbox *);
#endif
