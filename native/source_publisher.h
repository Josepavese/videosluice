#ifndef VIDEO_BROKER_SOURCE_PUBLISHER_H
#define VIDEO_BROKER_SOURCE_PUBLISHER_H
#include "causal_envelope.h"

/* Generic, detached RAM publisher. No camera/codec handles or process authority
 * are acquired here. The owner admits memory and supplies a fenced callback.
 * Two buffers total: pending work may be replaced, running input never is.
 * A single serialized owner submits/closes; the worker is independent.
 * Submit copies once and never waits for hashing, IPC, or the worker mutex.
 * The callback receives immutable bytes until return and owns no caller lease. */
enum {VBSP_SLOTS=2, VBSP_OK=0, VBSP_REPLACED=1, VBSP_BUSY=2,
      VBSP_EXPIRED=3, VBSP_INVALID=-1, VBSP_CLOSED=-2, VBSP_FAILED=-3};
enum {VBSP_STACK_BYTES=262144, VBSP_THREAD_ALLOWANCE=524288};
typedef struct vbsp_publisher vbsp_publisher;
typedef int (*vbsp_publish_fn)(void *,const vbcf_header *,const uint8_t *,size_t,uint64_t);
typedef struct {
    uint64_t submitted,attempted,published,replaced,busy,expired,canceled;
    uint64_t work_ns,work_max_ns,copy_ns,copy_max_ns;
    unsigned pending,inflight,capacity;
    int failure;
} vbsp_stats;
size_t vbsp_bytes(size_t capacity);
/* NULL on fully released setup failure. A non-NULL error result retains failed
 * cleanup ownership. Hash/provider workspace is additional to vbsp_bytes. */
vbsp_publisher *vbsp_create(size_t capacity,vbsp_publish_fn,void *,int *error);
int vbsp_submit(vbsp_publisher *,const vbcf_header *,const uint8_t *,size_t,uint64_t deadline_ns);
int vbsp_snapshot(vbsp_publisher *,vbsp_stats *);
int vbsp_stopping(const vbsp_publisher *);
int vbsp_failure(const vbsp_publisher *);
/* Cancel pending jobs and join, bounded by CLOCK_MONOTONIC. BUSY preserves all
 * memory and callback ownership; caller must not close borrowed resources. */
int vbsp_close(vbsp_publisher *,unsigned timeout_ms);
uint64_t vbsp_now_ns(void);
#endif
