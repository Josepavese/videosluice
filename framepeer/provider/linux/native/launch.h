#ifndef FRAMEPEER_LINUX_NATIVE_LAUNCH_H
#define FRAMEPEER_LINUX_NATIVE_LAUNCH_H
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "framepeer.h"

typedef struct {
    int pid;
    uint32_t uid,gid;
    char generation[65];
} framepeer_linux_identity;

/* One owner, initially all zero, never copied/reinitialized after accept.
 * All calls are serialized by that owner; no allocation, queue or thread.
 * The caller admits this state, transient fdinfo/ancillary storage, inherited
 * handles and socket/kernel footprint BEFORE accept. Failed Close remains a
 * recorded failure; it must not be interpreted as verified quota release. */
typedef struct {
    bool initialized,accepted,closed;
    int socket,controller_fd,self_fd,close_result;
    framepeer_linux_identity controller,self;
    const char *stage;
} framepeer_linux_launch;

/* Consume distinct nonnegative descriptors on every path after validating
 * state/descriptor arguments. LPFD metadata is trusted launcher input, not
 * authority by itself. Checks inherited pidfd and credentials on the bootstrap
 * record, then the exact self pidfd sent by the authorized launcher. */
int framepeer_linux_accept(framepeer_linux_launch *launch,int socket,int controller_fd,
    const uint8_t *metadata,size_t metadata_size,int timeout_ms);
int framepeer_linux_current(framepeer_linux_launch *launch);
/* Per-record credentials checked against the pinned controller. No capabilities
 * accepted after bootstrap. Any error reports zero usable bytes. The caller
 * exclusively owns/admit-charges the buffer. Idle deadlines do not retire the
 * channel automatically; retry policy is outside this provider. */
int framepeer_linux_receive(framepeer_linux_launch *launch,uint8_t *buffer,size_t capacity,
    size_t *received,int timeout_ms);
/* Same authenticated operation with an existing CLOCK_MONOTONIC deadline.
 * Multi-record callers must not grant a fresh relative timeout per record. */
int framepeer_linux_receive_until(framepeer_linux_launch *launch,uint8_t *buffer,size_t capacity,
    size_t *received,int64_t deadline_ns);
/* Borrow packet synchronously. No retry after a send error or ambiguous delivery.
 * No descriptor transfer; caller owns immutable bytes through return. */
int framepeer_linux_send(framepeer_linux_launch *launch,const uint8_t *packet,size_t size,
    int timeout_ms);
/* Provider-owned monotonic clock for multi-record operations. Create ONCE per
 * operation, then reuse the same deadline for its header and every chunk.
 * This does not admit buffers or establish a new process authority. */
int framepeer_linux_deadline_after(int timeout_ms,int64_t *deadline_ns);
int framepeer_linux_send_until(framepeer_linux_launch *launch,const uint8_t *packet,size_t size,
    int64_t deadline_ns);
/* Create a separately owned sender from an accepted owner. Duplicates the
 * authenticated socket and pidfds; the caller serializes this call with the
 * owner and closes the delegate after its send worker joins. */
int framepeer_linux_delegate_sender(framepeer_linux_launch *owner,framepeer_linux_launch *delegate);
int framepeer_linux_close(framepeer_linux_launch *launch);
#endif
