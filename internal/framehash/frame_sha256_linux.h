#ifndef VIDEO_BROKER_FRAME_SHA256_LINUX_H
#define VIDEO_BROKER_FRAME_SHA256_LINUX_H

/* SHA-256 implementation selection, not a different wire algorithm or media
 * fallback. Only the explicitly qualified kernel driver is considered. Each
 * synchronous request owns and closes its sockets: no pool, retained input,
 * worker thread, global sysctl or driver lease escapes this call. Callers must
 * reserve workspace_bytes for EVERY admitted concurrent hash operation. */
#include <errno.h>
#include <linux/if_alg.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define VB_FRAME_SHA256_KERNEL_MIN_BYTES ((size_t)65536)
#define VB_FRAME_SHA256_KERNEL_MAX_BYTES ((size_t)33554432)
/* Conservative allowance for the capped socket accounting, scatterlists and
 * request-private kernel workspace. Not a claim about the whole device/DMA. */
#define VB_FRAME_SHA256_KERNEL_WORKSPACE ((size_t)1048576)

static pthread_once_t vb_frame_sha256_once = PTHREAD_ONCE_INIT;
static int vb_frame_sha256_selected;
static int vb_frame_sha256_software_ready;

/* A positive deadline is CLOCK_MONOTONIC nanoseconds, shared with the owning
 * frame transaction. Zero is the ordinary hash API's bounded per-wait policy.
 * Recheck after GIL/driver waits in callers too: no late result is usable. */
static inline int vb_frame_sha256_timeout(int64_t until, struct timeval *timeout) {
    int64_t remaining = INT64_C(1000000000);
    if (until) {
        struct timespec now;
        if (until < 0 || clock_gettime(CLOCK_MONOTONIC, &now) || now.tv_sec < 0 ||
            now.tv_sec > (INT64_MAX-INT64_C(999999999))/INT64_C(1000000000))
            return -1;
        int64_t current = (int64_t)now.tv_sec*INT64_C(1000000000)+now.tv_nsec;
        if (current >= until) { errno = ETIMEDOUT; return -1; }
        if (until-current < remaining) remaining = until-current;
    }
    int64_t usec = (remaining+999)/1000;
    timeout->tv_sec = (time_t)(usec/INT64_C(1000000));
    timeout->tv_usec = (suseconds_t)(usec%INT64_C(1000000));
    return 0;
}

static inline int vb_frame_sha256_bound_wait(int operation, int64_t until) {
    struct timeval timeout;
    return vb_frame_sha256_timeout(until, &timeout) ||
        setsockopt(operation, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) ||
        setsockopt(operation, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
}

static inline int vb_frame_sha256_kernel_until(const uint8_t *pixels, size_t size,
                                               uint8_t digest[32], int64_t until) {
    int algorithm = -1, operation = -1, result = -1;
    uint8_t complete[32] = {0};
    struct sockaddr_alg address = {0};
    struct timeval timeout;
    int send_bytes = 65536, actual = 0;
    socklen_t actual_size = sizeof(actual);
    if (!digest || (!pixels && size) || size > VB_FRAME_SHA256_KERNEL_MAX_BYTES)
        return -1;
    memset(digest, 0, 32);
    if (vb_frame_sha256_timeout(until, &timeout)) return -1;
    address.salg_family = AF_ALG;
    memcpy(address.salg_type, "hash", 5);
    memcpy(address.salg_name, "sha256-te", 10);
    algorithm = socket(AF_ALG, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (algorithm < 0 || bind(algorithm, (struct sockaddr *)&address, sizeof(address)))
        goto done;
    operation = accept4(algorithm, NULL, NULL, SOCK_CLOEXEC);
    if (operation < 0 ||
        setsockopt(operation, SOL_SOCKET, SO_SNDBUF, &send_bytes, sizeof(send_bytes)) ||
        getsockopt(operation, SOL_SOCKET, SO_SNDBUF, &actual, &actual_size) ||
        actual_size != sizeof(actual) || actual != 2 * send_bytes ||
        vb_frame_sha256_bound_wait(operation, until))
        goto done;
    /* MSG_MORE prevents a short write from becoming a valid prefix digest.
     * No retries on a partial request; no result is exposed until exact finish.
     * Socket timeouts bound I/O waits, not an unresponsive kernel/driver itself. */
    if (size && send(operation, pixels, size, MSG_MORE | MSG_NOSIGNAL) != (ssize_t)size)
        goto done;
    if ((until && vb_frame_sha256_bound_wait(operation, until)) ||
        send(operation, "", 0, MSG_NOSIGNAL) != 0 ||
        (until && vb_frame_sha256_bound_wait(operation, until)) ||
        recv(operation, complete, sizeof(complete), 0) != (ssize_t)sizeof(complete))
        goto done;
    result = 0;
done:
    /* On Linux EINTR closes the descriptor; retrying could close a reused FD. */
    if (operation >= 0 && close(operation) && errno != EINTR) result = -1;
    if (algorithm >= 0 && close(algorithm) && errno != EINTR) result = -1;
    if (!result && vb_frame_sha256_timeout(until, &timeout)) result = -1;
    if (!result) memcpy(digest, complete, sizeof(complete));
    return result;
}

static inline int vb_frame_sha256_kernel(const uint8_t *pixels, size_t size,
                                         uint8_t digest[32]) {
    return vb_frame_sha256_kernel_until(pixels, size, digest, 0);
}

static inline void vb_frame_sha256_select_once(void) {
    static const uint8_t input[] = "abc";
    static const uint8_t expected[32] = {
        0xba,0x78,0x16,0xbf,0x8f,0x01,0xcf,0xea,0x41,0x41,0x40,0xde,0x5d,0xae,0x22,0x23,
        0xb0,0x03,0x61,0xa3,0x96,0x17,0x7a,0x9c,0xb4,0x10,0xff,0x61,0xf2,0x00,0x15,0xad};
    uint8_t digest[32];
    /* OpenSSL is also required with the kernel provider for small payloads.
     * Qualify it before READY: its first call may initialize providers and must
     * not consume the first frame's receive/publication deadline. */
    vb_frame_sha256_software_ready = SHA256(input, sizeof(input)-1, digest) &&
                                    !memcmp(digest, expected, sizeof(digest));
    if (!vb_frame_sha256_software_ready) return;
    vb_frame_sha256_selected = !vb_frame_sha256_kernel(input, sizeof(input)-1, digest) &&
                              !memcmp(digest, expected, sizeof(digest));
}

static inline const char *vb_frame_sha256_backend(void) {
    if (pthread_once(&vb_frame_sha256_once, vb_frame_sha256_select_once) ||
        !vb_frame_sha256_software_ready) return "unavailable";
    return vb_frame_sha256_selected ? "af_alg:sha256-te" : "openssl";
}

static inline size_t vb_frame_sha256_workspace_bytes(size_t payload_bytes) {
    (void)vb_frame_sha256_backend();
    return vb_frame_sha256_selected && payload_bytes >= VB_FRAME_SHA256_KERNEL_MIN_BYTES &&
           payload_bytes <= VB_FRAME_SHA256_KERNEL_MAX_BYTES ? VB_FRAME_SHA256_KERNEL_WORKSPACE : 0;
}

static inline int vb_frame_sha256_sum(const uint8_t *pixels, size_t size, uint8_t digest[32]) {
    if (digest) memset(digest, 0, 32);
    if (!digest || (!pixels && size)) return -1;
    if (pthread_once(&vb_frame_sha256_once, vb_frame_sha256_select_once) ||
        !vb_frame_sha256_software_ready) return -1;
    if (vb_frame_sha256_workspace_bytes(size))
        return vb_frame_sha256_kernel(pixels, size, digest);
    /* Only initial capability/size selection uses OpenSSL. Once selected for
     * this size, an AF_ALG failure is fatal: never silently retry in software. */
    if (SHA256(pixels, size, digest)) return 0;
    memset(digest, 0, 32);
    return -1;
}

/* Same qualified backend and full SHA-256, inside a frame's total deadline.
 * Selection is normally completed during channel construction, before IO. */
static inline int vb_frame_sha256_sum_until(const uint8_t *pixels, size_t size,
                                           uint8_t digest[32], int64_t until) {
    struct timeval timeout;
    if (digest) memset(digest, 0, 32);
    if (!digest || (!pixels && size) || until <= 0 ||
        vb_frame_sha256_timeout(until, &timeout) ||
        pthread_once(&vb_frame_sha256_once, vb_frame_sha256_select_once) ||
        !vb_frame_sha256_software_ready || vb_frame_sha256_timeout(until, &timeout))
        return -1;
    if (vb_frame_sha256_workspace_bytes(size))
        return vb_frame_sha256_kernel_until(pixels, size, digest, until);
    if (!SHA256(pixels, size, digest) || vb_frame_sha256_timeout(until, &timeout)) {
        memset(digest, 0, 32);
        return -1;
    }
    return 0;
}
#endif
