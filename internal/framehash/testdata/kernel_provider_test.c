#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <linux/if_alg.h>
#include <openssl/sha.h>
#include <pthread.h>
#include <stdint.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

/* Fault injection covers ownership and protocol, not hardware qualification. */
static _Thread_local int fault, algorithm_live, operation_live, sockets, receives;
static _Thread_local const uint8_t *borrowed;
static _Thread_local size_t borrowed_size;
static _Thread_local int64_t fake_ns = INT64_C(100000000000), step_ns;
static _Thread_local int clock_failed;
static _Thread_local int64_t observed_waits[6];
static _Thread_local int wait_count;
static atomic_int software_calls;
static int software_fault;
static int64_t software_step_ns;
static uint8_t *hash_software(const uint8_t *input, size_t size, uint8_t *digest) {
    atomic_fetch_add(&software_calls, 1);
    fake_ns += software_step_ns;
    if (software_fault == 1) return NULL;
    uint8_t *result = SHA256(input, size, digest);
    if (software_fault == 2 && result) digest[0] ^= 1;
    return result;
}
static int hash_clock(clockid_t clock, struct timespec *now) {
    assert(clock == CLOCK_MONOTONIC);
    if (clock_failed) return -1;
    now->tv_sec = (time_t)(fake_ns/INT64_C(1000000000));
    now->tv_nsec = (long)(fake_ns%INT64_C(1000000000));
    return 0;
}
static int hash_socket(int domain, int type, int protocol) {
    assert(domain == AF_ALG && type == (SOCK_SEQPACKET | SOCK_CLOEXEC) && protocol == 0);
    sockets++;
    if (fault == 1) { errno = EAFNOSUPPORT; return -1; }
    assert(!algorithm_live); algorithm_live = 1; return 17;
}
static int hash_bind(int fd, const struct sockaddr *raw, socklen_t size) {
    const struct sockaddr_alg *address = (const struct sockaddr_alg *)raw;
    assert(fd == 17 && size == sizeof(*address) && !strcmp((const char *)address->salg_name, "sha256-te"));
    if (fault == 2) { errno = ENOENT; return -1; } return 0;
}
static int hash_accept(int fd, struct sockaddr *address, socklen_t *size, int flags) {
    assert(fd == 17 && !address && !size && flags == SOCK_CLOEXEC);
    if (fault == 3) return -1;
    assert(!operation_live); operation_live = 1; borrowed = NULL; borrowed_size = 0; return 18;
}
static int hash_setopt(int fd, int level, int option, const void *value, socklen_t size) {
    assert(fd == 18 && level == SOL_SOCKET);
    if (option == SO_SNDBUF) { assert(size == sizeof(int) && *(const int *)value == 65536); }
    else {
        assert((option == SO_SNDTIMEO || option == SO_RCVTIMEO) && size == sizeof(struct timeval));
        if (step_ns) {
            const struct timeval *timeout = value;
            assert(wait_count < 6);
            observed_waits[wait_count++] = timeout->tv_sec*INT64_C(1000000)+timeout->tv_usec;
        }
    }
    return fault == 4 ? -1 : 0;
}
static int hash_getopt(int fd, int level, int option, void *value, socklen_t *size) {
    assert(fd == 18 && level == SOL_SOCKET && option == SO_SNDBUF && *size == sizeof(int));
    *(int *)value = fault == 5 ? 4096 : 131072; return 0;
}
static ssize_t hash_send(int fd, const void *data, size_t size, int flags) {
    assert(fd == 18 && (flags & MSG_NOSIGNAL));
    fake_ns += step_ns;
    if (size) {
        assert(flags & MSG_MORE);
        if (fault == 6) return (ssize_t)size - 1;
        if (fault == 7) { errno = EINTR; return -1; }
        borrowed = data; borrowed_size = size;
    } else {
        assert(!(flags & MSG_MORE));
        if (fault == 8) return -1;
    }
    return (ssize_t)size;
}
static ssize_t hash_recv(int fd, void *result, size_t size, int flags) {
    assert(fd == 18 && size == 32 && flags == 0); receives++;
    fake_ns += step_ns;
    if (fault == 9) return 31;
    if (fault == 10) { errno = EAGAIN; return -1; }
    assert(SHA256(borrowed, borrowed_size, result));
    if (fault == 11) ((uint8_t *)result)[0] ^= 1;
    return 32;
}
static int hash_close(int fd) {
    if (fd == 17) { assert(algorithm_live); algorithm_live = 0; }
    else { assert(fd == 18 && operation_live); operation_live = 0; }
    if (fault == 12) { errno = EIO; return -1; }
    return 0;
}
#define socket hash_socket
#define bind hash_bind
#define accept4 hash_accept
#define setsockopt hash_setopt
#define getsockopt hash_getopt
#define send hash_send
#define recv hash_recv
#define close hash_close
#define clock_gettime hash_clock
#define SHA256 hash_software
#include "../frame_sha256_linux.h"
#undef SHA256

static void *concurrent(void *unused) {
    (void)unused;
    assert(!strcmp(vb_frame_sha256_backend(), "af_alg:sha256-te"));
    uint8_t input[65537], expected[32], actual[32];
    memset(input, 0x5a, sizeof(input));
    for (size_t n = 65536; n <= sizeof(input); n++) {
        assert(SHA256(input, n, expected));
        assert(!vb_frame_sha256_sum(input, n, actual));
        assert(!memcmp(expected, actual, sizeof(actual)) && !algorithm_live && !operation_live);
    }
    return NULL;
}

int main(int argc, char **argv) {
    if (argc == 2 && (!strcmp(argv[1], "missing-software") ||
                      !strcmp(argv[1], "invalid-software"))) {
        software_fault = !strcmp(argv[1], "missing-software") ? 1 : 2;
        assert(!strcmp(vb_frame_sha256_backend(), "unavailable"));
        assert(atomic_load(&software_calls) == 1 && sockets == 0);
        uint8_t input[65536] = {0}, actual[32], zero[32] = {0};
        for (size_t n = 64; n <= sizeof(input); n *= 1024) {
            memset(actual, 0xaa, 32);
            assert(vb_frame_sha256_sum(input, n, actual) < 0);
            assert(!memcmp(actual, zero, 32));
            memset(actual, 0xaa, 32);
            assert(vb_frame_sha256_sum_until(input, n, actual, fake_ns+100) < 0);
            assert(!memcmp(actual, zero, 32));
        }
        assert(atomic_load(&software_calls) == 1 && sockets == 0);
        return 0;
    }
    if (argc == 2 && !strcmp(argv[1], "slow-startup")) {
        uint8_t actual[32], zero[32] = {0};
        fault = 1;
        software_step_ns = INT64_C(100000000);
        int64_t until = fake_ns+software_step_ns;
        assert(vb_frame_sha256_sum_until((const uint8_t *)"abc", 3, actual, until) < 0);
        assert(errno == ETIMEDOUT && !memcmp(actual, zero, 32));
        assert(atomic_load(&software_calls) == 1); /* no hash after startup consumed budget */
        return 0;
    }
    if (argc == 2 && !strcmp(argv[1], "cold")) {
        pthread_t threads[16];
        for (int i = 0; i < 16; i++) assert(!pthread_create(&threads[i], NULL, concurrent, NULL));
        for (int i = 0; i < 16; i++) assert(!pthread_join(threads[i], NULL));
        assert(!strcmp(vb_frame_sha256_backend(), "af_alg:sha256-te"));
        assert(atomic_load(&software_calls) == 1); /* once, before first frame in every thread */
        return 0;
    }
    if (argc == 2) {
        fault = !strcmp(argv[1], "absent") ? 1 : 11;
        assert(!strcmp(vb_frame_sha256_backend(), "openssl"));
        assert(vb_frame_sha256_workspace_bytes(65536) == 0);
        assert(!algorithm_live && !operation_live);
        assert(atomic_load(&software_calls) == 1);
        return 0;
    }
    assert(!strcmp(vb_frame_sha256_backend(), "af_alg:sha256-te"));
    assert(atomic_load(&software_calls) == 1);
    assert(vb_frame_sha256_workspace_bytes(65535) == 0);
    assert(vb_frame_sha256_workspace_bytes(65536) == VB_FRAME_SHA256_KERNEL_WORKSPACE);
    uint8_t input[65536], expected[32], actual[32], zero[32] = {0};
    memset(input, 0x39, sizeof(input));
    assert(SHA256(input, sizeof(input), expected));
    assert(!vb_frame_sha256_sum(input, sizeof(input), actual));
    assert(!memcmp(expected, actual, sizeof(actual)));
    int before = sockets;
    assert(!vb_frame_sha256_sum(input, 64, actual));
    assert(sockets == before); /* small input stays in OpenSSL */
    int software_before = atomic_load(&software_calls);
    for (fault = 1; fault <= 12; fault++) {
        if (fault == 11) continue; /* initial known-answer rejection tested separately */
        memset(actual, 0xaa, sizeof(actual));
        receives = 0;
        assert(vb_frame_sha256_sum(input, sizeof(input), actual) < 0);
        assert(!memcmp(actual, zero, sizeof(actual))); /* no silent software retry */
        assert(atomic_load(&software_calls) == software_before);
        assert(!algorithm_live && !operation_live);
        if (fault <= 8) assert(receives == 0);
    }
    fault = 0;
    /* A single frame deadline covers send, finalization, receive AND cleanup.
     * No socket is opened when already expired or the clock is unavailable. */
    before = sockets;
    memset(actual, 0xaa, sizeof(actual));
    assert(vb_frame_sha256_sum_until(input, sizeof(input), actual, fake_ns) < 0);
    assert(errno == ETIMEDOUT && sockets == before && !memcmp(actual, zero, 32));
    clock_failed = 1;
    assert(vb_frame_sha256_sum_until(input, sizeof(input), actual, fake_ns+100) < 0);
    assert(sockets == before && !memcmp(actual, zero, 32));
    clock_failed = 0;
    for (int phase = 1; phase <= 3; phase++) {
        step_ns = INT64_C(100000000);
        int64_t until = fake_ns+phase*step_ns;
        wait_count = 0; receives = 0;
        memset(actual, 0xaa, 32);
        assert(vb_frame_sha256_sum_until(input, sizeof(input), actual, until) < 0);
        assert(errno == ETIMEDOUT && !memcmp(actual, zero, 32));
        assert(!algorithm_live && !operation_live);
        assert(receives == (phase == 3 ? 1 : 0));
    }
    step_ns = INT64_C(100000000); wait_count = 0;
    assert(!vb_frame_sha256_sum_until(input, sizeof(input), actual, fake_ns+INT64_C(750000000)));
    assert(!memcmp(actual, expected, 32) && !algorithm_live && !operation_live);
    assert(wait_count == 6);
    for (int i = 0; i < 6; i++)
        assert(observed_waits[i] == 750000-(i/2)*100000);
    step_ns = 0;
    assert(vb_frame_sha256_sum_until(input, sizeof(input), actual, 0) < 0);
    assert(!memcmp(actual, zero, 32));
    pthread_t workers[16];
    for (int i = 0; i < 16; i++) assert(!pthread_create(&workers[i], NULL, concurrent, NULL));
    for (int i = 0; i < 16; i++) assert(!pthread_join(workers[i], NULL));
    return 0;
}
