#ifndef VIDEO_BROKER_DIAGNOSTIC_PIPE_H
#define VIDEO_BROKER_DIAGNOSTIC_PIPE_H

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

/* Optional telemetry only. Never use this lossy path for control envelopes,
 * SDK ownership/release receipts or protocol errors. An independent open file
 * description avoids making vendor stderr nonblocking. Only pipes admit an
 * atomic, bounded write; regular files are deliberately not a media-loop sink.
 * Single producer owns init/write/close; no queue, thread, heap or retry loop. */
typedef struct {
    int fd;
    size_t atomic_bytes;
    uint64_t dropped;
} vb_diagnostic_pipe;

static inline int vb_diagnostic_pipe_open(vb_diagnostic_pipe *p, int target) {
    struct stat before, after;
    char path[64];
    *p = (vb_diagnostic_pipe){.fd = -1};
    if (fstat(target, &before) || !S_ISFIFO(before.st_mode)) return -1;
    int n = snprintf(path, sizeof(path), "/proc/self/fd/%d", target);
    if (n < 0 || (size_t)n >= sizeof(path)) return -1;
    int fd = open(path, O_WRONLY | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0) return -1;
    long limit = fpathconf(fd, _PC_PIPE_BUF);
    if (fstat(fd, &after) || !S_ISFIFO(after.st_mode) ||
            before.st_dev != after.st_dev || before.st_ino != after.st_ino || limit < 512) {
        close(fd);
        return -1;
    }
    p->fd = fd;
    p->atomic_bytes = limit > 4096 ? 4096 : (size_t)limit;
    return 0;
}

static inline void vb_diagnostic_pipe_close(vb_diagnostic_pipe *p) {
    if (p->fd >= 0) close(p->fd);
    p->fd = -1;
}

static inline int vb_diagnostic_pipe_vprintf(vb_diagnostic_pipe *p, const char *fmt, va_list args) {
    char line[4096];
    if (p->fd < 0) { p->dropped++; return -1; }
    int n = vsnprintf(line, sizeof(line), fmt, args);
    if (n < 1 || (size_t)n >= sizeof(line)) { p->dropped++; return -1; }
    if (line[n-1] == '\n') n--;
    int suffix = snprintf(line+n, sizeof(line)-(size_t)n,
                          " telemetry_dropped=%llu\n", (unsigned long long)p->dropped);
    if (suffix < 0 || (size_t)suffix >= sizeof(line)-(size_t)n ||
            (size_t)n+(size_t)suffix > p->atomic_bytes) { p->dropped++; return -1; }
    /* Suppress only our own newly generated SIGPIPE; retain a signal already
     * pending for the caller and restore its mask. No global handler change. */
    sigset_t set, old, pending;
    sigemptyset(&set);
    sigaddset(&set, SIGPIPE);
    if (pthread_sigmask(SIG_BLOCK, &set, &old)) { p->dropped++; return -1; }
    if (sigpending(&pending)) {
        (void)pthread_sigmask(SIG_SETMASK, &old, NULL);
        p->dropped++;
        return -1;
    }
    ssize_t written = write(p->fd, line, (size_t)n+(size_t)suffix);
    int error = errno;
    if (written < 0 && error == EPIPE && !sigismember(&pending, SIGPIPE)) {
        const struct timespec zero = {0};
        (void)sigtimedwait(&set, NULL, &zero);
    }
    (void)pthread_sigmask(SIG_SETMASK, &old, NULL);
    if (written != (ssize_t)((size_t)n+(size_t)suffix)) { p->dropped++; return -1; }
    return 0;
}

static inline int vb_diagnostic_pipe_printf(vb_diagnostic_pipe *p, const char *fmt, ...) {
    int saved_errno = errno;
    va_list args;
    va_start(args, fmt);
    int result = vb_diagnostic_pipe_vprintf(p, fmt, args);
    va_end(args);
    errno = saved_errno;
    return result;
}
#endif
