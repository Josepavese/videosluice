#ifndef VIDEO_BROKER_WORKER_SIGNAL_H
#define VIDEO_BROKER_WORKER_SIGNAL_H

#include <errno.h>
#include <signal.h>
#include <stddef.h>

/* signal() can have one-shot System V semantics with strict POSIX/C11 feature
 * macros. A repeated stop must still enter checked teardown, never restore the
 * default termination action. Do not restart interrupted blocking I/O: each
 * caller retains its existing cancellation/deadline policy. */
static inline int video_broker_worker_install_stop_signals(void (*handler)(int)) {
    struct sigaction action = {0};
    if (handler == NULL) { errno = EINVAL; return -1; }
    action.sa_handler = handler;
    if (sigemptyset(&action.sa_mask) != 0) return -1;
    const int signals[] = {SIGINT, SIGTERM, SIGPIPE};
    for (unsigned i = 0; i < sizeof(signals) / sizeof(signals[0]); ++i) {
        if (sigaction(signals[i], &action, NULL) != 0) return -1;
    }
    return 0;
}

#endif
