#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include "../worker_signal.h"

static volatile sig_atomic_t received;
static void stop(int signal_number) { (void)signal_number; ++received; }

/* A dynamic-loader/constructor stall precedes main's signal installation.
 * Model the cancellation window without SDK, disk pressure or timing sleeps.
 * This is a limitation proof, not a claim about which page stalled on a test board. */
__attribute__((constructor)) static void cancel_before_main(void) {
    const char *early = getenv("VIDEO_BROKER_SIGNAL_TEST_BEFORE_MAIN");
    if (early != NULL && strcmp(early, "1") == 0) {
        if (raise(SIGTERM) != 0) _exit(2);
        _exit(3); /* Ignored/blocked termination would invalidate the fixture. */
    }
}

static void verify_pre_main_termination(const char *self) {
    pid_t child = fork();
    assert(child >= 0);
    if (child == 0) {
        struct sigaction action = {0};
        sigset_t set;
        action.sa_handler = SIG_DFL;
        if (sigemptyset(&action.sa_mask) != 0 ||
            sigaction(SIGTERM, &action, NULL) != 0 ||
            sigemptyset(&set) != 0 || sigaddset(&set, SIGTERM) != 0 ||
            sigprocmask(SIG_UNBLOCK, &set, NULL) != 0 ||
            setenv("VIDEO_BROKER_SIGNAL_TEST_BEFORE_MAIN", "1", 1) != 0) _exit(2);
        execl(self, self, (char *)NULL);
        _exit(127);
    }
    int status;
    assert(waitpid(child, &status, 0) == child);
    /* A reaped pre-main process is not a checked native release receipt. */
    assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGTERM);
    puts("worker pre-main cancellation: signal exit, no checked release PASS");
}

int main(int argc, char **argv) {
    assert(argc > 0 && argv[0] != NULL);
    verify_pre_main_termination(argv[0]);
    assert(video_broker_worker_install_stop_signals(NULL) == -1);
    assert(errno == EINVAL);
    pid_t child = fork();
    assert(child >= 0);
    if (child == 0) {
        if (video_broker_worker_install_stop_signals(stop) != 0) _exit(2);
        const int signals[] = {SIGTERM, SIGTERM, SIGINT, SIGINT, SIGPIPE, SIGPIPE};
        for (unsigned i = 0; i < sizeof(signals) / sizeof(signals[0]); ++i) {
            if (raise(signals[i]) != 0 || received != (sig_atomic_t)(i + 1)) _exit(3);
            struct sigaction action;
            if (sigaction(signals[i], NULL, &action) != 0 || action.sa_handler != stop ||
                (action.sa_flags & (SA_RESETHAND | SA_NODEFER | SA_RESTART)) != 0) _exit(4);
        }
        /* Represents the unchanged success-with-checked-release receipt. */
        _exit(64);
    }
    int status;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 64);
    puts("worker stop signals: persistent handlers and checked exit PASS");
    return 0;
}
