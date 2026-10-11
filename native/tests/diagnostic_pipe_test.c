#define _POSIX_C_SOURCE 200809L
#include "diagnostic_pipe.h"
#include <assert.h>
#include <string.h>
#include <time.h>

int main(void) {
    int pipefd[2];
    assert(pipe(pipefd) == 0);
    vb_diagnostic_pipe sink;
    int flags = fcntl(pipefd[1], F_GETFL);
    assert(vb_diagnostic_pipe_open(&sink, pipefd[1]) == 0);
    assert(fcntl(pipefd[1], F_GETFL) == flags);
    assert(vb_diagnostic_pipe_printf(&sink, "event=test value=1\n") == 0);
    char data[4096];
    ssize_t n = read(pipefd[0], data, sizeof(data)-1);
    assert(n > 0); data[n] = 0;
    assert(strcmp(data, "event=test value=1 telemetry_dropped=0\n") == 0);
    memset(data, 'x', sizeof(data));
    while (write(sink.fd, data, sizeof(data)) > 0) {}
    assert(errno == EAGAIN);
    errno = EBUSY;
    struct timespec start, end;
    assert(clock_gettime(CLOCK_MONOTONIC, &start) == 0);
    for (int frame = 0; frame < 10000; ++frame)
        assert(vb_diagnostic_pipe_printf(&sink, "event=metrics frame=%d\n", frame) == -1);
    assert(clock_gettime(CLOCK_MONOTONIC, &end) == 0);
    double seconds = end.tv_sec-start.tv_sec+(end.tv_nsec-start.tv_nsec)/1e9;
    assert(seconds < 2.0 && sink.dropped == 10000);
    assert(errno == EBUSY);
    assert(fcntl(pipefd[1], F_GETFL) == flags);
    assert(fcntl(pipefd[0], F_SETFL, O_NONBLOCK) == 0);
    while (read(pipefd[0], data, sizeof(data)) > 0) {}
    assert(vb_diagnostic_pipe_printf(&sink, "event=recovered\n") == 0);
    n = read(pipefd[0], data, sizeof(data)-1); assert(n > 0); data[n] = 0;
    assert(strstr(data, "telemetry_dropped=10000\n"));
    char too_long[4097]; memset(too_long, 'x', sizeof(too_long)-1); too_long[4096] = 0;
    assert(vb_diagnostic_pipe_printf(&sink, "%s", too_long) == -1);
    close(pipefd[0]);
    assert(vb_diagnostic_pipe_printf(&sink, "event=closed\n") == -1);
    assert(sink.dropped == 10002); /* No fatal SIGPIPE. */
    vb_diagnostic_pipe_close(&sink); close(pipefd[1]);
    FILE *file = tmpfile(); assert(file);
    assert(vb_diagnostic_pipe_open(&sink, fileno(file)) == -1);
    assert(vb_diagnostic_pipe_printf(&sink, "not written") == -1);
    assert(ftell(file) == 0); fclose(file);
    vb_diagnostic_pipe_close(&sink);
    puts("OK native telemetry pipe: bounded full/closed sink, no storage or vendor flag mutation");
    return 0;
}
