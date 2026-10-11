#define _POSIX_C_SOURCE 200809L
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

int main(int argc, char **argv) {
	if (argc != 2) return 2;
	FILE *file = fopen("/proc/self/cgroup", "r");
	if (!file) return 3;
	char line[1024];
	while (fgets(line, sizeof line, file)) fputs(line, stdout);
	fclose(file);
	puts("READY");
	fflush(stdout);
	if (!strcmp(argv[1], "memory")) {
		volatile unsigned char *bytes = malloc(256U * 1024U * 1024U);
		if (!bytes) return 7;
		for (size_t i = 0; i < 256U * 1024U * 1024U; i += 4096) bytes[i] = 1;
		return 8; /* A 16MiB boundary must not let this complete. */
	}
	pid_t children[128];
	int count = 0;
	if (!strcmp(argv[1], "fork")) {
		while (count < 128) {
			pid_t child = fork();
			if (child < 0) break;
			if (child == 0) { for (;;) pause(); }
			children[count++] = child;
		}
		printf("FORKED %d ERRNO %d\n", count, errno);
		fflush(stdout);
	}
	char stop;
	ssize_t received = read(STDIN_FILENO, &stop, 1);
	for (int i = 0; i < count; ++i) kill(children[i], SIGTERM);
	for (int i = 0; i < count; ++i) waitpid(children[i], NULL, 0);
	return received < 0 ? 6 : 0;
}
