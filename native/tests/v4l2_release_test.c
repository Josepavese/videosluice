/* Actual camera close function with deterministic syscall failure injection. */
#define main video_broker_v4l2_worker_main
#include "../rdkx5_v4l2_mux_bridge.c"
#undef main

#include <assert.h>

/* AArch64 sanitizer instrumentation retains address-taken fanout callbacks.
 * This syscall fixture must never initialize or touch a vendor codec. */
int video_broker_rdkx5_persistent_encoder_destroy(video_broker_rdkx5_persistent_encoder_t *p) {
	(void)p; abort();
}
bool video_broker_rdkx5_encoder_resources_quarantined(void) { abort(); }
int video_broker_rdkx5_persistent_encoder_init(video_broker_rdkx5_persistent_encoder_t *p,
	video_broker_rdkx5_encoder_codec_t codec, video_broker_rdkx5_rate_control_profile_t rate,
	int index, int width, int height, int fps, int bitrate) {
	(void)p; (void)codec; (void)rate; (void)index; (void)width; (void)height; (void)fps; (void)bitrate; abort();
}
int video_broker_rdkx5_persistent_encoder_set_control(video_broker_rdkx5_persistent_encoder_t *p,
	bool active, int fps, int bitrate) {
	(void)p; (void)active; (void)fps; (void)bitrate; abort();
}
const char *video_broker_worker_role_name(video_broker_worker_role_t role) { (void)role; abort(); }

enum { fail_streamoff = 1, fail_unmap = 2, fail_close = 4 };
static int failures, streamoff_error, streamoff_calls, unmap_calls, close_calls;

int __wrap_ioctl(int fd, unsigned long request, ...) {
	assert(fd == 123 && request == VIDIOC_STREAMOFF);
	streamoff_calls++;
	if (failures & fail_streamoff) { errno = streamoff_error; return -1; }
	return 0;
}
int __wrap_munmap(void *address, size_t length) {
	assert(address == (void *)0x1000 && length == 4096);
	unmap_calls++;
	if (failures & fail_unmap) { errno = EINVAL; return -1; }
	return 0;
}
int __wrap_close(int fd) {
	assert(fd == 123);
	close_calls++;
	if (failures & fail_close) { errno = EIO; return -1; }
	return 0;
}

int main(void) {
	/* Reproduce an encoder error after prior successful rc=0: checked SDK
	 * cleanup must produce failed/released (64), never clean exit (0). */
	assert(video_broker_worker_operation_status(0, false) != 0);
	assert(video_broker_worker_exit_status(video_broker_worker_operation_status(0, false), true) == 64);
	assert(video_broker_worker_exit_status(video_broker_worker_operation_status(0, false), false) == 65);
	assert(video_broker_worker_operation_status(-1, false) == -1);
	assert(video_broker_worker_operation_status(1, true) == 0);
	assert(video_broker_worker_exit_status(0, true) == 0);
	assert(video_broker_worker_exit_status(-1, true) == 64);
	assert(video_broker_worker_exit_status(0, false) == 65);
	assert(video_broker_worker_exit_status(1, false) == 65);
	assert(close_v4l2_capture(NULL) == 0);
	/* Only ENODEV means the V4L2 device was removed. A disconnect does not
	 * excuse a failed munmap/close or an unrelated ioctl/SDK failure. */
	const int streamoff_errors[] = {EIO, ENODEV, EINVAL, EBADF, ENXIO};
	for (size_t error_index = 0; error_index < sizeof(streamoff_errors) / sizeof(streamoff_errors[0]); ++error_index) {
		streamoff_error = streamoff_errors[error_index];
		for (failures = 0; failures < 8; ++failures) {
			v4l2_capture_t capture = {0};
			capture.fd = 123;
			capture.streaming = true;
			capture.buffer_count = 2;
			capture.buffers = calloc(capture.buffer_count, sizeof(*capture.buffers));
			assert(capture.buffers != NULL);
			capture.buffers[0].start = (void *)0x1000;
			capture.buffers[0].length = 4096;
			/* A failed/partial preparation has not mapped every requested slot. */
			capture.buffers[1].start = MAP_FAILED;
			streamoff_calls = unmap_calls = close_calls = 0;
			int result = close_v4l2_capture(&capture);
			const bool released = failures == 0 || (failures == fail_streamoff && streamoff_error == ENODEV);
			assert((result == 0) == released);
			assert(video_broker_worker_exit_status(0, result == 0) == (released ? 0 : 65));
			/* A recovered resource receipt must not turn capture failure into
			 * success; the broker still observes failed operation status 64. */
			assert(video_broker_worker_exit_status(-1, result == 0) == (released ? 64 : 65));
			assert(streamoff_calls == 1 && unmap_calls == 1 && close_calls == 1);
			assert(capture.fd == -1 && capture.buffers == NULL);
			/* No retry of an uncertain close/unmap against a reused descriptor. */
			assert(close_v4l2_capture(&capture) == 0);
			assert(streamoff_calls == 1 && unmap_calls == 1 && close_calls == 1);
		}
	}
	puts("OK_V4L2_CHECKED_RELEASE_AND_WORKER_RECEIPT");
	return 0;
}
