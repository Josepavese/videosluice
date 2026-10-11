/* Parse the real executable's argv without opening a camera or SDK. */
#define main video_broker_v4l2_worker_main
#define ioctl test_ioctl
#include "../rdkx5_v4l2_mux_bridge.c"
#undef ioctl
#undef main
#include <assert.h>
#include <stdarg.h>

/* AArch64 sanitizer instrumentation can retain address-taken fanout callbacks
 * even when main is discarded. None may reach hardware in an argv/ioctl
 * fixture: resolve their symbols with explicit traps, never a permissive mock. */
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

static struct v4l2_buffer expected_queue;
static int queue_attempts, interrupted_queues, queue_error;

int test_ioctl(int fd, unsigned long request, ...) {
	(void)fd;
	va_list args;
	va_start(args, request);
	struct v4l2_buffer *buffer = va_arg(args, struct v4l2_buffer *);
	va_end(args);
	if (request == VIDIOC_DQBUF) {
		*buffer = expected_queue;
		return 0;
	}
	assert(request == VIDIOC_QBUF);
	assert(memcmp(buffer, &expected_queue, sizeof(*buffer)) == 0);
	queue_attempts++;
	/* QBUF is an in/out ioctl: the RDK UVC driver clears bytesused. */
	memset(buffer, 0, sizeof(*buffer));
	if (interrupted_queues-- > 0) { errno = EINTR; return -1; }
	if (queue_error) { errno = queue_error; return -1; }
	return 0;
}

static void test_capture_metadata_survives_queue(void) {
	memset(&expected_queue, 0, sizeof(expected_queue));
	expected_queue.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
	expected_queue.memory = V4L2_MEMORY_MMAP;
	expected_queue.index = 2;
	expected_queue.bytesused = 98765;
	expected_queue.sequence = 1234;
	expected_queue.timestamp.tv_sec = 5678;
	expected_queue.flags = V4L2_BUF_FLAG_TIMESTAMP_MONOTONIC;
	for (int mode = 0; mode < 3; mode++) {
		struct v4l2_buffer captured = expected_queue;
		interrupted_queues = mode == 1 ? 1 : 0;
		queue_error = mode == 2 ? EIO : 0;
		queue_attempts = 0;
		int rc = xioctl(42, VIDIOC_QBUF, &captured);
		assert(rc == (queue_error ? -1 : 0));
		assert(queue_attempts == (mode == 1 ? 2 : 1));
		assert(memcmp(&captured, &expected_queue, sizeof(captured)) == 0);
	}
	struct v4l2_buffer dequeued = {0};
	assert(xioctl(42, VIDIOC_DQBUF, &dequeued) == 0);
	assert(memcmp(&dequeued, &expected_queue, sizeof(dequeued)) == 0);
	puts("OK_V4L2_CAPTURE_METADATA_SURVIVES_QBUF_SUCCESS_EINTR_ERROR");
}

int main(void) {
	vb_capture_origin clean = {19, 12};
	assert(!capture_recovery_output_ready(true, clean, clean));
	assert(!capture_recovery_output_ready(false, clean, (vb_capture_origin){19, 11}));
	assert(!capture_recovery_output_ready(false, clean, (vb_capture_origin){18, 999}));
	assert(capture_recovery_output_ready(false, clean, clean));
	assert(capture_recovery_output_ready(false, clean, (vb_capture_origin){19, 13}));
	assert(!capture_recovery_output_ready(true, (vb_capture_origin){0,0}, (vb_capture_origin){19,999}));
	puts("OK_STARTUP_RESYNC_OUTPUT_FENCE_REJECTS_LATE_AND_CROSS_EPOCH_SURFACES");
	const unsigned char hevc_idr[] = {0,0,1,38,1,128};
	const unsigned char hevc_cra[] = {0,0,0,1,42,1,128};
	const unsigned char hevc_params[] = {0,0,1,64,1,128};
	const unsigned char h264_idr[] = {0,0,1,0x65,128};
	const unsigned char h264_p[] = {0,0,1,0x41,128};
	assert(video_broker_h26x_first_picture(true, hevc_idr, sizeof(hevc_idr)) == 2);
	assert(video_broker_h26x_first_picture(true, hevc_cra, sizeof(hevc_cra)) == 1);
	assert(video_broker_h26x_first_picture(true, hevc_params, sizeof(hevc_params)) == 0);
	assert(video_broker_h26x_first_picture(false, h264_idr, sizeof(h264_idr)) == 2);
	assert(video_broker_h26x_first_picture(false, h264_p, sizeof(h264_p)) == 1);
	assert(video_broker_h26x_first_picture(true, hevc_idr, sizeof(hevc_idr)-2) == -1);
	assert(video_broker_h26x_first_picture(true, NULL, 0) == -1);
	puts("OK_GENERIC_H26X_STARTUP_IDR_NOT_CRA_OR_PARAMETER_ONLY");
	assert(capture_sequence_contiguous(0, 1));
	assert(capture_sequence_contiguous(UINT32_MAX, 0));
	assert(!capture_sequence_contiguous(41, 43));
	assert(!capture_sequence_contiguous(41, 41));
	assert(!capture_sequence_contiguous(41, 40));
	assert(!capture_sequence_contiguous(UINT32_MAX - 1, 0));
	puts("OK_V4L2_INTERFRAME_SEQUENCE_LOSS_DUPLICATE_REORDER_AND_WRAP");
	test_capture_metadata_survives_queue();
	char *args[] = {"worker", "--device", "/dev/fixture-camera", "--input-format", "h265",
		"--mjpeg-decode-accelerator", "jpu", "--mjpeg-decode-initial-backend", "jpu",
		"--live-enabled", "false", "--analysis-enabled", "true", "--archive-enabled", "false",
		"--live-codec", "h265", "--source-id", "overview", "--media-epoch", "1",
		"--frame-launch-socket-fd", "3", "--frame-launch-controller-fd", "4",
		"--frame-launch-controller-metadata", "00000000000000000000000000000000000000",
		"--frame-native-mailbox-fd", "5", "--frame-native-mailbox-generation", "1",
		"--frame-output-mailbox-fd", "6", "--frame-output-mailbox-generation", "2",
		"--frame-output-mailbox-capacity", "346048",
		"--frame-output-stream", "analysis"};
	int count = (int)(sizeof(args) / sizeof(args[0]));
	bridge_config_t cfg;
	default_config(&cfg);
	assert(parse_args(count, args, &cfg) == 0);
	assert(cfg.frame_launch.seen == 511);
	assert(strcmp(cfg.frame_launch.output_stream, "analysis") == 0);
	args[count - 1] = "custom.analysis-2";
	default_config(&cfg);
	assert(parse_args(count, args, &cfg) == 0);
	assert(strcmp(cfg.frame_launch.output_stream, "custom.analysis-2") == 0);
	args[count - 1] = "invalid/stream";
	default_config(&cfg);
	assert(parse_args(count, args, &cfg) != 0);
	default_config(&cfg);
	assert(parse_args(count - 1, args, &cfg) != 0);
	args[count - 2] = "--frame-output-unknown";
	args[count - 1] = "analysis";
	default_config(&cfg);
	assert(parse_args(count, args, &cfg) != 0);
	puts("OK_V4L2_REAL_FRAME_OUTPUT_ARGV");
	return 0;
}
