/* Real owner/destructor, fake API boundary: every release and queue drain can
 * fail independently. No GPU is accessed or attested by this test. */
#define VIDEO_BROKER_HAVE_OPENCL 1
#include "../nv12_tone_mapper_accelerator.h"
#include <assert.h>

static int fail_release, fail_finish, fail_read, releases, finishes, creates;
static bool released[17];
static unsigned char pixels[640 * 360 * 3 / 2];

static void reset(void) {
	fail_release = fail_finish = fail_read = releases = finishes = creates = 0;
	memset(released, 0, sizeof(released));
	atomic_store(&video_broker_tone_release_quarantined, false);
}

static cl_int release_object(void *handle) {
	uintptr_t id = (uintptr_t)handle;
	assert(id >= 1 && id <= 16 && !released[id]);
	releases++;
	if (releases == fail_release) return CL_OUT_OF_RESOURCES;
	released[id] = true;
	return CL_SUCCESS;
}
cl_int clReleaseMemObject(cl_mem h) { return release_object(h); }
cl_int clReleaseKernel(cl_kernel h) { return release_object(h); }
cl_int clReleaseProgram(cl_program h) { return release_object(h); }
cl_int clReleaseCommandQueue(cl_command_queue h) { return release_object(h); }
cl_int clReleaseContext(cl_context h) { return release_object(h); }
cl_int clFinish(cl_command_queue h) {
	assert(h != NULL && releases == 0);
	finishes++;
	return fail_finish ? CL_OUT_OF_RESOURCES : CL_SUCCESS;
}

/* Other entry points only support the deliberately partial initialization
 * and queued-apply error cases below. Unexpected use fails the test. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
cl_int clGetPlatformIDs(cl_uint n, cl_platform_id *p, cl_uint *count) {
	*p = (cl_platform_id)32; *count = 1; return CL_SUCCESS;
}
cl_int clGetDeviceIDs(cl_platform_id p, cl_device_type t, cl_uint n, cl_device_id *d, cl_uint *count) {
	*d = (cl_device_id)33; *count = 1; return CL_SUCCESS;
}
cl_context clCreateContext(const cl_context_properties *p, cl_uint n, const cl_device_id *d,
	void (*notify)(const char *, const void *, size_t, void *), void *data, cl_int *rc) {
	creates++; *rc = CL_SUCCESS; return (cl_context)16;
}
cl_command_queue clCreateCommandQueue(cl_context c, cl_device_id d, cl_command_queue_properties p, cl_int *rc) {
	creates++; *rc = CL_OUT_OF_RESOURCES; return NULL;
}
cl_program clCreateProgramWithSource(cl_context c, cl_uint n, const char **s, const size_t *l, cl_int *rc) {
	assert(false); return NULL;
}
cl_int clBuildProgram(cl_program p, cl_uint n, const cl_device_id *d, const char *options,
	void (*notify)(cl_program, void *), void *data) { assert(false); return CL_OUT_OF_RESOURCES; }
cl_kernel clCreateKernel(cl_program p, const char *name, cl_int *rc) { assert(false); return NULL; }
cl_mem clCreateBuffer(cl_context c, cl_mem_flags f, size_t n, void *p, cl_int *rc) {
	creates++; assert(false); return NULL;
}
cl_int clGetKernelWorkGroupInfo(cl_kernel k, cl_device_id d, cl_uint p, size_t n, void *v, size_t *s) {
	assert(false); return CL_OUT_OF_RESOURCES;
}
cl_int clSetKernelArg(cl_kernel k, cl_uint i, size_t n, const void *p) { return CL_SUCCESS; }
cl_int clEnqueueWriteBuffer(cl_command_queue q, cl_mem m, cl_bool blocking, size_t o, size_t n,
	const void *p, cl_uint count, const cl_event *wait, cl_event *event) { return CL_SUCCESS; }
cl_int clEnqueueReadBuffer(cl_command_queue q, cl_mem m, cl_bool blocking, size_t o, size_t n,
	void *p, cl_uint count, const cl_event *wait, cl_event *event) {
	assert(blocking == CL_TRUE); return fail_read ? CL_OUT_OF_RESOURCES : CL_SUCCESS;
}
cl_int clEnqueueNDRangeKernel(cl_command_queue q, cl_kernel k, cl_uint d, const size_t *o,
	const size_t *g, const size_t *l, cl_uint count, const cl_event *wait, cl_event *event) { return CL_SUCCESS; }
#pragma GCC diagnostic pop

static video_broker_tone_accelerator_t full_owner(void) {
	return (video_broker_tone_accelerator_t){
		.initialized = true, .requested = VIDEO_BROKER_TONE_ACCELERATOR_OPENCL,
		.selected = VIDEO_BROKER_TONE_ACCELERATOR_OPENCL,
		.y_weight_buffer = (cl_mem)1, .y_bottom_buffer = (cl_mem)2,
		.y_top_buffer = (cl_mem)3, .x_weight_buffer = (cl_mem)4,
		.x_right_buffer = (cl_mem)5, .x_left_buffer = (cl_mem)6,
		.spatial_buffer = (cl_mem)7, .frame_buffer = (cl_mem)8,
		.tile_buffer = (cl_mem)9, .curve_buffer = (cl_mem)10,
		.global_curve_buffer = (cl_mem)11, .spatial_kernel = (cl_kernel)12,
		.apply_kernel = (cl_kernel)13, .program = (cl_program)14,
		.queue = (cl_command_queue)15, .context = (cl_context)16,
		.host_frame = pixels, .width = 640, .height = 360, .pixel_count = 640 * 360,
		.apply_global_work_size = 640 * 360, .apply_local_work_size = 1,
	};
}

static void release_faults(void) {
	assert(video_broker_tone_accelerator_destroy(NULL) == 0);
	for (int failure = 0; failure <= 16; failure++) {
		reset(); fail_release = failure;
		video_broker_tone_accelerator_t a = full_owner();
		a.commands_pending = true;
		assert((video_broker_tone_accelerator_destroy(&a) == 0) == (failure == 0));
		assert(finishes == 1);
		assert(releases == (failure == 0 ? 16 : failure));
		assert(video_broker_tone_accelerator_resources_quarantined() == (failure != 0));
		int calls = releases;
		assert((video_broker_tone_accelerator_destroy(&a) == 0) == (failure == 0));
		assert(releases == calls && finishes == 1); /* No ambiguous retry. */
		if (failure != 0) {
			assert(a.release_failed && strstr(a.error, "clRelease") != NULL);
			if (failure <= 8) assert(a.host_frame == pixels && a.frame_buffer != NULL);
			video_broker_tone_accelerator_t other = {0};
			assert(video_broker_tone_accelerator_init(&other, VIDEO_BROKER_TONE_ACCELERATOR_CPU) == -1);
			assert(creates == 0); /* No new resources or silent CPU escape. */
		}
	}
	reset(); fail_finish = 1;
	video_broker_tone_accelerator_t a = full_owner(); a.commands_pending = true;
	assert(video_broker_tone_accelerator_destroy(&a) == -1);
	assert(releases == 0 && finishes == 1 && a.host_frame == pixels && a.commands_pending);
	assert(strcmp(a.error, "clFinish rc=-5") == 0);
	assert(video_broker_tone_accelerator_destroy(&a) == -1 && finishes == 1);
	reset(); a = full_owner();
	assert(video_broker_tone_accelerator_destroy(&a) == 0 && finishes == 0);
}

static void partial_start_and_fallback(void) {
	reset();
	video_broker_tone_accelerator_t a = {0};
	assert(video_broker_tone_accelerator_init(&a, VIDEO_BROKER_TONE_ACCELERATOR_OPENCL) == -1);
	assert(releases == 1 && !video_broker_tone_accelerator_resources_quarantined());
	reset(); fail_release = 1;
	assert(video_broker_tone_accelerator_init(&a, VIDEO_BROKER_TONE_ACCELERATOR_AUTO) == -1);
	assert(a.context == (cl_context)16 && a.release_failed && !a.initialized);
	assert(video_broker_tone_accelerator_destroy(&a) == -1 && releases == 1);
	reset(); fail_release = 1; a = full_owner();
	a.requested = VIDEO_BROKER_TONE_ACCELERATOR_AUTO;
	video_broker_nv12_tone_mapper_t mapper = {0};
	assert(video_broker_tone_accelerator_runtime_fallback(&a, &mapper, pixels, 640, 360) == -1);
	assert(a.selected == VIDEO_BROKER_TONE_ACCELERATOR_OPENCL && a.release_failed);
	reset(); fail_release = 1; a = full_owner();
	assert(video_broker_tone_accelerator_ensure_surface(&a, &mapper, pixels, 320, 180) == -1);
	assert(a.host_frame == pixels && a.width == 640 && creates == 0);
	video_broker_nv12_tone_mapper_destroy(&mapper);
}

static void queued_apply_failure(void) {
	for (int read_error = 0; read_error <= 1; read_error++) {
		reset(); fail_read = read_error;
		video_broker_tone_accelerator_t a = full_owner();
		video_broker_nv12_tone_mapper_t mapper = {
			.enabled = true, .initialized = true, .signal_informative = true,
			.maximum_gain = 1.5, .minimum_gain = 1, .active_mode = VIDEO_BROKER_TONE_GLOBAL,
		};
		assert(video_broker_tone_accelerator_apply(&a, &mapper, pixels, 640, 360) == (read_error ? -1 : 1));
		assert(a.commands_pending == (read_error != 0));
		assert(video_broker_tone_accelerator_destroy(&a) == 0);
		assert(finishes == read_error && releases == 16);
		video_broker_nv12_tone_mapper_destroy(&mapper);
	}
}

int main(void) {
	release_faults(); partial_start_and_fallback(); queued_apply_failure();
	puts("OK_OPENCL_CHECKED_RELEASE_FAILURE_LIFETIMES");
	return 0;
}
