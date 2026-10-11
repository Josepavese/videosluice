#ifndef VIDEO_BROKER_NV12_TONE_MAPPER_ACCELERATOR_H
#define VIDEO_BROKER_NV12_TONE_MAPPER_ACCELERATOR_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

#include "nv12_tone_mapper.h"

typedef enum {
	VIDEO_BROKER_TONE_ACCELERATOR_AUTO = 0,
	VIDEO_BROKER_TONE_ACCELERATOR_CPU = 1,
	VIDEO_BROKER_TONE_ACCELERATOR_OPENCL = 2,
} video_broker_tone_accelerator_kind_t;

/* A failed native release retires this worker. Never reuse uncertain handles
 * or certify teardown merely because their owning object was zeroed. */
static atomic_bool video_broker_tone_release_quarantined = ATOMIC_VAR_INIT(false);

static inline bool video_broker_tone_accelerator_resources_quarantined(void) {
	return atomic_load(&video_broker_tone_release_quarantined);
}

static inline video_broker_tone_accelerator_kind_t video_broker_tone_accelerator_parse(
	const char *value
) {
	if (value == NULL || *value == '\0' || strcmp(value, "auto") == 0) {
		return VIDEO_BROKER_TONE_ACCELERATOR_AUTO;
	}
	if (strcmp(value, "cpu") == 0) {
		return VIDEO_BROKER_TONE_ACCELERATOR_CPU;
	}
	if (strcmp(value, "opencl") == 0) {
		return VIDEO_BROKER_TONE_ACCELERATOR_OPENCL;
	}
	return VIDEO_BROKER_TONE_ACCELERATOR_AUTO;
}

static inline const char *video_broker_tone_accelerator_name(
	video_broker_tone_accelerator_kind_t kind
) {
	switch (kind) {
	case VIDEO_BROKER_TONE_ACCELERATOR_CPU:
		return "cpu";
	case VIDEO_BROKER_TONE_ACCELERATOR_OPENCL:
		return "opencl";
	case VIDEO_BROKER_TONE_ACCELERATOR_AUTO:
	default:
		return "auto";
	}
}

#if defined(VIDEO_BROKER_HAVE_OPENCL)

#define CL_TARGET_OPENCL_VERSION 120
#include <CL/cl.h>

typedef struct {
	video_broker_tone_accelerator_kind_t requested;
	video_broker_tone_accelerator_kind_t selected;
	bool initialized;
	bool release_failed;
	bool commands_pending;
	cl_context context;
	cl_command_queue queue;
	cl_program program;
	cl_kernel apply_kernel;
	cl_kernel spatial_kernel;
	cl_mem frame_buffer;
	cl_mem spatial_buffer;
	cl_mem tile_buffer;
	cl_mem curve_buffer;
	cl_mem global_curve_buffer;
	cl_mem x_left_buffer;
	cl_mem x_right_buffer;
	cl_mem x_weight_buffer;
	cl_mem y_top_buffer;
	cl_mem y_bottom_buffer;
	cl_mem y_weight_buffer;
	unsigned char *host_frame;
	int width;
	int height;
	size_t pixel_count;
	size_t apply_global_work_size;
	size_t apply_local_work_size;
	uint64_t uploaded_statistics_updates;
	char error[192];
} video_broker_tone_accelerator_t;

static const char *video_broker_tone_opencl_kernel_source =
	"__kernel void tone_build_spatial(__global ushort *spatial, "
	"                                 __global const ushort *tiles, "
	"                                 __global const ushort *x_left, "
	"                                 __global const ushort *x_right, "
	"                                 __global const ushort *x_weight, "
	"                                 __global const ushort *y_top, "
	"                                 __global const ushort *y_bottom, "
	"                                 __global const ushort *y_weight, "
	"                                 const uint width, const uint height, "
	"                                 const uint columns) {\n"
	"  const uint x = get_global_id(0);\n"
	"  const uint y = get_global_id(1);\n"
	"  if (x < width && y < height) {\n"
	"    const int left = (int)x_left[x];\n"
	"    const int right = (int)x_right[x];\n"
	"    const int wx = (int)x_weight[x];\n"
	"    const int top = (int)y_top[y];\n"
	"    const int bottom = (int)y_bottom[y];\n"
	"    const int wy = (int)y_weight[y];\n"
	"    const int tl = (int)tiles[top * (int)columns + left];\n"
	"    const int tr = (int)tiles[top * (int)columns + right];\n"
	"    const int bl = (int)tiles[bottom * (int)columns + left];\n"
	"    const int br = (int)tiles[bottom * (int)columns + right];\n"
	"    const int ti = tl + ((tr - tl) * wx + 128) / 256;\n"
	"    const int bi = bl + ((br - bl) * wx + 128) / 256;\n"
	"    spatial[y * width + x] = (ushort)(ti + ((bi - ti) * wy + 128) / 256);\n"
	"  }\n"
	"}\n"
	"__kernel void tone_apply(__global uchar *y, "
	"                         __global const ushort *spatial, "
	"                         __global const uchar *curves, "
	"                         __global const uchar *global_curve, "
	"                         const uint pixels, const uint local_mode) {\n"
	"  const uint offset = get_global_id(0);\n"
	"  if (offset < pixels) {\n"
	"    const uint value = (uint)y[offset];\n"
	"    y[offset] = local_mode ? curves[((uint)spatial[offset] << 8) + value] "
	"                           : global_curve[value];\n"
	"  }\n"
	"}\n";

static inline void video_broker_tone_accelerator_set_error(
	video_broker_tone_accelerator_t *accelerator,
	const char *operation,
	cl_int rc
) {
	if (accelerator != NULL) {
		(void)snprintf(accelerator->error, sizeof(accelerator->error), "%s rc=%d", operation, rc);
	}
}

static inline int video_broker_tone_accelerator_release_failure(
	video_broker_tone_accelerator_t *accelerator, const char *operation, cl_int rc
) {
	accelerator->release_failed = true;
	atomic_store(&video_broker_tone_release_quarantined, true);
	video_broker_tone_accelerator_set_error(accelerator, operation, rc);
	fprintf(stderr, "[tone-accelerator][release] operation=%s rc=%d"
		" release=unconfirmed action=retire_worker buffers=retained\n", operation, rc);
	return -1;
}

#define VIDEO_BROKER_TONE_RELEASE(member, function) do { \
	if (accelerator->member != NULL) { \
		cl_int release_rc = function(accelerator->member); \
		if (release_rc != CL_SUCCESS) \
			return video_broker_tone_accelerator_release_failure( \
				accelerator, #function "(" #member ")", release_rc); \
		accelerator->member = NULL; \
	} \
} while (0)

static inline int video_broker_tone_accelerator_release_surface(
	video_broker_tone_accelerator_t *accelerator
) {
	if (accelerator == NULL) return 0;
	if (accelerator->release_failed) return -1;
	/* Normal apply ends with a checked blocking read, so no extra queue wait
	 * is added per frame. Partial enqueue failures must drain before freeing
	 * CL_MEM_USE_HOST_PTR storage or the mapper's asynchronous upload inputs.
	 * A wedged driver remains inside the existing worker stop deadline; it
	 * cannot yield a successful native release receipt. */
	if (accelerator->commands_pending) {
		cl_int rc = accelerator->queue != NULL ? clFinish(accelerator->queue) : CL_INVALID_COMMAND_QUEUE;
		if (rc != CL_SUCCESS)
			return video_broker_tone_accelerator_release_failure(accelerator, "clFinish", rc);
		accelerator->commands_pending = false;
	}
	VIDEO_BROKER_TONE_RELEASE(y_weight_buffer, clReleaseMemObject);
	VIDEO_BROKER_TONE_RELEASE(y_bottom_buffer, clReleaseMemObject);
	VIDEO_BROKER_TONE_RELEASE(y_top_buffer, clReleaseMemObject);
	VIDEO_BROKER_TONE_RELEASE(x_weight_buffer, clReleaseMemObject);
	VIDEO_BROKER_TONE_RELEASE(x_right_buffer, clReleaseMemObject);
	VIDEO_BROKER_TONE_RELEASE(x_left_buffer, clReleaseMemObject);
	VIDEO_BROKER_TONE_RELEASE(spatial_buffer, clReleaseMemObject);
	VIDEO_BROKER_TONE_RELEASE(frame_buffer, clReleaseMemObject);
	accelerator->host_frame = NULL;
	accelerator->width = 0;
	accelerator->height = 0;
	accelerator->pixel_count = 0;
	accelerator->apply_global_work_size = 0;
	accelerator->uploaded_statistics_updates = UINT64_MAX;
	return 0;
}

static inline int video_broker_tone_accelerator_destroy(
	video_broker_tone_accelerator_t *accelerator
) {
	if (accelerator == NULL) return 0;
	if (video_broker_tone_accelerator_release_surface(accelerator) != 0) return -1;
	VIDEO_BROKER_TONE_RELEASE(tile_buffer, clReleaseMemObject);
	VIDEO_BROKER_TONE_RELEASE(curve_buffer, clReleaseMemObject);
	VIDEO_BROKER_TONE_RELEASE(global_curve_buffer, clReleaseMemObject);
	VIDEO_BROKER_TONE_RELEASE(spatial_kernel, clReleaseKernel);
	VIDEO_BROKER_TONE_RELEASE(apply_kernel, clReleaseKernel);
	VIDEO_BROKER_TONE_RELEASE(program, clReleaseProgram);
	VIDEO_BROKER_TONE_RELEASE(queue, clReleaseCommandQueue);
	VIDEO_BROKER_TONE_RELEASE(context, clReleaseContext);
	memset(accelerator, 0, sizeof(*accelerator));
	return 0;
}

#undef VIDEO_BROKER_TONE_RELEASE

static inline int video_broker_tone_accelerator_init(
	video_broker_tone_accelerator_t *accelerator,
	video_broker_tone_accelerator_kind_t requested
) {
	cl_platform_id platform = NULL;
	cl_device_id device = NULL;
	cl_uint count = 0;
	cl_int rc;
	size_t max_work_group_size = 0;
	if (accelerator == NULL) {
		return -1;
	}
	if (video_broker_tone_accelerator_resources_quarantined()) return -1;
	memset(accelerator, 0, sizeof(*accelerator));
	accelerator->requested = requested;
	accelerator->selected = VIDEO_BROKER_TONE_ACCELERATOR_CPU;
	accelerator->uploaded_statistics_updates = UINT64_MAX;
	if (requested == VIDEO_BROKER_TONE_ACCELERATOR_CPU) {
		accelerator->initialized = true;
		return 0;
	}
	rc = clGetPlatformIDs(1, &platform, &count);
	if (rc != CL_SUCCESS || count == 0) {
		video_broker_tone_accelerator_set_error(accelerator, "clGetPlatformIDs", rc);
		goto unavailable;
	}
	rc = clGetDeviceIDs(platform, CL_DEVICE_TYPE_GPU, 1, &device, &count);
	if (rc != CL_SUCCESS || count == 0) {
		video_broker_tone_accelerator_set_error(accelerator, "clGetDeviceIDs", rc);
		goto unavailable;
	}
	accelerator->context = clCreateContext(NULL, 1, &device, NULL, NULL, &rc);
	if (accelerator->context == NULL || rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clCreateContext", rc);
		goto unavailable;
	}
	accelerator->queue = clCreateCommandQueue(accelerator->context, device, 0, &rc);
	if (accelerator->queue == NULL || rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clCreateCommandQueue", rc);
		goto unavailable;
	}
	accelerator->program = clCreateProgramWithSource(
		accelerator->context, 1, &video_broker_tone_opencl_kernel_source, NULL, &rc);
	if (accelerator->program == NULL || rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clCreateProgramWithSource", rc);
		goto unavailable;
	}
	rc = clBuildProgram(accelerator->program, 1, &device, "-cl-std=CL1.2", NULL, NULL);
	if (rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clBuildProgram", rc);
		goto unavailable;
	}
	accelerator->apply_kernel = clCreateKernel(accelerator->program, "tone_apply", &rc);
	if (accelerator->apply_kernel == NULL || rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clCreateKernel(tone_apply)", rc);
		goto unavailable;
	}
	accelerator->spatial_kernel = clCreateKernel(accelerator->program, "tone_build_spatial", &rc);
	if (accelerator->spatial_kernel == NULL || rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clCreateKernel(tone_build_spatial)", rc);
		goto unavailable;
	}
	accelerator->curve_buffer = clCreateBuffer(
		accelerator->context, CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
		sizeof(video_broker_tone_gain_lut), video_broker_tone_gain_lut, &rc);
	if (accelerator->curve_buffer == NULL || rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clCreateBuffer(curves)", rc);
		goto unavailable;
	}
	accelerator->global_curve_buffer = clCreateBuffer(
		accelerator->context, CL_MEM_READ_ONLY,
		sizeof(((video_broker_nv12_tone_mapper_t *)0)->global_lut), NULL, &rc);
	if (accelerator->global_curve_buffer == NULL || rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clCreateBuffer(global curve)", rc);
		goto unavailable;
	}
	accelerator->tile_buffer = clCreateBuffer(
		accelerator->context, CL_MEM_READ_ONLY,
		sizeof(((video_broker_nv12_tone_mapper_t *)0)->tile_gain_lut_index), NULL, &rc);
	if (accelerator->tile_buffer == NULL || rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clCreateBuffer(tiles)", rc);
		goto unavailable;
	}
	if (clGetKernelWorkGroupInfo(
			accelerator->apply_kernel, device, CL_KERNEL_WORK_GROUP_SIZE,
			sizeof(max_work_group_size), &max_work_group_size, NULL) != CL_SUCCESS ||
	    max_work_group_size == 0) {
		max_work_group_size = 64;
	}
	accelerator->apply_local_work_size = 256;
	while (accelerator->apply_local_work_size > max_work_group_size &&
	       accelerator->apply_local_work_size > 1) {
		accelerator->apply_local_work_size /= 2;
	}
	accelerator->selected = VIDEO_BROKER_TONE_ACCELERATOR_OPENCL;
	accelerator->initialized = true;
	return 0;

unavailable:
	{
		char retained_error[sizeof(accelerator->error)];
		(void)snprintf(retained_error, sizeof(retained_error), "%s", accelerator->error);
		if (video_broker_tone_accelerator_destroy(accelerator) != 0) return -1;
		accelerator->requested = requested;
		accelerator->selected = VIDEO_BROKER_TONE_ACCELERATOR_CPU;
		accelerator->initialized = true;
		(void)snprintf(accelerator->error, sizeof(accelerator->error), "%s", retained_error);
	}
	return requested == VIDEO_BROKER_TONE_ACCELERATOR_OPENCL ? -1 : 0;
}

static inline cl_mem video_broker_tone_accelerator_copy_buffer(
	video_broker_tone_accelerator_t *accelerator,
	size_t size,
	void *host,
	cl_int *rc
) {
	return clCreateBuffer(
		accelerator->context, CL_MEM_READ_ONLY | CL_MEM_COPY_HOST_PTR,
		size, host, rc);
}

static inline int video_broker_tone_accelerator_ensure_surface(
	video_broker_tone_accelerator_t *accelerator,
	video_broker_nv12_tone_mapper_t *mapper,
	unsigned char *nv12,
	int width,
	int height
) {
	cl_int rc;
	cl_uint pixels;
	cl_uint width_arg;
	cl_uint height_arg;
	size_t pixel_count = (size_t)width * (size_t)height;
	if (!video_broker_nv12_tone_mapper_prepare_geometry_axes(mapper, width, height)) {
		(void)snprintf(accelerator->error, sizeof(accelerator->error), "tone geometry unavailable");
		return -1;
	}
	if (accelerator->frame_buffer != NULL && accelerator->host_frame == nv12 &&
	    accelerator->width == width && accelerator->height == height) {
		return 0;
	}
	if (video_broker_tone_accelerator_release_surface(accelerator) != 0) return -1;
	accelerator->frame_buffer = clCreateBuffer(
		accelerator->context, CL_MEM_READ_WRITE | CL_MEM_USE_HOST_PTR,
		pixel_count, nv12, &rc);
	if (accelerator->frame_buffer == NULL || rc != CL_SUCCESS) goto surface_error;
	accelerator->spatial_buffer = clCreateBuffer(
		accelerator->context, CL_MEM_READ_WRITE,
		pixel_count * sizeof(uint16_t), NULL, &rc);
	if (accelerator->spatial_buffer == NULL || rc != CL_SUCCESS) goto surface_error;
	accelerator->x_left_buffer = video_broker_tone_accelerator_copy_buffer(
		accelerator, (size_t)width * sizeof(uint16_t), mapper->x_left, &rc);
	if (accelerator->x_left_buffer == NULL || rc != CL_SUCCESS) goto surface_error;
	accelerator->x_right_buffer = video_broker_tone_accelerator_copy_buffer(
		accelerator, (size_t)width * sizeof(uint16_t), mapper->x_right, &rc);
	if (accelerator->x_right_buffer == NULL || rc != CL_SUCCESS) goto surface_error;
	accelerator->x_weight_buffer = video_broker_tone_accelerator_copy_buffer(
		accelerator, (size_t)width * sizeof(uint16_t), mapper->x_weight, &rc);
	if (accelerator->x_weight_buffer == NULL || rc != CL_SUCCESS) goto surface_error;
	accelerator->y_top_buffer = video_broker_tone_accelerator_copy_buffer(
		accelerator, (size_t)height * sizeof(uint16_t), mapper->y_top, &rc);
	if (accelerator->y_top_buffer == NULL || rc != CL_SUCCESS) goto surface_error;
	accelerator->y_bottom_buffer = video_broker_tone_accelerator_copy_buffer(
		accelerator, (size_t)height * sizeof(uint16_t), mapper->y_bottom, &rc);
	if (accelerator->y_bottom_buffer == NULL || rc != CL_SUCCESS) goto surface_error;
	accelerator->y_weight_buffer = video_broker_tone_accelerator_copy_buffer(
		accelerator, (size_t)height * sizeof(uint16_t), mapper->y_weight, &rc);
	if (accelerator->y_weight_buffer == NULL || rc != CL_SUCCESS) goto surface_error;

	accelerator->host_frame = nv12;
	accelerator->width = width;
	accelerator->height = height;
	accelerator->pixel_count = pixel_count;
	accelerator->apply_global_work_size =
		(pixel_count + accelerator->apply_local_work_size - 1) /
		accelerator->apply_local_work_size * accelerator->apply_local_work_size;
	accelerator->uploaded_statistics_updates = UINT64_MAX;
	pixels = (cl_uint)pixel_count;
	width_arg = (cl_uint)width;
	height_arg = (cl_uint)height;
	rc = clSetKernelArg(accelerator->apply_kernel, 0, sizeof(accelerator->frame_buffer), &accelerator->frame_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->apply_kernel, 1, sizeof(accelerator->spatial_buffer), &accelerator->spatial_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->apply_kernel, 2, sizeof(accelerator->curve_buffer), &accelerator->curve_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->apply_kernel, 3, sizeof(accelerator->global_curve_buffer), &accelerator->global_curve_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->apply_kernel, 4, sizeof(pixels), &pixels);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->spatial_kernel, 0, sizeof(accelerator->spatial_buffer), &accelerator->spatial_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->spatial_kernel, 1, sizeof(accelerator->tile_buffer), &accelerator->tile_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->spatial_kernel, 2, sizeof(accelerator->x_left_buffer), &accelerator->x_left_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->spatial_kernel, 3, sizeof(accelerator->x_right_buffer), &accelerator->x_right_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->spatial_kernel, 4, sizeof(accelerator->x_weight_buffer), &accelerator->x_weight_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->spatial_kernel, 5, sizeof(accelerator->y_top_buffer), &accelerator->y_top_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->spatial_kernel, 6, sizeof(accelerator->y_bottom_buffer), &accelerator->y_bottom_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->spatial_kernel, 7, sizeof(accelerator->y_weight_buffer), &accelerator->y_weight_buffer);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->spatial_kernel, 8, sizeof(width_arg), &width_arg);
	if (rc == CL_SUCCESS) rc = clSetKernelArg(accelerator->spatial_kernel, 9, sizeof(height_arg), &height_arg);
	if (rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clSetKernelArg(surface)", rc);
		video_broker_tone_accelerator_release_surface(accelerator);
		return -1;
	}
	return 0;

surface_error:
	video_broker_tone_accelerator_set_error(accelerator, "clCreateBuffer(surface)", rc);
	video_broker_tone_accelerator_release_surface(accelerator);
	return -1;
}

static inline int video_broker_tone_accelerator_runtime_fallback(
	video_broker_tone_accelerator_t *accelerator,
	video_broker_nv12_tone_mapper_t *mapper,
	unsigned char *nv12,
	int width,
	int height
) {
	char retained_error[sizeof(accelerator->error)];
	if (accelerator->requested != VIDEO_BROKER_TONE_ACCELERATOR_AUTO) {
		return -1;
	}
	(void)snprintf(retained_error, sizeof(retained_error), "%s", accelerator->error);
	if (video_broker_tone_accelerator_destroy(accelerator) != 0) return -1;
	accelerator->requested = VIDEO_BROKER_TONE_ACCELERATOR_AUTO;
	accelerator->selected = VIDEO_BROKER_TONE_ACCELERATOR_CPU;
	accelerator->initialized = true;
	(void)snprintf(
		accelerator->error,
		sizeof(accelerator->error),
		"runtime OpenCL fallback: %.150s",
		retained_error);
	return video_broker_nv12_tone_mapper_apply(mapper, nv12, width, height) ? 1 : 0;
}

static inline int video_broker_tone_accelerator_apply(
	video_broker_tone_accelerator_t *accelerator,
	video_broker_nv12_tone_mapper_t *mapper,
	unsigned char *nv12,
	int width,
	int height
) {
	cl_int rc;
	cl_uint local_mode;
	cl_uint columns_arg;
	if (accelerator == NULL || !accelerator->initialized || mapper == NULL ||
	    video_broker_tone_accelerator_resources_quarantined()) {
		return -1;
	}
	if (!video_broker_nv12_tone_mapper_active(mapper) || nv12 == NULL || width <= 0 || height <= 0) {
		return 0;
	}
	if (accelerator->selected != VIDEO_BROKER_TONE_ACCELERATOR_OPENCL) {
		return video_broker_nv12_tone_mapper_apply(mapper, nv12, width, height) ? 1 : 0;
	}
	if (video_broker_tone_accelerator_ensure_surface(accelerator, mapper, nv12, width, height) != 0) {
		return video_broker_tone_accelerator_runtime_fallback(
			accelerator, mapper, nv12, width, height);
	}
	local_mode = mapper->active_mode == VIDEO_BROKER_TONE_LOCAL &&
		mapper->columns > 1 && mapper->rows > 1 ? 1U : 0U;
	columns_arg = (cl_uint)mapper->columns;
	rc = clSetKernelArg(accelerator->apply_kernel, 5, sizeof(local_mode), &local_mode);
	if (rc == CL_SUCCESS) {
		rc = clSetKernelArg(accelerator->spatial_kernel, 10, sizeof(columns_arg), &columns_arg);
	}
	if (rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "clSetKernelArg(dynamic)", rc);
		return video_broker_tone_accelerator_runtime_fallback(
			accelerator, mapper, nv12, width, height);
	}
	accelerator->commands_pending = true;
	if (accelerator->uploaded_statistics_updates != mapper->statistics_updates) {
		if (local_mode != 0U) {
			size_t spatial_global[2] = {(size_t)width, (size_t)height};
			rc = clEnqueueWriteBuffer(
				accelerator->queue, accelerator->tile_buffer, CL_FALSE, 0,
				(size_t)mapper->columns * (size_t)mapper->rows * sizeof(mapper->tile_gain_lut_index[0]),
				mapper->tile_gain_lut_index, 0, NULL, NULL);
			if (rc == CL_SUCCESS) {
				rc = clEnqueueNDRangeKernel(
					accelerator->queue, accelerator->spatial_kernel, 2, NULL,
					spatial_global, NULL, 0, NULL, NULL);
			}
			if (rc != CL_SUCCESS) {
				video_broker_tone_accelerator_set_error(accelerator, "OpenCL spatial update", rc);
				return video_broker_tone_accelerator_runtime_fallback(
					accelerator, mapper, nv12, width, height);
			}
		}
		rc = clEnqueueWriteBuffer(
			accelerator->queue, accelerator->global_curve_buffer, CL_FALSE, 0,
			sizeof(mapper->global_lut), mapper->global_lut, 0, NULL, NULL);
		if (rc != CL_SUCCESS) {
			video_broker_tone_accelerator_set_error(accelerator, "clEnqueueWriteBuffer(global curve)", rc);
			return video_broker_tone_accelerator_runtime_fallback(
				accelerator, mapper, nv12, width, height);
		}
		accelerator->uploaded_statistics_updates = mapper->statistics_updates;
	}
	rc = clEnqueueWriteBuffer(
		accelerator->queue, accelerator->frame_buffer, CL_FALSE, 0,
		accelerator->pixel_count, nv12, 0, NULL, NULL);
	if (rc == CL_SUCCESS) {
		rc = clEnqueueNDRangeKernel(
			accelerator->queue, accelerator->apply_kernel, 1, NULL,
			&accelerator->apply_global_work_size, &accelerator->apply_local_work_size,
			0, NULL, NULL);
	}
	if (rc == CL_SUCCESS) {
		rc = clEnqueueReadBuffer(
			accelerator->queue, accelerator->frame_buffer, CL_TRUE, 0,
			accelerator->pixel_count, nv12, 0, NULL, NULL);
	}
	if (rc != CL_SUCCESS) {
		video_broker_tone_accelerator_set_error(accelerator, "OpenCL tone apply", rc);
		return video_broker_tone_accelerator_runtime_fallback(
			accelerator, mapper, nv12, width, height);
	}
	accelerator->commands_pending = false;
	return 1;
}

#else

typedef struct {
	video_broker_tone_accelerator_kind_t requested;
	video_broker_tone_accelerator_kind_t selected;
	bool initialized;
	char error[192];
} video_broker_tone_accelerator_t;

static inline int video_broker_tone_accelerator_release_surface(
	video_broker_tone_accelerator_t *accelerator
) {
	(void)accelerator;
	return 0;
}

static inline int video_broker_tone_accelerator_init(
	video_broker_tone_accelerator_t *accelerator,
	video_broker_tone_accelerator_kind_t requested
) {
	if (accelerator == NULL) {
		return -1;
	}
	memset(accelerator, 0, sizeof(*accelerator));
	accelerator->requested = requested;
	accelerator->selected = VIDEO_BROKER_TONE_ACCELERATOR_CPU;
	accelerator->initialized = true;
	if (requested == VIDEO_BROKER_TONE_ACCELERATOR_OPENCL) {
		(void)snprintf(accelerator->error, sizeof(accelerator->error), "OpenCL backend not built");
		return -1;
	}
	return 0;
}

static inline int video_broker_tone_accelerator_destroy(
	video_broker_tone_accelerator_t *accelerator
) {
	if (accelerator != NULL) {
		memset(accelerator, 0, sizeof(*accelerator));
	}
	return 0;
}

static inline int video_broker_tone_accelerator_apply(
	video_broker_tone_accelerator_t *accelerator,
	video_broker_nv12_tone_mapper_t *mapper,
	unsigned char *nv12,
	int width,
	int height
) {
	if (accelerator == NULL || !accelerator->initialized) {
		return -1;
	}
	return video_broker_nv12_tone_mapper_apply(mapper, nv12, width, height) ? 1 : 0;
}

#endif

static inline const char *video_broker_tone_accelerator_error(
	const video_broker_tone_accelerator_t *accelerator
) {
	return accelerator != NULL && accelerator->error[0] != '\0' ? accelerator->error : "none";
}

#endif
