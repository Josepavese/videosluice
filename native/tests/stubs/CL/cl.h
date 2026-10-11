#ifndef VIDEO_BROKER_TEST_OPENCL_DECLARATIONS_H
#define VIDEO_BROKER_TEST_OPENCL_DECLARATIONS_H

/* Test-only API declarations for deterministic calls into the real tone
 * owner. This is not an OpenCL implementation or a runtime fallback. */
#include <stddef.h>
#include <stdint.h>
typedef int32_t cl_int;
typedef uint32_t cl_uint;
typedef cl_uint cl_bool;
typedef uint64_t cl_device_type;
typedef uint64_t cl_mem_flags;
typedef uint64_t cl_command_queue_properties;
typedef intptr_t cl_context_properties;
typedef struct _cl_platform_id *cl_platform_id;
typedef struct _cl_device_id *cl_device_id;
typedef struct _cl_context *cl_context;
typedef struct _cl_command_queue *cl_command_queue;
typedef struct _cl_program *cl_program;
typedef struct _cl_kernel *cl_kernel;
typedef struct _cl_mem *cl_mem;
typedef struct _cl_event *cl_event;
#define CL_SUCCESS 0
#define CL_OUT_OF_RESOURCES -5
#define CL_INVALID_COMMAND_QUEUE -36
#define CL_TRUE 1
#define CL_FALSE 0
#define CL_DEVICE_TYPE_GPU (1ULL << 2)
#define CL_MEM_READ_WRITE (1ULL << 0)
#define CL_MEM_READ_ONLY (1ULL << 2)
#define CL_MEM_USE_HOST_PTR (1ULL << 3)
#define CL_MEM_COPY_HOST_PTR (1ULL << 5)
#define CL_KERNEL_WORK_GROUP_SIZE 0x11B0
cl_int clGetPlatformIDs(cl_uint, cl_platform_id *, cl_uint *);
cl_int clGetDeviceIDs(cl_platform_id, cl_device_type, cl_uint, cl_device_id *, cl_uint *);
cl_context clCreateContext(const cl_context_properties *, cl_uint, const cl_device_id *, void (*)(const char *, const void *, size_t, void *), void *, cl_int *);
cl_command_queue clCreateCommandQueue(cl_context, cl_device_id, cl_command_queue_properties, cl_int *);
cl_program clCreateProgramWithSource(cl_context, cl_uint, const char **, const size_t *, cl_int *);
cl_int clBuildProgram(cl_program, cl_uint, const cl_device_id *, const char *, void (*)(cl_program, void *), void *);
cl_kernel clCreateKernel(cl_program, const char *, cl_int *);
cl_mem clCreateBuffer(cl_context, cl_mem_flags, size_t, void *, cl_int *);
cl_int clGetKernelWorkGroupInfo(cl_kernel, cl_device_id, cl_uint, size_t, void *, size_t *);
cl_int clSetKernelArg(cl_kernel, cl_uint, size_t, const void *);
cl_int clEnqueueWriteBuffer(cl_command_queue, cl_mem, cl_bool, size_t, size_t, const void *, cl_uint, const cl_event *, cl_event *);
cl_int clEnqueueReadBuffer(cl_command_queue, cl_mem, cl_bool, size_t, size_t, void *, cl_uint, const cl_event *, cl_event *);
cl_int clEnqueueNDRangeKernel(cl_command_queue, cl_kernel, cl_uint, const size_t *, const size_t *, const size_t *, cl_uint, const cl_event *, cl_event *);
cl_int clFinish(cl_command_queue);
cl_int clReleaseMemObject(cl_mem);
cl_int clReleaseKernel(cl_kernel);
cl_int clReleaseProgram(cl_program);
cl_int clReleaseCommandQueue(cl_command_queue);
cl_int clReleaseContext(cl_context);
#endif
