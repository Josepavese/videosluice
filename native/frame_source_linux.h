#ifndef VIDEO_BROKER_FRAME_SOURCE_LINUX_H
#define VIDEO_BROKER_FRAME_SOURCE_LINUX_H
/* Native Linux source provider. The broker, not this provider, admits the
 * source layout/native footprint and owns the process/channel lifetime. */
#include "launch.h"
#include "causal_envelope.h"
#include "capture_correlation.h"
#include "../framewire/provider/linux/native/source_mailbox.h"
#include "source_publisher.h"
#include <pthread.h>

typedef struct {
    int socket_fd,controller_fd;
    int mailbox_fd;
    uint64_t mailbox_generation;
    int output_mailbox_fd;
    uint64_t output_mailbox_generation,output_mailbox_capacity;
    const char *controller_metadata;
    const char *output_stream;
    unsigned seen;
} video_broker_frame_launch_args;
typedef struct {
    framepeer_linux_launch launch;
    vbsm_source_mailbox *mailbox;
    vbsm_source_mailbox *output_mailbox;
    vbsp_publisher *publisher;
    vbsp_publisher *output_publisher;
    size_t output_capacity;
    pthread_mutex_t authority_mutex;
    int authority_mutex_ready;
    uint64_t publisher_sequence;
    uint64_t output_publisher_sequence;
    vbcf_header definition;
    char output_stream[65];
    uint64_t sequence,published,unavailable;
    struct { char stream[65]; uint64_t sequence; } outputs[16];
    int initialized,enabled,geometry_verified,closed,failed,output_retired;
    int failure_code,attempt_timeout_ms;
    uint64_t attempt_started_ns,integrity_finished_ns,attempt_origin_sequence;
    uint64_t last_slow_output_ns;
    uint64_t publish_attempts;
    uint64_t publish_phase_wall_max_ns[3],publish_phase_cpu_at_wall_max_ns[3];
    /* Capture-thread observations only; detached workers never write these. */
    uint64_t output_phase_wall_max_ns[4],output_phase_cpu_at_wall_max_ns[4];
    size_t attempt_payload_bytes;
    const char *stage;
} video_broker_frame_source;

/* Returns1 recognized,0 unrelated,-1 duplicate/invalid. No secret arguments. */
int video_broker_frame_launch_arg(video_broker_frame_launch_args *,const char *,const char *);
int video_broker_frame_launch_validate(const video_broker_frame_launch_args *);
/* All-zero source, one open only. Partial failures still require close. An
 * absent capability means no FAP, not a fallback or an authenticated source. */
int video_broker_frame_source_open(video_broker_frame_source *,const video_broker_frame_launch_args *,const char *source,uint64_t source_epoch);
int video_broker_frame_source_geometry(video_broker_frame_source *,uint32_t width,uint32_t height,uint32_t stride,uint8_t surface);
uint64_t video_broker_frame_source_epoch(const video_broker_frame_source *);
/* Allocate BEFORE submitting a capture. Never use an output arrival counter.
 * Disabled returns an unavailable zero origin; enabled counters never wrap. */
int video_broker_frame_source_next(video_broker_frame_source *,vb_capture_origin *);
/* Caller has released driver leases and retains detached pixels through return.
 * 0 accepted/disabled;1 unavailable/replaced;negative failure. Two fixed private
 * RAM slots feed two shared slots; no reader ACK or per-frame allocation.
 * Accepted does not mean published; statistics expose completion/saturation. */
int video_broker_frame_source_publish(video_broker_frame_source *,vb_capture_origin,const uint8_t *,size_t,int timeout_ms);
int video_broker_frame_source_stats(video_broker_frame_source *,vbsp_stats *);
int video_broker_frame_source_output_stats(video_broker_frame_source *,vbsp_stats *);
typedef struct {
    const char *stream;
    uint8_t codec;
    uint32_t width,height,stride_y,stride_uv,flags;
    const uint8_t *transform; /* Digest of the actual applied geometry. */
} video_broker_frame_output_layout;
/* Publish native output bytes with the origin returned WITH those pixels.
 * Output counters belong to this source generation, not a replaceable role.
 * They are not public DeliveryRefs; only the broker can issue those. Unknown
 * origin remains explicitly unavailable. No additional allocation or ACK. */
int video_broker_frame_source_output(video_broker_frame_source *,
    const video_broker_frame_output_layout *,vb_capture_origin,
    const uint8_t *,size_t,int timeout_ms);
/* For pixels produced by the native ONE-pass oriented/aspect NV12 resizer. */
int video_broker_frame_source_nv12_output(video_broker_frame_source *,const char *,
    vb_capture_origin,const uint8_t *,size_t,int width,int height,
    int orientation,int aspect_policy,int timeout_ms);
int video_broker_frame_source_close(video_broker_frame_source *);
#endif
