#ifndef VIDEO_BROKER_PIPELINE_LATENCY_H
#define VIDEO_BROKER_PIPELINE_LATENCY_H
#include <stdint.h>

/* Allocation-free, adapter-independent phase observer. Caller supplies clocks
 * and static phase names. No change to deadlines, scheduling or ownership. */
typedef struct {
    const char *phase;
    uint64_t wall_us, cpu_us, last_report_us, suppressed;
} vb_pipeline_latency;
typedef struct {
    const char *phase;
    uint64_t wall_us, cpu_us, suppressed;
} vb_pipeline_delay;

static inline int vb_pipeline_checkpoint(vb_pipeline_latency *p,
        const char *next, uint64_t wall_us, uint64_t cpu_us, vb_pipeline_delay *out) {
    int report = 0;
    if (p->phase && wall_us >= p->wall_us && cpu_us >= p->cpu_us &&
            wall_us-p->wall_us >= 100000) {
        if (!p->last_report_us || (wall_us >= p->last_report_us &&
                wall_us-p->last_report_us >= 1000000)) {
            *out = (vb_pipeline_delay){p->phase, wall_us-p->wall_us,
                                      cpu_us-p->cpu_us, p->suppressed};
            p->last_report_us = wall_us;
            p->suppressed = 0;
            report = 1;
        } else {
            p->suppressed++;
        }
    }
    p->phase = next; p->wall_us = wall_us; p->cpu_us = cpu_us;
    return report;
}
#endif
