#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <stdio.h>
#include "rdkx5_decoder_runtime.h"

extern void decoder_runtime_set_input_delay_ms(int milliseconds);

int main(void) {
    video_broker_rdkx5_decoder_t decoder = {0};
    unsigned char pixels[12], access_unit[] = {'a'};
    vb_capture_origin origin;
    assert(video_broker_rdkx5_decoder_open(&decoder,
        VIDEO_BROKER_RDKX5_CODEC_H265, 4, 2, 0) == 0);
    decoder_runtime_set_input_delay_ms(150);
    assert(video_broker_rdkx5_decoder_decode(&decoder, access_unit, 1,
        pixels, sizeof(pixels), 0, (vb_capture_origin){0, 0}, &origin) >= 0);
    uint64_t first_report = decoder.input_latency.last_report_us;
    assert(first_report != 0);
    assert(video_broker_rdkx5_decoder_decode(&decoder, access_unit, 1,
        pixels, sizeof(pixels), 0, (vb_capture_origin){0, 0}, &origin) >= 0);
    assert(decoder.input_latency.last_report_us == first_report);
    assert(decoder.input_latency.suppressed == 1);
    decoder_runtime_set_input_delay_ms(0);
    assert(video_broker_rdkx5_decoder_close(&decoder) == 0);
    puts("decoder latency localization/rate limit PASS (synthetic SDK delay)");
    return 0;
}
