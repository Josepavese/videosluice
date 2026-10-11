#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VIDEO_BROKER_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="${VIDEO_BROKER_OUTPUT_DIR:?set an absolute temporary output directory}"
OUTPUT_PATH="${1:-$OUTPUT_DIR/libgstvideobrokertone.so}"
OPENCL_CFLAGS=()
OPENCL_LIBS=()

log() {
    printf '[build_gst_tone_plugin] %s\n' "$*"
}

for package in gstreamer-1.0 gstreamer-base-1.0 gstreamer-video-1.0; do
    pkg-config --exists "$package" || {
        log "missing build dependency: $package"
        exit 1
    }
done

if pkg-config --exists OpenCL; then
    read -r -a OPENCL_CFLAGS <<<"$(pkg-config --cflags OpenCL)"
    read -r -a OPENCL_LIBS <<<"$(pkg-config --libs OpenCL)"
    OPENCL_CFLAGS=(-DVIDEO_BROKER_HAVE_OPENCL=1 "${OPENCL_CFLAGS[@]}")
elif [ -f /usr/include/CL/cl.h ]; then
    for opencl_dir in /usr/hobot/lib /opt/hobot/lib /usr/lib/aarch64-linux-gnu /usr/lib /lib/aarch64-linux-gnu /lib; do
        if [ -e "$opencl_dir/libOpenCL.so" ]; then
            OPENCL_CFLAGS=(-DVIDEO_BROKER_HAVE_OPENCL=1)
            OPENCL_LIBS=(-Wl,--enable-new-dtags,-rpath,"$opencl_dir" -L"$opencl_dir" -lOpenCL)
            break
        fi
    done
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"
read -r -a GST_CFLAGS <<<"$(pkg-config --cflags gstreamer-1.0 gstreamer-base-1.0 gstreamer-video-1.0)"
read -r -a GST_LIBS <<<"$(pkg-config --libs gstreamer-1.0 gstreamer-base-1.0 gstreamer-video-1.0)"

cc -O2 -Wall -Wextra -Werror -std=c11 -fPIC -shared \
    -D_FORTIFY_SOURCE=2 -fstack-protector-strong -Wl,-z,relro,-z,now \
    "${OPENCL_CFLAGS[@]}" \
    "${GST_CFLAGS[@]}" \
    -I"$SCRIPT_DIR" \
    "$SCRIPT_DIR/gst_video_broker_tone.c" \
    "${GST_LIBS[@]}" \
    "${OPENCL_LIBS[@]}" \
    -lm \
    -o "$OUTPUT_PATH"

linkage="$(ldd -r "$OUTPUT_PATH" 2>&1)" || {
    printf '%s\n' "$linkage" >&2
    log "runtime linkage check failed"
    exit 1
}
if grep -Eq 'not found|undefined symbol' <<<"$linkage"; then
    printf '%s\n' "$linkage" >&2
    log "unresolved runtime dependency"
    exit 1
fi

log "built plugin at $OUTPUT_PATH opencl=$([ ${#OPENCL_LIBS[@]} -gt 0 ] && printf true || printf false)"
