#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VIDEO_BROKER_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="${VIDEO_BROKER_OUTPUT_DIR:?set an absolute temporary output directory}"
MUX_BIN="${1:-$OUTPUT_DIR/rdkx5_hbn_mux_bridge}"
OUTPUT_DIR="$(dirname "$MUX_BIN")"
DECODER_BIN="${OUTPUT_DIR}/rdkx5_h26x_decode_bridge"
V4L2_MUX_BIN="${OUTPUT_DIR}/rdkx5_v4l2_mux_bridge"
DECODER_SRC="$SCRIPT_DIR/rdkx5_h26x_decode_bridge.c"
MUX_SRC="$SCRIPT_DIR/rdkx5_hbn_mux_bridge.c"
FANOUT_PROTOCOL_SRC="$SCRIPT_DIR/worker_fanout_protocol.c"
V4L2_MUX_SRC="$SCRIPT_DIR/rdkx5_v4l2_mux_bridge.c"
DECODER_RUNTIME_SRC="$SCRIPT_DIR/rdkx5_decoder_runtime.c"
PERSISTENT_ENCODER_SRC="$SCRIPT_DIR/rdkx5_persistent_encoder.c"
FRAME_SOURCE_SRC="$SCRIPT_DIR/frame_source_linux.c"
# Native frame sources ship in this same standalone module. The release build
# hashes all three source directories; no sibling checkout or module fetch.
FRAME_WIRE_DIR="$VIDEO_BROKER_ROOT/framewire"
FRAME_PEER_DIR="$VIDEO_BROKER_ROOT/framepeer"
MULTIMEDIA_LIB_DIR="${VIDEO_BROKER_RDKX5_MULTIMEDIA_LIB_DIR:-}"
RUNTIME_BINARIES=("$DECODER_BIN" "$MUX_BIN" "$V4L2_MUX_BIN")
OPENCL_CFLAGS=()
OPENCL_LIBS=()

log() {
    printf '[build_rdkx5_hbn_bridge] %s\n' "$*"
}

if [ -z "$MULTIMEDIA_LIB_DIR" ]; then
    for candidate in /usr/hobot/lib /usr/lib /lib; do
        if [ -e "$candidate/libmultimedia.so" ]; then
            MULTIMEDIA_LIB_DIR="$candidate"
            break
        fi
    done
fi
if [[ "$MULTIMEDIA_LIB_DIR" != /* ]] || [ ! -e "$MULTIMEDIA_LIB_DIR/libmultimedia.so" ]; then
    log "missing trusted libmultimedia.so (set VIDEO_BROKER_RDKX5_MULTIMEDIA_LIB_DIR to an absolute SDK path)"
    exit 1
fi
MULTIMEDIA_LIB_REAL="$(readlink -f -- "$MULTIMEDIA_LIB_DIR/libmultimedia.so")"
if [ ! -f "$MULTIMEDIA_LIB_REAL" ] || [ "$(stat -c '%u' "$MULTIMEDIA_LIB_REAL")" != 0 ] || \
   (( (8#$(stat -c '%a' "$MULTIMEDIA_LIB_REAL") & 0022) != 0 )); then
    log "unsafe multimedia SDK library: $MULTIMEDIA_LIB_REAL"
    exit 1
fi
if [ "$(stat -c '%u' "$MULTIMEDIA_LIB_DIR")" != 0 ] || \
   (( (8#$(stat -c '%a' "$MULTIMEDIA_LIB_DIR") & 0022) != 0 )); then
    log "unsafe multimedia SDK library directory: $MULTIMEDIA_LIB_DIR"
    exit 1
fi

for required in \
    "$DECODER_SRC" \
    "$MUX_SRC" \
    "$FANOUT_PROTOCOL_SRC" \
    "$V4L2_MUX_SRC" \
    "$DECODER_RUNTIME_SRC" \
    "$PERSISTENT_ENCODER_SRC" \
    "$FRAME_SOURCE_SRC" "$SCRIPT_DIR/source_publisher.c" \
    "$VIDEO_BROKER_ROOT/internal/framehash/frame_sha256_linux.h" \
    "$FRAME_WIRE_DIR/native/causal_envelope.c" \
    "$FRAME_WIRE_DIR/native/causal_chunks.c" \
    "$FRAME_WIRE_DIR/native/causal_snapshot.c" "$FRAME_WIRE_DIR/native/atomic_snapshot.c" \
    "$FRAME_WIRE_DIR/provider/linux/native/source_mailbox.c" \
    "$FRAME_PEER_DIR/provider/linux/native/launch.c" \
    /usr/include/openssl/sha.h \
    /usr/include/sp_vio.h \
    /usr/include/sp_codec.h \
    /usr/include/hb_media_codec.h \
    /usr/include/turbojpeg.h \
    /usr/lib/libspcdev.so \
    /lib/libhbspdev.so \
    "$MULTIMEDIA_LIB_DIR/libmultimedia.so"; do
    [ -e "$required" ] || { log "missing dependency: $required"; exit 1; }
done

if [ -f /usr/include/CL/cl.h ] && [ -e "$MULTIMEDIA_LIB_DIR/libOpenCL.so" ]; then
    OPENCL_CFLAGS=(-DVIDEO_BROKER_HAVE_OPENCL=1)
    OPENCL_LIBS=(-lOpenCL)
    log "tone accelerator backend=opencl"
else
    log "tone accelerator backend=cpu reason=opencl_sdk_unavailable"
fi

mkdir -p "$OUTPUT_DIR"

cc -O2 -Wall -Wextra -Werror -std=c11 \
    -D_FORTIFY_SOURCE=2 -fstack-protector-strong -Wl,-z,relro,-z,now \
    -Wl,--enable-new-dtags,-rpath,"$MULTIMEDIA_LIB_DIR" \
    -I/usr/include \
    "$DECODER_SRC" \
    -L/usr/lib -L"$MULTIMEDIA_LIB_DIR" \
    -lspcdev \
    -lhbspdev \
    -lmultimedia \
    -o "$DECODER_BIN"

cc -O2 -Wall -Wextra -Werror -std=c11 \
	"${OPENCL_CFLAGS[@]}" \
	-D_FORTIFY_SOURCE=2 -fstack-protector-strong -Wl,-z,relro,-z,now \
	-Wl,--enable-new-dtags,-rpath,"$MULTIMEDIA_LIB_DIR" \
	-I/usr/include \
	"$MUX_SRC" "$FANOUT_PROTOCOL_SRC" \
	-L/usr/lib -L"$MULTIMEDIA_LIB_DIR" \
	-lspcdev \
	-lhbspdev \
	"${OPENCL_LIBS[@]}" \
	-lm \
	-o "$MUX_BIN"

cc -O2 -Wall -Wextra -Werror -std=c11 \
    -pthread \
    "${OPENCL_CFLAGS[@]}" \
    -D_FORTIFY_SOURCE=2 -fstack-protector-strong -Wl,-z,relro,-z,now \
    -Wl,--enable-new-dtags,-rpath,"$MULTIMEDIA_LIB_DIR" \
    -I/usr/include \
    -I"$FRAME_WIRE_DIR/native" -I"$FRAME_PEER_DIR/native" -I"$FRAME_PEER_DIR/provider/linux/native" \
    "$V4L2_MUX_SRC" "$DECODER_RUNTIME_SRC" "$PERSISTENT_ENCODER_SRC" "$FANOUT_PROTOCOL_SRC" "$FRAME_SOURCE_SRC" "$SCRIPT_DIR/source_publisher.c" \
    "$FRAME_WIRE_DIR/native/causal_envelope.c" "$FRAME_WIRE_DIR/native/causal_chunks.c" "$FRAME_PEER_DIR/provider/linux/native/launch.c" \
    "$FRAME_WIRE_DIR/native/causal_snapshot.c" "$FRAME_WIRE_DIR/native/atomic_snapshot.c" "$FRAME_WIRE_DIR/provider/linux/native/source_mailbox.c" \
    -L/usr/lib -L"$MULTIMEDIA_LIB_DIR" \
    -lspcdev \
    -lhbspdev \
    -lturbojpeg \
    "${OPENCL_LIBS[@]}" \
    -lm \
    -lmultimedia \
    -lcrypto \
    -o "$V4L2_MUX_BIN"

if ! ldd "$DECODER_BIN" | grep -E 'libmultimedia\.so(\.[0-9]+)*[[:space:]]*=>' | grep -Fvq 'not found'; then
    log "decoder bridge cannot resolve libmultimedia.so at runtime"
    exit 1
fi

for binary in "${RUNTIME_BINARIES[@]}"; do
    linkage="$(LC_ALL=C ldd -r "$binary" 2>&1)" || {
        printf '%s\n' "$linkage" >&2
        log "runtime linkage check failed: $binary"
        exit 1
    }
    if grep -Eq 'not found|undefined symbol' <<<"$linkage"; then
        printf '%s\n' "$linkage" >&2
        log "unresolved runtime dependency: $binary"
        exit 1
    fi
done

log "built decoder bridge at $DECODER_BIN"
log "built mux bridge at $MUX_BIN"
log "built v4l2 mux bridge at $V4L2_MUX_BIN"
