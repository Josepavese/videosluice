#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VIDEO_BROKER_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="${VIDEO_BROKER_OUTPUT_DIR:?set an absolute temporary output directory}"
PEER_DIR="$VIDEO_BROKER_ROOT/framepeer"
mkdir -p "$OUTPUT_DIR"

# Linux transport and qualified payload integrity; no camera/media provider.
# All Linux SoCs share the ABI. Crypto workspace is a separate admission quote.
cc -std=c11 -O2 -Wall -Wextra -Werror -fPIC -shared \
    -D_FORTIFY_SOURCE=2 -fstack-protector-strong -Wl,-z,relro,-z,now \
    -I"$PEER_DIR/native" -I"$PEER_DIR/provider/linux/native" \
    "$SCRIPT_DIR/frame_peer_python.c" "$PEER_DIR/provider/linux/native/launch.c" \
    -o "$OUTPUT_DIR/libvideo_broker_frame_peer.so" -lcrypto -pthread
