import socket
import struct


BROKER_MAGIC = b"NMB2"
BROKER_VERSION = 1
BROKER_HEADER_SIZE = 32
BROKER_STREAM_H264 = 1
BROKER_STREAM_NV12 = 2
BROKER_STREAM_H265 = 3
BROKER_HEADER_STRUCT = struct.Struct(">4sBBHIQIII")
DEFAULT_MAX_PAYLOAD_SIZE = 16 * 1024 * 1024


def recv_exact(sock: socket.socket, size: int) -> bytes | None:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            return None
        data.extend(chunk)
    return bytes(data)


def read_broker_packet(
    sock: socket.socket,
    *,
    max_payload_size: int = DEFAULT_MAX_PAYLOAD_SIZE,
    expected_stream_id: int | None = None,
    expected_width: int | None = None,
    expected_height: int | None = None,
    expected_payload_size: int | None = None,
) -> tuple[dict, bytes] | tuple[None, None]:
    header_buf = recv_exact(sock, BROKER_HEADER_SIZE)
    if header_buf is None:
        return None, None
    magic, version, stream_id, flags, frame_id, timestamp_ns, width, height, payload_size = BROKER_HEADER_STRUCT.unpack(
        header_buf
    )
    if magic != BROKER_MAGIC or version != BROKER_VERSION:
        raise RuntimeError("invalid broker packet header")
    max_payload_size = max(1, int(max_payload_size))
    if payload_size <= 0 or payload_size > max_payload_size:
        raise RuntimeError(f"invalid broker payload size: {payload_size}")
    if expected_stream_id is not None and stream_id != int(expected_stream_id):
        raise RuntimeError(f"unexpected broker stream: {stream_id}")
    if expected_width is not None and width != int(expected_width):
        raise RuntimeError(f"unexpected broker width: {width}")
    if expected_height is not None and height != int(expected_height):
        raise RuntimeError(f"unexpected broker height: {height}")
    if expected_payload_size is not None and payload_size != int(expected_payload_size):
        raise RuntimeError(f"unexpected broker payload size: {payload_size}")
    payload = recv_exact(sock, payload_size)
    if payload is None:
        return None, None
    return (
        {
            "stream_id": stream_id,
            "flags": flags,
            "frame_id": frame_id,
            "timestamp_ns": timestamp_ns,
            "width": width,
            "height": height,
            "payload_size": payload_size,
        },
        payload,
    )
