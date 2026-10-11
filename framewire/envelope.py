"""Strict portable VBCF candidate. Integrity is not authentication."""
from dataclasses import dataclass
import hashlib
import struct

HEADER_SIZE = 448
MAX_PAYLOAD = 16 * 1024 * 1024


class Invalid(ValueError):
    pass


@dataclass(frozen=True)
class Header:
    kind: int
    codec: int
    surface: int
    correlation: int
    flags: int
    payload_bytes: int
    width: int
    height: int
    stride_y: int
    stride_uv: int
    source_epoch: int
    origin_sequence: int
    publication_epoch: int
    delivery_id: int
    capture_ns: int
    media_pts: int
    namespace: bytes
    graph: bytes
    transform: bytes
    payload_sha: bytes
    source: str
    stream: str
    process: str
    output_sequence: int = 0


def _token(value, empty=False):
    return (type(value) is str and (bool(value) or empty) and len(value) <= 64
            and all(c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for c in value))


def validate(h):
    for name in ("kind", "codec", "surface", "correlation"):
        if type(getattr(h, name)) is not int or not 0 <= getattr(h, name) <= 255:
            raise Invalid("byte enum")
    for name in ("flags", "payload_bytes", "width", "height", "stride_y", "stride_uv"):
        if type(getattr(h, name)) is not int or not 0 <= getattr(h, name) < 2**32:
            raise Invalid("u32")
    for name in ("source_epoch", "origin_sequence", "publication_epoch", "delivery_id", "capture_ns", "output_sequence"):
        if type(getattr(h, name)) is not int or not 0 <= getattr(h, name) < 2**64:
            raise Invalid("u64")
    if type(h.media_pts) is not int or not -(2**63) <= h.media_pts < 2**63:
        raise Invalid("media time")
    if (h.kind == 5) != (h.output_sequence != 0):
        raise Invalid("source output sequence")
    for name, size in (("namespace", 16), ("graph", 32), ("transform", 32), ("payload_sha", 32)):
        if type(getattr(h, name)) is not bytes or len(getattr(h, name)) != size:
            raise Invalid("digest shape")
    if (not _token(h.source) or not _token(h.process) or not _token(h.stream, True)
            or not any(h.namespace) or not any(h.graph) or not h.source_epoch
            or h.payload_bytes > MAX_PAYLOAD or h.flags & ~31 or h.correlation not in (0, 1)):
        raise Invalid("domain or flags")
    if h.kind == 3:
        if (any((h.codec, h.surface, h.correlation, h.flags, h.payload_bytes, h.width,
                 h.height, h.stride_y, h.stride_uv, h.origin_sequence, h.delivery_id,
                 h.capture_ns, h.media_pts)) or any(h.transform)
                or not h.publication_epoch or not _token(h.stream)
                or h.payload_sha != hashlib.sha256(b"").digest()):
            raise Invalid("epoch control")
        return
    if h.kind == 4:
        expected = h.stride_y*h.height + h.stride_uv*(h.height//2)
        if (h.codec != 3 or h.surface not in (1, 2)
                or any((h.correlation, h.flags, h.payload_bytes, h.origin_sequence,
                        h.publication_epoch, h.delivery_id, h.capture_ns, h.media_pts))
                or h.stream or h.payload_sha != hashlib.sha256(b"").digest()
                or not any(h.transform) or not 0 < h.width <= 16384 or not 0 < h.height <= 16384
                or (h.width | h.height | h.stride_y | h.stride_uv) & 1
                or h.stride_y < h.width or h.stride_uv < h.width or expected > MAX_PAYLOAD):
            raise Invalid("source definition")
        return
    if (h.kind not in (1, 2, 5) or not 0 < h.width <= 16384 or not 0 < h.height <= 16384
            or not h.payload_bytes or not any(h.transform)
            or (h.correlation == 1) != (h.origin_sequence != 0)):
        raise Invalid("media identity")
    if ((not h.flags & 1 and h.capture_ns != 0)
            or (h.flags & 1 and (not h.capture_ns or not h.correlation))
            or (not h.flags & 2 and h.media_pts != 0)):
        raise Invalid("clock presence")
    if h.kind == 1:
        if (h.stream or h.publication_epoch or h.delivery_id or h.correlation != 1
                or h.codec != 3 or h.surface not in (1, 2)):
            raise Invalid("retained surface")
    elif h.kind == 5:
        if not _token(h.stream) or h.publication_epoch or h.delivery_id or h.surface != 3:
            raise Invalid("source output is not a broker delivery")
    elif not _token(h.stream) or not h.publication_epoch or not h.delivery_id or h.surface != 3:
        raise Invalid("delivery")
    if h.codec == 3:
        expected = h.stride_y * h.height + h.stride_uv * (h.height // 2)
        if ((h.width | h.height | h.stride_y | h.stride_uv) & 1
                or h.stride_y < h.width or h.stride_uv < h.width
                or h.flags & 28 or expected != h.payload_bytes):
            raise Invalid("NV12 layout")
    elif h.codec not in (1, 2) or h.stride_y or h.stride_uv:
        raise Invalid("encoded layout")


def marshal_header(h):
    validate(h)
    b = bytearray(HEADER_SIZE)
    struct.pack_into(">4sHH4B6I", b, 0, b"VBCF", 1, HEADER_SIZE, h.kind, h.codec,
                     h.surface, h.correlation, h.flags, h.payload_bytes,
                     h.width, h.height, h.stride_y, h.stride_uv)
    struct.pack_into(">5Qq", b, 40, h.source_epoch, h.origin_sequence,
                     h.publication_epoch, h.delivery_id, h.capture_ns, h.media_pts)
    b[88:104], b[104:136], b[136:168], b[168:200] = h.namespace, h.graph, h.transform, h.payload_sha
    for offset, value in ((200, h.source), (264, h.stream), (328, h.process)):
        b[offset:offset + len(value)] = value.encode("ascii")
    struct.pack_into(">Q", b, 392, h.output_sequence)
    return bytes(b)


def _text(b):
    raw, separator, tail = b.partition(b"\0")
    if separator and any(tail):
        raise Invalid("noncanonical padding")
    try:
        value = raw.decode("ascii")
    except UnicodeError as exc:
        raise Invalid("non-ASCII token") from exc
    if not _token(value, True):
        raise Invalid("token")
    return value


def parse_header(b):
    if type(b) is not bytes or len(b) != HEADER_SIZE:
        raise Invalid("header length/type")
    magic, version, size, *fields = struct.unpack_from(">4sHH4B6I", b)
    if magic != b"VBCF" or version != 1 or size != HEADER_SIZE or any(b[36:40]) or any(b[400:]):
        raise Invalid("header version/reserved")
    h = Header(*fields, *struct.unpack_from(">5Qq", b, 40), b[88:104], b[104:136],
               b[136:168], b[168:200], _text(b[200:264]), _text(b[264:328]), _text(b[328:392]),
               struct.unpack_from(">Q", b, 392)[0])
    validate(h)
    return h


def marshal(h, payload):
    from dataclasses import replace
    if type(payload) is not bytes or len(payload) > MAX_PAYLOAD:
        raise Invalid("payload size/type")
    h = replace(h, payload_bytes=len(payload), payload_sha=hashlib.sha256(payload).digest())
    return marshal_header(h) + payload


def parse(b):
    if type(b) is not bytes or len(b) < HEADER_SIZE:
        raise Invalid("packet length/type")
    h = parse_header(b[:HEADER_SIZE])
    if len(b) != HEADER_SIZE + h.payload_bytes:
        raise Invalid("packet length")
    payload = b[HEADER_SIZE:]
    if hashlib.sha256(payload).digest() != h.payload_sha:
        raise Invalid("payload digest")
    return h, payload
