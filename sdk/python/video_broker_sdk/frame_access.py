"""Exact native ROI access over an already authenticated, admitted channel.

The launcher owns peer authentication, grants and resource admission. Hashes in
this protocol prove byte consistency, not the identity of a peer. No socket is
opened, grant inferred or timestamp matched by this module.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import logging
import math
import secrets
import struct
import threading
import time
from typing import Protocol

from .causal_envelope import Header, Invalid, MAX_PAYLOAD, validate
from .frame_data import DeliveryProgress, PayloadProgress

_LOG = logging.getLogger(__name__)


class PacketChannel(Protocol):
    def current(self) -> None: ...
    def send(self, packet: bytes, deadline: float) -> None: ...
    def receive(self, maximum: int, deadline: float) -> bytes: ...
    def read_delivery(self, request: bytes, pixels: bytearray, deadline: float,
                      progress: DeliveryProgress) -> tuple[bytes, bytes, bytes, int]:
        """One synchronous FAPN/FAPB/VBCF/FAPD transaction within the SAME
        deadline and authenticated binding. Fill only the admitted exclusive
        output. Return initial metadata or empty bytes, raw header/idle,
        independently computed payload SHA and payload size. DeliveryReader
        must validate descriptors before exposing bytes. No background queue.
        """
        ...
    def receive_payload(self, pixels: bytearray, digest: bytes, deadline: float,
                        progress: PayloadProgress) -> bytes:
        """Fill the exclusive admitted output; validate every record's sender,
        descriptor digest, offset and extent within the SAME total deadline.
        Return exactly 32 SHA-256 bytes calculated over the complete exclusive
        output, not copied from any record or proof. The SDK compares them to
        its trusted header. Backend admission belongs to this trusted provider;
        progress counters are diagnostics, never an integrity substitute.
        No result or retained input may escape failure or the total deadline.
        """
        ...
    def close(self) -> None: ...


class Unavailable(Exception):
    def __init__(self, state: str):
        self.state = state
        super().__init__(state)


_STATES = (
    "", "complete", "expired", "epoch_mismatch", "source_mismatch",
    "graph_mismatch", "transform_mismatch", "correlation_unavailable",
    "region_invalid", "capacity_rejected", "deadline_exceeded", "canceled",
    "provider_unavailable", "invalid_contract",
)


def _text(value: str) -> bytes:
    if (type(value) is not str or not 1 <= len(value) <= 64 or
            any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for c in value)):
        raise Invalid("frame access token")
    return bytes((len(value),)) + value.encode("ascii")


def _n(value: int) -> bytes:
    return struct.pack(">Q", value)


def _s(value: str) -> bytes:
    raw = value.encode("ascii")
    return _n(len(raw)) + raw


@dataclass(frozen=True)
class Region:
    name: str
    bbox: tuple[float, float, float, float]
    padding: float = 0.0

    def encoded(self) -> bytes:
        values = (*self.bbox, self.padding)
        if type(self.bbox) is not tuple or len(self.bbox) != 4 or any(
            type(v) not in (int, float) or not math.isfinite(v) or abs(v) > 1 << 24
            or v == 0 and math.copysign(1.0, v) < 0 for v in values
        ):
            raise Invalid("frame access geometry")
        if not 0 <= self.padding <= 1 or self.bbox[2] <= self.bbox[0] or self.bbox[3] <= self.bbox[1]:
            raise Invalid("frame access region")
        return _text(self.name) + b"\0" + struct.pack(">5d", *values)

    def delivery_bbox(self, width: int, height: int) -> tuple[int, int, int, int]:
        """Padded observation rectangle in delivery coordinates.

        This is metadata for consumers, not the native ROI rectangle: the
        manifest and admitted transform determine the latter independently.
        """
        self.encoded()
        if (type(width) is not int or type(height) is not int or
                not 2 <= width <= 8192 or not 2 <= height <= 8192 or
                width & 1 or height & 1):
            raise Invalid("frame access delivery dimensions")
        x1, y1, x2, y2 = map(float, self.bbox)
        pad_x = max(1.0, x2 - x1) * self.padding
        pad_y = max(1.0, y2 - y1) * self.padding
        return (
            max(0, min(width, int(round(x1 - pad_x)))),
            max(0, min(height, int(round(y1 - pad_y)))),
            max(0, min(width, int(round(x2 + pad_x)))),
            max(0, min(height, int(round(y2 + pad_y)))),
        )


@dataclass(frozen=True)
class FrameEvidence:
    """Trusted publication metadata, never populated from a returned proof.

    Transform order is Full(w,h,stride), Crop(x1,y1,x2,y2), rotation, mirror,
    resized(w,h), delivery(w,h), pad(left,top), matching the broker contract.
    """
    transform: tuple[int, ...]
    provider: str
    copy_evidence: bytes

    def validate(self, header: Header) -> None:
        t = self.transform
        if type(t) is not tuple or len(t) != 15 or any(type(v) is not int for v in t):
            raise Invalid("frame access transform shape")
        w, h, stride, x1, y1, x2, y2, rotation, mirror, rw, rh, dw, dh, px, py = t
        if (any(v < 2 or v > 8192 or v & 1 for v in (w, h, rw, rh, dw, dh))
                or not w <= stride <= 16384 or stride & 1
                or not 0 <= x1 < x2 <= w or not 0 <= y1 < y2 <= h
                or any(v & 1 for v in (x1, y1, x2, y2, px, py))
                or rotation not in (0, 90, 180, 270) or mirror not in (0, 1)
                or not 0 <= px <= dw-rw or not 0 <= py <= dh-rh
                or (dw, dh) != (header.width, header.height)):
            raise Invalid("frame access transform geometry")
        digest = hashlib.sha256(_s("video-broker.frame-transform.v1") + b"".join(_n(v) for v in t)).digest()
        if digest != header.transform:
            raise Invalid("frame access transform differs from delivery")
        _text(self.provider)
        if type(self.copy_evidence) is not bytes or len(self.copy_evidence) != 32 or not any(self.copy_evidence):
            raise Invalid("frame access copy admission")

    def map_region(self, region: Region) -> tuple[int, int, int, int]:
        region.encoded()
        w, h, _, cx1, cy1, cx2, cy2, rotation, mirror, rw, rh, _, _, left, top = self.transform
        x1, y1, x2, y2 = map(float, region.bbox)
        px, py = (x2-x1)*region.padding, (y2-y1)*region.padding
        x1, x2 = max(x1-px, left), min(x2+px, left+rw)
        y1, y2 = max(y1-py, top), min(y2+py, top+rh)
        if x2 <= x1 or y2 <= y1:
            raise Unavailable("region_invalid")
        cw, ch = float(cx2-cx1), float(cy2-cy1)
        ow, oh = (ch, cw) if rotation in (90, 270) else (cw, ch)
        points = []
        for x, y in ((x1, y1), (x1, y2), (x2, y1), (x2, y2)):
            u, v = (x-left)*ow/rw, (y-top)*oh/rh
            if mirror:
                u = ow-u
            sx, sy = {0: (u, v), 90: (v, ch-u), 180: (cw-u, ch-v), 270: (cw-v, u)}[rotation]
            points.append((sx+cx1, sy+cy1))
        xs, ys = zip(*points)
        return (max(0, math.floor(min(xs)/2)*2), max(0, math.floor(min(ys)/2)*2),
                min(w, math.ceil(max(xs)/2)*2), min(h, math.ceil(max(ys)/2)*2))


def encode_request(header: Header, surface: str, region: Region,
                   request_id: bytes, budget_ns: int) -> bytes:
    validate(header)
    if header.kind != 2 or header.codec != 3:
        raise Invalid("frame access requires an NV12 delivery")
    if surface not in ("source_decoded", "processed_full"):
        raise Invalid("frame access source surface")
    if not header.correlation:
        raise Unavailable("correlation_unavailable")
    if type(request_id) is not bytes or len(request_id) != 16 or not any(request_id):
        raise Invalid("frame access request identity")
    if type(budget_ns) is not int or not 0 < budget_ns <= 5_000_000_000:
        raise Invalid("frame access duration")
    body = (request_id + header.namespace + _text(header.process) + _text(header.stream)
            + _n(header.publication_epoch) + _n(header.delivery_id) + header.graph
            + _text(header.source) + _text(surface) + _n(budget_ns) + b"\1"
            + region.encoded())
    return b"FAPQ\1\0" + struct.pack(">H", 8 + len(body)) + body


class _Reader:
    def __init__(self, packet: bytes):
        self.packet, self.offset = packet, 8

    def take(self, count: int) -> bytes:
        end = self.offset + count
        if end > len(self.packet):
            raise Invalid("frame access truncated manifest")
        result = self.packet[self.offset:end]
        self.offset = end
        return result

    def number(self) -> int:
        return struct.unpack(">Q", self.take(8))[0]

    def text(self) -> str:
        try:
            value = self.take(self.take(1)[0]).decode("ascii")
        except UnicodeError as exc:
            raise Invalid("frame access manifest token") from exc
        _text(value)
        return value


@dataclass(frozen=True)
class RegionManifest:
    packet: bytes
    request_id: bytes
    region: str
    size: int
    bbox: tuple[int, int, int, int]
    width: int
    height: int
    stride: int
    payload_sha: bytes
    domain_sha: bytes

    @classmethod
    def parse(cls, packet: bytes, *, header: Header, surface: str,
              region: Region, request_id: bytes, maximum: int,
              expected: FrameEvidence) -> RegionManifest:
        expected.validate(header)
        if (type(packet) is not bytes or not 8 <= len(packet) <= 2048
                or packet[:6] != b"FAPM\1\0"
                or struct.unpack_from(">H", packet, 6)[0] != len(packet)):
            raise Invalid("frame access manifest framing")
        r = _Reader(packet)
        identity, namespace, process = r.take(16), r.take(16), r.text()
        index, state = r.take(1)[0], r.take(1)[0]
        name, size = r.text(), r.number()
        if (identity != request_id or namespace != header.namespace or process != header.process
                or index != 0 or name != region.name or not 1 <= state < len(_STATES)):
            raise Invalid("frame access response identity")
        if state != 1:
            if size != 0 or r.offset != len(packet):
                raise Invalid("frame access negative response carries data")
            raise Unavailable(_STATES[state])
        stream, epoch, frame_id, graph = r.text(), r.number(), r.number(), r.take(32)
        source, capture, sequence = r.text(), r.number(), r.number()
        actual_surface, transform = r.text(), r.take(32)
        geometry = tuple(r.number() for _ in range(7))
        payload_sha, provider = r.take(32), r.text()
        evidence, core_sha, domain_sha = r.take(32), r.take(32), r.take(32)
        if (r.offset != len(packet) or not header.correlation
                or (stream, epoch, frame_id, graph, source, capture, sequence, actual_surface, transform) !=
                (header.stream, header.publication_epoch, header.delivery_id, header.graph,
                 header.source, header.source_epoch, header.origin_sequence, surface, header.transform)):
            raise Invalid("frame access response differs from exact delivery")
        x1, y1, x2, y2, width, height, stride = geometry
        if (not 0 <= x1 < x2 <= 8192 or not 0 <= y1 < y2 <= 8192
                or width != x2-x1 or height != y2-y1 or stride != width
                or any(v & 1 for v in geometry) or not any(evidence)
                or size != stride*height*3//2 or not 0 < size <= min(maximum, MAX_PAYLOAD)):
            raise Invalid("frame access result geometry or capacity")
        expected_region = expected.map_region(region)
        mismatch = tuple(name for name, differs in (
            ("geometry", geometry[:4] != expected_region),
            ("provider", provider != expected.provider),
            ("copy_evidence", evidence != expected.copy_evidence),
        ) if differs)
        if mismatch:
            # Bounded schema labels and numeric geometry only; no pixel data or
            # untrusted provider text. Keep the exact admission rejection.
            raise Invalid("frame access result differs from admitted transform or copy provider"
                          f" fields={','.join(mismatch)} actual_region={geometry[:4]}"
                          f" expected_region={expected_region}")
        core = (_s("video-broker.frame-proof.v1") + _n(1) + identity + _s(stream)
                + _n(epoch) + _n(frame_id) + graph + _s(source) + _n(capture)
                + _n(sequence) + graph + _s(surface) + transform + _s(name)
                + b"".join(_n(v) for v in geometry) + payload_sha + _s(provider) + evidence)
        domain = (b"video-broker.domain-frame-proof.v1\0" + namespace
                  + struct.pack(">H", len(process)) + process.encode("ascii") + core_sha)
        if hashlib.sha256(core).digest() != core_sha or hashlib.sha256(domain).digest() != domain_sha:
            raise Invalid("frame access proof digest")
        return cls(packet, identity, name, size, geometry[:4], width, height, stride, payload_sha, domain_sha)


@dataclass(frozen=True)
class NativeRegion:
    header: Header
    surface: str
    request: Region
    manifest: RegionManifest
    pixels: bytes
    evidence: FrameEvidence

    def provenance(self) -> dict:
        # uint64 identities are serialized losslessly through JSON IPC/storage.
        from .causal_envelope import marshal_header
        return {"schema": "video-broker.roi.v1", "delivery": marshal_header(self.header).hex(),
                "surface": self.surface, "region": self.request.name,
                "requested_bbox": list(self.request.bbox), "padding": self.request.padding,
                "manifest": self.manifest.packet.hex(), "transform": list(self.evidence.transform),
                "provider": self.evidence.provider, "copy_evidence": self.evidence.copy_evidence.hex()}


class FrameAccessClient:
    """One outstanding ROI, bounded private pixels and one total deadline.

    An admitted PacketChannel must revalidate its pinned peer in current().
    A partial, malformed or uncertain exchange retires the channel; explicit
    complete negative replies do not. The client never retries another frame.
    Admission must reserve at least 2*maximum+2*16384+4096 private bytes per
    client (assembly plus immutable return at peak); encoded-image work belongs
    to the caller's separate budget. The channel's declared integrity/kernel
    workspace is additional and must be admitted for each concurrent receive.
    """
    def __init__(self, channel: PacketChannel, *, maximum: int, budget_ns: int):
        if type(maximum) is not int or not 0 < maximum <= MAX_PAYLOAD:
            raise Invalid("frame access client capacity")
        if type(budget_ns) is not int or not 0 < budget_ns <= 5_000_000_000:
            raise Invalid("frame access client duration")
        self.channel, self.maximum, self.budget_ns = channel, maximum, budget_ns
        self._lock, self._closed = threading.Lock(), False
        self._retirement_reason = ""

    def check_available(self) -> None:
        """Cheap local lifecycle check; never reopens or probes a retired grant.

        Delivery and ROI have separate transports but one admitted binding.
        The owner must replace that binding if either transport is retired.
        Keep only bounded text, never the exception/traceback and its pixels.
        """
        if self._closed:
            raise Unavailable("provider_unavailable") from RuntimeError(self._retirement_reason)

    def crop(self, header: Header, surface: str, region: Region, expected: FrameEvidence) -> NativeRegion:
        expected.validate(header)
        request_id = secrets.token_bytes(16)
        request = encode_request(header, surface, region, request_id, self.budget_ns)
        if not self._lock.acquire(blocking=False):
            raise Unavailable("capacity_rejected")
        negative_complete = False
        try:
            self.check_available()
            deadline = time.monotonic() + self.budget_ns / 1_000_000_000
            self._current(deadline)
            self.channel.send(request, deadline)
            self._current(deadline)
            raw = self.channel.receive(2048, deadline)
            self._current(deadline)
            try:
                manifest = RegionManifest.parse(raw, header=header, surface=surface, region=region,
                                                request_id=request_id, maximum=self.maximum, expected=expected)
            except Unavailable:
                negative_complete = True
                raise
            pixels = bytearray(manifest.size)
            digest = hashlib.sha256(raw).digest()
            self._current(deadline)
            payload_progress = PayloadProgress()
            payload_sha = self.channel.receive_payload(pixels, digest, deadline, payload_progress)
            self._current(deadline)
            payload_progress.validate_complete(len(pixels))
            if type(payload_sha) is not bytes or len(payload_sha) != 32 or payload_sha != manifest.payload_sha:
                raise Invalid("frame access pixel digest")
            immutable = bytes(pixels)
            self._current(deadline)
            return NativeRegion(header, surface, region, manifest, immutable, expected)
        except BaseException as exc:
            if not negative_complete and not self._closed:
                self._retirement_reason = f"{type(exc).__name__}: {str(exc)}".replace("\n", " ")[:240]
                self._closed = True
                cleanup = "confirmed"
                try:
                    self.channel.close()
                except Exception as close_error:
                    # Never replace the original transport/integrity failure.
                    cleanup = f"failed:{type(close_error).__name__}"
                try:
                    _LOG.error("roi_binding_retired source=%s epoch=%d delivery=%d role=%s reason=%s cleanup=%s action=new_binding_required",
                               header.source, header.source_epoch, header.delivery_id,
                               region.name, self._retirement_reason, cleanup)
                except Exception:
                    pass
            raise
        finally:
            self._lock.release()

    def _current(self, deadline: float) -> None:
        if time.monotonic() >= deadline:
            raise TimeoutError("frame access total deadline exceeded")
        self.channel.current()
        # A successful peer check may itself complete late; neither a new
        # request nor an otherwise verified ROI may escape the total budget.
        if time.monotonic() >= deadline:
            raise TimeoutError("frame access total deadline exceeded")


@dataclass(frozen=True)
class NativeFrame:
    """Immutable delivery context; never owns a decoder or full source image."""
    header: Header
    surface: str
    client: FrameAccessClient
    evidence: FrameEvidence

    def __post_init__(self):
        validate(self.header)
        _text(self.surface)
        if self.header.kind != 2 or self.header.codec != 3:
            raise Invalid("native frame requires NV12 delivery")
        self.evidence.validate(self.header)

    def crop(self, name: str, bbox: tuple, padding: float) -> NativeRegion:
        return self.client.crop(self.header, self.surface, Region(name, tuple(bbox), padding), self.evidence)

    @classmethod
    def from_packet(cls, packet: bytes, *, surface: str, client: FrameAccessClient,
                    evidence: FrameEvidence) -> tuple[NativeFrame, bytes]:
        """Called only after stream peer/binding authentication and admission."""
        from .causal_envelope import parse
        header, pixels = parse(packet)
        return cls(header, surface, client, evidence), pixels


def verify_provenance(value: dict, *, maximum: int = MAX_PAYLOAD) -> tuple[Header, RegionManifest]:
    """Validate forwarded proof structurally. IPC authenticity is separate."""
    from .causal_envelope import parse_header
    if type(value) is not dict or set(value) != {
        "schema", "delivery", "surface", "region", "requested_bbox", "padding", "manifest",
        "transform", "provider", "copy_evidence"
    } or value["schema"] != "video-broker.roi.v1":
        raise Invalid("ROI provenance schema")
    try:
        if (type(value["delivery"]) is not str or len(value["delivery"]) != 896
                or type(value["manifest"]) is not str or len(value["manifest"]) > 4096):
            raise Invalid("ROI provenance extent")
        header, packet = parse_header(bytes.fromhex(value["delivery"])), bytes.fromhex(value["manifest"])
        region = Region(value["region"], tuple(value["requested_bbox"]), value["padding"])
        if (type(value["transform"]) is not list or len(value["transform"]) != 15
                or type(value["copy_evidence"]) is not str or len(value["copy_evidence"]) != 64):
            raise Invalid("ROI provenance evidence extent")
        expected = FrameEvidence(tuple(value["transform"]), value["provider"], bytes.fromhex(value["copy_evidence"]))
        region.encoded()
        encode_request(header, value["surface"], region, packet[8:24], 1)
        manifest = RegionManifest.parse(packet, header=header, surface=value["surface"],
                                        region=region, request_id=packet[8:24], maximum=maximum, expected=expected)
        return header, manifest
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise Invalid("ROI provenance invalid") from exc
