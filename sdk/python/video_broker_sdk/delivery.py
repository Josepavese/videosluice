"""Latest-only NV12 delivery on an already admitted authenticated channel.

The stream channel is separate from ROI requests, so crop work cannot occupy
the analysis reader. One outstanding read, one complete immutable frame, no
prefetch queue, socket discovery, new grant or automatic cross-epoch reconnect.
Admission must reserve at least 2*maximum + 2*16384 + 4096 private bytes for this
reader, independently of native transport and detector/ROI allocations.
"""
from dataclasses import dataclass
import logging
import struct
import threading
import time

from .causal_envelope import Header, Invalid, MAX_PAYLOAD, parse_header
from .frame_access import FrameAccessClient, FrameEvidence, NativeFrame, PacketChannel, Unavailable, _text
from .frame_data import DeliveryProgress, PayloadProgress


_LOG = logging.getLogger(__name__)


@dataclass
class _ReadProgress:
    """Operation-local counters, never pixels or an untrusted exception message."""
    started: float
    cpu_started_ns: int
    budget_ns: int
    phase: str = "current"
    response_record: str = "unread"
    received_records: int = 0
    received_wire_bytes: int = 0
    validated_chunk_payload_bytes: int = 0
    declared_payload_bytes: int = 0
    integrity_verified: bool = False
    attempted_delivery_id: int = 0
    authority_checks: int = 0
    authority_us: int = 0
    authority_max_us: int = 0
    transaction_started: float | None = None
    transaction_finished: float | None = None
    header_validation_started: float | None = None
    header_validation_finished: float | None = None
    allocation_started: float | None = None
    allocation_finished: float | None = None
    allocation_cpu_started_ns: int | None = None
    allocation_cpu_finished_ns: int | None = None
    integrity_started: float | None = None
    integrity_finished: float | None = None
    native_send_us: int = 0
    native_descriptor_us: int = 0
    native_payload_receive_us: int = 0
    native_integrity_us: int = 0

    def failure(self, reader, error):
        now = time.monotonic()
        cpu_now_ns = time.thread_time_ns()
        def elapsed(start, end):
            return -1 if start is None else max(0, int(((now if end is None else end)-start)*1_000_000))
        allocation_cpu_us = -1 if self.allocation_cpu_started_ns is None else max(
            0, ((cpu_now_ns if self.allocation_cpu_finished_ns is None else self.allocation_cpu_finished_ns)
                - self.allocation_cpu_started_ns)//1000)
        publication = reader.publication
        _LOG.warning(
            "delivery_read_progress phase=%s response_record=%s source_epoch=%d publication_epoch=%d "
            "last_delivery_id=%d attempted_delivery_id=%d received_records=%d "
            "received_wire_bytes=%d validated_chunk_payload_bytes=%d declared_payload_bytes=%d "
            "integrity_verified=%s "
            "elapsed_us=%d budget_us=%d transaction_us=%d header_validation_us=%d "
            "allocation_us=%d allocation_thread_cpu_us=%d integrity_us=%d "
            "native_send_us=%d native_descriptor_us=%d native_payload_receive_us=%d native_integrity_us=%d "
            "authority_checks=%d authority_us=%d authority_max_us=%d "
            "thread_cpu_us=%d reason=%s",
            self.phase, self.response_record, publication.source_epoch if publication else 0,
            publication.publication_epoch if publication else 0,
            reader.last, self.attempted_delivery_id, self.received_records,
            self.received_wire_bytes, self.validated_chunk_payload_bytes, self.declared_payload_bytes,
            str(self.integrity_verified).lower(),
            elapsed(self.started, now), self.budget_ns//1000,
            elapsed(self.transaction_started, self.transaction_finished),
            elapsed(self.header_validation_started, self.header_validation_finished),
            elapsed(self.allocation_started, self.allocation_finished), allocation_cpu_us,
            elapsed(self.integrity_started, self.integrity_finished),
            self.native_send_us, self.native_descriptor_us,
            self.native_payload_receive_us, self.native_integrity_us,
            self.authority_checks, self.authority_us, self.authority_max_us,
            max(0, (cpu_now_ns-self.cpu_started_ns)//1000), type(error).__name__)


@dataclass(frozen=True)
class Publication:
    namespace: bytes
    source_epoch: int
    publication_epoch: int
    graph: bytes
    process: str
    source: str
    stream: str
    surface: str
    evidence: FrameEvidence

    @classmethod
    def parse(cls, packet: bytes):
        if (type(packet) is not bytes or not 174 <= len(packet) <= 512
                or packet[:6] != b"FAPB\1\0" or struct.unpack_from(">H", packet, 6)[0] != len(packet)):
            raise Invalid("publication metadata record")
        source_epoch, publication_epoch = struct.unpack_from(">2Q", packet, 24)
        if not any(packet[8:24]) or not source_epoch or not publication_epoch or not any(packet[40:72]):
            raise Invalid("publication metadata domain")
        offset, tokens = 164, []
        for _ in range(5):
            if offset >= len(packet):
                raise Invalid("publication metadata token length")
            size, offset = packet[offset], offset+1
            if not 1 <= size <= 64 or offset+size > len(packet):
                raise Invalid("publication metadata token extent")
            token = packet[offset:offset+size].decode("ascii", errors="strict")
            _text(token)
            tokens.append(token)
            offset += size
        if offset != len(packet):
            raise Invalid("publication metadata trailing bytes")
        evidence = FrameEvidence(struct.unpack_from(">15I", packet, 72), tokens[4], packet[132:164])
        # Full geometric verification follows against the first real header;
        # no fabricated delivery is needed to bootstrap the immutable metadata.
        return cls(packet[8:24], source_epoch, publication_epoch, packet[40:72], *tokens[:4], evidence)

    def validate(self, header: Header, maximum: int):
        if (header.kind != 2 or header.codec != 3 or header.namespace != self.namespace
                or header.process != self.process or header.source != self.source
                or header.source_epoch != self.source_epoch or header.graph != self.graph
                or header.stream != self.stream or header.publication_epoch != self.publication_epoch
                or header.stride_y != header.width or header.stride_uv != header.width
                or header.payload_bytes > maximum):
            raise Invalid("delivery differs from admitted publication")
        self.evidence.validate(header)


class DeliveryReader:
    def __init__(self, channel: PacketChannel, client: FrameAccessClient, *, maximum: int, budget_ns: int):
        if (channel is client.channel or type(maximum) is not int or not 0 < maximum <= MAX_PAYLOAD
                or type(budget_ns) is not int or not 0 < budget_ns <= 5_000_000_000):
            raise Invalid("delivery reader admission")
        self.channel, self.client, self.maximum, self.budget_ns = channel, client, maximum, budget_ns
        self.publication, self.last = None, 0
        self._lock, self._closed = threading.Lock(), False

    def current(self):
        if self._closed:
            raise Unavailable("provider_unavailable")
        self.client.check_available()
        self.channel.current()

    def _current(self, deadline, progress):
        started = time.monotonic()
        if started >= deadline:
            raise TimeoutError("delivery total deadline exceeded")
        try:
            self.current()
        finally:
            ended = time.monotonic()
            elapsed_us = max(0, int((ended-started)*1_000_000))
            progress.authority_checks += 1
            progress.authority_us += elapsed_us
            progress.authority_max_us = max(progress.authority_max_us, elapsed_us)
        # A native check can yield to the scheduler or another interpreter
        # thread. Its successful return does not extend the delivery deadline.
        # Native failures retain their original cause, even if also late.
        if ended >= deadline:
            raise TimeoutError("delivery total deadline exceeded")

    def read(self) -> tuple[NativeFrame, bytes] | None:
        if not self._lock.acquire(blocking=False):
            raise Unavailable("capacity_rejected")
        progress = None
        try:
            progress = _ReadProgress(time.monotonic(), time.thread_time_ns(), self.budget_ns)
            deadline = progress.started + self.budget_ns / 1_000_000_000
            self._current(deadline, progress)
            epoch = self.publication.publication_epoch if self.publication else 0
            request = b"FAPN\1\0\0\0" + struct.pack(">2Q", epoch, self.last)
            progress.phase, progress.allocation_started = "buffer_allocate", time.monotonic()
            progress.allocation_cpu_started_ns = time.thread_time_ns()
            # Admission, never an unvalidated descriptor, determines allocation.
            pixels = bytearray(self.maximum)
            progress.allocation_cpu_finished_ns = time.thread_time_ns()
            progress.allocation_finished = time.monotonic()
            if progress.allocation_finished >= deadline:
                raise TimeoutError("delivery total deadline exceeded")
            transaction = DeliveryProgress()
            progress.phase, progress.transaction_started = "frame_transaction", time.monotonic()
            try:
                metadata, packet, payload_sha, size = self.channel.read_delivery(
                    request, pixels, deadline, transaction)
            finally:
                progress.transaction_finished = time.monotonic()
                progress.received_records = (transaction.payload.received_records
                    + int(transaction.metadata_bytes > 0) + int(transaction.header_bytes > 0))
                progress.received_wire_bytes = (transaction.payload.received_wire_bytes
                    + transaction.metadata_bytes + transaction.header_bytes)
                progress.validated_chunk_payload_bytes = transaction.payload.validated_payload_bytes
                progress.declared_payload_bytes = transaction.declared_payload_bytes
                progress.native_send_us = transaction.native_send_us
                progress.native_descriptor_us = transaction.native_descriptor_us
                progress.native_payload_receive_us = transaction.native_payload_receive_us
                progress.native_integrity_us = transaction.native_integrity_us
                progress.response_record = ("frame_header" if transaction.declared_payload_bytes else
                    "idle_record" if transaction.header_bytes == 24 else "unread")
            self._current(deadline, progress)
            if self.publication is None:
                progress.phase = "metadata_validate"
                self.publication = Publication.parse(metadata)
            elif type(metadata) is not bytes or metadata:
                raise Invalid("unexpected publication metadata")
            expected_idle = b"FAPI\1\0\0\0" + struct.pack(">2Q", self.publication.publication_epoch, self.last)
            progress.response_record = (
                "idle_current" if type(packet) is bytes and packet == expected_idle else
                "idle_mismatch" if type(packet) is bytes and packet[:4] == b"FAPI" else
                "frame_header" if type(packet) is bytes and packet[:4] == b"VBCF" else "other")
            self._current(deadline, progress)
            if packet == expected_idle:
                if (type(size) is not int or size != 0 or type(payload_sha) is not bytes
                        or payload_sha != bytes(32) or transaction.payload != PayloadProgress()):
                    raise Invalid("idle carries frame payload")
                return None
            progress.phase, progress.header_validation_started = "header_validate", time.monotonic()
            header = parse_header(packet)
            self.publication.validate(header, self.maximum)
            progress.attempted_delivery_id = header.delivery_id
            progress.declared_payload_bytes = header.payload_bytes
            if header.delivery_id <= self.last:
                raise Invalid("duplicate or replayed delivery")
            progress.header_validation_finished = time.monotonic()
            self._current(deadline, progress)
            if type(size) is not int or size != header.payload_bytes or not 0 < size <= self.maximum:
                raise Invalid("delivery transaction payload extent")
            transaction.payload.validate_complete(size)
            progress.phase, progress.integrity_started = "integrity", time.monotonic()
            if type(payload_sha) is not bytes or len(payload_sha) != 32 or payload_sha != header.payload_sha:
                raise Invalid("delivery pixel digest")
            progress.integrity_verified = True
            progress.integrity_finished = time.monotonic()
            progress.phase = "finalize"
            frame = NativeFrame(header, self.publication.surface, self.client, self.publication.evidence)
            immutable = bytes(memoryview(pixels)[:size])
            self._current(deadline, progress)
            self.last = header.delivery_id
            return frame, immutable
        except BaseException as error:
            # Preserve the first cause even when retirement subsequently fails.
            # A caller's broken logging handler must not prevent retirement or
            # replace the original transport/protocol exception.
            if progress is not None and not self._closed:
                try:
                    progress.failure(self, error)
                except Exception:
                    pass
            self.close()
            raise
        finally:
            self._lock.release()

    def close(self):
        if not self._closed:
            self._closed = True
            self.channel.close()
