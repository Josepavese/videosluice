"""Linux PacketChannel using the broker's existing pidfd/credential provider.

Library selection, inherited descriptors and their footprint are supplied by
trusted launch composition. This adapter does not discover peers or authenticate
a pathname. Construct before accept(); after an accept failure the caller still
owns this object and must inspect close(), including any uncertain release.
"""
import ctypes as c
import logging
import math
import os
import socket
import threading
import time

from .causal_envelope import Invalid, MAX_PAYLOAD
from .frame_data import DeliveryProgress, PayloadProgress

_LOG = logging.getLogger(__name__)


class PeerError(OSError):
    def __init__(self, code: int, stage: str):
        self.code, self.stage = code, stage
        super().__init__(f"frame peer stage={stage} code={code}")


class LinuxPacketChannel:
    MAX_PACKET = 16384
    # Additional worst-case temporary quote per receive; two crypto FDs are
    # included. Each channel serializes receives. The owner must reserve this
    # for EVERY simultaneously usable delivery/ROI channel before attachment.
    INTEGRITY_WORKSPACE_BYTES = 1 << 20

    def __init__(self, library: c.CDLL):
        self._lib, self._lock = library, threading.Lock()
        signatures = {
            "video_broker_frame_peer_state_bytes": ([], c.c_size_t),
            "video_broker_frame_peer_stage": ([c.c_void_p], c.c_char_p),
            "framepeer_linux_accept": ([c.c_void_p, c.c_int, c.c_int, c.c_void_p, c.c_size_t, c.c_int], c.c_int),
            "framepeer_linux_current": ([c.c_void_p], c.c_int),
            "framepeer_linux_receive": ([c.c_void_p, c.c_void_p, c.c_size_t, c.POINTER(c.c_size_t), c.c_int], c.c_int),
            "framepeer_linux_send": ([c.c_void_p, c.c_void_p, c.c_size_t, c.c_int], c.c_int),
            "framepeer_linux_close": ([c.c_void_p], c.c_int),
            "video_broker_frame_data_workspace_bytes": ([], c.c_size_t),
            "video_broker_frame_data_integrity_backend": ([], c.c_char_p),
            "video_broker_frame_data_receive_sha256": ([c.c_void_p, c.c_void_p, c.c_size_t,
                c.c_void_p, c.c_size_t, c.c_void_p, c.c_int64, c.POINTER(c.c_uint64),
                c.c_void_p], c.c_int),
            "video_broker_frame_delivery_receive_sha256": ([c.c_void_p, c.c_void_p, c.c_size_t,
                c.c_void_p, c.c_size_t, c.c_void_p, c.c_size_t, c.c_void_p, c.c_size_t,
                c.c_void_p, c.c_size_t, c.c_int64, c.POINTER(c.c_uint64), c.c_void_p], c.c_int),
            "video_broker_frame_delivery_last_timing": ([c.POINTER(c.c_uint64), c.c_size_t], c.c_int),
        }
        for name, (args, result) in signatures.items():
            function = getattr(library, name)
            function.argtypes, function.restype = args, result
        workspace = int(library.video_broker_frame_data_workspace_bytes())
        backend = library.video_broker_frame_data_integrity_backend()
        if workspace != self.INTEGRITY_WORKSPACE_BYTES or backend not in (b"openssl", b"af_alg:sha256-te"):
            raise Invalid("frame payload integrity provider admission")
        self.integrity_backend = backend.decode("ascii")
        _LOG.info("frame_payload_integrity backend=%s workspace_bytes=%d temporary_fds=2 "
                  "kernel_minimum_bytes=65536 small_payload_backend=openssl",
                  self.integrity_backend, workspace)
        # This provider operation is strictly nonblocking: identity syscalls
        # and at most four poll(..., 0) attempts per pidfd. Releasing the GIL
        # here can turn a microsecond authority check into a wait behind an
        # unrelated native inference. Keep it only for this bounded check;
        # socket waits, payload transfer, bootstrap and close still release it.
        self._bounded_current = c.PYFUNCTYPE(c.c_int, c.c_void_p)(
            ("framepeer_linux_current", library))
        size = int(library.video_broker_frame_peer_state_bytes())
        if not 0 < size <= 1024:
            raise Invalid("frame peer native state size")
        # Native shim statically verifies uint64 alignment is sufficient.
        self._state = (c.c_uint64 * ((size+7)//8))()
        self._buffer = (c.c_ubyte * self.MAX_PACKET)()
        self._attempted, self._accepted, self._closed = False, False, False
        self._native_attempted = False
        self._close_error = None

    def _check(self, result: int) -> None:
        if result:
            stage = self._lib.video_broker_frame_peer_stage(self._state)
            raise PeerError(int(result), stage.decode("ascii") if stage else "unknown")

    def accept(self, socket_fd: int, controller_fd: int, metadata: bytes, *, timeout_ms: int) -> None:
        if (type(socket_fd) is not int or type(controller_fd) is not int
                or min(socket_fd, controller_fd) < 0 or max(socket_fd, controller_fd) > 2**31-1
                or socket_fd == controller_fd or type(metadata) is not bytes or not 1 <= len(metadata) <= 256
                or type(timeout_ms) is not int or not 1 <= timeout_ms <= 5000):
            raise Invalid("frame peer launch arguments; ownership not consumed")
        with self._lock:
            if self._attempted or self._closed:
                raise Invalid("frame peer bootstrap is single-use; ownership not consumed")
            self._attempted = True
            self._native_attempted = True
            self._check(self._lib.framepeer_linux_accept(self._state, socket_fd, controller_fd,
                                                        c.c_char_p(metadata), len(metadata), timeout_ms))
            self._accepted = True

    def _current(self) -> None:
        if not self._accepted or self._closed:
            raise PeerError(-4, "closed_or_not_accepted")
        self._check(self._bounded_current(self._state))

    def connect(self, path: str, controller_fd: int, metadata: bytes, *, timeout_ms: int) -> None:
        """Attach to the broker pinned by an inherited supervisor capability.

        The path locates a transport, never authenticates it. The caller has
        already admitted native/kernel/FD footprint. This consumes controller_fd
        on every path AFTER validating arguments and the single-use state.
        No connect/bootstrap retry; failure requires a fresh owner and binding.
        """
        if (type(path) is not str or not path.startswith("/") or "\0" in path
                or len(os.fsencode(path)) > 107 or type(controller_fd) is not int
                or not 0 <= controller_fd <= 2**31-1 or type(metadata) is not bytes
                or not 1 <= len(metadata) <= 256 or type(timeout_ms) is not int
                or not 1 <= timeout_ms <= 5000):
            raise Invalid("frame peer connect arguments; ownership not consumed")
        with self._lock:
            if self._attempted or self._closed:
                raise Invalid("frame peer bootstrap is single-use; ownership not consumed")
            # Keep one serialized owner from socket creation to native accept;
            # do not recurse into accept(), which would acquire this same lock.
            self._attempted = True
            pending = None
            transferred = False
            try:
                os.set_inheritable(controller_fd, False)
                deadline = time.monotonic()+timeout_ms/1000
                pending = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
                pending.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
                pending.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32768)
                pending.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32768)
                pending.settimeout(self._remaining(deadline)/1000)
                pending.connect(path)
                pending.settimeout(self._remaining(deadline)/1000)
                if pending.send(b"FAPH\1\0\0\0") != 8:
                    raise Invalid("frame peer partial attachment hello")
                remaining = self._remaining(deadline)
                fd = pending.detach()
                transferred = True
                self._native_attempted = True
                self._check(self._lib.framepeer_linux_accept(self._state, fd, controller_fd,
                    c.c_char_p(metadata), len(metadata), remaining))
                self._accepted = True
                if time.monotonic() >= deadline:
                    raise TimeoutError("frame peer attachment total deadline exceeded")
            except BaseException:
                if not transferred:
                    try:
                        if pending is not None: pending.close()
                    except OSError as exc:
                        self._close_error = exc
                    finally:
                        try: os.close(controller_fd)
                        except OSError as exc:
                            self._close_error = exc
                # Caller still owns this object's close obligation if native
                # accept was attempted; no blind repeated descriptor close.
                raise

    def current(self) -> None:
        with self._lock:
            self._current()

    @staticmethod
    def _remaining(deadline: float) -> int:
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise Invalid("frame peer deadline")
        remaining = deadline-time.monotonic()
        if remaining <= 0:
            raise TimeoutError("frame peer total deadline expired")
        return max(1, min(5000, math.ceil(remaining*1000)))

    def send(self, packet: bytes, deadline: float) -> None:
        if type(packet) is not bytes or not 0 < len(packet) <= self.MAX_PACKET:
            raise Invalid("frame peer send extent")
        with self._lock:
            self._current()
            self._check(self._lib.framepeer_linux_send(self._state, c.c_char_p(packet), len(packet), self._remaining(deadline)))

    def receive(self, maximum: int, deadline: float) -> bytes:
        if type(maximum) is not int or not 0 < maximum <= self.MAX_PACKET:
            raise Invalid("frame peer receive extent")
        with self._lock:
            self._current()
            received = c.c_size_t()
            self._check(self._lib.framepeer_linux_receive(self._state, self._buffer, maximum,
                                                        c.byref(received), self._remaining(deadline)))
            if not 0 < received.value <= maximum:
                raise Invalid("frame peer invalid received extent")
            return c.string_at(self._buffer, received.value)

    def receive_payload(self, pixels: bytearray, digest: bytes, deadline: float,
                        progress: PayloadProgress) -> bytes:
        if (type(pixels) is not bytearray or not 0 < len(pixels) <= MAX_PAYLOAD
                or type(digest) is not bytes or len(digest) != 32
                or type(progress) is not PayloadProgress):
            raise Invalid("frame data receive contract")
        self._remaining(deadline)
        if deadline >= (2**63-1)/1_000_000_000:
            raise Invalid("frame data deadline extent")
        until = int(deadline * 1_000_000_000)
        if not 0 < until < 2**63:
            raise Invalid("frame data deadline extent")
        with self._lock:
            self._current()
            # from_buffer pins the bytearray against resizing until native IO
            # has joined. The SDK exclusively owns it until final validation.
            output = (c.c_ubyte * len(pixels)).from_buffer(pixels)
            counters = (c.c_uint64 * 3)()
            payload_sha = (c.c_ubyte * 32)()
            try:
                self._check(self._lib.video_broker_frame_data_receive_sha256(
                    self._state, self._buffer, self.MAX_PACKET, output, len(pixels),
                    c.c_char_p(digest), until, counters, payload_sha))
                # Reacquiring the GIL after native IO may itself finish late.
                if time.monotonic() >= deadline:
                    raise TimeoutError("frame data total deadline expired")
                return bytes(payload_sha)
            finally:
                progress.received_records = int(counters[0])
                progress.received_wire_bytes = int(counters[1])
                progress.validated_payload_bytes = int(counters[2])

    def read_delivery(self, request: bytes, pixels: bytearray, deadline: float,
                      progress: DeliveryProgress) -> tuple[bytes, bytes, bytes, int]:
        """Request and receive one complete frame while native IO releases GIL.

        No result authenticates publication metadata or geometry: DeliveryReader
        must validate both returned descriptors before exposing these bytes.
        The exclusive output is already admitted; no queue or extra frame exists.
        """
        if (type(request) is not bytes or len(request) != 24
                or type(pixels) is not bytearray or not 0 < len(pixels) <= MAX_PAYLOAD
                or type(progress) is not DeliveryProgress):
            raise Invalid("delivery transaction contract")
        self._remaining(deadline)
        if deadline >= (2**63-1)/1_000_000_000:
            raise Invalid("delivery transaction deadline extent")
        until = int(deadline*1_000_000_000)
        if not 0 < until < 2**63:
            raise Invalid("delivery transaction deadline extent")
        with self._lock:
            self._current()
            output = (c.c_ubyte*len(pixels)).from_buffer(pixels)
            metadata, header = (c.c_ubyte*512)(), (c.c_ubyte*448)()
            counters, payload_sha = (c.c_uint64*6)(), (c.c_ubyte*32)()
            try:
                self._check(self._lib.video_broker_frame_delivery_receive_sha256(
                    self._state, self._buffer, self.MAX_PACKET, c.c_char_p(request), len(request),
                    metadata, 512, header, 448, output, len(pixels), until, counters, payload_sha))
                if time.monotonic() >= deadline:
                    raise TimeoutError("delivery transaction total deadline expired")
                self._current()
                if time.monotonic() >= deadline:
                    raise TimeoutError("delivery transaction total deadline expired")
                if counters[3] > 512 or counters[4] > 448 or counters[5] > len(pixels):
                    raise Invalid("delivery transaction native extent")
                return (c.string_at(metadata, counters[3]), c.string_at(header, counters[4]),
                        bytes(payload_sha), int(counters[5]))
            finally:
                timings = (c.c_uint64*4)()
                if self._lib.video_broker_frame_delivery_last_timing(timings, 4) == 0:
                    (progress.native_send_us, progress.native_descriptor_us,
                     progress.native_payload_receive_us, progress.native_integrity_us) = (
                        int(value) for value in timings)
                progress.payload.received_records = int(counters[0])
                progress.payload.received_wire_bytes = int(counters[1])
                progress.payload.validated_payload_bytes = int(counters[2])
                progress.metadata_bytes = int(counters[3])
                progress.header_bytes = int(counters[4])
                progress.declared_payload_bytes = int(counters[5])

    def close(self) -> None:
        with self._lock:
            if self._closed:
                if self._close_error is not None:
                    raise self._close_error
                return
            self._closed = True
            if self._native_attempted:
                try:
                    self._check(self._lib.framepeer_linux_close(self._state))
                except PeerError as exc:
                    self._close_error = exc
                    raise
            if self._close_error is not None:
                raise self._close_error
