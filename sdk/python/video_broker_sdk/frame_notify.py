import asyncio
import os
import socket
import struct
import time
from dataclasses import dataclass


FRAME_NOTIFY_MAGIC = b"NMFN"
FRAME_NOTIFY_VERSION = 1
FRAME_NOTIFY_STREAM_NV12 = 2
FRAME_NOTIFY_STRUCT = struct.Struct(">4sBBHQQ")


@dataclass(frozen=True)
class FrameNotification:
    stream_id: int
    flags: int
    frame_id: int
    timestamp_ns: int


def pack_frame_notification(frame_id: int, timestamp_ns: int, flags: int = 0) -> bytes:
    return FRAME_NOTIFY_STRUCT.pack(
        FRAME_NOTIFY_MAGIC,
        FRAME_NOTIFY_VERSION,
        FRAME_NOTIFY_STREAM_NV12,
        int(flags),
        int(frame_id),
        int(timestamp_ns),
    )


def parse_frame_notification(packet: bytes) -> FrameNotification:
    if len(packet) != FRAME_NOTIFY_STRUCT.size:
        raise ValueError("invalid frame notification size")
    magic, version, stream_id, flags, frame_id, timestamp_ns = FRAME_NOTIFY_STRUCT.unpack(packet)
    if magic != FRAME_NOTIFY_MAGIC:
        raise ValueError("invalid frame notification magic")
    if int(version) != FRAME_NOTIFY_VERSION:
        raise ValueError("unsupported frame notification version")
    return FrameNotification(
        stream_id=int(stream_id),
        flags=int(flags),
        frame_id=int(frame_id),
        timestamp_ns=int(timestamp_ns),
    )


class UnixDatagramFrameNotifier:
    def __init__(
        self,
        path: str,
        *,
        wait_timeout_sec: float = 0.05,
        suspend_after_timeouts: int = 3,
        retry_sec: float = 5.0,
    ):
        self.path = str(path or "").strip()
        self.wait_timeout_sec = max(0.0, float(wait_timeout_sec))
        self.suspend_after_timeouts = max(1, int(suspend_after_timeouts))
        self.retry_sec = max(0.1, float(retry_sec))
        self._socket: socket.socket | None = None
        self._bound_path = ""
        self._timeout_streak = 0
        self._suspended_until = 0.0
        self._stats = {
            "notify_enabled": False,
            "notify_path": self.path,
            "notify_wait_timeout_sec": self.wait_timeout_sec,
            "notify_suspend_after_timeouts": self.suspend_after_timeouts,
            "notify_retry_sec": self.retry_sec,
            "notify_waits": 0,
            "notify_hits": 0,
            "notify_timeouts": 0,
            "notify_timeout_streak": 0,
            "notify_suspend_count": 0,
            "notify_stale": 0,
            "notify_drained": 0,
            "notify_errors": 0,
            "notify_bind_errors": 0,
            "notify_last_frame_id": -1,
            "notify_suspended": False,
        }

    async def open(self, shutdown_event: asyncio.Event | None = None) -> None:
        await self.close()
        if not self.path or (shutdown_event is not None and shutdown_event.is_set()):
            return
        sock: socket.socket | None = None
        try:
            directory = os.path.dirname(self.path)
            if directory:
                os.makedirs(directory, mode=0o775, exist_ok=True)
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            sock.setblocking(False)
            sock.bind(self.path)
            try:
                os.chmod(self.path, 0o660)  # nosec B103
            except OSError:
                pass
            self._socket = sock
            self._bound_path = self.path
            self._timeout_streak = 0
            self._suspended_until = 0.0
            self._stats["notify_timeout_streak"] = 0
            self._stats["notify_enabled"] = True
            self._stats["notify_suspended"] = False
        except OSError:
            if sock is not None:
                sock.close()
            self._stats["notify_bind_errors"] += 1
            self._stats["notify_suspended"] = True
            await self.close()

    async def wait_for_frame_after(self, frame_id: int) -> bool:
        if self._socket is None or self.wait_timeout_sec <= 0:
            return False
        now = time.monotonic()
        if self._suspended_until > now:
            self._stats["notify_suspended"] = True
            return False
        self._stats["notify_suspended"] = False
        self._stats["notify_waits"] += 1
        deadline = now + self.wait_timeout_sec
        loop = asyncio.get_running_loop()
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._record_timeout()
                return False
            try:
                packet = await asyncio.wait_for(loop.sock_recv(self._socket, FRAME_NOTIFY_STRUCT.size), timeout=remaining)
                notification = parse_frame_notification(packet)
            # Python 3.10 keeps asyncio.TimeoutError distinct from the built-in
            # TimeoutError (they became aliases in Python 3.11).  The RDK X5
            # runtime uses Python 3.10, so catching only the built-in exception
            # turns an expected bounded notifier timeout into a tracker read
            # error.  Use the asyncio exception explicitly on every supported
            # Python version.
            except asyncio.TimeoutError:
                self._record_timeout()
                return False
            except (OSError, ValueError):
                self._stats["notify_errors"] += 1
                self._record_timeout()
                return False
            if notification.stream_id != FRAME_NOTIFY_STREAM_NV12:
                self._stats["notify_stale"] += 1
                continue
            self._stats["notify_last_frame_id"] = notification.frame_id
            if notification.frame_id > frame_id:
                self._timeout_streak = 0
                self._suspended_until = 0.0
                self._stats["notify_timeout_streak"] = 0
                self._stats["notify_hits"] += 1
                return True
            self._stats["notify_stale"] += 1

    def discard_pending(self, *, limit: int = 4096) -> int:
        """Coalesce readiness hints made obsolete by a shared-memory read."""
        sock = self._socket
        if sock is None:
            return 0
        drained = 0
        for _ in range(max(1, int(limit))):
            try:
                packet = sock.recv(FRAME_NOTIFY_STRUCT.size)
            except BlockingIOError:
                break
            except OSError:
                self._stats["notify_errors"] += 1
                break
            try:
                notification = parse_frame_notification(packet)
            except ValueError:
                self._stats["notify_errors"] += 1
                continue
            drained += 1
            self._stats["notify_last_frame_id"] = notification.frame_id
            self._stats["notify_stale"] += 1
        self._stats["notify_drained"] += drained
        return drained

    async def close(self) -> None:
        sock = self._socket
        bound_path = self._bound_path
        self._socket = None
        self._bound_path = ""
        if sock is not None:
            sock.close()
        if bound_path:
            try:
                os.unlink(bound_path)
            except (FileNotFoundError, OSError):
                pass
        self._stats["notify_enabled"] = False

    def stats(self) -> dict:
        return dict(self._stats)

    def _record_timeout(self) -> None:
        self._stats["notify_timeouts"] += 1
        self._timeout_streak += 1
        self._stats["notify_timeout_streak"] = self._timeout_streak
        if self._timeout_streak >= self.suspend_after_timeouts:
            self._suspended_until = time.monotonic() + self.retry_sec
            self._stats["notify_suspended"] = True
            self._stats["notify_suspend_count"] += 1
