import mmap
import os
import stat
import struct
from dataclasses import dataclass


SHM_MAGIC = b"NMSH"
SHM_VERSION = 2
SHM_STREAM_NV12 = 2
SHM_HEADER_STRUCT = struct.Struct(">4sBBHQQQQIIIIQ")
SHM_HEADER_SIZE = SHM_HEADER_STRUCT.size


@dataclass(frozen=True)
class SharedMemoryIdentity:
    device: int
    inode: int
    size: int

    @property
    def token(self) -> str:
        return f"{self.device}:{self.inode}"


class UnixSharedMemoryMapping:
    """Own a read-only mapping and follow atomic producer replacements.

    Producers are allowed to unlink and recreate the shared-memory file when
    their process epoch changes.  A pathname-only open performed once at
    consumer startup would then keep reading the deleted inode forever.  This
    SDK primitive makes replacement detection part of the broker contract,
    independently from any particular consumer.
    """

    def __init__(self, path: str, *, minimum_size: int):
        self.path = str(path)
        self.minimum_size = max(1, int(minimum_size))
        self.fd: int | None = None
        self.mapping: mmap.mmap | None = None
        self.identity: SharedMemoryIdentity | None = None
        self._replacement_pending = False

    def ensure_current(self) -> tuple[bool, bool]:
        """Return ``(available, replaced)`` for the current pathname epoch.

        Once replacement is observed, the old mapping is closed before a new
        one is admitted.  Failure to open a not-yet-ready replacement therefore
        never falls back to stale frames; a later call retries the new epoch.
        """

        if self.fd is None or self.mapping is None or self.identity is None:
            replaced = self._replacement_pending
            self._open_current()
            self._replacement_pending = False
            return True, replaced

        try:
            opened = os.fstat(self.fd)
            current = os.stat(self.path, follow_symlinks=False)
        except OSError:
            self._close_current()
            self._replacement_pending = True
            return False, True

        unchanged = (
            opened.st_nlink > 0
            and stat.S_ISREG(current.st_mode)
            and (opened.st_dev, opened.st_ino) == (current.st_dev, current.st_ino)
            and opened.st_size == current.st_size == self.identity.size == len(self.mapping)
        )
        if unchanged:
            return True, False

        self._close_current()
        self._replacement_pending = True
        try:
            self._open_current()
        except (OSError, ValueError, OverflowError):
            return False, True
        self._replacement_pending = False
        return True, True

    def open_current(self) -> None:
        self.close()
        self._open_current()

    def _open_current(self) -> None:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd: int | None = None
        mapped: mmap.mmap | None = None
        try:
            fd = os.open(self.path, flags)
            opened = os.fstat(fd)
            if not stat.S_ISREG(opened.st_mode) or opened.st_size < self.minimum_size:
                raise ValueError(f"shared memory file is invalid or too small: {opened.st_size}")
            current = os.stat(self.path, follow_symlinks=False)
            if (
                not stat.S_ISREG(current.st_mode)
                or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
                or opened.st_size != current.st_size
            ):
                raise OSError("shared memory path changed while opening")
            mapped = mmap.mmap(fd, opened.st_size, access=mmap.ACCESS_READ)
        except Exception:
            if mapped is not None:
                mapped.close()
            if fd is not None:
                os.close(fd)
            raise
        self.fd = fd
        self.mapping = mapped
        self.identity = SharedMemoryIdentity(
            device=int(opened.st_dev),
            inode=int(opened.st_ino),
            size=int(opened.st_size),
        )

    def close(self) -> None:
        self._replacement_pending = False
        self._close_current()

    def _close_current(self) -> None:
        mapped, fd = self.mapping, self.fd
        self.mapping = None
        self.fd = None
        self.identity = None
        if mapped is not None:
            mapped.close()
        if fd is not None:
            os.close(fd)
