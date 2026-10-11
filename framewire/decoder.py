"""Connection-scoped decoder. Transport supplies deadline-aware read(size)."""
import hashlib
from envelope import HEADER_SIZE, MAX_PAYLOAD, Header, Invalid, parse_header


class Decoder:
    def __init__(self, max_payload=MAX_PAYLOAD):
        if type(max_payload) is not int or not 0 < max_payload <= MAX_PAYLOAD:
            raise Invalid("payload quota")
        self.limit = max_payload
        self._header = bytearray()
        self._payload = bytearray()
        self._parsed = None
        self._terminal = None

    def abort(self, error=None):
        # Single-caller ownership; cancel the transport before calling abort.
        if self._terminal is None:
            self._terminal = error if error is not None else Invalid("aborted")
        self._header.clear()
        self._payload.clear()
        self._parsed = None

    @staticmethod
    def _fill(read, buffer, size):
        while len(buffer) < size:
            remaining = size - len(buffer)
            chunk = read(remaining)
            if type(chunk) is not bytes or len(chunk) > remaining:
                raise Invalid("reader returned invalid bytes/count")
            if not chunk:
                raise EOFError("end of stream")
            buffer.extend(chunk)

    def read(self, read):
        if self._terminal is not None:
            raise self._terminal
        try:
            if self._parsed is None:
                self._fill(read, self._header, HEADER_SIZE)
                self._parsed = parse_header(bytes(self._header))
                if self._parsed.payload_bytes > self.limit:
                    raise Invalid("payload quota")
            self._fill(read, self._payload, self._parsed.payload_bytes)
            if hashlib.sha256(self._payload).digest() != self._parsed.payload_sha:
                raise Invalid("payload digest")
            h, payload = self._parsed, bytes(self._payload)
            self._header.clear()
            self._payload.clear()
            self._parsed = None
            return h, payload
        except (TimeoutError, BlockingIOError):
            # The exception carries no consumed bytes; earlier chunks remain.
            raise
        except Exception as exc:
            self.abort(exc)
            raise
