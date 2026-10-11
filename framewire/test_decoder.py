import io
import ast
from pathlib import Path
import unittest
from crosscheck import vectors
from decoder import Decoder
from envelope import HEADER_SIZE, MAX_PAYLOAD, Invalid, marshal


class Interrupted:
    def __init__(self, packet, cut, error=TimeoutError):
        self.data = packet
        self.at = 0
        self.cut = cut
        self.fired = False
        self.calls = 0
        self.error = error

    def read(self, count):
        self.calls += 1
        if not self.fired and self.at == self.cut:
            self.fired = True
            raise self.error("transient")
        end = min(self.at + count, len(self.data))
        if not self.fired:
            end = min(end, self.cut)
        out = self.data[self.at:end]
        self.at = end
        return out


class Contract(unittest.TestCase):
    def test_codec_import_boundary(self):
        allowed = {"dataclasses", "hashlib", "struct", "envelope"}
        for name in ("envelope.py", "decoder.py", "snapshot.py"):
            tree = ast.parse(Path(name).read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    self.assertTrue(all(v.name in allowed for v in node.names))
                elif isinstance(node, ast.ImportFrom):
                    self.assertIn(node.module, allowed)

    def test_timeout_at_every_boundary_preserves_next_frame(self):
        for packet in vectors():
            for cut in range(len(packet)+1):
                reader, decoder = Interrupted(packet+packet, cut), Decoder()
                for _ in range(2):
                    try:
                        h, p = decoder.read(reader.read)
                    except TimeoutError:
                        h, p = decoder.read(reader.read)
                    self.assertEqual(marshal(h, p), packet)
                # Zero-payload control may finish without attempting the next
                # read at its boundary; consume its one scheduled timeout.
                try:
                    decoder.read(reader.read)
                except TimeoutError:
                    pass
                except EOFError:
                    pass
                with self.assertRaises(EOFError):
                    decoder.read(reader.read)
                calls = reader.calls
                with self.assertRaises(EOFError):
                    decoder.read(reader.read)
                self.assertEqual(reader.calls, calls)

    def test_nonblocking_resume(self):
        packet = vectors()[0]
        reader = Interrupted(packet, HEADER_SIZE+1, BlockingIOError)
        decoder = Decoder()
        with self.assertRaises(BlockingIOError):
            decoder.read(reader.read)
        h, p = decoder.read(reader.read)
        self.assertEqual(marshal(h, p), packet)

    def test_quota_before_payload_and_abort(self):
        packet = vectors()[2]
        reader, decoder = io.BytesIO(packet), Decoder(5)
        with self.assertRaises(Invalid):
            decoder.read(reader.read)
        self.assertEqual(reader.tell(), HEADER_SIZE)
        self.assertFalse(decoder._payload)
        for limit in (0, MAX_PAYLOAD+1, True, 3.0):
            with self.assertRaises(Invalid):
                Decoder(limit)
        reader, decoder = Interrupted(vectors()[0], HEADER_SIZE+1), Decoder()
        with self.assertRaises(TimeoutError):
            decoder.read(reader.read)
        decoder.abort()
        self.assertFalse(decoder._payload)
        with self.assertRaises(Invalid):
            decoder.read(reader.read)

    def test_partial_eof_or_invalid_framing_poison(self):
        p = vectors()[0]
        for bad in (p[:1], p[:HEADER_SIZE], p[:-1], b"NMB2"+p[4:], p[:-1]+b"\xff"):
            decoder = Decoder()
            with self.assertRaises((Invalid, EOFError)):
                decoder.read(io.BytesIO(bad).read)
            with self.assertRaises((Invalid, EOFError)):
                decoder.read(io.BytesIO(p).read)
            self.assertFalse(decoder._payload)

    def test_invalid_reader_counts_and_types(self):
        for value in (None, bytearray(b"x"), b"x"*(HEADER_SIZE+1)):
            decoder = Decoder()
            with self.assertRaises(Invalid):
                decoder.read(lambda _: value)


if __name__ == "__main__":
    unittest.main()
