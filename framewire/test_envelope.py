import unittest
from dataclasses import replace
from crosscheck import vectors
from envelope import Invalid, marshal, parse, marshal_header


class Contract(unittest.TestCase):
    def test_vectors(self):
        for b in vectors():
            h, p = parse(b)
            self.assertEqual(marshal(h, p), b)

    def test_python_type_and_overflow_rejected(self):
        h, _ = parse(vectors()[0])
        for key, value in [("flags", True), ("delivery_id", -1), ("delivery_id", 2**64),
                           ("width", 2**32), ("media_pts", 2**63), ("media_pts", -(2**63)-1),
                           ("source", "é"), ("process", "x"*65), ("namespace", bytes(16)),
                           ("graph", bytes(31)), ("payload_sha", bytearray(32)), ("kind", 1.0)]:
            with self.subTest(key=key, value=repr(value)):
                with self.assertRaises(Invalid):
                    marshal_header(replace(h, **{key: value}))

    def test_unavailable_cannot_claim_origin_or_capture(self):
        h, p = parse(vectors()[2])
        for change in ({"origin_sequence": 1}, {"capture_ns": 1}, {"flags": h.flags | 1}):
            with self.assertRaises(Invalid):
                marshal(replace(h, **change), p)

    def test_epoch_cannot_carry_pixels(self):
        h, _ = parse(vectors()[3])
        for change in ({"delivery_id": 1}, {"origin_sequence": 1}, {"width": 1},
                       {"codec": 1}, {"surface": 1}, {"stream": ""}):
            with self.assertRaises(Invalid):
                marshal(replace(h, **change), b"")
        with self.assertRaises(Invalid):
            marshal(h, b"pixels")

    def test_digest_and_exact_length(self):
        b = vectors()[0]
        for invalid in (b[:-1], b+b"\0", b[:-1]+b"\xff", b"NMB2"+b[4:]):
            with self.assertRaises(Invalid):
                parse(invalid)

    def test_nv12_overflow_padding(self):
        h, p = parse(vectors()[0])
        for change in ({"stride_y": 2**32-2}, {"stride_uv": 3}, {"height": 16386},
                       {"width": 6, "stride_y": 4}, {"flags": h.flags | 4}):
            with self.assertRaises(Invalid):
                marshal(replace(h, **change), p)


if __name__ == "__main__":
    unittest.main()
