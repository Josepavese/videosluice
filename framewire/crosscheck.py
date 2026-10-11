"""Independent fixture producer and batched parser oracle used only in tests."""
import hashlib
import json
import sys
from dataclasses import replace
from envelope import Header, Invalid, marshal, parse


def vectors():
    h = Header(2, 3, 3, 1, 3, 0, 4, 2, 8, 8, 2**64-4, 2**64-3,
               2**64-2, 2**64-1, 123456, -2345, b"\1"+bytes(15),
               hashlib.sha256(b"graph").digest(), hashlib.sha256(b"transform").digest(),
               bytes(32), "front", "analysis", "broker-1")
    p = bytes(range(24))
    return [marshal(h, p),
            marshal(replace(h, kind=1, surface=2, stream="", publication_epoch=0, delivery_id=0), p),
            marshal(replace(h, codec=2, stride_y=0, stride_uv=0, origin_sequence=0,
                            correlation=0, flags=30, capture_ns=0), bytes([0, 0, 0, 1, 64, 1])),
            marshal(replace(h, kind=3, codec=0, surface=0, correlation=0, flags=0,
                            width=0, height=0, stride_y=0, stride_uv=0, origin_sequence=0,
                            delivery_id=0, capture_ns=0, media_pts=0, transform=bytes(32)), b""),
            marshal(replace(h, codec=1, stride_y=0, stride_uv=0, flags=7,
                            media_pts=-(2**63), source="S"*64, stream="T"*64,
                            process="P"*64), bytes([0, 0, 0, 1, 103])),
            marshal(replace(h, kind=5, output_sequence=2**64-1, publication_epoch=0, delivery_id=0), p),
            marshal(replace(h, kind=5, output_sequence=2**64-1, publication_epoch=0, delivery_id=0,
                            correlation=0, origin_sequence=0, capture_ns=0, flags=2), p)]


def accepted(value):
    try:
        b = bytes.fromhex(value)
        h, p = parse(b)
        assert marshal(h, p) == b
        return True
    except Invalid:
        return False


if __name__ == "__main__":
    values = json.load(sys.stdin)
    print(json.dumps({"accepted": [accepted(v) for v in values],
                      "vectors": [v.hex() for v in vectors()]}))
