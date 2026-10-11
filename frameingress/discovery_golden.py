import struct
import sys

packet = bytes.fromhex(sys.argv[1])
body = bytes([1]) + bytes(15) + bytes([2]) + bytes(15)
body += bytes([8]) + b"broker.1" + bytes([2])
body += bytes([5]) + b"front" + struct.pack(">Q", 2**53 + 1)
body += bytes([4]) + b"rear" + struct.pack(">Q", 2**64 - 1)
expected = struct.pack(">4sBBH", b"FAPB", 1, 2, 8 + len(body)) + body
assert packet == expected, "discovery wire differs from independent exact uint64 fixture"
print("discovery-golden-ok")
