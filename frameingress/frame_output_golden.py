"""Independent manifest decode and core/domain/payload digest verification."""
import hashlib
import struct
import sys

packet, pixels = bytes.fromhex(sys.argv[1]), bytes.fromhex(sys.argv[2])
assert packet[:6] == b"FAPM\x01\x00"
assert struct.unpack_from(">H", packet, 6)[0] == len(packet)
offset = 8
def take(n):
    global offset
    value = packet[offset:offset+n]
    assert len(value) == n
    offset += n
    return value
def number():
    return struct.unpack(">Q", take(8))[0]
def text():
    return take(take(1)[0])
def n(value):
    return struct.pack(">Q", value)
def s(value):
    return n(len(value)) + value

request, namespace, generation = take(16), take(16), text()
index, state, region, size = take(1)[0], take(1)[0], text(), number()
assert state == 1 and size == len(pixels)
stream, publication, frame, graph = text(), number(), number(), take(32)
source, capture, sequence = text(), number(), number()
surface, transform = text(), take(32)
geometry = [number() for _ in range(7)]
payload_hash, provider, evidence, core_hash, domain_hash = take(32), text(), take(32), take(32), take(32)
assert offset == len(packet)
assert payload_hash == hashlib.sha256(pixels).digest()
core = s(b"video-broker.frame-proof.v1")
core += n(1) + request + s(stream) + n(publication) + n(frame) + graph
core += s(source) + n(capture) + n(sequence) + graph + s(surface) + transform + s(region)
core += b"".join(n(value) for value in geometry) + payload_hash + s(provider) + evidence
assert hashlib.sha256(core).digest() == core_hash, "core digest mismatch"
domain = b"video-broker.domain-frame-proof.v1\x00" + namespace + struct.pack(">H",len(generation)) + generation + core_hash
assert hashlib.sha256(domain).digest() == domain_hash, "domain digest mismatch"
print("manifest/core/domain/payload verified")
