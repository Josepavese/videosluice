"""Independent reply golden, exact unsigned64 counters; stdout only."""
import struct

def text(value):
    encoded = value.encode("ascii")
    return bytes([len(encoded)]) + encoded

command_id = bytes([2]) + bytes(15)
body = bytes([1]) + bytes(15) + text("broker-1") + command_id
body += bytes([0]) + text("")
body += struct.pack(">Q", (1 << 64) - 2) + command_id
body += text("reader") + text("installed") + text("control_installed")
body += bytes([0])
print((b"FAPR" + bytes([1, 1]) + struct.pack(">H", 8 + len(body)) + body).hex())
