"""Independent wire golden; stdout only, no files or device access."""
import struct

def text(value):
    encoded = value.encode("ascii")
    return bytes([len(encoded)]) + encoded

body = bytes([1]) + bytes(15) + text("broker-1")
body += bytes([2]) + bytes(15)
body += struct.pack(">QQ", (1 << 64) - 2, 9007199254740993) + text("reader")
body += text("front") + struct.pack(">Q", 9007199254740995) + text("consumer-1")
body += bytes([16, 64, 32, 1, 2]) + text("analysis") + text("live")
body += text("test.capability") + struct.pack(">H", 3) + bytes([0, 255, 1])
print((b"FAPC" + bytes([1, 1]) + struct.pack(">H", 8 + len(body)) + body).hex())
