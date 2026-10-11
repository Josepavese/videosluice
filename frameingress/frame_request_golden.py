import struct

def text(value):
    data = value.encode("ascii")
    return bytes([len(data)]) + data

body = bytes([2]) + bytes(15) + bytes([1]) + bytes(15)
body += text("broker-1") + text("analysis")
body += struct.pack(">QQ", 9007199254740993, 2**64-2) + bytes([3]) + bytes(31)
body += text("front") + text("native") + struct.pack(">Q",100) + bytes([2])
body += text("face") + bytes([0]) + struct.pack(">5d",1.5,2,5,7,0.25)
body += text("all") + bytes([1]) + struct.pack(">5d",0,0,0,0,0)
print((b"FAPQ" + bytes([1,0]) + struct.pack(">H",len(body)+8) + body).hex())
