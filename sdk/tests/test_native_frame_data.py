"""Real native FAPD framing, credentials, deadlines and ownership, no hardware."""
import array
import ctypes
import hashlib
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from video_broker_sdk.causal_envelope import Invalid
from video_broker_sdk.frame_data import DeliveryProgress, PayloadProgress
from video_broker_sdk.linux_frame_channel import LinuxPacketChannel, PeerError


DIGEST = hashlib.sha256(b"native-framing-fixture").digest()


def metadata():
    token = b"native-frame-data-test"
    return b"LPFD\1"+struct.pack(">3I", os.getpid(), os.getuid(), os.getgid())+bytes([len(token)])+token


def packet(data, offset=0):
    return b"FAPD\1\0"+struct.pack(">H", 44+len(data))+DIGEST+struct.pack(">I", offset)+data


class NativeFrameData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="video-broker-frame-data-test-")
        cls.addClassCleanup(cls.temporary.cleanup)
        root = Path(__file__).resolve().parents[2]
        native = root/"framepeer/provider/linux/native"
        cls.library = str(Path(cls.temporary.name)/"libpeer.so")
        subprocess.run(["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror", "-fPIC", "-shared",
            "-I"+str(native), "-I"+str(root/"framepeer/native"),
            str(root/"native/frame_peer_python.c"), str(native/"launch.c"), "-o", cls.library, "-lcrypto", "-pthread"],
            check=True, capture_output=True, timeout=30)

    def channel(self):
        left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        left.settimeout(1)
        for sock in (left, right):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 32768)
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        value = LinuxPacketChannel(ctypes.CDLL(self.library))
        self.addCleanup(value.close)
        controller, own = os.pidfd_open(os.getpid()), os.pidfd_open(os.getpid())
        try:
            left.sendmsg([metadata()], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [own]))])
        finally:
            os.close(own)
        value.accept(right.detach(), controller, metadata(), timeout_ms=1000)
        return value, left

    def test_bounded_authority_keeps_gil_but_socket_wait_releases_it(self):
        # Observe the interpreter state at the REAL native provider boundary,
        # without timer assumptions or replacing its pidfd/credential checks.
        root = Path(__file__).resolve().parents[2]
        native = root / "framepeer/provider/linux/native"
        probe = Path(self.temporary.name) / "gil_probe.c"
        probe.write_text("""
#define _GNU_SOURCE
#define framepeer_linux_current real_current
#define framepeer_linux_receive real_receive
#include "launch.c"
#undef framepeer_linux_current
#undef framepeer_linux_receive
extern int PyGILState_Check(void);
static int current_gil = -1, receive_gil = -1;
int framepeer_linux_current(framepeer_linux_launch *state) {
    current_gil = PyGILState_Check();
    return real_current(state);
}
int framepeer_linux_receive(framepeer_linux_launch *state, uint8_t *data,
        size_t capacity, size_t *size, int timeout_ms) {
    receive_gil = PyGILState_Check();
    return real_receive(state, data, capacity, size, timeout_ms);
}
int observed_current_gil(void) { return current_gil; }
int observed_receive_gil(void) { return receive_gil; }
#include "frame_peer_python.c"
""")
        self.library = str(Path(self.temporary.name) / "libgil_probe.so")
        subprocess.run(["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror",
            "-fPIC", "-shared", "-I"+str(native), "-I"+str(root/"framepeer/native"),
            "-I"+str(root/"native"), str(probe), "-o", self.library, "-lcrypto", "-pthread"],
            check=True, capture_output=True, timeout=30)
        value, writer = self.channel()
        value.current()
        self.assertEqual(value._lib.observed_current_gil(), 1)
        writer.send(b"record")
        self.assertEqual(value.receive(32, time.monotonic()+1), b"record")
        self.assertEqual(value._lib.observed_receive_gil(), 0)

    def test_complete_delivery_advances_while_python_gil_is_held(self):
        from concurrent.futures import ThreadPoolExecutor
        from dataclasses import replace
        from test_delivery import records
        from video_broker_sdk.causal_envelope import marshal_header
        root = Path(__file__).resolve().parents[2]
        native = root/"framepeer/provider/linux/native"
        self.library = str(Path(self.temporary.name)/"libdelivery_gil_probe.so")
        subprocess.run(["cc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror",
            "-fPIC", "-shared", "-I"+str(native), "-I"+str(root/"framepeer/native"),
            "-I"+str(root/"native"), str(Path(__file__).parent/"native/delivery_gil_probe.c"),
            "-o", self.library, "-lcrypto", "-pthread"],
            check=True, capture_output=True, timeout=30)
        value, writer = self.channel()
        header, _, _ = records()
        pixels = bytes(range(256))*1350
        transform = (640, 360, 640, 0, 0, 640, 360, 0, 0, 640, 360, 640, 640, 0, 0)
        label = b"video-broker.frame-transform.v1"
        transform_sha = hashlib.sha256(struct.pack(">Q", len(label))+label+b"".join(
            struct.pack(">Q", v) for v in transform)).digest()
        header = replace(header, width=640, height=360, stride_y=640, stride_uv=640,
                         payload_bytes=len(pixels), payload_sha=hashlib.sha256(pixels).digest(),
                         transform=transform_sha)
        raw = marshal_header(header)
        digest = hashlib.sha256(raw).digest()
        packets = [raw]
        for offset in range(0, len(pixels), 16340):
            chunk = pixels[offset:offset+16340]
            packets.append(b"FAPD\1\0"+struct.pack(">H", 44+len(chunk))+digest+struct.pack(">I", offset)+chunk)
        wire = b"".join(struct.pack(">H", len(record))+record for record in packets)
        lib = value._lib
        lib.delivery_probe_produce.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t]
        request = b"FAPN\1\0\0\0"+struct.pack(">2Q", header.publication_epoch, 0)
        output, progress = bytearray(len(pixels)), DeliveryProgress()
        with ThreadPoolExecutor(max_workers=2) as pool:
            send = pool.submit(lib.delivery_probe_produce, writer.fileno(), ctypes.c_char_p(wire), len(wire))
            read = pool.submit(value.read_delivery, request, output, time.monotonic()+1, progress)
            self.assertEqual(lib.delivery_probe_started(), 0)
            # Release producer only INSIDE the GIL-held call; completion cannot
            # be a frame that arrived before the blocked-interpreter interval.
            held = ctypes.PyDLL(self.library)
            self.assertEqual(held.delivery_probe_complete_with_gil(), 0)
            self.assertEqual(send.result(timeout=2), 0)
            self.assertEqual(read.result(timeout=2), (b"", raw, header.payload_sha, len(pixels)))
        self.assertEqual(output, pixels)
        progress.payload.validate_complete(len(pixels))

    def test_delivery_transaction_initial_subsequent_and_idle(self):
        from test_delivery import records
        header, expected, packets = records()
        for initial in (True, False):
            with self.subTest(initial=initial):
                value, writer = self.channel()
                request = b"FAPN\1\0\0\0"+struct.pack(">2Q", 0 if initial else header.publication_epoch, 0)
                for raw in packets if initial else packets[1:]: writer.send(raw)
                pixels, progress = bytearray(96), DeliveryProgress()
                metadata, descriptor, actual_sha, size = value.read_delivery(
                    request, pixels, time.monotonic()+1, progress)
                self.assertEqual(writer.recv(24), request)
                self.assertEqual(metadata, packets[0] if initial else b"")
                self.assertEqual(descriptor, packets[1])
                self.assertEqual((actual_sha, size, pixels), (header.payload_sha, 96, expected))
                progress.payload.validate_complete(96)
                self.assertEqual((progress.metadata_bytes, progress.header_bytes, progress.declared_payload_bytes),
                                 (len(metadata), 448, 96))
                request = b"FAPN\1\0\0\0"+struct.pack(">2Q", header.publication_epoch, header.delivery_id)
                idle = b"FAPI"+request[4:]
                writer.send(idle)
                pixels[:] = b"?"*96
                progress = DeliveryProgress()
                self.assertEqual(value.read_delivery(request, pixels, time.monotonic()+1, progress),
                                 (b"", idle, bytes(32), 0))
                self.assertEqual(writer.recv(24), request)
                self.assertEqual(pixels, b"?"*96)
                self.assertEqual(progress.header_bytes, 24)
                value.close(); writer.close()

    def test_delivery_timing_separates_server_response_and_payload_receive(self):
        from test_delivery import records
        header, expected, packets = records()
        value, writer = self.channel()
        request = b"FAPN\1\0\0\0"+struct.pack(">2Q", header.publication_epoch, 0)
        def delayed_response():
            self.assertEqual(writer.recv(24), request)
            time.sleep(0.03)
            writer.send(packets[1])
            time.sleep(0.03)
            for packet in packets[2:]:
                writer.send(packet)
        sender = threading.Thread(target=delayed_response)
        sender.start()
        pixels, progress = bytearray(96), DeliveryProgress()
        try:
            self.assertEqual(value.read_delivery(request, pixels, time.monotonic()+1, progress),
                             (b"", packets[1], header.payload_sha, 96))
        finally:
            sender.join(timeout=2)
        self.assertFalse(sender.is_alive())
        self.assertEqual(pixels, expected)
        self.assertGreaterEqual(progress.native_descriptor_us, 15_000)
        self.assertGreaterEqual(progress.native_payload_receive_us, 15_000)
        self.assertGreaterEqual(progress.native_send_us, 0)
        self.assertGreaterEqual(progress.native_integrity_us, 0)

    def test_delivery_transaction_rejects_over_admission_and_corrupt_pixels(self):
        from test_delivery import records
        header, expected, packets = records()
        request = b"FAPN\1\0\0\0"+bytes(16)
        for maximum, corrupt in ((95, False), (96, True)):
            with self.subTest(maximum=maximum, corrupt=corrupt):
                value, writer = self.channel()
                records_to_send = list(packets)
                if corrupt:
                    records_to_send[-1] = records_to_send[-1][:-1]+bytes([expected[-1]^1])
                for raw in records_to_send: writer.send(raw)
                pixels, progress = bytearray(b"?"*maximum), DeliveryProgress()
                with self.assertRaises(PeerError) as failure:
                    value.read_delivery(request, pixels, time.monotonic()+1, progress)
                self.assertEqual(failure.exception.code, -6)
                self.assertEqual(progress.declared_payload_bytes, 96)
                if not corrupt:
                    self.assertEqual(pixels, b"?"*maximum)
                    self.assertEqual(progress.payload.received_records, 0)
                    self.assertIn("payload_admission", failure.exception.stage)
                else:
                    self.assertEqual(progress.payload.validated_payload_bytes, 96)
                    self.assertIn("payload_integrity", failure.exception.stage)
                value.close(); writer.close()

    def test_complete_reader_validates_native_transaction_and_owns_immutable_pixels(self):
        from test_delivery import records, reader
        header, pixels, packets = records()
        value, writer = self.channel()
        for record in packets: writer.send(record)
        source = reader(value)
        native, actual = source.read()
        self.assertEqual(native.header, header)
        self.assertEqual(actual, pixels)
        self.assertIs(type(actual), bytes)
        self.assertEqual(writer.recv(24), b"FAPN\1\0\0\0"+bytes(16))
        writer.send(b"FAPI\1\0\0\0"+struct.pack(">2Q", header.publication_epoch, header.delivery_id))
        self.assertIsNone(source.read())
        self.assertEqual(actual, pixels)
        self.assertEqual(source.last, header.delivery_id)

    def test_native_payload_integrity_does_not_replace_publication_or_replay_validation(self):
        from dataclasses import replace
        from test_delivery import records, reader
        from video_broker_sdk.causal_envelope import marshal_header
        changes = {"source": {"source": "source-b"}, "stream": {"stream": "live"},
                   "process": {"process": "broker-2"}, "source_epoch": {"source_epoch": 7},
                   "publication": {"publication_epoch": 8}, "graph": {"graph": bytes([9])*32},
                   "namespace": {"namespace": bytes([9])*16}, "transform": {"transform": bytes([9])*32},
                   "geometry": {"width": 4, "height": 16, "stride_y": 4, "stride_uv": 4},
                   "replay": {}}
        for fault, change in changes.items():
            with self.subTest(fault=fault):
                header, pixels, packets = records()
                value, writer = self.channel()
                source = reader(value)
                if fault == "replay":
                    for record in packets: writer.send(record)
                    self.assertIsNotNone(source.read())
                    writer.recv(24)
                else:
                    writer.send(packets[0])
                raw = marshal_header(replace(header, **change))
                writer.send(raw)
                writer.send(b"FAPD\1\0"+struct.pack(">H", 44+len(pixels))+
                            hashlib.sha256(raw).digest()+bytes(4)+pixels)
                with self.assertLogs("video_broker_sdk.delivery", "WARNING"):
                    with self.assertRaises(Invalid): source.read()
                self.assertTrue(source._closed)
                self.assertEqual(source.last, header.delivery_id if fault == "replay" else 0)
                value.close(); writer.close()

    def test_complete_delivery_rejects_late_interpreter_return_without_extending_budget(self):
        from test_delivery import records, reader
        _, _, packets = records()
        value, writer = self.channel()
        for record in packets: writer.send(record)
        source = reader(value)
        source.budget_ns = 30_000_000
        receive = value._lib.video_broker_frame_delivery_receive_sha256
        def delayed(*args):
            self.assertEqual(receive(*args), 0)
            time.sleep(.05)
            return 0
        value._lib.video_broker_frame_delivery_receive_sha256 = delayed
        with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
            with self.assertRaises(TimeoutError): source.read()
        self.assertEqual(source.last, 0)
        self.assertTrue(source._closed)
        self.assertIn("validated_chunk_payload_bytes=96", " ".join(logs.output))

    def test_delivery_close_joins_total_deadline_and_does_not_restart_it_per_record(self):
        from test_delivery import records, reader
        _, _, packets = records()
        value, writer = self.channel()
        source = reader(value)
        source.budget_ns = 80_000_000
        for record in packets[:-1]: writer.send(record)
        errors = []
        def read():
            try: source.read()
            except PeerError as exc: errors.append(exc.code)
        # An explicit owner close suppresses routine retirement warnings.
        with self.assertNoLogs("video_broker_sdk.delivery", "WARNING"):
            worker = threading.Thread(target=read)
            worker.start()
            until = time.monotonic()+1
            while not value._lock.locked() and time.monotonic()<until: time.sleep(.001)
            self.assertTrue(value._lock.locked())
            source.close()
            worker.join(1)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [-5])
        self.assertEqual(source.last, 0)
        self.assertTrue(source._closed)

    def test_complete_chunks_fill_exclusive_output_and_report_progress(self):
        for size in (1, 96, 16340, 16341, 640*360*3//2, 4*1024*1024):
            with self.subTest(size=size):
                value, writer = self.channel()
                expected = (bytes(range(256))*((size+255)//256))[:size]
                failures = []
                def send():
                    try:
                        for offset in range(0, size, 16340):
                            data = packet(expected[offset:offset+16340], offset)
                            self.assertEqual(writer.send(data), len(data))
                    except BaseException as exc:
                        failures.append(exc)
                producer = threading.Thread(target=send)
                producer.start()
                progress, pixels = PayloadProgress(), bytearray(size)
                try:
                    payload_sha = value.receive_payload(pixels, DIGEST, time.monotonic()+2, progress)
                finally:
                    producer.join(3)
                self.assertFalse(producer.is_alive())
                self.assertEqual(failures, [])
                self.assertEqual(pixels, expected)
                self.assertEqual(type(payload_sha), bytes)
                self.assertEqual(payload_sha, hashlib.sha256(expected).digest())
                self.assertNotEqual(payload_sha, DIGEST)
                records = (size+16339)//16340
                self.assertEqual(progress, PayloadProgress(records, size+44*records, size))
                value.current()
                value.close()
                writer.close()

    def test_bad_record_never_overwrites_unverified_output(self):
        valid = packet(b"efgh", 4)
        bad = {
            "magic": b"X"+valid[1:], "version": valid[:4]+b"\2"+valid[5:],
            "reserved": valid[:5]+b"\1"+valid[6:], "length": valid[:6]+b"\0\1"+valid[8:],
            "digest": valid[:8]+b"x"+valid[9:],
            "offset": valid[:40]+bytes(4)+valid[44:],
            "oversize": packet(b"efghi", 4), "empty": packet(b"", 4), "short": b"FAPD",
        }
        for name, data in bad.items():
            with self.subTest(name=name):
                value, writer = self.channel()
                writer.send(packet(b"abcd")); writer.send(data)
                progress, pixels = PayloadProgress(), bytearray(b"????????")
                with self.assertRaises(PeerError) as failure:
                    value.receive_payload(pixels, DIGEST, time.monotonic()+1, progress)
                self.assertEqual(failure.exception.code, -6)
                self.assertEqual(pixels, b"abcd????")
                self.assertEqual(progress.validated_payload_bytes, 4)
                self.assertEqual(progress.received_records, 2)
                value.close(); writer.close()

    def test_total_deadline_does_not_restart_for_the_next_record(self):
        value, writer = self.channel()
        progress, pixels = PayloadProgress(), bytearray(8)
        def send_first():
            time.sleep(.07)
            writer.send(packet(b"abcd"))
        producer = threading.Thread(target=send_first)
        producer.start()
        start = time.monotonic()
        try:
            with self.assertRaises(PeerError) as failure:
                value.receive_payload(pixels, DIGEST, start+.12, progress)
        finally:
            producer.join(1)
        self.assertEqual(failure.exception.code, -5)
        self.assertLess(time.monotonic()-start, .19)
        self.assertEqual(progress, PayloadProgress(1, 48, 4))
        value.current()  # Caller owns retirement after a partial transaction.

    def test_truncation_and_transferred_capabilities_are_rejected_without_fd_leak(self):
        for mode in ("rights", "truncated"):
            with self.subTest(mode=mode):
                value, writer = self.channel()
                before = len(os.listdir("/proc/self/fd"))
                if mode == "rights":
                    fd = os.open("/dev/null", os.O_RDONLY)
                    try:
                        writer.sendmsg([packet(b"abcd")], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [fd]))])
                    finally:
                        os.close(fd)
                else:
                    writer.send(bytes(16385))
                progress = PayloadProgress()
                with self.assertRaises(PeerError) as failure:
                    value.receive_payload(bytearray(4), DIGEST, time.monotonic()+1, progress)
                self.assertEqual(failure.exception.code, -6)
                self.assertEqual(progress, PayloadProgress())
                self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_each_record_rejects_a_foreign_sender(self):
        value, writer = self.channel()
        writer.send(packet(b"abcd"))
        subprocess.run([sys.executable, "-I", "-B", "-c",
            "import socket,sys; s=socket.socket(fileno=int(sys.argv[1])); s.send(bytes.fromhex(sys.argv[2]))",
            str(writer.fileno()), packet(b"efgh", 4).hex()], pass_fds=(writer.fileno(),), check=True, timeout=2)
        progress, pixels = PayloadProgress(), bytearray(b"????????")
        with self.assertRaises(PeerError) as failure:
            value.receive_payload(pixels, DIGEST, time.monotonic()+1, progress)
        self.assertEqual(failure.exception.code, -3)
        self.assertEqual(progress, PayloadProgress(1, 48, 4))
        self.assertEqual(pixels, b"abcd????")

    def test_close_joins_owned_receive_and_remains_idempotent(self):
        value, _ = self.channel()
        errors, started = [], threading.Event()
        def receive():
            started.set()
            try:
                value.receive_payload(bytearray(4), DIGEST, time.monotonic()+.08, PayloadProgress())
            except PeerError as exc:
                errors.append(exc.code)
        worker = threading.Thread(target=receive)
        worker.start(); self.assertTrue(started.wait(1))
        # Wait until the owned native operation has the channel lock.
        until = time.monotonic()+1
        while not value._lock.locked() and time.monotonic()<until:
            time.sleep(.001)
        self.assertTrue(value._lock.locked())
        value.close(); worker.join(1)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [-5])
        reused = os.open("/dev/null", os.O_RDONLY)
        try:
            value.close(); os.fstat(reused)
        finally:
            os.close(reused)

    def test_invalid_contract_does_not_consume_the_packet(self):
        value, writer = self.channel()
        writer.send(packet(b"abcd"))
        for pixels, digest in ((b"abcd", DIGEST), (bytearray(), DIGEST), (bytearray(4), DIGEST[:-1])):
            with self.assertRaises(Invalid):
                value.receive_payload(pixels, digest, time.monotonic()+1, PayloadProgress())
        with self.assertRaises(Invalid):
            value.receive_payload(bytearray(4), DIGEST, float("nan"), PayloadProgress())
        with self.assertRaises(Invalid):
            value.receive_payload(bytearray(4), DIGEST, 1e300, PayloadProgress())
        with self.assertRaises(TimeoutError):
            value.receive_payload(bytearray(4), DIGEST, time.monotonic()-.01, PayloadProgress())
        pixels, progress = bytearray(4), PayloadProgress()
        value.receive_payload(pixels, DIGEST, time.monotonic()+1, progress)
        self.assertEqual(pixels, b"abcd")

    def test_late_python_return_does_not_extend_successful_native_deadline(self):
        value, writer = self.channel()
        writer.send(packet(b"abcd"))
        receive = value._lib.video_broker_frame_data_receive_sha256
        def delayed(*args):
            result = receive(*args)
            self.assertEqual(result, 0)
            time.sleep(.05)
            return result
        value._lib.video_broker_frame_data_receive_sha256 = delayed
        progress = PayloadProgress()
        with self.assertRaises(TimeoutError):
            value.receive_payload(bytearray(4), DIGEST, time.monotonic()+.03, progress)
        self.assertEqual(progress, PayloadProgress(1, 48, 4))
