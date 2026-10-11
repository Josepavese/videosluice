"""Standalone SDK regressions; no application, camera or device grants required."""
import hashlib
import struct
import time
import unittest
from unittest.mock import patch
from video_broker_sdk import delivery as delivery_module

from video_broker_sdk.causal_envelope import Header, Invalid, marshal_header
from video_broker_sdk.delivery import DeliveryReader
from video_broker_sdk.frame_access import FrameAccessClient, NativeFrame, Unavailable


def records():
    transform = (16, 16, 16, 0, 0, 16, 16, 0, 0, 8, 8, 8, 8, 0, 0)
    label = b"video-broker.frame-transform.v1"
    digest = hashlib.sha256(struct.pack(">Q", len(label)) + label + b"".join(
        struct.pack(">Q", value) for value in transform)).digest()
    pixels = bytes(range(96))
    header = Header(2, 3, 3, 1, 0, 96, 8, 8, 8, 8, 2**53+3, 2**53+4,
                    2**53+5, 2**53+6, 0, 0, bytes([1])*16, bytes([2])*32,
                    digest, hashlib.sha256(pixels).digest(), "source-a", "analysis", "broker-1")
    metadata = bytearray(b"FAPB\1\0\0\0" + header.namespace + struct.pack(
        ">2Q", header.source_epoch, header.publication_epoch) + header.graph
        + struct.pack(">15I", *transform) + bytes([3])*32)
    for token in (header.process, header.source, header.stream, "processed_full", "cpu_nv12_copy"):
        metadata.extend(bytes([len(token)]) + token.encode())
    struct.pack_into(">H", metadata, 6, len(metadata))
    raw = marshal_header(header)
    packets = [bytes(metadata), raw]
    for offset in (0, 48):
        packets.append(b"FAPD\1\0" + struct.pack(">H", 92) + hashlib.sha256(raw).digest()
                       + struct.pack(">I", offset) + pixels[offset:offset+48])
    return header, pixels, packets


class Channel:
    def __init__(self, packets=(), hook=None):
        self.packets, self.hook, self.reads = list(packets), hook, 0
        self.sent, self.closed, self.deadlines = [], False, []

    def current(self):
        if self.closed:
            raise RuntimeError("closed")

    def receive(self, maximum, deadline):
        self.deadlines.append(deadline)
        index, self.reads = self.reads, self.reads+1
        if self.hook:
            self.hook(index)
        packet = self.packets.pop(0)
        assert len(packet) <= maximum
        return packet

    def send(self, packet, deadline):
        self.deadlines.append(deadline)
        self.sent.append(packet)

    def read_delivery(self, request, pixels, deadline, progress):
        # Explicit reference implementation of the current transaction port.
        # Only native tests can prove progress while the interpreter is blocked.
        def current():
            self.current()
            if time.monotonic() >= deadline:
                raise TimeoutError("delivery total deadline exceeded")
        current()
        self.send(request, deadline)
        metadata = b""
        if not any(request[8:16]):
            metadata = self.receive(512, deadline)
            progress.metadata_bytes = len(metadata) if type(metadata) is bytes else 0
            current()
            if type(metadata) is not bytes:
                return metadata, b"", bytes(32), 0
        header = self.receive(448, deadline)
        progress.header_bytes = len(header) if type(header) is bytes else 0
        current()
        if type(header) is not bytes or len(header) != 448:
            return metadata, header, bytes(32), 0
        size = struct.unpack_from(">I", header, 16)[0]
        progress.declared_payload_bytes = size
        if not 0 < size <= len(pixels):
            raise Invalid("fixture delivery admission")
        digest = self.receive_payload(memoryview(pixels)[:size], hashlib.sha256(header).digest(),
                                      deadline, progress.payload)
        return metadata, header, digest, size

    def receive_payload(self, pixels, digest, deadline, progress):
        # Independent in-memory provider fixture. Real C framing/authentication
        # is exercised separately; no runtime compatibility fallback.
        offset = 0
        while offset < len(pixels):
            if time.monotonic() >= deadline:
                raise TimeoutError("fixture payload deadline")
            self.current()
            packet = self.receive(16384, deadline)
            if type(packet) is bytes:
                progress.received_records += 1
                progress.received_wire_bytes += len(packet)
            self.current()
            if time.monotonic() >= deadline:
                raise TimeoutError("fixture payload deadline")
            if (type(packet) is not bytes or not 44 < len(packet) <= 16384
                    or packet[:6] != b"FAPD\1\0" or packet[8:40] != digest
                    or struct.unpack_from(">H", packet, 6)[0] != len(packet)
                    or struct.unpack_from(">I", packet, 40)[0] != offset
                    or len(packet)-44 > len(pixels)-offset):
                raise Invalid("fixture payload identity or extent")
            end = offset+len(packet)-44
            pixels[offset:end] = packet[44:]
            offset = end
            progress.validated_payload_bytes = offset
        return hashlib.sha256(pixels).digest()

    def close(self):
        self.closed = True


def reader(channel):
    return DeliveryReader(channel, FrameAccessClient(Channel(), maximum=384, budget_ns=1_000_000_000),
                          maximum=96, budget_ns=1_000_000_000)


class DeliveryDiagnostics(unittest.TestCase):
    def test_provider_must_return_the_complete_independently_computed_digest(self):
        header, pixels, packets = records()
        for wrong in (None, b"", bytes(31), bytes(33), bytes(32),
                      bytearray(header.payload_sha), hashlib.sha256(packets[1]).digest()):
            with self.subTest(value_type=type(wrong).__name__, size=len(wrong) if wrong is not None else -1):
                class BadIntegrity(Channel):
                    def receive_payload(self, *args):
                        super().receive_payload(*args)
                        return wrong
                channel = BadIntegrity(packets)
                value = reader(channel)
                with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                    with self.assertRaisesRegex(Invalid, "delivery pixel digest"):
                        value.read()
                self.assertIn("phase=integrity", " ".join(logs.output))
                self.assertTrue(channel.closed)
                self.assertEqual(value.last, 0)

    def test_late_complete_idle_is_identified_but_still_rejected_and_retired(self):
        header, _, packets = records()
        clock = [100.0]
        idle = b"FAPI\1\0\0\0"+struct.pack(">2Q", header.publication_epoch, 0)
        def hook(index):
            if index == 1:
                clock[0] = 101.084
        channel = Channel([packets[0], idle], hook)
        value = reader(channel)
        with patch("video_broker_sdk.delivery.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                with self.assertRaises(TimeoutError):
                    value.read()
        self.assertIn("response_record=idle_record", " ".join(logs.output))
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)
        self.assertEqual(channel.reads, 2)

    def test_authority_delay_after_transaction_is_measured_before_validation(self):
        _, _, packets = records()
        clock = [100.0]
        class SlowAuthority(Channel):
            completed = False
            def read_delivery(self, *args):
                result = super().read_delivery(*args)
                self.completed = True
                return result
            def current(self):
                super().current()
                if self.completed:
                    clock[0] += 2.0
        channel = SlowAuthority(packets)
        value = reader(channel)
        with patch("video_broker_sdk.delivery.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                with self.assertRaises(TimeoutError):
                    value.read()
        text = " ".join(logs.output)
        for field in ("phase=frame_transaction", "transaction_us=0", "authority_us=2000000",
                      "authority_max_us=2000000", "header_validation_us=-1", "allocation_us=0"):
            self.assertIn(field, text)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)
        self.assertEqual(channel.reads, 4)

    def test_final_authority_completion_after_deadline_never_accepts_frame(self):
        _, _, packets = records()
        clock, finalized = [100.0], [False]
        class SlowAuthority(Channel):
            def current(self):
                super().current()
                if finalized[0]:
                    clock[0] += 2.0
        def native(*args):
            finalized[0] = True
            return NativeFrame(*args)
        channel = SlowAuthority(packets)
        value = reader(channel)
        with patch("video_broker_sdk.delivery.time.monotonic", side_effect=lambda: clock[0]), \
                patch("video_broker_sdk.delivery.NativeFrame", side_effect=native):
            with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                with self.assertRaises(TimeoutError):
                    value.read()
        text = " ".join(logs.output)
        for field in ("phase=finalize", "authority_max_us=2000000", "integrity_verified=true"):
            self.assertIn(field, text)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)

    def test_authority_error_is_preserved_even_after_deadline(self):
        _, _, packets = records()
        clock = [100.0]
        original_error = OSError("private authority diagnostic")
        class FailedAuthority(Channel):
            def current(self):
                super().current()
                if self.reads == 2:
                    clock[0] += 2.0
                    raise original_error
        channel = FailedAuthority(packets)
        value = reader(channel)
        with patch("video_broker_sdk.delivery.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                with self.assertRaises(OSError) as error:
                    value.read()
        self.assertIs(error.exception, original_error)
        text = " ".join(logs.output)
        self.assertIn("transaction_us=2000000", text)
        self.assertIn("reason=OSError", text)
        self.assertNotIn("private authority diagnostic", text)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)

    def test_allocation_delay_is_distinct_from_header_and_payload_work(self):
        _, _, packets = records()
        clock = [100.0]
        channel = Channel(packets)
        value = reader(channel)
        def allocate(size):
            clock[0] += 2.0
            return bytearray(size)
        with patch("video_broker_sdk.delivery.time.monotonic", side_effect=lambda: clock[0]), \
                patch("video_broker_sdk.delivery.time.thread_time_ns", return_value=5000), \
                patch("video_broker_sdk.delivery.bytearray", side_effect=allocate, create=True):
            with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                with self.assertRaises(TimeoutError):
                    value.read()
        text = " ".join(logs.output)
        for field in ("header_validation_us=-1", "allocation_us=2000000",
                      "allocation_thread_cpu_us=0", "transaction_us=-1", "thread_cpu_us=0"):
            self.assertIn(field, text)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)
        self.assertEqual(channel.reads, 0)

    def test_header_validation_delay_is_not_attributed_to_allocation(self):
        _, _, packets = records()
        clock = [100.0]
        original = delivery_module.parse_header
        def parse(packet):
            clock[0] += 2.0
            return original(packet)
        channel = Channel(packets)
        value = reader(channel)
        with patch("video_broker_sdk.delivery.time.monotonic", side_effect=lambda: clock[0]), \
                patch("video_broker_sdk.delivery.parse_header", side_effect=parse):
            with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                with self.assertRaises(TimeoutError):
                    value.read()
        text = " ".join(logs.output)
        for field in ("header_validation_us=2000000", "allocation_us=0", "transaction_us=0"):
            self.assertIn(field, text)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)

    def test_allocation_failure_preserves_cause_and_reports_unreached_payload(self):
        _, _, packets = records()
        channel = Channel(packets)
        value = reader(channel)
        failure = MemoryError("private-allocator-message")
        with patch("video_broker_sdk.delivery.bytearray", side_effect=failure, create=True):
            with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                with self.assertRaises(MemoryError) as error:
                    value.read()
        self.assertIs(error.exception, failure)
        text = " ".join(logs.output)
        for field in ("phase=buffer_allocate", "allocation_us=", "allocation_thread_cpu_us=",
                      "transaction_us=-1", "reason=MemoryError"):
            self.assertIn(field, text)
        self.assertNotIn(str(failure), text)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)

    def test_deadline_after_header_explains_wait_without_accepting_pixels(self):
        _, _, packets = records()
        clock = [100.0]
        def hook(index):
            if index == 1:
                clock[0] = 101.001
        channel = Channel(packets, hook)
        value = reader(channel)
        with patch("video_broker_sdk.delivery.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                with self.assertRaises(TimeoutError) as error:
                    value.read()
        self.assertEqual(str(error.exception), "delivery total deadline exceeded")
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)
        self.assertEqual(len(channel.sent), 1)
        self.assertEqual(channel.reads, 2)
        self.assertEqual(set(channel.deadlines), {101.0})
        text = " ".join(logs.output)
        for field in ("phase=frame_transaction", "received_records=2", "validated_chunk_payload_bytes=0",
                      "budget_us=1000000", "elapsed_us=", "transaction_us=", "thread_cpu_us=",
                      "reason=TimeoutError"):
            self.assertIn(field, text)

    def test_partial_error_preserves_exception_and_hides_private_provider_message(self):
        _, _, packets = records()
        failure = OSError("private-provider-bytes-do-not-log")
        def hook(index):
            if index == 3:
                raise failure
        channel = Channel(packets, hook)
        value = reader(channel)
        with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
            with self.assertRaises(OSError) as error:
                value.read()
        self.assertIs(error.exception, failure)
        text = " ".join(logs.output)
        self.assertNotIn(str(failure), text)
        for field in ("phase=frame_transaction", "received_records=3", "validated_chunk_payload_bytes=48",
                      "declared_payload_bytes=96", "attempted_delivery_id=0"):
            self.assertIn(field, text)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)

    def test_idle_and_success_do_not_log_and_failure_counts_only_current_read(self):
        header, pixels, packets = records()
        channel = Channel(packets)
        value = reader(channel)
        with self.assertNoLogs("video_broker_sdk.delivery", "WARNING"):
            frame, actual = value.read()
            self.assertEqual(frame.header, header)
            self.assertEqual(actual, pixels)
            channel.packets = [b"FAPI\1\0\0\0"+struct.pack(">2Q", header.publication_epoch, header.delivery_id)]
            self.assertIsNone(value.read())
        channel.hook = lambda _: (_ for _ in ()).throw(OSError("stopped"))
        with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
            with self.assertRaises(OSError):
                value.read()
        text = " ".join(logs.output)
        for field in ("phase=frame_transaction", "received_records=0", "validated_chunk_payload_bytes=0",
                      "last_delivery_id="+str(header.delivery_id)):
            self.assertIn(field, text)

    def test_first_failure_is_logged_even_when_cleanup_also_fails(self):
        failure, cleanup = OSError("private-original"), RuntimeError("cleanup-failed")
        channel = Channel(hook=lambda _: (_ for _ in ()).throw(failure))
        def close():
            raise cleanup
        channel.close = close
        value = reader(channel)
        with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
            with self.assertRaises(RuntimeError) as error:
                value.read()
        self.assertIs(error.exception, cleanup)
        self.assertIs(error.exception.__context__, failure)
        self.assertIn("reason=OSError", " ".join(logs.output))
        self.assertNotIn("private-original", " ".join(logs.output))

    def test_logging_failure_cannot_skip_retirement_or_replace_transport_error(self):
        failure = OSError("transport-failure")
        channel = Channel(hook=lambda _: (_ for _ in ()).throw(failure))
        value = reader(channel)
        with patch("video_broker_sdk.delivery._LOG.warning", side_effect=RuntimeError("broken-handler")):
            with self.assertRaises(OSError) as error:
                value.read()
        self.assertIs(error.exception, failure)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)
        with self.assertNoLogs("video_broker_sdk.delivery", "WARNING"):
            with self.assertRaises(Unavailable):
                value.read()

    def test_invalid_provider_value_reaches_existing_protocol_validator(self):
        channel = Channel()
        channel.receive = lambda *_: object()
        value = reader(channel)
        with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
            with self.assertRaises(Invalid) as error:
                value.read()
        self.assertEqual(str(error.exception), "publication metadata record")
        for field in ("phase=metadata_validate", "received_records=0", "reason=Invalid"):
            self.assertIn(field, " ".join(logs.output))
        self.assertTrue(channel.closed)

    def test_payload_deadline_counts_received_but_not_unverified_chunk(self):
        _, _, packets = records()
        clock = [100.0]
        def hook(index):
            if index == 3:
                clock[0] = 101.001
        channel = Channel(packets, hook)
        value = reader(channel)
        with patch("video_broker_sdk.delivery.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                with self.assertRaises(TimeoutError):
                    value.read()
        text = " ".join(logs.output)
        for field in ("phase=frame_transaction", "received_records=4", "validated_chunk_payload_bytes=48",
                      "declared_payload_bytes=96"):
            self.assertIn(field, text)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)
        self.assertEqual(set(channel.deadlines), {101.0})

    def test_corrupt_pixels_are_not_accepted_despite_complete_transport(self):
        _, _, packets = records()
        packets[-1] = packets[-1][:-1]+bytes([packets[-1][-1] ^ 1])
        channel = Channel(packets)
        value = reader(channel)
        with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
            with self.assertRaises(Invalid) as error:
                value.read()
        self.assertEqual(str(error.exception), "delivery pixel digest")
        text = " ".join(logs.output)
        for field in ("phase=integrity", "received_records=4", "validated_chunk_payload_bytes=96",
                      "declared_payload_bytes=96", "integrity_verified=false"):
            self.assertIn(field, text)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)

    def test_final_deadline_rejects_even_an_integrity_verified_frame(self):
        _, _, packets = records()
        clock = [100.0]
        def native(*args):
            clock[0] = 101.001
            return NativeFrame(*args)
        channel = Channel(packets)
        value = reader(channel)
        with patch("video_broker_sdk.delivery.time.monotonic", side_effect=lambda: clock[0]), \
                patch("video_broker_sdk.delivery.NativeFrame", side_effect=native):
            with self.assertLogs("video_broker_sdk.delivery", "WARNING") as logs:
                with self.assertRaises(TimeoutError):
                    value.read()
        text = " ".join(logs.output)
        for field in ("phase=finalize", "validated_chunk_payload_bytes=96", "integrity_verified=true"):
            self.assertIn(field, text)
        self.assertTrue(channel.closed)
        self.assertEqual(value.last, 0)
        self.assertEqual(set(channel.deadlines), {101.0})


if __name__ == "__main__":
    unittest.main()
