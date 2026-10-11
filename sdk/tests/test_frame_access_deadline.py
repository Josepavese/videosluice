"""Peer-check deadline regressions, independent of applications and hardware."""
import hashlib
import struct
import unittest
from unittest.mock import patch

from test_delivery import Channel, records
from video_broker_sdk.delivery import Publication
from video_broker_sdk.causal_envelope import Invalid
from video_broker_sdk.frame_access import FrameAccessClient, Region, RegionManifest


class FrameAccessDeadline(unittest.TestCase):
    def test_roi_rejects_missing_or_wrong_provider_digest_without_rehash_fallback(self):
        header, _, packets = records()
        evidence = Publication.parse(packets[0]).evidence
        pixels, raw = bytes(384), b"admitted region manifest"
        manifest = RegionManifest(raw, bytes(16), "face", 384, (0, 0, 16, 16),
                                  16, 16, 16, hashlib.sha256(pixels).digest(), bytes(32))
        for result in (None, bytes(32), bytearray(manifest.payload_sha)):
            with self.subTest(value_type=type(result).__name__):
                class BadIntegrity(Channel):
                    def receive_payload(self, output, digest, deadline, progress):
                        output[:] = pixels
                        progress.received_records = 1
                        progress.received_wire_bytes = len(pixels)+44
                        progress.validated_payload_bytes = len(pixels)
                        return result
                channel = BadIntegrity([raw])
                client = FrameAccessClient(channel, maximum=384, budget_ns=1_000_000_000)
                with patch.object(RegionManifest, "parse", return_value=manifest):
                    with self.assertRaisesRegex(Invalid, "frame access pixel digest"):
                        client.crop(header, "processed_full", Region("face", (0, 0, 8, 8), 0), evidence)
                self.assertTrue(channel.closed)

    def test_late_initial_authority_never_sends_a_request(self):
        header, _, packets = records()
        evidence = Publication.parse(packets[0]).evidence
        clock = [100.0]
        class SlowChannel(Channel):
            def current(self):
                super().current()
                clock[0] += 2.0
        channel = SlowChannel()
        client = FrameAccessClient(channel, maximum=384, budget_ns=1_000_000_000)
        with patch("video_broker_sdk.frame_access.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertRaises(TimeoutError):
                client.crop(header, "processed_full", Region("face", (0, 0, 8, 8), 0), evidence)
        self.assertEqual(channel.sent, [])
        self.assertTrue(channel.closed)

    def test_final_authority_completion_after_deadline_never_returns_pixels(self):
        # The protocol parser has separate golden/integration tests. Here the
        # admitted manifest isolates crop's final authority/return boundary.
        header, _, packets = records()
        evidence = Publication.parse(packets[0]).evidence
        pixels, raw = bytes(384), b"admitted region manifest"
        pixel_hash = hashlib.sha256(pixels).digest()
        manifest = RegionManifest(raw, bytes(16), "face", 384, (0, 0, 16, 16),
                                  16, 16, 16, pixel_hash, bytes(32))
        chunk = (b"FAPD\1\0" + struct.pack(">H", 44+len(pixels))
                 + hashlib.sha256(raw).digest() + bytes(4) + pixels)
        clock, verified = [100.0], [False]
        original_sha = hashlib.sha256
        def checksum(data):
            result = original_sha(data)
            if type(data) is bytearray:
                verified[0] = True
            return result
        class SlowChannel(Channel):
            def current(self):
                super().current()
                if verified[0]:
                    clock[0] += 2.0
        channel = SlowChannel([raw, chunk])
        client = FrameAccessClient(channel, maximum=384, budget_ns=1_000_000_000)
        with patch("video_broker_sdk.frame_access.time.monotonic", side_effect=lambda: clock[0]), \
                patch.object(RegionManifest, "parse", return_value=manifest), \
                patch("video_broker_sdk.frame_access.hashlib.sha256", side_effect=checksum):
            with self.assertRaises(TimeoutError):
                client.crop(header, "processed_full", Region("face", (0, 0, 8, 8), 0), evidence)
        self.assertTrue(verified[0])
        self.assertTrue(channel.closed)
        self.assertEqual(channel.reads, 2)

    def test_late_authority_error_retains_original_cause(self):
        clock = [100.0]
        original = OSError("authority failure")
        class FailedChannel(Channel):
            def current(self):
                clock[0] += 2.0
                raise original
        client = FrameAccessClient(FailedChannel(), maximum=384, budget_ns=1_000_000_000)
        with patch("video_broker_sdk.frame_access.time.monotonic", side_effect=lambda: clock[0]):
            with self.assertRaises(OSError) as error:
                client._current(101.0)
        self.assertIs(error.exception, original)


if __name__ == "__main__":
    unittest.main()
