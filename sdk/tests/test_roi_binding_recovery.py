"""A retired ROI session must invalidate its paired delivery, not stall silently."""
import unittest
from unittest.mock import patch

from test_delivery import Channel, records
from video_broker_sdk.delivery import DeliveryReader, Publication
from video_broker_sdk.frame_access import FrameAccessClient, Region, RegionManifest, Unavailable


class ROIBindingRecovery(unittest.TestCase):
    def fixture(self, roi_channel):
        header, _, packets = records()
        client = FrameAccessClient(roi_channel, maximum=384, budget_ns=1_000_000_000)
        channel = Channel(packets)
        reader = DeliveryReader(channel, client, maximum=96, budget_ns=1_000_000_000)
        return header, Publication.parse(packets[0]).evidence, client, channel, reader

    def test_roi_transport_failure_retires_delivery_without_reusing_old_binding(self):
        class Failed(Channel):
            def send(self, packet, deadline):
                raise TimeoutError("ROI reply deadline")
        header, evidence, client, channel, reader = self.fixture(Failed())
        with self.assertRaises(TimeoutError):
            client.crop(header, "processed_full", Region("body", (0, 0, 8, 8)), evidence)
        with self.assertRaises(Unavailable) as error:
            reader.current()
        self.assertEqual(error.exception.state, "provider_unavailable")
        self.assertIn("TimeoutError: ROI reply deadline", str(error.exception.__cause__))
        with self.assertRaises(Unavailable):
            reader.read()
        self.assertTrue(channel.closed)
        self.assertEqual(channel.sent, [])

    def test_complete_negative_crop_does_not_retire_delivery(self):
        for state in ("expired", "capacity_rejected", "region_invalid", "provider_unavailable"):
            with self.subTest(state=state):
                header, evidence, client, channel, reader = self.fixture(Channel([b"negative"]))
                with patch.object(RegionManifest, "parse", side_effect=Unavailable(state)):
                    with self.assertRaises(Unavailable):
                        client.crop(header, "processed_full", Region("body", (0, 0, 8, 8)), evidence)
                reader.current()
                self.assertIsNotNone(reader.read())
                self.assertFalse(channel.closed)


if __name__ == "__main__":
    unittest.main()
