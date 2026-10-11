"""Exact ROI validation remains strict, with bounded mismatch field labels."""
import hashlib
import struct
import unittest

from test_delivery import records
from video_broker_sdk.causal_envelope import Invalid
from video_broker_sdk.delivery import Publication
from video_broker_sdk.frame_access import FrameEvidence, Region, RegionManifest


def text(value):
    return bytes([len(value)]) + value.encode("ascii")


def number(value):
    return struct.pack(">Q", value)


def string(value):
    return number(len(value)) + value.encode("ascii")


class ROIManifestDiagnostics(unittest.TestCase):
    def test_delivery_bbox_matches_padded_observation_coordinates(self):
        region = Region("head", (8, 10, 83, 110), .08)
        self.assertEqual(region.delivery_bbox(640, 360), (2, 2, 89, 118))
        self.assertEqual(region.delivery_bbox(80, 100), (2, 2, 80, 100))
        with self.assertRaises(Invalid):
            region.delivery_bbox(641, 360)

    def test_padding_boundary_matches_native_binary64_contract(self):
        cases = (
            (640,360,0,0,(2,2,90,118)),
            (1280,720,0,0,(4,4,178,236)),
            (1920,1080,0,0,(6,6,268,354)),
            (1920,1080,0,1,(1652,6,1914,354)),
            (1080,1920,90,0,(6,1652,354,1914)),
            (1920,1080,180,0,(1652,726,1914,1074)),
            (1080,1920,270,1,(726,1652,1074,1914)),
        )
        for width,height,rotation,mirror,want in cases:
            with self.subTest(case=(width,height,rotation,mirror)):
                transform=(width,height,width,0,0,width,height,rotation,mirror,
                           640,360,640,360,0,0)
                evidence=FrameEvidence(transform,"cpu_nv12_copy",bytes([3])*32)
                self.assertEqual(evidence.map_region(Region("body",(8,10,83,110),.08)),want)

    def test_each_mismatch_is_rejected_without_exposing_provider_text(self):
        header, _, packets = records()
        expected = Publication.parse(packets[0]).evidence
        region = Region("body", (0, 0, 8, 8))
        identity = bytes([4])*16
        for field in (None, "geometry", "provider", "copy_evidence"):
            with self.subTest(field=field):
                rect = (0, 0, 14, 16) if field == "geometry" else (0, 0, 16, 16)
                geometry = (*rect, rect[2], rect[3], rect[2])
                size = geometry[4]*geometry[5]*3//2
                provider = "different_provider_private" if field == "provider" else expected.provider
                evidence = bytes([9])*32 if field == "copy_evidence" else expected.copy_evidence
                payload_sha = hashlib.sha256(bytes(size)).digest()
                core = (string("video-broker.frame-proof.v1") + number(1) + identity
                        + string(header.stream) + number(header.publication_epoch)
                        + number(header.delivery_id) + header.graph + string(header.source)
                        + number(header.source_epoch) + number(header.origin_sequence)
                        + header.graph + string("processed_full") + header.transform
                        + string(region.name) + b"".join(map(number, geometry))
                        + payload_sha + string(provider) + evidence)
                core_sha = hashlib.sha256(core).digest()
                domain_sha = hashlib.sha256(b"video-broker.domain-frame-proof.v1\0"
                    + header.namespace + struct.pack(">H", len(header.process))
                    + header.process.encode("ascii") + core_sha).digest()
                body = (identity + header.namespace + text(header.process) + b"\0\1"
                        + text(region.name) + number(size) + text(header.stream)
                        + number(header.publication_epoch) + number(header.delivery_id)
                        + header.graph + text(header.source) + number(header.source_epoch)
                        + number(header.origin_sequence) + text("processed_full")
                        + header.transform + b"".join(map(number, geometry))
                        + payload_sha + text(provider) + evidence + core_sha + domain_sha)
                packet = b"FAPM\1\0" + struct.pack(">H", len(body)+8) + body
                args = dict(header=header, surface="processed_full", region=region,
                            request_id=identity, maximum=384, expected=expected)
                if field is None:
                    self.assertEqual(RegionManifest.parse(packet, **args).bbox, rect)
                else:
                    with self.assertRaises(Invalid) as caught:
                        RegionManifest.parse(packet, **args)
                    self.assertIn("fields="+field, str(caught.exception))
                    self.assertNotIn("different_provider_private", str(caught.exception))
                    self.assertIn("expected_region=(0, 0, 16, 16)", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
