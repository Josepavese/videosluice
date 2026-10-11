"""Go native integration -> actual SDK/cropper/emitter -> Registry validator.

Original manifests and pixels come from the native C decoder/resize integration.
The channel uses the native authenticated peer; source/copy admission and the
durable outbox are test fixtures, not device release qualification.
"""
import copy
import asyncio
import ctypes
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import sys
import tempfile
import time
from unittest.mock import patch

import cv2
import numpy as np

from video_broker_sdk.causal_envelope import Invalid, parse_header, marshal_header
from video_broker_sdk.frame_access import FrameAccessClient, FrameEvidence, NativeFrame, Unavailable
from video_broker_sdk.linux_frame_channel import LinuxPacketChannel
from video_broker_sdk.delivery import DeliveryReader
from providers.video.causal_nv12_source import CausalNV12FrameSource
from middleware.contracts import DetectedEntity, FrameSourceRebindRequired, HeadGeometryTrace, VideoFrame
from modules.person_observation.emitter import PersonObservationEmitter
from modules.person_observation.outbox import DurableObservationOutbox
from providers.person_observation.cropper import ObservationCropper
from person_registry.frame_provenance import validate_observation_rois


raw = io.BytesIO(sys.stdin.buffer.read(3_000_001))
def record():
    size = struct.unpack(">I", raw.read(4))[0]
    assert 0 < size <= 2_000_000
    value = raw.read(size)
    assert len(value) == size
    return value

encoded, request, manifest, metadata, pixels, delivered = [record() for _ in range(6)]
assert not raw.read(1)
h, meta = parse_header(encoded), json.loads(metadata)
evidence = FrameEvidence(tuple(meta["transform"]), meta["provider"], bytes(meta["evidence"]))
request_id = request[8:24]

# Readiness belongs to this fixture only. Do not spend a native frame operation
# deadline importing Python dependencies on the target CPU.
os.write(7, b"\1")


class Channel:
    def __init__(self, corrupt=False):
        self.pending, self.closed, self.sent, self.checks = [], False, 0, 0
        self.corrupt = corrupt

    def current(self):
        assert not self.closed
        self.checks += 1

    def send(self, packet, deadline):
        assert packet == request and not self.pending
        self.sent += 1
        self.pending = [manifest]
        sha = hashlib.sha256(manifest).digest()
        for offset in range(0, len(pixels), 16340):
            data = pixels[offset:offset+16340]
            self.pending.append(b"FAPD\1\0"+struct.pack(">H", 44+len(data))+sha+struct.pack(">I", offset)+data)
        if self.corrupt:
            self.pending[-1] = self.pending[-1][:-1] + bytes((self.pending[-1][-1] ^ 1,))

    def receive(self, maximum, deadline):
        packet = self.pending.pop(0)
        assert len(packet) <= maximum
        return packet

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


with patch("video_broker_sdk.frame_access.secrets.token_bytes", return_value=request_id):
    channel = LinuxPacketChannel(ctypes.CDLL(sys.argv[1]))
    channel.accept(3, 4, bytes.fromhex(sys.argv[2]), timeout_ms=1000)
    client = FrameAccessClient(channel, maximum=len(pixels), budget_ns=1_000_000_000)
    delivery_channel = LinuxPacketChannel(ctypes.CDLL(sys.argv[1]))
    # Each launch exposes transport and controller pidfd. Metadata is supplied
    # separately by trusted composition; ROI owns 3..4, analysis owns 5..6.
    delivery_channel.accept(5, 6, bytes.fromhex(sys.argv[3]), timeout_ms=1000)
    reader = DeliveryReader(delivery_channel, client, maximum=len(delivered), budget_ns=1_000_000_000)
    source = CausalNV12FrameSource(reader, idle_poll_sec=0.01)
    async def read_video():
        await source.open()
        ok, video = await source.read()
        assert ok
        # Exactly one native lookahead may overlap consumer work. An idle
        # result must stop there, without polling for a paused consumer.
        async def idle_observed():
            while source.stats()["idle_reads"] == 0:
                await asyncio.sleep(.001)
        await asyncio.wait_for(idle_observed(), timeout=1)
        assert source.stats()["prefetch_reads"] == 1
        await asyncio.sleep(.02)
        assert source.stats()["idle_reads"] == 1
        return video
    # Keep the same event loop and admitted delivery through the ROI fault.
    # The lookahead is a completed idle result, not an orphaned socket read.
    loop = asyncio.new_event_loop()
    video = loop.run_until_complete(read_video())
    frame, analysis = video.metadata["native_frame"], video.data.tobytes()
    assert frame.evidence == evidence and frame.surface == meta["surface"]
    assert analysis == delivered and frame.header == h
    assert source.stats()["idle_reads"] >= 1  # Complete idle, no retransmission.
    assert source.stats()["frames_read"] == 1 and source.stats()["read_errors"] == 0
    result = frame.crop("head", tuple(meta["bbox"]), meta["padding"])
    assert result.pixels == pixels
    assert result.manifest.width == 2*(meta["bbox"][2]-meta["bbox"][0])
    channel.current()

    published = {}
    def publish(name, data):
        published[name] = data
        return hashlib.sha256(data).hexdigest()

    with tempfile.TemporaryDirectory(prefix="nemesi-roi-consumer-") as temporary:
        cropper = ObservationCropper(temporary, crop_publisher=publish)
        emitter = PersonObservationEmitter({"person_registry": {"enabled": False}})
        emitter._cropper = cropper
        emitter.min_face_px = min(result.manifest.width, result.manifest.height)
        assert emitter.min_face_px > min(meta["bbox"][2]-meta["bbox"][0], meta["bbox"][3]-meta["bbox"][1])
        emitter.face_padding_ratio = meta["padding"]
        emitter.max_observations_per_frame = 1
        # Sampled Tracker diagnostics must survive the real native ROI channel
        # without changing its proof or becoming independent facial evidence.
        bbox = tuple(meta["bbox"])
        trace = HeadGeometryTrace(
            "nemesi.head-geometry.v1", "pose_anatomical_envelope.v1", (h.width, h.height),
            .25, 2, ((float(bbox[0]), float(bbox[1]), .95),
                     (float(bbox[2]), float(bbox[1]), .95)),
            2, bbox, "estimated", (False, False, False, False),
        )
        head = DetectedEntity(bbox=bbox, entity_class=-1, score=0.95,
                              center_norm=(500, 500), distance_from_center_percent=0.0,
                              metadata={"head_geometry": trace, "pose_input_rotation_cw": 90})
        with patch.object(cropper, "to_bgr", side_effect=AssertionError("full analysis copy forbidden")):
            candidate = emitter._prepare_candidate(video, (h.height, h.width), [head], [], frame_ts=1., detector="fixture")
        assert candidate.image_bgr is frame
        sent = []
        emitter._send = lambda payload: sent.append(payload) or True
        assert emitter._emit_head_observation(candidate.image_bgr, head, [],
                    frame_shape=candidate.frame_shape, frame_ts=1., detector="fixture")
        payload = sent[0]
        assert validate_observation_rois(payload)
        decoded_payload = json.loads(json.dumps(payload, allow_nan=False))
        assert validate_observation_rois(decoded_payload)
        assert decoded_payload["quality"]["head_geometry"] == trace.to_payload()
        assert decoded_payload["quality"]["pose_input_rotation_cw"] == 90
        assert DurableObservationOutbox.payload_digest(payload) == DurableObservationOutbox.payload_digest(decoded_payload)
        assert type(payload["frame_id"]) is int and 0 <= payload["frame_id"] <= 2**53-1
        assert payload["face"]["present"] is False and payload["head"]["frame_access"]
        jpeg = published[Path(payload["head"]["crop_path"]).name]
        decoded = cv2.imdecode(np.frombuffer(jpeg, np.uint8), cv2.IMREAD_COLOR)
        assert decoded.shape[:2] == (result.manifest.height, result.manifest.width)
        for mutation in ("role", "dimensions", "delivery", "bbox", "face", "transform", "provider"):
            bad = copy.deepcopy(payload)
            if mutation == "role":
                bad["head"]["frame_access"]["region"] = "body"
            elif mutation == "dimensions":
                bad["quality"]["head_width"] += 2
            elif mutation == "delivery":
                bad["head"]["frame_access"]["delivery"] = marshal_header(replace(h, delivery_id=h.delivery_id+1)).hex()
            elif mutation == "bbox":
                bad["head"]["bbox"][0] += 2
            elif mutation == "face":
                bad["face"]["present"] = True
            elif mutation == "transform":
                bad["head"]["frame_access"]["transform"][0] += 2
            else:
                bad["head"]["frame_access"]["provider"] = "not_admitted"
            try:
                validate_observation_rois(bad)
            except Invalid:
                pass
            else:
                raise AssertionError("Registry accepted "+mutation)
        cropper.close()
    # Go has closed only the ROI socket. This exercises the real Linux peer
    # failure, not a mocked channel or a dead broker/delivery process.
    delivery_channel.current()
    try:
        frame.crop("head", tuple(meta["bbox"]), meta["padding"])
    except OSError:
        pass
    else:
        raise AssertionError("closed ROI transport accepted a crop")
    delivery_channel.current()
    try:
        loop.run_until_complete(source.read())
    except FrameSourceRebindRequired:
        assert isinstance(source._failure, Unavailable)
        assert source._failure.state == "provider_unavailable"
        assert "PeerError" in str(source._failure.__cause__)
    else:
        raise AssertionError("retired ROI left a silent active delivery")
    channel.close()
    channel.close()  # Idempotent verified release, no second close of reused FDs.
    loop.run_until_complete(source.close())
    loop.close()

    bad_channel = Channel(corrupt=True)
    bad_frame = NativeFrame(h, meta["surface"], FrameAccessClient(bad_channel, maximum=len(pixels),
                            budget_ns=1_000_000_000), evidence)
    try:
        bad_frame.crop("head", tuple(meta["bbox"]), meta["padding"])
    except Invalid:
        assert bad_channel.closed
    else:
        raise AssertionError("corrupt source pixels accepted")
print("NEMESI_NATIVE_ROI_PASS sdk cropper emitter registry paired_roi_retirement", flush=True)
os.write(7, b"\2")  # explicit completion after all assertions/native release
os.close(7)
