"""Ordinary app bootstrap, separate parent/broker/tracker, synthetic detector."""
import argparse
import asyncio
import os
import pathlib
import sys
from unittest.mock import patch

from app.bootstrap import run_tracker
from app.frame_bootstrap import FrameServiceInputs, add_frame_arguments


parser = argparse.ArgumentParser()
parser.add_argument("--video_source")
add_frame_arguments(parser)
endpoints = pathlib.Path(sys.argv[3])
args = parser.parse_args([
    "--video_source", "broker_frame", "--frame-peer-library", sys.argv[1],
    "--frame-broker-metadata", sys.argv[2], "--frame-launch-controller-metadata", sys.argv[4],
    "--frame-launch-socket-fd", "3", "--frame-launch-controller-fd", "4",
    "--frame-broker-delivery-fd", "5", "--frame-broker-roi-fd", "6",
    "--frame-delivery-path", str(endpoints / "delivery.sock"),
    "--frame-roi-path", str(endpoints / "roi.sock"),
])
inputs = FrameServiceInputs.from_args(args)
os.write(7, b"\1")  # fixture-only process preparation, before native operations
os.close(7)
config = {"video": {"source": "broker_frame", "resolution": {"width": 8, "height": 8}, "framerate": 20},
          "runtime": {"debug": False}, "detection": {"enabled": True}}


class UseCase:
    """Only detector/use-case policy is a fixture; bootstrap/source/ROI are real."""
    def __init__(self, source): self.source = source
    async def run(self, shutdown):
        await self.source.open(shutdown)
        ok, video = await self.source.read()
        assert ok and video.width == 8 and video.height == 8
        native = video.metadata["native_frame"]
        assert native.header.process == "broker-1" and native.header.source == "front"
        # One owned lookahead overlaps consumer work; an idle result does not
        # keep polling. ROI remains independently usable throughout.
        async def idle_observed():
            while self.source.stats()["idle_reads"] == 0:
                await asyncio.sleep(.001)
        await asyncio.wait_for(idle_observed(), timeout=1)
        await asyncio.sleep(.02)
        assert self.source.stats()["idle_reads"] == 1
        assert self.source.stats()["prefetch_reads"] == 1
        crop = await asyncio.to_thread(native.crop, "head", (1, 1, 3, 3), 0)
        assert crop.manifest.width == 4 and crop.manifest.height == 4 and len(crop.pixels) == 24
        print("SEPARATE_BROKER_TRACKER_PASS app_bootstrap delivery idle native_roi")
        await self.source.close()


try:
    with patch("app.bootstrap.build_usecase", side_effect=lambda config, logger, frame_source: UseCase(frame_source)):
        asyncio.run(run_tracker(config, inputs))
finally:
    inputs.close()
