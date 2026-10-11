# Capability discovery

Discovery belongs entirely to `video-broker`. It has no input for a Nemesi
machine id, install preset, `hardware_profile.json`, board catalog or hostname.
Those artifacts may help an embedding application choose a physical source or
set policy, but cannot prove an accelerator.

## Evidence hierarchy

1. Source discovery probes the actual camera component: V4L2 nodes and modes,
   or the selector owned by libcamera/HBN.
2. Engine discovery probes product-owned executable workers and plugins,
   relevant device nodes, system media elements and runtime libraries.
3. The planner first selects one complete source-worker boundary: native SP or
   portable GStreamer. It then resolves capture, decode, live encode, archive
   encode, tone map, conversion and resize inside that worker family. It can
   therefore use, for example, a hardware H265 live encoder and a permitted
   software H265 archive encoder. The application codec policy remains
   H265-first for both roles. H264 providers participate only when an explicit
   interoperability stream requests H264; they are never an automatic fallback.
4. Presence produces a `candidate`. Worker telemetry reconciles the exact
   backend selected, including fallback. Delivered codec bootstrap and frames
   promote selected stages to `attested`.
5. A runtime contradiction rejects the stage or the complete source admission;
   it is never hidden by a declared board capability.

Discovery runs once for the multi-source service. Planning is source-local and
the selected plan records `worker_provider` plus a `capture/source` stage.
Every discovered component, on every supported platform, publishes the
current-only `video-broker.capabilities.v2` contract: an explicit concurrency
mode (`exclusive`, `finite` or `shareable`), physical `concurrency_group` and
operation capacity. Hardware components are leased across source graphs by
physical group, not by backend label. Aliases through native, GStreamer, V4L2,
MPP or vendor APIs cannot multiply one engine's budget. An accelerator with no
attested capacity stays conservatively `exclusive` with one operation; a finite
capacity is consumed by the actual selected stages, not by a device-model
guess. Every transformed `live`, `analysis` or `archive` branch is represented
by its own role-keyed resize stage, so moving or warming one branch cannot hide
its incremental scaler/transform cost behind an unrelated analysis claim.
Non-exclusive CPU SIMD and software implementations are `shareable`.
If a native hardware lease is
unavailable, an `auto` policy may resolve the whole graph through the product
GStreamer worker and its permitted providers; native capture with portable
codec stages is not a valid mixed worker graph.

Discovery is per component and per operation, not per device model. `/dev/vpu`
does not imply MJPEG decode, `/dev/jpu` does not imply H265, and an OpenCL ICD
does not imply codec support. Likewise, a camera advertising H265 at 30 fps is
admitted only after the selected mode, cadence and archive bootstrap are seen.
An installed HBN/VSE worker remains only an inventory candidate for an HBN
source graph; it cannot be selected or attested by a V4L2 graph. Native and
portable workers emit the resize backend after processing a real analysis
surface, so VSE, NEON, `videoscale` and scalar CPU fallback remain distinct.

## Hardware families

- RDK X5: SP/VPU for H264/H265 and JPU for MJPEG decode, both with the vendor
  codec API's explicit 32-stream capacity; GC8000/OpenCL for the PAL,
  HBN/VSE for sensor-stack transforms, and NEON for the exact supported NV12
  resize ratios, including 1920x1080 to 1280x720 or 640x360. The BPU remains an AI-inference resource exposed to consumers; it is
  not used for generic tone mapping.
- Other supported Linux boards: every discovered hardware codec, GPU/OpenCL,
  image transform or capture accelerator receives the same contract. Unknown
  capacity remains `exclusive=1`; a higher finite value requires backend-local
  evidence and must be identical for every alias of the physical resource.
- Orange Pi/Rockchip: GStreamer MPP codec elements and, when installed and
  probed, RGA transform elements. RKISP/RKNN belong to source ISP and AI
  providers respectively.
- Raspberry Pi: libcamera owns CSI source/ISP selection; V4L2 stateless codec
  elements are used only when individually present, otherwise policy may admit
  software codecs.
- Generic Linux: GStreamer hardware elements are individually inspected;
  software codecs, `videoscale` and the product PAL are explicit fallbacks.

The product can add providers without changing its public stream protocol. A
provider must expose bounded discovery evidence, declare operations, emit
runtime selection telemetry and satisfy the existing frame contract.
Component recovery uses the same boundary: a source adapter explicitly
declares `in_place` only for an operation it can prove while capture and the
continuity backend stay live. Absence of that declaration means
`next_natural_epoch`; neither a device node nor a board family implies that a
running accelerator can be replaced safely.
