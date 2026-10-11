# Architecture

## Layers

```text
application integration
        |
        v  config.Config or routing catalog + opaque plan
public Service / daemon / status socket
        |  optional local routing control socket
        |
current-only routing transaction (prepare -> warmup -> admit -> frame -> cutover -> verify -> persist)
        |
component discovery -> capability plan -> runtime attestation
        |
graph policy and fan-out runtime
        |
quality + camera-control policy
        |
source and platform adapter contracts
   +-------+-----------+-------------+
   |       |           |             |
V4L2 UVC  V4L2 raw   vendor HBN   libcamera
   |       |           |             |
native RDK workers or portable GStreamer worker
        |
camera, decoder, NV12 processing, encoders
```

`config/` is the stable product graph. Schema v3 maps named streams to named
sources with a mandatory `source_ref`. `capability/` discovers local engines
once and resolves every source worker and media stage without application
identity. `internal/broker/` owns the multi-source manager, source-local
lifecycle, admission, queues, control, shared memory and status. `internal/source/` probes
sources and builds a worker command from a provider-neutral plan.
`internal/cameracontrol/` converts backend-specific control discovery into
neutral defaults and normalized residual envelopes. `quality/` owns the strict
observation contract shared by all workers, health and control policy. `native/`
contains the RDK X5 data plane. `protocol/` and `sdk/python/` are consumer
contracts. `cmd/` exposes the daemon and portable worker.

Application adapters may translate a larger system configuration into the
product schema, choose a source selector and express policy. They must not be
imported by this module, and their board catalogs are never capability evidence.
Operator-facing names and authorization also belong to those adapters. The
standalone routing core sees only catalog-owned option IDs and generic graph
deltas; it does not know whether a stream is consumed by WebRTC, a recorder or
an inference process.

Catalog preview has two explicit levels. `routing.PreviewGraph` is a pure
control-plane check suitable for offline tooling. `Service.PreviewRouting`
holds the same Store CAS boundary while a read-only backend validator checks
the exact running plan, graph identity, transition support, camera admission
and required hardware attestation. The validator receives detached data and is
forbidden to allocate, warm or mutate; apply still repeats all checks because a
preview is not a reservation.

The daemon projects that routing API over one bounded local Unix control
socket. Its current-only v2 contract contains only opaque catalog selections
plus a CAS revision and supports `read`, `preview`, asynchronous `apply` and
`rollback`, and `progress`. A daemon-owned operation survives the requesting
connection, serializes mutations and publishes each real transaction phase;
terminal results are retained in bounded memory. Read responses contain a safe
catalog projection with source IDs and admitted media options, never device
paths, endpoints, identities or hardware policy. The same inode-owned
publication primitive is shared by media, status and control listeners, so an
old generation cannot remove or silently replace a successor. Filesystem access
protects the local transport; user authentication, CSRF, localized labels and
role policy belong to the embedding application adapter.
No WebRTC, recorder, tracker or application type crosses into this boundary.

The central catalog may evolve independently from the operator plan. At daemon
startup only, the Store may reconcile a current-schema plan to a trusted newer
catalog when its complete active and rollback option sets remain admitted. The
single atomic plan replacement advances the CAS revision and all stream
generations before graph construction, because the old catalog definition is
not a valid basis for proving which media identities changed. Revocation,
stream-set drift or monotonic-counter exhaustion fails closed without changing
the persisted bytes. Read-only validation and runtime control requests retain
strict revision equality and never trigger this transition.

The public product surface is split by responsibility rather than by daemon:
continuous stream delivery is the media data plane; point-in-time
`Describe`/`Extract`/`Correlate` is the planned Frame Access Plane; status and
capability form the read-only technical metadata plane; catalog preview,
transactional apply, progress and rollback form the mutable control plane. One
authenticated application facade may expose all four contracts, but it cannot
merge their bounds or turn configuration into a media transport. Semantic
detections and identities remain consumer data; they correlate to broker frame
and epoch references instead of becoming broker-owned application concepts.

Source admission has two discovery strategies behind one runtime contract.
V4L2 adapters enumerate and fingerprint the physical node's advertised mode
catalog. HBN and libcamera use a deterministic fingerprint of their opaque
sensor selector and declared mode because those stacks own discovery. Neither
strategy is trusted on declaration alone: worker telemetry plus delivered
analysis/archive frames must prove codec, dimensions, cadence and codec
bootstrap before `camera_admitted=true`.

## Graph behaviour

The manager materializes one graph for every referenced source, in
deterministic source-ID order. Each graph owns exactly one capture worker and
up to one `live`, `analysis` and `archive` output. A role is absent when no
stream of that type is bound to the source. Processing, media epochs, camera
admission, component recovery and ephemeral state paths remain source-local.
Global discovery is shared. Every backend component has an explicit concurrency
mode. Hardware without a capacity attestation remains single-operation, while
components with a finite contract are leased by selected stage count across
source graphs. Shareable CPU SIMD and software paths do not acquire an exclusive
lease. This contract is platform-neutral; RDK, Rockchip, Raspberry Pi and generic
GStreamer inventories use the same allocator. Native, V4L2, MPP, vendor and
GStreamer aliases that reach one physical engine carry the same
`concurrency_group`; a contradictory group contract is rejected before capture.
Each source has its own supervisor. A startup or runtime failure releases only
that source's endpoints and hardware leases, publishes its bound streams as
degraded and retries with bounded exponential backoff. Other source graphs keep
their contexts, capture workers, epochs and progress. Parent cancellation or an
internal manager invariant is the only normal whole-service shutdown boundary.

For a native H265 UVC camera with `archive.content=adaptive`, PAL policy decides
the graph before the worker starts. With PAL enabled, one hardware decode
produces the corrected NV12 surface used by the H265 live track, the H265
archive and analysis, and the archive is
hardware-encoded back to H265. With PAL disabled,
the compatible compressed stream may remain native. The explicit `source`
policy is a whole-graph unconditioned-pixel contract and cannot be combined
with PAL.

The full-resolution correction still runs on every delivered frame. Only the
learning cadence for its cached zonal gain surface is resolution-bounded; this
separates scene adaptation cost from the 30 fps delivery contract.

The accelerator contract is output-invariant. `auto` prefers OpenCL, falls back
to the CPU renderer when the SDK, device or runtime path is unavailable, and
reports the selected backend. Explicit `opencl` fails rather than silently
changing the requested capability. On RDK X5 the normal UVC H26x graph combines
VPU decode, GC8000 OpenCL luma correction and hardware H264/H265 encode.

For MJPEG, YUYV or NV12 input, the broker obtains NV12, applies the selected
processing policy, produces the analysis view and uses independent persistent
H265 encoders for live and archive. A live bitrate or FPS change is applied to
the already-active live encoder without advancing its media epoch or gating
existing viewers, and therefore cannot disturb either live or archive
continuity. Only live activation creates a new epoch that requires an attested
decoder bootstrap. An explicitly configured H264 interoperability stream uses
the same isolated lifecycle contract.

The first hot-routing data-plane slice changes the FPS and bitrate of one live
output whose source, codec, geometry and endpoint remain unchanged. It
preserves the capture process and media epoch and requires the native adapter to
acknowledge the exact control sequence only after the encoder has published a
non-empty access unit. The transaction verifies a fresh codec bootstrap and
frame before persisting its opaque plan. A mismatched, stale, superseded or
wrong-process receipt causes an attested rollback.

The complete-source transaction moves all outputs of one retiring source to a
previously inactive source. The candidate acquires its own source-local leases
and starts on private endpoint names. Camera identity, every selected hardware
stage, fresh frames and compressed bootstrap are proven before cutover. Each
logical Unix socket or shared-memory pathname is then replaced atomically by
inode identity; the previous producer keeps a private link until persistence so
a failure after any individual replacement can restore the old endpoint without
reopening its camera. The prepared graph is adopted by the normal supervisor
without a second start. Peers outside the exact delta retain supervisor, PID,
media epoch, endpoint and progress. Existing stream clients reconnect through
the unchanged logical pathname; the shared-memory SDK detects the inode change
and rejects the old producer epoch.

The routing coordinator now classifies subset moves between sources that both
remain active as a dedicated preserved-owner fan-out transaction. It freezes
the affected supervisors in deterministic source order, records every capture
PID/media epoch and peer identity, delegates the normal prepare/warmup/admit/
first-frame/cutover/verify/rollback phases, and refuses retirement if any
capture owner changed. The adapter boundary explicitly requires incremental
capability reservation and branch warmup without reopening a camera. The
portable GStreamer worker implements that contract with isolated role
processes. Native HBN implements the same contract with role-owned in-process
resources over one capture/VSE owner and catalog-opened surfaces; candidates do
not enter the mux until cutover and incumbents survive until retire. Native
V4L2 implements the same controller with source-owned capture/decode and
role-owned buffers, cadence and VPU encoders. It advertises support only with a
complete source ID/media-epoch fence, waits asynchronously for candidate frame
and codec-bootstrap evidence, and preserves the incumbent when candidate
processing or post-cutover verification fails.
Every platform adapter must satisfy the neutral readiness contract; board or
application rules do not enter the routing coordinator.

The shared capability allocator provides a staged transition lease for that
adapter boundary. A backend submits the exact current, candidate and warmup-only
plans. The allocator verifies current ownership, accounts the peak overlap of
old and candidate branches against each physical `concurrency_group`, and keeps
that capacity unavailable to other source starts. `Commit` atomically replaces
the affected source claims; `Abort` releases only the temporary reservation.
The mechanism is stage-agnostic and therefore applies equally to codec, JPEG,
GPU, scaler/transform and future inference components. Transform requirements
are role-keyed: `live`, `analysis` and `archive` each consume an independently
visible stage when their geometry or orientation requires work. This prevents
an attach from being admitted against a single process-global resize claim.

The corresponding private worker boundary is a strict JSON-line command and
receipt protocol bound to source ID, transaction, sequence, capture PID and
media epoch. Prepare carries only concrete branch media profiles; device paths,
logical endpoints and accelerator selectors are structurally absent. Subsequent
phase commands carry only the identity fence, while first-frame receipts prove
branch epoch/frame and complete H264/H265 bootstrap where applicable. This
contract is shared by all adapters, but support is advertised only after a
worker implements the complete state machine.

The publication side of that transaction is role-local. Candidate live/archive
Unix listeners and analysis socket/shared-memory/notifier state are built on
private paths. A transaction-owned delivery gate is installed only on changed
roles before worker cutover; their logical inodes are then replaced atomically,
their clients reconnect, and publication resumes only on the candidate plan.
Verification requires a frame newer than the cutover baseline and a complete
encoded bootstrap. Rollback gates those roles again before restoring both the
incumbent worker branches and inode identities. Other roles and sources retain
their clients, counters and publication progress. Normal runtime attestation
may advance during warmup without being discarded; a changed component or
fallback is a conflicting hardware transition and stops cutover.

Provider geometry and memory layout are separate contracts. The RDK H265
encoder requires 16-pixel width and 8-pixel height alignment, including for a
fallback capture mode. After the codec dequeues an input surface, the broker
copies every NV12 row using the luma and chroma strides reported by that
surface and initializes alignment padding. It never assumes that logical
width is the physical row stride.

For vendor HBN, the sensor stack supplies role-sized NV12 channels. A routed
service retains the complete central-catalog profile universe even for inactive
sources and reduces it to unique, encoder-aligned surfaces. The bridge
preconfigures up to the five output resolutions exposed by
[`sp_open_camera_v2`](https://developer.d-robotics.cc/rdk_x_doc/en/Basic_Application/multi_media_sp_dev_api/RDK_X3/cdev_multimedia_api_x3/vio_api);
catalogs requiring more fail before capture opens. Passing these surfaces is a
prerequisite for later branch warmup and does not by itself advertise runtime
fan-out support. Live and archive always own distinct encoder instances,
cadence and codec state, so a role-local cutover cannot mutate or retire its
peer.

The native worker mux uses a measured Linux pipe capacity and the broker reuses
size-classed ingress buffers. Raw analysis frames then enter a bounded
latest-frame dispatch queue. Shared memory is written directly from that
payload; a complete `NMB2` packet is allocated only when an analysis socket
consumer is actually connected. This keeps garbage collection and a portable
diagnostic consumer from becoming capture or archive backpressure.

## Tone mapping

The tone mapper changes only the NV12 Y plane. Sparse robust histograms are
measured on a configurable grid. Each tile receives a bounded monotonic curve;
curves are spatially smoothed and bilinearly interpolated into a cached compact
gain-index surface. A process-wide quantized curve bank then reduces steady
frame rendering to one monotonic lookup per luma sample; the spatial surface is
rebuilt only when statistics change. Temporal inertia and mode hysteresis
prevent pumping. Shadow curves combine a contrast-preserving exposure lift with
a protected-toe perceptual midtone lift: retained texture becomes intelligible
without turning quantized code-zero regions into a grey veil. Chroma and
code-zero black are preserved, and a low-information noise guard refuses to
amplify sensor noise.

An optically non-informative scene is reported separately from data-plane
health. Darkness, a covered lens or saturation never changes a healthy capture
graph to `degraded` and therefore cannot roll back a release. Missing or stale
quality-monitor evidence still degrades the graph because the configured
processing observer itself is then unavailable.

`auto` begins globally and enters local mode only after repeated evidence of a
large spatial luma spread. It returns to global mode more slowly. This makes a
bright window and dark room independently recoverable without trusting a
camera-specific ISP control.

The native NV12 V4L2 and HBN workers and the portable product-owned GStreamer
`videobrokertone` element implement the same curve contract. The plugin runs once
before the portable raw tee, then role-specific resize/encode branches consume
that optimized surface.

The quality state file is an atomic, ephemeral
`video-broker.quality.v3` snapshot in the runtime directory. It contains three
non-interchangeable domains: `source` before broker processing, `processed` at
full output resolution and `analysis` after consumer resize. Camera feedback
is permitted to read only `source`; health also classifies the optical input
from `source`. Comparing `source` and `processed` measures PAL effectiveness
without closing the controller around its own output.

Each domain includes a fixed coarse 4x3 observation grid measured in the same
sample pass as the global metrics. Camera control uses only the `source` grid to
detect a clipped window or shadow region that a global average would hide. The
grid is deliberately independent of the local tone mapper's finer working grid,
so CPU, OpenCL and future platform backends share one public contract.

The snapshot is flushed to the kernel before rename but deliberately not
`fsync`ed: durability is irrelevant to a replaceable observation, while a
storage barrier in the capture thread would introduce visible cadence jitter.

## Camera control

The camera-control policy never imports a board catalog, VID/PID table or
application identity. USB/UVC `auto` deliberately performs no control-endpoint
I/O: it neither probes nor writes brightness, exposure, white balance or related
firmware controls. The module owns AE/AWB and the broker corrects the acquired
surface through its global or zone-aware post-ISP tone pipeline.

An operator may select `configured` to apply an explicit module calibration.
That profile is validated as a whole, applied exactly once before `STREAMON`,
aborts on the first write failure and must verify without drift. It is never
reconciled against a live UVC stream. Native CSI/ISP adapters may expose a
separate bounded residual controller without changing quality policy or
consumers.

## Failure boundaries

- Invalid graphs fail before capture starts.
- A source is admitted only after measured cadence and codec bootstrap.
- Root-owned camera lifecycle remains outside the product runtime. The
  standalone probe and enrolled VID/PID reset policy are operator diagnostics,
  not automatic updater or broker actions. A physical reset is permitted only
  at an explicit quiescent platform boundary; the real candidate graph itself
  must prove `STREAMON` and frame progress during the health trial.
- Runtime mode quarantine applies only to V4L2 sources with a discoverable
  mode catalog; a sensor-stack failure never invents a V4L2 fallback. A named
  V4L2 source may follow `/dev/videoN` renumbering only across candidates with
  the same sysfs identity as its canonical source path. Foreign source
  identities are rejected before capability I/O and can never become a retry.
- Cadence watchdogs are role-owned: a source without `analysis` cannot be
  degraded by missing analysis frames. For asynchronous MJPEG/JPU decode, every
  captured access unit reaches the bounded latest-frame worker; only completed
  analysis outputs are cadence-capped on a monotonic clock, exactly once.
- Runtime failure telemetry degrades only the selected component. CPU fallback
  is temporary continuity, never a stable terminal graph when the hardware
  exists. An adapter-declared in-place retry keeps capture, V4L2 buffers and
  CPU decode alive while a copied access unit is proved asynchronously. A
  token/PID/epoch-bound ready receipt requires a second broker commit before
  the candidate can switch after `QBUF`; cancel retains CPU. Other adapters
  retry only at the next independently occurring source epoch; accelerator
  preference never forces a camera reopen.
- Requested decoder policy and initial backend are separate, mandatory inputs
  to the native adapter. During cooldown the graph remains `auto` but starts on
  CPU, preserving same-epoch JPU reconvergence. Active MJPEG hardware decode is
  a one-slot latest-frame worker: DQBUF/copy/QBUF never waits for vendor input,
  output, stop or deinit. The watchdog measures only the age of the first input
  still lacking output; camera input gaps do not age decoder work. A bounded
  post-admission output watchdog returns only
  `auto` plus software fallback to CPU; strict JPU rejects the worker epoch.
  One failure starts a 30-second cooldown; only three consecutive failures mark
  the component rejected, retries continue with exponential backoff capped at
  five minutes, and five healthy hardware minutes clear the history. Independent
  hardware stages remain admitted. No board or camera identity participates in
  this decision. Recovery entries whose exact stage and normalized format leave
  the current graph are removed, including every child/token/epoch identity.
- V4L2 setup has a two-phase boundary. Negotiation, REQBUFS, QUERYBUF and MMAP
  happen first; decoder initialization, asynchronous workspaces and encoder
  preheat then complete while streaming is off. Only the final QBUF/STREAMON
  transition precedes the capture loop, and cleanup accepts a never-streamed
  mapping without issuing STREAMOFF.
- A worker exec does not reset restart history. Only full camera admission and
  healthy delivered frames clear it, so post-Start S_FMT/USB failures build
  bounded exponential backoff. Repeated UVC failures quarantine
  the exact fingerprinted mode without resetting or re-enumerating the device.
- Per-consumer queues are bounded. Live and analysis can drop; archive clients
  are disconnected rather than allowed to stall capture indefinitely.
- Every socket producer binds a private inode and publishes the configured
  logical endpoint as a same-inode hard link. A live incumbent, symlink or
  non-socket occupant is rejected. Teardown removes only names still bound to
  that exact inode, establishing the ownership primitive needed for a later
  atomic producer cutover without allowing an old generation to unlink the new
  one.
- Native telemetry reports both average and maximum capture-work, processing,
  encode and output times so camera gaps are distinguishable from data-plane
  backpressure.
- Runtime status distinguishes discovered candidates, selected stages and
  stages attested by decoder telemetry or delivered media frames.

## Extension contract

A new camera family implements `BrokerMuxAdapter` and, only when it exposes
controllable image parameters, a camera-control backend. A new accelerator
contributes a discoverable worker/plugin or system-media provider. Every worker
publishes the same quality contract and private worker mux. Presence creates
only a candidate; real frames attest the selected operation. No consumer or
camera-control policy changes are required.
