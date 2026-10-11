# Consumer protocols

All integer fields are network byte order. Magic values are opaque wire tokens;
their spelling is retained so existing packet recordings remain readable.

## Stream socket packet (`NMB2`)

```text
magic[4] | version:u8 | stream_id:u8 | flags:u16 |
frame_id:u32 | timestamp_ns:u64 | width:u32 | height:u32 | payload_size:u32
payload[payload_size]
```

The header is 32 bytes, version is `1`, and payloads are bounded to 16 MiB.
Stream IDs describe media encoding: `1=H264`, `2=NV12`, `3=H265`. Endpoint
selection gives the role (`live`, `archive` or `analysis`). Flags are
`1=keyframe`, `2=codec configuration`, and `4=stream reset`.

Every newly attached encoded consumer receives `stream reset` on its first
delivered packet, whether that packet comes from the retained bootstrap or
arrives later through the live queue. It marks a publication boundary: source-
local frame counters must not be compared across it. The marker does not recur
on later packets of the same attachment and does not waive codec-bootstrap,
freshness, or within-publication continuity checks. NV12 delivery uses the
shared-memory epoch contract below and is not given this encoded-stream marker.

Role and codec are orthogonal. A `live` endpoint normally carries H265 and may
carry H264 only when its stream policy says so; `archive` has its own codec
contract. Consumers must bind the declared endpoint and reject a different
stream ID instead of switching roles or codecs implicitly.

The declared endpoint is a real Unix socket hard link to a private
producer-owned inode, never a symlink. Startup rejects a live incumbent and any
non-socket object. Cleanup unlinks a pathname only while it still resolves to
the producer's recorded inode, so retirement or delayed teardown cannot remove
a successor that has acquired the logical name.

Each new encoded client receives a complete codec bootstrap followed by a
keyframe before dependent pictures. Consumers must reconnect after a protocol
or geometry violation; there is no permissive legacy framing.

Go consumers use `protocol/`. Python consumers use
`sdk/python/video_broker_sdk`.

## Analysis shared memory (`NMSH`)

The 64-byte version-2 header contains magic, version, stream ID, flags, two
sequence counters, frame ID, timestamp, geometry, payload length, capacity and
a non-zero producer epoch. The writer marks an odd sequence, writes the
payload, then publishes matching even counters. A reader accepts only matching
even sequences before and after copying the NV12 payload.

The producer epoch changes on every capture-child start, including restarts
that keep the same pathname and inode. Producers may also atomically replace
the shared-memory file between trial and stable runtimes. Consumers must bind
both the epoch and `st_dev:st_ino`, recheck the pathname before every read and
close an unlinked/replaced mapping before admitting a new one. During a
recreate gap they return no frame; reading the deleted inode as a fallback is
forbidden. The Python SDK implements this lifecycle in
`UnixSharedMemoryMapping` without rebinding the independent notification
socket.

This is the preferred steady-state transport for high-rate local AI. The
analysis stream socket carries the same pixels and remains fully supported,
but its framed packet requires an additional materialization and is intended
for portability, inspection and consumers that cannot map shared memory.

## Frame notification (`NMFN`)

An optional 24-byte Unix datagram contains magic, version, stream ID, flags,
frame ID and timestamp. It is only a readiness hint; shared memory remains the
source of truth and notifications may be coalesced or lost.

## Native decode pipe (`NHD2`)

`native/H26X_DECODE_PROTOCOL.md` defines the bounded correlation protocol used
by hardware decode helpers. It is private to the platform data plane and does
not change the public stream socket contract.

## Native worker control (`NMC2`)

The broker writes one current private control envelope to a native worker's
stdin:

```text
NMC2 seq=<u64> main_active=<0|1> main_fps=<int>
main_bitrate_kbps=<int> sub_fps=<int>
optional_compute_admitted=<0|1>
retry_action=<none|trial|commit|cancel>
retry_component=<none|component-id> retry_token=<u64>
retry_deadline_unix_ms=<i64>
```

The physical line contains those fields in that exact order. `none` requires
component `none`, token zero and deadline zero. `trial` and `commit` require a
component, non-zero token and positive deadline; `cancel` carries the exact
component/token with deadline zero. Tokens are monotonic for the broker process
and are bound by status to the running child PID and media epoch. Rate controls
may be coalesced, so the broker repeats the current phase until the worker
acknowledges it. There is no reader for an earlier worker-control envelope.
For the RDK X5 V4L2 worker, `optional_compute_admitted=0` immediately stops
optional image conditioning and releases its accelerator context; a later `1`
recreates the explicitly configured accelerator fail-closed before processing
resumes. The state bit therefore describes an applied actuator, not merely a
broker-side recommendation.

The worker startup capability line also ends with
`fanout_control_mode=<disabled|branch_transaction_v1>`. This field is mandatory
and exact: its absence is not interpreted as compatibility with either mode.
`branch_transaction_v1` is admitted only when the broker opened the same
child's stdin control channel; otherwise the runtime contract is rejected.
Backend type, board family and ordinary `NMC2` support never imply dynamic
fan-out. Portable GStreamer, native HBN and native V4L2 emit
`branch_transaction_v1` only with a complete broker-supplied source/media
fence; every unfenced start emits `disabled`.

An adapter may declare `in_place` only when it can keep capture and the software
continuity path active while proving a hardware candidate with a real frame.
RDK X5 MJPEG separates the requested policy from its initial backend. The
broker must pass both `--mjpeg-decode-accelerator` and
`--mjpeg-decode-initial-backend`; omission is invalid. Both selectors are
case-sensitive and accept only `auto`, `jpu`, or `cpu`; the initial backend
cannot be `auto`. An explicitly supplied `--allow-sw-fallback` accepts only
`true` or `false`. Missing values, aliases, and malformed tokens are rejected
instead of inheriting a default. This lets an `auto` policy start on CPU during
cooldown without disabling its JPU trial workspace.

The native worker copies each bounded MJPEG access unit and returns the V4L2
buffer immediately. An admitted JPU consumes a one-slot latest-frame queue;
replacement is counted and no vendor decode, stop or deinit runs on the capture
thread. Its output watchdog starts at the first submission not yet acknowledged
by decoder output and is not renewed by latest-frame replacement; a camera input
gap therefore cannot be misclassified as a JPU stall. If pending output stalls
for a bounded post-admission interval, `auto` with
software fallback returns to CPU in the same PID/media epoch while asynchronous
codec teardown completes. Strict JPU is fail-closed. A trial uses a separate
copied access unit while TurboJPEG and capture continue. It first emits a token
and immutable completion timestamp. Only an exact, still-live broker `commit`
authorizes a frame-boundary switch after `QBUF`; the committed hardware-frame
receipt opens half-open state. Timeout or supersession yields `cancel`, closes
the candidate asynchronously and keeps/returns to CPU. TurboJPEG remains
available until the broker accepts the committed receipt, so even a delayed
stderr pipe cannot leave native and broker state divergent. Adapters without
this operation use
`next_natural_epoch`; component recovery never creates a camera reopen merely
to test an accelerator. Recovery state is current-graph state: when the selected
plan no longer contains the exact stage and normalized format, its entry and all
trial identities are discarded. A later graph using that operation starts a new
bounded recovery history.

For V4L2 sources, device negotiation, buffer mapping and all decoder/encoder
initialization and preheat complete before buffers are queued and `STREAMON` is
issued. `QBUF` plus `STREAMON` form the final transition into the capture loop;
cleanup is valid both before and after that transition.

## Worker fan-out transaction

Dynamic output branches use a separate current-only pair of bounded JSON-line
schemas: `video-broker.worker-fanout-command.v1` from broker to worker and
`video-broker.worker-fanout-receipt.v1` from worker to broker. This is not an
operator API and does not replace `NMC2` resource/rate control.

Every command is bound to a canonical transaction ID and source ID, a monotonic
sequence, the exact capture PID and non-zero media epoch. `prepare` contains one
to three uniquely sorted changes for `analysis`, `archive` or `live`. Actions
are `attach`, `detach` or `replace`; attach/replace carry a concrete media
profile while detach carries none. Profiles contain only codec or pixel format,
geometry, cadence, bitrate, content and rate control. Device paths, publication
endpoints, providers, accelerator choices and application identities cannot be
represented. Later `warmup`, `admit`, `first_frame`, `cutover`, `verify`,
`rollback`, `abort` and `retire` commands carry only the process/transaction
fence. Every phase has exactly one matching receipt event; readiness cannot be
borrowed from an earlier phase.

Receipts repeat that complete fence and use a phase-specific event. A
first-frame receipt reports a non-zero branch epoch and frame ID for exactly
the candidate branches declared as `attach` or `replace` by that source's saved
prepare set, in canonical role order. A detach-only source has no candidate
branch and therefore returns an empty evidence set; missing, extra or wrong-role
evidence is rejected. Encoded live/archive evidence is valid only with
`codec_bootstrap=complete`; analysis requires `not_applicable`. Rejections carry
only a bounded canonical error code, never worker/device free text. The broker
matches the receipt to the exact command and child pipe and then to the saved
prepare set; a syntactically valid receipt from another transaction, source,
PID, epoch or sequence is rejected.

Every candidate branch also carries the required `runtime_mode` produced by the
worker that actually constructed it. Analysis uses the empty value because it
does not claim an encoded delivery path. Archive admits only
`native_passthrough`, `source_hardware_encode`,
`processed_hardware_encode` or explicitly authorized `software_encode`; live
admits only `persistent_encoder_epoch`, `native_passthrough_static` or
`backend_static`. The broker cannot infer these values from a platform or
hostname. It binds them before publication first-frame readiness, installs them
in status in the same locked cutover as plan/endpoint ownership and restores
the incumbent values on rollback. Missing, unknown or role-incompatible modes
fail before cutover.

Each line is at most 4096 bytes, has one LF terminator, rejects CRLF, unknown
fields, aliases and concatenated JSON. No reader for a prior schema exists.

`first_frame` and `verify` may remain pending while a capture loop advances.
The worker retains the exact command and sequence and emits no provisional
receipt; a later frame resumes that same phase. A second fan-out command during
the wait is rejected without consuming its sequence. Candidate-only processing
failure rejects the phase while preserving the incumbent, and failure of a
newly cut-over generation rejects verification so the broker can roll back the
same role without restarting the capture owner.

The broker compiles the validated graph delta separately for every affected
source in canonical `analysis`, `archive`, `live` order. An absent-to-present
branch is `attach`, present-to-absent is `detach`, and a changed present branch
is `replace`. `attach` and `replace` carry the concrete candidate media profile;
`detach` carries none. Archive codec `auto` must already be resolved by the
admitted capability plan. The compiler rejects capture-contract changes and
cannot serialize source devices, socket/shared-memory paths or providers.

For a phase spanning multiple source owners, the broker first validates every
command and compares every source/PID/media-epoch tuple with the transaction's
frozen handles. It performs no write until all participants pass. Commands are
then dispatched concurrently, and results are folded in canonical source order
so cutover coordination and failures do not depend on map or scheduler order.
The low-level sender refuses to retarget a phase to a replacement child even if
that child now owns the same logical source.

The broker-side transactional backend composes that worker phase set with the
shared hardware allocator and a publication coordinator. Preview executes the
same peak-capacity admission without retaining a reservation. Prepare first
acquires the finite hardware reservation, creates private publication state,
and only then sends worker `prepare`. Each later worker receipt precedes its
matching publication phase. The reservation is committed only after worker and
publication retirement both succeed; failed or unproven cleanup keeps capacity
reserved rather than silently undercounting a possibly live hardware branch.
The concrete publication coordinator stages changed Unix sockets, analysis
shared memory and notifiers on private paths. Before worker cutover it gates
only affected roles; after the worker receipt it replaces only their logical
inodes, disconnects only their clients and requires a post-cutover frame plus a
complete H264/H265 bootstrap where applicable. Rollback reinstalls the same
gates before restoring worker branches and incumbent inodes. Runtime
attestation state may advance during warmup and is preserved on rollback, but a
component or fallback change invalidates cutover. The production backend is
installed in the Service and delegates only after the running worker has
attested the exact control mode.

Portable GStreamer and native HBN/V4L2 implement the complete branch phase
machine and real first-frame receipts. HBN reduces the routed catalog to at most
five unique, encoder-aligned VSE surfaces and opens them once with the capture
owner. V4L2 keeps capture, decoder and role resources address-stable while
candidate branches warm beside incumbents. Warmup creates isolated role
resources, first-frame proves NV12 or complete H264/H265 bootstrap, cutover
moves only the affected role slots, verify requires later output, and rollback
retains the incumbent until retire.

When a stream moves from a running source to a previously inactive source, the
broker composes two current-only transactions. It first starts and admits the
new capture owner on private endpoints. The existing worker then detaches only
the moved branches; the broker replaces their logical endpoints with the
already-warm new source and leaves all remaining branches, PID and media epoch
untouched. Rollback restores endpoint ownership while incumbent branches are
still detached, then reattaches them before clearing the gates. Retirement
adopts the new supervisor and updates the preserved supervisor graph instead of
stopping it. Unrelated source owners never enter the transaction.

## Resource state

When enabled, the broker atomically writes
`video-broker.resource-governor.v1`. It reports thermal/BPU inputs, attested
capture/archive media, effective live and analysis caps, and
`optional_compute_admitted`. Integrations may map that generic admission bit to
their own deferred workloads; they must validate the exact schema.

The configured `runtime.governor.state_path` is a namespace root, not an
implicit camera selection. Each capture owner publishes source-local telemetry
under `sources/<source-id>/<basename>`. The broker additionally publishes one
regular, single-link snapshot for every named stream under
`streams/<stream-id>/<basename>`. A stream publication always reflects the
source selected by the active graph and follows transactional Cutover/Rollback;
candidate sources cannot write it during warmup. Consumers must bind to the
named stream publication. There is no process-global primary-source file or
camera-count special case.

## Quality state

When configured, each capture backend atomically publishes
`video-broker.quality.v3`. The JSON snapshot contains common luma, clipping and
detail metrics for three domains:

- `source`: immutable pixels immediately after source decode and before PAL;
- `processed`: full-resolution pixels after broker image conditioning;
- `analysis`: conditioned pixels after the analysis resize.

`capture.format` and `capture.orientation` identify the observed transport,
without embedding a camera model, board name or application identity. Missing,
stale or structurally incomplete snapshots are rejected; there is no reader
for earlier contracts. Every domain also carries the same fixed, row-major 4x3
`spatial` grid. It exposes local black/white clipping to camera control without
leaking the tone mapper's internal grid or an accelerator-specific layout.

## Status socket

The local status socket returns one bounded `video-broker.status.v5` JSON line.
It is a daemon-lifetime, owner-only control endpoint with exactly one pathname;
unlike media sockets, it is never part of a routing cutover publication.
Its top-level `sources` and `streams` objects are keyed by the exact schema-v3
IDs. Every stream repeats its `type` and `source_ref`; consumers must resolve
capture ownership through that binding rather than infer it from an endpoint
name. A source entry owns capture process identity, admission, codec
attestation, media epoch, processing evidence, capability inventory/plan and
hardware recovery. A stream entry owns delivery codec, cadence, clients,
frames, epoch, drops and disconnects. The aggregate `state` is the most severe
source state and never replaces inspection of a required named source/stream.

Each source `state` describes only graph health: source process, codec attestation, frame
freshness and required processing evidence. Each stream's `performance` reports
`requested_fps`, the current producer `target_fps`, `observed_fps`, `window_ms`
and `measured_at`. Its state is `inactive`, `measuring`, `on_target` or `reduced`.
Delivery below 90% of the current target for a watchdog interval produces a
warning; recovery needs at least two measurement windows (four seconds) above
95%. These are diagnostic warning bands, not admission or profile-qualification
thresholds. An intentional live adaptation can lower `target_fps` without
changing the persisted preference or representing a source fault.
`optical_signal_state` separately
reports `unmonitored`, `pending`, `informative`, `non_informative` or
`unavailable`. A dark, covered or saturated scene is `non_informative` while a
healthy graph remains `running`; consumers may alert on scene quality without
mistaking it for a transport failure.

Source `media_epoch` changes on every capture-child start. Each stream has its
own `epoch`, exact `frames` counter and `last_frame_at`; the analysis epoch is
the exact NMSH producer epoch. Release gates must observe progress on every
required named stream inside their stability window; non-zero historical totals
alone are not readiness evidence.

Live and archive socket streams are produced directly by the capture child and
therefore repeat its `media_epoch`. Analysis deliberately does not: its stream
epoch is the independently randomized NMSH producer epoch, which rotates on
every child start and is matched to the tracker and shared-memory header. A
release gate must bind analysis through `source_ref` and that NMSH consumer
identity; numerically equating it to the capture epoch would reject every real
shared-memory producer.

Compressed capture decoding is attested as one atomic group:
`capture_decode_codec`, `capture_decode_requested`, `capture_decode_backend`
and `capture_decode_fallback`. The fields are absent when no capture decoder is
used. On RDK X5 MJPEG input they distinguish JPU hardware decode from an
explicit bounded CPU fallback; consumers must reject partial or internally
inconsistent groups instead of inferring acceleration from the source codec.

`capture_child_pid` and `capture_child_running` identify the current capture
owner. `hardware_recovery` is always present. Each entry binds a failed
component to its stage/format, consecutive-failure count, retry scope and
causal state. States are `cooldown`, `retry_due`, `trial_requested`,
`next_natural_epoch` and `half_open`. `trial_requested` additionally carries a
non-zero `retry_token`, exact `trial_media_epoch`, `trial_child_pid` and bounded
`trial_deadline`; `half_open` carries `stable_after`. An empty list is the only
stable state.

`fanout_control_mode` reports the exact runtime worker declaration and
`fanout_control_supported` is true only for an admitted
`branch_transaction_v1` declaration on the owned child control channel. An
empty mode means that no current capability report has yet been admitted;
`disabled` is an explicit negative capability. Consumers must not infer support
from transport, board, accelerator or process liveness.

CPU decoding remains continuity-only while recovery is non-empty. Cooldown is
30 seconds with exponential backoff capped at five minutes. One failure does
not reject the component; three consecutive failures do. A rejected component
still receives a bounded trial. The transition to `half_open` occurs only
after the exact worker acknowledges hardware selection and produces a hardware
frame. Exit, epoch/PID change, timeout or fallback aborts/rearms the trial.
Five minutes of exact-child hardware operation plus subsequent frame progress
clear the failure history. No component retry cancels the capture graph.

`capability_inventory` is the shared component discovery snapshot and
`capability_plan` is the selected source-local graph. The plan carries the
complete `worker_provider` and an explicit `capture/source` stage in addition
to codec and processing stages. Component and stage states are
`candidate`, `attested` or `rejected`. A candidate means that bounded local
evidence exists; it is not a claim that real media has traversed the component.
Capability inventory schema `video-broker.capabilities.v2` is current-only.
Every inventory component publishes `concurrency_mode` as `exclusive`, `finite`
or `shareable`, a physical `concurrency_group`, and
`max_concurrent_operations`. `exclusive` is exactly one, `finite` is positive
and consumed by selected stage count across source graphs, and `shareable` uses
zero with an empty group because it has no exclusive lease budget. Components
from different backends that address the same physical engine publish the same
group and the same contract; inconsistent aliases invalidate the inventory.

## Runtime routing control socket

When enabled, the daemon publishes one local Unix stream socket using the
`video-broker.routing-control.v2` current-only contract. This is the standalone
control plane for a prevalidated routing catalog; it is not a media endpoint
and never accepts device paths, codecs, accelerator selectors or unrestricted
numeric values from its caller.

Each connection carries exactly one newline-terminated JSON request and one
newline-terminated JSON response. Requests are limited to 64 KiB, the daemon
admits at most eight concurrent clients and every action has a bounded
deadline. Mutations run in a daemon-owned context rather than the connection
context, and exactly one mutation may run at a time. The supported actions are:

- `read`: return the plan and a safe path-free catalog projection only when
  persisted and running routing authority are identical;
- `preview`: validate one complete opaque selection set against the catalog,
  current CAS revision and the running data plane without mutation;
- `apply`: accept an asynchronous runtime transaction that persists only after
  media readiness is verified;
- `rollback`: asynchronously apply the single bounded rollback selection
  retained by the current plan and create a new monotonic revision;
- `progress`: return the live or terminal snapshot of an operation ID,
  including its actual transaction phase, sanitized error and resulting plan.

Example request:

```json
{"schema":"video-broker.routing-control.v2","request_id":"apply-42","action":"apply","expected_revision":7,"selections":{"live":"front-fluid","analysis":"overview-balanced","archive":"front-retention"}}
```

The accepted mutation returns an operation snapshot immediately. Polling uses:

```json
{"schema":"video-broker.routing-control.v2","request_id":"progress-43","action":"progress","operation_id":"apply-42"}
```

The response repeats the schema and valid request ID, reports `ok`, and carries
a plan/catalog view, preview, operation, or sanitized error with stable `code`
and optional failure `phase`. Operation phases are `validate`, `prepare`,
`warmup`, `admission`, `first_frame`, `cutover`, `verify`, `persist`, `retire`,
`abort`, `rollback`, and terminal `complete`. Detailed backend errors stay in
the daemon journal. A stale revision is rejected; preview is never a
reservation, terminal history is bounded, and operation request IDs cannot be
reused during a daemon generation.

A complete operator-requested evacuation may retire a source whose exact
current graph still owns the incumbent publications but whose capture worker is
temporarily unavailable. Only the admitted destination participates in worker
prepare, warmup, admission, first-frame, cutover and verify; the unavailable
source must have no candidate role and is stopped after persistence. Recovery
of that retiring worker during the transaction invalidates the frozen snapshot
and aborts fail-closed. This rule does not select a fallback automatically: the
caller must still submit a complete catalog-bounded selection set and current
CAS revision.

The socket is published with the same inode-bound ownership rules as media and
status sockets. Filesystem ownership is the local transport boundary. An
embedding product is responsible for user authentication, authorization,
CSRF protection, localization and audit before relaying an allowed choice to
this socket. Those application concerns do not enter video-broker.

Catalog replacement is a daemon-start operation, not a control-socket action.
If the trusted central catalog revision changes, the daemon keeps a current-v2
operator plan only when every active and rollback opaque option remains valid.
It atomically records the new catalog revision, advances the plan CAS revision
and every stream generation, and only then starts the graph. A revoked option,
stream-set drift or counter exhaustion leaves the old plan unchanged and stops
startup. Ordinary reads, preview/apply/rollback and `--check-config` never
perform this reconciliation implicitly.

## Qualified acquisition modes and output profiles

The current routing catalog optionally contains `source_modes`, a map from
declared source ID to opaque mode ID and an exact `{format,width,height,fps}`
tuple. These are qualified alternatives supplied by the integrator, not the
Cartesian product of camera enumeration results. The base `sources[id]`
configuration remains the recommended default. Alternative IDs cannot be
`default`, duplicate another tuple, change device identity, controls, paths,
provider/accelerator policy or safety limits. Qualification belongs to the
specific source and its inherited platform/provider policy, not to all boards.

The safe control-catalog projection includes every source's `source_modes`,
including the reserved `default` entry. Each output option also includes
`source_modes`, the IDs compatible with that complete output profile. Static
compatibility is not live hardware admission: preview resolves the entire
candidate against available providers, leases and concurrent reservations,
without opening cameras or modifying those leases. Apply repeats validation.

`preview` and `apply` accept optional `source_selections` alongside the complete
output `selections`. Its keys are source IDs and values are qualified mode
IDs. Missing/null retains current acquisition selections; an explicit `{}`
resets all sources to their catalog default. Individual defaults are represented
by absent entries, never by a persisted `default` string. Preferences for
inactive sources are retained. Example:

```json
{"schema":"video-broker.routing-control.v2","request_id":"mode-42","action":"apply","expected_revision":7,"selections":{"live":"front-hd","analysis":"front-analysis","archive":"front-archive"},"source_selections":{"front":"qualified-hd-20"}}
```

The current v2 plan persists `source_selections` and its single-step
`rollback_source_selections` together with output choices. Empty override maps
are canonicalized to absence; this is a default-overlay rule in the current
contract, not a legacy reader or another operational configuration. Catalog
startup reconciliation validates both current and rollback mode references;
revoked choices fail explicitly rather than silently selecting another camera.

A capture-format/geometry/FPS change, or a structural output resize/reformat,
uses an exclusive-owner transaction. Its order is validate/prepare, read-only
warmup, **cutover**, admission, fresh frame, verify, persist and retire. All
affected owners stop and join before replacements start; all their consumers
advance generation, including unchanged outputs sharing that capture. Other
sources keep their owners. On failure, the previous graph is restarted and
attested with fresh media using the bounded rollback context. Failure to recover
is reported as degraded, never as a successful commit. This necessarily has a
brief interruption; it is not the concurrent warmup used by compatible fan-out
changes. No fallback provider or software transcode is implicitly enabled.

Temporary live demand exists only for media admission and is released on
commit or rollback. Current thermal limits still cap that demand, and source
startup samples temperature before launching its first capture child. An
inactive-source preference alone persists without starting a capture owner.
The contract is generic for N sources and the supported providers; physical
USB bandwidth, accelerator alignment and sustained device performance still
require platform qualification.

## Camera admission state

Adaptive capture publishes `video-broker.camera-admission.v2` only after the
selected source has produced the required analysis and archive evidence. Its
contract binds both the selected capture/archive geometry and the effective
NV12 archive encoder alignment. Release gates must derive the fitted archive
geometry from those exact alignment values; applying only generic even-pixel
rounding is not equivalent for hardware encoders such as RDK H.265 `16x8`.
Earlier camera-admission schemas are not accepted.

## Camera mode health

Discoverable V4L2 modes that repeatedly fail runtime admission are stored in a
bounded `video-broker.camera-mode-health.v10` circuit-breaker state. A mode is
excluded only until its per-entry `retry_after`; it then becomes half-open and
is automatically eligible for a new runtime proof. One successful startup no
longer erases an intermittent failure: the entry remains in probation until
`stable_after`, after five minutes of healthy runtime cadence. A successful
half-open admission reduces the history to one probation strike, while another
failure before that healthy window expires increases a bounded exponential
backoff. This prevents both a transient UVC reset from permanently removing the
only usable mode and a flaky high-bandwidth mode from being selected again
forever after one good second. Under `analysis_quality`, an
explicit MJPEG codec may use another advertised native MJPEG geometry while
the preferred geometry is quarantined; the codec contract remains unchanged
and the half-open retry can later restore the preferred mode.

A `VIDIOC_STREAMON` failure before the current broker process has produced any
frame, or while reopening a mode already admitted by that process, is first
treated as a bounded producer-handoff condition. The broker keeps the same
advertised native mode, leaves the device unopened for an increasing quiet
interval, and retries without USB reset or UVC control I/O. Only after the
process-local 90-second handoff budget expires does the failure enter the
persistent per-mode circuit breaker. Once real frame admission succeeds, the
handoff evidence is cleared while the process retains which native modes were
actually proven. This prevents both trial-to-stable transitions and recovery
from a live child exit from falsely quarantining every valid mode on slow UVC
firmware.

Low frame rate is performance evidence. Every active role is admitted after
three fresh frames and its required codec bootstrap, independent of nominal
FPS. The supported negotiated capture mode, codec, geometry and required
hardware remain structural constraints. The native worker applies the same
rule to decoded frames, using the capture stall deadline rather than a nominal
frame-period limit. Its rolling measured cadence follows slowdown and recovery
without duplicating frames or adding a backlog.

An explicitly selected camera that produces valid media stays selected and
`running`, with per-stream `performance.state=reduced` when appropriate. No
FPS-only event changes source bindings, restarts capture, enters quarantine,
or triggers a software fallback. True frame staleness, invalid decode, missing
codec bootstrap and unavailable required hardware retain bounded recovery.
The v10 circuit breaker discards earlier runtime failure classifications,
which could include FPS-only rejection; it preserves no legacy reader.

Routing-control `read` adds a safe `runtime` map keyed by stream ID. Every
measurement is bound to the confirmed `source_ref` and stream `generation`,
with `available`, performance state, requested/target/measured FPS and the
measurement window. An unavailable stream is explicit. A read during ApplyGraph
returns `busy`; consumers retain prior choices as stale read-only data and
reconcile the operation outcome before authorizing another mutation.

The continuity soak retains its duration, progress, latency and hardware
checks. A continuity pass does not certify requested FPS: qualification must
also inspect the actual measured performance for the required profile.

The native bridge's earlier decoded-frame proof is also mode-scoped. A bounded
`runtime_attestation_timeout` is attributed to the exact advertised
codec/geometry/FPS tuple instead of being treated as an anonymous child exit.
After repeated proof failure, adaptive UVC policy may retain codec and geometry
while selecting the next advertised cadence that still satisfies live and
analysis consumer floors. Archive remains the protected master timeline and is
rebound to that selected capture cadence; it is not an independent floor that
would make a native lower-FPS fallback impossible.

An entry is applicable only when both its camera capability fingerprint and
`runtime_fingerprint` match the current graph. The latter is derived from
processing configuration, the broker executable, native workers and semantic
environment overrides; socket, state paths and read-only release-trial
isolation do not change it.
