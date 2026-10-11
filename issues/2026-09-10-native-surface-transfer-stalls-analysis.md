# Native surface transfer can stall and retire the analysis capture

Status: **OPEN — reproduced on test board; cause of the transfer stall not yet proven**.
Goal allocation: N1-N3 transport isolation/bounds and N6 integrated qualification.

## Exact observed candidate

10 September 2026, development release
`dev-1788955416179052766-bd75a6b34ec39861-f0f22eb2ccb6`, derived from clean9ffc.
Broker payload manifest
`bd75a6b34ec39861aff993bb79a8c93c40c14d1aac3c390024be40f9c3c9b303`;
Go binary `a4b517bd98550d91dc87bc9a47013289f0bdab53e8493036164d8e6066ee781a`.
Includes all broker hotfixes and the verified-surface copy-budget correction.
Other components remain from9ffc; this is NOT the entire f97 signed candidate.
All five targeted Go suites passed locally and on test board; integrated run failed.

## Failure, 07:54:24 UTC

- Overview H2651920x1080@30; source epoch1, attempted origin2185.
- Native sender: `source_surface_send/io_deadline`,168records,2728780 of
  3110400payload bytes sent;1000ms budget,1001ms elapsed, integrity14ms.
- Broker receiver:165records,2679760payload bytes assembled; header12829us,
  assembly1486271us, total1499133us against999975us. Failure in payload receive,
  `receive_idle_deadline`; integrity/publication not reached.
- Capture retired after2139retained frames, drops0/failures1. Native UVC close
  reports streamoff0, four buffers unmapped, close0, SDK release confirmed.
- Tracker exits for retired causal binding. Automatic recovery starts epoch2
  and a new Tracker process; supervisor and broker processes do not restart.

This differs from the earlier complete-frame/copy-budget failure. A partial
frame MUST NOT be accepted or treated as a complete optional surface drop.
No deadline enlargement, partial-frame resynchronization, stale replay, generic
restart, source substitution or fallback is authorized by this diagnosis.

## Evidence and limits

Authenticated diagnostic61samples/300.210546s: Tracker exits1, two samples
without Tracker status during restart, maximum71.771C. Not a passed soak.
Raw broker telemetry additionally authenticated to the supervised child.
Tracker processed-frame rates on stable PID segments12.36/12.71fps, NOT proof
of completed BPU inferences/sec. Tracker log medians12.88-13.58fps, human
inference medians28.52-30.26ms. Many head/body ROI requests expired.
Registry did ingest observations; that does not certify ROI pixel/identity
correctness or absence of corpus contamination.

Before failure native summary:19.83frames/sec; sub19.34fps, archive14.84fps,
live disabled with zero viewers; decode mean1.02ms, accelerated sub resize1159
frames versus CPU resize0, frame work mean50.40ms, sub write mean17.79ms.
These figures identify costs worth isolating; they do not prove why one transfer
stalled for a second or qualify every accelerator/platform.

Mean observed CPU percentages of one core: broker58.87, Tracker49.21,
Registry5.99; RSS maxima164.45/129.96/121.11MiB. Native capture and other tasks
are NOT included in those per-process figures. Service CPU quota infinity,
Nice0, no explicit MemoryMax. Post-incident MemAvailable1370480kB; global
major-fault/allocstall/OOM counters are cumulative and cannot be attributed
to this incident. No matching kernel USB-disconnect/MMC/OOM line found in
the captured post-start window; this does not establish disk health.

## Next decisive work

### Startup defect isolated separately, 16:10 UTC

Do not conflate a pre-frame startup failure with the earlier partial transfers.
On new root27bc, mailbox readiness timed out before any frame, then immediate
SIGKILL destroyed the SDK release receipt. A scoped fix now uses the existing
TERM/CONT grace for an already attested group, drains bounded startup stderr
and includes the underlying release/process state in the error. Local and test board
race tests pass, including cooperative exit64 and uncooperative KILL rejection.
The previously skipped unattested-group test had an invalid exec.Command/Cancel
combination; it now uses CommandContext and genuinely passes, without a skip.

After promotion to ff266574, cold startup still failed after1001ms with
`signal: terminated`, before the native signal handler was operative. A SINGLE
guarded warm restart of the SAME bytes/configuration succeeded15:55UTC. Its
300.212s diagnostic has61samples, Tracker+3914frames (13.037fps), frame age
at most36ms, unchanged PIDs/epoch, Tmax80.271C and recorder video progress.
Registry is ready with queue0/integrity0 but obs0 in that window: not proof of
new ROI/observations, a soak, or resolution of historical transfer stalls.

The next correction separates bounded process startup/authentication from
frame transport: one absolute5s startup budget within the existing peer cap;
frame1s and copy100ms remain unchanged. Local native tests prove startup
delayed153ms while retaining50ms frame windows, exact pixels/ROI and restart;
missing startup response still expires and cannot replay. test board qualification
passed without skips in the startup-budget-smlvP2ZE PAL job. Promotion to
1be609f7/brokerdfae8dc6 finished16:26:31UTC; restart16:27:00, first capture
bound16:27:14 in219ms without an additional warm restart. This is operational
startup success, not proof that the prior cold-loading cost was reproduced
(219ms is also below the previous1s budget). Native/emitter/config preserved.
The subsequent300.252s/61sample diagnostic retained the same PIDs/epoch1,
Tracker+3951frames/13.159fps, recorder+4466video frames, Tmax81.541C.
No persons/Registryobservations in that window; not new ROI evidence or a soak.
Historical transfer-stall cause remains OPEN. Preserve preceding failures.

### Qualified diagnostics, promotion held by storage waits, 14:04 UTC

The startup-fixture correction passes on test board without tracing, including native
pixels/ROI, restart and race checks; the previous test fails before C main.
Go diagnostics binary95753d40/build manifest27bc6b92 is ready, native binaries
unchanged. It is NOT installed: initial update-lock conflict and subsequent
pre-stop quiescence rejection both occur before claim/drain/service stop.
Current root06f02892, supervisor1170824, broker1170868 remain unchanged.

The60.041s authenticated diagnostic14:02:56-14:03:56UTC contains13samples:
mmcblk1 receives31.740MB of writes/1448requests, mean observed write latency
383.96ms, disk busy60.97%, weighted average queue9.28. These block-counter
deltas include queued/writeback work; they do not assign every device write
to the process that dirtied it. Per-process charged writes: recorder16.826MB,
Registry8.045MB (3.895MB cancelled), event-captioning3.236MB, Tracker0.508MB.
Registry584.7MB and captioning227.2MB logical reads are largely cache hits,
NOT equivalent to that amount of physical SD traffic. No MMC/ext4/OOM/thermal
error appears in the captured kernel filter since13:35UTC; not a disk-health
certificate or evidence of a defective card by itself.

No observed task PID changes; Tracker816processed frames/~60s, source1201frames,
latest frame age30/31ms. Registry connected, queue0, integrity_errors_total0,
worker_stalled=false. Maximum78.268C. Does not prove fresh Registry observations,
20inferences/s, pixel correctness or a passed soak. D-state samples include
filesystem/journal waits and transient hardware/hash waits; D alone is not a
persistent accelerator hang. Read-only pre-stop checks at13:58 and14:04 remain
unsafe under unchanged3clean/60max/250ms criteria. No forced stop or third
promotion attempt; preserve the already-qualified build until I/O permits.

The first I/O collector stopped after two samples because canonical systemctl
inspection exceeded4000ms. The subsequent I/O-only collector retains peer
authentication and PID/start/root pins without querying systemctl each sample;
this does NOT relax the canonical soak timeout or qualify service health.
Artifacts: native-validation/storage-pressure-{readonly,proc-bound}.jsonl,
startup-quiesce-rejection.log and quiesce-after-storage-attribution.log in the
persistent pack. Next decision: attribute recurrent storage waits and use the
same guarded promotion only after a measured quiet window; no rebuild or
deadline/thermal threshold increase.

### 11:41 recurrence and 11:50 storage pressure

On root dev-1788955416179052766-06f028928c2670d8-c49f51f8b60f (only
Tracker emitter changed after the mailbox broker), capture epoch1 retired at
11:41:17UTC after1905162ms for output_send/io_deadline. Epoch2 then failed
at11:41:26: complete345600-byte output, header989308us, assembly1978us,
integrity3588us, publication5802us, total1000746us against999966us.
This is distinct from the earlier2721ms publication delay. New broker phase
diagnostics have NOT been installed: targeted observer/DeliveryCommit race
tests pass, but the native fixture fails source mailbox ready/receive_idle_deadline
after correcting the standalone test runner's working directory. Qualification
remains open; no source/pixel/deadline check was removed.

11:50UTC, with operator build units already stopped: two vmstat intervals show
48%iowait and6-9blocked tasks, including MMC journal/kernel worker, Registry
at jbd2_log_wait_commit, Tracker at folio_wait_bit_common and capture at
hash_sendmsg. Temperature about76C, compute CPU22-24%. This identifies real
storage pressure, not a definitive defective-SD diagnosis or proof that all
deadline failures share a single cause. MemAvailable1453276kB at11:51; no
matching MMC/ext4 kernel error in the captured filter. Pause further build I/O
and observe recovery before another qualification attempt. Existing asynchronous
camera-state persistence corrective is already separate; do not claim it proves
the absence of system-wide I/O stalls.

### Causal metadata separated from pixel retention, 08:31 UTC

Implemented in the generic core; not yet promoted into the running release.
Previously a valid SourceOutput was suppressed by BindExact when the native
surface was missing/expired. Binding now retains the authenticated exact
OriginRef independently, under the same MaxBindings/MaxAgeNS/replay fences.
Extract alone resolves retained pixels and returns Expired for that exact
missing origin. Late pixels can satisfy only the unchanged delivery, and
eviction during an in-flight copy still fences its result. No wait queue or
nearest-frame substitution; an unknown origin remains CorrelationUnavailable.

New regression tests were RED before/GREEN after. All five local integration
and race suites pass; final added concurrency tests pass with race. All five
onboard test board suites also pass,235.990908s/max77.536C;10source/test files hash-exact
against Git. Test unit terminated successfully; nemesi-agent997466/NRestarts0
remained active. Source transport is STILL synchronous/partial-fatal: this is
an architectural prerequisite, not a claimed timeout fix or completed soak.
Artifacts `source-retention-independent-*.log`, `source-retention-onboard-*.log`
and `source-retention-onboard-sources.sha256` in the persistent pack.

### Direct runtime inspection, 08:10 UTC

Second spontaneous occurrence08:07:05UTC, source epoch2/origin13422: only4records
and49020payload bytes sent,1002ms elapsed against1000ms. Capture then recovered
to epoch3; broker997508 and supervisor997466 unchanged. This is not a new patch
attempt or a passed soak. The user's two-unsuccessful-attempt rule now requires
revisiting the coupling/architecture, not another deadline-only adjustment.

Code inspection confirms full native surface publication BEFORE analysis output,
on the SAME synchronous source endpoint. Both the detached MJPG/JPU completion
path and the other decoded input path have this ordering. The UVC buffer is
already returned before decode/transport; a blocked loop is not evidence that a
client holds a driver lease. A slow optional surface prevents the next analysis
output despite their different roles. Preserve causal identity and bounded
ownership while evaluating their separation; do not swallow partial framing.

Read-only profiling, no restart or new binary: broker20.64353CPU-seconds during
30.00614wall-seconds (68.8% of ONE core),191953context switches,7731page faults
including2major. Next30s CPU-clock profile49Hz, broker+native bridge1514samples,
0lost, shows copy/allocation work and kernel scheduling; no single dominant
function. This is a normal-runtime cost sample, NOT a captured causal trace of
the1s incident. Artifacts `direct-path-{perf-stat-30s,pidstat-30s,cpu-profile-30s,
fresh-failures}.log` and `direct-path-cpu-profile/` in the persistent pack.

Correlate a fresh transfer stall with per-thread scheduling/wait state, page
faults, CPU and actual read progress before changing transport or budgets.
Distinguish peer authentication/ownership waits, recvmsg waiting, chunk handling
and scheduler delay. Explain the earlier384ms domain-begin delay too; common
cause remains a hypothesis. Evaluate FAP transport isolation so native pixel
access cannot stall normal analysis; preserve exact identities and bounded
ownership. Reuse the compiled native bridges and passing suites for unchanged
code. Include remaining Tracker/Event/Registry/setup hotfixes in the final
consolidated release; do not mistake this broker-only experiment for that stack.

Evidence outside Git, persistent pack `native-validation/`:
`copy-budget-runtime-300s.jsonl`, `copy-budget-broker-180s.jsonl`,
`copy-budget-runtime-journal-1.log`, `copy-budget-failure-window-and-limits.log`,
`copy-budget-runtime-memory-kernel.log`, `copy-budget-device-evidence/`.
Related: [verified surface copy-budget expiry](2026-09-10-surface-copy-budget-retires-source.md).
