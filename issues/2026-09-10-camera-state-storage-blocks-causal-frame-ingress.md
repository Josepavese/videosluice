# Camera state storage can block causal frame ingress

Status: **OPEN — local corrective tested, test board qualification pending**.

## Evidence and scope

test board recovery after failed signed update f2c1, 2026-09-10 03:46:55 UTC:
native source epoch1 timed out sending origin48. Payload3110400 bytes,
49020 bytes/4 records sent, budget1000ms, elapsed1001ms, payload hash13ms.
The source retired; Tracker subsequently exited after its FAP binding retired.
The later transport error with `lifecycle=stopping` is shutdown, not a second
independent runtime failure. The candidate was never built or activated.

The maintained broker held its media mutex across camera admission and
mode-health persistence, including file and directory fsync. Both FAP ingress
and mux/status/governor paths need that mutex. Deterministic blocked-storage
tests demonstrate this coupling can starve causal delivery. The exact kernel
wait that exhausted test board's send budget was not captured; do not claim this
static path alone proves the sole cause, a defective SD or an accelerator leak.

## Corrective

- Encode owned, immutable state snapshots under the media lock; filesystem I/O
  runs outside it. This is broker-owned, platform-neutral camera policy, with
  no Registry or Nemesi identity logic and no codec/accelerator special case.
- One source-local writer, one in-flight plus at most two latest pending
  documents (admission and mode health), maximum1MiB per document. Superseded
  pending states coalesce; no goroutine or queue per frame and no retry loop.
- Preserve schema, permissions0640, atomic rename and file+directory fsync.
  Queued is not durable: completion/failure includes sequence, bytes and I/O
  duration. Failure remains visible; the next real update can recover it.
- Runtime teardown joins actual writer completion; blocked kernel I/O remains
  owned. Do not detach it or claim a timeout released storage/process resources.
- No relaxed FAP deadlines, thermal limits, frame identity checks or implicit
  retransmission of partially sent frames.

## Verification / remaining work

Local full video-broker suite PASS (broker package25.435s). Targeted race tests
PASS. Tests include causal admission and subsequent frame while storage is
blocked,200 pending updates coalesced to two snapshots, orderly final writes,
shutdown ownership, failure without false commit/retry, immutable snapshots,
durable schema/permissions and bounded payload validation. Existing persistence
tests now wait for actual completion before reading disk, not for a fixed sleep.

Full broker race suite also PASS26.030s. Remaining: official setup gates; signed
test board deployment and startup observation, then relevant load/thermal/ROI tests.
Only after qualification continue the planned deployed board rollout. The historical
deployed board N0e milestone remains closed and is not repeated for this local change.

First packaging stopped after131 setup contracts passed: a standalone fixture
removed its TempDir before the asynchronous writer completed. Direct runtime
fixtures now register writer join in cleanup, including early assertion exit.
Four affected tests repeated30 times with race PASS3.547s. Preserve this failed
packaging result; it is not a device regression or a reason to skip the gate.

Evidence: persistent workstation pack
`<private-workspace>`,
`test-board-f2c1-recovery-journal.log`, `camera-persistence-*.log`,
`broker-camera-persistence-full.log`.
