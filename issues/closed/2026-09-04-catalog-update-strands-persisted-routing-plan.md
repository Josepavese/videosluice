# Catalog update strands the persisted routing plan

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. La riconciliazione conserva le scelte ancora valide e rifiuta quelle revocate senza distruggere il piano.

Evidenza: Commit 281d58b, transizione reale test board v6->v7 rev23 e regressioni Store correnti.

Riesame del repository e delle prove registrate, non nuova verifica live.
Registro della cernita e limiti residui (operational history retained by the application owner).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


- Status: fixed, host-qualified and verified on test board
- Severity: release blocking
- Discovered: 2026-09-04
- Components: video-broker routing Store, daemon startup, central device profile

## Symptom and evidence

test board's central `rdkx5-usb-uvc-test-board-dual-v6` catalog declares the front H265
source and live option at 1280x720@30. Direct device measurements under the
current conditions showed about 12.91 fps when requesting 30, about 14.16 fps
when requesting 15, and a stable 24.99 fps over 250 frames when requesting 25.
The camera still advertises HEVC, MJPEG and H264 at 1280x720 with nominal
30/25/15 intervals, and auto exposure remains Aperture Priority with 50 Hz
anti-flicker. This supports a central v7 profile at 25 fps without a generic
camera-control workaround.

The operator plan on test board is persistent revision 22 and is correctly bound to
the v6 catalog revision. Rendering a v7 catalog changes that revision even
though the current all-overview opaque choices remain admitted. The strict
Store read therefore rejects the old plan as stale. Without an explicit
transition, an otherwise valid central profile update would prevent the broker
from starting or would require discarding the user's persisted selections.

## Root cause

Catalog revision equality correctly protects live routing from applying a
choice against changed central policy, but daemon startup had no separate
current-schema reconciliation boundary. It treated every catalog update as a
revocation and could not distinguish a retained opaque option set from an
option that had actually disappeared.

## Correction and acceptance

- Reconcile only during daemon startup and only from a trusted normalized
  central catalog; ordinary Store reads, `--check-config` and routing API calls
  remain strictly read-only and revision-bound.
- Preserve active and rollback opaque selections only if both complete sets
  materialize under the new catalog. Any revoked option or stream-set drift
  fails closed and leaves the old plan bytes untouched.
- Atomically advance the catalog revision, plan CAS revision and every stream
  generation before workers start. Advancing all generations is required
  because the superseded catalog is not trusted to prove which endpoint/media
  definitions changed.
- Reject plan-revision or stream-generation overflow without mutation.
- Return an unpersisted catalog default when no operator plan exists; do not
  invent an operator write during first boot.
- Cover retained choices, active and rollback revocation, overflow, missing
  plan, read-only validation and idempotent second startup with unit tests.
- Run the complete Go, race, vet and release-contract suites. Only then build a
  signed artifact, deploy through the official A/B path to test board, verify the
  revision/generation transition and continue the front-camera/browser matrix.

The targeted tests, complete Go suite, race detector, vet, standalone and
integration contracts, atomic publication, HBN/V4L2 fan-out, native resize,
postflight, recovery-soak, identity, hotplug, capture-probe, UVC-trial,
package-assets, profile and deploy-policy contracts were green before release.
Commit `281d58b` was deployed through the official
signed A/B path as artifact
`971026b57af99f98e707bf3da67a1234f1b71d9b162c08dbae333f7cd32a9400`,
sequence `1788475151988595895`. Applying the central v7 preset produced update
transaction
`aca3e59a43178d0ce4fb938f6aa2456c70c405fd4d8b77853e5095f47c8f7021`.
test board started with plan revision 23, all three stream generations incremented
exactly once, active and rollback opaque selections preserved, and the catalog
revision equal to the newly rendered runtime catalog. Exact release measurement
and postflight passed. deployed board remains out of scope until the remaining test board
runtime/browser gates are green.
