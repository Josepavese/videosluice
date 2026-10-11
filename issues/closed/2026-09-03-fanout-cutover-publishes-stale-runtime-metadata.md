# Fan-out cutover publishes stale role runtime metadata

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. Cutover e rollback pubblicano i metadata del ramo realmente attestato.

Evidenza: Commit 8b7ebb8, test di attestazione per ruolo e matrice fisica dei successori fino ad a571.

Riesame del repository e delle prove registrate, non nuova verifica live.
Registro della cernita e limiti residui (operational history retained by the application owner).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


- Status: corrected and published on `main`; test board successor qualification pending
- Severity: release blocking
- Discovered: 2026-09-03
- Components: video-broker worker receipts, fan-out publication cutover, status v5

## Symptom and retained evidence

On test board, signed artifact
`52398f6ece13e97a5f8424049382b05b38617245309d51d5cea878cb5dd719a7`
successfully applied the live move `front -> overview` as operation
`operator-live-overview-r12`. Revision 12 completed in 1.391 seconds, the H265
subscriber reconnected in one attempt with zero reported downtime, frames
continued and both source PID/media epochs remained unchanged. The public
status nevertheless left the overview worker `live_control_mode=disabled`.

The independent archive move `front -> overview`, operation
`operator-archive-overview-r14`, completed revision 14 in 1.189 seconds and
delivered advancing archive frames at about 15 fps. Status still reported
`delivery=disabled` and omitted the observed archive cadence. The archive move
was immediately rolled back by `operator-archive-rollback-r15`; revision 15
completed in 1.385 seconds and restored the initial bindings. Live had already
been restored by `operator-live-rollback-r13` in 2.406 seconds. test board therefore
remains on the safe initial plan: analysis on overview, archive and live on
front.

This is a status/health correctness defect. It did not prove media loss, but a
successful data-plane cutover must not publish startup metadata for a different
role graph.

## Root cause

The worker `first_frame` receipt attested only branch epoch, frame ID and codec
bootstrap. The broker atomically changed plans and publication ownership, but
`childLiveControlMode`, `observedArchiveCodec`,
`observedArchiveDelivery` and `observedArchiveFPS` were populated only from the
worker startup capability report. Attaching a role to a source that did not own
that role at startup therefore retained `disabled`; detaching it could retain
the previous active values.

The broker could not infer the missing value safely: V4L2 may use native
passthrough or a persistent VPU encoder, HBN has its own static hardware
backend, and GStreamer may select native, hardware or explicitly authorized
software delivery.

## Correction and acceptance

- Make `runtime_mode` mandatory in every encoded candidate branch receipt;
  analysis must explicitly carry the empty non-codec value.
- Validate role-specific modes fail-closed and bind every receipt to the exact
  source, transaction and candidate-role set before the publication
  first-frame phase can advance.
- Have GStreamer, HBN and V4L2 attest the backend actually created by that
  candidate branch; do not derive it from hostname or platform policy.
- Publish live/archive runtime metadata in the same locked cutover as plan and
  endpoint ownership, clear it on detach, and include it in the role snapshot
  restored by rollback.
- Make verify compare the public runtime metadata with the attested candidate
  before retire or persistence can complete.
- Keep the current-only receipt schema atomic within one broker/worker artifact;
  no compatibility parser or fallback is retained.
- Run complete Go normal/race/vet suites and native GStreamer/HBN/V4L2 contract
  tests, then package and deploy only through the signed A/B workflow.
- On test board, repeat live and archive switch/rollback plus all-source permutations,
  validate browser continuity and complete a fresh canonical soak of at least
  2,700 seconds before any deployed board promotion.

The correction is published in commit `8b7ebb8`. The complete standalone Go
suite, race detector and vet pass locally. Native
media/protocol, HBN fan-out and V4L2 warmup/control contracts also pass. No
device has received this correction yet.
