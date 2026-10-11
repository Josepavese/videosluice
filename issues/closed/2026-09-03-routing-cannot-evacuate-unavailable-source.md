# Routing cannot evacuate an unavailable source

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. Una sorgente completamente ritirata puo' essere evacuata anche senza capture disponibile.

Evidenza: Replay reale test board rev22 in 1,346 s e rifiuto sicuro della destinazione non ammissibile, gia' documentati.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


- Status: fixed and deployed on test board; extended qualification continues
- Severity: release blocking
- Discovered: 2026-09-03
- Components: video-broker routing preview/apply, worker fan-out, publication cutover

## Symptom and evidence

test board was running signed artifact
`b6b56cde2a7e3567382135bef751cd2481ce15f30cf4e5a86fc402c0b2b1c624`
with routing plan revision 21: analysis on `overview`, live and archive on
`front`. Under the current low-light conditions, the front H265 module declared
1280x720@30 but repeatedly delivered 128-129 frames in the ten-second admission
window, about 12.95-12.96 fps. The broker correctly rejected the source rather
than weakening its 30 fps contract. Overview remained admitted and delivered
about 25 capture fps and 20 analysis fps.

An operator preview that explicitly moved all three roles to the healthy
overview source was nevertheless rejected. The plan, catalog and destination
were valid; the runtime validator failed because it required an exact live
worker handle for every touched source, including `front`, even though that
source had no candidate role and would be fully retired.

A bounded test board-only diagnostic changed `auto_exposure` from value 3 (Aperture
Priority Mode) to value 0 (Auto Mode). Both modes preserve automatic exposure,
but value 0 still delivered 12.96 fps. The original value 3 was restored. This
rules out that menu choice as the corrective and does not justify a generic
camera-control policy. The observed 25/12.5 fps family remains consistent with
50 Hz anti-flicker and long exposure; it is not evidence of CPU, VPU, JPU or
broker queue saturation.

## Root cause

The preserved-owner fan-out transaction treated a fully retiring source as a
mandatory worker participant. Handle acquisition rejected any stopped capture
child before the transaction could distinguish a destination that must prepare
new media from a source whose only legal candidate state is empty. The same
assumption propagated into worker phase targets and first-frame receipt counts.

The incumbent publication objects and exact source graph still exist while the
capture child is in bounded recovery. They are sufficient to provide the
pre-cutover and rollback ownership boundary. Sending prepare or detach commands
to a nonexistent worker is neither possible nor necessary.

## Correction and acceptance

- Admit an unavailable handle only for a source listed in the exact complete
  retirement set; every destination and partially retained source still
  requires an admitted capture owner.
- Preserve the retiring source graph, runtime and incumbent publications as the
  rollback boundary, but omit that source from every worker phase target and
  first-frame receipt requirement.
- Require the healthy destination to complete the unchanged capability,
  publication, warmup, admission, first-frame, cutover and verify gates.
- Abort fail-closed if the unavailable retiring worker recovers during the
  frozen transaction; a subsequent request can then use the normal live-owner
  path.
- Persist only after candidate media progresses, then stop and remove the fully
  retired source. Do not introduce automatic fallback or a test board-specific source
  choice.
- Keep hardware capacity reservation and publication rollback rules unchanged.
- Cover both the generic runtime coordinator and the real worker/publication
  backend with an unavailable-retiring-source regression, including proof that
  the dead worker receives no command and the healthy worker receives the full
  ordered phase sequence.
- Run the complete Go suite, race detector, vet and repository release gates;
  deploy through the signed A/B workflow, repeat the all-overview operation on
  test board and then continue the routing matrix and canonical soak.

The targeted regressions, complete video-broker Go suite, broker race detector,
vet and repository release gates pass. The correction was published in commit
`6524090de28bcd57f70024a280fbea51a7f31917` and deployed to test board through the
official signed A/B path as artifact
`f0b8399bf8844333637c02de812e5949f98043e141256bfe33f0abb64a178035`,
sequence `1788460217573669033`, transaction
`4d1d5712509d0844ee120d71101c5ab2a2f00855ddaadc12c137752b42d0aaaf`.
The trial, host commit and postflight passed; the active release verifies
exactly at 8,894 entries and 547,428,516 bytes with zero agent restarts.

While the front worker was unavailable after another measured 12.96 fps
admission rejection, routing revision 22 moved live and archive to the already
healthy overview worker in 1.346 seconds. The operation completed all phases,
persisted, retired front and survived the final service restart. Status v5 then
reported only overview as running and admitted, analysis at 640x360@20 and
archive H265 at 640x360@15 AVBR 750 with zero drops or disconnects. A bounded
local live consumer received an NMB2 H265 access unit immediately; the runtime
attested `native_sp` decode/live/archive encoding, `native_neon` resize,
`allow_sw_fallback=false` and a persistent live encoder epoch.

The unavailable-source evacuation defect is therefore closed on real hardware.
The wider release still requires the remaining multi-source/browser matrix and
a fresh canonical test board soak of at least 2,700 seconds before deployed board rollout.

The later signed successor `08e0934427e297d4f2949ed1fc5357d1ebb6326cfb95aa188191413f056cea14`
retested the inverse operator request without weakening admission. Preview from
revision 22 changed only live to `front-h265`, preserved the overview capture
owner for analysis/archive, and proposed revision 23. Apply then rejected two
independent front capture attempts at 12.96 fps against the required 30 fps and
finished `failed/abort` in phase `admission`. Revision 22, stream generations,
overview PID 2480756, media epoch 1 and the progressing analysis/archive
streams were unchanged, with zero drop, disconnect or agent restart. This
proves that an inadmissible source selection is also safely rejected without a
deploy or partial cutover.
