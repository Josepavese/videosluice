# Multi-source governor binding stalled Person Registry

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. I consumer seguono il governor dello stream analysis, non un file globale inesistente.

Evidenza: Proiezioni stream-scoped e dominio consumer corretti; ripresa delle claim gia' documentata, successivi soak verdi. Non certifica la qualita' del riconoscimento.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


## Status

Root cause proven on test board. The stream-binding and current consumer-domain
correctives were deployed and verified. Person Registry resumed claiming and
its backlog is draining. The replacement soak exposed a separate Python IPC
read-starvation fault under storage latency; that transport correction is
host-tested. A signed test board successor and canonical soak requalification remain
open.

## Runtime evidence

The installed dual-camera broker correctly published source-local governor
states at:

- `/run/nemesi/video-broker/sources/front/resource-governor.json`;
- `/run/nemesi/video-broker/sources/overview/resource-governor.json`.

Both were fresh, `nominal`, capability-attested and admitted optional compute.
Person Registry still had
`NEMESI_MEDIA_GOVERNOR_STATE_PATH=/run/nemesi/video-broker/resource-governor.json`.
That process-global file does not exist in a multi-source graph. The required
`MediaGovernorGate` therefore failed closed forever and the ingest worker never
claimed its durable inbox.

The read-only database audit found 9,617 completed inbox rows and 1,633 queued
rows (30,665,075 bytes). Every queued row had `attempts=0`,
`next_attempt_at=0`, and had never been claimed. The outbox had no pending
rows. Managed transient storage was approximately 50.99 MiB against a 50 MiB
budget: 68 tracker-pending files plus inbox assets caused new observations to
be rejected with `registry_transient_storage_capacity_exhausted`. No record was
deleted. This explains the proven Person Registry stall and its high retry/load
activity; it does not by itself prove the one `devices` heartbeat excursion.

## Cause

Schema v3 source isolation changed the governor writer from one process-global
path to one source-local path. The Nemesi consumer contract still exported the
old global path. Binding it directly to `overview` would only repair the static
test board graph: a later live move of `analysis` would leave the consumer on the old
camera. An implicit primary source or hostname-specific override would also
violate the standalone multi-source contract.

## Corrective

- The configured governor path is treated as a namespace root.
- Every active source retains its source-local implementation telemetry.
- The broker additionally publishes one atomic, regular, single-link governor
  snapshot per named stream at `streams/<stream-id>/<basename>`.
- The Service owns those publications, so warm candidates cannot double-write
  them. Its active graph selection changes with Cutover and is restored with
  Rollback for live profile, running-source fan-out and inactive-source
  activation transactions.
- Publication is generic over stream and source IDs. Tests cover rebinding and
  five independent sources; there is no camera-count limit or board/backend
  branch.
- Nemesi derives the consumer path from the actual policy stream whose type is
  `analysis`; the broker base path remains separate. The contract default is
  the stream-scoped path, not a compatibility read of the obsolete global
  publication.
- Missing, invalid or stale state remains fail-closed. No freshness threshold
  was widened.
- Future soak reports include exact `recorded_at`, snapshot time, age, future
  skew and maximum age when the devices heartbeat gate fails.

## First test board successor and consumer-domain follow-up

Commit `a5ee51e` was packaged and deployed through the signed A/B workflow as
artifact
`083f121cedbade375e72a446f41317f0172051399fec7d40dec6808d3ef77342`,
sequence `1788515550065235539`, transaction
`fd3da73f9fe353525eda604f53816760c2c74735d37807817e3cd2413cd65b9c`.
Independent postflight measured 8,905 entries and 547,615,950 bytes, with the
agent active at zero restarts. The persisted routing plan remained revision 35
with selections `front-h265`/`overview-nv12`/`overview-h265` and generations
`20/8/15`. The process environment and live files proved the new analysis
binding and all three stream projections.

The first drain observation did not pass. Person Registry imported pending
spool files into its durable inbox, reducing the physical spool from about
50.99 MiB to 7,181,544 bytes, but completed rows remained 9,617 while queued
rows grew from the pre-deploy 1,633 to 1,964 and then 1,989. All queued rows
still had zero attempts. Its log identified the projected state as
`state_invalid`.

The exact projected state is schema-valid broker output for a source that
serves analysis and archive but not live. It reports
`live_control_mode=disabled`. The shared Python contract omitted that current
enum value; it also omitted the broker's valid `archive_codec=none` and
`archive_delivery=disabled` values for sources without an archive branch.
Accepting only test board's value would leave live-only and analysis-only topologies
broken. The consumer domain now matches all current broker role-disabled
values. Regressions cover both analysis+archive without live and live without
archive. The combined nemesi-contracts and Person Registry suite passes 344
tests; all 176 Event Captioning tests also pass. Invalid, missing and stale
states still fail closed.

## Acceptance gates

- [x] Complete Go package, race and integration tests, Python contract tests
  and repository checks.
- [x] First signed A/B deployment on test board, exact release measurement and
  postflight proving stream binding.
- [x] Signed A/B successor carrying the current consumer-domain correction.
- [x] Prove Person Registry reads the analysis-stream path, starts claiming the
  existing backlog and remains bounded without deleting historical data.
- [x] Diagnose the failed replacement soak without weakening its IPC gate;
  decouple durable Python consumer socket I/O from storage/model latency.
- Deploy the bounded-transport successor on test board and prove zero IPC writer
  timeouts/reconnects while backlog processing continues.
- Re-run browser checks and a new uninterrupted canonical soak of at least
  2,700 seconds; preserve its protected report and artifact binding.
- Do not promote to deployed board until test board is green.
