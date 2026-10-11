# Status socket publication contract drift blocks the canonical soak

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. Il socket status ha un solo nome/link e il postflight verifica lo stesso contratto del soak.

Evidenza: Test reali di pubblicazione e postflight, successivi soak verdi fino ad a571.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


- Status: corrected and deployed on test board; successor qualification pending
- Severity: release blocking
- Discovered: 2026-09-03
- Components: video-broker status server, update postflight, recovery soak

## Symptom and evidence

The signed test board artifact
`07423911fba83619326f780e7a9cbad2a4fc4cd1b000010db6233bf83158587f`
passed update postflight, multi-client WebRTC and runtime routing cutover, but the
canonical 2,700-second soak stopped during its first sample:

```text
recovery soak error: video-broker status socket metadata is unsafe
```

Live metadata showed that `status.sock` had mode `0660` and two names for the
same inode: the public pathname and `.vb-8dbf80d93537`. The soak correctly
requires one link, while postflight did not inspect `st_nlink`.

The failed result is retained as root-owned mode `0600` evidence at
`/var/lib/nemesi/setup/recovery-soak-results/84146e7787fde791f0c0dcd1544edaa1.json`
with SHA-256
`b5077e21ad33ab8e5594bfbe4db2494d0577431a3269bfd2c180e4529b8a4311`.
It is bound to sequence `1788430211547091133` and transaction
`4071aa75eeb7003b821daa39116a599c0629b3f0b7f03248d4701735f9fa42ae`.

## Root cause

The status server used the replaceable publication helper intended for media
sockets. Status is a daemon-lifetime control endpoint and never participates in
a routing cutover, just like routing-control. Retaining a private hard-link
alias therefore expanded its filesystem authority without a lifecycle need.
The postflight/soak mismatch allowed the release gate to pass too early.

## Correction and acceptance

- Publish status through the existing single-owner helper: mode `0600`, one
  pathname, inode-bound cleanup and safe stale-socket reclamation.
- Make postflight require `st_nlink == 1`, matching the soak contract.
- Assert mode/link count in the real Go status-server test and reject an added
  hard link in the postflight contract fixture.
- Run video-broker package/race tests and the setup postflight/recovery-soak
  contracts, then deploy a new signed artifact to test board.
- Repeat the two-peer `front -> overview -> front` proof and complete a new
  canonical soak of at least 2,700 seconds before any deployed board promotion.

Local package tests, race detector, vet, update-postflight contract and
continuous-soak contract pass with the correction.

The correction was published in commit `c6ce27c` and activated through the
official A/B updater in signed artifact
`f2a66149cdda83d5bf3fd8125dc4c106267867cdf75a70856f3e8e644c5b01d8`,
sequence `1788434001394393459`, transaction
`12901f54fda4d123aeda5f98fe22fff32ebb807847d73e22e5d70f8639f86f49`.
The live endpoint is mode `0600`, owned by the service account and has exactly
one pathname/link. The next soak reached payload validation and exposed a
separate consumer-contract drift recorded in
`2026-09-03-soak-fanout-status-contract-drift.md`; it did not regress socket
publication.
