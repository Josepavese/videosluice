# Recovery soak rejected the current fan-out status contract

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. Il validatore soak accetta e valida esattamente i campi fan-out correnti.

Evidenza: Fixture positive/negative e successivi soak autenticati, incluso a571 test board.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


- Status: corrected locally; test board successor qualification pending
- Severity: release blocking
- Discovered: 2026-09-03
- Components: video-broker status v5 consumer, recovery soak

## Symptom and retained evidence

After the status endpoint publication fix, signed test board artifact
`f2a66149cdda83d5bf3fd8125dc4c106267867cdf75a70856f3e8e644c5b01d8`
passed the official updater postflight. The canonical soak reached its first
authenticated status payload but stopped with:

```text
video-broker source 'front' is invalid
```

The failed root-owned mode `0600` report remains at
`/var/lib/nemesi/setup/recovery-soak-results/25770837ee863f9bdb005535c46df7dc.json`
with SHA-256
`af6c4273165d47e1186e9d32e31eb1f51f7029e40e020ee7f4575ed8952f17cf`.
It is bound to sequence `1788434001394393459` and transaction
`12901f54fda4d123aeda5f98fe22fff32ebb807847d73e22e5d70f8639f86f49`.

## Root cause

The runtime correctly emitted current status-v5 fields
`fanout_control_mode=branch_transaction_v1` and
`fanout_control_supported=true` for both admitted capture owners. Update
postflight already required and cross-validated that pair. The independent
soak validator omitted both fields from its strict source field set, so it
rejected a valid current producer before collecting a stability sample. Its
fixture also omitted the fields and could not detect the consumer drift.

This is a release-gate implementation defect, not a capture, accelerator or
device failure.

## Correction and acceptance

- Require the boolean support flag and admit only `disabled` or
  `branch_transaction_v1` as the explicit mode.
- Require exact consistency: support is true if and only if the mode is
  `branch_transaction_v1`.
- Add negative recovery-soak fixtures for missing, non-boolean, unknown and
  inconsistent fan-out declarations.
- Run the complete relevant setup/package boundary, publish a new signed
  artifact through the official A/B workflow and repeat the test board browser
  switch/rollback proof.
- Complete a fresh canonical soak of at least 2,700 seconds before deployed board is
  considered.
