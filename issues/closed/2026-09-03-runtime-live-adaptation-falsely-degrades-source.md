# Runtime live adaptation falsely degrades a healthy source

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. La salute della cadenza usa il target runtime effettivamente controllabile.

Evidenza: Soak 778acf7cd5de08223ca5ea7dd18d570b e due consumer a 15 fps con broker running.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


- Status: fixed, deployed and qualified on test board
- Severity: release blocking
- Discovered: 2026-09-03
- Components: video-broker cadence health, WebRTC congestion adaptation, recovery soak

## Symptom and retained evidence

The canonical 2,700-second test board soak for signed artifact
`f0b8399bf8844333637c02de812e5949f98043e141256bfe33f0abb64a178035`
failed after about 17.5 minutes with the former diagnostic
`required video-broker is not running`. The broker and its capture worker had
not exited: both retained their original PID with zero agent restarts, while
analysis and archive continued at their contracted cadence.

The root-owned mode `0600` report remains at
`/var/lib/nemesi/setup/recovery-soak-results/adb7ca420baeda84d696e2440d26dccf.json`
with SHA-256
`a95fbb46b6ca1d3009740e766764c52cfaf13d326c930edc859e9798285825cb`.

Two real H265 WebRTC consumers were connected. A Tailscale latency increase
caused the WebRTC congestion controller to request the supported live target
of 15 fps and 921 kbit/s. The hardware bridge applied 15 fps, but the broker
cadence watchdog continued comparing the produced stream against the static
30 fps catalog profile. The resulting 15.0 fps was therefore below the stale
16.5 fps threshold and falsely degraded the whole source.

This was not a device stall, a software-encoding fallback or an accelerator
leak. The soak correctly rejected a non-healthy top-level state; its error text
was merely too generic.

## Root cause

The producer control already combines the safest request across all current
viewers with VPU/JPU/BPU resource-governor caps. The cadence watchdog applied
only governor caps and ignored the viewer target. It therefore measured an
intentionally reduced producer against a target that was no longer the active
runtime contract.

## Correction and acceptance

- For controllable capture backends, derive live and analysis cadence health
  from the same effective bridge-control contract sent to the producer.
- Keep the static catalog cadence for adapters that cannot apply runtime
  control; a request must never hide underproduction on those backends.
- Keep dormant on-demand live excluded, while routing demand without a viewer
  still requires the full configured live cadence.
- Preserve the strict soak gate: `degraded` remains a failure, but the report
  now names the exact unhealthy broker state.
- Cover viewer adaptation, governor caps, routing demand and static adapters in
  broker regressions; run the complete Go/race/vet and release-contract suites.
- Publish a signed A/B successor to test board and repeat the full canonical soak
  while real multi-client adaptation is allowed to operate. deployed board remains
  excluded until this gate is green.

The correction was published in commit
`44c04355b4e399cca910c68cc58b06fc6dbd4495` and deployed to test board through the
official signed A/B path as artifact
`08e0934427e297d4f2949ed1fc5357d1ebb6326cfb95aa188191413f056cea14`,
sequence `1788466077635804298` and transaction
`aab1bf13f61dd90ce5dd7f25f56442569441936ebc8222ec232d330fe8428ea0`.
Trial and postflight passed. An independent exact measurement verified 8,895
entries and 547,434,146 bytes; `nemesi-agent` is active with zero restarts and
status v5 reports the overview source running with no error. The required
2,700-second soak was then completed as recorded below.

## test board qualification

The canonical soak completed with `status=passed` after 2,700.102961 seconds
and 5,395 authenticated samples. Its root-owned mode `0600` report is
`/var/lib/nemesi/setup/recovery-soak-results/778acf7cd5de08223ca5ea7dd18d570b.json`
with SHA-256
`85ed3e98936a8679d00eae954517b6d7750188d437d250e048fa6531ff72b6a3`.
It observed a maximum temperature of 77.438 C, no reboot or supervisor restart,
49,551 tracker frames, 40,542 recorder video frames and zero recorder drops.
One 2.867-second systemd collection overrun was recorded, with maximum
consecutive overruns equal to one, and remained within the unchanged contract.

During the soak, two bounded live consumers both requested 15 fps and 921
kbit/s, exactly reproducing the producer adaptation that exposed the defect.
The bridge applied and attested that control at sequence 5 without changing
encoder epoch. For 240 seconds the live output stayed between approximately
14.94 and 15.16 fps, while broker health remained `running` and
`cadence_degraded=false`; archive and analysis remained at 15 and 20 fps with
zero drop or disconnect. The consumers received 30,065,638 and 30,056,837
bytes. Their clean detach returned live to its on-demand inactive state without
changing source PID or media epoch, and the soak remained valid through the
end.

After the soak, the release measurement still verified exactly at 8,895
entries and 547,434,146 bytes, the agent retained PID 2480408 with zero
restarts, and routing remained at revision 22 with all streams bound to
`overview`. This issue is closed on test board hardware.
