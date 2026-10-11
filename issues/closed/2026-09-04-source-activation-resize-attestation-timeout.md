# Source activation timed out on an unreported resize stage

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. Ogni ruolo attesta il proprio resize dopo telemetria e frame effettivo.

Evidenza: Replay OOO->OOF e matrice a244dc0; successivi soak verdi dopo i distinti fix IPC.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


## Status

Corrective implemented, host-qualified and distributed through the official
signed A/B path on test board. The formerly failing replay and the complete admitted
and rejected dual-source matrix are green. The replacement canonical soak is
not green: its successor exposed the independent stream-governor consumer bug
documented below and must be replaced after that correction. The operator has confirmed both the embedded
webserver and cloud-webapp visual paths on that installed successor; mobile
coverage remains separate unless included in the tested client matrix.

## Runtime evidence

On test board artifact
`06f60461b4ff4f68f6e1a00c70a032c923ab2fd347636ad180c0da7849aa7558`, the
`OOF -> OOO` archive move completed in 1.71 seconds and proved the explicit
publication reset marker: recorder accepted frame `2930 -> 15` with
`connection_changed=true`, remained `writing`, and retained zero archive/raw
drops.

The reverse `OOO -> OOF` operation `apply-oof-caac3cc` activated the previously
inactive front source. Its native worker opened `/dev/nemesi-video-sources/front`,
attested H265 camera cadence and bootstrap in 6.045 seconds, and continuously
produced the requested hardware-encoded archive at approximately 15 fps.
Nevertheless ApplyGraph remained in `admission` until its bounded 90-second
deadline, then aborted without changing persisted plan revision 29. The
incumbent overview capture PID, recorder PID and counters were preserved, and
recorder archive/raw drops remained zero.

## Cause

The capability plan correctly contained a distinct hardware resize stage for
each role. Runtime telemetry and frame attestation existed only for the
`analysis` resize role. A newly activated archive-only or live-only source could
therefore attest camera, decoder and encoder while leaving its actual
`resize/archive` or `resize/live` stage permanently in `candidate`. The
transaction correctly failed closed, but could never complete.

This was not specific to test board or to V4L2. The same incomplete role telemetry
contract was present in the native HBN worker and the portable GStreamer worker.

## Corrective

- V4L2 reports the selected native NEON or CPU resize backend separately for
  live and archive when each real branch first executes.
- HBN reports its `native_sp_resize` VIO backend for every enabled role.
- GStreamer reports `videoscale` for live and archive encoded branches as well
  as analysis.
- The broker records resize telemetry per role and promotes that role's stage
  to `attested` only after a corresponding output frame is actually produced.
  Telemetry alone, or media before telemetry, is insufficient.
- Child teardown clears all per-role observations so a later capture epoch must
  attest itself again.

The regression proves both negative boundaries and the eventual positive
admission. It is repeated 50 times; the complete broker race suite, vet, native
NV12/fan-out tests, V4L2 warmup/control contract and HBN fan-out contract are
green on the host.

## test board successor and matrix evidence

Commit `a244dc00e789a25e13f765781b1cce5ea339e85d` was packaged and promoted on
test board as signed artifact
`600f89d120f0e984db72f8537c38c5c9aba7adcca0c5b200ed139af7733eec86`,
sequence `1788501285214900023`, transaction
`1260e9395da4d8a75f7f14a0752e15b247262491142aa959c89d8e6102ef4bc4`.
Postflight is green and the exact active release measure is 8,901 entries and
547,577,256 bytes.

In the notation below, each position is `live/analysis/archive`, `F` means
`front` and `O` means `overview`. The formerly failing `OOO -> OOF` replay
completed in 6.73 seconds at revision 30. The newly activated front worker
reported `resize/archive=native_neon`, was admitted after 5.863 seconds and
the recorder accepted the publication reset `3828 -> 31` without drops or
degradation.

The remaining admitted matrix completed without deploy or process restart:

- `OOF -> OOO`, revision 31, archive-only generation 13, 1.464 seconds;
- `OOO -> FOO`, revision 32, live-only generation 20, 7.564 seconds;
- `FOO -> FFF`, revision 33, analysis/archive generations 7/14, 1.326 seconds;
- `FFF -> FOF`, revision 34, analysis-only generation 8, 8.378 seconds, with
  `resize/analysis=native_neon` attested on the reactivated overview worker;
- `FOF -> FOO`, revision 35, archive-only generation 15, 1.238 seconds.

Unchanged source workers retained their PIDs at every source-local cutover.
Agent, tracker and recorder remained at one start; broker and recorder retained
zero archive/raw drops. Preview rejected `FFO`, `OFF` and `OFO` fail-closed,
and a read afterward proved byte-consistent plan `FOO` revision 35 with stream
generations `20/8/15`.

## Remaining gate

The detached canonical test board soak
`nemesi-operator-video-soak-a244dc0-test-board-final` failed after about 2,284 seconds,
not because of the broker, but because a same-process Person Registry IPC
reconnect was counted as a supervisor authentication failure. The protected
report and generic supervisor correction are recorded in
`improvements/closed/2026-09-04-supervisor-same-generation-ipc-reconnect.md`. A new
signed test board successor was deployed, but its replacement soak also stopped
early after 1,557 seconds because one authenticated `devices` heartbeat exceeded
the strict five-second snapshot-age gate. Agent and all video workers retained
their PIDs and zero restart count; a subsequent 60-sample/30-second probe saw
`devices` age between 0.015 and 1.995 seconds. The failure remains valid and is
not treated as green. The report is
`2169d39a6c5f5340cdd92fbbd48d617e.json`, SHA-256
`bddbe2e1c1d648c1efcac750782fb61652816a165c208a724600d66a69ce39b9`.

The same inspection proved an independent multi-source governor binding defect
that had stalled Person Registry and filled its bounded transient storage. It
is tracked in
`2026-09-04-stream-governor-binding-stalled-person-registry.md`. That resource
pressure is a credible contributor to intermittent scheduling latency, but the
single stale heartbeat is not attributed to it until a corrected complete soak
proves the relation.
The agent browser control surface currently exposes no browser session and was
not replaced with an HTTP-only smoke check. After the successor deployment, the
operator repeated the real-browser checks and confirmed that both the embedded
webserver and cloud webapp render video. This closes those two visual paths on
the release actually under soak; mobile coverage remains a separate release
gate unless it was included in the tested client matrix.
