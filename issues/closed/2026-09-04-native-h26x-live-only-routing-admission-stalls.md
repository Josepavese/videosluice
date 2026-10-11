# Native H26x live-only routing admission stalls

## Esito della cernita - 2026-09-06

Stato: **CHIUSA - COMPLETATA**. L'ammissione H26x live-only attesta il decoder realmente verificato senza aspettare analysis.

Evidenza: Replay test board commit636e5f2: rev24 completata, 60 access unit a 25 fps; negative MJPG/portable nei test.

Riesame del repository e delle prove registrate, non nuova verifica live.
[Registro della cernita e limiti residui](../../../../improvements/TRIAGE_2026-09-06.md).
Le sezioni seguenti sono storico cronologico: stati e istruzioni di rilascio
precedenti non descrivono lo stato operativo corrente.

## Storico conservato


- Status: fixed, deployed and hardware-verified on test board
- Severity: release blocking
- Discovered: 2026-09-04
- Components: video-broker camera admission, capability attestation, runtime routing

## Symptom and evidence

After test board received the central dual-camera v7 profile, routing revision 23
correctly exposed `front-h265` as H265 1280x720@25. A complete preview moving
only `live` from overview to front was valid and predicted revision 24 with
only the live generation advancing from 17 to 18.

Operation `apply-front-live-20260904-1` started the front camera privately. The
native bridge selected H265 1280x720@25, admitted complete VPS/SPS/PPS and a
keyframe, and sustained about 25 fps with no capture timeout or corrupt frame.
Nevertheless the transaction remained in `admission` for its full 90-second
deadline and terminated `failed/abort`. The candidate was shut down
cooperatively and the persisted plan stayed at revision 23 with all roles on
overview. Tracker and recorder were not moved or restarted.

## Root cause

The native V4L2 bridge validates compressed H264/H265 capture through its
hardware decoder and admits the camera only after decoded NV12 cadence is
healthy. The Go runtime previously attested the selected capture decoder only
when an NV12 analysis packet crossed the broker. A live-only native-passthrough
candidate publishes no analysis packets, so its real VPU proof never reached
the capability plan and source-move admission waited forever.

This was a contract gap, not a slow camera, software fallback or test board-specific
policy. The same topology could affect any native V4L2 H264/H265 source whose
first selected role is direct live.

## Correction and acceptance

- Bind native V4L2 H264/H265 decoder attestation to successful camera admission,
  whose bridge contract already includes decoded-cadence proof.
- Keep the rule scoped to the generic `native_sp` V4L2 contract rather than a
  machine, USB identifier or camera model.
- Keep MJPEG on explicit capture-decoder telemetry so JPU and CPU fallback can
  never be conflated.
- Keep portable workers on their backend-specific stage telemetry; camera
  admission alone must not forge their decoder evidence.
- Add a regression reproducing the live-only H265 source-move gate and negative
  tests for portable H265 and native MJPEG.

The targeted broker suite, complete Go suite, race detector and vet pass.

## Hardware verification on test board

Commit `636e5f2` was deployed through the official signed A/B path with:

- artifact `557243cbf60179a23f3b071b338eda20694dda67f87d1822df42c0eb024edb85`;
- sequence `1788481841468987401`;
- transaction `6d2da1270e0bb11d4364423547630e10e9196234c925c1fc9ce18d3d0e9a683a`;
- exact postflight measurement: 8,897 entries and 547,543,953 bytes.

Trial, promotion and postflight completed successfully. The agent remained on
PID `2739796` with zero restarts. Operation
`apply-front-live-fixed-20260904-1` then completed in 7.56 seconds and advanced
the persisted plan to revision 24. Only `live` moved to `front-h265` and only
its generation advanced from 17 to 18; `analysis` and `archive` remained on
the overview source with generations 4 and 7.

The front capture owner remained healthy on PID `2745296`, epoch 1, at H265
1280x720@25. The overview capture owner remained healthy on PID `2740171`,
epoch 1, serving analysis at 20 fps and hardware H265 archive at 15 fps. An
independent live consumer received 60 distinct NMB2 H265 access units at
exactly 25.00 fps; the first access unit carried codec configuration and
keyframe flags. Live and archive reported zero drops and disconnects. The
analysis counter contained eight startup drops predating the switch, but the
post-switch interval added none. Tracker summaries remained healthy at about
18 processed fps with zero recognition or PTZ errors.

The defect and its hardware gate are therefore closed. Browser presentation
and the broader test board stability gates remain separate release qualifications;
deployed board remains untouched until those gates pass.
