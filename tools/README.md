# Capture release guard recovery

`recover_capture_guard.py` is a standalone administrative tool, not a Nemesi
watchdog and not an automatic interpretation of an OS/service restart. It
requires a **physically completed hardware power cycle**, confirmed by the
operator, and a current boot different from the boot of the incident. Every
accelerator and camera owned by that source must have been power-reset. If
this evidence is unavailable, retain the guard and diagnose the platform.

The embedding application must hold its maintenance/update lock, stop the
source owners, verify its installed release, preserve configuration, and
provide the exact guard SHA-256, incident identity, previous boot identity,
service state UID and executable paths of all source owners. The tool checks
quiescence twice, refuses symlinks and unexpectedly writable state, and moves
the exact guard into the required `--recovery-root` with a durable reset receipt.
That archive and its ancestors must be protected and root-owned; select the
administrative PAL, outside the capture state tree. An upgrade can legitimately
reassign runtime state to the service account, but must never transfer ownership
of reset attestations. Existing archives below runtime state remain historical
evidence: this tool does not delete, trust or silently migrate them.
It never changes a release, configuration, hardware control or service.
It is idempotent for the same incident/reset and refuses stale requests.
An interrupted receipt write/archival leaves either the guard or its archived
incident; restart the same request only after rechecking maintenance ownership.

After successful recovery, the integration may start its source owners and
must verify fresh capture, analysis and archive progress. A service being
active is not recording proof. This recovery does not qualify an untested
software candidate or automatically authorize deployment to another device.

New guards record the kernel boot identity. A guard written before that
metadata existed requires the previous boot identity from retained incident
evidence, explicitly supplied by the administrator; absence is never treated
as an automatic release receipt. Existing incident data is archived, not lost.

Regression command:

```sh
python3 -B -m unittest discover -s tools -p 'test_recover_capture_guard.py'
```
