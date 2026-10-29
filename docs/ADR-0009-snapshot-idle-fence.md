# ADR 0009: classify snapshot misses after the resource fence

A complete delivery can spend its transaction budget inside a snapshot and
then fail while trying to send the no-delivery record. Reporting only that
last write loses the snapshot checkpoints. More seriously, matching one
sentinel with `errors.Is` can turn a joined authority, mapping or cancellation
failure into ordinary contention or idle.

Keep the current wire protocol and all existing budgets. A provider miss is
recoverable only when its sole cause is the contention sentinel. Linear
diagnostic wrappers preserve that classification; joined causes remain hard
failures. Reuse the same linear-cause operation for output pressure, source
surface/output contention and delivery idle. No new provider or public error
model is introduced.

Inside a delivery snapshot, remember a miss as an operation-local flag and
return callback success. Emit the idle sentinel only after the complete
resource borrow, including source/process authority and cancellation fences,
has succeeded. A failed final fence therefore retains its snapshot phase,
timings and original error chain. The sender accepts only a sole idle cause
before constructing FAPI. No pixels, retry, queue, retained reference or late
result is introduced, and consumer/write failures remain fatal.

Deterministic regressions join busy with unauthorized, foreign mapping and
deadline failures, and cancel source authority after a busy provider returns.
The previous implementation either counted those failures as harmless misses
or blamed the later idle write. Tests also preserve normal contention followed
by delivery of the exact real frame on the same binding. A structural provider
failure must stop at its first slot rather than polling another one.

This corrects classification and observability. It does not prove the physical
cause of a slow snapshot or remove its transaction deadline. CPU scheduling,
page/driver waits and native copy/hash time still require evidence from the
same failing interval; absence of a recurrence is not a causal proof.
