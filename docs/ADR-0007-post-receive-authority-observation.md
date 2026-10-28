# ADR 0007: distinguish authority and interpreter delays after complete receive

A delivery can complete its native exchange and promptly return to Python, yet
fail the deadline after the following authority check. Native IO duration and
the first interpreter resumption do not identify this later interval. The outer
transaction duration also includes diagnostic collection, so attributing its
entire duration to authority, the interpreter, the driver or storage is unsound.

Keep the existing fail-closed lifetime: every expired operation is rejected,
the cursor stays unchanged and the channel is retired. A complete payload is
not a fully validated publication and does not authorize safe continuation.
Do not increase budgets, silently drop errors or make expiry look like idle.

The Linux provider wraps its existing nonblocking authority operation with
monotonic and thread-CPU stamps in four caller-owned integers. The Python call
continues to retain the interpreter lock. Compare the native boundaries with
the first Python timestamp to distinguish time inside authority from its return
to the interpreter. The latter remains a scheduling/return gap, not a measured
GIL wait. No new thread, descriptor, queue, lease or wire field is allocated.
Clock failures leave diagnostic values unavailable and preserve the original
authority result. The matching helper is required before descriptor admission.

Operation-local progress records the failing channel phase and its observed
elapsed time before collecting diagnostics; collection has its own duration.
Only failure logging projects these values. The outer reader retains that one
progress object instead of duplicating its native diagnostic fields. No frame
data or provider exception text enters the log.

Alternatives rejected: guessing a GIL/storage cause from an outer timer;
extending the deadline; accepting a late frame; treating completed native bytes
as sufficient authority to keep a failed channel alive; tracing every frame.

Authenticated native regressions delay the post-receive authority return and
the diagnostic copy independently. They distinguish those intervals while
retaining expiry, retirement and the unadvanced cursor. Existing native tests
prove authority and diagnostic getters retain the GIL, socket waits release it,
and partial/corrupt/foreign frames remain rejected. Physical cause and hardware
qualification require integrated observation of the same failing interval.

Review of the same failure path exposed a separate lifetime defect: the outer
reader treated its closed-admission flag as completed provider cleanup. A second
close could therefore return while the first was still joining native IO, or
hide an uncertain release. Always delegate close to the channel's existing
serialized, idempotent completion instead of introducing another lock/cache.
The deterministic concurrent regression holds cleanup in flight, verifies both
callers reach the provider, and preserves the same failure without repeating
native release. This fixes lifetime reporting; it does not explain delivery
latency or make a failed transaction reusable.
