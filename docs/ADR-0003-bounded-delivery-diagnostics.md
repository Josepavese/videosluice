# ADR 0003: keep bounded delivery diagnostics inside the interpreter boundary

A completed native delivery can pass its deadline checks and then wait again
when the Python adapter copies diagnostic timings. The ctypes CDLL getter
releases the GIL even though the C function only copies four thread-local
integers. An unrelated native inference holding the interpreter can therefore
delay the receiver after its successful transaction. An observed late return
with complete payload counters makes this a concrete boundary defect; it does
not establish the cause of every previously observed hardware or storage wait.

Call this fixed-size getter through PYFUNCTYPE, retaining the GIL as already
done for the bounded authority check. Socket waits, bootstrap, payload transfer,
hashing and close continue to release it. Do not extend deadlines, accept late
frames or change hashes, credentials, memory ownership, wire formats or data.

The regression uses an actual authenticated native idle transaction and observes
PyGILState_Check inside the timing getter. It fails before the correction and
passes afterward; the same fixture proves socket receive still releases the GIL.
Existing full-delivery concurrency, late-return rejection and integrity tests
remain required. Integrated hardware qualification is separate from this proof.

Delivery failure reports also copy the four existing native stage timings from
the operation-local progress object. Previously these were collected but lost
by the outer reader on an exception, making a native wait indistinguishable
from a delayed Python resumption. These are diagnostic durations, not a new
acceptance gate: zero can mean an uncompleted or unreached native stage. They
do not establish a GIL, scheduler, hardware or storage cause on their own. No
new native call, success log, wire field, deadline or fallback is introduced.
Tests retain exact failure/cause, channel retirement and private-message hiding,
and cover the late-return path through the actual authenticated native fixture.
