# ADR 0002: observe native output submission waits

A hardware qualification observed a 3.95-second analysis submission interval
with only 3 milliseconds of thread CPU, followed by an interframe capture
discontinuity. This identifies an off-CPU wait but does not identify the blocking
operation. A subsequent passive observation did not reproduce it.

Record lifetime maximum wall time and the corresponding thread CPU for transform
digest, authority check, layout/preparation and bounded queue submission. Only
the capture thread writes these counters. The existing nonblocking telemetry
pipe reports them at the ordinary metrics cadence; no per-frame log is added.
These observations change neither authority checks nor publication deadlines,
frame hashes, cadence, codec integrity or persisted data.

Guessing a lock/hash fix or relaxing capture integrity would conceal the cause.
The diagnostic change is not a stall fix or hardware qualification. A synthetic
authority delay tests wait attribution together with the independent delivered
pixel and sequence oracle under address/undefined sanitizers. Hardware evidence
must locate the wait before selecting a functional correction.
