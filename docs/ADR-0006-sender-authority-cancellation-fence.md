# ADR 0006: fence cancellation after sender authority checks

Status: implemented and host regression verified; hardware qualification open.

Process/consumer authority checks can return after cancellation. The sender
previously checked context before authorization and after WritePacket, but not
between authorization and that write, nor after confirming peer generation.
A deterministic regression reproduced an attempted write after cancellation
and a canceled post-write confirmation incorrectly attributed to snapshot work.
This proves a boundary defect; it does not establish the cause of the earlier
partially delivered frame or of any vendor encoder timeout.

Check the same context after each authority boundary, before side effects and
before accepting confirmation. No new timeout, clock, sleep, provider, queue,
buffer or wire format is added. Existing ownership remains: cancellation before
any attempted write permits reuse; any partial/attempted transfer retires the
sender. Keep Is/As causes and add bounded phase identifiers, without private
provider messages. Context deadline/cancellation now have explicit diagnostic
cause names instead of being mislabeled as an unclassified transport fault.

Alternatives rejected: extending deadlines, trusting only the provider to
check cancellation, or silently retrying an uncertain partial delivery.

Tests exercise cancellation in the peer authorization and post-write authority
checks, verify no late side effects, reusable pre-write state, retired partial
state and exact causal/phase attribution. The regression failed before the
change; the frameingress suite with race detector passed afterward. No native
accelerator, real stream or prolonged runtime claim follows from that proof.
