# Bounded startup process image residency

## Problem

On Linux systems with slow storage, reclaiming clean executable pages can stall
an otherwise asynchronous video daemon for seconds. The wait may be in Go
runtime metadata or formatting, before any diagnostic writer or media queue.
An output queue or longer deadline cannot prevent such a page-in.

## Decision

The daemon offers `--resident-image-budget-bytes=N` as an explicit process
startup resource policy. Zero leaves residency disabled. A positive value
requires the Linux PAL to identify all currently loaded file images with an
executable mapping and reserve their readable, non-writable mapped file ranges.
It includes runtime metadata and constant data, not only instructions.

The PAL opens and verifies each image's device/inode identity, creates a separate
read-only shared alias of each selected file range, and locks that alias. These
aliases keep the underlying file pages resident without changing protections
or lock ownership of the running program's VMAs. Each owner closes only its own
aliases; partial failure and cancellation release acquired mappings. Admission
fails before capture on budget exhaustion, changed identity, unsupported PAL
or insufficient `RLIMIT_MEMLOCK`. The launcher must provision that limit; the
SDK never raises its privileges or changes OS memory policy.

The daemon owns this reservation until its media/control loops and bootstrap
have joined. Its admission log reports actual reserved bytes and requested
budget. No frame protocol or persisted product schema changes. Library API
users are not silently subjected to process-wide memory policy.

## Limits and alternatives

This covers startup file images only. It does not pin heaps, stacks, models,
shared media, later `dlopen` images or independent child executables. Read-only
file pages of shared libraries can also be used by children, but that is not
an attestation of a child's complete working set. It does not fix storage
latency, guarantee hard real-time delivery or prove camera/driver health.
Immutable startup images are required; live truncation/replacement is outside
the contract. Fork/exec children acquire no reservation ownership.

`mlockall` would pin unbounded mutable buffers and future allocations. Locking
original VMAs would interfere with locks owned by an embedder. Copying entire
releases into RAM duplicates unrelated files and still needs swap discipline.
A bounded file-image reservation addresses the observed page-in path without
changing media quality, deadlines, accelerated backends or data durability.

## Verification

Linux regression tests verify real kernel memlock accounting, independent
owners sharing file pages, idempotent release, partial-failure cleanup, budget
and identity refusal, cancellation and insufficient memlock. A daemon test
proves refusal precedes capture bootstrap. Hardware qualification must still
measure the actual locked bytes, memory headroom, media progression, complete
original soak and closed recording decode under the unchanged workload.
