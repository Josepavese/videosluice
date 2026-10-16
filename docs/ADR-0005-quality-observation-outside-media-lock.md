# ADR 0005: quality observations cannot retain the media mutex

Health/status and pipeline summaries decoded the quality observation file while
holding the source's media mutex. A deterministic blocked-file regression stops
the actual archive dispatch in that path. Even a runtime/tmpfs pathname cannot
make file I/O a bounded in-memory operation; this does not retroactively prove
the cause of an earlier device gap or a hardware/storage defect.

Use one source-local observer, one read at a time, a one-second sampling cadence
and one decoded snapshot. File read and JSON decoding happen outside the media
mutex. The metadata read is bounded to 1 MiB, generously above current fixed-grid
producer output; no pixels enter this cache. The key is the exact observation
path plus source media epoch. Canceled or old-epoch reads cannot publish.

Health still checks the producer's original timestamp, the existing future/stale
bounds and the original startup grace on every request. Reader completion never
refreshes a sample. A newly observed read/decode failure replaces cached success;
missing/current-epoch-pending evidence is unavailable, never manufactured healthy.
Optical darkness remains distinct from transport failure. No backend, fallback,
profile, quality threshold, frame provenance or wire schema changes.

The worker is joined through the existing runtime wait group. Kernel file I/O is
not declared canceled by a timeout: shutdown retains ownership until the real
read returns. A blocked observation no longer holds frame/status locks or creates
replacement workers. Polling is independent of status-client request volume and
continues when the product has no status consumer; errors/slow reads are logged
at most once per thirty seconds without arbitrary file contents.

Alternatives rejected: synchronous reads outside the mutex still block status
RPCs; reading only when a metrics log is due makes healthy evidence spuriously
expire; extending freshness bounds or accepting stale data conceals the fault.

Regressions cover blocked-read archive progress, cancellation/epoch fencing,
unchanged timestamp validation, malformed replacement and metadata size limits.
Run the standalone suite, affected race tests and installer tests, then qualify
the exact integrated device candidate. Synthetic fault injection is host-only.
