# Extraction and evidence

Origin: Nemesi `embedded/src/video-broker`, a standalone Go/C/Python module.
All tracked code, native workers, SDKs, fixtures, examples and diagnostic history
were migrated. Application-specific identifiers in public history were redacted;
the original documentation remains recoverable in Nemesi Git history.

Module identity changed to `github.com/Josepavese/videosluice`; transport/schema
identifiers and the Python SDK package remain protocol contracts, not executable
aliases. The installed command is `videosluice`. No runtime API changes are
required for this extraction. A native test's source path was made repository
relative instead of dependent on the former parent directory name.

The release and installer include exact commit, platform, variant and file hashes.
Standalone CI is not live deployment proof; hardware evidence is recorded below.

The first Nemesi sandbox promotion rejected v0.1.0's RDK payload because the
builder umask produced group-writable executables (0775). Runtime compilation
and installation succeeded, but that is not release qualification. Nemesi
restored its previous measured release. v0.1.1 normalizes build and installation
payload permissions to 0644/0755; artifact smoke tests enforce this invariant.

## v0.1.1 hardware integration — 2026-10-11

Nemesi installed the public arm64 RDK X5 asset through this product's installer,
inside its build sandbox, with no broker source compilation. The archive SHA-256
was `8934bedc747223cdcd6282a40c508356bf4caf619369721521fa568929ecdbbf`;
all seven runtime file hashes matched. Exact application measurement and both
trial/final authenticated postflight passed. A separate actual 0.1.0 → 0.1.1
upgrade preserved useful config/data, retained the previous immutable release
and switched the launcher successfully.

The unchanged production monitor passed 1260.12 seconds with 2521 authenticated
samples: 19370 additional tracked frames, unchanged source epoch, two completed
archive finalizations, zero archive/raw drops and zero collection overruns.
Maximum board temperature was 77.633 °C. Native telemetry observed accelerated
resize with zero CPU resize frames and no capture timeout/corrupt frame in the
sampled intervals. These are measurements of one board/profile/workload, not
portable performance targets or a certification of every native worker.

Two earlier monitor windows failed and remain in the private application's
qualification record: stale Recorder audio (3798 ms against a 3000 ms limit)
during a concurrent isolated installer test, and a systemctl query blocked in
storage-backed page waiting beyond its 4000 ms deadline. The later pass used the
same software, configuration and limits after I/O settled. It does not establish
uninterrupted platform latency under concurrent installation. No hidden software
fallback, profile reduction, threshold increase or OS change was used.
