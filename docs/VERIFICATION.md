# Verification

`scripts/check.sh` runs the standalone Go suite, Python SDK and recovery tests,
Go vet and governance checks. Go tests include native C fixtures and sanitizer
checks where required by existing tests. Application integration tests are
maintained by the integrating applications, outside this repository.

`scripts/build-release.sh /absolute/temporary/output portable` builds a complete
Linux asset. `rdkx5` must run on an arm64 RDK X5 with the vendor development SDK.
`scripts/test-installer.py <asset> <sha256> <variant>` verifies a fresh install,
reinstall, state/config preservation, payload-only installation and rejection of
an invalid digest. No camera is opened by these installation tests.

Native libraries: GStreamer 1.0 development/runtime, OpenSSL, C toolchain; RDK X5
also needs the platform's `sp`, HBN, multimedia and TurboJPEG SDK. Runtime install
requires Python 3.10+ and the corresponding system libraries. Driver/OS updates
are outside the installer. Portable means generic Linux media discovery, not
universal codec availability. macOS and Windows PALs are not implemented.

Hardware qualification records acquisition format, negotiated branches, actual
decode/resize/encode backend, live/archive/analysis progress, CPU/memory, cadence,
latency and temperatures under the same configured workload. CI and fixture
results cannot substitute for this. See `docs/QUALIFICATION.md` for migration proof.
