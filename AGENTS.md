# VideoSluice governance

Canonical instructions for contributors and agents. Read `docs/ARCHITECTURE.md`,
`docs/GOVERNANCE.md` and `docs/VERIFICATION.md` before changing runtime code.

- VideoSluice is an independent video product. Never import an embedding
  application's configuration, model, device inventory, identity or supervisor.
- Domain policy depends on provider contracts. Concrete platform/hardware
  adapters are composed at startup. Preserve the existing public frame ports;
  do not manufacture generic abstractions merely to rename directories.
- PAL owns OS/process/socket/shared-memory/time primitives. HAL owns media
  engines and camera/accelerator capability discovery. Native RDK and GStreamer
  implementations stay behind adapters. A board label is not capability proof.
- `config/` is the runtime schema/default SSOT, `protocol/` and `framewire/`
  define wire constants, `VERSION` defines the release version. Consumer
  applications translate their own configuration into these contracts.
- One source owner, bounded queues and deadlines, authenticated process leases,
  frame provenance and source-local recovery are functional invariants.
- Prefer supported hardware per media stage. Never enable software fallback or
  lower cadence/quality to manufacture a passing qualification. Report actual
  backend and fallback reason. Never infer inside the broker.
- Preserve useful configs, selections, guard evidence and data on upgrade.
  Remove obsolete runtime paths only after replacement qualification. A
  retained immutable rollback release is not a second active implementation.
- Build/package only in temporary directories outside this repository; check
  the operation filesystem's capacity first. Reuse caches. Delete only owned
  scratch/output. Never delete operator caches, recordings or evidence.
- Run `scripts/check.sh` and artifact installer tests before publication.
  Tag only the exact commit whose CI passed. Linux amd64/arm64 are supported;
  new OS support requires complete PAL providers, packaging and runtime proof.
- Never publish credentials, private captures, customer identifiers or raw live
  logs. Review scoped diffs and the complete public file set before pushing.
- Installed, operational, functional and hardware-qualified are separate claims.
  Document exact version/hash, host, workload, measurements and limitations.
