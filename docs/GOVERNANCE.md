# Architecture and change policy

Inspired by Larva's PAL/HAL/SSOT/provider governance; adapted to Go/C/Python and
the existing video architecture rather than copying Rust or C API requirements.

| Boundary | Current owner | Rule |
|---|---|---|
| Product graph, defaults, validation | `config/` | One schema; no application fields or copied defaults |
| OS PAL | `framepeer/provider/linux`, `framewire/provider/linux`, `internal/unixsocket`, `framedomain/monotime`, `framedomain/entropy` | Domain ports stay independent of Linux types |
| Media HAL and adapter discovery | `capability/`, `internal/source/`, `native/`, portable ingest command | Capability-attested per-stage selection; vendor types do not leak to consumers |
| Ownership and admission | `framedomain/`, `frameaccess/`, `frameingress/` | Explicit lifetime, bounded resources, cancellation and single owner |
| Public transport | `protocol/`, `framewire/`, `framepeer/`, `sdk/python/` | Versioned contracts, validated sizes, deadlines and process identity |
| Composition | `cmd/video-broker/`, `service.go` | Inject providers here; clients cannot select internal adapters |
| Immutable distribution | `scripts/build-release.sh`, `install/` | Same-commit complete asset; verified hash; no source build during install |
| Installed PAL home | installer | `releases`, `current`, `bin`, `configs`, `data`, `logs`, `artifacts`, `backups`, `tmp` |

The PAL is both a platform abstraction boundary and an installed application
home; these meanings are distinct. The layout above maps implemented boundaries,
not a claim that every inherited package already meets ideal isolation. Existing
AST/native boundary tests enforce the frame ports; changes elsewhere must avoid
new platform leakage and document a concrete extraction when appropriate.

Nontrivial changes require a short ADR with problem, chosen boundary,
alternatives, data/contract impact and qualification. Add regression and provider
contract tests for altered behavior. Changes to ownership/wire/native resources
require affected producer/consumer, race and sanitizer coverage. Tests can use
fixtures, but hardware claims require real media and measured backend telemetry.

Dependencies are pinned in Go modules/checksums and wrapped where they cross
product contracts. Vendor SDK libraries remain system dependencies, not bundled
proprietary redistributions. Review licensing and ABI for new dependencies.
Cryptography uses Go/OS/OpenSSL primitives; never invent auth or hashes. Public
IPC accepts only validated, bounded requests; secrets and private media stay out
of logs and Git. The SDK knows geometry and opaque IDs, never people or models.

The only current runtime is the released product. A data schema change uses a
one-time verified conversion/export, not indefinite dual readers. Rollback keeps
an immutable previous asset; installer upgrades never erase mutable state.
Embedding platforms own hardware reset, service supervision and deployment
transactions; the broker owns only source/media workers and bounded recovery.

## Changes originating in embedding applications

An embedding application's materialized copy may serve as an integration/test
bench, but must not become a maintained fork or downstream patch set. Bring
framework corrections back to this repository within the same work batch and
distribute the resulting immutable release through the consumer's dependency
pin. Preserve experimental provenance until reconciliation is verified.

Every accepted correction must demonstrate concrete general product utility:
describe its problem, contract and regression without depending on the originating
application. Preserve this repository's architecture and public boundaries;
renaming application-specific structures or adding speculative abstractions
does not generalize a change. Application configuration, models, identities,
supervision and domain policy remain in their owner and use the public contracts.
