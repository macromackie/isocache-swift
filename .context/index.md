---
okf_version: '0.2'
---

# Isocache Swift context

The client renders a GRDB projection of authoritative rows plus pending local operations. An app supplies
typed reducers and queries inside the engine's transaction; the coordinator supplies account-bound transport
and recovery. See the [synchronization contract](../.contracts/001-synchronization/CONTRACT.md) and
[server design](https://github.com/macromackie/isocache-engine/blob/main/.context/cvr-design.md).

Keep domain conflict decisions in the consuming app. Keep shared wire fixtures under `Tests/IsocacheTests/Fixtures`.
The package currently exposes two runtime products; application-specific headless drivers stay with their apps.

The application retains its DatabaseQueue for domain data. ReplicaStore exposes typed metadata, observation and
atomic rejection recovery rather than its database or queue insertion/rebase internals. `ReceiptStatus` preserves
an unknown wire value at decoding and rejects it atomically during reconciliation. `SyncFailure` reports connection
and refresh failures while account scope and the foreground caller still own cancellation. The separate
[wire](../.contracts/002-wire/CONTRACT.md) and [session lifecycle](../.contracts/003-session-lifecycle/CONTRACT.md)
contracts keep those decisions reviewable. [Guidance adoption](../.agents/guidance-adoption.md) records the shared baseline.
