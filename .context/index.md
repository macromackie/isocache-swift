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
