---
name: synchronization
message: Commit durable optimistic work and reconciliation atomically without exposing sync metadata to applications.
paths: ["Sources/IsocacheGRDB/**/*.swift", "Tests/IsocacheGRDBTests/**/*.swift"]
---

# Durable replica state

IsocacheGRDB owns synchronization metadata. Applications own domain tables, deterministic reducers and queries.

## Rules

Commit sequence allocation, immutable command persistence and optimistic effects in one SQLite transaction.
Prepare commands with explicit values and stable identifiers before replay. Give domain callbacks that same GRDB
transaction; callbacks perform no network work or independent commits and generate no identifiers or clock values.

Reconcile authoritative rows, acknowledgements, terminal outcomes, cookie and optimistic replay together. Keep
pending and sent operations visible until a pull confirms their outcome. A clear reset preserves queued work.
Ignore stale ordinary views. Permit a valid clear reset to restart view order after server view loss while still
validating acknowledgements and operation identity.

Keep sync metadata private to the library. Expose typed status, failure and atomic command-preparation operations
instead of a database property, raw queue insertion or application-authored SQL against sync tables. Application
stores may retain the DatabaseQueue they supplied, solely for their own domain data and query observation.
Keep rejected intent until the application explicitly dismisses it. Dismissal followed by retry preparation must
commit with the new operation, so interruption cannot lose the user's intended edit.

Preserve installed data and migrations. A new migration may extend library metadata; never edit a historical
migration or replace an account's client identifier, sequence, pending, sent or rejected operations during adoption.

## Checks

Run the GRDB tests through `swift test --jobs 4`. Preserve checks for rollback, restart, reset, sent work, retained
failures and atomic retry. Review the public API and consuming adapters for direct sync-table reads or raw queue
writes. Test upgrades from the previous persisted schema with work in every retained status.
