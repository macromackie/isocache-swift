# Isocache

Isocache is a Swift synchronization library for applications that keep working offline. It
commits local commands with their optimistic results, pushes an ordered durable outbox, and
rebases pending work onto authoritative server updates.

The package has two products:

- **Isocache** supplies typed protocol-v2 envelopes, a session-bound sync coordinator, and the
  pull-hint WebSocket transport. It imports no database or UI framework.
- **IsocacheGRDB** supplies a transactional replica store using GRDB 7.11.1. Your application owns
  its tables, migrations, queries and deterministic command reducer.

## Install

Add the package to your SwiftPM manifest and select the products your targets need:

```swift
.package(url: "https://github.com/macromackie/isocache-swift", exact: "0.2.0")
```

```swift
.product(name: "Isocache", package: "isocache-swift"),
.product(name: "IsocacheGRDB", package: "isocache-swift")
```

These are manifest entries, not a complete package declaration. Commit the application's `Package.resolved`.
The matching [Isocache Engine](https://github.com/macromackie/isocache-engine) implements the Postgres server.

## Application boundary

Implement `SyncCommand` for your command enum. Its stable `name` and encoded arguments form the
wire operation `{id, sequence, name, args}`. Entities remain application types. Throw
`UnknownEntity` only for an unknown entity kind; malformed known entities must fail decoding.

Create your domain migrations, then call `ReplicaStore.registerMigrations(in:)`. Run the migrator
on a GRDB `DatabaseQueue` and pass that queue, the account ID and a `ReplicaAdapter` to the store.
The adapter applies remote rows, resets local projections, and applies optimistic commands.
Every callback runs inside the store's transaction. Retain the application's `DatabaseQueue` for
domain queries; the replica does not expose it. Read and observe domain queries through GRDB
or `store.observe`. Use `ReplicaStore.metadata(in:)` inside an observed query to include sync counts,
confirmation state, and failure summaries in the same database snapshot. The application never
queries the library's `sync_state` or `sync_operations` tables.

Use `enqueue(prepare:)` when a command captures a value from local state. Preparation receives the
same `Database` and immutable `OperationContext` as the optimistic reducer. A conditional edit can
capture the previous accepted or queued operation ID without racing another local edit. Replay
uses the saved command and its original ID; it never recaptures that predecessor.

Rejected operations retain their original payload, message and optional reason code. The app can
read retained failures with `rejections()` or `rejections(in:)` and decode the known commands with
`decodeOperation()`. Normal metadata and failure lists do not decode commands, so an older
message-only rejection cannot block recovery of an unrelated known command. Use
`dismissRejections(ids:)` to dismiss a historical failure explicitly without decoding its payload.

Resolve a conflict with `resolveRejections`. Its closure receives the current failures, the
transaction and a fresh operation context. Return `.dismiss(ids:)` to keep the server's result,
or `.retry(command, dismissing: ids)` to replace those failures with a new command. Selection,
dismissal, immutable command insertion, sequence advancement and optimistic application commit
together. A failed replacement leaves the prior failures intact. The library does not choose
which competing domain value should win.

## Synchronization

Construct `SyncCoordinator` with `store.syncStorage` and a `SyncTransport` whose push, pull and
optional socket closures capture one authenticated session. `sync()` coalesces concurrent calls;
`refresh()` waits for an existing sync and pulls again. A foreground task can call `listen()` to
pull after reconnect and on each hint. The caller cancels that task on backgrounding and retains
its foreground polling fallback.

An optional `onFailure` callback reports automatic listener failures with their phase and original
error. Explicit `sync` and `refresh` still throw to their caller. Reconnect delays are capped at one
minute; cancellation, session invalidation, or a failed retry sleep ends listener work.

Call `invalidate()` synchronously before replacing the account or session. It closes the live
socket and fences delayed network results from writing to the store. It does not promise to abort
an HTTP request already sent. Create a new coordinator for the new session.

Push success marks commands sent, but keeps their optimistic effects until a pull acknowledges
them. A failed pull can retry without losing that state. Reconciliation atomically applies the
patch, retires accepted commands, retains failures, records the cookie, and replays outstanding
work. A receipt's `ReceiptStatus` distinguishes rejection from a committed result with a notice.
Unknown status strings survive decoding but block reconciliation, preserving local work until the
client understands the outcome. A reset clears only the authoritative replica. A valid lower-order reset is accepted when
the server loses its cached view; ordinary older updates are ignored.

Use separate database paths for separate accounts. The stored account identity is checked when
opening the replica. `deleteAccountData` clears replica and queue state and rotates its client ID;
its callback clears application-owned auxiliary data in the same transaction.

## Upgrading from 0.1

Version 0.2 narrows the public API without changing persisted queue or wire formats. Keep all
previously registered migrations. Applications retain their own `DatabaseQueue` instead of reading
`ReplicaStore.database`. Replace raw queue reads with `metadata(in:)` or `rejections(in:)`, and
replace manual rejection deletion plus enqueue with `resolveRejections`. Raw insert, operation
selection and rebase helpers are private. No existing operation ID, sequence, client ID, cookie,
or pending/sent/rejected row is regenerated by this change.

## Development

The package requires Swift 6.1 or later and supports macOS 15, iOS 18, and Linux. Run `swift test --jobs 4`.
The tests cover rollback, restart, sent work, resets, retained rejection payloads and invalidated
network responses. See [AGENTS.md](AGENTS.md) for the invariants contributors must preserve.
