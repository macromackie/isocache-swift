---
name: synchronization
message: Preserve durable optimistic work and prevent a replaced session from applying delayed responses.
---

# Synchronization

Isocache owns the protocol and coordinator; IsocacheGRDB owns local synchronization state. Applications own
their domain schema, reducers, queries, and authorization and conflict policies.

## Rules

Commit sequence allocation, immutable command persistence, and optimistic effects in one SQLite transaction.
Prepare commands with explicit values and stable identifiers before replay. Pass the same GRDB transaction to
application callbacks; keep network work and independent commits out of those callbacks.

Reconcile authoritative rows, acknowledgements, retained rejections, cookie, and optimistic replay together.
Keep pending and sent operations visible until a pull confirms their outcome. Clear resets preserve queued work.
Ignore stale ordinary views; allow a valid clear reset to restart view order after server view loss while keeping
acknowledgement validation. Skip unknown entity kinds, but reject malformed known representations.

Bind each store to one account and each coordinator to one store and captured authenticated transport. Invalidate
the coordinator synchronously before replacing either. Delayed pushes or pulls cannot update an invalidated
scope. Pokes request a pull; they never carry authoritative domain state. The application owns foreground task
cancellation and its polling fallback.

Keep UI frameworks and authentication providers outside the package. Preserve installed schema and wire
compatibility through explicit migrations and fixtures. Do not generate IDs or read the clock inside a replayed
domain reducer.

## Checks

Run `swift test --jobs 4`. The store tests cover rollback, restart, reset, sent work and retained failures; the
coordinator tests fence delayed responses and share a versioned wire fixture with Isocache Engine. Confirm app
adapters preserve their existing database identity and historical migration ordering when adopting a new version.
