---
name: session-lifecycle
message: Fence delayed work to its original account and make connection recovery explicit and bounded.
paths: ["Sources/Isocache/SyncCoordinator.swift", "Sources/Isocache/LiveConnection.swift", "Tests/IsocacheTests/**/*"]
---

# Session lifecycle

## Rules

Bind a coordinator to one store and captured authenticated transport for its lifetime. Invalidate its scope
synchronously before replacing the store or session. A delayed push, pull or socket setup cannot mutate a replaced
scope, including when the transport ignores task cancellation.

Coalesce overlapping sync calls without losing a later refresh request. Keep task and socket ownership together.
The foreground caller owns cancellation; close owned sockets when that caller ends or the scope is invalidated.

Treat pokes as hints to pull; they contain no authoritative domain state. Keep retries bounded by the active scope
and a capped delay. Distinguish cancellation, retryable connection failure and a sync failure that callers need to
handle. Make those decisions visible through typed diagnostics without logging credentials or response bodies.
Applications own foreground polling and user-facing recovery policy.

## Checks

Run coordinator and live-connection tests. Hold a response, invalidate its scope, then complete it and confirm no
store mutation. Verify reconnect, cancellation during connection setup and overlapping refresh behavior with explicit
gates. Inspect error branches to distinguish deliberate best-effort recovery from an accidentally hidden failure.
