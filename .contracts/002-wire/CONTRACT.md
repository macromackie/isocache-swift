---
name: wire
message: Decode known representations strictly and preserve supported operation and receipt formats.
paths: ["Sources/Isocache/Protocol.swift", "Tests/IsocacheTests/**/*"]
---

# Wire representations

## Rules

Keep operation identifiers, sequences, names and arguments immutable after enqueue. Encode the same values for a
retry. Use named outcome values instead of requiring a caller to interpret an arbitrary status string.
Keep any supported legacy receipt shape explicit at the decoder boundary rather than spreading optional-field
interpretation through application code.

Skip unknown entity kinds so a supported client can read a view containing newer entities. Reject malformed known
entities and mismatched representations. Preserve supported wire fields and defaults through explicit versioning
and fixtures when changing the reader; a package API refactor alone does not authorize narrowing stored payloads.

Keep UI frameworks, authentication providers and application domain types outside the package. Share wire fixtures
with the engine through versioned, credential-free examples.

## Checks

Run the wire tests and decode the shared versioned fixture. Exercise absent legacy receipt fields, known terminal
outcomes, unknown entity kinds and malformed known entities. Compare encoded retry envelopes before and after a
reader or public API change.
