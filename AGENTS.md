# Isocache

Read [.contracts](.contracts/index.md) before changing synchronization behavior. Keep the package compatible
with Linux and its public examples independent of a consuming app, credentials, and production endpoints.
Applications own their domain adapters; the package owns protocol and persistence mechanics.

Run `mise run check` before handing off changes. Plain `swift test --jobs 4` runs the runtime tests without
the contract CLI. Read [README.md](README.md) for adoption and migration boundaries. Change contracts only
when that work is explicitly requested; design notes belong in `.context`.
