# Isocache Swift contracts

- [Durable replica state](001-synchronization/CONTRACT.md): atomic queue, reconciliation and library-owned metadata.
- [Wire representations](002-wire/CONTRACT.md): supported envelopes, outcomes and unknown entity handling.
- [Session lifecycle](003-session-lifecycle/CONTRACT.md): account fencing, cancellation and bounded connection recovery.
- [Shared ownership](004-code-ownership-and-boundaries/CONTRACT.md): one owner and explicit package dependency direction.
- [Contract maintenance](005-contract-maintenance/CONTRACT.md): scoped, verifiable requirements and deliberate updates.

Run `mise run check:contracts` for document validation and the root `mise run check` for required code checks.
Shared contracts are pinned in [`.tenet.lock`](../.tenet.lock). Their `Local adaptations` sections define repository
scope; [the adoption record](../.agents/guidance-adoption.md) explains the reviewed source and retained differences.
Model assessments are separate from deterministic checks and must report provider failures or incomplete scope.
