import Isocache

extension ReplicaStore {
  public var syncStorage: SyncStorage<Command, Updates<Entity>> {
    SyncStorage(
      requests: { try self.requests() }, markSent: { try self.markSent(through: $0) },
      reconcile: { _ = try self.reconcile($0) })
  }
}
