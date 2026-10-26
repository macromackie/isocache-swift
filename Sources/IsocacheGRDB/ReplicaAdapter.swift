import GRDB
import Isocache

public struct ReplicaAdapter<Command: SyncCommand, Entity: Codable & Sendable>: Sendable {
  public let clearRemote: @Sendable (Database) throws -> Void
  public let remove: @Sendable (Database, String) throws -> Void
  public let upsert: @Sendable (Database, Put<Entity>) throws -> Void
  public let resetLocal: @Sendable (Database) throws -> Void
  public let apply: @Sendable (Database, OperationContext, Command) throws -> Void

  public init(
    clearRemote: @escaping @Sendable (Database) throws -> Void,
    remove: @escaping @Sendable (Database, String) throws -> Void,
    upsert: @escaping @Sendable (Database, Put<Entity>) throws -> Void,
    resetLocal: @escaping @Sendable (Database) throws -> Void,
    apply: @escaping @Sendable (Database, OperationContext, Command) throws -> Void
  ) {
    self.clearRemote = clearRemote
    self.remove = remove
    self.upsert = upsert
    self.resetLocal = resetLocal
    self.apply = apply
  }
}
