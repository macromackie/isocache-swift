import Foundation
import GRDB
import Isocache

public final class ReplicaStore<Command: SyncCommand, Entity: Codable & Sendable>: Sendable {
  private let accountID: String
  private let database: DatabaseQueue
  private let adapter: ReplicaAdapter<Command, Entity>
  private let schema: Int

  public init(
    database: DatabaseQueue, accountID: String, schema: Int = 0,
    adapter: ReplicaAdapter<Command, Entity>
  ) throws {
    self.database = database
    self.accountID = accountID
    self.schema = schema
    self.adapter = adapter
    try database.write { db in
      if let existing = try String.fetchOne(db, sql: "SELECT account FROM sync_state") {
        guard existing == accountID else { throw ReplicaError.accountMismatch }
      } else {
        try db.execute(
          sql: "INSERT INTO sync_state(id, account, client_id) VALUES (1, ?, ?)",
          arguments: [accountID, UUID().uuidString.lowercased()])
      }
    }
  }

  public static func registerMigrations(in migrator: inout DatabaseMigrator) {
    migrator.registerMigration("isocache-v1") { db in
      try db.execute(
        sql: """
          CREATE TABLE IF NOT EXISTS sync_state (
            id INTEGER NOT NULL PRIMARY KEY CHECK(id = 1),
            account TEXT NOT NULL, client_id TEXT NOT NULL,
            next_sequence INTEGER NOT NULL DEFAULT 1,
            last_confirmed_sequence INTEGER NOT NULL DEFAULT 0,
            cookie TEXT, cookie_order INTEGER NOT NULL DEFAULT 0
          );
          CREATE TABLE IF NOT EXISTS sync_operations (
            id TEXT NOT NULL PRIMARY KEY, sequence INTEGER NOT NULL UNIQUE,
            name TEXT NOT NULL, args TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('pending', 'sent', 'rejected')),
            queued_at TEXT NOT NULL, sent_at TEXT, error TEXT
          );
          ALTER TABLE sync_operations ADD COLUMN error_code TEXT;
          ALTER TABLE sync_operations ADD COLUMN outcome_status TEXT;
          """)
    }
  }

  @discardableResult
  public func enqueue(_ command: Command) throws -> Isocache.Operation<Command> {
    try enqueue { _, _ in command }
  }

  @discardableResult
  public func enqueue(prepare: (Database, OperationContext) throws -> Command) throws
    -> Isocache.Operation<Command>
  {
    try database.write { db in
      let context = try nextOperationContext(in: db)
      return try append(prepare(db, context), context: context, to: db)
    }
  }

  public func requests() throws -> (OperationsRequest<Command>, UpdatesRequest) {
    try database.read { db in
      let state = try Self.requireState(db)
      return (
        OperationsRequest(
          clientID: state["client_id"], operations: try Self.operations(db, statuses: ["pending"])),
        UpdatesRequest(clientID: state["client_id"], cookie: state["cookie"], schema: schema)
      )
    }
  }

  public func markSent(through sequence: Int) throws {
    try database.write { db in
      try db.execute(
        sql: """
          UPDATE sync_operations SET status = 'sent', sent_at = ?
          WHERE status = 'pending' AND sequence <= ?
          """, arguments: [Date(), sequence])
    }
  }

  @discardableResult
  public func reconcile(
    _ updates: Updates<Entity>,
    additionalChanges: (Database) throws -> Void = { _ in }
  ) throws -> Bool {
    try database.write { db in
      let state = try Self.requireState(db)
      let confirmed: Int = state["last_confirmed_sequence"]
      let next: Int = state["next_sequence"]
      guard updates.version == 2, updates.lastSequence >= confirmed, updates.lastSequence < next
      else {
        throw ReplicaError.invalidUpdate
      }
      guard updates.clear || updates.order >= state["cookie_order"] as Int else { return false }
      var changed = updates.clear || !updates.put.isEmpty || !updates.del.isEmpty
      if updates.clear { try adapter.clearRemote(db) }
      for key in updates.del { try adapter.remove(db, key) }
      for put in updates.put { try adapter.upsert(db, put) }

      var receipts: [Int: Receipt] = [:]
      for receipt in updates.rejections {
        guard receipts[receipt.sequence] == nil, receipt.sequence > 0,
          receipt.sequence <= updates.lastSequence
        else { throw ReplicaError.invalidUpdate }
        if case .unknown(let value) = receipt.status {
          throw ReplicaError.unsupportedOutcome(value)
        }
        receipts[receipt.sequence] = receipt
      }
      for operation in try Self.operations(db, statuses: ["pending", "sent"])
      where operation.sequence <= updates.lastSequence {
        if let receipt = receipts[operation.sequence],
          receipt.error != nil || receipt.code != nil || receipt.status == .rejected
            || receipt.status == .appliedWithNotice
        {
          if let id = receipt.id, id != operation.id { throw ReplicaError.invalidUpdate }
          try db.execute(
            sql: """
              UPDATE sync_operations SET status = 'rejected', error = ?, error_code = ?, outcome_status = ?
              WHERE sequence = ?
              """,
            arguments: [receipt.error, receipt.code, receipt.status?.rawValue, operation.sequence])
        } else {
          try db.execute(
            sql: "DELETE FROM sync_operations WHERE sequence = ?", arguments: [operation.sequence])
        }
        changed = true
      }
      if changed { try rebase(db) }
      try db.execute(
        sql: """
          UPDATE sync_state SET cookie = ?, cookie_order = ?, last_confirmed_sequence = ?
          """, arguments: [updates.cookie, updates.order, updates.lastSequence])
      try additionalChanges(db)
      return true
    }
  }

  public func rejections() throws -> [RejectedOperation<Command>] {
    try database.read { try Self.rejections(in: $0) }
  }

  public static func rejections(in db: Database) throws -> [RejectedOperation<Command>] {
    try Row.fetchAll(
      db, sql: "SELECT * FROM sync_operations WHERE status = 'rejected' ORDER BY sequence"
    )
    .map { row in
      let failure = Self.failure(row)
      let name: String = row["name"]
      let arguments = Data((row["args"] as String).utf8)
      return RejectedOperation(failure: failure) {
        try Self.operation(
          id: failure.id, sequence: failure.sequence, name: name, arguments: arguments)
      }
    }
  }

  public func resolveRejections(
    _ prepare: (Database, OperationContext, [RejectedOperation<Command>]) throws ->
      RejectionResolution<Command>
  ) throws {
    try database.write { db in
      let failures = try Self.rejections(in: db)
      let context = try nextOperationContext(in: db)
      let resolution = try prepare(db, context, failures)
      switch resolution {
      case .dismiss(let ids):
        try Self.dismiss(ids, from: failures.map(\.failure), in: db)
      case .retry(let command, let ids):
        try Self.dismiss(ids, from: failures.map(\.failure), in: db)
        _ = try append(command, context: context, to: db)
      }
    }
  }

  public func dismissRejections(ids: [String]) throws {
    try database.write { db in
      try Self.dismiss(ids, from: Self.metadata(in: db).failures, in: db)
    }
  }

  private func nextOperationContext(in db: Database) throws -> OperationContext {
    let state = try Self.requireState(db)
    return OperationContext(
      accountID: accountID, clientID: state["client_id"],
      id: UUID().uuidString.lowercased(), sequence: state["next_sequence"])
  }

  private func append(_ command: Command, context: OperationContext, to db: Database) throws
    -> Isocache.Operation<Command>
  {
    let operation = Isocache.Operation(id: context.id, sequence: context.sequence, command: command)
    try Self.insert(operation, into: db)
    try db.execute(sql: "UPDATE sync_state SET next_sequence = next_sequence + 1")
    try adapter.apply(db, context, command)
    return operation
  }

  private static func dismiss(
    _ ids: [String], from failures: [OperationFailure], in db: Database
  ) throws {
    let available = Set(failures.map(\.id))
    guard !ids.isEmpty, Set(ids).count == ids.count, Set(ids).isSubset(of: available)
    else { throw ReplicaError.invalidResolution }
    for id in ids {
      try db.execute(
        sql: "DELETE FROM sync_operations WHERE id = ? AND status = 'rejected'", arguments: [id])
    }
  }

  public func metadata() throws -> ReplicaMetadata { try database.read(Self.metadata) }

  public static func metadata(in db: Database) throws -> ReplicaMetadata {
    let rows = try Row.fetchAll(
      db,
      sql:
        "SELECT id, sequence, error, error_code, outcome_status FROM sync_operations WHERE status = 'rejected' ORDER BY sequence"
    )
    return ReplicaMetadata(status: try Self.status(db), failures: rows.map(Self.failure))
  }

  private static func failure(_ row: Row) -> OperationFailure {
    let status: String? = row["outcome_status"]
    return OperationFailure(
      id: row["id"], sequence: row["sequence"], message: row["error"],
      code: row["error_code"], status: status.map(ReceiptStatus.init(rawValue:)))
  }

  public func status() throws -> SyncStatus { try database.read(Self.status) }

  public func observe<Value: Equatable & Sendable>(
    _ fetch: @escaping @Sendable (Database) throws -> Value
  ) -> AsyncThrowingStream<Value, Error> {
    let observation = ValueObservation.tracking(fetch).removeDuplicates()
    let database = database
    return AsyncThrowingStream { continuation in
      let task = Task {
        do {
          for try await value in observation.values(in: database) { continuation.yield(value) }
          continuation.finish()
        } catch { continuation.finish(throwing: error) }
      }
      continuation.onTermination = { _ in task.cancel() }
    }
  }

  public func observeStatus() -> AsyncThrowingStream<SyncStatus, Error> { observe(Self.status) }

  public func deleteAccountData(additionalChanges: (Database) throws -> Void = { _ in }) throws {
    try database.write { db in
      try db.execute(sql: "PRAGMA secure_delete = ON")
      try adapter.clearRemote(db)
      try db.execute(sql: "DELETE FROM sync_operations")
      try adapter.resetLocal(db)
      try db.execute(
        sql: """
          UPDATE sync_state SET client_id = ?, next_sequence = 1, last_confirmed_sequence = 0,
          cookie = NULL, cookie_order = 0
          """, arguments: [UUID().uuidString.lowercased()])
      try additionalChanges(db)
    }
    try database.writeWithoutTransaction { db in try db.execute(sql: "VACUUM") }
  }

  private func rebase(_ db: Database) throws {
    try Self.rebase(db, adapter: adapter)
  }

  private static func rebase(_ db: Database, adapter: ReplicaAdapter<Command, Entity>) throws {
    try adapter.resetLocal(db)
    let operations = try Self.operations(db, statuses: ["pending", "sent"])
    guard !operations.isEmpty else { return }
    let state = try requireState(db)
    let accountID: String = state["account"]
    let clientID: String = state["client_id"]
    for operation in operations {
      try adapter.apply(
        db,
        OperationContext(
          accountID: accountID, clientID: clientID,
          id: operation.id, sequence: operation.sequence), operation.command)
    }
  }

  private static func insert(
    _ operation: Isocache.Operation<Command>, queuedAt: Date = Date(), into db: Database
  ) throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let arguments = try encoder.encode(CommandArguments(command: operation.command))
    try db.execute(
      sql: """
        INSERT INTO sync_operations(id, sequence, name, args, status, queued_at)
        VALUES (?, ?, ?, ?, 'pending', ?)
        """,
      arguments: [
        operation.id, operation.sequence, operation.command.name,
        String(decoding: arguments, as: UTF8.self), queuedAt,
      ])
  }

  private static func operations(_ db: Database, statuses: [String]) throws -> [Isocache.Operation<
    Command
  >] {
    try Row.fetchAll(
      db,
      sql: """
        SELECT id, sequence, name, args FROM sync_operations
        WHERE status IN (\(statuses.map { _ in "?" }.joined(separator: ", "))) ORDER BY sequence
        """, arguments: StatementArguments(statuses)
    ).map(operation)
  }

  private static func operation(_ row: Row) throws -> Isocache.Operation<Command> {
    try operation(
      id: row["id"], sequence: row["sequence"], name: row["name"],
      arguments: Data((row["args"] as String).utf8))
  }

  private static func operation(
    id: String, sequence: Int, name: String, arguments: Data
  ) throws -> Isocache.Operation<Command> {
    let decoder = JSONDecoder()
    decoder.userInfo[commandNameKey] = name
    let command = try decoder.decode(CommandArguments<Command>.self, from: arguments).command
    return Isocache.Operation(id: id, sequence: sequence, command: command)
  }

  private static func requireState(_ db: Database) throws -> Row {
    guard let row = try Row.fetchOne(db, sql: "SELECT * FROM sync_state WHERE id = 1")
    else { throw ReplicaError.missingState }
    return row
  }

  private static func status(_ db: Database) throws -> SyncStatus {
    let state = try requireState(db)
    return SyncStatus(
      pendingCount: try Int.fetchOne(
        db, sql: "SELECT COUNT(*) FROM sync_operations WHERE status IN ('pending', 'sent')")!,
      rejectedCount: try Int.fetchOne(
        db, sql: "SELECT COUNT(*) FROM sync_operations WHERE status = 'rejected'")!,
      lastConfirmedSequence: state["last_confirmed_sequence"],
      hasSynced: (state["cookie"] as String?) != nil)
  }
}

private let commandNameKey = CodingUserInfoKey(rawValue: "isocache.commandName")!

private struct CommandArguments<Command: SyncCommand>: Codable {
  let command: Command
  init(command: Command) { self.command = command }
  init(from decoder: Decoder) throws {
    guard let name = decoder.userInfo[commandNameKey] as? String else {
      throw DecodingError.dataCorrupted(
        .init(codingPath: decoder.codingPath, debugDescription: "Missing command name"))
    }
    command = try Command(name: name, from: decoder)
  }
  func encode(to encoder: Encoder) throws { try command.encodeArguments(to: encoder) }
}
