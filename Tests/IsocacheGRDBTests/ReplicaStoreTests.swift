import Foundation
import GRDB
import Isocache
import IsocacheGRDB
import Synchronization
import Testing

private struct Edit: SyncCommand {
  let key: String
  let value: Int
  var name: String { "setValue" }
  init(key: String, value: Int) {
    self.key = key
    self.value = value
  }
  init(name: String, from decoder: Decoder) throws {
    guard name == "setValue" else { throw ReplicaError.invalidUpdate }
    self = try Self(from: decoder)
  }
  func encodeArguments(to encoder: Encoder) throws { try encode(to: encoder) }
}

private struct Record: Codable, Sendable {
  let key: String
  let value: Int
  let editID: String
}

private typealias Store = ReplicaStore<Edit, Record>
private enum Failure: Error { case interrupted }

private func open(_ path: String = ":memory:", account: String = "account") throws -> (
  store: Store, database: DatabaseQueue
) {
  let database = try DatabaseQueue(path: path)
  var migrator = DatabaseMigrator()
  migrator.registerMigration("records") { db in
    for name in ["remote", "local"] {
      try db.execute(
        sql:
          "CREATE TABLE \(name) (key TEXT PRIMARY KEY, value INTEGER NOT NULL, edit_id TEXT NOT NULL)"
      )
    }
  }
  Store.registerMigrations(in: &migrator)
  try migrator.migrate(database)
  let store = try Store(
    database: database, accountID: account,
    adapter: ReplicaAdapter(
      clearRemote: { try $0.execute(sql: "DELETE FROM remote") },
      remove: { try $0.execute(sql: "DELETE FROM remote WHERE key = ?", arguments: [$1]) },
      upsert: { db, put in
        guard put.key == put.value.key else { throw ReplicaError.invalidUpdate }
        try db.execute(
          sql: "INSERT OR REPLACE INTO remote VALUES (?, ?, ?)",
          arguments: [put.key, put.value.value, put.value.editID])
      },
      resetLocal: {
        try $0.execute(sql: "DELETE FROM local; INSERT INTO local SELECT * FROM remote")
      },
      apply: { db, context, command in
        try db.execute(
          sql: "INSERT OR REPLACE INTO local VALUES (?, ?, ?)",
          arguments: [command.key, command.value, context.id])
        if command.key == "fail" { throw Failure.interrupted }
      }
    ))
  return (store, database)
}

private func values(_ database: DatabaseQueue) throws -> [String: Int] {
  try database.read { db in
    Dictionary(
      uniqueKeysWithValues: try Row.fetchAll(db, sql: "SELECT * FROM local").map {
        ($0["key"], $0["value"])
      })
  }
}

@Test func enqueueRollsBackProjectionAndSequenceAndCapturesIdentityInTheTransaction() throws {
  let (store, database) = try open()
  #expect(throws: Failure.self) { try store.enqueue(Edit(key: "fail", value: 1)) }
  #expect(try values(database).isEmpty)
  #expect(try store.status().pendingCount == 0)
  let first = try store.enqueue(Edit(key: "a", value: 2))
  let second = try store.enqueue { db, context in
    #expect(context.sequence == 2)
    let predecessor = try String.fetchOne(db, sql: "SELECT edit_id FROM local WHERE key = 'a'")
    #expect(predecessor == first.id)
    return Edit(key: "a", value: 3)
  }
  #expect(first.sequence == 1)
  #expect(
    try database.read {
      try String.fetchOne($0, sql: "SELECT edit_id FROM local WHERE key = 'a'")
    } == second.id)
}

@Test func relaunchResetAndRejectionPreserveDurableIntent() throws {
  let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
  try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
  defer { try? FileManager.default.removeItem(at: directory) }
  let path = directory.appendingPathComponent("replica.sqlite").path
  let sent: Isocache.Operation<Edit>
  let pending: Isocache.Operation<Edit>
  let client: String
  do {
    let (store, _) = try open(path)
    try store.reconcile(Updates(cookie: "high", order: 9, lastSequence: 0))
    sent = try store.enqueue(Edit(key: "a", value: 1))
    pending = try store.enqueue(Edit(key: "b", value: 2))
    client = try store.requests().0.clientID
    try store.markSent(through: sent.sequence)
  }
  let (reopened, database) = try open(path)
  #expect(throws: ReplicaError.accountMismatch) { try open(path, account: "other") }
  #expect(try reopened.requests().0.clientID == client)
  #expect(try reopened.requests().0.operations == [pending])
  try reopened.reconcile(Updates(cookie: "reset", order: 1, clear: true, lastSequence: 0))
  #expect(try values(database) == ["a": 1, "b": 2])
  try reopened.reconcile(
    Updates(
      cookie: "confirmed", order: 2, lastSequence: 1,
      rejections: [Receipt(sequence: 1, error: "Changed elsewhere", id: sent.id, code: "conflict")])
  )
  #expect(try values(database) == ["b": 2])
  let rejection = try #require(reopened.rejections().first)
  #expect(try rejection.decodeOperation() == sent && rejection.code == "conflict")
  #expect(try reopened.status().pendingCount == 1)
  #expect(try !reopened.reconcile(Updates(cookie: "stale", order: 1, lastSequence: 1)))
  #expect(try reopened.requests().1.cookie == "confirmed")
  try reopened.deleteAccountData()
  #expect(try values(database).isEmpty)
  #expect(try reopened.requests().0.clientID != client)
  #expect(try reopened.rejections().isEmpty)
}

@Test func failedReconciliationRollsBackReplicaCookieAndAcknowledgement() throws {
  let (store, database) = try open()
  let operation = try store.enqueue(Edit(key: "a", value: 1))
  #expect(throws: Failure.self) {
    try store.reconcile(
      Updates(cookie: "next", order: 1, clear: true, lastSequence: operation.sequence)
    ) { _ in
      throw Failure.interrupted
    }
  }
  #expect(throws: ReplicaError.unsupportedOutcome("future")) {
    try store.reconcile(
      Updates(
        cookie: "future", order: 1, lastSequence: operation.sequence,
        rejections: [
          Receipt(
            sequence: operation.sequence, error: nil,
            status: .unknown("future"))
        ]))
  }
  #expect(try store.requests().0.operations == [operation])
  #expect(try store.requests().1.cookie == nil)
  #expect(try values(database) == ["a": 1])
}

@Test func successfulPushAndFailedPullKeepOptimisticWorkUntilConfirmation() async throws {
  let (store, database) = try open()
  let operation = try store.enqueue(Edit(key: "a", value: 8))
  let pulls = Mutex(0)
  let coordinator = SyncCoordinator(
    storage: store.syncStorage,
    transport: SyncTransport(
      push: { _ in },
      pull: { _ in
        let attempt = pulls.withLock { count in
          count += 1
          return count
        }
        if attempt == 1 { throw Failure.interrupted }
        return Updates(
          cookie: "confirmed", order: 1,
          put: [
            Put(key: "a", version: "1", value: Record(key: "a", value: 8, editID: operation.id))
          ],
          lastSequence: operation.sequence)
      }
    ))
  await #expect(throws: Failure.self) { try await coordinator.sync() }
  #expect(try values(database) == ["a": 8])
  #expect(try store.status().pendingCount == 1)
  #expect(try store.requests().0.operations.isEmpty)
  #expect(try await coordinator.sync())
  #expect(try store.status().pendingCount == 0)
  #expect(try values(database) == ["a": 8])
}

@Test func conflictResolutionRollsBackDismissalWhenTheReplacementCannotBeApplied() throws {
  let (store, database) = try open()
  let original = try store.enqueue(Edit(key: "a", value: 1))
  try store.reconcile(
    Updates(
      cookie: "rejected", order: 1, lastSequence: original.sequence,
      rejections: [
        Receipt(
          sequence: original.sequence, error: "Conflict", id: original.id,
          code: "conflict", status: .rejected)
      ]))
  #expect(throws: Failure.self) {
    try store.resolveRejections { _, _, failures in
      .retry(Edit(key: "fail", value: 2), dismissing: failures.map { $0.failure.id })
    }
  }
  #expect(try store.rejections().map { try $0.decodeOperation() } == [original])
  #expect(try store.status().pendingCount == 0)
  #expect(try values(database).isEmpty)
  try store.resolveRejections { _, _, failures in
    .retry(Edit(key: "a", value: 3), dismissing: failures.map { $0.failure.id })
  }
  let replacement = try #require(store.requests().0.operations.first)
  #expect(replacement.sequence == original.sequence + 1)
  #expect(replacement.id != original.id)
  #expect(try store.rejections().isEmpty)
  #expect(try values(database) == ["a": 3])
}

@Test func historicalFailureCanBeDismissedWithoutBlockingKnownConflictRecovery() throws {
  let (store, database) = try open()
  let historical = try store.enqueue(Edit(key: "old", value: 1))
  let conflict = try store.enqueue(Edit(key: "current", value: 2))
  try store.reconcile(
    Updates(
      cookie: "failed", order: 1, lastSequence: conflict.sequence,
      rejections: [
        Receipt(sequence: historical.sequence, error: "Historical failure", id: historical.id),
        Receipt(sequence: conflict.sequence, error: "Conflict", id: conflict.id, code: "conflict"),
      ]))
  try database.write { db in
    try db.execute(
      sql: "UPDATE sync_operations SET name = 'unknown', args = '{}' WHERE id = ?",
      arguments: [historical.id])
  }
  let metadata = try store.metadata()
  #expect(metadata.failures.map(\.id) == [historical.id, conflict.id])
  try store.resolveRejections { _, _, failures in
    let known = try #require(failures.first { $0.code == "conflict" })
    let command = try known.decodeOperation().command
    return .retry(command, dismissing: [known.failure.id])
  }
  #expect(try values(database) == ["current": 2])
  #expect(try store.metadata().failures.map(\.id) == [historical.id])
  try store.dismissRejections(ids: [historical.id])
  #expect(try store.metadata().failures.isEmpty)
  #expect(try store.requests().0.operations.first?.sequence == conflict.sequence + 1)
}
