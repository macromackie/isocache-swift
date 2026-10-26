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

private func open(_ path: String = ":memory:", account: String = "account") throws -> Store {
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
  return try Store(
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
}

private func values(_ store: Store) throws -> [String: Int] {
  try store.database.read { db in
    Dictionary(
      uniqueKeysWithValues: try Row.fetchAll(db, sql: "SELECT * FROM local").map {
        ($0["key"], $0["value"])
      })
  }
}

@Test func enqueueRollsBackProjectionAndSequenceAndCapturesIdentityInTheTransaction() throws {
  let store = try open()
  #expect(throws: Failure.self) { try store.enqueue(Edit(key: "fail", value: 1)) }
  #expect(try values(store).isEmpty)
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
    try store.database.read {
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
    let store = try open(path)
    try store.reconcile(Updates(cookie: "high", order: 9, lastSequence: 0))
    sent = try store.enqueue(Edit(key: "a", value: 1))
    pending = try store.enqueue(Edit(key: "b", value: 2))
    client = try store.requests().0.clientID
    try store.markSent(through: sent.sequence)
  }
  let reopened = try open(path)
  #expect(throws: ReplicaError.accountMismatch) { try open(path, account: "other") }
  #expect(try reopened.requests().0.clientID == client)
  #expect(try reopened.requests().0.operations == [pending])
  try reopened.reconcile(Updates(cookie: "reset", order: 1, clear: true, lastSequence: 0))
  #expect(try values(reopened) == ["a": 1, "b": 2])
  try reopened.reconcile(
    Updates(
      cookie: "confirmed", order: 2, lastSequence: 1,
      rejections: [Receipt(sequence: 1, error: "Changed elsewhere", id: sent.id, code: "conflict")])
  )
  #expect(try values(reopened) == ["b": 2])
  let rejection = try #require(reopened.rejections().first)
  #expect(rejection.operation == sent && rejection.code == "conflict")
  #expect(try reopened.status().pendingCount == 1)
  #expect(try !reopened.reconcile(Updates(cookie: "stale", order: 1, lastSequence: 1)))
  #expect(try reopened.requests().1.cookie == "confirmed")
  try reopened.deleteAccountData()
  #expect(try values(reopened).isEmpty)
  #expect(try reopened.requests().0.clientID != client)
  #expect(try reopened.rejections().isEmpty)
}

@Test func failedReconciliationRollsBackReplicaCookieAndAcknowledgement() throws {
  let store = try open()
  let operation = try store.enqueue(Edit(key: "a", value: 1))
  #expect(throws: Failure.self) {
    try store.reconcile(
      Updates(cookie: "next", order: 1, clear: true, lastSequence: operation.sequence)
    ) { _ in
      throw Failure.interrupted
    }
  }
  #expect(try store.requests().0.operations == [operation])
  #expect(try store.requests().1.cookie == nil)
  #expect(try values(store) == ["a": 1])
}

@Test func successfulPushAndFailedPullKeepOptimisticWorkUntilConfirmation() async throws {
  let store = try open()
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
  #expect(try values(store) == ["a": 8])
  #expect(try store.status().pendingCount == 1)
  #expect(try store.requests().0.operations.isEmpty)
  #expect(try await coordinator.sync())
  #expect(try store.status().pendingCount == 0)
  #expect(try values(store) == ["a": 8])
}
