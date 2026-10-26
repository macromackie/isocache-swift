import Foundation
import Isocache
import Synchronization
import Testing

private struct Edit: SyncCommand {
  let value: Int
  var name: String { "setValue" }
  init(value: Int) { self.value = value }
  init(name: String, from decoder: Decoder) throws { self = try Self(from: decoder) }
  func encodeArguments(to encoder: Encoder) throws { try encode(to: encoder) }
}

@Test(.timeLimit(.minutes(1)), arguments: [false, true])
func invalidationFencesDelayedPushAndPullResponses(duringPush: Bool) async throws {
  let arrived = AsyncStream<Void>.makeStream()
  let released = AsyncStream<Void>.makeStream()
  let writes = Mutex<[String]>([])
  let coordinator = SyncCoordinator(
    storage: SyncStorage<Edit, Int>(
      requests: {
        (
          OperationsRequest(
            clientID: "old-client",
            operations: [Isocache.Operation(id: "id", sequence: 1, command: Edit(value: 1))]),
          UpdatesRequest(clientID: "old-client", cookie: nil)
        )
      },
      markSent: { _ in writes.withLock { $0.append("sent") } },
      reconcile: { _ in writes.withLock { $0.append("reconciled") } }
    ),
    transport: SyncTransport(
      push: { _ in
        if duringPush {
          arrived.continuation.yield(())
          for await _ in released.stream { break }
        }
      },
      pull: { _ in
        arrived.continuation.yield(())
        for await _ in released.stream { break }
        return 1
      }
    )
  )
  let task = Task { try await coordinator.sync() }
  for await _ in arrived.stream { break }
  coordinator.invalidate()
  released.continuation.yield(())
  #expect(try await task.value == false)
  #expect(writes.withLock { $0 } == (duringPush ? [] : ["sent"]))
  #expect(try await coordinator.sync() == false)
}

private enum ListenerFailure: Error { case refresh, retry }
private enum ListenerEnd: CaseIterable, Equatable, Sendable {
  case cancellation, failedSleep, invalidation
}

@Test(.timeLimit(.minutes(1)), arguments: ListenerEnd.allCases)
private func listenerReportsFailuresAndStopsAfterCancellationOrFailedRetry(ending: ListenerEnd)
  async throws
{
  let requested = AsyncStream<Void>.makeStream()
  let response = AsyncStream<Void>.makeStream()
  let retrying = AsyncStream<Void>.makeStream()
  let retry = AsyncStream<Void>.makeStream()
  let heartbeat = AsyncStream<Void>.makeStream()
  let messages = AsyncThrowingStream<String, Error>.makeStream()
  messages.continuation.finish()
  let failures = Mutex<[SyncFailure.Phase]>([])
  let socketClosed = Mutex(false)
  let coordinator = SyncCoordinator(
    storage: SyncStorage<Edit, Int>(
      requests: {
        (
          OperationsRequest(clientID: "client", operations: []),
          UpdatesRequest(clientID: "client", cookie: nil)
        )
      },
      markSent: { _ in }, reconcile: { _ in }
    ),
    transport: SyncTransport(
      push: { _ in },
      pull: { _ in
        requested.continuation.yield(())
        for await _ in response.stream { break }
        throw ListenerFailure.refresh
      },
      open: {
        LiveConnection.Socket(
          messages: messages.stream, send: { _ in },
          close: { socketClosed.withLock { $0 = true } })
      }
    ),
    onFailure: { failure in failures.withLock { $0.append(failure.phase) } },
    sleep: { duration in
      if duration == .seconds(30) {
        for await _ in heartbeat.stream { break }
        try Task.checkCancellation()
        return
      }
      retrying.continuation.yield(())
      for await _ in retry.stream { break }
      try Task.checkCancellation()
      throw ListenerFailure.retry
    }
  )
  let listener = Task { await coordinator.listen() }
  for await _ in requested.stream { break }
  if ending == .invalidation { coordinator.invalidate() }
  response.continuation.yield(())
  if ending != .invalidation {
    for await _ in retrying.stream { break }
    if ending == .cancellation { listener.cancel() }
    retry.continuation.yield(())
  }
  await listener.value
  #expect(socketClosed.withLock { $0 })
  let expected: [SyncFailure.Phase]
  switch ending {
  case .cancellation: expected = [.refreshing]
  case .failedSleep: expected = [.refreshing, .retrying]
  case .invalidation: expected = []
  }
  #expect(failures.withLock { $0 } == expected)
}
