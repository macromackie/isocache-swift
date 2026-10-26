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
