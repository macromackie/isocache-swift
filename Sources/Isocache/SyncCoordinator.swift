import Foundation
import Synchronization

public struct SyncStorage<Command: SyncCommand, Response: Sendable>: Sendable {
  public var requests: @Sendable () throws -> (OperationsRequest<Command>, UpdatesRequest)
  public var markSent: @Sendable (Int) throws -> Void
  public var reconcile: @Sendable (Response) throws -> Void

  public init(
    requests: @escaping @Sendable () throws -> (OperationsRequest<Command>, UpdatesRequest),
    markSent: @escaping @Sendable (Int) throws -> Void,
    reconcile: @escaping @Sendable (Response) throws -> Void
  ) {
    self.requests = requests
    self.markSent = markSent
    self.reconcile = reconcile
  }
}

public struct SyncTransport<Command: SyncCommand, Response: Sendable>: Sendable {
  public var push: @Sendable (OperationsRequest<Command>) async throws -> Void
  public var pull: @Sendable (UpdatesRequest) async throws -> Response
  public var open: (@Sendable () async throws -> LiveConnection.Socket)?

  public init(
    push: @escaping @Sendable (OperationsRequest<Command>) async throws -> Void,
    pull: @escaping @Sendable (UpdatesRequest) async throws -> Response,
    open: (@Sendable () async throws -> LiveConnection.Socket)? = nil
  ) {
    self.push = push
    self.pull = pull
    self.open = open
  }
}

public struct SyncFailure: Sendable {
  public enum Phase: Equatable, Sendable {
    case connecting
    case refreshing
    case receiving
    case retrying
  }

  public let phase: Phase
  public let error: any Error

  public init(phase: Phase, error: any Error) {
    self.phase = phase
    self.error = error
  }
}

/// A coordinator is bound to one store and one authenticated session for its entire lifetime.
public actor SyncCoordinator<Command: SyncCommand, Response: Sendable> {
  private let storage: SyncStorage<Command, Response>
  private let transport: SyncTransport<Command, Response>
  private let didSync: @Sendable () async -> Void
  private let onFailure: @Sendable (SyncFailure) async -> Void
  private let sleep: @Sendable (Duration) async throws -> Void
  private let now: @Sendable () -> ContinuousClock.Instant
  private nonisolated let scope = SessionScope()
  private var task: Task<Bool, Error>?
  private var listenerID: UUID?

  public init(
    storage: SyncStorage<Command, Response>, transport: SyncTransport<Command, Response>,
    didSync: @escaping @Sendable () async -> Void = {},
    onFailure: @escaping @Sendable (SyncFailure) async -> Void = { _ in },
    sleep: @escaping @Sendable (Duration) async throws -> Void = { try await Task.sleep(for: $0) },
    now: @escaping @Sendable () -> ContinuousClock.Instant = { .now }
  ) {
    self.storage = storage
    self.transport = transport
    self.didSync = didSync
    self.onFailure = onFailure
    self.sleep = sleep
    self.now = now
  }

  /// Invalidates delayed responses synchronously, including transports that ignore cancellation.
  public nonisolated func invalidate() { scope.invalidate() }

  public func sync() async throws -> Bool {
    guard scope.isActive else { return false }
    if let task { return try await task.value }
    let storage = storage
    let transport = transport
    let scope = scope
    let didSync = didSync
    let running = Task {
      let requests = try scope.withActive { try storage.requests() }
      guard let (pending, pull) = requests else { return false }
      for offset in stride(from: 0, to: pending.operations.count, by: 100) {
        try Task.checkCancellation()
        guard scope.isActive else { return false }
        let batch = Array(pending.operations[offset..<min(offset + 100, pending.operations.count)])
        try await transport.push(OperationsRequest(clientID: pending.clientID, operations: batch))
        guard try scope.withActive({ try storage.markSent(batch[batch.count - 1].sequence) }) != nil
        else { return false }
      }
      try Task.checkCancellation()
      guard scope.isActive else { return false }
      let updates = try await transport.pull(pull)
      try Task.checkCancellation()
      let applied = try scope.withActive { try storage.reconcile(updates) } != nil
      if applied { await didSync() }
      return applied
    }
    task = running
    defer { if task == running { task = nil } }
    return try await running.value
  }

  public func refresh() async throws -> Bool {
    if let running = task {
      // A refresh makes a new attempt even when the preceding sync failed.
      _ = await running.result
      if task == running { task = nil }
    }
    try Task.checkCancellation()
    return try await sync()
  }

  /// The foreground caller owns cancellation. Each connection pulls before consuming hints.
  public func listen() async {
    guard let open = transport.open, scope.isActive, !Task.isCancelled else { return }
    let id = UUID()
    listenerID = id
    scope.closeSocket()
    defer {
      if listenerID == id {
        scope.closeSocket()
        listenerID = nil
      }
    }
    var failures = 0
    while scope.isActive, listenerID == id, !Task.isCancelled {
      var connectedAt: ContinuousClock.Instant?
      var phase = SyncFailure.Phase.connecting
      do {
        let socket = try await open()
        defer { socket.close() }
        guard listenerID == id, !Task.isCancelled, scope.install(socket) else { continue }
        connectedAt = now()
        phase = .receiving
        try await withTaskCancellationHandler {
          await refreshForListener(id)
          guard scope.isActive, listenerID == id, !Task.isCancelled else { return }
          let sleep = sleep
          try await withThrowingTaskGroup(of: Void.self) { group in
            defer {
              group.cancelAll()
              socket.close()
            }
            group.addTask {
              while true {
                try await sleep(.seconds(30))
                try await socket.send("ping")
              }
            }
            group.addTask {
              for try await message in socket.messages {
                try Task.checkCancellation()
                if message == "poke" { await self.refreshForListener(id) }
              }
            }
            try await group.next()
          }
        } onCancel: {
          socket.close()
        }
      } catch {
        await report(error, phase: phase, listener: id)
      }
      guard scope.isActive, listenerID == id, !Task.isCancelled else { continue }
      scope.closeSocket()
      if let connectedAt, connectedAt.duration(to: now()) >= .seconds(30) { failures = 0 }
      failures += 1
      do {
        try await sleep(.seconds(min(60, 1 << min(failures - 1, 6))))
      } catch {
        await report(error, phase: .retrying, listener: id)
        return
      }
    }
  }

  private func refreshForListener(_ id: UUID) async {
    guard scope.isActive, listenerID == id, !Task.isCancelled else { return }
    do {
      _ = try await refresh()
    } catch {
      await report(error, phase: .refreshing, listener: id)
    }
  }

  private func report(_ error: any Error, phase: SyncFailure.Phase, listener id: UUID) async {
    guard scope.isActive, listenerID == id, !Task.isCancelled else { return }
    await onFailure(SyncFailure(phase: phase, error: error))
  }
}

private final class SessionScope: Sendable {
  private struct State {
    var active = true
    var socket: LiveConnection.Socket?
  }
  private let state = Mutex(State())

  var isActive: Bool { state.withLock { $0.active } }

  func withActive<Value>(_ body: () throws -> Value) rethrows -> Value? {
    try state.withLock { state in
      guard state.active else { return nil }
      return try body()
    }
  }

  func install(_ socket: LiveConnection.Socket) -> Bool {
    state.withLock { state in
      guard state.active else { return false }
      state.socket = socket
      return true
    }
  }

  func closeSocket() {
    let socket = state.withLock { state in
      let socket = state.socket
      state.socket = nil
      return socket
    }
    socket?.close()
  }

  func invalidate() {
    let socket = state.withLock { state in
      state.active = false
      let socket = state.socket
      state.socket = nil
      return socket
    }
    socket?.close()
  }
}
