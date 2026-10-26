import Isocache

public enum ReplicaError: Error, Equatable, Sendable {
  case accountMismatch
  case invalidUpdate
  case invalidResolution
  case missingState
  case unsupportedOutcome(String)
}

public struct OperationFailure: Equatable, Sendable {
  public let id: String
  public let sequence: Int
  public let message: String?
  public let code: String?
  public let status: ReceiptStatus?
}

public struct RejectedOperation<Command: SyncCommand>: Sendable {
  public let failure: OperationFailure
  private let decode: @Sendable () throws -> Isocache.Operation<Command>

  init(
    failure: OperationFailure, decode: @escaping @Sendable () throws -> Isocache.Operation<Command>
  ) {
    self.failure = failure
    self.decode = decode
  }

  public func decodeOperation() throws -> Isocache.Operation<Command> { try decode() }

  public var error: String? { failure.message }
  public var code: String? { failure.code }
  public var status: ReceiptStatus? { failure.status }
}

public struct SyncStatus: Equatable, Sendable {
  public let pendingCount: Int
  public let rejectedCount: Int
  public let lastConfirmedSequence: Int
  public let hasSynced: Bool
}

public struct ReplicaMetadata: Equatable, Sendable {
  public let status: SyncStatus
  public let failures: [OperationFailure]
}

public enum RejectionResolution<Command: SyncCommand>: Sendable {
  case dismiss(ids: [String])
  case retry(Command, dismissing: [String])
}
