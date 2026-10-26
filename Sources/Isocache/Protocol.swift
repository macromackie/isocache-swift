import Foundation

public protocol SyncCommand: Codable, Equatable, Sendable {
  var name: String { get }
  init(name: String, from decoder: Decoder) throws
  func encodeArguments(to encoder: Encoder) throws
}

public struct OperationContext: Equatable, Sendable {
  public let accountID: String
  public let clientID: String
  public let id: String
  public let sequence: Int

  public init(accountID: String, clientID: String, id: String, sequence: Int) {
    self.accountID = accountID
    self.clientID = clientID
    self.id = id
    self.sequence = sequence
  }
}

public struct Operation<Command: SyncCommand>: Codable, Equatable, Sendable {
  public let id: String
  public let sequence: Int
  public let command: Command

  public init(id: String, sequence: Int, command: Command) {
    self.id = id
    self.sequence = sequence
    self.command = command
  }

  enum CodingKeys: String, CodingKey { case id, sequence, name, args }

  public init(from decoder: Decoder) throws {
    let c = try decoder.container(keyedBy: CodingKeys.self)
    id = try c.decode(String.self, forKey: .id)
    sequence = try c.decode(Int.self, forKey: .sequence)
    command = try Command(
      name: c.decode(String.self, forKey: .name), from: c.superDecoder(forKey: .args))
  }

  public func encode(to encoder: Encoder) throws {
    var c = encoder.container(keyedBy: CodingKeys.self)
    try c.encode(id, forKey: .id)
    try c.encode(sequence, forKey: .sequence)
    try c.encode(command.name, forKey: .name)
    try command.encodeArguments(to: c.superEncoder(forKey: .args))
  }
}

public struct OperationsRequest<Command: SyncCommand>: Codable, Equatable, Sendable {
  public var version = 2
  public var clientID: String
  public var operations: [Operation<Command>]

  public init(clientID: String, operations: [Operation<Command>]) {
    self.clientID = clientID
    self.operations = operations
  }
}

public struct UpdatesRequest: Codable, Equatable, Sendable {
  public var version = 2
  public var clientID: String
  public var cookie: String?
  public var schema: Int

  public init(clientID: String, cookie: String?, schema: Int = 0) {
    self.clientID = clientID
    self.cookie = cookie
    self.schema = schema
  }

  public func encode(to encoder: Encoder) throws {
    var c = encoder.container(keyedBy: CodingKeys.self)
    try c.encode(version, forKey: .version)
    try c.encode(clientID, forKey: .clientID)
    try c.encode(cookie, forKey: .cookie)
    try c.encode(schema, forKey: .schema)
  }
}

public struct UnknownEntity: Error, Sendable {
  public init() {}
}

public struct Put<Entity: Codable & Sendable>: Codable, Sendable {
  public var key: String
  public var version: String
  public var value: Entity

  public init(key: String, version: String, value: Entity) {
    self.key = key
    self.version = version
    self.value = value
  }
}

public enum ReceiptStatus: Equatable, Sendable, Codable {
  case rejected
  case appliedWithNotice
  case unknown(String)

  public init(rawValue: String) {
    switch rawValue {
    case "rejected": self = .rejected
    case "appliedWithNotice": self = .appliedWithNotice
    default: self = .unknown(rawValue)
    }
  }

  public var rawValue: String {
    switch self {
    case .rejected: "rejected"
    case .appliedWithNotice: "appliedWithNotice"
    case .unknown(let value): value
    }
  }

  public init(from decoder: Decoder) throws {
    self.init(rawValue: try decoder.singleValueContainer().decode(String.self))
  }

  public func encode(to encoder: Encoder) throws {
    var container = encoder.singleValueContainer()
    try container.encode(rawValue)
  }
}

public struct Receipt: Codable, Equatable, Sendable {
  public var sequence: Int
  public var error: String?
  public var id: String?
  public var code: String?
  public var status: ReceiptStatus?

  public init(
    sequence: Int, error: String?, id: String? = nil, code: String? = nil,
    status: ReceiptStatus? = nil
  ) {
    self.sequence = sequence
    self.error = error
    self.id = id
    self.code = code
    self.status = status
  }
}

public struct Updates<Entity: Codable & Sendable>: Codable, Sendable {
  public var version: Int
  public var cookie: String
  public var order: Int
  public var clear: Bool
  public var put: [Put<Entity>]
  public var del: [String]
  public var lastSequence: Int
  public var rejections: [Receipt]

  public init(
    version: Int = 2, cookie: String, order: Int, clear: Bool = false, put: [Put<Entity>] = [],
    del: [String] = [], lastSequence: Int, rejections: [Receipt] = []
  ) {
    self.version = version
    self.cookie = cookie
    self.order = order
    self.clear = clear
    self.put = put
    self.del = del
    self.lastSequence = lastSequence
    self.rejections = rejections
  }

  public init(from decoder: Decoder) throws {
    let c = try decoder.container(keyedBy: CodingKeys.self)
    version = try c.decode(Int.self, forKey: .version)
    cookie = try c.decode(String.self, forKey: .cookie)
    order = try c.decode(Int.self, forKey: .order)
    clear = try c.decode(Bool.self, forKey: .clear)
    put = try c.decode([KnownPut<Entity>].self, forKey: .put).compactMap(\.value)
    del = try c.decode([String].self, forKey: .del)
    lastSequence = try c.decode(Int.self, forKey: .lastSequence)
    rejections = try c.decode([Receipt].self, forKey: .rejections)
  }
}

private struct KnownPut<Entity: Codable & Sendable>: Decodable {
  let value: Put<Entity>?
  init(from decoder: Decoder) throws {
    do { value = try Put<Entity>(from: decoder) } catch is UnknownEntity { value = nil }
  }
}
