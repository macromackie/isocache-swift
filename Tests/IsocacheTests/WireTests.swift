import Foundation
import Isocache
import Testing

private struct ValueCommand: SyncCommand {
  let key: String
  let value: Int
  var name: String { "setValue" }
  init(name: String, from decoder: Decoder) throws {
    guard name == "setValue" else { throw UnknownEntity() }
    self = try Self(from: decoder)
  }
  func encodeArguments(to encoder: Encoder) throws { try encode(to: encoder) }
}

private struct ValueEntity: Codable, Sendable {
  let type: String
  let key: String
  let value: Int

  enum CodingKeys: String, CodingKey { case type, key, value }
  init(from decoder: Decoder) throws {
    let c = try decoder.container(keyedBy: CodingKeys.self)
    type = try c.decode(String.self, forKey: .type)
    guard type == "value" else { throw UnknownEntity() }
    key = try c.decode(String.self, forKey: .key)
    value = try c.decode(Int.self, forKey: .value)
  }
}

private struct WireFixture: Codable {
  let operations: OperationsRequest<ValueCommand>
  let request: UpdatesRequest
  let updates: Updates<ValueEntity>
}

@Test func versionTwoMatchesTheSharedServerFixture() throws {
  let url = try #require(
    Bundle.module.url(forResource: "v2", withExtension: "json", subdirectory: "Fixtures"))
  let data = try Data(contentsOf: url)
  let fixture = try JSONDecoder().decode(WireFixture.self, from: data)
  let encoded = try JSONEncoder().encode(fixture)
  #expect(
    try JSONSerialization.jsonObject(with: encoded) as? NSDictionary
      == JSONSerialization.jsonObject(with: data) as? NSDictionary)
}

@Test func updatesSkipUnknownEntitiesButRejectCorruptKnownEntities() throws {
  let url = try #require(
    Bundle.module.url(forResource: "v2", withExtension: "json", subdirectory: "Fixtures"))
  let fixture = try String(contentsOf: url, encoding: .utf8)
  let unknown = fixture.replacingOccurrences(
    of: "\"type\": \"value\"", with: "\"type\": \"future\"")
  #expect(try JSONDecoder().decode(WireFixture.self, from: Data(unknown.utf8)).updates.put.isEmpty)
  let corrupt = fixture.replacingOccurrences(
    of: "\"type\": \"value\", \"key\": \"counter\", \"value\": 4",
    with: "\"type\": \"value\", \"key\": \"counter\", \"value\": \"invalid\"")
  #expect(throws: DecodingError.self) {
    try JSONDecoder().decode(WireFixture.self, from: Data(corrupt.utf8))
  }
}
