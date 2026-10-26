import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// Opens the socket the server pokes when this account's data changes. A poke only says "pull
/// now", so the pull stays the one way data reaches the phone.
public struct LiveConnection: Sendable {
  public struct Socket: Sendable {
    /// Text messages until the socket closes, which finishes the stream or throws.
    public var messages: AsyncThrowingStream<String, Error>
    public var send: @Sendable (String) async throws -> Void
    public var close: @Sendable () -> Void
    public init(
      messages: AsyncThrowingStream<String, Error>,
      send: @escaping @Sendable (String) async throws -> Void,
      close: @escaping @Sendable () -> Void
    ) {
      self.messages = messages
      self.send = send
      self.close = close
    }
  }

  /// Returns once the server accepted the socket.
  public var open: @Sendable (URLRequest) async throws -> Socket
  public init(open: @escaping @Sendable (URLRequest) async throws -> Socket) { self.open = open }

  public static let unavailable = Self { _ in throw URLError(.notConnectedToInternet) }

  /// The phone's socket. Linux Foundation has no WebSocket support, so the headless phones bring
  /// their own.
  public static let urlSession = Self { request in
    let task = liveSession.webSocketTask(with: request)
    try await withTaskCancellationHandler {
      task.resume()
      do {
        // The send waits for the upgrade, so a refused socket throws here.
        try await task.send(.string("ping"))
        try Task.checkCancellation()
      } catch {
        task.cancel(with: .goingAway, reason: nil)
        throw error
      }
    } onCancel: {
      task.cancel(with: .goingAway, reason: nil)
    }
    let messages = AsyncThrowingStream<String, Error> { continuation in
      let reader = Task {
        do {
          while true {
            switch try await task.receive() {
            case .string(let text): continuation.yield(text)
            case .data(let data): continuation.yield(String(decoding: data, as: UTF8.self))
            @unknown default: break
            }
          }
        } catch { continuation.finish(throwing: error) }
      }
      continuation.onTermination = { _ in
        reader.cancel()
        task.cancel(with: .goingAway, reason: nil)
      }
    }
    return Socket(
      messages: messages, send: { try await task.send(.string($0)) },
      close: { task.cancel(with: .normalClosure, reason: nil) })
  }

  private static let liveSession: URLSession = {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.httpShouldSetCookies = false
    return URLSession(configuration: configuration)
  }()
}
