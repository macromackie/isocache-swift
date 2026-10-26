// swift-tools-version: 6.0
import PackageDescription

let package = Package(
  name: "Isocache",
  platforms: [.iOS(.v18), .macOS(.v15)],
  products: [
    .library(name: "Isocache", targets: ["Isocache"]),
    .library(name: "IsocacheGRDB", targets: ["IsocacheGRDB"]),
  ],
  dependencies: [.package(url: "https://github.com/groue/GRDB.swift", exact: "7.11.1")],
  targets: [
    .target(name: "Isocache"),
    .target(
      name: "IsocacheGRDB",
      dependencies: ["Isocache", .product(name: "GRDB", package: "GRDB.swift")]),
    .testTarget(name: "IsocacheTests", dependencies: ["Isocache"], resources: [.copy("Fixtures")]),
    .testTarget(name: "IsocacheGRDBTests", dependencies: ["IsocacheGRDB"]),
  ]
)
