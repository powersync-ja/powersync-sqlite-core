// swift-tools-version: 5.7

// NOTE! This is never released, we're only using this to support local builds builds for the
// Swift SDK.
import PackageDescription
let packageName = "PowerSyncSQLiteCore"

let package = Package(
    name: packageName,
    platforms: [
        .iOS(.v13),
        .macOS(.v10_15),
        .watchOS(.v9)
    ],
    products: [
        .library(
            name: packageName,
            targets: [packageName]),
    ],
    targets: [
        .binaryTarget(
            name: packageName,
            path: "powersync-sqlite-core.xcframework"
        )
    ]
)
