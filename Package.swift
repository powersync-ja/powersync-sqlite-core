// swift-tools-version:5.3
import PackageDescription
let packageName = "PowerSyncSQLiteCore"

let package = Package(
    name: packageName,
    platforms: [
        .iOS(.v11),
        .macOS(.v10_13)
    ],
    products: [
        .library(
            name: packageName,
            targets: [packageName]
        ),
    ],
    targets: [
        .binaryTarget(
            name: packageName,
            url: "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/0.3.0/powersync-sqlite-core.xcframework.tar.gz",
            checksum: "e8b8c22540b0af1c5a1e38c29917e6d6fd8098b3ef2052b2d5beb02d92009903"
        )
    ]
)
