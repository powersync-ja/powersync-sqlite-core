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
            url: "https://github.com/powersync-ja/powersync-sqlite-core/releases/download/v0.3.0/powersync-sqlite-core.xcframework.tar.gz",
            checksum: "65e09557520aabadfd5897c59a7b5e542f85a6c161b546dcb58109f63e0478e7"
        )
    ]
)
