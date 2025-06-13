#! /bin/bash
set -e

# Adapted from https://github.com/vlcn-io/cr-sqlite/blob/main/core/all-ios-loadable.sh

BUILD_DIR=./build

function createXcframework() {
  ios_plist=$(
    cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleExecutable</key>
  <string>powersync-sqlite-core</string>
  <key>CFBundleIdentifier</key>
  <string>co.powersync.sqlitecore</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundlePackageType</key>
  <string>FMWK</string>
  <key>CFBundleSignature</key>
  <string>????</string>
  <key>MinimumOSVersion</key>
  <string>11.0</string>
  <key>CFBundleVersion</key>
  <string>0.4.0</string>
  <key>CFBundleShortVersionString</key>
  <string>0.4.0</string>
</dict>
</plist>
EOF
  )

  watchos_plist=$(
    cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleExecutable</key>
  <string>powersync-sqlite-core</string>
  <key>CFBundleIdentifier</key>
  <string>co.powersync.sqlitecore</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundlePackageType</key>
  <string>FMWK</string>
  <key>CFBundleSignature</key>
  <string>????</string>
  <key>MinimumOSVersion</key>
  <string>7.0</string>
  <key>CFBundleVersion</key>
  <string>0.3.12</string>
  <key>CFBundleShortVersionString</key>
  <string>0.3.12</string>
  <key>UIDeviceFamily</key>
  <array>
    <integer>4</integer>
  </array>
  <key>DTSDKName</key>
  <string>watchos</string>
  <key>DTPlatformName</key>
  <string>watchos</string>
  <key>DTPlatformVersion</key>
  <string>7.0</string>
  <key>DTXcode</key>
  <string>1500</string>
  <key>DTXcodeBuild</key>
  <string>15A240d</string>
  <key>DTCompiler</key>
  <string>com.apple.compilers.llvm.clang.1_0</string>
  <key>DTPlatformBuild</key>
  <string>21R355</string>
  <key>BuildMachineOSBuild</key>
  <string>23D60</string>
</dict>
</plist>
EOF
  )

  echo "===================== create ios device framework ====================="
  mkdir -p "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework"
  echo "${ios_plist}" >"${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework/Info.plist"
  cp -f "./target/aarch64-apple-ios/release_apple/libpowersync.dylib" "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework/powersync-sqlite-core"
  install_name_tool -id "@rpath/powersync-sqlite-core.framework/powersync-sqlite-core" "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework/powersync-sqlite-core"
  # Generate dSYM for iOS Device
  dsymutil "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework/powersync-sqlite-core" -o "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework.dSYM"

  echo "===================== create ios simulator framework ====================="
  mkdir -p "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework"
  echo "${ios_plist}" >"${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework/Info.plist"
  lipo ./target/aarch64-apple-ios-sim/release_apple/libpowersync.dylib ./target/x86_64-apple-ios/release_apple/libpowersync.dylib -create -output "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework/powersync-sqlite-core"
  install_name_tool -id "@rpath/powersync-sqlite-core.framework/powersync-sqlite-core" "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework/powersync-sqlite-core"
  # Generate dSYM for iOS Simulator
  dsymutil "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework/powersync-sqlite-core" -o "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework.dSYM"

  echo "===================== create macos framework ====================="
  mkdir -p "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/A/Resources"
  echo "${ios_plist}" >"${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/A/Resources/Info.plist"
  lipo ./target/x86_64-apple-darwin/release_apple/libpowersync.dylib ./target/aarch64-apple-darwin/release_apple/libpowersync.dylib -create -output "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/A/powersync-sqlite-core"
  install_name_tool -id "@rpath/powersync-sqlite-core.framework/powersync-sqlite-core" "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/A/powersync-sqlite-core"
  ln -sf A "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/Current"
  ln -sf Versions/Current/powersync-sqlite-core "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/powersync-sqlite-core"
  ln -sf Versions/Current/Resources "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Resources"
  # Generate dSYM for macOS
  dsymutil "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/A/powersync-sqlite-core" -o "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework.dSYM"

  echo "===================== create watchos device framework ====================="
  mkdir -p "${BUILD_DIR}/watchos-arm64/powersync-sqlite-core.framework/Versions/A/Resources"
  echo "${watchos_plist}" >"${BUILD_DIR}/watchos-arm64/powersync-sqlite-core.framework/Versions/A/Resources/Info.plist"
  cp -f "./target/aarch64-apple-watchos/release_apple/libpowersync.a" "${BUILD_DIR}/watchos-arm64/powersync-sqlite-core.framework/Versions/A/powersync-sqlite-core"
  ln -sf A "${BUILD_DIR}/watchos-arm64/powersync-sqlite-core.framework/Versions/Current"
  ln -sf Versions/Current/powersync-sqlite-core "${BUILD_DIR}/watchos-arm64/powersync-sqlite-core.framework/powersync-sqlite-core"
  ln -sf Versions/Current/Resources "${BUILD_DIR}/watchos-arm64/powersync-sqlite-core.framework/Resources"

  echo "===================== create watchos simulator framework ====================="
  mkdir -p "${BUILD_DIR}/watchos-arm64-simulator/powersync-sqlite-core.framework/Versions/A/Resources"
  echo "${watchos_plist}" >"${BUILD_DIR}/watchos-arm64-simulator/powersync-sqlite-core.framework/Versions/A/Resources/Info.plist"
  lipo ./target/aarch64-apple-watchos-sim/release_apple/libpowersync.a ./target/x86_64-apple-watchos-sim/release_apple/libpowersync.a -create -output "${BUILD_DIR}/watchos-arm64-simulator/powersync-sqlite-core.framework/Versions/A/powersync-sqlite-core"
  ln -sf A "${BUILD_DIR}/watchos-arm64-simulator/powersync-sqlite-core.framework/Versions/Current"
  ln -sf Versions/Current/powersync-sqlite-core "${BUILD_DIR}/watchos-arm64-simulator/powersync-sqlite-core.framework/powersync-sqlite-core"
  ln -sf Versions/Current/Resources "${BUILD_DIR}/watchos-arm64-simulator/powersync-sqlite-core.framework/Resources"

  echo "===================== create xcframework ====================="
  rm -rf "${BUILD_DIR}/powersync-sqlite-core.xcframework"

  xcodebuild -create-xcframework \
    -framework "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework" \
    -debug-symbols "$(pwd -P)/${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework.dSYM" \
    -framework "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework" \
    -debug-symbols "$(pwd -P)/${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework.dSYM" \
    -framework "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework" \
    -debug-symbols "$(pwd -P)/${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework.dSYM" \
    -output "${BUILD_DIR}/powersync-sqlite-core.xcframework"

  # how to create a watchOS XCFramework with static libraries, possible?

  zip -r --symlinks powersync-sqlite-core.xcframework.zip powersync-sqlite-core.xcframework "${BUILD_DIR}/watchos-arm64/powersync-sqlite-core.framework/libpowersync.a" "${BUILD_DIR}/watchos-arm64-simulator/powersync-sqlite-core.framework/libpowersync.a" LICENSE README.md
  rm -rf ${BUILD_DIR}
}

# Make all the non-simulator libs
# Package into a universal ios lib

rm -rf powersync-sqlite-core.xcframework

# iOS
cargo build -p powersync_loadable --profile release_apple --target aarch64-apple-ios -Zbuild-std
# Simulator
cargo build -p powersync_loadable --profile release_apple --target aarch64-apple-ios-sim -Zbuild-std
cargo build -p powersync_loadable --profile release_apple --target x86_64-apple-ios -Zbuild-std
# macOS
cargo build -p powersync_loadable --profile release_apple --target aarch64-apple-darwin -Zbuild-std
cargo build -p powersync_loadable --profile release_apple --target x86_64-apple-darwin -Zbuild-std
# watchOS
cargo build -p powersync_loadable --profile release_apple -Zbuild-std=std,panic_abort --target aarch64-apple-watchos
cargo build -p powersync_loadable --profile release_apple -Zbuild-std=std,panic_abort --target aarch64-apple-watchos-sim
cargo build -p powersync_loadable --profile release_apple -Zbuild-std=std,panic_abort --target x86_64-apple-watchos-sim

createXcframework
