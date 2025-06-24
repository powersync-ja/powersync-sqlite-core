#! /bin/bash
set -e

# Adapted from https://github.com/vlcn-io/cr-sqlite/blob/main/core/all-ios-loadable.sh

BUILD_DIR=./build
TARGETS=(
  # iOS and simulator
  aarch64-apple-ios
  aarch64-apple-ios-sim
  x86_64-apple-ios

  # macOS
  aarch64-apple-darwin
  x86_64-apple-darwin

  # watchOS and simulator
  aarch64-apple-watchos
  aarch64-apple-watchos-sim
  x86_64-apple-watchos-sim
  arm64_32-apple-watchos
)
VERSION=0.4.1

function generatePlist() {
  min_os_version=0
  additional_keys=""
  # We support versions 11.0 or later for iOS and macOS. For watchOS, we need 9.0 or later.
  case $1 in
    *"watchos"*)
      additional_keys=$(cat <<EOF
	<key>CFBundleSupportedPlatforms</key>
	<array>
		<string>WatchOS</string>
	</array>
	<key>UIDeviceFamily</key>
	<array>
		<integer>4</integer>
	</array>
EOF
      )
      min_os_version="9.0";;
    *)
      min_os_version="11.0";;
  esac

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
  <string>$min_os_version</string>
  <key>CFBundleVersion</key>
  <string>$VERSION</string>
  <key>CFBundleShortVersionString</key>
  <string>$VERSION</string>
$additional_keys
</dict>
</plist>
EOF
}

function createXcframework() {
  ios_plist=$(generatePlist "ios")
  macos_plist=$(generatePlist "macos")
  watchos_plist=$(generatePlist "watchos")

  echo "===================== create ios device framework ====================="
  mkdir -p "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework"
  echo "${ios_plist}" > "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework/Info.plist"
  cp -f "./target/aarch64-apple-ios/release_apple/libpowersync.dylib" "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework/powersync-sqlite-core"
  install_name_tool -id "@rpath/powersync-sqlite-core.framework/powersync-sqlite-core" "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework/powersync-sqlite-core"
  # Generate dSYM for iOS Device
  dsymutil "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework/powersync-sqlite-core" -o "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework.dSYM"

  echo "===================== create ios simulator framework ====================="
  mkdir -p "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework"
  echo "${ios_plist}" > "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework/Info.plist"
  lipo ./target/aarch64-apple-ios-sim/release_apple/libpowersync.dylib ./target/x86_64-apple-ios/release_apple/libpowersync.dylib -create -output "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework/powersync-sqlite-core"
  install_name_tool -id "@rpath/powersync-sqlite-core.framework/powersync-sqlite-core" "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework/powersync-sqlite-core"
  # Generate dSYM for iOS Simulator
  dsymutil "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework/powersync-sqlite-core" -o "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework.dSYM"

  echo "===================== create macos framework ====================="
  mkdir -p "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/A/Resources"
  echo "${ios_plist}" > "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/A/Resources/Info.plist"
  lipo ./target/x86_64-apple-darwin/release_apple/libpowersync.dylib ./target/aarch64-apple-darwin/release_apple/libpowersync.dylib -create -output "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/A/powersync-sqlite-core"
  install_name_tool -id "@rpath/powersync-sqlite-core.framework/powersync-sqlite-core" "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/A/powersync-sqlite-core"
  ln -sf A "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/Current"
  ln -sf Versions/Current/powersync-sqlite-core "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/powersync-sqlite-core"
  ln -sf Versions/Current/Resources "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Resources"
  # Generate dSYM for macOS
  dsymutil "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework/Versions/A/powersync-sqlite-core" -o "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework.dSYM"

  echo "===================== create watchos device framework ====================="
  mkdir -p "${BUILD_DIR}/watchos-arm64_arm64_32_armv7k/powersync-sqlite-core.framework/Versions/A/Resources"
  echo "${watchos_plist}" > "${BUILD_DIR}/watchos-arm64_arm64_32_armv7k/powersync-sqlite-core.framework/Versions/A/Resources/Info.plist"
  lipo ./target/aarch64-apple-watchos/release_apple/libpowersync.a ./target/arm64_32-apple-watchos/release_apple/libpowersync.a -create -output "${BUILD_DIR}/watchos-arm64_arm64_32_armv7k/powersync-sqlite-core.framework/Versions/A/powersync-sqlite-core"
  # install_name_tool isn't necessary, we use a statically-linked library
  ln -sf A "${BUILD_DIR}/watchos-arm64_arm64_32_armv7k/powersync-sqlite-core.framework/Versions/Current"
  ln -sf Versions/Current/powersync-sqlite-core "${BUILD_DIR}/watchos-arm64_arm64_32_armv7k/powersync-sqlite-core.framework/powersync-sqlite-core"
  ln -sf Versions/Current/Resources "${BUILD_DIR}/watchos-arm64_arm64_32_armv7k/powersync-sqlite-core.framework/Resources"

  echo "===================== create watchos simulator framework ====================="
  mkdir -p "${BUILD_DIR}/watchos-arm64_x86_64-simulator/powersync-sqlite-core.framework/Versions/A/Resources"
  echo "${watchos_plist}" > "${BUILD_DIR}/watchos-arm64_x86_64-simulator/powersync-sqlite-core.framework/Versions/A/Resources/Info.plist"
  lipo ./target/aarch64-apple-watchos-sim/release_apple/libpowersync.a ./target/x86_64-apple-watchos-sim/release_apple/libpowersync.a -create -output "${BUILD_DIR}/watchos-arm64_x86_64-simulator/powersync-sqlite-core.framework/Versions/A/powersync-sqlite-core"
  # install_name_tool isn't necessary, we use a statically-linked library
  ln -sf A "${BUILD_DIR}/watchos-arm64_x86_64-simulator/powersync-sqlite-core.framework/Versions/Current"
  ln -sf Versions/Current/powersync-sqlite-core "${BUILD_DIR}/watchos-arm64_x86_64-simulator/powersync-sqlite-core.framework/powersync-sqlite-core"
  ln -sf Versions/Current/Resources "${BUILD_DIR}/watchos-arm64_x86_64-simulator/powersync-sqlite-core.framework/Resources"

  echo "===================== create xcframework ====================="
  rm -rf "${BUILD_DIR}/powersync-sqlite-core.xcframework"

  xcodebuild -create-xcframework \
    -framework "${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework" \
    -debug-symbols "$(pwd -P)/${BUILD_DIR}/ios-arm64/powersync-sqlite-core.framework.dSYM" \
    -framework "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework" \
    -debug-symbols "$(pwd -P)/${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync-sqlite-core.framework.dSYM" \
    -framework "${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework" \
    -debug-symbols "$(pwd -P)/${BUILD_DIR}/macos-arm64_x86_64/powersync-sqlite-core.framework.dSYM" \
    -framework "${BUILD_DIR}/watchos-arm64_arm64_32_armv7k/powersync-sqlite-core.framework" \
    -framework "${BUILD_DIR}/watchos-arm64_x86_64-simulator/powersync-sqlite-core.framework" \
    -output "${BUILD_DIR}/powersync-sqlite-core.xcframework"

  cp -Rf "${BUILD_DIR}/powersync-sqlite-core.xcframework" "powersync-sqlite-core.xcframework"
  zip -r --symlinks powersync-sqlite-core.xcframework.zip powersync-sqlite-core.xcframework LICENSE README.md
  rm -rf ${BUILD_DIR}
}

# Make all the non-simulator libs
# Package into a universal ios lib

rm -rf powersync-sqlite-core.xcframework

for TARGET in ${TARGETS[@]}; do
  echo "Building PowerSync loadable extension for $TARGET"

  if [[ $TARGET == *"watchos"* ]]; then
    cargo build \
      -p powersync_static \
      --profile release_apple \
      --target $TARGET \
      -Zbuild-std
  else
    cargo build -p powersync_loadable --profile release_apple --target $TARGET -Zbuild-std
  fi
done

createXcframework
