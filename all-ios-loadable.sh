#! /bin/bash
set -e

# Adapted from https://github.com/vlcn-io/cr-sqlite/blob/main/core/all-ios-loadable.sh


BUILD_DIR=./build
DIST_PACKAGE_DIR=./dist

function createXcframework() {
  plist=$(cat << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleExecutable</key>
  <string>powersync</string>
  <key>CFBundleIdentifier</key>
  <string>co.powersync</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundlePackageType</key>
  <string>FMWK</string>
  <key>CFBundleSignature</key>
  <string>????</string>
</dict>
</plist>
EOF
)
  printf "\n\n\t\t===================== create ios device framework =====================\n\n"
  mkdir -p "${BUILD_DIR}/ios-arm64/powersync.framework"
  echo "${plist}" > "${BUILD_DIR}/ios-arm64/powersync.framework/Info.plist"
  cp -f "./target/aarch64-apple-ios/release/libpowersync.dylib" "${BUILD_DIR}/ios-arm64/powersync.framework/powersync"
  install_name_tool -id "@rpath/powersync.framework/powersync" "${BUILD_DIR}/ios-arm64/powersync.framework/powersync"

  printf "\n\n\t\t===================== create ios simulator framework =====================\n\n"
  mkdir -p "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync.framework"
  echo "${plist}" > "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync.framework/Info.plist"
  cp -p "./target/powersync-universal-ios-sim.dylib" "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync.framework/powersync"
  install_name_tool -id "@rpath/powersync.framework/powersync" "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync.framework/powersync"

  printf "\n\n\t\t===================== create ios xcframework =====================\n\n"
  rm -rf "${BUILD_DIR}/powersync.xcframework"
  xcodebuild -create-xcframework -framework "${BUILD_DIR}/ios-arm64/powersync.framework" -framework "${BUILD_DIR}/ios-arm64_x86_64-simulator/powersync.framework" -output "${BUILD_DIR}/powersync.xcframework"

  mkdir -p ${DIST_PACKAGE_DIR}
  cp -Rf "${BUILD_DIR}/powersync.xcframework" "${DIST_PACKAGE_DIR}/powersync.xcframework"
  cd ${DIST_PACKAGE_DIR}
  tar -czvf powersync-ios-dylib.xcframework.tar.gz powersync.xcframework
  rm -rf ${BUILD_DIR}
}

# Make all the non-simulator libs
# Package into a universal ios lib

cargo build -p powersync_loadable --release --target aarch64-apple-ios -Zbuild-std
cargo build -p powersync_loadable --release --target aarch64-apple-ios-sim -Zbuild-std
cargo build -p powersync_loadable --release --target x86_64-apple-ios -Zbuild-std

lipo ./target/aarch64-apple-ios-sim/release/libpowersync.dylib ./target/x86_64-apple-ios/release/libpowersync.dylib -create -output ./target/powersync-universal-ios-sim.dylib

createXcframework
