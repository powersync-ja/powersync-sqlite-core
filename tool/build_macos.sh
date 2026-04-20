#!/bin/sh
set -e

function compile() {
  local triple=$1
  local suffix=$2
  local os=$3

  cargo build -p powersync_loadable -Z build-std=panic_abort,core,alloc --features nightly --release --target $triple

  mv "target/$triple/release/libpowersync.dylib" "libpowersync_$suffix.$os.dylib"
  mv "target/$triple/release/libpowersync.a" "libpowersync_$suffix.$os.a"
}

function compile_static() {
  local triple=$1
  local suffix=$2
  local os=$3

  cargo build -p powersync_static -Z build-std=panic_abort,core,alloc --features nightly --release --target $triple

  mv "target/$triple/release/libpowersync.a" "libpowersync_$suffix.$os.a"
}

case "$1" in
  x64)
    compile x86_64-apple-darwin x64 macos
    compile x86_64-apple-ios x64 ios-sim
    compile x86_64-apple-tvos x64 tvos-sim
    compile_static x86_64-apple-watchos-sim x64 watchos-sim
    ;;
  aarch64)
    compile aarch64-apple-darwin aarch64 macos
    compile aarch64-apple-ios-sim aarch64 ios-sim
    compile aarch64-apple-ios aarch64 ios
    compile aarch64-apple-tvos aarch64 tvos
    compile aarch64-apple-tvos-sim aarch64 tvos-sim
    compile_static aarch64-apple-watchos aarch64 watchos
    compile_static aarch64-apple-watchos-sim aarch64 watchos-sim
    compile_static arm64_32-apple-watchos arm64_32 watchos
    ;;
  *)
    echo "Unknown architecture"
    exit 1;
    ;;
esac
