#!/bin/sh
set -e

function compile() {
  local triple=$1
  local suffix=$2

  cargo build -p powersync_loadable -Z build-std=panic_abort,core,alloc --release --target $triple
  cargo build -p powersync_static -Z build-std=panic_abort,core,alloc --release --target $triple

  mv "target/$triple/release/libpowersync.dylib" "libpowersync_$suffix.dylib"
  mv "target/$triple/release/libpowersync.a" "libpowersync_$suffix.macos.a"
}

if [ "$1" = "x64" ]; then
  compile x86_64-apple-darwin x64
else
  compile aarch64-apple-darwin aarch64
fi
