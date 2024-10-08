#!/bin/sh
set -e

if [ "$1" = "x64" ]; then
  cargo build -Z build-std=panic_abort,core,alloc -p powersync_loadable --release --target x86_64-apple-darwin
  mv "target/x86_64-apple-darwin/release/libpowersync.dylib" "libpowersync_x64.dylib"
else
  cargo build -Z build-std=panic_abort,core,alloc -p powersync_loadable --release --target aarch64-apple-darwin
  mv "target/aarch64-apple-darwin/release/libpowersync.dylib" "libpowersync_aarch64.dylib"
fi
