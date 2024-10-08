#!/bin/sh
set -e

if [ "$1" = "x64" ]; then
  cargo build -p powersync_loadable -Z build-std=panic_abort,core,alloc --release --target x86_64-unknown-linux-gnu
  mv "target/x86_64-unknown-linux-gnu/release/libpowersync.so" "libpowersync_x64.so"
else
  cargo build -p powersync_loadable -Z build-std=panic_abort,core,alloc --release --target aarch64-unknown-linux-gnu
  mv "target/aarch64-unknown-linux-gnu/release/libpowersync.so" "libpowersync_aarch64.so"
fi
