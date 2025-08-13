#!/bin/sh
set -e

case "$1" in
  x64)
    cargo build -p powersync_loadable -Z build-std=panic_abort,core,alloc --release --target x86_64-unknown-linux-gnu
    mv "target/x86_64-unknown-linux-gnu/release/libpowersync.so" "libpowersync_x64.so"
    ;;
  aarch64)
    cargo build -p powersync_loadable -Z build-std=panic_abort,core,alloc --release --target aarch64-unknown-linux-gnu
    mv "target/aarch64-unknown-linux-gnu/release/libpowersync.so" "libpowersync_aarch64.so"
    ;;
  armv7)
    cargo build -p powersync_loadable -Z build-std=panic_abort,core,alloc --release --target armv7-unknown-linux-gnueabihf
    mv "target/armv7-unknown-linux-gnueabihf/release/libpowersync.so" "libpowersync_armv7.so"
    ;;
  riscv64gc)
    cargo build -p powersync_loadable -Z build-std=panic_abort,core,alloc --release --target riscv64gc-unknown-linux-gnu
    mv "target/riscv64gc-unknown-linux-gnu/release/libpowersync.so" "libpowersync_riscv64gc.so"
    ;;
  *)
    echo "Unknown architecture"
    exit 1;
    ;;
esac
