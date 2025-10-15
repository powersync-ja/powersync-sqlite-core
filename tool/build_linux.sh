#!/bin/bash
set -e

function compile() {
  local triple=$1
  local suffix=$2

  cargo build -p powersync_loadable -Z build-std=panic_abort,core,alloc --release --target $triple
  cargo build -p powersync_static -Z build-std=panic_abort,core,alloc --release --target $triple

  mv "target/$triple/release/libpowersync.so" "libpowersync_$suffix.linux.so"
  mv "target/$triple/release/libpowersync.a" "libpowersync_$suffix.linux.a"
}

case "$1" in
  x64)
    compile x86_64-unknown-linux-gnu x64
    ;;
  x86)
    compile i686-unknown-linux-gnu x86
    ;;
  aarch64)
    compile aarch64-unknown-linux-gnu aarch64
    ;;
  armv7)
    compile armv7-unknown-linux-gnueabihf armv7
    ;;
  riscv64gc)
    compile riscv64gc-unknown-linux-gnu riscv64gc
    ;;
  *)
    echo "Unknown architecture"
    exit 1;
    ;;
esac
