#!/bin/sh
set -e

case "$1" in
  x64)
    cargo build -Z build-std=panic_abort,core,alloc -p powersync_loadable --release --target x86_64-pc-windows-msvc
    mv "target/x86_64-pc-windows-msvc/release/powersync.dll" "powersync_x64.dll"
    ;;
  x86)
    cargo build -Z build-std=panic_abort,core,alloc -p powersync_loadable --release --target i686-pc-windows-msvc
    mv "target/i686-pc-windows-msvc/release/powersync.dll" "powersync_x86.dll"
    ;;
  aarch64)
    cargo build -Z build-std=panic_abort,core,alloc -p powersync_loadable --release --target aarch64-pc-windows-msvc
    mv "target/aarch64-pc-windows-msvc/release/powersync.dll" "powersync_aarch64.dll"
    ;;
  *)
    echo "Unknown architecture"
    exit 1
    ;;
esac
