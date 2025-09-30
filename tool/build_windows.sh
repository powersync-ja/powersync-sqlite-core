#!/bin/sh
set -e

function compile() {
  local triple=$1
  local suffix=$2

  cargo build -p powersync_loadable -Z build-std=panic_abort,core,alloc --release --target $triple
  cargo build -p powersync_static -Z build-std=panic_abort,core,alloc --release --target $triple

  mv "target/$triple/release/powersnc.dll" "powersync_$suffix.dll"
  mv "target/$triple/release/powersync.lib" "powersync_$suffix.lib"
}

case "$1" in
  x64)
    compile x86_64-pc-windows-msvc x64
    ;;
  x86)
    compile i686-pc-windows-msvc x86
    ;;
  aarch64)
    compile aarch64-pc-windows-msvc aarch64
    ;;
  *)
    echo "Unknown architecture"
    exit 1
    ;;
esac
