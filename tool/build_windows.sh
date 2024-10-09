#!/bin/sh
set -e

if [ "$1" = "x64" ]; then
  cargo build -Z build-std=panic_abort,core,alloc -p powersync_loadable --release --target x86_64-pc-windows-msvc
  mv "target/x86_64-pc-windows-msvc/release/powersync.dll" "powersync_x64.dll"
else
  #Note: aarch64-pc-windows-msvc has not been tested.
  cargo build -Z build-std=panic_abort,core,alloc -p powersync_loadable --release --target aarch64-pc-windows-msvc
  mv "target/aarch64-pc-windows-msvc/release/powersync.dll" "powersync_aarch64.dll"
fi