#!/bin/bash
set -e

# Static lib.
# Works for both sync and asyncify builds.
# Works for both emscripten and wasi.
# target/wasm32-unknown-unknown/wasm/libpowersync.a
cargo build \
  -p powersync_loadable \
  --profile wasm \
  --no-default-features \
  --features "static nightly" \
  -Z build-std=panic_abort,core,alloc \
  --target wasm32-unknown-unknown

cp "target/wasm32-unknown-unknown/wasm/libpowersync.a" "libpowersync-wasm.a"
