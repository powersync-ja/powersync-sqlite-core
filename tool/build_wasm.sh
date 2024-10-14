#!/bin/bash
set -e

# Normal build
# target/wasm32-unknown-emscripten/wasm/powersync.wasm
RUSTFLAGS="-C link-arg=-sSIDE_MODULE=2" \
  cargo build \
    -p powersync_loadable \
    --profile wasm \
    --no-default-features \
    --features "powersync_core/static powersync_core/omit_load_extension sqlite_nostd/omit_load_extension" \
    -Z build-std=panic_abort,core,alloc \
    --target wasm32-unknown-emscripten


# Asyncify
# target/wasm32-unknown-emscripten/wasm_asyncify/powersync.wasm
RUSTFLAGS="-C link-arg=-sSIDE_MODULE=2 -C link-arg=-sASYNCIFY=1 -C link-arg=-sJSPI_IMPORTS=@wasm/asyncify_imports.json" \
  cargo build \
    -p powersync_loadable \
    --profile wasm_asyncify \
    --no-default-features \
    --features "powersync_core/static powersync_core/omit_load_extension sqlite_nostd/omit_load_extension" \
    -Z build-std=panic_abort,core,alloc \
    --target wasm32-unknown-emscripten

# Static lib (works for both sync and asyncify builds)
# target/wasm32-unknown-emscripten/wasm/libpowersync.a
cargo build \
  -p powersync_static \
  --profile wasm \
  -Z build-std=panic_abort,core,alloc \
  --target wasm32-unknown-emscripten
