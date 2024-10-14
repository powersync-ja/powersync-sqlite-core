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

cp "target/wasm32-unknown-emscripten/wasm/powersync.wasm" "libpowersync.wasm"

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

cp "target/wasm32-unknown-emscripten/wasm_asyncify/powersync.wasm" "libpowersync-async.wasm"


# Static lib.
# Works for both sync and asyncify builds.
# Works for both emscripten and wasi.
# target/wasm32-wasi/wasm/libpowersync.a
cargo build \
  -p powersync_static \
  --profile wasm \
  -Z build-std=panic_abort,core,alloc \
  --target wasm32-wasi

cp "target/wasm32-wasi/wasm/libpowersync.a" "libpowersync.wasm"
