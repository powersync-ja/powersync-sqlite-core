#!/bin/bash
set -e

function compile_core() {
  local sanitizer=$1

  RUSTDOCFLAGS="-Zsanitizer=$sanitizer" RUSTFLAGS="-Zsanitizer=$sanitizer" cargo build \
    -p powersync_loadable \
    -Z build-std=panic_abort,core,alloc \
    --features nightly \
    --release \
    --target x86_64-unknown-linux-gnu

  mv "target/x86_64-unknown-linux-gnu/release/libpowersync.so" "sanitized/core_extension/libpowersync_$sanitizer.linux.so"
}

function compile_sqlite() {
  local sanitizer=$1

  clang -O3 -fsanitize=$sanitizer -fno-omit-frame-pointer -fuse-ld=lld -fPIC \
    -DSQLITE_ENABLE_API_ARMOR=1 \
    -DSQLITE_OMIT_DEPRECATED \
    -DSQLITE_DQS=0 \
    -DSQLITE_ENABLE_DBSTAT_VTAB \
    -DSQLITE_ENABLE_FTS5 \
    -DSQLITE_ENABLE_STMTVTAB \
    -shared \
    crates/sqlite/sqlite/sqlite3.c \
    -o sanitized/sqlite/libsqlite3_$sanitizer.so
}

mkdir -p sanitized/core_extension
compile_core address
compile_core memory

mkdir -p sanitized/sqlite
compile_sqlite address
compile_sqlite memory
