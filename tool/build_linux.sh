if [ "$1" = "x64" ]; then
  rustup toolchain install nightly-2024-05-18-x86_64-unknown-linux-gnu
  rustup component add rust-src --toolchain nightly-2024-05-18-x86_64-unknown-linux-gnu
  rustup target add target x86_64-unknown-linux-gnu
  cargo build -p powersync_loadable --release
  mv "target/release/libpowersync.so" "libpowersync_x64.so"
else
  rustup toolchain install nightly-2024-05-18-aarch64-unknown-linux-gnu
  rustup component add rust-src --toolchain nightly-2024-05-18-aarch64-unknown-linux-gnu
  rustup target add aarch64-unknown-linux-gnu
  cargo build -p powersync_loadable --release
  mv "target/release/libpowersync.so" "libpowersync_aarch64.so"
fi
