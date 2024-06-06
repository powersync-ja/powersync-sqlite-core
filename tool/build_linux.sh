if [ "$1" = "x64" ]; then
  rustup target add target x86_64-unknown-linux-gnu
  cargo build -p powersync_loadable --release
  mv "target/release/libpowersync.so" "libpowersync_x64.so"
else
  #Note: aarch64-unknown-linux-gnu has not been tested.
  rustup target add aarch64-unknown-linux-gnu
  cargo build -p powersync_loadable --release
  mv "target/release/libpowersync.so" "libpowersync_aarch64.so"
fi
