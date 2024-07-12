if [ "$1" = "x64" ]; then
  #Note: x86_64-apple-darwin has not been tested.
  rustup target add target x86_64-apple-darwin
  cargo build -p powersync_loadable --release
  mv "target/release/libpowersync.dylib" "libpowersync_x64.dylib"
else
  rustup target add aarch64-apple-darwin
  cargo build -p powersync_loadable --release
  mv "target/release/libpowersync.dylib" "libpowersync_aarch64.dylib"
fi
