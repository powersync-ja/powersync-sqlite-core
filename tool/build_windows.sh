if [ "$1" = "x64" ]; then
  rustup target add x86_64-pc-windows-msvc
  cargo build -p powersync_loadable --release
  mv "target/release/powersync.dll" "powersync_x64.dll"
else
  rustup target add aarch64-pc-windows-msvc
  cargo build -p powersync_loadable --release
  mv "target/release/powersync.dll" "powersync_aarch64.dll"
fi