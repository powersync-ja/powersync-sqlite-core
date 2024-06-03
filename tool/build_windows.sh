if [ "$1" = "x64" ]; then
  rustup toolchain install nightly-2024-05-18-x86_64-pc-windows-msvc
  rustup component add rust-src --toolchain nightly-2024-05-18-x86_64-pc-windows-msvc
  rustup target add x86_64-pc-windows-msvc
  cargo build -p powersync_loadable --release
  mv "target/release/powersync.dll" "powersync_x64.dll"
else
  rustup toolchain install nightly-2024-05-18-aarch64-pc-windows-msvc
  rustup component add rust-src --toolchain nightly-2024-05-18-aarch64-pc-windows-msvc
  rustup target add aarch64-pc-windows-msvc
  cargo build -p powersync_loadable --release
  mv "target/release/powersync.dll" "powersync_aarch64.dll"
fi