# Built core extension libraries

This document describes how builds of the PowerSync SQLite core extension are consumed in different PowerSync SDKs.

## Native

- [Native SDK](https://github.com/powersync-ja/powersync-native): Compiled from source (as a regular Rust dependency).
- [Dart SDK](https://github.com/powersync-ja/powersync.dart/): Uses build hooks to link dynamic libraries attached to releases.  
- [Swift SDK](https://github.com/powersync-ja/powersync-swift): Uses an XCFramework via SwiftPM and this [intermediate repository](https://github.com/powersync-ja/powersync-sqlite-core-swift).
- [Kotlin SDK](https://github.com/powersync-ja/powersync-kotlin):
  - On Android, we use cargo-ndk builds published to Maven Central (see the `android/` directory for more).
  - On the JVM, we download dynamic libraries attached to releases.
  - For Kotlin/Native:
    - Older versions only support Apple targets and expect a framework path to be present. Users are responsible for bundling the extension,
      we suggest a SwiftPM dependency through the intermediate repository.
    - We are migrating towards a setup where we can also support Linux and Windows by linking the core extension statically through cinterops.
- [dotnet SDK](https://github.com/powersync-ja/powersync-dotnet):
  - On maui/Android, we download the Maven Central asset and extract individual libraries.
  - On maui/iOS and maui/macCatalyst: We use the XCFramework downloaded directly from GitHub releases.
  - On Desktop platforms, we download a dynamic library attached to releases.
- [JavaScript SDKs](https://github.com/powersync-ja/powersync-js):
  - Capacitor: Same as React Native.
  - Node: Downloads dynamic libraries attached to releases. We also upload these libraries to npm as part of the package to avoid postinstall scripts.
  - React Native:
    - Android: Gradle dependency to builds published to Maven Central.
    - iOS: We use a CocoaPod dependency downloading the XCFramework.
  - Tauri: Depends on the native SDK, building the core extension from source as part of the app.

## Web

- Dart: We build the core extension as a static [WebAssembly object file](https://github.com/WebAssembly/tool-conventions/blob/main/Linking.md).
  A script in the Dart SDK builds `sqlite3.wasm` and `sqlite3mc.wasm` by linking this file and patching `sqlite3_os_init` to auto-load the extension.
- JavaScript: We have a fork of `wa-sqlite` that statically links the core extension object file and exports `powersync_init_static`.
  The web SDK calls `powersync_init_static` before connections are opened.
  We also build the core extension for [Emscripten dynamic linking](https://emscripten.org/docs/compiling/Dynamic-Linking.html), but this mode is not currently used.
