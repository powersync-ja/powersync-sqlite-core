Builds the core extension as a static library, exposing the `powersync_init_static` function to load it.

We only use this crate to compile for watchOS, since the regular `loadable` build compiling to a dylib
doesn't support that platform.

Most users should probably compile the `loadable` crate instead, which also emits a static library. If
SQLite is linked statically, compiling `loadable` with the `static` feature enabled works.
