This is a fork of https://github.com/vlcn-io/sqlite-rs-embedded with adaptations for the PowerSync core extension.

# SQLite no_std

> Note: these bindings are faithful to the base SQLite C-API as much as possible for minimum rust<->c overhead. This, however, means that the bindings are not entirely safe. E.g., the SQLite statement object will clear returned values out from under you if you step or finalize it while those references exist in your Rust program.

SQLite is lite. Its bindings should be lite too. They should be able to be used _anywhere_ SQLite is used, _not_ incur any performance impact, _not_ include any extra dependencies, and be usable against _any_ SQLite version.

Thus this repository was born.

These bindings:

- Do not require the rust standard library
- Can use the SQLite memory subsystem if no allocator exists
- Can be used to write SQLite extensions that compile to WASM and run in the browser
- Does 0 copying. E.g., through some tricks, Rust strings are passed directly to SQLite with no conversion to or copying to CString.

## Features

By default, this crate compiles to be used in a loadable SQLite extension: All calls are dispatched through
the `sqlite3_api_routines` struct, and one needs to call `EXTENSION_INIT2()` from an entrypoint before using
the library.

Outside of loadable extensions, one can enable the `static` feature. When enabled, calls go to `sqlite3_`
functions directly, SQLite needs to be linked for this library to work.
