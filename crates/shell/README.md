# powersync-sqlite

Builds sqlite with powersync extension embedded.

SQLite itself is built using [build.rs](./build.rs), and linked into the Rust binary.

The main function is defined in SQLite, so we use `#![no_main]` here.
