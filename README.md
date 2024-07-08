# PowerSync SQLite Extension

This extension is used by PowerSync client SDKs.

The APIs here not currently stable, and may change in any release. The APIs are intended to be used by PowerSync SDKs only.

# API

Primary APIs:

```sql
-- Load the extension
-- Sets up functions and views, but does not touch the database itself.
.load powersync

-- Configure the schemas.
-- Creates data tables, indexes and views.
SELECT powersync_replace_schema('{"tables": [{"name": "test", "columns": [{"name": "name", "type": "text"}]}]}');

```

Other APIs:

```sql
-- Initialize the extension data (creates internal tables).
-- Optional - also called as part of powersync_replace_schema().
-- Only useful to ensure internal tables are configured without touching the schema.
SELECT powersync_init();

```

# Building and running

Initialize submodules recursively

```
git submodule update --init --recursive
```

```sh
# Build the shell
cargo build -p powersync_sqlite
./target/debug/powersync_sqlite test.db "select powersync_rs_version()"

# Build the loadable extension
cargo build -p powersync_loadable
sqlite3 ":memory:" ".load ./target/debug/libpowersync" "select powersync_rs_version()" #This requires sqlite3 installed

# Build the release loadable extension
cargo build -p powersync_loadable --release

# Build for iOS
./all-ios-loadable.sh
```

# Acknowledgements

Structure of the SQLite extension using Rust is inspired by [cr-sqlite](https://github.com/vlcn-io/cr-sqlite/).
