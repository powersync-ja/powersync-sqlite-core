<p align="center">
  <a href="https://www.powersync.com" target="_blank"><img src="https://github.com/powersync-ja/.github/assets/7372448/d2538c43-c1a0-4c47-9a76-41462dba484f"/></a>
</p>

_[PowerSync](https://www.powersync.com) is a sync engine for building local-first apps with instantly-responsive UI/UX and simplified state transfer. Syncs between SQLite on the client-side and Postgres, MongoDB or MySQL on the server-side._

# powersync_core

This is the core SQLite extension, containing all the logic. This is used internally by PowerSync SDKs,
and would typically not be used by users directly.

The role of the extension is to create user-defined functions that higher-level SDKs would use to implement
schema management and a PowerSync client.
Not all of this is documented, but [this directory](https://github.com/powersync-ja/powersync-sqlite-core/tree/main/docs)
provides some hints on how a custom PowerSync SDK could be implemented.

For this reason, the crate doesn't have much of a public API. In the default build mode, it doesn't expect
SQLite to be linked and exposes a single function: `sqlite3_powersync_init`,
a [loadable extension](https://sqlite.org/loadext.html) entrypoint.

For applications linking SQLite, the `static` feature of this crate can be enabled.
With that feature, `powersync_init_static()` can be called to load the
extension for all new connections.
The application is responsible for linking SQLite in that case.
