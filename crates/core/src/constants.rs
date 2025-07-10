use core::ffi::c_int;

pub const CORE_PKG_VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const FULL_GIT_HASH: &'static str = env!("GIT_HASH");

// We need 3.44 or later to use an `ORDER BY` in an aggregate function invocation.
//
// When raising the minimum version requirement, also change the CI to ensure we're testing on the
// oldest SQLite version we claim to support.
pub const MIN_SQLITE_VERSION_NUMBER: c_int = 3044000;

pub fn short_git_hash() -> &'static str {
    &FULL_GIT_HASH[..8]
}
