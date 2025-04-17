#![no_main]
#![allow(internal_features)]
#![feature(lang_items)]
#![feature(core_intrinsics)]

use core::ffi::{c_char, c_int};

use powersync_core::powersync_init_static;

// Use the SQLite allocator, allowing us to freely transfer memory between SQLite and Rust.
#[cfg(not(test))]
use sqlite_nostd::SQLite3Allocator;

#[cfg(not(test))]
#[global_allocator]
static ALLOCATOR: SQLite3Allocator = SQLite3Allocator {};

#[no_mangle]
pub extern "C" fn core_init(_dummy: *mut c_char) -> c_int {
    powersync_init_static()
}
