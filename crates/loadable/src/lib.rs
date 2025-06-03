#![no_std]
#![feature(vec_into_raw_parts)]
#![feature(core_intrinsics)]
#![allow(internal_features)]
#![feature(lang_items)]

extern crate alloc;

// Defines sqlite3_powersync_init
#[allow(unused_imports)]
use powersync_core;

// Use the SQLite allocator, allowing us to freely transfer memory between SQLite and Rust.
#[cfg(not(test))]
use sqlite_nostd::SQLite3Allocator;

#[cfg(not(test))]
#[global_allocator]
static ALLOCATOR: SQLite3Allocator = SQLite3Allocator {};

// Custom Panic handler for WASM and other no_std builds
#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    core::intrinsics::abort()
}

#[cfg(not(target_family = "wasm"))]
#[cfg(not(test))]
#[lang = "eh_personality"]
extern "C" fn eh_personality() {}
