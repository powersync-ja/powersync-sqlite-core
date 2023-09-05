#![no_main]
#![no_std]
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


#[no_mangle]
pub extern "C" fn core_init(_dummy: *mut c_char) -> c_int {
    powersync_init_static()
}
