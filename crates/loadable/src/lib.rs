#![no_std]
#![allow(internal_features)]
#![cfg_attr(feature = "nightly", feature(core_intrinsics))]

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
mod panic_handler {
    #[cfg(feature = "nightly")]
    #[panic_handler]
    fn panic(_info: &core::panic::PanicInfo) -> ! {
        core::intrinsics::abort()
    }

    #[cfg(not(feature = "nightly"))]
    #[panic_handler]
    fn panic(_info: &core::panic::PanicInfo) -> ! {
        loop {}
    }
}
