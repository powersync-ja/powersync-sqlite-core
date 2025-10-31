#![no_std]
#![allow(internal_features)]
#![cfg_attr(feature = "nightly", feature(core_intrinsics))]
#![cfg_attr(feature = "nightly", feature(lang_items))]

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

    #[cfg(not(target_family = "wasm"))]
    #[cfg(feature = "nightly")]
    #[lang = "eh_personality"]
    extern "C" fn eh_personality() {}

    #[cfg(not(target_family = "wasm"))]
    #[cfg(not(feature = "nightly"))]
    #[unsafe(no_mangle)]
    extern "C" fn rust_eh_personality() {
        // This avoids missing _rust_eh_personality symbol errors.
        // This isn't used for any builds we distribute, but it's heplful to compile the library
        // with stable Rust, which we do for testing.
    }
}
