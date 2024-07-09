#![no_std]
#![feature(vec_into_raw_parts)]
#![feature(core_intrinsics)]
#![allow(internal_features)]
#![feature(lang_items)]
#![feature(error_in_core)]

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

#[cfg(target_family = "wasm")]
#[no_mangle]
pub fn __rust_alloc_error_handler(_: core::alloc::Layout) -> ! {
    core::intrinsics::abort()
}

#[cfg(target_family = "wasm")]
#[no_mangle]
static __rust_alloc_error_handler_should_panic: u8 = 0;

#[cfg(target_family = "wasm")]
#[no_mangle]
static _CLOCK_PROCESS_CPUTIME_ID: i32 = 1;

#[cfg(target_family = "wasm")]
#[no_mangle]
static _CLOCK_THREAD_CPUTIME_ID: i32 = 1;

// Not used, but must be defined in some cases. Most notably when using native sqlite3 and loading
// the extension.
// #[allow(non_upper_case_globals)]
// #[no_mangle]
// pub static mut _Unwind_Resume: *mut core::ffi::c_void = core::ptr::null_mut();

