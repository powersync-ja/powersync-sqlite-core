use core::alloc::{GlobalAlloc, Layout};

/// A [GlobalAlloc] implementation forwarding allocations to the
/// [memory allocation subsystem](https://sqlite.org/c3ref/free.html) in SQLite.
///
/// Using this allocator allows moving allocated Rust values to SQLite.
pub struct SQLite3Allocator {}

unsafe impl GlobalAlloc for SQLite3Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        crate::capi::malloc(layout.size())
    }

    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        crate::capi::free(ptr as *mut core::ffi::c_void);
    }

    unsafe fn realloc(&self, ptr: *mut u8, _layout: Layout, new_size: usize) -> *mut u8 {
        crate::capi::realloc(ptr.cast(), new_size)
    }
}
