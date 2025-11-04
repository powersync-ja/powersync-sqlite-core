#![no_std]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

mod allocator;
mod capi;
mod nostd;

pub use allocator::SQLite3Allocator;
pub use nostd::*;
