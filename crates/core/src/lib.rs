#![no_std]
#![feature(vec_into_raw_parts)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(error_in_core)]
#![feature(assert_matches)]

extern crate alloc;

use core::ffi::{c_char, c_int};

use sqlite::ResultCode;
use sqlite_nostd as sqlite;

mod bucket_priority;
mod checkpoint;
mod crud_vtab;
mod diff;
mod error;
mod ext;
mod fix035;
mod json_merge;
mod kv;
mod macros;
mod migrations;
mod operations;
mod operations_vtab;
mod schema;
mod sync_local;
mod sync_types;
mod util;
mod uuid;
mod version;
mod view_admin;
mod views;
mod vtab_util;

#[no_mangle]
pub extern "C" fn sqlite3_powersync_init(
    db: *mut sqlite::sqlite3,
    _err_msg: *mut *mut c_char,
    api: *mut sqlite::api_routines,
) -> c_int {
    sqlite::EXTENSION_INIT2(api);

    let result = init_extension(db);

    return if let Err(code) = result {
        code as c_int
    } else {
        ResultCode::OK as c_int
    };
}

fn init_extension(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    crate::version::register(db)?;
    crate::views::register(db)?;
    crate::uuid::register(db)?;
    crate::diff::register(db)?;
    crate::json_merge::register(db)?;
    crate::view_admin::register(db)?;
    crate::checkpoint::register(db)?;
    crate::kv::register(db)?;

    crate::schema::register(db)?;
    crate::operations_vtab::register(db)?;
    crate::crud_vtab::register(db)?;

    Ok(())
}

extern "C" {
    #[cfg(feature = "static")]
    #[allow(non_snake_case)]
    pub fn sqlite3_auto_extension(
        xEntryPoint: Option<
            extern "C" fn(
                *mut sqlite::sqlite3,
                *mut *mut c_char,
                *mut sqlite::api_routines,
            ) -> c_int,
        >,
    ) -> ::core::ffi::c_int;
}

#[cfg(feature = "static")]
#[no_mangle]
pub extern "C" fn powersync_init_static() -> c_int {
    unsafe {
        let f = sqlite3_powersync_init;
        return sqlite3_auto_extension(Some(f));
    }
}
