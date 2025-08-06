#![no_std]
#![feature(vec_into_raw_parts)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(assert_matches)]
#![feature(strict_overflow_ops)]

extern crate alloc;

use core::ffi::{c_char, c_int};

use alloc::{ffi::CString, format, sync::Arc};
use sqlite::ResultCode;
use sqlite_nostd as sqlite;

use crate::{error::PowerSyncError, state::DatabaseState};

mod bson;
mod checkpoint;
mod constants;
mod crud_vtab;
mod diff;
mod error;
mod ext;
mod fix_data;
mod json_merge;
mod kv;
mod macros;
mod migrations;
mod operations;
mod operations_vtab;
mod schema;
mod state;
mod sync;
mod sync_local;
mod util;
mod uuid;
mod version;
mod view_admin;
mod views;
mod vtab_util;

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_powersync_init(
    db: *mut sqlite::sqlite3,
    err_msg: *mut *mut c_char,
    api: *mut sqlite::api_routines,
) -> c_int {
    debug_assert!(unsafe { *err_msg }.is_null());
    sqlite::EXTENSION_INIT2(api);

    let result = init_extension(db);

    return if let Err(code) = result {
        if let Ok(desc) = CString::new(format!("Could not initialize PowerSync: {}", code)) {
            // Note: This is fine since we're using sqlite3_malloc to allocate in Rust
            unsafe { *err_msg = desc.into_raw() as *mut c_char };
        }

        code.sqlite_error_code() as c_int
    } else {
        ResultCode::OK as c_int
    };
}

fn init_extension(db: *mut sqlite::sqlite3) -> Result<(), PowerSyncError> {
    PowerSyncError::check_sqlite3_version()?;

    let state = Arc::new(DatabaseState::new());

    crate::version::register(db)?;
    crate::views::register(db)?;
    crate::uuid::register(db)?;
    crate::diff::register(db)?;
    crate::fix_data::register(db)?;
    crate::json_merge::register(db)?;
    crate::view_admin::register(db)?;
    crate::checkpoint::register(db)?;
    crate::kv::register(db)?;
    crate::state::register(db, state.clone())?;
    sync::register(db, state.clone())?;

    crate::schema::register(db)?;
    crate::operations_vtab::register(db, state.clone())?;
    crate::crud_vtab::register(db, state)?;

    Ok(())
}

unsafe extern "C" {
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
