extern crate alloc;

use alloc::boxed::Box;
use core::ffi::{c_char, c_int, c_void};
use core::slice;

use sqlite::{Connection, ResultCode, Value};
use sqlite_nostd as sqlite;

use crate::operations::{
    clear_remove_ops, delete_bucket, delete_pending_buckets, insert_operation,
};
use crate::sync_local::sync_local;
use crate::vtab_util::*;

#[repr(C)]
struct VirtualTable {
    base: sqlite::vtab,
    db: *mut sqlite::sqlite3,

    target_applied: bool,
    target_validated: bool,
}

extern "C" fn connect(
    db: *mut sqlite::sqlite3,
    _aux: *mut c_void,
    _argc: c_int,
    _argv: *const *const c_char,
    vtab: *mut *mut sqlite::vtab,
    _err: *mut *mut c_char,
) -> c_int {
    if let Err(rc) =
        sqlite::declare_vtab(db, "CREATE TABLE powersync_operations(op TEXT, data TEXT);")
    {
        return rc as c_int;
    }

    unsafe {
        let tab = Box::into_raw(Box::new(VirtualTable {
            base: sqlite::vtab {
                nRef: 0,
                pModule: core::ptr::null(),
                zErrMsg: core::ptr::null_mut(),
            },
            db,
            target_validated: false,
            target_applied: false,
        }));
        *vtab = tab.cast::<sqlite::vtab>();
        let _ = sqlite::vtab_config(db, 0);
    }
    ResultCode::OK as c_int
}

extern "C" fn disconnect(vtab: *mut sqlite::vtab) -> c_int {
    unsafe {
        drop(Box::from_raw(vtab));
    }
    ResultCode::OK as c_int
}

extern "C" fn update(
    vtab: *mut sqlite::vtab,
    argc: c_int,
    argv: *mut *mut sqlite::value,
    p_row_id: *mut sqlite::int64,
) -> c_int {
    let args = sqlite::args!(argc, argv);

    let rowid = args[0];

    return if args.len() == 1 {
        // DELETE
        ResultCode::MISUSE as c_int
    } else if rowid.value_type() == sqlite::ColumnType::Null {
        // INSERT
        let op = args[2].text();

        let tab = unsafe { &mut *vtab.cast::<VirtualTable>() };
        let db = tab.db;

        if op == "save" {
            let result = insert_operation(db, args[3].text());
            vtab_result(vtab, result)
        } else if op == "sync_local" {
            let result = sync_local(db, args[3]);
            if let Ok(result_row) = result {
                unsafe {
                    *p_row_id = result_row;
                }
            }
            vtab_result(vtab, result)
        } else if op == "clear_remove_ops" {
            let result = clear_remove_ops(db, args[3].text());
            vtab_result(vtab, result)
        } else if op == "delete_pending_buckets" {
            let result = delete_pending_buckets(db, args[3].text());
            vtab_result(vtab, result)
        } else if op == "delete_bucket" {
            let result = delete_bucket(db, args[3].text());
            vtab_result(vtab, result)
        } else {
            ResultCode::MISUSE as c_int
        }
    } else {
        // UPDATE - not supported
        ResultCode::MISUSE as c_int
    } as c_int;
}

// Insert-only virtual table.
// The primary functionality here is in update.
// connect and disconnect configures the table and allocates the required resources.
static MODULE: sqlite_nostd::module = sqlite_nostd::module {
    iVersion: 0,
    xCreate: None,
    xConnect: Some(connect),
    xBestIndex: Some(vtab_no_best_index),
    xDisconnect: Some(disconnect),
    xDestroy: None,
    xOpen: Some(vtab_no_open),
    xClose: Some(vtab_no_close),
    xFilter: Some(vtab_no_filter),
    xNext: Some(vtab_no_next),
    xEof: Some(vtab_no_eof),
    xColumn: Some(vtab_no_column),
    xRowid: Some(vtab_no_rowid),
    xUpdate: Some(update),
    xBegin: None,
    xSync: None,
    xCommit: None,
    xRollback: None,
    xFindFunction: None,
    xRename: None,
    xSavepoint: None,
    xRelease: None,
    xRollbackTo: None,
    xShadowName: None,
};

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    db.create_module_v2("powersync_operations", &MODULE, None, None)?;

    Ok(())
}
