extern crate alloc;

use alloc::boxed::Box;
use alloc::rc::Rc;
use core::ffi::{c_char, c_int, c_void};

use powersync_sqlite_nostd as sqlite;
use sqlite::{Connection, ResultCode};

use crate::state::DatabaseState;
use crate::vtab_util::*;

/// A virtual table hack to implement a "pre-close hook" for databases.
///
/// When `sqlite3_close` is called, SQLite invokes the disconnect callback of virtual tables
/// attached to the connection. This gives us an opportunity to call
/// [DatabaseState::release_resources], allowing us to close active sync clients and associated
/// prepared statements. Without this, our internal statements might lease resources.
#[repr(C)]
struct VirtualTable {
    base: sqlite::vtab,
    state: Rc<DatabaseState>,
}

extern "C" fn connect(
    db: *mut sqlite::sqlite3,
    aux: *mut c_void,
    _argc: c_int,
    _argv: *const *const c_char,
    vtab: *mut *mut sqlite::vtab,
    _err: *mut *mut c_char,
) -> c_int {
    if let Err(rc) = sqlite::declare_vtab(db, "CREATE TABLE powersync_internal_close(_ TEXT);") {
        return rc as c_int;
    }

    unsafe {
        let tab = Box::into_raw(Box::new(VirtualTable {
            base: sqlite::vtab {
                nRef: 0,
                pModule: core::ptr::null(),
                zErrMsg: core::ptr::null_mut(),
            },
            state: DatabaseState::clone_from(aux),
        }));
        *vtab = tab.cast::<sqlite::vtab>();
        let _ = sqlite::vtab_config(db, 0);
    }
    ResultCode::OK as c_int
}

extern "C" fn disconnect(vtab: *mut sqlite::vtab) -> c_int {
    // Assume ownership of vtab since xDisconnect is supposed to destroy the connection.
    let vtab = unsafe { Box::from_raw(vtab as *mut VirtualTable) };

    // This is an eponymous virtual table. It will only be disconnected when the database is closed.
    // So we can use this as a "pre-close" hook and ensure we clear prepared statements the core
    // extension might hold.
    vtab.state.release_resources();

    ResultCode::OK as c_int
}

extern "C" fn update(
    _vtab: *mut sqlite::vtab,
    _argc: c_int,
    _argv: *mut *mut sqlite::value,
    _p_row_id: *mut sqlite::int64,
) -> c_int {
    0
}

// Insert-only virtual table.
// The primary functionality here is in update.
// connect and disconnect configures the table and allocates the required resources.
static MODULE: sqlite::module = sqlite::module {
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
    xIntegrity: None,
};

pub fn register(db: *mut sqlite::sqlite3, state: Rc<DatabaseState>) -> Result<(), ResultCode> {
    db.create_module_v2(
        "powersync_internal_close",
        &MODULE,
        Some(Rc::into_raw(state) as *mut c_void),
        Some(DatabaseState::destroy_rc),
    )?;

    Ok(())
}
