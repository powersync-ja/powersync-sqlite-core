extern crate alloc;

use alloc::boxed::Box;
use alloc::string::String;
use const_format::formatcp;
use core::ffi::{c_char, c_int, c_void, CStr};
use core::ptr::null_mut;

use sqlite::{Connection, ResultCode, Value};
use sqlite_nostd as sqlite;
use sqlite_nostd::ManagedStmt;
use sqlite_nostd::ResultCode::NULL;

use crate::error::SQLiteError;
use crate::ext::SafeManagedStmt;
use crate::schema::TableInfoFlags;
use crate::vtab_util::*;

const MANUAL_NAME: &CStr = c"powersync_crud";
const SIMPLE_NAME: &CStr = c"powersync_crud";

// Structure:
//   CREATE TABLE powersync_crud_(data TEXT, options INT HIDDEN);
//
// This is a insert-only virtual table. It generates transaction ids in ps_tx, and inserts data in
// ps_crud(tx_id, data).
//
// Using a virtual table like this allows us to hook into xBegin, xCommit and xRollback to automatically
// increment transaction ids. These are only called when powersync_crud_ is used as part of a transaction,
// meaning there is no transaction increment and no overhead when using local-only tables.

#[repr(C)]
struct VirtualTable {
    base: sqlite::vtab,
    db: *mut sqlite::sqlite3,
    current_tx: Option<i64>,
    insert_statement: Option<ManagedStmt>,
}

extern "C" fn connect(
    db: *mut sqlite::sqlite3,
    _aux: *mut c_void,
    argc: c_int,
    argv: *const *const c_char,
    vtab: *mut *mut sqlite::vtab,
    _err: *mut *mut c_char,
) -> c_int {
    let args = sqlite::args!(argc, argv);
    let Some(name) = args.get(0) else {
        return ResultCode::MISUSE as c_int;
    };

    let name = unsafe { CStr::from_ptr(*name) };
    let is_simple = name == SIMPLE_NAME;

    let sql = if is_simple {
        "CREATE TABLE powersync_crud(op TEXT, id TEXT, data TEXT old_values TEXT, metadata TEXT);"
    } else {
        "CREATE TABLE powersync_crud_(data TEXT, options INT HIDDEN);"
    };

    if let Err(rc) = sqlite::declare_vtab(db, sql) {
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
            current_tx: None,
            insert_statement: None,
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

fn begin_impl(tab: &mut VirtualTable) -> Result<(), SQLiteError> {
    let db = tab.db;

    const SQL: &str = formatcp!(
        "\
WITH insertion (tx_id, data) AS (VALUES (?1, ?2))
INSERT INTO ps_crud(tx_id, data)
SELECT * FROM insertion WHERE (NOT (?3 & {})) OR data->>'op' != 'PATCH' OR data->'data' != '{{}}';
    ",
        TableInfoFlags::IGNORE_EMPTY_UPDATE
    );

    let insert_statement = db.prepare_v3(SQL, 0)?;
    tab.insert_statement = Some(insert_statement);

    // language=SQLite
    let statement =
        db.prepare_v2("UPDATE ps_tx SET next_tx = next_tx + 1 WHERE id = 1 RETURNING next_tx")?;
    if statement.step()? == ResultCode::ROW {
        let tx_id = statement.column_int64(0) - 1;
        tab.current_tx = Some(tx_id);
    } else {
        return Err(SQLiteError::from(ResultCode::ABORT));
    }

    Ok(())
}

extern "C" fn begin(vtab: *mut sqlite::vtab) -> c_int {
    let tab = unsafe { &mut *(vtab.cast::<VirtualTable>()) };
    let result = begin_impl(tab);
    vtab_result(vtab, result)
}

extern "C" fn commit(vtab: *mut sqlite::vtab) -> c_int {
    let tab = unsafe { &mut *(vtab.cast::<VirtualTable>()) };
    tab.current_tx = None;
    tab.insert_statement = None;
    ResultCode::OK as c_int
}

extern "C" fn rollback(vtab: *mut sqlite::vtab) -> c_int {
    let tab = unsafe { &mut *(vtab.cast::<VirtualTable>()) };
    tab.current_tx = None;
    tab.insert_statement = None;
    // ps_tx will be rolled back automatically
    ResultCode::OK as c_int
}

fn insert_operation(
    vtab: *mut sqlite::vtab,
    data: &str,
    flags: TableInfoFlags,
) -> Result<(), SQLiteError> {
    let tab = unsafe { &mut *(vtab.cast::<VirtualTable>()) };
    if tab.current_tx.is_none() {
        return Err(SQLiteError(
            ResultCode::MISUSE,
            Some(String::from("No tx_id")),
        ));
    }
    let current_tx = tab.current_tx.unwrap();
    // language=SQLite
    let statement = tab
        .insert_statement
        .as_ref()
        .ok_or(SQLiteError::from(NULL))?;
    statement.bind_int64(1, current_tx)?;
    statement.bind_text(2, data, sqlite::Destructor::STATIC)?;
    statement.bind_int(3, flags.0 as i32)?;
    statement.exec()?;

    Ok(())
}

extern "C" fn update(
    vtab: *mut sqlite::vtab,
    argc: c_int,
    argv: *mut *mut sqlite::value,
    _p_row_id: *mut sqlite::int64,
) -> c_int {
    let args = sqlite::args!(argc, argv);

    let rowid = args[0];

    return if args.len() == 1 {
        // DELETE
        ResultCode::MISUSE as c_int
    } else if rowid.value_type() == sqlite::ColumnType::Null {
        // INSERT
        let data = args[2].text();
        let flags = match args[3].value_type() {
            sqlite_nostd::ColumnType::Null => TableInfoFlags::default(),
            _ => TableInfoFlags(args[3].int() as u32),
        };
        let result = insert_operation(vtab, data, flags);
        vtab_result(vtab, result)
    } else {
        // UPDATE - not supported
        ResultCode::MISUSE as c_int
    } as c_int;
}

// Insert-only virtual table.
// The primary functionality here is in begin, update, commit and rollback.
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
    xBegin: Some(begin),
    xSync: None,
    xCommit: Some(commit),
    xRollback: Some(rollback),
    xFindFunction: None,
    xRename: None,
    xSavepoint: None,
    xRelease: None,
    xRollbackTo: None,
    xShadowName: None,
    xIntegrity: None,
};

pub fn register(db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
    sqlite::convert_rc(sqlite::create_module_v2(
        db,
        SIMPLE_NAME.as_ptr(),
        &MODULE,
        null_mut(),
        None,
    ))?;
    sqlite::convert_rc(sqlite::create_module_v2(
        db,
        MANUAL_NAME.as_ptr(),
        &MODULE,
        null_mut(),
        None,
    ))?;

    Ok(())
}
