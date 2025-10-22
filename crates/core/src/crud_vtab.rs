extern crate alloc;

use alloc::boxed::Box;
use alloc::rc::Rc;
use const_format::formatcp;
use core::ffi::{CStr, c_char, c_int, c_void};
use serde::Serialize;
use serde_json::value::RawValue;

use sqlite::{Connection, ResultCode, Value};
use sqlite_nostd::ManagedStmt;
use sqlite_nostd::{self as sqlite, ColumnType};

use crate::error::PowerSyncError;
use crate::ext::SafeManagedStmt;
use crate::schema::TableInfoFlags;
use crate::state::DatabaseState;
use crate::util::MAX_OP_ID;
use crate::vtab_util::*;

const MANUAL_NAME: &CStr = c"powersync_crud_";
const SIMPLE_NAME: &CStr = c"powersync_crud";

// Structure:
//   CREATE TABLE powersync_crud_(data TEXT, options INT HIDDEN);
//   CREATE TABLE powersync_crud(op TEXT, id TEXT, type TEXT, data TEXT, old_values TEXT, metadata TEXT, options INT HIDDEN);
//
// This is a insert-only virtual table. It generates transaction ids in ps_tx, and inserts data in
// ps_crud(tx_id, data).
// The second form (without the trailing underscore) takes the data to insert as individual
// components and constructs the data to insert into `ps_crud` internally. It will also update
// `ps_updated_rows` and the `$local` bucket.
//
// Using a virtual table like this allows us to hook into xBegin, xCommit and xRollback to automatically
// increment transaction ids. These are only called when powersync_crud_ is used as part of a transaction,
// meaning there is no transaction increment and no overhead when using local-only tables.

#[repr(C)]
struct VirtualTable {
    base: sqlite::vtab,
    db: *mut sqlite::sqlite3,
    current_tx: Option<ActiveCrudTransaction>,
    is_simple: bool,
    state: Rc<DatabaseState>,
}

struct ActiveCrudTransaction {
    tx_id: i64,
    mode: CrudTransactionMode,
}

enum CrudTransactionMode {
    Manual(ManualCrudTransactionMode),
    Simple(SimpleCrudTransactionMode),
}

#[derive(Default)]
struct ManualCrudTransactionMode {
    stmt: Option<ManagedStmt>,
}

#[derive(Default)]
struct SimpleCrudTransactionMode {
    stmt: Option<ManagedStmt>,
    set_updated_rows: Option<ManagedStmt>,
    had_writes: bool,
}

impl VirtualTable {
    fn value_to_json<'a>(value: &'a *mut sqlite::value) -> Option<&'a RawValue> {
        match value.value_type() {
            ColumnType::Text => {
                Some(unsafe {
                    // Safety: RawValue is a transparent type wrapping a str. We assume that it
                    // contains valid JSON.
                    core::mem::transmute::<&'a str, &'a RawValue>(value.text())
                })
            }
            _ => None,
        }
    }

    fn handle_insert(&mut self, args: &[*mut sqlite::value]) -> Result<(), PowerSyncError> {
        let current_tx = self
            .current_tx
            .as_mut()
            .ok_or_else(|| PowerSyncError::state_error("Not in tx"))?;
        let db = self.db;

        if self.state.is_in_sync_local.get() {
            // Don't collect CRUD writes while we're syncing the local database - writes made here
            // aren't writes we should upload.
            // This normally doesn't happen because we insert directly into the data tables, but
            // users might have custom raw tables used for sycing with triggers on them to call
            // this function. And those specifically should not trigger here.
            return Ok(());
        }

        match &mut current_tx.mode {
            CrudTransactionMode::Manual(manual) => {
                // Columns are (data TEXT, options INT HIDDEN)
                let data = args[0].text();
                let flags = match args[1].value_type() {
                    sqlite_nostd::ColumnType::Null => TableInfoFlags::default(),
                    _ => TableInfoFlags(args[1].int() as u32),
                };

                let stmt = manual.raw_crud_statement(db)?;
                stmt.bind_int64(1, current_tx.tx_id)?;
                stmt.bind_text(2, data, sqlite::Destructor::STATIC)?;
                stmt.bind_int(3, flags.0 as i32)?;
                stmt.exec()?;
            }
            CrudTransactionMode::Simple(simple) => {
                // Columns are (op TEXT, id TEXT, type TEXT, data TEXT, old_values TEXT, metadata TEXT, options INT HIDDEN)
                let flags = match args[6].value_type() {
                    sqlite_nostd::ColumnType::Null => TableInfoFlags::default(),
                    _ => TableInfoFlags(args[6].int() as u32),
                };
                let op = args[0].text();
                let id = args[1].text();
                let row_type = args[2].text();
                let metadata = args[5];
                let data = Self::value_to_json(&args[3]);

                if flags.ignore_empty_update()
                    && op == "PATCH"
                    && data.map(|r| r.get()) == Some("{}")
                {
                    // Ignore this empty update
                    return Ok(());
                }

                #[derive(Serialize)]
                struct CrudEntry<'a> {
                    op: &'a str,
                    id: &'a str,
                    #[serde(rename = "type")]
                    row_type: &'a str,
                    #[serde(skip_serializing_if = "Option::is_none")]
                    data: Option<&'a RawValue>,
                    #[serde(skip_serializing_if = "Option::is_none")]
                    old: Option<&'a RawValue>,
                    #[serde(skip_serializing_if = "Option::is_none")]
                    metadata: Option<&'a str>,
                }

                // First, we insert into ps_crud like the manual vtab would too. We have to create
                // the JSON out of the individual components for that.
                let stmt = simple.raw_crud_statement(db)?;
                stmt.bind_int64(1, current_tx.tx_id)?;

                let serialized = serde_json::to_string(&CrudEntry {
                    op,
                    id,
                    row_type,
                    data: data,
                    old: Self::value_to_json(&args[4]),
                    metadata: if metadata.value_type() == ColumnType::Text {
                        Some(metadata.text())
                    } else {
                        None
                    },
                })
                .map_err(PowerSyncError::internal)?;
                stmt.bind_text(2, &serialized, sqlite::Destructor::STATIC)?;
                stmt.exec()?;

                // However, we also set ps_updated_rows and update the $local bucket
                let set_updated_rows = simple.set_updated_rows_statement(db)?;
                set_updated_rows.bind_text(1, row_type, sqlite::Destructor::STATIC)?;
                set_updated_rows.bind_text(2, id, sqlite::Destructor::STATIC)?;
                set_updated_rows.exec()?;
                simple.record_local_write(db)?;
            }
        }

        Ok(())
    }

    fn begin(&mut self) -> Result<(), PowerSyncError> {
        let db = self.db;

        // language=SQLite
        let statement =
            db.prepare_v2("UPDATE ps_tx SET next_tx = next_tx + 1 WHERE id = 1 RETURNING next_tx")?;
        let tx_id = if statement.step()? == ResultCode::ROW {
            statement.column_int64(0) - 1
        } else {
            return Err(PowerSyncError::unknown_internal());
        };

        self.current_tx = Some(ActiveCrudTransaction {
            tx_id,
            mode: if self.is_simple {
                CrudTransactionMode::Simple(Default::default())
            } else {
                CrudTransactionMode::Manual(Default::default())
            },
        });

        Ok(())
    }

    fn end_transaction(&mut self) {
        self.current_tx = None;
    }
}

impl ManualCrudTransactionMode {
    fn raw_crud_statement(&mut self, db: *mut sqlite::sqlite3) -> Result<&ManagedStmt, ResultCode> {
        prepare_lazy(&mut self.stmt, || {
            const SQL: &str = formatcp!(
                "\
WITH insertion (tx_id, data) AS (VALUES (?1, ?2))
INSERT INTO ps_crud(tx_id, data)
SELECT * FROM insertion WHERE (NOT (?3 & {})) OR data->>'op' != 'PATCH' OR data->'data' != '{{}}';
    ",
                TableInfoFlags::IGNORE_EMPTY_UPDATE
            );

            db.prepare_v3(SQL, 0)
        })
    }
}

impl SimpleCrudTransactionMode {
    fn raw_crud_statement(&mut self, db: *mut sqlite::sqlite3) -> Result<&ManagedStmt, ResultCode> {
        prepare_lazy(&mut self.stmt, || {
            // language=SQLite
            db.prepare_v3("INSERT INTO ps_crud(tx_id, data) VALUES (?, ?)", 0)
        })
    }

    fn set_updated_rows_statement(
        &mut self,
        db: *mut sqlite::sqlite3,
    ) -> Result<&ManagedStmt, ResultCode> {
        prepare_lazy(&mut self.set_updated_rows, || {
            // language=SQLite
            db.prepare_v3(
                "INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id) VALUES(?, ?)",
                0,
            )
        })
    }

    fn record_local_write(&mut self, db: *mut sqlite::sqlite3) -> Result<(), ResultCode> {
        if !self.had_writes {
            db.exec_safe(formatcp!("INSERT OR REPLACE INTO ps_buckets(name, last_op, target_op) VALUES('$local', 0, {MAX_OP_ID})"))?;
            self.had_writes = true;
        }

        Ok(())
    }
}

/// A variant of `Option.get_or_insert` that handles insertions returning errors.
fn prepare_lazy(
    stmt: &mut Option<ManagedStmt>,
    prepare: impl FnOnce() -> Result<ManagedStmt, ResultCode>,
) -> Result<&ManagedStmt, ResultCode> {
    if let None = stmt {
        *stmt = Some(prepare()?);
    }

    return Ok(unsafe { stmt.as_ref().unwrap_unchecked() });
}

extern "C" fn connect(
    db: *mut sqlite::sqlite3,
    aux: *mut c_void,
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
        "CREATE TABLE powersync_crud(op TEXT, id TEXT, type TEXT, data TEXT, old_values TEXT, metadata TEXT, options INT HIDDEN);"
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
            state: DatabaseState::clone_from(aux),
            db,
            current_tx: None,
            is_simple,
        }));
        *vtab = tab.cast::<sqlite::vtab>();
        let _ = sqlite::vtab_config(db, 0);
    }
    ResultCode::OK as c_int
}

extern "C" fn disconnect(vtab: *mut sqlite::vtab) -> c_int {
    unsafe {
        drop(Box::from_raw(vtab as *mut VirtualTable));
    }
    ResultCode::OK as c_int
}

extern "C" fn begin(vtab: *mut sqlite::vtab) -> c_int {
    let tab = unsafe { &mut *(vtab.cast::<VirtualTable>()) };
    let result = tab.begin();
    vtab_result(vtab, result)
}

extern "C" fn commit(vtab: *mut sqlite::vtab) -> c_int {
    let tab = unsafe { &mut *(vtab.cast::<VirtualTable>()) };
    tab.end_transaction();
    ResultCode::OK as c_int
}

extern "C" fn rollback(vtab: *mut sqlite::vtab) -> c_int {
    let tab = unsafe { &mut *(vtab.cast::<VirtualTable>()) };
    tab.end_transaction();
    // ps_tx will be rolled back automatically
    ResultCode::OK as c_int
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
        let tab = unsafe { &mut *(vtab.cast::<VirtualTable>()) };
        let result = tab.handle_insert(&args[2..]);
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

pub fn register(db: *mut sqlite::sqlite3, state: Rc<DatabaseState>) -> Result<(), ResultCode> {
    sqlite::convert_rc(sqlite::create_module_v2(
        db,
        SIMPLE_NAME.as_ptr(),
        &MODULE,
        Rc::into_raw(state.clone()) as *mut c_void,
        Some(DatabaseState::destroy_rc),
    ))?;
    sqlite::convert_rc(sqlite::create_module_v2(
        db,
        MANUAL_NAME.as_ptr(),
        &MODULE,
        Rc::into_raw(state) as *mut c_void,
        Some(DatabaseState::destroy_rc),
    ))?;

    Ok(())
}
