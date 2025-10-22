use core::{
    ffi::{CStr, c_char, c_int, c_void},
    ptr::null_mut,
    sync::atomic::{AtomicBool, Ordering},
};

use alloc::{boxed::Box, rc::Rc};
use sqlite_nostd::{
    self as sqlite, Connection, Context, ResultCode, Value, bindings::SQLITE_RESULT_SUBTYPE,
};

use crate::{constants::SUBTYPE_JSON, error::PowerSyncError, state::DatabaseState};

/// The `powersync_update_hooks` methods works like this:
///
///   1. `powersync_update_hooks('install')` installs update hooks on the database, failing if
///      another hook already exists.
///   2. `powersync_update_hooks('get')` returns a JSON array of table names that have been changed
///      and comitted since the last `powersync_update_hooks` call.
///
/// The update hooks don't have to be uninstalled manually, that happens when the connection is
/// closed and the function is unregistered.
pub fn register(db: *mut sqlite::sqlite3, state: Rc<DatabaseState>) -> Result<(), ResultCode> {
    let state = Box::new(HookState {
        has_registered_hooks: AtomicBool::new(false),
        db,
        state,
    });

    db.create_function_v2(
        "powersync_update_hooks",
        1,
        sqlite::UTF8 | sqlite::DETERMINISTIC | SQLITE_RESULT_SUBTYPE,
        Some(Box::into_raw(state) as *mut c_void),
        Some(powersync_update_hooks),
        None,
        None,
        Some(destroy_function),
    )?;
    Ok(())
}

struct HookState {
    has_registered_hooks: AtomicBool,
    db: *mut sqlite::sqlite3,
    state: Rc<DatabaseState>,
}

extern "C" fn destroy_function(ctx: *mut c_void) {
    let state = unsafe { Box::from_raw(ctx as *mut HookState) };

    if state.has_registered_hooks.load(Ordering::Relaxed) {
        check_previous(
            "update",
            &state.state,
            state.db.update_hook(None, null_mut()),
        );
        check_previous(
            "commit",
            &state.state,
            state.db.commit_hook(None, null_mut()),
        );
        check_previous(
            "rollback",
            &state.state,
            state.db.rollback_hook(None, null_mut()),
        );
    }
}

extern "C" fn powersync_update_hooks(
    ctx: *mut sqlite::context,
    argc: c_int,
    argv: *mut *mut sqlite::value,
) {
    let args = sqlite::args!(argc, argv);
    let op = args[0].text();
    let db = ctx.db_handle();
    let user_data = ctx.user_data() as *const HookState;

    match op {
        "install" => {
            let state = unsafe { user_data.as_ref().unwrap_unchecked() };
            let db_state = &state.state;

            check_previous(
                "update",
                db_state,
                db.update_hook(
                    Some(update_hook_impl),
                    Rc::into_raw(db_state.clone()) as *mut c_void,
                ),
            );
            check_previous(
                "commit",
                db_state,
                db.commit_hook(
                    Some(commit_hook_impl),
                    Rc::into_raw(db_state.clone()) as *mut c_void,
                ),
            );
            check_previous(
                "rollback",
                db_state,
                db.rollback_hook(
                    Some(rollback_hook_impl),
                    Rc::into_raw(db_state.clone()) as *mut c_void,
                ),
            );
            state.has_registered_hooks.store(true, Ordering::Relaxed);
        }
        "get" => {
            let state = unsafe { user_data.as_ref().unwrap_unchecked() };
            let formatted = serde_json::to_string(&state.state.take_updates())
                .map_err(PowerSyncError::internal);
            match formatted {
                Ok(result) => {
                    ctx.result_text_transient(&result);
                    ctx.result_subtype(SUBTYPE_JSON);
                }
                Err(e) => e.apply_to_ctx("powersync_update_hooks", ctx),
            }
        }
        _ => {
            ctx.result_error("Unknown operation");
            ctx.result_error_code(ResultCode::MISUSE);
        }
    };
}

unsafe extern "C" fn update_hook_impl(
    ctx: *mut c_void,
    _kind: c_int,
    _db: *const c_char,
    table: *const c_char,
    _rowid: i64,
) {
    let state = unsafe { (ctx as *const DatabaseState).as_ref().unwrap_unchecked() };
    let table = unsafe { CStr::from_ptr(table) };
    let Ok(table) = table.to_str() else {
        return;
    };

    state.track_update(table);
}

unsafe extern "C" fn commit_hook_impl(ctx: *mut c_void) -> c_int {
    let state = unsafe { (ctx as *const DatabaseState).as_ref().unwrap_unchecked() };
    state.track_commit();
    return 0; // Allow commit to continue normally
}

unsafe extern "C" fn rollback_hook_impl(ctx: *mut c_void) {
    let state = unsafe { (ctx as *const DatabaseState).as_ref().unwrap_unchecked() };
    state.track_rollback();
}

fn check_previous(desc: &'static str, expected: &Rc<DatabaseState>, previous: *const c_void) {
    let expected = Rc::as_ptr(expected);

    assert!(
        previous.is_null() || previous == expected.cast(),
        "Previous call to {desc} hook outside of PowerSync: Expected {expected:p}, installed was {previous:p}",
    );
    if !previous.is_null() {
        // The hook callbacks own an Arc<DatabaseState> that needs to be dropped now.
        unsafe {
            Rc::decrement_strong_count(previous);
        }
    }
}
