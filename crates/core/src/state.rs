use core::{
    ffi::{c_int, c_void},
    sync::atomic::{AtomicBool, Ordering},
};

use alloc::sync::Arc;
use sqlite::{Connection, ResultCode};
use sqlite_nostd::{self as sqlite, Context};

/// State that is shared for a SQLite database connection after the core extension has been
/// registered on it.
///
/// `init_extension` allocates an instance of this in an `Arc` that is shared as user-data for
/// functions/vtabs that need access to it.
pub struct DatabaseState {
    pub is_in_sync_local: AtomicBool,
}

impl DatabaseState {
    pub fn new() -> Self {
        DatabaseState {
            is_in_sync_local: AtomicBool::new(false),
        }
    }

    pub fn sync_local_guard<'a>(&'a self) -> impl Drop + use<'a> {
        self.is_in_sync_local
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Acquire)
            .expect("should not be syncing already");

        struct ClearOnDrop<'a>(&'a DatabaseState);

        impl Drop for ClearOnDrop<'_> {
            fn drop(&mut self) {
                self.0.is_in_sync_local.store(false, Ordering::Release);
            }
        }

        ClearOnDrop(self)
    }

    pub unsafe extern "C" fn destroy_arc(ptr: *mut c_void) {
        drop(unsafe { Arc::from_raw(ptr.cast::<DatabaseState>()) });
    }
}

pub fn register(db: *mut sqlite::sqlite3, state: Arc<DatabaseState>) -> Result<(), ResultCode> {
    unsafe extern "C" fn func(
        ctx: *mut sqlite::context,
        _argc: c_int,
        _argv: *mut *mut sqlite::value,
    ) {
        let data = ctx.user_data().cast::<DatabaseState>();
        let data = unsafe { data.as_ref() }.unwrap();

        ctx.result_int(if data.is_in_sync_local.load(Ordering::Relaxed) {
            1
        } else {
            0
        });
    }

    db.create_function_v2(
        "powersync_in_sync_operation",
        0,
        0,
        Some(Arc::into_raw(state) as *mut c_void),
        Some(func),
        None,
        None,
        Some(DatabaseState::destroy_arc),
    )?;
    Ok(())
}
