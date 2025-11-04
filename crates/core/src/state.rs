use core::{
    cell::{Cell, Ref, RefCell},
    ffi::{c_int, c_void},
};

use alloc::{
    collections::btree_set::BTreeSet,
    rc::Rc,
    string::{String, ToString},
};
use powersync_sqlite_nostd::{self as sqlite, Context};
use sqlite::{Connection, ResultCode};

use crate::schema::Schema;

/// State that is shared for a SQLite database connection after the core extension has been
/// registered on it.
///
/// `init_extension` allocates an instance of this in an `Arc` that is shared as user-data for
/// functions/vtabs that need access to it.
#[derive(Default)]
pub struct DatabaseState {
    pub is_in_sync_local: Cell<bool>,
    schema: RefCell<Option<Schema>>,
    pending_updates: RefCell<BTreeSet<String>>,
    commited_updates: RefCell<BTreeSet<String>>,
}

impl DatabaseState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn view_schema(&'_ self) -> Option<Ref<'_, Schema>> {
        let schema_ref = self.schema.borrow();
        if schema_ref.is_none() {
            None
        } else {
            Some(Ref::map(schema_ref, |f| f.as_ref().unwrap()))
        }
    }

    /// Marks the given [Schema] as being the one currently installed to the database.
    pub fn set_schema(&self, schema: Schema) {
        self.schema.replace(Some(schema));
    }

    pub fn sync_local_guard<'a>(&'a self) -> impl Drop + use<'a> {
        if self.is_in_sync_local.replace(true) {
            panic!("Should ont be syncing already");
        }

        struct ClearOnDrop<'a>(&'a DatabaseState);

        impl Drop for ClearOnDrop<'_> {
            fn drop(&mut self) {
                self.0.is_in_sync_local.set(false);
            }
        }

        ClearOnDrop(self)
    }

    pub fn track_update(&self, tbl: &str) {
        let mut set = self.pending_updates.borrow_mut();
        // TODO: Use set.get_or_insert_with(tbl, str::to_string) after btree_set_entry is stable,
        // https://github.com/rust-lang/rust/issues/133549
        if !set.contains(tbl) {
            // Check whether the set contains the entry first to avoid an unconditional allocation.
            set.insert(tbl.to_string());
        }
    }

    pub fn track_rollback(&self) {
        self.pending_updates.borrow_mut().clear();
    }

    pub fn track_commit(&self) {
        let mut commited = self.commited_updates.borrow_mut();
        let mut pending = self.pending_updates.borrow_mut();
        let pending = core::mem::replace(&mut *pending, Default::default());

        for pending in pending.into_iter() {
            commited.insert(pending);
        }
    }

    pub fn take_updates(&self) -> BTreeSet<String> {
        let mut committed = self.commited_updates.borrow_mut();
        core::mem::replace(&mut *committed, Default::default())
    }

    /// ## Safety
    ///
    /// This is only safe to call when an `Rc<DatabaseState>` has been installed as the `user_data`
    /// pointer when registering the function.
    pub unsafe fn from_context(context: &impl Context) -> &Self {
        let user_data = context.user_data().cast::<DatabaseState>();
        unsafe {
            // Safety: user_data() points to valid DatabaseState reference alive as long as the
            // context.
            &*user_data
        }
    }

    /// ## Safety
    ///
    /// This is only save to call if `context` is the user-data pointer of a function or virtual
    /// table created with an `Rc<DatabaesState`, and only from within a call where that pointer is
    /// guaranteed to still be valid.
    pub unsafe fn clone_from(context: *const c_void) -> Rc<Self> {
        let context = context as *mut DatabaseState;

        unsafe {
            // Safety: It's a valid pointer that has at least one reference (owned by SQLite while
            // the function is registered).
            Rc::increment_strong_count(context);
            // Safety: Moves the clone we've just created into Rust.
            Rc::from_raw(context)
        }
    }

    pub unsafe extern "C" fn destroy_rc(ptr: *mut c_void) {
        drop(unsafe { Rc::from_raw(ptr.cast::<DatabaseState>()) });
    }
}

pub fn register(db: *mut sqlite::sqlite3, state: Rc<DatabaseState>) -> Result<(), ResultCode> {
    unsafe extern "C" fn func(
        ctx: *mut sqlite::context,
        _argc: c_int,
        _argv: *mut *mut sqlite::value,
    ) {
        let data = unsafe { DatabaseState::from_context(&ctx) };

        ctx.result_int(if data.is_in_sync_local.get() { 1 } else { 0 });
    }

    db.create_function_v2(
        "powersync_in_sync_operation",
        0,
        0,
        Some(Rc::into_raw(state) as *mut c_void),
        Some(func),
        None,
        None,
        Some(DatabaseState::destroy_rc),
    )?;
    Ok(())
}
