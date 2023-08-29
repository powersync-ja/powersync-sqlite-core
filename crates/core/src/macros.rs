
#[macro_export]
macro_rules! create_sqlite_text_fn {
    ($fn_name:ident, $fn_impl_name:ident, $description:literal) => {
        extern "C" fn $fn_name(
            ctx: *mut sqlite::context,
            argc: c_int,
            argv: *mut *mut sqlite::value,
        ) {
            let args = sqlite::args!(argc, argv);

            let result = $fn_impl_name(ctx, args);

            if let Err(err) = result {
                let SQLiteError(code, message) = SQLiteError::from(err);
                if message.is_some() {
                    ctx.result_error(&format!("{:} {:}", $description, message.unwrap()));
                } else {
                    let error = ctx.db_handle().errmsg().unwrap();
                    if error == "not an error" {
                        ctx.result_error(&format!("{:}", $description));
                    } else {
                        ctx.result_error(&format!("{:} {:}", $description, error));
                    }
                }
                ctx.result_error_code(code);
            } else if let Ok(r) = result {
                ctx.result_text_transient(&r);
            }
        }
    };
}


// Wrap a function in an auto-transaction.
// Gives the equivalent of SQLite's auto-commit behaviour, except that applies to all statements
// inside the function. Otherwise, each statement inside the function would be a transaction on its
// own if the function itself is not wrapped in a transaction.
#[macro_export]
macro_rules! create_auto_tx_function {
    ($fn_name:ident, $fn_impl_name:ident) => {
        fn $fn_name(
            ctx: *mut sqlite::context,
            args: &[*mut sqlite::value],
        ) -> Result<String, SQLiteError> {
            let db = ctx.db_handle();

            // Auto-start a transaction if we're not in a transaction
            let started_tx = if db.get_autocommit() {
                db.exec_safe("BEGIN")?;
                true
            } else {
                false
            };

            let result = $fn_impl_name(ctx, args);
            if result.is_err() {
                // Always ROLLBACK, even when we didn't start the transaction.
                // Otherwise the user may be able to continue the transaction and end up in an inconsistent state.
                // We ignore rollback errors.
                if !db.get_autocommit() {
                    let _ignore = db.exec_safe("ROLLBACK");
                }
            } else if started_tx {
                // Only COMMIT our own transactions.
                db.exec_safe("COMMIT")?;
            }

            result
        }
    };
}
