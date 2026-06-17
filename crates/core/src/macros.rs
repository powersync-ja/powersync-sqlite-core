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
                PowerSyncError::from(err).apply_to_ctx($description, ctx);
            } else if let Ok(r) = result {
                ctx.result_text_transient(&r);
            }
        }
    };
}

#[macro_export]
macro_rules! create_sqlite_optional_text_fn {
    ($fn_name:ident, $fn_impl_name:ident, $description:literal) => {
        extern "C" fn $fn_name(
            ctx: *mut sqlite::context,
            argc: c_int,
            argv: *mut *mut sqlite::value,
        ) {
            let args = sqlite::args!(argc, argv);

            let result = $fn_impl_name(ctx, args);

            if let Err(err) = result {
                PowerSyncError::from(err).apply_to_ctx($description, ctx);
            } else if let Ok(r) = result {
                if let Some(s) = r {
                    ctx.result_text_transient(&s);
                } else {
                    ctx.result_null();
                }
            }
        }
    };
}
