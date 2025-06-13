
fn main() {
    let mut cfg = cc::Build::new();

    // Compile the SQLite source
    cfg.file("../sqlite/sqlite/sqlite3.c");
    cfg.file("../sqlite/sqlite/shell.c");
    cfg.include("../sqlite/sqlite");

    // General SQLite options
    cfg.define("SQLITE_THREADSAFE", Some("0"));
    cfg.define("SQLITE_ENABLE_BYTECODE_VTAB", Some("1"));

    // Call core_init() in main.rs
    cfg.define("SQLITE_EXTRA_INIT", Some("core_init"));

    // Compile with readline support (also requires -lreadline / cargo:rustc-link-lib=readline below)
    cfg.define("HAVE_READLINE", Some("1"));

    // Silence warnings generated for SQLite
    cfg.flag("-Wno-implicit-fallthrough");
    cfg.flag("-Wno-unused-parameter");
    cfg.flag("-Wno-null-pointer-subtraction");

    cfg.compile("sqlite-ps");

    println!("cargo:rustc-link-lib=readline");
}
