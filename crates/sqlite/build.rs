
fn main() {
    let mut cfg = cc::Build::new();

    // Compile the SQLite source
    cfg.file("./sqlite/sqlite3.c");
    cfg.file("./sqlite/shell.c");
    cfg.include("./sqlite");

    // General SQLite options
    cfg.define("SQLITE_THREADSAFE", Some("0"));
    cfg.define("SQLITE_ENABLE_BYTECODE_VTAB", Some("1"));

    // Compile with readline support (also requires -lreadline / cargo:rustc-link-lib=readline below)
    cfg.define("HAVE_READLINE", Some("1"));

    // Silence warnings generated for SQLite
    cfg.flag("-Wno-implicit-fallthrough");
    cfg.flag("-Wno-unused-parameter");
    cfg.flag("-Wno-null-pointer-subtraction");

    cfg.compile("sqlite");

    println!("cargo:rustc-link-lib=readline");
}
