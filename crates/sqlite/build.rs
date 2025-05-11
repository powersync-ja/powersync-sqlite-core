fn main() {
    let mut cfg = cc::Build::new();
    let target = std::env::var("TARGET").unwrap();
    let is_watchos = target.contains("watchos") || target.contains("watchsimulator");

    // Compile the SQLite source
    cfg.file("./sqlite/sqlite3.c");
    cfg.include("./sqlite");

    // General SQLite options
    cfg.define("SQLITE_THREADSAFE", Some("0"));
    cfg.define("SQLITE_ENABLE_BYTECODE_VTAB", Some("1"));

    if is_watchos {
        // For watchOS, don't build the shell and disable readline
        cfg.define("HAVE_READLINE", Some("0"));
        cfg.define("HAVE_EDITLINE", Some("0"));
        cfg.define("SQLITE_OMIT_SYSTEM", Some("1"));
    } else {
        // For other platforms, build the shell with readline
        cfg.file("./sqlite/shell.c");
        cfg.define("HAVE_READLINE", Some("1"));
        println!("cargo:rustc-link-lib=readline");
    }

    // Silence warnings generated for SQLite
    cfg.flag("-Wno-implicit-fallthrough");
    cfg.flag("-Wno-unused-parameter");
    cfg.flag("-Wno-null-pointer-subtraction");

    cfg.compile("sqlite");
}
