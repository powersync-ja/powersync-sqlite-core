extern crate bindgen;

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=deps/sqlite3ext.h");

    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("deps/sqlite3ext.h")
        .clang_arg("-fvisibility=default")
        // ^-- to get functions exposed in wasm
        // https://github.com/rust-lang/rust-bindgen/issues/751
        .use_core()
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
