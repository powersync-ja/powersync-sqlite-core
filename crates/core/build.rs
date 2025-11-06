use std::process::Command;
fn main() {
    let mut git_hash = Command::new("git")
        .args(&["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .unwrap_or_default();

    if git_hash.is_empty() {
        // We can't compute the git hash for versions pushed to crates.io. That's fine, we'll use a
        // separate designator for that instead. The designator needs to be 8 chars in length since
        // that's the substring used in version numbers.
        git_hash = "cratesio".to_owned();
    }

    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
}
