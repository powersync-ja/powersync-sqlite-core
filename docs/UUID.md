### sqlite3 sqlite3_randomness

As part of the extension, we provide `uuid()` and `gen_random_uuid()` functions, which generate a UUIDv4. We want reasonable guarantees that these uuids are unguessable, so we need to use a [cryptographically secure pseudorandom number generator](https://en.wikipedia.org/wiki/Cryptographically_secure_pseudorandom_number_generator) (CSPRNG). Additionally, we want this to be fast (generating uuids shouldn't be a bottleneck for inserting rows).

We provide two options:

1. The `getrandom` crate, via the `getrandom` feature (enabled by default).
2. `sqlite3_randomness`, when `getradnom` feature is not enabled.

Everywhere it's available, `getrandom` is recommended. One exception is WASI, where `sqlite3_randomness` is simpler. In that case, make sure the VFS xRandomness function provides secure random values.

## Details

SQLite has a sqlite3_randomness function, that does:

1. Seed using the VFS xRandomness function.
2. Generate a sequence using ChaCha20 (CSPRNG, as long as the seed is sufficiently random).

The VFS implementations differ:

1. For unix (Linux, macOS, Android and iOS), the default VFS uses `/dev/urandom` for the xRandomness seed above. This is generally secure.
2. For Windows, the default VFS uses a some system state such as pid, current time, current tick count for the entropy. This is okay for generating random numbers, but not quite secure (may be possible to guess).
3. For wa-sqlite, it defaults to the unix VFS. Emscripten intercepts the `/dev/urandom` call and provides values using `crypto.getRandomValues()`: https://github.com/emscripten-core/emscripten/blob/2a00e26013b0a02411af09352c6731b89023f382/src/library.js#L2151-L2163
4. Dart sqlite3 WASM uses `Random.secure()` to provide values. This is relatively slow, but only for the seed, so that's fine. This translates to `crypto.getRandomValues()` in JS.

### getrandom crate

The Rust uuid crate uses the getrandom crate for the "v4" feature by default.

Full platform support is listed here: https://docs.rs/getrandom/latest/getrandom/

Summary:

- Linux, Android: getrandom system call if available, falling batch to `/dev/urandom`.
- Windows: [BCryptGenRandom](https://docs.microsoft.com/en-us/windows/win32/api/bcrypt/nf-bcrypt-bcryptgenrandom)
- macOS: [getentropy](https://www.unix.com/man-page/mojave/2/getentropy/)
- iOS: [CCRandomGenerateBytes](https://opensource.apple.com/source/CommonCrypto/CommonCrypto-60074/include/CommonRandom.h.auto.html)
- WASI (used for Dart sqlite3 WASM build): [random_get](https://wasix.org/docs/api-reference/wasi/random_get)

The WASI one may be problematic, since that's not provided in all environments.
