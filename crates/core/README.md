# powersync_core

This is the core SQLite extension, containing all the logic.

Since the different build configurations (loadable extension, bundled SQLite shell)
use different cargo config, we can't completely separate them just using features.
Instead, we use different crates depending on this core lib.

