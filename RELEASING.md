# Preparing Release

Bump the version number in these places:

1. Cargo.toml
2. powersync-sqlite-core.podspec.
3. android/build.gradle.kts
4. android/src/prefab/prefab.json
5. tool/build_xcframework.sh - `VERSION` variable.
6. `cargo build` to update Cargo.lock

Create a tag:

```sh
git tag -am v1.2.3 v1.2.3
git push --tags
```

# Perform Release

Build:

```
gh workflow run release --ref v1.2.3 -f publish=true
```

The above does the following:

1. Create a draft GitHub release.
2. Build the xcframework for iOS and macOS, and upload to GitHub (attached to the above release).
3. Build and publish an Android aar to Sonatype. Afterwards, you can monitor the status of the publishing step [here](https://central.sonatype.com/publishing/deployments).

Publish the cocoapod:

```sh
pod trunk push powersync-sqlite-core.podspec
```
