# Preparing Release

First, bump the version number in these places:

1. Cargo.toml
2. powersync-sqlite-core.podspec.
3. android/build.gradle.kts
4. android/src/prefab/prefab.json
5. tool/build_xcframework.sh - `VERSION` variable.
6. `cargo build` to update Cargo.lock

Next, open a PR with these changes and wait for it to get approved and merged.

# Perform Release

Create a tag, which will trigger a release workflow when pushed:

```sh
git tag -am v1.2.3 v1.2.3
git push --tags
```

The publishing workflow does the following:

1. Create a draft GitHub release.
2. Build the xcframework for iOS and macOS, and upload to GitHub (attached to the above release).
3. Build and publish an Android aar to Sonatype. Afterwards, you can monitor the status of the publishing step [here](https://central.sonatype.com/publishing/deployments).

The cocoapod needs to be published manually:

```sh
pod trunk push powersync-sqlite-core.podspec
```

# Updating SDKs

The release workflow will create an issue with a list of items to update the individual SDKs and intermediate packages.
