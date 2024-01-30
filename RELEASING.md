# Preparing Release

Bump the version number in these places:

1. Cargo.toml
2. powersync-sqlite-core.podspec.
3. android/build.gradle.kts
4. build-pod.sh - CFBundleVersion and CFBundleShortVersionString.
5. `cargo build` to update Cargo.lock

Create a tag:

```sh
git tag -am v1.2.3 v1.2.3
git push --tags
```

# Perform Release

Build:

```
gh workflow run release --ref v1.2.3
```

The above does the following:

1. Create a draft GitHub release.
2. Build the xcframework for iOS and macOS, and upload to GitHub (attached to the above release).
3. Publish the cocoapod for iOS and macOS.
4. Build and publish an Android aar to Sonatype staging.

Once that is done, go to the Maven staging repository, and "Close", wait, and "Release" the
repository:

https://s01.oss.sonatype.org/#stagingRepositories

Docs: https://central.sonatype.org/publish/release/

Go to GitHub Releases on the repository, update the description, then "Publish Release".
