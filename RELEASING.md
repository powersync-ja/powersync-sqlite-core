# Preparing Release

Bump the version number in these places:
1. Cargo.toml
2. powersync-sqlite-core.podspec.
3. android/build.gradle.kts

Create a tag:

```sh
git tag -am v1.2.3 v1.2.3
git push --tags
```

# Perform Release

```
gh workflow run release --ref v1.2.3
```

Once that is done, go to the Maven staging repository, and promote the build:

https://s01.oss.sonatype.org/#stagingRepositories
