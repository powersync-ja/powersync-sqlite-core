import 'dart:io';

import 'package:path/path.dart' as p;

/// Runs `all_tests.dart` as a single AOT-compiled executable with sanitizers
/// enabled.
///
/// To run tests with a sanitizer, use `dart tool/run_tests.dart
/// --sanitizer $sanitizer`, where `$sanitizer` is either `asan` or `msan`.
/// Note that sanitizers are only supported on X64 Linux hosts.
///
/// Running tests with sanitizers also requires an instrumented build of SQLite
/// and the core extension, which can be built with
/// `tool/build_linux_sanitized.sh`.
void main(List<String> args) async {
  Never invalidArgs() {
    print('Usage: dart tool/run_tests.dart asan|msan');
    exit(1);
  }

  if (args.length != 1) {
    invalidArgs();
  }

  final sanitizer = args.single;
  if (sanitizer != 'asan' && sanitizer != 'msan') invalidArgs();
  final expandedName = switch (sanitizer) {
    'asan' => 'address',
    'msan' => 'memory',
    _ => throw AssertionError(),
  };

  final dir = await Directory.systemTemp.createTemp('core-extension-tests');
  final aotPath = p.join(dir.path, 'test.aot');
  final assetsConfig = await _createNativeAssetsConfig(dir, expandedName);

  try {
    print('AOT-compiling tests');
    final result = await Process.run(Platform.executable, [
      'compile',
      'aot-snapshot',
      'tool/all_tests.dart',
      '--output',
      aotPath,
      '--target-sanitizer=$sanitizer',
      '--extra-gen-kernel-options=--native-assets=${assetsConfig.path}',
    ]);

    if (result.exitCode != 0 || !await File(aotPath).exists()) {
      throw '''
could not compile test script

exitCode: ${result.exitCode}
stdout: ${result.stdout}
stderr: ${result.stderr}
''';
    }

    var runtimeName = 'dartaotruntime_$sanitizer';

    print('Running with $runtimeName');
    final runtime = p.join(p.dirname(Platform.resolvedExecutable), runtimeName);
    final process = await Process.start(
        runtime,
        [
          aotPath,
          expandedName,
        ],
        mode: ProcessStartMode.inheritStdio);
    final exit = await process.exitCode;
    if (exit != 0) {
      throw 'Expected exit code 0, got $exit';
    }
  } finally {
    await dir.delete(recursive: true);
  }
}

Future<File> _createNativeAssetsConfig(
  Directory tmpForRun,
  String? expandedName,
) async {
  final sqliteFile = p.normalize(
    p.absolute('../sanitized/sqlite', 'libsqlite3_$expandedName.so'),
  );

  final yaml = '''
format-version: [1, 0, 0]
native-assets:
  linux_x64:
    "package:sqlite3/src/ffi/libsqlite3.g.dart":
      - absolute
      - "$sqliteFile"
''';

  final file = File(p.join(tmpForRun.path, 'assets.yaml'));
  await file.writeAsString(yaml);
  return file;
}
