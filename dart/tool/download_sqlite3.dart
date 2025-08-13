import 'dart:ffi';
import 'dart:io';

import 'package:archive/archive_io.dart';
import 'package:path/path.dart' as p;
import 'package:http/http.dart';

typedef SqliteVersion = ({String version, String year});

const SqliteVersion latest = (version: '3500200', year: '2025');
const SqliteVersion minimum = (version: '3440000', year: '2023');

Future<void> main(List<String> args) async {
  if (args.contains('version')) {
    print(latest.version);
    exit(0);
  }

  await _downloadAndCompile('latest', latest, force: args.contains('--force'));
  await _downloadAndCompile('minimum', minimum,
      force: args.contains('--force'));
}

extension on SqliteVersion {
  String get autoconfUrl =>
      'https://sqlite.org/$year/sqlite-autoconf-$version.tar.gz';

  String get windowsArm64Url =>
      'https://sqlite.org/$year/sqlite-dll-win-arm64-$version.zip';

  String get windowsX64Url =>
      'https://sqlite.org/$year/sqlite-dll-win-x64-$version.zip';
}

Future<void> _downloadAndCompile(String name, SqliteVersion version,
    {bool force = false}) async {
  final dartDirectory = p.dirname(p.dirname(Platform.script.toFilePath()));
  final target = p.join(dartDirectory, '.dart_tool', 'sqlite3', name);
  final versionFile = File(p.join(target, 'version'));

  final needsDownload = force ||
      !versionFile.existsSync() ||
      versionFile.readAsStringSync() != version.version;

  if (!needsDownload) {
    print(
      'Not downloading sqlite3 $name as it has already been downloaded. Use '
      '--force to re-compile it.',
    );
    return;
  }

  print('Downloading and compiling sqlite3 $name (${version.version})');
  final targetDirectory = Directory(target);

  if (!targetDirectory.existsSync()) {
    targetDirectory.createSync(recursive: true);
  }

  final temporaryDir =
      await Directory.systemTemp.createTemp('powersync-core-compile-sqlite3');
  final temporaryDirPath = temporaryDir.path;

  // Compiling on Windows is ugly because we need users to have Visual Studio
  // installed and all those tools activated in the current shell.
  // Much easier to just download precompiled builds.
  if (Platform.isWindows) {
    final windowsUri = Abi.current() == Abi.windowsX64
        ? version.windowsX64Url
        : version.windowsArm64Url;
    final sqlite3Zip = p.join(temporaryDirPath, 'sqlite3.zip');
    final client = Client();
    final response = await client
        .send(Request('GET', Uri.parse(windowsUri))..followRedirects = true);
    if (response.statusCode != 200) {
      print(
          'Could not download $windowsUri, status code ${response.statusCode}');
      exit(1);
    }
    await response.stream.pipe(File(sqlite3Zip).openWrite());

    final inputStream = InputFileStream(sqlite3Zip);
    final archive = ZipDecoder().decodeStream(inputStream);

    for (final file in archive.files) {
      if (file.isFile && file.name == 'sqlite3.dll') {
        final outputStream = OutputFileStream(p.join(target, 'sqlite3.dll'));

        file.writeContent(outputStream);
        outputStream.close();
      }
    }

    await File(p.join(target, 'version')).writeAsString(version.version);
    exit(0);
  }

  await _run('curl -L ${version.autoconfUrl} --output sqlite.tar.gz',
      workingDirectory: temporaryDirPath);
  await _run('tar zxvf sqlite.tar.gz', workingDirectory: temporaryDirPath);

  final sqlitePath =
      p.join(temporaryDirPath, 'sqlite-autoconf-${version.version}');

  await _run('./configure', workingDirectory: sqlitePath);
  await _run('make -j', workingDirectory: sqlitePath);

  await File(p.join(sqlitePath, 'sqlite3')).copy(p.join(target, 'sqlite3'));
  final libsPath = name == 'latest' ? sqlitePath : p.join(sqlitePath, '.libs');

  if (Platform.isLinux) {
    await File(p.join(libsPath, 'libsqlite3.so'))
        .copy(p.join(target, 'libsqlite3.so'));
  } else if (Platform.isMacOS) {
    await File(p.join(libsPath, 'libsqlite3.dylib'))
        .copy(p.join(target, 'libsqlite3.dylib'));
  }

  await File(p.join(target, 'version')).writeAsString(version.version);
}

Future<void> _run(String command, {String? workingDirectory}) async {
  print('Running $command');

  final proc = await Process.start(
    'sh',
    ['-c', command],
    mode: ProcessStartMode.inheritStdio,
    workingDirectory: workingDirectory,
  );
  final exitCode = await proc.exitCode;

  if (exitCode != 0) {
    exit(exitCode);
  }
}
