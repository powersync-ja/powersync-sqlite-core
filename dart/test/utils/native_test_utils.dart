import 'dart:ffi';
import 'dart:io';

import 'package:sqlite3/common.dart';
import 'package:sqlite3/open.dart' as sqlite_open;
import 'package:sqlite3/sqlite3.dart';
import 'package:path/path.dart' as p;

const defaultSqlitePath = 'libsqlite3.so.0';

const libPath = '../target/debug';
var didLoadExtension = false;

void applyOpenOverride() {
  if (Platform.environment['CORE_TEST_SQLITE'] case final sqlite?) {
    sqlite_open.open
        .overrideForAll(() => DynamicLibrary.open(p.absolute(sqlite)));
  }

  sqlite_open.open.overrideFor(sqlite_open.OperatingSystem.linux, () {
    return DynamicLibrary.open('libsqlite3.so.0');
  });
  sqlite_open.open.overrideFor(sqlite_open.OperatingSystem.macOS, () {
    // Prefer using Homebrew's SQLite which allows loading extensions.
    const fromHomebrew = '/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib';
    if (File(fromHomebrew).existsSync()) {
      return DynamicLibrary.open(fromHomebrew);
    }

    return DynamicLibrary.open('libsqlite3.dylib');
  });
}

CommonDatabase openTestDatabase(
    {VirtualFileSystem? vfs, String fileName = ':memory:'}) {
  applyOpenOverride();
  if (!didLoadExtension) {
    loadExtension();
  }

  return sqlite3.open(fileName, vfs: vfs?.name);
}

void loadExtension() {
  applyOpenOverride();

  var lib = DynamicLibrary.open(getLibraryForPlatform(path: libPath));
  var extension = SqliteExtension.inLibrary(lib, 'sqlite3_powersync_init');
  sqlite3.ensureExtensionLoaded(extension);
  didLoadExtension = true;
}

String getLibraryForPlatform({String? path = "."}) {
  // Using an absolute path is required for macOS, where Dart can't dlopen
  // relative paths due to being a "hardened program".
  return p.normalize(p.absolute(switch (Abi.current()) {
    Abi.androidArm ||
    Abi.androidArm64 ||
    Abi.androidX64 =>
      '$path/libpowersync.so',
    Abi.macosArm64 || Abi.macosX64 => '$path/libpowersync.dylib',
    Abi.linuxX64 || Abi.linuxArm64 => '$path/libpowersync.so',
    Abi.windowsX64 => '$path/powersync.dll',
    Abi.androidIA32 => throw ArgumentError(
        'Unsupported processor architecture. X86 Android emulators are not '
        'supported. Please use an x86_64 emulator instead. All physical '
        'Android devices are supported including 32bit ARM.',
      ),
    _ => throw ArgumentError(
        'Unsupported processor architecture "${Abi.current()}". '
        'Please open an issue on GitHub to request it.',
      )
  }));
}
