import 'dart:ffi';

import 'package:sqlite3/common.dart';
import 'package:sqlite3/open.dart' as sqlite_open;
import 'package:sqlite3/sqlite3.dart';
import 'package:path/path.dart' as p;

const defaultSqlitePath = 'libsqlite3.so.0';

const libPath = '../target/debug';

CommonDatabase openTestDatabase() {
  sqlite_open.open.overrideFor(sqlite_open.OperatingSystem.linux, () {
    return DynamicLibrary.open('libsqlite3.so.0');
  });
  sqlite_open.open.overrideFor(sqlite_open.OperatingSystem.macOS, () {
    return DynamicLibrary.open('/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib');
  });
  var lib = DynamicLibrary.open(getLibraryForPlatform(path: libPath));
  var extension = SqliteExtension.inLibrary(lib, 'sqlite3_powersync_init');
  sqlite3.ensureExtensionLoaded(extension);
  return sqlite3.open(':memory:');
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
