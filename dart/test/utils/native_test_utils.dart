import 'dart:ffi';

import 'package:sqlite3/common.dart';
import 'package:sqlite3/open.dart' as sqlite_open;
import 'package:sqlite3/sqlite3.dart';

const defaultSqlitePath = 'libsqlite3.so.0';

const libPath = '../target/debug';

CommonDatabase openTestDatabase() {
  sqlite_open.open.overrideFor(sqlite_open.OperatingSystem.linux, () {
    return DynamicLibrary.open('libsqlite3.so.0');
  });
  sqlite_open.open.overrideFor(sqlite_open.OperatingSystem.macOS, () {
    return DynamicLibrary.open('libsqlite3.dylib');
  });
  var lib = DynamicLibrary.open(getLibraryForPlatform(path: libPath));
  var extension = SqliteExtension.inLibrary(lib, 'sqlite3_powersync_init');
  sqlite3.ensureExtensionLoaded(extension);
  return sqlite3.open(':memory:');
}

String getLibraryForPlatform({String? path = "."}) {
  switch (Abi.current()) {
    case Abi.androidArm:
    case Abi.androidArm64:
    case Abi.androidX64:
      return '$path/libpowersync.so';
    case Abi.macosArm64:
    case Abi.macosX64:
      return '$path/libpowersync.dylib';
    case Abi.linuxX64:
    case Abi.linuxArm64:
      return '$path/libpowersync.so';
    case Abi.windowsX64:
      return '$path/powersync.dll';
    case Abi.androidIA32:
      throw ArgumentError(
        'Unsupported processor architecture. X86 Android emulators are not '
        'supported. Please use an x86_64 emulator instead. All physical '
        'Android devices are supported including 32bit ARM.',
      );
    default:
      throw ArgumentError(
        'Unsupported processor architecture "${Abi.current()}". '
        'Please open an issue on GitHub to request it.',
      );
  }
}
