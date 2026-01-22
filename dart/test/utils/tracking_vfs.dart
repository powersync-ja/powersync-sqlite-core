import 'dart:typed_data';

import 'package:sqlite3/sqlite3.dart';

final class TrackingFileSystem extends BaseVirtualFileSystem {
  BaseVirtualFileSystem parent;
  int tempReads = 0;
  int tempWrites = 0;
  int dataReads = 0;
  int dataWrites = 0;
  int openFiles = 0;

  TrackingFileSystem({super.name = 'tracking', required this.parent});

  @override
  int xAccess(String path, int flags) {
    return parent.xAccess(path, flags);
  }

  @override
  void xDelete(String path, int syncDir) {
    parent.xDelete(path, syncDir);
  }

  @override
  String xFullPathName(String path) {
    return parent.xFullPathName(path);
  }

  @override
  XOpenResult xOpen(Sqlite3Filename path, int flags) {
    final result = parent.xOpen(path, flags);
    openFiles++;
    return (
      outFlags: result.outFlags,
      file: TrackingFile(
          result.file, this, flags & SqlFlag.SQLITE_OPEN_DELETEONCLOSE != 0),
    );
  }

  @override
  void xSleep(Duration duration) {}

  String stats() {
    return "Reads: $dataReads + $tempReads | Writes: $dataWrites + $tempWrites";
  }

  void clearStats() {
    tempReads = 0;
    tempWrites = 0;
    dataReads = 0;
    dataWrites = 0;
  }
}

class TrackingFile implements VirtualFileSystemFile {
  final TrackingFileSystem vfs;
  final VirtualFileSystemFile parentFile;
  final bool deleteOnClose;

  TrackingFile(this.parentFile, this.vfs, this.deleteOnClose);

  @override
  void xWrite(Uint8List buffer, int fileOffset) {
    if (deleteOnClose) {
      vfs.tempWrites++;
    } else {
      vfs.dataWrites++;
    }
    parentFile.xWrite(buffer, fileOffset);
  }

  @override
  void xRead(Uint8List buffer, int offset) {
    if (deleteOnClose) {
      vfs.tempReads++;
    } else {
      vfs.dataReads++;
    }
    parentFile.xRead(buffer, offset);
  }

  @override
  int xCheckReservedLock() {
    return parentFile.xCheckReservedLock();
  }

  @override
  void xClose() {
    vfs.openFiles--;
    return parentFile.xClose();
  }

  @override
  int xFileSize() {
    return parentFile.xFileSize();
  }

  @override
  void xLock(int mode) {
    return parentFile.xLock(mode);
  }

  @override
  void xSync(int flags) {
    return parentFile.xSync(flags);
  }

  @override
  void xTruncate(int size) {
    return parentFile.xTruncate(size);
  }

  @override
  void xUnlock(int mode) {
    return parentFile.xUnlock(mode);
  }

  @override
  int get xDeviceCharacteristics => parentFile.xDeviceCharacteristics;
}
