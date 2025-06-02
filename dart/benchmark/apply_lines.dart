import 'dart:io';
import 'dart:typed_data';

import '../test/utils/native_test_utils.dart';

/// Usage: dart run benchmark/apply_lines.dart path/to/lines.bin
///
/// This creates a new in-memory database and applies concatenated BSON sync
/// lines from a file.
void main(List<String> args) {
  if (args.length != 1) {
    throw 'Usage: dart run benchmark/apply_lines.dart path/to/lines.bin';
  }

  final [path] = args;
  final file = File(path).openSync();
  final db = openTestDatabase();

  db
    ..execute('select powersync_init()')
    ..execute('select powersync_control(?, null)', ['start']);

  final stopwatch = Stopwatch()..start();

  final lengthBuffer = Uint8List(4);
  while (file.positionSync() < file.lengthSync()) {
    // BSON document: <int32LE length, ... bytes>
    final bytesRead = file.readIntoSync(lengthBuffer);
    if (bytesRead != 4) {
      throw 'short read, expected length';
    }
    final length = lengthBuffer.buffer.asByteData().getInt32(0, Endian.little);
    file.setPositionSync(file.positionSync() - 4);

    final syncLineBson = file.readSync(length);
    if (syncLineBson.length != length) {
      throw 'short read for bson document';
    }

    db
      ..execute('BEGIN')
      ..execute('SELECT powersync_control(?, ?)', ['line_binary', syncLineBson])
      ..execute('COMMIT;');
  }

  stopwatch.stop();
  print('Applying $path took ${stopwatch.elapsed}');
}
