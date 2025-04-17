import 'dart:convert';
import 'dart:typed_data';

import 'package:bson/bson.dart';
import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

void main() {
  group('text lines', () {
    _syncTests(isBson: false);
  });

  group('bson lines', () {
    _syncTests(isBson: true);
  });
}

void _syncTests<T>({
  required bool isBson,
}) {
  late CommonDatabase db;

  setUp(() async {
    db = openTestDatabase()
      ..select('select powersync_init();')
      ..select('select powersync_replace_schema(?)', [json.encode(_schema)]);
  });

  tearDown(() {
    db.dispose();
  });

  List<Object?> invokeControl(String operation, Object? data) {
    final [row] =
        db.select('SELECT powersync_control(?, ?)', [operation, data]);
    return jsonDecode(row.columnAt(0));
  }

  List<Object?> syncLine(Object? line) {
    if (isBson) {
      final serialized = BsonCodec.serialize(line).byteList;
      print(serialized.asRustByteString);
      return invokeControl('line_binary', serialized);
    } else {
      return invokeControl('line_text', jsonEncode(line));
    }
  }

  test('starting stream', () {
    final result = invokeControl('start', null);

    expect(result, [
      {'UpdateSyncStatus': anything},
      {'EstablishSyncStream': anything},
    ]);
  });

  test('simple sync iteration', () {
    invokeControl('start', null);

    expect(
      syncLine({
        'checkpoint': {
          'last_op_id': '1',
          'write_checkpoint': null,
          'buckets': [
            {
              'bucket': 'a',
              'checksum': 0,
              'priority': 3,
              'count': 1,
            }
          ],
        },
      }),
      [
        {
          'UpdateSyncStatus': {
            'status': {
              'connected': true,
              'connecting': false,
              'priority_status': [],
              'downloading': {
                'buckets': {
                  'a': {
                    'priority': 3,
                    'at_last': 0,
                    'since_last': 0,
                    'target_count': 1
                  }
                }
              }
            }
          }
        }
      ],
    );
  });
}

extension on Uint8List {
  String get asRustByteString {
    final buffer = StringBuffer('b"');

    for (final byte in this) {
      switch (byte) {
        case >= 32 && < 127:
          buffer.writeCharCode(byte);
        default:
          // Escape
          buffer.write('\\x${byte.toRadixString(16).padLeft(2, '0')}');
      }
    }

    buffer.write('"');
    return buffer.toString();
  }
}

const _schema = {
  'tables': [
    {
      'name': 'items',
      'columns': [
        {'name': 'col', 'type': 'text'}
      ],
    }
  ]
};

const _bucketDescriptions = {
  'prio0': {'priority': 0},
  'prio1': {'priority': 1},
  'prio2': {'priority': 2},
  'prio3': {'priority': 3},
};
