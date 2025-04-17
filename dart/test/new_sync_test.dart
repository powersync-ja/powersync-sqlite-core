import 'dart:convert';

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
      return invokeControl('line_binary', BsonCodec.serialize(line).byteList);
    } else {
      return invokeControl('line_text', jsonEncode(line));
    }
  }

  test('starting stream', () {
    final result = invokeControl('start', null);
  });
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
