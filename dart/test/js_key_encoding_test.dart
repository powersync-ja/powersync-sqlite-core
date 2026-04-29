import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

void main() {
  late CommonDatabase db;

  setUp(() async {
    db = openTestDatabase()
      ..select('select powersync_init();')
      ..select('select powersync_replace_schema(?)', [json.encode(_schema)]);
  });

  test('can fix JS key encoding', () {
    var [row] = db
        .select('INSERT INTO ps_buckets(name) VALUES (?) RETURNING id', ['a']);
    final bucketId = row.columnAt(0) as int;

    db.execute(
      'INSERT INTO ps_oplog(bucket, op_id, key, row_type, row_id, data, hash) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [
        bucketId,
        '1',
        // The JavaScript client used to insert keys like this (encoding the
        // subkey part as JSON).
        'items/1/"subkey"',
        'items',
        '1',
        '{}',
        0
      ],
    );

    // Apply migration
    db.execute(
        'UPDATE ps_oplog SET key = powersync_remove_duplicate_key_encoding(key);');

    [row] = db.select('select * from ps_oplog');
    expect(row['key'], 'items/1/subkey');
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
