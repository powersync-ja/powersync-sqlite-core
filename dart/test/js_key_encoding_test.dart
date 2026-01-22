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
    db.execute('insert into powersync_operations (op, data) VALUES (?, ?);', [
      'save',
      json.encode({
        'buckets': [
          {
            'bucket': 'a',
            'data': [
              {
                'op_id': '1',
                'op': 'PUT',
                'object_type': 'items',
                'object_id': '1',
                'subkey': json.encode('subkey'),
                'checksum': 0,
                'data': json.encode({'col': 'a'}),
              }
            ],
          }
        ],
      })
    ]);

    db.execute('INSERT INTO powersync_operations(op, data) VALUES (?, ?)',
        ['sync_local', null]);
    var [row] = db.select('select * from ps_oplog');
    expect(row['key'], 'items/1/"subkey"');

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
