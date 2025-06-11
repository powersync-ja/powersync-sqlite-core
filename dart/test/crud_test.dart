import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

void main() {
  group('crud tests', () {
    late CommonDatabase db;

    setUp(() async {
      db = openTestDatabase();
    });

    tearDown(() {
      db.dispose();
    });

    test('powersync_diff - single value', () {
      var r1 =
          db.select('select powersync_diff(?, ?) as diff', ['{}', '{}']).first;
      expect(r1['diff'], equals('{}'));

      var r2 = db.select(
          'select powersync_diff(?, ?) as diff', ['{}', '{"test":1}']).first;
      expect(r2['diff'], equals('{"test":1}'));

      var r3 = db.select('select powersync_diff(?, ?) as diff',
          ['{"test":1}', '{"test":1}']).first;
      expect(r3['diff'], equals('{}'));

      var r4 = db.select(
          'select powersync_diff(?, ?) as diff', ['{"test":1}', '{}']).first;
      expect(r4['diff'], equals('{"test":null}'));

      var r5 = db.select('select powersync_diff(?, ?) as diff',
          ['{"test":1}', '{"test":null}']).first;
      expect(r5['diff'], equals('{"test":null}'));

      var r6 = db.select('select powersync_diff(?, ?) as diff',
          ['{"test":1}', '{"test":2}']).first;
      expect(r6['diff'], equals('{"test":2}'));
    });

    test('powersync_diff - multiple values', () {
      var r1 = db.select('select powersync_diff(?, ?) as diff',
          ['{"a":1,"b":"test"}', '{}']).first;
      expect(r1['diff'], equals('{"a":null,"b":null}'));

      var r2 = db.select('select powersync_diff(?, ?) as diff',
          ['{}', '{"a":1,"b":"test"}']).first;
      expect(r2['diff'], equals('{"a":1,"b":"test"}'));

      var r3 = db.select('select powersync_diff(?, ?) as diff',
          ['{"a":1,"b":"test"}', '{"a":1,"b":"test"}']).first;
      expect(r3['diff'], equals('{}'));

      var r4 = db.select('select powersync_diff(?, ?) as diff',
          ['{"a":1}', '{"b":"test"}']).first;
      expect(r4['diff'], equals('{"a":null,"b":"test"}'));

      var r5 = db.select('select powersync_diff(?, ?) as diff',
          ['{"a":1}', '{"a":1,"b":"test"}']).first;
      expect(r5['diff'], equals('{"b":"test"}'));
    });

    var runCrudTest = (int numberOfColumns) {
      var columns = [];
      for (var i = 0; i < numberOfColumns; i++) {
        columns.add({'name': 'column$i', 'type': 'TEXT'});
      }
      var tableSchema = {
        'tables': [
          {'name': 'items', 'columns': columns}
        ]
      };
      db.select('select powersync_init()');

      // 1. Test schema initialization
      db.select(
          'select powersync_replace_schema(?)', [jsonEncode(tableSchema)]);

      var columnNames = columns.map((c) => c['name']).join(', ');
      var columnValues = columns.map((c) => "'${c['name']}'").join(', ');

      // 2. Test insert
      db.select(
          "insert into items(id, ${columnNames}) values('test_id', ${columnValues})");
      var item = db.select('select * from items').first;
      var expectedData =
          Map.fromEntries(columns.map((c) => MapEntry(c['name'], c['name'])));

      expect(item, equals({'id': 'test_id', ...expectedData}));
      var crud = db.select('select * from ps_crud').first;
      var crudData = jsonDecode(crud['data']);
      expect(crud['tx_id'], equals(1));
      expect(
          crudData,
          equals({
            'op': 'PUT',
            'type': 'items',
            'id': 'test_id',
            'data': expectedData
          }));

      // 3. Test update
      db.select('update items set column0 = ?', ['new_value']);
      var itemUpdated = db.select('select * from items').first;
      expect(itemUpdated,
          equals({'id': 'test_id', ...expectedData, 'column0': 'new_value'}));

      var crudUpdated = db.select('select * from ps_crud where id = 2').first;
      var crudDataUpdated = jsonDecode(crudUpdated['data']);
      expect(crudUpdated['tx_id'], equals(2));
      expect(
          crudDataUpdated,
          equals({
            'op': 'PATCH',
            'type': 'items',
            'id': 'test_id',
            'data': {'column0': 'new_value'}
          }));

      // 4. Test delete
      db.select('delete from items');
      var itemDeleted = db.select('select * from items').firstOrNull;
      expect(itemDeleted, equals(null));

      var crudDeleted = db.select('select * from ps_crud where id = 3').first;
      var crudDataDeleted = jsonDecode(crudDeleted['data']);
      expect(crudDeleted['tx_id'], equals(3));
      expect(crudDataDeleted,
          equals({'op': 'DELETE', 'type': 'items', 'id': 'test_id'}));
    };

    var runCrudTestLocalOnly = (int numberOfColumns) {
      var columns = [];
      for (var i = 0; i < numberOfColumns; i++) {
        columns.add({'name': 'column$i', 'type': 'TEXT'});
      }
      var tableSchema = {
        'tables': [
          {'name': 'items', 'columns': columns, 'local_only': true}
        ]
      };
      db.select('select powersync_init()');

      // 1. Test schema initialization
      db.select(
          'select powersync_replace_schema(?)', [jsonEncode(tableSchema)]);

      var columnNames = columns.map((c) => c['name']).join(', ');
      var columnValues = columns.map((c) => "'${c['name']}'").join(', ');

      // 2. Test insert
      db.select(
          "insert into items(id, ${columnNames}) values('test_id', ${columnValues})");
      var item = db.select('select * from items').first;
      var expectedData =
          Map.fromEntries(columns.map((c) => MapEntry(c['name'], c['name'])));

      expect(item, equals({'id': 'test_id', ...expectedData}));

      // 3. Test update
      db.select('update items set column0 = ?', ['new_value']);
      var itemUpdated = db.select('select * from items').first;
      expect(itemUpdated,
          equals({'id': 'test_id', ...expectedData, 'column0': 'new_value'}));

      // 4. Test delete
      db.select('delete from items');
      var itemDeleted = db.select('select * from items').firstOrNull;
      expect(itemDeleted, equals(null));
    };

    var runCrudTestInsertOnly = (int numberOfColumns) {
      var columns = [];
      for (var i = 0; i < numberOfColumns; i++) {
        columns.add({'name': 'column$i', 'type': 'TEXT'});
      }
      var tableSchema = {
        'tables': [
          {'name': 'items', 'columns': columns, 'insert_only': true}
        ]
      };
      db.select('select powersync_init()');

      // 1. Test schema initialization
      db.select(
          'select powersync_replace_schema(?)', [jsonEncode(tableSchema)]);

      var columnNames = columns.map((c) => c['name']).join(', ');
      var columnValues = columns.map((c) => "'${c['name']}'").join(', ');

      // 2. Test insert
      db.select(
          "insert into items(id, ${columnNames}) values('test_id', ${columnValues})");
      var item = db.select('select * from items').firstOrNull;
      expect(item, equals(null));
      var expectedData =
          Map.fromEntries(columns.map((c) => MapEntry(c['name'], c['name'])));

      var crud = db.select('select * from ps_crud').first;
      var crudData = jsonDecode(crud['data']);
      expect(crud['tx_id'], equals(1));
      expect(
          crudData,
          equals({
            'op': 'PUT',
            'type': 'items',
            'id': 'test_id',
            'data': expectedData
          }));
    };

    for (var numberOfColumns in [1, 49, 50, 51, 63, 64, 100, 1999]) {
      test('crud test with $numberOfColumns columns', () async {
        runCrudTest(numberOfColumns);
      });
      test('crud test with $numberOfColumns columns - local_only', () async {
        runCrudTestLocalOnly(numberOfColumns);
      });

      test('crud test with $numberOfColumns columns - insert_only', () async {
        runCrudTestInsertOnly(numberOfColumns);
      });
    }

    group('crud vtab', () {
      setUp(() {
        db.select('select powersync_init()');
      });

      group('simple', () {
        test('can insert', () {
          db.execute(
              'INSERT INTO powersync_crud (op, id, type, data) VALUES (?, ?, ?, ?)',
              [
                'PUT',
                'foo',
                'users',
                json.encode({'my': 'value'})
              ]);

          final [row] = db.select('SELECT * FROM ps_crud');
          expect(row, {
            'id': 1,
            'tx_id': 1,
            'data':
                '{"op":"PUT","id":"foo","type":"users","data":{"my":"value"}}',
          });
        });

        test('updates local bucket and updated rows', () {
          db.execute(
              'INSERT INTO powersync_crud (op, id, type, data) VALUES (?, ?, ?, ?)',
              [
                'PUT',
                'foo',
                'users',
                json.encode({'my': 'value'})
              ]);

          expect(db.select('SELECT * FROM ps_updated_rows'), [
            {'row_type': 'users', 'row_id': 'foo'}
          ]);
          expect(db.select('SELECT * FROM ps_buckets'), [
            allOf(
              containsPair('name', r'$local'),
              containsPair('target_op', 9223372036854775807),
            )
          ]);
        });

        test('does not require data', () {
          db.execute(
              'INSERT INTO powersync_crud (op, id, type) VALUES (?, ?, ?)', [
            'DELETE',
            'foo',
            'users',
          ]);

          final [row] = db.select('SELECT * FROM ps_crud');
          expect(row, {
            'id': 1,
            'tx_id': 1,
            'data': '{"op":"DELETE","id":"foo","type":"users"}',
          });
        });

        test('can insert metadata', () {
          db.execute(
              'INSERT INTO powersync_crud (op, id, type, metadata) VALUES (?, ?, ?, ?)',
              ['DELETE', 'foo', 'users', 'my metadata']);

          final [row] = db.select('SELECT * FROM ps_crud');
          expect(row, {
            'id': 1,
            'tx_id': 1,
            'data':
                '{"op":"DELETE","id":"foo","type":"users","metadata":"my metadata"}',
          });
        });

        test('can insert old data', () {
          db.execute(
              'INSERT INTO powersync_crud (op, id, type, data, old_values) VALUES (?, ?, ?, ?, ?)',
              [
                'PUT',
                'foo',
                'users',
                json.encode({'my': 'value'}),
                json.encode({'previous': 'value'})
              ]);

          final [row] = db.select('SELECT * FROM ps_crud');
          expect(row, {
            'id': 1,
            'tx_id': 1,
            'data':
                '{"op":"PUT","id":"foo","type":"users","data":{"my":"value"},"old":{"previous":"value"}}',
          });
        });
      });
    });

    group('tracks previous values', () {
      void createTable([Map<String, Object?> options = const {}]) {
        final tableSchema = {
          'tables': [
            {
              'name': 'test',
              'columns': [
                {'name': 'name', 'type': 'text'},
                {'name': 'name2', 'type': 'text'},
              ],
              ...options,
            }
          ]
        };

        db.select('select powersync_init()');
        db.select(
            'select powersync_replace_schema(?)', [jsonEncode(tableSchema)]);
      }

      group('for updates', () {
        void insertThenUpdate() {
          db
            ..execute('insert into test (id, name, name2) values (?, ?, ?)',
                ['id', 'name', 'name2'])
            ..execute('delete from ps_crud')
            ..execute('update test set name = name || ?', ['.']);
        }

        test('is not tracked by default', () {
          createTable();
          insertThenUpdate();

          final [row] = db.select('select data from ps_crud');
          expect(jsonDecode(row[0] as String), isNot(contains('old')));
        });

        test('can be disabled', () {
          createTable({'include_old': false});
          insertThenUpdate();

          final [row] = db.select('select data from ps_crud');
          expect(jsonDecode(row[0] as String), isNot(contains('old')));
        });

        test('can be enabled for all columns', () {
          createTable({'include_old': true});
          insertThenUpdate();

          final [row] = db.select('select data from ps_crud');
          final op = jsonDecode(row[0] as String);
          expect(op['data'], {'name': 'name.'});
          expect(op['old'], {'name': 'name', 'name2': 'name2'});
        });

        test('can be enabled for some columns', () {
          createTable({
            'include_old': ['name']
          });
          insertThenUpdate();

          final [row] = db.select('select data from ps_crud');
          final op = jsonDecode(row[0] as String);
          expect(op['data'], {'name': 'name.'});
          expect(op['old'], {'name': 'name'});
        });

        test('can track changed values only', () {
          createTable({
            'include_old': true,
            'include_old_only_when_changed': true,
          });
          insertThenUpdate();

          final [row] = db.select('select data from ps_crud');
          final op = jsonDecode(row[0] as String);
          expect(op['data'], {'name': 'name.'});
          expect(op['old'], {'name': 'name'});
        });

        test('combined column filter and only tracking changes', () {
          createTable({
            'include_old': ['name2'],
            'include_old_only_when_changed': true,
          });
          insertThenUpdate();

          final [row] = db.select('select data from ps_crud');
          final op = jsonDecode(row[0] as String);
          expect(op['data'], {'name': 'name.'});
          expect(op['old'], {});
        });
      });

      group('for deletes', () {
        void insertThenDelete() {
          db
            ..execute('insert into test (id, name, name2) values (?, ?, ?)',
                ['id', 'name', 'name2'])
            ..execute('delete from ps_crud')
            ..execute('delete from test');
        }

        test('is not tracked by default', () {
          createTable();
          insertThenDelete();

          final [row] = db.select('select data from ps_crud');
          expect(jsonDecode(row[0] as String), isNot(contains('old')));
        });

        test('can be disabled', () {
          createTable({'include_old': false});
          insertThenDelete();

          final [row] = db.select('select data from ps_crud');
          expect(jsonDecode(row[0] as String), isNot(contains('old')));
        });

        test('can be enabled for all columns', () {
          createTable({'include_old': true});
          insertThenDelete();

          final [row] = db.select('select data from ps_crud');
          final op = jsonDecode(row[0] as String);
          expect(op['data'], null);
          expect(op['old'], {'name': 'name', 'name2': 'name2'});
        });

        test('can be enabled for some columns', () {
          createTable({
            'include_old': ['name']
          });
          insertThenDelete();

          final [row] = db.select('select data from ps_crud');
          final op = jsonDecode(row[0] as String);
          expect(op['data'], null);
          expect(op['old'], {'name': 'name'});
        });
      });
    });

    group('including metadata', () {
      void createTable([Map<String, Object?> options = const {}]) {
        final tableSchema = {
          'tables': [
            {
              'name': 'test',
              'columns': [
                {'name': 'name', 'type': 'text'},
              ],
              ...options,
            }
          ]
        };

        db.select('select powersync_init()');
        db.select(
            'select powersync_replace_schema(?)', [jsonEncode(tableSchema)]);
      }

      test('is disabled by default', () {
        createTable();
        expect(
          () => db.execute(
            'INSERT INTO test (id, name, _metadata) VALUES (?, ?, ?)',
            ['id', 'name', 'test insert'],
          ),
          throwsA(isA<SqliteException>()),
        );
      });

      test('can be disabled', () {
        createTable({'include_metadata': false});
        expect(
          () => db.execute(
            'INSERT INTO test (id, name, _metadata) VALUES (?, ?, ?)',
            ['id', 'name', 'test insert'],
          ),
          throwsA(isA<SqliteException>()),
        );
      });

      test('supports insert statements', () {
        createTable({'include_metadata': true});
        db.execute(
          'INSERT INTO test (id, name, _metadata) VALUES (?, ?, ?)',
          ['id', 'name', 'test insert'],
        );

        final [row] = db.select('select data from ps_crud');
        final op = jsonDecode(row[0] as String);
        expect(op['data'], {'name': 'name'});
        expect(op['metadata'], 'test insert');
      });

      test('supports update statements', () {
        createTable({'include_metadata': true});
        db.execute(
          'INSERT INTO test (id, name, _metadata) VALUES (?, ?, ?)',
          ['id', 'name', 'test insert'],
        );
        db.execute('delete from ps_crud;');
        db.execute(
            'update test set name = name || ?, _metadata = ?', ['.', 'update']);

        final [row] = db.select('select data from ps_crud');
        final op = jsonDecode(row[0] as String);
        expect(op['data'], {'name': 'name.'});
        expect(op['metadata'], 'update');
      });

      test('supports regular delete statements', () {
        createTable({'include_metadata': true});
        db.execute(
          'INSERT INTO test (id, name, _metadata) VALUES (?, ?, ?)',
          ['id', 'name', 'test insert'],
        );
        db.execute('delete from ps_crud;');
        db.execute('delete from test');

        final [row] = db.select('select data from ps_crud');
        final op = jsonDecode(row[0] as String);
        expect(op['op'], 'DELETE');
        expect(op['metadata'], null);
      });

      test('supports deleting updates with metadata', () {
        createTable({'include_metadata': true});
        db.execute(
          'INSERT INTO test (id, name, _metadata) VALUES (?, ?, ?)',
          ['id', 'name', 'test insert'],
        );
        db.execute('delete from ps_crud;');
        db.execute('update test set _deleted = TRUE, _metadata = ?',
            ['custom delete']);

        expect(db.select('select * from test'), hasLength(0));

        final [row] = db.select('select data from ps_crud');
        final op = jsonDecode(row[0] as String);
        expect(op['op'], 'DELETE');
        expect(op['metadata'], 'custom delete');
      });
    });

    test('includes empty updates by default', () {
      db
        ..execute('select powersync_replace_schema(?)', [
          json.encode({
            'tables': [
              {
                'name': 'items',
                'columns': [
                  {'name': 'col', 'type': 'text'}
                ],
              }
            ]
          })
        ])
        ..execute(
            'INSERT INTO items (id, col) VALUES (uuid(), ?)', ['new item'])
        ..execute('UPDATE items SET col = LOWER(col)');

      // Should record insert and update operation.
      expect(db.select('SELECT * FROM ps_crud'), hasLength(2));
    });

    test('can ignore empty updates', () {
      db
        ..execute('select powersync_replace_schema(?)', [
          json.encode({
            'tables': [
              {
                'name': 'items',
                'columns': [
                  {'name': 'col', 'type': 'text'}
                ],
                'ignore_empty_update': true,
              }
            ]
          })
        ])
        ..execute(
            'INSERT INTO items (id, col) VALUES (uuid(), ?)', ['new item'])
        ..execute('UPDATE items SET col = LOWER(col)');

      // The update which didn't change any rows should not be recorded.
      expect(db.select('SELECT * FROM ps_crud'), hasLength(1));
    });
  });
}
