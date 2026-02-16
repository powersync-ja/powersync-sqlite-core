import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';
import 'utils/test_utils.dart';

void main() {
  group('crud tests', () {
    late CommonDatabase db;

    setUp(() async {
      db = openTestDatabase();
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

        test('resets state after commit', () {
          db.execute('BEGIN');
          db.execute(
              'INSERT INTO powersync_crud (op, id, type) VALUES (?, ?, ?)', [
            'DELETE',
            'foo',
            'users',
          ]);
          db.execute('commit');

          db.execute(
              'INSERT INTO powersync_crud (op, id, type) VALUES (?, ?, ?)', [
            'DELETE',
            'foo',
            'users',
          ]);
          expect(db.select('SELECT * FROM ps_crud').map((r) => r['tx_id']),
              [1, 2]);
        });

        test('resets state after rollback', () {
          db.execute('BEGIN');
          db.execute(
              'INSERT INTO powersync_crud (op, id, type) VALUES (?, ?, ?)', [
            'DELETE',
            'foo',
            'users',
          ]);
          db.execute('rollback');

          db.execute(
              'INSERT INTO powersync_crud (op, id, type) VALUES (?, ?, ?)', [
            'DELETE',
            'foo2',
            'users',
          ]);
          expect(db.select('SELECT * FROM ps_crud'), [
            {
              'id': 1,
              'data': '{"op":"DELETE","id":"foo2","type":"users"}',
              'tx_id': 1,
            }
          ]);
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

    test('preserves values in text column', () {
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
        ]);

      db.execute('INSERT INTO items (id, col) VALUES (uuid(), json_object())');
      final [insert] = db.select('SELECT data FROM ps_crud');
      expect(json.decode(insert['data']), containsPair('data', {'col': '{}'}));
      db.execute('DELETE FROM ps_crud');

      db.execute('UPDATE items SET col = NULL;');
      final [update] = db.select('SELECT data FROM ps_crud');
      expect(json.decode(update['data']), containsPair('data', {'col': null}));
      db.execute('DELETE FROM ps_crud');
    });

    test('preserves mismatched type', () {
      db
        ..execute('select powersync_replace_schema(?)', [
          json.encode({
            'tables': [
              {
                'name': 'items',
                'columns': [
                  {'name': 'col', 'type': 'int'}
                ],
              }
            ]
          })
        ])
        ..execute('insert into items (id, col) values (uuid(), json_object())')
        ..execute('insert into items (id, col) values (uuid(), null)')
        ..execute('insert into items (id, col) values (uuid(), ?)',
            ['not an integer']);

      final data = db.select('SELECT data FROM ps_crud');
      expect(data.map((row) => jsonDecode(row['data'])), [
        containsPair('data', {'col': '{}'}),
        containsPair('data', {}),
        containsPair('data', {'col': 'not an integer'}),
      ]);
    });

    group('insert only', () {
      test('smoke test', () {
        db
          ..execute('select powersync_replace_schema(?)', [
            json.encode({
              'tables': [
                {
                  'name': 'items',
                  'insert_only': true,
                  'columns': [
                    {'name': 'col', 'type': 'int'}
                  ],
                }
              ]
            })
          ])
          ..execute(
            'INSERT INTO items (id, col) VALUES (uuid(), 1)',
          );

        expect(db.select('SELECT * FROM ps_crud'), hasLength(1));
        // Insert-only tables don't update the $local bucket
        expect(db.select('SELECT * FROM ps_buckets'), isEmpty);

        // Can't update or delete insert-only tables.
        expect(() => db.execute('UPDATE items SET col = col + 1'),
            throwsA(anything));
        expect(() => db.execute('DELETE FROM items WHERE col = 1'),
            throwsA(anything));
      });

      test('has no effect on local-only tables', () {
        db
          ..execute('select powersync_replace_schema(?)', [
            json.encode({
              'tables': [
                {
                  'name': 'items',
                  'insert_only': true,
                  'local_only': true,
                  'columns': [
                    {'name': 'col', 'type': 'int'}
                  ],
                }
              ]
            })
          ]);

        db.execute(
          'INSERT INTO items (id, col) VALUES (uuid(), 1)',
        );
        expect(db.select('SELECT * FROM items'), hasLength(1));

        db
          ..execute('UPDATE items SET col = col + 1')
          ..execute('DELETE FROM items WHERE col = 2');
        expect(db.select('SELECT * FROM items'), isEmpty);

        // because this is a local-only table, no crud items should have been
        // created.
        expect(db.select('SELECT * FROM ps_crud'), isEmpty);
      });
    });

    group('raw tables', () {
      void createRawTableTriggers(Object table,
          {bool insert = true, bool update = true, bool delete = true}) {
        db.execute('SELECT powersync_init()');

        if (insert) {
          db.execute('SELECT powersync_create_raw_table_crud_trigger(?, ?, ?)',
              [json.encode(table), 'test_trigger_insert', 'INSERT']);
        }
        if (update) {
          db.execute('SELECT powersync_create_raw_table_crud_trigger(?, ?, ?)',
              [json.encode(table), 'test_trigger_update', 'UPDATE']);
        }
        if (delete) {
          db.execute('SELECT powersync_create_raw_table_crud_trigger(?, ?, ?)',
              [json.encode(table), 'test_trigger_delete', 'DELETE']);
        }
      }

      Object rawTableDescription(Map<String, Object?> options) {
        return {
          'name': 'row_type',
          'put': {'sql': '', 'params': []},
          'delete': {'sql': '', 'params': []},
          ...options,
        };
      }

      test('missing id column', () {
        db.execute('CREATE TABLE users (name TEXT);');
        expect(
          () => createRawTableTriggers(
              rawTableDescription({'table_name': 'users'})),
          throwsA(isSqliteException(
              3091, contains('Table users has no id column'))),
        );
      });

      test('missing local table name', () {
        db.execute('CREATE TABLE users (name TEXT);');
        expect(
          () => createRawTableTriggers(rawTableDescription({})),
          throwsA(isSqliteException(
              3091, contains('Raw table row_type has no local name'))),
        );
      });

      test('missing local table', () {
        expect(
          () => createRawTableTriggers(
              rawTableDescription({'table_name': 'users'})),
          throwsA(isSqliteException(
              3091, contains('Could not find users in local schema'))),
        );
      });

      test('default options', () {
        db.execute('CREATE TABLE users (id TEXT, name TEXT) STRICT;');
        createRawTableTriggers(rawTableDescription({'table_name': 'users'}));

        db
          ..execute(
              'INSERT INTO users (id, name) VALUES (?, ?)', ['id', 'name'])
          ..execute('UPDATE users SET name = ?', ['new name'])
          ..execute('DELETE FROM users WHERE id = ?', ['id']);

        final psCrud = db.select('SELECT * FROM ps_crud');
        expect(psCrud, [
          {
            'id': 1,
            'tx_id': 1,
            'data': json.encode({
              'op': 'PUT',
              'id': 'id',
              'type': 'row_type',
              'data': {'name': 'name'}
            }),
          },
          {
            'id': 2,
            'tx_id': 2,
            'data': json.encode({
              'op': 'PATCH',
              'id': 'id',
              'type': 'row_type',
              'data': {'name': 'new name'}
            }),
          },
          {
            'id': 3,
            'tx_id': 3,
            'data':
                json.encode({'op': 'DELETE', 'id': 'id', 'type': 'row_type'}),
          },
        ]);
      });

      test('insert only', () {
        db.execute('CREATE TABLE users (id TEXT, name TEXT) STRICT;');
        createRawTableTriggers(
            rawTableDescription({'table_name': 'users', 'insert_only': true}));

        db.execute(
            'INSERT INTO users (id, name) VALUES (?, ?)', ['id', 'name']);
        expect(db.select('SELECT * FROM ps_crud'), hasLength(1));

        // Should not update the $local bucket
        expect(db.select('SELECT * FROM ps_buckets'), hasLength(0));

        // The trigger should prevent other writes.
        expect(
            () => db.execute('UPDATE users SET name = ?', ['new name']),
            throwsA(isSqliteException(
                1811, contains('Unexpected update on insert-only table'))));
        expect(
            () => db.execute('DELETE FROM users WHERE id = ?', ['id']),
            throwsA(isSqliteException(
                1811, contains('Unexpected update on insert-only table'))));
      });

      test('tracking old values', () {
        db.execute(
            'CREATE TABLE users (id TEXT, name TEXT, email TEXT) STRICT;');
        createRawTableTriggers(rawTableDescription({
          'table_name': 'users',
          'include_old': ['name'],
          'include_old_only_when_changed': true,
        }));

        db
          ..execute('INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
              ['id', 'name', 'test@example.org'])
          ..execute('UPDATE users SET name = ?, email = ?',
              ['new name', 'newmail@example.org'])
          ..execute('DELETE FROM users WHERE id = ?', ['id']);

        final psCrud = db.select(
            r"SELECT id, data->>'$.op' AS op, data->>'$.old' as old FROM ps_crud");
        expect(psCrud, [
          {
            'id': 1,
            'op': 'PUT',
            'old': null,
          },
          {
            'id': 2,
            'op': 'PATCH',
            'old': json.encode({'name': 'name'}),
          },
          {
            'id': 3,
            'op': 'DELETE',
            'old': json.encode({'name': 'new name'}),
          },
        ]);
      });

      test('skipping empty updates', () {
        db.execute('CREATE TABLE users (id TEXT, name TEXT) STRICT;');
        createRawTableTriggers(rawTableDescription(
            {'table_name': 'users', 'ignore_empty_update': true}));

        db.execute(
            'INSERT INTO users (id, name) VALUES (?, ?)', ['id', 'name']);
        expect(db.select('SELECT * FROM ps_crud'), hasLength(1));

        // Empty update should not be recorded
        db.execute('UPDATE users SET name = ?', ['name']);
        expect(db.select('SELECT * FROM ps_crud'), hasLength(1));
      });
    });
  });
}
