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

      void insertThenUpdate() {
        db
          ..execute('insert into test (id, name, name2) values (?, ?, ?)',
              ['id', 'name', 'name2'])
          ..execute('delete from ps_crud')
          ..execute('update test set name = name || ?', ['.']);
      }

      test('are not tracked by default', () {
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
  });
}
