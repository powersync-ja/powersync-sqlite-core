import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';
import 'utils/migration_fixtures.dart' as fixtures;
import 'utils/fix_035_fixtures.dart' as fix035;
import 'utils/schema.dart';

void main() {
  group('Migration Tests', () {
    /// These tests test up and down migrations between various schema versions.
    /// We hardcode the starting schema state of older library versions.
    /// Note that down migrations are "simulated" using the current library version,
    /// which could in theory be different from actually running the down
    /// migrations on older library versions.

    late CommonDatabase db;

    setUp(() async {
      db = openTestDatabase();
    });

    /// This tests that the extension can load
    test('extension setup', () async {
      final row1 = db.select('select sqlite_version() as version').first;
      print('sqlite ${row1['version']}');
      final row = db.select('select powersync_rs_version() as version').first;
      print('powersync-sqlite-core ${row['version']}');
    });

    /// This tests that the tests are setup correctly
    test('match database version', () async {
      expect(
          fixtures.expectedState.keys.last, equals(fixtures.databaseVersion));
    });

    /// Test the basic database setup (no migrations from other versions).
    /// Get this test passing before any others below, since it tests the
    /// finalState fixture, which is an input in other tests.
    test('create database from scratch', () async {
      db.select('select powersync_init()');
      final schema = '${getSchema(db)}\n${getMigrations(db)}';
      final expected = fixtures.finalState.trim();
      if (expected != schema) {
        // This gives more usable output if the test fails
        print('-- CURRENT SCHEMA:');
        print(schema);
      }
      expect(schema, equals(expected));
    });

    // We test that we can _start_ at any state, and get to
    // the same final state. We don't test individual target migrations.
    for (var startState = 2;
        startState <= fixtures.databaseVersion;
        startState++) {
      /// This tests with just the base tables
      test('migrate from $startState', () async {
        db.execute(fixtures.expectedState[startState]!);
        db.select('select powersync_init()');
        final schema = '${getSchema(db)}\n${getMigrations(db)}';
        expect(schema, equals(fixtures.finalState.trim()));
      });

      /// This tests with some data
      test('migrate from $startState with data1', () async {
        db.execute(fixtures.expectedState[startState]!);
        db.execute(fixtures.data1[startState]!);
        db.select('select powersync_init()');
        final data = getData(db);
        expect(data, equals(fixtures.finalData1.trim()));
      });
    }

    for (var endState = 2; endState < fixtures.databaseVersion; endState++) {
      /// Test that we can _start_ at the final state, and down migrate to
      /// any version.

      /// This tests with just the base tables
      test('migrate down to $endState', () async {
        db.execute(fixtures.finalState);
        db.select('select powersync_test_migration(?)', [endState]);
        final schema = '${getSchema(db)}\n${getMigrations(db)}';
        expect(schema, equals(fixtures.expectedState[endState]!.trim()));
      });

      /// This tests with some data
      test('migrate down to $endState with data1', () async {
        db.execute(fixtures.finalState);
        db.execute(fixtures.data1[fixtures.databaseVersion]!);
        db.select('select powersync_test_migration(?)', [endState]);
        final data = getData(db);
        expect(data, equals(fixtures.dataDown1[endState]!.trim()));
      });
    }

    /// Here we apply a developer schema _after_ migrating
    test('schema after migration', () async {
      db.execute(fixtures.expectedState[2]!);
      var tableSchema = {
        'tables': [
          {
            'name': 'lists',
            'columns': [
              {'name': 'description', 'type': 'TEXT'}
            ]
          }
        ]
      };
      db.select('select powersync_init()');
      db.select(
          'select powersync_replace_schema(?)', [jsonEncode(tableSchema)]);

      final schema = getSchema(db);
      final expected =
          '${fixtures.finalState.replaceAll(RegExp(r';INSERT INTO ps_migration.*'), '').trim()}\n${fixtures.currentDeveloperSchema.trim()}';
      expect(schema, equals(expected));
    });

    /// Here we start with a schema from fixtures on db version 3
    test('schema 3 -> 5', () async {
      db.execute(fixtures.expectedState[3]!);
      db.execute(fixtures.schema3);
      var tableSchema = {
        'tables': [
          {
            'name': 'lists',
            'columns': [
              {'name': 'description', 'type': 'TEXT'}
            ]
          }
        ]
      };
      db.select('select powersync_init()');
      db.select(
          'select powersync_replace_schema(?)', [jsonEncode(tableSchema)]);

      final schema = getSchema(db);
      final expected =
          '${fixtures.finalState.replaceAll(RegExp(r';INSERT INTO ps_migration.*'), '').trim()}\n${fixtures.schema5.trim()}';
      expect(schema, equals(expected));
    });

    /// Here we start with a schema from fixtures on db version 5,
    /// and down migrate to version 4.
    test('schema 5 -> 4', () async {
      db.execute(fixtures.expectedState[5]!);
      db.execute(fixtures.schema5);
      db.select('select powersync_test_migration(4)');

      final schema = getSchema(db);
      // Note that this schema contains no views - those are deleted during the migration
      final expected =
          '${fixtures.expectedState[4]!.replaceAll(RegExp(r';INSERT INTO ps_migration.*'), '').trim()}\n${fixtures.schemaDown3.trim()}';
      expect(schema, equals(expected));
    });

    /// Here we start with a schema from fixtures on db version 5,
    /// and down migrate to version 3.
    /// While the schema for views and triggers is the same from version 2 -> 4,
    /// some errors only occurred when going down two versions.
    test('schema 5 -> 3', () async {
      db.execute(fixtures.expectedState[5]!);
      db.execute(fixtures.schema5);
      db.select('select powersync_test_migration(3)');

      final schema = getSchema(db);
      // Note that this schema contains no views - those are deleted during the migration
      final expected =
          '${fixtures.expectedState[3]!.replaceAll(RegExp(r';INSERT INTO ps_migration.*'), '').trim()}\n${fixtures.schemaDown3.trim()}';
      expect(schema, equals(expected));
    });

    test('migrate from 5 with broken data', () async {
      var tableSchema = {
        'tables': [
          {
            'name': 'lists',
            'columns': [
              {'name': 'description', 'type': 'TEXT'}
            ]
          },
          {
            'name': 'todos',
            'columns': [
              {'name': 'description', 'type': 'TEXT'}
            ]
          }
        ]
      };
      db.select('select powersync_init()');
      db.select(
          'select powersync_replace_schema(?)', [jsonEncode(tableSchema)]);

      db.select('select powersync_test_migration(5)');
      db.execute(fix035.dataBroken);

      db.select('select powersync_init()');
      final data = getData(db);
      expect(data, equals(fix035.dataMigrated.trim()));

      db.select('insert into powersync_operations(op, data) values(?, ?)',
          ['sync_local', '']);

      final data2 = getData(db);
      expect(data2, equals(fix035.dataFixed.trim()));
    });

    /// Here we start with a schema from fixtures on db version 3
    test('schema 3 -> 5 with custom triggers', () async {
      db.execute(fixtures.expectedState[3]!);
      db.execute(fixtures.schema3);
      // This is a contrived example, but gives us a trigger
      // that references the "lists" view, affecting migrations.
      db.execute('''
create trigger t1 after delete on ps_data__lists begin
  insert into lists(id, description) values(OLD.id, 'deleted');
end''');

      var tableSchema = {
        'tables': [
          {
            'name': 'lists',
            'columns': [
              {'name': 'description', 'type': 'TEXT'}
            ]
          }
        ]
      };
      db.select('select powersync_init()');
      db.select(
          'select powersync_replace_schema(?)', [jsonEncode(tableSchema)]);

      final schema = getSchema(db);
      final expected =
          '''${fixtures.finalState.replaceAll(RegExp(r';INSERT INTO ps_migration.*'), '').trim()}
${fixtures.schema5.trim()}
;CREATE TRIGGER t1 after delete on ps_data__lists begin
  insert into lists(id, description) values(OLD.id, \'deleted\');
end''';
      expect(schema, equals(expected));
    });

    test('schema 7 -> 8 migrates last_synced_at data', () {
      db.execute(fixtures.expectedState[7]!);

      for (var i = 0; i < 10; i++) {
        db.execute(
          'INSERT OR REPLACE INTO ps_sync_state (priority, last_synced_at) VALUES (?, ?);',
          [2147483647, '2025-03-05 14:58:${i.toString().padLeft(2, '0')}'],
        );

        db.execute(
          'INSERT OR REPLACE INTO ps_sync_state (priority, last_synced_at) VALUES (?, ?);',
          [3, '2025-03-05 13:58:${i.toString().padLeft(2, '0')}'],
        );
      }

      db.execute('SELECT powersync_test_migration(8);');

      expect(db.select('SELECT * FROM ps_sync_state ORDER BY priority'), [
        {
          'priority': 3,
          'last_synced_at': '2025-03-05 13:58:09',
        },
        {
          'priority': 2147483647,
          'last_synced_at': '2025-03-05 14:58:09',
        }
      ]);
    });
  });
}
