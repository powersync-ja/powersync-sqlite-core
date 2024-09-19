import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

void main() {
  group('Migration Tests', () {
    late CommonDatabase db;

    setUp(() async {
      db = openTestDatabase();
    });

    tearDown(() {
      db.dispose();
    });

    test('extension setup', () async {
      final row = db.select('select powersync_rs_version() as version').first;
      print('version: $row');
    });

    test('match database version', () async {
      // This just tests that the tests are setup correctly
      expect(expectedState.keys.last, equals(databaseVersion));
    });

    test('create database from scratch', () async {
      db.select('select powersync_init()');
      final schema = '${getSchema(db)}\n${getMigrations(db)}';
      expect(schema, equals(finalState.trim()));
    });

    // We test that we can _start_ at any state, and get to
    // the same final state. We don't test individual target migrations.
    for (var startState = 2; startState <= databaseVersion; startState++) {
      test('migrate from $startState', () async {
        db.execute(expectedState[startState]!);
        db.select('select powersync_init()');
        final schema = '${getSchema(db)}\n${getMigrations(db)}';
        expect(schema, equals(finalState.trim()));
      });

      test('migrate from $startState with data1', () async {
        db.execute(expectedState[startState]!);
        db.execute(data1[startState]!);
        db.select('select powersync_init()');
        final data = getData(db);
        expect(data, equals(finalData1.trim()));
      });
    }
    // We test that we can _start_ at any state, and get to
    // the same final state. We don't test individual target migrations.
    for (var endState = 2; endState < databaseVersion; endState++) {
      test('migrate down to $endState', () async {
        db.execute(finalState);
        db.select('select powersync_test_migration(?)', [endState]);
        final schema = '${getSchema(db)}\n${getMigrations(db)}';
        expect(schema, equals(expectedState[endState]!.trim()));
      });

      test('migrate down to $endState with data1', () async {
        db.execute(finalState);
        db.execute(data1[databaseVersion]!);
        db.select('select powersync_test_migration(?)', [endState]);
        final data = getData(db);
        expect(data, equals(dataDown1[endState]!.trim()));
      });
    }

    test('schema after migration', () async {
      db.execute(expectedState[2]!);
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
          '${finalState.replaceAll(RegExp(r';INSERT INTO ps_migration.*'), '').trim()}\n${views5.trim()}';
      expect(schema, equals(expected));
    });

    test('schema 3 -> 5', () async {
      db.execute(expectedState[3]!);
      db.execute(schema3);
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
          '${finalState.replaceAll(RegExp(r';INSERT INTO ps_migration.*'), '').trim()}\n${views5.trim()}';
      expect(schema, equals(expected));
    });

    test('schema 5 -> 4', () async {
      db.execute(expectedState[5]!);
      db.execute(views5);
      db.select('select powersync_test_migration(4)');

      final schema = getSchema(db);
      final expected =
          '${expectedState[4]!.replaceAll(RegExp(r';INSERT INTO ps_migration.*'), '').trim()}\n${schemaDown3.trim()}';
      expect(schema, equals(expected));
    });

    test('schema 5 -> 3', () async {
      db.execute(expectedState[5]!);
      db.execute(views5);
      db.select('select powersync_test_migration(3)');

      final schema = getSchema(db);
      final expected =
          '${expectedState[3]!.replaceAll(RegExp(r';INSERT INTO ps_migration.*'), '').trim()}\n${schemaDown3.trim()}';
      expect(schema, equals(expected));
    });
  });
}

String getSchema(CommonDatabase db) {
  final rows = db.select("""
SELECT type, name, sql FROM sqlite_master ORDER BY
  CASE
    WHEN type = 'table' AND name LIKE 'ps_data_%' THEN 3
    WHEN type = 'table' THEN 1
    WHEN type = 'index' THEN 2
    WHEN type = 'view' THEN 4
    WHEN type = 'trigger' THEN 5
  END ASC, name ASC""");

  List<String> result = [];
  for (var row in rows) {
    if (row['name'].startsWith('__') || row['name'] == 'sqlite_sequence') {
      // Internal SQLite tables.
      continue;
    }
    if (row['sql'] != null) {
      var sql = (row['sql'] as String).trim();
      // We put a semicolon before each statement instead of after,
      // so that comments at the end of the statement are preserved.
      result.add(';$sql');
    }
  }
  return result.join('\n');
}

String getMigrations(CommonDatabase db) {
  List<String> result = [];
  var migrationRows =
      db.select('SELECT id, down_migrations FROM ps_migration ORDER BY id ASC');

  for (var row in migrationRows) {
    var version = row['id']!;
    var downMigrations = row['down_migrations'];
    if (downMigrations == null) {
      result.add(
          ';INSERT INTO ps_migration(id, down_migrations) VALUES($version, null)');
    } else {
      result.add(
          ';INSERT INTO ps_migration(id, down_migrations) VALUES($version, ${escapeSqlString(downMigrations)})');
    }
  }
  return result.join('\n');
}

String getData(CommonDatabase db) {
  const queries = [
    {'table': 'ps_buckets', 'query': 'select * from ps_buckets order by name'},
    {
      'table': 'ps_oplog',
      'query': 'select * from ps_oplog order by bucket, op_id'
    },
    {
      'table': 'ps_updated_rows',
      'query': 'select * from ps_updated_rows order by row_type, row_id'
    }
  ];
  List<String> result = [];
  for (var q in queries) {
    try {
      final rs = db.select(q['query']!);
      if (rs.isEmpty) {
        continue;
      }

      result.add(
          ';INSERT INTO ${q['table']}(${rs.columnNames.join(', ')}) VALUES');
      var values = rs.rows
          .map((row) =>
              '(${row.map((column) => escapeSqlValue(column)).join(', ')})')
          .join(',\n  ');
      result.add('  $values');
    } catch (e) {
      if (e.toString().contains('no such table')) {
        // Table doesn't exist - ignore
      } else {
        rethrow;
      }
    }
  }
  return result.join('\n');
}

String escapeSqlValue(dynamic value) {
  if (value == null) {
    return 'null';
  } else if (value is String) {
    return escapeSqlString(value);
  } else if (value is int) {
    return '$value';
  } else {
    throw ArgumentError('Unsupported value type: $value');
  }
}

/// Quote a string for usage in a SQLite query.
///
/// Not safe for general usage, but should be sufficient for these tests.
String escapeSqlString(String text) {
  return """'${text.replaceAll(RegExp(r"'"), "''")}'""";
}

final databaseVersion = 5;

final expectedState = <int, String>{
  2: '''
;CREATE TABLE ps_buckets(
  name TEXT PRIMARY KEY,
  last_applied_op INTEGER NOT NULL DEFAULT 0,
  last_op INTEGER NOT NULL DEFAULT 0,
  target_op INTEGER NOT NULL DEFAULT 0,
  add_checksum INTEGER NOT NULL DEFAULT 0,
  pending_delete INTEGER NOT NULL DEFAULT 0
)
;CREATE TABLE ps_crud (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, tx_id INTEGER)
;CREATE TABLE ps_migration(id INTEGER PRIMARY KEY, down_migrations TEXT)
;CREATE TABLE ps_oplog(
  bucket TEXT NOT NULL,
  op_id INTEGER NOT NULL,
  op INTEGER NOT NULL,
  row_type TEXT,
  row_id TEXT,
  key TEXT,
  data TEXT,
  hash INTEGER NOT NULL,
  superseded INTEGER NOT NULL)
;CREATE TABLE ps_tx(id INTEGER PRIMARY KEY NOT NULL, current_tx INTEGER, next_tx INTEGER)
;CREATE TABLE ps_untyped(type TEXT NOT NULL, id TEXT NOT NULL, data TEXT, PRIMARY KEY (type, id))
;CREATE INDEX ps_oplog_by_key ON ps_oplog (bucket, key) WHERE superseded = 0
;CREATE INDEX ps_oplog_by_opid ON ps_oplog (bucket, op_id)
;CREATE INDEX ps_oplog_by_row ON ps_oplog (row_type, row_id) WHERE superseded = 0
;INSERT INTO ps_migration(id, down_migrations) VALUES(1, null)
;INSERT INTO ps_migration(id, down_migrations) VALUES(2, '[{"sql":"DELETE FROM ps_migration WHERE id >= 2","params":[]},{"sql":"DROP TABLE ps_tx","params":[]},{"sql":"ALTER TABLE ps_crud DROP COLUMN tx_id","params":[]}]')
''',
  3: '''
;CREATE TABLE ps_buckets(
  name TEXT PRIMARY KEY,
  last_applied_op INTEGER NOT NULL DEFAULT 0,
  last_op INTEGER NOT NULL DEFAULT 0,
  target_op INTEGER NOT NULL DEFAULT 0,
  add_checksum INTEGER NOT NULL DEFAULT 0,
  pending_delete INTEGER NOT NULL DEFAULT 0
)
;CREATE TABLE ps_crud (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, tx_id INTEGER)
;CREATE TABLE ps_kv(key TEXT PRIMARY KEY NOT NULL, value BLOB)
;CREATE TABLE ps_migration(id INTEGER PRIMARY KEY, down_migrations TEXT)
;CREATE TABLE ps_oplog(
  bucket TEXT NOT NULL,
  op_id INTEGER NOT NULL,
  op INTEGER NOT NULL,
  row_type TEXT,
  row_id TEXT,
  key TEXT,
  data TEXT,
  hash INTEGER NOT NULL,
  superseded INTEGER NOT NULL)
;CREATE TABLE ps_tx(id INTEGER PRIMARY KEY NOT NULL, current_tx INTEGER, next_tx INTEGER)
;CREATE TABLE ps_untyped(type TEXT NOT NULL, id TEXT NOT NULL, data TEXT, PRIMARY KEY (type, id))
;CREATE INDEX ps_oplog_by_key ON ps_oplog (bucket, key) WHERE superseded = 0
;CREATE INDEX ps_oplog_by_opid ON ps_oplog (bucket, op_id)
;CREATE INDEX ps_oplog_by_row ON ps_oplog (row_type, row_id) WHERE superseded = 0
;INSERT INTO ps_migration(id, down_migrations) VALUES(1, null)
;INSERT INTO ps_migration(id, down_migrations) VALUES(2, '[{"sql":"DELETE FROM ps_migration WHERE id >= 2","params":[]},{"sql":"DROP TABLE ps_tx","params":[]},{"sql":"ALTER TABLE ps_crud DROP COLUMN tx_id","params":[]}]')
;INSERT INTO ps_migration(id, down_migrations) VALUES(3, '[{"sql":"DELETE FROM ps_migration WHERE id >= 3"},{"sql":"DROP TABLE ps_kv"}]')
''',
  4: '''
;CREATE TABLE ps_buckets(
  name TEXT PRIMARY KEY,
  last_applied_op INTEGER NOT NULL DEFAULT 0,
  last_op INTEGER NOT NULL DEFAULT 0,
  target_op INTEGER NOT NULL DEFAULT 0,
  add_checksum INTEGER NOT NULL DEFAULT 0,
  pending_delete INTEGER NOT NULL DEFAULT 0
, op_checksum INTEGER NOT NULL DEFAULT 0, remove_operations INTEGER NOT NULL DEFAULT 0)
;CREATE TABLE ps_crud (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, tx_id INTEGER)
;CREATE TABLE ps_kv(key TEXT PRIMARY KEY NOT NULL, value BLOB)
;CREATE TABLE ps_migration(id INTEGER PRIMARY KEY, down_migrations TEXT)
;CREATE TABLE ps_oplog(
  bucket TEXT NOT NULL,
  op_id INTEGER NOT NULL,
  op INTEGER NOT NULL,
  row_type TEXT,
  row_id TEXT,
  key TEXT,
  data TEXT,
  hash INTEGER NOT NULL,
  superseded INTEGER NOT NULL)
;CREATE TABLE ps_tx(id INTEGER PRIMARY KEY NOT NULL, current_tx INTEGER, next_tx INTEGER)
;CREATE TABLE ps_untyped(type TEXT NOT NULL, id TEXT NOT NULL, data TEXT, PRIMARY KEY (type, id))
;CREATE INDEX ps_oplog_by_key ON ps_oplog (bucket, key) WHERE superseded = 0
;CREATE INDEX ps_oplog_by_opid ON ps_oplog (bucket, op_id)
;CREATE INDEX ps_oplog_by_row ON ps_oplog (row_type, row_id) WHERE superseded = 0
;INSERT INTO ps_migration(id, down_migrations) VALUES(1, null)
;INSERT INTO ps_migration(id, down_migrations) VALUES(2, '[{"sql":"DELETE FROM ps_migration WHERE id >= 2","params":[]},{"sql":"DROP TABLE ps_tx","params":[]},{"sql":"ALTER TABLE ps_crud DROP COLUMN tx_id","params":[]}]')
;INSERT INTO ps_migration(id, down_migrations) VALUES(3, '[{"sql":"DELETE FROM ps_migration WHERE id >= 3"},{"sql":"DROP TABLE ps_kv"}]')
;INSERT INTO ps_migration(id, down_migrations) VALUES(4, '[{"sql":"DELETE FROM ps_migration WHERE id >= 4"},{"sql":"ALTER TABLE ps_buckets DROP COLUMN op_checksum"},{"sql":"ALTER TABLE ps_buckets DROP COLUMN remove_operations"}]')
''',
  5: '''
;CREATE TABLE ps_buckets(
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    last_applied_op INTEGER NOT NULL DEFAULT 0,
    last_op INTEGER NOT NULL DEFAULT 0,
    target_op INTEGER NOT NULL DEFAULT 0,
    add_checksum INTEGER NOT NULL DEFAULT 0,
    op_checksum INTEGER NOT NULL DEFAULT 0,
    pending_delete INTEGER NOT NULL DEFAULT 0
  ) STRICT
;CREATE TABLE ps_crud (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, tx_id INTEGER)
;CREATE TABLE ps_kv(key TEXT PRIMARY KEY NOT NULL, value BLOB)
;CREATE TABLE ps_migration(id INTEGER PRIMARY KEY, down_migrations TEXT)
;CREATE TABLE ps_oplog(
  bucket INTEGER NOT NULL,
  op_id INTEGER NOT NULL,
  row_type TEXT,
  row_id TEXT,
  key TEXT,
  data TEXT,
  hash INTEGER NOT NULL) STRICT
;CREATE TABLE ps_tx(id INTEGER PRIMARY KEY NOT NULL, current_tx INTEGER, next_tx INTEGER)
;CREATE TABLE ps_untyped(type TEXT NOT NULL, id TEXT NOT NULL, data TEXT, PRIMARY KEY (type, id))
;CREATE TABLE ps_updated_rows(
  row_type TEXT,
  row_id TEXT) STRICT
;CREATE UNIQUE INDEX ps_buckets_name ON ps_buckets (name)
;CREATE INDEX ps_oplog_key ON ps_oplog (bucket, key)
;CREATE INDEX ps_oplog_opid ON ps_oplog (bucket, op_id)
;CREATE INDEX ps_oplog_row ON ps_oplog (row_type, row_id)
;CREATE UNIQUE INDEX ps_updated_rows_row ON ps_updated_rows (row_type, row_id)
;INSERT INTO ps_migration(id, down_migrations) VALUES(1, null)
;INSERT INTO ps_migration(id, down_migrations) VALUES(2, '[{"sql":"DELETE FROM ps_migration WHERE id >= 2","params":[]},{"sql":"DROP TABLE ps_tx","params":[]},{"sql":"ALTER TABLE ps_crud DROP COLUMN tx_id","params":[]}]')
;INSERT INTO ps_migration(id, down_migrations) VALUES(3, '[{"sql":"DELETE FROM ps_migration WHERE id >= 3"},{"sql":"DROP TABLE ps_kv"}]')
;INSERT INTO ps_migration(id, down_migrations) VALUES(4, '[{"sql":"DELETE FROM ps_migration WHERE id >= 4"},{"sql":"ALTER TABLE ps_buckets DROP COLUMN op_checksum"},{"sql":"ALTER TABLE ps_buckets DROP COLUMN remove_operations"}]')
;INSERT INTO ps_migration(id, down_migrations) VALUES(5, '[{"sql":"DELETE FROM ps_migration WHERE id >= 5"}]')
'''
};

final finalState = expectedState[databaseVersion]!;

/// data for "up" migrations
final data1 = <int, String>{
  2: '''
;INSERT INTO ps_buckets(name, last_applied_op, last_op, target_op, add_checksum, pending_delete) VALUES
  ('b1', 0, 0, 0, 0, 0),
  ('b2', 0, 0, 0, 1000, 0)
;INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, key, data, hash, superseded) VALUES
  ('b1', 1, 3, 'todos', 't1', '', '{}', 100, 0),
  ('b1', 2, 3, 'todos', 't2', '', '{}', 20, 0),
  ('b2', 3, 3, 'lists', 'l1', '', '{}', 3, 0),
  ('b2', 4, 4, 'lists', 'l2', '', null, 5, 0)
''',
  3: '''
;INSERT INTO ps_buckets(name, last_applied_op, last_op, target_op, add_checksum, pending_delete) VALUES
  ('b1', 0, 0, 0, 0, 0),
  ('b2', 0, 0, 0, 1000, 0)
;INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, key, data, hash, superseded) VALUES
  ('b1', 1, 3, 'todos', 't1', '', '{}', 100, 0),
  ('b1', 2, 3, 'todos', 't2', '', '{}', 20, 0),
  ('b2', 3, 3, 'lists', 'l1', '', '{}', 3, 0),
  ('b2', 4, 4, 'lists', 'l2', '', null, 5, 0)
''',
  4: '''
;INSERT INTO ps_buckets(name, last_applied_op, last_op, target_op, add_checksum, op_checksum, pending_delete) VALUES
  ('b1', 0, 0, 0, 0, 120, 0),
  ('b2', 0, 0, 0, 1000, 8, 0)
;INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, key, data, hash, superseded) VALUES
  ('b1', 1, 3, 'todos', 't1', '', '{}', 100, 0),
  ('b1', 2, 3, 'todos', 't2', '', '{}', 20, 0),
  ('b2', 3, 3, 'lists', 'l1', '', '{}', 3, 0),
  ('b2', 4, 4, 'lists', 'l2', '', null, 5, 0)
''',
  5: '''
;INSERT INTO ps_buckets(id, name, last_applied_op, last_op, target_op, add_checksum, op_checksum, pending_delete) VALUES
  (1, 'b1', 0, 0, 0, 0, 120, 0),
  (2, 'b2', 0, 0, 0, 1005, 3, 0)
;INSERT INTO ps_oplog(bucket, op_id, row_type, row_id, key, data, hash) VALUES
  (1, 1, 'todos', 't1', '', '{}', 100),
  (1, 2, 'todos', 't2', '', '{}', 20),
  (2, 3, 'lists', 'l1', '', '{}', 3)
;INSERT INTO ps_updated_rows(row_type, row_id) VALUES
  ('lists', 'l2')
'''
};

/// data for "down" migrations
/// This is slightly different from the above,
/// since we don't preserve all data in the up migration process
final dataDown1 = <int, String>{
  2: '''
;INSERT INTO ps_buckets(name, last_applied_op, last_op, target_op, add_checksum, pending_delete) VALUES
  ('\$local', 0, 0, 9223372036854775807, 0, 1),
  ('b1', 0, 0, 0, 0, 0),
  ('b2', 0, 0, 0, 1005, 0)
;INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, key, data, hash, superseded) VALUES
  ('\$local', 1, 4, 'lists', 'l2', null, null, 0, 0),
  ('b1', 1, 3, 'todos', 't1', '', '{}', 100, 0),
  ('b1', 2, 3, 'todos', 't2', '', '{}', 20, 0),
  ('b2', 3, 3, 'lists', 'l1', '', '{}', 3, 0)
''',
  3: '''
;INSERT INTO ps_buckets(name, last_applied_op, last_op, target_op, add_checksum, pending_delete) VALUES
  ('\$local', 0, 0, 9223372036854775807, 0, 1),
  ('b1', 0, 0, 0, 0, 0),
  ('b2', 0, 0, 0, 1005, 0)
;INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, key, data, hash, superseded) VALUES
  ('\$local', 1, 4, 'lists', 'l2', null, null, 0, 0),
  ('b1', 1, 3, 'todos', 't1', '', '{}', 100, 0),
  ('b1', 2, 3, 'todos', 't2', '', '{}', 20, 0),
  ('b2', 3, 3, 'lists', 'l1', '', '{}', 3, 0)
''',
  4: '''
;INSERT INTO ps_buckets(name, last_applied_op, last_op, target_op, add_checksum, pending_delete, op_checksum, remove_operations) VALUES
  ('\$local', 0, 0, 9223372036854775807, 0, 1, 0, 0),
  ('b1', 0, 0, 0, 0, 0, 120, 0),
  ('b2', 0, 0, 0, 1005, 0, 3, 0)
;INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, key, data, hash, superseded) VALUES
  ('\$local', 1, 4, 'lists', 'l2', null, null, 0, 0),
  ('b1', 1, 3, 'todos', 't1', '', '{}', 100, 0),
  ('b1', 2, 3, 'todos', 't2', '', '{}', 20, 0),
  ('b2', 3, 3, 'lists', 'l1', '', '{}', 3, 0)
'''
};

final finalData1 = data1[databaseVersion]!;

final schema3 = '''
;CREATE TABLE "ps_data__lists"(id TEXT PRIMARY KEY NOT NULL, data TEXT)
;CREATE VIEW "lists"("id", "description") AS SELECT id, CAST(json_extract(data, '\$.description') as TEXT) FROM "ps_data__lists" -- powersync-auto-generated
;CREATE TRIGGER "ps_view_delete_lists"
INSTEAD OF DELETE ON "lists"
FOR EACH ROW
BEGIN
DELETE FROM "ps_data__lists" WHERE id = OLD.id;
INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'DELETE', 'type', 'lists', 'id', OLD.id));
INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, hash, superseded)
      SELECT '\$local',
              1,
              'REMOVE',
              'lists',
              OLD.id,
              0,
              0;
INSERT OR REPLACE INTO ps_buckets(name, pending_delete, last_op, target_op) VALUES('\$local', 1, 0, 9223372036854775807);
END
;CREATE TRIGGER "ps_view_insert_lists"
    INSTEAD OF INSERT ON "lists"
    FOR EACH ROW
    BEGIN
      SELECT CASE
      WHEN (NEW.id IS NULL)
      THEN RAISE (FAIL, 'id is required')
      END;
      INSERT INTO "ps_data__lists"
      SELECT NEW.id, json_object('description', NEW."description");
      INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PUT', 'type', 'lists', 'id', NEW.id, 'data', json(powersync_diff('{}', json_object('description', NEW."description")))));
      INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, hash, superseded)
      SELECT '\$local',
              1,
              'REMOVE',
              'lists',
              NEW.id,
              0,
              0;
      INSERT OR REPLACE INTO ps_buckets(name, pending_delete, last_op, target_op) VALUES('\$local', 1, 0, 9223372036854775807);
    END
;CREATE TRIGGER "ps_view_update_lists"
INSTEAD OF UPDATE ON "lists"
FOR EACH ROW
BEGIN
  SELECT CASE
  WHEN (OLD.id != NEW.id)
  THEN RAISE (FAIL, 'Cannot update id')
  END;
  UPDATE "ps_data__lists"
      SET data = json_object('description', NEW."description")
      WHERE id = NEW.id;
  INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PATCH', 'type', 'lists', 'id', NEW.id, 'data', json(powersync_diff(json_object('description', OLD."description"), json_object('description', NEW."description")))));
  INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, hash, superseded)
  SELECT '\$local',
          1,
          'REMOVE',
          'lists',
          NEW.id,
          0,
          0;
  INSERT OR REPLACE INTO ps_buckets(name, pending_delete, last_op, target_op) VALUES('\$local', 1, 0, 9223372036854775807);
END
''';

final schemaDown3 = '''
;CREATE TABLE "ps_data__lists"(id TEXT PRIMARY KEY NOT NULL, data TEXT)
''';

final views5 = '''
;CREATE TABLE "ps_data__lists"(id TEXT PRIMARY KEY NOT NULL, data TEXT)
;CREATE VIEW "lists"("id", "description") AS SELECT id, CAST(json_extract(data, '\$.description') as TEXT) FROM "ps_data__lists" -- powersync-auto-generated
;CREATE TRIGGER "ps_view_delete_lists"
INSTEAD OF DELETE ON "lists"
FOR EACH ROW
BEGIN
DELETE FROM "ps_data__lists" WHERE id = OLD.id;
INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'DELETE', 'type', 'lists', 'id', OLD.id));
INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id) VALUES('lists', OLD.id);
INSERT OR REPLACE INTO ps_buckets(name, last_op, target_op) VALUES('\$local', 0, 9223372036854775807);
END
;CREATE TRIGGER "ps_view_insert_lists"
    INSTEAD OF INSERT ON "lists"
    FOR EACH ROW
    BEGIN
      SELECT CASE
      WHEN (NEW.id IS NULL)
      THEN RAISE (FAIL, 'id is required')
      END;
      INSERT INTO "ps_data__lists"
      SELECT NEW.id, json_object('description', NEW."description");
      INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PUT', 'type', 'lists', 'id', NEW.id, 'data', json(powersync_diff('{}', json_object('description', NEW."description")))));
      INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id) VALUES('lists', NEW.id);
      INSERT OR REPLACE INTO ps_buckets(name, last_op, target_op) VALUES('\$local', 0, 9223372036854775807);
    END
;CREATE TRIGGER "ps_view_update_lists"
INSTEAD OF UPDATE ON "lists"
FOR EACH ROW
BEGIN
  SELECT CASE
  WHEN (OLD.id != NEW.id)
  THEN RAISE (FAIL, 'Cannot update id')
  END;
  UPDATE "ps_data__lists"
      SET data = json_object('description', NEW."description")
      WHERE id = NEW.id;
  INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PATCH', 'type', 'lists', 'id', NEW.id, 'data', json(powersync_diff(json_object('description', OLD."description"), json_object('description', NEW."description")))));
  INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id) VALUES('lists', NEW.id);
  INSERT OR REPLACE INTO ps_buckets(name, last_op, target_op) VALUES('\$local', 0, 9223372036854775807);
END
''';
