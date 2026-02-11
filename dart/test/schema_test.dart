import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

void main() {
  late CommonDatabase db;

  setUp(() async {
    db = openTestDatabase();
  });

  group('Schema Tests', () {
    test('Schema versioning', () {
      // Test that powersync_replace_schema() is a no-op when the schema is not
      // modified.
      db.execute('SELECT powersync_replace_schema(?)', [json.encode(schema)]);

      final [versionBefore] = db.select('PRAGMA schema_version');
      db.execute('SELECT powersync_replace_schema(?)', [json.encode(schema)]);
      final [versionAfter] = db.select('PRAGMA schema_version');

      // No change
      expect(versionAfter['schema_version'],
          equals(versionBefore['schema_version']));

      db.execute('SELECT powersync_replace_schema(?)', [json.encode(schema2)]);
      final [versionAfter2] = db.select('PRAGMA schema_version');

      // Updated
      expect(versionAfter2['schema_version'],
          greaterThan(versionAfter['schema_version'] as int));

      db.execute('SELECT powersync_replace_schema(?)', [json.encode(schema3)]);
      final [versionAfter3] = db.select('PRAGMA schema_version');

      // Updated again (index)
      expect(versionAfter3['schema_version'],
          greaterThan(versionAfter2['schema_version'] as int));
    });

    group('migrate tables', () {
      final local = {
        "tables": [
          {
            "name": "users",
            "local_only": true,
            "insert_only": false,
            "columns": [
              {"name": "name", "type": "TEXT"},
            ],
          },
        ]
      };

      final synced = {
        "tables": [
          {
            "name": "users",
            "local_only": false,
            "insert_only": false,
            "columns": [
              {"name": "name", "type": "TEXT"},
            ],
          },
        ]
      };

      test('from synced to local', () {
        // Start with synced table, and sync row
        db.execute('SELECT powersync_replace_schema(?)', [json.encode(synced)]);
        db.execute(
          'INSERT INTO ps_data__users (id, data) VALUES (?, ?)',
          [
            'synced-id',
            json.encode({'name': 'name'})
          ],
        );

        // Migrate to local table.
        db.execute('SELECT powersync_replace_schema(?)', [json.encode(local)]);

        // The synced table should not exist anymore.
        expect(() => db.select('SELECT * FROM ps_data__users'),
            throwsA(isA<SqliteException>()));

        // Data should still be there.
        expect(db.select('SELECT * FROM ps_untyped'), hasLength(1));
        expect(db.select('SELECT * FROM users'), isEmpty);

        // Inserting into local-only table should not record CRUD item.
        db.execute(
            'INSERT INTO users (id, name) VALUES (uuid(), ?)', ['local']);
        expect(db.select('SELECT * FROM ps_crud'), isEmpty);
      });

      test('from local to synced', () {
        // Start with local table, and local row
        db.execute('SELECT powersync_replace_schema(?)', [json.encode(local)]);
        db.execute(
            'INSERT INTO users (id, name) VALUES (uuid(), ?)', ['local']);

        // Migrate to synced table. Because the previous local write would never
        // get uploaded, this clears local data.
        db.execute('SELECT powersync_replace_schema(?)', [json.encode(synced)]);
        expect(db.select('SELECT * FROM users'), isEmpty);

        // The local table should not exist anymore.
        expect(() => db.select('SELECT * FROM ps_data_local__users'),
            throwsA(isA<SqliteException>()));
      });
    });

    group('metadata', () {
      // This is a special because we have two delete triggers when
      // include_metadata is true (one for actual `DELETE` statements and one
      // for `UPDATE ... SET _deleted = TRUE` that allows attaching metadata).
      Object createSchema(bool withMetadata) {
        return {
          "tables": [
            {
              "name": "customers",
              "view_name": null,
              "local_only": false,
              "insert_only": false,
              "include_metadata": withMetadata,
              "columns": [
                {"name": "name", "type": "TEXT"},
                {"name": "email", "type": "TEXT"}
              ],
              "indexes": []
            },
          ]
        };
      }

      test('enabling', () {
        db.execute('SELECT powersync_replace_schema(?)',
            [json.encode(createSchema(false))]);
        expect(
          db.select("select * from sqlite_schema where type = 'trigger' "
              "AND tbl_name = 'customers' "
              "AND name GLOB 'ps_view_delete*'"),
          hasLength(1),
        );

        db.execute('SELECT powersync_replace_schema(?)',
            [json.encode(createSchema(true))]);
        expect(
          db.select("select * from sqlite_schema where type = 'trigger' "
              "AND tbl_name = 'customers' "
              "AND name GLOB 'ps_view_delete*'"),
          hasLength(2),
        );
      });

      test('unchanged', () {
        final schema = createSchema(true);
        db.execute('SELECT powersync_replace_schema(?)', [json.encode(schema)]);

        final [versionBefore] = db.select('PRAGMA schema_version');
        db.execute('SELECT powersync_replace_schema(?)', [json.encode(schema)]);
        final [versionAfter] = db.select('PRAGMA schema_version');

        expect(versionAfter['schema_version'],
            equals(versionBefore['schema_version']));
      });

      test('disabling', () {
        db.execute('SELECT powersync_replace_schema(?)',
            [json.encode(createSchema(true))]);
        expect(
          db.select("select * from sqlite_schema where type = 'trigger' "
              "AND tbl_name = 'customers' "
              "AND name GLOB 'ps_view_delete*'"),
          hasLength(2),
        );

        db.execute('SELECT powersync_replace_schema(?)',
            [json.encode(createSchema(false))]);
        expect(
          db.select("select * from sqlite_schema where type = 'trigger' "
              "AND tbl_name = 'customers' "
              "AND name GLOB 'ps_view_delete*'"),
          hasLength(1),
        );
      });
    });

    test('raw tables', () {
      db.execute('SELECT powersync_replace_schema(?)', [
        json.encode({
          'raw_tables': [
            {
              'name': 'users',
              'put': {
                'sql': 'INSERT OR REPLACE INTO users (id, name) VALUES (?, ?);',
                'params': [
                  'Id',
                  {'Column': 'name'}
                ],
              },
              'delete': {
                'sql': 'DELETE FROM users WHERE id = ?',
                'params': ['Id'],
              },
            }
          ],
          'tables': [],
        })
      ]);

      expect(
        db.select(
            "SELECT * FROM sqlite_schema WHERE type = 'table' AND name LIKE 'ps_data%'"),
        isEmpty,
      );
    });

    group('triggers for raw tables', () {
      const createUsers =
          'CREATE TABLE users (id TEXT, email TEXT, email_verified INTEGER);';

      const testCases = <_RawTableTestCase>[
        // Default options
        _RawTableTestCase(
          createTable: createUsers,
          tableOptions: {'table_name': 'users'},
          insert: '''
CREATE TRIGGER "test_insert" AFTER INSERT ON "users" FOR EACH ROW WHEN NOT powersync_in_sync_operation() BEGIN
INSERT INTO powersync_crud(op,id,type,data) VALUES ('PUT', NEW.id, 'sync_type', json(powersync_diff('{}', json_object('email', powersync_strip_subtype(NEW."email"), 'email_verified', powersync_strip_subtype(NEW."email_verified")))));
END''',
          update: '''
CREATE TRIGGER "test_update" AFTER UPDATE ON "users" FOR EACH ROW WHEN NOT powersync_in_sync_operation() BEGIN
SELECT CASE WHEN (OLD.id != NEW.id) THEN RAISE (FAIL, 'Cannot update id') END;
INSERT INTO powersync_crud(op,id,type,data,options) VALUES ('PATCH', NEW.id, 'sync_type', json(powersync_diff(json_object('email', powersync_strip_subtype(OLD."email"), 'email_verified', powersync_strip_subtype(OLD."email_verified")), json_object('email', powersync_strip_subtype(NEW."email"), 'email_verified', powersync_strip_subtype(NEW."email_verified")))), 0);
END''',
          delete: '''
CREATE TRIGGER "test_delete" AFTER DELETE ON "users" FOR EACH ROW WHEN NOT powersync_in_sync_operation() BEGIN
INSERT INTO powersync_crud(op,id,type) VALUES ('DELETE', OLD.id, 'sync_type');
END''',
        ),
        // Insert-only
        _RawTableTestCase(
          createTable: createUsers,
          tableOptions: {
            'table_name': 'users',
            'insert_only': true,
          },
          insert: '''
CREATE TRIGGER "test_insert" AFTER INSERT ON "users" FOR EACH ROW WHEN NOT powersync_in_sync_operation() BEGIN
INSERT INTO powersync_crud_(data) VALUES(json_object('op', 'PUT', 'type', 'sync_type', 'id', NEW.id, 'data', json(powersync_diff('{}', json_object('email', powersync_strip_subtype(NEW."email"), 'email_verified', powersync_strip_subtype(NEW."email_verified"))))));END''',
          update: '''
CREATE TRIGGER "test_update" AFTER UPDATE ON "users" FOR EACH ROW WHEN NOT powersync_in_sync_operation() BEGIN
SELECT RAISE(FAIL, 'Unexpected update on insert-only table');
END''',
          delete: '''
CREATE TRIGGER "test_delete" AFTER DELETE ON "users" FOR EACH ROW WHEN NOT powersync_in_sync_operation() BEGIN
SELECT RAISE(FAIL, 'Unexpected update on insert-only table');
END''',
        ),
        // Tracking old values
        _RawTableTestCase(
          createTable: createUsers,
          tableOptions: {
            'table_name': 'users',
            'include_old': true,
          },
          insert: '''
CREATE TRIGGER "test_insert" AFTER INSERT ON "users" FOR EACH ROW WHEN NOT powersync_in_sync_operation() BEGIN
INSERT INTO powersync_crud(op,id,type,data) VALUES ('PUT', NEW.id, 'sync_type', json(powersync_diff('{}', json_object('email', powersync_strip_subtype(NEW."email"), 'email_verified', powersync_strip_subtype(NEW."email_verified")))));
END''',
          update: '''
CREATE TRIGGER "test_update" AFTER UPDATE ON "users" FOR EACH ROW WHEN NOT powersync_in_sync_operation() BEGIN
SELECT CASE WHEN (OLD.id != NEW.id) THEN RAISE (FAIL, 'Cannot update id') END;
INSERT INTO powersync_crud(op,id,type,data,old_values,options) VALUES ('PATCH', NEW.id, 'sync_type', json(powersync_diff(json_object('email', powersync_strip_subtype(OLD."email"), 'email_verified', powersync_strip_subtype(OLD."email_verified")), json_object('email', powersync_strip_subtype(NEW."email"), 'email_verified', powersync_strip_subtype(NEW."email_verified")))), json_object('email', powersync_strip_subtype(OLD."email"), 'email_verified', powersync_strip_subtype(OLD."email_verified")), 0);
END''',
          delete: '''
CREATE TRIGGER "test_delete" AFTER DELETE ON "users" FOR EACH ROW WHEN NOT powersync_in_sync_operation() BEGIN
INSERT INTO powersync_crud(op,id,type,old_values) VALUES ('DELETE', OLD.id, 'sync_type', json_object('email', powersync_strip_subtype(OLD."email"), 'email_verified', powersync_strip_subtype(OLD."email_verified")));
END''',
        ),
      ];

      for (final (i, testCase) in testCases.indexed) {
        test('#$i', () => testCase.testWith(db));
      }
    });
  });
}

final class _RawTableTestCase {
  final String createTable;
  final Map<String, Object?> tableOptions;
  final String insert, update, delete;

  const _RawTableTestCase({
    required this.createTable,
    required this.tableOptions,
    required this.insert,
    required this.update,
    required this.delete,
  });

  void testWith(CommonDatabase db) {
    db.execute(createTable);
    db.execute('''
SELECT
  powersync_create_raw_table_crud_trigger(?1, 'test_insert', 'INSERT'),
  powersync_create_raw_table_crud_trigger(?1, 'test_update', 'UPDATE'),
  powersync_create_raw_table_crud_trigger(?1, 'test_delete', 'DELETE')
''', [
      json.encode({
        'name': 'sync_type',
        'put': {
          'sql': 'unused',
          'params': [],
        },
        'delete': {
          'sql': 'unused',
          'params': [],
        },
        ...tableOptions,
      })
    ]);

    final foundTriggers =
        db.select("SELECT name, sql FROM sqlite_schema WHERE type = 'trigger'");

    // Uncomment to help update expectations
    // for (final row in foundTriggers) {
    //  print(row['sql']);
    // }

    expect(foundTriggers, [
      {'name': 'test_insert', 'sql': insert},
      {'name': 'test_update', 'sql': update},
      {'name': 'test_delete', 'sql': delete},
    ]);
  }
}

final schema = {
  "tables": [
    {
      "name": "assets",
      "view_name": null,
      "local_only": false,
      "insert_only": false,
      "columns": [
        {"name": "created_at", "type": "TEXT"},
        {"name": "make", "type": "TEXT"},
        {"name": "model", "type": "TEXT"},
        {"name": "serial_number", "type": "TEXT"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "user_id", "type": "TEXT"},
        {"name": "weight", "type": "REAL"},
        {"name": "description", "type": "TEXT"}
      ],
      "indexes": [
        {
          "name": "makemodel",
          "columns": [
            {"name": "make", "ascending": true, "type": "TEXT"},
            {"name": "model", "ascending": true, "type": "TEXT"}
          ]
        }
      ]
    },
    {
      "name": "customers",
      "view_name": null,
      "local_only": false,
      "insert_only": false,
      "columns": [
        {"name": "name", "type": "TEXT"},
        {"name": "email", "type": "TEXT"}
      ],
      "indexes": []
    },
    {
      "name": "logs",
      "view_name": null,
      "local_only": false,
      "insert_only": true,
      "columns": [
        {"name": "level", "type": "TEXT"},
        {"name": "content", "type": "TEXT"}
      ],
      "indexes": []
    },
    {
      "name": "credentials",
      "view_name": null,
      "local_only": true,
      "insert_only": false,
      "columns": [
        {"name": "key", "type": "TEXT"},
        {"name": "value", "type": "TEXT"}
      ],
      "indexes": []
    },
    {
      "name": "aliased",
      "view_name": "test1",
      "local_only": false,
      "insert_only": false,
      "columns": [
        {"name": "name", "type": "TEXT"}
      ],
      "indexes": []
    }
  ]
};

final schema2 = {
  "tables": [
    {
      "name": "assets",
      "view_name": null,
      "local_only": false,
      "insert_only": false,
      "columns": [
        {"name": "created_at", "type": "TEXT"},
        {"name": "make", "type": "TEXT"},
        {"name": "model", "type": "TEXT"},
        {"name": "serial_number", "type": "TEXT"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "user_id", "type": "TEXT"},
        {"name": "weights", "type": "REAL"},
        {"name": "description", "type": "TEXT"}
      ],
      "indexes": [
        {
          "name": "makemodel",
          "columns": [
            {"name": "make", "ascending": true, "type": "TEXT"},
            {"name": "model", "ascending": true, "type": "TEXT"}
          ]
        }
      ]
    },
    {
      "name": "customers",
      "view_name": null,
      "local_only": false,
      "insert_only": false,
      "columns": [
        {"name": "name", "type": "TEXT"},
        {"name": "email", "type": "TEXT"}
      ],
      "indexes": []
    },
    {
      "name": "logs",
      "view_name": null,
      "local_only": false,
      "insert_only": true,
      "columns": [
        {"name": "level", "type": "TEXT"},
        {"name": "content", "type": "TEXT"}
      ],
      "indexes": []
    },
    {
      "name": "credentials",
      "view_name": null,
      "local_only": true,
      "insert_only": false,
      "columns": [
        {"name": "key", "type": "TEXT"},
        {"name": "value", "type": "TEXT"}
      ],
      "indexes": []
    },
    {
      "name": "aliased",
      "view_name": "test1",
      "local_only": false,
      "insert_only": false,
      "columns": [
        {"name": "name", "type": "TEXT"}
      ],
      "indexes": []
    }
  ]
};

final schema3 = {
  "tables": [
    {
      "name": "assets",
      "view_name": null,
      "local_only": false,
      "insert_only": false,
      "columns": [
        {"name": "created_at", "type": "TEXT"},
        {"name": "make", "type": "TEXT"},
        {"name": "model", "type": "TEXT"},
        {"name": "serial_number", "type": "TEXT"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "user_id", "type": "TEXT"},
        {"name": "weights", "type": "REAL"},
        {"name": "description", "type": "TEXT"}
      ],
      "indexes": [
        {
          "name": "makemodel",
          "columns": [
            {"name": "make", "ascending": true, "type": "TEXT"},
            {"name": "model", "ascending": false, "type": "TEXT"}
          ]
        }
      ]
    },
    {
      "name": "customers",
      "view_name": null,
      "local_only": false,
      "insert_only": false,
      "columns": [
        {"name": "name", "type": "TEXT"},
        {"name": "email", "type": "TEXT"}
      ],
      "indexes": []
    },
    {
      "name": "logs",
      "view_name": null,
      "local_only": false,
      "insert_only": true,
      "columns": [
        {"name": "level", "type": "TEXT"},
        {"name": "content", "type": "TEXT"}
      ],
      "indexes": []
    },
    {
      "name": "credentials",
      "view_name": null,
      "local_only": true,
      "insert_only": false,
      "columns": [
        {"name": "key", "type": "TEXT"},
        {"name": "value", "type": "TEXT"}
      ],
      "indexes": []
    },
    {
      "name": "aliased",
      "view_name": "test1",
      "local_only": false,
      "insert_only": false,
      "columns": [
        {"name": "name", "type": "TEXT"}
      ],
      "indexes": []
    }
  ]
};
