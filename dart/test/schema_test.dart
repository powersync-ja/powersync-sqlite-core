import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

void main() {
  late CommonDatabase db;

  setUp(() async {
    db = openTestDatabase();
  });

  tearDown(() {
    db.dispose();
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
  });
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
