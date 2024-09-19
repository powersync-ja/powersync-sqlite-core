import 'package:sqlite3/common.dart';

/// Utilities for getting the SQLite schema

/// Get tables, indexes, views and triggers, as one big string
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

/// Get data from the ps_migration table
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

/// Get data from specific tables, as INSERT INTO statements.
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
              '(${row.map((column) => escapeSqlLiteral(column)).join(', ')})')
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

/// Escape an integer, string or null value as a literal for a query.
String escapeSqlLiteral(dynamic value) {
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
