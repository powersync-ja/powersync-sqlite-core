import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

void main() {
  group('error reporting', () {
    late CommonDatabase db;

    setUp(() async {
      db = openTestDatabase();
    });

    tearDown(() {
      db.dispose();
    });

    test('contain inner SQLite descriptions', () {
      expect(
        () => db.execute('SELECT powersync_replace_schema(?)', [
          json.encode({
            // This fails because we're trying to json_extract from the string
            // in e.g. update_tables.
            'tables': ['invalid entry'],
          })
        ]),
        throwsA(isSqliteException(
          1,
          'powersync_replace_schema: internal SQLite call returned ERROR',
        )),
      );
    });
  });
}

Matcher isSqliteException(int code, String message) {
  return isA<SqliteException>()
      .having((e) => e.extendedResultCode, 'extendedResultCode', code)
      .having((e) => e.message, 'message', message);
}
