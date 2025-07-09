import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';
import 'utils/test_utils.dart';

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
          'powersync_replace_schema: internal SQLite call returned ERROR: malformed JSON',
        )),
      );
    });

    test('missing client id', () {
      db
        ..execute('SELECT powersync_init()')
        ..execute('DELETE FROM ps_kv;');

      expect(
        () => db.execute('SELECT powersync_client_id()'),
        throwsA(isSqliteException(
          4,
          'powersync_client_id: No client_id found in ps_kv',
        )),
      );
    });

    group('sync protocol', () {
      setUp(() => db.execute('SELECT powersync_init()'));

      test('invalid json', () {
        const stmt = 'SELECT powersync_control(?,?)';
        db.execute('BEGIN');
        final control = db.prepare(stmt);

        control.execute(['start', null]);
        expect(
          () => control.execute(['line_text', 'invalid sync line']),
          throwsA(isSqliteException(
            4,
            'powersync_control: Sync protocol error: invalid text line. cause: expected value at line 1 column 1',
          )),
        );
      });
    });
  });
}
