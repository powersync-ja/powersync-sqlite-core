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
      // Create a wrong migrations table for the core extension to trip over.
      db.execute('CREATE TABLE IF NOT EXISTS ps_migration(foo TEXT)');

      expect(
        () => db.execute('SELECT powersync_init()'),
        throwsA(isSqliteException(
          1,
          'powersync_init: internal SQLite call returned ERROR: no such column: id',
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
