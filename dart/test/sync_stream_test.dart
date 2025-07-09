import 'dart:convert';

import 'package:file/local.dart';
import 'package:sqlite3/common.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite3_test/sqlite3_test.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';
import 'utils/test_utils.dart';

void main() {
  final vfs = TestSqliteFileSystem(
      fs: const LocalFileSystem(), name: 'vfs-stream-test');

  setUpAll(() {
    loadExtension();
    sqlite3.registerVirtualFileSystem(vfs, makeDefault: false);
  });
  tearDownAll(() => sqlite3.unregisterVirtualFileSystem(vfs));

  late CommonDatabase db;
  Object? lastStatus;

  setUp(() async {
    db = openTestDatabase(vfs: vfs)
      ..select('select powersync_init();')
      ..select('select powersync_replace_schema(?)', [json.encode(testSchema)])
      ..execute('update ps_kv set value = ?2 where key = ?1',
          ['client_id', 'test-test-test-test']);
  });

  tearDown(() {
    db.dispose();
  });

  List<Object?> control(String operation, Object? data) {
    db.execute('begin');
    ResultSet result;

    try {
      result = db.select('SELECT powersync_control(?, ?)', [operation, data]);
    } catch (e) {
      db.execute('rollback');
      rethrow;
    }

    db.execute('commit');
    final [row] = result;
    final instructions = jsonDecode(row.columnAt(0)) as List;
    for (final instruction in instructions) {
      if (instruction case {'UpdateSyncStatus': final status}) {
        lastStatus = status['status']!;
      }
    }

    return instructions;
  }

  group('default streams', () {
    test('are created on-demand', () {
      control('start', null);
      control(
        'line_text',
        json.encode(
          checkpoint(
            lastOpId: 1,
            buckets: [
              bucketDescription('a',
                  subscriptions: 'my_default_stream', priority: 1),
            ],
            streams: [('my_default_stream', true)],
          ),
        ),
      );

      expect(
        lastStatus,
        containsPair(
          'streams',
          [
            {
              'name': 'my_default_stream',
              'parameters': null,
              'associated_buckets': ['a'],
              'active': true,
              'is_default': true,
              'expires_at': null,
              'last_synced_at': null
            }
          ],
        ),
      );
    });
  });
}
