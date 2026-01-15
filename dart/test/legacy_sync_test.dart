import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:file/local.dart';
import 'package:sqlite3/common.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite3_test/sqlite3_test.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

/// Tests that the older sync interfaces requiring clients to decode and handle
/// sync lines still work.
void main() {
  final vfs = TestSqliteFileSystem(
      fs: const LocalFileSystem(), name: 'legacy-sync-test');

  setUpAll(() {
    loadExtension();
    sqlite3.registerVirtualFileSystem(vfs, makeDefault: false);
  });
  tearDownAll(() => sqlite3.unregisterVirtualFileSystem(vfs));

  group('sync tests', () {
    late CommonDatabase db;

    setUp(() async {
      db = openTestDatabase(vfs: vfs)
        ..select('select powersync_init();')
        ..select('select powersync_replace_schema(?)', [json.encode(_schema)]);
    });

    tearDown(() {
      db.close();
    });

    void pushSyncData(
      String bucket,
      String opId,
      String rowId,
      Object op,
      Object? data, {
      Object? descriptions = _bucketDescriptions,
    }) {
      final encoded = json.encode({
        'buckets': [
          {
            'bucket': bucket,
            'data': [
              {
                'op_id': opId,
                'op': op,
                'object_type': 'items',
                'object_id': rowId,
                'checksum': 0,
                'data': json.encode(data),
              }
            ],
          }
        ],
        if (descriptions != null) 'descriptions': descriptions,
      });

      db.execute('insert into powersync_operations (op, data) VALUES (?, ?);',
          ['save', encoded]);
    }

    bool pushCheckpointComplete(
        String lastOpId, String? writeCheckpoint, List<Object?> checksums,
        {int? priority}) {
      final [row] = db.select('select powersync_validate_checkpoint(?) as r;', [
        json.encode({
          'last_op_id': lastOpId,
          'write_checkpoint': writeCheckpoint,
          'buckets': [
            for (final cs in checksums.cast<Map<String, dynamic>>())
              if (priority == null || cs['priority'] <= priority) cs
          ],
          'priority': priority,
        })
      ]);

      final decoded = json.decode(row['r']);
      if (decoded['valid'] != true) {
        fail(row['r']);
      }

      db.execute(
        'UPDATE ps_buckets SET last_op = ? WHERE name IN (SELECT json_each.value FROM json_each(?))',
        [
          lastOpId,
          json.encode(checksums.map((e) => (e as Map)['bucket']).toList())
        ],
      );

      db.execute('INSERT INTO powersync_operations(op, data) VALUES (?, ?)', [
        'sync_local',
        priority != null
            ? jsonEncode({
                'priority': priority,
                'buckets': [
                  for (final cs in checksums.cast<Map<String, dynamic>>())
                    if (cs['priority'] <= priority) cs['bucket']
                ],
              })
            : null,
      ]);
      return db.lastInsertRowId == 1;
    }

    ResultSet fetchRows() {
      return db.select('select * from items');
    }

    test('does not publish until reaching checkpoint', () {
      expect(fetchRows(), isEmpty);
      pushSyncData('prio1', '1', 'row-0', 'PUT', {'col': 'hi'});
      expect(fetchRows(), isEmpty);

      expect(
          pushCheckpointComplete(
              '1', null, [_bucketChecksum('prio1', 1, checksum: 0)]),
          isTrue);
      expect(fetchRows(), [
        {'id': 'row-0', 'col': 'hi'}
      ]);
    });

    test('does not publish with pending local data', () {
      expect(fetchRows(), isEmpty);
      db.execute("insert into items (id, col) values ('local', 'data');");
      expect(fetchRows(), isNotEmpty);

      pushSyncData('prio1', '1', 'row-0', 'PUT', {'col': 'hi'});
      expect(
          pushCheckpointComplete(
              '1', null, [_bucketChecksum('prio1', 1, checksum: 0)]),
          isFalse);
      expect(fetchRows(), [
        {'id': 'local', 'col': 'data'}
      ]);
    });

    test('publishes with local data for prio=0 buckets', () {
      expect(fetchRows(), isEmpty);
      db.execute("insert into items (id, col) values ('local', 'data');");
      expect(fetchRows(), isNotEmpty);

      pushSyncData('prio0', '1', 'row-0', 'PUT', {'col': 'hi'});
      expect(
        pushCheckpointComplete(
          '1',
          null,
          [_bucketChecksum('prio0', 0, checksum: 0)],
          priority: 0,
        ),
        isTrue,
      );
      expect(fetchRows(), [
        {'id': 'local', 'col': 'data'},
        {'id': 'row-0', 'col': 'hi'},
      ]);
    });

    test('can publish partial checkpoints under different priorities', () {
      for (var i = 0; i < 4; i++) {
        pushSyncData('prio$i', '1', 'row-$i', 'PUT', {'col': '$i'});
      }
      expect(fetchRows(), isEmpty);

      // Simulate a partial checkpoint complete for each of the buckets.
      for (var i = 0; i < 4; i++) {
        expect(
          pushCheckpointComplete(
            '1',
            null,
            [
              for (var j = 0; j <= 4; j++)
                _bucketChecksum(
                  'prio$j',
                  j,
                  // Give buckets outside of the current priority a wrong
                  // checksum. They should not be validated yet.
                  checksum: j <= i ? 0 : 1234,
                ),
            ],
            priority: i,
          ),
          isTrue,
        );

        expect(fetchRows(), [
          for (var j = 0; j <= i; j++) {'id': 'row-$j', 'col': '$j'},
        ]);

        expect(db.select('select 1 from ps_sync_state where priority = ?', [i]),
            isNotEmpty);
        // A sync at this priority includes all higher priorities too, so they
        // should be cleared.
        expect(db.select('select 1 from ps_sync_state where priority < ?', [i]),
            isEmpty);
      }
    });

    test('can sync multiple times', () {
      fakeAsync((controller) {
        for (var i = 0; i < 10; i++) {
          for (var prio in const [1, 2, 3, null]) {
            pushCheckpointComplete('1', null, [], priority: prio);

            // Make sure there's only a single row in last_synced_at
            expect(
              db.select(
                  "SELECT datetime(last_synced_at, 'localtime') AS last_synced_at FROM ps_sync_state WHERE priority = ?",
                  [prio ?? 2147483647]),
              [
                {'last_synced_at': '2025-03-01 ${10 + i}:00:00'}
              ],
            );

            if (prio == null) {
              expect(
                db.select(
                    "SELECT datetime(powersync_last_synced_at(), 'localtime') AS last_synced_at"),
                [
                  {'last_synced_at': '2025-03-01 ${10 + i}:00:00'}
                ],
              );
            }
          }

          controller.elapse(const Duration(hours: 1));
        }
      }, initialTime: DateTime(2025, 3, 1, 10));
    });

    test('clearing database clears sync status', () {
      pushSyncData('prio1', '1', 'row-0', 'PUT', {'col': 'hi'});

      expect(
          pushCheckpointComplete(
              '1', null, [_bucketChecksum('prio1', 1, checksum: 0)]),
          isTrue);
      expect(db.select('SELECT powersync_last_synced_at() AS r').single,
          {'r': isNotNull});
      expect(db.select('SELECT priority FROM ps_sync_state').single,
          {'priority': 2147483647});

      db.execute('SELECT powersync_clear(0)');
      expect(db.select('SELECT powersync_last_synced_at() AS r').single,
          {'r': isNull});
      expect(db.select('SELECT * FROM ps_sync_state'), hasLength(0));
    });

    test('tracks download progress', () {
      const bucket = 'bkt';
      void expectProgress(int atLast, int sinceLast) {
        final [row] = db.select(
          'SELECT count_at_last, count_since_last FROM ps_buckets WHERE name = ?',
          [bucket],
        );
        final [actualAtLast, actualSinceLast] = row.values;

        expect(actualAtLast, atLast, reason: 'count_at_last mismatch');
        expect(actualSinceLast, sinceLast, reason: 'count_since_last mismatch');
      }

      pushSyncData(bucket, '1', 'row-0', 'PUT', {'col': 'hi'});
      expectProgress(0, 1);

      pushSyncData(bucket, '2', 'row-1', 'PUT', {'col': 'hi'});
      expectProgress(0, 2);

      expect(
        pushCheckpointComplete(
          '2',
          null,
          [_bucketChecksum(bucket, 1, checksum: 0)],
          priority: 1,
        ),
        isTrue,
      );

      // Running partial or complete checkpoints should not reset stats, client
      // SDKs are responsible for that.
      expectProgress(0, 2);
      expect(db.select('SELECT * FROM items'), isNotEmpty);

      expect(
        pushCheckpointComplete(
          '2',
          null,
          [_bucketChecksum(bucket, 1, checksum: 0)],
        ),
        isTrue,
      );
      expectProgress(0, 2);

      db.execute('''
UPDATE ps_buckets SET count_since_last = 0, count_at_last = ?1->name
  WHERE ?1->name IS NOT NULL
''', [
        json.encode({bucket: 2}),
      ]);
      expectProgress(2, 0);

      // Run another iteration of this
      pushSyncData(bucket, '3', 'row-3', 'PUT', {'col': 'hi'});
      expectProgress(2, 1);
      db.execute('''
UPDATE ps_buckets SET count_since_last = 0, count_at_last = ?1->name
  WHERE ?1->name IS NOT NULL
''', [
        json.encode({bucket: 3}),
      ]);
      expectProgress(3, 0);
    });
  });
}

Object? _bucketChecksum(String bucket, int prio, {int checksum = 0}) {
  return {'bucket': bucket, 'priority': prio, 'checksum': checksum};
}

const _schema = {
  'tables': [
    {
      'name': 'items',
      'columns': [
        {'name': 'col', 'type': 'text'}
      ],
    }
  ]
};

const _bucketDescriptions = {
  'prio0': {'priority': 0},
  'prio1': {'priority': 1},
  'prio2': {'priority': 2},
  'prio3': {'priority': 3},
};
