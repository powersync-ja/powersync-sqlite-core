import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:bson/bson.dart';
import 'package:file/local.dart';
import 'package:sqlite3/common.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite3_test/sqlite3_test.dart';
import 'package:test/test.dart';
import 'package:test_descriptor/test_descriptor.dart' as d;
import 'package:path/path.dart';

import 'utils/native_test_utils.dart';
import 'utils/test_utils.dart';

void main() {
  final vfs =
      TestSqliteFileSystem(fs: const LocalFileSystem(), name: 'vfs-sync-test');

  setUpAll(() {
    loadExtension();
    sqlite3.registerVirtualFileSystem(vfs, makeDefault: false);
  });
  tearDownAll(() => sqlite3.unregisterVirtualFileSystem(vfs));

  group('text lines', () {
    _syncTests(vfs: vfs, isBson: false);
  });

  group('bson lines', () {
    _syncTests(vfs: vfs, isBson: true);
  });
}

void _syncTests<T>({
  required VirtualFileSystem vfs,
  required bool isBson,
}) {
  late CommonDatabase db;
  late SyncLinesGoldenTest matcher;

  List<Object?> invokeControlRaw(String operation, Object? data) {
    db.execute('begin');
    ResultSet result;

    try {
      result = db.select('SELECT powersync_control(?, ?)', [operation, data]);

      // Make sure that powersync_control doesn't leave any busy statements
      // behind.
      // TODO: Re-enable after we can guarantee sqlite_stmt being available
      // const statement = 'SELECT * FROM sqlite_stmt WHERE busy AND sql != ?;';
      // final busy = db.select(statement, [statement]);
      // expect(busy, isEmpty);
    } catch (e) {
      db.execute('rollback');
      rethrow;
    }

    db.execute('commit');
    final [row] = result;
    return jsonDecode(row.columnAt(0));
  }

  List<Object?> invokeControl(String operation, Object? data) {
    if (matcher.enabled) {
      // Trace through golden matcher
      return matcher.invoke(operation, data);
    } else {
      return invokeControlRaw(operation, data);
    }
  }

  setUp(() async {
    db = openTestDatabase(vfs: vfs)
      ..select('select powersync_init();')
      ..select('select powersync_replace_schema(?)', [json.encode(testSchema)])
      ..execute('update ps_kv set value = ?2 where key = ?1',
          ['client_id', 'test-test-test-test']);

    matcher = SyncLinesGoldenTest(isBson, invokeControlRaw);
  });

  tearDown(() {
    matcher.finish();
    db.dispose();
  });

  List<Object?> syncLine(Object? line) {
    if (isBson) {
      final serialized = BsonCodec.serialize(line).byteList;
      // print(serialized.asRustByteString);
      return invokeControl('line_binary', serialized);
    } else {
      return invokeControl('line_text', jsonEncode(line));
    }
  }

  List<Object?> pushSyncData(
      String bucket, String opId, String rowId, Object op, Object? data,
      {int checksum = 0, String objectType = 'items'}) {
    return syncLine({
      'data': {
        'bucket': bucket,
        'has_more': false,
        'after': null,
        'next_after': null,
        'data': [
          {
            'op_id': opId,
            'op': op,
            'object_type': objectType,
            'object_id': rowId,
            'checksum': checksum,
            'data': json.encode(data),
          }
        ],
      },
    });
  }

  List<Object?> pushCheckpoint(
      {int lastOpId = 1, List<Object> buckets = const []}) {
    return syncLine(checkpoint(lastOpId: lastOpId, buckets: buckets));
  }

  List<Object?> pushCheckpointComplete({int? priority, String lastOpId = '1'}) {
    return syncLine(checkpointComplete(priority: priority, lastOpId: lastOpId));
  }

  ResultSet fetchRows() {
    return db.select('select * from items');
  }

  group('goldens', () {
    syncTest('starting stream', (_) {
      matcher.load('starting_stream');
      invokeControl(
        'start',
        json.encode({
          'parameters': {'foo': 'bar'}
        }),
      );
    });

    syncTest('simple sync iteration', (_) {
      matcher.load('simple_iteration');
      invokeControl('start', null);

      syncLine({
        'checkpoint': {
          'last_op_id': '1',
          'write_checkpoint': null,
          'buckets': [
            {
              'bucket': 'a',
              'checksum': 0,
              'priority': 3,
              'count': 1,
            }
          ],
        },
      });
      syncLine({'token_expires_in': 60});
      pushSyncData('a', '1', '1', 'PUT', {'col': 'hi'});

      syncLine({
        'checkpoint_complete': {'last_op_id': '1'},
      });

      syncLine({'token_expires_in': 10});
    });
  });

  test('does not publish until reaching checkpoint', () {
    invokeControl('start', null);
    pushCheckpoint(buckets: priorityBuckets);
    expect(fetchRows(), isEmpty);
    db.execute("insert into items (id, col) values ('local', 'data');");

    pushSyncData('prio1', '1', 'row-0', 'PUT', {'col': 'hi'});

    pushCheckpointComplete();
    expect(fetchRows(), [
      {'id': 'local', 'col': 'data'}
    ]);
  });

  test('publishes with local data for prio=0 buckets', () {
    invokeControl('start', null);
    pushCheckpoint(buckets: priorityBuckets);
    expect(fetchRows(), isEmpty);
    db.execute("insert into items (id, col) values ('local', 'data');");

    pushSyncData('prio0', '1', 'row-0', 'PUT', {'col': 'hi'});

    pushCheckpointComplete(priority: 0);
    expect(fetchRows(), [
      {'id': 'local', 'col': 'data'},
      {'id': 'row-0', 'col': 'hi'},
    ]);
  });

  test('does not publish with pending local data', () {
    invokeControl('start', null);
    pushCheckpoint(buckets: priorityBuckets);
    db.execute("insert into items (id, col) values ('local', 'data');");
    expect(fetchRows(), isNotEmpty);

    pushCheckpoint(buckets: priorityBuckets);
    pushSyncData('prio1', '1', 'row-0', 'PUT', {'col': 'hi'});
    pushCheckpointComplete();

    expect(fetchRows(), [
      {'id': 'local', 'col': 'data'}
    ]);
  });

  test('can publish partial checkpoints under different priorities', () {
    invokeControl('start', null);
    pushCheckpoint(buckets: priorityBuckets);
    for (var i = 0; i < 4; i++) {
      pushSyncData('prio$i', '1', 'row-$i', 'PUT', {'col': '$i'});
    }

    expect(fetchRows(), isEmpty);

    // Simulate a partial checkpoint complete for each of the buckets.
    for (var i = 0; i < 4; i++) {
      pushCheckpointComplete(
        priority: i,
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

  syncTest('can sync multiple times', (controller) {
    invokeControl('start', null);

    for (var i = 0; i < 10; i++) {
      pushCheckpoint(buckets: priorityBuckets);

      for (var prio in const [1, 2, 3, null]) {
        pushCheckpointComplete(priority: prio);

        // Make sure there's only a single row in last_synced_at
        expect(
          db.select(
              "SELECT datetime(last_synced_at) AS last_synced_at FROM ps_sync_state WHERE priority = ?",
              [prio ?? 2147483647]),
          [
            {'last_synced_at': '2025-03-01 ${10 + i}:00:00'}
          ],
        );

        if (prio == null) {
          expect(
            db.select(
                "SELECT datetime(powersync_last_synced_at()) AS last_synced_at"),
            [
              {'last_synced_at': '2025-03-01 ${10 + i}:00:00'}
            ],
          );
        }
      }

      controller.elapse(const Duration(hours: 1));
    }
  });

  syncTest('remembers sync state', (controller) {
    invokeControl('start', null);

    pushCheckpoint(buckets: priorityBuckets);
    pushCheckpointComplete();

    controller.elapse(Duration(minutes: 10));
    pushCheckpoint(buckets: priorityBuckets);
    pushCheckpointComplete(priority: 2);
    invokeControl('stop', null);

    final instructions = invokeControl('start', null);
    expect(
      instructions,
      contains(
        containsPair(
          'UpdateSyncStatus',
          containsPair(
            'status',
            containsPair(
              'priority_status',
              [
                {
                  'priority': 2,
                  'last_synced_at': 1740823800,
                  'has_synced': true
                },
                {
                  'priority': 2147483647,
                  'last_synced_at': 1740823200,
                  'has_synced': true
                },
              ],
            ),
          ),
        ),
      ),
    );

    final [row] = db.select('select powersync_offline_sync_status();');
    expect(json.decode(row[0]), {
      'connected': false,
      'connecting': false,
      'priority_status': [
        {'priority': 2, 'last_synced_at': 1740823800, 'has_synced': true},
        {
          'priority': 2147483647,
          'last_synced_at': 1740823200,
          'has_synced': true
        }
      ],
      'downloading': null,
      'streams': [],
    });
  });

  test('clearing database clears sync status', () {
    invokeControl('start', null);
    pushCheckpoint(buckets: priorityBuckets);
    pushCheckpointComplete();

    expect(db.select('SELECT powersync_last_synced_at() AS r').single,
        {'r': isNotNull});
    expect(db.select('SELECT priority FROM ps_sync_state').single,
        {'priority': 2147483647});

    db.execute('SELECT powersync_clear(0)');
    expect(db.select('SELECT powersync_last_synced_at() AS r').single,
        {'r': isNull});
    expect(db.select('SELECT * FROM ps_sync_state'), hasLength(0));
  });

  test('persists download progress', () {
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

    invokeControl('start', null);
    pushCheckpoint(buckets: [bucketDescription(bucket, count: 2)]);
    pushCheckpointComplete();

    pushSyncData(bucket, '1', 'row-0', 'PUT', {'col': 'hi'});
    expectProgress(0, 1);

    pushSyncData(bucket, '1', 'row-1', 'PUT', {'col': 'hi again'});
    expectProgress(0, 2);

    pushCheckpointComplete(lastOpId: '2');
    expectProgress(2, 0);
  });

  test('deletes old buckets', () {
    for (final name in ['one', 'two', 'three', r'$local']) {
      db.execute('INSERT INTO ps_buckets (name) VALUES (?)', [name]);
    }

    expect(
      invokeControl('start', null),
      contains(
        containsPair(
          'EstablishSyncStream',
          containsPair('request', containsPair('buckets', hasLength(3))),
        ),
      ),
    );

    syncLine({
      'checkpoint': {
        'last_op_id': '1',
        'write_checkpoint': null,
        'buckets': [
          {
            'bucket': 'one',
            'checksum': 0,
            'priority': 3,
            'count': 1,
          }
        ],
      },
    });

    // Should delete the old buckets two and three
    expect(db.select('select name from ps_buckets order by id'), [
      {'name': 'one'},
      {'name': r'$local'}
    ]);
  });

  if (isBson) {
    test('can parse checksums from JS numbers', () {
      invokeControl('start', null);
      pushCheckpoint(buckets: [bucketDescription('global[]')]);

      syncLine({
        'data': {
          'bucket': 'a',
          'has_more': false,
          'after': null,
          'next_after': null,
          'data': [
            {
              'op_id': '1',
              'op': 'PUT',
              'object_type': 'items',
              'object_id': 'id',
              'checksum': 3573495687.0,
              'data': '{}',
            }
          ],
        },
      });
    });
  }

  group('progress', () {
    Map<String, BucketProgress>? progress = null;
    var lastOpId = 0;

    setUp(() {
      lastOpId = 0;
      return progress = null;
    });

    (int, int) totalProgress() {
      return progress!.values.downloadAndTargetCount();
    }

    (int, int) priorityProgress(int priority) {
      return progress!.values
          .where((e) => e.priority <= priority)
          .downloadAndTargetCount();
    }

    void applyInstructions(List<Object?> instructions) {
      for (final instruction in instructions.cast<Map>()) {
        if (instruction['UpdateSyncStatus'] case final updateStatus?) {
          final downloading = updateStatus['status']['downloading'];
          if (downloading == null) {
            progress = null;
          } else {
            progress = {
              for (final MapEntry(:key, :value)
                  in downloading['buckets'].entries)
                key: (
                  atLast: value['at_last'] as int,
                  sinceLast: value['since_last'] as int,
                  targetCount: value['target_count'] as int,
                  priority: value['priority'] as int,
                ),
            };
          }
        }
      }
    }

    void pushSyncData(String bucket, int amount) {
      final instructions = syncLine({
        'data': {
          'bucket': bucket,
          'has_more': false,
          'after': null,
          'next_after': null,
          'data': [
            for (var i = 0; i < amount; i++)
              {
                'op_id': (++lastOpId).toString(),
                'op': 'PUT',
                'object_type': 'items',
                'object_id': '$lastOpId',
                'checksum': 0,
                'data': '{}',
              }
          ],
        },
      });

      applyInstructions(instructions);
    }

    void addCheckpointComplete({int? priority}) {
      applyInstructions(
          pushCheckpointComplete(priority: priority, lastOpId: '$lastOpId'));
    }

    test('without priorities', () {
      applyInstructions(invokeControl('start', null));
      expect(progress, isNull);

      applyInstructions(pushCheckpoint(
          buckets: [bucketDescription('a', count: 10)], lastOpId: 10));
      expect(totalProgress(), (0, 10));

      pushSyncData('a', 10);
      expect(totalProgress(), (10, 10));

      addCheckpointComplete();
      expect(progress, isNull);

      // Emit new data, progress should be 0/2 instead of 10/12
      applyInstructions(syncLine({
        'checkpoint_diff': {
          'last_op_id': '12',
          'updated_buckets': [
            {
              'bucket': 'a',
              'priority': 3,
              'checksum': 0,
              'count': 12,
              'last_op_id': null
            },
          ],
          'removed_buckets': [],
          'write_checkpoint': null,
        }
      }));
      expect(totalProgress(), (0, 2));

      pushSyncData('a', 2);
      expect(totalProgress(), (2, 2));

      addCheckpointComplete();
      expect(progress, isNull);
    });

    test('interrupted sync', () {
      applyInstructions(invokeControl('start', null));
      applyInstructions(pushCheckpoint(
          buckets: [bucketDescription('a', count: 10)], lastOpId: 10));
      expect(totalProgress(), (0, 10));

      pushSyncData('a', 5);
      expect(totalProgress(), (5, 10));

      // Emulate stream closing
      applyInstructions(invokeControl('stop', null));
      expect(progress, isNull);

      applyInstructions(invokeControl('start', null));
      applyInstructions(pushCheckpoint(
          buckets: [bucketDescription('a', count: 10)], lastOpId: 10));
      expect(totalProgress(), (5, 10));

      pushSyncData('a', 5);
      expect(totalProgress(), (10, 10));
      addCheckpointComplete();
      expect(progress, isNull);
    });

    test('interrupted sync with new checkpoint', () {
      applyInstructions(invokeControl('start', null));
      applyInstructions(pushCheckpoint(
          buckets: [bucketDescription('a', count: 10)], lastOpId: 10));
      expect(totalProgress(), (0, 10));

      pushSyncData('a', 5);
      expect(totalProgress(), (5, 10));

      // Emulate stream closing
      applyInstructions(invokeControl('stop', null));
      expect(progress, isNull);

      applyInstructions(invokeControl('start', null));
      applyInstructions(pushCheckpoint(
          buckets: [bucketDescription('a', count: 12)], lastOpId: 12));
      expect(totalProgress(), (5, 12));

      pushSyncData('a', 7);
      expect(totalProgress(), (12, 12));
      addCheckpointComplete();
      expect(progress, isNull);
    });

    test('interrupt and defrag', () {
      applyInstructions(invokeControl('start', null));
      applyInstructions(pushCheckpoint(
          buckets: [bucketDescription('a', count: 10)], lastOpId: 10));
      expect(totalProgress(), (0, 10));

      pushSyncData('a', 5);
      expect(totalProgress(), (5, 10));

      // Emulate stream closing
      applyInstructions(invokeControl('stop', null));
      expect(progress, isNull);

      applyInstructions(invokeControl('start', null));
      // A defrag in the meantime shrank the bucket.
      applyInstructions(pushCheckpoint(
          buckets: [bucketDescription('a', count: 4)], lastOpId: 14));
      // So we shouldn't report 5/4.
      expect(totalProgress(), (0, 4));

      // This should also reset the persisted progress counters.
      final [bucket] = db.select('SELECT * FROM ps_buckets');
      expect(bucket, containsPair('count_since_last', 0));
      expect(bucket, containsPair('count_at_last', 0));
    });

    test('different priorities', () {
      void expectProgress((int, int) prio0, (int, int) prio2) {
        expect(priorityProgress(0), prio0);
        expect(priorityProgress(1), prio0);
        expect(priorityProgress(2), prio2);
        expect(totalProgress(), prio2);
      }

      applyInstructions(invokeControl('start', null));
      applyInstructions(pushCheckpoint(buckets: [
        bucketDescription('a', count: 5, priority: 0),
        bucketDescription('b', count: 5, priority: 2),
      ], lastOpId: 10));
      expectProgress((0, 5), (0, 10));

      pushSyncData('a', 5);
      expectProgress((5, 5), (5, 10));

      pushSyncData('b', 2);
      expectProgress((5, 5), (7, 10));

      // Before syncing b fully, send a new checkpoint
      applyInstructions(pushCheckpoint(buckets: [
        bucketDescription('a', count: 8, priority: 0),
        bucketDescription('b', count: 6, priority: 2),
      ], lastOpId: 14));
      expectProgress((5, 8), (7, 14));

      pushSyncData('a', 3);
      expectProgress((8, 8), (10, 14));
      pushSyncData('b', 4);
      expectProgress((8, 8), (14, 14));

      addCheckpointComplete();
      expect(progress, isNull);
    });
  });

  group('errors', () {
    syncTest('diff without prior checkpoint', (_) {
      invokeControl('start', null);

      expect(
        () => syncLine({
          'checkpoint_diff': {
            'last_op_id': '1',
            'write_checkpoint': null,
            'updated_buckets': [],
            'removed_buckets': [],
          },
        }),
        throwsA(
          isA<SqliteException>().having(
            (e) => e.message,
            'message',
            contains('checkpoint_diff without previous checkpoint'),
          ),
        ),
      );
    });

    syncTest('checksum mismatch', (_) {
      invokeControl('start', null);

      syncLine({
        'checkpoint': {
          'last_op_id': '1',
          'write_checkpoint': null,
          'buckets': [
            {
              'bucket': 'a',
              'checksum': 1234,
              'priority': 3,
              'count': 1,
            }
          ],
        },
      });
      pushSyncData('a', '1', '1', 'PUT', {'col': 'hi'}, checksum: 4321);

      expect(db.select('SELECT * FROM ps_buckets'), hasLength(1));

      expect(
        syncLine({
          'checkpoint_complete': {'last_op_id': '1'},
        }),
        [
          {
            'LogLine': {
              'severity': 'WARNING',
              'line': contains(
                  "Checksums didn't match, failed for: a (expected 0x000004d2, got 0x000010e1 = 0x000010e1 (op) + 0x00000000 (add))")
            }
          },
          {
            'CloseSyncStream': {'hide_disconnect': false}
          },
        ],
      );

      // Should delete bucket with checksum mismatch
      expect(db.select('SELECT * FROM ps_buckets'), isEmpty);
    });

    group('recoverable', () {
      late CommonDatabase secondary;
      final checkpoint = {
        'checkpoint': {
          'last_op_id': '1',
          'write_checkpoint': null,
          'buckets': [
            {
              'bucket': 'a',
              'checksum': 0,
              'priority': 3,
              'count': 1,
            }
          ],
        },
      };

      setUp(() {
        final fileName = d.path('test.db');

        db = openTestDatabase(fileName: fileName)
          ..select('select powersync_init();')
          ..select(
              'select powersync_replace_schema(?)', [json.encode(testSchema)])
          ..execute('update ps_kv set value = ?2 where key = ?1',
              ['client_id', 'test-test-test-test']);

        secondary = openTestDatabase(fileName: fileName);
      });

      test('starting checkpoints', () {
        db.execute('INSERT INTO ps_buckets (name) VALUES (?)', ['unrelated']);
        invokeControl('start', null);

        // Lock the db so that the checkpoint line can't delete the unrelated
        // bucket.
        secondary.execute('begin exclusive');
        expect(
          () => syncLine(checkpoint),
          throwsA(
            isSqliteException(
                5, 'powersync_control: internal SQLite call returned BUSY'),
          ),
        );
        secondary.execute('commit');
        expect(db.select('SELECT name FROM ps_buckets'), [
          {'name': 'unrelated'}
        ]);

        syncLine(checkpoint);
        expect(db.select('SELECT name FROM ps_buckets'), isEmpty);
      });

      test('saving oplog data', () {
        invokeControl('start', null);
        syncLine(checkpoint);

        // Lock the database before the data line
        secondary.execute('begin exclusive');

        // This should make powersync_control unable to save oplog data.
        expect(
          () => pushSyncData('a', '1', '1', 'PUT', {'col': 'hi'}),
          throwsA(isSqliteException(
              5, 'powersync_control: internal SQLite call returned BUSY')),
        );

        // But we should be able to retry
        secondary.execute('commit');

        expect(pushSyncData('a', '1', '1', 'PUT', {'col': 'hi'}), [
          containsPair(
            'UpdateSyncStatus',
            containsPair(
              'status',
              containsPair(
                'downloading',
                {
                  'buckets': {
                    'prio_3': {
                      'priority': 3,
                      'at_last': 0,
                      'since_last': 1,
                      'target_count': 1
                    },
                  }
                },
              ),
            ),
          )
        ]);
      });

      test('applying local changes', () {
        invokeControl('start', null);
        syncLine(checkpoint);
        pushSyncData('a', '1', '1', 'PUT', {'col': 'hi'});

        secondary.execute('begin exclusive');
        expect(
          () => pushCheckpointComplete(),
          throwsA(
            isSqliteException(
                5, 'powersync_control: internal SQLite call returned BUSY'),
          ),
        );
        secondary.execute('commit');

        pushCheckpointComplete();
        expect(db.select('SELECT * FROM items'), hasLength(1));
      });
    });
  });

  syncTest('sets powersync_in_sync_operation', (_) {
    var [row] = db.select('SELECT powersync_in_sync_operation() as r');
    expect(row, {'r': 0});

    var testInSyncInvocations = <bool>[];

    db.createFunction(
      functionName: 'test_in_sync',
      function: (args) {
        testInSyncInvocations.add((args[0] as int) != 0);
        return null;
      },
      argumentCount: const AllowedArgumentCount(1),
      directOnly: false,
    );

    db.execute('''
CREATE TRIGGER foo AFTER INSERT ON ps_data__items BEGIN
  SELECT test_in_sync(powersync_in_sync_operation());
END;
''');

    // Run an insert sync iteration to start the trigger
    invokeControl('start', null);
    pushCheckpoint(buckets: [bucketDescription('a')]);
    pushSyncData(
      'a',
      '1',
      '1',
      'PUT',
      {'col': 'foo'},
      objectType: 'items',
    );
    pushCheckpointComplete();

    expect(testInSyncInvocations, [true]);

    [row] = db.select('SELECT powersync_in_sync_operation() as r');
    expect(row, {'r': 0});
  });

  group('raw tables', () {
    syncTest('smoke test', (_) {
      db.execute(
          'CREATE TABLE users (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL) STRICT;');

      invokeControl(
        'start',
        json.encode({
          'schema': {
            'raw_tables': [
              {
                'name': 'users',
                'put': {
                  'sql':
                      'INSERT OR REPLACE INTO users (id, name) VALUES (?, ?);',
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
          },
        }),
      );

      // Insert
      pushCheckpoint(buckets: [bucketDescription('a')]);
      pushSyncData(
        'a',
        '1',
        'my_user',
        'PUT',
        {'name': 'First user'},
        objectType: 'users',
      );
      pushCheckpointComplete();

      final users = db.select('SELECT * FROM users;');
      expect(users, [
        {'id': 'my_user', 'name': 'First user'}
      ]);

      // Delete
      pushCheckpoint(buckets: [bucketDescription('a')]);
      pushSyncData(
        'a',
        '1',
        'my_user',
        'REMOVE',
        null,
        objectType: 'users',
      );
      pushCheckpointComplete();

      expect(db.select('SELECT * FROM users'), isEmpty);
    });

    test("crud vtab is no-op during sync", () {
      db.execute(
          'CREATE TABLE users (id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL) STRICT;');

      invokeControl(
        'start',
        json.encode({
          'schema': {
            'raw_tables': [
              {
                'name': 'users',
                'put': {
                  'sql': "INSERT INTO powersync_crud_(data) VALUES (?);",
                  'params': [
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
          },
        }),
      );

      // Insert
      pushCheckpoint(buckets: [bucketDescription('a')]);
      pushSyncData(
        'a',
        '1',
        'my_user',
        'PUT',
        {'name': 'First user'},
        objectType: 'users',
      );

      pushCheckpointComplete();
      expect(db.select('SELECT * FROM ps_crud'), isEmpty);
    });
  });
}

final priorityBuckets = [
  for (var i = 0; i < 4; i++) bucketDescription('prio$i', priority: i)
];

typedef BucketProgress = ({
  int priority,
  int atLast,
  int sinceLast,
  int targetCount
});

extension on Iterable<BucketProgress> {
  (int, int) downloadAndTargetCount() {
    return fold((0, 0), (counters, entry) {
      final (downloaded, total) = counters;

      return (
        downloaded + entry.sinceLast,
        total + entry.targetCount - entry.atLast
      );
    });
  }
}

extension on Uint8List {
  // ignore: unused_element
  String get asRustByteString {
    final buffer = StringBuffer('b"');

    for (final byte in this) {
      switch (byte) {
        case >= 32 && < 127:
          buffer.writeCharCode(byte);
        default:
          // Escape
          buffer.write('\\x${byte.toRadixString(16).padLeft(2, '0')}');
      }
    }

    buffer.write('"');
    return buffer.toString();
  }
}

final class SyncLinesGoldenTest {
  static bool _update = Platform.environment['UPDATE_GOLDENS'] == '1';

  final List<Object?> Function(String operation, Object? data) _invokeControl;

  String? name;

  final bool isBson;
  final List<ExpectedSyncLine> expectedLines = [];
  final List<ExpectedSyncLine> actualLines = [];

  String get path => join('test', 'goldens', '$name.json');

  bool get enabled => name != null;

  SyncLinesGoldenTest(this.isBson, this._invokeControl);

  ExpectedSyncLine get _nextExpectation {
    return expectedLines[actualLines.length];
  }

  void _checkMismatch(void Function() compare) {
    try {
      compare();
    } catch (e) {
      print(
          'Golden test for sync lines failed, set UPDATE_GOLDENS=1 to update');
      rethrow;
    }
  }

  void load(String name) {
    this.name = name;
    final file = File(path);
    try {
      final loaded = json.decode(file.readAsStringSync());
      for (final entry in loaded) {
        expectedLines.add(ExpectedSyncLine.fromJson(entry));
      }
    } catch (e) {
      if (!_update) {
        rethrow;
      }
    }
  }

  List<Object?> invoke(String operation, Object? data) {
    final matchData = switch (data) {
      final String s => json.decode(s),
      _ => data,
    };

    if (_update) {
      final result = _invokeControl(operation, data);
      actualLines.add(ExpectedSyncLine(operation, matchData, result));
      return result;
    } else {
      final expected = _nextExpectation;
      if (!isBson) {
        // We only want to compare the JSON inputs. We compare outputs
        // regardless of the encoding mode.
        _checkMismatch(() {
          expect(operation, expected.operation);
          expect(matchData, expected.data);
        });
      }

      final result = _invokeControl(operation, data);
      _checkMismatch(() {
        expect(result, expected.output);
      });

      actualLines.add(ExpectedSyncLine(operation, matchData, result));
      return result;
    }
  }

  void finish() {
    if (_update && enabled) {
      if (!isBson) {
        File(path).writeAsStringSync(
            JsonEncoder.withIndent('  ').convert(actualLines));
      }
    } else {
      _checkMismatch(
          () => expect(actualLines, hasLength(expectedLines.length)));
    }
  }
}

final class ExpectedSyncLine {
  final String operation;
  final Object? data;
  final List<Object?> output;

  ExpectedSyncLine(this.operation, this.data, this.output);

  factory ExpectedSyncLine.fromJson(Map<String, Object?> json) {
    return ExpectedSyncLine(
        json['operation'] as String, json['data'], json['output'] as List);
  }

  Map<String, Object?> toJson() {
    return {
      'operation': operation,
      'data': data,
      'output': output,
    };
  }
}
