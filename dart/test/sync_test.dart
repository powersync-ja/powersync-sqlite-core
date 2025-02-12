import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

void main() {
  group('sync tests', () {
    late CommonDatabase db;

    setUp(() async {
      db = openTestDatabase()
        ..select('select powersync_init();')
        ..select('select powersync_replace_schema(?)', [json.encode(_schema)]);
    });

    tearDown(() {
      db.dispose();
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
                'data': data,
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
      }
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
