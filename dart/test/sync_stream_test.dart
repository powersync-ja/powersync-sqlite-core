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

    final rawResult = row.columnAt(0);
    if (rawResult is String) {
      final instructions = jsonDecode(row.columnAt(0)) as List;
      for (final instruction in instructions) {
        if (instruction case {'UpdateSyncStatus': final status}) {
          lastStatus = status['status']!;
        }
      }
      return instructions;
    } else {
      return const [];
    }
  }

  group('default streams', () {
    syncTest('are created on-demand', (_) {
      control('start', null);
      control(
        'line_text',
        json.encode(
          checkpoint(
            lastOpId: 1,
            buckets: [
              bucketDescription('a',
                  subscriptions: [
                    {'default': 0}
                  ],
                  priority: 1),
            ],
            streams: [stream('my_default_stream', true)],
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
              'has_explicit_subscription': false,
              'expires_at': null,
              'last_synced_at': null,
              'priority': 1,
            }
          ],
        ),
      );

      control(
        'line_text',
        json.encode(checkpointComplete(priority: 1)),
      );

      expect(
        lastStatus,
        containsPair(
          'streams',
          [containsPair('last_synced_at', 1740823200)],
        ),
      );

      final [stored] = db.select('SELECT * FROM ps_stream_subscriptions');
      expect(stored, containsPair('last_synced_at', 1740823200));
    });

    syncTest('are deleted', (_) {
      control('start', null);

      for (final name in ['s1', 's2']) {
        control(
          'line_text',
          json.encode(
            checkpoint(
              lastOpId: 1,
              buckets: [
                bucketDescription('a',
                    subscriptions: [
                      {'default': 0}
                    ],
                    priority: 1),
              ],
              streams: [stream(name, true)],
            ),
          ),
        );
        control(
          'line_text',
          json.encode(checkpointComplete(priority: 1)),
        );
      }

      expect(
        lastStatus,
        containsPair(
          'streams',
          [containsPair('name', 's2')],
        ),
      );
    });

    syncTest('can be made explicit', (_) {
      control('start', null);
      control(
        'line_text',
        json.encode(
          checkpoint(
            lastOpId: 1,
            buckets: [
              bucketDescription('a',
                  subscriptions: [
                    {'default': 0}
                  ],
                  priority: 1),
            ],
            streams: [stream('a', true)],
          ),
        ),
      );

      var [stored] = db.select('SELECT * FROM ps_stream_subscriptions');
      expect(stored, containsPair('is_default', 1));
      expect(stored, containsPair('ttl', isNull));

      control(
        'subscriptions',
        json.encode({
          'subscribe': {'stream': 'a'},
        }),
      );

      [stored] = db.select('SELECT * FROM ps_stream_subscriptions');
      expect(stored, containsPair('active', 1));
      // It's still a default stream, but it now has a TTL to indicate the
      // explicit subscription.
      expect(stored, containsPair('is_default', 1));
      expect(stored, containsPair('ttl', isNotNull));

      // Remove the stream from the checkpoint, should still be included due to
      // the explicit subscription.
      control(
        'line_text',
        json.encode(
          checkpoint(
            lastOpId: 1,
            buckets: [
              bucketDescription('a', priority: 1),
            ],
          ),
        ),
      );

      [stored] = db.select('SELECT * FROM ps_stream_subscriptions');
      expect(stored, containsPair('active', 0));
      expect(stored, containsPair('is_default', 0));
      expect(stored, containsPair('ttl', isNotNull));
    });

    syncTest('reports errors', (_) {
      control('start', null);
      final response = control(
        'line_text',
        json.encode(
          checkpoint(
            lastOpId: 1,
            buckets: [
              bucketDescription('a',
                  subscriptions: [
                    {'default': 0}
                  ],
                  priority: 1),
            ],
            streams: [
              stream('a', true, errors: [
                {'message': 'error message', 'subscription': 'default'}
              ])
            ],
          ),
        ),
      );

      expect(
        response,
        contains(
          containsPair(
            'LogLine',
            containsPair(
                'line', 'Default subscription a has errors: error message'),
          ),
        ),
      );
    });
  });

  group('explicit subscriptions', () {
    syncTest('unsubscribe', (_) {
      db.execute(
          'INSERT INTO ps_stream_subscriptions (stream_name, ttl) VALUES (?, ?);',
          ['my_stream', 3600]);

      var startInstructions = control('start', null);
      expect(
        startInstructions,
        contains(
          containsPair(
            'EstablishSyncStream',
            containsPair(
              'request',
              containsPair(
                'streams',
                {
                  'include_defaults': true,
                  'subscriptions': isNotEmpty,
                },
              ),
            ),
          ),
        ),
      );
      control('stop', null);

      control(
        'subscriptions',
        json.encode({
          'unsubscribe': {
            'stream': 'my_stream',
            'params': null,
          }
        }),
      );
      startInstructions = control('start', null);
      expect(
        startInstructions,
        contains(
          containsPair(
            'EstablishSyncStream',
            containsPair(
              'request',
              containsPair(
                'streams',
                {
                  'include_defaults': true,
                  'subscriptions': isEmpty,
                },
              ),
            ),
          ),
        ),
      );
    });

    syncTest('ttl', (controller) {
      control(
        'subscriptions',
        json.encode({
          'subscribe': {
            'stream': 'my_stream',
            'ttl': 3600,
          }
        }),
      );

      final [row] = db.select('SELECT * FROM ps_stream_subscriptions');
      expect(row, containsPair('expires_at', 1740826800));

      var startInstructions = control('start', null);
      expect(
        startInstructions,
        contains(
          containsPair(
            'EstablishSyncStream',
            containsPair(
              'request',
              containsPair(
                'streams',
                {
                  'include_defaults': true,
                  'subscriptions': [
                    {
                      'stream': 'my_stream',
                      'parameters': null,
                      'override_priority': null,
                    }
                  ],
                },
              ),
            ),
          ),
        ),
      );
      control('stop', null);

      // Elapse beyond end of TTL
      controller.elapse(const Duration(hours: 2));
      startInstructions = control('start', null);
      expect(
        startInstructions,
        contains(
          containsPair(
            'EstablishSyncStream',
            containsPair(
              'request',
              containsPair(
                'streams',
                {
                  'include_defaults': true,
                  // Outdated subscription should no longer be included.
                  'subscriptions': isEmpty,
                },
              ),
            ),
          ),
        ),
      );
    });

    syncTest('can be made implicit', (_) {
      control(
          'subscriptions',
          json.encode({
            'subscribe': {'stream': 'a'}
          }));
      control('start', null);
      control(
        'line_text',
        json.encode(
          checkpoint(
            lastOpId: 1,
            buckets: [],
            streams: [stream('a', true)],
          ),
        ),
      );

      var [stored] = db.select('SELECT * FROM ps_stream_subscriptions');
      expect(stored, containsPair('is_default', 1));
      expect(stored, containsPair('ttl', isNotNull));

      control(
        'subscriptions',
        json.encode({
          'unsubscribe': {'stream': 'a'}
        }),
      );
      control('stop', null);

      // The stream should no longer be requested
      var startInstructions = control('start', null);
      expect(
        startInstructions,
        contains(
          containsPair(
            'EstablishSyncStream',
            containsPair(
              'request',
              containsPair(
                'streams',
                {
                  'include_defaults': true,
                  'subscriptions': isEmpty,
                },
              ),
            ),
          ),
        ),
      );
    });

    syncTest('reports errors', (_) {
      control(
        'subscriptions',
        json.encode({
          'subscribe': {'stream': 'a', 'params': 'invalid'}
        }),
      );
      control(
        'subscriptions',
        json.encode({
          'subscribe': {'stream': 'a', 'params': 'valid'}
        }),
      );

      final start = control('start', null);
      expect(
        start,
        contains(
          containsPair(
            'EstablishSyncStream',
            containsPair(
              'request',
              containsPair(
                'streams',
                {
                  'include_defaults': true,
                  'subscriptions': [
                    {
                      'stream': 'a',
                      'parameters': 'invalid',
                      'override_priority': null
                    },
                    {
                      'stream': 'a',
                      'parameters': 'valid',
                      'override_priority': null
                    }
                  ]
                },
              ),
            ),
          ),
        ),
      );
      final response = control(
        'line_text',
        json.encode(
          checkpoint(
            lastOpId: 1,
            buckets: [],
            streams: [
              stream('a', true, errors: [
                {'message': 'error message', 'subscription': 0}
              ])
            ],
          ),
        ),
      );

      expect(
        response,
        contains(
          containsPair(
            'LogLine',
            containsPair('line',
                'Subscription to stream a (with parameters "invalid") could not be resolved: error message'),
          ),
        ),
      );
    });
  });
}
