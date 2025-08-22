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
              'progress': {'total': 1, 'downloaded': 0},
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
          'subscribe': {
            'stream': {'name': 'a'}
          },
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
            'name': 'my_stream',
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

    syncTest('delete after ttl', (controller) {
      control(
        'subscriptions',
        json.encode({
          'subscribe': {
            'stream': {'name': 'my_stream'},
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

    syncTest('increase ttl', (controller) {
      const startTime = 1740826800;

      control(
        'subscriptions',
        json.encode({
          'subscribe': {
            'stream': {'name': 'my_stream'},
            'ttl': 3600,
          }
        }),
      );

      var [row] = db.select('SELECT * FROM ps_stream_subscriptions');
      expect(row, containsPair('expires_at', startTime));

      controller.elapse(const Duration(minutes: 30));

      // Mark the stream as active in the connect procedure
      control(
        'start',
        json.encode({
          'active_streams': [
            {'name': 'my_stream'}
          ]
        }),
      );

      // Which should increase its expiry date.
      [row] = db.select('SELECT * FROM ps_stream_subscriptions');
      expect(row, containsPair('expires_at', startTime + 1800));

      // The sync client uses token_expires_in lines to extend the expiry date
      // of active stream subscriptions.
      controller.elapse(const Duration(minutes: 30));
      control('line_text', json.encode({'token_expires_in': 3600}));

      [row] = db.select('SELECT * FROM ps_stream_subscriptions');
      expect(row, containsPair('expires_at', startTime + 3600));

      // Stopping should not increase the expiry date.
      controller.elapse(const Duration(minutes: 30));
      control('stop', null);

      [row] = db.select('SELECT * FROM ps_stream_subscriptions');
      expect(row, containsPair('expires_at', startTime + 3600));
    });

    syncTest('can be made implicit', (_) {
      control(
          'subscriptions',
          json.encode({
            'subscribe': {
              'stream': {'name': 'a'}
            }
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
          'unsubscribe': {'name': 'a'}
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
          'subscribe': {
            'stream': {'name': 'a', 'params': 'invalid'}
          }
        }),
      );
      control(
        'subscriptions',
        json.encode({
          'subscribe': {
            'stream': {'name': 'a', 'params': 'valid'}
          }
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

  syncTest('changing subscriptions', (controller) {
    var activeAppSubscriptions = <Object>[];

    void startIteration() {
      control('start', json.encode({'active_streams': activeAppSubscriptions}));
    }

    bool checkRestart() {
      final instructions =
          control('update_subscriptions', json.encode(activeAppSubscriptions));
      return instructions.any((e) => (e as Map).containsKey('CloseSyncStream'));
    }

    startIteration();
    control(
      'subscriptions',
      json.encode({
        'subscribe': {
          'stream': {'name': 'a'},
          'ttl': 3600,
        }
      }),
    );
    activeAppSubscriptions.add({'name': 'a'});

    // Adding the subscription requires another request.
    expect(checkRestart(), isTrue);
    startIteration();

    // Now assume the app calls unsubscribe() on the subscription. Because it
    // has a TTL, this doesn't invalidate the current session.
    activeAppSubscriptions.clear();
    expect(checkRestart(), isFalse);

    // While we're still connected, the TTL of the subscription is running out.
    controller.elapse(const Duration(hours: 1, seconds: 1));
    // So, the client should request a reconnect!
    final instructions =
        control('line_text', json.encode({'token_expires_in': 1800}));
    expect(instructions,
        contains(containsPair('CloseSyncStream', {'hide_disconnect': true})));
  });

  syncTest('persists stream state', (_) {
    control(
      'subscriptions',
      json.encode({
        'subscribe': {
          'stream': {'name': 'a'},
        }
      }),
    );

    control(
      'start',
      json.encode({
        'active_streams': [
          {'name': 'a'}
        ]
      }),
    );
    control(
      'line_text',
      json.encode(
        checkpoint(
          lastOpId: 1,
          buckets: [
            bucketDescription(
              'a',
              subscriptions: [
                {'sub': 0}
              ],
              priority: 1,
            )
          ],
          streams: [stream('a', false)],
        ),
      ),
    );
    control('line_text', json.encode(checkpointComplete()));

    final [row] = db.select('select powersync_offline_sync_status();');
    expect(
        json.decode(row[0]),
        containsPair('streams', [
          {
            'name': 'a',
            'parameters': null,
            // not persisted, only needed for download progress
            'progress': {'total': 0, 'downloaded': 0},
            'priority': null, // same
            'active': true,
            'is_default': false,
            'has_explicit_subscription': true,
            'expires_at': 1740909600,
            'last_synced_at': 1740823200
          }
        ]));
  });
}
