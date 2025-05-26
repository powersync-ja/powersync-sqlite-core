import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';
import 'utils/tracking_vfs.dart';
import './schema_test.dart' show schema;

void main() {
  late TrackingFileSystem vfs;
  late CommonDatabase db;

  setUpAll(() {
    loadExtension();
  });

  setUp(() async {
    // Needs an unique name per test file to avoid concurrency issues
    vfs = new TrackingFileSystem(
        parent: new InMemoryFileSystem(), name: 'perf-test-vfs');
    sqlite3.registerVirtualFileSystem(vfs, makeDefault: false);
    db = openTestDatabase(vfs: vfs, fileName: 'test.db');
  });

  tearDown(() {
    db.dispose();
    sqlite3.unregisterVirtualFileSystem(vfs);
  });

  setUp(() {
    db.execute('SELECT powersync_replace_schema(?)', [json.encode(schema)]);
    db.execute('''
BEGIN TRANSACTION;

WITH RECURSIVE generate_rows(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM generate_rows WHERE n < 200000
)
INSERT INTO ps_oplog (bucket, op_id, row_type, row_id, key, data, hash)
SELECT 
    (n % 10), -- Generate 10 different buckets
    n,
    'assets',
    uuid(),
    uuid(),
    '{"description": "' || n || '", "make": "test", "model": "this is just filler data. this is just filler data. this is just filler data. this is just filler data. this is just filler data. this is just filler data. this is just filler data. "}',
    (n * 17) % 1000000000 -- Some pseudo-random hash
    
FROM generate_rows;

WITH RECURSIVE generate_rows(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM generate_rows WHERE n < 10
)
INSERT INTO ps_buckets (id, name, last_applied_op)
SELECT 
    (n % 10),
    'bucket' || n,
    10000
    
FROM generate_rows;

COMMIT;
''');
    print('init stats: ${vfs.stats()}');

    vfs.clearStats();
  });

  test('sync_local (full)', () {
    var timer = Stopwatch()..start();
    db.select('insert into powersync_operations(op, data) values(?, ?)',
        ['sync_local', '']);
    print('${timer.elapsed.inMilliseconds}ms ${vfs.stats()}');
  });

  test('sync_local (partial)', () {
    var timer = Stopwatch()..start();
    db.select('insert into powersync_operations(op, data) values(?, ?)', [
      'sync_local',
      jsonEncode({
        'buckets': ['bucket0', 'bucket3', 'bucket4', 'bucket5', 'bucket6'],
        'priority': 2
      })
    ]);
    print('${timer.elapsed.inMilliseconds}ms ${vfs.stats()}');
  });
}
