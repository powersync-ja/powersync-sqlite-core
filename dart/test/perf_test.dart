import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';
import 'utils/tracking_vfs.dart';
import './schema_test.dart' show schema;

// These test how many filesystem reads and writes are performed during sync_local.
// The real world performane of filesystem operations depend a lot on the specific system.
// For example, on native desktop systems, the performance of temporary filesystem storage could
// be close to memory performance. However, on web and mobile, (temporary) filesystem operations
// could drastically slow down performance. So rather than only testing the real time for these
// queries, we count the number of filesystem operations.
void testFilesystemOperations(
    {bool unique = true,
    int count = 200000,
    int alreadyApplied = 10000,
    int buckets = 10}) {
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
    // Generate dummy data
    // We can replace this with actual similated download operations later
    db.execute('''
BEGIN TRANSACTION;

WITH RECURSIVE generate_rows(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM generate_rows WHERE n < $count
)
INSERT INTO ps_oplog (bucket, op_id, row_type, row_id, key, data, hash)
SELECT 
    (n % $buckets), -- Generate n different buckets
    n,
    'assets',
    ${unique ? 'uuid()' : "'duplicated_id'"},
    uuid(),
    '{"description": "' || n || '", "make": "test", "model": "this is just filler data. this is just filler data. this is just filler data. this is just filler data. this is just filler data. this is just filler data. this is just filler data. "}',
    (n * 17) % 1000000000 -- Some pseudo-random hash
    
FROM generate_rows;

WITH RECURSIVE generate_bucket_rows(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM generate_bucket_rows WHERE n < $buckets
)
INSERT INTO ps_buckets (id, name, last_applied_op)
SELECT 
    (n % $buckets),
    'bucket' || n,
    $alreadyApplied -- simulate a percentage of operations previously applied
    
FROM generate_bucket_rows;

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

    // These are fairly generous limits, to catch significant regressions only.
    expect(vfs.tempWrites, lessThan(count / 50));
    expect(timer.elapsed,
        lessThan(Duration(milliseconds: 100 + (count / 50).round())));
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
    expect(vfs.tempWrites, lessThan(count / 50));
    expect(timer.elapsed,
        lessThan(Duration(milliseconds: 100 + (count / 50).round())));
  });
}

main() {
  group('test filesystem operations with unique ids', () {
    testFilesystemOperations(
        unique: true, count: 500000, alreadyApplied: 10000, buckets: 10);
  });
  group('test filesytem operations with duplicate ids', () {
    // If this takes more than a couple of milliseconds to complete, there is a performance bug
    testFilesystemOperations(
        unique: false, count: 5000, alreadyApplied: 1000, buckets: 10);
  });

  group('test filesystem operations with a small number of changes', () {
    testFilesystemOperations(
        unique: true, count: 100000, alreadyApplied: 95000, buckets: 10);
  });

  group('test filesystem operations with a large number of buckets', () {
    testFilesystemOperations(
        unique: true, count: 100000, alreadyApplied: 10000, buckets: 1000);
  });
}
