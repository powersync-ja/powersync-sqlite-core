@Tags(['slow'])
library;

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
    int buckets = 10,
    bool rawQueries = false}) {
  late TrackingFileSystem vfs;
  late CommonDatabase db;
  final skip = rawQueries == false ? 'For manual query testing only' : null;

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
    // Optional: set a custom cache size - it affects the number of filesystem operations.
    // db.execute('PRAGMA cache_size=-50000');
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
    // Enable this to see stats for initial data generation
    // print('init stats: ${vfs.stats()}');

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

  // The tests below are for comparing different queries, not run as part of the
  // standard test suite.

  test('sync_local new query', () {
    // This is the query we're using now.
    // This query only uses a single TEMP B-TREE for the GROUP BY operation,
    // leading to fairly efficient execution.

    // QUERY PLAN
    // |--CO-ROUTINE updated_rows
    // |  `--COMPOUND QUERY
    // |     |--LEFT-MOST SUBQUERY
    // |     |  |--SCAN buckets
    // |     |  `--SEARCH b USING INDEX ps_oplog_opid (bucket=? AND op_id>?)
    // |     `--UNION ALL
    // |        `--SCAN ps_updated_rows
    // |--SCAN b
    // |--USE TEMP B-TREE FOR GROUP BY
    // `--CORRELATED SCALAR SUBQUERY 3
    //    `--SEARCH r USING INDEX ps_oplog_row (row_type=? AND row_id=?)
    //
    // For details on the max(r.op_id) clause, see:
    // https://sqlite.org/lang_select.html#bare_columns_in_an_aggregate_query
    // > If there is exactly one min() or max() aggregate in the query, then all bare columns in the result
    // > set take values from an input row which also contains the minimum or maximum.

    var timer = Stopwatch()..start();
    final q = '''
-- 1. Filter oplog by the ops added but not applied yet (oplog b).
--    We do not do any DISTINCT operation here, since that introduces a temp b-tree.
--    We filter out duplicates using the GROUP BY below.
WITH updated_rows AS (
    SELECT b.row_type, b.row_id FROM ps_buckets AS buckets
        CROSS JOIN ps_oplog AS b ON b.bucket = buckets.id
        AND (b.op_id > buckets.last_applied_op)
    UNION ALL SELECT row_type, row_id FROM ps_updated_rows
)

-- 2. Find *all* current ops over different buckets for those objects (oplog r).
SELECT
    b.row_type,
    b.row_id,
    (
        -- 3. For each unique row, select the data from the latest oplog entry.
        -- The max(r.op_id) clause is used to select the latest oplog entry.
        -- The iif is to avoid the max(r.op_id) column ending up in the results.
        SELECT iif(max(r.op_id), r.data, null)
                 FROM ps_oplog r
                WHERE r.row_type = b.row_type
                  AND r.row_id = b.row_id

    ) as data
    FROM updated_rows b
    -- Group for (2)
    GROUP BY b.row_type, b.row_id;
''';
    db.select(q);
    print('${timer.elapsed.inMilliseconds}ms ${vfs.stats()}');
  }, skip: skip);

  test('old query', () {
    // This query used a TEMP B-TREE for the first part of finding unique updated rows,
    // then another TEMP B-TREE for the second GROUP BY. This redundant B-TREE causes
    // a lot of temporary storage overhead.

    // QUERY PLAN
    // |--CO-ROUTINE updated_rows
    // |  `--COMPOUND QUERY
    // |     |--LEFT-MOST SUBQUERY
    // |     |  |--SCAN buckets
    // |     |  `--SEARCH b USING INDEX ps_oplog_opid (bucket=? AND op_id>?)
    // |     `--UNION USING TEMP B-TREE
    // |        `--SCAN ps_updated_rows
    // |--SCAN b
    // |--SEARCH r USING INDEX ps_oplog_row (row_type=? AND row_id=?) LEFT-JOIN
    // `--USE TEMP B-TREE FOR GROUP BY

    var timer = Stopwatch()..start();
    final q = '''
WITH updated_rows AS (
  SELECT DISTINCT b.row_type, b.row_id FROM ps_buckets AS buckets
    CROSS JOIN ps_oplog AS b ON b.bucket = buckets.id
  AND (b.op_id > buckets.last_applied_op)
  UNION SELECT row_type, row_id FROM ps_updated_rows
)
SELECT b.row_type as type,
    b.row_id as id,
    r.data as data,
    count(r.bucket) as buckets,
    max(r.op_id) as op_id
FROM updated_rows b
    LEFT OUTER JOIN ps_oplog AS r
                ON r.row_type = b.row_type
                    AND r.row_id = b.row_id
GROUP BY b.row_type, b.row_id;
''';
    db.select(q);
    print('${timer.elapsed.inMilliseconds}ms ${vfs.stats()}');
  }, skip: skip);

  test('group_by query', () {
    // This is similar to the new query, but uses a GROUP BY .. LIMIT 1 clause instead of the max(op_id) hack.
    // It is similar in the number of filesystem operations, but slightly slower in real time.

    // QUERY PLAN
    // |--CO-ROUTINE updated_rows
    // |  `--COMPOUND QUERY
    // |     |--LEFT-MOST SUBQUERY
    // |     |  |--SCAN buckets
    // |     |  `--SEARCH b USING INDEX ps_oplog_opid (bucket=? AND op_id>?)
    // |     `--UNION ALL
    // |        `--SCAN ps_updated_rows
    // |--SCAN b
    // |--USE TEMP B-TREE FOR GROUP BY
    // `--CORRELATED SCALAR SUBQUERY 3
    //    |--SEARCH r USING INDEX ps_oplog_row (row_type=? AND row_id=?)
    //    `--USE TEMP B-TREE FOR ORDER BY

    var timer = Stopwatch()..start();
    final q = '''
WITH updated_rows AS (
    SELECT b.row_type, b.row_id FROM ps_buckets AS buckets
        CROSS JOIN ps_oplog AS b ON b.bucket = buckets.id
        AND (b.op_id > buckets.last_applied_op)
    UNION ALL SELECT row_type, row_id FROM ps_updated_rows
)

SELECT
    b.row_type,
    b.row_id,
    (
        SELECT r.data FROM ps_oplog r
                WHERE r.row_type = b.row_type
                  AND r.row_id = b.row_id
                  ORDER BY r.op_id DESC
                  LIMIT 1

    ) as data
    FROM updated_rows b
    GROUP BY b.row_type, b.row_id;
''';
    db.select(q);
    print('${timer.elapsed.inMilliseconds}ms ${vfs.stats()}');
  }, skip: skip);

  test('full scan query', () {
    // This is a nice alternative for initial sync or resyncing large amounts of data.
    // This is very efficient for reading all data, but not for incremental updates.

    // QUERY PLAN
    // |--SCAN r USING INDEX ps_oplog_row
    // |--CORRELATED SCALAR SUBQUERY 1
    // |  `--SEARCH ps_buckets USING INTEGER PRIMARY KEY (rowid=?)
    // `--CORRELATED SCALAR SUBQUERY 1
    //    `--SEARCH ps_buckets USING INTEGER PRIMARY KEY (rowid=?)

    var timer = Stopwatch()..start();
    final q = '''
SELECT r.row_type as type,
    r.row_id as id,
    r.data as data,
    max(r.op_id) as op_id,
    sum((select 1 from ps_buckets where ps_buckets.id = r.bucket and r.op_id > ps_buckets.last_applied_op)) as buckets
    
FROM ps_oplog r
GROUP BY r.row_type, r.row_id
HAVING buckets > 0;
''';
    db.select(q);
    print('${timer.elapsed.inMilliseconds}ms ${vfs.stats()}');
  }, skip: skip);
}

main() {
  group('test filesystem operations with unique ids', () {
    testFilesystemOperations(
        unique: true,
        count: 500000,
        alreadyApplied: 10000,
        buckets: 10,
        rawQueries: false);
  });
  group('test filesytem operations with duplicate ids', () {
    // If this takes more than a couple of milliseconds to complete, there is a performance bug
    testFilesystemOperations(
        unique: false,
        count: 500000,
        alreadyApplied: 1000,
        buckets: 10,
        rawQueries: false);
  });

  group('test filesystem operations with a small number of changes', () {
    testFilesystemOperations(
        unique: true,
        count: 100000,
        alreadyApplied: 95000,
        buckets: 10,
        rawQueries: false);
  });

  group('test filesystem operations with a large number of buckets', () {
    testFilesystemOperations(
        unique: true,
        count: 100000,
        alreadyApplied: 10000,
        buckets: 1000,
        rawQueries: false);
  });
}
