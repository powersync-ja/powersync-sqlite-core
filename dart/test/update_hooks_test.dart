import 'dart:convert';

import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

void main() {
  late CommonDatabase db;

  setUp(() async {
    db = openTestDatabase()
      ..select('select powersync_init()')
      ..execute('CREATE TABLE foo (bar INTEGER);')
      ..select("SELECT powersync_update_hooks('install')");
  });

  tearDown(() {
    db.close();
  });

  List<String> collectUpdates() {
    final [row] = db.select("SELECT powersync_update_hooks('get')");
    return (json.decode(row.values[0] as String) as List).cast();
  }

  test('is empty initially', () {
    expect(collectUpdates(), isEmpty);
  });

  test('reports changed tables', () {
    db.execute('INSERT INTO foo DEFAULT VALUES');
    expect(collectUpdates(), ['foo']);
  });

  test('deduplicates tables', () {
    final stmt = db.prepare('INSERT INTO foo (bar) VALUES (?)');
    for (var i = 0; i < 1000; i++) {
      stmt.execute([i]);
    }
    stmt.close();

    expect(collectUpdates(), ['foo']);
  });

  test('does not report changes before end of transaction', () {
    db.execute('BEGIN');
    db.execute('INSERT INTO foo DEFAULT VALUES');
    expect(collectUpdates(), isEmpty);
    db.execute('COMMIT');

    expect(collectUpdates(), ['foo']);
  });

  test('does not report rollbacks', () {
    db.execute('BEGIN');
    db.execute('INSERT INTO foo DEFAULT VALUES');
    expect(collectUpdates(), isEmpty);
    db.execute('ROLLBACK');

    expect(collectUpdates(), isEmpty);
  });
}
