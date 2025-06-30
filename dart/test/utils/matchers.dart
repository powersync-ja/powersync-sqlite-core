import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

Matcher isSqliteException(int code, dynamic message) {
  return isA<SqliteException>()
      .having((e) => e.extendedResultCode, 'extendedResultCode', code)
      .having((e) => e.message, 'message', message);
}
