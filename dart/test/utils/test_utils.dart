import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

/// Creates a `checkpoint` line.
Object checkpoint({
  required int lastOpId,
  List<Object> buckets = const [],
  String? writeCheckpoint,
  List<(String, bool)> streams = const [],
}) {
  return {
    'checkpoint': {
      'last_op_id': '$lastOpId',
      'write_checkpoint': null,
      'buckets': buckets,
      'streams': [
        for (final (name, isDefault) in streams)
          {'name': name, 'is_default': isDefault},
      ],
    }
  };
}

Object bucketDescription(
  String name, {
  int checksum = 0,
  int priority = 3,
  int count = 1,
  Object? subscriptions,
}) {
  return {
    'bucket': name,
    'checksum': checksum,
    'priority': priority,
    'count': count,
    'subscriptions': subscriptions,
  };
}

Matcher isSqliteException(int code, dynamic message) {
  return isA<SqliteException>()
      .having((e) => e.extendedResultCode, 'extendedResultCode', code)
      .having((e) => e.message, 'message', message);
}

const testSchema = {
  'tables': [
    {
      'name': 'items',
      'columns': [
        {'name': 'col', 'type': 'text'}
      ],
    }
  ]
};
