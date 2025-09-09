import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

/// Creates a `checkpoint` line.
Object checkpoint({
  required int lastOpId,
  List<Object> buckets = const [],
  String? writeCheckpoint,
  List<Object> streams = const [],
}) {
  return {
    'checkpoint': {
      'last_op_id': '$lastOpId',
      'write_checkpoint': writeCheckpoint,
      'buckets': buckets,
      'streams': streams,
    }
  };
}

Object stream(String name, bool isDefault, {List<Object> errors = const []}) {
  return {'name': name, 'is_default': isDefault, 'errors': errors};
}

/// Creates a `checkpoint_complete` or `partial_checkpoint_complete` line.
Object checkpointComplete({int? priority, String lastOpId = '1'}) {
  return {
    priority == null ? 'checkpoint_complete' : 'partial_checkpoint_complete': {
      'last_op_id': lastOpId,
      if (priority != null) 'priority': priority,
    },
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
    if (subscriptions != null) 'subscriptions': subscriptions,
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
