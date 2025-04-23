import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:bson/bson.dart';
import 'package:fake_async/fake_async.dart';
import 'package:file/local.dart';
import 'package:meta/meta.dart';
import 'package:sqlite3/common.dart';
import 'package:sqlite3/sqlite3.dart';
import 'package:sqlite3_test/sqlite3_test.dart';
import 'package:test/test.dart';
import 'package:path/path.dart';

import 'utils/native_test_utils.dart';

@isTest
void syncTest(String description, void Function(FakeAsync controller) body) {
  return test(description, () {
    // Give each test the same starting time to make goldens easier to compare.
    fakeAsync(body, initialTime: DateTime(2025, 3, 1, 10));
  });
}

void main() {
  final vfs = TestSqliteFileSystem(fs: const LocalFileSystem());

  setUpAll(() {
    loadExtension();
    sqlite3.registerVirtualFileSystem(vfs, makeDefault: false);
  });
  tearDownAll(() => sqlite3.unregisterVirtualFileSystem(vfs));

  group('text lines', () {
    _syncTests(vfs: vfs, isBson: false);
  });

  group('bson lines', () {
    _syncTests(vfs: vfs, isBson: true);
  });
}

void _syncTests<T>({
  required VirtualFileSystem vfs,
  required bool isBson,
}) {
  late CommonDatabase db;
  late SyncLinesGoldenTest matcher;

  List<Object?> invokeControlRaw(String operation, Object? data) {
    final [row] =
        db.select('SELECT powersync_control(?, ?)', [operation, data]);
    return jsonDecode(row.columnAt(0));
  }

  List<Object?> invokeControl(String operation, Object? data) {
    if (matcher.enabled) {
      // Trace through golden matcher
      return matcher.invoke(operation, data);
    } else {
      final [row] =
          db.select('SELECT powersync_control(?, ?)', [operation, data]);
      return jsonDecode(row.columnAt(0));
    }
  }

  setUp(() async {
    db = openTestDatabase(vfs)
      ..select('select powersync_init();')
      ..select('select powersync_replace_schema(?)', [json.encode(_schema)])
      ..execute('update ps_kv set value = ?2 where key = ?1',
          ['client_id', 'test-test-test-test']);

    matcher = SyncLinesGoldenTest(isBson, invokeControlRaw);
  });

  tearDown(() {
    matcher.finish();
    db.dispose();
  });

  List<Object?> syncLine(Object? line) {
    if (isBson) {
      final serialized = BsonCodec.serialize(line).byteList;
      // print(serialized.asRustByteString);
      return invokeControl('line_binary', serialized);
    } else {
      return invokeControl('line_text', jsonEncode(line));
    }
  }

  List<Object?> pushSyncData(
      String bucket, String opId, String rowId, Object op, Object? data) {
    return syncLine({
      'data': {
        'bucket': bucket,
        'has_more': false,
        'after': null,
        'next_after': null,
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
      },
    });
  }

  group('goldens', () {
    syncTest('starting stream', (_) {
      matcher.load('starting_stream');
      matcher.invoke('start', null);
    });

    syncTest('simple sync iteration', (_) {
      matcher.load('simple_iteration');
      matcher.invoke('start', null);

      syncLine({
        'checkpoint': {
          'last_op_id': '1',
          'write_checkpoint': null,
          'buckets': [
            {
              'bucket': 'a',
              'checksum': 0,
              'priority': 3,
              'count': 1,
            }
          ],
        },
      });
      pushSyncData('a', '1', '1', 'PUT', {'col': 'hi'});

      syncLine({
        'checkpoint_complete': {'last_op_id': '1'},
      });
    });
  });
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

extension on Uint8List {
  // ignore: unused_element
  String get asRustByteString {
    final buffer = StringBuffer('b"');

    for (final byte in this) {
      switch (byte) {
        case >= 32 && < 127:
          buffer.writeCharCode(byte);
        default:
          // Escape
          buffer.write('\\x${byte.toRadixString(16).padLeft(2, '0')}');
      }
    }

    buffer.write('"');
    return buffer.toString();
  }
}

final class SyncLinesGoldenTest {
  static bool _update = Platform.environment['UPDATE_GOLDENS'] == '1';

  final List<Object?> Function(String operation, Object? data) _invokeControl;

  String? name;

  final bool isBson;
  final List<ExpectedSyncLine> expectedLines = [];
  final List<ExpectedSyncLine> actualLines = [];

  String get path => join('test', 'goldens', '$name.json');

  bool get enabled => name != null;

  SyncLinesGoldenTest(this.isBson, this._invokeControl);

  ExpectedSyncLine get _nextExpectation {
    return expectedLines[actualLines.length];
  }

  Never _mismatch() {
    throw 'Golden test for sync lines failed, set UPDATE_GOLDENS=1 to update';
  }

  void load(String name) {
    this.name = name;
    final file = File(path);
    try {
      final loaded = json.decode(file.readAsStringSync());
      for (final entry in loaded) {
        expectedLines.add(ExpectedSyncLine.fromJson(entry));
      }
    } catch (e) {
      if (!_update) {
        rethrow;
      }
    }
  }

  List<Object?> invoke(String operation, Object? data) {
    final matchData = switch (data) {
      final String s => json.decode(s),
      _ => data,
    };

    if (_update) {
      final result = _invokeControl(operation, data);
      actualLines.add(ExpectedSyncLine(operation, matchData, result));
      return result;
    } else {
      final expected = _nextExpectation;
      if (!isBson) {
        // We only want to compare the JSON inputs. We compare outputs
        // regardless of the encoding mode.
        if (expected.operation != operation ||
            json.encode(expected.data) != json.encode(matchData)) {
          _mismatch();
        }
      }

      final result = _invokeControl(operation, data);
      if (json.encode(result) != json.encode(expected.output)) {
        _mismatch();
      }

      actualLines.add(ExpectedSyncLine(operation, matchData, result));
      return result;
    }
  }

  void finish() {
    if (_update && enabled) {
      if (!isBson) {
        File(path).writeAsStringSync(
            JsonEncoder.withIndent('  ').convert(actualLines));
      }
    } else if (expectedLines.length != actualLines.length) {
      _mismatch();
    }
  }
}

final class ExpectedSyncLine {
  final String operation;
  final Object? data;
  final List<Object?> output;

  ExpectedSyncLine(this.operation, this.data, this.output);

  factory ExpectedSyncLine.fromJson(Map<String, Object?> json) {
    return ExpectedSyncLine(
        json['operation'] as String, json['data'], json['output'] as List);
  }

  Map<String, Object?> toJson() {
    return {
      'operation': operation,
      'data': data,
      'output': output,
    };
  }
}
