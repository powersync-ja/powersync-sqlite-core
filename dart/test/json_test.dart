import 'dart:convert';
import 'dart:typed_data';

import 'package:bson/bson.dart';
import 'package:convert/convert.dart'; // ignore: unused_import
import 'package:sqlite3/common.dart';
import 'package:test/test.dart';

import 'utils/native_test_utils.dart';

void main() {
  late CommonDatabase db;

  setUpAll(() {
    db = openTestDatabase()..select('select powersync_init();');
  });

  Object? Function(Object?, String) powersyncExtract(bool isBson) {
    return (value, key) {
      if (isBson) {
        //print("unhex('${hex.encode(BsonCodec.serialize(value).byteList)}')");
      }

      final [row] = db.select('select powersync_extract(?, ?);', [
        if (isBson) BsonCodec.serialize(value).byteList else json.encode(value),
        key,
      ]);

      return row.columnAt(0);
    };
  }

  void sharedTests(bool isBson) {
    final extract = powersyncExtract(isBson);

    test('missing key', () {
      expect(extract({'foo': 'bar'}, 'baz'), isNull);
    });

    test('string', () {
      expect(extract({'foo': 'bar'}, 'foo'), 'bar');
    });

    test('number', () {
      expect(extract({'foo': 3.14}, 'foo'), 3.14);
      expect(extract({'foo': 42}, 'foo'), 42);
    });

    test('null', () {
      expect(extract({'a': '', 'b': null}, 'b'), isNull);
    });
  }

  group('powersync_extract', () {
    group('json', () {
      sharedTests(false);
    });

    group('bson', () {
      sharedTests(true);

      final extract = powersyncExtract(true);

      test('blob', () {
        final bytes = Uint8List.fromList([1, 2, 3]);
        expect(extract({'a': '', 'b': BsonBinary.from(bytes)}, 'b'), bytes);
      });
    });
  });
}
