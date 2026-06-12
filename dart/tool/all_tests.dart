import 'package:test/test.dart';

import '../test/crud_test.dart' as crud_test;
import '../test/error_test.dart' as error_test;
import '../test/js_key_encoding_test.dart' as json_key_encoding_test;
import '../test/migration_test.dart' as migration_test;
import '../test/schema_test.dart' as schema_test;
// Skipping sync_local_performance_test because it's slow. It's functionality is
// covered by other tests.
import '../test/sync_stream_test.dart' as sync_stream_test;
import '../test/update_hooks_test.dart' as update_hooks_test;

import '../test/utils/native_test_utils.dart';

/// Runs all native tests.
///
/// We aot-compile this file to run tests with different sanitizers.
void main(List<String> args) {
  testingWithSanitizers = args.single;

  group('crud_test.dart', crud_test.main);
  group('error_test.dart', error_test.main);
  group('js_key_encoding_test.dart', json_key_encoding_test.main);
  group('migration_test.dart', migration_test.main);
  group('schema_test.dart', schema_test.main);
  group('sync_stream_test.dart', sync_stream_test.main);
  group('update_hooks_test.dart', update_hooks_test.main);
}
