/// Data with some records in actual tables but not in ps_oplog
const dataBroken = '''
;INSERT INTO ps_buckets(id, name, last_applied_op, last_op, target_op, add_checksum, op_checksum, pending_delete) VALUES
  (1, 'b1', 0, 0, 0, 0, 120, 0),
  (2, 'b2', 0, 0, 0, 0, 3, 0)
;INSERT INTO ps_oplog(bucket, op_id, row_type, row_id, key, data, hash) VALUES
  (1, 1, 'todos', 't1', '', '{}', 100),
  (1, 2, 'todos', 't2', '', '{}', 20),
  (2, 3, 'lists', 'l1', '', '{}', 3)
;INSERT INTO ps_data__lists(id, data) VALUES
  ('l1', '{}'),
  ('l3', '{}')
;INSERT INTO ps_data__todos(id, data) VALUES
  ('t1', '{}'),
  ('t2', '{}'),
  ('t3', '{}')
''';

/// Data after applying the migration fix, but before sync_local
const dataMigrated = '''
;INSERT INTO ps_buckets(id, name, last_applied_op, last_op, target_op, add_checksum, op_checksum, pending_delete, count_at_last, count_since_last, download_size) VALUES
  (1, 'b1', 0, 0, 0, 0, 120, 0, 0, 0, 0),
  (2, 'b2', 0, 0, 0, 0, 3, 0, 0, 0, 0)
;INSERT INTO ps_oplog(bucket, op_id, row_type, row_id, key, data, hash) VALUES
  (1, 1, 'todos', 't1', '', '{}', 100),
  (1, 2, 'todos', 't2', '', '{}', 20),
  (2, 3, 'lists', 'l1', '', '{}', 3)
;INSERT INTO ps_updated_rows(row_type, row_id) VALUES
  ('lists', 'l3'),
  ('todos', 't3')
;INSERT INTO ps_data__lists(id, data) VALUES
  ('l1', '{}'),
  ('l3', '{}')
;INSERT INTO ps_data__todos(id, data) VALUES
  ('t1', '{}'),
  ('t2', '{}'),
  ('t3', '{}')
''';

/// Data after applying the migration fix and sync_local
const dataFixed = '''
;INSERT INTO ps_buckets(id, name, last_applied_op, last_op, target_op, add_checksum, op_checksum, pending_delete, count_at_last, count_since_last, download_size) VALUES
  (1, 'b1', 0, 0, 0, 0, 120, 0, 0, 0, 0),
  (2, 'b2', 0, 0, 0, 0, 3, 0, 0, 0, 0)
;INSERT INTO ps_oplog(bucket, op_id, row_type, row_id, key, data, hash) VALUES
  (1, 1, 'todos', 't1', '', '{}', 100),
  (1, 2, 'todos', 't2', '', '{}', 20),
  (2, 3, 'lists', 'l1', '', '{}', 3)
;INSERT INTO ps_data__lists(id, data) VALUES
  ('l1', '{}')
;INSERT INTO ps_data__todos(id, data) VALUES
  ('t1', '{}'),
  ('t2', '{}')
''';
