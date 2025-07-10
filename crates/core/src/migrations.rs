extern crate alloc;

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use sqlite::ResultCode;
use sqlite_nostd as sqlite;
use sqlite_nostd::{Connection, Context};

use crate::error::{PSResult, PowerSyncError};
use crate::fix_data::apply_v035_fix;
use crate::sync::BucketPriority;

pub const LATEST_VERSION: i32 = 11;

pub fn powersync_migrate(
    ctx: *mut sqlite::context,
    target_version: i32,
) -> Result<(), PowerSyncError> {
    let local_db = ctx.db_handle();

    // language=SQLite
    local_db.exec_safe(
        "\
CREATE TABLE IF NOT EXISTS ps_migration(id INTEGER PRIMARY KEY, down_migrations TEXT)",
    )?;

    // language=SQLite
    let current_version_stmt =
        local_db.prepare_v2("SELECT ifnull(max(id), 0) as version FROM ps_migration")?;
    let rc = current_version_stmt.step()?;
    if rc != ResultCode::ROW {
        return Err(PowerSyncError::unknown_internal());
    }

    let mut current_version = current_version_stmt.column_int(0);

    while current_version > target_version {
        // Run down migrations.
        // This is rare, we don't worry about optimizing this.

        current_version_stmt.reset()?;

        let down_migrations_stmt = local_db.prepare_v2("select e.value ->> 'sql' as sql from (select id, down_migrations from ps_migration where id > ?1 order by id desc limit 1) m, json_each(m.down_migrations) e")?;
        down_migrations_stmt.bind_int(1, target_version)?;

        let mut down_sql: Vec<String> = alloc::vec![];

        while down_migrations_stmt.step()? == ResultCode::ROW {
            let sql = down_migrations_stmt.column_text(0)?;
            down_sql.push(sql.to_string());
        }

        for sql in down_sql {
            let rs = local_db.exec_safe(&sql);
            if let Err(code) = rs {
                return Err(PowerSyncError::from_sqlite(
                    local_db,
                    code,
                    format!(
                        "Down migration failed for {:} {:} {:}",
                        current_version,
                        sql,
                        local_db
                            .errmsg()
                            .unwrap_or(String::from("Conversion error"))
                    ),
                ));
            }
        }

        // Refresh the version
        current_version_stmt.reset()?;
        let rc = current_version_stmt.step()?;
        if rc != ResultCode::ROW {
            return Err(PowerSyncError::from_sqlite(
                local_db,
                rc,
                "Down migration failed - could not get version",
            ));
        }
        let new_version = current_version_stmt.column_int(0);
        if new_version >= current_version {
            // Database down from version $currentVersion to $version failed - version not updated after down migration
            return Err(PowerSyncError::down_migration_did_not_update_version(
                current_version,
            ));
        }
        current_version = new_version;
    }
    current_version_stmt.reset()?;

    if current_version < 1 {
        // language=SQLite
        local_db
            .exec_safe(
                "
CREATE TABLE ps_oplog(
bucket TEXT NOT NULL,
op_id INTEGER NOT NULL,
op INTEGER NOT NULL,
row_type TEXT,
row_id TEXT,
key TEXT,
data TEXT,
hash INTEGER NOT NULL,
superseded INTEGER NOT NULL);

CREATE INDEX ps_oplog_by_row ON ps_oplog (row_type, row_id) WHERE superseded = 0;
CREATE INDEX ps_oplog_by_opid ON ps_oplog (bucket, op_id);
CREATE INDEX ps_oplog_by_key ON ps_oplog (bucket, key) WHERE superseded = 0;

CREATE TABLE ps_buckets(
name TEXT PRIMARY KEY,
last_applied_op INTEGER NOT NULL DEFAULT 0,
last_op INTEGER NOT NULL DEFAULT 0,
target_op INTEGER NOT NULL DEFAULT 0,
add_checksum INTEGER NOT NULL DEFAULT 0,
pending_delete INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE ps_untyped(type TEXT NOT NULL, id TEXT NOT NULL, data TEXT, PRIMARY KEY (type, id));

CREATE TABLE ps_crud (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT);

INSERT INTO ps_migration(id, down_migrations) VALUES(1, NULL);
",
            )
            .into_db_result(local_db)?;
    }

    if current_version < 2 && target_version >= 2 {
        // language=SQLite
        local_db.exec_safe("\
CREATE TABLE ps_tx(id INTEGER PRIMARY KEY NOT NULL, current_tx INTEGER, next_tx INTEGER);
INSERT INTO ps_tx(id, current_tx, next_tx) VALUES(1, NULL, 1);

ALTER TABLE ps_crud ADD COLUMN tx_id INTEGER;

INSERT INTO ps_migration(id, down_migrations) VALUES(2, json_array(json_object('sql', 'DELETE FROM ps_migration WHERE id >= 2', 'params', json_array()), json_object('sql', 'DROP TABLE ps_tx', 'params', json_array()), json_object('sql', 'ALTER TABLE ps_crud DROP COLUMN tx_id', 'params', json_array())));
").into_db_result(local_db)?;
    }

    if current_version < 3 && target_version >= 3 {
        // language=SQLite
        local_db.exec_safe("\
CREATE TABLE ps_kv(key TEXT PRIMARY KEY NOT NULL, value BLOB);
INSERT INTO ps_kv(key, value) values('client_id', uuid());

INSERT INTO ps_migration(id, down_migrations) VALUES(3, json_array(json_object('sql', 'DELETE FROM ps_migration WHERE id >= 3'), json_object('sql', 'DROP TABLE ps_kv')));
  ").into_db_result(local_db)?;
    }

    if current_version < 4 && target_version >= 4 {
        // language=SQLite
        local_db.exec_safe("\
ALTER TABLE ps_buckets ADD COLUMN op_checksum INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ps_buckets ADD COLUMN remove_operations INTEGER NOT NULL DEFAULT 0;

UPDATE ps_buckets SET op_checksum = (
SELECT IFNULL(SUM(ps_oplog.hash), 0) & 0xffffffff FROM ps_oplog WHERE ps_oplog.bucket = ps_buckets.name
);

INSERT INTO ps_migration(id, down_migrations)
VALUES(4,
  json_array(
    json_object('sql', 'DELETE FROM ps_migration WHERE id >= 4'),
    json_object('sql', 'ALTER TABLE ps_buckets DROP COLUMN op_checksum'),
    json_object('sql', 'ALTER TABLE ps_buckets DROP COLUMN remove_operations')
  ));
  ").into_db_result(local_db)?;
    }

    if current_version < 5 && target_version >= 5 {
        // Start by dropping all triggers on views (but not views tables).
        // This is because the triggers are restructured in this version, and
        // need to be re-created from scratch. Not dropping them can make it
        // refer to tables or columns not existing anymore, which can case
        // issues later on.
        //
        // Similarly, dropping the views themselves can cause issues with
        // user-defined triggers that refer to them.
        //
        // The same applies for the down migration, except there we do drop
        // the views, since we cannot use the `powersync_views` view.
        // Down migrations are less common, so we're okay about that breaking
        // in some cases.

        // language=SQLite
        local_db
          .exec_safe(
              "\
UPDATE powersync_views SET
        delete_trigger_sql = '',
        update_trigger_sql = '',
        insert_trigger_sql = '';

ALTER TABLE ps_buckets RENAME TO ps_buckets_old;
ALTER TABLE ps_oplog RENAME TO ps_oplog_old;

CREATE TABLE ps_buckets(
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  last_applied_op INTEGER NOT NULL DEFAULT 0,
  last_op INTEGER NOT NULL DEFAULT 0,
  target_op INTEGER NOT NULL DEFAULT 0,
  add_checksum INTEGER NOT NULL DEFAULT 0,
  op_checksum INTEGER NOT NULL DEFAULT 0,
  pending_delete INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE UNIQUE INDEX ps_buckets_name ON ps_buckets (name);

CREATE TABLE ps_oplog(
  bucket INTEGER NOT NULL,
  op_id INTEGER NOT NULL,
  row_type TEXT,
  row_id TEXT,
  key TEXT,
  data TEXT,
  hash INTEGER NOT NULL) STRICT;

CREATE INDEX ps_oplog_row ON ps_oplog (row_type, row_id);
CREATE INDEX ps_oplog_opid ON ps_oplog (bucket, op_id);
CREATE INDEX ps_oplog_key ON ps_oplog (bucket, key);

CREATE TABLE ps_updated_rows(
  row_type TEXT,
  row_id TEXT,
  PRIMARY KEY(row_type, row_id)) STRICT, WITHOUT ROWID;

INSERT INTO ps_buckets(name, last_applied_op, last_op, target_op, add_checksum, op_checksum, pending_delete)
SELECT name, last_applied_op, last_op, target_op, add_checksum, op_checksum, pending_delete FROM ps_buckets_old;

DROP TABLE ps_buckets_old;

INSERT INTO ps_oplog(bucket, op_id, row_type, row_id, key, data, hash)
SELECT ps_buckets.id, oplog.op_id, oplog.row_type, oplog.row_id, oplog.key, oplog.data, oplog.hash
  FROM ps_oplog_old oplog
  JOIN ps_buckets
    ON ps_buckets.name = oplog.bucket
    WHERE oplog.superseded = 0 AND oplog.op = 3
    ORDER BY oplog.bucket, oplog.op_id;

INSERT OR IGNORE INTO ps_updated_rows(row_type, row_id)
SELECT row_type, row_id
 FROM ps_oplog_old oplog
 WHERE oplog.op != 3;

UPDATE ps_buckets SET add_checksum = 0xffffffff & (add_checksum + (
SELECT IFNULL(SUM(oplog.hash), 0)
  FROM ps_oplog_old oplog
  WHERE oplog.bucket = ps_buckets.name
    AND (oplog.superseded = 1 OR oplog.op != 3)
));

UPDATE ps_buckets SET op_checksum = 0xffffffff & (op_checksum - (
  SELECT IFNULL(SUM(oplog.hash), 0)
    FROM ps_oplog_old oplog
    WHERE oplog.bucket = ps_buckets.name
      AND (oplog.superseded = 1 OR oplog.op != 3)
));

DROP TABLE ps_oplog_old;

INSERT INTO ps_migration(id, down_migrations)
VALUES(5,
  json_array(
    -- Drop existing views and triggers if any
    json_object('sql', 'SELECT powersync_drop_view(view.name)\n  FROM sqlite_master view\n  WHERE view.type = ''view''\n    AND view.sql GLOB  ''*-- powersync-auto-generated'''),

    json_object('sql', 'ALTER TABLE ps_buckets RENAME TO ps_buckets_5'),
    json_object('sql', 'ALTER TABLE ps_oplog RENAME TO ps_oplog_5'),
    json_object('sql', 'CREATE TABLE ps_buckets(\n  name TEXT PRIMARY KEY,\n  last_applied_op INTEGER NOT NULL DEFAULT 0,\n  last_op INTEGER NOT NULL DEFAULT 0,\n  target_op INTEGER NOT NULL DEFAULT 0,\n  add_checksum INTEGER NOT NULL DEFAULT 0,\n  pending_delete INTEGER NOT NULL DEFAULT 0\n, op_checksum INTEGER NOT NULL DEFAULT 0, remove_operations INTEGER NOT NULL DEFAULT 0)'),
    json_object('sql', 'INSERT INTO ps_buckets(name, last_applied_op, last_op, target_op, add_checksum, op_checksum, pending_delete)\n    SELECT name, last_applied_op, last_op, target_op, add_checksum, op_checksum, pending_delete FROM ps_buckets_5'),
    json_object('sql', 'CREATE TABLE ps_oplog(\n  bucket TEXT NOT NULL,\n  op_id INTEGER NOT NULL,\n  op INTEGER NOT NULL,\n  row_type TEXT,\n  row_id TEXT,\n  key TEXT,\n  data TEXT,\n  hash INTEGER NOT NULL,\n  superseded INTEGER NOT NULL)'),
    json_object('sql', 'CREATE INDEX ps_oplog_by_row ON ps_oplog (row_type, row_id) WHERE superseded = 0'),
    json_object('sql', 'CREATE INDEX ps_oplog_by_opid ON ps_oplog (bucket, op_id)'),
    json_object('sql', 'CREATE INDEX ps_oplog_by_key ON ps_oplog (bucket, key) WHERE superseded = 0'),
    json_object('sql', 'INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, key, data, hash, superseded)\n    SELECT ps_buckets_5.name, oplog.op_id, 3, oplog.row_type, oplog.row_id, oplog.key, oplog.data, oplog.hash, 0\n    FROM ps_oplog_5 oplog\n    JOIN ps_buckets_5\n        ON ps_buckets_5.id = oplog.bucket'),
    json_object('sql', 'DROP TABLE ps_oplog_5'),
    json_object('sql', 'DROP TABLE ps_buckets_5'),
    json_object('sql', 'INSERT INTO ps_oplog(bucket, op_id, op, row_type, row_id, hash, superseded)\n    SELECT ''$local'', 1, 4, r.row_type, r.row_id, 0, 0\n    FROM ps_updated_rows r'),
    json_object('sql', 'INSERT OR REPLACE INTO ps_buckets(name, pending_delete, last_op, target_op) VALUES(''$local'', 1, 0, 9223372036854775807)'),
    json_object('sql', 'DROP TABLE ps_updated_rows'),

    json_object('sql', 'DELETE FROM ps_migration WHERE id >= 5')
  ));
  ",
          )
          .into_db_result(local_db)?;
    }

    if current_version < 6 && target_version >= 6 {
        if current_version != 0 {
            // Remove dangling rows, but skip if the database is created from scratch.
            apply_v035_fix(local_db)?;
        }

        local_db
            .exec_safe(
                "\
INSERT INTO ps_migration(id, down_migrations)
VALUES(6,
json_array(
  json_object('sql', 'DELETE FROM ps_migration WHERE id >= 6')
));
",
            )
            .into_db_result(local_db)?;
    }

    if current_version < 7 && target_version >= 7 {
        const SENTINEL_PRIORITY: i32 = BucketPriority::SENTINEL.number;
        let stmt = format!("\
CREATE TABLE ps_sync_state (
  priority INTEGER NOT NULL,
  last_synced_at TEXT NOT NULL
) STRICT;
INSERT OR IGNORE INTO ps_sync_state (priority, last_synced_at)
  SELECT {}, value from ps_kv where key = 'last_synced_at';

INSERT INTO ps_migration(id, down_migrations)
VALUES(7,
json_array(
json_object('sql', 'INSERT OR REPLACE INTO ps_kv(key, value) SELECT ''last_synced_at'', last_synced_at FROM ps_sync_state WHERE priority = {}'),
json_object('sql', 'DROP TABLE ps_sync_state'),
json_object('sql', 'DELETE FROM ps_migration WHERE id >= 7')
));
", SENTINEL_PRIORITY, SENTINEL_PRIORITY);

        local_db.exec_safe(&stmt).into_db_result(local_db)?;
    }

    if current_version < 8 && target_version >= 8 {
        let stmt = "\
ALTER TABLE ps_sync_state RENAME TO ps_sync_state_old;
CREATE TABLE ps_sync_state (
  priority INTEGER NOT NULL PRIMARY KEY,
  last_synced_at TEXT NOT NULL
) STRICT;
INSERT INTO ps_sync_state (priority, last_synced_at)
  SELECT priority, MAX(last_synced_at) FROM ps_sync_state_old GROUP BY priority;
DROP TABLE ps_sync_state_old;
INSERT INTO ps_migration(id, down_migrations) VALUES(8, json_array(
json_object('sql', 'ALTER TABLE ps_sync_state RENAME TO ps_sync_state_new'),
json_object('sql', 'CREATE TABLE ps_sync_state (\n  priority INTEGER NOT NULL,\n  last_synced_at TEXT NOT NULL\n) STRICT'),
json_object('sql', 'INSERT INTO ps_sync_state SELECT * FROM ps_sync_state_new'),
json_object('sql', 'DROP TABLE ps_sync_state_new'),
json_object('sql', 'DELETE FROM ps_migration WHERE id >= 8')
));
";
        local_db.exec_safe(&stmt).into_db_result(local_db)?;
    }

    if current_version < 9 && target_version >= 9 {
        let stmt = "\
ALTER TABLE ps_buckets ADD COLUMN count_at_last INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ps_buckets ADD COLUMN count_since_last INTEGER NOT NULL DEFAULT 0;
INSERT INTO ps_migration(id, down_migrations) VALUES(9, json_array(
json_object('sql', 'ALTER TABLE ps_buckets DROP COLUMN count_at_last'),
json_object('sql', 'ALTER TABLE ps_buckets DROP COLUMN count_since_last'),
json_object('sql', 'DELETE FROM ps_migration WHERE id >= 9')
));
";

        local_db.exec_safe(stmt).into_db_result(local_db)?;
    }

    if current_version < 10 && target_version >= 10 {
        // We want to re-create views and triggers because their definition at version 10 and above
        // might reference vtabs that don't exist on older versions. These views will be re-created
        // by applying the PowerSync user schema after these internal migrations finish.
        local_db
            .exec_safe(
                "\
INSERT INTO ps_migration(id, down_migrations) VALUES (10, json_array(
  json_object('sql', 'SELECT powersync_drop_view(view.name)\n  FROM sqlite_master view\n  WHERE view.type = ''view''\n    AND view.sql GLOB  ''*-- powersync-auto-generated'''),
  json_object('sql', 'DELETE FROM ps_migration WHERE id >= 10')
));
        ",
            )
            .into_db_result(local_db)?;
    }

    if current_version < 11 && target_version >= 11 {
        let stmt = "\
CREATE TABLE ps_stream_subscriptions (
  id INTEGER NOT NULL PRIMARY KEY,
  stream_name TEXT NOT NULL,
  active INTEGER NOT NULL DEFAULT FALSE,
  is_default INTEGER NOT NULL DEFAULT FALSE,
  local_priority INTEGER,
  local_params TEXT NOT NULL DEFAULT 'null',
  ttl INTEGER,
  expires_at INTEGER,
  last_synced_at INTEGER,
  UNIQUE (stream_name, local_params)
) STRICT;

INSERT INTO ps_migration(id, down_migrations) VALUES(11, json_array(
json_object('sql', 'todo down migration'),
json_object('sql', 'DELETE FROM ps_migration WHERE id >= 11')
));
";
        local_db.exec_safe(stmt).into_db_result(local_db)?;
    }

    Ok(())
}
