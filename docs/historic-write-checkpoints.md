# Write Checkpointing

The general flow for mutations is as follows.

A client makes a write to a table/view. Triggers are used to populate the `ps_crud` table with an entry for the operation. Every local write marks the `$local` bucket in `ps_buckets` as having a `target_op` of the maximum i64 value - this effectively blocks incoming synced checkpoints from being applied.

```sql
INSERT OR REPLACE INTO ps_buckets(name, last_op, target_op) VALUES('$local', 0, {MAX_OP_ID})
```

A connected client SDK monitors the `ps_crud` table - or the sync state machine triggers CRUD uploads when ready. The user's `uploadData` gets CRUD transactions with `getNextCrudTransaction` or some equivalent method. Calling the `complete` method on a CRUD transaction-like object will:

- Remove the entries from `ps_crud`
- Depending on the write checkpoint method used:
  - Optionally apply a custom_write_checkpoint as the target_op - ONLY if the `ps_crud` queue is empty
  - Ensure that the `target_op` is at the MAX_OP_ID

```TypeScript
      await tx.execute(`DELETE FROM ${PSInternalTable.CRUD} WHERE id <= ?`, [lastClientId]);
      if (writeCheckpoint) {
        const check = await tx.execute(`SELECT 1 FROM ${PSInternalTable.CRUD} LIMIT 1`);
        if (!check.rows?.length) {
          await tx.execute(`UPDATE ${PSInternalTable.BUCKETS} SET target_op = CAST(? as INTEGER) WHERE name='$local'`, [
            writeCheckpoint
          ]);
        }
      } else {
        await tx.execute(`UPDATE ${PSInternalTable.BUCKETS} SET target_op = CAST(? as INTEGER) WHERE name='$local'`, [
          this.bucketStorageAdapter.getMaxOpId()
        ]);
      }
```

Once all the uploads have completed, the Sync implementation will attempt to update the local target.

```typescript
// private async _uploadAllCrud(signal: AbortSignal): Promise<void> {

// AbstractStreamingSyncImplementation.ts line 418
// Uploading is completed
const neededUpdate = await this.options.adapter.updateLocalTarget(() => this.getWriteCheckpoint());

// ...
// }

// SqliteBucketStorage.ts line 67
// async updateLocalTarget(cb: () => Promise<string>): Promise<boolean> {
const rs1 = await this.db.getAll(
  "SELECT target_op FROM ps_buckets WHERE name = '$local' AND target_op = CAST(? as INTEGER)",
  [MAX_OP_ID]
);

// If the target op is not the MAX_OP_ID (it's a concrete checkpoint ID)
// Then: Don't fetch a new write checkpoint from the service, leave it as is.
// This essentially caters for the custom write checkpoint case where a concrete `writeCheckpoint`
// is set after all items have been uploaded.
// In the managed checkpoint flow, the target_op should be the MAX_OP_ID here.
if (!rs1.length) {
  // Nothing to update
  return false;
}

// The logic below tries to ensure that no uploads happened in-between async operations,
// like fetching a write-checkpoint from the PowerSync service
const rs = await this.db.getAll<{ seq: number }>("SELECT seq FROM main.sqlite_sequence WHERE name = 'ps_crud'");
if (!rs.length) {
  // Nothing to update
  return false;
}

const seqBefore: number = rs[0]['seq'];

// This callback usually connects to the PowerSync service write-checkpoint2.json endpoint
const opId = await cb();

// Now we apply the target_op, only if no other CRUD items have dirtied the local state meanwhile.
return this.writeTransaction(async (tx) => {
  const anyData = await tx.execute('SELECT 1 FROM ps_crud LIMIT 1');
  if (anyData.rows?.length) {
    // if isNotEmpty
    this.logger.debug(`New data uploaded since write checkpoint ${opId} - need new write checkpoint`);
    return false;
  }

  const rs = await tx.execute("SELECT seq FROM main.sqlite_sequence WHERE name = 'ps_crud'");
  if (!rs.rows?.length) {
    // assert isNotEmpty
    throw new Error('SQLite Sequence should not be empty');
  }

  const seqAfter: number = rs.rows?.item(0)['seq'];
  if (seqAfter != seqBefore) {
    this.logger.debug(
      `New data uploaded since write checkpoint ${opId} - need new write checkpoint (sequence updated)`
    );

    // New crud data may have been uploaded since we got the checkpoint. Abort.
    return false;
  }

  this.logger.debug(`Updating target write checkpoint to ${opId}`);
  await tx.execute("UPDATE ps_buckets SET target_op = CAST(? as INTEGER) WHERE name='$local'", [opId]);
  return true;
});
// }
```

Concurrently, the streaming sync implementation is reading checkpoints from the PowerSync service `/sync/stream/` endpoint. The PowerSync service reports which checkpoints are associated with a corresponding write checkpoint.

The client does not publish guarded changes as soon as it sees that value. After a full checkpoint has completed and its checksums have been validated, `sync_local` stores the checkpoint's `write_checkpoint` as `$local.last_op`. Incoming changes can then be applied locally only when `$local.target_op <= $local.last_op` and the `ps_crud` queue is empty. Partial priority 0 syncs are the exception: they may publish while uploads are outstanding.

```Rust
// sync_local.rs

fn can_apply_sync_changes(&self) -> Result<bool, PowerSyncError> {
        // Don't publish downloaded data until the upload queue is empty (except for downloaded data
        // in priority 0, which is published earlier).

        let needs_check = match &self.partial {
            Some(p) => !p.priority.may_publish_with_outstanding_uploads(),
            None => true,
        };

        if needs_check {
            // language=SQLite
            let statement = self.db.prepare_v2(
                "SELECT 1 FROM ps_buckets WHERE target_op > last_op AND name = '$local'",
            )?;

            if statement.step()? == ResultCode::ROW {
                return Ok(false);
            }

            let statement = self.db.prepare_v2("SELECT 1 FROM ps_crud LIMIT 1")?;
            if statement.step()? != ResultCode::DONE {
                return Ok(false);
            }
        }

        Ok(true)
    }

// storage_adapter.rs

pub fn sync_local(
  // ...
) {

// ...
        if let (None, Some(write_checkpoint)) = (&priority, &checkpoint.write_checkpoint) {
            update_bucket.bind_int64(1, *write_checkpoint)?;
            update_bucket.bind_text(2, "$local", sqlite::Destructor::STATIC)?;
            update_bucket.exec()?;
        }

// ...
}
```

## The $local bucket

`$local` is a special row in `ps_buckets` used to track whether downloaded changes are safe to
publish while local writes are being uploaded. It is stored in the same table as real sync buckets,
but it is not sent to the sync service as a bucket request.

- `target_op`: The write checkpoint that must be reached before guarded upstream changes may be
  published locally. Local writes set this to `MAX_OP_ID`; custom write checkpoints or managed
  write-checkpoint requests replace it with a concrete checkpoint id once the relevant upload has
  completed.

  ```sql
  -- Local writes block guarded publishes until a concrete write checkpoint is known.
  INSERT OR REPLACE INTO ps_buckets(name, last_op, target_op)
  VALUES('$local', 0, {MAX_OP_ID});

  -- After upload completion, custom or managed checkpointing stores the target checkpoint.
  UPDATE ps_buckets SET target_op = CAST(? AS INTEGER) WHERE name = '$local';
  ```

- `last_op`: The latest write checkpoint observed from the sync service. This is updated from
  `checkpoint.write_checkpoint` when a full checkpoint is validated.

  ```rust
  let update_bucket = self.db.prepare_v2("UPDATE ps_buckets SET last_op = ? WHERE name = ?")?;

  if let (None, Some(write_checkpoint)) = (&priority, &checkpoint.write_checkpoint) {
      update_bucket.bind_int64(1, *write_checkpoint)?;
      update_bucket.bind_text(2, "$local", sqlite::Destructor::STATIC)?;
      update_bucket.exec()?;
  }
  ```

- `last_applied_op`: The latest write checkpoint whose guarded changes have actually been published
  locally. This advances to `last_op` after a full `sync_local` apply succeeds.

  ```sql
  UPDATE ps_buckets
     SET last_applied_op = last_op
   WHERE last_applied_op != last_op;
  ```

The apply gate checks `$local.target_op > $local.last_op` before publishing full checkpoints and
non-priority-0 partial checkpoints. It also checks that `ps_crud` is empty. This means downloaded
changes remain buffered until the client has both uploaded local CRUD and seen the corresponding
write checkpoint in the sync stream.

Clearing the database removes `$local`: a hard clear deletes all rows from `ps_buckets`, while a
soft clear deletes only the `$local` row and keeps reusable remote bucket state.
