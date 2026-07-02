# Write Checkpoint State in `ps_kv`

The new write checkpoint logic moves the historic `$local` bucket bookkeeping into `ps_kv`.
`ps_buckets` now tracks real sync buckets, while local upload gating and checkpoint-request
progress are represented as key/value state.

At a high level:

- `local_target_op` replaces `$local.target_op` as the local write apply gate.
- `last_seen_checkpoint_request_id` replaces `$local.last_op`.
- `last_applied_checkpoint_request_id` replaces `$local.last_applied_op`.
- `last_requested_checkpoint_request_id` tracks the latest concrete checkpoint request id known to
  the client.

SDKs should not write these keys directly. They update the local target through
`powersync_control('local_target_op', value)`, which is the shared helper for both legacy write
checkpoints and new client-created checkpoint requests. The
`powersync_control('next_checkpoint_request_id', NULL)` command only allocates a checkpoint request
id; after the service accepts that request, the SDK uses
`powersync_control('local_target_op', id)` to make the accepted id the local target.

For the historic `$local` bucket flow, see `historic-write-checkpoints.md`.

## Local writes

A client write to a synced table/view records an entry in `ps_crud`. For simple CRUD triggers, the
same transaction also records the affected row in `ps_updated_rows` and sets `local_target_op` to
the maximum i64 value. This is the `ps_kv` equivalent of the old `$local.target_op` sentinel: it
means "there are local writes, but we do not yet know the concrete checkpoint id that will
acknowledge them".

The sentinel is stored in `ps_kv`, not in `ps_buckets`:

```sql
INSERT OR REPLACE INTO ps_kv(key, value)
VALUES('local_target_op', MAX_OP_ID);
```

## Completing uploaded CRUD

SDK upload code removes uploaded items from `ps_crud`. If the connector supplies a custom write
checkpoint and the queue is empty, that concrete checkpoint becomes the local target immediately.
Otherwise the target is reset to `MAX_OP_ID`, allowing the sync client to create a standard
checkpoint request after the queue drains.

```text
transaction {
    deleteUploadedCrud(upTo: lastUploadedId)

    if let customCheckpoint, crudQueueIsEmpty {
        powersync_control('local_target_op', customCheckpoint)
    } else {
        powersync_control('local_target_op', MAX_OP_ID)
    }
}
```

## Updating the local target

Once uploads are complete, the sync client updates the local target through
`powersync_control('local_target_op', value)`. It only does this when the current target is still
`MAX_OP_ID`, which avoids overwriting a custom checkpoint that was already stored by
`complete(writeCheckpoint:)`.

The SDK implementation:

1. Probes the current target with `powersync_control('local_target_op', NULL)`.
2. Reads `sqlite_sequence.seq` for `ps_crud`.
3. Gets a concrete checkpoint id from either the new or legacy service API.
4. Re-enters a write transaction.
5. Verifies that `ps_crud` is still empty and that its sequence did not change.
6. Stores the concrete target with `powersync_control('local_target_op', opId)`.

```text
let previousTarget = transaction {
    powersync_control('local_target_op', NULL).LocalTargetOp.target_op
}

if previousTarget == MAX_OP_ID {
    let seqBefore = psCrudSequence()
    let checkpointId = await createOrFetchCheckpointId()

    transaction {
        guard ps_crud.isEmpty && psCrudSequence() == seqBefore else {
            return
        }

        powersync_control('local_target_op', checkpointId)
    }
}
```

In checkpoint-request mode, `getWriteCheckpoint()` calls `requestCheckpoint()`. That allocates an
id locally, sends it to `/sync/checkpoint-request`, and returns the same id once the service accepts
the request. Only then does the upload path store that id as `local_target_op` with
`powersync_control('local_target_op', id)`.

```text
let requestId = transaction {
    powersync_control('next_checkpoint_request_id', NULL).CheckpointRequestId.request_id
}

POST /sync/checkpoint-request {
    client_id,
    checkpoint_request_id: requestId
}

return requestId
```

The legacy fallback still calls `/write-checkpoint2.json`; the returned write checkpoint is stored
through the same `powersync_control('local_target_op', opId)` helper. This keeps SDK target updates
consistent across both protocols.

## Sync control commands

These `powersync_control` commands are the SDK-facing API for the new `ps_kv` checkpoint state.

`powersync_control('start', payload)` begins a sync iteration and emits `EstablishSyncStream` with a
`last_checkpoint_request_id` hint. This is core's local `last_requested_checkpoint_request_id` value
before opening the stream, or `NULL` when no local seed exists. On connect, SDKs can use this hint
to re-request the client's last checkpoint request state from the service, then call
`powersync_control('seed_checkpoint_request_id', value)` with the actual response for
reconciliation. `value` may be `NULL` when the service has no record for the client; core stores `0`
only when no local seed exists. Integer seeds use `max(local, service)` semantics so the local
counter never moves backwards. SDKs may also refresh service state when their user/client context
changes.

`powersync_control('next_checkpoint_request_id', NULL)` must be called inside a transaction during
an active sync iteration after `last_requested_checkpoint_request_id` exists locally. It increments
and returns `last_requested_checkpoint_request_id` in a `CheckpointRequestId` instruction.

```sql
INSERT INTO ps_kv(key, value)
VALUES('last_requested_checkpoint_request_id', 1)
ON CONFLICT(key) DO UPDATE SET value = CAST(value AS INTEGER) + 1
RETURNING value;
```

This command only allocates an id. It does not update `local_target_op`.

Note on sequences: SQLite does not have standalone sequences. The sequence-like alternatives are
either an `AUTOINCREMENT` table backed by SQLite's internal `sqlite_sequence`, or a dedicated
single-row counter table like the existing `ps_tx` transaction counter. The checkpoint request
counter currently lives in `ps_kv` because it is also migrated and seeded from legacy/custom
concrete targets via `powersync_control('local_target_op', value)`. If we want stricter structure
later, a dedicated checkpoint-request counter table would be the closest match to a sequence.

`powersync_control('local_target_op', op_id)` probes and optionally updates the local target. Like
`subscriptions`, this command is handled directly by `powersync_control` and can run outside an
active sync iteration:

- `NULL` returns the current `local_target_op` without changing it.
- `0` clears `local_target_op`.
- A positive value stores `local_target_op`.
- A positive value other than `i64::MAX` also stores `last_requested_checkpoint_request_id`.
- Negative values and non-integer inputs are rejected.

The command returns the previous target value in a `LocalTargetOp` result, or `NULL` if there was no
target.

```text
previous = ps_kv['local_target_op']

if target_op == NULL:
    return previous
if target_op == 0:
    delete ps_kv['local_target_op']
else:
    ps_kv['local_target_op'] = target_op
    if target_op != MAX_OP_ID:
        ps_kv['last_requested_checkpoint_request_id'] = target_op

return previous
```

## Applying downloaded checkpoints

The sync stream reports the checkpoint request id in `checkpoint.write_checkpoint`. After a full
checkpoint validates, core persists it as `last_seen_checkpoint_request_id`.

```text
on full checkpoint with write_checkpoint:
    ps_kv['last_seen_checkpoint_request_id'] = checkpoint.write_checkpoint
```

Before publishing downloaded rows, `sync_local` checks the local gate. Full checkpoints and
non-priority-0 partial checkpoints can only apply when:

- `local_target_op` is absent, or it is less than or equal to `last_seen_checkpoint_request_id`.
- `ps_crud` is empty.

Priority 0 partial syncs are the exception: they may publish while uploads are outstanding.

```sql
SELECT 1
FROM ps_kv AS target
LEFT JOIN ps_kv AS seen ON seen.key = 'last_seen_checkpoint_request_id'
WHERE target.key = 'local_target_op'
  AND CAST(target.value AS INTEGER) > COALESCE(CAST(seen.value AS INTEGER), 0);
```

If a full checkpoint validated but cannot apply because local CRUD is pending, the state machine
keeps it as `validated_but_not_applied`. When the SDK later sends `completed_upload`, core retries
that checkpoint unless its `write_checkpoint` is older than the current `local_target_op`.

```text
on completed_upload:
    if pending_checkpoint.write_checkpoint >= local_target_op:
        retry applying pending_checkpoint
```

After a full checkpoint applies, core stores the applied checkpoint request id as
`last_applied_checkpoint_request_id` and emits it in sync status.

```text
after full checkpoint apply:
    ps_kv['last_applied_checkpoint_request_id'] = checkpoint.write_checkpoint
```

## Explicit checkpoint requests

Swift exposes `PowerSyncDatabaseProtocol.requestCheckpoint()` for callers that want to wait until
the local database has caught up to the service. This creates a checkpoint request id through the
connected sync client and returns a `CheckpointRequest`.

This explicit API does not update `local_target_op`: it is a wait marker, not a local upload gate.
The returned object waits until sync status reports `last_applied_checkpoint_request_id >= requestId`.

```text
isSynced = status.lastAppliedCheckpointRequestId >= requestId

waitForSync() {
    for status in syncStatusUpdates {
        return when status.lastAppliedCheckpointRequestId >= requestId
        throw if status reports a sync error
    }
}
```

The public database method requires an active or connecting sync client, because a disconnected
request could not be delivered to the service or observed in the sync stream.

## `ps_kv` checkpoint state

- `local_target_op`: The current apply gate. It is either `MAX_OP_ID` while local writes are
  pending, a concrete checkpoint request id after upload completion, or absent when there is no
  local write gate.
- `last_requested_checkpoint_request_id`: The last client-created checkpoint request id allocated
  by `powersync_control('next_checkpoint_request_id', NULL)`.
  `powersync_control('local_target_op', value)` also writes this key for positive, non-sentinel
  targets so migrated or legacy-created concrete targets can seed the client request counter.
- `last_seen_checkpoint_request_id`: The latest full checkpoint `write_checkpoint` observed and
  validated from the sync stream.
- `last_applied_checkpoint_request_id`: The latest full checkpoint `write_checkpoint` that has been
  applied locally. SDKs expose this in sync status and use it to resolve `CheckpointRequest` waits.

## Migration from `$local`

Migration v14 moves the old `$local` bucket state into `ps_kv`:

- `$local.last_applied_op` becomes `last_applied_checkpoint_request_id`.
- `$local.last_op` becomes `last_seen_checkpoint_request_id`.
- A concrete `$local.target_op` becomes `last_requested_checkpoint_request_id`.
- Any positive `$local.target_op`, including `MAX_OP_ID`, becomes `local_target_op`.

An absent `local_target_op` is safe: there is no local write gate waiting for a checkpoint, so an
SDK can start client-created checkpoint requests from `1`. The sync stream will only report that
request id after the service has accepted and reached it.

The ambiguous case is a migrated `local_target_op` of `MAX_OP_ID` with no
`last_requested_checkpoint_request_id`. That means there is a pending local write gate but no
concrete request id to wait for yet. The `MAX_OP_ID` sentinel only says that local writes dirtied
the gate; it does not prove that no earlier uploads were already associated with legacy
service-created write checkpoints. Those existing checkpoint ids may be higher than a restarted
client counter such as `1`, and using a lower target could let an older seen checkpoint satisfy the
gate too early. In that state, the SDK should create one legacy write checkpoint first, store the
concrete id with `powersync_control('local_target_op', id)`, and then switch to client-created
checkpoint requests.

The down migration rebuilds a `$local` row only when `local_target_op` exists, using:

- `last_seen_checkpoint_request_id` as `$local.last_op`
- `last_applied_checkpoint_request_id` as `$local.last_applied_op`
- `local_target_op` as `$local.target_op`

This keeps older SDKs able to use the historic target-op gate after a downgrade without inventing a
synthetic `$local` bucket when there was no local target state.
