# Write Checkpoint State in `ps_kv`

The new write checkpoint logic moves the historic `$local` bucket bookkeeping into `ps_kv`.
`ps_buckets` now tracks real sync buckets, while local upload gating and checkpoint-request
progress are represented as key/value state.

At a high level:

- `local_target_op` replaces `$local.target_op` as the local write apply gate.
- `last_seen_checkpoint_request_id` replaces `$local.last_op`.
- `last_applied_checkpoint_request_id` replaces `$local.last_applied_op`.
- `last_requested_checkpoint_request_id` tracks the latest concrete checkpoint request id allocated
  by the client, so `next_checkpoint_request_id` can allocate increasing ids for each checkpoint
  request.

These keys are internal SDK/core state, not user-facing sync progress.
`last_requested_checkpoint_request_id` is functional allocation state, while
`last_seen_checkpoint_request_id` and `last_applied_checkpoint_request_id` are mostly diagnostic
high-water marks. Explicit checkpoint waits should follow
`DidCompleteSync.applied_checkpoint_request_id`.

SDKs should not write these keys directly. They update the local target through
`powersync_control('local_target_op', value)`, which is the shared helper for both legacy write
checkpoints and new client-created checkpoint requests. The
`powersync_control('next_checkpoint_request_id', NULL)` command only allocates a checkpoint request
id; after the service accepts that request, the SDK uses
`powersync_control('local_target_op', id)` to make the accepted id the local target for write
checkpoints.

For the historic `$local` bucket flow, see `historic-write-checkpoints.md`.

## Local writes

A client write to a synced table/view records an entry in `ps_crud`. For simple CRUD triggers, the
same transaction also records the affected row in `ps_updated_rows` and sets `local_target_op` to
the maximum i64 value. This is the `ps_kv` equivalent of the old `$local.target_op` sentinel: it
means "there are local writes, but we do not yet know the concrete checkpoint id that will
acknowledge them".

The sentinel is stored in `ps_kv`, not in `ps_buckets`. The same statement clears the
`last_seen_checkpoint_request_id` and `last_applied_checkpoint_request_id` high-water marks:

```sql
INSERT OR REPLACE INTO ps_kv(key, value)
VALUES('local_target_op', MAX_OP_ID);

DELETE FROM ps_kv
WHERE key IN ('last_seen_checkpoint_request_id', 'last_applied_checkpoint_request_id');
```

Clearing the high-water marks mirrors how the legacy flow reset the whole `$local` row on local
writes. A checkpoint request id observed before the write cannot acknowledge it, and a stale seen
value may even predate a request counter restart. If such a value stayed around, a newly allocated
target id could compare below it and open the apply gate before the service acknowledged the write.
After a local write, only checkpoint request ids observed from that point on count towards the gate.

## Completing uploaded CRUD

SDK upload code removes uploaded items from `ps_crud`. If the connector supplies a legacy custom
write checkpoint and the queue is empty, that concrete checkpoint becomes the local target
immediately.
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
before opening the stream, or `NULL` when no local seed exists. On every connection attempt, SDKs
should reconcile this hint with the service checkpoint-request state before creating new requests.
The reconciliation is bidirectional: if the service still has a higher value, the SDK uses that
response to bump core locally so following requests are accepted; if the service has cleared stale
state but core still has a local hint, the SDK can use the hint to restore the service-side value.
After reconciliation, call `powersync_control('seed_checkpoint_request_id', value)` with the
reconciled value.

`last_requested_checkpoint_request_id` is a best-effort counter seed, not durable application state.
Core stores whatever value is seeded, without enforcing monotonicity: the SDK owns the
reconciliation and is expected to seed the effective state accepted by the service. If either the
client or the service still remembers a higher id, the SDK's reconciliation keeps the local counter
moving forward. If the service has cleared stale state but the client still has a local seed, the
local hint can restore the service-side value and keep the counter from moving backwards. If the
client lost local state but the service still has a record, the service response restores the seed
locally. If both sides have lost the value, it is acceptable for the counter to restart; this can
happen after local state is cleared and stale service state expires, or when multiple user ids share
the same client id. SDKs may also refresh service state when their user/client context changes.

Because seeds are stored verbatim, seeding a low id while core holds a higher counter resets that
counter, and previously allocated ids would be handed out again. SDKs must therefore never forward
a raw service response without reconciling: the recommended pattern is to always post an id of at
least `1` (the maximum of the local hint and any concrete local target) during reconciliation and
seed core with the service's response to that request. When there is no local record, posting a
checkpoint request with id `1` works and doubles as a probe of the service's checkpoint-request
support. Core rejects `NULL` seeds.

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

Calling it without an active iteration or before seeding raises a state error. This is a normal
part of the connection lifecycle (for example a `requestCheckpoint` call racing a stream restart),
not a programming error — SDKs should surface it as a retryable condition.

The increment participates in the caller's transaction. If the transaction rolls back after the id
was already posted to the service, the same id is allocated and posted again on retry; this is safe
because the service treats the latest posted id as the effective request state.

Note on sequences: SQLite does not have standalone sequences. The sequence-like alternatives are
either an `AUTOINCREMENT` table backed by SQLite's internal `sqlite_sequence`, or a dedicated
single-row counter table like the existing `ps_tx` transaction counter. The checkpoint request
counter currently lives in `ps_kv` so it can persist across requests and be reconciled with service
state on connect. If we want stricter structure later, a dedicated checkpoint-request counter table
would be the closest match to a sequence.

`powersync_control('local_target_op', op_id)` probes and optionally updates the local target. Like
`subscriptions`, this command is handled directly by `powersync_control` and can run outside an
active sync iteration:

- `NULL` returns the current `local_target_op` without changing it.
- `0` clears `local_target_op`.
- A positive value stores `local_target_op`.
- Negative values and non-integer inputs are rejected.

This command only updates the apply gate. It does not allocate, seed, or overwrite
`last_requested_checkpoint_request_id`.

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
`last_applied_checkpoint_request_id` and emits it on the `DidCompleteSync` instruction.

```text
after full checkpoint apply:
    ps_kv['last_applied_checkpoint_request_id'] = checkpoint.write_checkpoint
    emit DidCompleteSync { applied_checkpoint_request_id: checkpoint.write_checkpoint }
```

## Explicit checkpoint requests

SDKs can expose a `requestCheckpoint()`-style API for callers that want to wait until the local
database has caught up to the service. The SDK creates a checkpoint request id through the connected
sync client and returns a `CheckpointRequest`-style waiter.

This explicit API does not update `local_target_op`: it is a wait marker, not a local upload gate.
The returned object waits until core emits `DidCompleteSync` with
`applied_checkpoint_request_id` greater than or equal to the requested id.

```text
waitForSync() {
    for instruction in syncInstructions {
        return when instruction.DidCompleteSync.applied_checkpoint_request_id >= requestId
        throw if sync status reports a sync error
    }
}
```

The public database method requires an active or connecting sync client, because a disconnected
request could not be delivered to the service or observed in the sync stream.

Waiters do not need durable applied state across reconnects or app restarts. The connect-time
counter reconciliation doubles as a re-request: the SDK posts the effective checkpoint request id
to the service on every connection attempt, so the next checkpoint carries a `write_checkpoint`
greater than or equal to any previously requested id and core emits a fresh
`DidCompleteSync.applied_checkpoint_request_id` that resolves outstanding waits.

## `ps_kv` checkpoint state

- `local_target_op`: The current apply gate. It is either `MAX_OP_ID` while local writes are
  pending, a concrete checkpoint request id after upload completion, or absent when there is no
  local write gate.
- `last_requested_checkpoint_request_id`: The last client-created checkpoint request id allocated
  by `powersync_control('next_checkpoint_request_id', NULL)`. This is the counter used to allocate
  increasing ids for each client-created checkpoint request, including multiple requests in one
  client lifetime. The persisted value is also useful for debugging and for seeding the next
  connection attempt. SDKs should reconcile it with the service on every connect, and should
  tolerate it restarting when both the client and service have lost the previous value.
- `last_seen_checkpoint_request_id`: The latest full checkpoint `write_checkpoint` observed and
  validated from the sync stream since the last local write. Local writes clear this key, so only
  checkpoint request ids observed after the write can satisfy the apply gate.
- `last_applied_checkpoint_request_id`: The latest full checkpoint `write_checkpoint` that has been
  applied locally since the last local write, which clears this key. Core persists this for
  migration/downgrade state and debugging; SDKs should use
  `DidCompleteSync.applied_checkpoint_request_id` to resolve `CheckpointRequest` waits.

`powersync_clear` deletes all of these keys in both clear modes (it removes every `ps_kv` entry
except `client_id`). This is deliberate and mirrors the legacy behavior of deleting the `$local`
row: pending CRUD is wiped in the same operation, so no apply gate is needed, and the request
counter is restored by the connect-time reconciliation described above. If the service has also
lost its record, the counter restarting is acceptable.

## Migration from `$local`

Migration v14 moves the old `$local` bucket state into `ps_kv`:

- `$local.last_applied_op` becomes `last_applied_checkpoint_request_id`.
- `$local.last_op` becomes `last_seen_checkpoint_request_id`.
- Any positive `$local.target_op`, including `MAX_OP_ID`, becomes `local_target_op`.

A concrete `$local.target_op` could be used to seed `last_requested_checkpoint_request_id`, but it
should be redundant because SDKs reconcile the request counter with service state on connect before
advancing it through `next_checkpoint_request_id`.

After copying this state, the migration deletes the `$local` row — version 14 tracks this state
exclusively in `ps_kv`, so `ps_buckets` only contains real sync buckets — and drops
`ps_buckets.target_op`. Dropping the column intentionally makes older SDKs fail with a hard SQLite
error if they try to keep using a migrated database without first downgrading.

The up migration first deletes any existing `last_applied_checkpoint_request_id`,
`last_seen_checkpoint_request_id` and `local_target_op` keys. Those can be present when a database
was previously on version 14 and then downgraded, because the down migration keeps the ps_kv keys
while rebuilding `$local`. Clearing them makes the `$local` row the source of truth on re-upgrade,
picking up any progress an older SDK made while downgraded. `last_requested_checkpoint_request_id`
is unrelated to `$local` and survives a downgrade/upgrade cycle unchanged.

An absent `local_target_op` is safe: there is no local write gate waiting for a checkpoint, so an
SDK can seed the request counter on connect and start client-created checkpoint requests normally.
If neither the client nor service has a previous request id, the first allocated id is `1`. The sync
stream will only report that request id after the service has accepted and reached it.

The ambiguous case is a migrated `local_target_op` of `MAX_OP_ID`. That means there is a pending
local write gate but no concrete request id to wait for yet. The `MAX_OP_ID` sentinel only says that
local writes dirtied the gate; it does not prove that no earlier uploads were already associated
with legacy service-created write checkpoints. In that state, the SDK should create one legacy write
checkpoint first, store the concrete id with `powersync_control('local_target_op', id)`, let that
gate resolve, and then switch to client-created checkpoint requests after the request counter has
been reconciled on connect.

The down migration restores `ps_buckets.target_op` and rebuilds a `$local` row only when
`local_target_op` exists, using:

- `last_seen_checkpoint_request_id` as `$local.last_op`
- `last_applied_checkpoint_request_id` as `$local.last_applied_op`
- `local_target_op` as `$local.target_op`

This keeps older SDKs able to use the historic target-op gate after a downgrade without inventing a
synthetic `$local` bucket when there was no local target state.

Two properties make the restored gate safe rather than a potential stall:

- **Shared id namespace.** Client-created checkpoint request ids and legacy write checkpoint ids
  are one namespace, compatible in both directions — including values migrated from the historic
  service-generated write checkpoint scheme. The service reports the checkpoint request ids it
  accepted as the `write_checkpoint` values older-protocol clients observe, so a restored concrete
  `$local.target_op` is satisfiable by the next write checkpoint the downgraded SDK sees; it does
  not wait on an incomparable id sequence.
- **Downgrade fidelity.** `last_seen_checkpoint_request_id` and
  `last_applied_checkpoint_request_id` only advance on full checkpoint completions — but so did the
  legacy `$local.last_op`/`last_applied_op` bookkeeping (partial priority applies never updated
  `$local`, which is not a real bucket). The rebuilt `$local` row therefore matches exactly what a
  legacy client would have recorded at the same point in the stream; the down migration cannot lag
  behind legacy behavior.
