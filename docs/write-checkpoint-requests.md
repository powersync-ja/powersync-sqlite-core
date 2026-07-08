# Write Checkpoint State in `ps_kv`

This document describes the checkpoint state used to keep downloaded rows behind local writes until
the service has observed those writes, while also supporting explicit "wait until synced"
checkpoint requests.

The state is internal core/SDK bookkeeping. Apps should not read it as user-facing sync progress.

## State Model

| Key | Purpose | Who updates it |
| --- | --- | --- |
| `local_target_op` | Apply gate for local writes. Downloaded full checkpoints can apply only after the stream has seen this id. `MAX_OP_ID` means "local writes exist, but no concrete checkpoint id is known yet". | Core CRUD triggers set the sentinel; SDK upload code stores the accepted concrete id through `powersync_control('local_target_op', id)`. |
| `last_requested_checkpoint_request_id` | Allocation counter for client-created checkpoint requests. | SDKs seed it after connection reconciliation, then core increments it through `powersync_control('next_checkpoint_request_id', NULL)`. |
| `last_seen_checkpoint_request_id` | Latest full checkpoint `write_checkpoint` observed in the stream since the last local write. | Core updates it when a full checkpoint validates. Local writes clear it. |
| `last_applied_checkpoint_request_id` | Latest full checkpoint `write_checkpoint` applied locally since the last local write. | Core updates it after a full checkpoint applies. Local writes clear it. |

Why four keys?

- `local_target_op` is the gate that protects local writes.
- `last_requested_checkpoint_request_id` is the counter used to create new requests.
- `last_seen_checkpoint_request_id` answers "has the stream reached the gate yet?"
- `last_applied_checkpoint_request_id` is persisted for diagnostics. SDK waiters should use
  `DidCompleteSync.applied_checkpoint_request_id` instead.

## SDK Expectations

SDKs should use `powersync_control` as the public API for this state:

1. On each sync connection, read `EstablishSyncStream.last_checkpoint_request_id`.
2. Reconcile that local hint with service-side checkpoint-request state before allocating new ids.
3. Seed core with the reconciled positive id by calling
   `powersync_control('seed_checkpoint_request_id', id)`.
4. Wait for seeding to complete before creating checkpoint requests.
5. When upload code needs a write checkpoint, allocate an id inside a transaction with
   `powersync_control('next_checkpoint_request_id', NULL)`.
6. Post that id to the service checkpoint-request endpoint.
7. After the service accepts it, store it as the local write gate with
   `powersync_control('local_target_op', id)`.
8. Resolve explicit waiters from `DidCompleteSync.applied_checkpoint_request_id`, not from `ps_kv`.

`seed_checkpoint_request_id` stores the id verbatim and does not enforce monotonicity. SDKs own
reconciliation and must not seed stale service state. The recommended reconciliation pattern is:

```text
effectiveId = max(localHint ?? 0, concreteLocalTarget ?? 0, 1)
acceptedId = postCheckpointRequestStateToService(effectiveId)
powersync_control('seed_checkpoint_request_id', acceptedId)
```

Posting at least `1` covers the no-record case and probes whether the service supports checkpoint
requests. Core rejects `NULL` and `0` seeds.

The service returns the maximum of the client-provided id and its service-side record. If the client
lost local state, for example after `disconnectAndClear`, this response hydrates core's counter. If
the service lost its record, posting the local hint recreates the service-side state.

`powersync_control('next_checkpoint_request_id', NULL)` requires an active sync iteration and a
seeded request counter. SDKs should wait for the connection reconciliation and seed step before
creating checkpoint requests.

## Local Write Gate

A local write records CRUD and sets:

```sql
ps_kv['local_target_op'] = MAX_OP_ID
```

It also clears `last_seen_checkpoint_request_id` and `last_applied_checkpoint_request_id`, because
checkpoint ids observed before the write cannot acknowledge it.

After uploaded CRUD is accepted by the backend, SDK code replaces `MAX_OP_ID` with a concrete
checkpoint id:

```text
transaction {
    requestId = powersync_control('next_checkpoint_request_id', NULL)
}

POST /sync/checkpoint-request {
    client_id,
    checkpoint_request_id: requestId
}

transaction {
    previousTarget = powersync_control('local_target_op', NULL)
    if previousTarget == MAX_OP_ID && ps_crud is still empty {
        powersync_control('local_target_op', requestId)
    }
}
```

`local_target_op` is intentionally separate from `last_requested_checkpoint_request_id`: allocating
a checkpoint request id does not mean it should block or unblock local writes.

## Applying Downloaded Checkpoints

The service reports accepted checkpoint request ids as `checkpoint.write_checkpoint`. For full
checkpoints, core stores that value as `last_seen_checkpoint_request_id`.
The gate uses "seen" rather than "applied" because this check runs before the current checkpoint
can be applied.

Full checkpoints and non-priority-0 partial checkpoints can publish only when:

- `ps_crud` is empty, and
- `local_target_op` is absent or less than or equal to `last_seen_checkpoint_request_id`.

Priority 0 partial syncs may publish while uploads are outstanding.

If a full checkpoint validates but cannot apply because local CRUD is pending, core keeps it as a
pending checkpoint. When the SDK later sends `completed_upload`, core retries it unless the
pending checkpoint is older than the current `local_target_op`.

After a full checkpoint applies, core emits:

```text
DidCompleteSync { applied_checkpoint_request_id?: checkpoint.write_checkpoint }
```

SDKs should resolve `requestCheckpoint()` / `waitForSync()` waiters when this value is greater than
or equal to the requested id.

## Control Commands

`powersync_control('seed_checkpoint_request_id', id)`

- Payload: positive integer or integer string.
- Stores the reconciled checkpoint-request counter seed.
- Must be called after connection reconciliation and before allocating new ids.

`powersync_control('next_checkpoint_request_id', NULL)`

- Returns: next checkpoint request id as a SQLite integer.
- Requires an active sync iteration and a seeded request counter.
- SDKs should call this only after the connection reconciliation and seed step has completed.
- Participates in the caller's transaction. If the transaction rolls back after the id was posted
  to the service, retrying posts the same id again, which is safe because the service treats the
  latest posted id as effective state.

`powersync_control('local_target_op', value)`

- Payload `NULL`: return current target without changing it.
- Payload `0`: clear the target.
- Positive payload: store a concrete target.
- Returns the previous target as a SQLite integer, or SQL `NULL` if absent.
