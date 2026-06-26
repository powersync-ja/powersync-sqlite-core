## Sync interface

The core extension implements the state machine and necessary SQL handling to decode and apply
sync line sent from a PowerSync service instance.

After registering the PowerSync extension, this client is available through the `powersync_control`
function, which takes two arguments: A command (text), and a payload (text, blob, or null).
The function should always be called in a transaction.

The following commands are supported:

1. `start`: Payload is a JSON-encoded object. This requests the client to start a sync iteration.
   The payload can either be `null` or an JSON object with:
    - An optional `parameters: Record<string, any>` entry, specifying parameters to include in the request
      to the sync service.
    - A `schema: { tables: Table[], raw_tables: RawTable[] }` entry specifying the schema of the database to
      use. Regular tables are also inferred from the database itself, but raw tables need to be specified.
      If no raw tables are used, the `schema` entry can be omitted.
    - `active_streams`: An array of `{name: string, params: Record<string, any>}` entries representing streams that
      have an active subscription object in the application at the time the stream was opened.
2. `stop`: No payload, requests the current sync iteration (if any) to be shut down.
3. `line_text`: Payload is a serialized JSON object received from the sync service.
4. `line_binary`: Payload is a BSON-encoded object received from the sync service.
5. `refreshed_token`: Notify the sync client that the JWT used to authenticate to the PowerSync service has
   changed.
   - The client will emit an instruction to stop the current stream, clients should restart by sending another `start`
     command.
6. `completed_upload`: Notify the sync implementation that all local changes have been uploaded.
7. `update_subscriptions`: Notify the sync implementation that subscriptions which are currently active in the app
   have changed. Depending on the TTL of caches, this may cause it to request a reconnect.
8. `connection`: Notify the sync implementation about the connection being opened (second parameter should be `established`)
   or the HTTP stream closing (second parameter should be `end`).
   This is used to set `connected` to true in the sync status without waiting for the first sync line.
9. `subscriptions`: Store a new sync steam subscription in the database or remove it.
   This command can run outside of a sync iteration and does not affect it.
10. `update_subscriptions`: Second parameter is a JSON-encoded array of `{name: string, params: Record<string, any>}`.
    If a new subscription is created, or when a subscription without a TTL has been removed, the client will ask to
    restart the connection.

When uploads request a write checkpoint, SDKs should call
`powersync_next_checkpoint_request_id()` inside a transaction to allocate the id to pass to the
request-checkpoint API. In checkpoint-request mode, the SDK should first allocate the id, then post
that id to the service, and then call `powersync_probe_local_target_op(id)` with the same id once
the service accepts the request. This sets the local target op to the request op, replacing the
pending-write sentinel with the concrete checkpoint request id that the sync stream can satisfy.
`powersync_next_checkpoint_request_id()` only advances the request counter; it does not update the
local target op used to block applying downloaded rows.

`powersync_probe_local_target_op(op_id)` reads and optionally updates the internal local target op.
The same function is used for compatibility when a new SDK is used with an older PowerSync service
that does not yet support client-created checkpoint requests; after the service-side write
checkpoint request returns a concrete id, call `powersync_probe_local_target_op(id)` with that id.
Pass `NULL` to probe the current internal `$local` target op from `ps_kv` without updating it, or
pass an integer or integer string to update that target op. In both cases it returns the value from
before the call, or `NULL` if no value existed. Updating to a positive, non-sentinel target op also
stores it as `last_requested_checkpoint_request_id` to support migrating to client-created
checkpoint requests. Passing `0` clears the local target, and sentinel values such as max op id are
not stored as requested checkpoint ids.

Database migration v14 moves legacy `$local` checkpoint state into `ps_kv`: `$local.last_applied_op`
becomes `last_synced_checkpoint_request_id`, `$local.last_op` becomes the internal
`last_seen_checkpoint_request_id`, a concrete `$local.target_op` advances the request counter, and
`$local.target_op` is stored as `local_target_op`. Downgrading restores a `$local` row only when
`local_target_op` exists, so older SDKs can keep using target-op based blocking without inventing a
synthetic local bucket when there was no local target state.

If the migrated target op is not concrete, for example max op id while local writes are pending,
`last_requested_checkpoint_request_id` may be undefined. SDKs must detect that before calling
`powersync_next_checkpoint_request_id()`, since that function would allocate `1` or another value
lower than the legacy service-side counter. In that ambiguous state, create one old-style write
checkpoint first, store the returned concrete id with `powersync_probe_local_target_op(id)`, and
then switch to client-created checkpoint requests.

`powersync_control` returns a JSON-encoded array of instructions for the client:

```typescript
type Instruction = { LogLine: LogLine }
   | { UpdateSyncStatus: UpdateSyncStatus }
   | { EstablishSyncStream: EstablishSyncStream }
   | { FetchCredentials: FetchCredentials }
   // Close a connection previously started after EstablishSyncStream
   | { CloseSyncStream: { hide_disconnect: boolean } }
   // For the Dart web client, flush the (otherwise non-durable) file system.
   | { FlushFileSystem: {} }
   // Notify clients that a checkpoint was completed. Clients can clear the
   // download error state in response to this.
   | { DidCompleteSync: {} }

interface LogLine {
  severity: 'DEBUG' | 'INFO' | 'WARNING',
  line: String,
}

// Instructs client SDKs to open a connection to the sync service.
interface EstablishSyncStream {
  request: any // The JSON-encoded StreamingSyncRequest to send to the sync service
}

// Instructs SDKS to update the downloading state of their SyncStatus.
interface UpdateSyncStatus {
  connected: boolean,
  connecting: boolean,
  priority_status: [],
  downloading: null | DownloadProgress,
  streams: [],
  last_synced_checkpoint_request_id: null | number,
}

// Instructs SDKs to refresh credentials from the backend connector.
// They don't necessary have to close the connection, a CloseSyncStream instruction
// will be sent when the token has already expired.
interface FetchCredentials {
  // Set as an option in case fetching and prefetching should be handled differently.
  did_expire: boolean
}

interface SyncPriorityStatus {
  priority: int,
  last_synced_at: null | int,
  has_synced: null | boolean,
}

interface DownloadProgress {
  buckets: Record<string, BucketProgress>
}

interface BucketProgress {
  priority: int,
  at_last: int,
  since_last: int,
  target_count: int
}
```
