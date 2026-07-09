## Sync interface

The core extension implements the state machine and necessary SQL handling to decode and apply
sync line sent from a PowerSync service instance.

After registering the PowerSync extension, this client is available through the `powersync_control`
function, which takes two arguments: A command (text), and a payload (text, blob, or null).
The function should always be called in a transaction.

The following commands are supported:

1. `start`: Payload is a JSON-encoded object. This requests the client to start a sync iteration.
   The payload can either be `null` or a JSON object with:
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
7. `update_subscriptions`: Payload is a JSON-encoded array of
   `{name: string, params: Record<string, any>}`. Notify the sync implementation that subscriptions
   which are currently active in the app have changed. Depending on the TTL of caches, this may
   cause it to request a reconnect.
8. `connection`: Notify the sync implementation about the connection being opened (second parameter should be `established`)
   or the HTTP stream closing (second parameter should be `end`).
   This is used to set `connected` to true in the sync status without waiting for the first sync line.
9. `subscriptions`: Store a new sync stream subscription in the database or remove it.
   This command can run outside of a sync iteration and does not affect it.
10. `next_checkpoint_request_id`: No payload. During an active sync iteration after checkpoint
    request state exists locally, allocates and returns the next checkpoint request id as an
    integer result.
11. `current_checkpoint_request_id`: No payload. Returns the current checkpoint request sequence
    value as an integer result, or SQL `NULL` if absent. This command does not allocate a new id and
    can run outside a sync iteration.
12. `local_target_op`: Payload is `null`, an integer, or an integer string. Probes, updates or
    clears the local target op and returns the previously-observed value as an integer result, or
    SQL `NULL` if there was no target. This command can run outside of a sync iteration and does not
    affect it.
13. `seed_checkpoint_request_id`: Payload is a positive integer or integer string. After receiving
    `EstablishSyncStream`, SDKs should reconcile the local hint with service-side
    checkpoint-request state, then seed core with the accepted positive id.

## Checkpoint Request Expectations

Checkpoint request state exists to protect local writes and to support explicit "wait until synced"
requests. The detailed state model lives in `write-checkpoint-requests.md`; this section summarizes
what SDKs need to do.

- On every connection, reconcile `EstablishSyncStream.last_checkpoint_request_id` with the service.
  Post at least `1` when there is no known id, then call
  `powersync_control('seed_checkpoint_request_id', acceptedId)`.
  The service returns the maximum of client and service-side state, so this hydrates a client that
  lost its local value and recreates service-side state when the service lost its record.
- Wait for seeding to complete before creating checkpoint requests. For an upload write checkpoint,
  call `powersync_control('next_checkpoint_request_id', NULL)` in a transaction, post the returned
  id to the service, then store the accepted id with `powersync_control('local_target_op', id)`.
- `local_target_op` is the apply gate for local writes. `next_checkpoint_request_id` only allocates
  ids; it does not update that gate.
- To retry a checkpoint request without incrementing the counter, read
  `powersync_control('current_checkpoint_request_id', NULL)` and repost that id when the SDK's
  runtime last-applied checkpoint request id is absent or lower.
- Resolve explicit checkpoint waiters from `DidCompleteSync.applied_checkpoint_request_id`. SDKs
  that drive waiters from status snapshots can also watch
  `UpdateSyncStatus.status.internal_last_applied_checkpoint_request_id`. Treat that status field as
  runtime-only SDK state, not persisted checkpoint state or app-visible progress.

Most `powersync_control` commands return a JSON-encoded array of instructions for the client.
`next_checkpoint_request_id`, `current_checkpoint_request_id` and `local_target_op` return values
directly.

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
   // download error state in response to this. If a full checkpoint with a
   // write_checkpoint was applied, applied_checkpoint_request_id is set.
   | { DidCompleteSync: DidCompleteSync }

interface LogLine {
  severity: 'DEBUG' | 'INFO' | 'WARNING',
  line: String,
}

// Instructs client SDKs to open a connection to the sync service.
// last_checkpoint_request_id is the client's current counter state before this stream request.
// On every connect, SDKs use it to re-affirm checkpoint request state with the service (which may
// have deleted its record). The re-affirmation is bidirectional: the hint can restore the
// service-side value, or the service's response can bump the local counter via
// powersync_control('seed_checkpoint_request_id', response).
interface EstablishSyncStream {
  request: any // The JSON-encoded StreamingSyncRequest to send to the sync service
  last_checkpoint_request_id: null | number
}

// Instructs SDKS to update the downloading state of their SyncStatus.
interface UpdateSyncStatus {
  connected: boolean,
  connecting: boolean,
  priority_status: [],
  downloading: null | DownloadProgress,
  streams: [],
  internal_last_applied_checkpoint_request_id?: number,
}

interface DidCompleteSync {
  applied_checkpoint_request_id?: number,
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
