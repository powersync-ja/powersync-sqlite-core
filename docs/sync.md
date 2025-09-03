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
