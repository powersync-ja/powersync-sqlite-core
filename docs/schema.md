# Internal PowerSync tables

This document is intended as a reference when working on the core PowerSync extension itself.
For informtion relevant to PowerSync users, see [client-architecture](https://docs.powersync.com/architecture/client-architecture#schema).
The document is also incomplete at the moment.

## `ps_migration`

__TODO__: Document

## `ps_buckets`

`ps_buckets` stores information about [buckets](https://docs.powersync.com/architecture/powersync-protocol#buckets) relevant to clients.
A bucket is instantiated for every row returned by a parameter query in a [bucket definition](https://docs.powersync.com/usage/sync-rules/organize-data-into-buckets#organize-data-into-buckets).

Clients create entries in `ps_buckets` when receiving a checkpoint message from the sync service, they are also
responsible for removing buckets that are no longer relevant to the client.
Older schema versions also used a special `$local` bucket to represent pending uploads. Current
schema versions keep that local write gate in `ps_kv` instead.

We store the following information in `ps_buckets`:

1. `id`: Internal (client-side only), alias to rowid for foreign references.
2. `name`: The name of the bucket as received from the sync service.
3. `last_applied_op`: The last operation id that has been verified and published to views (meaning that it was part of
a checkpoint and that we have validated its checksum).
4. `add_checksum`: TODO: Document further.
5. `op_checksum`: TODO: Document further.
6. `pending_delete`: TODO: Appears to be unused, document further.
7. `count_at_last`: The amount of operations in the bucket at the last verified checkpoint.
8. `count_since_last`: The amount of operations downloaded since the last verified checkpoint.

Schema version 14 removes the legacy `target_op` column after migrating `$local.target_op` to
`ps_kv.local_target_op`. This makes older SDKs fail with a hard SQLite error if they try to keep
using the migrated database without downgrading. The down migration restores `target_op` for older
schema versions.

## `ps_crud`

__TODO__: Document

## `ps_kv`

__TODO__: Document

## `ps_oplog`

__TODO__: Document

## `ps_sync_state`

__TODO__: Document

## `ps_tx`

__TODO__: Document

## `ps_untyped`

__TODO__: Document

## `ps_updated_rows`

__TODO__: Document
