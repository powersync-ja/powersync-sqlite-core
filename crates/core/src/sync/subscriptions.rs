use core::{cmp::Ordering, hash::Hash, time::Duration};

use alloc::{boxed::Box, string::String};
use serde::Deserialize;
use serde_with::{serde_as, DurationSeconds};
use sqlite_nostd::{self as sqlite, Connection};

use crate::{
    error::{PSResult, PowerSyncError},
    ext::SafeManagedStmt,
    sync::BucketPriority,
    util::JsonString,
};

/// A key that uniquely identifies a stream subscription.
#[derive(Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct SubscriptionKey {
    pub stream_name: String,
    pub params: Option<Box<JsonString>>,
}

/// A row in the `ps_stream_subscriptions` table.
pub struct LocallyTrackedSubscription {
    pub id: i64,
    pub stream_name: String,
    pub active: bool,
    pub is_default: bool,
    pub local_priority: Option<BucketPriority>,
    pub local_params: Option<Box<JsonString>>,
    pub ttl: Option<i64>,
    pub expires_at: Option<i64>,
    pub last_synced_at: Option<i64>,
}

impl LocallyTrackedSubscription {
    /// The default TTL of non-default subscriptions if none is set: One day.
    pub const DEFAULT_TTL: i64 = 60 * 60 * 24;

    pub fn key(&self) -> SubscriptionKey {
        SubscriptionKey {
            stream_name: self.stream_name.clone(),
            params: self.local_params.clone(),
        }
    }

    pub fn has_subscribed_manually(&self) -> bool {
        self.ttl.is_some()
    }
}

/// A request sent from a PowerSync SDK to alter the subscriptions managed by this client.
#[derive(Deserialize)]
pub enum SubscriptionChangeRequest {
    #[serde(rename = "subscribe")]
    Subscribe(SubscribeToStream),
    #[serde(rename = "unsubscribe")]
    Unsubscribe(UnsubscribeFromStream),
}

#[serde_as]
#[derive(Deserialize)]
pub struct SubscribeToStream {
    pub stream: String,
    #[serde(default)]
    pub params: Option<Box<serde_json::value::RawValue>>,
    #[serde_as(as = "Option<DurationSeconds>")]
    #[serde(default)]
    pub ttl: Option<Duration>,
    #[serde(default)]
    pub priority: Option<BucketPriority>,
}

#[derive(Deserialize)]
pub struct UnsubscribeFromStream {
    pub stream: String,
    pub params: Option<Box<serde_json::value::RawValue>>,
    pub immediate: bool,
}

pub fn apply_subscriptions(
    db: *mut sqlite::sqlite3,
    subscription: SubscriptionChangeRequest,
) -> Result<(), PowerSyncError> {
    match subscription {
        SubscriptionChangeRequest::Subscribe(subscription) => {
            let stmt = db
                .prepare_v2("INSERT INTO ps_stream_subscriptions (stream_name, local_priority, local_params, ttl) VALUES (?, ?2, ?, ?4) ON CONFLICT DO UPDATE SET local_priority = min(coalesce(?2, local_priority), local_priority), ttl = ?4, is_default = FALSE")
                .into_db_result(db)?;

            stmt.bind_text(1, &subscription.stream, sqlite::Destructor::STATIC)?;
            match &subscription.priority {
                Some(priority) => stmt.bind_int(2, priority.number),
                None => stmt.bind_null(2),
            }?;
            stmt.bind_text(
                3,
                match &subscription.params {
                    Some(params) => params.get(),
                    None => "null",
                },
                sqlite::Destructor::STATIC,
            )?;
            match &subscription.ttl {
                Some(ttl) => stmt.bind_int64(4, ttl.as_secs() as i64),
                None => stmt.bind_null(4),
            }?;
            stmt.exec()?;
        }
        SubscriptionChangeRequest::Unsubscribe(subscription) => {
            let stmt = db
                .prepare_v2("UPDATE ps_stream_subscriptions SET ttl = NULL WHERE stream_name = ? AND local_params = ?")
                .into_db_result(db)?;
            stmt.bind_text(1, &subscription.stream, sqlite::Destructor::STATIC)?;
            stmt.bind_text(
                2,
                match &subscription.params {
                    Some(params) => params.get(),
                    None => "null",
                },
                sqlite::Destructor::STATIC,
            )?;
            stmt.exec()?;
        }
    }

    Ok(())
}
