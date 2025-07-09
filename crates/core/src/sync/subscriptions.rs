use core::{cmp::Ordering, hash::Hash, time::Duration};

use alloc::{boxed::Box, string::String};
use serde::Deserialize;
use serde_with::{serde_as, DurationSeconds};

use crate::{sync::BucketPriority, util::JsonString};

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
    pub fn key(&self) -> SubscriptionKey {
        SubscriptionKey {
            stream_name: self.stream_name.clone(),
            params: self.local_params.clone(),
        }
    }
}

/// A request sent from a PowerSync SDK to alter the subscriptions managed by this client.
#[derive(Deserialize)]
pub enum SubscriptionChangeRequest {
    Subscribe(SubscribeToStream),
}

#[serde_as]
#[derive(Deserialize)]
pub struct SubscribeToStream {
    pub stream: String,
    pub params: Option<Box<serde_json::value::RawValue>>,
    #[serde_as(as = "Option<DurationSeconds>")]
    pub ttl: Option<Duration>,
    pub priority: Option<BucketPriority>,
}

#[derive(Deserialize)]
pub struct UnsubscribeFromStream {
    pub stream: String,
    pub params: Option<Box<serde_json::value::RawValue>>,
    pub immediate: bool,
}
