use core::time::Duration;

use alloc::string::String;
use serde::Deserialize;
use serde_with::{serde_as, DurationSeconds};

use crate::sync::BucketPriority;

/// A request sent from a PowerSync SDK to alter the subscriptions managed by this client.
#[derive(Deserialize)]
pub enum SubscriptionChangeRequest {
    Subscribe(SubscribeToStream),
}

#[serde_as]
#[derive(Deserialize)]
pub struct SubscribeToStream {
    pub stream: String,
    pub params: Option<serde_json::value::RawValue>,
    #[serde_as(as = "Option<DurationSeconds>")]
    pub ttl: Option<Duration>,
    pub priority: Option<BucketPriority>,
}

#[derive(Deserialize)]
pub struct UnsubscribeFromStream {
    pub stream: String,
    pub params: Option<serde_json::value::RawValue>,
    pub immediate: bool,
}
