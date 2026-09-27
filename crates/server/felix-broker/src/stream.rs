//! One stream shard in memory: its subscribers, its replay ring, and the
//! batches fanned out to them.
//!
//! [`StreamState`] is the per-shard core the publish path appends to and
//! subscribers register with. Each subscriber has its own bounded queue and
//! [`SubQueuePolicy`] decides what a full one costs, so a slow subscriber
//! never slows the publisher. A [`DeliveryEnvelope`] is shared by every
//! subscriber and encodes its frame once.

mod delivery;
mod producers;
mod state;
mod subscription;

pub use delivery::{DeliveryEnvelope, SubQueuePolicy};
pub use subscription::{Subscription, SubscriptionGuard, SubscriptionReceiver};

pub(crate) use delivery::QueuedDelivery;
pub(crate) use producers::Sequenced;
pub(crate) use state::{StreamState, SubscriberEntry};
