//! Consumer groups: a stream read as a queue.
//!
//! A consumer claims a record, acknowledges it, and the group's cursor moves
//! past it once every record before it is settled too. A claim not
//! acknowledged within the visibility timeout is handed out again, and a
//! record that has failed too many times is dead-lettered so the group can
//! move on.
//!
//! Start at [`GroupReader`], which joins the other three: [`ConsumerGroups`]
//! keeps each group's durable cursor, the in-memory tracker holds what is
//! currently handed out, and [`DeadLetters`] records the offsets a group gave
//! up on.

mod cursors;
mod dead_letters;
mod reader;
mod tracker;

pub use cursors::ConsumerGroups;
pub use dead_letters::DeadLetters;
pub use reader::{Claimed, GroupKey, GroupReader, GroupSnapshot, Seek};
pub use tracker::GroupConsumer;
