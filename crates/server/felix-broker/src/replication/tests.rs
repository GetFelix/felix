//! The follower's append rule, against a real log on disk.
//!
//! Every case here is about *position*: where a batch claims to belong versus
//! where the follower actually is. A fake log would decide that itself, so
//! these use the same `DiskLog` a broker runs on and read the records back.
use felix_storage::log::{FsyncMode, LogConfig};
use tempfile::TempDir;

use super::*;
use crate::durable::DurableStorage;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";

/// A follower's log for one shard, with `OnCommit` so "durable" means the
/// bytes reached the device rather than a buffer.
async fn follower() -> (StreamLog, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            segment_size_bytes: 4 * 1024,
            index_spacing_bytes: 256,
            fsync_mode: FsyncMode::OnCommit,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open the shard log");
    (log, dir)
}

fn payload(value: &str) -> Bytes {
    Bytes::copy_from_slice(value.as_bytes())
}

fn batch(values: &[&str]) -> Vec<Bytes> {
    values.iter().map(|value| payload(value)).collect()
}

/// Ship a batch the way a leader would, with a checksum it computed itself.
async fn ship(
    log: &StreamLog,
    first_offset: u64,
    values: &[&str],
) -> std::result::Result<Applied, Divergence> {
    let payloads = batch(values);
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &[]);
    apply(log, first_offset, checksum, &payloads, &[], &[], &[])
        .await
        .expect("apply")
}

/// Everything the follower holds, in offset order.
async fn stored(log: &StreamLog) -> Vec<String> {
    log.read_from(0, 1024 * 1024)
        .await
        .expect("read")
        .into_iter()
        .map(|record| String::from_utf8(record.payload.to_vec()).expect("utf8"))
        .collect()
}

/// The ordinary case: batches arrive in order and land at the leader's offsets.
#[tokio::test]
async fn batches_in_order_land_at_the_leaders_offsets() {
    let (log, _dir) = follower().await;

    let first = ship(&log, 0, &["a", "b"]).await.expect("applied");
    assert_eq!(first.durable_offset, 2);
    assert_eq!(first.appended, 2);

    let second = ship(&log, 2, &["c"]).await.expect("applied");
    assert_eq!(second.durable_offset, 3);
    assert_eq!(second.appended, 1);

    assert_eq!(stored(&log).await, vec!["a", "b", "c"]);
}

/// **A batch starting past the tail is refused, and says where to resume.**
/// Applying it would leave a hole, and a log with a hole cannot be read back —
/// the follower would report a durable offset covering records it does not have.
#[tokio::test]
async fn a_batch_that_would_leave_a_hole_is_refused() {
    let (log, _dir) = follower().await;
    ship(&log, 0, &["a", "b"]).await.expect("applied");

    let divergence = ship(&log, 5, &["f"]).await.expect_err("should be a gap");

    assert_eq!(
        divergence,
        Divergence::Gap {
            expected: 2,
            first_offset: 5,
        },
    );
    assert_eq!(
        divergence.expected_offset(),
        2,
        "the leader must resume at 2"
    );
    assert_eq!(
        stored(&log).await,
        vec!["a", "b"],
        "the hole was written anyway"
    );
}

/// **A resent batch is not stored twice.** Replication has to be able to resend
/// a batch whose acknowledgement was lost, and a duplicate record is invisible
/// to a subscriber until the offsets stop matching the leader's.
#[tokio::test]
async fn a_resent_batch_is_acknowledged_without_being_stored_again() {
    let (log, _dir) = follower().await;
    ship(&log, 0, &["a", "b"]).await.expect("applied");

    let retry = ship(&log, 0, &["a", "b"]).await.expect("applied");

    assert_eq!(
        retry.durable_offset, 2,
        "the retry moved the acknowledged mark"
    );
    assert_eq!(retry.appended, 0, "the retry stored the records again");
    assert_eq!(stored(&log).await, vec!["a", "b"]);
}

/// A retry that overlaps the tail stores only the part the follower is missing.
#[tokio::test]
async fn a_retry_that_overlaps_the_tail_stores_only_the_new_suffix() {
    let (log, _dir) = follower().await;
    ship(&log, 0, &["a", "b"]).await.expect("applied");

    // The leader resends from 1, having not heard about 1 landing.
    let applied = ship(&log, 1, &["b", "c", "d"]).await.expect("applied");

    assert_eq!(applied.appended, 2, "the overlap was stored again");
    assert_eq!(applied.durable_offset, 4);
    assert_eq!(stored(&log).await, vec!["a", "b", "c", "d"]);
}

/// **Bytes that disagree with what is stored stop replication.** Records are
/// never rewritten, so there is no repair — and continuing would leave two logs
/// that agree on offsets while disagreeing on contents, which nothing
/// downstream could detect.
#[tokio::test]
async fn a_batch_that_disagrees_with_stored_bytes_stops_progress() {
    let (log, _dir) = follower().await;
    ship(&log, 0, &["a", "b"]).await.expect("applied");

    let divergence = ship(&log, 0, &["a", "DIFFERENT", "c"])
        .await
        .expect_err("should conflict");

    assert!(
        matches!(divergence, Divergence::Conflict { offset: 1, .. }),
        "{divergence:?}",
    );
    assert_eq!(
        stored(&log).await,
        vec!["a", "b"],
        "a conflicting batch appended its suffix anyway",
    );
}

/// A batch corrupted in transit is refused before anything is written, so a bad
/// frame cannot become a stored record.
#[tokio::test]
async fn a_batch_that_did_not_survive_the_trip_is_refused() {
    let (log, _dir) = follower().await;
    let payloads = batch(&["a", "b"]);

    let divergence = apply(&log, 0, 0xdead_beef, &payloads, &[], &[], &[])
        .await
        .expect("apply")
        .expect_err("should be corrupt");

    assert!(
        matches!(divergence, Divergence::Corrupt { .. }),
        "{divergence:?}"
    );
    assert!(stored(&log).await.is_empty(), "a corrupt batch was stored");
}

/// **The checksum distinguishes a different split of the same bytes.** Without
/// the length in the hash, `["ab", "c"]` and `["a", "bc"]` are the same input —
/// and they are different records, which is exactly the divergence this exists
/// to catch.
#[test]
fn the_checksum_separates_records_that_concatenate_alike() {
    let one = felix_wire::internal::batch_checksum(&batch(&["ab", "c"]), &[], &[]);
    let other = felix_wire::internal::batch_checksum(&batch(&["a", "bc"]), &[], &[]);

    assert_ne!(one, other);
}

/// An empty batch is a position probe: it asks where the follower is without
/// claiming anything, so it is answered rather than refused.
#[tokio::test]
async fn an_empty_batch_reports_the_tail() {
    let (log, _dir) = follower().await;
    ship(&log, 0, &["a", "b"]).await.expect("applied");

    let applied = ship(&log, 2, &[]).await.expect("applied");

    assert_eq!(applied.durable_offset, 2);
    assert_eq!(applied.appended, 0);
}

/// A fresh follower starts at zero, so the leader's first batch has to start
/// there too.
#[tokio::test]
async fn a_fresh_follower_expects_the_first_offset() {
    let (log, _dir) = follower().await;

    let divergence = ship(&log, 1, &["b"]).await.expect_err("should be a gap");

    assert_eq!(divergence.expected_offset(), 0);
}

/// **The acknowledged offset is durable, not buffered.** Under `OnCommit` the
/// records are readable back from disk the instant `apply` returns; a follower
/// that acknowledged before its own fsync would let the leader count a record
/// toward a quorum it would not survive.
#[tokio::test]
async fn the_acknowledged_offset_is_on_disk_before_it_is_reported() {
    let (log, _dir) = follower().await;

    let applied = ship(&log, 0, &["a", "b", "c"]).await.expect("applied");

    assert_eq!(applied.durable_offset, 3);
    assert_eq!(
        log.durable_offset(),
        3,
        "the batch was acknowledged before it was durable",
    );
    assert_eq!(stored(&log).await, vec!["a", "b", "c"]);
}

/// Applying the same sequence twice, in any resend pattern, converges on the
/// leader's log rather than accumulating.
#[tokio::test]
async fn resends_converge_rather_than_accumulate() {
    let (log, _dir) = follower().await;

    ship(&log, 0, &["a", "b"]).await.expect("applied");
    ship(&log, 0, &["a", "b"]).await.expect("applied");
    ship(&log, 1, &["b", "c"]).await.expect("applied");
    ship(&log, 2, &["c"]).await.expect("applied");
    let last = ship(&log, 3, &["d"]).await.expect("applied");

    assert_eq!(last.durable_offset, 4);
    assert_eq!(stored(&log).await, vec!["a", "b", "c", "d"]);
}

/// **A follower holding more than the batch answers with the batch's end, not
/// its own tail.**
///
/// The answer is what the leader advances its cursor to, so answering `tail`
/// skips everything between the batch and that tail — and those records were
/// never compared against anything. A follower that kept an uncommitted record
/// from a dead leader has exactly that shape (#406): the extra record is at an
/// offset the new leader will reuse, and the skip means the conflict is never
/// found.
#[tokio::test]
async fn a_batch_wholly_overlapping_does_not_confirm_past_itself() {
    let (log, _dir) = follower().await;
    // The follower has three records; the last never reached a majority.
    ship(&log, 0, &["a", "b", "orphan"]).await.expect("applied");

    // A new leader resends what it has, which stops short of the orphan.
    let retry = ship(&log, 0, &["a", "b"]).await.expect("applied");

    assert_eq!(
        retry.durable_offset, 2,
        "the follower confirmed offset {} when the batch only verified up to 2; \
         the leader will resume past its orphan and never compare it",
        retry.durable_offset,
    );
}

/// The skip above is what lets a conflict go unnoticed.
///
/// Same shape, one step further: the new leader writes its own record at the
/// offset the orphan occupies. If the cursor skipped past it, this batch is
/// never checked and the two logs silently disagree.
#[tokio::test]
async fn an_orphan_at_a_reused_offset_is_reported_as_a_conflict() {
    let (log, _dir) = follower().await;
    ship(&log, 0, &["a", "b", "orphan"]).await.expect("applied");

    let confirmed = ship(&log, 0, &["a", "b"]).await.expect("applied");

    // The leader resumes from what the follower confirmed, and writes its own
    // record at offset 2.
    let divergence = ship(&log, confirmed.durable_offset, &["committed"])
        .await
        .expect_err("the follower holds a different record at this offset");

    assert!(
        matches!(divergence, Divergence::Conflict { .. }),
        "expected a conflict at the reused offset, got {divergence:?}",
    );
}

/// Marks are stored as shipped, and a record whose mark disagrees with the
/// leader's is a divergence even when its bytes agree: kept, it would leave
/// this follower and the leader disagreeing about where a producer stands.
#[tokio::test]
async fn marks_are_stored_as_shipped_and_a_different_mark_is_a_conflict() {
    use felix_storage::disk_log::ProducerSequence;

    let (log, _dir) = follower().await;
    let payloads = batch(&["a", "b"]);
    let opens = ProducerMark::Opens {
        producer_id: 9,
        sequence: 0,
        len: 2,
    };
    let marks = [opens, ProducerMark::Continues];
    let checksum = felix_wire::internal::batch_checksum(&payloads, &marks, &[]);
    apply(&log, 0, checksum, &payloads, &marks, &[], &[])
        .await
        .expect("apply")
        .expect("in order");
    assert_eq!(
        log.producer_sequence(9, 0),
        ProducerSequence::Held {
            first: 0,
            last: 1,
            digest: Some(felix_storage::log::PayloadDigest::of(&payloads)),
        }
    );

    // The same bytes, unmarked, at the same offsets.
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &[]);
    let refused = apply(&log, 0, checksum, &payloads, &[], &[], &[])
        .await
        .expect("apply")
        .expect_err("a record with a different mark was taken as the same");
    assert!(matches!(refused, Divergence::Conflict { offset: 0, .. }));
}

/// **A resend racing the original lands once, at the leader's offsets.** Each
/// apply decides from the tail it read; without the write being conditional
/// on that tail, two copies of one batch that both read tail 0 would both be
/// appended, the second at offsets the leader never assigned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_resends_of_one_batch_land_once() {
    let (log, _dir) = follower().await;
    let log = std::sync::Arc::new(log);
    for round in 0..20u64 {
        let first = round * 2;
        let values = [format!("r{round}a"), format!("r{round}b")];
        let copies: Vec<_> = (0..4)
            .map(|_| {
                let log = std::sync::Arc::clone(&log);
                let values = values.clone();
                tokio::spawn(async move {
                    let values: Vec<&str> = values.iter().map(String::as_str).collect();
                    ship(&log, first, &values).await
                })
            })
            .collect();
        for copy in copies {
            let applied = copy.await.expect("task").expect("applied");
            assert_eq!(applied.durable_offset, first + 2);
        }
        assert_eq!(log.tail_offset().await.expect("tail"), first + 2);
    }
    let held = stored(&log).await;
    assert_eq!(held.len(), 40, "a resend was stored twice: {held:?}");
}

/// The conditional append refuses a batch whose tail moved, and writes
/// nothing.
#[tokio::test]
async fn an_append_at_a_stale_tail_writes_nothing() {
    let (log, _dir) = follower().await;
    ship(&log, 0, &["a"]).await.expect("first");
    let refused = log
        .begin_append_marked_at(0, &batch(&["b"]), &[RecordMark::None], &[], &[])
        .await
        .expect("append");
    assert!(refused.is_none());
    assert_eq!(stored(&log).await, vec!["a".to_string()]);
}

/// A follower stores each record's publisher as shipped, so a promoted
/// replica reports what the leader did, and a resend whose publisher differs
/// from the stored one is a different record.
#[tokio::test]
async fn publishers_are_stored_as_shipped_and_compared_on_a_resend() {
    let (log, _dir) = follower().await;
    let payloads = batch(&["a", "b"]);
    let publishers = vec![Some(payload("alice")), None];
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &publishers);
    apply(&log, 0, checksum, &payloads, &[], &publishers, &[])
        .await
        .expect("apply")
        .expect("stored");
    let held = log.read_from(0, 1 << 20).await.expect("read");
    assert_eq!(
        held.iter()
            .map(|record| record.publisher.clone())
            .collect::<Vec<_>>(),
        publishers
    );
    // The leader's records, shipped again: an overlap that agrees.
    assert!(
        apply(&log, 0, checksum, &payloads, &[], &publishers, &[])
            .await
            .expect("apply")
            .is_ok()
    );

    let other = vec![Some(payload("mallory")), None];
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &other);
    let refused = apply(&log, 0, checksum, &payloads, &[], &other, &[])
        .await
        .expect("apply");
    assert!(matches!(
        refused,
        Err(Divergence::Conflict { offset: 0, .. })
    ));
    assert_eq!(
        publishers_to_wire(&held),
        vec![Some(payload("alice")), None]
    );
}

async fn stored_times(log: &StreamLog) -> Vec<u64> {
    log.read_log_from(0, 1 << 20)
        .await
        .expect("read")
        .iter()
        .map(|record| record.timestamp_micros)
        .collect()
}

/// A follower stores each record's time as the leader shipped it, so a
/// replica promoted later reports what the leader's readers saw. A resend
/// that overlaps stores only the new suffix's times, and the stored ones are
/// not compared: a copy from a leader that sent none holds this broker's own.
#[tokio::test]
async fn times_are_stored_as_shipped() {
    let (log, _dir) = follower().await;
    let payloads = batch(&["a", "b"]);
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &[]);
    apply(&log, 0, checksum, &payloads, &[], &[], &[11, 12])
        .await
        .expect("apply")
        .expect("stored");
    assert_eq!(stored_times(&log).await, vec![11, 12]);

    let payloads = batch(&["b", "c"]);
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &[]);
    apply(&log, 1, checksum, &payloads, &[], &[], &[99, 13])
        .await
        .expect("apply")
        .expect("the overlap agrees on everything compared");
    assert_eq!(stored_times(&log).await, vec![11, 12, 13]);
}

/// A leader that predates shipped times sends none, and the follower stamps
/// the records with its own clock.
#[tokio::test]
async fn a_batch_without_times_takes_this_brokers_clock() {
    let (log, _dir) = follower().await;
    let payloads = batch(&["a"]);
    let checksum = felix_wire::internal::batch_checksum(&payloads, &[], &[]);
    let before = crate::durable::append_time_now();
    apply(&log, 0, checksum, &payloads, &[], &[], &[])
        .await
        .expect("apply")
        .expect("stored");
    let stored = stored_times(&log).await;
    assert!(stored[0] >= before, "{stored:?} predates {before}");
}
