use felix_broker::{BrokerError, NotReadable};
use felix_replication::engine::CommitError;
use felix_replication::raft::GroupWriteError;

use super::proposal_error;

fn stopped() -> CommitError {
    CommitError::LeadershipLost {
        what: "write",
        detail: "the shard's group stopped",
    }
}

/// A group write the proposer took and lost (leader deposed, group
/// stopped) may still commit: retrying could redrive or delete twice.
#[test]
fn a_group_write_lost_after_it_was_queued_is_outcome_unknown() {
    let lost = proposal_error(GroupWriteError {
        error: stopped(),
        may_commit: true,
    });
    assert!(
        matches!(
            lost,
            BrokerError::WriteUnsettled {
                timed_out: false,
                ..
            }
        ),
        "{lost:?}"
    );
}

/// One refused before it was appended, on a lapsed lease or before the
/// proposer took it, is retryable.
#[test]
fn a_group_write_refused_before_its_append_is_retryable() {
    let lapsed = CommitError::LeadershipLost {
        what: "write",
        detail: "no majority has answered this leader lately, so nothing was written",
    };
    for error in [lapsed, stopped()] {
        let refused = proposal_error(GroupWriteError {
            error,
            may_commit: false,
        });
        assert!(
            matches!(refused, BrokerError::StateNotReadable(NotReadable::Refused)),
            "{refused:?}"
        );
    }
}
