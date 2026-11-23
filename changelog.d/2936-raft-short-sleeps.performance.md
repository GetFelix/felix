A Raft write that completes right after another is no longer answered up to a millisecond late: openraft's 1 us batching sleep yields instead of waiting for tokio's next timer tick (#2936).
