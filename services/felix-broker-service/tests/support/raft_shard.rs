//! A one-node cluster whose test stream is on a Raft shard: the second
//! fixture the Kafka and MQTT client matrices run against.

use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};

/// A started broker process and the one stream a test writes to.
pub(crate) struct RaftShard {
    pub(crate) cluster: Cluster,
    pub(crate) node: String,
    stream: StreamSpec,
}

impl RaftShard {
    /// Start `config` as one Raft broker, then create `stream`. Created after
    /// the start, so the harness's readiness probes write to a stream of their
    /// own and every shard of this one starts empty. Without
    /// `record_headers` the broker does not report the features a cluster
    /// stores keys and headers under, so they stay off, as on a local broker
    /// that was not opted in; a new cluster would finalize them itself.
    pub(crate) async fn start(
        config: ClusterConfig,
        stream: StreamSpec,
        record_headers: bool,
    ) -> Self {
        let mut broker_env = config.broker_env;
        // Named, so the fixture is Raft whatever engine a suite defaults to.
        broker_env.push(("FELIX_SHARD_ENGINE".to_string(), "raft".to_string()));
        let headers = [
            felix_common::fleet::RECORD_HEADERS.name(),
            felix_common::fleet::RAFT_RECORD_HEADERS.name(),
        ];
        if !record_headers {
            let reported: Vec<&str> = felix_common::fleet::IMPLEMENTED
                .iter()
                .map(|feature| feature.name())
                .filter(|name| !headers.contains(name))
                .collect();
            broker_env.push(("FELIX_TEST_FLEET_FEATURES".to_string(), reported.join(",")));
        }
        let cluster = Cluster::start(ClusterConfig {
            nodes: 1,
            streams: vec![StreamSpec::new("ready", 1)],
            broker_env,
            ..config
        })
        .await
        .expect("start a one-node Raft cluster");
        // A new Raft cluster has every Raft feature finalized; a test that
        // names its engine finalizes them itself.
        let mut features: Vec<&str> = felix_common::fleet::IMPLEMENTED
            .iter()
            .map(|feature| feature.name())
            .filter(|name| name.starts_with("raft_"))
            .filter(|name| record_headers || !headers.contains(name))
            .collect();
        if record_headers {
            features.insert(0, felix_common::fleet::RECORD_HEADERS.name());
        }
        cluster
            .finalize_fleet_features(&features, Duration::from_secs(30))
            .await
            .expect("finalize the Raft features");
        cluster
            .create_stream(&stream)
            .await
            .expect("create the stream");
        let node = cluster.node_ids().remove(0);
        let shard = Self {
            cluster,
            node,
            stream,
        };
        shard.await_serving().await;
        shard
    }

    /// Stop the broker and start it again with what the cluster now says,
    /// keeping its data.
    #[allow(dead_code)] // Only the Kafka matrix switches listeners.
    pub(crate) async fn restart(&mut self) {
        self.cluster
            .stop_node(&self.node)
            .await
            .expect("stop the broker");
        self.cluster
            .restart_node(&self.node)
            .await
            .expect("restart the broker");
        self.await_serving().await;
    }

    /// Wait until every shard of the stream has a leader that serves a read,
    /// without writing to it.
    async fn await_serving(&self) {
        self.cluster
            .await_raft_leaders_placed(Duration::from_secs(60))
            .await
            .expect("every shard led");
        let shards = self.stream.shards;
        felix_cluster::wait::until(
            Duration::from_secs(60),
            "the broker to route the stream at its full width",
            || async move {
                self.cluster
                    .stream_shards_via(&self.node, &self.stream.name)
                    .await
                    .is_ok_and(|width| width == shards)
            },
        )
        .await
        .expect("stream width");
        for shard in 0..shards {
            self.replay(shard).await;
        }
    }

    /// Every record of `shard`, from one whole replay through a Felix client.
    pub(crate) async fn replay(&self, shard: u32) -> Vec<felix_client::Event> {
        self.cluster
            .whole_replay_of_shard(
                &self.node,
                &self.stream.name,
                shard,
                Duration::from_secs(1),
                Duration::from_secs(30),
            )
            .await
            .expect("replay the shard")
    }
}
