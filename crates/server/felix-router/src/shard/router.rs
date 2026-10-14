//! Resolving a shard against the current table: here, elsewhere, or not at all.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;

use super::{NodeRef, RoutingTable, ShardKey};
use crate::RegionRouter;

/// Resolves shards to nodes, hot-path cheap and updated by whole-table swap.
#[derive(Debug)]
pub struct ShardRouter {
    local_node_id: String,
    local_region: String,
    routes: ArcSwap<Routes>,
    /// Which regions this node may reach. Policy, not placement.
    regions: RegionRouter<String>,
}

/// One published set of routes: the table and the node ids it was built
/// against, swapped in together so a reader never pairs a table with another
/// publish's catalog.
#[derive(Debug, Default)]
pub struct Routes {
    table: Arc<RoutingTable>,
    /// Node ids this router has been told about, so an assignment naming one
    /// it has never seen is reported as unknown rather than not live.
    known_nodes: HashSet<String>,
}

impl Routes {
    pub fn table(&self) -> &Arc<RoutingTable> {
        &self.table
    }
}

impl ShardRouter {
    pub fn new(
        local_node_id: impl Into<String>,
        local_region: impl Into<String>,
        regions: RegionRouter<String>,
    ) -> Self {
        Self {
            local_node_id: local_node_id.into(),
            local_region: local_region.into(),
            routes: ArcSwap::from_pointee(Routes::default()),
            regions,
        }
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    /// Replace every route in one swap.
    ///
    /// Whole-table rather than per-shard: a partial update would let a reader
    /// see half of a rebalance, and routes are small enough that rebuilding is
    /// cheaper than reasoning about that.
    ///
    /// Returns what was published, for a caller that pairs it with state of its
    /// own and needs both to change in one swap.
    pub fn publish(&self, table: RoutingTable, nodes: &HashMap<String, NodeRef>) -> Arc<Routes> {
        let routes = Arc::new(Routes {
            table: Arc::new(table),
            known_nodes: nodes.keys().cloned().collect(),
        });
        self.routes.store(Arc::clone(&routes));
        routes
    }

    /// The current table, for a caller that wants to read several routes
    /// against one consistent snapshot.
    pub fn snapshot(&self) -> Arc<RoutingTable> {
        Arc::clone(&self.routes.load().table)
    }

    /// The current routes, to resolve against later with [`Self::resolve_with`].
    pub fn routes(&self) -> Arc<Routes> {
        self.routes.load_full()
    }

    /// Resolve a shard against the current table.
    pub fn resolve(&self, key: &ShardKey) -> Resolution {
        self.resolve_in(&self.routes.load(), key, None)
    }

    /// Resolve a shard against routes the caller already holds.
    pub fn resolve_with(&self, routes: &Routes, key: &ShardKey) -> Resolution {
        self.resolve_in(routes, key, None)
    }

    /// Resolve a shard, rejecting a table older than the caller already knows
    /// about.
    ///
    /// The caller has seen generation `wanted`; if this router is behind, the
    /// honest answer is that its copy is stale, not that the shard is somewhere
    /// it has since moved from.
    pub fn resolve_at(&self, key: &ShardKey, wanted: u64) -> Resolution {
        self.resolve_in(&self.routes.load(), key, Some(wanted))
    }

    /// Whether this node should store records for `key` shipped at `generation`.
    pub fn replica_role(&self, key: &ShardKey, generation: u64) -> ReplicaRole {
        let routes = self.routes.load();
        let Some(route) = routes.table.get(key) else {
            // Nothing known about the shard at all, so this node cannot confirm
            // membership. Behind rather than NotAReplica: the assignment may
            // simply not have arrived, and refusing permanently would strand a
            // follower whose watch is a moment late.
            return ReplicaRole::Behind {
                have: 0,
                named: generation,
            };
        };
        if generation < route.generation {
            return ReplicaRole::Fenced {
                have: route.generation,
                named: generation,
            };
        }
        if generation > route.generation {
            return ReplicaRole::Behind {
                have: route.generation,
                named: generation,
            };
        }
        if route
            .replicas
            .iter()
            .any(|replica| replica.node_id == self.local_node_id)
        {
            ReplicaRole::Follower
        } else {
            ReplicaRole::NotAReplica
        }
    }

    /// The stream incarnation `key`'s assignment names, if the table has one.
    pub fn incarnation(&self, key: &ShardKey) -> Option<u64> {
        self.routes
            .load()
            .table
            .get(key)
            .map(|route| route.incarnation)
    }

    fn resolve_in(&self, routes: &Routes, key: &ShardKey, wanted: Option<u64>) -> Resolution {
        let Some(route) = routes.table.get(key) else {
            // A caller expecting a generation for a shard this table has never
            // heard of is ahead of us, not looking at a missing assignment.
            return match wanted {
                Some(wanted) => Resolution::Stale { have: 0, wanted },
                None => Resolution::Unavailable(Unavailable::NoAssignment),
            };
        };

        if let Some(wanted) = wanted
            && route.generation < wanted
        {
            return Resolution::Stale {
                have: route.generation,
                wanted,
            };
        }

        // Local first: this node's own liveness is not something it needs the
        // catalog's opinion on, and a broker that stopped serving its own shards
        // because it had not yet seen its own heartbeat land would be absurd.
        if route.leader.node_id == self.local_node_id {
            return Resolution::Local {
                generation: route.generation,
            };
        }

        if !routes.known_nodes.contains(&route.leader.node_id) {
            return Resolution::Unavailable(Unavailable::LeaderUnknown {
                node_id: route.leader.node_id.clone(),
            });
        }
        if !route.leader.live {
            return Resolution::Unavailable(Unavailable::LeaderNotLive {
                node_id: route.leader.node_id.clone(),
            });
        }
        // Policy last: a blocked region is a real refusal, but saying so about a
        // leader that is also dead would be the less useful of the two facts.
        if !self
            .regions
            .can_route(&self.local_region, &route.leader.region)
        {
            return Resolution::Unavailable(Unavailable::RegionNotRoutable {
                region: route.leader.region.clone(),
            });
        }

        Resolution::Remote {
            node_id: route.leader.node_id.clone(),
            advertise_addr: route.leader.advertise_addr,
            generation: route.generation,
        }
    }
}

/// What to do with a request for a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// This node leads the shard. Handle it here.
    Local {
        generation: u64,
    },
    /// Another node leads it, at this address. The caller forwards there.
    Remote {
        node_id: String,
        advertise_addr: SocketAddr,
        generation: u64,
    },
    /// The caller's view of the assignment is newer than this router's.
    ///
    /// Distinct from unavailable: the route is not wrong, this node's copy is
    /// behind, and the answer is to wait for the watch rather than to fail the
    /// stream.
    Stale {
        have: u64,
        wanted: u64,
    },
    Unavailable(Unavailable),
}

impl Resolution {
    pub fn is_local(&self) -> bool {
        matches!(self, Resolution::Local { .. })
    }
}

/// Why a shard cannot be routed.
///
/// Separate variants because they need different responses: a missing
/// assignment waits for placement, a dead leader waits for failover, and a
/// blocked region is a policy decision that will not resolve on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unavailable {
    /// Placement has not assigned this shard.
    NoAssignment,
    /// The assignment names a node this router has no address for.
    LeaderUnknown { node_id: String },
    /// The leader is registered but not live.
    LeaderNotLive { node_id: String },
    /// Region policy forbids reaching the leader's region from here.
    RegionNotRoutable { region: String },
}

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoAssignment => write!(f, "shard has no assignment"),
            Self::LeaderUnknown { node_id } => {
                write!(f, "leader {node_id} is not in the routing table")
            }
            Self::LeaderNotLive { node_id } => write!(f, "leader {node_id} is not live"),
            Self::RegionNotRoutable { region } => {
                write!(f, "region {region} is not routable from here")
            }
        }
    }
}

/// Whether this node may store records another broker replicates to it.
///
/// Separate from [`Resolution`], which answers "who serves reads and writes".
/// A follower serves neither and still has to store, so the two questions have
/// different answers for the same shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaRole {
    /// This node is in the shard's replica set at the epoch the leader named.
    Follower,
    /// The leader named an epoch older than this node's. It has been
    /// superseded, and a superseded leader's records must not be stored: it may
    /// have written them after losing the shard.
    Fenced { have: u64, named: u64 },
    /// This node's view is older than the epoch named. Its copy is behind, so
    /// it cannot yet tell whether it is a replica at that epoch.
    Behind { have: u64, named: u64 },
    /// This node is not in the shard's replica set. Includes leading it: a
    /// leader does not replicate to itself.
    NotAReplica,
}
