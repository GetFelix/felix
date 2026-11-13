//! The control-plane metadata group a data directory was written under.
//!
//! A Raft control plane picks a random group id when its metadata group forms.
//! A broker with durable storage keeps the id it first served under in
//! `controlplane_group` and checks it at every start and against every
//! catalog snapshot, so data written under a control plane that was later
//! recreated is refused instead of served under a catalog that never knew it.

use std::io::Write as _;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

/// The record's name in the data directory.
pub(crate) const FILE_NAME: &str = "controlplane_group";

/// Where the operator is told what to do about a refusal.
const UPGRADES: &str = "https://docs.getfelix.dev/deployment/upgrades/#a-recreated-control-plane";

/// How long to wait between asks while the control plane has no identity yet.
const RETRY: Duration = Duration::from_secs(1);

/// What `GET /v1/cluster/identity` answers.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub(crate) struct Identity {
    /// Absent when the control plane has no metadata group.
    #[serde(default)]
    pub(crate) group_id: Option<String>,
    pub(crate) shard_engine: String,
}

/// The shard engine this broker runs.
// FELIX_SHARD_ENGINE (#2448) replaces this with the configured value.
pub(crate) fn own_shard_engine() -> &'static str {
    "hybrid"
}

/// What a start does with the control plane's group id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The control plane has no group, so there is nothing to compare or store.
    Skip,
    /// The record names the control plane's group.
    Matches,
    /// No record and no data yet: store the control plane's group.
    Adopt,
    /// The record names another group.
    Other { stored: String },
    /// Data from before the record, which says nothing of its catalog.
    Unrecorded,
}

/// Decide a start from the record, the control plane's group and whether the
/// directory already holds data.
pub(crate) fn decide(stored: Option<&str>, current: Option<&str>, holds_data: bool) -> Verdict {
    match (stored, current) {
        (_, None) => Verdict::Skip,
        (Some(stored), Some(current)) if stored == current => Verdict::Matches,
        (Some(stored), Some(_)) => Verdict::Other {
            stored: stored.to_owned(),
        },
        (None, Some(_)) if holds_data => Verdict::Unrecorded,
        (None, Some(_)) => Verdict::Adopt,
    }
}

/// Whether a broker has run on `root`. The storage id is written when a run
/// begins, before any log, so a directory without one holds nothing; one that
/// cannot be told apart counts as holding data.
pub(crate) fn holds_data(root: &Path) -> bool {
    root.join(felix_storage::storage_identity::FILE_NAME)
        .try_exists()
        .unwrap_or(true)
}

/// The group id recorded under `root`. An empty or unreadable file is an
/// error: guessing would adopt over data.
pub(crate) fn read(root: &Path) -> Result<Option<String>> {
    let path = root.join(FILE_NAME);
    match std::fs::read_to_string(&path) {
        Ok(text) => {
            let id = text.trim();
            anyhow::ensure!(!id.is_empty(), "{} is empty", path.display());
            Ok(Some(id.to_owned()))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {}", path.display())),
    }
}

/// Record `id` under `root`, durably: a crash leaves no record or this one.
pub(crate) fn write(root: &Path, id: &str) -> Result<()> {
    std::fs::create_dir_all(root).with_context(|| format!("create {}", root.display()))?;
    let path = root.join(FILE_NAME);
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary)
        .with_context(|| format!("create {}", temporary.display()))?;
    file.write_all(id.as_bytes())
        .and_then(|()| file.sync_all())
        .with_context(|| format!("write {}", temporary.display()))?;
    drop(file);
    std::fs::rename(&temporary, &path).with_context(|| format!("rename to {}", path.display()))?;
    std::fs::File::open(root)
        .and_then(|dir| dir.sync_all())
        .with_context(|| format!("sync {}", root.display()))
}

/// The refusal for a directory written under another group.
pub(crate) fn other_group_message(stored: &str, current: &str) -> String {
    format!(
        "the data directory was written under control-plane metadata group {stored} but the \
         control plane is group {current}; the control plane was recreated, so this data cannot \
         be served under it. Stop the broker, wipe its whole data directory and start it again: \
         {UPGRADES}"
    )
}

/// The refusal for a directory that holds data and names no group.
pub(crate) fn unrecorded_message(current: &str) -> String {
    format!(
        "the data directory holds data but records no control-plane metadata group (it was \
         written by 0.7 or by a build before this check), and the control plane is group \
         {current}; this data may belong to a catalog that no longer exists. Stop the broker, \
         wipe its whole data directory and start it again: {UPGRADES}"
    )
}

/// The refusal for a broker whose engine is not the cluster's.
pub(crate) fn engine_message(own: &str, cluster: &str) -> String {
    format!(
        "this broker runs shard engine {own} but the cluster was formed with {cluster}; set \
         FELIX_SHARD_ENGINE={cluster} on this broker (the control plane's record does not change)"
    )
}

/// Ask the control plane for its identity once.
pub(crate) async fn fetch(
    client: &reqwest::Client,
    base_url: &str,
    bearer: &str,
) -> Result<Identity> {
    let url = format!("{}/v1/cluster/identity", base_url.trim_end_matches('/'));
    let response = client
        .get(url)
        .bearer_auth(bearer)
        .send()
        .await
        .context("request")?
        .error_for_status()
        .context("status")?;
    response.json().await.context("body")
}

/// Ask until the control plane answers; it answers 503 until its group has
/// recorded an identity. Ends only by an answer, so callers race it with the
/// shutdown.
pub(crate) async fn await_identity(
    client: &reqwest::Client,
    base_url: &str,
    credential: &crate::cluster::credential::NodeCredential,
) -> Identity {
    loop {
        match fetch(client, base_url, credential.bearer().as_str()).await {
            Ok(identity) => return identity,
            Err(err) => tracing::warn!(
                error = %format!("{err:#}"),
                "the control plane has not given its identity; retrying"
            ),
        }
        tokio::time::sleep(RETRY).await;
    }
}

/// Judge the data directory at `root` against the control plane's `identity`
/// and, for a new directory, record its group. Returns the group the
/// directory is recorded under, which the catalog sync holds snapshots to.
pub(crate) fn admit(root: &Path, identity: &Identity) -> Result<Option<String>> {
    let engine = own_shard_engine();
    anyhow::ensure!(
        identity.shard_engine == engine,
        engine_message(engine, &identity.shard_engine)
    );
    let stored = read(root)?;
    let current = identity.group_id.as_deref().unwrap_or_default();
    match decide(
        stored.as_deref(),
        identity.group_id.as_deref(),
        holds_data(root),
    ) {
        Verdict::Skip | Verdict::Matches => Ok(stored),
        Verdict::Adopt => {
            write(root, current)?;
            tracing::info!(
                group_id = current,
                "recorded the control-plane metadata group"
            );
            Ok(Some(current.to_owned()))
        }
        Verdict::Other { stored } => anyhow::bail!(other_group_message(&stored, current)),
        Verdict::Unrecorded => anyhow::bail!(unrecorded_message(current)),
    }
}

/// What the catalog sync holds the control plane's snapshots to.
#[derive(Debug, Clone)]
pub(crate) struct Guard {
    /// The group this directory is recorded under.
    pub(crate) stored: String,
    /// Cancelled when a snapshot names another group: the broker stops.
    pub(crate) refused: CancellationToken,
}

/// A snapshot named a group other than the one this directory is under.
#[derive(Debug, thiserror::Error)]
#[error("{}", other_group_message(.stored, .current))]
pub(crate) struct Mismatch {
    pub(crate) stored: String,
    pub(crate) current: String,
}

impl Guard {
    /// Whether a snapshot claiming `claim` may be applied. No claim is not a
    /// mismatch.
    pub(crate) fn check(&self, claim: Option<&str>) -> Result<(), Mismatch> {
        match claim {
            Some(current) if current != self.stored => Err(Mismatch {
                stored: self.stored.clone(),
                current: current.to_owned(),
            }),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;
