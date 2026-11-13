//! The control-plane metadata group a data directory was written under.
//!
//! A Raft control plane picks a random group id when its metadata group forms.
//! A broker with durable storage keeps the id it first served under in
//! `controlplane_group` and checks it at every start and against every
//! catalog snapshot and change-feed page, so data written under a control
//! plane that was later recreated is refused instead of served under a
//! catalog that never knew it. A group upgraded in place says so
//! (`adopts_existing`), and a directory with data from before the record
//! adopts it.

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
    /// The group recorded its id over a catalog it already held: it was
    /// upgraded in place, so a directory with data and no record is this
    /// group's. Absent from a control plane that predates it.
    #[serde(default)]
    pub(crate) adopts_existing: bool,
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
    /// Data from before the record, under a group formed on an empty
    /// catalog: nothing says the data is this group's.
    Unrecorded,
}

/// Decide a start from the record, the control plane's group, whether that
/// group recorded its id over an existing catalog, and whether the directory
/// already holds data.
pub(crate) fn decide(
    stored: Option<&str>,
    current: Option<&str>,
    adopts_existing: bool,
    holds_data: bool,
) -> Verdict {
    match (stored, current) {
        (_, None) => Verdict::Skip,
        (Some(stored), Some(current)) if stored == current => Verdict::Matches,
        (Some(stored), Some(_)) => Verdict::Other {
            stored: stored.to_owned(),
        },
        (None, Some(_)) if holds_data && !adopts_existing => Verdict::Unrecorded,
        (None, Some(_)) => Verdict::Adopt,
    }
}

/// Whether anything is under `root` besides this module's own record. The
/// storage id only exists since 0.6.0-preview.8, so any other entry counts
/// too. `lost+found` is what a fresh filesystem starts with. A directory that
/// cannot be listed counts as holding data.
pub(crate) fn holds_data(root: &Path) -> bool {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return false,
        Err(_) => return true,
    };
    let own = Path::new(FILE_NAME).with_extension("tmp");
    for entry in entries {
        let Ok(entry) = entry else { return true };
        let name = entry.file_name();
        if name != FILE_NAME && name != own.as_os_str() && name != "lost+found" {
            return true;
        }
    }
    false
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

/// What to do about a refusal: restoring the control plane comes first.
pub(crate) const RECOVER: &str = "If the control plane lost its volumes, restore its latest export \
     with `import` and restart the broker: an export with a group id brings that id back, and one \
     taken before group ids existed makes the control plane adopt the brokers' data. Only if no \
     export exists: stop the broker, wipe its whole data directory and start it again";

/// The refusal for a directory written under another group.
pub(crate) fn other_group_message(stored: &str, current: &str) -> String {
    format!(
        "the data directory was written under control-plane metadata group {stored} but the \
         control plane is group {current}; the control plane was recreated, so this data cannot \
         be served under it. {RECOVER}: {UPGRADES}"
    )
}

/// The refusal for a directory that holds data and names no group.
pub(crate) fn unrecorded_message(current: &str) -> String {
    format!(
        "the data directory holds data but records no control-plane metadata group, and the \
         control plane is group {current}, which recorded its id on an empty catalog; this data \
         may belong to a catalog that no longer exists. {RECOVER}: {UPGRADES}"
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

/// Why an ask failed, as far as the operator can act on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failure {
    /// The node token lacks the permission.
    Forbidden,
    /// The control plane has no identity route: it is older than this broker.
    Older,
    /// Anything else, including 503 until the group records its identity.
    Other,
}

impl Failure {
    fn of(err: &anyhow::Error) -> Self {
        let status = err
            .chain()
            .find_map(|cause| cause.downcast_ref::<reqwest::Error>())
            .and_then(reqwest::Error::status);
        match status {
            Some(reqwest::StatusCode::FORBIDDEN) => Self::Forbidden,
            Some(reqwest::StatusCode::NOT_FOUND) => Self::Older,
            _ => Self::Other,
        }
    }
}

/// Ask until the control plane answers; it answers 503 until its group has
/// recorded an identity. Ends only by an answer, so callers race it with the
/// shutdown: the fence is never bypassed.
pub(crate) async fn await_identity(
    client: &reqwest::Client,
    base_url: &str,
    credential: &crate::cluster::credential::NodeCredential,
) -> Identity {
    loop {
        match fetch(client, base_url, credential.bearer().as_str()).await {
            Ok(identity) => return identity,
            Err(err) => {
                let error = format!("{err:#}");
                match Failure::of(&err) {
                    Failure::Forbidden => tracing::error!(
                        error,
                        "the control plane refused this node's identity lookup: the node token \
                         needs the node.view:cluster:* permission; retrying"
                    ),
                    Failure::Older => tracing::warn!(
                        error,
                        "the control plane has no identity route: it is older than this broker \
                         and must be upgraded first; retrying"
                    ),
                    Failure::Other => tracing::warn!(
                        error,
                        "the control plane has not given its identity; retrying"
                    ),
                }
            }
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
        identity.adopts_existing,
        holds_data(root),
    ) {
        Verdict::Skip | Verdict::Matches => Ok(stored),
        Verdict::Adopt => {
            write(root, current)?;
            tracing::info!(
                group_id = current,
                adopts_existing = identity.adopts_existing,
                "recorded the control-plane metadata group"
            );
            Ok(Some(current.to_owned()))
        }
        Verdict::Other { stored } => anyhow::bail!(other_group_message(&stored, current)),
        Verdict::Unrecorded => anyhow::bail!(unrecorded_message(current)),
    }
}

/// What the catalog sync holds the control plane's snapshots and change-feed
/// pages to.
#[derive(Debug, Clone)]
pub(crate) struct Guard {
    /// The group this directory is recorded under.
    pub(crate) stored: String,
    /// Cancelled when an answer names another group: the broker stops.
    pub(crate) refused: CancellationToken,
}

/// An answer named a group other than the one this directory is under.
#[derive(Debug, thiserror::Error)]
#[error("{}", other_group_message(.stored, .current))]
pub(crate) struct Mismatch {
    pub(crate) stored: String,
    pub(crate) current: String,
}

/// An answer named no group while this directory is recorded under one: from
/// a member that has not applied the record yet, or one older than it. Not
/// applied, and asked again next pass.
#[derive(Debug, thiserror::Error)]
#[error(
    "the control plane's {what} names no metadata group but this data directory is recorded \
     under {stored}; not applied, retrying"
)]
pub(crate) struct Unclaimed {
    pub(crate) what: &'static str,
    pub(crate) stored: String,
}

/// Why an answer may not be applied.
#[derive(Debug)]
pub(crate) enum Refusal {
    Mismatch(Mismatch),
    Unclaimed(Unclaimed),
}

impl Guard {
    /// Whether an answer (`what`) claiming `claim` may be applied.
    pub(crate) fn check(&self, claim: Option<&str>, what: &'static str) -> Result<(), Refusal> {
        match claim {
            Some(current) if current == self.stored => Ok(()),
            Some(current) => Err(Refusal::Mismatch(Mismatch {
                stored: self.stored.clone(),
                current: current.to_owned(),
            })),
            None => Err(Refusal::Unclaimed(Unclaimed {
                what,
                stored: self.stored.clone(),
            })),
        }
    }
}

#[cfg(test)]
mod tests;
