//! The migration tool: Postgres out, Raft group in.
//!
//! Two subcommands, deliberately dumb, because the intelligence lives
//! elsewhere: the export reuses the store traits (so it reads exactly what
//! the API serves), and the import is one `ImportState` command proposed to
//! the running group (so it is atomic, replicated, and refused by a group
//! that already holds state unless the operator says `--overwrite`). The
//! import speaks to a member's Raft peer listener as a peer, so it needs the
//! cluster's `FELIX_RAFT_CLUSTER_ID` and `FELIX_RAFT_PEER_TOKEN` (and the
//! `FELIX_RAFT_TLS_*` files under peer mTLS): replacing the store is a
//! cluster-admin act, and that token is the cluster-admin credential. The
//! ceremony that wraps these — freeze, export, import, verify, retire — is
//! documented in `docs/metadata-raft-design.md#migration-from-postgres`.
//!
//! The exported file doubles as the disaster-recovery artifact: restoring
//! beyond quorum loss is a fresh group plus `import --overwrite`.
use anyhow::{Context, Result, bail};

use crate::store::raft::command::{MetaCommand, MetaResult, decode_result, encode_command};

/// Run one `migrate` subcommand; `args` starts after the word `migrate`.
pub async fn run(args: Vec<String>) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("export-postgres") => {
            let out = args
                .get(1)
                .context("usage: felix-controlplane migrate export-postgres <out.json>")?;
            export_postgres(out).await
        }
        Some("import") => {
            let file = args
                .get(1)
                .context(IMPORT_USAGE)
                .context("missing snapshot file")?;
            let target = args
                .get(2)
                .context(IMPORT_USAGE)
                .context("missing target url")?;
            let overwrite = args.iter().any(|arg| arg == "--overwrite");
            import(file, target, overwrite).await
        }
        _ => bail!(
            "usage: felix-controlplane migrate <export-postgres <out.json> | import <file.json> <http(s)://raft-peer-addr> [--overwrite]>"
        ),
    }
}

const IMPORT_USAGE: &str = "usage: felix-controlplane migrate import <file.json> <http(s)://raft-peer-addr> [--overwrite] \
     (with FELIX_RAFT_CLUSTER_ID and FELIX_RAFT_PEER_TOKEN set)";

/// Read everything from Postgres — through the same store traits the API
/// serves from — and write the state machine's snapshot format.
///
/// Run this only against a frozen database (the ceremony's first step): the
/// reads span many queries, and consistency is the freeze's job.
async fn export_postgres(out: &str) -> Result<()> {
    let url = std::env::var("FELIX_CONTROLPLANE_POSTGRES_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .context("set FELIX_CONTROLPLANE_POSTGRES_URL (or DATABASE_URL) to the source database")?;
    let store = crate::store::postgres::PostgresStore::connect(
        &crate::config::PostgresConfig {
            url,
            ..Default::default()
        },
        crate::store::StoreConfig {
            changes_limit: crate::config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: Some(crate::config::DEFAULT_CHANGE_RETENTION_MAX_ROWS),
        },
    )
    .await
    .context("connect to the source database")?;

    let state = crate::store::export::export_state_from(&store)
        .await
        .map_err(|err| anyhow::anyhow!("{err}"))
        .context("export state")?;
    let summary = state.summary();
    std::fs::write(
        out,
        serde_json::to_vec_pretty(&state).context("serialize state")?,
    )
    .with_context(|| format!("write {out}"))?;
    println!("exported {summary} to {out}");
    Ok(())
}

/// Propose the snapshot to a running group as one `ImportState` command.
///
/// Any member's peer address works — the propose route forwards to the leader —
/// and the group refuses a non-empty store unless `--overwrite`, so pointing
/// this at the wrong cluster is an error message, not a catastrophe.
async fn import(file: &str, target: &str, overwrite: bool) -> Result<()> {
    let bytes = std::fs::read(file).with_context(|| format!("read {file}"))?;
    let state: crate::store::export::ExportedState =
        serde_json::from_slice(&bytes).context("parse exported state")?;
    let summary = state.summary();

    let command = encode_command(&MetaCommand::ImportState {
        state: Box::new(state),
        overwrite,
    });
    let security = crate::config::peer_security_from_env()?;
    anyhow::ensure!(
        !security.cluster_id.is_empty(),
        "set FELIX_RAFT_CLUSTER_ID to the target group's cluster id"
    );
    anyhow::ensure!(
        security.token.is_some() || crate::config::insecure_peers_from_env(),
        "set FELIX_RAFT_PEER_TOKEN to the target group's peer token"
    );
    let url = format!("{}/internal/raft/propose", target.trim_end_matches('/'));
    let response = security
        .client(std::time::Duration::from_secs(60))?
        .post(&url)
        .body(command)
        .send()
        .await
        .with_context(|| format!("propose to {url}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let detail = response.text().await.unwrap_or_default();
        bail!("the group refused the proposal: {status} {detail}");
    }
    let result: MetaResult = decode_result(&response.bytes().await.context("read response")?)
        .map_err(|err| anyhow::anyhow!("{err}"))?;
    match result {
        Ok(_) => {
            println!("imported {summary} into {target}");
            Ok(())
        }
        Err(err) => bail!("the state machine refused the import: {err}"),
    }
}
