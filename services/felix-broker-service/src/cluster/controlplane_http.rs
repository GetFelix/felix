//! The HTTP clients the broker reaches the control plane with.
//!
//! Every control-plane call must trust the same roots, and the clients are
//! built in several places (auth's JWKS fetch, the catalog sync, membership),
//! so the extra CA from `FELIX_CONTROLPLANE_CA` is installed once, process
//! wide, at startup, and every client is built through [`builder`].

use std::sync::RwLock;

use anyhow::{Context, Result};

static EXTRA_ROOTS: RwLock<Vec<reqwest::Certificate>> = RwLock::new(Vec::new());

/// Trust the PEM bundle at `path`, in addition to the public roots, for
/// every control-plane client built after this. `None` trusts the public
/// roots only.
///
/// Fails at startup on an unreadable or empty bundle rather than letting
/// every later call fail verification.
pub(crate) fn trust_ca(path: Option<&str>) -> Result<()> {
    let roots = match path {
        Some(path) => {
            let pem = std::fs::read(path)
                .with_context(|| format!("read FELIX_CONTROLPLANE_CA {path}"))?;
            let roots = reqwest::Certificate::from_pem_bundle(&pem)
                .with_context(|| format!("parse FELIX_CONTROLPLANE_CA {path}"))?;
            anyhow::ensure!(
                !roots.is_empty(),
                "no certificates in FELIX_CONTROLPLANE_CA {path}"
            );
            roots
        }
        None => Vec::new(),
    };
    *EXTRA_ROOTS.write().unwrap_or_else(|e| e.into_inner()) = roots;
    Ok(())
}

/// A client builder that trusts the configured control-plane CA.
pub(crate) fn builder() -> reqwest::ClientBuilder {
    let roots = EXTRA_ROOTS.read().unwrap_or_else(|e| e.into_inner());
    roots
        .iter()
        .cloned()
        .fold(reqwest::Client::builder(), |builder, root| {
            builder.add_root_certificate(root)
        })
}

#[cfg(test)]
mod tests;
