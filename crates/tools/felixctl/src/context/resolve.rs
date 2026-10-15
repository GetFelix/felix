//! Turning a profile, the environment and flags into one set of settings.
//!
//! Each setting is taken from the first of: a flag, its environment variable,
//! the chosen context. A token and its file count as one setting, so a
//! `--token-file` flag beats a token saved in the context.

use std::path::PathBuf;

use super::{ConfigFile, Profile};
use crate::cli::ConnectionFlags;
use crate::error::{Exit, MarkExit, fail};

pub(crate) const ENV_CONFIG: &str = "FELIX_CLI_CONFIG";
const ENV_CONTEXT: &str = "FELIX_CONTEXT";
const ENV_BROKERS: &str = "FELIX_BROKERS";
const ENV_CONTROLPLANE_URL: &str = "FELIX_CONTROLPLANE_URL";
const ENV_TENANT: &str = "FELIX_AUTH_TENANT";
const ENV_NAMESPACE: &str = "FELIX_NAMESPACE";
const ENV_TOKEN: &str = "FELIX_AUTH_TOKEN";
const ENV_TOKEN_FILE: &str = "FELIX_AUTH_TOKEN_FILE";
const ENV_CONTROLPLANE_TOKEN: &str = "FELIX_CONTROLPLANE_TOKEN";
const ENV_CONTROLPLANE_TOKEN_FILE: &str = "FELIX_CONTROLPLANE_TOKEN_FILE";
const ENV_CA_FILE: &str = "FELIX_CA_FILE";
const ENV_CLIENT_CERT_FILE: &str = "FELIX_CLIENT_CERT_FILE";
const ENV_CLIENT_KEY_FILE: &str = "FELIX_CLIENT_KEY_FILE";
const ENV_CONTROLPLANE_CA: &str = "FELIX_CONTROLPLANE_CA";
const ENV_SERVER_NAME: &str = "FELIX_SERVER_NAME";

const DEFAULT_NAMESPACE: &str = "default";

/// Everything a command needs to connect, already layered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Settings {
    /// The context the profile layer came from, if any.
    pub(crate) context: Option<String>,
    pub(crate) brokers: Vec<String>,
    pub(crate) controlplane_url: Option<String>,
    pub(crate) tenant: Option<String>,
    pub(crate) namespace: String,
    pub(crate) token: Option<Secret>,
    pub(crate) controlplane_token: Option<Secret>,
    pub(crate) ca_file: Option<PathBuf>,
    pub(crate) client_cert_file: Option<PathBuf>,
    pub(crate) client_key_file: Option<PathBuf>,
    pub(crate) controlplane_ca_file: Option<PathBuf>,
    pub(crate) server_name: Option<String>,
    pub(crate) alpn: bool,
}

impl Settings {
    pub(crate) fn brokers(&self) -> anyhow::Result<&[String]> {
        if self.brokers.is_empty() {
            return Err(missing("broker addresses", "--brokers", ENV_BROKERS));
        }
        Ok(&self.brokers)
    }

    pub(crate) fn tenant(&self) -> anyhow::Result<&str> {
        self.tenant
            .as_deref()
            .ok_or_else(|| missing("a tenant", "--tenant", ENV_TENANT))
    }

    pub(crate) fn token(&self) -> anyhow::Result<String> {
        self.token
            .as_ref()
            .ok_or_else(|| missing("a broker token", "--token or --token-file", ENV_TOKEN))?
            .read()
    }

    pub(crate) fn controlplane_url(&self) -> anyhow::Result<&str> {
        self.controlplane_url.as_deref().ok_or_else(|| {
            missing(
                "a control-plane URL",
                "--controlplane-url",
                ENV_CONTROLPLANE_URL,
            )
        })
    }

    /// The control-plane token, if one is configured. Requests go without
    /// one otherwise, which a control plane with auth turned off accepts.
    pub(crate) fn controlplane_token(&self) -> anyhow::Result<Option<String>> {
        self.controlplane_token
            .as_ref()
            .map(Secret::read)
            .transpose()
    }
}

/// A token given inline or as a file to read when it is needed.
#[derive(Clone, PartialEq, Eq)]
pub(crate) enum Secret {
    Inline(String),
    File(PathBuf),
}

impl Secret {
    pub(crate) fn read(&self) -> anyhow::Result<String> {
        match self {
            Secret::Inline(token) => Ok(token.clone()),
            Secret::File(path) => std::fs::read_to_string(path)
                .map(|token| token.trim().to_string())
                .mark(Exit::Usage, format!("read token file {}", path.display())),
        }
    }

    /// The highest layer that set either the token or its file. Within a
    /// layer the inline token wins.
    fn pick(layers: [(Option<String>, Option<PathBuf>); 3]) -> Option<Self> {
        layers.into_iter().find_map(|(inline, file)| {
            inline
                .map(Secret::Inline)
                .or_else(|| file.map(Secret::File))
        })
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Secret::Inline(_) => f.write_str("Inline(<redacted>)"),
            Secret::File(path) => f.debug_tuple("File").field(path).finish(),
        }
    }
}

/// Layer `flags` over the environment (`env`) over the context they name.
///
/// The context is `--context`, then `FELIX_CONTEXT`, then the file's
/// `current`. Naming one that does not exist is an error; having none at all
/// is not.
pub(crate) fn resolve(
    flags: &ConnectionFlags,
    env: &dyn Fn(&str) -> Option<String>,
    config: &ConfigFile,
) -> anyhow::Result<Settings> {
    let env = |name: &str| env(name).filter(|value| !value.trim().is_empty());
    let named = flags
        .context
        .clone()
        .or_else(|| env(ENV_CONTEXT))
        .or_else(|| config.current.clone());
    let profile = match &named {
        Some(name) => config
            .contexts
            .get(name)
            .cloned()
            .ok_or_else(|| fail(Exit::NotFound, format!("no context named {name:?}")))?,
        None => Profile::default(),
    };
    let path = |name: &str| env(name).map(PathBuf::from);

    let brokers = match (&flags.brokers, env(ENV_BROKERS)) {
        (Some(brokers), _) => brokers.clone(),
        (None, Some(list)) => split_list(&list),
        (None, None) => profile.brokers.clone(),
    };

    Ok(Settings {
        context: named,
        brokers,
        controlplane_url: flags
            .controlplane_url
            .clone()
            .or_else(|| env(ENV_CONTROLPLANE_URL))
            .or(profile.controlplane_url),
        tenant: flags
            .tenant
            .clone()
            .or_else(|| env(ENV_TENANT))
            .or(profile.tenant),
        namespace: flags
            .namespace
            .clone()
            .or_else(|| env(ENV_NAMESPACE))
            .or(profile.namespace)
            .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string()),
        token: Secret::pick([
            (flags.token.clone(), flags.token_file.clone()),
            (env(ENV_TOKEN), path(ENV_TOKEN_FILE)),
            (profile.token, profile.token_file),
        ]),
        controlplane_token: Secret::pick([
            (
                flags.controlplane_token.clone(),
                flags.controlplane_token_file.clone(),
            ),
            (
                env(ENV_CONTROLPLANE_TOKEN),
                path(ENV_CONTROLPLANE_TOKEN_FILE),
            ),
            (profile.controlplane_token, profile.controlplane_token_file),
        ]),
        ca_file: flags
            .ca_file
            .clone()
            .or_else(|| path(ENV_CA_FILE))
            .or(profile.ca_file),
        client_cert_file: flags
            .client_cert_file
            .clone()
            .or_else(|| path(ENV_CLIENT_CERT_FILE))
            .or(profile.client_cert_file),
        client_key_file: flags
            .client_key_file
            .clone()
            .or_else(|| path(ENV_CLIENT_KEY_FILE))
            .or(profile.client_key_file),
        controlplane_ca_file: flags
            .controlplane_ca_file
            .clone()
            .or_else(|| path(ENV_CONTROLPLANE_CA))
            .or(profile.controlplane_ca_file),
        server_name: flags
            .server_name
            .clone()
            .or_else(|| env(ENV_SERVER_NAME))
            .or(profile.server_name),
        alpn: flags.alpn || profile.alpn,
    })
}

fn split_list(list: &str) -> Vec<String> {
    list.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

fn missing(what: &str, flag: &str, env: &str) -> anyhow::Error {
    fail(
        Exit::Usage,
        format!("no {what}: pass {flag}, set {env}, or add it to a context"),
    )
}
