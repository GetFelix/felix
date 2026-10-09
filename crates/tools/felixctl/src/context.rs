//! Named connection profiles, the file they live in, and `felixctl context`.
//!
//! The file is TOML: a `current` name and a `[contexts.<name>]` table per
//! profile. Unknown keys are refused rather than ignored, so a misspelt
//! `ca_flie` is an error instead of a silently unverified connection.
//! [`resolve`] layers flags and environment variables over the chosen
//! profile.

mod resolve;

pub(crate) use resolve::{Settings, resolve};

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::cli::{ConnectionFlags, ContextCommand};
use crate::error::{Exit, MarkExit, fail};
use crate::output::{Output, table};

/// The config file's contents.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigFile {
    /// The context used when none is named.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) current: Option<String>,
    #[serde(default)]
    pub(crate) contexts: BTreeMap<String, Profile>,
}

impl ConfigFile {
    /// Read `path`. A file that does not exist is an empty config.
    pub(crate) fn load(path: &Path) -> anyhow::Result<Self> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(err) => {
                return Err(err).mark(Exit::Usage, format!("read {}", path.display()));
            }
        };
        toml::from_str(&text).mark(Exit::Usage, format!("parse {}", path.display()))
    }

    /// Write to `path`, creating its directory. Owner-only on Unix, since a
    /// context can hold a token, including when the file was there before.
    ///
    /// Written to a temporary file beside it and renamed over it, so a crash
    /// leaves the old config or the new one, never an empty or partial file.
    pub(crate) fn save(&self, path: &Path) -> anyhow::Result<()> {
        let dir = match path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => Path::new("."),
        };
        std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
        let text = toml::to_string_pretty(self).context("encode the config")?;
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let (temp, mut file) =
            create_temp(dir, &name).with_context(|| format!("write {}", path.display()))?;
        let written = (|| {
            std::io::Write::write_all(&mut file, text.as_bytes())?;
            file.sync_all()?;
            std::fs::rename(&temp, path)?;
            #[cfg(unix)]
            std::fs::File::open(dir)?.sync_all()?;
            std::io::Result::Ok(())
        })();
        if written.is_err() {
            let _ = std::fs::remove_file(&temp);
        }
        written.with_context(|| format!("write {}", path.display()))
    }
}

/// Create a temporary file for `name` in `dir` under a name nobody can guess,
/// so another user of a shared directory cannot plant a file or symlink there
/// first.
fn create_temp(dir: &Path, name: &str) -> std::io::Result<(PathBuf, std::fs::File)> {
    use std::hash::BuildHasher as _;
    let mut attempt = 0u32;
    loop {
        // `RandomState` is seeded from the OS, which is all the randomness
        // std offers without another dependency.
        let random = std::hash::RandomState::new().hash_one((
            std::process::id(),
            std::time::SystemTime::now(),
            attempt,
        ));
        let temp = dir.join(format!(".{name}.{random:016x}.tmp"));
        match open_new_private(&temp) {
            Ok(file) => return Ok((temp, file)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists && attempt < 16 => {
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Create `path`, failing if anything is already there, a symlink included.
/// Owner-only on Unix, since a context can hold a token.
fn open_new_private(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path)
}

/// One named profile. Every field is optional; what is missing can come from
/// the environment or a flag.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Profile {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) brokers: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) controlplane_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) tenant: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) token_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) controlplane_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) controlplane_token_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) ca_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) client_cert_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) client_key_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) controlplane_ca_file: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) server_name: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) alpn: bool,
}

impl Profile {
    /// The profile `felixctl context add` saves: exactly the flags given.
    /// Relative file paths are made absolute, so the context works from any
    /// directory.
    pub(crate) fn from_flags(flags: &ConnectionFlags) -> anyhow::Result<Self> {
        let absolute = |path: &Option<PathBuf>| -> anyhow::Result<Option<PathBuf>> {
            path.as_ref()
                .map(|path| std::path::absolute(path).with_context(|| path.display().to_string()))
                .transpose()
        };
        Ok(Self {
            brokers: flags.brokers.clone().unwrap_or_default(),
            controlplane_url: flags.controlplane_url.clone(),
            tenant: flags.tenant.clone(),
            namespace: flags.namespace.clone(),
            token: flags.token.clone(),
            token_file: absolute(&flags.token_file)?,
            controlplane_token: flags.controlplane_token.clone(),
            controlplane_token_file: absolute(&flags.controlplane_token_file)?,
            ca_file: absolute(&flags.ca_file)?,
            client_cert_file: absolute(&flags.client_cert_file)?,
            client_key_file: absolute(&flags.client_key_file)?,
            controlplane_ca_file: absolute(&flags.controlplane_ca_file)?,
            server_name: flags.server_name.clone(),
            alpn: flags.alpn,
        })
    }
}

/// Where the config file is: `--config`, then `FELIX_CLI_CONFIG`, then
/// `felixctl/config.toml` under `XDG_CONFIG_HOME`, then the platform default.
pub(crate) fn config_path(
    flag: Option<&Path>,
    env: &dyn Fn(&str) -> Option<String>,
) -> anyhow::Result<PathBuf> {
    if let Some(path) = flag {
        return Ok(path.to_path_buf());
    }
    if let Some(path) = env(resolve::ENV_CONFIG) {
        return Ok(PathBuf::from(path));
    }
    if let Some(dir) = env("XDG_CONFIG_HOME").filter(|dir| !dir.is_empty()) {
        return Ok(PathBuf::from(dir).join("felixctl").join("config.toml"));
    }
    platform_config_dir(env)
        .map(|dir| dir.join("felixctl").join("config.toml"))
        .ok_or_else(|| {
            fail(
                Exit::Usage,
                "no config directory: set HOME, XDG_CONFIG_HOME or FELIX_CLI_CONFIG",
            )
        })
}

/// Run a `felixctl context` command.
pub(crate) fn run(
    command: &ContextCommand,
    flags: &ConnectionFlags,
    path: &Path,
    out: &Output,
) -> anyhow::Result<()> {
    let mut config = ConfigFile::load(path)?;
    match command {
        ContextCommand::Add {
            name,
            make_current,
            replace,
        } => {
            if config.contexts.contains_key(name) && !replace {
                return Err(fail(
                    Exit::Usage,
                    format!("context {name:?} exists; pass --replace or `felixctl context rm` it"),
                ));
            }
            config
                .contexts
                .insert(name.clone(), Profile::from_flags(flags)?);
            if *make_current || config.current.is_none() {
                config.current = Some(name.clone());
            }
            config.save(path)?;
            out.done(
                &format!("saved context {name} in {}", path.display()),
                serde_json::json!({ "context": name, "path": path, "current": config.current }),
            )
        }
        ContextCommand::Use { name } => {
            if !config.contexts.contains_key(name) {
                return Err(fail(Exit::NotFound, format!("no context named {name:?}")));
            }
            config.current = Some(name.clone());
            config.save(path)?;
            out.done(
                &format!("using context {name}"),
                serde_json::json!({ "current": name }),
            )
        }
        ContextCommand::Rm { name } => {
            if config.contexts.remove(name).is_none() {
                return Err(fail(Exit::NotFound, format!("no context named {name:?}")));
            }
            if config.current.as_deref() == Some(name.as_str()) {
                config.current = None;
            }
            config.save(path)?;
            out.done(
                &format!("deleted context {name}"),
                serde_json::json!({ "deleted": name }),
            )
        }
        ContextCommand::Ls => {
            if out.json {
                let contexts: Vec<serde_json::Value> = config
                    .contexts
                    .iter()
                    .map(|(name, profile)| {
                        let mut value = redacted(profile);
                        value["name"] = name.as_str().into();
                        value["current"] = (config.current.as_deref() == Some(name)).into();
                        value
                    })
                    .collect();
                return out.json_value(&serde_json::json!({ "contexts": contexts }));
            }
            let rows = config
                .contexts
                .iter()
                .map(|(name, profile)| {
                    let marker = if config.current.as_deref() == Some(name) {
                        "*"
                    } else {
                        ""
                    };
                    vec![
                        marker.to_string(),
                        name.clone(),
                        profile.brokers.join(","),
                        profile.tenant.clone().unwrap_or_default(),
                        profile.namespace.clone().unwrap_or_default(),
                        profile.controlplane_url.clone().unwrap_or_default(),
                    ]
                })
                .collect();
            out.text(&table(
                &[
                    "",
                    "NAME",
                    "BROKERS",
                    "TENANT",
                    "NAMESPACE",
                    "CONTROL PLANE",
                ],
                rows,
            ))
        }
    }
}

/// A profile as JSON with inline tokens hidden.
pub(crate) fn redacted(profile: &Profile) -> serde_json::Value {
    let mut value = serde_json::to_value(profile).unwrap_or_default();
    for field in ["token", "controlplane_token"] {
        if value.get(field).is_some() {
            value[field] = "<redacted>".into();
        }
    }
    value
}

fn platform_config_dir(env: &dyn Fn(&str) -> Option<String>) -> Option<PathBuf> {
    if cfg!(windows) {
        return env("APPDATA").map(PathBuf::from);
    }
    let home = PathBuf::from(env("HOME").filter(|home| !home.is_empty())?);
    if cfg!(target_os = "macos") {
        Some(home.join("Library").join("Application Support"))
    } else {
        Some(home.join(".config"))
    }
}

#[cfg(test)]
mod tests;
