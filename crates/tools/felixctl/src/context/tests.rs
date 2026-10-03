use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::resolve::Secret;
use super::*;
use crate::error::exit_for;

fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |name| vars.get(name).cloned()
}

fn config_with(name: &str, profile: Profile) -> ConfigFile {
    ConfigFile {
        current: Some(name.to_string()),
        contexts: [(name.to_string(), profile)].into_iter().collect(),
    }
}

fn full_profile() -> Profile {
    Profile {
        brokers: vec!["profile:5000".to_string()],
        controlplane_url: Some("http://profile".to_string()),
        tenant: Some("profile-tenant".to_string()),
        namespace: Some("profile-ns".to_string()),
        token: Some("profile-token".to_string()),
        token_file: None,
        controlplane_token: Some("profile-cp".to_string()),
        controlplane_token_file: None,
        ca_file: Some(PathBuf::from("/profile/ca.pem")),
        client_cert_file: Some(PathBuf::from("/profile/cert.pem")),
        client_key_file: Some(PathBuf::from("/profile/key.pem")),
        controlplane_ca_file: Some(PathBuf::from("/profile/cp-ca.pem")),
        server_name: Some("profile.example".to_string()),
        alpn: false,
    }
}

const ALL_ENV: &[(&str, &str)] = &[
    ("FELIX_BROKERS", "env1:5000, env2:5000"),
    ("FELIX_CONTROLPLANE_URL", "http://env"),
    ("FELIX_AUTH_TENANT", "env-tenant"),
    ("FELIX_NAMESPACE", "env-ns"),
    ("FELIX_AUTH_TOKEN", "env-token"),
    ("FELIX_CONTROLPLANE_TOKEN", "env-cp"),
    ("FELIX_CA_FILE", "/env/ca.pem"),
    ("FELIX_CLIENT_CERT_FILE", "/env/cert.pem"),
    ("FELIX_CLIENT_KEY_FILE", "/env/key.pem"),
    ("FELIX_CONTROLPLANE_CA", "/env/cp-ca.pem"),
    ("FELIX_SERVER_NAME", "env.example"),
];

fn all_flags() -> ConnectionFlags {
    ConnectionFlags {
        brokers: Some(vec!["flag:5000".to_string()]),
        controlplane_url: Some("http://flag".to_string()),
        tenant: Some("flag-tenant".to_string()),
        namespace: Some("flag-ns".to_string()),
        token: Some("flag-token".to_string()),
        controlplane_token: Some("flag-cp".to_string()),
        ca_file: Some(PathBuf::from("/flag/ca.pem")),
        client_cert_file: Some(PathBuf::from("/flag/cert.pem")),
        client_key_file: Some(PathBuf::from("/flag/key.pem")),
        controlplane_ca_file: Some(PathBuf::from("/flag/cp-ca.pem")),
        server_name: Some("flag.example".to_string()),
        ..ConnectionFlags::default()
    }
}

#[test]
fn the_profile_is_used_when_nothing_overrides_it() {
    let config = config_with("dev", full_profile());
    let settings = resolve(&ConnectionFlags::default(), &env(&[]), &config).expect("resolve");
    assert_eq!(settings.context.as_deref(), Some("dev"));
    assert_eq!(settings.brokers, ["profile:5000"]);
    assert_eq!(settings.controlplane_url.as_deref(), Some("http://profile"));
    assert_eq!(settings.tenant.as_deref(), Some("profile-tenant"));
    assert_eq!(settings.namespace, "profile-ns");
    assert_eq!(settings.token, Some(Secret::Inline("profile-token".into())));
    assert_eq!(settings.ca_file, Some(PathBuf::from("/profile/ca.pem")));
    assert_eq!(settings.server_name.as_deref(), Some("profile.example"));
}

#[test]
fn the_environment_overrides_the_profile() {
    let config = config_with("dev", full_profile());
    let settings = resolve(&ConnectionFlags::default(), &env(ALL_ENV), &config).expect("resolve");
    assert_eq!(settings.brokers, ["env1:5000", "env2:5000"]);
    assert_eq!(settings.controlplane_url.as_deref(), Some("http://env"));
    assert_eq!(settings.tenant.as_deref(), Some("env-tenant"));
    assert_eq!(settings.namespace, "env-ns");
    assert_eq!(settings.token, Some(Secret::Inline("env-token".into())));
    assert_eq!(
        settings.controlplane_token,
        Some(Secret::Inline("env-cp".into()))
    );
    assert_eq!(settings.ca_file, Some(PathBuf::from("/env/ca.pem")));
    assert_eq!(
        settings.client_cert_file,
        Some(PathBuf::from("/env/cert.pem"))
    );
    assert_eq!(
        settings.client_key_file,
        Some(PathBuf::from("/env/key.pem"))
    );
    assert_eq!(
        settings.controlplane_ca_file,
        Some(PathBuf::from("/env/cp-ca.pem"))
    );
    assert_eq!(settings.server_name.as_deref(), Some("env.example"));
}

#[test]
fn flags_override_the_environment_and_the_profile() {
    let config = config_with("dev", full_profile());
    let settings = resolve(&all_flags(), &env(ALL_ENV), &config).expect("resolve");
    assert_eq!(settings.brokers, ["flag:5000"]);
    assert_eq!(settings.controlplane_url.as_deref(), Some("http://flag"));
    assert_eq!(settings.tenant.as_deref(), Some("flag-tenant"));
    assert_eq!(settings.namespace, "flag-ns");
    assert_eq!(settings.token, Some(Secret::Inline("flag-token".into())));
    assert_eq!(
        settings.controlplane_token,
        Some(Secret::Inline("flag-cp".into()))
    );
    assert_eq!(settings.ca_file, Some(PathBuf::from("/flag/ca.pem")));
    assert_eq!(
        settings.client_cert_file,
        Some(PathBuf::from("/flag/cert.pem"))
    );
    assert_eq!(
        settings.client_key_file,
        Some(PathBuf::from("/flag/key.pem"))
    );
    assert_eq!(
        settings.controlplane_ca_file,
        Some(PathBuf::from("/flag/cp-ca.pem"))
    );
    assert_eq!(settings.server_name.as_deref(), Some("flag.example"));
}

#[test]
fn a_token_file_flag_beats_a_token_in_the_environment() {
    let flags = ConnectionFlags {
        token_file: Some(PathBuf::from("/flag/token")),
        ..ConnectionFlags::default()
    };
    let settings = resolve(
        &flags,
        &env(&[("FELIX_AUTH_TOKEN", "env-token")]),
        &ConfigFile::default(),
    )
    .expect("resolve");
    assert_eq!(settings.token, Some(Secret::File("/flag/token".into())));
}

#[test]
fn a_token_file_in_the_environment_beats_a_token_in_the_profile() {
    let config = config_with("dev", full_profile());
    let settings = resolve(
        &ConnectionFlags::default(),
        &env(&[("FELIX_AUTH_TOKEN_FILE", "/env/token")]),
        &config,
    )
    .expect("resolve");
    assert_eq!(settings.token, Some(Secret::File("/env/token".into())));
}

#[test]
fn an_empty_environment_variable_counts_as_unset() {
    let config = config_with("dev", full_profile());
    let settings = resolve(
        &ConnectionFlags::default(),
        &env(&[("FELIX_AUTH_TENANT", ""), ("FELIX_BROKERS", " ")]),
        &config,
    )
    .expect("resolve");
    assert_eq!(settings.tenant.as_deref(), Some("profile-tenant"));
    assert_eq!(settings.brokers, ["profile:5000"]);
}

#[test]
fn the_context_is_chosen_by_flag_then_environment_then_current() {
    let profile = |tenant: &str| Profile {
        tenant: Some(tenant.into()),
        ..Profile::default()
    };
    let mut config = config_with("a", profile("from-a"));
    config.contexts.insert("b".into(), profile("from-b"));
    config.contexts.insert("c".into(), profile("from-c"));
    let tenant = |flags: &ConnectionFlags, vars: &[(&str, &str)]| {
        resolve(flags, &env(vars), &config)
            .expect("resolve")
            .tenant
            .unwrap()
    };
    let flag_c = ConnectionFlags {
        context: Some("c".into()),
        ..ConnectionFlags::default()
    };
    assert_eq!(tenant(&ConnectionFlags::default(), &[]), "from-a");
    assert_eq!(
        tenant(&ConnectionFlags::default(), &[("FELIX_CONTEXT", "b")]),
        "from-b"
    );
    assert_eq!(tenant(&flag_c, &[("FELIX_CONTEXT", "b")]), "from-c");
}

#[test]
fn naming_a_missing_context_is_an_error() {
    let flags = ConnectionFlags {
        context: Some("nope".into()),
        ..ConnectionFlags::default()
    };
    let err = resolve(&flags, &env(&[]), &ConfigFile::default()).unwrap_err();
    assert_eq!(exit_for(&err), crate::error::Exit::NotFound);
}

#[test]
fn no_context_at_all_is_fine_and_the_namespace_defaults() {
    let settings = resolve(
        &ConnectionFlags::default(),
        &env(&[]),
        &ConfigFile::default(),
    )
    .expect("resolve");
    assert_eq!(settings.context, None);
    assert_eq!(settings.namespace, "default");
    let err = settings.brokers().unwrap_err();
    assert_eq!(exit_for(&err), crate::error::Exit::Usage);
    assert!(err.to_string().contains("FELIX_BROKERS"), "{err}");
}

#[test]
fn a_token_file_is_read_and_trimmed_when_needed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("token");
    std::fs::write(&path, "abc.def\n").expect("write");
    assert_eq!(Secret::File(path).read().expect("read"), "abc.def");
    let err = Secret::File(dir.path().join("missing")).read().unwrap_err();
    assert_eq!(exit_for(&err), crate::error::Exit::Usage);
}

#[test]
fn a_debug_print_does_not_show_an_inline_token() {
    let text = format!("{:?}", Secret::Inline("secret-value".into()));
    assert!(!text.contains("secret-value"), "{text}");
}

#[test]
fn the_config_path_is_flag_then_env_then_xdg_then_platform() {
    let flag = Path::new("/flag/config.toml");
    assert_eq!(
        config_path(Some(flag), &env(&[("FELIX_CLI_CONFIG", "/env.toml")])).unwrap(),
        flag
    );
    assert_eq!(
        config_path(
            None,
            &env(&[
                ("FELIX_CLI_CONFIG", "/env.toml"),
                ("XDG_CONFIG_HOME", "/xdg")
            ])
        )
        .unwrap(),
        Path::new("/env.toml")
    );
    assert_eq!(
        config_path(
            None,
            &env(&[("XDG_CONFIG_HOME", "/xdg"), ("HOME", "/home/u")])
        )
        .unwrap(),
        Path::new("/xdg/felixctl/config.toml")
    );
    let platform =
        config_path(None, &env(&[("HOME", "/home/u"), ("APPDATA", "/appdata")])).unwrap();
    if cfg!(target_os = "macos") {
        assert_eq!(
            platform,
            Path::new("/home/u/Library/Application Support/felixctl/config.toml")
        );
    } else if cfg!(windows) {
        assert_eq!(platform, Path::new("/appdata/felixctl/config.toml"));
    } else {
        assert_eq!(platform, Path::new("/home/u/.config/felixctl/config.toml"));
    }
}

#[test]
fn a_missing_config_file_is_an_empty_config() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = ConfigFile::load(&dir.path().join("none.toml")).expect("load");
    assert_eq!(config, ConfigFile::default());
}

#[test]
fn the_config_file_round_trips_and_is_private() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("nested").join("config.toml");
    let config = config_with("dev", full_profile());
    config.save(&path).expect("save");
    assert_eq!(ConfigFile::load(&path).expect("load"), config);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(
            mode & 0o077,
            0,
            "the config is readable by others: {mode:o}"
        );
    }
}

/// An existing config readable by others is made private when saved over,
/// not only one the save creates.
#[cfg(unix)]
#[test]
fn saving_over_a_readable_config_makes_it_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "").expect("write");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");

    let config = config_with("dev", full_profile());
    config.save(&path).expect("save");

    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(
        mode & 0o077,
        0,
        "the config is readable by others: {mode:o}"
    );
    assert_eq!(ConfigFile::load(&path).expect("load"), config);
    let leftovers: Vec<_> = std::fs::read_dir(dir.path())
        .expect("list")
        .map(|entry| entry.expect("entry").file_name())
        .collect();
    assert_eq!(leftovers, vec![std::ffi::OsString::from("config.toml")]);
}

#[test]
fn an_unknown_key_in_the_config_file_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.toml");
    std::fs::write(&path, "[contexts.dev]\nca_flie = \"/ca.pem\"\n").expect("write");
    let err = ConfigFile::load(&path).unwrap_err();
    assert_eq!(exit_for(&err), crate::error::Exit::Usage);
    assert!(format!("{err:#}").contains("ca_flie"), "{err:#}");
}

#[test]
fn context_add_saves_only_the_flags_with_absolute_paths() {
    let flags = ConnectionFlags {
        brokers: Some(vec!["127.0.0.1:5000".into()]),
        tenant: Some("t1".into()),
        token_file: Some(PathBuf::from("token.jwt")),
        ..ConnectionFlags::default()
    };
    let profile = Profile::from_flags(&flags).expect("profile");
    assert_eq!(profile.brokers, ["127.0.0.1:5000"]);
    assert_eq!(profile.tenant.as_deref(), Some("t1"));
    assert!(profile.token_file.unwrap().is_absolute());
    assert_eq!(profile.namespace, None);
}

#[test]
fn context_commands_add_use_and_remove() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.toml");
    let out = Output { json: true };
    let flags = ConnectionFlags {
        tenant: Some("t1".into()),
        ..ConnectionFlags::default()
    };
    let add = |name: &str, make_current: bool, replace: bool| {
        run(
            &ContextCommand::Add {
                name: name.into(),
                make_current,
                replace,
            },
            &flags,
            &path,
            &out,
        )
    };

    add("a", false, false).expect("add a");
    add("b", false, false).expect("add b");
    assert_eq!(
        ConfigFile::load(&path).unwrap().current.as_deref(),
        Some("a"),
        "the first context becomes current"
    );

    let err = add("a", false, false).unwrap_err();
    assert_eq!(exit_for(&err), crate::error::Exit::Usage);
    add("a", true, true).expect("replace a");

    let use_ = |name: &str| {
        run(
            &ContextCommand::Use { name: name.into() },
            &flags,
            &path,
            &out,
        )
    };
    use_("b").expect("use b");
    assert_eq!(
        ConfigFile::load(&path).unwrap().current.as_deref(),
        Some("b")
    );
    let err = use_("zzz").unwrap_err();
    assert_eq!(exit_for(&err), crate::error::Exit::NotFound);

    run(
        &ContextCommand::Rm { name: "b".into() },
        &flags,
        &path,
        &out,
    )
    .expect("rm b");
    let config = ConfigFile::load(&path).unwrap();
    assert_eq!(config.current, None);
    assert!(config.contexts.contains_key("a"));
    assert!(!config.contexts.contains_key("b"));
}

#[test]
fn listing_redacts_inline_tokens() {
    let value = redacted(&full_profile());
    assert_eq!(value["token"], "<redacted>");
    assert_eq!(value["controlplane_token"], "<redacted>");
    assert_eq!(value["tenant"], "profile-tenant");
}
