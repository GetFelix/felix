//! The config file keeps the meaning it had before the YAML parser changed.

use super::ClientConfigOverride;

/// The file reads the same way it did before the YAML parser changed.
#[test]
fn config_yaml_keeps_its_meaning() {
    let parse = |text: &str| {
        ClientConfigOverride::parse(text).unwrap_or_else(|err| panic!("{text:?}: {err}"))
    };
    assert_eq!(parse("bench_embed_ts: true").bench_embed_ts, Some(true));
    assert_eq!(parse("bench_embed_ts: False").bench_embed_ts, Some(false));
    for text in [
        "bench_embed_ts: yes",
        "bench_embed_ts: off",
        "bench_embed_ts: 1",
    ] {
        assert!(ClientConfigOverride::parse(text).is_err(), "{text}");
    }
    assert_eq!(parse("publish_conn_pool: 0x10").publish_conn_pool, Some(16));
    for text in [
        "publish_conn_pool: 0777",
        "publish_conn_pool: 1e3",
        "publish_conn_pool: -1",
    ] {
        assert!(ClientConfigOverride::parse(text).is_err(), "{text}");
    }
    assert_eq!(parse("auth_token: yes").auth_token.as_deref(), Some("yes"));
    assert_eq!(parse("publish_conn_pool: ~").publish_conn_pool, None);
    assert_eq!(parse("").publish_conn_pool, None);
    // No `deny_unknown_fields` here: an unknown key, and `<<`, which is not
    // a merge, are both ignored.
    assert_eq!(parse("not_a_setting: 1").publish_conn_pool, None);
    assert_eq!(parse("<<: {publish_conn_pool: 3}").publish_conn_pool, None);
    assert!(ClientConfigOverride::parse("publish_conn_pool: 1\npublish_conn_pool: 2").is_err());
}

/// The file holds the auth token, so a parse error must not quote it.
#[test]
fn config_yaml_error_does_not_quote_the_token() {
    let err = ClientConfigOverride::parse("auth_token: s3cret-token\npublish_conn_pool: [1]\n")
        .expect_err("wrong type")
        .to_string();
    assert!(!err.contains("s3cret-token"), "{err}");
}

/// The client's copy of the services' options has to stay the documented set.
#[test]
fn config_yaml_options_match_the_services() {
    let options = super::options();
    assert!(options.strict_booleans);
    assert!(matches!(
        options.merge_keys,
        serde_saphyr::options::MergeKeyPolicy::AsOrdinary
    ));
    assert!(matches!(
        options.duplicate_keys,
        serde_saphyr::options::DuplicateKeyPolicy::Error
    ));
    assert!(!options.with_snippet);
    assert!(options.reject_unsupported_tags);
    assert!(!options.legacy_octal_numbers);
    assert!(ClientConfigOverride::parse("auth_token: !custom x").is_err());
    assert!(options.ignore_binary_tag_for_string);
    assert_eq!(
        ClientConfigOverride::parse("auth_token: !!binary aGk=")
            .expect("parse")
            .auth_token
            .as_deref(),
        Some("aGk=")
    );
}
