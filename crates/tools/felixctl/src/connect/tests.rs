use std::path::PathBuf;

use super::*;
use crate::cli::ConnectionFlags;
use crate::context::{ConfigFile, resolve};
use crate::error::exit_for;

fn settings(brokers: &[&str]) -> Settings {
    let flags = ConnectionFlags {
        brokers: Some(brokers.iter().map(|b| b.to_string()).collect()),
        tenant: Some("t1".into()),
        token: Some("token".into()),
        ..ConnectionFlags::default()
    };
    resolve(&flags, &|_| None, &ConfigFile::default()).expect("resolve")
}

fn config_error(settings: &Settings) -> anyhow::Error {
    match client_config(settings) {
        Ok(_) => panic!("the configuration was accepted"),
        Err(err) => err,
    }
}

#[test]
fn the_server_name_is_the_first_brokers_host_name() {
    assert_eq!(
        server_name(&settings(&["broker-1.example:5000"])),
        "broker-1.example"
    );
}

#[test]
fn an_ip_address_gets_the_generated_certificates_name() {
    assert_eq!(server_name(&settings(&["127.0.0.1:5000"])), "localhost");
    assert_eq!(server_name(&settings(&["[::1]:5000"])), "localhost");
    assert_eq!(server_name(&settings(&[])), "localhost");
}

#[test]
fn an_explicit_server_name_wins() {
    let mut s = settings(&["broker-1.example:5000"]);
    s.server_name = Some("felixctl.internal".into());
    assert_eq!(server_name(&s), "felixctl.internal");
}

#[tokio::test]
async fn a_broker_address_needs_a_port() {
    let err = addresses(&["localhost".to_string()]).await.unwrap_err();
    assert_eq!(exit_for(&err), Exit::Usage);
    let addrs = addresses(&["127.0.0.1:5000".to_string()])
        .await
        .expect("resolve");
    assert_eq!(addrs, ["127.0.0.1:5000".parse().unwrap()]);
}

#[test]
fn a_client_config_carries_the_tenant_and_token() {
    let config = client_config(&settings(&["127.0.0.1:5000"])).expect("config");
    assert_eq!(config.auth_tenant_id.as_deref(), Some("t1"));
    assert_eq!(config.auth_token.as_deref(), Some("token"));
}

#[test]
fn a_client_config_without_a_tenant_is_a_usage_error() {
    let mut s = settings(&["127.0.0.1:5000"]);
    s.tenant = None;
    let err = config_error(&s);
    assert_eq!(exit_for(&err), Exit::Usage);
}

#[test]
fn a_client_certificate_needs_its_key() {
    let mut s = settings(&["127.0.0.1:5000"]);
    s.client_cert_file = Some(PathBuf::from("/cert.pem"));
    let err = config_error(&s);
    assert_eq!(exit_for(&err), Exit::Usage);
    assert!(err.to_string().contains("--client-key-file"), "{err}");
}

#[test]
fn a_ca_file_without_certificates_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("empty.pem");
    std::fs::write(&path, "").expect("write");
    let mut s = settings(&["127.0.0.1:5000"]);
    s.ca_file = Some(path);
    let err = config_error(&s);
    assert_eq!(exit_for(&err), Exit::Usage);
}
