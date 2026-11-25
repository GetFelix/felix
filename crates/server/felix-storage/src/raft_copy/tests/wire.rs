//! The report as it crosses the wire between a broker and `felixctl`.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use super::*;
use crate::disk_log::CarriedFloor;
use crate::raft_log::LogPoint;

fn copy(path: &str, rotated_by: Option<RotatedBy>, floor: Option<CarriedFloor>) -> CopyRead {
    CopyRead {
        path: PathBuf::from(path),
        copy: Some(7),
        epoch: Some(EpochFile {
            incarnation: 3,
            bases: vec![
                EpochBase {
                    base_last: None,
                    first_index: 1,
                },
                EpochBase {
                    base_last: Some(EpochLogId {
                        index: 40,
                        epoch: 1,
                        term: 2,
                    }),
                    first_index: 1_000,
                },
            ],
        }),
        last: Some(LogPoint {
            term: 4,
            index: 1_200,
        }),
        purged: Some(900),
        committed: Some(1_150),
        vote: Some(4),
        reserved: Some(5_000),
        floor,
        rotated_by,
        terms: BTreeMap::from([(40, Some(2)), (1_200, Some(4)), (2_000, None)]),
    }
}

fn report() -> BrokerReport<Arc<str>> {
    let floor = CarriedFloor {
        offset: 950,
        floor: 920,
        headers: Bytes::from_static(b"trim-headers"),
    };
    BrokerReport {
        leads: true,
        names: BTreeSet::from([Arc::from("broker-a"), Arc::from("broker-b")]),
        memberships: Some(vec![BTreeSet::from([
            (Arc::from("broker-a"), 7),
            (Arc::from("broker-b"), 9),
        ])]),
        copies: BrokerCopies {
            current: copy("/data/current", None, Some(floor)),
            kept: vec![
                copy("/data/kept-1", Some(RotatedBy::PowerCut { to: 8 }), None),
                copy("/data/kept-2", Some(RotatedBy::Restore), None),
            ],
        },
    }
}

/// **A report survives the wire field for field**: the epoch bases, floor
/// with its headers, both rotation causes, and probed terms (absent entries
/// included) all come back as they were read.
#[test]
fn a_report_round_trips_through_json() {
    let sent = report();
    let wire = WireReport::from(&sent);
    assert_eq!(wire.version, REPORT_VERSION);
    let json = serde_json::to_string(&wire).expect("encode");
    let decoded: WireReport = serde_json::from_str(&json).expect("decode");
    assert_eq!(decoded, wire);

    let received = BrokerReport::<String>::try_from(decoded).expect("same version");
    assert!(received.leads);
    assert_eq!(
        received.names,
        BTreeSet::from(["broker-a".to_string(), "broker-b".to_string()])
    );
    assert_eq!(
        received.memberships,
        Some(vec![BTreeSet::from([
            ("broker-a".to_string(), 7),
            ("broker-b".to_string(), 9),
        ])])
    );
    assert_eq!(received.copies, sent.copies);
}

/// **The rotation cause is spelled in snake case on the wire**, so a reader
/// in another build or language sees `power_cut` and `restore`.
#[test]
fn rotation_causes_have_stable_names() {
    let wire = WireReport::from(&report());
    let json = serde_json::to_value(&wire).expect("encode");
    assert_eq!(
        json["kept"][0]["rotated_by"],
        serde_json::json!({ "power_cut": { "to": 8 } })
    );
    assert_eq!(json["kept"][1]["rotated_by"], serde_json::json!("restore"));
    assert_eq!(json["current"]["rotated_by"], serde_json::Value::Null);
}

/// **A report of another version is refused, naming both versions**, rather
/// than read as this layout.
#[test]
fn a_report_of_another_version_is_refused() {
    let mut wire = WireReport::from(&report());
    wire.version = REPORT_VERSION + 1;
    let err = BrokerReport::<String>::try_from(wire).expect_err("other version");
    assert_eq!(err, VersionMismatch(REPORT_VERSION + 1));
    assert_eq!(
        err.to_string(),
        format!(
            "report version {} is not {REPORT_VERSION}",
            REPORT_VERSION + 1
        )
    );
}

/// **A report from a build that predates memberships, kept copies and probed
/// terms still decodes**, with those read as absent.
#[test]
fn optional_report_fields_default_when_missing() {
    let json = serde_json::json!({
        "version": REPORT_VERSION,
        "leads": false,
        "names": ["broker-a"],
        "current": {
            "path": "/data/current",
            "copy": null,
            "epoch": null,
            "last": null,
            "purged": null,
            "committed": null,
            "vote": null,
            "reserved": null,
            "floor": null,
            "rotated_by": null
        }
    });
    let wire: WireReport = serde_json::from_value(json).expect("decode");
    assert_eq!(wire.memberships, None);
    assert!(wire.kept.is_empty());
    assert!(wire.current.terms.is_empty());
    let report = BrokerReport::<String>::try_from(wire).expect("report");
    assert_eq!(report.copies.current.path, PathBuf::from("/data/current"));
    assert_eq!(report.copies.current.epoch, None);
}
