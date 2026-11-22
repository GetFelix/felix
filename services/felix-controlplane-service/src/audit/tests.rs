use super::*;

fn entry(path: &str) -> AuditEntry {
    AuditEntry {
        at_ms: 1,
        member: "1".to_string(),
        listener: "api".to_string(),
        peer: None,
        method: "POST".to_string(),
        route: path.to_string(),
        path: path.to_string(),
        tenant: None,
        subject: None,
        actor: None,
        cert_sha256: None,
        credential: "none".to_string(),
        status: 204,
        refusal: None,
        suppressed: 0,
    }
}

#[test]
fn the_ring_keeps_the_newest_and_numbers_without_gaps() {
    let mut log = AuditLog::default();
    log.append((0..AUDIT_RETAIN + 5).map(|_| entry("/v1/x")).collect());
    let page = log.page(0, 3);
    assert_eq!(page.oldest_seq, Some(6));
    assert_eq!(page.last_seq, (AUDIT_RETAIN + 5) as u64);
    assert_eq!(
        page.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        [6, 7, 8]
    );
}
