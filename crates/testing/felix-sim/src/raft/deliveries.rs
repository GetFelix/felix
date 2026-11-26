//! What live subscribers were handed, judged against the final logs and
//! against each other. A delivered record is committed, so it is in the log
//! for good at its offset, and no two subscribers are handed different
//! records at one offset. A blank or membership entry takes an offset no
//! read returns, so a delivery there is a finding too.

use std::collections::BTreeMap;

use felix_cluster::history::model::Element;

use super::{Delivery, name};

/// Every delivery that is not the record the final log holds at its offset,
/// and every offset of a list two different values were delivered at.
///
/// A list with no final read is judged only on reuse. Offsets past the final
/// read are too: a batch whose publisher timed out can commit, and be
/// delivered, after the read was taken.
pub(super) fn judge(
    deliveries: &[Delivery],
    final_reads: &BTreeMap<String, Vec<Element>>,
) -> Vec<String> {
    let mut findings = Vec::new();
    let mut first: BTreeMap<(&str, u64), &Delivery> = BTreeMap::new();
    for delivery in deliveries {
        let seen = *first
            .entry((&delivery.list, delivery.offset))
            .or_insert(delivery);
        if seen.value != delivery.value {
            findings.push(format!(
                "{}: offset {} was delivered as value {} by {} and as value {} by {}",
                delivery.list,
                delivery.offset,
                seen.value,
                name(seen.member),
                delivery.value,
                name(delivery.member)
            ));
        }
        let Some(log) = final_reads.get(&delivery.list) else {
            continue;
        };
        if log.last().is_none_or(|last| delivery.offset > last.offset) {
            continue;
        }
        let held = log.binary_search_by_key(&delivery.offset, |element| element.offset);
        let held = held.ok().map(|at| log[at].value);
        if held != Some(delivery.value) {
            let held = held.map_or("no record".to_string(), |value| format!("value {value}"));
            findings.push(format!(
                "{}: {} delivered value {} at offset {}, where the log holds {held}",
                delivery.list,
                name(delivery.member),
                delivery.value,
                delivery.offset
            ));
        }
    }
    findings
}

#[cfg(test)]
mod tests;
