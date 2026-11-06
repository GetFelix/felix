use super::*;
use crate::jitter::sim;

fn on_a_thread<T: Send + 'static>(seed: Option<u64>, f: fn() -> T) -> T {
    std::thread::spawn(move || {
        if let Some(seed) = seed {
            sim::seed(seed);
        }
        f()
    })
    .join()
    .unwrap()
}

fn uuids() -> Vec<Uuid> {
    (0..4).map(|_| uuid_v4()).collect()
}

#[test]
fn a_seeded_thread_makes_the_same_uuids_every_time() {
    assert_eq!(on_a_thread(Some(3), uuids), on_a_thread(Some(3), uuids));
    assert_ne!(on_a_thread(Some(3), uuids), on_a_thread(Some(4), uuids));
}

#[test]
fn a_seeded_uuid_is_still_a_v4() {
    let id = on_a_thread(Some(3), uuid_v4);
    assert_eq!(id.get_version_num(), 4);
}

#[test]
fn an_unseeded_thread_makes_fresh_uuids() {
    assert_ne!(on_a_thread(None, uuids), on_a_thread(None, uuids));
}

#[cfg(feature = "hasher")]
mod hasher {
    use super::*;

    fn hashes() -> Vec<u64> {
        (0..4).map(|_| hash_state().hash_one(42u64)).collect()
    }

    #[test]
    fn a_seeded_thread_hashes_the_same_way_every_time() {
        assert_eq!(on_a_thread(Some(3), hashes), on_a_thread(Some(3), hashes));
        assert_ne!(on_a_thread(Some(3), hashes), on_a_thread(Some(4), hashes));
    }

    fn map_order() -> Vec<u64> {
        let map: HashMap<u64, ()> = (0..64).map(|key| (key, ())).collect();
        map.into_keys().collect()
    }

    #[test]
    fn a_collected_map_iterates_the_same_way_on_a_seeded_thread() {
        assert_eq!(
            on_a_thread(Some(3), map_order),
            on_a_thread(Some(3), map_order)
        );
        assert_ne!(on_a_thread(None, map_order), on_a_thread(None, map_order));
    }

    /// A map built once per process, such as a static, must not move the
    /// jitter sequence, or only the first run in a process would differ.
    #[test]
    fn building_a_map_draws_no_jitter() {
        let plain = || crate::jitter::up_to(u64::MAX);
        let after_a_map = || {
            let _map: HashMap<u64, ()> = HashMap::default();
            crate::jitter::up_to(u64::MAX)
        };
        assert_eq!(
            on_a_thread(Some(3), plain),
            on_a_thread(Some(3), after_a_map)
        );
    }

    #[test]
    fn an_unseeded_thread_hashes_differently_each_time() {
        assert_ne!(on_a_thread(None, hashes), on_a_thread(None, hashes));
    }
}
