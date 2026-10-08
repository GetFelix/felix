//! Which container engine the tests drive, and how its containers reach the
//! host.
//!
//! Docker and Podman take the same `run` arguments for everything the tests
//! do; what differs is the binary and the host's name inside a VM-backed
//! container. `CONTAINER_ENGINE` picks one; otherwise it is `docker` if that
//! answers, then `podman`. `task test` exports the same choice.
use std::process::Command;
use std::sync::OnceLock;

/// The container CLI to run: `CONTAINER_ENGINE`, else the first of `docker`
/// and `podman` whose daemon or machine answers, else `docker`, so a missing
/// engine is reported under the name most setups expect.
pub fn engine() -> &'static str {
    static ENGINE: OnceLock<String> = OnceLock::new();
    ENGINE.get_or_init(|| {
        if let Ok(chosen) = std::env::var("CONTAINER_ENGINE")
            && !chosen.is_empty()
        {
            return chosen;
        }
        ["docker", "podman"]
            .into_iter()
            .find(|engine| answers(engine))
            .unwrap_or("docker")
            .to_string()
    })
}

/// The host a container on this machine reaches the host's ports by.
///
/// On Linux a `--network host` container shares loopback. Docker Desktop and
/// `podman machine` run containers in a VM, where loopback is the VM's own,
/// so it goes through the engine's host alias, and the listener has to bind
/// every interface.
pub fn host() -> &'static str {
    if cfg!(target_os = "linux") {
        "127.0.0.1"
    } else if engine().ends_with("podman") {
        "host.containers.internal"
    } else {
        "host.docker.internal"
    }
}

/// `version` talks to the daemon (or the Podman machine), so a CLI that is
/// installed but has nothing to run containers on does not count.
fn answers(engine: &str) -> bool {
    Command::new(engine)
        .arg("version")
        .output()
        .is_ok_and(|out| out.status.success())
}
