//! Cutting the power to every broker at once, and starting them all again.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};

use super::Cluster;
use crate::node::{power_loss_image, spawn_broker, storage_fault_file};

/// How long a broker told to lose power has to build its image and die. The
/// image is a copy of its storage, which in a harness run is small.
const POWER_OFF_WITHIN: Duration = Duration::from_secs(30);

impl Cluster {
    /// Cut the power to every running broker. Each builds the storage a
    /// reboot would find, where writes no flush covered are lost, torn or
    /// zeroed as `seed` decides, and dies; that image then replaces its
    /// storage directory. The brokers stay down until
    /// [`Self::restart_stopped_nodes`].
    ///
    /// The brokers are told one after another and each notices within a few
    /// tens of milliseconds, so they go down close together rather than at one
    /// instant. That is enough: what a broker acknowledged was flushed, and a
    /// flush it had started is in its image.
    ///
    /// Needs a cluster started with
    /// [`ClusterConfig::power_loss`](crate::ClusterConfig::power_loss), brokers
    /// built with debug assertions, and Linux.
    pub async fn power_off(&mut self, seed: u64) -> Result<()> {
        if !cfg!(target_os = "linux") {
            bail!("a simulated power loss needs Linux: the model reads flushes through /proc");
        }
        if !self.config.power_loss {
            bail!("the cluster was not started with ClusterConfig::power_loss");
        }
        let running: Vec<usize> = (0..self.nodes.len())
            .filter(|index| self.nodes[*index].is_running())
            .collect();
        for (offset, index) in running.iter().enumerate() {
            let node = &self.nodes[*index];
            let image = power_loss_image(&node.data_dir);
            remove_dir_if_present(&image)?;
            let fault_file = storage_fault_file(&node.data_dir);
            let mut body = std::fs::read_to_string(&fault_file).unwrap_or_default();
            body.push_str(&format!(
                "power_loss={}\npower_loss_into={}\n",
                seed.wrapping_add(offset as u64),
                image.display()
            ));
            // Renamed into place: the broker polls this file, and a half
            // written image path would send its image somewhere else.
            let staged = fault_file.with_extension("next");
            std::fs::write(&staged, body)
                .and_then(|()| std::fs::rename(&staged, &fault_file))
                .with_context(|| format!("tell {} to lose power", node.node_id))?;
        }

        let deadline = Instant::now() + POWER_OFF_WITHIN;
        let mut up: Vec<usize> = running.clone();
        while !up.is_empty() && Instant::now() < deadline {
            up.retain(|index| {
                let node = &mut self.nodes[*index];
                if node.exited().is_some() {
                    node.take_process();
                    false
                } else {
                    true
                }
            });
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let mut problems = Vec::new();
        for index in &up {
            let node = &mut self.nodes[*index];
            if let Some(mut process) = node.take_process() {
                let _ = process.kill();
                let _ = process.wait();
            }
            problems.push(format!(
                "{} was still up after {POWER_OFF_WITHIN:?} and was killed instead",
                node.node_id
            ));
        }

        for index in running {
            let node = &self.nodes[index];
            clear_power_loss(&storage_fault_file(&node.data_dir))?;
            let image = power_loss_image(&node.data_dir);
            remove_dir_if_present(&image.with_extension("partial"))?;
            if !image.exists() {
                if !up.contains(&index) {
                    problems.push(format!("{} built no power-loss image", node.node_id));
                }
                continue;
            }
            let storage = node.storage_dir();
            std::fs::remove_dir_all(&storage)
                .with_context(|| format!("remove {}", storage.display()))?;
            std::fs::rename(&image, &storage)
                .with_context(|| format!("move {}'s power-loss image into place", node.node_id))?;
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(anyhow!("{}", problems.join("; ")))
        }
    }

    /// Start every stopped broker, all at once, and wait until each is
    /// placeable. Started together because a whole cluster coming back is
    /// what a power loss ends in, and none of them should wait on another.
    pub async fn restart_stopped_nodes(&mut self) -> Result<()> {
        let stopped: Vec<usize> = (0..self.nodes.len())
            .filter(|index| !self.nodes[*index].is_running())
            .collect();
        for index in &stopped {
            let log = self.nodes[*index].data_dir.join("broker.log");
            let _ = std::fs::rename(&log, log.with_extension("log.previous"));
            let control_plane = self
                .control_plane
                .as_ref()
                .ok_or_else(|| anyhow!("control plane is gone"))?;
            let node = spawn_broker(
                &self.binary,
                control_plane,
                &self.config,
                self._root.path(),
                *index,
                self.links.as_ref(),
            )
            .with_context(|| format!("restart {}", self.nodes[*index].node_id))?;
            self.nodes[*index] = node;
        }
        for index in stopped {
            self.await_placeable(index).await?;
        }
        Ok(())
    }
}

/// Drop the `power_loss` directive from a fault file, keeping any other fault
/// in it, so the restarted broker does not act on it again.
fn clear_power_loss(path: &std::path::Path) -> Result<()> {
    let Ok(body) = std::fs::read_to_string(path) else {
        return Ok(());
    };
    let kept: String = body
        .lines()
        .filter(|line| !line.starts_with("power_loss"))
        .map(|line| format!("{line}\n"))
        .collect();
    std::fs::write(path, kept).with_context(|| format!("rewrite {}", path.display()))
}

fn remove_dir_if_present(path: &std::path::Path) -> Result<()> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("remove {}", path.display())),
    }
}
