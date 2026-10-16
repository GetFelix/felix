//! Which incarnation of a stream this broker's data belongs to, kept on disk.
//!
//! One small file per stream under `.streams/` in the stream store's root,
//! replaced whole by rename. It is what lets a broker that restarts, or that
//! is told a stream without its incarnation, tell its own data from another
//! incarnation's, and what records a delete that has not finished.

use std::path::{Path, PathBuf};

use crate::disk_log::layout;
use crate::io::{create_dir_all_durable, sync_dir};
use crate::{Result, StorageError};

/// Directory under the stream store's root that holds the markers. Dotted, so
/// it is never taken for a shard directory.
pub(crate) const STREAMS_DIR: &str = ".streams";

const HEADER: &str = "felix-stream-marker 1";

/// What reading a stream's marker found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerRead {
    /// No marker: this broker has never written one for the stream.
    Absent,
    Valid(StreamMarker),
    /// A marker that does not parse. Writes are atomic, so this is damage,
    /// and what it said is unknown.
    Unreadable,
}

/// What this broker holds of one stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamMarker {
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    /// The incarnation last registered here under the name. Its data stays
    /// until `deleted` covers it.
    pub incarnation: u64,
    /// Set before a delete starts moving directories away and cleared once
    /// every store has. A marker left set is a delete to finish.
    pub retiring: bool,
    /// The most shards the stream had here: finishing a delete of
    /// incarnation 0 reads it to find the dead letters the layout before 0.6
    /// kept per group.
    pub shards: Option<u32>,
    /// The control plane said every incarnation of the name up to and
    /// including this one is deleted. Their data goes, and none of them is
    /// registered again.
    pub deleted: Option<u64>,
}

pub(crate) fn path(root: &Path, tenant: &str, namespace: &str, stream: &str) -> PathBuf {
    root.join(STREAMS_DIR)
        .join(layout::stream_dir_name(tenant, namespace, stream))
}

pub(crate) fn read(root: &Path, tenant: &str, namespace: &str, stream: &str) -> Result<MarkerRead> {
    read_path(&path(root, tenant, namespace, stream))
}

fn read_path(path: &Path) -> Result<MarkerRead> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(MarkerRead::Absent),
        Err(err) => return Err(StorageError::Io(err)),
    };
    let decoded = String::from_utf8(bytes).ok().as_deref().and_then(decode);
    Ok(match decoded {
        Some(marker) => MarkerRead::Valid(marker),
        None => {
            tracing::warn!(
                path = %path.display(),
                "unreadable stream marker; keeping every incarnation of the stream on disk",
            );
            MarkerRead::Unreadable
        }
    })
}

/// Replace the stream's marker: written to a sibling, flushed, renamed over
/// it, and the directory flushed, so a crash leaves the old marker or the new
/// one. The flushes go through `crate::io`, which is also what a simulated
/// power loss sees.
pub(crate) fn write(root: &Path, marker: &StreamMarker) -> Result<()> {
    write_file(
        root,
        &path(root, &marker.tenant, &marker.namespace, &marker.stream),
        encode(marker).as_bytes(),
    )
}

/// Name of the file holding deleted floors recorded from catalog tombstones,
/// for every name in one place. Dotted, so it is never read as a marker.
const FLOORS: &str = ".floors";

const FLOORS_HEADER: &str = "felix-stream-floors 1";

/// Deleted floors by (tenant, namespace, stream).
pub(crate) type Floors = std::collections::HashMap<(String, String, String), u64>;

/// The floors file, empty when there is none. A line that does not parse is
/// skipped with a warning: the file is replaced whole and flushed, so that is
/// damage, and a floor only ever refuses a registration.
pub(crate) fn read_floors(root: &Path) -> Result<Floors> {
    let path = root.join(STREAMS_DIR).join(FLOORS);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Floors::new()),
        Err(err) => return Err(StorageError::Io(err)),
    };
    let mut lines = text.lines();
    if lines.next() != Some(FLOORS_HEADER) {
        tracing::warn!(path = %path.display(), "unreadable stream floors file; ignoring it");
        return Ok(Floors::new());
    }
    let mut floors = Floors::new();
    for line in lines {
        let mut parts = line.split(' ');
        let parsed = (|| {
            let deleted = parts.next()?.parse().ok()?;
            let tenant = unhex(parts.next()?)?;
            let namespace = unhex(parts.next()?)?;
            let stream = unhex(parts.next()?)?;
            parts
                .next()
                .is_none()
                .then_some(((tenant, namespace, stream), deleted))
        })();
        match parsed {
            Some((key, deleted)) => {
                let floor = floors.entry(key).or_insert(deleted);
                *floor = (*floor).max(deleted);
            }
            None => tracing::warn!(path = %path.display(), "skipping a damaged stream floor"),
        }
    }
    Ok(floors)
}

/// Replace the floors file with `floors`, durably: one file flush and one
/// directory flush however many names it holds.
pub(crate) fn write_floors(root: &Path, floors: &Floors) -> Result<()> {
    let mut entries: Vec<_> = floors.iter().collect();
    entries.sort();
    let mut text = format!("{FLOORS_HEADER}\n");
    for ((tenant, namespace, stream), deleted) in entries {
        text.push_str(&format!(
            "{deleted} {} {} {}\n",
            hex(tenant),
            hex(namespace),
            hex(stream)
        ));
    }
    write_file(root, &root.join(STREAMS_DIR).join(FLOORS), text.as_bytes())
}

fn write_file(root: &Path, path: &Path, contents: &[u8]) -> Result<()> {
    use std::io::Write;

    let dir = root.join(STREAMS_DIR);
    create_dir_all_durable(&dir).map_err(StorageError::Io)?;
    let temporary = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temporary).map_err(StorageError::Io)?;
        file.write_all(contents).map_err(StorageError::Io)?;
        crate::io::sync_all(&file).map_err(StorageError::Io)?;
    }
    std::fs::rename(&temporary, path).map_err(StorageError::Io)?;
    sync_dir(&dir).map_err(StorageError::Io)
}

/// Name of the file recording that this broker has seen a stream with an
/// incarnation. Dotted, so it is never read as a marker.
const IN_PLAY: &str = ".incarnations";

/// Record, durably, that streams here carry incarnations.
pub(crate) fn record_in_play(root: &Path) -> Result<()> {
    let path = root.join(STREAMS_DIR).join(IN_PLAY);
    if path.exists() {
        return Ok(());
    }
    write_file(root, &path, b"felix-stream-incarnations 1\n")
}

/// Whether this broker has seen a stream with an incarnation: the record of
/// it, or a marker naming one.
pub(crate) fn in_play(root: &Path) -> Result<bool> {
    let dir = root.join(STREAMS_DIR);
    if dir.join(IN_PLAY).exists() {
        return Ok(true);
    }
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(StorageError::Io(err)),
    };
    for entry in entries {
        let path = entry.map_err(StorageError::Io)?.path();
        if path.extension().is_some()
            || path
                .file_name()
                .is_some_and(|name| name == IN_PLAY || name == FLOORS)
        {
            continue;
        }
        if let MarkerRead::Valid(marker) = read_path(&path)?
            && marker.incarnation != 0
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn remove(root: &Path, tenant: &str, namespace: &str, stream: &str) -> Result<()> {
    let path = path(root, tenant, namespace, stream);
    match std::fs::remove_file(&path) {
        Ok(()) => sync_dir(&root.join(STREAMS_DIR)).map_err(StorageError::Io),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(StorageError::Io(err)),
    }
}

/// Every marker whose delete has not finished.
pub(crate) fn retiring(root: &Path) -> Result<Vec<StreamMarker>> {
    markers(root, |marker| marker.retiring)
}

/// Every readable marker.
pub(crate) fn all(root: &Path) -> Result<Vec<StreamMarker>> {
    markers(root, |_| true)
}

fn markers(root: &Path, wanted: impl Fn(&StreamMarker) -> bool) -> Result<Vec<StreamMarker>> {
    let entries = match std::fs::read_dir(root.join(STREAMS_DIR)) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(StorageError::Io(err)),
    };
    let mut markers = Vec::new();
    for entry in entries {
        let path = entry.map_err(StorageError::Io)?.path();
        // A temporary left by a crash mid-write, whose marker is still in
        // place, or the record of incarnations, or the floors.
        if path.extension().is_some()
            || path
                .file_name()
                .is_some_and(|name| name == IN_PLAY || name == FLOORS)
        {
            continue;
        }
        // An unreadable one is left alone: deleting on a guess could take a
        // live stream's data.
        if let MarkerRead::Valid(marker) = read_path(&path)?
            && wanted(&marker)
        {
            markers.push(marker);
        }
    }
    Ok(markers)
}

fn encode(marker: &StreamMarker) -> String {
    let mut text = format!(
        "{HEADER}\nincarnation {}\nretiring {}\ntenant {}\nnamespace {}\nstream {}\n",
        marker.incarnation,
        u8::from(marker.retiring),
        hex(&marker.tenant),
        hex(&marker.namespace),
        hex(&marker.stream),
    );
    if let Some(shards) = marker.shards {
        text.push_str(&format!("shards {shards}\n"));
    }
    if let Some(deleted) = marker.deleted {
        text.push_str(&format!("deleted {deleted}\n"));
    }
    text
}

fn decode(text: &str) -> Option<StreamMarker> {
    let mut lines = text.lines();
    if lines.next()? != HEADER {
        return None;
    }
    let mut field = |name: &str| {
        let line = lines.next()?;
        line.strip_prefix(name)?
            .strip_prefix(' ')
            .map(str::to_string)
    };
    let incarnation = field("incarnation")?.parse().ok()?;
    let retiring = match field("retiring")?.as_str() {
        "0" => false,
        "1" => true,
        _ => return None,
    };
    let tenant = unhex(&field("tenant")?)?;
    let namespace = unhex(&field("namespace")?)?;
    let stream = unhex(&field("stream")?)?;
    // Optional lines, each at most once and in this order.
    let mut shards = None;
    let mut deleted = None;
    let mut order = 0;
    for line in lines {
        let (name, value) = line.split_once(' ')?;
        let at = match name {
            "shards" => {
                shards = Some(value.parse().ok()?);
                1
            }
            "deleted" => {
                deleted = Some(value.parse().ok()?);
                2
            }
            _ => return None,
        };
        if at <= order {
            return None;
        }
        order = at;
    }
    Some(StreamMarker {
        tenant,
        namespace,
        stream,
        incarnation,
        retiring,
        shards,
        deleted,
    })
}

/// Names are hex in the file, so no name can break a line.
fn hex(value: &str) -> String {
    value.bytes().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(value: &str) -> Option<String> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    let bytes = (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(value.get(i..i + 2)?, 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    String::from_utf8(bytes).ok()
}

/// The markers, floors and in-play record under one store's root, with the
/// floors file read once and kept.
#[derive(Debug)]
pub(crate) struct MarkerFiles {
    root: PathBuf,
    floors: parking_lot::Mutex<Option<Floors>>,
}

impl MarkerFiles {
    pub(crate) fn new(root: PathBuf) -> Self {
        Self {
            root,
            floors: parking_lot::Mutex::new(None),
        }
    }

    pub(crate) fn read(&self, tenant: &str, namespace: &str, name: &str) -> Result<MarkerRead> {
        read(&self.root, tenant, namespace, name)
    }

    pub(crate) fn write(&self, marker: &StreamMarker) -> Result<()> {
        write(&self.root, marker)
    }

    pub(crate) fn remove(&self, tenant: &str, namespace: &str, name: &str) -> Result<()> {
        remove(&self.root, tenant, namespace, name)
    }

    pub(crate) fn retiring(&self) -> Result<Vec<StreamMarker>> {
        retiring(&self.root)
    }

    pub(crate) fn all(&self) -> Result<Vec<StreamMarker>> {
        all(&self.root)
    }

    pub(crate) fn in_play(&self) -> Result<bool> {
        in_play(&self.root)
    }

    pub(crate) fn record_in_play(&self) -> Result<()> {
        record_in_play(&self.root)
    }

    pub(crate) fn floor(&self, tenant: &str, namespace: &str, name: &str) -> Result<Option<u64>> {
        let mut held = self.floors.lock();
        if held.is_none() {
            *held = Some(read_floors(&self.root)?);
        }
        let key = (tenant.to_string(), namespace.to_string(), name.to_string());
        Ok(held.as_ref().and_then(|floors| floors.get(&key).copied()))
    }

    /// Raise the floors of many names with one flush. A floor never goes down.
    pub(crate) fn record_floors(&self, floors: &[(&str, &str, &str, u64)]) -> Result<()> {
        let mut held = self.floors.lock();
        let mut next = match held.as_ref() {
            Some(floors) => floors.clone(),
            None => read_floors(&self.root)?,
        };
        for &(tenant, namespace, name, deleted) in floors {
            let floor = next
                .entry((tenant.to_string(), namespace.to_string(), name.to_string()))
                .or_insert(deleted);
            *floor = (*floor).max(deleted);
        }
        write_floors(&self.root, &next)?;
        *held = Some(next);
        Ok(())
    }

    pub(crate) fn clear_floor(&self, tenant: &str, namespace: &str, name: &str) -> Result<()> {
        let mut held = self.floors.lock();
        let mut next = match held.as_ref() {
            Some(floors) => floors.clone(),
            None => read_floors(&self.root)?,
        };
        let key = (tenant.to_string(), namespace.to_string(), name.to_string());
        if next.remove(&key).is_some() {
            write_floors(&self.root, &next)?;
        }
        *held = Some(next);
        Ok(())
    }
}
