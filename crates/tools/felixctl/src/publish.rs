//! `felixctl pub`.

use std::io::BufRead;

use felix_client::AckMode;

use crate::cli::{AckArg, PubArgs};
use crate::connect::Broker;
use crate::context::Settings;
use crate::error::{Exit, MarkExit};
use crate::output::Output;

/// Where the messages come from.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Source {
    Literal(Vec<u8>),
    File(std::path::PathBuf),
    StdinLines,
    StdinWhole,
}

impl Source {
    pub(crate) fn of(args: &PubArgs) -> Self {
        if let Some(data) = &args.data {
            Source::Literal(data.as_bytes().to_vec())
        } else if let Some(file) = &args.file {
            Source::File(file.clone())
        } else if args.whole {
            Source::StdinWhole
        } else {
            Source::StdinLines
        }
    }
}

pub(crate) async fn run(args: &PubArgs, settings: &Settings, out: &Output) -> anyhow::Result<()> {
    let broker = Broker::connect(settings).await?;
    let mut sender = Sender::new(&broker, args).await?;
    match Source::of(args) {
        Source::Literal(bytes) => sender.send_repeated(bytes).await?,
        Source::File(path) => {
            let bytes = tokio::fs::read(&path)
                .await
                .mark(Exit::Usage, format!("read {}", path.display()))?;
            sender.send_repeated(bytes).await?;
        }
        Source::StdinWhole => {
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut std::io::stdin().lock(), &mut bytes)
                .mark(Exit::Usage, "read stdin")?;
            sender.send_repeated(bytes).await?;
        }
        Source::StdinLines => {
            // Read on a blocking thread so a slow pipe does not stall the
            // runtime the client's connections run on.
            let (tx, mut rx) = tokio::sync::mpsc::channel::<std::io::Result<Vec<u8>>>(64);
            std::thread::spawn(move || {
                for line in std::io::stdin().lock().split(b'\n') {
                    let line = line.map(|mut bytes| {
                        if bytes.last() == Some(&b'\r') {
                            bytes.pop();
                        }
                        bytes
                    });
                    if tx.blocking_send(line).is_err() {
                        break;
                    }
                }
            });
            while let Some(line) = rx.recv().await {
                sender
                    .send_repeated(line.mark(Exit::Usage, "read stdin")?)
                    .await?;
            }
        }
    }
    sender.finish().await?;

    let stream = &args.stream;
    let text = summary(
        stream,
        &sender.offsets,
        matches!(sender.mode, Mode::Unacked(_)),
    );
    out.done(
        &text,
        serde_json::json!({
            "stream": stream,
            "published": sender.offsets.len(),
            "offsets": sender.offsets,
        }),
    )
}

/// The line `pub` prints when it is done.
///
/// An acked publish can come back without an offset: a broker that owns the
/// shard and acks on enqueue answers before the record has one, while a
/// forwarded publish is answered by the owner after the write. Saying so keeps
/// a missing offset from reading as a failure.
fn summary(stream: &str, offsets: &[Option<u64>], unacked: bool) -> String {
    let published = offsets.len();
    let first = offsets.iter().flatten().min().copied();
    let last = offsets.iter().flatten().max().copied();
    let mut text = match (first, last) {
        (Some(first), Some(last)) if first == last => {
            format!("published {published} to {stream} at offset {first}")
        }
        (Some(first), Some(last)) => {
            format!("published {published} to {stream}, offsets {first}..={last}")
        }
        _ => format!("published {published} to {stream}"),
    };
    let missing = offsets.iter().filter(|offset| offset.is_none()).count();
    if !unacked && missing > 0 {
        text.push_str(&format!(
            "; {missing} without an offset (acknowledged before the write, or the stream has no log)"
        ));
    }
    text
}

/// One of the three ways to publish, chosen once from the flags.
struct Sender<'a> {
    broker: &'a Broker,
    stream: String,
    key: Option<bytes::Bytes>,
    count: u64,
    mode: Mode<'a>,
    /// One per message sent, `None` where the broker reported no offset.
    offsets: Vec<Option<u64>>,
}

enum Mode<'a> {
    /// Acknowledged, through the cluster client, which routes to the owner.
    Acked,
    /// Fire and forget. Through a single client's publisher, because that has
    /// `finish()` to flush before the process exits; the cluster client has
    /// no equivalent.
    Unacked(felix_client::Publisher),
    Idempotent(Box<felix_client::IdempotentProducer<'a>>),
}

impl<'a> Sender<'a> {
    async fn new(broker: &'a Broker, args: &PubArgs) -> anyhow::Result<Self> {
        let mode = if args.idempotent {
            Mode::Idempotent(Box::new(broker.cluster.idempotent_producer().await?))
        } else if args.ack == AckArg::None {
            Mode::Unacked(broker.cluster.client().await.publisher().await?)
        } else {
            Mode::Acked
        };
        Ok(Self {
            broker,
            stream: args.stream.clone(),
            key: args.key.clone().map(bytes::Bytes::from),
            count: args.count,
            mode,
            offsets: Vec::new(),
        })
    }

    async fn send_repeated(&mut self, payload: Vec<u8>) -> anyhow::Result<()> {
        for _ in 0..self.count {
            let offset = self.send(payload.clone()).await?;
            self.offsets.push(offset);
        }
        Ok(())
    }

    async fn send(&self, payload: Vec<u8>) -> anyhow::Result<Option<u64>> {
        let (tenant, namespace, stream) = (
            self.broker.tenant.as_str(),
            self.broker.namespace.as_str(),
            self.stream.as_str(),
        );
        let cluster = &self.broker.cluster;
        match (&self.mode, &self.key) {
            (Mode::Acked, None) => {
                cluster
                    .publish(tenant, namespace, stream, payload, AckMode::PerMessage)
                    .await
            }
            (Mode::Acked, Some(key)) => {
                cluster
                    .publish_keyed(
                        tenant,
                        namespace,
                        stream,
                        payload,
                        key.clone(),
                        AckMode::PerMessage,
                    )
                    .await
            }
            (Mode::Unacked(publisher), None) => {
                publisher
                    .publish(tenant, namespace, stream, payload, AckMode::None)
                    .await
            }
            (Mode::Unacked(publisher), Some(key)) => {
                publisher
                    .publish_keyed(
                        tenant,
                        namespace,
                        stream,
                        key.clone(),
                        payload,
                        AckMode::None,
                    )
                    .await
            }
            // clap refuses --idempotent with --key.
            (Mode::Idempotent(producer), _) => {
                producer.publish(tenant, namespace, stream, payload).await
            }
        }
    }

    async fn finish(&self) -> anyhow::Result<()> {
        if let Mode::Unacked(publisher) = &self.mode {
            publisher.finish().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
