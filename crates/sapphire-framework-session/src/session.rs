//! One replication session over one stream.
//!
//! Both sides run this same function. It is symmetric: each sends what the other lacks and
//! applies what it is sent, and neither is a client.
//!
//! The exchange, in order:
//!
//! 1. `Hello` — format, workspace, replica id and the sender's version vector. A format
//!    mismatch, or a different workspace, ends the session with `Refused` before any state
//!    is exchanged.
//! 2. Every path state the peer's version vector does not cover, in pages of [`PAGE`], each
//!    page preceded by the content it inlines (64 KiB or less), then `Done`.
//! 3. `Want` for content that was not inlined, answered with a blob or `Missing`.
//! 4. `Settled`, once the sender will ask for nothing more.
//! 5. `commit_session` last. An interrupted session leaves the version vector where it was,
//!    and everything is simply resent next time; committing earlier would make the replica
//!    claim versions whose content never arrived.
//!
//! # Why there are two end markers
//!
//! `Done` says "my pages are complete" and is sent as soon as they are queued, so the peer
//! can start asking for content without waiting for anything. That leaves a race worth
//! naming: a peer that has just applied a page and is about to send `Want` looks exactly
//! like a peer that needs nothing. A side that stopped reading at `Done` would therefore
//! drop the `Want` for every file over [`INLINE_LIMIT`] — and, with it, the file. `Settled`
//! closes the race. It is sent once the sender's want list can no longer grow (the peer's
//! pages are in and every hash asked for has been answered), so the receiver knows no `Want`
//! can follow, and both sides exit only after seeing the peer's `Done` *and* `Settled`.
//!
//! Content is addressed by its hash, so a page's inline blobs may travel in front of the page
//! that references them. They do: a receiver that had already applied the page would
//! otherwise ask for content that is on the wire, turning every inlined file into a `Want`
//! and a second copy of its bytes.
//!
//! # Writers
//!
//! Everything this side sends goes through one task, so the read loop never blocks on a
//! write. Two `run_session` calls facing each other would otherwise deadlock the moment both
//! wrote more than the socket buffer while each waited to read.

use std::collections::{HashMap, HashSet};

use grain_id::GrainId;
use sapphire_sync::{
    Content, ContentHash, ContentSource, PathUpdate, Replica, Report, VersionVector,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::frame::{Frame, read_frame, write_blob, write_control};
use crate::message::Message;
use crate::{INLINE_LIMIT, SESSION_FORMAT_VERSION};

/// How many path states travel in one `Updates` message.
const PAGE: usize = 256;

/// How many frames may wait for the writer before the read loop waits with them. Bounds how
/// far one peer can make the other buffer by asking faster than it reads.
const QUEUE: usize = 64;

/// What a session did.
#[derive(Clone, Debug, Default)]
pub struct SessionOutcome {
    /// Path updates sent.
    pub sent: usize,
    /// Path updates received.
    pub received: usize,
    /// What applying them did.
    pub report: Report,
}

/// One thing to put on the wire.
enum Out {
    Control(Message),
    Blob(ContentHash, Vec<u8>),
}

/// Content received inline or fetched during this session.
#[derive(Default)]
struct Received(HashMap<ContentHash, Vec<u8>>);

impl ContentSource for Received {
    fn fetch(&self, hash: &ContentHash) -> Option<Vec<u8>> {
        self.0.get(hash).cloned()
    }
}

/// Why the read loop stopped.
enum Stop {
    /// The peer's `Done` and `Settled` are in and this side wants nothing more.
    Complete,
    /// The stream ended; the peer is gone.
    Gone,
    /// The peer refused to continue, and why.
    Refused(String),
    /// The peer sent something this session does not allow.
    Protocol(String),
}

/// Run a session to completion.
///
/// Returns once both sides have said `Done` and `Settled` and every piece of content this
/// side asked for has been answered — with the bytes, or with `Missing`. A stream that ends
/// first returns without an error, but commits nothing.
///
/// `S` must be `'static` because the write half is moved into a task; a stream owned by the
/// caller (a socket, a spliced pipe, a duplex) is.
pub async fn run_session<S>(
    stream: S,
    replica: &mut Replica,
    workspace_id: GrainId,
) -> Result<SessionOutcome>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);

    // The writer task owns the write half and drains the channel in order, so the read loop
    // is never blocked by a peer that is slow to read.
    let (out_tx, mut out_rx) = mpsc::channel::<Out>(QUEUE);
    let writer_task = tokio::spawn(async move {
        while let Some(item) = out_rx.recv().await {
            let result = match item {
                Out::Control(msg) => write_control(&mut writer, &msg).await,
                Out::Blob(hash, bytes) => write_blob(&mut writer, &hash, &bytes).await,
            };
            if result.is_err() {
                break;
            }
        }
    });

    // 1. Hello, before anything else.
    send(
        &out_tx,
        Out::Control(Message::Hello {
            format: SESSION_FORMAT_VERSION,
            workspace_id,
            replica_id: replica.replica_id(),
            vv: replica.vv().clone(),
        }),
    )
    .await?;

    let peer_vv = match read_frame(&mut reader).await? {
        Some(Frame::Control(Message::Hello {
            format,
            workspace_id: theirs,
            vv,
            ..
        })) => {
            if format != SESSION_FORMAT_VERSION {
                let _ = send(
                    &out_tx,
                    Out::Control(Message::Refused(format!(
                        "session format {format}, expected {SESSION_FORMAT_VERSION}"
                    ))),
                )
                .await;
                return close(
                    out_tx,
                    writer_task,
                    Err(Error::VersionMismatch {
                        ours: SESSION_FORMAT_VERSION,
                        theirs: format,
                    }),
                )
                .await;
            }
            if theirs != workspace_id {
                let _ = send(
                    &out_tx,
                    Out::Control(Message::Refused(format!(
                        "this stream is for workspace {workspace_id}, not {theirs}"
                    ))),
                )
                .await;
                return close(
                    out_tx,
                    writer_task,
                    Err(Error::Protocol(format!(
                        "workspace mismatch: the peer is syncing {theirs}, this session is for \
                     {workspace_id}"
                    ))),
                )
                .await;
            }
            vv
        }
        Some(Frame::Control(Message::Refused(why))) => {
            return close(out_tx, writer_task, Err(Error::Refused(why))).await;
        }
        other => {
            return close(
                out_tx,
                writer_task,
                Err(Error::Protocol(format!("expected Hello, got {other:?}"))),
            )
            .await;
        }
    };

    // 2. Build the outgoing side as owned data, then hand it to a task. Computing it up
    //    front keeps the read loop free while it is written.
    let delta = replica
        .delta_for(&peer_vv)
        .map_err(|e| Error::Sync(e.to_string()))?;
    let sent = delta.len();
    let mut outgoing: Vec<Out> = Vec::new();
    for page in delta.chunks(PAGE) {
        for update in page {
            for entry in &update.versions {
                let Content::File { hash, len } = entry.content else {
                    continue;
                };
                if len as usize > INLINE_LIMIT {
                    continue;
                }
                if let Ok(Some(bytes)) = replica.read_content(&hash) {
                    outgoing.push(Out::Blob(hash, bytes));
                }
            }
        }
        outgoing.push(Out::Control(Message::Updates(page.to_vec())));
    }
    outgoing.push(Out::Control(Message::Done));

    let sender = out_tx.clone();
    let send_task = tokio::spawn(async move {
        for item in outgoing {
            if sender.send(item).await.is_err() {
                break;
            }
        }
    });

    // 3. Read until the peer is done and settled and every hash we asked for has been
    //    answered.
    let mut received = Received::default();
    let mut wanted: HashSet<ContentHash> = HashSet::new();
    let mut report = Report::default();
    let mut received_count = 0usize;
    let mut peer_done = false;
    let mut peer_settled = false;
    let mut settled = false;

    let stop = loop {
        if peer_done && peer_settled && wanted.is_empty() {
            break Stop::Complete;
        }
        let Some(frame) = read_frame(&mut reader).await? else {
            break Stop::Gone;
        };
        match frame {
            Frame::Control(Message::Updates(updates)) => {
                received_count += updates.len();
                // The join is idempotent, so a redelivered page costs nothing.
                let page = replica
                    .apply(&updates, &received)
                    .map_err(|e| Error::Sync(e.to_string()))?;
                merge_report(&mut report, page);
                // Ask for content the page needs that this session does not have.
                for hash in needed(replica, &updates, &received)? {
                    if wanted.insert(hash) {
                        send(&out_tx, Out::Control(Message::Want(hash))).await?;
                    }
                }
            }
            Frame::Control(Message::Done) => peer_done = true,
            Frame::Control(Message::Settled) => peer_settled = true,
            Frame::Control(Message::Want(hash)) => {
                let answer = match replica.read_content(&hash) {
                    Ok(Some(bytes)) => Out::Blob(hash, bytes),
                    _ => Out::Control(Message::Missing(hash)),
                };
                send(&out_tx, answer).await?;
            }
            Frame::Control(Message::Missing(hash)) => {
                // The peer does not have it either. Another peer may; the path simply stays
                // unmaterialised until one does.
                wanted.remove(&hash);
            }
            Frame::Control(Message::Refused(why)) => {
                break Stop::Refused(why);
            }
            Frame::Control(Message::Hello { .. }) => {
                break Stop::Protocol("a second Hello".to_owned());
            }
            Frame::Blob { hash, bytes } => {
                // Verify before storing: content is addressed by hash, so a peer that sent
                // the wrong bytes under one must not be able to plant them.
                if ContentHash::of_bytes(&bytes) != hash {
                    break Stop::Protocol(format!("content does not match {hash}"));
                }
                wanted.remove(&hash);
                received.0.insert(hash, bytes);
            }
        }
        // The peer's pages are all in, so this side's want list can no longer grow: settling
        // tells the peer it may close as soon as it wants nothing more either.
        if peer_done && wanted.is_empty() && !settled {
            settled = true;
            send(&out_tx, Out::Control(Message::Settled)).await?;
        }
    };

    let outcome = SessionOutcome {
        sent,
        received: received_count,
        report,
    };
    match stop {
        Stop::Complete => {
            // The peer is still reading: it exits only once it has seen this side's
            // `Settled`, which is queued but not yet written. Draining the writer delivers
            // `Done`, `Settled` and every pending answer.
            let _ = send_task.await;
            let result = materialise(replica, &received, &peer_vv, outcome);
            close(out_tx, writer_task, result).await
        }
        // The peer is gone or has misbehaved, so there is nothing to deliver and a write
        // may never complete. Abort: nothing is committed either way, and everything is
        // resent next session.
        Stop::Gone => {
            send_task.abort();
            abort(out_tx, writer_task);
            Ok(outcome)
        }
        Stop::Refused(why) => {
            send_task.abort();
            abort(out_tx, writer_task);
            Err(Error::Refused(why))
        }
        Stop::Protocol(why) => {
            send_task.abort();
            abort(out_tx, writer_task);
            Err(Error::Protocol(why))
        }
    }
}

/// Materialise what was waiting on content, then commit.
///
/// Committing is what makes this replica claim it has what the peer had. It runs last, and
/// only when the peer said `Done` and `Settled`; an interrupted session leaves the version
/// vector where it was.
fn materialise(
    replica: &mut Replica,
    received: &Received,
    peer_vv: &VersionVector,
    mut outcome: SessionOutcome,
) -> Result<SessionOutcome> {
    let fetched = replica
        .fetch_missing(received)
        .map_err(|e| Error::Sync(e.to_string()))?;
    merge_report(&mut outcome.report, fetched);
    replica
        .commit_session(peer_vv)
        .map_err(|e| Error::Sync(e.to_string()))?;
    tracing::debug!(
        sent = outcome.sent,
        received = outcome.received,
        changed = outcome.report.changed,
        "session complete"
    );
    Ok(outcome)
}

/// Queue one frame, or report that the writer is gone.
async fn send(tx: &mpsc::Sender<Out>, item: Out) -> Result<()> {
    tx.send(item)
        .await
        .map_err(|_| Error::Protocol("the stream closed".to_owned()))
}

/// Close the outgoing side, let the writer flush what is queued, and return `result`.
///
/// Dropping the sender is what ends the writer task; it drains first, so a `Refused` or a
/// `Settled` queued moments ago still reaches the peer before the stream goes away.
async fn close(
    out_tx: mpsc::Sender<Out>,
    writer: JoinHandle<()>,
    result: Result<SessionOutcome>,
) -> Result<SessionOutcome> {
    drop(out_tx);
    let _ = writer.await;
    result
}

/// Close the outgoing side without waiting to deliver what is queued.
///
/// Used only once the peer can no longer be expected to read, where a write may otherwise
/// never complete.
fn abort(out_tx: mpsc::Sender<Out>, writer: JoinHandle<()>) {
    drop(out_tx);
    writer.abort();
}

/// Hashes these updates need that neither the store nor this session has.
fn needed(
    replica: &Replica,
    updates: &[PathUpdate],
    received: &Received,
) -> Result<Vec<ContentHash>> {
    let mut out = Vec::new();
    for update in updates {
        for entry in &update.versions {
            let Some(hash) = entry.content.hash() else {
                continue;
            };
            if received.0.contains_key(&hash) {
                continue;
            }
            let have = replica
                .read_content(&hash)
                .map_err(|e| Error::Sync(e.to_string()))?
                .is_some();
            if !have {
                out.push(hash);
            }
        }
    }
    Ok(out)
}

fn merge_report(into: &mut Report, from: Report) {
    into.changed += from.changed;
    into.recorded.extend(from.recorded);
    into.conflicts.extend(from.conflicts);
    into.skipped.extend(from.skipped);
}
