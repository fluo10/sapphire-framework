//! A framed, bidirectional message stream.
//!
//! Frames are newline-delimited compact JSON (spec §2.2). Whatever carries them — a Unix
//! socket, a Windows named pipe, or nothing at all in the in-process case — is pumped by
//! two background tasks into a pair of channels, so every caller sees the same type.

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::MAX_FRAME_LEN;
use crate::error::{Error, Result};
use crate::message::Message;

/// How many outgoing messages may queue before `send` waits.
const OUTGOING_CAPACITY: usize = 256;
/// How many incoming messages may queue before the reader waits.
const INCOMING_CAPACITY: usize = 256;

/// A cloneable handle for sending on a [`Connection`].
///
/// Used to hand a connection's write side to many tasks — for example so a request handler
/// can emit notifications while the read loop keeps running.
#[derive(Clone, Debug)]
pub struct Sender(mpsc::Sender<Message>);

impl Sender {
    /// Queue a message. Waits while the outgoing queue is full; fails once the connection
    /// has closed.
    pub async fn send(&self, msg: Message) -> Result<()> {
        self.0.send(msg).await.map_err(|_| Error::Closed)
    }
}

/// A framed, bidirectional message stream.
#[derive(Debug)]
pub struct Connection {
    outgoing: mpsc::Sender<Message>,
    incoming: mpsc::Receiver<Result<Message>>,
    pumps: Vec<JoinHandle<()>>,
}

impl Connection {
    /// Wrap anything that reads and writes bytes.
    ///
    /// Reading and writing run in **separate tasks**, not in one `select!` loop. Reading a
    /// frame is not cancel-safe — it consumes bytes from the buffer before it has a whole
    /// line — so a `select!` that dropped the read future whenever an outgoing message
    /// arrived would silently lose the partial frame.
    pub fn from_io<S>(io: S) -> Connection
    where
        S: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (out_tx, mut out_rx) = mpsc::channel::<Message>(OUTGOING_CAPACITY);
        let (in_tx, in_rx) = mpsc::channel::<Result<Message>>(INCOMING_CAPACITY);
        let (read_half, mut write_half) = tokio::io::split(io);

        // The writer is deliberately detached rather than kept in `pumps`. `send` only
        // queues, so a caller may drop its `Connection` the moment a send returns; if the
        // writer were aborted here, that just-queued message could be lost. Left alone it
        // drains the queue, shuts the write half down and exits on its own.
        std::mem::drop(tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                let Ok(mut bytes) = msg.encode() else {
                    // Our own message failed to serialise. Nothing useful can be sent, and
                    // the caller already owns the value, so drop it and keep serving.
                    tracing::error!("dropping an outgoing message that could not be encoded");
                    continue;
                };
                bytes.push(b'\n');
                if write_half.write_all(&bytes).await.is_err() {
                    break;
                }
                if write_half.flush().await.is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        }));

        let reader = tokio::spawn(async move {
            let mut reader = BufReader::new(read_half);
            loop {
                let mut line = Vec::new();
                match read_limited(&mut reader, &mut line).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        while line.last() == Some(&b'\n') || line.last() == Some(&b'\r') {
                            line.pop();
                        }
                        if line.is_empty() {
                            continue;
                        }
                        if in_tx.send(Message::decode(&line)).await.is_err() {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = in_tx.send(Err(err)).await;
                        break;
                    }
                }
            }
        });

        Connection {
            outgoing: out_tx,
            incoming: in_rx,
            pumps: vec![reader],
        }
    }

    /// Two connections wired directly to each other, with no carrier.
    ///
    /// This is the in-process transport of spec §2.1, used on mobile and in tests. It still
    /// moves [`Message`] values rather than bytes; the serialisation the spec asks for
    /// happens wherever the message is built, so both sides run the same code path.
    pub fn pair() -> (Connection, Connection) {
        let (a_out, b_in) = mpsc::channel::<Message>(OUTGOING_CAPACITY);
        let (b_out, a_in) = mpsc::channel::<Message>(OUTGOING_CAPACITY);
        (
            Connection::from_channels(a_out, a_in),
            Connection::from_channels(b_out, b_in),
        )
    }

    fn from_channels(
        outgoing: mpsc::Sender<Message>,
        mut raw_in: mpsc::Receiver<Message>,
    ) -> Connection {
        let (in_tx, in_rx) = mpsc::channel::<Result<Message>>(INCOMING_CAPACITY);
        let pump = tokio::spawn(async move {
            while let Some(msg) = raw_in.recv().await {
                // Serialise and parse even though both ends are in this process: mobile
                // runs this carrier, and it must exercise the same code path as the
                // others (spec §2.1).
                let round_tripped = msg.encode().and_then(|bytes| Message::decode(&bytes));
                if in_tx.send(round_tripped).await.is_err() {
                    break;
                }
            }
        });
        Connection {
            outgoing,
            incoming: in_rx,
            pumps: vec![pump],
        }
    }

    /// A cloneable handle for the write side.
    pub fn sender(&self) -> Sender {
        Sender(self.outgoing.clone())
    }

    /// Queue a message.
    pub async fn send(&self, msg: Message) -> Result<()> {
        self.outgoing.send(msg).await.map_err(|_| Error::Closed)
    }

    /// Await the next message. `None` once the connection has closed.
    pub async fn recv(&mut self) -> Option<Result<Message>> {
        self.incoming.recv().await
    }

    /// Close the connection.
    ///
    /// Messages already queued are still flushed, then the carrier's write half is shut
    /// down and the read half is released.
    pub fn close(self) {
        drop(self);
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        for pump in self.pumps.drain(..) {
            pump.abort();
        }
    }
}

/// `read_until(b'\n')`, refusing to grow past [`MAX_FRAME_LEN`].
///
/// `AsyncBufReadExt::read_until` has no limit, so a peer that never sends a newline would
/// otherwise make us allocate without bound.
async fn read_limited<R>(reader: &mut R, out: &mut Vec<u8>) -> Result<usize>
where
    R: AsyncBufRead + Unpin,
{
    loop {
        let available = match reader.fill_buf().await {
            Ok(buf) => buf,
            Err(err) => return Err(Error::Io(err)),
        };
        if available.is_empty() {
            return Ok(out.len());
        }
        match available.iter().position(|b| *b == b'\n') {
            Some(idx) => {
                out.extend_from_slice(&available[..=idx]);
                reader.consume(idx + 1);
                return Ok(out.len());
            }
            None => {
                let len = available.len();
                if out.len() + len > MAX_FRAME_LEN {
                    return Err(Error::FrameTooLarge {
                        len: out.len() + len,
                        max: MAX_FRAME_LEN,
                    });
                }
                out.extend_from_slice(available);
                reader.consume(len);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    use crate::{Notification, Request};

    fn req(id: u64) -> Message {
        Message::Request(Request {
            id,
            method: "ping".into(),
            params: json!({ "n": id }),
        })
    }

    #[tokio::test]
    async fn a_pair_carries_messages_both_ways() {
        let (mut a, mut b) = Connection::pair();
        a.send(req(1)).await.unwrap();
        assert_eq!(b.recv().await.unwrap().unwrap(), req(1));

        b.send(req(2)).await.unwrap();
        assert_eq!(a.recv().await.unwrap().unwrap(), req(2));
    }

    #[tokio::test]
    async fn dropping_one_end_of_a_pair_closes_the_other() {
        let (a, mut b) = Connection::pair();
        drop(a);
        assert!(b.recv().await.is_none());
    }

    #[tokio::test]
    async fn io_framing_round_trips_many_messages() {
        let (client_io, server_io) = tokio::io::duplex(8 * 1024);
        let client = Connection::from_io(client_io);
        let mut server = Connection::from_io(server_io);

        for id in 1..=100 {
            client.send(req(id)).await.unwrap();
        }
        for id in 1..=100 {
            assert_eq!(server.recv().await.unwrap().unwrap(), req(id));
        }
    }

    #[tokio::test]
    async fn a_message_larger_than_the_buffer_still_round_trips() {
        let (client_io, server_io) = tokio::io::duplex(1024);
        let client = Connection::from_io(client_io);
        let mut server = Connection::from_io(server_io);

        let big = "x".repeat(200_000);
        let msg = Message::Notification(Notification {
            method: "big".into(),
            params: json!({ "content": big }),
        });
        let sent = msg.clone();
        tokio::spawn(async move { client.send(sent).await.unwrap() });
        assert_eq!(server.recv().await.unwrap().unwrap(), msg);
    }

    #[tokio::test]
    async fn an_oversized_frame_is_reported_and_closes_the_connection() {
        let (mut raw, server_io) = tokio::io::duplex(1024);
        let mut server = Connection::from_io(server_io);

        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            // A single line longer than the limit, written without ever ending it.
            let chunk = vec![b'x'; 1024 * 1024];
            for _ in 0..(MAX_FRAME_LEN / chunk.len() + 1) {
                if raw.write_all(&chunk).await.is_err() {
                    break;
                }
            }
        });

        let err = server.recv().await.unwrap().unwrap_err();
        assert!(matches!(err, Error::FrameTooLarge { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn a_malformed_frame_is_reported_without_killing_the_connection() {
        let (mut raw, server_io) = tokio::io::duplex(1024);
        let mut server = Connection::from_io(server_io);

        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            raw.write_all(b"{ not json }\n").await.unwrap();
            let good = Message::Notification(Notification {
                method: "after".into(),
                params: serde_json::Value::Null,
            });
            raw.write_all(&good.encode().unwrap()).await.unwrap();
            raw.write_all(b"\n").await.unwrap();
            // Keep the write half alive so the reader does not see EOF first.
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        });

        assert!(server.recv().await.unwrap().is_err());
        let next = server.recv().await.unwrap().unwrap();
        assert_eq!(
            next,
            Message::Notification(Notification {
                method: "after".into(),
                params: serde_json::Value::Null
            })
        );
    }
}
