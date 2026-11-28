//! The connection loop, pipelined: decode every command
//! the client sent, answer them in order, flush once per batch, and
//! deliver pub/sub messages between batches.
//!
//! Commands already buffered are taken up to
//! [`GROUP_MAX_COMMANDS`] at a time; a run of guarded writes and `GET`s
//! among them runs as one group ([`crate::batch::Group`]: one statement,
//! one commit), its replies written after the commit. On a database stored
//! on the storage engine, a run of the engine's commands is one engine
//! batch instead ([`Session::handle_engine`]).
//!
//! While a command runs (a blocking pop may wait for minutes), the loop
//! keeps reading into its buffer, so a client that goes away ends the
//! command at once.

use std::future::Future;

use bytes::BytesMut;
use sprig_proto_redis::{Command, Decoder, Frame, Limits, encode};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::batch::GROUP_MAX_COMMANDS;
use crate::session::{HandOff, Session};

/// `c` as the client could have sent it: a RESP array of bulk strings.
fn encode_command(c: &Command, out: &mut BytesMut) {
    out.extend_from_slice(format!("*{}\r\n", c.args.len()).as_bytes());
    for a in &c.args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
}

/// Serves `sess` on `stream` until the client quits, the stream ends, or
/// `stop` completes.
///
/// # Errors
/// I/O errors on the stream.
pub async fn serve<S, F>(stream: S, sess: Session, limits: Limits, stop: F) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: Future<Output = ()> + Send,
{
    serve_until_handoff(stream, sess, limits, stop)
        .await
        .map(|_| ())
}

/// [`serve`], ending early when a login names a tenant served by another
/// node ([`crate::session::Auth::serve_elsewhere`]): the replies so far are
/// flushed, and the hand-off comes back with the client's bytes from the
/// login command on (re-encoded commands, then what was buffered), for the
/// caller to send to that node.
///
/// # Errors
/// I/O errors on the stream.
pub async fn serve_until_handoff<S, F>(
    mut stream: S,
    mut sess: Session,
    limits: Limits,
    stop: F,
) -> std::io::Result<Option<(HandOff, BytesMut)>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: Future<Output = ()> + Send,
{
    let decoder = Decoder::new(limits);
    let mut inbuf = BytesMut::with_capacity(16 * 1024);
    let mut out = BytesMut::with_capacity(16 * 1024);
    let mut msgs = sess.take_messages();
    tokio::pin!(stop);
    let r = async {
        let mut cmds: Vec<Command> = Vec::new();
        loop {
            // Answer every whole command already buffered, a few at a
            // time: a run of them may share one statement and one commit
            // (Session::group), its replies sent once it has committed.
            loop {
                let mut bad = None;
                cmds.clear();
                while cmds.len() < GROUP_MAX_COMMANDS {
                    match decoder.decode(&mut inbuf) {
                        Ok(Some(c)) => cmds.push(c),
                        Ok(None) => break,
                        Err(e) => {
                            bad = Some(e);
                            break;
                        }
                    }
                }
                if cmds.is_empty() && bad.is_none() {
                    break;
                }
                let mut at = 0;
                while at < cmds.len() {
                    // A run of commands on a storage-engine database is one
                    // batch: one commit, replies once it is durable.
                    let engine = sess.engine_len(&cmds[at..]);
                    let group = if engine == 0 {
                        sess.group(&cmds[at..])
                    } else {
                        None
                    };
                    let replies = {
                        let fut = async {
                            if engine > 0 {
                                return sess.handle_engine(&cmds[at..at + engine]).await;
                            }
                            match &group {
                                Some(g) => sess.handle_group(g, &cmds[at..]).await,
                                None => vec![sess.handle(cmds[at].clone()).await],
                            }
                        };
                        tokio::pin!(fut);
                        loop {
                            tokio::select! {
                                f = &mut fut => break Some(f),
                                n = stream.read_buf(&mut inbuf) => {
                                    if n? == 0 {
                                        // The client went away mid-command.
                                        break None;
                                    }
                                }
                            }
                        }
                    };
                    let Some(replies) = replies else {
                        return Ok(None);
                    };
                    if let Some(h) = sess.handoff.take() {
                        // The login command's answer is the serving node's.
                        if !out.is_empty() {
                            stream.write_all(&out).await?;
                            stream.flush().await?;
                        }
                        let mut rest = BytesMut::new();
                        for c in &cmds[at..] {
                            encode_command(c, &mut rest);
                        }
                        rest.extend_from_slice(&inbuf);
                        return Ok(Some((h, rest)));
                    }
                    crate::stats::count(&cmds[at..], &replies);
                    at += replies.len().max(1);
                    for f in replies.iter().flatten() {
                        encode(f, sess.proto, &mut out);
                    }
                    if sess.closing {
                        stream.write_all(&out).await?;
                        return Ok(None);
                    }
                    if out.len() > 64 * 1024 {
                        stream.write_all(&out).await?;
                        out.clear();
                    }
                }
                if let Some(e) = bad {
                    encode(&Frame::err(&e.to_string()), sess.proto, &mut out);
                    stream.write_all(&out).await?;
                    return Ok(None);
                }
            }
            if !out.is_empty() {
                stream.write_all(&out).await?;
                stream.flush().await?;
                out.clear();
            }
            tokio::select! {
                n = stream.read_buf(&mut inbuf) => {
                    if n? == 0 {
                        return Ok(None);
                    }
                }
                m = async {
                    match &mut msgs {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(m) = m
                        && let Some(f) = sess.deliver(m).await
                    {
                        encode(&f, sess.proto, &mut out);
                    }
                }
                () = &mut stop => return Ok(None),
            }
        }
    }
    .await;
    sess.close().await;
    r
}
