//! The home side of a forwarded connection: the splice stream as an
//! `AsyncRead + AsyncWrite` for a front end.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use futures::Stream;
use sprig_api::proto::internal::{SpliceDown, SpliceUp, splice_down, splice_up};
use sprig_common::CancellationToken;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

use crate::frontend::TlsInfo;

type Down = std::result::Result<SpliceDown, tonic::Status>;
/// The up half of a splice: the client's frames.
pub type UpStream =
    Pin<Box<dyn Stream<Item = std::result::Result<SpliceUp, tonic::Status>> + Send>>;
type Reserve = Pin<
    Box<dyn Future<Output = Result<mpsc::OwnedPermit<Down>, mpsc::error::SendError<()>>> + Send>,
>;

/// A client connection that another node accepted and handed here: reads
/// come from the splice's up stream (after the bytes the accepting node had
/// already read), writes go down to that node, which sends them to the
/// client.
pub struct Spliced {
    // In mutexes only so the stream is `Sync` (front ends hold `&` to their
    // client stream across awaits); reached through `get_mut`, never
    // locked.
    up: Mutex<UpStream>,
    pending: Bytes,
    eof: bool,
    down: mpsc::Sender<Down>,
    reserve: Mutex<Option<Reserve>>,
    tls: Option<TlsInfo>,
    resume: Mutex<Option<String>>,
    from_node: u64,
    gone: CancellationToken,
}

impl fmt::Debug for Spliced {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Spliced")
            .field("from_node", &self.from_node)
            .field("tls", &self.tls.is_some())
            .finish_non_exhaustive()
    }
}

impl Spliced {
    /// A stream reading `prelude` first, then `up`'s data frames, and
    /// writing to `down`.
    pub fn new(
        up: UpStream,
        prelude: Bytes,
        down: mpsc::Sender<Down>,
        tls: Option<TlsInfo>,
        resume: String,
        from_node: u64,
    ) -> Self {
        Self {
            up: Mutex::new(up),
            pending: prelude,
            eof: false,
            down,
            reserve: Mutex::new(None),
            tls,
            resume: Mutex::new((!resume.is_empty()).then_some(resume)),
            from_node,
            gone: CancellationToken::new(),
        }
    }

    /// The client's TLS as the accepting node negotiated it.
    #[must_use]
    pub fn tls_info(&self) -> Option<TlsInfo> {
        self.tls.clone()
    }

    /// What the accepting node's front end had established with the
    /// client (its protocol's JSON); taken once.
    pub fn take_resume(&self) -> Option<String> {
        self.resume
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// The accepting node's Raft id.
    #[must_use]
    pub fn from_node(&self) -> u64 {
        self.from_node
    }

    /// Resolves once the client side has ended (the up stream finished or
    /// failed), for front ends that watch for a client leaving mid-statement.
    pub async fn gone(&self) {
        self.gone.cancelled().await;
    }
}

impl AsyncRead for Spliced {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        loop {
            if !me.pending.is_empty() {
                let n = me.pending.len().min(buf.remaining());
                buf.put_slice(&me.pending[..n]);
                me.pending.advance(n);
                return Poll::Ready(Ok(()));
            }
            if me.eof {
                return Poll::Ready(Ok(()));
            }
            let up = me.up.get_mut().unwrap_or_else(PoisonError::into_inner);
            match up.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(SpliceUp {
                    msg: Some(splice_up::Msg::Data(d)),
                }))) => me.pending = Bytes::from(d),
                Poll::Ready(Some(Ok(_))) => {
                    me.eof = true;
                    me.gone.cancel();
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "a forwarded connection sent a second open",
                    )));
                }
                Poll::Ready(Some(Err(_)) | None) => {
                    me.eof = true;
                    me.gone.cancel();
                }
            }
        }
    }
}

fn closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "the node that accepted this connection closed it",
    )
}

impl AsyncWrite for Spliced {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let reserve = me.reserve.get_mut().unwrap_or_else(PoisonError::into_inner);
        let fut = reserve.get_or_insert_with(|| Box::pin(me.down.clone().reserve_owned()));
        match fut.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(r) => {
                *reserve = None;
                let Ok(permit) = r else {
                    return Poll::Ready(Err(closed()));
                };
                permit.send(Ok(SpliceDown {
                    msg: Some(splice_down::Msg::Data(buf.to_vec())),
                }));
                Poll::Ready(Ok(buf.len()))
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.down.is_closed() {
            return Poll::Ready(Err(closed()));
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // The down stream ends when the front end drops the stream.
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[allow(clippy::unnecessary_wraps)] // the stream's item type
    fn data(b: &[u8]) -> std::result::Result<SpliceUp, tonic::Status> {
        Ok(SpliceUp {
            msg: Some(splice_up::Msg::Data(b.to_vec())),
        })
    }

    #[tokio::test]
    async fn reads_the_prelude_then_the_frames_and_writes_down() {
        let up = futures::stream::iter(vec![data(b"world"), data(b"!")]);
        let (tx, mut rx) = mpsc::channel(4);
        let mut s = Spliced::new(
            Box::pin(up),
            Bytes::from_static(b"hello "),
            tx,
            None,
            "{\"a\":1}".into(),
            7,
        );
        assert_eq!(s.take_resume().as_deref(), Some("{\"a\":1}"));
        assert_eq!(s.take_resume(), None);
        assert_eq!(s.from_node(), 7);
        let mut got = String::new();
        s.read_to_string(&mut got).await.unwrap();
        assert_eq!(got, "hello world!");
        // The client side ended: `gone` fires.
        tokio::time::timeout(std::time::Duration::from_secs(1), s.gone())
            .await
            .unwrap();
        s.write_all(b"reply").await.unwrap();
        s.flush().await.unwrap();
        let Some(Ok(SpliceDown {
            msg: Some(splice_down::Msg::Data(d)),
        })) = rx.recv().await
        else {
            panic!("no data frame");
        };
        assert_eq!(d, b"reply");
        drop(rx);
        assert!(
            s.write_all(b"x").await.is_err(),
            "writes fail once the node is gone"
        );
    }

    #[tokio::test]
    async fn a_second_open_is_an_error() {
        let open = Ok(SpliceUp {
            msg: Some(splice_up::Msg::Open(
                sprig_api::proto::internal::SpliceOpen::default(),
            )),
        });
        let (tx, _rx) = mpsc::channel(4);
        let mut s = Spliced::new(
            Box::pin(futures::stream::iter(vec![open])),
            Bytes::new(),
            tx,
            None,
            String::new(),
            1,
        );
        assert_eq!(s.take_resume(), None);
        let mut b = [0u8; 4];
        assert!(s.read(&mut b).await.is_err());
    }
}
