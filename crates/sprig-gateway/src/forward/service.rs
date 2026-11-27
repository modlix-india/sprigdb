//! `NodeForward.Splice` on the peer listener: client connections handed
//! here by the node that accepted them.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Mutex, OnceLock, PoisonError};

use bytes::Bytes;
use futures::Stream;
use sprig_api::proto::internal::node_forward_server::{NodeForward, NodeForwardServer};
use sprig_api::proto::internal::{
    SpliceDown, SpliceRefused, SpliceTls, SpliceUp, splice_down, splice_up,
};
use sprig_common::SprigCode;
use tokio::sync::mpsc;

use super::io::Spliced;
use crate::frontend::TlsInfo;
use crate::node_engine::{self, UnitLeader};

type Down = std::result::Result<SpliceDown, tonic::Status>;
type DownStream = Pin<Box<dyn Stream<Item = Down> + Send>>;

/// The service.
#[derive(Debug, Default)]
pub struct ForwardService;

impl ForwardService {
    /// The tonic server for the peer listener.
    #[must_use]
    pub fn server() -> NodeForwardServer<Self> {
        NodeForwardServer::new(Self)
            .max_decoding_message_size(64 << 20)
            .max_encoding_message_size(64 << 20)
    }
}

#[allow(clippy::unnecessary_wraps)] // the response stream's item type
fn down(msg: splice_down::Msg) -> Down {
    Ok(SpliceDown { msg: Some(msg) })
}

fn one(msg: splice_down::Msg) -> DownStream {
    Box::pin(futures::stream::once(async move { down(msg) }))
}

/// TLS versions and cipher names as the `&'static str`s [`TlsInfo`] keeps:
/// a handful of distinct values, interned once each.
fn intern(s: &str) -> &'static str {
    static SEEN: OnceLock<Mutex<Vec<&'static str>>> = OnceLock::new();
    let mut seen = SEEN
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if let Some(v) = seen.iter().find(|v| **v == s) {
        return v;
    }
    if seen.len() >= 64 {
        return "unknown";
    }
    let v: &'static str = Box::leak(s.to_owned().into_boxed_str());
    seen.push(v);
    v
}

fn tls_info(t: &SpliceTls) -> TlsInfo {
    TlsInfo {
        version: intern(&t.version),
        cipher: intern(&t.cipher),
        sni: (!t.sni.is_empty()).then(|| t.sni.clone()),
        client_cert: t.client_cert,
        client_dn: (!t.client_subject.is_empty() || !t.client_issuer.is_empty()).then(|| {
            sprig_auth::mysql::ClientDn {
                subject: t.client_subject.clone(),
                issuer: t.client_issuer.clone(),
            }
        }),
    }
}

/// Whether this node should serve `tenant`'s connection that has already
/// been handed on `hops` times, or send it to the leader it knows.
fn redirect_to(tenant: u64, hops: u32) -> Option<u64> {
    if hops + 1 >= super::MAX_HOPS {
        return None;
    }
    let me = node_engine::raft_id_from_env()?;
    match node_engine::tenant_leader(tenant) {
        UnitLeader::Node(n) if n != me => Some(n),
        _ => None,
    }
}

#[tonic::async_trait]
impl NodeForward for ForwardService {
    type SpliceStream = DownStream;

    async fn splice(
        &self,
        req: tonic::Request<tonic::Streaming<SpliceUp>>,
    ) -> std::result::Result<tonic::Response<Self::SpliceStream>, tonic::Status> {
        let mut up = req.into_inner();
        let Some(SpliceUp {
            msg: Some(splice_up::Msg::Open(open)),
        }) = up.message().await?
        else {
            return Err(tonic::Status::invalid_argument("the first frame must open"));
        };
        let refuse = |code: SprigCode, message: String| {
            Ok(tonic::Response::new(one(splice_down::Msg::Refused(
                SpliceRefused {
                    code: u32::from(code.number()),
                    message,
                },
            ))))
        };
        let Some(host) = super::host() else {
            return refuse(
                SprigCode::ForwardFailed,
                "the node is still starting; retry".into(),
            );
        };
        if let Some(n) = redirect_to(open.tenant_id, open.hops) {
            return Ok(tonic::Response::new(one(splice_down::Msg::Redirect(n))));
        }
        let peer: SocketAddr = open
            .client_addr
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        let (tx, rx) = mpsc::channel::<Down>(16);
        let _ = tx.send(down(splice_down::Msg::Accepted(true))).await;
        let stream = Spliced::new(
            Box::pin(up),
            Bytes::from(open.prelude),
            tx,
            open.tls.as_ref().map(tls_info),
            open.resume_json,
            open.from_node,
        );
        if let Err(e) = host
            .listeners
            .serve_forwarded(&open.listener, stream, peer)
            .await
        {
            return refuse(e.code().unwrap_or(SprigCode::ForwardFailed), e.to_string());
        }
        tracing::debug!(tenant = open.tenant_id, from = open.from_node, listener = %open.listener, %peer, "serving a forwarded connection");
        if !super::coalesce() {
            return Ok(tonic::Response::new(Box::pin(futures::stream::unfold(
                rx,
                |mut rx| async move { rx.recv().await.map(|m| (m, rx)) },
            ))));
        }
        Ok(tonic::Response::new(coalesced(rx)))
    }
}

/// The most bytes merged into one down frame.
const COALESCE_MAX: usize = 64 * 1024;

/// The down stream: data the front end wrote while the previous frame was
/// being sent goes out as one frame (fewer gRPC messages for a result the
/// front end writes packet by packet), never waiting for more.
fn coalesced(rx: mpsc::Receiver<Down>) -> DownStream {
    Box::pin(futures::stream::unfold(
        (rx, None::<Down>),
        |(mut rx, held)| async move {
            let mut m = match held {
                Some(m) => m,
                None => rx.recv().await?,
            };
            let mut next = None;
            if let Ok(SpliceDown {
                msg: Some(splice_down::Msg::Data(d)),
            }) = &mut m
            {
                while d.len() < COALESCE_MAX {
                    match rx.try_recv() {
                        Ok(Ok(SpliceDown {
                            msg: Some(splice_down::Msg::Data(more)),
                        })) => d.extend_from_slice(&more),
                        Ok(other) => {
                            next = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
            }
            Some((m, (rx, next)))
        },
    ))
}

#[cfg(test)]
mod coalesce_tests {
    use futures::StreamExt as _;

    use super::*;

    fn data(b: &[u8]) -> Down {
        down(splice_down::Msg::Data(b.to_vec()))
    }

    #[tokio::test]
    async fn waiting_data_goes_out_as_one_frame_in_order() {
        let (tx, rx) = mpsc::channel(16);
        tx.send(down(splice_down::Msg::Accepted(true)))
            .await
            .unwrap();
        for p in [&b"ab"[..], b"cd", b"ef"] {
            tx.send(data(p)).await.unwrap();
        }
        drop(tx);
        let got: Vec<Down> = coalesced(rx).collect().await;
        assert_eq!(got.len(), 2);
        assert!(matches!(
            &got[0],
            Ok(SpliceDown {
                msg: Some(splice_down::Msg::Accepted(true))
            })
        ));
        let Ok(SpliceDown {
            msg: Some(splice_down::Msg::Data(d)),
        }) = &got[1]
        else {
            panic!("no data")
        };
        assert_eq!(d, b"abcdef");
    }
}
