//! Change streams of tenants homed in another data centre.
//!
//! ```text
//! open at a gateway outside the tenant's home DC:
//!   GatewayPeer.WatchChanges on a ready gateway of the home DC (each in
//!   turn until one answers), with the client's Authorization header, the
//!   tenant for credentials that don't name it, the client's address and TLS
//!   the home gateway authenticates the caller again (credentials are decided
//!   in the home DC, also under residency) and opens the stream there, with
//!   its limits; its first message says opened, or carries the error
//!   every event and heartbeat comes back as it is, and this gateway frames
//!   it for its own client (gRPC, SSE, WebSocket, NDJSON)
//! backpressure: this gateway reads the next message only once its client
//!   took the last, so a slow client fills the home gateway's buffer and
//!   gets its 39790 there
//! the home gateway gone: 39788, and the client resumes from its last token
//! ```
//!
//! A forwarded stream is served where it lands, never forwarded again.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use sprig_api::proto::internal::{WatchChangesEvent, WatchChangesRequest};
use sprig_common::ids::{DcId, TenantId};
use sprig_common::{Result, SprigCode, SprigError};
use tokio::sync::mpsc;

use super::{Core, WatchEvent, WatchSpec};
use crate::http::ConnInfo;
use crate::http::sql::Caller;

/// How long the home gateway may take to open the stream.
const OPEN_TIMEOUT: Duration = Duration::from_secs(30);

/// Messages buffered between the peer call and the client.
const RELAY_BUFFER: usize = 64;

/// The tenant's home DC when it isn't this gateway's.
///
/// # Errors
/// The directory's.
pub(super) async fn remote_home(
    svc: &crate::tenant::TenantServices,
    tenant: TenantId,
) -> Result<Option<DcId>> {
    Ok(sprig_shard::home::read_home(&*svc.global, tenant)
        .await?
        .map(|e| e.home_dc)
        .filter(|h| *h != svc.dc))
}

/// The tenant to send with the credentials: none when a Basic user name
/// already starts with it, else the caller's tenant (what a host name or
/// `x-sprig-tenant` named here).
fn tenant_hint(caller: &Caller) -> String {
    let tenant = caller.principal.tenant_name.clone().unwrap_or_default();
    let named = caller
        .authorization
        .as_deref()
        .and_then(|a| {
            a.get(..6)
                .filter(|s| s.eq_ignore_ascii_case("basic "))
                .map(|_| a[6..].trim())
        })
        .and_then(|b| base64::engine::general_purpose::STANDARD.decode(b).ok())
        .and_then(|b| String::from_utf8(b).ok())
        .is_some_and(|s| {
            s.split_once(':')
                .map_or(s.as_str(), |(u, _)| u)
                .split_once('.')
                .is_some_and(|(t, _)| t == tenant)
        });
    if named { String::new() } else { tenant }
}

fn request(caller: &Caller, spec: &WatchSpec) -> WatchChangesRequest {
    let tls = caller.tls.as_ref();
    let dn = tls
        .filter(|t| t.client_cert)
        .and_then(|t| t.client_dn.clone())
        .unwrap_or_default();
    WatchChangesRequest {
        authorization: caller.authorization.clone().unwrap_or_default(),
        tenant: tenant_hint(caller),
        client_addr: caller.peer.to_string(),
        tls: tls.is_some(),
        sni: tls.and_then(|t| t.sni.clone()).unwrap_or_default(),
        client_subject: dn.subject,
        client_issuer: dn.issuer,
        database: spec.database.clone(),
        objects: spec.objects.clone(),
        start_after: spec.start_after.clone(),
        start_at_hlc: spec.start_at_hlc,
        full_document: spec.full_document,
        space: spec.space.clone().unwrap_or_default(),
    }
}

fn error_of(json: &str) -> SprigError {
    match serde_json::from_str::<sprig_api::status::ErrorBody>(json) {
        Ok(b) => sprig_api::status::from_rest(&b),
        Err(e) => SprigError::internal(format!("relayed change stream error: {e}")),
    }
}

fn event_of(m: WatchChangesEvent) -> WatchEvent {
    WatchEvent {
        resume_token: m.resume_token,
        commit_hlc: m.commit_hlc,
        object: m.object,
        op: m.op,
        key: m.key,
        after: m.after,
        update: m.update,
        ddl: m.ddl,
    }
}

fn lost(home: &DcId, e: impl std::fmt::Display) -> SprigError {
    SprigError::new(
        SprigCode::ForwardFailed,
        format!(
            "ER_SPRIG_FORWARD_FAILED: the change stream's gateway in {home} is gone ({e}); \
             resume from the last token"
        ),
    )
}

/// Opens the stream on a gateway of `home` and relays it (module docs).
///
/// # Errors
/// The home gateway's error (authentication, privileges, limits, tokens);
/// 39788 when no gateway of `home` answers.
pub(super) async fn forward(
    core: &Arc<Core>,
    caller: &Caller,
    spec: &WatchSpec,
    home: &DcId,
) -> Result<mpsc::Receiver<Result<WatchEvent>>> {
    let peers = core.engine.peers.as_ref().ok_or_else(|| {
        SprigError::new(
            SprigCode::ForwardFailed,
            format!(
                "ER_SPRIG_FORWARD_FAILED: the tenant is homed in {home}, and this gateway has no \
                 peer service to relay its change stream"
            ),
        )
    })?;
    let req = request(caller, spec);
    let gateways = peers.gateways_in(home).await?;
    let mut last = None;
    for rec in &gateways {
        let mut s = match peers.watch_changes(rec, req.clone()).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(%home, gateway = %rec.gateway_id, error = %e, "change stream: home gateway unreachable; trying the next");
                last = Some(e);
                continue;
            }
        };
        let first = match tokio::time::timeout(OPEN_TIMEOUT, s.message()).await {
            Ok(Ok(Some(m))) => m,
            Ok(Ok(None)) => {
                last = Some(lost(home, "the stream ended before it opened"));
                continue;
            }
            Ok(Err(status)) => {
                last = Some(lost(home, status.message()));
                continue;
            }
            Err(_) => {
                last = Some(lost(home, "no answer within 30 s"));
                continue;
            }
        };
        if !first.error_json.is_empty() {
            metrics::counter!("sprig_api_change_streams_forwarded_total", "outcome" => "refused")
                .increment(1);
            return Err(error_of(&first.error_json));
        }
        metrics::counter!("sprig_api_change_streams_forwarded_total", "outcome" => "opened")
            .increment(1);
        tracing::debug!(%home, gateway = %rec.gateway_id, "change stream relayed from the tenant's home DC");
        let (tx, rx) = mpsc::channel(RELAY_BUFFER);
        let home = home.clone();
        tokio::spawn(async move {
            loop {
                let item = match s.message().await {
                    Ok(Some(m)) if m.error_json.is_empty() => Ok(event_of(m)),
                    Ok(Some(m)) => Err(error_of(&m.error_json)),
                    // The home gateway ended the stream.
                    Ok(None) => break,
                    Err(status) => Err(lost(&home, status.message())),
                };
                let end = item.is_err();
                // The client is gone, or the stream ended: dropping the
                // call ends it in the home DC too.
                if tx.send(item).await.is_err() || end {
                    break;
                }
            }
        });
        return Ok(rx);
    }
    metrics::counter!("sprig_api_change_streams_forwarded_total", "outcome" => "unavailable")
        .increment(1);
    Err(last.unwrap_or_else(|| lost(home, "no ready gateway there")))
}

/// Serves `GatewayPeer.WatchChanges` (the home DC's side): authenticates
/// the caller again, opens the stream here, and sends `opened` or the
/// error, then every event.
pub async fn serve(
    core: &Arc<Core>,
    req: WatchChangesRequest,
) -> mpsc::Receiver<WatchChangesEvent> {
    let (tx, rx) = mpsc::channel(RELAY_BUFFER);
    let core = Arc::clone(core);
    tokio::spawn(async move {
        let opened = async {
            let peer = req
                .client_addr
                .parse()
                .unwrap_or_else(|_| std::net::SocketAddr::from(([0, 0, 0, 0], 0)));
            let client_cert = !req.client_subject.is_empty();
            let info = ConnInfo {
                peer,
                tls: req.tls.then(|| crate::frontend::TlsInfo {
                    version: "relayed",
                    cipher: "relayed",
                    sni: (!req.sni.is_empty()).then(|| req.sni.clone()),
                    client_cert,
                    client_dn: client_cert.then(|| sprig_auth::mysql::ClientDn {
                        subject: req.client_subject.clone(),
                        issuer: req.client_issuer.clone(),
                    }),
                }),
                listener: "peer",
                generation: 0,
            };
            let auth = (!req.authorization.is_empty()).then(|| req.authorization.clone());
            let tenant = (!req.tenant.is_empty()).then(|| req.tenant.clone());
            let caller = core.caller(&info, auth, tenant, None).await?;
            let spec = WatchSpec {
                database: req.database.clone(),
                objects: req.objects.clone(),
                start_after: req.start_after.clone(),
                start_at_hlc: req.start_at_hlc,
                full_document: req.full_document,
                space: (!req.space.is_empty()).then(|| req.space.clone()),
            };
            super::open_here(&core, caller, spec).await
        }
        .await;
        let mut events = match opened {
            Ok(rx) => rx,
            Err(e) => {
                let _ = tx.send(error_event(&e)).await;
                return;
            }
        };
        if tx
            .send(WatchChangesEvent {
                opened: true,
                ..WatchChangesEvent::default()
            })
            .await
            .is_err()
        {
            return;
        }
        while let Some(item) = events.recv().await {
            let (m, end) = match item {
                Ok(ev) => (
                    WatchChangesEvent {
                        resume_token: ev.resume_token,
                        commit_hlc: ev.commit_hlc,
                        object: ev.object,
                        op: ev.op,
                        key: ev.key,
                        after: ev.after,
                        update: ev.update,
                        ddl: ev.ddl,
                        ..WatchChangesEvent::default()
                    },
                    false,
                ),
                Err(e) => (error_event(&e), true),
            };
            // The relaying gateway gone: the stream ends here too.
            if tx.send(m).await.is_err() || end {
                break;
            }
        }
    });
    rx
}

fn error_event(e: &SprigError) -> WatchChangesEvent {
    let (_, body) = sprig_api::status::to_rest(e);
    WatchChangesEvent {
        error_json: serde_json::to_string(&body).unwrap_or_default(),
        ..WatchChangesEvent::default()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn errors_and_events_cross_unchanged() {
        let e = SprigError::new(SprigCode::StreamSlowConsumer, "slow");
        let m = error_event(&e);
        let back = error_of(&m.error_json);
        assert_eq!(back.code(), Some(SprigCode::StreamSlowConsumer));
        let back =
            error_of(&error_event(&SprigError::standard(1142, *b"42000", "denied")).error_json);
        assert_eq!(back.protocol().mysql_errno, 1142);
        let ev = event_of(WatchChangesEvent {
            resume_token: "ab".into(),
            commit_hlc: 7,
            object: "t".into(),
            op: "insert".into(),
            after: "{}".into(),
            ..WatchChangesEvent::default()
        });
        assert_eq!(ev.json()["op"], "insert");
        assert_eq!(ev.json()["commit_hlc"], "7");
    }
}
