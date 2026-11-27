//! MongoDB connections handed to the node that serves their tenant
//! ([`crate::forward`]).
//!
//! The hand-off happens at the first message that names a user: a `hello`
//! with `speculativeAuthenticate` (what current drivers send first) or a
//! `saslStart`, before it is answered. The receiving node replays that
//! message; what earlier `hello`s settled (compressors, client metadata,
//! the DC hint) goes along as the resume.

use bson::{Bson, Document, RawDocumentBuf};
use serde::{Deserialize, Serialize};
use sprig_proto_mongo::codec::{Compressor, Frame};

use super::conn::Conn;
use crate::frontend::{ClientConn, MaybeTls, Protocol};

/// What earlier `hello`s settled, as JSON in the hand-off.
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct MongoResume {
    compressors: Vec<String>,
    /// The client metadata as extended JSON.
    client_meta: Option<serde_json::Value>,
    dc: Option<String>,
    restore: bool,
    hello_done: bool,
}

/// The resume of `st`.
pub(crate) fn resume_json(st: &Conn) -> String {
    let r = MongoResume {
        compressors: st.compressors.iter().map(|c| c.name().to_owned()).collect(),
        client_meta: st
            .client_meta
            .as_ref()
            .map(|d| Bson::Document(d.clone()).into_relaxed_extjson()),
        dc: st.dc.clone(),
        restore: st.restore,
        hello_done: st.hello_done,
    };
    serde_json::to_string(&r).unwrap_or_default()
}

/// Applies a handed-over connection's resume to `st`.
pub(crate) fn resume(conn: &ClientConn, st: &mut Conn) {
    let MaybeTls::Forwarded(f) = &conn.stream else {
        return;
    };
    let Some(r) = f
        .take_resume()
        .and_then(|j| serde_json::from_str::<MongoResume>(&j).ok())
    else {
        return;
    };
    st.compressors = r
        .compressors
        .iter()
        .filter_map(|n| Compressor::from_name(n))
        .collect();
    st.client_meta = r
        .client_meta
        .and_then(|v| Bson::try_from(v).ok())
        .and_then(|b| b.as_document().cloned());
    st.client_dc = crate::dc::hint("mongodb", r.dc.as_deref()).known();
    st.dc = r.dc;
    st.restore = r.restore;
    st.hello_done = r.hello_done;
}

/// The user a SASL payload names: SCRAM's client-first (`n,,n=user,r=…`),
/// or PLAIN's `authzid \0 user \0 password`.
fn sasl_user(mechanism: &str, payload: &[u8]) -> Option<String> {
    if mechanism.eq_ignore_ascii_case("PLAIN") {
        let mut parts = payload.split(|b| *b == 0);
        let _authz = parts.next()?;
        return Some(String::from_utf8_lossy(parts.next()?).into_owned());
    }
    if !mechanism.starts_with("SCRAM-") {
        return None;
    }
    let text = std::str::from_utf8(payload).ok()?;
    // gs2 header: two fields, then the bare message.
    let bare = text.splitn(3, ',').nth(2)?;
    let user = bare.split(',').find_map(|f| f.strip_prefix("n="))?;
    Some(user.replace("=2C", ",").replace("=3D", "="))
}

fn payload_of(d: &Document) -> Option<Vec<u8>> {
    match d.get("payload")? {
        Bson::Binary(b) => Some(b.bytes.clone()),
        Bson::String(s) => {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.decode(s).ok()
        }
        _ => None,
    }
}

fn user_in(cmd: &Document) -> Option<String> {
    let name = cmd.keys().next()?.as_str();
    let spec = match name {
        "saslStart" => cmd,
        "hello" | "isMaster" | "ismaster" => cmd.get_document("speculativeAuthenticate").ok()?,
        _ => return None,
    };
    sasl_user(spec.get_str("mechanism").ok()?, &payload_of(spec)?)
}

fn doc_of(raw: &RawDocumentBuf) -> Option<Document> {
    raw.to_document().ok()
}

/// The user a client message logs in as, if it is a login message.
pub(crate) fn login_user(frame: &Frame) -> Option<String> {
    match frame {
        Frame::Query(q) => user_in(&doc_of(&q.query)?),
        Frame::Msg(m) => user_in(&doc_of(&m.body)?),
        Frame::Reply(_) => None,
    }
}

/// The node to hand this login to and its tenant, if not this one.
pub(crate) async fn target(
    shared: &super::Shared,
    sni: Option<&str>,
    user: &str,
) -> Option<(u64, u64)> {
    let tenant = shared
        .d
        .login
        .tenant_of(user, sni, sprig_auth::admin::MAX_NAME)
        .await?;
    match crate::forward::route(tenant.0, Protocol::Mongo).await {
        crate::forward::Route::Node(n) => Some((n, tenant.0)),
        crate::forward::Route::Here => {
            crate::forward::count_direct(Protocol::Mongo);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sasl_payloads_name_their_user() {
        assert_eq!(
            sasl_user("SCRAM-SHA-256", b"n,,n=acme.app,r=abc").as_deref(),
            Some("acme.app")
        );
        assert_eq!(
            sasl_user("SCRAM-SHA-1", b"n,,n=a=2Cb=3Dc,r=x").as_deref(),
            Some("a,b=c")
        );
        assert_eq!(
            sasl_user("PLAIN", b"\0acme.app\0secret").as_deref(),
            Some("acme.app")
        );
        assert_eq!(sasl_user("MONGODB-X509", b"anything"), None);
        let hello = bson::doc! {
            "hello": 1,
            "speculativeAuthenticate": {
                "saslStart": 1,
                "mechanism": "SCRAM-SHA-256",
                "payload": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: b"n,,n=t1.u,r=z".to_vec() },
            },
        };
        assert_eq!(user_in(&hello).as_deref(), Some("t1.u"));
        assert_eq!(user_in(&bson::doc! {"hello": 1}), None);
        assert_eq!(user_in(&bson::doc! {"find": "c"}), None);
    }
}
