//! MySQL and MariaDB connections handed to the node that serves their
//! tenant ([`crate::forward`]).
//!
//! The accepting node has sent its greeting (nonce, connection id,
//! capabilities), done TLS if the client asked, and read the login packet;
//! the receiving node continues from that packet with the same nonce, so
//! the client's scramble checks there, and the same capabilities.

use serde::{Deserialize, Serialize};
use sprig_proto_mysql::handshake::{NONCE_LEN, TlsPolicy, write_packet};
use sprig_proto_mysql::{Caps, ErrPacket};
use tokio::net::TcpStream;

use super::{Shared, protocol_of};
use crate::frontend::{ClientConn, MaybeTls};

/// What the accepting node had established, as JSON in the hand-off.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MysqlResume {
    /// The greeting's nonce.
    pub(crate) nonce: [u8; NONCE_LEN],
    /// The capabilities the greeting offered.
    pub(crate) server_caps: u64,
    /// The sequence number of the client's login packet.
    pub(crate) seq: u8,
    /// The listener's TLS policy at the accepting node: 0 off, 1 optional,
    /// 2 required.
    pub(crate) policy: u8,
    /// The client switched to TLS there before its login packet.
    pub(crate) tls: bool,
}

impl MysqlResume {
    /// `seq` is the sequence number after the login packet was read.
    pub(crate) fn new(nonce: [u8; NONCE_LEN], server: Caps, seq: u8, policy: TlsPolicy) -> Self {
        Self {
            nonce,
            server_caps: server.bits(),
            seq: seq.wrapping_sub(1),
            policy: match policy {
                TlsPolicy::Off => 0,
                TlsPolicy::Optional => 1,
                TlsPolicy::Required => 2,
            },
            tls: false,
        }
    }

    /// The accepting listener's TLS policy.
    pub(crate) fn policy(&self) -> TlsPolicy {
        match self.policy {
            0 => TlsPolicy::Off,
            1 => TlsPolicy::Optional,
            _ => TlsPolicy::Required,
        }
    }
}

/// The resume of a connection handed here, if this is one.
pub(crate) fn resume_of(conn: &ClientConn) -> Option<MysqlResume> {
    let MaybeTls::Forwarded(f) = &conn.stream else {
        return None;
    };
    serde_json::from_str(&f.take_resume()?).ok()
}

/// The node to hand this login to and the login's tenant, if not this one.
pub(crate) async fn target(shared: &Shared, conn: &ClientConn, user: &[u8]) -> Option<(u64, u64)> {
    let persona = shared.cfg.persona;
    let sni = conn.tls_info().and_then(|t| t.sni);
    let tenant = shared
        .cfg
        .login
        .tenant_of(
            &String::from_utf8_lossy(user),
            sni.as_deref(),
            sprig_auth::mysql::max_user_len(persona),
        )
        .await?;
    match crate::forward::route(tenant.0, protocol_of(persona)).await {
        crate::forward::Route::Node(n) => Some((n, tenant.0)),
        crate::forward::Route::Here => None,
    }
}

/// A login to hand over once the login phase has returned.
pub(crate) struct Pending {
    pub(crate) conn: ClientConn,
    /// The node and the tenant.
    pub(crate) to: (u64, u64),
    pub(crate) resume: MysqlResume,
    pub(crate) login_packet: bytes::Bytes,
    pub(crate) caps: Caps,
}

/// Hands the connection to the node `p.to` names and relays it until it
/// ends. When no node takes it, the client gets the error (39788,
/// retryable) in place of the login's answer.
pub(crate) async fn hand_over(shared: &Shared, p: Pending) {
    let Pending {
        mut conn,
        to: (to, tenant),
        mut resume,
        login_packet,
        caps,
    } = p;
    let login_packet: &[u8] = &login_packet;
    let persona = shared.cfg.persona;
    let tls = conn.tls_info();
    resume.tls = tls.is_some();
    let mut prelude = Vec::with_capacity(login_packet.len() + 4);
    let mut s = resume.seq;
    if write_packet(&mut prelude, &mut s, login_packet)
        .await
        .is_err()
    {
        return;
    }
    let h = crate::forward::Handoff {
        listener: conn.listener.name,
        protocol: protocol_of(persona),
        peer: conn.peer,
        tls,
        tenant,
        resume: serde_json::to_string(&resume).unwrap_or_default(),
        prelude: prelude.into(),
        to,
    };
    let stream: &mut MaybeTls<TcpStream> = &mut conn.stream;
    if let Err(e) = crate::forward::splice(&mut *stream, h).await {
        tracing::debug!(error = %e, to, "MySQL connection not handed over");
        let mut seq = resume.seq.wrapping_add(1);
        let p = ErrPacket::from_error(persona, &e);
        let _ = write_packet(&mut conn.stream, &mut seq, &p.encode(caps)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_resume_keeps_the_greeting() {
        let r = MysqlResume::new([7; NONCE_LEN], Caps::PROTOCOL_41, 2, TlsPolicy::Required);
        assert_eq!(r.seq, 1, "the login packet's own sequence number");
        let back: MysqlResume = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(back.nonce, [7; NONCE_LEN]);
        assert_eq!(Caps::from_bits_retain(back.server_caps), Caps::PROTOCOL_41);
        assert_eq!(back.policy(), TlsPolicy::Required);
    }
}
