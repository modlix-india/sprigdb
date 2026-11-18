//! Server auth plugins.
//!
//! This module moves bytes; a [`LoginFlow`] (the login decisions,
//! implemented by the gateway) decides. The plugins:
//!
//! - [`CachingSha2Password`] (both personas, the default): the empty password
//!   (an empty response or a single NUL, answered with the verdict at once),
//!   the fast path (`AuthMoreData 0x03` then the verdict), and the full path
//!   (`AuthMoreData 0x04`, then the clear-text password over TLS). Without TLS
//!   the full path is the RSA exchange: the client asks for the server's
//!   public key (`0x02`) and gets it in an `AuthMoreData`, or already has it
//!   (`--server-public-key-path`), and sends its password encrypted with
//!   RSA-OAEP. A clear-text password is never read without TLS: anything
//!   that isn't a ciphertext of the server's key is refused with
//!   [`Outcome::NeedsTls`], as is the whole path when the listener has no
//!   key ([`AuthIo::rsa`]).
//! - [`NativePassword`]: `mysql_native_password`, when the flow allows it.
//! - [`ClearPassword`]: `mysql_clear_password`, refused with 3159 without
//!   TLS.
//!
//! [`authenticate`] runs the exchange: it asks the flow which plugin the
//! login uses, switches to it with a fresh nonce when the client started
//! with another one (and always for `COM_CHANGE_USER`), then runs it to the
//! end. An unknown account takes exactly the same packets as a real one
//! that misses the fast cache: the flow fails it at the end, after the same
//! hash work (the decoy).
//!
//! The verdict ([`AuthVerdict`]) comes back to the caller, which sends the
//! OK packet (or the ERR) itself, since the OK carries session state.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use zeroize::Zeroizing;

use crate::error::{ErrPacket, StdErr};
use crate::handshake::{GREETING_PLUGIN, NONCE_LEN, PacketIo, new_nonce};
use crate::packets::{AuthMoreData, AuthSwitchRequest, Persona};
use crate::proto_error::ProtoResult;

/// `caching_sha2_password`.
pub const CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";
/// `mysql_native_password`.
pub const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";
/// `mysql_clear_password`.
pub const MYSQL_CLEAR_PASSWORD: &str = "mysql_clear_password";

/// `AuthMoreData` byte: the client asks for the server's RSA public key.
pub const REQUEST_PUBLIC_KEY: u8 = 0x02;
/// `AuthMoreData` byte: the fast path succeeded.
pub const FAST_AUTH_SUCCESS: u8 = 0x03;
/// `AuthMoreData` byte: send the password (full authentication).
pub const PERFORM_FULL_AUTHENTICATION: u8 = 0x04;

/// The scramble length of `caching_sha2_password` (SHA-256).
pub const SHA2_SCRAMBLE_LEN: usize = 32;

/// The mechanism a login uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    /// `caching_sha2_password`.
    CachingSha2,
    /// `mysql_native_password`.
    Native,
    /// `mysql_clear_password`.
    ClearText,
}

impl Method {
    /// The plugin name on the wire.
    #[must_use]
    pub fn plugin_name(self) -> &'static str {
        match self {
            Method::CachingSha2 => CACHING_SHA2_PASSWORD,
            Method::Native => MYSQL_NATIVE_PASSWORD,
            Method::ClearText => MYSQL_CLEAR_PASSWORD,
        }
    }

    /// The method a plugin name stands for, if the gateway implements it.
    #[must_use]
    pub fn from_plugin(name: &str) -> Option<Self> {
        match name {
            CACHING_SHA2_PASSWORD => Some(Method::CachingSha2),
            MYSQL_NATIVE_PASSWORD => Some(Method::Native),
            MYSQL_CLEAR_PASSWORD => Some(Method::ClearText),
            _ => None,
        }
    }

    /// The plugin that runs this method.
    #[must_use]
    pub fn plugin(self) -> &'static dyn ServerAuthPlugin {
        match self {
            Method::CachingSha2 => &CachingSha2Password,
            Method::Native => &NativePassword,
            Method::ClearText => &ClearPassword,
        }
    }
}

/// Which path an exchange took (the `path` label of
/// `sprig_mysql_auth_total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthPath {
    /// The empty password.
    Empty,
    /// `caching_sha2_password` fast path.
    Fast,
    /// `caching_sha2_password` full path over TLS.
    Full,
    /// `caching_sha2_password` full path without TLS: the RSA exchange.
    Rsa,
    /// `mysql_native_password`.
    Native,
    /// `mysql_clear_password`.
    Clear,
}

impl AuthPath {
    /// The metric label.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            AuthPath::Empty => "empty",
            AuthPath::Fast => "fast",
            AuthPath::Full => "full",
            AuthPath::Rsa => "rsa",
            AuthPath::Native => "native",
            AuthPath::Clear => "clear",
        }
    }
}

/// What the credential check found, before the login policy runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Outcome {
    /// The credential matched.
    Verified,
    /// The credential didn't match (or the account doesn't exist).
    WrongCredentials,
    /// The full path was needed without TLS, and the RSA exchange couldn't
    /// run: the listener has no key, or the client sent something other than
    /// a ciphertext of it (a clear-text password, never checked). The client
    /// should use TLS.
    NeedsTls,
}

/// The end of an exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthVerdict {
    /// Logged in; the caller sends the OK packet.
    Accept,
    /// Refused; the caller sends this ERR packet and closes.
    Refuse(ErrPacket),
}

/// One login conversation's decisions: the
/// plugins call these and never learn whether the account exists.
///
/// Implementations must cost the same for an unknown account as for a real
/// one on every path: [`LoginFlow::check_password`] runs a decoy hash at
/// the default cost, and [`LoginFlow::method`] picks for an unknown account
/// what it picks for an account with only a `caching_sha2_password`
/// verifier.
#[async_trait]
pub trait LoginFlow: Send {
    /// The mechanism this login uses, given the plugin the client named in
    /// its response (`None` if it named none).
    fn method(&self, client_plugin: Option<&str>) -> Method;

    /// The `caching_sha2_password` fast path: does `scramble` match the
    /// account's fast-cache entry for `nonce`? `false` on a miss or a
    /// mismatch; the full path follows either way.
    fn check_fast(&mut self, nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool;

    /// `mysql_native_password`: does `scramble` (empty for the empty
    /// password) match the account's native verifier for `nonce`?
    fn check_native(&mut self, nonce: &[u8; NONCE_LEN], scramble: &[u8]) -> bool;

    /// The full path: does the clear-text `password` match? CPU-heavy:
    /// implementations run it off the async runtime, and fill the fast
    /// cache on success.
    async fn check_password(&mut self, password: Zeroizing<Vec<u8>>) -> bool;

    /// The verdict: the policy checks after the credential check,
    /// and the ERR packet (1045, 3118, ...) on refusal.
    async fn decide(&mut self, outcome: Outcome, path: AuthPath) -> AuthVerdict;
}

/// The server's RSA key pair for `caching_sha2_password`'s full path
/// without TLS. The client encrypts `password || 0x00`, combined by XOR with the
/// nonce repeated, with RSA-OAEP (SHA-1, MGF1-SHA-1, empty label), as
/// MySQL 8.0.5 and later and MariaDB's Connector/C do.
#[async_trait]
pub trait ServerRsaKey: Send + Sync {
    /// The public key as PEM (`-----BEGIN PUBLIC KEY-----`, X.509
    /// `SubjectPublicKeyInfo`), sent to a client that asks for it.
    fn public_pem(&self) -> Bytes;

    /// The modulus length in bytes: every ciphertext is exactly this long.
    fn size(&self) -> usize;

    /// The RSA-OAEP plaintext of `ciphertext`, or `None` when it isn't a
    /// valid ciphertext of this key. CPU-bound: implementations run it off
    /// the async runtime.
    async fn decrypt(&self, ciphertext: Bytes) -> Option<Zeroizing<Vec<u8>>>;
}

/// The connection an exchange runs on.
pub struct AuthIo<'a> {
    /// The packet transport.
    pub io: &'a mut dyn PacketIo,
    /// The listener's persona.
    pub persona: Persona,
    /// Whether the connection is TLS.
    pub tls: bool,
    /// The nonce the client's scramble answers; replaced by a fresh one on
    /// an `AuthSwitchRequest`.
    pub nonce: [u8; NONCE_LEN],
    /// The server's RSA key for the full path without TLS; `None` refuses
    /// that path with [`Outcome::NeedsTls`]. Unused over TLS.
    pub rsa: Option<Arc<dyn ServerRsaKey>>,
}

impl std::fmt::Debug for AuthIo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthIo")
            .field("persona", &self.persona)
            .field("tls", &self.tls)
            .field("rsa", &self.rsa.is_some())
            .finish_non_exhaustive()
    }
}

impl AuthIo<'_> {
    async fn more_data(&mut self, byte: u8) -> ProtoResult<()> {
        let p = AuthMoreData {
            data: Bytes::copy_from_slice(&[byte]),
        };
        self.io.write_packet(p.encode()).await
    }
}

/// The server side of one auth plugin.
#[async_trait]
pub trait ServerAuthPlugin: Send + Sync {
    /// The plugin name.
    fn name(&self) -> &'static str;

    /// Runs the exchange from the client's first response `first` to the
    /// verdict. The caller sends the final OK or ERR packet.
    async fn run(
        &self,
        io: &mut AuthIo<'_>,
        first: Bytes,
        flow: &mut dyn LoginFlow,
    ) -> ProtoResult<AuthVerdict>;
}

/// Strips one trailing NUL: clear-text passwords arrive NUL-terminated.
fn password_of(packet: &[u8]) -> Zeroizing<Vec<u8>> {
    let p = packet.strip_suffix(&[0]).unwrap_or(packet);
    Zeroizing::new(p.to_vec())
}

/// Whether a scramble response stands for the empty password: no bytes,
/// or the single NUL byte that libmysqlclient's `caching_sha2_password`
/// sends for an empty password (the `mysql` CLI, Connector/C and every
/// driver built on them). MySQL's server plugin accepts both forms
/// (`sha2_password.cc`: `!pkt_len || (pkt_len == 1 && *pkt == 0)`); a real
/// scramble is 20 or 32 bytes, so the NUL can't be one.
fn is_empty_response(r: &[u8]) -> bool {
    r.is_empty() || r == [0]
}

/// `caching_sha2_password`.
#[derive(Debug, Clone, Copy, Default)]
pub struct CachingSha2Password;

#[async_trait]
impl ServerAuthPlugin for CachingSha2Password {
    fn name(&self) -> &'static str {
        CACHING_SHA2_PASSWORD
    }

    async fn run(
        &self,
        io: &mut AuthIo<'_>,
        first: Bytes,
        flow: &mut dyn LoginFlow,
    ) -> ProtoResult<AuthVerdict> {
        // Step 2: the empty password (no bytes, or libmysqlclient's single
        // NUL): the verdict at once, no AuthMoreData, for every account.
        if is_empty_response(&first) {
            let ok = flow.check_password(Zeroizing::new(Vec::new())).await;
            return Ok(flow.decide(verified(ok), AuthPath::Empty).await);
        }
        // Step 3: the fast path. A miss or a mismatch falls through.
        if first.len() == SHA2_SCRAMBLE_LEN && flow.check_fast(&io.nonce, &first) {
            io.more_data(FAST_AUTH_SUCCESS).await?;
            return Ok(flow.decide(Outcome::Verified, AuthPath::Fast).await);
        }
        // Step 4: the full path.
        io.more_data(PERFORM_FULL_AUTHENTICATION).await?;
        let reply = io.io.read_packet().await?;
        if !io.tls {
            return rsa_exchange(io, reply, flow).await;
        }
        let ok = flow.check_password(password_of(&reply)).await;
        Ok(flow.decide(verified(ok), AuthPath::Full).await)
    }
}

/// The full path without TLS, from the client's reply to `0x04`: the
/// public-key request (`0x02`) answered with the PEM key, then the
/// ciphertext; or the ciphertext at once (the client has the key).
///
/// A reply of any other length (a clear-text password, a key of another
/// size) is refused with [`Outcome::NeedsTls`] and never checked. A
/// ciphertext that doesn't decrypt to `password || 0x00` (combined by XOR with the
/// nonce) costs the same hash work as a wrong password and gets the same
/// error, so the answer doesn't tell a padding failure from a wrong
/// password.
async fn rsa_exchange(
    io: &mut AuthIo<'_>,
    reply: Bytes,
    flow: &mut dyn LoginFlow,
) -> ProtoResult<AuthVerdict> {
    let Some(key) = io.rsa.clone() else {
        return Ok(flow.decide(Outcome::NeedsTls, AuthPath::Rsa).await);
    };
    let ciphertext = if reply.as_ref() == [REQUEST_PUBLIC_KEY] {
        let p = AuthMoreData {
            data: key.public_pem(),
        };
        io.io.write_packet(p.encode()).await?;
        io.io.read_packet().await?
    } else {
        reply
    };
    if ciphertext.len() != key.size() {
        return Ok(flow.decide(Outcome::NeedsTls, AuthPath::Rsa).await);
    }
    let password = match key.decrypt(ciphertext).await {
        Some(plain) => unscramble(&plain, &io.nonce),
        None => None,
    };
    let Some(password) = password else {
        // A random password: the decoy's hash work, never a match.
        let _ = flow
            .check_password(Zeroizing::new(new_nonce()?.to_vec()))
            .await;
        return Ok(flow.decide(Outcome::WrongCredentials, AuthPath::Rsa).await);
    };
    let ok = flow.check_password(password).await;
    Ok(flow.decide(verified(ok), AuthPath::Rsa).await)
}

/// The password in an RSA plaintext: combined by XOR with the nonce repeated, then
/// its terminating NUL removed. `None` without the NUL (MySQL refuses that
/// too).
fn unscramble(plain: &[u8], nonce: &[u8; NONCE_LEN]) -> Option<Zeroizing<Vec<u8>>> {
    let mut p: Zeroizing<Vec<u8>> = Zeroizing::new(
        plain
            .iter()
            .zip(nonce.iter().cycle())
            .map(|(b, n)| b ^ n)
            .collect(),
    );
    if p.last() != Some(&0) {
        return None;
    }
    p.pop();
    Some(p)
}

/// `mysql_native_password`.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativePassword;

#[async_trait]
impl ServerAuthPlugin for NativePassword {
    fn name(&self) -> &'static str {
        MYSQL_NATIVE_PASSWORD
    }

    async fn run(
        &self,
        io: &mut AuthIo<'_>,
        first: Bytes,
        flow: &mut dyn LoginFlow,
    ) -> ProtoResult<AuthVerdict> {
        let (scramble, path) = if is_empty_response(&first) {
            (&[][..], AuthPath::Empty)
        } else {
            (&first[..], AuthPath::Native)
        };
        let ok = flow.check_native(&io.nonce, scramble);
        Ok(flow.decide(verified(ok), path).await)
    }
}

/// `mysql_clear_password`: accepted only over TLS.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClearPassword;

#[async_trait]
impl ServerAuthPlugin for ClearPassword {
    fn name(&self) -> &'static str {
        MYSQL_CLEAR_PASSWORD
    }

    async fn run(
        &self,
        io: &mut AuthIo<'_>,
        first: Bytes,
        flow: &mut dyn LoginFlow,
    ) -> ProtoResult<AuthVerdict> {
        if !io.tls {
            return Ok(AuthVerdict::Refuse(ErrPacket::std(
                io.persona,
                StdErr::SecureTransportRequired,
            )));
        }
        let ok = flow.check_password(password_of(&first)).await;
        Ok(flow.decide(verified(ok), AuthPath::Clear).await)
    }
}

fn verified(ok: bool) -> Outcome {
    if ok {
        Outcome::Verified
    } else {
        Outcome::WrongCredentials
    }
}

/// Runs a whole exchange: picks the method ([`LoginFlow::method`]),
/// switches the client to it with a fresh nonce when it started with
/// another plugin, when `force_switch` is set (`COM_CHANGE_USER`), or when
/// the client's empty response is libmysqlclient's placeholder (below),
/// then runs the plugin.
///
/// libmysqlclient hides the greeting's nonce from a plugin other than the
/// greeting's (`--default-auth=mysql_native_password`, since the greeting
/// names `caching_sha2_password`): it sends an empty response naming its
/// plugin and waits for a nonce, which MySQL 8.4 sends in an
/// `AuthSwitchRequest`. So an empty response under a plugin other than the
/// greeting's is answered with a switch, not taken as the empty password;
/// a client that really has the empty password answers the switch with an
/// empty response again.
///
/// `client_plugin` and `first` are the plugin name and auth response from
/// the handshake response (or `COM_CHANGE_USER`); `io.nonce` is the nonce
/// the client answered.
pub async fn authenticate(
    io: &mut AuthIo<'_>,
    client_plugin: Option<&str>,
    first: Bytes,
    force_switch: bool,
    flow: &mut dyn LoginFlow,
) -> ProtoResult<AuthVerdict> {
    let method = flow.method(client_plugin);
    let placeholder =
        first.is_empty() && client_plugin != Some(GREETING_PLUGIN) && method != Method::ClearText;
    let mut first = first;
    if force_switch || placeholder || client_plugin != Some(method.plugin_name()) {
        let data = if method == Method::ClearText {
            Bytes::new()
        } else {
            io.nonce = new_nonce()?;
            let mut d = Vec::with_capacity(NONCE_LEN + 1);
            d.extend_from_slice(&io.nonce);
            d.push(0);
            Bytes::from(d)
        };
        let switch = AuthSwitchRequest {
            plugin: method.plugin_name().to_owned(),
            data,
        };
        io.io.write_packet(switch.encode()).await?;
        first = io.io.read_packet().await?;
    }
    method.plugin().run(io, first, flow).await
}

#[cfg(test)]
mod tests;
