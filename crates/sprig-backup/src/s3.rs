//! S3-compatible object storage: AWS S3, OCI Object Storage's S3
//! compatibility API and `MinIO`, over HTTP/1.1 with Signature Version 4.
//!
//! Path-style addressing (`<endpoint>/<bucket>/<key>`), which all three
//! accept. One kept-alive connection per target, reopened when the server
//! closes it; each call is retried three times on network errors and 5xx
//! answers. `https://` endpoints use rustls, checking the server against
//! `ca_file=` or the system's CA bundle.

use std::io::{BufRead as _, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs as _};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ring::{digest, hmac};
use sprig_common::Result;

use crate::crypt::hex;
use crate::target::{BackupTarget, err};

/// Where and how to reach the storage.
#[derive(Clone)]
pub struct S3Config {
    /// `http://host:port` or `https://host[:port]`.
    pub endpoint: String,
    /// The signing region (`us-east-1` for `MinIO`).
    pub region: String,
    /// The bucket.
    pub bucket: String,
    /// A prefix for every object name (empty, or ending in `/`).
    pub prefix: String,
    /// The access key id.
    pub access_key: String,
    /// The secret key.
    pub secret_key: String,
    /// CA certificates (PEM) for `https://`; the system bundle when unset.
    pub ca_file: Option<PathBuf>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

trait Conn: Read + Write + Send {}
impl<T: Read + Write + Send> Conn for T {}

/// An S3-compatible bucket.
pub struct S3Target {
    cfg: S3Config,
    host: String,
    port: u16,
    tls: Option<Arc<rustls::ClientConfig>>,
    conn: Mutex<Option<BufReader<Box<dyn Conn>>>>,
}

impl std::fmt::Debug for S3Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Target")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

/// `%XX` encoding of everything but unreserved characters (and `/` when
/// `slash` is set).
fn enc(s: &str, slash: bool) -> String {
    use std::fmt::Write as _;
    let mut o = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) || (slash && b == b'/') {
            o.push(char::from(b));
        } else {
            let _ = write!(o, "%{b:02X}");
        }
    }
    o
}

fn sha_hex(b: &[u8]) -> String {
    hex(digest::digest(&digest::SHA256, b).as_ref())
}

fn hmac_of(key: &[u8], msg: &[u8]) -> Vec<u8> {
    hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), msg)
        .as_ref()
        .to_vec()
}

/// `YYYYMMDD'T'HHMMSS'Z'` for a time.
pub(crate) fn amz_date(at: SystemTime) -> String {
    let secs = at.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let (year, month, day) = civil(secs / 86_400);
    let sod = secs % 86_400;
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        sod / 3600,
        (sod / 60) % 60,
        sod % 60
    )
}

/// Days since 1970-01-01 to (year, month, day) (H. Hinnant's algorithm).
#[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
pub(crate) fn civil(days: u64) -> (i64, u64, u64) {
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe as i64 + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

struct Reply {
    status: u16,
    body: Vec<u8>,
}

fn load_roots(ca: Option<&PathBuf>) -> Result<rustls::RootCertStore> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject as _;
    let paths: Vec<PathBuf> = match ca {
        Some(p) => vec![p.clone()],
        None => [
            "/etc/ssl/cert.pem",
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/pki/tls/certs/ca-bundle.crt",
            "/etc/ssl/ca-bundle.pem",
        ]
        .iter()
        .map(PathBuf::from)
        .filter(|p| p.exists())
        .take(1)
        .collect(),
    };
    let mut roots = rustls::RootCertStore::empty();
    for p in &paths {
        for c in CertificateDer::pem_file_iter(p)
            .map_err(|e| err("CA file", format!("{}: {e}", p.display())))?
        {
            let c = c.map_err(|e| err("CA file", format!("{}: {e}", p.display())))?;
            let _ = roots.add(c);
        }
    }
    if roots.is_empty() {
        return Err(err(
            "https",
            "no CA certificates: set ca_file= in backup.location",
        ));
    }
    Ok(roots)
}

impl S3Target {
    /// A target from its settings.
    ///
    /// # Errors
    /// The endpoint isn't `http://` or `https://host[:port]`, or the CA
    /// file can't be read.
    pub fn new(mut cfg: S3Config) -> Result<S3Target> {
        let (tls, rest) = if let Some(r) = cfg.endpoint.strip_prefix("https://") {
            (true, r)
        } else if let Some(r) = cfg.endpoint.strip_prefix("http://") {
            (false, r)
        } else {
            return Err(err(
                "endpoint",
                format!("'{}' must start with http:// or https://", cfg.endpoint),
            ));
        };
        let hostport = rest.trim_end_matches('/');
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) if !h.contains(']') || h.ends_with(']') => (
                h.to_owned(),
                p.parse::<u16>()
                    .map_err(|_| err("endpoint", format!("bad port in '{}'", cfg.endpoint)))?,
            ),
            _ => (hostport.to_owned(), if tls { 443 } else { 80 }),
        };
        if !cfg.prefix.is_empty() && !cfg.prefix.ends_with('/') {
            cfg.prefix.push('/');
        }
        let tls = if tls {
            let roots = load_roots(cfg.ca_file.as_ref())?;
            let c = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|e| err("tls", e))?
            .with_root_certificates(roots)
            .with_no_client_auth();
            Some(Arc::new(c))
        } else {
            None
        };
        Ok(S3Target {
            cfg,
            host,
            port,
            tls,
            conn: Mutex::new(None),
        })
    }

    /// From the part of an `s3://` URL after the scheme:
    /// `bucket[/prefix]?endpoint=URL[&region=R][&ca_file=P][&credentials=P]`.
    ///
    /// # Errors
    /// A part is missing or malformed.
    pub fn from_url(rest: &str) -> Result<S3Target> {
        let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
        let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
        if bucket.is_empty() {
            return Err(err("backup.location", "s3:// needs a bucket"));
        }
        let mut endpoint = None;
        let mut region = "us-east-1".to_owned();
        let mut ca_file = None;
        let mut creds_file = None;
        for kv in query.split('&').filter(|s| !s.is_empty()) {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            match k {
                "endpoint" => endpoint = Some(v.to_owned()),
                "region" => v.clone_into(&mut region),
                "ca_file" => ca_file = Some(PathBuf::from(v)),
                "credentials" => creds_file = Some(PathBuf::from(v)),
                _ => return Err(err("backup.location", format!("unknown parameter '{k}'"))),
            }
        }
        let endpoint =
            endpoint.ok_or_else(|| err("backup.location", "s3:// needs endpoint=URL"))?;
        let (mut ak, mut sk) = (
            std::env::var("SPRIGDB_BACKUP_S3_ACCESS_KEY").unwrap_or_default(),
            std::env::var("SPRIGDB_BACKUP_S3_SECRET_KEY").unwrap_or_default(),
        );
        if let Some(p) = creds_file {
            let s = std::fs::read_to_string(&p)
                .map_err(|e| err("credentials", format!("{}: {e}", p.display())))?;
            for line in s.lines() {
                if let Some((k, v)) = line.split_once('=') {
                    let v = v.trim().trim_matches('"').to_owned();
                    match k.trim() {
                        "access_key" => ak = v,
                        "secret_key" => sk = v,
                        _ => {}
                    }
                }
            }
        }
        if ak.is_empty() || sk.is_empty() {
            return Err(err(
                "credentials",
                "set SPRIGDB_BACKUP_S3_ACCESS_KEY and SPRIGDB_BACKUP_S3_SECRET_KEY, or credentials=FILE",
            ));
        }
        S3Target::new(S3Config {
            endpoint,
            region,
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            access_key: ak,
            secret_key: sk,
            ca_file,
        })
    }

    fn connect(&self) -> Result<BufReader<Box<dyn Conn>>> {
        // Every address the name resolves to, in turn (`localhost` may
        // give ::1 first while the server listens on 127.0.0.1 only).
        let mut last = err("resolve", &self.host);
        let mut tcp = None;
        for addr in (self.host.trim_matches(['[', ']']), self.port)
            .to_socket_addrs()
            .map_err(|e| err("resolve", format!("{}: {e}", self.host)))?
        {
            match TcpStream::connect_timeout(&addr, Duration::from_secs(10)) {
                Ok(t) => {
                    tcp = Some(t);
                    break;
                }
                Err(e) => last = err("connect", format!("{addr}: {e}")),
            }
        }
        let tcp = tcp.ok_or(last)?;
        let _ = tcp.set_read_timeout(Some(Duration::from_secs(120)));
        let _ = tcp.set_write_timeout(Some(Duration::from_secs(120)));
        let _ = tcp.set_nodelay(true);
        let c: Box<dyn Conn> = match &self.tls {
            None => Box::new(tcp),
            Some(cfg) => {
                let name = rustls::pki_types::ServerName::try_from(self.host.clone())
                    .map_err(|e| err("tls name", e))?;
                let conn = rustls::ClientConnection::new(Arc::clone(cfg), name)
                    .map_err(|e| err("tls", e))?;
                Box::new(rustls::StreamOwned::new(conn, tcp))
            }
        };
        Ok(BufReader::with_capacity(64 << 10, c))
    }

    fn host_header(&self) -> String {
        let default = if self.tls.is_some() { 443 } else { 80 };
        if self.port == default {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// One signed request; retried on network errors and 5xx.
    fn call(
        &self,
        method: &str,
        key: &str,
        query: &[(&str, String)],
        body: &[u8],
    ) -> Result<Reply> {
        let mut last = None;
        for attempt in 0..4u32 {
            if attempt > 0 {
                std::thread::sleep(Duration::from_millis(200 * u64::from(attempt * attempt)));
            }
            match self.call_once(method, key, query, body) {
                Ok(r) if r.status >= 500 => {
                    last = Some(err(
                        method,
                        format!(
                            "{} answered {}: {}",
                            key,
                            r.status,
                            String::from_utf8_lossy(&r.body)
                        ),
                    ));
                }
                Ok(r) => return Ok(r),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| err(method, "failed")))
    }

    fn call_once(
        &self,
        method: &str,
        key: &str,
        query: &[(&str, String)],
        body: &[u8],
    ) -> Result<Reply> {
        let path = if key.is_empty() {
            format!("/{}", enc(&self.cfg.bucket, false))
        } else {
            format!("/{}/{}", enc(&self.cfg.bucket, false), enc(key, true))
        };
        let mut pairs: Vec<(String, String)> = query
            .iter()
            .map(|(k, v)| (enc(k, false), enc(v, false)))
            .collect();
        pairs.sort();
        let qs = pairs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let now = SystemTime::now();
        let date = amz_date(now);
        let day = &date[..8];
        let payload = sha_hex(body);
        let host = self.host_header();
        let canonical = format!(
            "{method}\n{path}\n{qs}\nhost:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{date}\n\nhost;x-amz-content-sha256;x-amz-date\n{payload}"
        );
        let scope = format!("{day}/{}/s3/aws4_request", self.cfg.region);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
            sha_hex(canonical.as_bytes())
        );
        let mut signing = hmac_of(
            format!("AWS4{}", self.cfg.secret_key).as_bytes(),
            day.as_bytes(),
        );
        signing = hmac_of(&signing, self.cfg.region.as_bytes());
        signing = hmac_of(&signing, b"s3");
        signing = hmac_of(&signing, b"aws4_request");
        let sig = hex(&hmac_of(&signing, to_sign.as_bytes()));
        let target = if qs.is_empty() {
            path
        } else {
            format!("{path}?{qs}")
        };
        let head = format!(
            "{method} {target} HTTP/1.1\r\nHost: {host}\r\nx-amz-date: {date}\r\nx-amz-content-sha256: {payload}\r\nAuthorization: AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={sig}\r\nContent-Length: {}\r\n\r\n",
            self.cfg.access_key,
            body.len()
        );
        let mut slot = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        // A kept connection the server closed fails on first use: retry
        // once on a new one.
        for fresh in [false, true] {
            let reused = slot.is_some() && !fresh;
            if slot.is_none() || fresh {
                *slot = Some(self.connect()?);
            }
            let Some(c) = slot.as_mut() else { continue };
            let r = (|| -> std::io::Result<(Reply, bool)> {
                let w = c.get_mut();
                w.write_all(head.as_bytes())?;
                w.write_all(body)?;
                w.flush()?;
                read_reply(c, method == "HEAD")
            })();
            match r {
                Ok((reply, keep)) => {
                    if !keep {
                        *slot = None;
                    }
                    return Ok(reply);
                }
                Err(e) => {
                    *slot = None;
                    if !reused {
                        return Err(err(method, format!("{key}: {e}")));
                    }
                }
            }
        }
        Err(err(method, key))
    }

    /// Creates the bucket (success when it exists already and is ours).
    ///
    /// # Errors
    /// The storage refused.
    pub fn create_bucket(&self) -> Result<()> {
        let r = self.call("PUT", "", &[], &[])?;
        let body = String::from_utf8_lossy(&r.body);
        if r.status / 100 == 2 || body.contains("BucketAlreadyOwnedByYou") {
            Ok(())
        } else {
            Err(status_err("create bucket", &self.cfg.bucket, &r))
        }
    }

    fn full(&self, name: &str) -> String {
        format!("{}{name}", self.cfg.prefix)
    }
}

/// Reads one response; returns it and whether the connection stays open.
fn read_reply(c: &mut BufReader<Box<dyn Conn>>, head_only: bool) -> std::io::Result<(Reply, bool)> {
    let bad = |m: &str| std::io::Error::new(std::io::ErrorKind::InvalidData, m.to_owned());
    let mut line = String::new();
    if c.read_line(&mut line)? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    let status: u16 = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| bad("bad status line"))?;
    let mut len: Option<usize> = None;
    let mut chunked = false;
    let mut keep = !line.starts_with("HTTP/1.0");
    loop {
        line.clear();
        if c.read_line(&mut line)? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let l = line.trim_end();
        if l.is_empty() {
            break;
        }
        if let Some((k, v)) = l.split_once(':') {
            let v = v.trim();
            match k.trim().to_ascii_lowercase().as_str() {
                "content-length" => len = v.parse().ok(),
                "transfer-encoding" => chunked = v.eq_ignore_ascii_case("chunked"),
                "connection" => keep = !v.eq_ignore_ascii_case("close"),
                _ => {}
            }
        }
    }
    let mut body = Vec::new();
    if head_only || status == 204 || status == 304 {
    } else if chunked {
        loop {
            line.clear();
            c.read_line(&mut line)?;
            let n = usize::from_str_radix(line.trim().split(';').next().unwrap_or(""), 16)
                .map_err(|_| bad("bad chunk size"))?;
            if n == 0 {
                // Trailers, then the blank line.
                loop {
                    line.clear();
                    if c.read_line(&mut line)? == 0 || line.trim_end().is_empty() {
                        break;
                    }
                }
                break;
            }
            let at = body.len();
            body.resize(at + n, 0);
            c.read_exact(&mut body[at..])?;
            line.clear();
            c.read_line(&mut line)?;
        }
    } else if let Some(n) = len {
        body.resize(n, 0);
        c.read_exact(&mut body)?;
    } else {
        c.read_to_end(&mut body)?;
        keep = false;
    }
    Ok((Reply { status, body }, keep))
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

/// The texts of every `<tag>` element in `xml`.
fn tags<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else { break };
        out.push(&after[..j]);
        rest = &after[j + close.len()..];
    }
    out
}

fn status_err(what: &str, name: &str, r: &Reply) -> sprig_common::SprigError {
    let body = String::from_utf8_lossy(&r.body);
    let code = tags(&body, "Code")
        .first()
        .map(|s| (*s).to_owned())
        .unwrap_or_default();
    err(what, format!("{name}: HTTP {} {code}", r.status))
}

impl BackupTarget for S3Target {
    fn put(&self, name: &str, bytes: &[u8]) -> Result<()> {
        let r = self.call("PUT", &self.full(name), &[], bytes)?;
        if r.status / 100 == 2 {
            Ok(())
        } else {
            Err(status_err("put", name, &r))
        }
    }

    fn get(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let r = self.call("GET", &self.full(name), &[], &[])?;
        match r.status {
            200 => Ok(Some(r.body)),
            404 => Ok(None),
            _ => Err(status_err("get", name, &r)),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![("list-type", "2".to_owned()), ("prefix", self.full(prefix))];
            if let Some(t) = &token {
                q.push(("continuation-token", t.clone()));
            }
            let r = self.call("GET", "", &q, &[])?;
            if r.status != 200 {
                return Err(status_err("list", prefix, &r));
            }
            let body = String::from_utf8_lossy(&r.body);
            for k in tags(&body, "Key") {
                let k = xml_unescape(k);
                if let Some(n) = k.strip_prefix(&self.cfg.prefix) {
                    out.push(n.to_owned());
                }
            }
            let truncated = tags(&body, "IsTruncated").first() == Some(&"true");
            token = tags(&body, "NextContinuationToken")
                .first()
                .map(|t| xml_unescape(t));
            if !truncated || token.is_none() {
                break;
            }
        }
        out.sort();
        Ok(out)
    }

    fn delete(&self, name: &str) -> Result<()> {
        let r = self.call("DELETE", &self.full(name), &[], &[])?;
        if r.status / 100 == 2 || r.status == 404 {
            Ok(())
        } else {
            Err(status_err("delete", name, &r))
        }
    }

    fn describe(&self) -> String {
        format!(
            "s3 bucket {} at {} (prefix '{}')",
            self.cfg.bucket, self.cfg.endpoint, self.cfg.prefix
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_and_encoding() {
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(19_723), (2024, 1, 1));
        assert_eq!(
            amz_date(UNIX_EPOCH + Duration::from_secs(1_440_938_160)),
            "20150830T123600Z"
        );
        assert_eq!(enc("a b/c~", true), "a%20b/c~");
        assert_eq!(enc("a/b", false), "a%2Fb");
    }

    /// The signing key example from the AWS Signature Version 4 docs.
    #[test]
    fn signing_key_matches_aws_example() {
        let secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
        let mut k = hmac_of(format!("AWS4{secret}").as_bytes(), b"20120215");
        k = hmac_of(&k, b"us-east-1");
        k = hmac_of(&k, b"iam");
        k = hmac_of(&k, b"aws4_request");
        assert_eq!(
            hex(&k),
            "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
        );
    }

    #[test]
    fn list_xml() {
        let x = "<ListBucketResult><Contents><Key>a&amp;b</Key></Contents><Contents><Key>c</Key></Contents><IsTruncated>false</IsTruncated></ListBucketResult>";
        assert_eq!(tags(x, "Key"), vec!["a&amp;b", "c"]);
        assert_eq!(xml_unescape("a&amp;b"), "a&b");
    }

    #[test]
    fn location_needs_endpoint_and_credentials() {
        assert!(S3Target::from_url("bucket").is_err());
        assert!(S3Target::from_url("bucket/p?endpoint=ftp://x").is_err());
    }
}
