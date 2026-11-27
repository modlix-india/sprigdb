//! Destinations, the allowlist and residency:
//!
//! ```text
//! check_destination(tenant T, host h, port n):
//!  1. sink.enabled and the target kind in sink.allowed_kinds, else 39230
//!  2. e := the most specific allowlist entry matching (h, n), else 39213
//!  3. resolve h (a literal address is its own result); an internal address
//!     (private, loopback, link-local, shared, unspecified, reserved) is
//!     refused unless e lists the address itself AND the operator's
//!     sink.internal_destinations covers it (39213); instance metadata
//!     needs sink.platform_identity = full as well (39228); connect to the
//!     address checked, so a second lookup can't swap it
//!  4. T has a residency policy P: tags := e.tags, else the built-in region
//!     table; no tags → 39214; P doesn't admit tags → 39214
//!  5. record e's tags in sink_connections.residency_tags
//! ```
//!
//! Entries are `host`, `host:port`, `*.domain` or an address, written as
//! a JSON list of strings or of `{"host": ..., "tags": {...}}` objects, or
//! as comma-separated text. Without a port an
//! entry allows 80 and 443, plus the default port of the connection's kind
//! (5432, 3306, 9092 or 9093) for database and Kafka connections.
//!
//! The allowlist is the tenant's own setting, so by itself it never opens
//! the platform's network: `sink.internal_destinations` and
//! `sink.platform_identity` are operator-only, and a sink's `WITH` can set
//! neither ([`crate::settings::with_may_set`]). A platform connection's
//! hosts were listed by an operator and need no second listing.
//!
//! The residency policy is an expression over tags; the caller
//! evaluates it ([`Residency`]), so this crate needs no controller types.

use std::net::{IpAddr, SocketAddr};

use serde_json::{Map, Value};
use sprig_common::outbound::InternalAllow;
pub use sprig_common::outbound::{is_internal, is_metadata};
use sprig_common::{Result, SprigCode};

use crate::model::{ConnKind, TargetKind, err};

/// One allowlist entry.
#[derive(Debug, Clone, PartialEq)]
pub struct AllowEntry {
    /// `host`, `host:port`, `*.domain`, an address, `[v6]:port`.
    pub host: String,
    /// Residency tags (`jurisdiction`, `country`, ...).
    pub tags: Option<Map<String, Value>>,
}

/// Parses `sink.destination_allowlist`.
///
/// # Errors
/// 39218 for a malformed list.
pub fn parse_allowlist(text: &str) -> Result<Vec<AllowEntry>> {
    let t = text.trim();
    if t.is_empty() || t == "[]" {
        return Ok(Vec::new());
    }
    if t.starts_with('[') {
        let v: Value = serde_json::from_str(t)
            .map_err(|e| crate::model::bad_option(format!("sink.destination_allowlist: {e}")))?;
        let Value::Array(items) = v else {
            return Err(crate::model::bad_option(
                "sink.destination_allowlist must be a JSON list",
            ));
        };
        let mut out = Vec::new();
        for it in items {
            match it {
                Value::String(h) => out.push(AllowEntry {
                    host: h.trim().to_ascii_lowercase(),
                    tags: None,
                }),
                Value::Object(m) => {
                    let host = m
                        .get("host")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            crate::model::bad_option(
                                "sink.destination_allowlist: an entry without \"host\"",
                            )
                        })?
                        .trim()
                        .to_ascii_lowercase();
                    let tags = match m.get("tags") {
                        None | Some(Value::Null) => None,
                        Some(Value::Object(t)) => Some(t.clone()),
                        Some(_) => {
                            return Err(crate::model::bad_option(
                                "sink.destination_allowlist: \"tags\" must be an object",
                            ));
                        }
                    };
                    out.push(AllowEntry { host, tags });
                }
                other => {
                    return Err(crate::model::bad_option(format!(
                        "sink.destination_allowlist: bad entry {other}"
                    )));
                }
            }
        }
        return Ok(out);
    }
    Ok(t.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|h| AllowEntry {
            host: h.to_ascii_lowercase(),
            tags: None,
        })
        .collect())
}

fn split_port(entry: &str) -> (&str, Option<u16>) {
    if let Some(rest) = entry.strip_prefix('[') {
        if let Some((h, p)) = rest.split_once("]:") {
            return (h, p.parse().ok());
        }
        return (rest.trim_end_matches(']'), None);
    }
    if entry.matches(':').count() == 1
        && let Some((h, p)) = entry.split_once(':')
    {
        return (h, p.parse().ok());
    }
    (entry, None)
}

/// The default port of a connection kind, allowed by entries without a
/// port besides 80 and 443.
#[must_use]
pub fn default_port(kind: Option<ConnKind>) -> Option<u16> {
    match kind? {
        ConnKind::Postgresql => Some(5432),
        ConnKind::Mysql => Some(3306),
        ConnKind::Kafka => Some(9092),
        ConnKind::Webhook | ConnKind::S3 => None,
    }
}

/// How an entry matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Matched {
    /// By name: internal addresses still refused.
    ByName,
    /// The entry is this literal address: an internal address is allowed
    /// when the operator's list covers it too.
    Literal,
}

/// The most specific entry matching `host:port`: an exact host with its
/// port, then an exact host, then the longest `*.domain`.
#[must_use]
pub fn match_entry<'a>(
    entries: &'a [AllowEntry],
    host: &str,
    port: u16,
    kind: Option<ConnKind>,
) -> Option<(&'a AllowEntry, Matched)> {
    let host = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    let literal = host.parse::<IpAddr>().is_ok();
    let mut best: Option<(&AllowEntry, u32)> = None;
    for e in entries {
        let (h, p) = split_port(&e.host);
        let port_ok = match p {
            Some(p) => p == port,
            None => port == 80 || port == 443 || Some(port) == default_port(kind),
        };
        if !port_ok {
            continue;
        }
        let score = if let Some(domain) = h.strip_prefix("*.") {
            if !host.ends_with(&format!(".{domain}")) {
                continue;
            }
            u32::try_from(domain.len()).unwrap_or(0)
        } else if h == host {
            if p.is_some() { 100_000 } else { 50_000 }
        } else {
            continue;
        };
        if best.is_none_or(|(_, s)| score > s) {
            best = Some((e, score));
        }
    }
    best.map(|(e, _)| {
        (
            e,
            if literal {
                Matched::Literal
            } else {
                Matched::ByName
            },
        )
    })
}

/// The built-in region table: tags for cloud endpoints whose
/// names state their region. `None` for other hosts.
#[must_use]
pub fn region_tags(host: &str, options: Option<&Map<String, Value>>) -> Option<Map<String, Value>> {
    let h = host.to_ascii_lowercase();
    let region = if let Some(rest) = h.strip_prefix("s3.") {
        rest.strip_suffix(".amazonaws.com")
            .map(|r| r.trim_start_matches("dualstack.").to_owned())
    } else if h.contains(".s3.") && h.ends_with(".amazonaws.com") {
        h.split(".s3.")
            .nth(1)
            .and_then(|r| r.strip_suffix(".amazonaws.com"))
            .map(str::to_owned)
    } else if let Some(i) = h.find("objectstorage.") {
        h[i + "objectstorage.".len()..]
            .strip_suffix(".oraclecloud.com")
            .map(str::to_owned)
    } else if h.ends_with(".r2.cloudflarestorage.com") {
        // Jurisdiction-specific endpoints: <account>.eu.r2.cloudflarestorage.com.
        if h.ends_with(".eu.r2.cloudflarestorage.com") {
            return Some(tags(&[("jurisdiction", "EU")]));
        }
        if h.ends_with(".fedramp.r2.cloudflarestorage.com") {
            return Some(tags(&[("jurisdiction", "US")]));
        }
        return None;
    } else if h == "storage.googleapis.com" {
        options
            .and_then(|o| o.get("location"))
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase)
    } else {
        None
    }?;
    region_of(&region)
}

fn tags(pairs: &[(&str, &str)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), Value::String((*v).to_owned())))
        .collect()
}

/// Tags of a cloud region name (AWS, OCI and GCS spellings).
#[must_use]
pub fn region_of(region: &str) -> Option<Map<String, Value>> {
    const TABLE: &[(&str, &str, &str)] = &[
        // AWS
        ("eu-central-1", "EU", "DE"),
        ("eu-central-2", "CH", "CH"),
        ("eu-west-1", "EU", "IE"),
        ("eu-west-2", "UK", "GB"),
        ("eu-west-3", "EU", "FR"),
        ("eu-north-1", "EU", "SE"),
        ("eu-south-1", "EU", "IT"),
        ("eu-south-2", "EU", "ES"),
        ("us-east-1", "US", "US"),
        ("us-east-2", "US", "US"),
        ("us-west-1", "US", "US"),
        ("us-west-2", "US", "US"),
        ("ca-central-1", "CA", "CA"),
        ("ap-south-1", "IN", "IN"),
        ("ap-south-2", "IN", "IN"),
        ("ap-southeast-1", "SG", "SG"),
        ("ap-southeast-2", "AU", "AU"),
        ("ap-northeast-1", "JP", "JP"),
        ("sa-east-1", "BR", "BR"),
        // OCI
        ("eu-frankfurt-1", "EU", "DE"),
        ("eu-amsterdam-1", "EU", "NL"),
        ("uk-london-1", "UK", "GB"),
        ("us-ashburn-1", "US", "US"),
        ("us-phoenix-1", "US", "US"),
        ("ap-mumbai-1", "IN", "IN"),
        ("ap-hyderabad-1", "IN", "IN"),
        // GCS locations
        ("europe-west3", "EU", "DE"),
        ("europe-west1", "EU", "BE"),
        ("europe-west4", "EU", "NL"),
        ("asia-south1", "IN", "IN"),
        ("us-central1", "US", "US"),
        ("eu", "EU", ""),
        ("us", "US", ""),
    ];
    let r = region.to_ascii_lowercase();
    let (_, j, c) = TABLE.iter().find(|(n, _, _)| *n == r)?;
    let mut m = tags(&[("jurisdiction", j), ("region", &r)]);
    if !c.is_empty() {
        m.insert("country".into(), Value::String((*c).to_owned()));
    }
    Some(m)
}

/// A tenant's residency policy, evaluated by the caller (an expression
/// over tags).
pub trait Residency: Send + Sync {
    /// Whether the tenant is pinned (has a policy).
    fn pinned(&self) -> bool;
    /// Whether the policy admits a destination with `tags`.
    fn admits(&self, tags: &Map<String, Value>) -> bool;
}

/// No residency policy.
#[derive(Debug, Clone, Copy, Default)]
pub struct Unpinned;

impl Residency for Unpinned {
    fn pinned(&self) -> bool {
        false
    }
    fn admits(&self, _tags: &Map<String, Value>) -> bool {
        true
    }
}

/// `sink.enabled` and `sink.allowed_kinds` (step 1).
///
/// # Errors
/// 39230.
pub fn check_enabled(enabled: bool, allowed_kinds: &str, kind: TargetKind) -> Result<()> {
    if !enabled {
        return Err(err(SprigCode::SinkDisabled, "sink.enabled is off"));
    }
    let ok = allowed_kinds
        .split(',')
        .map(str::trim)
        .any(|k| k.eq_ignore_ascii_case(kind.as_str()));
    if !ok {
        return Err(err(
            SprigCode::SinkDisabled,
            format!("{kind} targets aren't in sink.allowed_kinds ({allowed_kinds})"),
        ));
    }
    Ok(())
}

/// The destination rules one connection is checked and opened under.
pub struct Destinations {
    /// `sink.destination_allowlist`.
    pub allow: Vec<AllowEntry>,
    /// The tenant's residency policy.
    pub residency: Box<dyn Residency>,
    /// A platform connection: the operator vetted its hosts, listed here
    /// (the allowlist doesn't apply; internal addresses are allowed when
    /// listed).
    pub platform_hosts: Option<Vec<String>>,
    /// `sink.platform_identity` allows the link-local metadata address.
    pub metadata_allowed: bool,
    /// `sink.internal_destinations` (operator only): the internal
    /// addresses an allowlist entry may open.
    pub internal: InternalAllow,
}

impl std::fmt::Debug for Destinations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Destinations")
            .field("allow", &self.allow)
            .field("platform_hosts", &self.platform_hosts)
            .finish_non_exhaustive()
    }
}

/// What a check found.
#[derive(Debug, Clone, PartialEq)]
pub struct Checked {
    /// Internal addresses are allowed (the entry names the address).
    pub literal: bool,
    /// The residency tags that applied.
    pub tags: Option<Map<String, Value>>,
}

impl Destinations {
    /// Rules allowing every listed host, with no residency and with every
    /// internal address open to a literal entry (tests, which serve on
    /// loopback).
    #[must_use]
    pub fn allow_list(hosts: &[&str]) -> Self {
        Self {
            allow: hosts
                .iter()
                .map(|h| AllowEntry {
                    host: (*h).to_ascii_lowercase(),
                    tags: None,
                })
                .collect(),
            residency: Box::new(Unpinned),
            platform_hosts: None,
            metadata_allowed: false,
            internal: InternalAllow::everything(),
        }
    }

    /// Steps 2 and 4: the allowlist and residency for `host:port`.
    ///
    /// # Errors
    /// 39213 off the allowlist; 39214 for residency.
    pub fn check(
        &self,
        host: &str,
        port: u16,
        kind: Option<ConnKind>,
        options: Option<&Map<String, Value>>,
    ) -> Result<Checked> {
        let (tags, literal) = if let Some(hosts) = &self.platform_hosts {
            let entries: Vec<AllowEntry> = hosts
                .iter()
                .map(|h| AllowEntry {
                    host: h.to_ascii_lowercase(),
                    tags: None,
                })
                .collect();
            if match_entry(&entries, host, port, kind).is_none() {
                return Err(err(
                    SprigCode::SinkDestinationNotAllowed,
                    format!("{host}:{port} isn't among the platform connection's hosts"),
                ));
            }
            (
                options
                    .and_then(|o| o.get("__platform_tags"))
                    .and_then(Value::as_object)
                    .cloned(),
                true,
            )
        } else {
            let Some((e, m)) = match_entry(&self.allow, host, port, kind) else {
                metrics::counter!("sprig_sink_destination_refused_total", "reason" => "allowlist")
                    .increment(1);
                return Err(err(
                    SprigCode::SinkDestinationNotAllowed,
                    format!("{host}:{port} is not on sink.destination_allowlist"),
                ));
            };
            (
                e.tags.clone().or_else(|| region_tags(host, options)),
                m == Matched::Literal,
            )
        };
        if self.residency.pinned() {
            let Some(t) = &tags else {
                metrics::counter!("sprig_sink_destination_refused_total", "reason" => "residency")
                    .increment(1);
                return Err(err(
                    SprigCode::SinkResidency,
                    format!("{host} has no residency tags, and the tenant has a residency policy"),
                ));
            };
            if !self.residency.admits(t) {
                metrics::counter!("sprig_sink_destination_refused_total", "reason" => "residency")
                    .increment(1);
                return Err(err(
                    SprigCode::SinkResidency,
                    format!(
                        "{host}'s tags {} don't satisfy the tenant's residency policy",
                        Value::Object(t.clone())
                    ),
                ));
            }
        }
        Ok(Checked { literal, tags })
    }

    /// Step 3: resolves `host:port` and checks the addresses. The caller
    /// connects to the returned address, never to the name again.
    ///
    /// # Errors
    /// 39213 for an internal address not listed literally or not covered
    /// by the operator's list, or one that doesn't resolve; 39228 for a
    /// metadata address.
    pub async fn resolve(
        &self,
        host: &str,
        port: u16,
        kind: Option<ConnKind>,
        options: Option<&Map<String, Value>>,
    ) -> Result<SocketAddr> {
        let c = self.check(host, port, kind, options)?;
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        let addrs: Vec<SocketAddr> = if let Ok(ip) = bare.parse::<IpAddr>() {
            vec![SocketAddr::new(ip, port)]
        } else {
            tokio::net::lookup_host((bare, port))
                .await
                .map_err(|e| {
                    let mut x = err(
                        SprigCode::SinkDeliveryFailed,
                        format!("{host} doesn't resolve: {e}"),
                    );
                    x.retryable = true;
                    x
                })?
                .collect()
        };
        let Some(a) = addrs.first().copied() else {
            return Err(err(
                SprigCode::SinkDeliveryFailed,
                format!("{host} resolved to no address"),
            ));
        };
        for a in &addrs {
            let ip = a.ip();
            if is_metadata(ip) && !self.metadata_allowed {
                metrics::counter!("sprig_sink_destination_refused_total", "reason" => "identity")
                    .increment(1);
                return Err(err(
                    SprigCode::SinkPlatformIdentityDenied,
                    format!("instance metadata ({ip}) needs sink.platform_identity = full"),
                ));
            }
            if !is_internal(ip) {
                continue;
            }
            if !c.literal {
                metrics::counter!("sprig_sink_destination_refused_total", "reason" => "allowlist")
                    .increment(1);
                return Err(err(
                    SprigCode::SinkDestinationNotAllowed,
                    format!(
                        "{host} resolves to the internal address {ip}; list the address itself to allow it"
                    ),
                ));
            }
            // A platform connection's hosts are the operator's own list.
            if self.platform_hosts.is_none() && !self.internal.covers(ip, port) {
                metrics::counter!("sprig_sink_destination_refused_total", "reason" => "internal")
                    .increment(1);
                return Err(err(
                    SprigCode::SinkDestinationNotAllowed,
                    format!(
                        "{host}:{port} is the internal address {ip}: an operator must list it in \
                         sink.internal_destinations"
                    ),
                ));
            }
        }
        Ok(a)
    }
}

/// `host:port` of a URL (`https://h:p/...`), with the scheme's default.
///
/// # Errors
/// 39218 for a URL that doesn't parse.
pub fn url_host_port(url: &str) -> Result<(String, u16, bool)> {
    let u: hyper::Uri = url
        .parse()
        .map_err(|e| crate::model::bad_option(format!("bad url '{url}': {e}")))?;
    let tls = match u.scheme_str() {
        Some("https") => true,
        Some("http") => false,
        _ => {
            return Err(crate::model::bad_option(format!(
                "url '{url}' must start with http:// or https://"
            )));
        }
    };
    let host = u
        .host()
        .ok_or_else(|| crate::model::bad_option(format!("url '{url}' has no host")))?
        .to_owned();
    Ok((
        host,
        u.port_u16().unwrap_or(if tls { 443 } else { 80 }),
        tls,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlists_parse_and_match_most_specific() {
        let l = parse_allowlist(
            r#"[{"host": "*.s3.eu-central-1.amazonaws.com", "tags": {"jurisdiction": "EU"}},
                {"host": "billing.example.com"}, "10.20.0.15:5432", "*.example.com"]"#,
        )
        .unwrap();
        assert_eq!(l.len(), 4);
        let (e, m) = match_entry(&l, "billing.example.com", 443, None).unwrap();
        assert_eq!(e.host, "billing.example.com");
        assert_eq!(m, Matched::ByName);
        let (e, _) = match_entry(&l, "x.example.com", 443, None).unwrap();
        assert_eq!(e.host, "*.example.com");
        assert!(match_entry(&l, "example.com", 443, None).is_none());
        assert!(match_entry(&l, "billing.example.com", 8443, None).is_none());
        let (_, m) = match_entry(&l, "10.20.0.15", 5432, Some(ConnKind::Postgresql)).unwrap();
        assert_eq!(m, Matched::Literal);
        assert!(match_entry(&l, "10.20.0.15", 5433, None).is_none());
        // A database entry without a port allows the kind's default port.
        let l = parse_allowlist("pg.internal, kafka.internal").unwrap();
        assert!(match_entry(&l, "pg.internal", 5432, Some(ConnKind::Postgresql)).is_some());
        assert!(match_entry(&l, "pg.internal", 5432, Some(ConnKind::Webhook)).is_none());
        assert!(parse_allowlist("[1]").is_err());
    }

    #[test]
    fn region_table() {
        let t = region_tags("s3.eu-central-1.amazonaws.com", None).unwrap();
        assert_eq!(t["jurisdiction"], "EU");
        assert_eq!(t["country"], "DE");
        let t = region_tags("acme-lake.s3.us-east-1.amazonaws.com", None).unwrap();
        assert_eq!(t["jurisdiction"], "US");
        let t = region_tags(
            "ns.compat.objectstorage.eu-frankfurt-1.oraclecloud.com",
            None,
        )
        .unwrap();
        assert_eq!(t["country"], "DE");
        assert_eq!(
            region_tags("acct.eu.r2.cloudflarestorage.com", None).unwrap()["jurisdiction"],
            "EU"
        );
        assert!(region_tags("acct.r2.cloudflarestorage.com", None).is_none());
        let o: Map<String, Value> = [("location".to_owned(), Value::from("europe-west3"))]
            .into_iter()
            .collect();
        assert_eq!(
            region_tags("storage.googleapis.com", Some(&o)).unwrap()["jurisdiction"],
            "EU"
        );
        assert!(region_tags("minio.local", None).is_none());
    }

    struct EuOnly;
    impl Residency for EuOnly {
        fn pinned(&self) -> bool {
            true
        }
        fn admits(&self, t: &Map<String, Value>) -> bool {
            t.get("jurisdiction").and_then(Value::as_str) == Some("EU")
        }
    }

    #[tokio::test]
    async fn a_tenants_allowlist_alone_never_opens_an_internal_address() {
        // What a sink task gets for a tenant whose administrator listed
        // internal addresses, with no operator setting.
        let tenant = |metadata_allowed: bool, internal: &str| Destinations {
            allow: parse_allowlist(
                r#"["127.0.0.1:7480", "10.0.0.5:5432", "192.168.0.1", "169.254.169.254",
                    "[fd00:ec2::254]", "[::ffff:169.254.169.254]", "[::ffff:127.0.0.1]:7480",
                    "169.254.170.2", "[fe80::1]"]"#,
            )
            .unwrap(),
            residency: Box::new(Unpinned),
            platform_hosts: None,
            metadata_allowed,
            internal: InternalAllow::parse(internal).unwrap(),
        };
        let refused = sprig_common::ErrorKind::Sprig(SprigCode::SinkDestinationNotAllowed);
        let identity = sprig_common::ErrorKind::Sprig(SprigCode::SinkPlatformIdentityDenied);
        let d = tenant(false, "[]");
        for (host, port) in [
            ("127.0.0.1", 7480),
            ("::ffff:127.0.0.1", 7480),
            ("10.0.0.5", 5432),
            ("192.168.0.1", 443),
            ("fe80::1", 443),
        ] {
            let e = d.resolve(host, port, None, None).await.unwrap_err();
            assert_eq!(e.kind, refused, "{host}: {e}");
            assert!(e.message.contains("sink.internal_destinations"), "{e}");
        }
        // Instance metadata in every spelling, not only 169.254.169.254.
        for host in [
            "169.254.169.254",
            "fd00:ec2::254",
            "::ffff:169.254.169.254",
            "169.254.170.2",
        ] {
            let e = d.resolve(host, 80, None, None).await.unwrap_err();
            assert_eq!(e.kind, identity, "{host}: {e}");
        }
        // The operator lists the database: that address and port only.
        let d = tenant(false, "10.0.0.0/24:5432");
        assert!(d.resolve("10.0.0.5", 5432, None, None).await.is_ok());
        let e = d.resolve("127.0.0.1", 7480, None, None).await.unwrap_err();
        assert_eq!(e.kind, refused, "{e}");
        // The operator's ranges don't open metadata: that is
        // sink.platform_identity, and then the range must cover it too.
        let d = tenant(false, "169.254.0.0/16, fd00::/8");
        let e = d
            .resolve("169.254.169.254", 80, None, None)
            .await
            .unwrap_err();
        assert_eq!(e.kind, identity, "{e}");
        let d = tenant(true, "[]");
        let e = d
            .resolve("169.254.169.254", 80, None, None)
            .await
            .unwrap_err();
        assert_eq!(e.kind, refused, "{e}");
        let d = tenant(true, "169.254.169.254");
        assert!(d.resolve("169.254.169.254", 80, None, None).await.is_ok());
        // A platform connection's hosts are the operator's: no second list.
        let p = Destinations {
            allow: Vec::new(),
            residency: Box::new(Unpinned),
            platform_hosts: Some(vec!["10.0.0.9:9092".into()]),
            metadata_allowed: false,
            internal: InternalAllow::none(),
        };
        assert!(p.resolve("10.0.0.9", 9092, None, None).await.is_ok());
        assert!(p.resolve("10.0.0.5", 5432, None, None).await.is_err());
    }

    #[tokio::test]
    async fn checks_refuse_internal_untagged_and_metadata() {
        let mut d =
            Destinations::allow_list(&["127.0.0.1:8080", "localhost:8080", "169.254.169.254"]);
        assert!(d.resolve("127.0.0.1", 8080, None, None).await.is_ok());
        let e = d.resolve("localhost", 8080, None, None).await.unwrap_err();
        assert!(e.message.contains("internal address"), "{e}");
        let e = d
            .resolve("169.254.169.254", 80, None, None)
            .await
            .unwrap_err();
        assert_eq!(
            e.kind,
            sprig_common::ErrorKind::Sprig(SprigCode::SinkPlatformIdentityDenied)
        );
        let e = d.resolve("example.org", 443, None, None).await.unwrap_err();
        assert_eq!(
            e.kind,
            sprig_common::ErrorKind::Sprig(SprigCode::SinkDestinationNotAllowed)
        );
        d.residency = Box::new(EuOnly);
        let e = d.check("127.0.0.1", 8080, None, None).unwrap_err();
        assert_eq!(
            e.kind,
            sprig_common::ErrorKind::Sprig(SprigCode::SinkResidency)
        );
        d.allow =
            parse_allowlist(r#"["*.s3.us-east-1.amazonaws.com", "s3.eu-central-1.amazonaws.com"]"#)
                .unwrap();
        assert!(
            d.check("s3.eu-central-1.amazonaws.com", 443, None, None)
                .is_ok()
        );
        assert!(
            d.check("b.s3.us-east-1.amazonaws.com", 443, None, None)
                .is_err()
        );
        assert!(check_enabled(true, "webhook,kafka", TargetKind::Kafka).is_ok());
        assert!(check_enabled(true, "webhook", TargetKind::S3).is_err());
        assert!(check_enabled(false, "webhook", TargetKind::Webhook).is_err());
        assert_eq!(
            url_host_port("https://h.example.com/x").unwrap(),
            ("h.example.com".into(), 443, true)
        );
    }
}
