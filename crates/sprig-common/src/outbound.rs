//! Which addresses a tenant's code and sinks may connect to.
//!
//! Tenants choose their destinations (a function's `fetch`, a sink's
//! webhook, Kafka, S3 or database endpoint), so those connections are made
//! under two rules that a tenant's own settings can't lift:
//!
//! ```text
//! address                                  reached when
//! public                                   the tenant's allowlist names the host
//! internal (private, loopback, link-local, the tenant's allowlist names the
//!   shared, unspecified, multicast,          address itself AND an operator's
//!   reserved, or one embedded in an          list covers it ([`InternalAllow`])
//!   IPv6 transition address)
//! instance metadata ([`is_metadata`])      never by tenant code; a sink only
//!                                            under its own operator setting
//! ```
//!
//! Every check is made on the address a name resolved to, which is also the
//! address then connected to, and on a literal address in the same way.
//!
//! [`InternalAllow`] is an operator-only setting's value: addresses or
//! CIDR ranges, each optionally with one port, as a JSON list of strings or
//! comma-separated: `["10.0.5.0/24:5432", "127.0.0.1:9000", "[fd12::/32]:443"]`.
//! An entry without a port covers every port of its range.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Unwraps the IPv4 address an IPv6 address stands for, if it is one of the
/// forms that reach an IPv4 host: IPv4-mapped (`::ffff:a.b.c.d`),
/// IPv4-compatible (`::a.b.c.d`), NAT64 (`64:ff9b::/96`) or 6to4
/// (`2002:AABB:CCDD::/48`).
fn embedded_v4(v: Ipv6Addr) -> Option<Ipv4Addr> {
    if let Some(v4) = v.to_ipv4_mapped() {
        return Some(v4);
    }
    let seg = v.segments();
    let v4_of = |high: u16, low: u16| {
        let (high, low) = (high.to_be_bytes(), low.to_be_bytes());
        Ipv4Addr::new(high[0], high[1], low[0], low[1])
    };
    // `::a.b.c.d`, except `::` and `::1` (handled as IPv6).
    if seg[..6] == [0; 6] && (seg[6] != 0 || seg[7] > 1) {
        return Some(v4_of(seg[6], seg[7]));
    }
    if seg[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
        return Some(v4_of(seg[6], seg[7]));
    }
    if seg[0] == 0x2002 {
        return Some(v4_of(seg[1], seg[2]));
    }
    None
}

fn internal_v4(v: Ipv4Addr) -> bool {
    let o = v.octets();
    v.is_private()
        || v.is_loopback()
        || v.is_link_local()
        || v.is_unspecified()
        || v.is_broadcast()
        || v.is_multicast()
        // 0.0.0.0/8 "this network"
        || o[0] == 0
        // 100.64.0.0/10 shared (carrier-grade NAT; cloud metadata lives here on some clouds)
        || (o[0] == 100 && (64..128).contains(&o[1]))
        // 192.0.0.0/24 protocol assignments
        || (o[0] == 192 && o[1] == 0 && o[2] == 0)
        // 198.18.0.0/15 benchmarking
        || (o[0] == 198 && (18..20).contains(&o[1]))
        // 240.0.0.0/4 reserved
        || o[0] >= 240
}

/// Whether `ip` is not a public address: private, loopback, link-local,
/// shared (100.64/10), unspecified, broadcast, multicast or reserved, in
/// IPv4 or IPv6 (unique-local, link-local and site-local included), or an
/// IPv6 transition address that embeds such an IPv4 address.
#[must_use]
pub fn is_internal(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => internal_v4(v),
        IpAddr::V6(v) => {
            if let Some(v4) = embedded_v4(v) {
                return internal_v4(v4);
            }
            let s = v.segments();
            v.is_loopback()
                || v.is_unspecified()
                || v.is_multicast()
                // fc00::/7 unique local
                || (s[0] & 0xfe00) == 0xfc00
                // fe80::/10 link-local and fec0::/10 site-local
                || (s[0] & 0xff80) == 0xfe80
                // 64:ff9b:1::/48 local-use NAT64
                || (s[0] == 0x64 && s[1] == 0xff9b && s[2] == 1)
                // 2001::/32 Teredo
                || (s[0] == 0x2001 && s[1] == 0)
        }
    }
}

/// Whether `ip` is a cloud's instance-metadata or credentials address:
/// `169.254.169.254` (most clouds), `169.254.170.2` and `169.254.170.23`
/// (container credentials), `100.100.100.200`, `fd00:ec2::254`, or one of
/// those behind an IPv6 transition form. Whoever reaches it can read the
/// machine's cloud credentials.
#[must_use]
pub fn is_metadata(ip: IpAddr) -> bool {
    const V4: [Ipv4Addr; 4] = [
        Ipv4Addr::new(169, 254, 169, 254),
        Ipv4Addr::new(169, 254, 170, 2),
        Ipv4Addr::new(169, 254, 170, 23),
        Ipv4Addr::new(100, 100, 100, 200),
    ];
    match ip {
        IpAddr::V4(v) => V4.contains(&v),
        IpAddr::V6(v) => {
            if let Some(v4) = embedded_v4(v) {
                return V4.contains(&v4);
            }
            v.segments() == [0xfd00, 0xec2, 0, 0, 0, 0, 0, 0x254]
        }
    }
}

/// One range of an [`InternalAllow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Range {
    net: IpAddr,
    prefix: u8,
    port: Option<u16>,
}

impl Range {
    fn contains(&self, ip: IpAddr) -> bool {
        // An IPv4 range also covers the IPv6 forms that reach its hosts.
        let ip = match (self.net, ip) {
            (IpAddr::V4(_), IpAddr::V6(v)) => embedded_v4(v).map_or(ip, IpAddr::V4),
            _ => ip,
        };
        match (self.net, ip) {
            (IpAddr::V4(n), IpAddr::V4(a)) => {
                let m = u32::MAX
                    .checked_shl(32 - u32::from(self.prefix))
                    .unwrap_or(0);
                u32::from(n) & m == u32::from(a) & m
            }
            (IpAddr::V6(n), IpAddr::V6(a)) => {
                let m = u128::MAX
                    .checked_shl(128 - u32::from(self.prefix))
                    .unwrap_or(0);
                u128::from(n) & m == u128::from(a) & m
            }
            _ => false,
        }
    }
}

/// The internal addresses an operator lets a tenant's code or sinks reach
/// (module notes). Empty by default: none.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InternalAllow(Vec<Range>);

impl InternalAllow {
    /// No internal address.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Every address and port (unit tests that serve on loopback).
    #[must_use]
    pub fn everything() -> Self {
        Self(vec![
            Range {
                net: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                prefix: 0,
                port: None,
            },
            Range {
                net: IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                prefix: 0,
                port: None,
            },
        ])
    }

    /// Parses the setting's text: a JSON list of strings, or
    /// comma-separated entries.
    ///
    /// # Errors
    /// A message naming the entry that isn't `address`, `address/prefix`,
    /// `address:port`, `address/prefix:port` or, for IPv6 with a port,
    /// `[address/prefix]:port`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let t = text.trim();
        // `["10.0.0.1"]` and `[]` are JSON; `[::1]:9000` is an entry.
        let json = t
            .strip_prefix('[')
            .is_some_and(|rest| rest.trim_start().starts_with(['"', ']']));
        let entries: Vec<String> = if json {
            serde_json::from_str(t).map_err(|e| format!("not a JSON list of strings: {e}"))?
        } else {
            t.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .collect()
        };
        entries
            .iter()
            .map(|e| parse_range(e.trim()))
            .collect::<Result<Vec<_>, _>>()
            .map(Self)
    }

    /// As [`Self::parse`], but an unreadable value allows nothing (the
    /// stored value was validated when it was set).
    #[must_use]
    pub fn parse_or_none(text: &str) -> Self {
        Self::parse(text).unwrap_or_default()
    }

    /// Whether `ip:port` is covered.
    #[must_use]
    pub fn covers(&self, ip: IpAddr, port: u16) -> bool {
        self.0
            .iter()
            .any(|r| r.port.is_none_or(|p| p == port) && r.contains(ip))
    }

    /// Whether nothing is covered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn parse_range(entry: &str) -> Result<Range, String> {
    let bad = || {
        format!(
            "'{entry}' isn't an address or CIDR range with an optional port \
             (10.0.5.0/24, 10.0.5.7:5432, [fd12::/32]:443)"
        )
    };
    let (net, port) = if let Some(rest) = entry.strip_prefix('[') {
        match rest.split_once(']') {
            Some((n, "")) => (n, None),
            Some((n, p)) => (n, Some(p.strip_prefix(':').ok_or_else(bad)?)),
            None => return Err(bad()),
        }
    } else if entry.matches(':').count() == 1 {
        let (n, p) = entry.split_once(':').ok_or_else(bad)?;
        (n, Some(p))
    } else {
        (entry, None)
    };
    let port = match port {
        None => None,
        Some(p) => Some(p.parse::<u16>().ok().filter(|p| *p != 0).ok_or_else(bad)?),
    };
    let (addr, prefix) = match net.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (net, None),
    };
    let net: IpAddr = addr.trim().parse().map_err(|_| bad())?;
    let max = if net.is_ipv4() { 32 } else { 128 };
    let prefix = match prefix {
        None => max,
        Some(p) => p.parse::<u8>().ok().filter(|p| *p <= max).ok_or_else(bad)?,
    };
    Ok(Range { net, prefix, port })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn internal_addresses() {
        for a in [
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "127.0.0.1",
            "127.8.8.8",
            "169.254.169.254",
            "169.254.1.1",
            "100.64.0.1",
            "100.100.100.200",
            "0.0.0.0",
            "0.1.2.3",
            "192.0.0.8",
            "198.18.0.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fc00::1",
            "fd00:ec2::254",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "::ffff:10.0.0.1",
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            "::127.0.0.1",
            "::10.0.0.1",
            "64:ff9b::7f00:1",
            "64:ff9b::a9fe:a9fe",
            "64:ff9b:1::1",
            "2002:7f00:1::1",
            "2002:a9fe:a9fe::1",
            "2001:0:1::1",
        ] {
            assert!(is_internal(ip(a)), "{a}");
        }
        for a in [
            "93.184.216.34",
            "8.8.8.8",
            "100.63.0.1",
            "100.128.0.1",
            "198.17.0.1",
            "198.20.0.1",
            "2606:2800:220:1::1",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:808:808::1",
            "2001:db9::1",
        ] {
            assert!(!is_internal(ip(a)), "{a}");
        }
    }

    #[test]
    fn metadata_addresses_in_every_form() {
        for a in [
            "169.254.169.254",
            "169.254.170.2",
            "169.254.170.23",
            "100.100.100.200",
            "fd00:ec2::254",
            "::ffff:169.254.169.254",
            "::169.254.169.254",
            "64:ff9b::a9fe:a9fe",
            "2002:a9fe:a9fe::",
        ] {
            assert!(is_metadata(ip(a)), "{a}");
            assert!(is_internal(ip(a)), "{a}");
        }
        for a in [
            "169.254.169.253",
            "10.0.0.1",
            "127.0.0.1",
            "8.8.8.8",
            "fd00:ec2::253",
        ] {
            assert!(!is_metadata(ip(a)), "{a}");
        }
    }

    #[test]
    fn an_operators_list_covers_ranges_and_ports() {
        assert!(InternalAllow::parse("").unwrap().is_empty());
        assert!(InternalAllow::parse("[]").unwrap().is_empty());
        assert!(!InternalAllow::none().covers(ip("127.0.0.1"), 80));
        let a = InternalAllow::parse(
            r#"["10.0.5.0/24:5432", "127.0.0.1:9000", "192.168.7.7", "[fd12::/32]:443", "fd34::1"]"#,
        )
        .unwrap();
        assert!(a.covers(ip("10.0.5.7"), 5432));
        assert!(!a.covers(ip("10.0.5.7"), 22));
        assert!(!a.covers(ip("10.0.6.7"), 5432));
        assert!(a.covers(ip("127.0.0.1"), 9000));
        assert!(!a.covers(ip("127.0.0.1"), 7480));
        assert!(!a.covers(ip("127.0.0.2"), 9000));
        assert!(a.covers(ip("192.168.7.7"), 1));
        assert!(a.covers(ip("192.168.7.7"), 65535));
        assert!(a.covers(ip("fd12::9"), 443));
        assert!(!a.covers(ip("fd12::9"), 80));
        assert!(!a.covers(ip("fd13::9"), 443));
        assert!(a.covers(ip("fd34::1"), 8080));
        // The IPv6 spellings of a covered IPv4 host are covered with it,
        // and of an uncovered one are not.
        assert!(a.covers(ip("::ffff:10.0.5.7"), 5432));
        assert!(!a.covers(ip("::ffff:10.0.9.7"), 5432));
        // Comma-separated, the same.
        let b = InternalAllow::parse(" 10.0.5.0/24:5432 , [::1]:9000, ::1 ").unwrap();
        assert!(b.covers(ip("10.0.5.1"), 5432));
        assert!(b.covers(ip("::1"), 9000));
        assert!(b.covers(ip("::1"), 1));
        let one = InternalAllow::parse("[::1]:9000").unwrap();
        assert!(one.covers(ip("::1"), 9000));
        assert!(!one.covers(ip("::1"), 9001));
        // Everything, for unit tests.
        let all = InternalAllow::everything();
        assert!(all.covers(ip("127.0.0.1"), 1) && all.covers(ip("fd00::1"), 1));
    }

    #[test]
    fn a_bad_entry_is_named() {
        for bad in [
            "localhost",
            "10.0.0.0/33",
            "10.0.0.1:0",
            "10.0.0.1:http",
            "fd12::/129",
            "[fd12::1]443",
            "[fd12::1",
            "*.internal",
            r#"["10.0.0.1", 5]"#,
        ] {
            let e = InternalAllow::parse(bad).unwrap_err();
            assert!(!e.is_empty(), "{bad}");
            assert!(InternalAllow::parse_or_none(bad).is_empty(), "{bad}");
        }
    }
}
