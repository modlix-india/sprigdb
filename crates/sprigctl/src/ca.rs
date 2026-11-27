//! `sprigctl ca`: the internal CA that certifies engine nodes for the links
//! between them, over the Admin API's `/admin/v1/ca`.
//!
//! ```text
//! sprigctl ca init                       # make the root and this DC's issuing CA (where missing)
//! sprigctl ca show                       # the CAs, every node's certificate, revocations
//! sprigctl ca renew <node>               # issue the node a new certificate now
//! sprigctl ca revoke <node> [--reason r] # revoke its certificates; nodes close its connections
//! sprigctl ca readmit <node>             # a revoked node may have certificates again
//! sprigctl ca dev-certs <dir> <node>...  # offline: a development CA and node certificates
//! ```
//!
//! Nodes started with `SPRIGDB_ENGINE_RAFT_TLS_ISSUED=on` make their own
//! keys, publish the public halves and receive 30-day certificates the DC
//! leader's controller renews, with no restart. `dev-certs` needs no
//! controller: it writes `<dir>/ca.pem` and, per node,
//! `<dir>/node-<id>/{ca.pem,node.pem,node-key.pem}` for a node's
//! `SPRIGDB_ENGINE_RAFT_TLS_DIR`.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use clap::{Arg, ArgMatches, Command};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::client::AdminClient;
use crate::render::table;
use crate::{Failed, Output, print};

/// `sprigctl ca`.
pub(crate) fn group() -> Command {
    let node = || Arg::new("node").required(true).help("The node's Raft id");
    Command::new("ca")
        .about("The internal CA: node certificates for the links between nodes")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("init")
                .about("Make the root CA and this DC's issuing CA where they are missing"),
        )
        .subcommand(Command::new("show").about("The CAs, every node's certificate and revocations"))
        .subcommand(
            Command::new("renew")
                .about("Issue a node a new certificate now")
                .arg(node()),
        )
        .subcommand(
            Command::new("revoke")
                .about("Revoke a node's certificates: every node closes its connections, and it gets no new one")
                .arg(node())
                .arg(Arg::new("reason").long("reason").help("Why (recorded)")),
        )
        .subcommand(
            Command::new("readmit")
                .about("Let a revoked node have certificates again (issues one now)")
                .arg(node()),
        )
        .subcommand(
            Command::new("dev-certs")
                .about("Offline: a development CA and certificates for the given nodes")
                .arg(Arg::new("dir").required(true).help("Where to write them"))
                .arg(
                    Arg::new("nodes")
                        .required(true)
                        .num_args(1..)
                        .value_parser(clap::value_parser!(u64))
                        .help("Node ids"),
                )
                .arg(
                    Arg::new("days")
                        .long("days")
                        .default_value("30")
                        .value_parser(clap::value_parser!(u64))
                        .help("Validity of the node certificates"),
                ),
        )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CaSummary {
    subject: String,
    fingerprint: String,
    not_after: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeView {
    node: u64,
    requested: bool,
    serial: String,
    not_after: u64,
    renew_at: u64,
    key_matches: bool,
    revoked: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Revoked {
    serial: String,
    dc: String,
    node: u64,
    not_after: u64,
    at: u64,
    #[serde(default)]
    reason: String,
}

/// `GET /admin/v1/ca`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CaView {
    enabled: bool,
    node_cert_seconds: u64,
    root: Option<CaSummary>,
    issuing: Option<CaSummary>,
    nodes: Vec<NodeView>,
    revoked: Vec<Revoked>,
}

/// Unix seconds as `YYYY-MM-DD HH:MM:SS` (UTC); `-` for 0.
#[allow(clippy::many_single_char_names)] // the civil-date algorithm's names
fn when(s: u64) -> String {
    if s == 0 {
        return "-".to_owned();
    }
    let days = i64::try_from(s / 86_400).unwrap_or(0);
    let rem = s % 86_400;
    // Days to a civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn show_text(v: &CaView) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "CA key on this controller: {}; node certificates valid {} s",
        if v.enabled { "yes" } else { "no" },
        v.node_cert_seconds
    );
    for (what, c) in [("root", &v.root), ("issuing", &v.issuing)] {
        match c {
            Some(c) => {
                let _ = writeln!(
                    s,
                    "{what}: {} (until {}, SHA-256 {})",
                    c.subject,
                    when(c.not_after),
                    c.fingerprint
                );
            }
            None => {
                let _ = writeln!(s, "{what}: none (sprigctl ca init)");
            }
        }
    }
    let rows: Vec<[String; 6]> = v
        .nodes
        .iter()
        .map(|n| {
            [
                n.node.to_string(),
                if n.serial.is_empty() {
                    "-".to_owned()
                } else {
                    n.serial.clone()
                },
                when(n.not_after),
                when(n.renew_at),
                if n.revoked {
                    "revoked".to_owned()
                } else if !n.requested {
                    "no key".to_owned()
                } else if n.key_matches {
                    "ok".to_owned()
                } else {
                    "waiting".to_owned()
                },
                if n.key_matches { "yes" } else { "no" }.to_owned(),
            ]
        })
        .collect();
    s.push_str(&table(
        [
            "NODE",
            "SERIAL",
            "NOT_AFTER",
            "RENEW_AT",
            "STATE",
            "KEY_MATCHES",
        ],
        &rows,
    ));
    if !v.revoked.is_empty() {
        let _ = writeln!(s, "revoked:");
        for r in &v.revoked {
            let _ = writeln!(
                s,
                "  {} node {} ({}) at {}: {}",
                r.serial,
                r.node,
                r.dc,
                when(r.at),
                r.reason
            );
        }
    }
    s
}

fn json_text(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default() + "\n"
}

/// Runs a `ca` command.
pub(crate) async fn run(c: &AdminClient, sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let node = |m: &ArgMatches| m.get_one::<String>("node").cloned().unwrap_or_default();
    match sub.subcommand() {
        Some(("init", _)) => {
            let v: Value = c.post("/admin/v1/ca:init", &json!({})).await?;
            print(out, &v, json_text);
        }
        Some(("show", _)) => {
            let v: CaView = c.get("/admin/v1/ca").await?;
            print(out, &v, show_text);
        }
        Some((action @ ("renew" | "revoke" | "readmit"), m)) => {
            let body = json!({
                "action": action,
                "reason": m.try_get_one::<String>("reason").ok().flatten(),
            });
            let v: Value = c
                .post(&format!("/admin/v1/ca/nodes/{}", node(m)), &body)
                .await?;
            print(out, &v, json_text);
        }
        Some(("dev-certs", m)) => {
            let dir = PathBuf::from(m.get_one::<String>("dir").cloned().unwrap_or_default());
            let ids: Vec<u64> = m
                .get_many::<u64>("nodes")
                .map(|v| v.copied().collect())
                .unwrap_or_default();
            let days = m.get_one::<u64>("days").copied().unwrap_or(30);
            let written = dev_certs(&dir, &ids, days).map_err(Failed)?;
            for w in written {
                println!("{}", w.display());
            }
        }
        _ => {
            return Err(Failed(
                "ca: expected init, show, renew, revoke, readmit or dev-certs".into(),
            ));
        }
    }
    Ok(())
}

/// Writes a development CA and certificates for `ids` under `dir`;
/// returns each node's directory.
fn dev_certs(dir: &Path, ids: &[u64], days: u64) -> Result<Vec<PathBuf>, String> {
    use rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose,
        IsCa, Issuer, KeyPair, KeyUsagePurpose,
    };
    let e = |w: &str, x: &dyn std::fmt::Display| format!("ca dev-certs: {w}: {x}");
    std::fs::create_dir_all(dir).map_err(|x| e(&dir.display().to_string(), &x))?;
    let now = SystemTime::now();
    let mut ca = CertificateParams::default();
    ca.distinguished_name = DistinguishedName::new();
    ca.distinguished_name
        .push(DnType::CommonName, "SprigDB internal CA (development)");
    ca.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    ca.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    ca.not_before = (now - Duration::from_secs(3600)).into();
    ca.not_after = (now + Duration::from_secs(5 * 365 * 86_400)).into();
    let ca_key = KeyPair::generate().map_err(|x| e("the CA key", &x))?;
    let ca_cert = ca.self_signed(&ca_key).map_err(|x| e("the CA", &x))?;
    std::fs::write(dir.join("ca.pem"), ca_cert.pem()).map_err(|x| e("ca.pem", &x))?;
    write_private(&dir.join("ca-key.pem"), &ca_key.serialize_pem())?;
    let issuer = Issuer::new(ca, ca_key);
    let mut out = Vec::new();
    for &id in ids {
        let name = format!("node-{id}");
        let mut p = CertificateParams::new(vec![name.clone()]).map_err(|x| e(&name, &x))?;
        p.distinguished_name = DistinguishedName::new();
        p.distinguished_name.push(DnType::CommonName, name.clone());
        p.not_before = (now - Duration::from_secs(3600)).into();
        p.not_after = (now + Duration::from_secs(days.max(1) * 86_400)).into();
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        p.use_authority_key_identifier_extension = true;
        let key = KeyPair::generate().map_err(|x| e(&name, &x))?;
        let cert = p.signed_by(&key, &issuer).map_err(|x| e(&name, &x))?;
        let d = dir.join(&name);
        std::fs::create_dir_all(&d).map_err(|x| e(&d.display().to_string(), &x))?;
        std::fs::write(d.join("ca.pem"), ca_cert.pem()).map_err(|x| e("ca.pem", &x))?;
        std::fs::write(d.join("node.pem"), cert.pem()).map_err(|x| e("node.pem", &x))?;
        write_private(&d.join("node-key.pem"), &key.serialize_pem())?;
        out.push(d);
    }
    Ok(out)
}

fn write_private(path: &Path, pem: &str) -> Result<(), String> {
    use std::io::Write as _;
    let mut o = std::fs::OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        o.mode(0o600);
    }
    o.open(path)
        .and_then(|mut f| f.write_all(pem.as_bytes()))
        .map_err(|e| format!("ca dev-certs: {}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn the_group_parses_its_commands() {
        let m = group()
            .try_get_matches_from(["ca", "revoke", "4", "--reason", "decommissioned"])
            .unwrap();
        let (cmd, r) = m.subcommand().unwrap();
        assert_eq!(cmd, "revoke");
        assert_eq!(r.get_one::<String>("node").unwrap(), "4");
        assert!(group().try_get_matches_from(["ca", "renew"]).is_err());
        let m = group()
            .try_get_matches_from(["ca", "dev-certs", "/tmp/x", "1", "2", "3"])
            .unwrap();
        let (_, d) = m.subcommand().unwrap();
        assert_eq!(d.get_many::<u64>("nodes").unwrap().count(), 3);
    }

    #[test]
    fn dates_render_in_utc() {
        assert_eq!(when(0), "-");
        assert_eq!(when(1_800_000_000), "2027-01-15 08:00:00");
    }

    #[test]
    fn dev_certs_name_their_nodes() {
        let dir = std::env::temp_dir().join(format!("sprigctl-ca-{}", std::process::id()));
        let written = dev_certs(&dir, &[1, 2], 30).unwrap();
        assert_eq!(written.len(), 2);
        let pem = std::fs::read_to_string(written[1].join("node.pem")).unwrap();
        assert!(pem.contains("BEGIN CERTIFICATE"));
        assert!(written[0].join("node-key.pem").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
