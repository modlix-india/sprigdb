//! `sprigctl tenant backup|backups|restore|backup-check`: a tenant's
//! backups on the storage engine. These go to a gateway's own HTTP
//! endpoint (`--endpoint`, the gateway whose engine holds the tenant),
//! which serves the node-local routes under `/admin/v1/engine/backups`
//! and passes tenant lookups and creation on to the controller.
//!
//! - `backup <tenant>`: a backup now (daily backups run on their own).
//! - `backups <tenant>`: the tenant's backups.
//! - `restore <tenant> --to <time> [--as <name>]`: creates tenant `<name>`
//!   (default `<tenant>-restored`) and restores into it the tenant as it
//!   stood at `<time>`: the newest backup at or before it plus the
//!   archived changes up to it.
//! - `backup-check <tenant> [--to <time>]`: rebuilds the tenant as of
//!   `<time>` in a scratch engine on the gateway and prints its checksum.
//!
//! `<time>`: `now`, an RFC 3339 time (`2026-11-23T10:15:00Z`,
//! `2026-11-23T15:45:00+05:30`), milliseconds since the epoch, or a
//! duration ago (`-30s`, `-15m`, `-2h`, `-1d`).

use std::fmt::Write as _;

use clap::{Arg, ArgMatches, Command};
use serde_json::{Value, json};
use sprig_controller::admin::types::{CreateTenantRequest, CreateTenantResponse};

use crate::client::AdminClient;
use crate::render::table;
use crate::{Failed, Output, print};

const ROUTE: &str = "/admin/v1/engine/backups";

/// Adds the commands to `sprigctl tenant`.
pub(crate) fn add_cmds(g: Command) -> Command {
    let name = || Arg::new("name").required(true).help("Tenant name");
    let to = |required: bool| {
        Arg::new("to")
            .long("to")
            .required(required)
            .help("The moment: now, RFC 3339, ms since the epoch, or -30s/-15m/-2h/-1d")
    };
    g.subcommand(
        Command::new("backup")
            .about("Take a backup of the tenant's engine data now (on the gateway at --endpoint)")
            .arg(name()),
    )
    .subcommand(
        Command::new("backups")
            .about("List the tenant's backups")
            .arg(name()),
    )
    .subcommand(
        Command::new("restore")
            .about("Restore the tenant as it stood at a moment into a new tenant")
            .arg(name())
            .arg(to(true))
            .arg(
                Arg::new("as")
                    .long("as")
                    .help("The new tenant's name (default <tenant>-restored)"),
            ),
    )
    .subcommand(
        Command::new("backup-check")
            .about("Rebuild the tenant as of a moment in a scratch engine and print its checksum")
            .arg(name())
            .arg(to(false)),
    )
}

/// Days since 1970-01-01 of a civil date (H. Hinnant's algorithm).
#[allow(clippy::many_single_char_names)]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Parses a moment; `None` is "now" (the newest archived state).
#[allow(clippy::many_single_char_names)]
pub(crate) fn parse_time(s: &str, now_ms: u64) -> Result<Option<u64>, String> {
    let s = s.trim();
    let bad = || {
        format!(
            "--to {s:?}: expected now, RFC 3339 (2026-11-23T10:15:00Z), ms since the epoch, or -15m/-2h/-1d"
        )
    };
    if s.eq_ignore_ascii_case("now") || s.eq_ignore_ascii_case("latest") {
        return Ok(None);
    }
    if let Some(d) = s.strip_prefix('-') {
        let (n, unit) = d.split_at(d.find(|c: char| !c.is_ascii_digit()).unwrap_or(d.len()));
        let n: u64 = n.parse().map_err(|_| bad())?;
        let ms = match unit {
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            _ => return Err(bad()),
        };
        return Ok(Some(now_ms.saturating_sub(n.saturating_mul(ms))));
    }
    if s.bytes().all(|b| b.is_ascii_digit()) {
        return s.parse().map(Some).map_err(|_| bad());
    }
    // RFC 3339: YYYY-MM-DDTHH:MM[:SS[.fff]](Z|±HH:MM)
    let b = s.as_bytes();
    if b.len() < 17
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't' | b' ')
        || b[13] != b':'
    {
        return Err(bad());
    }
    let num = |r: std::ops::Range<usize>| -> Result<i64, String> {
        s.get(r).and_then(|x| x.parse().ok()).ok_or_else(bad)
    };
    let (y, mo, d, h, mi) = (
        num(0..4)?,
        num(5..7)?,
        num(8..10)?,
        num(11..13)?,
        num(14..16)?,
    );
    let mut at = 16;
    let mut sec = 0i64;
    let mut frac_ms = 0i64;
    if b.get(at) == Some(&b':') {
        sec = num(at + 1..at + 3)?;
        at += 3;
        if b.get(at) == Some(&b'.') {
            let end = s[at + 1..]
                .find(|c: char| !c.is_ascii_digit())
                .map_or(s.len(), |i| at + 1 + i);
            let digits = &s[at + 1..end];
            let mut f = String::from(digits);
            f.truncate(3);
            while f.len() < 3 {
                f.push('0');
            }
            frac_ms = f.parse().map_err(|_| bad())?;
            at = end;
        }
    }
    let offset_min = match s.get(at..) {
        Some("Z" | "z") => 0,
        Some(o) if o.len() == 6 && (o.starts_with('+') || o.starts_with('-')) => {
            let oh: i64 = o[1..3].parse().map_err(|_| bad())?;
            let om: i64 = o[4..6].parse().map_err(|_| bad())?;
            let v = oh * 60 + om;
            if o.starts_with('-') { -v } else { v }
        }
        _ => return Err(bad()),
    };
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return Err(bad());
    }
    let secs = days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + sec - offset_min * 60;
    let ms = secs * 1000 + frac_ms;
    u64::try_from(ms).map(Some).map_err(|_| bad())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// `YYYY-MM-DD HH:MM:SS.mmm UTC` of a time in ms.
#[allow(clippy::many_single_char_names)]
fn utc(ms: u64) -> String {
    let secs = ms / 1000;
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    // Inverse of days_from_civil.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let sod = secs % 86_400;
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{:03} UTC",
        sod / 3600,
        (sod / 60) % 60,
        sod % 60,
        ms % 1000
    )
}

/// The physical milliseconds of an engine commit timestamp.
fn ts_ms(ts: u64) -> u64 {
    ts >> 16
}

async fn tenant_id(c: &AdminClient, name: &str) -> Result<u64, Failed> {
    let v: Value = c.get(&format!("/admin/v1/tenants/{name}")).await?;
    v["tenant_id"]
        .as_u64()
        .or_else(|| v["tenant_id"].as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| Failed(format!("tenant {name}: no tenant_id in the answer")))
}

fn backups_text(v: &Value) -> String {
    let rows: Vec<[String; 6]> = v["backups"]
        .as_array()
        .map_or(&[][..], Vec::as_slice)
        .iter()
        .map(|b| {
            let u = &b["units"][0];
            [
                b["taken_ms"].to_string(),
                utc(b["taken_ms"].as_u64().unwrap_or(0)),
                utc(ts_ms(b["ts"].as_u64().unwrap_or(0))),
                u["index"].to_string(),
                format!(
                    "{}/{}",
                    b["new_chunks"],
                    u["chunks"].as_array().map_or(0, Vec::len)
                        + u["parts"].as_array().map_or(0, Vec::len)
                ),
                format!("{} KiB", b["total_bytes"].as_u64().unwrap_or(0) / 1024),
            ]
        })
        .collect();
    table(
        ["ID", "TAKEN", "STATE AT", "LOG INDEX", "NEW/CHUNKS", "SIZE"],
        &rows,
    )
}

fn restore_text(v: &Value) -> String {
    let mut s = String::new();
    if v["tenant"] == v["into"] {
        let _ = writeln!(s, "rebuilt tenant {} in a scratch engine", v["tenant"]);
    } else {
        let _ = writeln!(
            s,
            "restored tenant {} into tenant {} ({})",
            v["tenant"],
            v["into"],
            v["mode"].as_str().unwrap_or("")
        );
    }
    let _ = writeln!(
        s,
        "from backup {} (log index {}), {} archived change(s) replayed",
        utc(v["backup"].as_u64().unwrap_or(0)),
        v["backup_index"],
        v["replayed"]
    );
    let _ = writeln!(
        s,
        "state as of the last change at {} (log index {})",
        utc(ts_ms(v["ts"].as_u64().unwrap_or(0))),
        v["index"]
    );
    if !v["reached"].as_bool().unwrap_or(true) && !v["to_ms"].is_null() {
        let _ = writeln!(
            s,
            "note: the archive ends before the requested moment; changes after the last archived one are not in it"
        );
    }
    let _ = writeln!(
        s,
        "checksum: {} entries, {} bytes, crc {:08x}",
        v["entries"],
        v["bytes"],
        v["crc"].as_u64().unwrap_or(0)
    );
    s
}

/// Runs `sprigctl tenant <cmd>` for the backup commands.
pub(crate) async fn run(
    c: &AdminClient,
    cmd: &str,
    sub: &ArgMatches,
    out: Output,
) -> Result<(), Failed> {
    let name = sub.get_one::<String>("name").cloned().unwrap_or_default();
    let to = match sub.get_one::<String>("to") {
        Some(t) => parse_time(t, now_ms()).map_err(Failed)?,
        None => None,
    };
    let id = tenant_id(c, &name).await?;
    match cmd {
        "backup" => {
            let v: Value = c
                .post(&format!("{ROUTE}:take"), &json!({"tenant": id}))
                .await?;
            print(out, &v, |v| {
                let m = &v["manifest"];
                format!(
                    "backup {} of tenant {name}: log index {}, {} new chunk(s) of {}, {} KiB new, {} ms\n",
                    m["taken_ms"],
                    m["units"][0]["index"],
                    m["new_chunks"],
                    m["units"][0]["chunks"].as_array().map_or(0, Vec::len),
                    m["new_bytes"].as_u64().unwrap_or(0) / 1024,
                    v["took_ms"]
                )
            });
        }
        "backups" => {
            let v: Value = c
                .post(&format!("{ROUTE}:list"), &json!({"tenant": id}))
                .await?;
            print(out, &v, backups_text);
        }
        "backup-check" => {
            let v: Value = c
                .post(
                    &format!("{ROUTE}:verify"),
                    &json!({"tenant": id, "to_ms": to}),
                )
                .await?;
            print(out, &v, restore_text);
        }
        "restore" => {
            let as_name = sub
                .get_one::<String>("as")
                .cloned()
                .unwrap_or_else(|| format!("{name}-restored"));
            if c.get::<Value>(&format!("/admin/v1/tenants/{as_name}"))
                .await
                .is_ok()
            {
                return Err(Failed(format!(
                    "tenant {as_name} exists already; choose another name with --as"
                )));
            }
            let created: CreateTenantResponse = c
                .post(
                    "/admin/v1/tenants",
                    &CreateTenantRequest {
                        name: as_name.clone(),
                        parent: None,
                        cluster: None,
                    },
                )
                .await?;
            let into = created.tenant.tenant_id.0;
            let v: Value = c
                .post(
                    &format!("{ROUTE}:restore"),
                    &json!({"tenant": id, "into": into, "to_ms": to, "replace": true}),
                )
                .await
                .map_err(|e| {
                    Failed(format!(
                        "{e}\n(tenant {as_name} was created for the restore and holds nothing usable; delete it or rerun with --as {as_name}-2)"
                    ))
                })?;
            print(out, &v, |v| {
                format!("tenant {as_name} (id {into}):\n{}", restore_text(v))
            });
        }
        _ => return Err(Failed(format!("unknown command {cmd}"))),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times() {
        let now = 1_763_900_000_000;
        assert_eq!(parse_time("now", now).unwrap(), None);
        assert_eq!(parse_time("-15m", now).unwrap(), Some(now - 900_000));
        assert_eq!(parse_time("-1d", now).unwrap(), Some(now - 86_400_000));
        assert_eq!(
            parse_time("1700000000123", now).unwrap(),
            Some(1_700_000_000_123)
        );
        assert_eq!(
            parse_time("1970-01-01T00:00:01Z", now).unwrap(),
            Some(1_000)
        );
        assert_eq!(
            parse_time("2026-11-23T10:15:00.25Z", now).unwrap(),
            parse_time("2026-11-23T15:45:00.250+05:30", now).unwrap()
        );
        let t = parse_time("2026-11-23T10:15:00Z", now).unwrap().unwrap();
        assert_eq!(utc(t), "2026-11-23 10:15:00.000 UTC");
        assert!(parse_time("yesterday", now).is_err());
        assert!(parse_time("2026-13-01T00:00:00Z", now).is_err());
        assert!(parse_time("-5w", now).is_err());
    }
}
