//! `sprigctl gateway list|show|drain` (the Admin API's
//! `Gateways` service): the DC's registered gateways, and draining one.
//! `sprigctl gateway replace-replica` replaces a lost replica of the
//! storage engine's units: sent to a surviving gateway's own HTTP endpoint
//! (`--endpoint`), which moves each unit's leadership to itself and puts
//! the new node (started with `SPRIGDB_ENGINE_RAFT_JOIN=true`) in place of
//! the lost one. Run it against each survivor so every one learns the new
//! node's address; the first does the work.
//! Any gateway answers; a drain reaches the named gateway through the peer
//! service, which then finishes its work, closes its listeners and exits.
//! A gateway with a part off (its record's `degraded`, such as a peer
//! service without `[tls]` off loopback) shows `ready (degraded)` and a
//! line naming the part and the reason.

use std::fmt::Write as _;

use clap::{Arg, ArgMatches, Command};
use serde_json::Value;

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl gateway`.
pub(crate) fn group() -> Command {
    let id = || Arg::new("id").required(true).help("Gateway id ([node] id)");
    Command::new("gateway")
        .about("Gateways of the DC")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(Command::new("list").about("List the registered gateways"))
        .subcommand(
            Command::new("show")
                .about("Show one gateway's record")
                .arg(id()),
        )
        .subcommand(
            Command::new("drain")
                .about("Drain a gateway: readiness off, listeners closed, work finished, exit")
                .arg(id()),
        )
        .subcommand(
            Command::new("replace-replica")
                .about(
                    "Replace a lost engine replica with a new node, on the gateway at --endpoint",
                )
                .arg(
                    Arg::new("dead")
                        .long("dead")
                        .required(true)
                        .value_parser(clap::value_parser!(u64))
                        .help("Raft id of the lost node"),
                )
                .arg(
                    Arg::new("new")
                        .long("new")
                        .required(true)
                        .value_parser(clap::value_parser!(u64))
                        .help(
                            "Raft id of the new node (started with SPRIGDB_ENGINE_RAFT_JOIN=true)",
                        ),
                )
                .arg(
                    Arg::new("new-addr")
                        .long("new-addr")
                        .help("The new node's Raft address, host:port"),
                )
                .arg(
                    Arg::new("timeout")
                        .long("timeout")
                        .value_parser(clap::value_parser!(u64))
                        .default_value("120")
                        .help("Seconds to wait per unit"),
                ),
        )
}

/// The text `replace-replica` prints for the gateway's report.
fn replace_text(v: &Value) -> String {
    let n = |k: &str| v[k].as_array().map_or(0, Vec::len);
    let mut out = format!(
        "replaced in {} unit(s); {} already had the new replica; {} failed\n",
        n("replaced"),
        v["already"].as_u64().unwrap_or(0),
        n("failed")
    );
    for f in v["failed"].as_array().map_or(&[][..], Vec::as_slice) {
        let _ = writeln!(out, "  unit {}: {}", f[0], f[1].as_str().unwrap_or(""));
    }
    out
}

fn row(g: &Value) -> [String; 6] {
    let s = |k: &str| {
        g.get(k).map_or_else(String::new, |v| {
            v.as_str().map_or_else(|| v.to_string(), str::to_owned)
        })
    };
    let listeners = g["listeners"].as_object().map_or_else(String::new, |m| {
        m.iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<_>>()
            .join(" ")
    });
    let state = if degraded(g).is_empty() {
        s("state")
    } else {
        format!("{} (degraded)", s("state"))
    };
    [
        s("gateway_id"),
        s("slot"),
        state,
        s("addr"),
        format!("{}/{}", g["load"]["connections"], g["load"]["active"]),
        listeners,
    ]
}

/// The parts of a gateway that are off, with their reasons
/// (`degraded` in its record).
fn degraded(g: &Value) -> Vec<(String, String)> {
    g["degraded"].as_object().map_or_else(Vec::new, |m| {
        m.iter()
            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_owned()))
            .collect()
    })
}

/// A table of gateways, then one line per degraded part.
fn render(gateways: &[Value]) -> String {
    let rows: Vec<[String; 6]> = gateways.iter().map(row).collect();
    let mut out = table(
        [
            "GATEWAY",
            "SLOT",
            "STATE",
            "ADDR",
            "CONNS/ACTIVE",
            "LISTENERS",
        ],
        &rows,
    );
    for g in gateways {
        for (part, why) in degraded(g) {
            let id = g["gateway_id"].as_str().unwrap_or_default();
            let _ = writeln!(out, "{id}: {part} {why}");
        }
    }
    out
}

/// Runs `sprigctl gateway <cmd>`.
pub(crate) async fn run(c: &AdminClient, group: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((cmd, sub)) = group.subcommand() else {
        return Err(Failed("expected a subcommand".into()));
    };
    let id = || sub.get_one::<String>("id").cloned().unwrap_or_default();
    match cmd {
        "list" => {
            let v: Value = c.get("/admin/v1/gateways").await?;
            print(out, &v, |v| {
                render(v["gateways"].as_array().map_or(&[], Vec::as_slice))
            });
        }
        "show" => {
            let v: Value = c
                .get(&format!("/admin/v1/gateways/{}", escape(&id())))
                .await?;
            print(out, &v, |v| render(std::slice::from_ref(v)));
        }
        "drain" => {
            let v: Value = c
                .post(
                    &format!("/admin/v1/gateways/{}:drain", escape(&id())),
                    &serde_json::json!({}),
                )
                .await?;
            print(out, &v, |v| {
                if v["started"].as_bool().unwrap_or(false) {
                    format!("gateway {} is draining\n", id())
                } else {
                    format!("gateway {} was already draining\n", id())
                }
            });
        }
        "replace-replica" => {
            let num = |k: &str| sub.get_one::<u64>(k).copied().unwrap_or(0);
            let mut body = serde_json::json!({
                "dead": num("dead"),
                "new": num("new"),
                "timeout_seconds": num("timeout"),
            });
            if let Some(a) = sub.get_one::<String>("new-addr") {
                body["new_addr"] = Value::String(a.clone());
            }
            let v: Value = c.post("/admin/v1/engine/replicas:replace", &body).await?;
            print(out, &v, replace_text);
        }
        other => return Err(Failed(format!("unknown subcommand {other}"))),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replace_replica_report() {
        let v = serde_json::json!({"replaced": [7, 8], "already": 1,
            "failed": [[9, "its leadership didn't move here in time"]]});
        let t = replace_text(&v);
        assert!(t.starts_with("replaced in 2 unit(s); 1 already"), "{t}");
        assert!(t.contains("unit 9: its leadership"), "{t}");
        let m = group().try_get_matches_from([
            "gateway",
            "replace-replica",
            "--dead",
            "1",
            "--new",
            "4",
            "--new-addr",
            "h:1",
        ]);
        assert!(m.is_ok());
    }

    #[test]
    fn a_degraded_gateway_says_which_part_and_why() {
        let ok = serde_json::json!({"gateway_id": "gw-1", "slot": 1, "state": "ready",
            "addr": "h1", "load": {"connections": 2, "active": 0}, "listeners": {"mysql": 3306}});
        let off = serde_json::json!({"gateway_id": "gw-2", "slot": 2, "state": "ready",
            "addr": "h2", "load": {"connections": 0, "active": 0}, "listeners": {"mysql": 3306},
            "degraded": {"peer": "degraded: 1 registered worker(s) can't run triggers"}});
        let text = render(&[ok, off]);
        assert!(text.contains("ready (degraded)"), "{text}");
        assert_eq!(text.matches("(degraded)").count(), 1, "{text}");
        assert!(
            text.contains("gw-2: peer degraded: 1 registered worker(s) can't run triggers\n"),
            "{text}"
        );
    }
}
