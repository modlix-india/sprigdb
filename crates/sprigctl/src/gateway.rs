//! `sprigctl gateway list|show|drain` (the Admin API's
//! `Gateways` service): the DC's registered gateways, and draining one.
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
        other => return Err(Failed(format!("unknown subcommand {other}"))),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
