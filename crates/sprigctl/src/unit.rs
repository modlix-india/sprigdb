//! `sprigctl unit list`: the storage engine's units one gateway holds,
//! from that gateway's own HTTP endpoint (`--endpoint`,
//! `GET /admin/v1/engine/units`): each unit's voters, learners, leader,
//! term, committed and applied indexes, and with units placed per tenant
//! its descriptor's home node. A gateway whose engine doesn't replicate
//! answers with no node and no units.
//!
//! `sprigctl unit read-copies <tenant> <n>`: the tenant's extra read-only
//! copies, its `placement.read_copies` setting, written through the
//! controller's Admin API (`--endpoint`); each unit's leader then lists that
//! many learners in the unit's descriptor and adds them to its group.
//!
//! `sprigctl unit move <tenant> --from <node> --to <node>`: a planned move
//! of one replica of the tenant's unit, written into its descriptor through
//! the controller (`POST /admin/v1/units/{tenant}:move`); the unit's leader
//! copies the unit to the new node and takes the old one out. `sprigctl
//! unit home <tenant> <node>` names a voter the unit's home (its preferred
//! leader: a leadership move). `sprigctl unit balance` shows units and homes
//! per node, moves pending and what the balancer's next pass would do
//! (`GET /admin/v1/balancer/units`).

use std::fmt::Write as _;

use clap::{Arg, ArgMatches, Command};
use serde_json::Value;

use sprig_controller::admin::types::{ScopeRef, SetSettingRequest, SettingWriteResponse};

use crate::client::AdminClient;
use crate::render::table;
use crate::{Failed, Output, print};

/// `sprigctl unit`.
pub(crate) fn group() -> Command {
    Command::new("unit")
        .about("Storage engine units held by the gateway at --endpoint")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("List the units this gateway holds, with their Raft state")
                .arg(
                    Arg::new("leading")
                        .long("leading")
                        .action(clap::ArgAction::SetTrue)
                        .help("Only the units this gateway leads"),
                ),
        )
        .subcommand(
            Command::new("read-copies")
                .about(
                    "Set a tenant's extra read-only copies (placement.read_copies) through the controller at --endpoint",
                )
                .arg(Arg::new("tenant").required(true).help("The tenant's name"))
                .arg(
                    Arg::new("n")
                        .required(true)
                        .value_parser(clap::value_parser!(u64).range(0..=16))
                        .help("How many non-voting copies each of its units keeps (0 to 16)"),
                )
                .arg(
                    Arg::new("reason")
                        .long("reason")
                        .help("Why, for the settings audit"),
                ),
        )
        .subcommand(
            Command::new("move")
                .about("Move one replica of a tenant's unit to another node (controller at --endpoint)")
                .arg(Arg::new("tenant").required(true).help("The tenant's id or name"))
                .arg(
                    Arg::new("from")
                        .long("from")
                        .required(true)
                        .value_parser(clap::value_parser!(u64))
                        .help("The voter that leaves (Raft id)"),
                )
                .arg(
                    Arg::new("to")
                        .long("to")
                        .required(true)
                        .value_parser(clap::value_parser!(u64))
                        .help("The node that takes its place (Raft id)"),
                )
                .arg(Arg::new("reason").long("reason").help("Why, for the log")),
        )
        .subcommand(
            Command::new("home")
                .about("Name a voter the home (preferred leader) of a tenant's unit (controller at --endpoint)")
                .arg(Arg::new("tenant").required(true).help("The tenant's id or name"))
                .arg(
                    Arg::new("node")
                        .required(true)
                        .value_parser(clap::value_parser!(u64))
                        .help("A voter's Raft id"),
                ),
        )
        .subcommand(
            Command::new("balance").about(
                "Units and homes per node, moves pending, and the balancer's next pass (controller at --endpoint)",
            ),
        )
}

fn render_balance(v: &Value) -> String {
    let n = |x: &Value| match x {
        Value::Null => "-".to_owned(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let rows: Vec<[String; 5]> = v["nodes"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|r| {
                    [
                        n(&r["id"]),
                        n(&r["state"]),
                        n(&r["usable"]),
                        n(&r["units"]),
                        n(&r["homes"]),
                    ]
                })
                .collect()
        })
        .unwrap_or_default();
    let mut s = table(["NODE", "STATE", "USABLE", "UNITS", "HOMES"], &rows);
    s.push('\n');
    for p in v["pending"].as_array().into_iter().flatten() {
        let _ = writeln!(
            s,
            "pending: unit {} from node {} to node {}",
            n(&p[0]),
            n(&p[1]),
            n(&p[2])
        );
    }
    for p in v["planned"].as_array().into_iter().flatten() {
        let _ = writeln!(
            s,
            "next pass: {} tenant {} {} -> {} ({})",
            n(&p["kind"]),
            n(&p["tenant"]),
            n(&p["from"]),
            n(&p["to"]),
            n(&p["reason"])
        );
    }
    s
}

fn ids(v: &Value) -> String {
    v.as_array().map_or_else(String::new, |a| {
        a.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    })
}

fn row(u: &Value) -> [String; 8] {
    let n = |k: &str| {
        u.get(k)
            .filter(|v| !v.is_null())
            .map_or_else(|| "-".to_owned(), ToString::to_string)
    };
    let leader = if u["leading"].as_bool().unwrap_or(false) {
        format!("{} (here)", n("leader"))
    } else {
        n("leader")
    };
    [
        n("unit"),
        ids(&u["voters"]),
        ids(&u["learners"]),
        n("home"),
        leader,
        n("term"),
        n("committed"),
        n("applied"),
    ]
}

/// The text `unit list` prints for the gateway's report.
fn render(v: &Value, leading_only: bool) -> String {
    let Some(node) = v["node"].as_u64() else {
        return "this gateway's engine doesn't replicate: it holds every unit itself\n".into();
    };
    let units: Vec<&Value> = v["units"]
        .as_array()
        .map_or(&[][..], Vec::as_slice)
        .iter()
        .filter(|u| !leading_only || u["leading"].as_bool().unwrap_or(false))
        .collect();
    let rows: Vec<[String; 8]> = units.iter().map(|u| row(u)).collect();
    let placed = if v["placed"].as_bool().unwrap_or(false) {
        "placed per tenant"
    } else {
        "every unit on every node"
    };
    let leads = units
        .iter()
        .filter(|u| u["leading"].as_bool().unwrap_or(false))
        .count();
    let mut out = format!(
        "node {node} ({placed}): {} unit(s), leading {leads}\n",
        units.len()
    );
    out.push_str(&table(
        [
            "UNIT",
            "VOTERS",
            "LEARNERS",
            "HOME",
            "LEADER",
            "TERM",
            "COMMITTED",
            "APPLIED",
        ],
        &rows,
    ));
    out
}

/// Runs `sprigctl unit <cmd>`.
pub(crate) async fn run(c: &AdminClient, group: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((cmd, sub)) = group.subcommand() else {
        return Err(Failed("expected a subcommand".into()));
    };
    match cmd {
        "list" => {
            let leading = sub.get_flag("leading");
            let mut v: Value = c.get("/admin/v1/engine/units").await?;
            if leading && let Some(a) = v["units"].as_array_mut() {
                a.retain(|u| u["leading"].as_bool().unwrap_or(false));
            }
            print(out, &v, |v| render(v, leading));
        }
        "read-copies" => {
            let tenant = sub.get_one::<String>("tenant").cloned().unwrap_or_default();
            let n = sub.get_one::<u64>("n").copied().unwrap_or(0);
            let scope: ScopeRef = format!("tenant:{tenant}").parse().map_err(Failed)?;
            let req = SetSettingRequest {
                scope: Some(scope),
                value: None,
                value_text: Some(n.to_string()),
                reason: sub
                    .get_one::<String>("reason")
                    .cloned()
                    .unwrap_or_else(|| format!("{n} extra read copies")),
                actor: std::env::var("USER").ok().filter(|a| !a.trim().is_empty()),
            };
            let r: SettingWriteResponse = c
                .put("/admin/v1/settings/placement.read_copies", &req)
                .await?;
            print(out, &r, crate::render::settings_write);
        }
        "move" | "home" => {
            let tenant = sub.get_one::<String>("tenant").cloned().unwrap_or_default();
            let body = if cmd == "move" {
                serde_json::json!({
                    "from": sub.get_one::<u64>("from").copied().unwrap_or(0),
                    "to": sub.get_one::<u64>("to").copied().unwrap_or(0),
                    "reason": sub.get_one::<String>("reason").cloned().unwrap_or_else(|| "sprigctl unit move".to_owned()),
                })
            } else {
                serde_json::json!({"node": sub.get_one::<u64>("node").copied().unwrap_or(0)})
            };
            let v: Value = c
                .post(&format!("/admin/v1/units/{tenant}:{cmd}"), &body)
                .await?;
            print(out, &v, |v| {
                serde_json::to_string_pretty(v).unwrap_or_default()
            });
        }
        "balance" => {
            let v: Value = c.get("/admin/v1/balancer/units").await?;
            print(out, &v, render_balance);
        }
        other => return Err(Failed(format!("unknown subcommand {other}"))),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn units_report_as_a_table() {
        let v = serde_json::json!({"node": 2, "placed": true, "units": [
            {"unit": 0, "voters": [1, 2, 3], "learners": [], "home": null, "leader": 1,
             "leading": false, "term": 6, "committed": 40, "applied": 40},
            {"unit": 7, "voters": [2, 4, 5], "learners": [], "home": 2, "leader": 2,
             "leading": true, "term": 6, "committed": 12, "applied": 11}]});
        let t = render(&v, false);
        assert!(
            t.starts_with("node 2 (placed per tenant): 2 unit(s), leading 1\n"),
            "{t}"
        );
        assert!(t.contains("2,4,5"), "{t}");
        assert!(t.contains("2 (here)"), "{t}");
        let only = render(&v, true);
        assert!(
            only.contains("1 unit(s)") && !only.contains("1,2,3"),
            "{only}"
        );
        let one = render(
            &serde_json::json!({"node": null, "placed": false, "units": []}),
            false,
        );
        assert!(one.contains("doesn't replicate"), "{one}");
        assert!(
            group()
                .try_get_matches_from(["unit", "list", "--leading"])
                .is_ok()
        );
    }
}
