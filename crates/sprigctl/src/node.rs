//! `sprigctl node`: the DC's storage engine nodes.
//!
//! - `list`, `show <id>`: the registry (`GET /admin/v1/nodes` on the
//!   controller at `--endpoint`): state, readiness, version, units;
//! - `drain <id>`, `undrain <id>`: through the node's own gateway when it
//!   is up (`POST /admin/v1/engine/drain`: it hands every unit it leads to
//!   another voter and answers when it leads none), else on the registry;
//! - `remove <id>`, `restore <id>`: an operator's removal (its units are
//!   re-replicated on other nodes at once) and its undoing;
//! - `version`, `finalize <version>`: the cluster version (`GET` and
//!   `POST /admin/v1/cluster-version`);
//! - `rollout`: a rolling upgrade, one node at a time: drain, restart hook,
//!   health gate, undrain; a node that fails its gate is rolled back with
//!   the rollback hook and the rollout stops.

use std::fmt::Write as _;
use std::time::{Duration, Instant};

use clap::{Arg, ArgAction, ArgMatches, Command};
use serde_json::{Value, json};

use crate::client::AdminClient;
use crate::render::table;
use crate::{Failed, Output, print};

/// `sprigctl node`.
pub(crate) fn group() -> Command {
    let id = || Arg::new("id").required(true).help("The node's Raft id");
    let reason = || {
        Arg::new("reason")
            .long("reason")
            .default_value("")
            .help("Why, for the registry and the decision log")
    };
    Command::new("node")
        .about("Storage engine nodes: states, drains, removal, versions, rolling upgrades")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(Command::new("list").about("List the DC's engine nodes"))
        .subcommand(Command::new("show").about("Show one engine node").arg(id()))
        .subcommand(
            Command::new("drain")
                .about("Hand every unit the node leads to other voters; it keeps its replicas")
                .arg(id())
                .arg(reason())
                .arg(
                    Arg::new("deadline")
                        .long("deadline")
                        .value_parser(clap::value_parser!(u64))
                        .default_value("90")
                        .help("Seconds to wait until it leads nothing"),
                ),
        )
        .subcommand(
            Command::new("undrain")
                .about("Let the node lead units again")
                .arg(id()),
        )
        .subcommand(
            Command::new("remove")
                .about("Remove the node: its units are re-replicated on other nodes now")
                .arg(id())
                .arg(reason()),
        )
        .subcommand(
            Command::new("restore")
                .about("Undo a removal (or a dead mark): the node takes units again")
                .arg(id())
                .arg(reason()),
        )
        .subcommand(Command::new("version").about("Show the cluster version and each node's"))
        .subcommand(
            Command::new("finalize")
                .about("Raise the cluster version once every node runs it")
                .arg(Arg::new("version").required(true)),
        )
        .subcommand(
            Command::new("rollout")
                .about("Rolling upgrade: per node drain, restart hook, health gate, undrain; rollback on a failed gate")
                .arg(
                    Arg::new("to")
                        .long("to")
                        .required(true)
                        .help("The version every upgraded node must report"),
                )
                .arg(
                    Arg::new("restart-cmd")
                        .long("restart-cmd")
                        .required(true)
                        .help("Shell command that restarts a node on the new binary ({id}, {gateway}, {version} are replaced)"),
                )
                .arg(
                    Arg::new("rollback-cmd")
                        .long("rollback-cmd")
                        .help("Shell command that restarts a node on the old binary"),
                )
                .arg(
                    Arg::new("nodes")
                        .long("nodes")
                        .help("Node ids in order, comma-separated (default: every node not removed, by id)"),
                )
                .arg(
                    Arg::new("health-timeout")
                        .long("health-timeout")
                        .value_parser(clap::value_parser!(u64))
                        .default_value("180")
                        .help("Seconds a restarted node has to pass its health gate"),
                )
                .arg(
                    Arg::new("drain-timeout")
                        .long("drain-timeout")
                        .value_parser(clap::value_parser!(u64))
                        .default_value("90"),
                )
                .arg(
                    Arg::new("finalize")
                        .long("finalize")
                        .action(ArgAction::SetTrue)
                        .help("Raise the cluster version when every node is upgraded"),
                ),
        )
}

fn s(v: &Value, k: &str) -> String {
    match v.get(k) {
        Some(Value::String(x)) => x.clone(),
        Some(Value::Null) | None => "-".to_owned(),
        Some(x) => x.to_string(),
    }
}

fn render_list(v: &Value) -> String {
    let rows: Vec<[String; 9]> = v
        .as_array()
        .map_or(&[][..], Vec::as_slice)
        .iter()
        .map(|n| {
            [
                s(n, "id"),
                s(n, "state"),
                if n["ready"].as_bool().unwrap_or(false) {
                    "yes".into()
                } else {
                    "no".into()
                },
                s(n, "version"),
                s(n, "failure_domain"),
                s(n, "units"),
                s(n, "homes"),
                s(n, "http"),
                s(n, "reason"),
            ]
        })
        .collect();
    table(
        [
            "ID", "STATE", "READY", "VERSION", "DOMAIN", "UNITS", "HOMES", "HTTP", "REASON",
        ],
        &rows,
    )
}

async fn nodes(c: &AdminClient) -> Result<Vec<Value>, Failed> {
    let v: Value = c.get("/admin/v1/nodes").await?;
    Ok(v.as_array().cloned().unwrap_or_default())
}

async fn node(c: &AdminClient, id: u64) -> Result<Value, Failed> {
    nodes(c)
        .await?
        .into_iter()
        .find(|n| n["id"].as_u64() == Some(id))
        .ok_or_else(|| Failed(format!("node {id} has no registry record")))
}

fn id_of(sub: &ArgMatches) -> Result<u64, Failed> {
    sub.get_one::<String>("id")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| Failed("the node id must be a number".into()))
}

/// The node's gateway, when it is registered and ready.
fn gateway(c: &AdminClient, n: &Value) -> Option<AdminClient> {
    if !n["ready"].as_bool().unwrap_or(false) {
        return None;
    }
    n["http"].as_str().map(|h| c.at(h))
}

/// Drains node `id`, through its gateway when it is up.
async fn drain(c: &AdminClient, id: u64, reason: &str, deadline: u64) -> Result<Value, Failed> {
    let n = node(c, id).await?;
    if let Some(g) = gateway(c, &n) {
        let r: Value = g
            .post(
                "/admin/v1/engine/drain",
                &json!({"deadline_seconds": deadline, "reason": reason}),
            )
            .await?;
        return Ok(r);
    }
    let r: Value = c
        .post(
            &format!("/admin/v1/nodes/{id}:drain"),
            &json!({"reason": reason}),
        )
        .await?;
    Ok(r)
}

async fn undrain(c: &AdminClient, id: u64) -> Result<Value, Failed> {
    let n = node(c, id).await?;
    if let Some(g) = gateway(c, &n) {
        let r: Value = g.post("/admin/v1/engine/undrain", &json!({})).await?;
        return Ok(r);
    }
    let r: Value = c
        .post(&format!("/admin/v1/nodes/{id}:undrain"), &json!({}))
        .await?;
    Ok(r)
}

/// Runs `sprigctl node <cmd>`.
pub(crate) async fn run(c: &AdminClient, group: &ArgMatches, out: Output) -> Result<bool, Failed> {
    let Some((cmd, sub)) = group.subcommand() else {
        return Err(Failed("expected a subcommand".into()));
    };
    match cmd {
        "list" => {
            let v: Value = c.get("/admin/v1/nodes").await?;
            print(out, &v, render_list);
        }
        "show" => {
            let n = node(c, id_of(sub)?).await?;
            print(out, &n, |n| {
                serde_json::to_string_pretty(n).unwrap_or_default() + "\n"
            });
        }
        "drain" => {
            let deadline = *sub.get_one::<u64>("deadline").unwrap_or(&90);
            let reason = sub.get_one::<String>("reason").cloned().unwrap_or_default();
            let r = drain(c, id_of(sub)?, &reason, deadline).await?;
            print(out, &r, |r| {
                format!(
                    "node {}: led {} unit(s), now {} after {} ms{}\n",
                    s(r, "node"),
                    s(r, "leading_before"),
                    s(r, "leading_now"),
                    s(r, "took_ms"),
                    if r["drained"].as_bool().unwrap_or(true) {
                        ""
                    } else {
                        " (NOT drained by the deadline)"
                    }
                )
            });
            return Ok(r["drained"].as_bool().unwrap_or(true));
        }
        "undrain" => {
            let r = undrain(c, id_of(sub)?).await?;
            print(out, &r, |r| format!("state {}\n", s(r, "state")));
        }
        "remove" | "restore" => {
            let id = id_of(sub)?;
            let reason = sub.get_one::<String>("reason").cloned().unwrap_or_default();
            let r: Value = c
                .post(
                    &format!("/admin/v1/nodes/{id}:{cmd}"),
                    &json!({"reason": reason}),
                )
                .await?;
            print(out, &r, |r| {
                format!("node {}: {}\n", s(r, "id"), s(r, "state"))
            });
        }
        "version" => {
            let v: Value = c.get("/admin/v1/cluster-version").await?;
            let ns = nodes(c).await?;
            let all = json!({"cluster": v, "nodes": ns});
            print(out, &all, |a| {
                let mut t = format!(
                    "cluster version {}\n",
                    a["cluster"]["version"].as_str().unwrap_or("(not set)")
                );
                for n in a["nodes"].as_array().map_or(&[][..], Vec::as_slice) {
                    let _ = writeln!(
                        t,
                        "node {} {} {}",
                        s(n, "id"),
                        s(n, "version"),
                        s(n, "state")
                    );
                }
                t
            });
        }
        "finalize" => {
            let version = sub
                .get_one::<String>("version")
                .cloned()
                .unwrap_or_default();
            let r: Value = c
                .post(
                    "/admin/v1/cluster-version:finalize",
                    &json!({"version": version}),
                )
                .await?;
            print(out, &r, |r| {
                format!("cluster version {}\n", s(r, "version"))
            });
        }
        "rollout" => return rollout(c, sub).await,
        other => return Err(Failed(format!("unknown subcommand {other}"))),
    }
    Ok(true)
}

fn hook(cmd: &str, n: &Value, version: &str) -> String {
    cmd.replace("{id}", &s(n, "id"))
        .replace("{gateway}", &s(n, "gateway_id"))
        .replace("{version}", version)
}

async fn sh(cmd: &str) -> Result<(), Failed> {
    let c = cmd.to_owned();
    let st = tokio::task::spawn_blocking(move || {
        std::process::Command::new("sh").arg("-c").arg(&c).status()
    })
    .await
    .map_err(|e| Failed(format!("running {cmd:?}: {e}")))?
    .map_err(|e| Failed(format!("running {cmd:?}: {e}")))?;
    if st.success() {
        Ok(())
    } else {
        Err(Failed(format!("{cmd:?} exited with {st}")))
    }
}

/// The health gate of a restarted node: registered and ready, reporting
/// `version` (any, when empty), holding at least `units` replicas, every
/// one with a known leader and applied up to its commit index, twice in
/// a row. Returns what failed last when `timeout` passes.
async fn healthy(
    c: &AdminClient,
    id: u64,
    version: &str,
    units: usize,
    timeout: Duration,
) -> Result<(), String> {
    let end = Instant::now() + timeout;
    let mut good = 0;
    let mut why = String::from("not checked");
    while Instant::now() < end {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let n = match node(c, id).await {
            Ok(n) => n,
            Err(e) => {
                // A failed registry read says nothing about the node: keep
                // the last reason it gave.
                if why == "not checked" {
                    why = e.0;
                }
                good = 0;
                continue;
            }
        };
        let ok = async {
            if !n["ready"].as_bool().unwrap_or(false) {
                return Err("its gateway isn't ready".to_owned());
            }
            if !version.is_empty() && n["version"].as_str() != Some(version) {
                return Err(format!("it reports version {}", s(&n, "version")));
            }
            let g = gateway(c, &n).ok_or("no HTTP endpoint")?;
            let r: Value = g
                .get("/admin/v1/engine/units")
                .await
                .map_err(|e| e.to_string())?;
            let us = r["units"].as_array().cloned().unwrap_or_default();
            if us.len() < units {
                return Err(format!("it holds {} of {units} replicas", us.len()));
            }
            let behind = us
                .iter()
                .filter(|u| {
                    u["leader"].is_null()
                        || u["applied"].as_u64().unwrap_or(0) < u["committed"].as_u64().unwrap_or(0)
                })
                .count();
            if behind > 0 {
                return Err(format!("{behind} replica(s) without a leader or behind"));
            }
            Ok(())
        }
        .await;
        match ok {
            Ok(()) => {
                good += 1;
                if good >= 2 {
                    return Ok(());
                }
            }
            Err(e) => {
                why = e;
                good = 0;
            }
        }
    }
    Err(why)
}

async fn rollout(c: &AdminClient, sub: &ArgMatches) -> Result<bool, Failed> {
    let to = sub.get_one::<String>("to").cloned().unwrap_or_default();
    let restart = sub
        .get_one::<String>("restart-cmd")
        .cloned()
        .unwrap_or_default();
    let rollback = sub.get_one::<String>("rollback-cmd").cloned();
    let health = Duration::from_secs(*sub.get_one::<u64>("health-timeout").unwrap_or(&180));
    let drain_s = *sub.get_one::<u64>("drain-timeout").unwrap_or(&90);
    let all = nodes(c).await?;
    let order: Vec<u64> = match sub.get_one::<String>("nodes") {
        Some(list) => list
            .split(',')
            .filter_map(|x| x.trim().parse().ok())
            .collect(),
        None => all
            .iter()
            .filter(|n| n["state"].as_str() != Some("removed"))
            .filter_map(|n| n["id"].as_u64())
            .collect(),
    };
    let started = Instant::now();
    for id in order {
        let n = node(c, id).await?;
        if n["version"].as_str() == Some(to.as_str()) && n["ready"].as_bool().unwrap_or(false) {
            println!("node {id}: already at {to}");
            continue;
        }
        let units = match gateway(c, &n) {
            Some(g) => g
                .get::<Value>("/admin/v1/engine/units")
                .await
                .ok()
                .and_then(|r| r["units"].as_array().map(Vec::len))
                .unwrap_or(0),
            None => 0,
        };
        let node_started = Instant::now();
        let drained = drain(c, id, &format!("rolling upgrade to {to}"), drain_s).await?;
        if !drained["drained"].as_bool().unwrap_or(true) {
            undrain(c, id).await.ok();
            return Err(Failed(format!(
                "node {id} still led {} unit(s) after {drain_s} s; rollout stopped before restarting it",
                s(&drained, "leading_now")
            )));
        }
        println!(
            "node {id}: drained ({} unit(s) handed away in {} ms); restarting",
            s(&drained, "leading_before"),
            s(&drained, "took_ms")
        );
        let restarted = sh(&hook(&restart, &n, &to)).await;
        let gate = match restarted {
            Ok(()) => healthy(c, id, &to, units, health).await,
            Err(e) => Err(e.0),
        };
        match gate {
            Ok(()) => {
                undrain(c, id).await?;
                println!(
                    "node {id}: at {to}, healthy, undrained ({} ms)",
                    node_started.elapsed().as_millis()
                );
            }
            Err(why) => {
                eprintln!("node {id}: FAILED its health gate: {why}");
                if let Some(rb) = &rollback {
                    eprintln!("node {id}: rolling back");
                    let back = sh(&hook(rb, &n, &to)).await;
                    let ok = match back {
                        Ok(()) => healthy(c, id, "", units, health).await,
                        Err(e) => Err(e.0),
                    };
                    match ok {
                        Ok(()) => {
                            undrain(c, id).await.ok();
                            eprintln!("node {id}: rolled back to {}, undrained", s(&n, "version"));
                        }
                        Err(e) => eprintln!("node {id}: rollback ALSO failed: {e}"),
                    }
                }
                return Err(Failed(format!(
                    "rollout to {to} stopped at node {id}: {why}"
                )));
            }
        }
    }
    println!("rollout to {to} done in {} s", started.elapsed().as_secs());
    if sub.get_flag("finalize") {
        let r: Value = c
            .post(
                "/admin/v1/cluster-version:finalize",
                &json!({"version": to}),
            )
            .await?;
        println!("cluster version {}", s(&r, "version"));
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nodes_list_as_a_table_and_hooks_fill_in() {
        let v = json!([
            {"id": 1, "state": "active", "ready": true, "version": "0.1.0", "failure_domain": "a",
             "units": 7, "homes": 3, "http": "http://127.0.0.1:31511", "reason": "started"},
            {"id": 2, "state": "dead", "ready": false, "version": "0.1.0", "failure_domain": "",
             "units": 7, "homes": 2, "http": null, "reason": "gone"}
        ]);
        let t = render_list(&v);
        assert!(
            t.contains("dead") && t.contains("http://127.0.0.1:31511"),
            "{t}"
        );
        let h = hook("up.sh gw{id} {gateway} {version}", &v[0], "0.2.0");
        assert_eq!(h, "up.sh gw1 - 0.2.0");
        for args in [
            vec!["node", "list"],
            vec!["node", "drain", "3", "--deadline", "30"],
            vec!["node", "remove", "3", "--reason", "disk"],
            vec!["node", "restore", "3"],
            vec![
                "node",
                "rollout",
                "--to",
                "0.2.0",
                "--restart-cmd",
                "x {id}",
                "--finalize",
            ],
        ] {
            assert!(group().try_get_matches_from(args).is_ok());
        }
    }
}
