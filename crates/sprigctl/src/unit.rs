//! `sprigctl unit list`: the storage engine's units one gateway holds,
//! from that gateway's own HTTP endpoint (`--endpoint`,
//! `GET /admin/v1/engine/units`): each unit's voters, learners, leader,
//! term, committed and applied indexes, and with units placed per tenant
//! its descriptor's home node. A gateway whose engine doesn't replicate
//! answers with no node and no units.

use clap::{Arg, ArgMatches, Command};
use serde_json::Value;

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
