//! `sprigctl ai incident list|show`: the administration agent's incidents,
//! over the Admin API's `/admin/v1/ai/incidents`.
//!
//! ```text
//! sprigctl ai incident list
//! sprigctl ai incident show 3f09a1c2b7de
//! sprigctl ai incident show 3f09a1c2b7de --payload
//! ```
//!
//! `show` prints the incident, the intake text exactly as it was sent, the
//! triage verdict and the advice; `--payload` adds every exact payload
//! sent to the provider, from the incident's conversation.

use std::fmt::Write as _;

use clap::{Arg, ArgAction, ArgMatches, Command};
use serde_json::Value;

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// `sprigctl ai incident`.
pub(super) fn command() -> Command {
    Command::new("incident")
        .about("Incidents the administration agent counted, triaged and advised on")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(Command::new("list").about("This DC's incidents, newest first"))
        .subcommand(
            Command::new("show")
                .about("One incident: what was sent, triage and the advice")
                .arg(
                    Arg::new("fingerprint")
                        .required(true)
                        .help("The incident's fingerprint (12 hex digits)"),
                )
                .arg(
                    Arg::new("payload")
                        .long("payload")
                        .action(ArgAction::SetTrue)
                        .help("Also print every exact payload sent to the AI provider"),
                ),
        )
}

fn s(v: &Value, k: &str) -> String {
    match v.get(k) {
        Some(Value::String(x)) => x.clone(),
        Some(Value::Null) | None => String::new(),
        Some(x) => x.to_string(),
    }
}

/// The listing as a table.
pub(super) fn list_table(v: &Value) -> String {
    let rows: Vec<[String; 7]> = v["incidents"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|i| {
            let code = match s(i, "code_name").as_str() {
                "" => s(i, "code"),
                n => format!("{} {n}", s(i, "code")),
            };
            [
                s(i, "fingerprint"),
                code,
                format!("{} {}", s(i, "component"), s(i, "span")),
                s(i, "last_hour"),
                s(i, "status"),
                s(i, "severity"),
                s(i, "advice_id"),
            ]
        })
        .collect();
    if rows.is_empty() {
        return "no incidents\n".to_owned();
    }
    table(
        [
            "FINGERPRINT",
            "CODE",
            "WHERE",
            "LAST HOUR",
            "STATUS",
            "SEVERITY",
            "ADVICE",
        ],
        &rows,
    )
}

/// One incident as text.
pub(super) fn show_text(v: &Value, payloads: bool) -> String {
    let i = &v["incident"];
    let mut t = String::new();
    let _ = writeln!(t, "Incident {}", s(i, "fingerprint"));
    let _ = writeln!(
        t,
        "Code:      {} {}\nWhere:     {} {}\nStatus:    {}{}",
        s(i, "code"),
        s(i, "code_name"),
        s(i, "component"),
        s(i, "span"),
        s(i, "status"),
        match s(i, "held").as_str() {
            "" => String::new(),
            h => format!(" (held: {h})"),
        }
    );
    let _ = writeln!(
        t,
        "Counts:    {} in the last hour, baseline {:.1} per hour, {} in all",
        s(v, "last_hour"),
        v["baseline_per_hour"].as_f64().unwrap_or(0.0),
        s(i, "total")
    );
    for (label, k) in [
        ("Triage", "verdict"),
        ("Severity", "severity"),
        ("Advice id", "advice_id"),
        ("Error", "error"),
    ] {
        let x = s(i, k);
        if !x.is_empty() {
            let _ = writeln!(t, "{:<10} {x}", format!("{label}:"));
        }
    }
    let intake = s(i, "intake");
    if !intake.is_empty() {
        let _ = writeln!(t, "\nWhat was sent (the intake):\n{intake}");
    }
    let advice = s(i, "advice");
    if !advice.is_empty() {
        let _ = writeln!(t, "\nAdvice:\n{advice}");
    }
    if let Some(sent) = i["sent"].as_array().filter(|a| !a.is_empty()) {
        let _ = writeln!(t, "\nPayloads sent (SHA-256):");
        for h in sent {
            let _ = writeln!(t, "  {}", h.as_str().unwrap_or_default());
        }
    }
    let conv = &v["conversation"];
    if !conv.is_null() {
        let _ = writeln!(t, "\nConversation {}", s(conv, "id"));
        if let Some(why) = conv.get("unavailable") {
            let _ = writeln!(t, "  unavailable: {}", why.as_str().unwrap_or_default());
        }
        if payloads {
            for m in conv["messages"].as_array().into_iter().flatten() {
                if let Some(p) = m["body"].get("payload") {
                    let _ = writeln!(
                        t,
                        "\n--- payload {} ---\n{}",
                        s(m, "sent_sha256"),
                        p.as_str().map_or_else(|| p.to_string(), str::to_owned)
                    );
                }
            }
        }
    }
    t
}

/// Runs `sprigctl ai incident ...`.
///
/// # Errors
/// The API's.
pub(super) async fn run(c: &AdminClient, m: &ArgMatches, out: Output) -> Result<(), Failed> {
    match m.subcommand() {
        Some(("list", _)) => {
            let r: Value = c.get("/admin/v1/ai/incidents").await?;
            print(out, &r, list_table);
        }
        Some(("show", sub)) => {
            let fp = sub
                .get_one::<String>("fingerprint")
                .cloned()
                .unwrap_or_default();
            let payloads = sub.get_flag("payload");
            let r: Value = c
                .get(&format!(
                    "/admin/v1/ai/incidents/{}?payloads={payloads}",
                    escape(&fp)
                ))
                .await?;
            print(out, &r, |r| show_text(r, payloads));
        }
        _ => return Err(Failed("sprigctl ai incident list|show".into())),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn list_and_show_render() {
        let t = list_table(
            &json!({"incidents": [{"fingerprint": "3f09a1c2b7de", "code": 39100,
            "code_name": "ER_SPRIG_CLUSTER_UNAVAILABLE", "component": "controller",
            "span": "loop.balancer", "last_hour": 12, "status": "advised",
            "severity": "high", "advice_id": "dc.dev1.4"}]}),
        );
        assert!(
            t.contains("3f09a1c2b7de") && t.contains("dc.dev1.4") && t.contains("loop.balancer")
        );
        assert_eq!(list_table(&json!({"incidents": []})), "no incidents\n");
        let v = json!({
            "incident": {"fingerprint": "3f09a1c2b7de", "code": 39100, "code_name": "ER_SPRIG_CLUSTER_UNAVAILABLE",
                "component": "controller", "span": "loop.balancer", "status": "advised", "total": 40,
                "verdict": "needs_advice", "severity": "high", "intake": "Incident fingerprint 3f09a1c2b7de: ...",
                "advice": "Severity: high", "sent": ["ab12"]},
            "last_hour": 12, "baseline_per_hour": 0.5,
            "conversation": {"id": "a7", "messages": [{"kind": "system_note", "sent_sha256": "ab12",
                "body": {"payload": "{\"model\":\"m\"}"}}]}
        });
        let plain = show_text(&v, false);
        assert!(plain.contains("What was sent (the intake):\nIncident fingerprint"));
        assert!(plain.contains("Conversation a7") && !plain.contains("--- payload"));
        let full = show_text(&v, true);
        assert!(full.contains("--- payload ab12 ---\n{\"model\":\"m\"}"));
        let m = command().get_matches_from(["incident", "show", "3f09a1c2b7de", "--payload"]);
        let (_, sub) = m.subcommand().unwrap();
        assert!(sub.get_flag("payload"));
    }
}
