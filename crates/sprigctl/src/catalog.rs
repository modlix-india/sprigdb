//! `sprigctl catalog contracts [show|run]`, over the Admin API's
//! `/admin/v1/catalog/contracts`.
//!
//! ```text
//! sprigctl catalog contracts [show]       # readers, and each module's contract ceiling
//! sprigctl catalog contracts run --reason "<why>" --operator <who> [--tenant acme]
//! ```
//!
//! A catalog module's contract migration removes what older binaries
//! read. The controller runs it once every running gateway, worker and
//! controller reads the new format and `catalog.contract_delay_days` has
//! passed since the module's last expand. `run` runs the contracts the
//! readers allow now, before the delay is over: the reason and the
//! operator are required (39084 otherwise) and are recorded in each
//! migration's history. It never runs a contract past a registered
//! reader's format.

use std::fmt::Write as _;

use clap::{Arg, ArgMatches, Command};
use serde::{Deserialize, Serialize};

use crate::client::AdminClient;
use crate::render::table;
use crate::{Failed, Output, print};

/// One module, as the Admin API returns it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModuleView {
    module: String,
    newest: u32,
    contracts: Vec<u32>,
    contract_ceiling: u32,
    held_by: Vec<String>,
}

/// `GET /admin/v1/catalog/contracts`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContractsView {
    contract_delay_days: u64,
    readers: Vec<String>,
    unregistered: Vec<String>,
    modules: Vec<ModuleView>,
}

/// One tenant's part of a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TenantView {
    tenant: String,
    applied: Vec<String>,
    delayed: Vec<String>,
    #[serde(default)]
    error: Option<String>,
}

/// `POST /admin/v1/catalog/contracts:run`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunView {
    #[serde(default)]
    runnable: std::collections::BTreeMap<String, u32>,
    #[serde(default)]
    tenants: Vec<TenantView>,
    #[serde(default)]
    note: Option<String>,
}

/// `sprigctl catalog`.
pub(crate) fn group() -> Command {
    Command::new("catalog")
        .about("Tenant catalogs' schema migrations")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("contracts")
                .about("Contract migrations: who holds them back, and running them early")
                .subcommand(
                    Command::new("show")
                        .about("The registered readers and each catalog module's contract ceiling"),
                )
                .subcommand(
                    Command::new("run")
                        .about(
                            "Run the contract migrations the readers allow now, before \
                             catalog.contract_delay_days is over (recorded with the reason)",
                        )
                        .arg(
                            Arg::new("reason")
                                .long("reason")
                                .required(true)
                                .help("Why the delay is cut short (recorded)"),
                        )
                        .arg(
                            Arg::new("operator")
                                .long("operator")
                                .required(true)
                                .help("Who decided, such as op:alice (recorded)"),
                        )
                        .arg(
                            Arg::new("tenant")
                                .long("tenant")
                                .help("Only this tenant's catalog"),
                        ),
                ),
        )
}

fn show_text(v: &ContractsView) -> String {
    let mut s = format!(
        "catalog.contract_delay_days: {}\nreaders: {}\n",
        v.contract_delay_days,
        if v.readers.is_empty() {
            "none".to_owned()
        } else {
            v.readers.join(", ")
        }
    );
    for u in &v.unregistered {
        let _ = writeln!(s, "no reader record (holds every contract back): {u}");
    }
    let rows: Vec<[String; 5]> = v
        .modules
        .iter()
        .map(|m| {
            [
                m.module.clone(),
                m.newest.to_string(),
                if m.contracts.is_empty() {
                    "-".to_owned()
                } else {
                    m.contracts
                        .iter()
                        .map(u32::to_string)
                        .collect::<Vec<_>>()
                        .join(",")
                },
                m.contract_ceiling.to_string(),
                if m.held_by.is_empty() {
                    "-".to_owned()
                } else {
                    m.held_by.join("; ")
                },
            ]
        })
        .collect();
    s.push_str(&table(
        ["MODULE", "NEWEST", "CONTRACTS", "CEILING", "HELD_BY"],
        &rows,
    ));
    s
}

fn run_text(r: &RunView) -> String {
    let mut s = String::new();
    if let Some(n) = &r.note {
        let _ = writeln!(s, "{n}");
    }
    for t in &r.tenants {
        let _ = writeln!(
            s,
            "{}: applied {}; delayed {}{}",
            t.tenant,
            if t.applied.is_empty() {
                "-".to_owned()
            } else {
                t.applied.join(", ")
            },
            if t.delayed.is_empty() {
                "-".to_owned()
            } else {
                t.delayed.join(", ")
            },
            t.error
                .as_deref()
                .map(|e| format!("; skipped: {e}"))
                .unwrap_or_default()
        );
    }
    if s.is_empty() {
        s.push_str("nothing to run\n");
    }
    s
}

/// Runs a `catalog` command.
pub(crate) async fn run(c: &AdminClient, sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some(("contracts", m)) = sub.subcommand() else {
        return Err(Failed("catalog: only `contracts` is available".into()));
    };
    match m.subcommand() {
        None | Some(("show", _)) => {
            let v: ContractsView = c.get("/admin/v1/catalog/contracts").await?;
            print(out, &v, show_text);
        }
        Some(("run", r)) => {
            let arg = |k: &str| r.get_one::<String>(k).cloned();
            let body = serde_json::json!({
                "reason": arg("reason").unwrap_or_default(),
                "operator": arg("operator").unwrap_or_default(),
                "tenant": arg("tenant"),
            });
            let v: RunView = c.post("/admin/v1/catalog/contracts:run", &body).await?;
            print(out, &v, run_text);
            let failed = v.tenants.iter().filter(|t| t.error.is_some()).count();
            if failed > 0 {
                return Err(Failed(format!(
                    "{failed} tenant(s) were skipped; the controller's pass retries them"
                )));
            }
        }
        Some((other, _)) => {
            return Err(Failed(format!(
                "catalog contracts {other}: not available in this version"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn the_group_parses_its_commands() {
        let m = group()
            .try_get_matches_from([
                "catalog",
                "contracts",
                "run",
                "--reason",
                "hotfix",
                "--operator",
                "op:alice",
            ])
            .unwrap();
        let (_, c) = m.subcommand().unwrap();
        let (cmd, r) = c.subcommand().unwrap();
        assert_eq!(cmd, "run");
        assert_eq!(r.get_one::<String>("operator").unwrap(), "op:alice");
        assert!(
            group()
                .try_get_matches_from(["catalog", "contracts", "run", "--reason", "x"])
                .is_err(),
            "the operator is required"
        );
        assert!(
            group()
                .try_get_matches_from(["catalog", "contracts"])
                .is_ok()
        );
    }

    #[test]
    fn views_render() {
        let v: ContractsView = serde_json::from_value(serde_json::json!({
            "contract_delay_days": 7,
            "readers": ["gateway g1 (dc dc-a)"],
            "unregistered": [],
            "modules": [{"module": "auth", "newest": 9, "contracts": [9],
                         "contract_ceiling": 8, "held_by": ["worker w1 (dc dc-a) reads 8"]}]
        }))
        .unwrap();
        let s = show_text(&v);
        assert!(s.contains("auth") && s.contains("reads 8"), "{s}");
        let r: RunView = serde_json::from_value(serde_json::json!({
            "runnable": {"auth": 9},
            "tenants": [{"tenant": "acme", "applied": ["auth 9 contract drop"],
                         "delayed": [], "error": null}]
        }))
        .unwrap();
        assert!(run_text(&r).contains("acme: applied auth 9 contract drop"));
    }
}
