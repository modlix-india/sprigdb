//! `sprigctl logic`:
//!
//! - `compile <file>`: compiles a KIRun definition offline and prints its
//!   IR, placement and PL/SQL (what `EXPLAIN FUNCTION ... FORMAT=SQL`
//!   will show); `--table name:col=type,...` declares the tables its SQL
//!   reads (types `int`, `float`, `str`, `json`, `bool`);
//! - `deploy <file> --db <db> --gateway <url>`: sends `CREATE OR REPLACE
//!   PROCEDURE ... LANGUAGE kirun AS '...'` through a gateway as the user
//!   in the URL, and prints the notes the deploy left;
//! - `list --tenant t` and `show <name> --tenant t`: the tenant's logic
//!   modules, through the Admin API (`/admin/v1/tenants/{t}/logic/...`);
//!   `logs` and `dead-letters` take `--tenant` too;
//!
//! - `logs [name] [--limit n] [--follow]`: what after-commit triggers and
//!   schedules did (`logic_log`), oldest first;
//! - `dead-letters list [--binding b] [--state s]`, `dead-letters replay
//!   <id>... | --all [--binding b]` (back to the trigger's worker, which
//!   runs them in order with the active version) and `dead-letters
//!   discard <id>... | --all [--binding b]`.
//!
//! - `rollback <name> --db <db> [--to-version n] [--procedure] --gateway
//!   <url>`: sends `ALTER FUNCTION | PROCEDURE ... ROLLBACK [TO VERSION n]`
//!   through a gateway and prints the notes.

use clap::{Arg, ArgAction, ArgMatches, Command};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};
use serde_json::{Value, json};
use sprig_logic::declare::TierPref;
use sprig_logic_compile::ir::Ty;
use sprig_logic_compile::{Options, Runtime, StaticColumns, compile};

use crate::client::AdminClient;
use crate::settings::escape;
use crate::{Failed, Output};

/// `sprigctl logic`.
#[allow(clippy::too_many_lines)] // one builder chain
pub(crate) fn group() -> Command {
    let file = || {
        Arg::new("file")
            .required(true)
            .help("A KIRun definition (.json)")
    };
    let catalog = || {
        Arg::new("tenant")
            .long("tenant")
            .required(true)
            .help("The tenant")
    };
    Command::new("logic")
        .about("Logic functions")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("compile")
                .about("Compile a KIRun definition and print its IR, placement and PL/SQL")
                .arg(file())
                .arg(
                    Arg::new("table")
                        .long("table")
                        .action(ArgAction::Append)
                        .help("A table its SQL reads: name:col=type,col=type"),
                ),
        )
        .subcommand(
            Command::new("deploy")
                .about("Deploy a KIRun definition through a gateway (CREATE OR REPLACE PROCEDURE ... LANGUAGE kirun)")
                .arg(file())
                .arg(Arg::new("db").long("db").required(true).help("The database"))
                .arg(Arg::new("name").long("name").help("The routine's name (default: the file's stem)"))
                .arg(
                    Arg::new("gateway")
                        .long("gateway")
                        .required(true)
                        .help("mysql://tenant.user:password@host:port"),
                )
                .arg(
                    Arg::new("tier")
                        .long("tier")
                        .value_parser(["auto", "mariadb", "engine"])
                        .default_value("auto"),
                )
                .arg(
                    Arg::new("definer")
                        .long("definer")
                        .action(ArgAction::SetTrue)
                        .help("SQL SECURITY DEFINER"),
                ),
        )
        .subcommand(Command::new("list").about("List logic modules").arg(catalog()))
        .subcommand(
            Command::new("show")
                .about("Show a module's versions and placement")
                .arg(Arg::new("name").required(true))
                .arg(catalog()),
        )
        .subcommand(
            Command::new("logs")
                .about("What triggers and schedules did (logic_log), oldest first")
                .arg(Arg::new("name").help("A module's name (default: every module)"))
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .value_parser(clap::value_parser!(u32))
                        .default_value("50"),
                )
                .arg(
                    Arg::new("follow")
                        .long("follow")
                        .short('f')
                        .action(ArgAction::SetTrue)
                        .help("Keep printing new lines"),
                )
                .arg(catalog()),
        )
        .subcommand(
            Command::new("rollback")
                .about("Make an earlier version active again (ALTER FUNCTION f ROLLBACK [TO VERSION n])")
                .arg(Arg::new("name").required(true).help("The function or procedure"))
                .arg(Arg::new("db").long("db").required(true).help("The database"))
                .arg(
                    Arg::new("to-version")
                        .long("to-version")
                        .value_parser(clap::value_parser!(u64))
                        .help("The version (default: the newest earlier one that deployed)"),
                )
                .arg(
                    Arg::new("procedure")
                        .long("procedure")
                        .action(ArgAction::SetTrue)
                        .help("It is a procedure"),
                )
                .arg(
                    Arg::new("gateway")
                        .long("gateway")
                        .required(true)
                        .help("mysql://tenant.user:password@host:port"),
                ),
        )
        .subcommand(
            Command::new("dead-letters")
                .about("Trigger events that kept failing (after-commit, stream and queue triggers)")
                .subcommand_required(true)
                .subcommand(
                    Command::new("list")
                        .about("List dead letters, oldest first")
                        .arg(Arg::new("binding").long("binding").value_parser(clap::value_parser!(u64)))
                        .arg(
                            Arg::new("state")
                                .long("state")
                                .value_parser(["dead", "replaying", "replayed", "discarded"]),
                        )
                        .arg(
                            Arg::new("limit")
                                .long("limit")
                                .value_parser(clap::value_parser!(u32))
                                .default_value("100"),
                        )
                        .arg(catalog()),
                )
                .subcommand(dl_change("replay", "Replay dead letters with the active version, in order"))
                .subcommand(dl_change("discard", "Discard dead letters"))
                .arg_required_else_help(true),
        )
}

fn dl_change(name: &'static str, about: &'static str) -> Command {
    Command::new(name)
        .about(about)
        .arg(
            Arg::new("ids")
                .num_args(0..)
                .value_parser(clap::value_parser!(u64))
                .help("Dead letter ids"),
        )
        .arg(
            Arg::new("all")
                .long("all")
                .action(ArgAction::SetTrue)
                .help("Every dead letter (of --binding)"),
        )
        .arg(
            Arg::new("binding")
                .long("binding")
                .value_parser(clap::value_parser!(u64)),
        )
        .arg(
            Arg::new("tenant")
                .long("tenant")
                .required(true)
                .help("The tenant"),
        )
}

/// Whether the subcommand is implemented.
pub(crate) fn implemented(name: &str) -> bool {
    matches!(
        name,
        "compile" | "deploy" | "list" | "show" | "logs" | "dead-letters" | "rollback"
    )
}

fn read(m: &ArgMatches) -> Result<(String, String), Failed> {
    let path = m.get_one::<String>("file").cloned().unwrap_or_default();
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{path}: {e}"))?;
    Ok((path, text))
}

fn tables(m: &ArgMatches) -> Result<StaticColumns, Failed> {
    let mut cols = StaticColumns::new();
    for spec in m.get_many::<String>("table").into_iter().flatten() {
        let (t, rest) = spec
            .split_once(':')
            .ok_or_else(|| format!("--table {spec}: expected name:col=type,..."))?;
        for c in rest.split(',').filter(|c| !c.is_empty()) {
            let (col, ty) = c
                .split_once('=')
                .ok_or_else(|| format!("--table {spec}: expected col=type"))?;
            let ty = match ty {
                "int" => Ty::Int,
                "float" => Ty::Float,
                "str" => Ty::Str,
                "json" => Ty::Json,
                "bool" => Ty::Bool,
                other => return Err(format!("--table {spec}: unknown type {other}").into()),
            };
            cols.add(t, col, ty);
        }
    }
    Ok(cols)
}

async fn conn(url: &str) -> Result<Conn, Failed> {
    let opts = Opts::from_url(url).map_err(|e| format!("bad URL: {e}"))?;
    Conn::new(opts).await.map_err(|e| Failed(e.to_string()))
}

/// Runs `sprigctl logic <sub>`.
#[allow(clippy::too_many_lines)]
pub(crate) async fn run(api: &AdminClient, sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    match sub.subcommand() {
        Some(("compile", m)) => {
            let (_, text) = read(m)?;
            let c = compile(Runtime::Kirun, &text, &tables(m)?, &Options::default())
                .map_err(|e| e.to_string())?;
            let placed = sprig_logic::placement::place(&c, TierPref::Auto, true)
                .map_err(|e| e.to_string())?;
            if out == Output::Json {
                let v = serde_json::json!({
                    "placement": placed,
                    "ir": c.ir.as_ref().map(ToString::to_string),
                    "plsql": c.plsql.as_ref().map(|p| p.statements.clone()),
                });
                println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
                return Ok(());
            }
            if let Some(f) = &c.ir {
                println!("{f}");
            }
            println!("{}\n", sprig_logic::placement::explain(&placed));
            if let Some(p) = &c.plsql {
                println!("{};", p.statements.join(";\n\n"));
            }
            Ok(())
        }
        Some(("deploy", m)) => {
            let (path, text) = read(m)?;
            let name = m.get_one::<String>("name").cloned().unwrap_or_else(|| {
                std::path::Path::new(&path)
                    .file_stem()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
            });
            let db = m.get_one::<String>("db").cloned().unwrap_or_default();
            let tier = m
                .get_one::<String>("tier")
                .cloned()
                .unwrap_or_else(|| "auto".into());
            let security = if m.get_flag("definer") {
                "DEFINER"
            } else {
                "INVOKER"
            };
            let body = text.replace('\\', "\\\\").replace('\'', "''");
            let sql = format!(
                "CREATE OR REPLACE PROCEDURE `{}`.`{}` LANGUAGE kirun SQL SECURITY {security} \
                 WITH (SPRIG_TIER = '{tier}') AS '{body}'",
                db.replace('`', "``"),
                name.replace('`', "``")
            );
            let url = m.get_one::<String>("gateway").cloned().unwrap_or_default();
            let mut c = conn(&url).await?;
            c.query_drop(&sql)
                .await
                .map_err(|e| Failed(e.to_string()))?;
            let notes: Vec<(String, u32, String)> = c
                .query("SHOW WARNINGS")
                .await
                .map_err(|e| Failed(e.to_string()))?;
            for (_, _, msg) in notes {
                println!("{msg}");
            }
            let _ = c.disconnect().await;
            Ok(())
        }
        Some(("rollback", m)) => {
            let name = m.get_one::<String>("name").cloned().unwrap_or_default();
            let db = m.get_one::<String>("db").cloned().unwrap_or_default();
            let kind = if m.get_flag("procedure") {
                "PROCEDURE"
            } else {
                "FUNCTION"
            };
            let to = m
                .get_one::<u64>("to-version")
                .map(|v| format!(" TO VERSION {v}"))
                .unwrap_or_default();
            let sql = format!(
                "ALTER {kind} `{}`.`{}` ROLLBACK{to}",
                db.replace('`', "``"),
                name.replace('`', "``")
            );
            let url = m.get_one::<String>("gateway").cloned().unwrap_or_default();
            let mut c = conn(&url).await?;
            c.query_drop(&sql)
                .await
                .map_err(|e| Failed(e.to_string()))?;
            let notes: Vec<(String, u32, String)> = c
                .query("SHOW WARNINGS")
                .await
                .map_err(|e| Failed(e.to_string()))?;
            for (_, _, msg) in notes {
                println!("{msg}");
            }
            let _ = c.disconnect().await;
            Ok(())
        }
        Some(("list", m)) => {
            let rows: Vec<Value> = api.get(&format!("{}/modules", base(m))).await?;
            if out == Output::Json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&rows).unwrap_or_default()
                );
            } else {
                println!(
                    "{:<8} {:<6} {:<24} {:<10} {:<10} {:<8} ACTIVE",
                    "MODULE", "DB", "NAME", "KIND", "RUNTIME", "TIER"
                );
                for r in rows {
                    println!(
                        "{:<8} {:<6} {:<24} {:<10} {:<10} {:<8} {}",
                        r["module_id"],
                        r["db_id"],
                        st(&r, "name"),
                        st(&r, "kind"),
                        st(&r, "runtime"),
                        st(&r, "tier_pref"),
                        opt(&r["active_version"])
                    );
                }
            }
            Ok(())
        }
        Some(("show", m)) => {
            let name = m.get_one::<String>("name").cloned().unwrap_or_default();
            let v: Value = api
                .get(&format!("{}/modules/{}", base(m), escape(&name)))
                .await?;
            if out == Output::Json {
                println!("{}", serde_json::to_string_pretty(&v).unwrap_or_default());
            } else {
                let module = &v["module"];
                println!(
                    "{} {} (module {}, {}, active version {})",
                    st(module, "kind"),
                    st(module, "name"),
                    module["module_id"],
                    st(module, "runtime"),
                    opt(&module["active_version"])
                );
                for v in v["versions"].as_array().into_iter().flatten() {
                    println!(
                        "  v{} {} tier {} ({})",
                        v["version"],
                        st(v, "state"),
                        st(v, "tier"),
                        st(v, "compiler_version")
                    );
                    for r in v["placement"]["reasons"].as_array().into_iter().flatten() {
                        println!("    reason {} at {}: {}", r["code"], r["at"], r["detail"]);
                    }
                }
            }
            Ok(())
        }
        Some(("logs", m)) => logs(api, m, out).await,
        Some(("dead-letters", m)) => dead_letters(api, m, out).await,
        _ => Err("not available in this version".to_owned().into()),
    }
}

/// `/admin/v1/tenants/{tenant}/logic`.
fn base(m: &ArgMatches) -> String {
    let tenant = m.get_one::<String>("tenant").cloned().unwrap_or_default();
    format!("/admin/v1/tenants/{}/logic", escape(&tenant))
}

fn st(v: &Value, k: &str) -> String {
    v[k].as_str().unwrap_or_default().to_owned()
}

fn opt(v: &Value) -> String {
    if v.is_null() {
        "-".to_owned()
    } else {
        v.to_string()
    }
}

async fn logs(api: &AdminClient, m: &ArgMatches, out: Output) -> Result<(), Failed> {
    let limit = m.get_one::<u32>("limit").copied().unwrap_or(50);
    let follow = m.get_flag("follow");
    let module = m
        .get_one::<String>("name")
        .map(|n| format!("&module={}", escape(n)))
        .unwrap_or_default();
    let mut after = 0u64;
    loop {
        let rows: Vec<Value> = api
            .get(&format!(
                "{}/logs?after={after}&limit={limit}{module}",
                base(m)
            ))
            .await?;
        for r in &rows {
            after = after.max(r["log_id"].as_u64().unwrap_or(0));
            if out == Output::Json {
                println!("{}", serde_json::to_string(r).unwrap_or_default());
            } else {
                println!(
                    "{} {:<5} {:<8} module {} {}{}",
                    st(r, "at"),
                    st(r, "level"),
                    st(r, "source"),
                    r["module_id"],
                    r["version"]
                        .as_u64()
                        .map_or_else(String::new, |v| format!("v{v} ")),
                    st(r, "message")
                );
            }
        }
        if !follow {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    Ok(())
}

async fn dead_letters(api: &AdminClient, m: &ArgMatches, out: Output) -> Result<(), Failed> {
    match m.subcommand() {
        Some(("list", m)) => {
            let mut q = format!(
                "limit={}",
                m.get_one::<u32>("limit").copied().unwrap_or(100)
            );
            if let Some(b) = m.get_one::<u64>("binding") {
                use std::fmt::Write as _;
                let _ = write!(q, "&binding={b}");
            }
            if let Some(s) = m.get_one::<String>("state") {
                q.push_str("&state=");
                q.push_str(&escape(s));
            }
            let rows: Vec<Value> = api.get(&format!("{}/dead-letters?{q}", base(m))).await?;
            if out == Output::Json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&rows).unwrap_or_default()
                );
            } else {
                println!(
                    "{:<6} {:<8} {:<4} {:<10} {:<8} {:<20} {:<28} ERROR",
                    "ID", "BINDING", "VER", "STATE", "TRIES", "FAILED", "EVENT"
                );
                for r in rows {
                    println!(
                        "{:<6} {:<8} {:<4} {:<10} {:<8} {:<20} {:<28} {} {}",
                        r["dl_id"],
                        r["binding_id"],
                        r["version"],
                        st(&r, "state"),
                        r["attempts"],
                        st(&r, "last_failed_at"),
                        format!(
                            "{} {}",
                            r["event"]["op"].as_str().unwrap_or(""),
                            st(&r, "event_key")
                        ),
                        r["error_code"],
                        st(&r, "error_message")
                    );
                    println!("       row {}", r["event"]["row"]);
                }
            }
            Ok(())
        }
        Some((verb @ ("replay" | "discard"), m)) => {
            let ids: Vec<u64> = m
                .get_many::<u64>("ids")
                .into_iter()
                .flatten()
                .copied()
                .collect();
            if ids.is_empty() && !m.get_flag("all") {
                return Err("name dead letter ids, or --all".to_owned().into());
            }
            let body = json!({"binding": m.get_one::<u64>("binding"), "ids": ids});
            let v: Value = api
                .post(&format!("{}/dead-letters:{verb}", base(m)), &body)
                .await?;
            if verb == "replay" {
                // After-commit triggers' dead letters, and those of
                // triggers on streams and queues, go back to their
                // jobs, which run them in order with the active version.
                println!(
                    "{} dead letters queued for replay (the trigger's worker runs them in order)",
                    v["replaying"]
                );
                let left = v["skipped"].as_u64().unwrap_or(0);
                if left > 0 {
                    println!(
                        "{left} dead letters of other kinds of triggers stay dead: they can't \
                         be replayed; discard them"
                    );
                }
                return Ok(());
            }
            println!("{} dead letters discarded", v["discarded"]);
            Ok(())
        }
        _ => Err("dead-letters takes list, replay or discard"
            .to_owned()
            .into()),
    }
}
