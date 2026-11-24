//! `sprigctl index`: automatic indexing, over the controller's
//! `GET /admin/v1/tenants/{name}/indexes`.
//!
//! - `list --tenant T [--all]`: automatic indexes (`--all` adds dropped,
//!   rejected and failed ones)
//! - `events --tenant T [--kind K]`: the loop's decisions of the last 30
//!   days: candidates, builds, drops, deferrals, refusals
//!
//! Approving a recommendation (mode `recommend`) is `sprigctl
//! recommendations approve <id>`: the record's `approval` column names it.

use clap::{Arg, ArgAction, ArgMatches, Command};
use sprig_controller::autoindex_admin::IndexesResponse;

use crate::client::AdminClient;
use crate::render::table;
use crate::settings::escape;
use crate::{Failed, Output, print};

fn tenant() -> Arg {
    Arg::new("tenant")
        .long("tenant")
        .env("SPRIGCTL_TENANT")
        .required(true)
        .help("The tenant")
}

/// `sprigctl index`.
pub(crate) fn group() -> Command {
    Command::new("index")
        .about("Automatic indexes and the decisions behind them")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("list")
                .about("Automatic indexes (--all adds ended ones)")
                .arg(tenant())
                .arg(
                    Arg::new("all")
                        .long("all")
                        .action(ArgAction::SetTrue)
                        .help("Dropped, rejected and failed too"),
                ),
        )
        .subcommand(
            Command::new("events")
                .about("Candidates, builds, drops and deferrals of the last 30 days")
                .arg(tenant())
                .arg(
                    Arg::new("kind")
                        .long("kind")
                        .help("Only this kind (candidate, built, dropped, deferred, ...)"),
                ),
        )
}

fn ms(us: u64) -> String {
    sprig_approvals::events::rfc3339(us.try_into().unwrap_or_default())
}

fn list_text(r: &IndexesResponse, all: bool) -> String {
    let rows: Vec<[String; 7]> = r
        .indexes
        .iter()
        .filter(|x| all || !x.state.terminal())
        .map(|x| {
            [
                x.name.clone(),
                x.spec.table_key(),
                x.spec.canonical(),
                x.state.name().to_owned(),
                format!("{:.0} -> {:.0}", x.est_before, x.est_after),
                x.reads_total.to_string(),
                x.end_reason
                    .clone()
                    .or_else(|| x.approval.clone())
                    .unwrap_or_default(),
            ]
        })
        .collect();
    table(
        [
            "NAME",
            "TABLE",
            "DEFINITION",
            "STATE",
            "ROWS EXAMINED",
            "READS",
            "NOTE",
        ],
        &rows,
    )
}

fn events_text(r: &IndexesResponse, kind: Option<&str>) -> String {
    let rows: Vec<[String; 5]> = r
        .events
        .iter()
        .filter(|e| kind.is_none_or(|k| e.kind == k))
        .map(|e| {
            [
                ms(e.at_us),
                e.kind.clone(),
                e.table.clone(),
                e.def.clone(),
                e.reason.clone(),
            ]
        })
        .collect();
    table(["AT", "KIND", "TABLE", "DEFINITION", "WHY"], &rows)
}

/// Runs `sprigctl index <cmd>`.
pub(crate) async fn run(c: &AdminClient, group: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((cmd, sub)) = group.subcommand() else {
        return Err(Failed("a subcommand is required".into()));
    };
    let t = sub.get_one::<String>("tenant").cloned().unwrap_or_default();
    let r: IndexesResponse = c
        .get(&format!("/admin/v1/tenants/{}/indexes", escape(&t)))
        .await?;
    match cmd {
        "list" => {
            let all = sub.get_flag("all");
            print(out, &r, |r| list_text(r, all));
        }
        "events" => {
            let kind = sub.get_one::<String>("kind").cloned();
            print(out, &r, |r| events_text(r, kind.as_deref()));
        }
        other => return Err(Failed(format!("sprigctl index {other}: unknown"))),
    }
    Ok(())
}
