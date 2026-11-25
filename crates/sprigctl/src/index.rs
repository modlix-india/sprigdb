//! `sprigctl index`: automatic indexing, over the controller's
//! `GET /admin/v1/tenants/{name}/indexes`.
//!
//! - `list --tenant T [--all]`: automatic indexes (`--all` adds dropped,
//!   rejected and failed ones)
//! - `events --tenant T [--kind K]`: the loop's decisions of the last 30
//!   days: candidates, builds, drops, deferrals, refusals
//! - `drop|pin|unpin|restore <index> --tenant T [--reason R] [--no-wait]`:
//!   an operator's action, over `POST .../indexes/{index}:{action}`. The
//!   node running the tenant's loop applies it within a minute; the command
//!   waits for that (up to two minutes) unless `--no-wait`, then prints the
//!   index. `pin` keeps an index whatever the watch finds (alias `freeze`;
//!   `unpin`, `unfreeze`); `drop` drops it and blocks its definition for
//!   `auto_index.recreate_block_days`; `restore` builds a dropped or failed
//!   one again, pinned.
//!
//! Approving a recommendation (mode `recommend`) is `sprigctl
//! recommendations approve <id>`: the record's `approval` column names it.

use std::time::Duration;

use clap::{Arg, ArgAction, ArgMatches, Command};
use sprig_controller::autoindex_admin::{
    ActionBody, IndexRecord, IndexesResponse, OperatorRequest,
};

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
        .subcommand(action(
            "drop",
            "Drop an automatic index now (its definition is then blocked)",
        ))
        .subcommand(
            action("pin", "Keep an automatic index whatever the watch finds")
                .visible_alias("freeze"),
        )
        .subcommand(
            action("unpin", "Let the watch judge a pinned index again").visible_alias("unfreeze"),
        )
        .subcommand(action(
            "restore",
            "Build a dropped or failed automatic index again, pinned",
        ))
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

fn action(name: &'static str, about: &'static str) -> Command {
    Command::new(name)
        .about(about)
        .arg(
            Arg::new("index")
                .required(true)
                .help("The index: sprig_auto_<hex>, table.name or db.table.name"),
        )
        .arg(tenant())
        .arg(
            Arg::new("reason")
                .long("reason")
                .help("Why (kept in the events)"),
        )
        .arg(
            Arg::new("no-wait")
                .long("no-wait")
                .action(ArgAction::SetTrue)
                .help("Return once the request is recorded"),
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
                    .or_else(|| x.pinned.then(|| "pinned".to_owned()))
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

fn record_text(r: &IndexRecord) -> String {
    let note = r
        .end_reason
        .clone()
        .or_else(|| r.approval.clone())
        .unwrap_or_default();
    table(
        [
            "NAME",
            "TABLE",
            "DEFINITION",
            "STATE",
            "PINNED",
            "READS",
            "NOTE",
        ],
        &[[
            r.name.clone(),
            r.spec.table_key(),
            r.spec.canonical(),
            r.state.name().to_owned(),
            if r.pinned { "yes" } else { "no" }.to_owned(),
            r.reads_total.to_string(),
            note,
        ]],
    )
}

/// `sprigctl index drop|pin|unpin|restore`.
async fn act(c: &AdminClient, cmd: &str, sub: &ArgMatches, out: Output) -> Result<(), Failed> {
    let t = sub.get_one::<String>("tenant").cloned().unwrap_or_default();
    let index = sub.get_one::<String>("index").cloned().unwrap_or_default();
    let body = ActionBody {
        reason: sub.get_one::<String>("reason").cloned().unwrap_or_default(),
        actor: String::new(),
    };
    let req: OperatorRequest = c
        .post(
            &format!(
                "/admin/v1/tenants/{}/indexes/{}:{cmd}",
                escape(&t),
                escape(&index)
            ),
            &body,
        )
        .await?;
    if sub.get_flag("no-wait") {
        print(out, &req, |r| {
            format!(
                "{} {}: requested; the node applies it within a minute\n",
                r.action.name(),
                r.index
            )
        });
        return Ok(());
    }
    // The node applies it at its next minute (a restore also builds).
    let path = format!("/admin/v1/tenants/{}/indexes", escape(&t));
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let r: IndexesResponse = c.get(&path).await?;
        if r.requests.iter().any(|q| q.index == req.index) {
            continue;
        }
        let Some(rec) = r.indexes.into_iter().find(|x| x.name == req.index) else {
            break;
        };
        print(out, &rec, record_text);
        return Ok(());
    }
    Err(Failed(format!(
        "{} {}: still waiting after two minutes; is the tenant's node running? \
         (sprigctl index list --tenant {t} shows it once applied)",
        req.action.name(),
        req.index
    )))
}

/// Runs `sprigctl index <cmd>`.
pub(crate) async fn run(c: &AdminClient, group: &ArgMatches, out: Output) -> Result<(), Failed> {
    let Some((cmd, sub)) = group.subcommand() else {
        return Err(Failed("a subcommand is required".into()));
    };
    if matches!(cmd, "drop" | "pin" | "unpin" | "restore") {
        return act(c, cmd, sub, out).await;
    }
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
