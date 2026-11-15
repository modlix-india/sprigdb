//! A tenant's token issuers (chapter 14's external tokens):
//!
//! - `sprigctl tenant issuer list <tenant>`: `GET /admin/v1/tenants/{name}/issuers`;
//! - `sprigctl tenant issuer put <tenant> --spec <file|->`: `POST` the same
//!   path with the issuer as JSON (`issuer`, `audiences`, `jwks_url`,
//!   `algorithms`, `protocols`, `jit_users`, `max_lifetime_s`, `mappings`,
//!   ...), creating or replacing it;
//! - `sprigctl tenant issuer disable <tenant> <issuer>`: the stored issuer
//!   put back with `"active": false`;
//! - `sprigctl tenant token-login <tenant> <user> [--host h] [--off]`:
//!   `POST /admin/v1/tenants/{name}/users:setTokenLogin`, the account's
//!   `token` method.

use std::fmt::Write as _;
use std::io::Read as _;

use clap::{Arg, ArgAction, ArgMatches, Command};
use sprig_auth::oidc::IssuerSpec;
use sprig_controller::users::{
    ListIssuersResponse, PutIssuerResponse, TokenLoginRequest, TokenLoginResponse,
};

use crate::client::AdminClient;
use crate::settings::escape;
use crate::{Failed, Output, print};

/// Adds `issuer` and `token-login` to `sprigctl tenant`.
pub(crate) fn add_cmds(g: Command) -> Command {
    let tenant = || Arg::new("name").required(true).help("The tenant");
    g.subcommand(
        Command::new("issuer")
            .about("The tenant's token issuers: OIDC providers whose JWTs log in, and their claim mappings")
            .subcommand_required(true)
            .arg_required_else_help(true)
            .subcommand(Command::new("list").about("List the issuers").arg(tenant()))
            .subcommand(
                Command::new("put")
                    .about("Create or replace an issuer (by its issuer URL) with its mappings")
                    .arg(tenant())
                    .arg(
                        Arg::new("spec")
                            .long("spec")
                            .required(true)
                            .help("The issuer as JSON: a file, or - for standard input"),
                    ),
            )
            .subcommand(
                Command::new("disable")
                    .about("Disable an issuer: its tokens are refused")
                    .arg(tenant())
                    .arg(Arg::new("issuer").required(true).help("The issuer (its iss value)")),
            ),
    )
    .subcommand(
        Command::new("token-login")
            .about("Let an account log in with an issuer's token over MySQL and Redis (its token method)")
            .arg(tenant())
            .arg(Arg::new("user").required(true).help("The user name, without the tenant prefix"))
            .arg(Arg::new("host").long("host").help("The host pattern (default %)"))
            .arg(
                Arg::new("off")
                    .long("off")
                    .action(ArgAction::SetTrue)
                    .help("Remove the token method instead"),
            ),
    )
}

fn render_list(r: &ListIssuersResponse) -> String {
    let mut s = String::new();
    if r.issuers.is_empty() {
        let _ = writeln!(s, "tenant {} has no issuers", r.tenant);
    }
    for i in &r.issuers {
        let _ = writeln!(
            s,
            "{} {} ({}{}) keys: {} audiences: {} protocols: {} mappings: {}",
            i.issuer_id.unwrap_or(0),
            i.issuer,
            if i.active { "active" } else { "disabled" },
            if i.jit_users { ", jit users" } else { "" },
            i.jwks_url.as_deref().unwrap_or("discovery"),
            i.audiences.join(","),
            i.protocols
                .iter()
                .map(|p| p.as_str())
                .collect::<Vec<_>>()
                .join(","),
            i.mappings.len()
        );
    }
    s
}

fn read_spec(path: &str) -> Result<IssuerSpec, Failed> {
    let text = if path == "-" {
        let mut t = String::new();
        std::io::stdin()
            .read_to_string(&mut t)
            .map_err(|e| Failed(format!("standard input: {e}")))?;
        t
    } else {
        std::fs::read_to_string(path).map_err(|e| Failed(format!("{path}: {e}")))?
    };
    serde_json::from_str(&text).map_err(|e| Failed(format!("{path}: {e}")))
}

/// Runs `issuer ...`.
///
/// # Errors
/// The API's; a spec that doesn't read.
pub(crate) async fn run_issuer(
    c: &AdminClient,
    sub: &ArgMatches,
    dry_run: bool,
    out: Output,
) -> Result<(), Failed> {
    let Some((cmd, m)) = sub.subcommand() else {
        return Err(Failed("issuer: list, put or disable".into()));
    };
    let tenant = m.get_one::<String>("name").cloned().unwrap_or_default();
    let path = format!("/admin/v1/tenants/{}/issuers", escape(&tenant));
    let put = |r: &PutIssuerResponse| {
        format!(
            "issuer {} ({}) of tenant {}: {}, {} mappings, auth version {}\n",
            r.issuer_id,
            r.issuer,
            r.tenant,
            if r.active { "active" } else { "disabled" },
            r.mappings,
            r.auth_version
        )
    };
    match cmd {
        "list" => {
            let r: ListIssuersResponse = c.get(&path).await?;
            print(out, &r, render_list);
        }
        "put" => {
            let spec = read_spec(m.get_one::<String>("spec").map_or("-", String::as_str))?;
            if dry_run {
                println!("would put issuer {} in tenant {tenant}", spec.issuer);
                return Ok(());
            }
            let r: PutIssuerResponse = c.post(&path, &spec).await?;
            print(out, &r, put);
        }
        "disable" => {
            let which = m.get_one::<String>("issuer").cloned().unwrap_or_default();
            let list: ListIssuersResponse = c.get(&path).await?;
            let Some(mut spec) = list.issuers.into_iter().find(|i| i.issuer == which) else {
                return Err(Failed(format!("tenant {tenant} has no issuer {which}")));
            };
            spec.active = false;
            if dry_run {
                println!("would disable issuer {which} of tenant {tenant}");
                return Ok(());
            }
            let r: PutIssuerResponse = c.post(&path, &spec).await?;
            print(out, &r, put);
        }
        _ => return Err(Failed(format!("issuer: unknown command {cmd}"))),
    }
    Ok(())
}

/// Runs `token-login`.
///
/// # Errors
/// The API's.
pub(crate) async fn run_token_login(
    c: &AdminClient,
    m: &ArgMatches,
    dry_run: bool,
    out: Output,
) -> Result<(), Failed> {
    let tenant = m.get_one::<String>("name").cloned().unwrap_or_default();
    let req = TokenLoginRequest {
        user: m.get_one::<String>("user").cloned().unwrap_or_default(),
        host: m.get_one::<String>("host").cloned(),
        enabled: !m.get_flag("off"),
    };
    if dry_run {
        println!(
            "would {} token logins for {} in tenant {tenant}",
            if req.enabled { "allow" } else { "stop" },
            req.user
        );
        return Ok(());
    }
    let r: TokenLoginResponse = c
        .post(
            &format!("/admin/v1/tenants/{}/users:setTokenLogin", escape(&tenant)),
            &req,
        )
        .await?;
    print(out, &r, |r| {
        format!(
            "'{}'@'{}' (id {}) in tenant {}: token logins {}\n",
            r.user,
            r.host,
            r.user_id,
            r.tenant,
            if r.enabled { "allowed" } else { "off" }
        )
    });
    Ok(())
}
