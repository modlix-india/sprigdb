//! The DBA assistant's skills on the gateway's side, run as the caller
//! like every tool:
//!
//! - **`sensitive_columns`**: reads `information_schema` (columns, CHECK
//!   clauses, row-level security) and hands it to
//!   [`sprig_ai::tools::sensitive`]. With `ai.share_data` on, it also reads
//!   up to 200 values of each text column in a read-only transaction and
//!   keeps only pattern counts; the values never leave this function.
//! - **Policies tried before they exist** (`test_policy` with `policy`,
//!   and the check of `propose_policy`): the `CREATE POLICY` is parsed and
//!   its expressions validated as `CREATE POLICY` would; its `USING` is
//!   bound to the test user's values (account, id, tenant, attributes,
//!   simulated claims) as the rewrite binds it, and run as that user with
//!   the tester (`POST /v1/policies:testAs`): how many rows the user sees
//!   now and how many of them the new policy admits. Counts only. Nothing
//!   is created.
//! - **Logic compiled before it is proposed** (`propose_logic`): the
//!   `Logic.Compile` dry run; a statement that doesn't compile goes back
//!   to the model.
//! - **Test rows** (`generate_rows`): the table's shape, its parent keys
//!   and the largest values of its unique integers are read here, and
//!   [`sprig_ai::tools::testdata`] generates the rows. The rows ride on the
//!   proposal's `generated` part, for the user only; the model gets the
//!   summary.

#![allow(
    clippy::similar_names,
    clippy::many_single_char_names,
    clippy::cast_possible_truncation
)] // short names for rows and columns; counts clamped before casts

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};
use sprig_ai::tools::sensitive::{self, ColumnInfo, TableSecurity};
use sprig_ai::tools::testdata::{self, GenTable};
use sprig_ai::tools::{ProposalCheck, ToolBody, ToolOutput};
use sprig_auth::authz::AuthzState;
use sprig_auth::{LoginSource, TenantAuth};
use sprig_common::{Result, SprigCode};
use sprig_sql::lex::LexOptions;
use sprig_sql::rls::expr::{self, LiteralBinder};
use sprig_sql::rls::stmt::{
    Commands, PolicyGrantee, PolicyMode, PolicyStmt, SecuritySwitch, TableName,
};

use super::{
    Caps, GatewayHost, clip, ident, objects, quote_literal, reply_json, result_set, s, source,
    tool_error,
};

/// The qualifier a tried policy's columns get.
const PROBE: &str = "`ai_probe`";

/// A JSON value as a SQL literal, exactly as the gateway's rewrite binds
/// session values (text, as MariaDB's policy views read them), so a trial
/// gives the verdict the database will enforce.
fn literal(v: &Value) -> String {
    expr::json_literal(v, false)
}

/// A JSON object given as text (`attributes`, `claims`), if any.
fn object_text(input: &Value, k: &str) -> Option<Map<String, Value>> {
    match serde_json::from_str::<Value>(s(input, k)) {
        Ok(Value::Object(m)) => Some(m),
        _ => None,
    }
}

/// `bob`, `bob@%` or `'bob'@'%'` → (`bob`, `%` when given).
fn user_ref(u: &str) -> (String, Option<String>) {
    let u = u.trim();
    let unq = |x: &str| x.trim().trim_matches(['\'', '`', '"']).to_owned();
    match u.rsplit_once('@') {
        Some((n, h)) => (unq(n), Some(unq(h))),
        None => (unq(u), None),
    }
}

/// One `CREATE POLICY`, parsed.
struct NewPolicy {
    name: String,
    table: TableName,
    restrictive: bool,
    commands: Commands,
    to: Vec<PolicyGrantee>,
    using: Option<String>,
    check: Option<String>,
}

impl NewPolicy {
    fn db<'a>(&'a self, current: &'a str) -> &'a str {
        self.table.db.as_deref().unwrap_or(current)
    }

    fn commands_text(&self) -> String {
        let names = [
            (Commands::SELECT.0, "SELECT"),
            (Commands::INSERT.0, "INSERT"),
            (Commands::UPDATE.0, "UPDATE"),
            (Commands::DELETE.0, "DELETE"),
        ];
        if self.commands.0 & Commands::ALL.0 == Commands::ALL.0 {
            return "ALL".to_owned();
        }
        names
            .iter()
            .filter(|(b, _)| self.commands.0 & b != 0)
            .map(|(_, n)| *n)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Parses and validates statements of `propose_policy` (or the one
/// `CREATE POLICY` of `test_policy`): the policies created, and the tables
/// whose row-level security is switched on.
fn parse_policies(sql: &str) -> Result<(Vec<NewPolicy>, Vec<TableName>)> {
    let lex = LexOptions::mysql84();
    let mut policies = Vec::new();
    let mut enables = Vec::new();
    let stmts = super::super::super::catalog::split(sql);
    if stmts.is_empty() {
        return Err(tool_error("there is no statement"));
    }
    for (i, st) in stmts.iter().enumerate() {
        let n = i + 1;
        let parsed = sprig_sql::rls::stmt::parse(st.text.as_bytes(), &lex).ok_or_else(|| {
            tool_error(format!(
                "statement {n} ({}) isn't a row-level security statement; propose it with propose_sql",
                clip(&st.text, 60)
            ))
        })?;
        let stmt = parsed.map_err(|e| tool_error(format!("statement {n}: {}", e.message)))?;
        match stmt {
            PolicyStmt::Create {
                name,
                table,
                mode,
                commands,
                to,
                using,
                check,
                ..
            } => {
                for e in [using.as_deref(), check.as_deref()].into_iter().flatten() {
                    expr::validate(e, &lex).map_err(|err| {
                        tool_error(format!("statement {n}, policy {name}: {}", err.message))
                    })?;
                }
                policies.push(NewPolicy {
                    name,
                    table,
                    restrictive: mode == PolicyMode::Restrictive,
                    commands,
                    to,
                    using,
                    check,
                });
            }
            PolicyStmt::Security {
                table,
                switch: SecuritySwitch::Enable,
            } => enables.push(table),
            _ => {}
        }
    }
    Ok((policies, enables))
}

/// What trying one policy as one user found.
#[derive(Debug, Default)]
struct Trial {
    user: String,
    applies: Option<bool>,
    visible_now: Option<u64>,
    admitted: Option<u64>,
    sql_rows: Option<u64>,
    reads: Vec<(&'static str, String, bool)>,
    warnings: Vec<String>,
    error: Option<String>,
}

impl Trial {
    fn json(&self) -> Value {
        let mut v = json!({"user": self.user});
        if let Some(e) = &self.error {
            v["not_tried"] = json!(e);
            return v;
        }
        v["policy_applies_to_user"] = json!(self.applies);
        v["rows_user_sees_now"] = json!(self.visible_now);
        v["rows_new_policy_admits"] = json!(self.admitted);
        if let Some(n) = self.sql_rows {
            v["rows_of_your_select"] = json!(n);
        }
        if !self.reads.is_empty() {
            v["reads"] = Value::Array(
                self.reads
                    .iter()
                    .map(|(k, key, set)| json!({"kind": k, "key": key, "set": set}))
                    .collect(),
            );
        }
        if !self.warnings.is_empty() {
            v["warnings"] = json!(self.warnings);
        }
        v
    }
}

/// The test user's session values, from the tenant catalog.
struct UserValues {
    name: String,
    host: String,
    user_id: u64,
    tenant: u64,
    attrs: Option<Map<String, Value>>,
    roles: Vec<sprig_common::ids::RoleId>,
}

/// The count in a tester reply's first cell.
fn count_of(v: &Value) -> Option<u64> {
    let c = &v["rows"][0][0];
    c.as_u64()
        .or_else(|| c.as_str().and_then(|x| x.parse().ok()))
}

fn days_today() -> i64 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    i64::try_from(secs / 86_400).unwrap_or(0)
}

/// A new seed for each generation.
fn seed() -> u64 {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    #[allow(clippy::cast_possible_truncation)] // the low bits vary most
    let low = n as u64;
    low ^ u64::from(std::process::id()).rotate_left(32)
}

impl GatewayHost {
    fn caps(&self, db: &str, max_rows: u64) -> Caps {
        Caps {
            max_rows,
            max_bytes: 0,
            timeout: Some(self.timeout(db)),
        }
    }

    async fn info_rows(&self, sql: String) -> Result<Vec<Map<String, Value>>> {
        Ok(objects(&self.sql("-", sql).await?))
    }

    /// Row-level security per table of `db`: enabled, and how many
    /// policies; empty when the view can't be read.
    async fn security_of(&self, db: &str) -> BTreeMap<String, TableSecurity> {
        let mut out: BTreeMap<String, TableSecurity> = BTreeMap::new();
        let Ok(rows) = self
            .info_rows(format!(
                "SELECT TABLE_NAME AS t, POLICY_NAME AS p, RLS_ENABLED AS e \
                 FROM information_schema.SPRIG_ROW_POLICIES WHERE TABLE_SCHEMA = {}",
                quote_literal(db)
            ))
            .await
        else {
            return out;
        };
        for r in rows {
            let t = r.get("t").and_then(Value::as_str).unwrap_or_default();
            let e = out.entry(t.to_owned()).or_default();
            e.rls_enabled |= r.get("e").and_then(Value::as_str) == Some("YES");
            if r.get("p").is_some_and(|p| !p.is_null()) {
                e.policies += 1;
            }
        }
        out
    }

    /// `sensitive_columns`.
    #[allow(clippy::too_many_lines)] // columns, checks, samples, then the report
    pub(super) async fn sensitive_columns(&self, input: &Value) -> Result<ToolOutput> {
        let (db, table) = (s(input, "db"), s(input, "table"));
        let only = if table.is_empty() {
            String::new()
        } else {
            format!(" AND TABLE_NAME = {}", quote_literal(table))
        };
        let rows = self
            .info_rows(format!(
                "SELECT TABLE_NAME AS t, COLUMN_NAME AS c, DATA_TYPE AS dt, COLUMN_TYPE AS ct, \
                 COLUMN_COMMENT AS cm FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA = {}{only} ORDER BY TABLE_NAME, ORDINAL_POSITION LIMIT {}",
                quote_literal(db),
                sensitive::MAX_COLUMNS
            ))
            .await?;
        if rows.is_empty() {
            return Err(tool_error(if table.is_empty() {
                format!("there is no database {db} with tables the user can see")
            } else {
                format!("there is no table {db}.{table} that the user can see")
            }));
        }
        let text = |r: &Map<String, Value>, k: &str| {
            r.get(k)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        // CHECK clauses, by table (best effort: a view that can't be read
        // leaves them out).
        let mut checks: BTreeMap<String, Vec<String>> = BTreeMap::new();
        if let Ok(cs) = self
            .info_rows(format!(
                "SELECT TABLE_NAME AS t, CHECK_CLAUSE AS c FROM information_schema.CHECK_CONSTRAINTS \
                 WHERE CONSTRAINT_SCHEMA = {}",
                quote_literal(db)
            ))
            .await
        {
            for r in cs {
                checks.entry(text(&r, "t")).or_default().push(text(&r, "c"));
            }
        }
        let columns: Vec<ColumnInfo> = rows
            .iter()
            .map(|r| {
                let t = text(r, "t");
                let c = text(r, "c");
                let lc = c.to_lowercase();
                ColumnInfo {
                    checks: checks
                        .get(&t)
                        .into_iter()
                        .flatten()
                        .filter(|k| k.to_lowercase().contains(&lc))
                        .cloned()
                        .collect(),
                    table: t,
                    column: c,
                    data_type: text(r, "dt").to_ascii_lowercase(),
                    column_type: text(r, "ct").to_ascii_lowercase(),
                    comment: text(r, "cm"),
                }
            })
            .collect();
        // Values, only with sharing: pattern counts per column.
        let mut samples: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
        let mut sampled = None;
        if self.share_data {
            let mut by_table: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
            for c in columns.iter().filter(|c| sensitive::sample_worthy(c)) {
                let e = by_table.entry(c.table.as_str()).or_default();
                if e.len() < sensitive::MAX_SAMPLED_COLUMNS {
                    e.push(c.column.as_str());
                }
            }
            let mut n = 0;
            for (t, cs) in by_table.into_iter().take(sensitive::MAX_SAMPLED_TABLES) {
                let list = cs.iter().map(|c| ident(c)).collect::<Vec<_>>().join(", ");
                let sql = format!(
                    "SELECT {list} FROM {}.{} LIMIT {}",
                    ident(db),
                    ident(t),
                    sensitive::SAMPLE_VALUES
                );
                let caps = self.caps(db, sensitive::SAMPLE_VALUES as u64);
                let Ok(r) = self.read_only(db, sql, caps).await else {
                    continue;
                };
                let rs = result_set(&r);
                for (i, c) in cs.iter().enumerate() {
                    let vals: Vec<String> = rs
                        .rows
                        .iter()
                        .filter_map(|row| row.get(i))
                        .filter_map(|v| match v {
                            Value::String(x) => Some(x.chars().take(512).collect()),
                            Value::Number(x) => Some(x.to_string()),
                            _ => None,
                        })
                        .collect();
                    samples.insert(((*t).to_owned(), (*c).to_owned()), vals);
                    n += 1;
                }
            }
            sampled = Some(n);
        }
        let findings: Vec<sensitive::Finding> = columns
            .iter()
            .filter_map(|c| {
                let f = sensitive::by_schema(c);
                let m = samples
                    .get(&(c.table.clone(), c.column.clone()))
                    .and_then(|v| sensitive::by_values(v, f.as_ref().map(|f| f.category)));
                sensitive::with_values(c, f, m)
            })
            .collect();
        // The values are dropped here; only counts went into findings.
        drop(samples);
        let security = self.security_of(db).await;
        let reference = if table.is_empty() {
            db.to_owned()
        } else {
            format!("{db}.{table}")
        };
        Ok(ToolOutput {
            body: ToolBody::Json(sensitive::report(
                db,
                columns.len(),
                &findings,
                &security,
                sampled,
            )),
            sources: vec![source("database", reference.clone(), reference)],
        })
    }

    /// The test user's values, from the tenant catalog.
    async fn user_values(&self, user: &str) -> Result<UserValues> {
        let tid = self
            .caller
            .principal
            .tenant
            .ok_or_else(|| tool_error("trying a policy needs a tenant login"))?;
        let login = self
            .core
            .studio
            .login
            .as_ref()
            .ok_or_else(|| tool_error("trying a policy needs catalog logins"))?;
        let LoginSource::Catalog(cl) = login.source() else {
            return Err(tool_error("trying a policy needs catalog logins"));
        };
        let cat = cl.catalogs().catalog(tid).await?;
        let auth = TenantAuth::of(&cat)
            .ok_or_else(|| tool_error("the tenant's accounts aren't loaded"))?;
        let (name, host) = user_ref(user);
        let accounts = auth.accounts(&name);
        let u = accounts
            .iter()
            .find(|a| host.as_deref().is_none_or(|h| a.row.host == h))
            .or_else(|| accounts.iter().find(|a| a.row.host == "%"))
            .or_else(|| accounts.first())
            .ok_or_else(|| tool_error(format!("this tenant has no user {user}")))?;
        let roles = AuthzState::of(&cat)
            .map(|st| st.granted_roles(u.row.user_id))
            .unwrap_or_default();
        Ok(UserValues {
            name: u.row.name.clone(),
            host: u.row.host.clone(),
            user_id: u64::from(u.row.user_id.0),
            tenant: tid.0,
            attrs: match &u.row.attributes {
                Some(Value::Object(m)) => Some(m.clone()),
                _ => None,
            },
            roles,
        })
    }

    /// Whether `p`'s `TO` list takes in `u`.
    async fn applies(&self, p: &NewPolicy, u: &UserValues) -> (bool, Option<String>) {
        if p.to.is_empty() || p.to.iter().any(|g| matches!(g, PolicyGrantee::Public)) {
            return (true, None);
        }
        let Some(tid) = self.caller.principal.tenant else {
            return (false, None);
        };
        let cat = match &self.core.studio.login.as_ref().map(|l| l.source()) {
            Some(LoginSource::Catalog(cl)) => cl.catalogs().catalog(tid).await.ok(),
            _ => None,
        };
        for g in &p.to {
            match g {
                PolicyGrantee::Account(a) => {
                    if a.name == u.name && (a.host == u.host || a.host == "%") {
                        return (true, None);
                    }
                    let role = cat
                        .as_ref()
                        .and_then(|c| AuthzState::of(c))
                        .and_then(|st| st.role_named(&a.name, &a.host));
                    if role.is_some_and(|r| u.roles.contains(&r)) {
                        return (true, None);
                    }
                }
                PolicyGrantee::CurrentUser | PolicyGrantee::Public => {}
            }
        }
        let why = if p.to.iter().any(|g| matches!(g, PolicyGrantee::CurrentUser)) {
            "TO CURRENT_USER means the account that creates the policy, not each user; use TO PUBLIC or a role with CURRENT_USER() in USING".to_owned()
        } else {
            format!("{} isn't named in TO and holds none of its roles", u.name)
        };
        (false, Some(why))
    }

    /// Tries `p` as `user`: counts only.
    async fn try_policy_as(
        &self,
        db: &str,
        p: &NewPolicy,
        user: &str,
        attrs_over: Option<&Map<String, Value>>,
        claims: Option<&Map<String, Value>>,
        sql: Option<&str>,
    ) -> Trial {
        let mut t = Trial {
            user: user.to_owned(),
            ..Trial::default()
        };
        let u = match self.user_values(user).await {
            Ok(u) => u,
            Err(e) => {
                t.error = Some(e.message);
                return t;
            }
        };
        let pdb = p.db(db).to_owned();
        let (applies, why) = self.applies(p, &u).await;
        t.applies = Some(applies);
        if let Some(w) = why {
            t.warnings.push(format!(
                "the policy doesn't apply to {user}: {w}; once row-level security is on, it admits no rows for {user}"
            ));
        }
        // Bind the USING to the user's values, as the rewrite would.
        let rendered = match &p.using {
            None => {
                t.warnings.push(
                    "the policy has no USING: it limits writes only, and WITH CHECK isn't tried here"
                        .to_owned(),
                );
                None
            }
            Some(using) => {
                let mut attrs = u.attrs.clone().unwrap_or_default();
                if let Some(o) = attrs_over {
                    attrs.extend(o.clone());
                }
                match bind(using, &pdb, &p.table.name, &u, &attrs, claims) {
                    Ok((r, reads)) => {
                        t.reads = reads;
                        Some(r)
                    }
                    Err(e) => {
                        t.error = Some(format!("the policy can't be tried here: {e}"));
                        return t;
                    }
                }
            }
        };
        for (kind, key, set) in &t.reads {
            if *set {
                continue;
            }
            t.warnings.push(if *kind == "attribute" {
                format!(
                    "{user} has no attribute {key}: SPRIG_USER_ATTR('{key}') is NULL for {user}, so the policy admits none of their rows. Set it with ALTER USER '{}'@'{}' ATTRIBUTE '{{\"{key}\": \"<value>\"}}', or pass attributes to test_policy to try a value",
                    u.name, u.host
                )
            } else {
                format!(
                    "SPRIG_CLAIM('{key}') is NULL unless the session logged in with an identity provider token that carries {key}; password, Basic and API-key sessions have no claims. Pass claims to test_policy to try a value"
                )
            });
        }
        let q = format!("{}.{}", ident(&pdb), ident(&p.table.name));
        let run = |sql: String| {
            let b = json!({"user": user, "database": pdb, "sql": sql});
            let core = std::sync::Arc::clone(&self.core);
            let caller = self.caller.clone();
            async move { super::super::super::policies::run_as(&core, &caller, &b).await }
        };
        match run(format!("SELECT COUNT(*) FROM {q}")).await {
            Ok(v) => t.visible_now = count_of(&v),
            Err(e) => {
                t.error = Some(e.message);
                return t;
            }
        }
        if let Some(r) = rendered {
            match run(format!("SELECT COUNT(*) FROM {q} AS {PROBE} WHERE ({r})")).await {
                Ok(v) => t.admitted = count_of(&v),
                Err(e) => t
                    .warnings
                    .push(format!("the new policy's USING failed: {}", e.message)),
            }
        }
        if let Some(sql) = sql {
            match run(sql.to_owned()).await {
                Ok(v) => t.sql_rows = v["rows"].as_array().map(|a| a.len() as u64),
                Err(e) => t
                    .warnings
                    .push(format!("your SELECT failed: {}", e.message)),
            }
        }
        t
    }

    /// `test_policy` with `policy`: the tester's run of `sql`, and the
    /// policy tried.
    pub(super) async fn try_policy(&self, input: &Value) -> Result<ToolOutput> {
        let db = s(input, "db");
        let (mut ps, _) = parse_policies(s(input, "policy"))?;
        if ps.len() != 1 {
            return Err(tool_error("policy is one CREATE POLICY statement"));
        }
        let p = ps.remove(0);
        let attrs = object_text(input, "attributes");
        let claims = object_text(input, "claims");
        let t = self
            .try_policy_as(
                db,
                &p,
                s(input, "as_user"),
                attrs.as_ref(),
                claims.as_ref(),
                Some(s(input, "sql")),
            )
            .await;
        let pdb = p.db(db).to_owned();
        let on = self
            .security_of(&pdb)
            .await
            .get(&p.table.name)
            .is_some_and(|x| x.rls_enabled);
        Ok(ToolOutput {
            body: ToolBody::Json(json!({
                "policy": policy_json(&p, &pdb),
                "row_level_security": if on { "on" } else { "off" },
                "trial": t.json(),
                "note": "Counts only. The policy wasn't created: rows_new_policy_admits counts the rows the user sees now that its USING lets through, with the user's own values bound.",
            })),
            sources: vec![source(
                "policy",
                "row-level security tester",
                format!("{pdb}.{}", p.table.name),
            )],
        })
    }

    /// `propose_policy`'s check: every statement parses and validates;
    /// every new policy is tried as every test user.
    async fn check_policies(&self, input: &Value) -> Result<ProposalCheck> {
        let db = s(input, "db");
        let (ps, enables) = parse_policies(s(input, "sql"))?;
        if ps.is_empty() {
            return Err(tool_error(
                "propose_policy needs at least one CREATE POLICY",
            ));
        }
        let users: Vec<&str> = input["test_as"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        let mut out = Vec::new();
        let mut warnings = Vec::new();
        for p in &ps {
            let pdb = p.db(db).to_owned();
            let mut trials = Vec::new();
            for u in &users {
                trials.push(self.try_policy_as(db, p, u, None, None, None).await.json());
            }
            let on = self
                .security_of(&pdb)
                .await
                .get(&p.table.name)
                .is_some_and(|x| x.rls_enabled);
            let enabled_here = enables
                .iter()
                .any(|t| t.name.eq_ignore_ascii_case(&p.table.name));
            if !on && !enabled_here {
                warnings.push(format!(
                    "row-level security is off on {}: the policy does nothing until ALTER TABLE {} ENABLE ROW LEVEL SECURITY",
                    p.table.name, p.table.name
                ));
            }
            if !on && enabled_here {
                warnings.push(format!(
                    "enabling row-level security on {} hides every row from users no policy admits (users with ALTER on it still see all rows unless it is FORCEd)",
                    p.table.name
                ));
            }
            let mut v = policy_json(p, &pdb);
            v["tried_as"] = Value::Array(trials);
            out.push(v);
        }
        Ok(ProposalCheck {
            checks: Some(json!({
                "policies": out,
                "warnings": warnings,
                "note": "Each policy parsed and its expressions passed CREATE POLICY's rules. Counts are per test user, with the policy's USING bound to that user's values; nothing was created.",
            })),
            generated: None,
        })
    }

    /// `propose_logic`'s check: the dry run compiles it.
    async fn check_logic(&self, input: &Value) -> Result<ProposalCheck> {
        let db = s(input, "db");
        let r = super::super::super::logic::compile(
            &self.core,
            &self.caller,
            db,
            &json!({"sql": s(input, "sql")}),
        )
        .await;
        let v = match r {
            Ok(r) => reply_json(r).await?,
            Err(e) if e.code() == Some(SprigCode::LogicUnsupported) => {
                return Ok(ProposalCheck {
                    checks: Some(json!({"compiled": false, "why": e.message})),
                    generated: None,
                });
            }
            Err(e) => return Err(e),
        };
        if v["ok"] != Value::Bool(true) {
            let msgs: Vec<String> = v["diagnostics"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|d| d["message"].as_str().map(str::to_owned))
                .collect();
            return Err(tool_error(format!(
                "it doesn't compile: {}. Fix it (check with compile_logic) and propose it again",
                if msgs.is_empty() {
                    "no diagnostics".to_owned()
                } else {
                    msgs.join("; ")
                }
            )));
        }
        let mut c = json!({"compiled": true});
        for k in [
            "kind",
            "name",
            "runtime",
            "tier",
            "table",
            "notes",
            "signature",
        ] {
            if let Some(x) = v.get(k).filter(|x| !x.is_null()) {
                c[k] = x.clone();
            }
        }
        Ok(ProposalCheck {
            checks: Some(c),
            generated: None,
        })
    }

    /// `generate_rows`'s check: the rows, generated here.
    async fn generate(&self, input: &Value) -> Result<ProposalCheck> {
        let (db, table) = (s(input, "db"), s(input, "table"));
        let n = usize::try_from(input["n"].as_u64().unwrap_or(0)).unwrap_or(0);
        let hints = testdata::hints_of(input.get("columns"));
        let [cq, iq, fq, kq] = testdata::shape_queries(db, table);
        let columns = self.info_rows(cq).await?;
        let indexes = self.info_rows(iq).await.unwrap_or_default();
        let fks = self.info_rows(fq).await.unwrap_or_default();
        let checks = self.info_rows(kq).await.unwrap_or_default();
        let (mut t, shapes): (GenTable, _) =
            GenTable::from_parts(db, table, &columns, &indexes, &fks, &checks)
                .map_err(tool_error)?;
        for (i, f) in shapes.iter().enumerate() {
            let caps = self.caps(db, testdata::MAX_PARENT_KEYS as u64);
            let r = self.read_only(db, f.keys_query(), caps).await?;
            t.foreign_keys[i].keys = result_set(&r).rows;
        }
        if let Some(q) = t.max_query() {
            let r = self.read_only(db, q, self.caps(db, 1)).await?;
            let row = result_set(&r).rows.into_iter().next().unwrap_or_default();
            for (i, c) in t.unique_ints().iter().enumerate() {
                let v = row.get(i).and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_f64().map(|f| f.round().clamp(-9.0e18, 9.0e18) as i64))
                        .or_else(|| v.as_str().and_then(|x| x.parse().ok()))
                });
                if let Some(col) = t.columns.iter_mut().find(|x| &x.name == c) {
                    col.max_existing = v;
                }
            }
        }
        let g = testdata::generate(&t, n, &hints, seed(), days_today()).map_err(tool_error)?;
        let stmts = g.statements(db, table);
        let sql = format!("{};", stmts.join(";\n"));
        Ok(ProposalCheck {
            checks: Some(g.summary(&t, stmts.len(), sql.len())),
            generated: Some(json!({
                "db": db, "table": table, "rows": g.rows.len(),
                "statements": stmts.len(), "sql": sql,
            })),
        })
    }

    /// The checks of the DBA skills' proposals.
    pub(super) async fn check_skill(&self, name: &str, input: &Value) -> Result<ProposalCheck> {
        match name {
            "propose_policy" => self.check_policies(input).await,
            "propose_logic" => self.check_logic(input).await,
            "generate_rows" => tokio::time::timeout(Duration::from_secs(50), self.generate(input))
                .await
                .map_err(|_| tool_error("generating the rows took too long; ask for fewer"))?,
            _ => Ok(ProposalCheck::default()),
        }
    }
}

/// A policy as the checks report it.
fn policy_json(p: &NewPolicy, db: &str) -> Value {
    json!({
        "policy": p.name, "table": format!("{db}.{}", p.table.name),
        "mode": if p.restrictive { "restrictive" } else { "permissive" },
        "commands": p.commands_text(),
        "using": p.using, "with_check": p.check,
    })
}

/// `using` with its session functions bound to `u`'s values, qualified
/// with [`PROBE`]; and the attributes and claims it read, with whether
/// each was set.
#[allow(clippy::type_complexity)]
fn bind(
    using: &str,
    db: &str,
    table: &str,
    u: &UserValues,
    attrs: &Map<String, Value>,
    claims: Option<&Map<String, Value>>,
) -> std::result::Result<(String, Vec<(&'static str, String, bool)>), String> {
    let lex = LexOptions::mysql84();
    let reads: RefCell<Vec<(&'static str, String, bool)>> = RefCell::new(Vec::new());
    let attr = |k: &str| {
        let v = attrs.get(k).filter(|v| !v.is_null()).map(literal);
        reads
            .borrow_mut()
            .push(("attribute", k.to_owned(), v.is_some()));
        v
    };
    let claim = |k: &str| {
        let v = claims
            .and_then(|c| c.get(k))
            .filter(|v| !v.is_null())
            .map(literal);
        reads
            .borrow_mut()
            .push(("claim", k.to_owned(), v.is_some()));
        v
    };
    let current = format!("{}@{}", u.name, u.host);
    let b = LiteralBinder {
        current_user: &current,
        user_id: u.user_id,
        tenant_id: u.tenant,
        home_tenant_id: u.tenant,
        space: None,
        claim: &claim,
        attr: &attr,
        subtree: None,
        managed: None,
        subtree_table: false,
        managed_table: false,
        no_backslash_escapes: false,
    };
    let q = expr::qualify_tables(using, &lex, db).map_err(|e| e.message)?;
    let r = expr::render(&q, &lex, table, PROBE, &b).map_err(|e| e.message)?;
    let mut out = reads.into_inner();
    out.sort();
    out.dedup();
    Ok((r, out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policies_parse_and_bind_to_a_user() {
        let (ps, en) = parse_policies(
            "CREATE POLICY reps_region ON sales.orders FOR SELECT TO sales_reps \
             USING (region = SPRIG_USER_ATTR('region') AND owner = CURRENT_USER()); \
             ALTER TABLE sales.orders ENABLE ROW LEVEL SECURITY",
        )
        .unwrap();
        assert_eq!(ps.len(), 1);
        assert_eq!(en.len(), 1);
        assert_eq!(ps[0].commands_text(), "SELECT");
        let u = UserValues {
            name: "bob".into(),
            host: "%".into(),
            user_id: 7,
            tenant: 42,
            attrs: None,
            roles: Vec::new(),
        };
        let mut attrs = Map::new();
        attrs.insert("region".into(), json!("EU"));
        let (r, reads) = bind(
            ps[0].using.as_deref().unwrap(),
            "sales",
            "orders",
            &u,
            &attrs,
            None,
        )
        .unwrap();
        assert!(r.contains("'EU'") && r.contains("'bob@%'"), "{r}");
        assert!(r.contains("`ai_probe`."), "{r}");
        assert_eq!(reads, vec![("attribute", "region".to_owned(), true)]);
        let (_, reads) = bind(
            ps[0].using.as_deref().unwrap(),
            "sales",
            "orders",
            &u,
            &Map::new(),
            None,
        )
        .unwrap();
        assert_eq!(reads, vec![("attribute", "region".to_owned(), false)]);
        // Not a policy statement, or an invalid expression: refused.
        assert!(parse_policies("DROP TABLE x").is_err());
        assert!(
            parse_policies("CREATE POLICY p ON t USING (id > 0 AND NOW() > '2026-01-01')").is_err()
        );
        assert_eq!(user_ref("'bob'@'%'"), ("bob".into(), Some("%".into())));
        assert_eq!(user_ref("ann"), ("ann".into(), None));
    }

    /// The logic the DBA evaluation cases propose really compiles with
    /// SprigDB's compiler: functions by the dry run, validations and
    /// after-commit triggers by the deploy's checks against their table.
    #[tokio::test]
    async fn evaluation_logic_compiles() {
        use sprig_logic::declare::{Kind, parse};
        let set: toml::Value = toml::from_str(
            &std::fs::read_to_string(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../sprig-ai/evals/dba.toml"
            ))
            .unwrap(),
        )
        .unwrap();
        let mut cols = sprig_logic::compiler::StaticColumns::new();
        for (t, c, dt, ct) in [
            ("orders", "id", "bigint", "bigint"),
            ("orders", "product_id", "bigint", "bigint"),
            ("orders", "qty", "int", "int"),
            ("orders", "note", "varchar", "varchar(64)"),
            ("audit", "id", "bigint", "bigint"),
            ("audit", "order_id", "bigint", "bigint"),
            ("audit", "qty", "int", "int"),
        ] {
            cols.add(
                t,
                c,
                sprig_logic::compiler::sql::ty_of_column(dt, ct).unwrap(),
            );
        }
        let opts = sprig_logic::compiler::Options {
            module_id: 0,
            version: 0,
            max_loop_iterations: 100_000,
            compile_max_cost: 100_000,
            definer_security: true,
        };
        let mut n = 0;
        for case in set["case"].as_array().unwrap() {
            for call in case["calls"].as_array().unwrap() {
                if call["tool"].as_str() != Some("propose_logic") {
                    continue;
                }
                let sql = call["args"]["sql"].as_str().unwrap();
                let d = parse(sql).unwrap_or_else(|e| panic!("{}: {}", case["id"], e.message));
                match d.kind {
                    Kind::Validation | Kind::Trigger => {
                        let t = d.table.clone().unwrap();
                        let notes =
                            sprig_logic::service::check_bound(&d, "shop", &t.name, &cols, &opts)
                                .unwrap_or_else(|e| panic!("{}: {}", case["id"], e.message));
                        assert!(!notes.is_empty());
                    }
                    _ => {
                        let v = super::super::super::super::logic::dry_run(&d, &cols, false).await;
                        assert_eq!(v["ok"], json!(true), "{}: {v}", case["id"]);
                    }
                }
                n += 1;
            }
        }
        assert!(n >= 3, "{n} logic proposals");
    }

    #[test]
    fn session_values_bind_as_text_like_the_rewrite() {
        // MariaDB's policy views read attributes and claims as text, so a
        // trial must compare against text too: numbers and booleans bind as
        // quoted strings, exactly as the gateway's rewrite binds them.
        for (v, want) in [
            (json!(3), "'3'"),
            (json!(2.5), "'2.5'"),
            (json!(true), "'1'"),
            (json!(false), "'0'"),
            (json!("EU"), "'EU'"),
            (Value::Null, "NULL"),
        ] {
            assert_eq!(literal(&v), want, "{v}");
            assert_eq!(literal(&v), sprig_sql::rls::expr::json_literal(&v, false));
        }
    }
}
