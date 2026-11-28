//! A retrieval index's chunk table (`__sprig_rx_<id>`) out of every
//! client's reach on an engine-only tenant: no client statement may read or
//! write it by name, whatever the entry point. The probes run as a user with
//! no privilege at all, as a user with only routine and view rights on the
//! database (no table privilege), and as the tenant's admin login, through:
//! the MySQL port (3306 persona), the MariaDB port (3307 persona), the HTTP
//! SQL API, text `PREPARE`/`EXECUTE`, the binary protocol's prepare, views,
//! and stored procedures and functions (created and called, `SQL SECURITY
//! DEFINER` and `INVOKER`, by the probe user and by the admin), in plain,
//! backtick-quoted, qualified and mixed-case spellings, and with the name
//! built at run time (PREPARE from a variable, EXECUTE IMMEDIATE, a routine
//! running its argument) and in a trigger. Every probe must be
//! refused; the refusal codes are printed. A probe that succeeds is a
//! security failure: the test prints `HIGH` with the statement and port.
//!
//! ```sh
//! export SPRIG_TEST_RXE_SQL=127.0.0.1:46107      # a gateway's MariaDB port (3307 persona)
//! export SPRIG_TEST_RXE_MYSQL=127.0.0.1:46106    # its MySQL port (3306 persona)
//! export SPRIG_TEST_RXE_HTTP=127.0.0.1:46101     # its HTTP listener
//! export SPRIG_TEST_RXE_TENANT=rxe SPRIG_TEST_RXE_USER=app SPRIG_TEST_RXE_PASSWORD=...
//! export SPRIG_TEST_RXE_GW_LOG=.../gw1.log,...      # every gateway's JSON log (comma-separated)
//! cargo test -p sprig-gateway --test retrieval_chunk_access_live -- --ignored --nocapture
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    missing_docs,
    clippy::too_many_lines
)]

use std::time::{Duration, Instant};

use mysql_async::prelude::Queryable;

const DB: &str = "rxc_live";
const NONE_USER: (&str, &str) = ("rxcnone", "Rxc-none-Passw0rd");
const ROUTINE_USER: (&str, &str) = ("rxcroutine", "Rxc-routine-Passw0rd");

fn need(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| panic!("set {k} (see the file's header)"))
}

fn tenant() -> String {
    need("SPRIG_TEST_RXE_TENANT")
}

async fn login(addr: &str, user: &str, password: &str) -> mysql_async::Conn {
    let (host, port) = addr.split_once(':').unwrap();
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname(host)
        .tcp_port(port.parse().unwrap())
        .user(Some(format!("{}.{user}", tenant())))
        .pass(Some(password))
        .prefer_socket(false);
    mysql_async::Conn::new(opts).await.unwrap()
}

/// A statement's outcome: `Ok(rows)` or the server's (code, message).
type Outcome = Result<usize, (u16, String)>;

fn server_error(e: &mysql_async::Error) -> (u16, String) {
    match e {
        mysql_async::Error::Server(s) => (s.code, s.message.clone()),
        other => (0, other.to_string()),
    }
}

async fn text(c: &mut mysql_async::Conn, s: &str) -> Outcome {
    c.query::<mysql_async::Row, _>(s)
        .await
        .map(|r| r.len())
        .map_err(|e| server_error(&e))
}

async fn binary(c: &mut mysql_async::Conn, s: &str) -> Outcome {
    c.exec::<mysql_async::Row, _, _>(s, ())
        .await
        .map(|r| r.len())
        .map_err(|e| server_error(&e))
}

async fn must(c: &mut mysql_async::Conn, s: &str) {
    if let Err(e) = c.query_drop(s).await {
        panic!("{s}: {e}");
    }
}

/// The HTTP SQL API (`sql:query` for reads, `sql:execute` for writes) with
/// the user's basic credentials: `Ok(0)` for a 2xx answer with no error in
/// it, else (status, body).
fn http(user: &str, password: &str, db: &str, sql: &str) -> Outcome {
    use base64::Engine as _;
    let addr = need("SPRIG_TEST_RXE_HTTP");
    let auth =
        base64::engine::general_purpose::STANDARD.encode(format!("{}.{user}:{password}", tenant()));
    let verb = if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
        "sql:query"
    } else {
        "sql:execute"
    };
    let out = std::process::Command::new("curl")
        .args([
            "-sS",
            "--max-time",
            "30",
            "-X",
            "POST",
            "-w",
            "\n%{http_code}",
            "-H",
            "content-type: application/json",
            "-H",
            &format!("authorization: Basic {auth}"),
            "-d",
            &serde_json::json!({ "sql": sql }).to_string(),
            &format!("http://{addr}/v1/databases/{db}/{verb}"),
        ])
        .output()
        .unwrap();
    let body = String::from_utf8_lossy(&out.stdout).into_owned();
    let (body, code) = body.rsplit_once('\n').unwrap_or(("", "0"));
    let status: u16 = code.trim().parse().unwrap_or(0);
    let lower = body.to_ascii_lowercase();
    if (200..300).contains(&status) && !lower.contains("error") && !lower.contains("\"code\"") {
        Ok(0)
    } else {
        Err((status, body.chars().take(240).collect()))
    }
}

/// The chunk table's name (`__sprig_rx_<index id>`), from the gateway log
/// line the index's creation writes (the last one for this database and
/// index name).
fn chunk_table(index: &str) -> Option<String> {
    let mut found = None;
    for path in need("SPRIG_TEST_RXE_GW_LOG").split(',') {
        let Ok(log) = std::fs::read_to_string(path.trim()) else {
            continue;
        };
        for line in log.lines() {
            if !line.contains("retrieval index created on an engine database") {
                continue;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if v["db"] == DB
                && v["name"] == index
                && let Some(id) = v["index"].as_u64()
            {
                found = Some(format!("__sprig_rx_{id}"));
            }
        }
    }
    found
}

/// Every probe's line for the summary, and the ones that succeeded.
#[derive(Default)]
struct Probes {
    lines: Vec<String>,
    leaks: Vec<String>,
    odd: Vec<String>,
}

/// Codes a refusal may carry: no such table (1146), table or column
/// privilege (1142, 1143), database access (1044), a view or routine
/// naming something it may not (1356), a statement the engine doesn't
/// run (1235, 39960), a routine that doesn't exist because its creation
/// was refused (1305) or may not be run (1370), a reserved name (39999),
/// and the HTTP API's 4xx; on the MySQL persona, `EXECUTE IMMEDIATE` is
/// MariaDB's and a syntax error (1064).
fn refused_ok(port: &str, stmt: &str, code: u16) -> bool {
    matches!(
        code,
        1044 | 1142 | 1143 | 1146 | 1235 | 1305 | 1356 | 1370 | 39960 | 39999 | 400..=499
    ) || (code == 1064 && port == "3306" && stmt.contains("EXECUTE IMMEDIATE"))
}

impl Probes {
    fn record(&mut self, port: &str, who: &str, stmt: &str, r: &Outcome) {
        match r {
            Ok(n) => {
                let l = format!("HIGH {port} as {who}: `{stmt}` succeeded ({n} rows)");
                eprintln!("{l}");
                self.leaks.push(l.clone());
                self.lines.push(l);
            }
            Err((code, msg)) => {
                let short: String = msg.chars().take(110).collect();
                let l = format!("ok   {port} as {who}: `{stmt}` refused {code}: {short}");
                eprintln!("{l}");
                if !refused_ok(port, stmt, *code) {
                    self.odd.push(l.clone());
                }
                self.lines.push(l);
            }
        }
    }
}

/// The spellings of the chunk table a client might try.
fn spellings(table: &str, qualified_only: bool) -> Vec<String> {
    let upper = table.to_ascii_uppercase();
    let mixed = table.replacen("__sprig_rx_", "__Sprig_Rx_", 1);
    let mut v = vec![
        format!("{DB}.{table}"),
        format!("`{DB}`.`{table}`"),
        format!("{DB}.{upper}"),
        format!("`{DB}`.`{mixed}`"),
    ];
    if !qualified_only {
        v.extend([
            table.to_owned(),
            format!("`{table}`"),
            upper,
            format!("`{mixed}`"),
        ]);
    }
    v
}

/// Reads and writes that would run, and change nothing, if allowed.
fn statements(t: &str) -> Vec<String> {
    vec![
        format!("SELECT * FROM {t}"),
        format!("SELECT COUNT(*) FROM {t}"),
        format!("INSERT INTO {t} SELECT * FROM {t} WHERE 1 = 0"),
        format!("UPDATE {t} SET chunk_no = chunk_no WHERE 1 = 0"),
        format!("DELETE FROM {t} WHERE 1 = 0"),
    ]
}

async fn wait_ready(c: &mut mysql_async::Conn, index: &str) -> bool {
    let until = Instant::now() + Duration::from_secs(120);
    while Instant::now() < until {
        let rows: Vec<mysql_async::Row> = c
            .query(format!("SHOW RETRIEVAL INDEXES FROM {DB}"))
            .await
            .unwrap_or_default();
        let ready = rows.iter().any(|r| {
            r.get::<String, _>("Index").as_deref() == Some(index)
                && r.get::<String, _>("State").as_deref() == Some("ready")
        });
        if ready {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    false
}

#[tokio::test]
#[ignore = "live: needs SPRIG_TEST_RXE_SQL, SPRIG_TEST_RXE_MYSQL, SPRIG_TEST_RXE_HTTP, SPRIG_TEST_RXE_TENANT, SPRIG_TEST_RXE_USER, SPRIG_TEST_RXE_PASSWORD, SPRIG_TEST_RXE_GW_LOG; tools/live/README.md"]
async fn no_client_statement_reaches_a_chunk_table() {
    let maria = need("SPRIG_TEST_RXE_SQL");
    let mysql = need("SPRIG_TEST_RXE_MYSQL");
    let admin = need("SPRIG_TEST_RXE_USER");
    let admin_pw = need("SPRIG_TEST_RXE_PASSWORD");
    let ports = [("3307", maria.as_str()), ("3306", mysql.as_str())];

    // ---- setup: a source table, its index, two users ------------------------
    let mut a = login(&maria, &admin, &admin_pw).await;
    for (u, _) in [NONE_USER, ROUTINE_USER] {
        let _ = a.query_drop(format!("DROP USER IF EXISTS '{u}'@'%'")).await;
    }
    must(&mut a, &format!("DROP DATABASE IF EXISTS {DB}")).await;
    must(&mut a, &format!("CREATE DATABASE {DB}")).await;
    must(&mut a, &format!("USE {DB}")).await;
    must(&mut a, "CREATE TABLE src (id INT PRIMARY KEY, body TEXT)").await;
    must(
        &mut a,
        "INSERT INTO src VALUES (1, 'secret alpha text'), (2, 'secret beta text'), \
         (3, 'secret gamma text')",
    )
    .await;
    must(&mut a, "CREATE RETRIEVAL INDEX ck ON src (body)").await;
    assert!(wait_ready(&mut a, "ck").await, "the index never got ready");
    let table = chunk_table("ck").expect("no index-created line in the gateway logs");
    eprintln!("     chunk table: {DB}.{table}");
    for (u, pw) in [NONE_USER, ROUTINE_USER] {
        must(
            &mut a,
            &format!("CREATE USER '{u}'@'%' IDENTIFIED BY '{pw}'"),
        )
        .await;
    }
    must(
        &mut a,
        &format!(
            "GRANT CREATE ROUTINE, ALTER ROUTINE, EXECUTE, CREATE VIEW ON {DB}.* TO '{}'@'%'",
            ROUTINE_USER.0
        ),
    )
    .await;

    let mut p = Probes::default();

    // ---- 1. by name, every spelling, both personas, three users -------------
    for (port, addr) in ports {
        for (who, pw, qualified_only) in [
            (NONE_USER.0, NONE_USER.1, true),
            (ROUTINE_USER.0, ROUTINE_USER.1, false),
            (admin.as_str(), admin_pw.as_str(), false),
        ] {
            let mut c = login(addr, who, pw).await;
            if !qualified_only {
                must(&mut c, &format!("USE {DB}")).await;
            }
            for t in spellings(&table, qualified_only) {
                for s in statements(&t) {
                    let r = text(&mut c, &s).await;
                    p.record(port, who, &s, &r);
                }
            }
            // Prepared: the text protocol's PREPARE/EXECUTE and the binary
            // protocol's COM_STMT_PREPARE.
            for t in spellings(&table, true).into_iter().take(2) {
                for s in statements(&t) {
                    let prep = format!("PREPARE rxc_s FROM '{}'", s.replace('\'', "''"));
                    let r = text(&mut c, &prep).await;
                    if r.is_ok() {
                        let r = text(&mut c, "EXECUTE rxc_s").await;
                        p.record(port, who, &format!("{prep}; EXECUTE rxc_s"), &r);
                        let _ = c.query_drop("DEALLOCATE PREPARE rxc_s").await;
                    } else {
                        p.record(port, who, &prep, &r);
                    }
                    let r = binary(&mut c, &s).await;
                    p.record(&format!("{port} binary"), who, &s, &r);
                }
            }
            // Text built at run time, so no parse of the client's statement
            // sees the name: PREPARE from a user variable, EXECUTE IMMEDIATE.
            let (head, tail) = table.split_at(7);
            for s in statements(&format!("{DB}.{table}")) {
                let built = s.replacen(table.as_str(), &format!("', '{head}', '{tail}', '"), 1);
                let set = format!("SET @rxc_q = CONCAT('{built}')");
                let r = text(&mut c, &set).await;
                if let Err(e) = r {
                    p.record(port, who, &set, &Err(e));
                    continue;
                }
                let r = text(&mut c, "PREPARE rxc_v FROM @rxc_q").await;
                if r.is_ok() {
                    let r = text(&mut c, "EXECUTE rxc_v").await;
                    p.record(
                        port,
                        who,
                        &format!("{set}; PREPARE FROM @rxc_q; EXECUTE"),
                        &r,
                    );
                    let _ = c.query_drop("DEALLOCATE PREPARE rxc_v").await;
                } else {
                    p.record(port, who, &format!("{set}; PREPARE rxc_v FROM @rxc_q"), &r);
                }
                let imm = "EXECUTE IMMEDIATE @rxc_q";
                let r = text(&mut c, imm).await;
                p.record(port, who, &format!("{set}; {imm}"), &r);
            }
            let _ = c.disconnect().await;
        }
    }

    // ---- 2. the HTTP SQL API ------------------------------------------------
    for (who, pw, db, qualified_only) in [
        (NONE_USER.0, NONE_USER.1, "-", true),
        (ROUTINE_USER.0, ROUTINE_USER.1, DB, false),
        (admin.as_str(), admin_pw.as_str(), DB, false),
    ] {
        for t in spellings(&table, qualified_only) {
            for s in statements(&t) {
                let r = http(who, pw, db, &s);
                p.record("http", who, &s, &r);
            }
        }
    }

    // ---- 3. views and stored routines ---------------------------------------
    // Created by the routine user (definer and invoker) and by the admin
    // (definer, executable by the routine user); each creation that passes is
    // then used.
    let routines = |owner: &str| {
        let t = format!("{DB}.{table}");
        let mut v: Vec<(String, String, bool)> = vec![
            (
                format!(
                    "CREATE PROCEDURE {DB}.p_arg_def_{owner}(IN q TEXT) SQL SECURITY DEFINER BEGIN \
                     SET @rxc_p = q; PREPARE s FROM @rxc_p; EXECUTE s; DEALLOCATE PREPARE s; END"
                ),
                format!("CALL {DB}.p_arg_def_{owner}('SELECT * FROM {t}')"),
                true,
            ),
            (
                format!(
                    "CREATE PROCEDURE {DB}.p_arg_inv_{owner}(IN q TEXT) SQL SECURITY INVOKER BEGIN \
                     SET @rxc_p = q; PREPARE s FROM @rxc_p; EXECUTE s; DEALLOCATE PREPARE s; END"
                ),
                format!("CALL {DB}.p_arg_inv_{owner}('DELETE FROM {t} WHERE 1 = 0')"),
                true,
            ),
            (
                format!(
                    "CREATE PROCEDURE {DB}.p_imm_{owner}(IN q TEXT) SQL SECURITY DEFINER \
                     EXECUTE IMMEDIATE q"
                ),
                format!("CALL {DB}.p_imm_{owner}('SELECT COUNT(*) FROM {t}')"),
                true,
            ),
            (
                format!(
                    "CREATE TRIGGER {DB}.tr_{owner} AFTER INSERT ON {DB}.src FOR EACH ROW \
                     DELETE FROM {t} WHERE 1 = 0"
                ),
                format!(
                    "INSERT INTO {DB}.src VALUES ({}, 'trigger probe')",
                    owner[1..].parse::<usize>().unwrap_or(0) * 10
                        + usize::from(owner.starts_with('a'))
                ),
                false,
            ),
        ];
        let fixed = vec![
            (
                format!("CREATE VIEW {DB}.v_{owner} AS SELECT * FROM {t}"),
                format!("SELECT * FROM {DB}.v_{owner}"),
            ),
            (
                format!(
                    "CREATE PROCEDURE {DB}.p_def_{owner}() SQL SECURITY DEFINER SELECT * FROM {t}"
                ),
                format!("CALL {DB}.p_def_{owner}()"),
            ),
            (
                format!(
                    "CREATE PROCEDURE {DB}.p_inv_{owner}() SQL SECURITY INVOKER SELECT * FROM {t}"
                ),
                format!("CALL {DB}.p_inv_{owner}()"),
            ),
            (
                format!(
                    "CREATE PROCEDURE {DB}.p_del_{owner}() SQL SECURITY DEFINER DELETE FROM {t} \
                     WHERE 1 = 0"
                ),
                format!("CALL {DB}.p_del_{owner}()"),
            ),
            (
                format!(
                    "CREATE PROCEDURE {DB}.p_ins_{owner}() SQL SECURITY DEFINER INSERT INTO {t} \
                     SELECT * FROM {t} WHERE 1 = 0"
                ),
                format!("CALL {DB}.p_ins_{owner}()"),
            ),
            (
                format!(
                    "CREATE PROCEDURE {DB}.p_upd_{owner}() SQL SECURITY DEFINER UPDATE {t} SET \
                     chunk_no = chunk_no WHERE 1 = 0"
                ),
                format!("CALL {DB}.p_upd_{owner}()"),
            ),
            (
                format!(
                    "CREATE FUNCTION {DB}.f_def_{owner}() RETURNS INT READS SQL DATA SQL SECURITY \
                     DEFINER RETURN (SELECT COUNT(*) FROM {t})"
                ),
                format!("SELECT {DB}.f_def_{owner}()"),
            ),
            (
                format!(
                    "CREATE FUNCTION {DB}.f_inv_{owner}() RETURNS INT READS SQL DATA SQL SECURITY \
                     INVOKER RETURN (SELECT COUNT(*) FROM {t})"
                ),
                format!("SELECT {DB}.f_inv_{owner}()"),
            ),
            (
                format!(
                    "CREATE PROCEDURE {DB}.p_dyn_{owner}() SQL SECURITY DEFINER BEGIN PREPARE s \
                     FROM 'SELECT * FROM {t}'; EXECUTE s; END"
                ),
                format!("CALL {DB}.p_dyn_{owner}()"),
            ),
        ];
        v.extend(fixed.into_iter().map(|(a, b)| (a, b, false)));
        v
    };
    for (port, addr) in ports {
        let mut r_user = login(addr, ROUTINE_USER.0, ROUTINE_USER.1).await;
        must(&mut r_user, &format!("USE {DB}")).await;
        let mut adm = login(addr, &admin, &admin_pw).await;
        must(&mut adm, &format!("USE {DB}")).await;
        for (owner, conn_is_admin) in [(format!("r{port}"), false), (format!("a{port}"), true)] {
            for (create, use_it, built_at_call) in routines(&owner) {
                let r = if conn_is_admin {
                    text(&mut adm, &create).await
                } else {
                    text(&mut r_user, &create).await
                };
                let creator = if conn_is_admin {
                    admin.as_str()
                } else {
                    ROUTINE_USER.0
                };
                if built_at_call && r.is_ok() {
                    // The name is only in the call's argument: checked there.
                    eprintln!(
                        "     {port} as {creator}: created `{create}`; the call is the probe"
                    );
                } else {
                    p.record(port, creator, &create, &r);
                }
                if r.is_err() {
                    continue;
                }
                if conn_is_admin {
                    let _ = adm
                        .query_drop(format!(
                            "GRANT SELECT, EXECUTE ON {DB}.* TO '{}'@'%'",
                            ROUTINE_USER.0
                        ))
                        .await;
                }
                // Used by the routine user, and by the admin.
                let r = text(&mut r_user, &use_it).await;
                p.record(port, ROUTINE_USER.0, &use_it, &r);
                let r = text(&mut adm, &use_it).await;
                p.record(port, admin.as_str(), &use_it, &r);
                if conn_is_admin {
                    let _ = adm
                        .query_drop(format!(
                            "REVOKE SELECT ON {DB}.* FROM '{}'@'%'",
                            ROUTINE_USER.0
                        ))
                        .await;
                }
            }
        }
        let _ = r_user.disconnect().await;
        let _ = adm.disconnect().await;
    }

    // ---- the index still answers and its chunks are untouched ---------------
    let hits: Vec<mysql_async::Row> = a
        .query("SELECT id FROM RETRIEVE(ck, 'gamma', 3)")
        .await
        .unwrap();
    eprintln!("     the index still answers: {} hits", hits.len());

    for (u, _) in [NONE_USER, ROUTINE_USER] {
        let _ = a.query_drop(format!("DROP USER IF EXISTS '{u}'@'%'")).await;
    }
    let _ = a.query_drop(format!("DROP DATABASE IF EXISTS {DB}")).await;
    let _ = a.disconnect().await;

    eprintln!(
        "     {} probes, {} succeeded (HIGH), {} refused with an unexpected code",
        p.lines.len(),
        p.leaks.len(),
        p.odd.len()
    );
    for l in &p.odd {
        eprintln!("     unexpected code: {l}");
    }
    assert!(!hits.is_empty(), "the index stopped answering");
    assert!(
        p.leaks.is_empty(),
        "HIGH: client statements reached a chunk table:\n{}",
        p.leaks.join("\n")
    );
    assert!(
        p.odd.is_empty(),
        "refusals with an unexpected code:\n{}",
        p.odd.join("\n")
    );
}
