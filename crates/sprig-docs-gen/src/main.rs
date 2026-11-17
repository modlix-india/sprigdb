//! `sprig-docs-gen`: writes SprigDB's generated reference pages
//! (`docs/reference/*.md`) from the code, or checks they're current
//! (`docs/README.md`, "Keeping the reference current").
//!
//! ```text
//! cargo run -p sprig-docs-gen -- [--root <repo>] [--check]
//!     [--sprigctl <path> --sprig <path>]     # also the command pages
//! ```
//!
//! | Page | From |
//! |---|---|
//! | `settings.md` | not here: `sprigdb docs-settings`, checked and rewritten by `cargo test -p sprigdb --test docs_settings` |
//! | `errors.md` | `SprigCode::ALL` and `docs/errors-what-to-do.toml` |
//! | `metrics.md` | the metric macros under `crates/`, and `docs/metrics-help.toml` |
//! | `api.md` | the gateway's `OpenAPI` document |
//! | `sprigctl.md`, `sprig.md` | each command's `--help` (only with the binaries' paths) |
//!
//! With `--check` it writes nothing and exits 1 if any page would change.
//!
//! It also prints warnings on stderr about `metrics.md`, which never change
//! the exit code, with `--check` or without:
//!
//! - metric calls whose name or labels the scanner can't read (see
//!   `sprig_docs::metrics_scan`), unless [`LABEL_OVERRIDES`] gives the labels;
//! - [`LABEL_OVERRIDES`] entries no longer needed;
//! - keys in `docs/metrics-help.toml` that name no recorded metric. They
//!   don't fail the check because a help line may land before the code that
//!   records its metric; a key still listed once that code is in is a typo or
//!   a removed metric.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use sprig_docs::metrics_scan::{Scan, Scanner};
use sprig_docs::reference::{
    self, ApiOp, CommandHelp, ErrorDoc, api_page, cli_page, errors_page, metrics_page,
};

/// Label keys of metrics whose labels the scanner can't read from the
/// source, such as a label set built in another function: the metric, and
/// every key it is recorded with. Only used for calls the scanner reports as
/// unread, and reported when no longer needed. Empty while every call is
/// read.
const LABEL_OVERRIDES: &[(&str, &[&str])] = &[];

/// The pages generated from code alone, by file name, and warnings about
/// them.
fn pages(root: &Path) -> Result<(Vec<(String, String)>, Vec<String>), String> {
    let error_help = read(&root.join("docs/errors-what-to-do.toml"))
        .map(|t| reference::parse_error_help(&t))
        .transpose()?
        .unwrap_or_default();
    let metric_help = read(&root.join("docs/metrics-help.toml"))
        .map(|t| reference::parse_metric_help(&t))
        .transpose()?
        .unwrap_or_default();
    let mut scan = metrics(root)?;
    let warnings = metric_warnings(&mut scan, &metric_help, LABEL_OVERRIDES);
    let pages = vec![
        ("errors.md".into(), errors_page(&errors(), &error_help)),
        (
            "metrics.md".into(),
            metrics_page(&scan.metrics, &metric_help),
        ),
        ("api.md".into(), api_page(&api_ops())),
    ];
    Ok((pages, warnings))
}

/// Applies `overrides` to `scan`'s unread labels, and returns warnings: what
/// the scanner couldn't read, overrides not needed, and help keys that name
/// no recorded metric.
fn metric_warnings(
    scan: &mut Scan,
    help: &BTreeMap<String, String>,
    overrides: &[(&str, &[&str])],
) -> Vec<String> {
    let mut out = Vec::new();
    let mut used = BTreeSet::new();
    for u in &scan.unresolved {
        let at = format!("{}:{}", u.file, u.line);
        let Some(metric) = &u.metric else {
            out.push(format!(
                "{at}: metric name `{}` is no literal or `&str` constant the scanner \
                 finds; the metric is missing from metrics.md",
                u.expr
            ));
            continue;
        };
        if let Some((_, keys)) = overrides.iter().find(|(m, _)| m == metric) {
            used.insert(metric.clone());
            if let Some(m) = scan.metrics.iter_mut().find(|m| &m.name == metric) {
                m.labels.extend(keys.iter().map(|k| (*k).to_owned()));
            }
        } else {
            out.push(format!(
                "{at}: labels `{}` of `{metric}` not read; add its label keys to \
                 LABEL_OVERRIDES in sprig-docs-gen",
                u.expr
            ));
        }
    }
    for (m, _) in overrides {
        if !used.contains(*m) {
            out.push(format!(
                "LABEL_OVERRIDES entry `{m}` is not needed: no unread labels of it"
            ));
        }
    }
    let recorded: BTreeSet<&str> = scan.metrics.iter().map(|m| m.name.as_str()).collect();
    let unrecorded: Vec<&str> = help
        .keys()
        .map(String::as_str)
        .filter(|k| !recorded.contains(k))
        .collect();
    if !unrecorded.is_empty() {
        out.push(format!(
            "{} keys in docs/metrics-help.toml name no recorded metric (fine while \
             their code is on its way): {}",
            unrecorded.len(),
            unrecorded.join(", ")
        ));
    }
    out
}

fn read(p: &Path) -> Option<String> {
    std::fs::read_to_string(p).ok()
}

fn errors() -> Vec<ErrorDoc> {
    use sprig_common::{SprigCode, SprigError};
    SprigCode::ALL
        .iter()
        .map(|&c| {
            let p = SprigError::new(c, "").protocol();
            ErrorDoc {
                number: c.number(),
                name: c.name().to_owned(),
                mongo: c.mongo_code_name().to_owned(),
                sqlstate: String::from_utf8_lossy(&c.sqlstate()).into_owned(),
                retryable: c.retryable(),
                http: p.http_status,
                grpc: format!("{:?}", p.grpc_status),
            }
        })
        .collect()
}

fn metrics(root: &Path) -> Result<Scan, String> {
    let crates = root.join("crates");
    let crates = crates.as_path();
    let mut scanner = Scanner::default();
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(crates)
        .map_err(|e| format!("{}: {e}", crates.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join("src").is_dir())
        .collect();
    dirs.sort();
    for d in dirs {
        let name = d
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut files = Vec::new();
        walk(&d.join("src"), &mut files);
        files.sort();
        for f in files {
            if let Some(t) = read(&f) {
                let shown = f.strip_prefix(root).unwrap_or(&f);
                scanner.scan_file(&name, &shown.display().to_string(), &t);
            }
        }
    }
    Ok(scanner.finish())
}

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            walk(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

fn api_ops() -> Vec<ApiOp> {
    let doc = sprig_gateway::http::studio::openapi();
    let mut out = Vec::new();
    for (path, item) in doc["paths"].as_object().into_iter().flatten() {
        for (method, op) in item.as_object().into_iter().flatten() {
            if !matches!(method.as_str(), "get" | "post" | "put" | "patch" | "delete") {
                continue;
            }
            let s = |k: &str| op.get(k).and_then(serde_json::Value::as_str).unwrap_or("");
            out.push(ApiOp {
                tag: op["tags"][0].as_str().unwrap_or("other").to_owned(),
                method: method.to_uppercase(),
                path: path.clone(),
                id: s("operationId").to_owned(),
                summary: s("summary").to_owned(),
            });
        }
    }
    out
}

/// `tool`'s help and every subcommand's, found by reading the "Commands:"
/// section of each help text.
fn command_help(bin: &Path, tool: &str) -> Result<Vec<CommandHelp>, String> {
    fn help(bin: &Path, args: &[String]) -> Result<String, String> {
        let out = std::process::Command::new(bin)
            .args(args)
            .arg("--help")
            .env("NO_COLOR", "1")
            .output()
            .map_err(|e| format!("{}: {e}", bin.display()))?;
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }
    fn subcommands(help: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut on = false;
        for l in help.lines() {
            if l.trim_end() == "Commands:" {
                on = true;
                continue;
            }
            if on {
                if l.trim().is_empty() || !l.starts_with("  ") {
                    break;
                }
                if let Some(name) = l.split_whitespace().next()
                    && name != "help"
                {
                    out.push(name.to_owned());
                }
            }
        }
        out
    }
    let mut out = Vec::new();
    let mut queue: Vec<Vec<String>> = vec![Vec::new()];
    while let Some(args) = queue.pop() {
        let text = help(bin, &args)?;
        // `sprig admin …` passes every `sprigctl` command through; its
        // subcommands are documented once, on the sprigctl page.
        let leaf = tool == "sprig" && args.first().is_some_and(|a| a == "admin");
        let mut subs: Vec<Vec<String>> = if leaf { Vec::new() } else { subcommands(&text) }
            .into_iter()
            .map(|s| {
                let mut a = args.clone();
                a.push(s);
                a
            })
            .collect();
        subs.reverse();
        queue.extend(subs);
        let path = std::iter::once(tool.to_owned())
            .chain(args.iter().cloned())
            .collect::<Vec<_>>()
            .join(" ");
        out.push(CommandHelp { path, help: text });
    }
    Ok(out)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
    };
    let check = args.iter().any(|a| a == "--check");
    let root = flag("--root").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."),
        PathBuf::from,
    );
    let mut all = match pages(&root) {
        Ok((p, warnings)) => {
            for w in warnings {
                eprintln!("sprig-docs-gen: warning: {w}");
            }
            p
        }
        Err(e) => {
            eprintln!("sprig-docs-gen: {e}");
            return ExitCode::from(2);
        }
    };
    for (tool, intro) in [
        (
            "sprigctl",
            "`sprigctl` is the operator's command-line tool: clusters, tenants, settings, \
             indexes, approvals, moves, failover and the rest of the Admin API.",
        ),
        (
            "sprig",
            "`sprig` is the shell for tenant users and operators: a full-screen TUI and a \
             scriptable CLI over the REST API or the native protocols.",
        ),
    ] {
        if let Some(bin) = flag(&format!("--{tool}")) {
            match command_help(Path::new(bin), tool) {
                Ok(c) => all.push((format!("{tool}.md"), cli_page(tool, intro, &c))),
                Err(e) => {
                    eprintln!("sprig-docs-gen: {e}");
                    return ExitCode::from(2);
                }
            }
        }
    }
    let dir = root.join("docs/reference");
    let mut stale = BTreeSet::new();
    for (name, text) in &all {
        let path = dir.join(name);
        if read(&path).as_deref() == Some(text.as_str()) {
            continue;
        }
        if check {
            stale.insert(name.clone());
        } else if let Err(e) =
            std::fs::create_dir_all(&dir).and_then(|()| std::fs::write(&path, text))
        {
            eprintln!("sprig-docs-gen: {}: {e}", path.display());
            return ExitCode::from(2);
        } else {
            println!("wrote {}", path.display());
        }
    }
    if !stale.is_empty() {
        eprintln!(
            "docs/reference is stale: {}. Run `cargo run -p sprig-docs-gen` and commit the pages.",
            stale.iter().cloned().collect::<Vec<_>>().join(", ")
        );
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The generated pages in `docs/reference` match the code.
    #[test]
    fn docs_reference_is_current() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        for (name, text) in pages(&root).expect("pages").0 {
            let have = read(&root.join("docs/reference").join(&name)).unwrap_or_default();
            assert!(
                have == text,
                "docs/reference/{name} is stale: run `cargo run -p sprig-docs-gen`"
            );
        }
    }

    fn scan_of(src: &str) -> Scan {
        let mut s = Scanner::default();
        s.scan_file("sprig-x", "x.rs", src);
        s.finish()
    }

    #[test]
    fn overrides_fill_unread_labels_and_warn_when_unneeded() {
        let mut scan = scan_of(
            r#"fn f(ls: Vec<Label>) {
                gauge!("sprig_a", ls).set(1.0);
                gauge!("sprig_b", ls).set(1.0);
                gauge!("sprig_c", "k" => 1).set(1.0);
            }"#,
        );
        let overrides: &[(&str, &[&str])] = &[("sprig_a", &["dc", "cluster"]), ("sprig_c", &["k"])];
        let w = metric_warnings(&mut scan, &BTreeMap::new(), overrides);
        let a = &scan.metrics[0];
        assert_eq!(
            a.labels.iter().map(String::as_str).collect::<Vec<_>>(),
            ["cluster", "dc"]
        );
        assert_eq!(w.len(), 2, "{w:?}");
        assert!(
            w[0].starts_with("x.rs:3: labels `ls` of `sprig_b` not read"),
            "{w:?}"
        );
        assert!(w[1].contains("`sprig_c` is not needed"), "{w:?}");
    }

    #[test]
    fn help_keys_without_a_metric_are_warned_about() {
        let mut scan = scan_of(r#"counter!("sprig_a_total").increment(1);"#);
        let help: BTreeMap<String, String> = [("sprig_a_total", "x"), ("sprig_gone", "y")]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        let w = metric_warnings(&mut scan, &help, &[]);
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(
            w[0].starts_with("1 keys in docs/metrics-help.toml"),
            "{w:?}"
        );
        assert!(w[0].ends_with(": sprig_gone"), "{w:?}");
        assert!(metric_warnings(&mut scan, &BTreeMap::new(), &[]).is_empty());
    }
}
