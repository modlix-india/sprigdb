//! Upgrade readiness: what the next version changes, against how this
//! deployment is used. The diff is deterministic; the model only explains
//! it.
//!
//! ```text
//! running := this build's settings registry and error codes
//! target  := the next version's docs pack: its settings and error code
//!            reference pages, read back into the same form
//! diff    := settings added, removed, renamed (a removed key and an added one
//!            with the same type and default and the same last part or the
//!            same description), default, type, scope or apply changes;
//!            codes added, removed and renamed
//! usage   := settings set away from their default (global, dc and cluster
//!            scopes), statement kinds and front ends of the workload
//!            fingerprints, features in use (records in etcd)
//! report  := each change, ranked:
//!     breaks  a key set here is removed or renamed (its value is ignored
//!             after the upgrade), or loses the scope it is set at
//!     changes a default changes for a key used at its default, in a group
//!             in use; a type change of a key set here; a code renamed or
//!             removed (clients matching on its name)
//!     info    the rest
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

/// What one setting is, as the settings reference shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettingInfo {
    /// `bool`, `int`, `float` or `text`.
    pub kind: String,
    /// The default, as text.
    pub default: String,
    /// The scopes that may set it.
    pub scopes: Vec<String>,
    /// `live`, `new sessions` or `rebind`.
    pub apply: String,
    /// Its description.
    pub doc: String,
}

/// One version's settings and error codes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    /// The version.
    pub version: String,
    /// Settings by key.
    pub settings: BTreeMap<String, SettingInfo>,
    /// Error names by code.
    pub codes: BTreeMap<u32, String>,
}

/// The settings reference page in a docs pack.
pub const SETTINGS_PAGE: &str = "reference/settings";
/// The error codes reference page in a docs pack.
pub const ERRORS_PAGE: &str = "reference/errors";

fn cells(line: &str) -> Option<Vec<String>> {
    let t = line.trim();
    if !t.starts_with('|') || !t.ends_with('|') || t.starts_with("|---") {
        return None;
    }
    // Cells are split on unescaped pipes.
    let inner = &t[1..t.len() - 1];
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut prev = '\0';
    for c in inner.chars() {
        if c == '|' && prev != '\\' {
            out.push(cur.trim().replace("\\|", "|"));
            cur.clear();
        } else {
            cur.push(c);
        }
        prev = c;
    }
    out.push(cur.trim().replace("\\|", "|"));
    Some(out)
}

fn ticked(s: &str) -> Option<&str> {
    let s = s.trim();
    let rest = s.strip_prefix('`')?;
    let end = rest.find('`')?;
    Some(&rest[..end])
}

impl Catalog {
    /// Reads the settings and error-code rows out of the reference pages'
    /// text (chunks of [`SETTINGS_PAGE`] and [`ERRORS_PAGE`], in any
    /// order). Lines that aren't table rows are skipped.
    #[must_use]
    pub fn from_reference(version: &str, texts: &[&str]) -> Self {
        let mut c = Catalog {
            version: version.to_owned(),
            ..Catalog::default()
        };
        for text in texts {
            for line in text.lines() {
                let Some(cells) = cells(line) else { continue };
                // | `key` | kind | `default` | scopes | bounds | applies | doc |
                if cells.len() >= 7
                    && let Some(key) = ticked(&cells[0])
                    && key.contains('.')
                    && matches!(cells[1].as_str(), "bool" | "int" | "float" | "text")
                {
                    let scopes = cells[3]
                        .replace("(operator only)", "")
                        .split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect();
                    c.settings.insert(
                        key.to_owned(),
                        SettingInfo {
                            kind: cells[1].clone(),
                            default: ticked(&cells[2]).unwrap_or(&cells[2]).to_owned(),
                            scopes,
                            apply: cells[5].clone(),
                            doc: cells[6..].join(" | "),
                        },
                    );
                    continue;
                }
                // | 39000 | `ER_SPRIG_...`<br>`Sprig...` | ...
                if cells.len() >= 3
                    && let Ok(code) = cells[0].parse::<u32>()
                    && (39_000..=39_999).contains(&code)
                    && let Some(name) = ticked(&cells[1])
                {
                    c.codes.insert(code, name.to_owned());
                }
            }
        }
        c
    }
}

/// A setting that exists in both versions but changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Changed {
    /// The key.
    pub key: String,
    /// What changed: `default`, `type`, `scopes` or `apply`.
    pub what: String,
    /// Before.
    pub from: String,
    /// After.
    pub to: String,
}

/// What the next version changes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diff {
    /// The running version.
    pub from: String,
    /// The target version.
    pub to: String,
    /// Settings only the target has.
    pub added: Vec<String>,
    /// Settings only the running version has (renames excluded).
    pub removed: Vec<String>,
    /// `(old, new)` keys that look renamed.
    pub renamed: Vec<(String, String)>,
    /// Settings in both that changed.
    pub changed: Vec<Changed>,
    /// Codes only the target has.
    pub codes_added: Vec<u32>,
    /// Codes only the running version has, with their names.
    pub codes_removed: Vec<(u32, String)>,
    /// `(code, old name, new name)`.
    pub codes_renamed: Vec<(u32, String, String)>,
}

impl Diff {
    /// Whether nothing changed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.renamed.is_empty()
            && self.changed.is_empty()
            && self.codes_added.is_empty()
            && self.codes_removed.is_empty()
            && self.codes_renamed.is_empty()
    }
}

fn last_part(k: &str) -> &str {
    k.rsplit('.').next().unwrap_or(k)
}

/// The diff from `running` to `target`.
#[must_use]
pub fn diff(running: &Catalog, target: &Catalog) -> Diff {
    let mut d = Diff {
        from: running.version.clone(),
        to: target.version.clone(),
        ..Diff::default()
    };
    let mut added: BTreeSet<&String> = target
        .settings
        .keys()
        .filter(|k| !running.settings.contains_key(*k))
        .collect();
    for (k, old) in &running.settings {
        let Some(new) = target.settings.get(k) else {
            // Renamed: one added key of the same type and default, with the
            // same last part or the same description.
            let cands: Vec<&String> = added
                .iter()
                .copied()
                .filter(|a| {
                    let n = &target.settings[*a];
                    n.kind == old.kind
                        && n.default == old.default
                        && (last_part(a) == last_part(k)
                            || (!old.doc.is_empty() && n.doc == old.doc))
                })
                .collect();
            if let [one] = cands.as_slice() {
                d.renamed.push((k.clone(), (*one).clone()));
                added.remove(*one);
            } else {
                d.removed.push(k.clone());
            }
            continue;
        };
        for (what, a, b) in [
            ("default", &old.default, &new.default),
            ("type", &old.kind, &new.kind),
            ("apply", &old.apply, &new.apply),
        ] {
            if a != b {
                d.changed.push(Changed {
                    key: k.clone(),
                    what: what.to_owned(),
                    from: a.clone(),
                    to: b.clone(),
                });
            }
        }
        if old.scopes != new.scopes {
            d.changed.push(Changed {
                key: k.clone(),
                what: "scopes".to_owned(),
                from: old.scopes.join(", "),
                to: new.scopes.join(", "),
            });
        }
    }
    d.added = added.into_iter().cloned().collect();
    for (c, name) in &running.codes {
        match target.codes.get(c) {
            None => d.codes_removed.push((*c, name.clone())),
            Some(n) if n != name => d.codes_renamed.push((*c, name.clone(), n.clone())),
            Some(_) => {}
        }
    }
    d.codes_added = target
        .codes
        .keys()
        .filter(|c| !running.codes.contains_key(c))
        .copied()
        .collect();
    // An empty target page reads as "everything removed": say nothing
    // about codes rather than that.
    if target.codes.is_empty() {
        d.codes_removed.clear();
        d.codes_renamed.clear();
    }
    d
}

/// A setting set away from its default here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetKey {
    /// The key.
    pub key: String,
    /// Where: `global`, `dc` or `cluster`.
    pub scope: String,
}

/// How this deployment is used. Counts and keys only: never values.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    /// Settings set away from their defaults.
    pub settings_set: Vec<SetKey>,
    /// Statement fingerprints by `front_end kind stmt_type`, over the
    /// tenants read.
    pub statements: BTreeMap<String, u64>,
    /// Records per feature in use (`streams`, `sinks`, ...).
    pub features: BTreeMap<String, u64>,
    /// Tenants whose workload was read, and how many were skipped.
    pub tenants_read: u64,
    /// Tenants skipped (catalog unreachable, or past the cap).
    pub tenants_skipped: u64,
}

impl Usage {
    /// The setting groups this deployment uses: those of the keys it
    /// sets, of its front ends and of its features.
    #[must_use]
    pub fn groups(&self) -> BTreeSet<String> {
        let mut g: BTreeSet<String> = self
            .settings_set
            .iter()
            .map(|s| s.key.split('.').next().unwrap_or("").to_owned())
            .collect();
        for k in self.statements.keys() {
            let fe = k.split(' ').next().unwrap_or("");
            let groups: &[&str] = match fe {
                "mysql" | "mariadb" => &["mysql", "gateway", "backend", "limits", "sql"],
                "mongo" => &["mongo", "gateway", "limits"],
                "redis" => &["redis", "gateway", "limits"],
                "http" | "grpc" => &["api", "gateway", "limits", "studio"],
                "logic" => &["logic"],
                _ => &[],
            };
            g.extend(groups.iter().map(|s| (*s).to_owned()));
        }
        g.extend(self.features.keys().cloned());
        g
    }

    fn set(&self, key: &str) -> Option<&SetKey> {
        self.settings_set.iter().find(|s| s.key == key)
    }
}

/// How much an item matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Impact {
    /// Something set here stops working.
    Breaks,
    /// Behaviour here changes.
    Changes,
    /// Worth knowing.
    Info,
}

impl Impact {
    /// The text form.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Impact::Breaks => "breaks",
            Impact::Changes => "changes",
            Impact::Info => "info",
        }
    }
}

/// One line of the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    /// How much it matters here.
    pub impact: Impact,
    /// The setting key or error code it is about.
    pub subject: String,
    /// What changes, and what to do.
    pub text: String,
}

/// The deterministic report.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    /// The running version.
    pub from: String,
    /// The target version.
    pub to: String,
    /// Items, most important first.
    pub items: Vec<Item>,
    /// Settings added in the target, by group.
    pub added_by_group: BTreeMap<String, u64>,
    /// Codes added in the target.
    pub codes_added: u64,
}

/// Added keys listed in the text at most.
const MAX_ADDED_LISTED: usize = 20;

/// `diff` matched against `usage`.
#[must_use]
#[allow(clippy::too_many_lines)] // one rule per kind of change
pub fn report(d: &Diff, usage: &Usage) -> Report {
    let groups = usage.groups();
    let in_use = |k: &str| groups.contains(k.split('.').next().unwrap_or(""));
    let mut items = Vec::new();
    for k in &d.removed {
        match usage.set(k) {
            Some(s) => items.push(Item {
                impact: Impact::Breaks,
                subject: k.clone(),
                text: format!(
                    "`{k}` is set here ({} scope) and is removed in {}: its value is ignored after the upgrade (the controller logs it as a bad setting record). Find what replaces it in the release notes, and unset it before upgrading.",
                    s.scope, d.to
                ),
            }),
            None => items.push(Item {
                impact: Impact::Info,
                subject: k.clone(),
                text: format!("`{k}` is removed in {}; it isn't set here.", d.to),
            }),
        }
    }
    for (old, new) in &d.renamed {
        match usage.set(old) {
            Some(s) => items.push(Item {
                impact: Impact::Breaks,
                subject: old.clone(),
                text: format!(
                    "`{old}` is set here ({} scope) and looks renamed to `{new}` in {}: the old key's value is ignored after the upgrade. Set `{new}` to the same value at the same scope, then unset `{old}`.",
                    s.scope, d.to
                ),
            }),
            None => items.push(Item {
                impact: Impact::Info,
                subject: old.clone(),
                text: format!(
                    "`{old}` looks renamed to `{new}` in {}; it isn't set here.",
                    d.to
                ),
            }),
        }
    }
    for c in &d.changed {
        let set = usage.set(&c.key);
        let (impact, text) = match (c.what.as_str(), set) {
            ("default", None) if in_use(&c.key) => (
                Impact::Changes,
                format!(
                    "The default of `{}` changes from `{}` to `{}`; this deployment uses the default, so it gets the new value. Set `{}` to `{}` before upgrading to keep today's behaviour.",
                    c.key, c.from, c.to, c.key, c.from
                ),
            ),
            ("default", None) => (
                Impact::Info,
                format!(
                    "The default of `{}` changes from `{}` to `{}` (its group isn't in use here).",
                    c.key, c.from, c.to
                ),
            ),
            ("default", Some(s)) => (
                Impact::Info,
                format!(
                    "The default of `{}` changes from `{}` to `{}`; it is set here ({} scope), so nothing changes.",
                    c.key, c.from, c.to, s.scope
                ),
            ),
            ("scopes", Some(s)) if !c.to.split(", ").any(|x| x == s.scope) => (
                Impact::Breaks,
                format!(
                    "`{}` is set here at {} scope, which {} no longer accepts (scopes: {}). Move the value to an accepted scope before upgrading.",
                    c.key, s.scope, d.to, c.to
                ),
            ),
            ("type", Some(s)) => (
                Impact::Changes,
                format!(
                    "`{}` changes type from {} to {} and is set here ({} scope): check its value still reads the same.",
                    c.key, c.from, c.to, s.scope
                ),
            ),
            (what, _) => (
                Impact::Info,
                format!("`{}`: {what} changes from {} to {}.", c.key, c.from, c.to),
            ),
        };
        items.push(Item {
            impact,
            subject: c.key.clone(),
            text,
        });
    }
    for (code, old, new) in &d.codes_renamed {
        items.push(Item {
            impact: Impact::Changes,
            subject: code.to_string(),
            text: format!(
                "Error {code} is renamed from {old} to {new}: the number stays, but tools and alerts that match on the name need the new one."
            ),
        });
    }
    for (code, name) in &d.codes_removed {
        items.push(Item {
            impact: Impact::Changes,
            subject: code.to_string(),
            text: format!(
                "Error {code} ({name}) is no longer raised in {}: alerts or retries that wait for it won't fire.",
                d.to
            ),
        });
    }
    let mut added_by_group: BTreeMap<String, u64> = BTreeMap::new();
    for k in &d.added {
        *added_by_group
            .entry(k.split('.').next().unwrap_or("").to_owned())
            .or_default() += 1;
    }
    let used_added: Vec<&String> = d
        .added
        .iter()
        .filter(|k| in_use(k))
        .take(MAX_ADDED_LISTED)
        .collect();
    if !used_added.is_empty() {
        items.push(Item {
            impact: Impact::Info,
            subject: "settings".to_owned(),
            text: format!(
                "New settings in groups used here: {}.",
                used_added
                    .iter()
                    .map(|k| format!("`{k}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        });
    }
    items.sort_by(|a, b| {
        a.impact
            .cmp(&b.impact)
            .then_with(|| a.subject.cmp(&b.subject))
    });
    Report {
        from: d.from.clone(),
        to: d.to.clone(),
        items,
        added_by_group,
        codes_added: d.codes_added.len() as u64,
    }
}

impl Report {
    /// Items of `impact`.
    #[must_use]
    pub fn count(&self, impact: Impact) -> usize {
        self.items.iter().filter(|i| i.impact == impact).count()
    }

    /// The report as text: the summary line, then each item.
    #[must_use]
    pub fn text(&self) -> String {
        let mut t = String::new();
        let _ = writeln!(
            t,
            "Upgrade from {} to {}: {} that break, {} that change behaviour, {} for information; {} new settings, {} new error codes.",
            self.from,
            self.to,
            self.count(Impact::Breaks),
            self.count(Impact::Changes),
            self.count(Impact::Info),
            self.added_by_group.values().sum::<u64>(),
            self.codes_added
        );
        for i in &self.items {
            let _ = writeln!(t, "- [{}] {}", i.impact.as_str(), i.text);
        }
        t
    }
}

/// What the model is told when it explains a report.
pub const RULES: &str = "\
You are explaining an upgrade readiness report to SprigDB's operators. The report was computed by SprigDB: the next version's settings and error codes compared with this deployment's settings, workload and features. You see keys, codes and counts, never values or names.
- Explain what breaks first, then what changes, in plain words, with what to do before upgrading for each. Leave pure information items out unless they matter.
- Look up the upgrade procedure and any setting you discuss with search_docs (audience operator), and cite the pages. Name only settings, codes and commands the report or a search result shows.
- Don't add changes the report doesn't list, and say when you are unsure.
- Nothing is changed by your answer.";
