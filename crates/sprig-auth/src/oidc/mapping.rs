//! Claim mappings: from a verified token's claims to the account it logs
//! in as and the roles it adds.
//!
//! ```text
//! user mappings, by ascending priority: the first whose claim matches
//!   names the account: target with each {claim} replaced by the claim's text
//!   (a template naming a missing claim doesn't match)
//! role mappings: every one whose claim matches adds its target
//! match: any    → the claim is present and not null
//!        equals → its text, or one element's of an array, equals the value
//!        regex  → its text, or one element's, matches the whole expression
//! ```

use regex::Regex;
use serde_json::Value;

use super::store::{ClaimMapping, MappingKind, MatchKind};

/// The largest compiled expression accepted.
const REGEX_SIZE_LIMIT: usize = 1 << 16;

/// Compiles a mapping's expression, anchored so it matches whole values.
///
/// # Errors
/// The compiler's message.
pub fn compile(expr: &str) -> Result<Regex, String> {
    regex::RegexBuilder::new(&format!("^(?:{expr})$"))
        .size_limit(REGEX_SIZE_LIMIT)
        .build()
        .map_err(|e| format!("match_value {expr:?} isn't a regular expression: {e}"))
}

/// A mapping ready to evaluate.
#[derive(Debug, Clone)]
pub struct Mapping {
    /// As stored.
    pub row: ClaimMapping,
    regex: Option<Regex>,
}

impl Mapping {
    /// Prepares `row`; `None` when its expression doesn't compile (it was
    /// checked when written, so only a hand-edited row fails).
    #[must_use]
    pub fn new(row: ClaimMapping) -> Option<Self> {
        let regex = match (row.match_kind, &row.match_value) {
            (MatchKind::Regex, Some(v)) => Some(compile(v).ok()?),
            _ => None,
        };
        Some(Self { row, regex })
    }

    fn matches(&self, claims: &Value) -> bool {
        let Some(v) = claim(claims, &self.row.claim) else {
            return false;
        };
        let one = |x: &Value| -> bool {
            let Some(t) = text(x) else {
                return false;
            };
            match self.row.match_kind {
                MatchKind::Any => true,
                MatchKind::Equals => self.row.match_value.as_deref() == Some(t.as_str()),
                MatchKind::Regex => self.regex.as_ref().is_some_and(|r| r.is_match(&t)),
            }
        };
        match v {
            Value::Null => false,
            Value::Array(a) => {
                if self.row.match_kind == MatchKind::Any {
                    !a.is_empty()
                } else {
                    a.iter().any(one)
                }
            }
            Value::Object(_) => self.row.match_kind == MatchKind::Any,
            x => one(x),
        }
    }
}

/// The claim `name`: a top-level claim, else a dotted path into objects.
#[must_use]
pub fn claim<'a>(claims: &'a Value, name: &str) -> Option<&'a Value> {
    if let Some(v) = claims.get(name) {
        return Some(v);
    }
    if !name.contains('.') {
        return None;
    }
    name.split('.').try_fold(claims, |v, part| v.get(part))
}

/// A scalar claim's text (strings as they are; numbers and booleans as
/// JSON writes them).
fn text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// `target` with each `{claim}` replaced by that claim's text; `None` when
/// a named claim is missing or isn't a scalar, or a brace is unmatched.
#[must_use]
pub fn expand(target: &str, claims: &Value) -> Option<String> {
    let mut out = String::with_capacity(target.len());
    let mut rest = target;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let close = after.find('}')?;
        out.push_str(&text(claim(claims, &after[..close])?)?);
        rest = &after[close + 1..];
    }
    if rest.contains('}') {
        return None;
    }
    out.push_str(rest);
    Some(out)
}

/// What a token's claims map to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Mapped {
    /// The account name the first matching user mapping names.
    pub user: Option<String>,
    /// The roles every matching role mapping adds, in priority order,
    /// without repeats.
    pub roles: Vec<String>,
}

/// Evaluates `mappings` (in any order) on `claims`.
#[must_use]
pub fn map(mappings: &[Mapping], claims: &Value) -> Mapped {
    let mut sorted: Vec<&Mapping> = mappings.iter().collect();
    sorted.sort_by_key(|m| m.row.priority);
    let mut out = Mapped::default();
    for m in sorted {
        if !m.matches(claims) {
            continue;
        }
        match m.row.kind {
            MappingKind::User => {
                if out.user.is_none()
                    && let Some(u) = expand(&m.row.target, claims).filter(|u| !u.is_empty())
                {
                    out.user = Some(u);
                }
            }
            MappingKind::Role => {
                if let Some(r) = expand(&m.row.target, claims).filter(|r| !r.is_empty())
                    && !out.roles.contains(&r)
                {
                    out.roles.push(r);
                }
            }
        }
    }
    out
}
