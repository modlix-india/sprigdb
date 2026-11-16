//! The AI operators in aggregation: `$aiEmbed`, `$aiClassify`,
//! `$aiExtract`, `$aiSummarize` and `$aiComplete`.
//!
//! They are the SQL functions `AI_EMBED` and the others, answered by the
//! same gateway step (settings, egress, budgets, cache and audit are the
//! same). A stage holding them (`$project`, `$addFields`/`$set`, `$group`,
//! `$match` with `$expr`, ...) runs in two steps:
//!
//! 1. each operator's input is evaluated against every input document, and
//!    the distinct values are sent as one SQL statement per batch:
//!    `/* sprig-ai-db:<db> */ SELECT i, AI_CLASSIFY(v, 'a', 'b') FROM
//!    (SELECT 0 AS i, _utf8mb4 X'..' AS v UNION ALL ...) AS d`;
//! 2. the answers are put in a hidden field of each document, the operator
//!    is replaced by that field's path, and the stage runs as usual. The
//!    hidden fields are removed from what the stage returns.
//!
//! Forms:
//!
//! | Operator | Takes |
//! |---|---|
//! | `$aiEmbed` | `<expr>` or `{input, model?}`: an array of doubles |
//! | `$aiClassify` | `{input, labels: [..], model?}`: one of the labels |
//! | `$aiExtract` | `{input, schema, model?}`: a document (or value) fitting the JSON schema |
//! | `$aiSummarize` | `<expr>` or `{input, maxWords?, model?}`: a string |
//! | `$aiComplete` | `<expr>` or `{prompt, model?}`: a string |
//!
//! A `null` or missing input gives `null`; any other input that isn't a
//! string fails with 14. `$vectorSearch`'s `queryVector` may be `{$aiEmbed:
//! "<text>"}`. Operators inside `$facet`, `$lookup` or `$unionWith`
//! pipelines, and in `find` projections, aren't answered.

use std::collections::HashMap;

use bson::{Bson, DateTime, Document};
use sprig_proto_mongo::errors::{MongoError, code};

use crate::agg::{self, Vars};
use crate::engine::{SqlRunner, mongo_error};

/// The operators and the SQL functions they are.
pub const OPS: [(&str, &str); 5] = [
    ("$aiEmbed", "AI_EMBED"),
    ("$aiClassify", "AI_CLASSIFY"),
    ("$aiExtract", "AI_EXTRACT"),
    ("$aiSummarize", "AI_SUMMARIZE"),
    ("$aiComplete", "AI_COMPLETE"),
];

/// The hidden fields' prefix.
pub const HIDDEN: &str = "__sprig_ai_";

/// Distinct inputs in one statement at most.
const BATCH: usize = 500;

/// The longest statement built, in bytes.
const MAX_STATEMENT: usize = 4 << 20;

fn sql_name(op: &str) -> Option<&'static str> {
    OPS.iter().find(|(m, _)| *m == op).map(|(_, s)| *s)
}

/// Whether `v` holds an AI operator anywhere.
#[must_use]
pub fn has_ai(v: &Bson) -> bool {
    match v {
        Bson::Document(d) => d
            .iter()
            .any(|(k, v)| sql_name(k).is_some() || has_ai(v)),
        Bson::Array(a) => a.iter().any(has_ai),
        _ => false,
    }
}

/// Whether the stage holds an AI operator.
#[must_use]
pub fn stage_has_ai(stage: &Document) -> bool {
    stage.values().any(has_ai)
}

/// One operator of a stage.
#[derive(Debug, Clone, PartialEq)]
struct Op {
    /// The SQL function.
    f: &'static str,
    /// The input expression.
    input: Bson,
    /// The later SQL arguments.
    rest: Vec<String>,
}

fn bad(m: impl Into<String>) -> MongoError {
    MongoError::new(code::FAILED_TO_PARSE, m)
}

fn text_arg(op: &str, k: &str, v: &Bson) -> Result<String, MongoError> {
    match v {
        Bson::String(s) => Ok(s.clone()),
        _ => Err(bad(format!("{op}'s {k} must be a string"))),
    }
}

/// The operator `op` with argument `arg`.
fn parse_op(op: &str, f: &'static str, arg: &Bson) -> Result<Op, MongoError> {
    let input_key = if op == "$aiComplete" { "prompt" } else { "input" };
    let spec = match arg {
        Bson::Document(d) if d.contains_key(input_key) || d.contains_key("input") => Some(d),
        _ => None,
    };
    let Some(d) = spec else {
        if matches!(op, "$aiClassify" | "$aiExtract") {
            return Err(bad(format!(
                "{op} takes a document with input and {}",
                if op == "$aiClassify" { "labels" } else { "schema" }
            )));
        }
        return Ok(Op {
            f,
            input: arg.clone(),
            rest: Vec::new(),
        });
    };
    let allowed: &[&str] = match op {
        "$aiClassify" => &["input", "labels", "model"],
        "$aiExtract" => &["input", "schema", "model"],
        "$aiSummarize" => &["input", "maxWords", "model"],
        "$aiComplete" => &["prompt", "input", "model"],
        _ => &["input", "model"],
    };
    if let Some(k) = d.keys().find(|k| !allowed.contains(&k.as_str())) {
        return Err(bad(format!("{op} has no argument {k}")));
    }
    let input = d
        .get(input_key)
        .or_else(|| d.get("input"))
        .cloned()
        .unwrap_or(Bson::Null);
    let mut rest = Vec::new();
    match op {
        "$aiClassify" => {
            let labels = d
                .get_array("labels")
                .map_err(|_| bad("$aiClassify needs labels: an array of strings"))?;
            for l in labels {
                rest.push(text_arg(op, "labels", l)?);
            }
        }
        "$aiExtract" => match d.get("schema") {
            Some(Bson::Document(s)) => {
                rest.push(Bson::Document(s.clone()).into_relaxed_extjson().to_string());
            }
            Some(Bson::String(s)) => rest.push(s.clone()),
            _ => return Err(bad("$aiExtract needs schema: a JSON schema document")),
        },
        "$aiSummarize" => {
            if let Some(w) = d.get("maxWords") {
                let n = crate::filter::as_i64(w)
                    .ok_or_else(|| bad("$aiSummarize's maxWords must be a number"))?;
                rest.push(n.to_string());
            } else if d.contains_key("model") {
                rest.push("100".to_owned());
            }
        }
        _ => {}
    }
    if let Some(m) = d.get("model") {
        rest.push(text_arg(op, "model", m)?);
    }
    Ok(Op { f, input, rest })
}

/// The stage with each operator replaced by its hidden field's path, and
/// the operators in order.
fn rewrite(v: &Bson, ops: &mut Vec<Op>) -> Result<Bson, MongoError> {
    Ok(match v {
        Bson::Document(d) => {
            if d.len() == 1
                && let Some((k, arg)) = d.iter().next()
                && let Some(f) = sql_name(k)
            {
                let op = parse_op(k, f, arg)?;
                if has_ai(&op.input) {
                    return Err(bad("AI operators can't be nested"));
                }
                ops.push(op);
                return Ok(Bson::String(format!("${HIDDEN}{}", ops.len() - 1)));
            }
            let mut out = Document::new();
            for (k, v) in d {
                out.insert(k.clone(), rewrite(v, ops)?);
            }
            Bson::Document(out)
        }
        Bson::Array(a) => Bson::Array(a.iter().map(|x| rewrite(x, ops)).collect::<Result<_, _>>()?),
        x => x.clone(),
    })
}

/// A string as `_utf8mb4 X'..'`, safe whatever the session's `sql_mode`.
fn hex_text(s: &str) -> String {
    use std::fmt::Write as _;
    let mut o = String::with_capacity(s.len() * 2 + 12);
    o.push_str("_utf8mb4 X'");
    for b in s.as_bytes() {
        let _ = write!(o, "{b:02X}");
    }
    o.push('\'');
    o
}

/// The statement answering `inputs` with `f(v, rest...)`.
#[must_use]
pub fn statement(db: &str, f: &str, rest: &[String], inputs: &[&str]) -> String {
    let args: String = rest.iter().map(|r| format!(", {}", hex_text(r))).collect();
    let rows: Vec<String> = inputs
        .iter()
        .enumerate()
        .map(|(i, v)| {
            if i == 0 {
                format!("SELECT {i} AS i, {} AS v", hex_text(v))
            } else {
                format!("SELECT {i}, {}", hex_text(v))
            }
        })
        .collect();
    format!(
        "/* sprig-ai-db:{} */ SELECT i, {f}(v{args}) FROM ({}) AS d",
        db.replace("*/", ""),
        rows.join(" UNION ALL ")
    )
}

/// An answer's bytes as a BSON value.
fn value_of(f: &str, b: Option<&[u8]>) -> Bson {
    let Some(b) = b else { return Bson::Null };
    match f {
        "AI_EMBED" => Bson::Array(
            b.chunks_exact(4)
                .map(|c| Bson::Double(f64::from(f32::from_le_bytes([c[0], c[1], c[2], c[3]]))))
                .collect(),
        ),
        "AI_EXTRACT" => serde_json::from_slice::<serde_json::Value>(b)
            .ok()
            .and_then(|v| Bson::try_from(v).ok())
            .unwrap_or(Bson::Null),
        _ => Bson::String(String::from_utf8_lossy(b).into_owned()),
    }
}

/// A SQL error as the operators report it: argument errors are 2.
fn sql_failed(e: &crate::engine::SqlError) -> MongoError {
    match e.errno {
        Some(1210 | 1582 | 1235) => MongoError::new(code::BAD_VALUE, e.message.clone()),
        _ => mongo_error(e),
    }
}

/// The answers of `f(input, rest...)` for `inputs` (distinct).
///
/// # Errors
/// The gateway's: 39991, 39990, 39995, 39992, 39993, 39994; 2 for bad
/// arguments.
pub async fn answer(
    sql: &mut dyn SqlRunner,
    db: &str,
    f: &str,
    rest: &[String],
    inputs: &[&str],
) -> Result<Vec<Bson>, MongoError> {
    let mut out = vec![Bson::Null; inputs.len()];
    let mut start = 0;
    while start < inputs.len() {
        let mut end = start;
        let mut size = 0;
        while end < inputs.len() && end - start < BATCH && (end == start || size < MAX_STATEMENT) {
            size += inputs[end].len() * 2 + 40;
            end += 1;
        }
        let stmt = statement(db, f, rest, &inputs[start..end]);
        let rows = sql.query(&stmt).await.map_err(|e| sql_failed(&e))?;
        for r in rows {
            let i = r
                .first()
                .and_then(Option::as_deref)
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|s| s.parse::<usize>().ok());
            if let Some(i) = i
                && let Some(slot) = out.get_mut(start + i)
            {
                *slot = value_of(f, r.get(1).and_then(Option::as_deref));
            }
        }
        start = end;
    }
    Ok(out)
}

/// The input documents with each operator's answer in a hidden field,
/// the stage that reads them, and the hidden fields' names.
///
/// # Errors
/// A malformed operator (9); an input that isn't a string (14); the
/// answers' errors.
pub async fn prepare(
    sql: &mut dyn SqlRunner,
    db: &str,
    mut docs: Vec<Document>,
    stage: &Document,
    now: DateTime,
) -> Result<(Vec<Document>, Document, Vec<String>), MongoError> {
    let mut ops = Vec::new();
    let rewritten = match rewrite(&Bson::Document(stage.clone()), &mut ops)? {
        Bson::Document(d) => d,
        _ => return Err(MongoError::new(code::INTERNAL_ERROR, "a stage")),
    };
    let mut hidden = Vec::with_capacity(ops.len());
    for (k, op) in ops.iter().enumerate() {
        let name = format!("{HIDDEN}{k}");
        let mut slots: Vec<Option<usize>> = Vec::with_capacity(docs.len());
        let mut index: HashMap<String, usize> = HashMap::new();
        let mut distinct: Vec<String> = Vec::new();
        for d in &docs {
            let v = agg::eval(&op.input, &Vars::new(d, now))?;
            slots.push(match v {
                Bson::Null | Bson::Undefined => None,
                Bson::String(s) => Some(*index.entry(s.clone()).or_insert_with(|| {
                    distinct.push(s);
                    distinct.len() - 1
                })),
                other => {
                    return Err(MongoError::new(
                        code::TYPE_MISMATCH,
                        format!(
                            "{} needs a string input, not {}",
                            OPS.iter().find(|(_, s)| *s == op.f).map_or("", |x| x.0),
                            crate::update::type_name(&other)
                        ),
                    ));
                }
            });
        }
        let refs: Vec<&str> = distinct.iter().map(String::as_str).collect();
        let answers = if refs.is_empty() {
            Vec::new()
        } else {
            answer(sql, db, op.f, &op.rest, &refs).await?
        };
        for (d, s) in docs.iter_mut().zip(slots) {
            let v = s.and_then(|i| answers.get(i).cloned()).unwrap_or(Bson::Null);
            d.insert(name.clone(), v);
        }
        hidden.push(name);
    }
    Ok((docs, rewritten, hidden))
}

/// Removes the hidden fields from `docs`.
pub fn strip(docs: &mut [Document], hidden: &[String]) {
    for d in docs {
        for h in hidden {
            d.remove(h);
        }
    }
}

/// `$vectorSearch`'s `queryVector: {$aiEmbed: "<text>"}` answered: the
/// pipeline with the vector in its place; `None` when it has none.
///
/// # Errors
/// As [`answer`]; 9 for an input that isn't a string literal.
pub async fn query_vector(
    sql: &mut dyn SqlRunner,
    db: &str,
    pipeline: &[Document],
) -> Result<Option<Vec<Document>>, MongoError> {
    let Some(vs) = pipeline
        .first()
        .and_then(|s| s.get_document("$vectorSearch").ok())
    else {
        return Ok(None);
    };
    let Some(Bson::Document(q)) = vs.get("queryVector") else {
        return Ok(None);
    };
    let Some(arg) = q.get("$aiEmbed").filter(|_| q.len() == 1) else {
        return Ok(None);
    };
    let op = parse_op("$aiEmbed", "AI_EMBED", arg)?;
    let Bson::String(text) = &op.input else {
        return Err(bad("$vectorSearch's queryVector takes {$aiEmbed: \"<text>\"}"));
    };
    let v = answer(sql, db, op.f, &op.rest, &[text.as_str()]).await?;
    let mut first = pipeline[0].clone();
    let mut vs = vs.clone();
    vs.insert("queryVector", v.into_iter().next().unwrap_or(Bson::Null));
    first.insert("$vectorSearch", vs);
    let mut out = pipeline.to_vec();
    out[0] = first;
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use async_trait::async_trait;
    use bson::doc;

    use super::*;
    use crate::engine::{Done, Rows, SqlError};

    /// Answers each statement as the gateway would: reads the inputs
    /// back out of the hex literals and labels them by length.
    #[derive(Default)]
    struct Fake {
        seen: Vec<String>,
    }

    fn unhex(s: &str) -> String {
        let b: Vec<u8> = (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect();
        String::from_utf8(b).unwrap()
    }

    #[async_trait]
    impl SqlRunner for Fake {
        async fn query(&mut self, sql: &str) -> Result<Rows, SqlError> {
            self.seen.push(sql.to_owned());
            let embed = sql.contains("AI_EMBED(");
            let from = sql.find(" FROM (").unwrap();
            let mut rows = Vec::new();
            for (i, part) in sql[from..].split(" UNION ALL ").enumerate() {
                let h = part.split("X'").nth(1).unwrap().split('\'').next().unwrap();
                let v = unhex(h);
                let ans: Vec<u8> = if embed {
                    [1.0f32, 0.5].iter().flat_map(|x| x.to_le_bytes()).collect()
                } else if v.len() > 5 {
                    b"long".to_vec()
                } else {
                    b"short".to_vec()
                };
                rows.push(vec![Some(i.to_string().into_bytes()), Some(ans)]);
            }
            Ok(rows)
        }
        async fn exec(&mut self, _sql: &str) -> Result<Done, SqlError> {
            Ok(Done::default())
        }
    }

    #[test]
    fn statements_carry_values_as_hex() {
        let s = statement("media", "AI_CLASSIFY", &["a".into(), "b'c".into()], &["x", "it's"]);
        assert_eq!(
            s,
            "/* sprig-ai-db:media */ SELECT i, AI_CLASSIFY(v, _utf8mb4 X'61', _utf8mb4 X'622763') \
             FROM (SELECT 0 AS i, _utf8mb4 X'78' AS v UNION ALL SELECT 1, _utf8mb4 X'69742773') AS d"
        );
        assert!(has_ai(&Bson::Document(doc! {"a": [{"$aiEmbed": "$t"}]})));
        assert!(!has_ai(&Bson::Document(doc! {"a": "$aiEmbed"})));
    }

    #[tokio::test]
    async fn a_stage_gets_hidden_answers() {
        let docs = vec![
            doc! {"_id": 1, "body": "hi"},
            doc! {"_id": 2, "body": "a longer text"},
            doc! {"_id": 3},
            doc! {"_id": 4, "body": "hi"},
        ];
        let stage = doc! {"$addFields": {
            "kind": {"$aiClassify": {"input": "$body", "labels": ["short", "long"]}},
            "loud": {"$toUpper": {"$aiSummarize": "$body"}},
        }};
        let mut sql = Fake::default();
        let (docs, st, hidden) = prepare(&mut sql, "media", docs, &stage, DateTime::now())
            .await
            .unwrap();
        assert_eq!(hidden, ["__sprig_ai_0", "__sprig_ai_1"]);
        assert_eq!(sql.seen.len(), 2, "one statement per operator");
        assert!(sql.seen[0].contains("AI_CLASSIFY(v, _utf8mb4 X'73686F7274'"));
        assert_eq!(sql.seen[0].matches("UNION ALL").count(), 1, "distinct inputs only");
        let lim = agg::AggLimits {
            stage_bytes: 1 << 20,
            now: DateTime::now(),
        };
        let mut out = agg::run_stage(docs, &st, &lim).unwrap();
        strip(&mut out, &hidden);
        assert_eq!(out[0].get_str("kind").unwrap(), "short");
        assert_eq!(out[1].get_str("kind").unwrap(), "long");
        assert_eq!(out[1].get_str("loud").unwrap(), "LONG");
        assert_eq!(out[2].get("kind"), Some(&Bson::Null));
        assert!(!out[0].contains_key("__sprig_ai_0"));
        // A number isn't text.
        let e = prepare(
            &mut sql,
            "media",
            vec![doc! {"body": 3}],
            &doc! {"$set": {"e": {"$aiEmbed": "$body"}}},
            DateTime::now(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code, code::TYPE_MISMATCH);
        // Classify needs labels.
        let e = prepare(
            &mut sql,
            "media",
            vec![],
            &doc! {"$set": {"e": {"$aiClassify": "$body"}}},
            DateTime::now(),
        )
        .await
        .unwrap_err();
        assert_eq!(e.code, code::FAILED_TO_PARSE);
    }

    #[tokio::test]
    async fn query_vectors_and_forms() {
        let mut sql = Fake::default();
        let p = vec![doc! {"$vectorSearch": {"index": "v", "path": "e", "queryVector": {"$aiEmbed": "red shoes"}, "limit": 3}}];
        let out = query_vector(&mut sql, "shop", &p).await.unwrap().unwrap();
        let q = out[0]
            .get_document("$vectorSearch")
            .unwrap()
            .get_array("queryVector")
            .unwrap();
        assert_eq!(q, &vec![Bson::Double(1.0), Bson::Double(0.5)]);
        let op = parse_op(
            "$aiSummarize",
            "AI_SUMMARIZE",
            &Bson::Document(doc! {"input": "$b", "model": "x/y"}),
        )
        .unwrap();
        assert_eq!(op.rest, ["100", "x/y"]);
        let op = parse_op(
            "$aiExtract",
            "AI_EXTRACT",
            &Bson::Document(doc! {"input": "$b", "schema": {"type": "object"}}),
        )
        .unwrap();
        assert_eq!(op.rest, [r#"{"type":"object"}"#]);
        let op = parse_op(
            "$aiComplete",
            "AI_COMPLETE",
            &Bson::Document(doc! {"prompt": {"$concat": ["Say ", "$w"]}}),
        )
        .unwrap();
        assert!(op.rest.is_empty());
        assert!(op.input.as_document().unwrap().contains_key("$concat"));
        assert_eq!(value_of("AI_EXTRACT", Some(br#"{"a":1}"#)), Bson::Document(doc! {"a": 1}));
    }
}
