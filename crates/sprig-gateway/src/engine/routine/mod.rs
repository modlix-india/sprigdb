//! SQL stored procedures and functions on engine databases.
//!
//! A routine is a catalog object (kind `procedure` or `function`) whose
//! definition text is what `SHOW CREATE PROCEDURE | FUNCTION` prints, after
//! one line of creation context (`sql_mode`, character sets, times). The
//! text is parsed ([`parse`]) and compiled ([`code`]) once per catalog
//! load into a [`code::Program`]: bytecode over SQL fragments, run by the
//! session (`exec/engine_seam/routine.rs`). Procedures, functions and,
//! later, triggers and events share that one runtime.

pub(crate) mod code;
pub(crate) mod ddl;
pub(crate) mod parse;
pub(crate) mod subst;
pub(crate) mod trigger;

use std::sync::Arc;

use sprig_common::SprigError;
use sprig_common::ids::{DbId, ObjectId};

pub(crate) use parse::Kind;

/// The refusal for a form the engine doesn't run yet.
pub(crate) fn not_on_engine(what: &str) -> SprigError {
    super::not_on_engine(what)
}

/// What a routine recorded when it was created.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Context {
    /// The session's `sql_mode`.
    pub sql_mode: String,
    /// `character_set_client`.
    pub charset_client: String,
    /// `collation_connection`.
    pub collation_connection: String,
    /// The database's default collation.
    pub db_collation: String,
    /// When it was created (`YYYY-MM-DD hh:mm:ss`).
    pub created: String,
    /// When it was last changed.
    pub modified: String,
    /// The definer: (user, host).
    pub definer: (String, String),
    /// A trigger's place among its table's triggers of the same timing
    /// and event (smaller first).
    #[serde(default)]
    pub order: i64,
}

/// The first line of a stored definition.
const HEAD: &str = "-- sprig-routine ";

/// One routine of an engine database, as the catalog has it.
#[derive(Debug)]
pub(crate) struct Routine {
    /// Its database.
    pub db: DbId,
    /// Its name as created.
    pub name: String,
    /// Procedure or function.
    pub kind: Kind,
    /// The creation context.
    pub cx: Context,
    /// The text `SHOW CREATE` prints.
    pub show: String,
    /// The parsed definition and its compiled code, or why the engine can't
    /// run it.
    pub def: std::result::Result<(parse::Create, Arc<code::Program>), String>,
}

/// The catalog text of a routine: the context line, then the `SHOW CREATE`
/// text.
pub(crate) fn stored(cx: &Context, show: &str) -> String {
    let head = serde_json::to_string(cx).unwrap_or_default();
    format!("{HEAD}{head}\n{show}")
}

/// Reads a routine's catalog text.
pub(crate) fn load(_object: ObjectId, db: DbId, db_name: &str, name: &str, text: &str) -> Routine {
    let (cx, show) = split_stored(text);
    let def = parse::parse_create(show)
        .and_then(|c| {
            let p = code::compile(&c, db_name)?;
            Ok((c, Arc::new(p)))
        })
        .map_err(|e| e.message);
    let kind = match &def {
        Ok((c, _)) => c.kind,
        Err(_) if show.contains(" FUNCTION ") => Kind::Function,
        Err(_) => Kind::Procedure,
    };
    Routine {
        db,
        name: name.to_owned(),
        kind,
        cx,
        show: show.to_owned(),
        def,
    }
}

/// A stored definition's context and `SHOW CREATE` text.
pub(crate) fn split_stored(text: &str) -> (Context, &str) {
    match text.strip_prefix(HEAD).and_then(|t| t.split_once('\n')) {
        Some((head, show)) => (serde_json::from_str(head).unwrap_or_default(), show),
        None => (Context::default(), text),
    }
}

/// `'text'` quoted as `SHOW CREATE` quotes a comment.
pub(crate) fn sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// `` `name` `` quoted.
pub(crate) fn ident(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// The `SHOW CREATE` text of a routine: the header as the servers print it
/// (`returns`: the return type as printed), then the body as written.
pub(crate) fn show_text(c: &parse::Create, definer: &(String, String), returns: &str) -> String {
    let mut s = format!(
        "CREATE DEFINER={}@{} {} {}({})",
        ident(&definer.0),
        ident(&definer.1),
        c.kind.word(),
        ident(&c.name),
        c.params_text
    );
    if c.kind == Kind::Function {
        s.push_str(" RETURNS ");
        s.push_str(returns);
    }
    let ch = &c.chars;
    if let Some(a) = ch.access
        && a != parse::Access::Contains
    {
        s.push_str("\n    ");
        s.push_str(a.words());
    }
    if ch.deterministic == Some(true) {
        s.push_str("\n    DETERMINISTIC");
    }
    if ch.invoker == Some(true) {
        s.push_str("\n    SQL SECURITY INVOKER");
    }
    if let Some(cm) = &ch.comment {
        s.push_str("\n    COMMENT ");
        s.push_str(&sql_string(cm));
    }
    s.push('\n');
    s.push_str(&c.body_text);
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_routine_reads_back() {
        let c = parse::parse_create(
            "CREATE PROCEDURE p1(IN a INT) COMMENT 'it''s' BEGIN SELECT a; END",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let show = show_text(&c, &("root".into(), "%".into()), "");
        assert_eq!(
            show,
            "CREATE DEFINER=`root`@`%` PROCEDURE `p1`(IN a INT)\n    COMMENT 'it''s'\nBEGIN SELECT a; END"
        );
        let cx = Context {
            sql_mode: "STRICT_TRANS_TABLES".into(),
            definer: ("root".into(), "%".into()),
            ..Context::default()
        };
        let r = load(ObjectId(7), DbId(3), "d", "p1", &stored(&cx, &show));
        assert_eq!(r.cx, cx);
        assert_eq!(r.show, show);
        assert!(r.def.is_ok(), "{:?}", r.def.err());
        assert_eq!(r.kind, Kind::Procedure);
    }
}
