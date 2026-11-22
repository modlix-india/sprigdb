//! The session's cache of bound text statements: a statement sent again
//! as the same text, in the same context, reuses the plan it was bound to
//! instead of being parsed and bound again.
//!
//! The key is everything binding reads besides the catalog's tables: the
//! text itself, the session's current database and the lexer's options
//! (`ANSI_QUOTES`, `NO_BACKSLASH_ESCAPES`, the persona). An entry is good
//! for one catalog version: any definition change, of any table, starts the
//! cache afresh. Row-level security needs nothing more: the tenant layer's
//! rewrite puts the session's predicates into the text, so another policy
//! is another text. Privileges and `WITH CHECK` conditions are checked at
//! every execution, after the cache, never kept in it.
//!
//! Prepared statements keep their plan in their own record already; this
//! cache is for the text protocol.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use sprig_common::ids::DbId;

use super::plan::Plan;

/// The most statements a session keeps.
const ENTRIES: usize = 256;

/// The longest text kept (longer statements are rarely sent twice).
const LONGEST: usize = 4096;

/// What binding a text depends on besides the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Key {
    text: Bytes,
    current: Option<DbId>,
    lex: (bool, bool, u32, bool),
}

/// One session's bound statements, for one catalog version.
#[derive(Debug, Default)]
pub(crate) struct TextPlans {
    version: u64,
    plans: HashMap<Key, Arc<Plan>>,
}

fn key(text: &Bytes, current: Option<DbId>, lex: sprig_sql::lex::LexOptions) -> Option<Key> {
    (text.len() <= LONGEST).then(|| Key {
        text: text.clone(),
        current,
        lex: (
            lex.ansi_quotes,
            lex.no_backslash_escapes,
            lex.version,
            lex.persona == sprig_proto_mysql::Persona::MySql84,
        ),
    })
}

impl TextPlans {
    /// The plan `text` was bound to under catalog version `version`.
    pub(crate) fn get(
        &self,
        version: u64,
        text: &Bytes,
        current: Option<DbId>,
        lex: sprig_sql::lex::LexOptions,
    ) -> Option<Arc<Plan>> {
        if self.version != version || self.plans.is_empty() {
            return None;
        }
        self.plans.get(&key(text, current, lex)?).cloned()
    }

    /// Keeps `plan`, bound from `text` under catalog version `version`.
    pub(crate) fn put(
        &mut self,
        version: u64,
        text: &Bytes,
        current: Option<DbId>,
        lex: sprig_sql::lex::LexOptions,
        plan: &Plan,
    ) {
        let Some(k) = key(text, current, lex) else {
            return;
        };
        if self.version != version || self.plans.len() >= ENTRIES {
            self.plans.clear();
            self.version = version;
        }
        self.plans.insert(k, Arc::new(plan.clone()));
    }
}

/// The most statement shapes a session keeps.
const SHAPES: usize = 256;

/// One statement shape the gateway's text passes leave alone (see
/// `sprig_sql::compat::fastpath`): its kind and table, in a context. A
/// statement of a shape seen to reach the engine unchanged skips the
/// passes. The context is what the passes read besides the text: the
/// session's current database and lexer options (the persona among them)
/// and the catalog's version, which every definition change moves (a
/// stream, typed table or inherited share of the same name included).
/// Row-level security and privileges are checked at every statement,
/// never kept here.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Shape {
    /// The statement's kind.
    pub kind: sprig_sql::compat::fastpath::FastKind,
    /// The engine table it names.
    pub object: sprig_common::ids::ObjectId,
    /// The session's current database.
    pub current: DbId,
    lex: (bool, bool, u32, bool),
}

impl Shape {
    /// The shape of a statement of `kind` on `object`.
    pub(crate) fn new(
        kind: sprig_sql::compat::fastpath::FastKind,
        object: sprig_common::ids::ObjectId,
        current: DbId,
        lex: sprig_sql::lex::LexOptions,
    ) -> Self {
        Self {
            kind,
            object,
            current,
            lex: (
                lex.ansi_quotes,
                lex.no_backslash_escapes,
                lex.version,
                lex.persona == sprig_proto_mysql::Persona::MySql84,
            ),
        }
    }
}

/// The shapes a session has seen reach the engine unchanged, for one
/// catalog version.
#[derive(Debug, Default)]
pub(crate) struct FastShapes {
    version: u64,
    shapes: std::collections::HashSet<Shape>,
}

impl FastShapes {
    /// Whether statements of `shape` skip the text passes under catalog
    /// version `version`.
    pub(crate) fn allows(&self, version: u64, shape: &Shape) -> bool {
        self.version == version && self.shapes.contains(shape)
    }

    /// Records that a statement of `shape` reached the engine unchanged
    /// under catalog version `version`.
    pub(crate) fn learn(&mut self, version: u64, shape: Shape) {
        if self.version != version || self.shapes.len() >= SHAPES {
            self.shapes.clear();
            self.version = version;
        }
        self.shapes.insert(shape);
    }

    /// Forgets `shape` (a statement of it was no longer the engine's).
    pub(crate) fn forget(&mut self, shape: &Shape) {
        self.shapes.remove(shape);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a_plan() -> Plan {
        // Any plan will do: the cache never looks inside.
        let def = crate::engine::table::parse(
            sprig_common::ids::ObjectId(1),
            1,
            DbId(1),
            "CREATE TABLE `t` (\n  `a` int(11) NOT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB",
        )
        .unwrap();
        Plan {
            op: crate::engine::plan::Op::Delete,
            table: def,
            db_name: Bytes::new(),
            alias: Bytes::new(),
            org_table: Bytes::new(),
            access: crate::engine::plan::Access::Full,
            filter: None,
            unkeyed: None,
            reverse: false,
            limit: None,
            offset: 0,
            params: 0,
            param_types: Vec::new(),
            read: Vec::new(),
            written: Vec::new(),
            catalog_version: 7,
            ordered: false,
            lock: None,
            check: None,
            fks: None,
        }
    }

    #[test]
    fn a_plan_serves_its_text_context_and_catalog_version_only() {
        let lex = sprig_sql::lex::LexOptions::for_persona(sprig_proto_mysql::Persona::MariaDb);
        let mut c = TextPlans::default();
        let t = Bytes::from_static(b"DELETE FROM t");
        c.put(7, &t, Some(DbId(1)), lex, &a_plan());
        assert!(c.get(7, &t, Some(DbId(1)), lex).is_some());
        // Another database, other lexer rules, another catalog version.
        assert!(c.get(7, &t, Some(DbId(2)), lex).is_none());
        let ansi = sprig_sql::lex::LexOptions {
            ansi_quotes: true,
            ..lex
        };
        assert!(c.get(7, &t, Some(DbId(1)), ansi).is_none());
        assert!(c.get(8, &t, Some(DbId(1)), lex).is_none());
        // A new version starts the cache afresh.
        c.put(
            8,
            &Bytes::from_static(b"DELETE FROM t WHERE a = 1"),
            Some(DbId(1)),
            lex,
            &a_plan(),
        );
        assert!(c.get(7, &t, Some(DbId(1)), lex).is_none());
        assert_eq!(c.plans.len(), 1);
        // Long texts aren't kept.
        let long = Bytes::from(vec![b' '; LONGEST + 1]);
        c.put(8, &long, None, lex, &a_plan());
        assert!(c.get(8, &long, None, lex).is_none());
    }

    #[test]
    fn a_shape_serves_its_context_and_catalog_version_only() {
        use sprig_common::ids::ObjectId;
        use sprig_sql::compat::fastpath::FastKind;
        let lex = sprig_sql::lex::LexOptions::for_persona(sprig_proto_mysql::Persona::MySql84);
        let maria = sprig_sql::lex::LexOptions::for_persona(sprig_proto_mysql::Persona::MariaDb);
        let mut s = FastShapes::default();
        let ins = Shape::new(FastKind::Insert, ObjectId(20), DbId(1), lex);
        assert!(!s.allows(3, &ins));
        s.learn(3, ins.clone());
        assert!(s.allows(3, &ins));
        assert!(!s.allows(4, &ins));
        assert!(!s.allows(3, &Shape::new(FastKind::Select, ObjectId(20), DbId(1), lex)));
        assert!(!s.allows(3, &Shape::new(FastKind::Insert, ObjectId(21), DbId(1), lex)));
        assert!(!s.allows(3, &Shape::new(FastKind::Insert, ObjectId(20), DbId(2), lex)));
        assert!(!s.allows(
            3,
            &Shape::new(FastKind::Insert, ObjectId(20), DbId(1), maria)
        ));
        s.forget(&ins);
        assert!(!s.allows(3, &ins));
        s.learn(3, ins.clone());
        s.learn(4, Shape::new(FastKind::Delete, ObjectId(20), DbId(1), lex));
        assert!(!s.allows(4, &ins));
    }
}
