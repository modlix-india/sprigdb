//! `ALTER TABLE`, `CREATE INDEX` and `DROP INDEX` applied to a definition.
//!
//! The servers apply an `ALTER TABLE`'s clauses in phases, not in the
//! order written (observed): every `DROP` first; then column changes in
//! place (`MODIFY`, `CHANGE`, `RENAME COLUMN`, `ALTER COLUMN`); then new
//! columns at the end; then the moves (`FIRST`, `AFTER x`) of new and
//! changed columns in the order written, so an `AFTER` may name a column
//! the same statement adds; then index renames and new keys and
//! constraints in the order written.

use super::expr::{self, E};
use super::lex::{T, Tok};
use super::{
    Col, Ctx, Def, Default, Fail, Key, KeyKind, P, Raw, RawCol, add_check, add_column, add_fk,
    add_key, column, element, err, key_body, refuse, settle, table_options,
};

/// One clause, as read.
#[allow(clippy::large_enum_variant)] // a few per statement
enum Clause {
    DropColumn(String, bool),
    DropKey(String, bool),
    DropPrimary,
    DropFk(String, bool),
    DropConstraint(String, bool),
    Change {
        old: String,
        col: RawCol,
        if_exists: bool,
    },
    RenameColumn(String, String),
    SetDefault(String, Default),
    DropDefault(String),
    SetVisible(String, bool),
    AddColumns(Vec<RawCol>, bool),
    RenameKey(String, String),
    Add(Raw),
    KeyIgnored(String, bool),
    Options(super::Opts),
    Convert(String, Option<String>),
    /// A partitioning change.
    Partition(super::partition::Change),
    Nothing,
}

/// Applies the statement in `t` to `def`.
pub(super) fn apply(def: &Def, t: &[Tok], cx: &Ctx) -> Result<Def, Fail> {
    let mut p = P {
        t,
        i: 0,
        mysql: cx.mysql,
    };
    let clauses = if p.is("CREATE") {
        create_index(&mut p, &def.cs.0)?
    } else if p.is("DROP") {
        drop_index(&mut p)?
    } else {
        read_alter(&mut p, &def.cs.0)?
    };
    let mut d = def.clone();
    run(&mut d, clauses, cx)?;
    Ok(d)
}

fn skip_algorithm_lock(p: &mut P<'_>) -> Result<bool, Fail> {
    if p.eat("ALGORITHM") || p.eat("LOCK") {
        p.eat_op("=");
        match p.peek().map(|x| &x.t) {
            Some(T::Word(_)) => {
                p.i += 1;
                Ok(true)
            }
            _ => Err(Fail::Syntax(p.i)),
        }
    } else {
        Ok(false)
    }
}

fn create_index(p: &mut P<'_>, cs: &str) -> Result<Vec<Clause>, Fail> {
    p.want("CREATE")?;
    if p.eat("OR") {
        p.want("REPLACE")?;
    }
    let kind = if p.eat("UNIQUE") {
        KeyKind::Unique
    } else if p.eat("FULLTEXT") {
        KeyKind::Fulltext
    } else if p.eat("SPATIAL") {
        KeyKind::Spatial
    } else {
        KeyKind::Plain
    };
    p.want("INDEX")?;
    if p.eat("IF") {
        p.want("NOT")?;
        p.want("EXISTS")?;
    }
    let name = p.ident()?;
    let using = if p.eat("USING") || p.eat("TYPE") {
        Some(p.ident()?.to_ascii_uppercase())
    } else {
        None
    };
    p.want("ON")?;
    p.qualified()?;
    let mut k = key_body(p, kind, false)?;
    k.name = name;
    if k.using.is_none() {
        k.using = using;
    }
    while skip_algorithm_lock(p)? {}
    super::end(p)?;
    let _ = cs;
    Ok(vec![Clause::Add(Raw {
        keys: vec![k],
        ..Raw::default()
    })])
}

fn drop_index(p: &mut P<'_>) -> Result<Vec<Clause>, Fail> {
    p.want("DROP")?;
    p.want("INDEX")?;
    let ie = if p.eat("IF") {
        p.want("EXISTS")?;
        true
    } else {
        false
    };
    let name = p.ident()?;
    p.want("ON")?;
    p.qualified()?;
    while skip_algorithm_lock(p)? {}
    super::end(p)?;
    Ok(vec![if name.eq_ignore_ascii_case("PRIMARY") {
        Clause::DropPrimary
    } else {
        Clause::DropKey(name, ie)
    }])
}

fn if_exists(p: &mut P<'_>) -> Result<bool, Fail> {
    if p.eat("IF") {
        p.want("EXISTS")?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn if_not_exists(p: &mut P<'_>) -> Result<bool, Fail> {
    if p.eat("IF") {
        p.want("NOT")?;
        p.want("EXISTS")?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// `FIRST` / `AFTER x` after a column definition.
#[allow(clippy::option_option)] // no place, `FIRST`, or `AFTER x`
fn place(p: &mut P<'_>) -> Result<Option<Option<String>>, Fail> {
    if p.eat("FIRST") {
        Ok(Some(None))
    } else if p.eat("AFTER") {
        Ok(Some(Some(p.ident()?)))
    } else {
        Ok(None)
    }
}

#[allow(clippy::too_many_lines)] // one arm per clause
fn read_alter(p: &mut P<'_>, cs: &str) -> Result<Vec<Clause>, Fail> {
    p.want("ALTER")?;
    p.eat("ONLINE");
    p.eat("IGNORE");
    p.want("TABLE")?;
    if_exists(p)?;
    p.qualified()?;
    let mut out = Vec::new();
    if super::end(p).is_ok() {
        return Ok(out);
    }
    loop {
        if let Some(c) = super::partition::read_change(p, cs)? {
            out.push(Clause::Partition(c));
            // `PARTITION BY ...` ends the statement (or comes last).
            if !p.op(",") {
                super::end(p)?;
                return Ok(out);
            }
        } else if skip_algorithm_lock(p)? {
            out.push(Clause::Nothing);
        } else if p.eat("ADD") {
            if p.eat("COLUMN") {
                let ine = if_not_exists(p)?;
                if p.eat_op("(") {
                    let mut cols = vec![column(p, cs)?];
                    while p.eat_op(",") {
                        cols.push(column(p, cs)?);
                    }
                    p.want_op(")")?;
                    out.push(Clause::AddColumns(cols, ine));
                } else {
                    let mut c = column(p, cs)?;
                    c.place = place(p)?;
                    out.push(Clause::AddColumns(vec![c], ine));
                }
            } else if p.is("INDEX")
                || p.is("KEY")
                || p.is("UNIQUE")
                || p.is("PRIMARY")
                || p.is("FOREIGN")
                || p.is("CHECK")
                || p.is("CONSTRAINT")
                || p.is("FULLTEXT")
                || p.is("SPATIAL")
            {
                // `ADD INDEX IF NOT EXISTS name`.
                let save = p.i;
                if (p.is("INDEX") || p.is("KEY")) && p.is_at(1, "IF") {
                    p.i += 1;
                    if_not_exists(p)?;
                    let mut raw = Raw::default();
                    raw.keys.push(key_body(p, KeyKind::Plain, true)?);
                    out.push(Clause::Add(raw));
                } else {
                    p.i = save;
                    let mut raw = Raw::default();
                    element(p, &mut raw, cs)?;
                    out.push(Clause::Add(raw));
                }
            } else if p.op("(") {
                p.i += 1;
                let mut cols = Vec::new();
                let mut raw = Raw::default();
                loop {
                    if p.is("INDEX")
                        || p.is("KEY")
                        || p.is("UNIQUE")
                        || p.is("PRIMARY")
                        || p.is("FOREIGN")
                        || p.is("CHECK")
                        || p.is("CONSTRAINT")
                    {
                        element(p, &mut raw, cs)?;
                    } else {
                        cols.push(column(p, cs)?);
                    }
                    if p.eat_op(",") {
                        continue;
                    }
                    p.want_op(")")?;
                    break;
                }
                out.push(Clause::AddColumns(cols, false));
                out.push(Clause::Add(raw));
            } else {
                let ine = if_not_exists(p)?;
                let mut c = column(p, cs)?;
                c.place = place(p)?;
                out.push(Clause::AddColumns(vec![c], ine));
            }
        } else if p.eat("DROP") {
            if p.eat("PRIMARY") {
                p.want("KEY")?;
                out.push(Clause::DropPrimary);
            } else if p.eat("INDEX") || p.eat("KEY") {
                let ie = if_exists(p)?;
                let n = p.ident()?;
                out.push(if n.eq_ignore_ascii_case("PRIMARY") {
                    Clause::DropPrimary
                } else {
                    Clause::DropKey(n, ie)
                });
            } else if p.eat("FOREIGN") {
                p.want("KEY")?;
                let ie = if_exists(p)?;
                out.push(Clause::DropFk(p.ident()?, ie));
            } else if p.eat("CONSTRAINT") {
                let ie = if_exists(p)?;
                out.push(Clause::DropConstraint(p.ident()?, ie));
            } else {
                p.eat("COLUMN");
                let ie = if_exists(p)?;
                out.push(Clause::DropColumn(p.ident()?, ie));
                if p.eat("RESTRICT") || p.eat("CASCADE") {}
            }
        } else if p.eat("MODIFY") {
            p.eat("COLUMN");
            let ie = if_exists(p)?;
            let mut c = column(p, cs)?;
            c.place = place(p)?;
            out.push(Clause::Change {
                old: c.col.name.clone(),
                col: c,
                if_exists: ie,
            });
        } else if p.eat("CHANGE") {
            p.eat("COLUMN");
            let ie = if_exists(p)?;
            let old = p.ident()?;
            let mut c = column(p, cs)?;
            c.place = place(p)?;
            out.push(Clause::Change {
                old,
                col: c,
                if_exists: ie,
            });
        } else if p.eat("RENAME") {
            if p.eat("COLUMN") {
                let a = p.ident()?;
                p.want("TO")?;
                out.push(Clause::RenameColumn(a, p.ident()?));
            } else if p.eat("INDEX") || p.eat("KEY") {
                let a = p.ident()?;
                p.want("TO")?;
                out.push(Clause::RenameKey(a, p.ident()?));
            } else {
                return Err(refuse(
                    "ALTER TABLE that renames the table among other changes (rename it in a statement of its own)",
                ));
            }
        } else if p.eat("ALTER") {
            if p.eat("INDEX") || p.eat("KEY") {
                let n = p.ident()?;
                if p.eat("IGNORED") || p.eat("INVISIBLE") {
                    out.push(Clause::KeyIgnored(n, true));
                } else if p.eat("NOT") {
                    p.want("IGNORED")?;
                    out.push(Clause::KeyIgnored(n, false));
                } else {
                    p.want("VISIBLE")?;
                    out.push(Clause::KeyIgnored(n, false));
                }
            } else {
                p.eat("COLUMN");
                p.eat("IF");
                if p.is("EXISTS") {
                    p.i += 1;
                }
                let n = p.ident()?;
                if p.eat("SET") {
                    if p.eat("DEFAULT") {
                        let d = super::default_value(p, cs)?;
                        out.push(Clause::SetDefault(n, d));
                    } else if p.eat("VISIBLE") {
                        out.push(Clause::SetVisible(n, true));
                    } else {
                        p.want("INVISIBLE")?;
                        out.push(Clause::SetVisible(n, false));
                    }
                } else {
                    p.want("DROP")?;
                    p.want("DEFAULT")?;
                    out.push(Clause::DropDefault(n));
                }
            }
        } else if p.eat("CONVERT") {
            p.want("TO")?;
            if !p.eat("CHARSET") {
                p.want("CHARACTER")?;
                p.want("SET")?;
            }
            let cs = p.ident()?;
            let co = if p.eat("COLLATE") {
                Some(p.ident()?)
            } else {
                None
            };
            out.push(Clause::Convert(cs, co));
        } else if p.eat("FORCE") {
            out.push(Clause::Nothing);
        } else if p.is("ENABLE") || p.is("DISABLE") {
            p.i += 1;
            p.want("KEYS")?;
            out.push(Clause::Nothing);
        } else if p.is("ORDER")
            || p.is("DISCARD")
            || p.is("IMPORT")
            || p.is("PARTITION")
            || p.is("REMOVE")
            || p.is("COALESCE")
            || p.is("REORGANIZE")
            || p.is("EXCHANGE")
            || p.is("ANALYZE")
            || p.is("CHECK") && !p.is_at(1, "(")
            || p.is("OPTIMIZE")
            || p.is("REBUILD")
            || p.is("REPAIR")
            || p.is("TRUNCATE")
            || p.is("WITH")
            || p.is("WITHOUT")
        {
            return Err(refuse("this ALTER TABLE clause"));
        } else {
            let before = p.i;
            let mut o = super::Opts::default();
            table_options(p, &mut o)?;
            if p.i == before {
                return Err(Fail::Syntax(p.i));
            }
            out.push(Clause::Options(o));
            // Options need no commas between them; one may follow.
            if !p.op(",") {
                super::end(p)?;
                return Ok(out);
            }
        }
        if p.eat_op(",") {
            continue;
        }
        super::end(p)?;
        return Ok(out);
    }
}

/// Columns of `e` named `c`.
fn reads(e: &E, c: &str) -> bool {
    let mut v = Vec::new();
    expr::columns(e, &mut v);
    v.iter().any(|x| x.eq_ignore_ascii_case(c))
}

#[allow(clippy::too_many_lines)] // the phases in order
fn run(d: &mut Def, clauses: Vec<Clause>, cx: &Ctx) -> Result<(), Fail> {
    let part_changes: Vec<super::partition::Change> = clauses
        .iter()
        .filter_map(|c| match c {
            Clause::Partition(ch) => Some(ch.clone()),
            _ => None,
        })
        .collect();
    // Phase 1: drops.
    let mut dropped_cols: Vec<String> = Vec::new();
    for c in &clauses {
        match c {
            Clause::DropColumn(n, ie) => match d.col(n) {
                Some(i) => {
                    dropped_cols.push(d.cols[i].name.clone());
                    d.cols.remove(i);
                }
                None if *ie => {}
                None => {
                    return Err(err(
                        1091,
                        "42000",
                        format!("Can't DROP COLUMN `{n}`; check that it exists"),
                    ));
                }
            },
            Clause::DropKey(n, ie) => match d.key(n) {
                Some(i) => {
                    d.keys.remove(i);
                }
                None if *ie => {}
                None => {
                    return Err(err(
                        1091,
                        "42000",
                        format!("Can't DROP INDEX `{n}`; check that it exists"),
                    ));
                }
            },
            Clause::DropPrimary => match d.keys.iter().position(|k| k.kind == KeyKind::Primary) {
                Some(i) => {
                    d.keys.remove(i);
                }
                None => {
                    return Err(err(
                        1091,
                        "42000",
                        "Can't DROP INDEX `PRIMARY`; check that it exists",
                    ));
                }
            },
            Clause::DropFk(n, ie) => {
                match d.fks.iter().position(|f| f.name.eq_ignore_ascii_case(n)) {
                    Some(i) => {
                        d.fks.remove(i);
                    }
                    None if *ie => {}
                    None => {
                        return Err(err(
                            1091,
                            "42000",
                            format!("Can't DROP FOREIGN KEY `{n}`; check that it exists"),
                        ));
                    }
                }
            }
            Clause::DropConstraint(n, ie) => {
                if let Some(i) = d.checks.iter().position(|c| c.name.eq_ignore_ascii_case(n)) {
                    d.checks.remove(i);
                } else if let Some(i) = d.fks.iter().position(|f| f.name.eq_ignore_ascii_case(n)) {
                    d.fks.remove(i);
                } else if let Some(i) = d
                    .keys
                    .iter()
                    .position(|k| k.kind == KeyKind::Unique && k.name.eq_ignore_ascii_case(n))
                {
                    d.keys.remove(i);
                } else if !*ie {
                    return Err(err(
                        1091,
                        "42000",
                        format!("Can't DROP CONSTRAINT `{n}`; check that it exists"),
                    ));
                }
            }
            _ => {}
        }
    }
    if d.cols.is_empty() {
        return Err(err(
            1090,
            "42000",
            "You can't delete all columns with ALTER TABLE; use DROP TABLE instead",
        ));
    }
    for gone in &dropped_cols {
        for c in &d.cols {
            if c.generated.as_ref().is_some_and(|(g, _)| reads(g, gone)) {
                return Err(err(
                    1054,
                    "42S22",
                    format!("Unknown column '{gone}' in 'GENERATED ALWAYS AS'"),
                ));
            }
            if c.check.as_ref().is_some_and(|ch| reads(ch, gone)) {
                return Err(err(
                    1054,
                    "42S22",
                    format!("Unknown column '{gone}' in 'CHECK'"),
                ));
            }
            if matches!(&c.default, Some(Default::Expr(e)) if reads(e, gone)) {
                return Err(err(
                    1054,
                    "42S22",
                    format!("Unknown column '{gone}' in 'DEFAULT'"),
                ));
            }
        }
        if let Some(f) = d
            .fks
            .iter()
            .find(|f| f.cols.iter().any(|c| c.eq_ignore_ascii_case(gone)))
        {
            return Err(err(
                1828,
                "HY000",
                format!(
                    "Cannot drop column '{gone}': needed in a foreign key constraint '{}'",
                    f.name
                ),
            ));
        }
        // A table check on only dropped columns goes with them.
        let mut keep = Vec::new();
        for ch in std::mem::take(&mut d.checks) {
            let mut used = Vec::new();
            expr::columns(&ch.expr, &mut used);
            let all_gone = !used.is_empty()
                && used
                    .iter()
                    .all(|u| dropped_cols.iter().any(|g| g.eq_ignore_ascii_case(u)));
            if all_gone {
                continue;
            }
            if used.iter().any(|u| u.eq_ignore_ascii_case(gone)) {
                return Err(err(
                    1054,
                    "42S22",
                    format!("Unknown column '{gone}' in 'CHECK'"),
                ));
            }
            keep.push(ch);
        }
        d.checks = keep;
        for k in &mut d.keys {
            k.parts.retain(|p| !p.col.eq_ignore_ascii_case(gone));
        }
        d.keys.retain(|k| !k.parts.is_empty());
    }
    // Phase 2: changes in place.
    let mut moves: Vec<(usize, String, Option<String>)> = Vec::new(); // (clause index, column, after)
    let mut new_keys: Vec<Key> = Vec::new();
    for (ci, c) in clauses.iter().enumerate() {
        match c {
            Clause::Change {
                old,
                col,
                if_exists,
            } => {
                let Some(i) = d.col(old) else {
                    if *if_exists {
                        continue;
                    }
                    return Err(err(
                        1054,
                        "42S22",
                        format!("Unknown column '{old}' in '{}'", d.name),
                    ));
                };
                let mut rc = col.clone();
                let newname = rc.col.name.clone();
                if !newname.eq_ignore_ascii_case(&d.cols[i].name) && d.col(&newname).is_some() {
                    return Err(err(
                        1060,
                        "42S21",
                        format!("Duplicate column name '{newname}'"),
                    ));
                }
                let oldname = d.cols[i].name.clone();
                // Put the new definition in place: resolve it as a column of
                // its own, then swap it in.
                let mut scratch = Def {
                    cols: Vec::new(),
                    keys: Vec::new(),
                    fks: Vec::new(),
                    checks: Vec::new(),
                    ..d.clone()
                };
                rc.place = None;
                let mut keys = Vec::new();
                let pos = rc.place.clone();
                add_column(&mut scratch, rc, &mut keys, None, cx.mysql)?;
                let _ = pos;
                let newcol = scratch.cols.remove(0);
                rename_everywhere(d, &oldname, &newcol.name);
                d.cols[i] = newcol;
                for k in keys {
                    new_keys.push(k);
                }
                if let Some(pl) = &col.place {
                    moves.push((ci, d.cols[i].name.clone(), pl.clone()));
                }
            }
            Clause::RenameColumn(a, b) => {
                let Some(i) = d.col(a) else {
                    return Err(err(
                        1054,
                        "42S22",
                        format!("Unknown column '{a}' in '{}'", d.name),
                    ));
                };
                if !a.eq_ignore_ascii_case(b) && d.col(b).is_some() {
                    return Err(err(1060, "42S21", format!("Duplicate column name '{b}'")));
                }
                let old = d.cols[i].name.clone();
                rename_everywhere(d, &old, b);
                d.cols[i].name.clone_from(b);
            }
            Clause::SetDefault(n, dv) => {
                let Some(i) = d.col(n) else {
                    return Err(err(
                        1054,
                        "42S22",
                        format!("Unknown column '{n}' in '{}'", d.name),
                    ));
                };
                if d.cols[i].generated.is_some() {
                    return Err(err(1064, "42000", "generated column default"));
                }
                d.cols[i].default = Some(dv.clone());
            }
            Clause::DropDefault(n) => {
                let Some(i) = d.col(n) else {
                    return Err(err(
                        1054,
                        "42S22",
                        format!("Unknown column '{n}' in '{}'", d.name),
                    ));
                };
                d.cols[i].default = None;
            }
            Clause::SetVisible(n, v) => {
                let Some(i) = d.col(n) else {
                    return Err(err(
                        1054,
                        "42S22",
                        format!("Unknown column '{n}' in '{}'", d.name),
                    ));
                };
                d.cols[i].invisible = !v;
            }
            _ => {}
        }
    }
    // Phase 3: new columns at the end.
    for (ci, c) in clauses.iter().enumerate() {
        if let Clause::AddColumns(cols, ine) = c {
            for rc in cols {
                if *ine && d.col(&rc.col.name).is_some() {
                    continue;
                }
                let name = rc.col.name.clone();
                let pl = rc.place.clone();
                add_column(d, rc.clone(), &mut new_keys, None, cx.mysql)?;
                if let Some(pl) = pl {
                    moves.push((ci, name, pl));
                }
            }
        }
    }
    // Phase 4: moves, in the order written.
    moves.sort_by_key(|m| m.0);
    for (_, name, after) in moves {
        let Some(i) = d.col(&name) else { continue };
        let col: Col = d.cols.remove(i);
        let at = match &after {
            None => 0,
            Some(a) => match d.col(a) {
                Some(k) => k + 1,
                None => {
                    return Err(err(
                        1054,
                        "42S22",
                        format!("Unknown column '{a}' in '{}'", d.name),
                    ));
                }
            },
        };
        d.cols.insert(at, col);
    }
    // Phase 5: index renames, keys and constraints, options.
    for k in new_keys {
        add_key(d, k)?;
    }
    for c in clauses {
        match c {
            Clause::RenameKey(a, b) => {
                let Some(i) = d.key(&a) else {
                    return Err(err(
                        1176,
                        "42000",
                        format!("Key '{a}' doesn't exist in table '{}'", d.name),
                    ));
                };
                if !a.eq_ignore_ascii_case(&b) && d.key(&b).is_some() {
                    return Err(err(1061, "42000", format!("Duplicate key name '{b}'")));
                }
                d.keys[i].name = b;
            }
            Clause::KeyIgnored(n, ig) => {
                let Some(i) = d.key(&n) else {
                    return Err(err(
                        1176,
                        "42000",
                        format!("Key '{n}' doesn't exist in table '{}'", d.name),
                    ));
                };
                d.keys[i].ignored = ig;
            }
            Clause::Add(raw) => {
                for mut k in raw.keys {
                    // A plain or unique index a user's statement builds
                    // online (`engine::ddl::USER_ONLINE`): published
                    // ignored, made public once its entries are written. A
                    // unique key of a table without a primary key may become
                    // the table's key, so it takes the usual path.
                    let keyed = d.keys.iter().any(|x| x.kind == KeyKind::Primary);
                    let online = (k.kind == KeyKind::Plain || (k.kind == KeyKind::Unique && keyed))
                        && !k.ignored
                        && crate::engine::ddl::USER_ONLINE.try_with(|_| ()).is_ok();
                    if online {
                        k.ignored = true;
                    }
                    add_key(d, k)?;
                    if online && let Some(name) = d.keys.last().map(|k| k.name.clone()) {
                        let _ = crate::engine::ddl::USER_ONLINE.try_with(|v| {
                            v.lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push(name);
                        });
                    }
                }
                for f in raw.fks {
                    add_fk(d, f)?;
                }
                for (n, e) in raw.checks {
                    add_check(d, n, e)?;
                }
            }
            Clause::Options(o) => {
                if let Some(e) = &o.engine
                    && !e.eq_ignore_ascii_case("InnoDB")
                {
                    return Err(err(1286, "42000", format!("Unknown storage engine '{e}'")));
                }
                if o.charset.is_some() || o.collate.is_some() {
                    let spec = super::types::CsSpec {
                        charset: o.charset.clone(),
                        collate: o.collate.clone(),
                        ..super::types::CsSpec::default()
                    };
                    d.cs = super::types::resolve_cs(&spec, (&d.cs.0, &d.cs.1), cx.mysql)?;
                }
                if let Some(a) = o.auto_increment {
                    d.auto_increment = Some(a);
                }
                if o.comment.is_some() {
                    d.comment = o.comment;
                }
                if o.row_format.is_some() {
                    d.row_format = o.row_format;
                }
                if o.key_block_size.is_some() {
                    d.key_block_size = o.key_block_size;
                }
                if o.stats_persistent.is_some() {
                    d.stats_persistent = o.stats_persistent;
                }
            }
            Clause::Convert(cs, co) => {
                let spec = super::types::CsSpec {
                    charset: Some(cs),
                    collate: co,
                    ..super::types::CsSpec::default()
                };
                let to = super::types::resolve_cs(&spec, (&d.cs.0, &d.cs.1), cx.mysql)?;
                for c in &mut d.cols {
                    if c.ty.cs.is_some() && !c.ty.json {
                        c.ty.cs = Some(to.clone());
                    }
                }
                d.cs = to;
            }
            _ => {}
        }
    }
    settle(d, cx)?;
    // Partitioning changes last, against the table as the statement left
    // it; the partitioning is checked again in any case.
    for ch in &part_changes {
        super::partition::apply(d, ch, cx.mysql)?;
    }
    if part_changes.is_empty()
        && let Some(s) = &d.partition
    {
        super::partition::check(s, d, cx.mysql)?;
    }
    Ok(())
}

/// A column renamed in the keys, foreign keys and expressions that name
/// it.
fn rename_everywhere(d: &mut Def, old: &str, new: &str) {
    if old == new {
        return;
    }
    for k in &mut d.keys {
        for p in &mut k.parts {
            if p.col.eq_ignore_ascii_case(old) {
                new.clone_into(&mut p.col);
            }
        }
    }
    for f in &mut d.fks {
        for c in &mut f.cols {
            if c.eq_ignore_ascii_case(old) {
                new.clone_into(c);
            }
        }
    }
    for ch in &mut d.checks {
        expr::rename_column(&mut ch.expr, old, new);
    }
    for c in &mut d.cols {
        if let Some((g, _)) = &mut c.generated {
            expr::rename_column(g, old, new);
        }
        if let Some(ch) = &mut c.check {
            expr::rename_column(ch, old, new);
        }
        if let Some(Default::Expr(e)) = &mut c.default {
            expr::rename_column(e, old, new);
        }
    }
}
