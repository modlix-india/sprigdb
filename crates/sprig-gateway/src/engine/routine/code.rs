//! The bytecode a stored program compiles to, and the compiler: a flat list
//! of instructions with jumps, one slot per variable, handler and cursor
//! tables. Statements and expressions stay SQL text with their variable
//! uses found ([`super::subst`]); the runtime
//! (`exec/engine_seam/routine.rs`) runs them through the session.

use sprig_common::{Result, SprigError};

use super::parse::{Assign, Cond, Create, Decl, Event, Kind, Mode, Stmt, Target, TrigCreate};
use super::subst::{self, Text};

/// Where a value goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Tgt {
    /// A local variable or parameter.
    Slot(usize),
    /// `@name`.
    User(String),
}

/// A handler's condition, names resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HCond {
    /// An error number.
    Code(u16),
    /// A SQLSTATE.
    State(String),
    /// `SQLWARNING`.
    Warning,
    /// `NOT FOUND`.
    NotFound,
    /// `SQLEXCEPTION`.
    Exception,
}

/// One instruction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Op {
    /// The slot takes the expression's value (NULL without one),
    /// converted to its type.
    Set {
        /// The slot.
        slot: usize,
        /// The expression.
        expr: Option<Text>,
    },
    /// A SQL statement; `results`: its result set goes to the client.
    Sql {
        /// The statement.
        text: Text,
        /// It may send a result set.
        results: bool,
    },
    /// `SELECT ... INTO`.
    SelectInto {
        /// The query (without `INTO`).
        text: Text,
        /// The targets.
        into: Vec<Tgt>,
    },
    /// `CALL` of a procedure.
    Call {
        /// The database, when written.
        db: Option<String>,
        /// The procedure.
        name: String,
        /// The arguments.
        args: Vec<Text>,
        /// For each argument that is a variable, where an `OUT` value goes.
        outs: Vec<Option<Tgt>>,
    },
    /// Goes on at the instruction.
    Jump(usize),
    /// Goes on at `to` unless the condition is true; a `CONTINUE` handler
    /// of an error in the condition goes on at `cont`.
    Unless {
        /// The condition.
        cond: Text,
        /// Where to go when it isn't true.
        to: usize,
        /// After the whole statement.
        cont: usize,
    },
    /// `CASE` with no branch taken: 1339.
    NoCase {
        /// After the statement.
        cont: usize,
    },
    /// `RETURN expr`.
    Return(Text),
    /// A handler becomes active.
    HPush(usize),
    /// The last `n` handlers end.
    HPop(usize),
    /// `OPEN`.
    Open(usize),
    /// `FETCH`.
    Fetch {
        /// The cursor.
        cursor: usize,
        /// The targets.
        into: Vec<Tgt>,
    },
    /// `CLOSE`.
    Close(usize),
    /// Cursors closed without a word (their block ends or is entered).
    Reset(Vec<usize>),
    /// `SIGNAL` / `RESIGNAL`.
    Signal {
        /// `RESIGNAL`.
        resignal: bool,
        /// (code, SQLSTATE) of the condition, when named.
        cond: Option<(Option<u16>, Option<String>)>,
        /// Items.
        items: Vec<(String, Text)>,
    },
    /// `GET DIAGNOSTICS`.
    Diag {
        /// `STACKED`.
        stacked: bool,
        /// `CONDITION n`.
        condition: Option<Text>,
        /// (target, item).
        items: Vec<(Tgt, String)>,
    },
    /// A handler's body ended.
    HEnd(usize),
    /// The end of the main code.
    End,
}

/// A handler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Handler {
    /// `EXIT`.
    pub exit: bool,
    /// Its conditions.
    pub conds: Vec<HCond>,
    /// Its body's first instruction.
    pub start: usize,
    /// The block that declared it.
    pub block: usize,
    /// `EXIT`: where to go on (after the block's end).
    pub exit_to: usize,
    /// `EXIT`: the cursors of the block and the blocks inside it.
    pub close: Vec<usize>,
}

/// A variable's slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Var {
    /// Its name.
    pub name: String,
    /// Its type, as written.
    pub ty: String,
}

/// A compiled routine.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Program {
    /// Slots: the parameters first, in order.
    pub vars: Vec<Var>,
    /// Parameter directions, by slot.
    pub modes: Vec<Mode>,
    /// The code.
    pub ops: Vec<Op>,
    /// Handlers.
    pub handlers: Vec<Handler>,
    /// Cursors: (name, query).
    pub cursors: Vec<(String, Text)>,
    /// It may send result sets (a procedure).
    pub results: bool,
}

#[derive(Default)]
struct Scope {
    vars: Vec<(String, usize)>,
    conds: Vec<(String, Cond)>,
    cursors: Vec<(String, usize)>,
    handlers: usize,
    block: usize,
    /// Cursors of this block.
    own_cursors: Vec<usize>,
}

struct LabelInfo {
    name: String,
    /// The loop's continue target (`None`: a block).
    iterate: Option<usize>,
    /// Jumps to patch with the end.
    breaks: Vec<usize>,
    /// Scope depth at the label (blocks inside it are left by a jump).
    depth: usize,
    /// Handler-body depth when the label was opened.
    in_handler: usize,
}

struct C {
    kind: Kind,
    p: Program,
    scopes: Vec<Scope>,
    labels: Vec<LabelInfo>,
    blocks: usize,
    returns: bool,
    /// Handler bodies to compile after the main code: (handler, body,
    /// scopes visible to it).
    pending: Vec<(usize, Stmt, Vec<ScopeSnap>)>,
    handler_depth: usize,
    /// Cursors per block, including nested blocks (for EXIT).
    block_cursors: Vec<Vec<usize>>,
    block_parent: Vec<Option<usize>>,
    /// A trigger's timing (`BEFORE`), and the `NEW.x` / `OLD.x` names its
    /// body used that aren't the table's.
    before: bool,
    bad_rows: std::cell::RefCell<Vec<String>>,
}

/// `NEW.x` or `OLD.x`.
fn row_ref(n: &str) -> bool {
    let u = n.get(..4).map(str::to_ascii_uppercase);
    matches!(u.as_deref(), Some("NEW." | "OLD."))
}

#[derive(Clone)]
struct ScopeSnap {
    vars: Vec<(String, usize)>,
    conds: Vec<(String, Cond)>,
    cursors: Vec<(String, usize)>,
}

fn err(code: u16, state: [u8; 5], msg: String) -> SprigError {
    SprigError::standard(code, state, msg)
}

impl C {
    fn emit(&mut self, op: Op) -> usize {
        self.p.ops.push(op);
        self.p.ops.len() - 1
    }

    fn here(&self) -> usize {
        self.p.ops.len()
    }

    fn find_var(&self, name: &str) -> Option<usize> {
        self.scopes.iter().rev().find_map(|s| {
            s.vars
                .iter()
                .rev()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, slot)| *slot)
        })
    }

    fn find_cursor(&self, name: &str) -> Result<usize> {
        self.scopes
            .iter()
            .rev()
            .find_map(|s| {
                s.cursors
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case(name))
                    .map(|(_, c)| *c)
            })
            .ok_or_else(|| err(1324, *b"42000", format!("Undefined CURSOR: {name}")))
    }

    fn find_cond(&self, name: &str) -> Option<Cond> {
        self.scopes.iter().rev().find_map(|s| {
            s.conds
                .iter()
                .find(|(n, _)| n.eq_ignore_ascii_case(name))
                .map(|(_, c)| c.clone())
        })
    }

    fn text(&self, sql: &str, expr: bool) -> Text {
        subst::text(sql, expr, &|n| {
            let r = self.find_var(n);
            if r.is_none() && self.kind == Kind::Trigger && row_ref(n) {
                self.bad_rows.borrow_mut().push(n.to_owned());
            }
            r
        })
    }

    fn target(&self, t: &Target) -> Result<Tgt> {
        match t {
            Target::User(u) => Ok(Tgt::User(u.clone())),
            Target::Local(n) => self
                .find_var(n)
                .map(Tgt::Slot)
                .ok_or_else(|| err(1327, *b"42000", format!("Undeclared variable: {n}"))),
        }
    }

    fn hcond(&self, c: &Cond) -> Result<HCond> {
        Ok(match c {
            Cond::Code(n) => HCond::Code(*n),
            Cond::State(s) => HCond::State(s.clone()),
            Cond::Warning => HCond::Warning,
            Cond::NotFound => HCond::NotFound,
            Cond::Exception => HCond::Exception,
            Cond::Name(n) => match self.find_cond(n) {
                Some(Cond::Code(x)) => HCond::Code(x),
                Some(Cond::State(s)) => HCond::State(s),
                _ => return Err(err(1319, *b"42000", format!("Undefined CONDITION: {n}"))),
            },
        })
    }

    /// Instructions that leave the scopes above `depth`: their handlers
    /// end and their cursors close.
    fn leave_scopes(&mut self, depth: usize) {
        let mut pops = 0;
        let mut cursors = Vec::new();
        for s in &self.scopes[depth..] {
            pops += s.handlers;
            cursors.extend(s.own_cursors.iter().copied());
        }
        if pops > 0 {
            self.emit(Op::HPop(pops));
        }
        if !cursors.is_empty() {
            self.emit(Op::Reset(cursors));
        }
    }

    fn label_index(&self, name: &str) -> Option<usize> {
        self.labels
            .iter()
            .rposition(|l| l.name.eq_ignore_ascii_case(name) && l.in_handler == self.handler_depth)
    }

    fn open_label(&mut self, label: Option<&String>, iterate: Option<usize>) -> Result<bool> {
        let Some(l) = label else { return Ok(false) };
        if self.label_index(l).is_some() {
            return Err(err(1309, *b"42000", format!("Redefining label {l}")));
        }
        self.labels.push(LabelInfo {
            name: l.clone(),
            iterate,
            breaks: Vec::new(),
            depth: self.scopes.len(),
            in_handler: self.handler_depth,
        });
        Ok(true)
    }

    fn close_label(&mut self, opened: bool) {
        if !opened {
            return;
        }
        if let Some(l) = self.labels.pop() {
            let end = self.here();
            for b in l.breaks {
                if let Op::Jump(to) = &mut self.p.ops[b] {
                    *to = end;
                }
            }
        }
    }

    fn stmts(&mut self, body: &[Stmt]) -> Result<()> {
        for s in body {
            self.stmt(s)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_lines)] // one arm per statement
    fn stmt(&mut self, s: &Stmt) -> Result<()> {
        match s {
            Stmt::Block { label, decls, body } => self.block(label.as_ref(), decls, body),
            Stmt::Sql {
                text,
                into,
                results,
            } => {
                if let Some(into) = into {
                    let into = into
                        .iter()
                        .map(|t| self.target(t))
                        .collect::<Result<Vec<_>>>()?;
                    let text = self.text(text, false);
                    self.emit(Op::SelectInto { text, into });
                } else {
                    if *results && self.kind != Kind::Procedure {
                        return Err(err(
                            1415,
                            *b"0A000",
                            format!(
                                "Not allowed to return a result set from a {}",
                                self.kind.word().to_ascii_lowercase()
                            ),
                        ));
                    }
                    if *results {
                        self.p.results = true;
                    }
                    let text = self.text(text, false);
                    self.emit(Op::Sql {
                        text,
                        results: *results,
                    });
                }
                Ok(())
            }
            Stmt::Call { db, name, args } => {
                self.p.results |= self.kind == Kind::Procedure;
                let texts: Vec<Text> = args.iter().map(|a| self.text(a, true)).collect();
                let outs = args
                    .iter()
                    .map(|a| {
                        let a = a.trim();
                        if let Some(u) = a.strip_prefix('@')
                            && !u.starts_with('@')
                        {
                            return Some(Tgt::User(u.trim_matches('`').to_owned()));
                        }
                        self.find_var(a.trim_matches('`')).map(Tgt::Slot)
                    })
                    .collect();
                self.emit(Op::Call {
                    db: db.clone(),
                    name: name.clone(),
                    args: texts,
                    outs,
                });
                Ok(())
            }
            Stmt::Set(items) => {
                for a in items {
                    match a {
                        Assign::Local(n, e) => {
                            if self.kind == Kind::Trigger && row_ref(n) {
                                if n[..3].eq_ignore_ascii_case("OLD") {
                                    return Err(err(
                                        1362,
                                        *b"HY000",
                                        "Updating of OLD row is not allowed in trigger".into(),
                                    ));
                                }
                                if !self.before {
                                    return Err(err(
                                        1362,
                                        *b"HY000",
                                        "Updating of NEW row is not allowed in after trigger"
                                            .into(),
                                    ));
                                }
                                if self.find_var(n).is_none() {
                                    self.bad_rows.borrow_mut().push(n.clone());
                                    continue;
                                }
                            }
                            if let Some(slot) = self.find_var(n) {
                                let expr = Some(self.text(e, true));
                                self.emit(Op::Set { slot, expr });
                            } else {
                                // A system variable named without `@@`.
                                let text = self.text(&format!("SET {n} = {e}"), false);
                                self.emit(Op::Sql {
                                    text,
                                    results: false,
                                });
                            }
                        }
                        Assign::Other(t) => {
                            let text = self.text(&format!("SET {t}"), false);
                            self.emit(Op::Sql {
                                text,
                                results: false,
                            });
                        }
                    }
                }
                Ok(())
            }
            Stmt::If { arms, other } => {
                let mut ends = Vec::new();
                let mut unless = Vec::new();
                for (c, body) in arms {
                    let cond = self.text(c, true);
                    let u = self.emit(Op::Unless {
                        cond,
                        to: 0,
                        cont: 0,
                    });
                    unless.push(u);
                    self.stmts(body)?;
                    ends.push(self.emit(Op::Jump(0)));
                    let next = self.here();
                    if let Op::Unless { to, .. } = &mut self.p.ops[u] {
                        *to = next;
                    }
                }
                if let Some(o) = other {
                    self.stmts(o)?;
                }
                let end = self.here();
                for e in ends {
                    self.p.ops[e] = Op::Jump(end);
                }
                for u in unless {
                    if let Op::Unless { cont, .. } = &mut self.p.ops[u] {
                        *cont = end;
                    }
                }
                Ok(())
            }
            Stmt::Case {
                subject,
                arms,
                other,
            } => {
                // The simple form keeps its value in a hidden slot.
                let hidden = match subject {
                    Some(e) => {
                        let slot = self.p.vars.len();
                        self.p.vars.push(Var {
                            name: format!(" case{slot}"),
                            ty: String::new(),
                        });
                        let expr = Some(self.text(e, true));
                        self.emit(Op::Set { slot, expr });
                        Some(slot)
                    }
                    None => None,
                };
                let mut ends = Vec::new();
                let mut unless = Vec::new();
                for (v, body) in arms {
                    let cond = match hidden {
                        Some(slot) => {
                            let mut t = self.text(&format!("? = ({v})"), true);
                            // The `?` stands for the hidden slot.
                            t.refs.insert(
                                0,
                                subst::Ref {
                                    start: 0,
                                    end: 1,
                                    slot,
                                },
                            );
                            t
                        }
                        None => self.text(v, true),
                    };
                    let u = self.emit(Op::Unless {
                        cond,
                        to: 0,
                        cont: 0,
                    });
                    unless.push(u);
                    self.stmts(body)?;
                    ends.push(self.emit(Op::Jump(0)));
                    let next = self.here();
                    if let Op::Unless { to, .. } = &mut self.p.ops[u] {
                        *to = next;
                    }
                }
                let nocase = match other {
                    Some(o) => {
                        self.stmts(o)?;
                        None
                    }
                    None => Some(self.emit(Op::NoCase { cont: 0 })),
                };
                let end = self.here();
                for e in ends {
                    self.p.ops[e] = Op::Jump(end);
                }
                for u in unless {
                    if let Op::Unless { cont, .. } = &mut self.p.ops[u] {
                        *cont = end;
                    }
                }
                if let Some(n) = nocase {
                    self.p.ops[n] = Op::NoCase { cont: end };
                }
                Ok(())
            }
            Stmt::Loop { label, body } => {
                let start = self.here();
                let opened = self.open_label(label.as_ref(), Some(start))?;
                self.stmts(body)?;
                self.emit(Op::Jump(start));
                self.close_label(opened);
                Ok(())
            }
            Stmt::While { label, cond, body } => {
                let start = self.here();
                let opened = self.open_label(label.as_ref(), Some(start))?;
                let cond = self.text(cond, true);
                let u = self.emit(Op::Unless {
                    cond,
                    to: 0,
                    cont: 0,
                });
                self.stmts(body)?;
                self.emit(Op::Jump(start));
                let end = self.here();
                if let Op::Unless { to, cont, .. } = &mut self.p.ops[u] {
                    *to = end;
                    *cont = end;
                }
                self.close_label(opened);
                Ok(())
            }
            Stmt::Repeat { label, body, until } => {
                let start = self.here();
                let opened = self.open_label(label.as_ref(), Some(start))?;
                self.stmts(body)?;
                // Again unless the condition is true: `Unless` jumps back.
                let cond = self.text(until, true);
                let u = self.emit(Op::Unless {
                    cond,
                    to: start,
                    cont: 0,
                });
                let end = self.here();
                if let Op::Unless { cont, .. } = &mut self.p.ops[u] {
                    *cont = end;
                }
                self.close_label(opened);
                Ok(())
            }
            Stmt::Leave(l) | Stmt::Iterate(l) => {
                let iterate = matches!(s, Stmt::Iterate(_));
                let word = if iterate { "ITERATE" } else { "LEAVE" };
                let Some(k) = self.label_index(l) else {
                    return Err(err(
                        1308,
                        *b"42000",
                        format!("{word} with no matching label: {l}"),
                    ));
                };
                if iterate && self.labels[k].iterate.is_none() {
                    return Err(err(
                        1308,
                        *b"42000",
                        format!("{word} with no matching label: {l}"),
                    ));
                }
                let depth = self.labels[k].depth;
                // A block's label: its own scope ends too.
                let block_label = self.labels[k].iterate.is_none();
                self.leave_scopes(if block_label {
                    depth.saturating_sub(1)
                } else {
                    depth
                });
                if iterate {
                    let to = self.labels[k].iterate.unwrap_or(0);
                    self.emit(Op::Jump(to));
                } else {
                    let j = self.emit(Op::Jump(0));
                    self.labels[k].breaks.push(j);
                }
                Ok(())
            }
            Stmt::Return(e) => {
                if self.kind != Kind::Function {
                    return Err(err(
                        1313,
                        *b"42000",
                        "RETURN is only allowed in a FUNCTION".into(),
                    ));
                }
                self.returns = true;
                let t = self.text(e, true);
                self.emit(Op::Return(t));
                Ok(())
            }
            Stmt::Open(c) => {
                let c = self.find_cursor(c)?;
                self.emit(Op::Open(c));
                Ok(())
            }
            Stmt::Close(c) => {
                let c = self.find_cursor(c)?;
                self.emit(Op::Close(c));
                Ok(())
            }
            Stmt::Fetch { cursor, into } => {
                let cursor = self.find_cursor(cursor)?;
                let into = into
                    .iter()
                    .map(|t| self.target(t))
                    .collect::<Result<Vec<_>>>()?;
                self.emit(Op::Fetch { cursor, into });
                Ok(())
            }
            Stmt::Signal {
                resignal,
                cond,
                items,
            } => {
                let cond = match cond {
                    None => None,
                    Some(Cond::Code(n)) => Some((Some(*n), None)),
                    Some(Cond::State(s)) => Some((None, Some(s.clone()))),
                    Some(Cond::Name(n)) => match self.find_cond(n) {
                        Some(Cond::Code(x)) => {
                            if !resignal {
                                return Err(err(
                                    1645,
                                    *b"0K000",
                                    "SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE"
                                        .into(),
                                ));
                            }
                            Some((Some(x), None))
                        }
                        Some(Cond::State(s)) => Some((None, Some(s))),
                        _ => return Err(err(1319, *b"42000", format!("Undefined CONDITION: {n}"))),
                    },
                    Some(_) => return Err(super::parse::syntax("", 0)),
                };
                if let Some((Some(_), None)) = &cond
                    && !resignal
                {
                    return Err(super::parse::syntax("", 0));
                }
                let mut seen: Vec<&str> = Vec::new();
                let mut out = Vec::new();
                for (item, v) in items {
                    if !SIGNAL_ITEMS.contains(&item.as_str()) {
                        return Err(super::parse::syntax("", 0));
                    }
                    if seen.contains(&item.as_str()) {
                        return Err(err(
                            1641,
                            *b"42000",
                            format!("Duplicate condition information item '{item}'"),
                        ));
                    }
                    seen.push(item);
                    out.push((item.clone(), self.text(v, true)));
                }
                self.emit(Op::Signal {
                    resignal: *resignal,
                    cond,
                    items: out,
                });
                Ok(())
            }
            Stmt::Diagnostics {
                stacked,
                condition,
                items,
            } => {
                let condition = condition.as_ref().map(|c| self.text(c, true));
                let items = items
                    .iter()
                    .map(|d| Ok((self.target(&d.target)?, d.item.clone())))
                    .collect::<Result<Vec<_>>>()?;
                self.emit(Op::Diag {
                    stacked: *stacked,
                    condition,
                    items,
                });
                Ok(())
            }
        }
    }

    #[allow(clippy::too_many_lines)] // declarations, statements, the end
    fn block(&mut self, label: Option<&String>, decls: &[Decl], body: &[Stmt]) -> Result<()> {
        let id = self.blocks;
        self.blocks += 1;
        let parent = self.scopes.last().map(|s| s.block);
        self.block_parent.push(parent);
        self.block_cursors.push(Vec::new());
        // The label sees the block's own scope: LEAVE ends it.
        self.scopes.push(Scope {
            block: id,
            ..Scope::default()
        });
        let opened = self.open_label(label, None)?;
        // Declarations: variables and conditions, then cursors, then
        // handlers.
        let mut stage = 0u8;
        let mut handler_conds: Vec<HCond> = Vec::new();
        for d in decls {
            match d {
                Decl::Var { names, ty, default } => {
                    if stage > 0 {
                        return Err(err(
                            1337,
                            *b"42000",
                            "Variable or condition declaration after cursor or handler declaration"
                                .into(),
                        ));
                    }
                    // The default reads the variables declared before.
                    let expr = default.as_ref().map(|e| self.text(e, true));
                    for n in names {
                        let scope = self.scopes.last_mut().expect("block scope");
                        if scope.vars.iter().any(|(x, _)| x.eq_ignore_ascii_case(n)) {
                            return Err(err(1331, *b"42000", format!("Duplicate variable: {n}")));
                        }
                        let slot = self.p.vars.len();
                        self.p.vars.push(Var {
                            name: n.clone(),
                            ty: ty.clone(),
                        });
                        self.emit(Op::Set {
                            slot,
                            expr: expr.clone(),
                        });
                        self.scopes
                            .last_mut()
                            .expect("block scope")
                            .vars
                            .push((n.clone(), slot));
                    }
                }
                Decl::Cond { name, value } => {
                    if stage > 0 {
                        return Err(err(
                            1337,
                            *b"42000",
                            "Variable or condition declaration after cursor or handler declaration"
                                .into(),
                        ));
                    }
                    let scope = self.scopes.last_mut().expect("block scope");
                    if scope
                        .conds
                        .iter()
                        .any(|(x, _)| x.eq_ignore_ascii_case(name))
                    {
                        return Err(err(1332, *b"42000", format!("Duplicate condition: {name}")));
                    }
                    scope.conds.push((name.clone(), value.clone()));
                }
                Decl::Cursor { name, query } => {
                    if stage > 1 {
                        return Err(err(
                            1338,
                            *b"42000",
                            "Cursor declaration after handler declaration".into(),
                        ));
                    }
                    stage = 1;
                    if self.scopes.last().is_some_and(|s| {
                        s.cursors.iter().any(|(x, _)| x.eq_ignore_ascii_case(name))
                    }) {
                        return Err(err(1333, *b"42000", format!("Duplicate cursor: {name}")));
                    }
                    let c = self.p.cursors.len();
                    let q = self.text(query, false);
                    self.p.cursors.push((name.clone(), q));
                    let scope = self.scopes.last_mut().expect("block scope");
                    scope.cursors.push((name.clone(), c));
                    scope.own_cursors.push(c);
                    // Every enclosing block's EXIT closes it too.
                    let mut b = Some(id);
                    while let Some(x) = b {
                        self.block_cursors[x].push(c);
                        b = self.block_parent[x];
                    }
                }
                Decl::Handler { exit, conds, body } => {
                    stage = 2;
                    let mut hc = Vec::new();
                    for c in conds {
                        let h = self.hcond(c)?;
                        if hc.contains(&h) || handler_conds.contains(&h) {
                            return Err(err(
                                1413,
                                *b"42000",
                                "Duplicate handler declared in the same block".into(),
                            ));
                        }
                        hc.push(h);
                    }
                    handler_conds.extend(hc.iter().cloned());
                    let h = self.p.handlers.len();
                    self.p.handlers.push(Handler {
                        exit: *exit,
                        conds: hc,
                        start: 0,
                        block: id,
                        exit_to: 0,
                        close: Vec::new(),
                    });
                    let snaps = self
                        .scopes
                        .iter()
                        .map(|s| ScopeSnap {
                            vars: s.vars.clone(),
                            conds: s.conds.clone(),
                            cursors: s.cursors.clone(),
                        })
                        .collect();
                    self.pending.push((h, (**body).clone(), snaps));
                    self.emit(Op::HPush(h));
                    self.scopes.last_mut().expect("block scope").handlers += 1;
                }
            }
        }
        // Cursors start closed each time the block is entered.
        let own = self
            .scopes
            .last()
            .map(|s| s.own_cursors.clone())
            .unwrap_or_default();
        if !own.is_empty() {
            self.emit(Op::Reset(own));
        }
        self.stmts(body)?;
        let depth = self.scopes.len() - 1;
        self.leave_scopes(depth);
        // A LEAVE of the block's label cleaned up itself: it lands after
        // the cleanup.
        self.close_label(opened);
        let after = self.here();
        self.scopes.pop();
        for h in &mut self.p.handlers {
            if h.block == id {
                h.exit_to = after;
                h.close.clone_from(&self.block_cursors[id]);
            }
        }
        Ok(())
    }
}

/// The items `SIGNAL`/`RESIGNAL` may set.
const SIGNAL_ITEMS: [&str; 12] = [
    "CLASS_ORIGIN",
    "SUBCLASS_ORIGIN",
    "CONSTRAINT_CATALOG",
    "CONSTRAINT_SCHEMA",
    "CONSTRAINT_NAME",
    "CATALOG_NAME",
    "SCHEMA_NAME",
    "TABLE_NAME",
    "COLUMN_NAME",
    "CURSOR_NAME",
    "MESSAGE_TEXT",
    "MYSQL_ERRNO",
];

/// Compiles a routine.
///
/// # Errors
/// The servers' creation-time errors (1308, 1313, 1319, 1320, 1324, 1327,
/// 1330, 1331, 1332, 1333, 1337, 1338, 1413, 1415, ...).
pub(crate) fn compile(c: &Create, db: &str) -> Result<Program> {
    let mut cx = new_c(c.kind, false);
    for prm in &c.params {
        if cx
            .p
            .vars
            .iter()
            .any(|v| v.name.eq_ignore_ascii_case(&prm.name))
        {
            return Err(err(
                1330,
                *b"42000",
                format!("Duplicate parameter: {}", prm.name),
            ));
        }
        let slot = cx.p.vars.len();
        cx.p.vars.push(Var {
            name: prm.name.clone(),
            ty: prm.ty.clone(),
        });
        cx.p.modes.push(prm.mode);
        cx.scopes[0].vars.push((prm.name.clone(), slot));
    }
    cx.stmt(&c.body)?;
    cx.emit(Op::End);
    if c.kind == Kind::Function && !cx.returns {
        return Err(err(
            1320,
            *b"42000",
            format!("No RETURN found in FUNCTION {db}.{}", c.name),
        ));
    }
    handlers(&mut cx)?;
    Ok(cx.p)
}

/// Compiles a trigger on a table with columns `cols`: slots `NEW.col` (not
/// for `DELETE`) then `OLD.col` (not for `INSERT`), in column order.
///
/// # Errors
/// As for a routine, plus 1054, 1362 and 1363.
pub(crate) fn compile_trigger(t: &TrigCreate, cols: &[String]) -> Result<Program> {
    let mut cx = new_c(Kind::Trigger, t.before);
    let mut rows = Vec::new();
    if t.event != Event::Delete {
        rows.push("NEW");
    }
    if t.event != Event::Insert {
        rows.push("OLD");
    }
    for row in rows {
        for c in cols {
            let name = format!("{row}.{c}");
            let slot = cx.p.vars.len();
            cx.p.vars.push(Var {
                name: name.clone(),
                ty: String::new(),
            });
            cx.p.modes.push(Mode::In);
            cx.scopes[0].vars.push((name, slot));
        }
    }
    cx.stmt(&t.body)?;
    cx.emit(Op::End);
    handlers(&mut cx)?;
    if let Some(n) = cx.bad_rows.borrow().first() {
        let (row, col) = n.split_at(3);
        let row = row.to_ascii_uppercase();
        return Err(match (row.as_str(), t.event) {
            ("OLD", Event::Insert) => err(
                1363,
                *b"HY000",
                "There is no OLD row in on INSERT trigger".into(),
            ),
            ("NEW", Event::Delete) => err(
                1363,
                *b"HY000",
                "There is no NEW row in on DELETE trigger".into(),
            ),
            _ => err(
                1054,
                *b"42S22",
                format!("Unknown column '{}' in '{row}'", &col[1..]),
            ),
        });
    }
    Ok(cx.p)
}

fn new_c(kind: Kind, before: bool) -> C {
    C {
        kind,
        p: Program::default(),
        scopes: vec![Scope::default()],
        labels: Vec::new(),
        blocks: 1,
        returns: false,
        pending: Vec::new(),
        handler_depth: 0,
        block_cursors: vec![Vec::new()],
        block_parent: vec![None],
        before,
        bad_rows: std::cell::RefCell::new(Vec::new()),
    }
}

/// Compiles the handler bodies, after the main code.
fn handlers(cx: &mut C) -> Result<()> {
    // Handler bodies: each sees the scopes of its declaration, and no
    // label outside it.
    while let Some((h, body, snaps)) = cx.pending.pop() {
        let saved = std::mem::take(&mut cx.scopes);
        cx.scopes = snaps
            .into_iter()
            .map(|s| Scope {
                vars: s.vars,
                conds: s.conds,
                cursors: s.cursors,
                ..Scope::default()
            })
            .collect();
        cx.handler_depth += 1;
        let start = cx.here();
        cx.p.handlers[h].start = start;
        cx.stmt(&body)?;
        cx.emit(Op::HEnd(h));
        cx.handler_depth -= 1;
        cx.scopes = saved;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::parse::parse_create;
    use super::*;

    fn prog(s: &str) -> Result<Program> {
        compile(&parse_create(s)?, "d")
    }

    #[test]
    fn loops_labels_and_handlers_compile() {
        let p = prog(
            "CREATE PROCEDURE p(IN n INT) BEGIN DECLARE i INT DEFAULT 0; \
             DECLARE EXIT HANDLER FOR SQLEXCEPTION SELECT 'err'; \
             l: LOOP SET i = i + 1; IF i >= n THEN LEAVE l; END IF; END LOOP l; SELECT i; END",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(p.vars.len(), 2);
        assert_eq!(p.handlers.len(), 1);
        assert!(p.results);
        assert!(matches!(p.ops.last(), Some(Op::HEnd(0))));
        // The LEAVE jumps past the loop.
        let leave = p
            .ops
            .iter()
            .filter_map(|o| if let Op::Jump(t) = o { Some(*t) } else { None })
            .max()
            .unwrap_or(0);
        assert!(matches!(p.ops[leave], Op::Sql { .. }), "{:?}", p.ops);
    }

    #[test]
    fn creation_time_errors_match_the_servers() {
        let code = |s: &str| prog(s).unwrap_err().protocol().mysql_errno;
        assert_eq!(code("CREATE PROCEDURE p() BEGIN LEAVE nope; END"), 1308);
        assert_eq!(code("CREATE PROCEDURE p() RETURN 1"), 1313);
        assert_eq!(
            code("CREATE FUNCTION f() RETURNS INT BEGIN SET @a = 1; END"),
            1320
        );
        assert_eq!(
            code("CREATE FUNCTION f() RETURNS INT BEGIN SELECT 1; RETURN 1; END"),
            1415
        );
        assert_eq!(
            code("CREATE PROCEDURE p() BEGIN DECLARE a INT; DECLARE a INT; END"),
            1331
        );
        assert_eq!(
            code(
                "CREATE PROCEDURE p() BEGIN DECLARE CONTINUE HANDLER FOR NOT FOUND SET @a = 1; DECLARE a INT; END"
            ),
            1337
        );
        assert_eq!(
            code("CREATE PROCEDURE p() BEGIN SELECT 1 INTO nope; END"),
            1327
        );
        assert_eq!(code("CREATE PROCEDURE p(a INT, A INT) BEGIN END"), 1330);
        assert_eq!(code("CREATE PROCEDURE p() BEGIN OPEN c; END"), 1324);
    }

    fn trig(s: &str) -> Result<Program> {
        let t = super::super::parse::parse_trigger(s)?;
        compile_trigger(&t, &["id".to_owned(), "v".to_owned()])
    }

    fn trig_code(s: &str) -> u16 {
        trig(s).err().map_or(0, |e| e.protocol().mysql_errno)
    }

    #[test]
    fn triggers_compile_with_row_slots() {
        let p = trig(
            "CREATE TRIGGER t BEFORE UPDATE ON x FOR EACH ROW \
             BEGIN IF NEW.v < OLD.v THEN SET NEW.v = OLD.v; END IF; END",
        )
        .unwrap_or_else(|e| panic!("{e}"));
        let names: Vec<&str> = p.vars.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(names, ["NEW.id", "NEW.v", "OLD.id", "OLD.v"]);
        assert!(p.ops.iter().any(|o| matches!(o, Op::Set { slot: 1, .. })));
        let p = trig("CREATE TRIGGER t AFTER DELETE ON x FOR EACH ROW SET @a = old.id")
            .unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(p.vars.len(), 2);
    }

    #[test]
    fn trigger_creation_errors_match_the_servers() {
        let before = "CREATE TRIGGER t BEFORE INSERT ON x FOR EACH ROW ";
        assert_eq!(trig_code(&format!("{before}SET OLD.v = 1")), 1362);
        assert_eq!(trig_code(&format!("{before}SET @a = OLD.v")), 1363);
        assert_eq!(trig_code(&format!("{before}SET NEW.nosuch = 1")), 1054);
        assert_eq!(trig_code(&format!("{before}SELECT 1")), 1415);
        assert_eq!(trig_code(&format!("{before}RETURN 1")), 1313);
        assert_eq!(
            trig_code("CREATE TRIGGER t AFTER INSERT ON x FOR EACH ROW SET NEW.v = 1"),
            1362
        );
        assert_eq!(
            trig_code("CREATE TRIGGER t BEFORE DELETE ON x FOR EACH ROW SET @a = NEW.v"),
            1363
        );
    }
}
