//! Storage engines other than `InnoDB` (`compat.engine_substitution`), on
//! both ports: `CREATE TABLE ... ENGINE=MyISAM` makes an `InnoDB` table with
//! the server's warning, or fails with `reject`
//! ([`sprig_sql::compat::engine`] has the rules). The step runs before the
//! name layer, so the catalog's DDL history and every shard get the `InnoDB`
//! text; the DDL step answers the warnings (`exec::tenant`).

use std::borrow::Cow;

use bytes::Bytes;
use sprig_common::SprigError;
use sprig_settings::{Apply, HardBounds, Scopes, Setting};
use sprig_sql::compat::engine::{self, Mode, Outcome};

use super::PassSession;
use crate::tenant::session::LocalWarning;

/// `compat.engine_substitution`: `innodb` or `reject`.
pub(crate) static ENGINE_SUBSTITUTION: Setting<Cow<'static, str>> = Setting {
    key: "compat.engine_substitution",
    default: Cow::Borrowed("innodb"),
    // Tenant and global.
    scopes: Scopes(0b10_0100),
    hard: HardBounds::one_of(&["innodb", "reject"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 28,
    doc: "innodb: a MyISAM, MEMORY, Aria, CSV or ARCHIVE table is created as InnoDB, with the server's warning; reject: the statement fails (3161 on 3306, 1290 on 3307)",
};
sprig_settings::register!(ENGINE_SUBSTITUTION);

/// What [`PassSession::engine_substitution`] decided.
pub(super) enum EngineStep {
    /// The statement runs as it is.
    Run,
    /// The statement runs as this text; the DDL step answers the warnings
    /// left in the tenant session.
    Rewritten(Bytes),
    /// The statement fails.
    Refused(SprigError),
}

impl PassSession {
    /// The mode for the session's tenant.
    fn engine_mode(&self) -> Mode {
        let Some(ts) = self.tenant.as_deref() else {
            return Mode::Innodb;
        };
        let Some(cat) = ts.catalog() else {
            return Mode::parse(&ENGINE_SUBSTITUTION.default);
        };
        let svc = ts.services();
        let ctx = cat.setting_ctx(&svc.dc);
        Mode::parse(
            &svc.settings
                .get(&ENGINE_SUBSTITUTION, &ctx, Some(&cat.settings)),
        )
    }

    /// A `CREATE TABLE` or `ALTER TABLE` that names (or, through the
    /// session's `default_storage_engine`, gets) an engine other than
    /// `InnoDB`. The warnings go to the tenant session; the DDL step sends
    /// them with its OK.
    pub(super) fn engine_substitution(&mut self, text: &[u8]) -> EngineStep {
        let default = self
            .bs
            .as_ref()
            .and_then(|b| b.vars().vars.get("default_storage_engine").cloned())
            .filter(|e| !e.eq_ignore_ascii_case("innodb"));
        if !engine::may_apply(text, default.is_some()) {
            return EngineStep::Run;
        }
        let (lex, _) = self.lex_opts();
        let Some(plan) = engine::plan(text, &lex) else {
            return EngineStep::Run;
        };
        // The session's temporary tables keep their engine.
        if self
            .tenant
            .as_deref()
            .is_some_and(|ts| ts.is_temp(plan.db.as_deref(), &plan.table))
        {
            return EngineStep::Run;
        }
        let mode = self.engine_mode();
        match plan.apply(text, self.persona, mode, default.as_deref()) {
            Outcome::Unchanged => EngineStep::Run,
            Outcome::Refused(e) => {
                EngineStep::Refused(SprigError::standard(e.code, e.sqlstate, e.message))
            }
            Outcome::Rewritten { sql, notes } => {
                if let Some(ts) = self.tenant.as_deref_mut()
                    && !notes.is_empty()
                {
                    ts.warnings = Some(
                        notes
                            .into_iter()
                            .map(|n| LocalWarning {
                                level: n.level,
                                code: n.code,
                                message: n.message,
                            })
                            .collect(),
                    );
                }
                EngineStep::Rewritten(Bytes::from(sql))
            }
        }
    }
}
