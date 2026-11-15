//! Triage and advice for one incident: two turns of the usual turn loop,
//! stored as one administration conversation.
//!
//! ```text
//! text := brief::intake(incident, why, topology)       -- through the scrubber
//! conversation := "Incident <fingerprint>: ..." in the system store
//! critical code: triage is skipped
//! else triage turn (ai.model.triage, else ai.model.admin; no tools; one word)
//!      noise -> done
//! advice turn (ai.model.admin; search_docs only, audience operator)
//!      -> Advice {severity, cause, text} and the docs it cited
//! ```
//!
//! Both turns go through the egress gate with the administration policy
//! and a scrubber that knows every name ([`super::brief::Names`]); both
//! are budgeted against the system store's usage (the incident intake is
//! one more "operator" there, [`INCIDENTS`]), and their exact payloads are
//! kept with the conversation for `ai.audit_payload_days`.

use std::sync::{Arc, LazyLock};

use sprig_common::ids::UserId;
use sprig_common::{Result, SprigError};

use super::brief::{self, ADVICE_RULES, Advice, Names, TRIAGE_RULES, Topology, Verdict};
use super::record::{IncidentRecord, Why, critical};
use crate::audit::{AuditStore, ConvId};
use crate::provider::{Block, Provider};
use crate::role::Role;
use crate::settings::TurnSettings;
use crate::tools::{Source, ToolHost, ToolRegistry};
use crate::turn::{Agent, TurnConfig, TurnEvent, TurnRequest};

/// The login the incident intake's conversations and usage are kept
/// under in the system store. No operator can have it: logins are
/// `tenant.user`.
pub const INCIDENTS: &str = "sprig:incidents";

/// The most output triage may use.
pub const TRIAGE_TOKENS: u32 = 1024;

/// The tools an advice turn offers: the docs search only.
static ADVICE_TOOLS: LazyLock<Arc<ToolRegistry>> = LazyLock::new(|| {
    let b = ToolRegistry::builtin();
    let specs: Vec<_> = b
        .get("search_docs")
        .map(|s| (**s).clone())
        .into_iter()
        .collect();
    Arc::new(ToolRegistry::new(specs).unwrap_or_else(|_| (*b).clone()))
});

/// The tools an advice turn offers.
#[must_use]
pub fn advice_tools() -> Arc<ToolRegistry> {
    Arc::clone(&ADVICE_TOOLS)
}

/// A role's model: its provider and the model id.
#[derive(Clone)]
pub struct Model {
    /// The provider.
    pub provider: Arc<dyn Provider>,
    /// The model, as the provider names it.
    pub model: String,
    /// The role's settings (read at the global scope).
    pub settings: TurnSettings,
}

impl std::fmt::Debug for Model {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Model")
            .field("provider", &self.provider.name())
            .field("model", &self.model)
            .finish_non_exhaustive()
    }
}

/// What to advise on.
#[derive(Debug)]
pub struct Case<'a> {
    /// The incident.
    pub incident: &'a IncidentRecord,
    /// Why it goes further.
    pub why: Why,
    /// The topology summary.
    pub topology: &'a Topology,
    /// Every name that could appear.
    pub names: &'a Names,
    /// Now, ms since the Unix epoch.
    pub now_ms: u64,
}

/// What came of it.
#[derive(Debug, Clone, PartialEq)]
pub struct Report {
    /// The intake text exactly as sent (after the scrubber).
    pub intake: String,
    /// Triage's answer; `None` when it was skipped (a critical code).
    pub verdict: Option<Verdict>,
    /// The advice, when triage said it was needed.
    pub advice: Option<Advice>,
    /// The docs the advice drew on.
    pub sources: Vec<Source>,
    /// The conversation in the system store; `None` when it couldn't be
    /// reached (the turns ran unstored).
    pub conversation: Option<ConvId>,
    /// SHA-256 of each payload sent, hexadecimal.
    pub sent: Vec<String>,
}

impl Report {
    /// Whether advice should be filed.
    #[must_use]
    pub fn advised(&self) -> bool {
        self.advice.is_some()
    }
}

/// Runs triage and advice.
pub struct Advisor {
    audit: Arc<dyn AuditStore>,
    host: Arc<dyn ToolHost>,
    stored: bool,
}

impl std::fmt::Debug for Advisor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Advisor")
            .field("stored", &self.stored)
            .finish_non_exhaustive()
    }
}

impl Advisor {
    /// An advisor keeping its conversations in `audit` (the system store
    /// opened for [`INCIDENTS`]) and running `search_docs` on `host`.
    /// `stored` is false when `audit` only stands in for a store that
    /// couldn't be reached.
    #[must_use]
    pub fn new(audit: Arc<dyn AuditStore>, host: Arc<dyn ToolHost>, stored: bool) -> Self {
        Self {
            audit,
            host,
            stored,
        }
    }

    async fn turn(
        &self,
        agent: &Agent,
        cfg: &TurnConfig,
        conv: Option<ConvId>,
        system: Vec<Block>,
        text: &str,
        scrubber: &Arc<crate::egress::Scrubber>,
    ) -> Result<crate::turn::TurnResult> {
        let mut events: Vec<TurnEvent> = Vec::new();
        agent
            .run(
                cfg,
                TurnRequest {
                    tenant: None,
                    user: UserId(0),
                    conv,
                    system,
                    history: Vec::new(),
                    message: text.to_owned(),
                    host: Arc::clone(&self.host),
                    scrubber: Some(Arc::clone(scrubber)),
                    cancel: None,
                },
                &mut events,
            )
            .await
    }

    /// Triage (unless the code is critical), then advice when triage
    /// says it is needed. `triage` is `None` to use the admin model for
    /// triage too.
    ///
    /// # Errors
    /// The turn's: 39992 when a budget is spent, 39993 when the provider
    /// fails, 39994 when the advice was refused, 39990 for a model that is
    /// set but empty.
    pub async fn advise(
        &self,
        case: &Case<'_>,
        triage: Option<&Model>,
        admin: &Model,
    ) -> Result<Report> {
        let scrubber = Arc::new(case.names.scrubber());
        let gate = crate::egress::EgressGate::new(
            crate::egress::EgressPolicy {
                role: Role::Admin,
                share_data: false,
            },
            Some(Arc::clone(&scrubber)),
        );
        let raw = brief::intake(case.incident, case.why, case.topology, case.now_ms);
        let text = gate.user_text(&raw);
        let rec = case.incident;
        let conv = if self.stored {
            let title = format!(
                "Incident {}: {} {} in {} {}",
                rec.fingerprint, rec.code, rec.code_name, rec.component, rec.span
            );
            match self.audit.create(UserId(0), Role::Admin, &title).await {
                Ok(c) => Some(c),
                Err(e) => {
                    tracing::warn!(error = %e, "incident advice runs unstored: the system store can't be reached");
                    None
                }
            }
        } else {
            None
        };
        let mut report = Report {
            intake: text.clone(),
            verdict: None,
            advice: None,
            sources: Vec::new(),
            conversation: conv,
            sent: Vec::new(),
        };
        let is_critical = critical(rec.code) || case.why == Why::Critical;
        if is_critical {
            crate::metrics::incident_triage("critical");
        } else {
            let m = triage.unwrap_or(admin);
            let mut cfg = TurnConfig::from_settings(Role::Triage, &m.settings, m.model.clone());
            cfg.max_tool_rounds = 0;
            cfg.max_output_tokens = cfg.max_output_tokens.min(TRIAGE_TOKENS);
            let agent = Agent::new(
                Arc::clone(&m.provider),
                ToolRegistry::builtin(),
                Arc::clone(&self.audit),
            );
            let mut system = crate::prompt::base(Role::Triage);
            system.push(Block::text(TRIAGE_RULES));
            let r = self
                .turn(&agent, &cfg, conv, system, &text, &scrubber)
                .await?;
            report.sent.extend(r.sent.iter().cloned());
            let v = if r.refusal().is_some() {
                Verdict::NeedsAdvice
            } else {
                Verdict::read(&r.text())
            };
            crate::metrics::incident_triage(v.as_str());
            report.verdict = Some(v);
            if v == Verdict::Noise {
                return Ok(report);
            }
        }
        let cfg = TurnConfig::from_settings(Role::Admin, &admin.settings, admin.model.clone());
        let agent = Agent::new(
            Arc::clone(&admin.provider),
            advice_tools(),
            Arc::clone(&self.audit),
        );
        let mut system = crate::prompt::base(Role::Admin);
        system.push(Block::text(ADVICE_RULES));
        let r = self
            .turn(&agent, &cfg, conv, system, &text, &scrubber)
            .await?;
        report.sent.extend(r.sent.iter().cloned());
        if let Some(e) = r.refusal() {
            return Err(e);
        }
        let answer = r.text();
        if answer.trim().is_empty() {
            return Err(SprigError::internal(format!(
                "the advice turn ended without an answer ({})",
                r.stop.label()
            )));
        }
        report.advice = Some(Advice::read(&answer, is_critical));
        report.sources = r.sources;
        Ok(report)
    }

    /// One turn of the admin model for the agent's other work (a health
    /// warning, a write-up, an upgrade report's explanation): `w.text`
    /// through the scrubber, `w.rules` after the administration prompt,
    /// `w.tools` offered, stored as one administration conversation.
    ///
    /// # Errors
    /// The turn's (39992, 39993, 39994, 39990); internal for a turn that
    /// ended without an answer.
    pub async fn write(&self, w: &Writing<'_>, admin: &Model) -> Result<Written> {
        let scrubber = Arc::new(w.names.scrubber());
        let gate = crate::egress::EgressGate::new(
            crate::egress::EgressPolicy {
                role: Role::Admin,
                share_data: false,
            },
            Some(Arc::clone(&scrubber)),
        );
        let text = gate.user_text(&w.text);
        let conv = if self.stored {
            match self.audit.create(UserId(0), Role::Admin, &w.title).await {
                Ok(c) => Some(c),
                Err(e) => {
                    tracing::warn!(error = %e, "the administration agent runs unstored: the system store can't be reached");
                    None
                }
            }
        } else {
            None
        };
        let cfg = TurnConfig::from_settings(Role::Admin, &admin.settings, admin.model.clone());
        let agent = Agent::new(
            Arc::clone(&admin.provider),
            Arc::clone(&w.tools),
            Arc::clone(&self.audit),
        );
        let mut system = crate::prompt::base(Role::Admin);
        system.push(Block::text(w.rules));
        let r = self
            .turn(&agent, &cfg, conv, system, &text, &scrubber)
            .await?;
        if let Some(e) = r.refusal() {
            return Err(e);
        }
        let mut answer = r.text();
        if answer.trim().is_empty() {
            return Err(SprigError::internal(format!(
                "the administration agent's turn ended without an answer ({})",
                r.stop.label()
            )));
        }
        // The operators' copy names what the placeholders stand for.
        for id in &w.restore {
            for kind in crate::egress::NameKind::ALL {
                let ph = scrubber.placeholder(*kind, id);
                if answer.contains(&ph) {
                    answer = answer.replace(&ph, id);
                }
            }
        }
        Ok(Written {
            intake: text,
            text: answer.trim().to_owned(),
            sources: r.sources,
            conversation: conv,
            sent: r.sent,
        })
    }
}

/// What [`Advisor::write`] is asked to do.
#[derive(Debug)]
pub struct Writing<'a> {
    /// The conversation's title (ids only; it stays in the system store).
    pub title: String,
    /// The text sent, before the scrubber.
    pub text: String,
    /// What the model is told after the administration prompt.
    pub rules: &'static str,
    /// The tools offered.
    pub tools: Arc<ToolRegistry>,
    /// Every name that could appear.
    pub names: &'a Names,
    /// Ids put back into the answer where the model wrote their
    /// placeholders, for the operators' copy (what was sent stays
    /// scrubbed).
    pub restore: Vec<String>,
}

/// What [`Advisor::write`] produced.
#[derive(Debug, Clone, PartialEq)]
pub struct Written {
    /// The text exactly as sent (after the scrubber).
    pub intake: String,
    /// The model's answer, with the ids of [`Writing::restore`] put back.
    pub text: String,
    /// What it drew on.
    pub sources: Vec<Source>,
    /// The conversation in the system store.
    pub conversation: Option<ConvId>,
    /// SHA-256 of each payload sent, hexadecimal.
    pub sent: Vec<String>,
}
