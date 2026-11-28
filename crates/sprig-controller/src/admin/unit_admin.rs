//! `sprigctl unit move|home` and `sprigctl balancer show`: planned moves of
//! engine units placed per tenant.
//!
//! - `POST /admin/v1/units/{tenant}:move` `{from, to, reason?}`: writes a
//!   replica move into the unit's descriptor; the unit's leader copies the
//!   unit to `to` and takes `from` out (the balancer's moves are the same
//!   writes);
//! - `POST /admin/v1/units/{tenant}:home` `{node}`: names a voter the unit's
//!   home; that node then asks for the leadership (a leadership move);
//! - `GET /admin/v1/balancer/units`: per node units and homes, moves
//!   pending, and what the balancer's next pass would do.
//!
//! `{tenant}` is a tenant id or name.

use sprig_catalog::units::{self, UnitDescriptor};
use sprig_common::Result;

use super::{AdminService, invalid};
use crate::unit_balance::{self, BalanceReport};

impl AdminService {
    async fn unit_tenant(&self, tenant: &str) -> Result<u64> {
        if let Ok(id) = tenant.parse::<u64>() {
            return Ok(id);
        }
        Ok(self.get_tenant(tenant).await?.tenant_id.get())
    }

    /// `POST /admin/v1/units/{tenant}:{method}` (module docs).
    ///
    /// # Errors
    /// 39792 for an unknown method or a move that can't start; 39470 while
    /// another move of the unit is pending; the store's.
    pub async fn unit_method(
        &self,
        tenant: &str,
        method: &str,
        body: &serde_json::Value,
    ) -> Result<UnitDescriptor> {
        let id = self.unit_tenant(tenant).await?;
        let node = |k: &str| {
            body.get(k)
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| invalid(format!("the body needs \"{k}\": a node id")))
        };
        let dc = self.stores.dc.as_ref();
        let d = match method {
            "move" => {
                let (from, to) = (node("from")?, node("to")?);
                let reason = body
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("sprigctl unit move");
                units::start_move(dc, id, from, to, &format!("operator: {reason}")).await?
            }
            "home" => units::set_home(dc, id, node("node")?).await?,
            m => {
                return Err(invalid(format!(
                    "POST /admin/v1/units/{tenant}:{m}: expected :move or :home"
                )));
            }
        };
        tracing::info!(event = "decision", component = "controller", kind = %format!("unit.{method}"),
            tenant = id, voters = ?d.voters, home = d.home, moving = ?d.moving,
            "an operator moved an engine unit or its leadership");
        Ok(d)
    }

    /// `GET /admin/v1/balancer/units` (module docs; the planner's limits
    /// at their defaults).
    ///
    /// # Errors
    /// The store's.
    pub async fn balancer_units(&self) -> Result<BalanceReport> {
        unit_balance::report(self.stores.dc.as_ref(), &unit_balance::knobs(None)).await
    }
}
