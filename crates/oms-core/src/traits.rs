use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::types::*;

// ---------------------------------------------------------------------------
// OMS events (real-time stream for downstream consumers)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum OmsEvent {
    Ready,
    OrderInflight(ClientOrderId),
    OrderAccepted { client_id: ClientOrderId, exchange_id: String },
    OrderPartialFill(Fill),
    OrderFilled(Fill),
    OrderCancelled(ClientOrderId),
    OrderRejected { client_id: ClientOrderId, reason: String },
    OrderTimedOut(ClientOrderId),
    PositionUpdate(Position),
    BalanceUpdate(Balance),
    Disconnected,
    Reconnected,
}

// ---------------------------------------------------------------------------
// Core OMS trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ExchangeOms: Send + Sync {
    /// False until initial state snapshot is loaded and reconciled.
    fn is_ready(&self) -> bool;

    /// Blocks until ready or returns error on init failure.
    async fn wait_ready(&self) -> Result<()>;

    /// Place a new order. Returns error if OMS not ready.
    async fn place_order(&self, req: OrderRequest) -> Result<ClientOrderId>;

    /// Cancel an existing order by client ID.
    async fn cancel_order(&self, id: &ClientOrderId) -> Result<()>;

    /// Cancel all open orders, optionally filtered by symbol.
    async fn cancel_all(&self, symbol: Option<&str>) -> Result<()>;

    /// Atomic cancel-replace. Returns new client ID.
    /// Falls back to cancel + place if exchange doesn't support atomic modify.
    async fn modify_order(
        &self,
        id: &ClientOrderId,
        req: ModifyRequest,
    ) -> Result<ClientOrderId>;

    /// Snapshot of a single order.
    fn get_order(&self, id: &ClientOrderId) -> Option<OrderHandle>;

    /// All open/inflight orders, optionally filtered by symbol.
    fn open_orders(&self, symbol: Option<&str>) -> Vec<OrderHandle>;

    /// Only inflight (unacked) orders, optionally filtered by symbol.
    fn inflight_orders(&self, symbol: Option<&str>) -> Vec<OrderHandle>;

    /// Current positions.
    fn positions(&self) -> Vec<Position>;

    /// Current balances.
    fn balances(&self) -> Vec<Balance>;

    /// Subscribe to real-time OMS events.
    fn subscribe(&self) -> broadcast::Receiver<OmsEvent>;
}
