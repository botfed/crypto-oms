use anyhow::Result;
use async_trait::async_trait;

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
    OrderCancelling(ClientOrderId),
    OrderCancelled(ClientOrderId),
    OrderRejected { client_id: ClientOrderId, reason: String },
    OrderTimedOut(ClientOrderId),
    PositionUpdate(Position),
    BalanceUpdate(Balance),
    /// Full state snapshot from REST poller — Vec<OrderHandle> + positions + balances
    Snapshot {
        orders: Vec<OrderHandle>,
        positions: Vec<Position>,
        balances: Vec<Balance>,
    },
    Disconnected,
    Reconnected,
}

// ---------------------------------------------------------------------------
// Core OMS trait
// ---------------------------------------------------------------------------

#[async_trait]
pub trait ExchangeOms: Send + Sync {
    /// Exchange-specific prepared order (output of prepare, input to sign).
    type PreparedOrder: Send + 'static;

    /// Exchange-specific signed payload (output of sign, input to post).
    type SignedPayload: Send + 'static;

    /// False until initial state snapshot is loaded and reconciled.
    fn is_ready(&self) -> bool;

    /// Blocks until ready or returns error on init failure.
    async fn wait_ready(&self) -> Result<()>;

    // -----------------------------------------------------------------------
    // Simple order interface (convenience / fallback)
    // -----------------------------------------------------------------------

    /// Place a new order (combines prepare + sign + post). Returns client ID.
    async fn place_order(&self, req: OrderRequest) -> Result<ClientOrderId>;

    /// Cancel an existing order by client ID.
    async fn cancel_order(&self, id: &ClientOrderId) -> Result<()>;

    /// Cancel all open orders, optionally filtered by symbol.
    async fn cancel_all(&self, symbol: Option<&str>) -> Result<()>;

    /// Atomic cancel-replace. Returns new client ID.
    async fn modify_order(
        &self,
        id: &ClientOrderId,
        req: ModifyRequest,
    ) -> Result<ClientOrderId>;

    // -----------------------------------------------------------------------
    // Split order placement (hot-path: prepare sync, sign sync, post async)
    // -----------------------------------------------------------------------

    /// Sync hot-path: validate, generate client ID, insert inflight state.
    /// Returns (client_id, exchange-specific prepared order).
    fn prepare_place_order(&self, req: &OrderRequest) -> Result<(ClientOrderId, Self::PreparedOrder)>;

    /// Sync: sign (hash + crypto-sign) a prepared order.
    /// For exchanges without signing, this is a no-op passthrough.
    fn sign_order(&self, prepared: Self::PreparedOrder) -> Result<Self::SignedPayload>;

    /// Async: post a signed order payload and process the response.
    async fn post_order(&self, cid: u64, signed: Self::SignedPayload);

    // -----------------------------------------------------------------------
    // Split cancel (hot-path: sign/presign sync, post async)
    // -----------------------------------------------------------------------

    /// Sync: sign a cancel and mark the order as Cancelling.
    fn sign_cancel(&self, id: &ClientOrderId) -> Result<Self::SignedPayload>;

    /// Sync: pre-sign a cancel without changing order state.
    /// Called at accept time to cache the signed payload for fast-path cancel.
    fn presign_cancel(&self, id: &ClientOrderId) -> Result<Self::SignedPayload>;

    /// Sync: mark an order as Cancelling (state transition only, no signing).
    fn mark_cancelling(&self, id: &ClientOrderId);

    /// Async: post a signed cancel payload and handle the response.
    async fn post_cancel(&self, id: &ClientOrderId, signed: Self::SignedPayload);

    /// Shutdown: cancel all orders and clean up.
    async fn shutdown_cancel_all(&self, symbol: Option<&str>) -> Result<()>;

    // -----------------------------------------------------------------------
    // Query interface
    // -----------------------------------------------------------------------

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

    /// Get the event receiver for OMS events (single consumer).
    fn event_receiver(&self) -> crossbeam_channel::Receiver<OmsEvent>;

    /// Round a price to the exchange's tick/precision rules.
    fn round_price(&self, price: f64) -> f64;
}
