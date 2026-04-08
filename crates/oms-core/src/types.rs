use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Instant;

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientOrderId(pub u64);

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeInForce {
    GTC,
    IOC,
    PostOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum OrderType {
    Limit { price: f64, tif: TimeInForce },
    Market,
    StopLimit { price: f64, trigger: f64, tif: TimeInForce },
    StopMarket { trigger: f64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderState {
    /// Sent to exchange, awaiting ack
    Inflight,
    /// Exchange accepted, working
    Accepted,
    PartiallyFilled,
    /// Cancel sent to exchange, awaiting confirmation
    Cancelling,
    Filled,
    Cancelled,
    Rejected,
    /// No ack within deadline
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Liquidity {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InstrumentType {
    Spot,
    Perp,
}

// ---------------------------------------------------------------------------
// Order request (strategy -> OMS)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub size: f64,
    pub reduce_only: bool,
}

// ---------------------------------------------------------------------------
// Modify request
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyRequest {
    pub price: Option<f64>,
    pub size: Option<f64>,
    pub trigger: Option<f64>,
}

// ---------------------------------------------------------------------------
// Order handle (OMS internal state, exposed read-only)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderHandle {
    pub client_id: ClientOrderId,
    pub exchange_id: Option<String>,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub size: f64,
    pub filled_size: f64,
    pub avg_fill_price: Option<f64>,
    pub state: OrderState,
    pub reduce_only: bool,
    pub reject_reason: Option<String>,
    pub exchange_ts: Option<DateTime<Utc>>,
    #[serde(skip)]
    pub submitted_at: Option<Instant>,
    /// Updated on every state change — used for stray order detection
    #[serde(skip)]
    pub last_modified: Option<Instant>,
}

// ---------------------------------------------------------------------------
// Fill
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub client_id: ClientOrderId,
    pub exchange_id: String,
    pub symbol: String,
    pub side: Side,
    pub price: f64,
    pub size: f64,
    pub fee: f64,
    pub fee_asset: String,
    pub liquidity: Liquidity,
    pub ts: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// Position
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub side: Side,
    pub size: f64,
    pub entry_price: f64,
    pub unrealized_pnl: f64,
    pub leverage: f64,
    pub liquidation_price: Option<f64>,
}

// ---------------------------------------------------------------------------
// Balance
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance {
    pub asset: String,
    /// Withdrawable / free collateral
    pub available: f64,
    /// Margin in use
    pub locked: f64,
    /// Deposited balance (totalRawUsd on HL)
    pub total: f64,
    /// Account equity (total + unrealized PnL)
    pub equity: f64,
}
