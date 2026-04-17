use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Info API request types (POST /info)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(tag = "type")]
pub enum InfoRequest<'a> {
    #[serde(rename = "meta")]
    Meta,
    #[serde(rename = "openOrders")]
    OpenOrders { user: &'a str },
    #[serde(rename = "clearinghouseState")]
    ClearinghouseState { user: &'a str },
    #[serde(rename = "userFills")]
    UserFills { user: &'a str },
    #[serde(rename = "orderStatus")]
    OrderStatus { user: &'a str, oid: u64 },
    #[serde(rename = "spotClearinghouseState")]
    SpotClearinghouseState { user: &'a str },
}

// ---------------------------------------------------------------------------
// Spot clearinghouse response
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SpotClearinghouseResponse {
    pub balances: Vec<SpotBalance>,
}

#[derive(Debug, Deserialize)]
pub struct SpotBalance {
    pub coin: String,
    pub hold: String,
    pub total: String,
    #[serde(rename = "entryNtl")]
    pub entry_ntl: Option<String>,
    pub token: Option<u32>,
}

// ---------------------------------------------------------------------------
// Info API response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct AssetMeta {
    pub name: String,
    #[serde(rename = "szDecimals")]
    pub sz_decimals: u32,
}

#[derive(Debug, Deserialize)]
pub struct MetaResponse {
    pub universe: Vec<AssetMeta>,
}

#[derive(Debug, Deserialize)]
pub struct OpenOrderWire {
    pub coin: String,
    pub oid: u64,
    #[serde(default)]
    pub cloid: Option<String>,
    pub side: String,       // "A" (ask/sell) or "B" (bid/buy)
    #[serde(rename = "limitPx")]
    pub limit_px: String,
    pub sz: String,
    #[serde(rename = "origSz")]
    pub orig_sz: String,
    pub timestamp: u64,
    #[serde(default, rename = "orderType")]
    pub order_type: Option<String>,
    #[serde(default, rename = "reduceOnly")]
    pub reduce_only: Option<bool>,
    #[serde(default)]
    pub tif: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AssetPosition {
    pub position: PositionData,
    #[serde(rename = "type")]
    pub position_type: String,
}

#[derive(Debug, Deserialize)]
pub struct PositionData {
    pub coin: String,
    #[serde(rename = "entryPx")]
    pub entry_px: Option<String>,
    pub leverage: LeverageData,
    #[serde(rename = "liquidationPx")]
    pub liquidation_px: Option<String>,
    #[serde(rename = "positionValue")]
    pub position_value: String,
    #[serde(rename = "returnOnEquity")]
    pub return_on_equity: String,
    pub szi: String,
    #[serde(rename = "unrealizedPnl")]
    pub unrealized_pnl: String,
}

#[derive(Debug, Deserialize)]
pub struct LeverageData {
    #[serde(rename = "type")]
    pub leverage_type: String,
    pub value: u32,
}

#[derive(Debug, Deserialize)]
pub struct MarginSummary {
    #[serde(rename = "accountValue")]
    pub account_value: String,
    #[serde(rename = "totalMarginUsed")]
    pub total_margin_used: String,
    #[serde(rename = "totalNtlPos")]
    pub total_ntl_pos: String,
    #[serde(rename = "totalRawUsd")]
    pub total_raw_usd: String,
}

#[derive(Debug, Deserialize)]
pub struct ClearinghouseResponse {
    #[serde(rename = "assetPositions")]
    pub asset_positions: Vec<AssetPosition>,
    /// Cross-margin only
    #[serde(rename = "crossMarginSummary")]
    pub cross_margin_summary: MarginSummary,
    /// Total across cross + isolated
    #[serde(rename = "marginSummary")]
    pub margin_summary: MarginSummary,
    #[serde(rename = "withdrawable")]
    pub withdrawable: String,
}

#[derive(Debug, Deserialize)]
pub struct UserFillWire {
    pub coin: String,
    pub dir: String,       // "Open Long", "Close Long", "Open Short", "Close Short"
    pub hash: String,
    pub oid: u64,
    pub cloid: Option<String>,
    pub px: String,
    pub sz: String,
    pub fee: String,
    pub side: String,      // "A" or "B"
    pub time: u64,
    /// Unique trade ID per fill
    pub tid: u64,
    #[serde(rename = "startPosition")]
    pub start_position: String,
    #[serde(rename = "closedPnl")]
    pub closed_pnl: String,
    pub crossed: bool,     // true = taker
}

// ---------------------------------------------------------------------------
// Exchange API types (POST /exchange)
// ---------------------------------------------------------------------------

#[derive(Serialize, Clone, Debug)]
pub struct OrderWire {
    pub a: u32,            // asset index
    pub b: bool,           // is buy
    pub p: String,         // price
    pub s: String,         // size
    pub r: bool,           // reduce only
    pub t: OrderTypeWire,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub c: Option<String>, // client order id (cloid), hex encoded
}

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
pub enum OrderTypeWire {
    Limit(LimitOrderType),
    Trigger(TriggerOrderType),
}

#[derive(Serialize, Clone, Debug)]
pub struct LimitOrderType {
    pub limit: LimitInner,
}

#[derive(Serialize, Clone, Debug)]
pub struct LimitInner {
    pub tif: String, // "Gtc", "Ioc", "Alo"
}

#[derive(Serialize, Clone, Debug)]
pub struct TriggerOrderType {
    pub trigger: TriggerInner,
}

#[derive(Serialize, Clone, Debug)]
pub struct TriggerInner {
    #[serde(rename = "triggerPx")]
    pub trigger_px: String,
    #[serde(rename = "isMarket")]
    pub is_market: bool,
    pub tpsl: String, // "tp" or "sl"
}

#[derive(Serialize, Debug)]
#[serde(tag = "type")]
pub enum ExchangeAction {
    #[serde(rename = "order")]
    Order {
        orders: Vec<OrderWire>,
        grouping: String, // "na"
    },
    #[serde(rename = "cancel")]
    Cancel {
        cancels: Vec<CancelWire>,
    },
    #[serde(rename = "cancelByClid")]
    CancelByClid {
        cancels: Vec<CancelByCloidWire>,
    },
    #[serde(rename = "batchModify")]
    BatchModify {
        modifies: Vec<ModifyWire>,
    },
}

// ---------------------------------------------------------------------------
// Hash-only payload structs (no "type" tag — used for msgpack signing hash)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct BulkOrderPayload<'a> {
    pub orders: &'a [OrderWire],
    pub grouping: &'a str,
}

#[derive(Serialize)]
pub struct CancelPayload<'a> {
    pub cancels: &'a [CancelWire],
}

#[derive(Serialize)]
pub struct CancelByCloidPayload<'a> {
    pub cancels: &'a [CancelByCloidWire],
}

#[derive(Serialize)]
pub struct BatchModifyPayload<'a> {
    pub modifies: &'a [ModifyWire],
}

// ---------------------------------------------------------------------------

#[derive(Serialize, Debug)]
pub struct CancelWire {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "o")]
    pub oid: u64,
}

#[derive(Serialize, Debug)]
pub struct CancelByCloidWire {
    pub asset: u32,
    pub cloid: String,
}

#[derive(Serialize, Debug)]
pub struct ModifyWire {
    pub oid: u64,
    pub order: OrderWire,
}

#[derive(Serialize, Debug)]
pub struct ExchangeRequest {
    pub action: ExchangeAction,
    pub nonce: u64,
    pub signature: Signature,
    #[serde(rename = "vaultAddress")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_address: Option<String>,
    #[serde(rename = "expiresAfter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_after: Option<u64>,
}

#[derive(Serialize, Debug)]
pub struct Signature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

// ---------------------------------------------------------------------------
// Exchange API response
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ExchangeResponse {
    pub status: String, // "ok" or "err"
    pub response: Option<ExchangeResponseData>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ExchangeResponseData {
    #[serde(rename = "order")]
    Order { data: OrderResponseData },
    #[serde(rename = "cancel")]
    Cancel { data: CancelResponseData },
    #[serde(rename = "batchModify")]
    BatchModify { data: Vec<ModifyResponseItem> },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
pub struct OrderResponseData {
    pub statuses: Vec<OrderStatusResponse>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum OrderStatusResponse {
    Filled { filled: FilledStatus },
    Resting { resting: RestingStatus },
    Error { error: String },
}

#[derive(Debug, Deserialize)]
pub struct FilledStatus {
    #[serde(rename = "totalSz")]
    pub total_sz: String,
    #[serde(rename = "avgPx")]
    pub avg_px: String,
    pub oid: u64,
}

#[derive(Debug, Deserialize)]
pub struct RestingStatus {
    pub oid: u64,
    #[serde(rename = "cloid")]
    pub cloid: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CancelResponseData {
    pub statuses: Vec<String>, // "success" or error message
}

#[derive(Debug, Deserialize)]
pub struct ModifyResponseItem {
    pub status: String,
    pub oid: Option<u64>,
}

// ---------------------------------------------------------------------------
// WebSocket types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct WsSubscription<'a> {
    pub method: &'a str,
    pub subscription: WsChannel<'a>,
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub enum WsChannel<'a> {
    #[serde(rename = "userEvents")]
    UserEvents { user: &'a str },
    #[serde(rename = "userFills")]
    UserFills { user: &'a str },
}

#[derive(Debug, Deserialize)]
pub struct WsMessage {
    pub channel: Option<String>,
    pub data: Option<WsData>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WsData {
    UserEvents(UserEventsData),
    Raw(serde_json::Value),
}

#[derive(Debug, Deserialize)]
pub struct UserEventsData {
    #[serde(default)]
    pub fills: Vec<UserFillWire>,
    #[serde(default, rename = "orderUpdates")]
    pub order_updates: Vec<OrderUpdateWire>,
    #[serde(default, rename = "ledgerUpdates")]
    pub ledger_updates: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct OrderUpdateWire {
    pub order: OpenOrderWire,
    pub status: String,         // "open", "filled", "canceled", "triggered", "rejected", "marginCanceled"
    #[serde(rename = "statusTimestamp")]
    pub status_timestamp: u64,
}

// ---------------------------------------------------------------------------
// Asset mapping helper
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct AssetMap {
    name_to_idx: HashMap<String, u32>,
    idx_to_name: HashMap<u32, String>,
    sz_decimals: HashMap<u32, u32>,
}

impl AssetMap {
    pub fn from_meta(meta: &MetaResponse) -> Self {
        let mut name_to_idx = HashMap::new();
        let mut idx_to_name = HashMap::new();
        let mut sz_decimals = HashMap::new();

        for (i, asset) in meta.universe.iter().enumerate() {
            let idx = i as u32;
            name_to_idx.insert(asset.name.clone(), idx);
            idx_to_name.insert(idx, asset.name.clone());
            sz_decimals.insert(idx, asset.sz_decimals);
        }

        Self { name_to_idx, idx_to_name, sz_decimals }
    }

    pub fn asset_index(&self, name: &str) -> Option<u32> {
        self.name_to_idx.get(name).copied()
    }

    pub fn asset_name(&self, idx: u32) -> Option<&str> {
        self.idx_to_name.get(&idx).map(|s| s.as_str())
    }

    pub fn sz_decimals(&self, idx: u32) -> Option<u32> {
        self.sz_decimals.get(&idx).copied()
    }

    /// Convert our canonical symbol (PERP_BTC_USDT) to HL asset name (BTC)
    pub fn canonical_to_asset(symbol: &str) -> Option<&str> {
        // PERP_BTC_USDT -> BTC, PERP_ETH_USDC -> ETH
        let parts: Vec<&str> = symbol.split('_').collect();
        if parts.len() >= 2 { Some(parts[1]) } else { None }
    }

    /// Convert HL asset name (BTC) to our canonical symbol
    pub fn asset_to_canonical(coin: &str) -> String {
        // HL is perps only for now; quote is always USDC on HL
        format!("PERP_{coin}_USDC")
    }
}
