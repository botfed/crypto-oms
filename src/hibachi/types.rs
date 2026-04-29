use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Exchange info (GET /market/exchange-info)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ExchangeInfoResponse {
    #[serde(rename = "futureContracts")]
    pub future_contracts: Vec<FutureContract>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FutureContract {
    pub id: u32,
    pub symbol: String,
    #[serde(rename = "underlyingDecimals")]
    pub underlying_decimals: u32,
    #[serde(rename = "settlementDecimals")]
    pub settlement_decimals: u32,
    #[serde(rename = "tickSize")]
    pub tick_size: Option<String>,
    #[serde(rename = "lotSize")]
    pub lot_size: Option<String>,
}

// ---------------------------------------------------------------------------
// Account info (GET /trade/account/info)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct AccountInfoResponse {
    pub balance: String,
    #[serde(default)]
    pub positions: Vec<HibachiPosition>,
    #[serde(rename = "totalPositionNotional", default)]
    pub total_position_notional: Option<String>,
    #[serde(rename = "totalUnrealizedPnl", default)]
    pub total_unrealized_pnl: Option<String>,
    #[serde(rename = "maximalWithdraw", default)]
    pub maximal_withdraw: Option<String>,
    #[serde(rename = "tradeMakerFeeRate", default)]
    pub trade_maker_fee_rate: Option<String>,
    #[serde(rename = "tradeTakerFeeRate", default)]
    pub trade_taker_fee_rate: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HibachiPosition {
    pub symbol: String,
    pub direction: String, // "Long" or "Short"
    pub quantity: String,
    #[serde(rename = "openPrice")]
    pub open_price: String,
    #[serde(rename = "markPrice", default)]
    pub mark_price: Option<String>,
    #[serde(rename = "notionalValue", default)]
    pub notional_value: Option<String>,
    #[serde(rename = "entryNotional", default)]
    pub entry_notional: Option<String>,
    #[serde(rename = "unrealizedTradingPnl", default)]
    pub unrealized_trading_pnl: Option<String>,
    #[serde(rename = "unrealizedFundingPnl", default)]
    pub unrealized_funding_pnl: Option<String>,
}

// ---------------------------------------------------------------------------
// Order types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct HibachiOrder {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "accountId", deserialize_with = "de_string_or_u64")]
    pub account_id: u64,
    pub symbol: String,
    #[serde(rename = "orderType")]
    pub order_type: String,
    pub side: String,
    pub status: String,
    pub price: Option<String>,
    #[serde(rename = "availableQuantity")]
    pub available_quantity: Option<String>,
    #[serde(rename = "totalQuantity")]
    pub total_quantity: Option<String>,
    #[serde(rename = "triggerPrice")]
    pub trigger_price: Option<String>,
    #[serde(rename = "creationTime", default, deserialize_with = "de_opt_string_or_u64")]
    pub creation_time: Option<u64>,
    #[serde(rename = "finishTime", default, deserialize_with = "de_opt_string_or_u64")]
    pub finish_time: Option<u64>,
    #[serde(rename = "orderFlags")]
    pub order_flags: Option<String>,
}

fn de_string_or_u64<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    use serde::de;
    struct Visitor;
    impl<'de> de::Visitor<'de> for Visitor {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("u64 or string-encoded u64")
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<u64, E> { Ok(v) }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<u64, E> {
            v.parse().map_err(de::Error::custom)
        }
    }
    deserializer.deserialize_any(Visitor)
}

fn de_opt_string_or_u64<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    use serde::de;
    struct Visitor;
    impl<'de> de::Visitor<'de> for Visitor {
        type Value = Option<u64>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("null, u64, or string-encoded u64")
        }
        fn visit_none<E: de::Error>(self) -> Result<Option<u64>, E> { Ok(None) }
        fn visit_unit<E: de::Error>(self) -> Result<Option<u64>, E> { Ok(None) }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Option<u64>, E> { Ok(Some(v)) }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Option<u64>, E> {
            v.parse().map(Some).map_err(de::Error::custom)
        }
    }
    deserializer.deserialize_any(Visitor)
}

/// Response to POST /trade/order
#[derive(Debug, Deserialize)]
pub struct PlaceOrderResponse {
    #[serde(rename = "orderId")]
    pub order_id: String,
}

// ---------------------------------------------------------------------------
// Account trades (GET /trade/account/trades)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct AccountTrade {
    #[serde(rename = "orderId")]
    pub order_id: String,
    pub symbol: String,
    pub side: String,
    pub price: String,
    pub quantity: String,
    pub fee: Option<String>,
    pub timestamp: Option<u64>,
    #[serde(rename = "tradeId")]
    pub trade_id: Option<String>,
}

// ---------------------------------------------------------------------------
// WebSocket Trade RPC types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct WsRpcRequest {
    pub id: u64,
    pub method: String,
    pub params: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WsRpcResponse {
    pub id: Option<u64>,
    pub status: Option<u32>,
    pub result: Option<serde_json::Value>,
    #[serde(default)]
    pub error: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// WebSocket Account types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct WsAccountRequest {
    pub id: u64,
    pub method: String,
    pub params: serde_json::Value,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize)]
pub struct WsAccountResponse {
    pub id: Option<u64>,
    pub status: Option<u32>,
    pub result: Option<serde_json::Value>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub data: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Symbol helpers
// ---------------------------------------------------------------------------

/// Convert canonical symbol (PERP_BTC_USDT) to hibachi native (BTC/USDT-P)
pub fn canonical_to_hibachi(canonical: &str) -> String {
    let parts: Vec<&str> = canonical.split('_').collect();
    match parts.as_slice() {
        ["PERP", base, quote] => format!("{base}/{quote}-P"),
        [base, quote] => format!("{base}/{quote}-P"),
        _ => canonical.to_string(),
    }
}

/// Convert hibachi native (BTC/USDT-P) to canonical (PERP_BTC_USDT)
pub fn hibachi_to_canonical(native: &str) -> String {
    let stripped = native.strip_suffix("-P").unwrap_or(native);
    let parts: Vec<&str> = stripped.split('/').collect();
    match parts.as_slice() {
        [base, quote] => format!("PERP_{base}_{quote}"),
        _ => native.to_string(),
    }
}
