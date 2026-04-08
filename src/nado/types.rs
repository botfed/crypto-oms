use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const X18: f64 = 1e18;

pub fn parse_x18(s: &str) -> Option<f64> {
    s.parse::<f64>().ok().map(|v| v / X18)
}

pub fn to_x18(v: f64) -> String {
    format!("{:.0}", v * X18)
}

/// Construct the 32-byte sender: 20-byte EOA address + 12-byte subaccount name (right-padded)
pub fn build_sender(address: &str, subaccount_name: &str) -> String {
    let addr = address.strip_prefix("0x").unwrap_or(address);
    // Pad subaccount name to 12 bytes (24 hex chars), right-padded with zeros
    let name_bytes = subaccount_name.as_bytes();
    let mut sub_hex = String::with_capacity(24);
    for &b in name_bytes.iter().take(12) {
        sub_hex.push_str(&format!("{b:02x}"));
    }
    // Pad remaining with zeros
    while sub_hex.len() < 24 {
        sub_hex.push('0');
    }
    format!("0x{addr}{sub_hex}")
}

/// Build verifying contract for order signing: product_id as 20-byte big-endian address
pub fn order_verifying_contract(product_id: u32) -> String {
    format!("0x{product_id:040x}")
}

// ---------------------------------------------------------------------------
// Nonce construction: 44 MSBs = recv_time_ms, 20 LSBs = random
// ---------------------------------------------------------------------------

pub fn make_nonce(recv_time_ms: u64, random_bits: u32) -> u64 {
    ((recv_time_ms + 90_000) << 20) | (random_bits as u64 & 0xFFFFF)
}

// ---------------------------------------------------------------------------
// Order appendix (128-bit encoded as string)
// ---------------------------------------------------------------------------

pub fn build_appendix(
    order_type: NadoOrderType,
    reduce_only: bool,
    isolated: bool,
) -> u128 {
    let version: u128 = 1;                           // bits 0-7
    let iso_bit: u128 = if isolated { 1 } else { 0 }; // bit 8
    let ot_bits: u128 = match order_type {             // bits 9-10
        NadoOrderType::Default => 0,
        NadoOrderType::IOC => 1,
        NadoOrderType::FOK => 2,
        NadoOrderType::PostOnly => 3,
    };
    let ro_bit: u128 = if reduce_only { 1 } else { 0 }; // bit 11

    version | (iso_bit << 8) | (ot_bits << 9) | (ro_bit << 11)
}

#[derive(Debug, Clone, Copy)]
pub enum NadoOrderType {
    Default,
    IOC,
    FOK,
    PostOnly,
}

// ---------------------------------------------------------------------------
// Gateway WebSocket messages (outgoing)
// ---------------------------------------------------------------------------

#[derive(Serialize, Debug)]
pub struct PlaceOrderMsg {
    pub place_order: PlaceOrderPayload,
}

#[derive(Serialize, Debug)]
pub struct PlaceOrderPayload {
    pub product_id: u32,
    pub order: OrderFields,
    pub signature: String,
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spot_leverage: Option<bool>,
}

#[derive(Serialize, Debug)]
pub struct OrderFields {
    pub sender: String,
    #[serde(rename = "priceX18")]
    pub price_x18: String,
    pub amount: String,
    pub expiration: String,
    pub nonce: String,
    pub appendix: String,
}

#[derive(Serialize, Debug)]
pub struct CancelOrdersMsg {
    pub cancel_orders: CancelOrdersPayload,
}

#[derive(Serialize, Debug)]
pub struct CancelOrdersPayload {
    pub tx: CancelTx,
    pub signature: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
}

#[derive(Serialize, Debug)]
pub struct CancelTx {
    pub sender: String,
    #[serde(rename = "productIds")]
    pub product_ids: Vec<u32>,
    pub digests: Vec<String>,
    pub nonce: String,
}

// ---------------------------------------------------------------------------
// Gateway query messages
// ---------------------------------------------------------------------------

#[derive(Serialize, Debug)]
#[serde(tag = "type")]
pub enum QueryRequest {
    #[serde(rename = "subaccount_info")]
    SubaccountInfo { subaccount: String },
    #[serde(rename = "contracts")]
    Contracts,
}

// ---------------------------------------------------------------------------
// Gateway responses
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct GatewayResponse {
    pub status: String,
    pub data: Option<serde_json::Value>,
    pub error: Option<String>,
    pub error_code: Option<i64>,
    pub request_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct PlaceOrderData {
    pub digest: String,
}

#[derive(Debug, Deserialize)]
pub struct ContractsData {
    pub chain_id: String,
    pub endpoint_addr: String,
}

// ---------------------------------------------------------------------------
// Linked signer response
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct LinkedSignerInfo {
    pub linked_signer: String,
}

// ---------------------------------------------------------------------------
// Subaccount info response
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct SubaccountInfo {
    pub subaccount: String,
    pub exists: bool,
    #[serde(default)]
    pub spot_balances: Vec<SpotBalanceEntry>,
    #[serde(default)]
    pub perp_balances: Vec<PerpBalanceEntry>,
    #[serde(default)]
    pub spot_products: Vec<ProductInfo>,
    #[serde(default)]
    pub perp_products: Vec<ProductInfo>,
}

#[derive(Debug, Deserialize)]
pub struct ProductInfo {
    pub product_id: u32,
    pub oracle_price_x18: String,
}

#[derive(Debug, Deserialize)]
pub struct SpotBalanceEntry {
    pub product_id: u32,
    pub balance: SpotBalance,
}

#[derive(Debug, Deserialize)]
pub struct SpotBalance {
    pub amount: String,
}

#[derive(Debug, Deserialize)]
pub struct PerpBalanceEntry {
    pub product_id: u32,
    pub balance: PerpBalance,
}

#[derive(Debug, Deserialize)]
pub struct PerpBalance {
    pub amount: String,
    pub v_quote_balance: String,
    #[serde(default)]
    pub last_cumulative_funding_x18: Option<String>,
}

// ---------------------------------------------------------------------------
// Subscription messages
// ---------------------------------------------------------------------------

#[derive(Serialize, Debug)]
pub struct SubscribeMsg {
    pub method: String,
    pub stream: serde_json::Value,
    pub id: u64,
}

#[derive(Serialize, Debug)]
pub struct AuthenticateMsg {
    pub method: String,
    pub id: u64,
    pub tx: AuthTx,
    pub signature: String,
}

#[derive(Serialize, Debug)]
pub struct AuthTx {
    pub sender: String,
    pub expiration: String,
}

// ---------------------------------------------------------------------------
// Subscription events (incoming)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum NadoEvent {
    #[serde(rename = "order_update")]
    OrderUpdate {
        timestamp: String,
        product_id: u32,
        digest: String,
        amount: String,    // unfilled qty, "0" if complete
        reason: String,    // "placed", "filled", "cancelled"
        #[serde(default)]
        id: Option<u64>,
    },
    #[serde(rename = "fill")]
    Fill {
        timestamp: String,
        product_id: u32,
        subaccount: String,
        order_digest: String,
        #[serde(default)]
        appendix: Option<String>,
        filled_qty: String,
        remaining_qty: String,
        original_qty: String,
        price: String,
        is_taker: bool,
        is_bid: bool,
        fee: String,
        #[serde(default)]
        submission_idx: Option<u64>,
        #[serde(default)]
        id: Option<u64>,
    },
    #[serde(rename = "position_change")]
    PositionChange {
        timestamp: String,
        product_id: u32,
        subaccount: String,
        #[serde(default)]
        isolated: bool,
        amount: String,
        #[serde(default)]
        v_quote_amount: Option<String>,
        reason: String,
    },
}

// ---------------------------------------------------------------------------
// Product mapping (from /v2/pairs)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct PairRow {
    pub product_id: u32,
    pub base: String,
}

#[derive(Debug, Clone)]
pub struct ProductMap {
    name_to_id: HashMap<String, u32>,
    id_to_name: HashMap<u32, String>,
}

impl ProductMap {
    pub fn from_pairs(pairs: &[PairRow]) -> Self {
        let mut name_to_id = HashMap::new();
        let mut id_to_name = HashMap::new();
        for p in pairs {
            name_to_id.insert(p.base.clone(), p.product_id);
            id_to_name.insert(p.product_id, p.base.clone());
        }
        Self { name_to_id, id_to_name }
    }

    pub fn product_id(&self, base: &str) -> Option<u32> {
        self.name_to_id.get(base).copied()
    }

    pub fn product_name(&self, id: u32) -> Option<&str> {
        self.id_to_name.get(&id).map(|s| s.as_str())
    }

    /// PERP_BTC_USDT -> "BTC-PERP"
    pub fn canonical_to_native(symbol: &str) -> Option<String> {
        let parts: Vec<&str> = symbol.split('_').collect();
        if parts.len() >= 2 {
            Some(format!("{}-PERP", parts[1]))
        } else {
            None
        }
    }

    /// "BTC-PERP" -> PERP_BTC_USDT
    pub fn native_to_canonical(native: &str) -> String {
        let base = native.split('-').next().unwrap_or(native);
        format!("PERP_{base}_USDT")
    }

    /// product_id -> canonical symbol
    pub fn id_to_canonical(&self, id: u32) -> Option<String> {
        self.product_name(id).map(|n| Self::native_to_canonical(n))
    }
}
