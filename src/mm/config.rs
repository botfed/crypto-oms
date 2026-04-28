use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

use crate::hibachi::HibachiOmsConfig;
use crate::hyperliquid::HyperliquidOmsConfig;
use crypto_feeds::app_config::AppConfig;
use crypto_feeds::market_data::ClockCorrectionConfig;

// ---------------------------------------------------------------------------
// Top-level config (YAML root)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct MmConfig {
    /// Target exchange: "hyperliquid" or "hibachi"
    #[serde(default = "default_exchange")]
    pub exchange: String,

    #[serde(default)]
    pub hyperliquid: Option<HlConfig>,
    #[serde(default)]
    pub hibachi: Option<HibachiExchangeConfig>,

    #[serde(default)]
    pub perp: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub spot: HashMap<String, Vec<String>>,
    #[serde(default = "default_sample_interval")]
    pub sample_interval_ms: u64,

    pub fair_price: FairPriceConfig,
    pub inventory: InventoryConfig,
    pub symbols: Vec<StrategyConfig>,

    /// Global default: USD deviation from target at which inventory skew hits max_skew_bps
    #[serde(default = "default_skew_scale_usd")]
    pub skew_scale_usd: f64,

    /// Global default: use PostOnly (true) or GTC (false) for quotes
    #[serde(default = "default_post_only")]
    pub post_only: bool,

    /// Global defaults for spread/edge params (per-symbol overrides if set)
    #[serde(default = "default_ref_half_spread")]
    pub ref_half_spread_bps: f64,
    #[serde(default = "default_ref_min_spread")]
    pub ref_min_spread_bps: f64,
    #[serde(default = "default_max_skew")]
    pub max_skew_bps: f64,
    #[serde(default = "default_min_edge_bps")]
    pub min_edge_bps: f64,
    #[serde(default = "default_requote_tolerance")]
    pub ref_requote_tolerance_bps: f64,

    /// Optional vol model config for vol-adjusted spreads
    #[serde(default)]
    pub vol_models: Option<crypto_feeds::app_config::VolModelConfig>,
}

#[derive(Debug, Deserialize)]
pub struct HlConfig {
    pub base_url: Option<String>,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_inflight_timeout")]
    pub inflight_timeout_ms: u64,
}

fn default_exchange() -> String { "hyperliquid".to_string() }
fn default_sample_interval() -> u64 { 10 }
fn default_poll_interval() -> u64 { 3000 }
fn default_inflight_timeout() -> u64 { 5000 }
fn default_max_fees_percent() -> f64 { 0.001 }

#[derive(Debug, Deserialize)]
pub struct HibachiExchangeConfig {
    pub api_url: Option<String>,
    pub data_api_url: Option<String>,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_inflight_timeout")]
    pub inflight_timeout_ms: u64,
    #[serde(default = "default_max_fees_percent")]
    pub max_fees_percent: f64,
}

impl MmConfig {
    pub fn to_feeds_config(&self) -> AppConfig {
        AppConfig {
            spot: self.spot.clone(),
            perp: self.perp.clone(),
            sample_interval_ms: self.sample_interval_ms,
            onchain: None,
            fair_price: Default::default(),
            vol_models: None,
            clock_correction: ClockCorrectionConfig::default(),
            trades: Default::default(),
        }
    }

    pub fn to_hl_oms_config(&self, private_key: String, account_address: String) -> HyperliquidOmsConfig {
        let hl = self.hyperliquid.as_ref().expect("hyperliquid config required");
        HyperliquidOmsConfig {
            private_key,
            account_address,
            base_url: hl.base_url.clone(),
            poll_interval: Duration::from_millis(hl.poll_interval_ms),
            inflight_timeout: Duration::from_millis(hl.inflight_timeout_ms),
            stray_order_age: Duration::from_millis(
                self.symbols.first().map(|s| s.stray_order_age_ms).unwrap_or(5000)
            ),
        }
    }

    pub fn to_hibachi_oms_config(&self, api_key: String, private_key: String, account_id: u64) -> HibachiOmsConfig {
        let hb = self.hibachi.as_ref().expect("hibachi config required");
        let stray = Duration::from_millis(
            self.symbols.first().map(|s| s.stray_order_age_ms).unwrap_or(5000)
        );
        HibachiOmsConfig {
            api_key,
            private_key,
            account_id,
            api_url: hb.api_url.clone(),
            data_api_url: hb.data_api_url.clone(),
            poll_interval: Duration::from_millis(hb.poll_interval_ms),
            inflight_timeout: Duration::from_millis(hb.inflight_timeout_ms),
            stray_order_age: stray,
            max_fees_percent: hb.max_fees_percent,
        }
    }
}

// ---------------------------------------------------------------------------
// Fair price config
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct FairPriceConfig {
    /// EWMA halflife for basis tracking, in seconds
    #[serde(default = "default_basis_halflife")]
    pub basis_halflife_secs: f64,
    /// How often to update the basis EWMA (ms)
    #[serde(default = "default_basis_tick")]
    pub basis_tick_ms: u64,
    /// Venue pairs for basis tracking
    pub pairs: Vec<BasisPairConfig>,
    /// Path to cache basis values on disk (JSON). Survives restarts.
    #[serde(default = "default_basis_cache_path")]
    pub basis_cache_path: String,
}

fn default_basis_cache_path() -> String { "/tmp/mm_basis_cache.json".to_string() }

#[derive(Debug, Clone, Deserialize)]
pub struct BasisPairConfig {
    pub target_exchange: String,
    pub target_symbol: String,
    pub reference_exchange: String,
    pub reference_symbol: String,
}

fn default_basis_halflife() -> f64 { 30.0 }
fn default_basis_tick() -> u64 { 100 }

// ---------------------------------------------------------------------------
// Inventory config
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct InventoryConfig {
    pub mode: InventoryMode,
    #[serde(default)]
    pub static_target: StaticTargetConfig,
    #[serde(default)]
    pub hedge: HedgeConfig,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InventoryMode {
    Static,
    Hedge,
}

#[derive(Debug, Default, Deserialize)]
pub struct StaticTargetConfig {
    #[serde(default)]
    pub target: f64,
}

#[derive(Debug, Default, Deserialize)]
pub struct HedgeConfig {
    #[serde(default)]
    pub exposure_file: String,
    #[serde(default = "default_hedge_poll")]
    pub poll_ms: u64,
    /// Map canonical base asset (e.g. "BTC") → list of token addresses to sum
    #[serde(default)]
    pub mappings: HashMap<String, Vec<String>>,
}

fn default_hedge_poll() -> u64 { 100 }

// ---------------------------------------------------------------------------
// Strategy config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    /// HL symbol to market-make, e.g. "PERP_BTC_USDC"
    pub symbol: String,
    /// Order size per side in USD notional (engine computes qty from notional / fair_price)
    pub order_notional_usd: f64,
    /// Max absolute position before hard pause
    pub max_position_usd: f64,
    /// Max inventory skew in bps (linear ramp, clamped at this value)
    #[serde(default)]
    pub max_skew_bps: Option<f64>,
    /// USD deviation from target at which skew hits max_skew_bps (overrides global)
    #[serde(default)]
    pub skew_scale_usd: Option<f64>,
    /// Absolute minimum edge from fair in bps — hard floor, never quotes closer
    #[serde(default)]
    pub min_edge_bps: Option<f64>,
    /// Quote placement distance from skewed mid in bps (at ref_vol)
    #[serde(default)]
    pub ref_half_spread_bps: Option<f64>,
    /// Cancel line: min distance from fair in bps (at ref_vol). Fast cancel fires inside this.
    #[serde(default)]
    pub ref_min_spread_bps: Option<f64>,
    /// Outer-side drift in bps before slow-path requote (at ref_vol, scaled by vol_mult)
    #[serde(default)]
    pub ref_requote_tolerance_bps: Option<f64>,
    // -- Vol adjustment --
    /// Mean annualized vol — calibration anchor for spread scaling
    #[serde(default = "default_ref_vol")]
    pub ref_vol: f64,
    /// Min vol multiplier (default 0.3)
    #[serde(default = "default_vol_mult_floor")]
    pub vol_mult_floor: f64,
    /// Max vol multiplier (default 5.0)
    #[serde(default = "default_vol_mult_cap")]
    pub vol_mult_cap: f64,
    /// Symbol in the vol engine (e.g., "AIXBT")
    #[serde(default)]
    pub vol_symbol: String,
    /// Slow-path cadence for quote evaluation (ms)
    #[serde(default = "default_quote_interval")]
    pub quote_interval_ms: u64,
    /// Pause engine if feed older than this (ms) — feed is dead
    #[serde(default = "default_max_feed_age")]
    pub max_feed_age_ms: u64,
    /// Cancel all quotes if ref feed age exceeds this (ms) — trading staleness
    #[serde(default = "default_max_stale")]
    pub max_stale_ms: u64,
    /// Minimum age (ms) before an unrecognized order is treated as stray and cancelled
    #[serde(default = "default_stray_age_ms")]
    pub stray_order_age_ms: u64,
    /// Use PostOnly (true) or GTC (false) for quotes (overrides global)
    #[serde(default)]
    pub post_only: Option<bool>,
    /// Warmup period in seconds before placing first quotes (let EWMA settle)
    #[serde(default = "default_warmup_secs")]
    pub warmup_secs: u64,
    /// Optional multi-factor correlation model for fair price adjustment
    #[serde(default)]
    pub factor_model: Option<FactorModelConfig>,
}

// ---------------------------------------------------------------------------
// Factor model config (cross-instrument correlation)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct FactorModelConfig {
    #[serde(default)]
    pub enabled: bool,
    pub factors: Vec<FactorConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FactorConfig {
    /// Factor symbol, e.g. "PERP_ETH_USDC"
    pub symbol: String,
    /// Exchanges to listen for this factor (picks freshest mid)
    pub exchanges: Vec<String>,
    /// Beta coefficient from regression
    pub beta: f64,
}

fn default_max_skew() -> f64 { 5.0 }
fn default_skew_scale_usd() -> f64 { 100.0 }
fn default_min_edge_bps() -> f64 { 0.1 }
fn default_requote_tolerance() -> f64 { 1.5 }
fn default_ref_vol() -> f64 { 1.0 }
fn default_vol_mult_floor() -> f64 { 0.3 }
fn default_vol_mult_cap() -> f64 { 5.0 }
fn default_quote_interval() -> u64 { 150 }
fn default_max_feed_age() -> u64 { 5_000 }
fn default_max_stale() -> u64 { 500 }
fn default_stray_age_ms() -> u64 { 1000 }
fn default_post_only() -> bool { true }
fn default_warmup_secs() -> u64 { 10 }

