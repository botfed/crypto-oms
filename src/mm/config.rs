use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;

use crate::hyperliquid::HyperliquidOmsConfig;
use crypto_feeds::app_config::AppConfig;
use crypto_feeds::market_data::ClockCorrectionConfig;

// ---------------------------------------------------------------------------
// Top-level config (YAML root)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct MmConfig {
    pub hyperliquid: HlConfig,

    #[serde(default)]
    pub perp: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub spot: HashMap<String, Vec<String>>,
    #[serde(default = "default_sample_interval")]
    pub sample_interval_ms: u64,

    pub fair_price: FairPriceConfig,
    pub inventory: InventoryConfig,
    pub strategy: StrategyConfig,
}

#[derive(Debug, Deserialize)]
pub struct HlConfig {
    pub base_url: Option<String>,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_inflight_timeout")]
    pub inflight_timeout_ms: u64,
}

fn default_sample_interval() -> u64 { 10 }
fn default_poll_interval() -> u64 { 3000 }
fn default_inflight_timeout() -> u64 { 5000 }

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
        }
    }

    pub fn to_oms_config(&self, private_key: String, account_address: String) -> HyperliquidOmsConfig {
        HyperliquidOmsConfig {
            private_key,
            account_address,
            base_url: self.hyperliquid.base_url.clone(),
            poll_interval: Duration::from_millis(self.hyperliquid.poll_interval_ms),
            inflight_timeout: Duration::from_millis(self.hyperliquid.inflight_timeout_ms),
            stray_order_age: Duration::from_millis(self.strategy.stray_order_age_ms),
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
    /// Half-spread in basis points from skewed mid to each side
    pub half_spread_bps: f64,
    /// Order size per side in USD notional (engine computes qty from notional / fair_price)
    pub order_notional_usd: f64,
    /// Max absolute position before hard pause
    pub max_position_usd: f64,
    /// Max inventory skew in bps (log formula, capped at this value)
    #[serde(default = "default_max_skew")]
    pub max_skew_bps: f64,
    /// Minimum edge from fair price in bps — quotes are clamped to not cross this
    #[serde(default = "default_min_edge_bps")]
    pub min_edge_bps: f64,
    /// Inner-side (adverse) drift in bps before fast-path cancel
    #[serde(default = "default_cancel_threshold")]
    pub cancel_threshold_bps: f64,
    /// Outer-side drift in bps before slow-path requote
    #[serde(default = "default_requote_tolerance")]
    pub requote_tolerance_bps: f64,
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
    /// Use PostOnly (true) or GTC (false) for quotes
    #[serde(default = "default_post_only")]
    pub post_only: bool,
    /// Warmup period in seconds before placing first quotes (let EWMA settle)
    #[serde(default = "default_warmup_secs")]
    pub warmup_secs: u64,
}

fn default_max_skew() -> f64 { 5.0 }
fn default_min_edge_bps() -> f64 { 0.1 }
fn default_cancel_threshold() -> f64 { 0.5 }
fn default_requote_tolerance() -> f64 { 1.5 }
fn default_quote_interval() -> u64 { 150 }
fn default_max_feed_age() -> u64 { 5_000 }
fn default_max_stale() -> u64 { 500 }
fn default_stray_age_ms() -> u64 { 1000 }
fn default_post_only() -> bool { true }
fn default_warmup_secs() -> u64 { 10 }

// ---------------------------------------------------------------------------
// MmParamSource trait
// ---------------------------------------------------------------------------

pub trait MmParamSource: Send + Sync {
    fn target_position_usd(&self) -> f64;
    fn half_spread_bps(&self) -> f64;
    fn order_notional_usd(&self) -> f64;
    fn max_position_usd(&self) -> f64;
    fn enabled(&self) -> bool;
}

// ---------------------------------------------------------------------------
// WatchParams — MmParamSource backed by watch channels
// ---------------------------------------------------------------------------

pub struct WatchParams {
    target_position_usd: watch::Receiver<f64>,
    half_spread_bps: watch::Receiver<f64>,
    order_notional_usd: watch::Receiver<f64>,
    max_position_usd: watch::Receiver<f64>,
    enabled: watch::Receiver<bool>,
}

/// Controller side — hand this to external tasks to update params at runtime.
pub struct WatchParamController {
    pub target_position_usd: watch::Sender<f64>,
    pub half_spread_bps: watch::Sender<f64>,
    pub order_notional_usd: watch::Sender<f64>,
    pub max_position_usd: watch::Sender<f64>,
    pub enabled: watch::Sender<bool>,
}

impl WatchParams {
    /// Create from strategy config + a pre-built target_position receiver
    /// (target comes from the inventory manager, not from strategy config).
    pub fn new(
        config: &StrategyConfig,
        target_rx: watch::Receiver<f64>,
    ) -> (Self, WatchParamController) {
        let (spread_tx, spread_rx) = watch::channel(config.half_spread_bps);
        let (size_tx, size_rx) = watch::channel(config.order_notional_usd);
        let (max_tx, max_rx) = watch::channel(config.max_position_usd);
        let (en_tx, en_rx) = watch::channel(true);

        // We need a sender for target_position too, so the controller is complete.
        // But the *real* driver is the inventory manager which owns the original sender.
        // We just expose a dummy sender here that's not connected to the rx.
        // Instead, the controller's target_position sender is separate.
        let (target_tx, _) = watch::channel(0.0);

        let params = WatchParams {
            target_position_usd: target_rx,
            half_spread_bps: spread_rx,
            order_notional_usd: size_rx,
            max_position_usd: max_rx,
            enabled: en_rx,
        };

        let controller = WatchParamController {
            target_position_usd: target_tx,
            half_spread_bps: spread_tx,
            order_notional_usd: size_tx,
            max_position_usd: max_tx,
            enabled: en_tx,
        };

        (params, controller)
    }
}

impl MmParamSource for WatchParams {
    fn target_position_usd(&self) -> f64 {
        *self.target_position_usd.borrow()
    }

    fn half_spread_bps(&self) -> f64 {
        *self.half_spread_bps.borrow()
    }

    fn order_notional_usd(&self) -> f64 {
        *self.order_notional_usd.borrow()
    }

    fn max_position_usd(&self) -> f64 {
        *self.max_position_usd.borrow()
    }

    fn enabled(&self) -> bool {
        *self.enabled.borrow()
    }
}
