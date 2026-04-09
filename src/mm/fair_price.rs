use chrono::Utc;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crypto_feeds::market_data::InstrumentType;
use crypto_feeds::symbol_registry::{REGISTRY, SymbolId};
use crypto_feeds::{AllMarketData, MarketDataCollection};
use dashmap::DashMap;
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use super::config::FairPriceConfig;

// ---------------------------------------------------------------------------
// ExchangeId — maps to a MarketDataCollection on AllMarketData
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExchangeId {
    Binance,
    Bybit,
    Okx,
    Hyperliquid,
}

impl fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExchangeId::Binance => write!(f, "binance"),
            ExchangeId::Bybit => write!(f, "bybit"),
            ExchangeId::Okx => write!(f, "okx"),
            ExchangeId::Hyperliquid => write!(f, "hyperliquid"),
        }
    }
}

// ---------------------------------------------------------------------------
// Resolved basis pair (symbol strings → SymbolIds)
// ---------------------------------------------------------------------------

struct ResolvedPair {
    target_exchange: ExchangeId,
    target_symbol_id: SymbolId,
    reference_exchange: ExchangeId,
    reference_symbol_id: SymbolId,
    /// Cache key: "target_exchange:target_symbol:ref_exchange:ref_symbol"
    cache_key: String,
}

// ---------------------------------------------------------------------------
// FairPriceEngine
// ---------------------------------------------------------------------------

pub struct FairPriceEngine {
    market_data: Arc<AllMarketData>,
    /// pair_index → current basis EWMA (one basis per pair)
    basis: Arc<DashMap<usize, f64>>,
    /// pair_index → whether basis has been seeded
    seeded: Arc<DashMap<usize, bool>>,
    pairs: Vec<ResolvedPair>,
    config: FairPriceConfig,
}

impl FairPriceEngine {
    pub fn new(
        market_data: Arc<AllMarketData>,
        config: FairPriceConfig,
    ) -> anyhow::Result<Self> {
        let mut pairs = Vec::new();

        for pair_cfg in &config.pairs {
            let target_exchange = parse_exchange_id(&pair_cfg.target_exchange)?;
            let ref_exchange = parse_exchange_id(&pair_cfg.reference_exchange)?;

            let target_itype = parse_instrument_type(&pair_cfg.target_symbol)?;
            let target_base = strip_instrument_prefix(&pair_cfg.target_symbol);
            let target_symbol_id = *REGISTRY
                .lookup(&target_base, &target_itype)
                .ok_or_else(|| anyhow::anyhow!(
                    "symbol not found in registry: {} ({:?})",
                    target_base, target_itype
                ))?;

            let ref_itype = parse_instrument_type(&pair_cfg.reference_symbol)?;
            let ref_base = strip_instrument_prefix(&pair_cfg.reference_symbol);
            let ref_symbol_id = *REGISTRY
                .lookup(&ref_base, &ref_itype)
                .ok_or_else(|| anyhow::anyhow!(
                    "symbol not found in registry: {} ({:?})",
                    ref_base, ref_itype
                ))?;

            pairs.push(ResolvedPair {
                target_exchange,
                target_symbol_id,
                reference_exchange: ref_exchange,
                reference_symbol_id: ref_symbol_id,
                cache_key: format!(
                    "{}:{}:{}:{}",
                    pair_cfg.target_exchange, pair_cfg.target_symbol,
                    pair_cfg.reference_exchange, pair_cfg.reference_symbol,
                ),
            });
        }

        let basis: Arc<DashMap<usize, f64>> = Arc::new(DashMap::new());
        let seeded: Arc<DashMap<usize, bool>> = Arc::new(DashMap::new());

        // Load cached basis from disk
        load_basis_cache(&config.basis_cache_path, &pairs, &basis, &seeded);

        Ok(Self {
            market_data,
            basis,
            seeded,
            pairs,
            config,
        })
    }

    /// Start the background basis updater task.
    pub fn start(self: &Arc<Self>, shutdown: Arc<Notify>) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            engine.run_basis_updater(shutdown).await;
        });
    }

    /// Returns fair price using the freshest reference feed across all matching pairs.
    pub fn get_fair_price(&self, exchange: ExchangeId, symbol_id: SymbolId) -> Option<f64> {
        self.get_fair_price_with_age(exchange, symbol_id).map(|(price, _, _)| price)
    }

    /// Returns (fair_price, ref_age_ms, ref_exchange_name). Uses the freshest reference feed.
    pub fn get_fair_price_with_age(
        &self,
        exchange: ExchangeId,
        symbol_id: SymbolId,
    ) -> Option<(f64, i64, &str)> {
        let now = Utc::now();
        let mut best: Option<(f64, i64, usize)> = None; // (price, age, pair_idx)

        for (idx, pair) in self.pairs.iter().enumerate() {
            if pair.target_exchange != exchange || pair.target_symbol_id != symbol_id {
                continue;
            }

            let coll = self.collection_for(pair.reference_exchange);
            let Some(md) = coll.latest(&pair.reference_symbol_id) else { continue };
            let Some(ref_mid) = md.midquote() else { continue };

            let age_ms = md.exchange_ts
                .map(|ts| (now - ts).num_milliseconds())
                .unwrap_or(i64::MAX);

            let basis = self.basis.get(&idx).map(|v| *v).unwrap_or(0.0);
            let fair = ref_mid + basis;

            match &best {
                Some((_, best_age, _)) if age_ms < *best_age => {
                    best = Some((fair, age_ms, idx));
                }
                None => {
                    best = Some((fair, age_ms, idx));
                }
                _ => {}
            }
        }

        best.map(|(price, age, idx)| {
            let ref_name = match self.pairs[idx].reference_exchange {
                ExchangeId::Binance => "binance",
                ExchangeId::Bybit => "bybit",
                ExchangeId::Okx => "okx",
                ExchangeId::Hyperliquid => "hyperliquid",
            };
            (price, age, ref_name)
        })
    }

    /// Get the current basis estimate for the freshest pair (for diagnostics).
    pub fn get_basis(&self, exchange: ExchangeId, symbol_id: SymbolId) -> Option<f64> {
        // Return basis of the freshest pair
        self.get_fair_price_with_age(exchange, symbol_id)
            .and_then(|(_, _, _)| {
                // Find the freshest pair index
                let now = Utc::now();
                let mut best_idx = None;
                let mut best_age = i64::MAX;
                for (idx, pair) in self.pairs.iter().enumerate() {
                    if pair.target_exchange != exchange || pair.target_symbol_id != symbol_id {
                        continue;
                    }
                    let age = self.collection_for(pair.reference_exchange)
                        .latest(&pair.reference_symbol_id)
                        .and_then(|md| md.exchange_ts)
                        .map(|ts| (now - ts).num_milliseconds())
                        .unwrap_or(i64::MAX);
                    if age < best_age {
                        best_age = age;
                        best_idx = Some(idx);
                    }
                }
                best_idx.and_then(|idx| self.basis.get(&idx).map(|v| *v))
            })
    }

    /// Get the best (lowest) ref age across all matching pairs.
    pub fn get_ref_age_ms(&self, exchange: ExchangeId, symbol_id: SymbolId) -> Option<i64> {
        self.get_fair_price_with_age(exchange, symbol_id).map(|(_, age, _)| age)
    }

    /// Read live mid from any exchange (public, for diagnostics).
    pub fn get_mid(&self, exchange: ExchangeId, symbol_id: SymbolId) -> Option<f64> {
        self.collection_for(exchange).get_midquote(&symbol_id)
    }

    fn collection_for(&self, exchange: ExchangeId) -> &MarketDataCollection {
        match exchange {
            ExchangeId::Binance => &self.market_data.binance,
            ExchangeId::Bybit => &self.market_data.bybit,
            ExchangeId::Okx => &self.market_data.okx,
            ExchangeId::Hyperliquid => &self.market_data.hyperliquid,
        }
    }

    // -----------------------------------------------------------------------
    // Background basis updater
    // -----------------------------------------------------------------------

    async fn run_basis_updater(&self, shutdown: Arc<Notify>) {
        let tick = Duration::from_millis(self.config.basis_tick_ms);
        let dt_secs = self.config.basis_tick_ms as f64 / 1000.0;
        let alpha = 1.0 - (-dt_secs * std::f64::consts::LN_2 / self.config.basis_halflife_secs).exp();

        let mut interval = tokio::time::interval(tick);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut last_cache_save = std::time::Instant::now();
        let cache_save_interval = Duration::from_secs(10);

        let shutdown_fut = shutdown.notified();
        tokio::pin!(shutdown_fut);

        loop {
            tokio::select! {
                _ = &mut shutdown_fut => {
                    save_basis_cache(&self.config.basis_cache_path, &self.pairs, &self.basis);
                    return;
                }
                _ = interval.tick() => {
                    self.update_basis(alpha);

                    if last_cache_save.elapsed() >= cache_save_interval {
                        save_basis_cache(&self.config.basis_cache_path, &self.pairs, &self.basis);
                        last_cache_save = std::time::Instant::now();
                    }
                }
            }
        }
    }

    fn update_basis(&self, alpha: f64) {
        for (idx, pair) in self.pairs.iter().enumerate() {
            let target_mid = self.get_mid(pair.target_exchange, pair.target_symbol_id);
            let coll = self.collection_for(pair.reference_exchange);
            let ref_mid = coll.get_midquote(&pair.reference_symbol_id);

            let (Some(t_mid), Some(r_mid)) = (target_mid, ref_mid) else {
                continue;
            };

            let instantaneous = t_mid - r_mid;

            let is_seeded = self.seeded.get(&idx).map(|v| *v).unwrap_or(false);
            if !is_seeded {
                self.basis.insert(idx, instantaneous);
                self.seeded.insert(idx, true);
                debug!(
                    "basis seeded: pair={} ({}) = {:.6} (target={:.6}, ref={:.6})",
                    idx, pair.cache_key, instantaneous, t_mid, r_mid
                );
            } else {
                let prev = self.basis.get(&idx).map(|v| *v).unwrap_or(0.0);
                let new_val = alpha * instantaneous + (1.0 - alpha) * prev;
                self.basis.insert(idx, new_val);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_exchange_id(s: &str) -> anyhow::Result<ExchangeId> {
    match s.to_lowercase().as_str() {
        "binance" => Ok(ExchangeId::Binance),
        "bybit" => Ok(ExchangeId::Bybit),
        "okx" => Ok(ExchangeId::Okx),
        "hyperliquid" | "hl" => Ok(ExchangeId::Hyperliquid),
        _ => anyhow::bail!("unknown exchange: {s}"),
    }
}

/// Parse "PERP_BTC_USDC" → InstrumentType::Perp, or "SPOT_BTC_USDT" → InstrumentType::Spot
fn parse_instrument_type(symbol: &str) -> anyhow::Result<InstrumentType> {
    if symbol.starts_with("PERP_") {
        Ok(InstrumentType::Perp)
    } else if symbol.starts_with("SPOT_") {
        Ok(InstrumentType::Spot)
    } else {
        anyhow::bail!("cannot determine instrument type from symbol: {symbol}")
    }
}

/// "PERP_BTC_USDC" → "BTC_USDC"
fn strip_instrument_prefix(symbol: &str) -> String {
    if let Some(rest) = symbol.strip_prefix("PERP_")
        .or_else(|| symbol.strip_prefix("SPOT_"))
    {
        rest.to_string()
    } else {
        symbol.to_string()
    }
}

// ---------------------------------------------------------------------------
// Basis disk cache (keyed by pair cache_key)
// ---------------------------------------------------------------------------

fn load_basis_cache(
    path: &str,
    pairs: &[ResolvedPair],
    basis: &DashMap<usize, f64>,
    seeded: &DashMap<usize, bool>,
) {
    let contents = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return,
    };

    let cache: HashMap<String, f64> = match serde_json::from_str(&contents) {
        Ok(c) => c,
        Err(e) => {
            warn!("failed to parse basis cache {}: {e}", path);
            return;
        }
    };

    for (idx, pair) in pairs.iter().enumerate() {
        if let Some(&val) = cache.get(&pair.cache_key) {
            basis.insert(idx, val);
            seeded.insert(idx, true);
            info!("basis loaded from cache: {} = {:.6}", pair.cache_key, val);
        }
    }
}

fn save_basis_cache(
    path: &str,
    pairs: &[ResolvedPair],
    basis: &DashMap<usize, f64>,
) {
    let mut cache: HashMap<String, f64> = HashMap::new();
    for (idx, pair) in pairs.iter().enumerate() {
        if let Some(val) = basis.get(&idx) {
            cache.insert(pair.cache_key.clone(), *val);
        }
    }

    match serde_json::to_string_pretty(&cache) {
        Ok(json) => {
            if let Err(e) = std::fs::write(path, json) {
                warn!("failed to write basis cache {}: {e}", path);
            }
        }
        Err(e) => warn!("failed to serialize basis cache: {e}"),
    }
}
