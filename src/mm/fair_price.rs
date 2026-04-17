use chrono::Utc;
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crypto_feeds::market_data::InstrumentType;
use crypto_feeds::symbol_registry::{REGISTRY, SymbolId};
use crypto_feeds::{AllMarketData, MarketDataCollection};
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
    /// pair_index → current basis EWMA. Fixed size, set at construction.
    basis: Box<[Cell<f64>]>,
    /// pair_index → whether basis has been seeded
    seeded: Box<[Cell<bool>]>,
    /// Pre-computed EWMA alpha
    alpha: f64,
    pairs: Vec<ResolvedPair>,
    config: FairPriceConfig,
}

// Safety: FairPriceEngine is shared via Arc but only mutated (Cell writes)
// from the single engine thread. Cell is !Sync by default, but our usage is
// single-threaded — the engine spin loop is the only caller of update_basis()
// and the only reader of basis values.
unsafe impl Sync for FairPriceEngine {}

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

        let n = pairs.len();
        let basis: Box<[Cell<f64>]> = (0..n).map(|_| Cell::new(0.0)).collect();
        let seeded: Box<[Cell<bool>]> = (0..n).map(|_| Cell::new(false)).collect();

        // Pre-compute alpha from config
        let dt_secs = config.basis_tick_ms as f64 / 1000.0;
        let alpha = 1.0 - (-dt_secs * std::f64::consts::LN_2 / config.basis_halflife_secs).exp();

        // Load cached basis from disk
        load_basis_cache(&config.basis_cache_path, &pairs, &basis, &seeded);

        Ok(Self {
            market_data,
            basis,
            seeded,
            alpha,
            pairs,
            config,
        })
    }

    // -----------------------------------------------------------------------
    // Read methods — called from engine hot path
    // -----------------------------------------------------------------------

    /// Returns fair price using the freshest reference feed across all matching pairs.
    pub fn get_fair_price(&self, exchange: ExchangeId, symbol_id: SymbolId) -> Option<f64> {
        self.get_fair_price_with_age(exchange, symbol_id).map(|(price, _, _, _)| price)
    }

    /// Returns (fair_price, exchange_ts_ms, ref_exchange_name, received_instant).
    /// Picks the feed with the freshest exchange_ts.
    pub fn get_fair_price_with_age(
        &self,
        exchange: ExchangeId,
        symbol_id: SymbolId,
    ) -> Option<(f64, i64, &str, Option<std::time::Instant>)> {
        let mut best: Option<(f64, i64, usize, Option<std::time::Instant>)> = None;
        let mut best_ts = i64::MIN;

        for (idx, pair) in self.pairs.iter().enumerate() {
            if pair.target_exchange != exchange || pair.target_symbol_id != symbol_id {
                continue;
            }

            let coll = self.collection_for(pair.reference_exchange);
            let Some(md) = coll.latest(&pair.reference_symbol_id) else { continue };
            let Some(ref_mid) = md.midquote() else { continue };

            let exchange_ts_ms = md.exchange_ts
                .map(|ts| ts.timestamp_millis())
                .unwrap_or(0);

            if exchange_ts_ms > best_ts {
                let basis = self.basis[idx].get();
                best = Some((ref_mid + basis, exchange_ts_ms, idx, md.received_instant));
                best_ts = exchange_ts_ms;
            }
        }

        best.map(|(price, ex_ts, idx, received_instant)| {
            let ref_name = match self.pairs[idx].reference_exchange {
                ExchangeId::Binance => "binance",
                ExchangeId::Bybit => "bybit",
                ExchangeId::Okx => "okx",
                ExchangeId::Hyperliquid => "hyperliquid",
            };
            (price, ex_ts, ref_name, received_instant)
        })
    }

    /// Get the current basis estimate for the freshest pair (for diagnostics).
    pub fn get_basis(&self, exchange: ExchangeId, symbol_id: SymbolId) -> Option<f64> {
        self.get_fair_price_with_age(exchange, symbol_id)
            .and_then(|(_, _, _, _)| {
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
                best_idx.map(|idx| self.basis[idx].get())
            })
    }

    /// Get the best (lowest) exchange_ts age in ms across all matching pairs.
    pub fn get_ref_age_ms(&self, exchange: ExchangeId, symbol_id: SymbolId) -> Option<i64> {
        let now = Utc::now();
        let mut best = i64::MAX;
        for pair in &self.pairs {
            if pair.target_exchange != exchange || pair.target_symbol_id != symbol_id {
                continue;
            }
            let coll = self.collection_for(pair.reference_exchange);
            if let Some(md) = coll.latest(&pair.reference_symbol_id) {
                if let Some(ts) = md.exchange_ts {
                    let age = (now - ts).num_milliseconds();
                    if age < best { best = age; }
                }
            }
        }
        if best == i64::MAX { None } else { Some(best) }
    }

    /// Age in nanoseconds since the winning ref feed's received_ts.
    pub fn get_received_age_ns(&self, exchange: ExchangeId, symbol_id: SymbolId) -> Option<u64> {
        let now = Utc::now();
        let mut best_age_ns: Option<u64> = None;
        let mut best_exchange_age = i64::MAX;

        for pair in &self.pairs {
            if pair.target_exchange != exchange || pair.target_symbol_id != symbol_id {
                continue;
            }
            let coll = self.collection_for(pair.reference_exchange);
            let Some(md) = coll.latest(&pair.reference_symbol_id) else { continue };

            let ex_age = md.exchange_ts
                .map(|ts| (now - ts).num_milliseconds())
                .unwrap_or(i64::MAX);

            if ex_age < best_exchange_age {
                best_exchange_age = ex_age;
                best_age_ns = md.received_ts
                    .map(|ts| (now - ts).num_nanoseconds().unwrap_or(0).max(0) as u64);
            }
        }
        best_age_ns
    }

    /// Sum of write_counts across all ref feeds for this target.
    pub fn ref_write_count(&self, exchange: ExchangeId, symbol_id: SymbolId) -> u64 {
        let mut total = 0u64;
        for pair in &self.pairs {
            if pair.target_exchange == exchange && pair.target_symbol_id == symbol_id {
                total += self.collection_for(pair.reference_exchange)
                    .write_count(&pair.reference_symbol_id);
            }
        }
        total
    }

    /// Read live mid from any exchange (for diagnostics).
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
    // Basis update — called by engine from slow path
    // -----------------------------------------------------------------------

    /// Update EWMA basis for all pairs. Called by the engine at its chosen cadence.
    pub fn update_basis(&self) {
        for (idx, pair) in self.pairs.iter().enumerate() {
            let target_mid = self.get_mid(pair.target_exchange, pair.target_symbol_id);
            let coll = self.collection_for(pair.reference_exchange);
            let ref_mid = coll.get_midquote(&pair.reference_symbol_id);

            let (Some(t_mid), Some(r_mid)) = (target_mid, ref_mid) else {
                continue;
            };

            let instantaneous = t_mid - r_mid;

            if !self.seeded[idx].get() {
                self.basis[idx].set(instantaneous);
                self.seeded[idx].set(true);
                debug!(
                    "basis seeded: pair={} ({}) = {:.6} (target={:.6}, ref={:.6})",
                    idx, pair.cache_key, instantaneous, t_mid, r_mid
                );
            } else {
                let prev = self.basis[idx].get();
                let new_val = self.alpha * instantaneous + (1.0 - self.alpha) * prev;
                self.basis[idx].set(new_val);
            }
        }
    }

    /// Save basis cache to disk.
    pub fn save_basis_cache(&self) {
        save_basis_cache(&self.config.basis_cache_path, &self.pairs, &self.basis);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) fn parse_exchange_id(s: &str) -> anyhow::Result<ExchangeId> {
    match s.to_lowercase().as_str() {
        "binance" => Ok(ExchangeId::Binance),
        "bybit" => Ok(ExchangeId::Bybit),
        "okx" => Ok(ExchangeId::Okx),
        "hyperliquid" | "hl" => Ok(ExchangeId::Hyperliquid),
        _ => anyhow::bail!("unknown exchange: {s}"),
    }
}

fn parse_instrument_type(symbol: &str) -> anyhow::Result<InstrumentType> {
    if symbol.starts_with("PERP_") {
        Ok(InstrumentType::Perp)
    } else if symbol.starts_with("SPOT_") {
        Ok(InstrumentType::Spot)
    } else {
        anyhow::bail!("cannot determine instrument type from symbol: {symbol}")
    }
}

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
    basis: &[Cell<f64>],
    seeded: &[Cell<bool>],
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
            basis[idx].set(val);
            seeded[idx].set(true);
            info!("basis loaded from cache: {} = {:.6}", pair.cache_key, val);
        }
    }
}

fn save_basis_cache(
    path: &str,
    pairs: &[ResolvedPair],
    basis: &[Cell<f64>],
) {
    let mut cache: HashMap<String, f64> = HashMap::new();
    for (idx, pair) in pairs.iter().enumerate() {
        cache.insert(pair.cache_key.clone(), basis[idx].get());
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
