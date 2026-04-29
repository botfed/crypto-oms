pub mod config;
pub mod display;
pub mod fair_price;
pub mod inventory;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use oms_core::state_tracker::OmsStateTracker;
use oms_core::*;
use tracing::{debug, info, warn};

use crate::ExchangeOms;
use config::StrategyConfig;
use crypto_feeds::MarketDataCollection;
use crypto_feeds::market_data::InstrumentType;
use crypto_feeds::symbol_registry::{REGISTRY, SymbolId};
use fair_price::{ExchangeId, FairPriceEngine, parse_exchange_id};

// ---------------------------------------------------------------------------
// Latency profiling (compiled in with --features profiling)
// ---------------------------------------------------------------------------

#[cfg(feature = "profiling")]
pub mod latency {
    pub const METRIC_TICK_FAST: u8 = 0;
    pub const METRIC_T2D: u8 = 1;
    pub const METRIC_FAIR: u8 = 2;
    pub const METRIC_VOL: u8 = 3;
    pub const METRIC_TICK_SLOW: u8 = 4;
    pub const METRIC_T2T: u8 = 5;
    pub const METRIC_SIGN: u8 = 6;
    pub const METRIC_TICK_END: u8 = 7;
    pub const METRIC_PRECONDITIONS: u8 = 8;
    pub const METRIC_FEED: u8 = 9;
    pub const METRIC_DRAIN: u8 = 10;
    pub const NUM_METRICS: usize = 11;

    pub const GLOBAL_HEADER: usize = 8;
    pub const METRIC_HEADER: usize = 16;
    pub const SAMPLE_SIZE: usize = 8;
    pub const SAMPLES_PER_METRIC: usize = 1_048_576;
    pub const METRIC_SECTION: usize = METRIC_HEADER + SAMPLES_PER_METRIC * SAMPLE_SIZE;
    pub const FILE_SIZE: usize = GLOBAL_HEADER + NUM_METRICS * METRIC_SECTION;
    pub fn latency_path(symbol: &str) -> String {
        format!("/tmp/mm_latency_{symbol}.bin")
    }

    /// Direct-to-mmap recorder. No mutex, no buffering, no background task.
    /// Single-threaded: only the engine spin loop calls record().
    pub struct LatencyRecorder {
        mmap: *mut u8,
        write_positions: [u64; NUM_METRICS],
    }

    unsafe impl Send for LatencyRecorder {}

    impl LatencyRecorder {
        #[inline]
        pub fn record(&mut self, metric: u8, nanos: u64) {
            let m = metric as usize;
            if m >= NUM_METRICS {
                return;
            }
            let wp = &mut self.write_positions[m];
            let section = metric_offset(m);
            let idx = (*wp % SAMPLES_PER_METRIC as u64) as usize;
            let offset = section + METRIC_HEADER + idx * SAMPLE_SIZE;
            unsafe {
                self.mmap.add(offset).cast::<u64>().write(nanos);
            }
            *wp += 1;
            // Update write_pos in header so mm_profile can read live
            unsafe {
                self.mmap.add(section).cast::<u64>().write(*wp);
            }
        }
    }

    fn metric_offset(metric: usize) -> usize {
        GLOBAL_HEADER + metric * METRIC_SECTION
    }

    /// Initialize the profiler for a specific symbol. Returns a recorder for the hot loop.
    pub fn init(symbol: &str) -> LatencyRecorder {
        let path = latency_path(symbol);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap_or_else(|e| panic!("failed to open latency mmap file {path}: {e}"));
        file.set_len(FILE_SIZE as u64)
            .expect("failed to set latency file size");

        let mut mmap =
            unsafe { memmap2::MmapMut::map_mut(&file).expect("failed to mmap latency file") };

        // Reset file on startup
        mmap.fill(0);
        // Write global header
        mmap[0..8].copy_from_slice(&(NUM_METRICS as u64).to_le_bytes());
        // Write per-metric capacity
        for m in 0..NUM_METRICS {
            let off = metric_offset(m);
            mmap[off + 8..off + 16].copy_from_slice(&(SAMPLES_PER_METRIC as u64).to_le_bytes());
        }

        let ptr = mmap.as_mut_ptr();
        // Leak the MmapMut — lives for process lifetime, no cleanup needed
        std::mem::forget(mmap);

        LatencyRecorder {
            mmap: ptr,
            write_positions: [0u64; NUM_METRICS],
        }
    }
}

// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct LiveQuote {
    client_id: ClientOrderId,
    exchange_id: Option<String>,
    price: f64,
    #[allow(dead_code)]
    size: f64,
    #[allow(dead_code)]
    placed_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EngineState {
    Initializing,
    Running,
    Paused,
}

// ---------------------------------------------------------------------------
// Factor model (cross-instrument correlation)
// ---------------------------------------------------------------------------

struct FactorState {
    symbol_id: SymbolId,
    exchanges: Vec<ExchangeId>,
    beta: f64,
    snapshot_log_mid: f64, // ln(mid) snapshotted at last direct tick
}

struct FactorModelState {
    factors: Vec<FactorState>,
    last_direct_fair: f64,
    last_factor_wc: u64,
    seeded: bool,
}

struct TickSource {
    fair: f64,
    exchange_ts_ms: i64,
    /// Monotonic instant when the trigger feed data was received.
    /// Used for rt2d/rt2t measurement via .elapsed().
    trigger_received_instant: Option<Instant>,
    is_direct: bool,
    /// Feed processing latency (WS recv → ring buffer write) in nanoseconds
    #[cfg_attr(not(feature = "profiling"), allow(dead_code))]
    feed_latency_ns: u64,
}

fn collection_for(
    market_data: &crypto_feeds::AllMarketData,
    exchange: ExchangeId,
) -> &MarketDataCollection {
    match exchange {
        ExchangeId::Binance => &market_data.binance,
        ExchangeId::Bybit => &market_data.bybit,
        ExchangeId::Okx => &market_data.okx,
        ExchangeId::Hyperliquid => &market_data.hyperliquid,
        ExchangeId::Hibachi => &market_data.hibachi,
    }
}

/// Sum write counts across all factor exchanges.
fn total_factor_wc(market_data: &crypto_feeds::AllMarketData, fm: &FactorModelState) -> u64 {
    let mut total = 0u64;
    for f in &fm.factors {
        for &ex in &f.exchanges {
            total += collection_for(market_data, ex).write_count(&f.symbol_id);
        }
    }
    total
}

/// Get freshest mid across exchanges for a factor (by exchange_ts).
/// Returns (mid, exchange_ts_ms, received_instant).
/// No Utc::now() — compares exchange_ts directly (largest = freshest).
fn factor_mid(
    market_data: &crypto_feeds::AllMarketData,
    factor: &FactorState,
) -> Option<(f64, i64, Option<Instant>)> {
    let mut best: Option<(f64, i64, Option<Instant>)> = None;
    let mut best_ts = i64::MIN;

    for &ex in &factor.exchanges {
        let coll = collection_for(market_data, ex);
        let Some(md) = coll.latest(&factor.symbol_id) else {
            continue;
        };
        let Some(mid) = md.midquote() else { continue };

        let exchange_ts_ms = md.exchange_ts.map(|ts| ts.timestamp_millis()).unwrap_or(0);

        if exchange_ts_ms > best_ts {
            best = Some((mid, exchange_ts_ms, md.received_instant));
            best_ts = exchange_ts_ms;
        }
    }
    best
}

// ---------------------------------------------------------------------------
// MmEngine — one per symbol
// ---------------------------------------------------------------------------

pub struct MmEngine<O: ExchangeOms> {
    oms: Arc<O>,
    fair_price: Arc<FairPriceEngine>,
    market_data: Arc<crypto_feeds::AllMarketData>,
    target_exchange: ExchangeId,
    target_position_rx: Option<tokio::sync::watch::Receiver<f64>>,
    config: StrategyConfig,
    ghost: bool,
    warmed_up: bool,

    bid_quote: Option<LiveQuote>,
    ask_quote: Option<LiveQuote>,
    state: EngineState,

    hl_symbol_id: SymbolId,
    vol_group_idx: usize,
    last_quote_eval: Instant,
    last_status_log: Instant,
    running_since: Option<Instant>,
    consecutive_rejects: u32,
    reject_pause_until: Option<Instant>,
    last_ref_wc: u64, // track seqlock write count to detect new ticks
    trigger_received_instant: Option<Instant>, // monotonic receive time of the feed that triggered the current tick
    last_exchange_ts_ms: i64,                  // global freshness watermark
    last_direct_exchange_ts_ms: i64, // freshest direct tick exchange_ts (for factor snapshot gating)
    factor_model: Option<FactorModelState>,
    cached_vol_mult: f64,
    cached_skew_bps: f64,
    has_vol_params: bool,
    presigned_cancels: HashMap<ClientOrderId, O::SignedPayload>,
    display: Option<display::DisplayBus>,
    // TODO: per-engine profiling files for multi-ticker
    #[cfg(feature = "profiling")]
    latency: latency::LatencyRecorder,
}

const MAX_CONSECUTIVE_REJECTS: u32 = 5;
const REJECT_COOLDOWN: Duration = Duration::from_secs(1);

impl<O: ExchangeOms + 'static> MmEngine<O> {
    pub fn new(
        oms: Arc<O>,
        fair_price: Arc<FairPriceEngine>,
        market_data: Arc<crypto_feeds::AllMarketData>,
        target_exchange: ExchangeId,
        target_position_rx: Option<tokio::sync::watch::Receiver<f64>>,
        config: StrategyConfig,
        ghost: bool,
        vol_group_idx: usize,
        has_vol_params: bool,
        display: Option<display::DisplayBus>,
    ) -> Result<Self> {
        // Resolve the HL symbol to a SymbolId for fair price lookups
        let itype = if config.symbol.starts_with("PERP_") {
            InstrumentType::Perp
        } else {
            InstrumentType::Spot
        };
        let base = config
            .symbol
            .strip_prefix("PERP_")
            .or_else(|| config.symbol.strip_prefix("SPOT_"))
            .unwrap_or(&config.symbol);

        let hl_symbol_id = *REGISTRY
            .lookup(base, &itype)
            .ok_or_else(|| anyhow::anyhow!("symbol not in registry: {}", config.symbol))?;

        #[cfg(feature = "profiling")]
        let latency = latency::init(&config.symbol);

        // Initialize factor model if configured
        let factor_model = if let Some(ref fm_cfg) = config.factor_model {
            if fm_cfg.enabled && !fm_cfg.factors.is_empty() {
                let mut factors = Vec::with_capacity(fm_cfg.factors.len());
                for fc in &fm_cfg.factors {
                    let exchanges: Vec<ExchangeId> = fc
                        .exchanges
                        .iter()
                        .map(|s| parse_exchange_id(s))
                        .collect::<anyhow::Result<Vec<_>>>()?;

                    let itype = if fc.symbol.starts_with("PERP_") {
                        InstrumentType::Perp
                    } else {
                        InstrumentType::Spot
                    };
                    let base = fc
                        .symbol
                        .strip_prefix("PERP_")
                        .or_else(|| fc.symbol.strip_prefix("SPOT_"))
                        .unwrap_or(&fc.symbol);
                    let symbol_id = *REGISTRY.lookup(base, &itype).ok_or_else(|| {
                        anyhow::anyhow!("factor symbol not in registry: {}", fc.symbol)
                    })?;

                    factors.push(FactorState {
                        symbol_id,
                        exchanges,
                        beta: fc.beta,
                        snapshot_log_mid: 0.0,
                    });
                }
                info!(
                    "[{}] factor model: {} factor(s) [{}]",
                    config.symbol,
                    factors.len(),
                    fm_cfg
                        .factors
                        .iter()
                        .map(|f| format!("{}@{:.3}", f.symbol, f.beta))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                Some(FactorModelState {
                    factors,
                    last_direct_fair: 0.0,
                    last_factor_wc: 0,
                    seeded: false,
                })
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            oms,
            fair_price,
            market_data,
            target_exchange,
            target_position_rx,
            config,
            ghost,
            warmed_up: false,
            bid_quote: None,
            ask_quote: None,
            state: EngineState::Initializing,
            hl_symbol_id,
            vol_group_idx,
            last_quote_eval: Instant::now(),
            last_status_log: Instant::now(),
            running_since: None,
            consecutive_rejects: 0,
            reject_pause_until: None,
            last_ref_wc: 0,
            trigger_received_instant: None,
            last_exchange_ts_ms: 0,
            last_direct_exchange_ts_ms: 0,
            factor_model,
            cached_vol_mult: 1.0,
            cached_skew_bps: 0.0,
            has_vol_params,
            presigned_cancels: HashMap::new(),
            display,
            #[cfg(feature = "profiling")]
            latency,
        })
    }

    pub fn symbol(&self) -> &str {
        &self.config.symbol
    }

    // -----------------------------------------------------------------------
    // Tick — one iteration of the per-symbol loop
    // -----------------------------------------------------------------------

    /// Run one tick for this engine's symbol. Returns true if new data was processed.
    /// The caller (outer loop) is responsible for draining OMS events and dispatching
    /// them via handle_oms_event before calling tick.
    pub fn tick(
        &mut self,
        oms_state: &mut OmsStateTracker,
        vol_provider: &mut Option<crypto_feeds::vol_provider::VolProvider>,
    ) -> bool {
        #[cfg(feature = "profiling")]
        let t0 = Instant::now();

        if !self.check_preconditions(oms_state) {
            if self.state == EngineState::Running {
                self.transition_to_paused();
            }
            if self.display.is_some() && self.last_status_log.elapsed() >= Duration::from_secs(1) {
                self.send_display_status(oms_state, vol_provider);
                self.last_status_log = Instant::now();
            }
            return false;
        }

        #[cfg(feature = "profiling")]
        let t1 = Instant::now(); // after preconditions

        if self.state != EngineState::Running {
            info!(
                "[{}] entering Running state (warmup={}s)",
                self.config.symbol, self.config.warmup_secs.unwrap()
            );
            self.state = EngineState::Running;
            self.running_since = Some(Instant::now());
        }

        self.warmed_up = self
            .running_since
            .map(|t| t.elapsed() >= Duration::from_secs(self.config.warmup_secs.unwrap()))
            .unwrap_or(false);

        // ── CHECK FOR NEW DATA ──
        let Some(tick) = self.resolve_tick() else {
            if self.display.is_some() && self.last_status_log.elapsed() >= Duration::from_secs(1) {
                self.send_display_status(oms_state, vol_provider);
                self.last_status_log = Instant::now();
            }
            return false;
        };

        #[cfg(feature = "profiling")]
        let t2 = Instant::now(); // after resolve_tick

        self.trigger_received_instant = tick.trigger_received_instant;

        // Staleness check — direct feed only
        if tick.is_direct {
            if let Some(inst) = tick.trigger_received_instant {
                let recv_age_ms = inst.elapsed().as_millis() as i64;
                if recv_age_ms > self.config.stale_cancel_ms.unwrap() as i64 {
                    if self.bid_quote.is_some() || self.ask_quote.is_some() {
                        warn!(
                            "[{}] ref feed stale (recv_age={}ms > {}ms), cancelling quotes",
                            self.config.symbol, recv_age_ms, self.config.stale_cancel_ms.unwrap()
                        );
                        self.cancel_all_quotes();
                    }
                    return false;
                }
            }
        }

        // Sync local trackers with OMS reality
        self.sync_quote_state(oms_state);

        // ── FAST PATH: adverse cancel guard ──
        self.fast_cancel_check(tick.fair, oms_state);

        #[cfg(feature = "profiling")]
        let t3 = Instant::now(); // after fast path

        #[cfg(feature = "profiling")]
        let mut vol_ns: u64 = 0;
        #[cfg(feature = "profiling")]
        let mut is_slow = false;

        // ── SLOW PATH (~quote_interval_ms): quote placement ──
        let interval = Duration::from_millis(self.config.quote_interval_ms.unwrap());
        if self.warmed_up && self.last_quote_eval.elapsed() >= interval {
            #[cfg(feature = "profiling")]
            { is_slow = true; }

            self.fair_price.update_basis();

            #[cfg(feature = "profiling")]
            let vol_start = Instant::now();

            self.cached_vol_mult =
                self.get_vol_multiplier(tick.fair, tick.exchange_ts_ms, vol_provider);

            #[cfg(feature = "profiling")]
            { vol_ns = vol_start.elapsed().as_nanos() as u64; }

            self.evaluate_and_place_quotes(tick.fair, oms_state);

            self.last_quote_eval = Instant::now();
        }

        // ── STATUS LOG / DISPLAY ──
        #[cfg(any(feature = "log_status", feature = "log_state"))]
        if self.last_status_log.elapsed() >= Duration::from_secs(1) {
            #[cfg(feature = "log_status")]
            self.log_status(oms_state, vol_provider);
            #[cfg(feature = "log_state")]
            self.log_state(oms_state);
            self.last_status_log = Instant::now();
        }
        if self.display.is_some() && self.last_status_log.elapsed() >= Duration::from_secs(1) {
            self.send_display_status(oms_state, vol_provider);
            self.last_status_log = Instant::now();
        }

        // ── FLUSH PROFILING ──
        #[cfg(feature = "profiling")]
        if self.warmed_up {
            let t4 = Instant::now(); // end of tick

            self.latency.record(latency::METRIC_PRECONDITIONS, (t1 - t0).as_nanos() as u64);
            self.latency.record(latency::METRIC_FAIR, (t2 - t1).as_nanos() as u64);
            self.latency.record(latency::METRIC_TICK_FAST, (t3 - t2).as_nanos() as u64);
            self.latency.record(latency::METRIC_TICK_END, (t4 - t2).as_nanos() as u64);

            if let Some(inst) = tick.trigger_received_instant {
                self.latency.record(latency::METRIC_T2D, (t2 - inst).as_nanos() as u64);
            }
            if tick.feed_latency_ns > 0 {
                self.latency.record(latency::METRIC_FEED, tick.feed_latency_ns);
            }
            if is_slow {
                self.latency.record(latency::METRIC_VOL, vol_ns);
                self.latency.record(latency::METRIC_TICK_SLOW, (t4 - t2).as_nanos() as u64);
            }
        }

        true
    }

    // -----------------------------------------------------------------------
    // Factor model helpers
    // -----------------------------------------------------------------------

    /// Resolve the next tick. Returns the freshest fair price by exchange_ts,
    /// either direct or factor-adjusted. Snapshots factor mids only when the
    /// direct feed genuinely advances. Global exchange_ts watermark prevents
    /// stale or backwards ticks.
    fn resolve_tick(&mut self) -> Option<TickSource> {
        // 1. Check for new direct tick
        let mut direct_advanced = false;
        let mut best_received_instant: Option<Instant> = None;
        let mut best_feed_latency_ns: u64 = 0;

        let wc = self
            .fair_price
            .ref_write_count(self.target_exchange, self.hl_symbol_id);
        if wc != self.last_ref_wc {
            self.last_ref_wc = wc;

            if let Some((fair, exchange_ts_ms, _, received_ts, feed_lat)) = self
                .fair_price
                .get_fair_price_detail(self.target_exchange, self.hl_symbol_id)
            {
                if exchange_ts_ms > self.last_direct_exchange_ts_ms {
                    self.last_direct_exchange_ts_ms = exchange_ts_ms;
                    best_feed_latency_ns = feed_lat;
                    if received_ts > best_received_instant {
                        best_received_instant = received_ts;
                    }
                    direct_advanced = true;
                    if let Some(ref mut fm) = self.factor_model {
                        fm.last_direct_fair = fair;
                        fm.seeded = true;
                    }
                }
            }
        }

        // 2. Single pass through factors: snapshot on direct advance, compute adjustment
        if let Some(ref mut fm) = self.factor_model {
            if !fm.seeded {
                return None;
            }

            let factor_wc_changed = {
                let current_wc = total_factor_wc(&self.market_data, fm);
                if current_wc != fm.last_factor_wc {
                    fm.last_factor_wc = current_wc;
                    true
                } else {
                    false
                }
            };

            if direct_advanced || factor_wc_changed {
                let mut sum = 0.0f64;
                let mut best_exchange_ts = i64::MIN;

                for f in &mut fm.factors {
                    let Some((mid, exch_ts_ms, recv_ts)) = factor_mid(&self.market_data, f) else {
                        continue;
                    };

                    // Snapshot ALL factors unconditionally on direct advance
                    if direct_advanced {
                        f.snapshot_log_mid = mid.ln();
                    }

                    // Only compute returns for factors fresher than the direct tick
                    if exch_ts_ms > self.last_direct_exchange_ts_ms {
                        let r = mid.ln() - f.snapshot_log_mid;
                        sum += f.beta * r;
                        if exch_ts_ms > best_exchange_ts {
                            best_exchange_ts = exch_ts_ms;
                        }
                        if recv_ts > best_received_instant {
                            best_received_instant = recv_ts;
                        }
                    }
                }

                // Determine best tick to return
                let (tick_fair, tick_ts, tick_recv_ts, tick_is_direct) =
                    if direct_advanced && self.last_direct_exchange_ts_ms >= best_exchange_ts {
                        // Direct is freshest — factor returns are ~0
                        (
                            fm.last_direct_fair,
                            self.last_direct_exchange_ts_ms,
                            best_received_instant,
                            true,
                        )
                    } else if sum.abs() > 1e-15 && best_exchange_ts > i64::MIN {
                        // Factors are fresher and meaningful
                        let corr_fair = fm.last_direct_fair * sum.exp();
                        (corr_fair, best_exchange_ts, best_received_instant, false)
                    } else {
                        return None;
                    };

                // Global freshness gate
                if tick_ts <= self.last_exchange_ts_ms {
                    return None;
                }
                self.last_exchange_ts_ms = tick_ts;

                return Some(TickSource {
                    fair: tick_fair,
                    exchange_ts_ms: tick_ts,
                    trigger_received_instant: tick_recv_ts,
                    is_direct: tick_is_direct,
                    feed_latency_ns: best_feed_latency_ns,
                });
            }
        }

        // 3. Direct advanced but no factor model
        if direct_advanced {
            if self.last_direct_exchange_ts_ms <= self.last_exchange_ts_ms {
                return None;
            }
            self.last_exchange_ts_ms = self.last_direct_exchange_ts_ms;

            // Need fair price again (no factor model to store it)
            if let Some((fair, _, _, _, _)) = self
                .fair_price
                .get_fair_price_detail(self.target_exchange, self.hl_symbol_id)
            {
                return Some(TickSource {
                    fair,
                    exchange_ts_ms: self.last_direct_exchange_ts_ms,
                    trigger_received_instant: best_received_instant,
                    is_direct: true,
                    feed_latency_ns: best_feed_latency_ns,
                });
            }
        }

        None
    }

    // -----------------------------------------------------------------------
    // Preconditions
    // -----------------------------------------------------------------------

    fn check_preconditions(&mut self, oms_state: &OmsStateTracker) -> bool {
        let should_log = self.last_status_log.elapsed() >= Duration::from_secs(1);

        if !oms_state.is_ready() {
            if should_log {
                warn!("[{}] waiting: OMS not ready", self.config.symbol);
                self.last_status_log = Instant::now();
            }
            return false;
        }
        if self.consecutive_rejects >= MAX_CONSECUTIVE_REJECTS {
            if let Some(until) = self.reject_pause_until {
                if Instant::now() >= until {
                    info!("[{}] reject cooldown elapsed, retrying", self.config.symbol);
                    self.consecutive_rejects = 0;
                    self.reject_pause_until = None;
                } else {
                    if should_log {
                        let remaining = until.duration_since(Instant::now());
                        warn!(
                            "[{}] paused: {} consecutive rejects (retry in {:.0}s)",
                            self.config.symbol,
                            self.consecutive_rejects,
                            remaining.as_secs_f64()
                        );
                        self.last_status_log = Instant::now();
                    }
                    return false;
                }
            } else {
                warn!(
                    "[{}] paused: {} consecutive rejects, cooldown {}s",
                    self.config.symbol,
                    self.consecutive_rejects,
                    REJECT_COOLDOWN.as_secs()
                );
                self.reject_pause_until = Some(Instant::now() + REJECT_COOLDOWN);
                self.last_status_log = Instant::now();
                return false;
            }
        }

        // Check feed is alive — read received_instant from seqlock directly
        // so we can detect recovery while paused (tick loop doesn't run).
        let live_recv = self
            .fair_price
            .get_fair_price_detail(self.target_exchange, self.hl_symbol_id)
            .and_then(|(_, _, _, recv, _)| recv);
        if let Some(recv) = live_recv {
            if recv.elapsed().as_millis() as u64 > self.config.max_feed_age_ms.unwrap() {
                if should_log {
                    warn!(
                        "[{}] paused: reference feed dead (recv_age={}ms > {}ms)",
                        self.config.symbol,
                        recv.elapsed().as_millis(),
                        self.config.max_feed_age_ms.unwrap()
                    );
                    self.last_status_log = Instant::now();
                }
                return false;
            }
        }

        true
    }

    // -----------------------------------------------------------------------
    // Quote state sync
    // -----------------------------------------------------------------------

    fn sync_quote_state(&mut self, oms_state: &OmsStateTracker) {
        const CANCEL_TIMEOUT: Duration = Duration::from_secs(10);

        if let Some(ref mut q) = self.bid_quote {
            match oms_state.get_order(q.client_id) {
                Some(h)
                    if matches!(
                        h.state,
                        OrderState::Accepted
                            | OrderState::PartiallyFilled
                            | OrderState::Inflight
                    ) =>
                {
                    if q.exchange_id.is_none() && h.exchange_id.is_some() {
                        q.exchange_id = h.exchange_id.clone();
                    }
                    q.size = h.size - h.filled_size;
                }
                Some(h) if h.state == OrderState::Cancelling => {
                    let cancel_age = h.last_modified
                        .map(|t| t.elapsed())
                        .unwrap_or(Duration::ZERO);
                    if cancel_age > CANCEL_TIMEOUT {
                        warn!("[{}] bid cid={} stuck in Cancelling for >{}s, force-clearing",
                            self.config.symbol, q.client_id.0, CANCEL_TIMEOUT.as_secs());
                        self.bid_quote = None;
                    }
                }
                _ => {
                    debug!(
                        "bid quote {} no longer active, clearing tracker",
                        q.client_id.0
                    );
                    self.bid_quote = None;
                }
            }
        }
        if let Some(ref mut q) = self.ask_quote {
            match oms_state.get_order(q.client_id) {
                Some(h)
                    if matches!(
                        h.state,
                        OrderState::Accepted
                            | OrderState::PartiallyFilled
                            | OrderState::Inflight
                    ) =>
                {
                    if q.exchange_id.is_none() && h.exchange_id.is_some() {
                        q.exchange_id = h.exchange_id.clone();
                    }
                    q.size = h.size - h.filled_size;
                }
                Some(h) if h.state == OrderState::Cancelling => {
                    let cancel_age = h.last_modified
                        .map(|t| t.elapsed())
                        .unwrap_or(Duration::ZERO);
                    if cancel_age > CANCEL_TIMEOUT {
                        warn!("[{}] ask cid={} stuck in Cancelling for >{}s, force-clearing",
                            self.config.symbol, q.client_id.0, CANCEL_TIMEOUT.as_secs());
                        self.ask_quote = None;
                    }
                }
                _ => {
                    debug!(
                        "ask quote {} no longer active, clearing tracker",
                        q.client_id.0
                    );
                    self.ask_quote = None;
                }
            }
        }
    }

    #[cfg(feature = "log_state")]
    fn log_state(&self, oms_state: &OmsStateTracker) {
        let open = oms_state.open_orders(Some(&self.config.symbol));
        let open_summary: Vec<String> = open
            .iter()
            .map(|h| format!("cid={} {:?} {:?}", h.client_id.0, h.side, h.state))
            .collect();
        let pos = self.get_position(oms_state);
        info!(
            "[{}] state: orders=[{}] pos={:+.4}",
            self.config.symbol,
            open_summary.join(", "),
            pos,
        );
    }

    // -----------------------------------------------------------------------
    // Fast path: cancel guard (inner-side adverse detection)
    // -----------------------------------------------------------------------

    fn fast_cancel_check(&mut self, fair: f64, oms_state: &mut OmsStateTracker) {
        if self.bid_quote.is_none() && self.ask_quote.is_none() {
            return;
        }

        let mid = self.skewed_mid(fair);
        let min_edge = self.config.min_edge_bps.unwrap() * fair / 10_000.0;
        let min_spread = self.config.ref_min_spread_bps.unwrap() * self.cached_vol_mult * fair / 10_000.0;
        let max_bid = (mid - min_spread).min(fair - min_edge);
        let min_ask = (mid + min_spread).max(fair + min_edge);

        // Check bid: above max allowed bid?
        if let Some(ref q) = self.bid_quote {
            let state = oms_state.get_order(q.client_id).map(|h| h.state);
            let can_cancel = matches!(
                state,
                Some(OrderState::Accepted) | Some(OrderState::PartiallyFilled)
            );
            if can_cancel && q.price > self.oms.round_price(max_bid) + 1e-12 {
                let bid_from_fair_bps = (q.price - fair) / fair * 10_000.0;
                let min_spread_bps = self.config.ref_min_spread_bps.unwrap() * self.cached_vol_mult;
                let edge_bps = self.config.min_edge_bps.unwrap();
                debug!(
                    "[{}] fast cancel BID cid={} | bid {:.1}bps from fair (min_spread={:.1}bps edge={:.1}bps skew={:+.1}bps)",
                    self.config.symbol, q.client_id.0, bid_from_fair_bps, min_spread_bps, edge_bps, self.cached_skew_bps,
                );
                #[cfg(feature = "profiling")]
                let sign_start = Instant::now();
                let signed = if let Some(s) = self.presigned_cancels.remove(&q.client_id) {
                    self.oms.mark_cancelling(&q.client_id);
                    Ok(s)
                } else {
                    self.oms.sign_cancel(&q.client_id)
                };
                match signed {
                    Ok(signed) => {
                        oms_state.mark_cancelling(q.client_id);
                        #[cfg(feature = "profiling")]
                        self.latency
                            .record(latency::METRIC_SIGN, sign_start.elapsed().as_nanos() as u64);
                        let oms = Arc::clone(&self.oms);
                        let cid = q.client_id;
                        #[cfg(feature = "profiling")]
                        self.record_t2t();
                        if self.ghost {
                            self.bid_quote = None;
                        } else {
                            tokio::spawn(async move {
                                oms.post_cancel(&cid, signed).await;
                            });
                        }
                    }
                    Err(e) => warn!("failed to sign cancel bid {}: {e}", q.client_id.0),
                }
            }
        }

        // Check ask: below min allowed ask?
        if let Some(ref q) = self.ask_quote {
            let state = oms_state.get_order(q.client_id).map(|h| h.state);
            let can_cancel = matches!(
                state,
                Some(OrderState::Accepted) | Some(OrderState::PartiallyFilled)
            );
            if can_cancel && q.price < self.oms.round_price(min_ask) - 1e-12 {
                let ask_from_fair_bps = (q.price - fair) / fair * 10_000.0;
                let min_spread_bps = self.config.ref_min_spread_bps.unwrap() * self.cached_vol_mult;
                let edge_bps = self.config.min_edge_bps.unwrap();
                debug!(
                    "[{}] fast cancel ASK cid={} | ask {:.1}bps from fair (min_spread={:.1}bps edge={:.1}bps skew={:+.1}bps)",
                    self.config.symbol, q.client_id.0, ask_from_fair_bps, min_spread_bps, edge_bps, self.cached_skew_bps,
                );
                #[cfg(feature = "profiling")]
                let sign_start = Instant::now();
                let signed = if let Some(s) = self.presigned_cancels.remove(&q.client_id) {
                    self.oms.mark_cancelling(&q.client_id);
                    Ok(s)
                } else {
                    self.oms.sign_cancel(&q.client_id)
                };
                match signed {
                    Ok(signed) => {
                        oms_state.mark_cancelling(q.client_id);
                        #[cfg(feature = "profiling")]
                        self.latency
                            .record(latency::METRIC_SIGN, sign_start.elapsed().as_nanos() as u64);
                        let oms = Arc::clone(&self.oms);
                        let cid = q.client_id;
                        #[cfg(feature = "profiling")]
                        self.record_t2t();
                        if self.ghost {
                            self.ask_quote = None;
                        } else {
                            tokio::spawn(async move {
                                oms.post_cancel(&cid, signed).await;
                            });
                        }
                    }
                    Err(e) => warn!("failed to sign cancel ask {}: {e}", q.client_id.0),
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Slow path: evaluate and place quotes
    // -----------------------------------------------------------------------

    fn evaluate_and_place_quotes(&mut self, fair: f64, oms_state: &mut OmsStateTracker) {
        // Cancel any stray orders on our symbol that we don't own
        self.cancel_stray_orders(oms_state);

        let position = self.get_position(oms_state);
        let target_usd = self.target_position_rx.as_ref().map(|rx| *rx.borrow()).unwrap_or(0.0);
        let target = target_usd / fair;
        let notional = self.config.order_notional_usd.unwrap();
        let order_size = notional / fair;
        let max_pos = self.config.max_position_usd.unwrap() / fair;

        let half_spread =
            self.config.ref_half_spread_bps.unwrap() * self.cached_vol_mult * fair / 10_000.0;
        let requote_thresh =
            self.config.ref_requote_tolerance_bps.unwrap() * self.cached_vol_mult * fair / 10_000.0;

        self.cached_skew_bps = self.compute_skew_bps(fair, position, target);
        let skewed_mid = self.skewed_mid(fair);
        let place_floor_bps = self.config.ref_min_spread_bps.unwrap() * self.cached_vol_mult;
        let (desired_bid, desired_ask) = Self::clamp_to_fair(
            fair,
            skewed_mid - half_spread,
            skewed_mid + half_spread,
            place_floor_bps,
        );

        // Determine if we should quote each side (position limits)
        let want_bid = position - target < max_pos;
        let want_ask = position - target > -max_pos;

        // ── CANCEL stale/outer-side quotes ──

        // Bid: cancel if too passive (outer side) or shouldn't quote
        if let Some(ref q) = self.bid_quote {
            let should_cancel = !want_bid || (desired_bid - q.price) > requote_thresh;

            if should_cancel {
                let skip_cancel = oms_state
                    .get_order(q.client_id)
                    .map(|h| matches!(h.state, OrderState::Inflight | OrderState::Cancelling))
                    .unwrap_or(false);

                if !skip_cancel {
                    if self.ghost {
                        debug!(
                            "[GHOST] would CANCEL bid cid={} price={:.6} (slow path requote)",
                            q.client_id.0, q.price
                        );
                        self.bid_quote = None;
                    } else {
                        debug!("slow cancel bid cid={} price={:.6}", q.client_id.0, q.price);
                        oms_state.mark_cancelling(q.client_id);
                        let oms = Arc::clone(&self.oms);
                        let cid = q.client_id;
                        tokio::spawn(async move {
                            if let Err(e) = oms.cancel_order(&cid).await {
                                warn!("failed to cancel bid {}: {e}", cid.0);
                            }
                        });
                    }
                }
            }
        }

        // Ask: cancel if too passive or shouldn't quote
        if let Some(ref q) = self.ask_quote {
            let should_cancel = !want_ask || (q.price - desired_ask) > requote_thresh;

            if should_cancel {
                let skip_cancel = oms_state
                    .get_order(q.client_id)
                    .map(|h| matches!(h.state, OrderState::Inflight | OrderState::Cancelling))
                    .unwrap_or(false);

                if !skip_cancel {
                    if self.ghost {
                        debug!(
                            "[GHOST] would CANCEL ask cid={} price={:.6} (slow path requote)",
                            q.client_id.0, q.price
                        );
                        self.ask_quote = None;
                    } else {
                        debug!("slow cancel ask cid={} price={:.6}", q.client_id.0, q.price);
                        oms_state.mark_cancelling(q.client_id);
                        let oms = Arc::clone(&self.oms);
                        let cid = q.client_id;
                        tokio::spawn(async move {
                            if let Err(e) = oms.cancel_order(&cid).await {
                                warn!("failed to cancel ask {}: {e}", cid.0);
                            }
                        });
                    }
                }
            }
        }

        // ── PLACE new quotes where needed ──

        if self.bid_quote.is_none() && want_bid && self.ghost {}
        if self.bid_quote.is_none() && want_bid && !self.ghost {
            let tif = if self.config.post_only.unwrap_or(true) {
                TimeInForce::PostOnly
            } else {
                TimeInForce::GTC
            };
            let req = OrderRequest {
                symbol: self.config.symbol.clone(),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: desired_bid,
                    tif,
                },
                size: order_size,
                reduce_only: false,
            };
            match self.oms.prepare_place_order(&req) {
                Ok((cid, sdk_req)) => {
                    let bid_from_fair_bps = (desired_bid - fair) / fair * 10_000.0;
                    debug!("[{}] place BID cid={} | {:.1}bps from fair (spread={:.1}bps edge={:.1}bps skew={:+.1}bps vmul={:.2})",
                        self.config.symbol, cid.0, bid_from_fair_bps,
                        self.config.ref_half_spread_bps.unwrap() * self.cached_vol_mult,
                        self.config.min_edge_bps.unwrap(), self.cached_skew_bps, self.cached_vol_mult);
                    oms_state.insert_inflight(OrderHandle {
                        client_id: cid,
                        exchange_id: None,
                        symbol: self.config.symbol.clone(),
                        side: Side::Buy,
                        order_type: OrderType::Limit {
                            price: desired_bid,
                            tif,
                        },
                        size: order_size,
                        filled_size: 0.0,
                        avg_fill_price: None,
                        state: OrderState::Inflight,
                        reduce_only: false,
                        reject_reason: None,
                        exchange_ts: None,
                        submitted_at: Some(Instant::now()),
                        last_modified: Some(Instant::now()),
                    });
                    self.bid_quote = Some(LiveQuote {
                        client_id: cid,
                        exchange_id: None,
                        price: desired_bid,
                        size: order_size,
                        placed_at: Instant::now(),
                    });
                    let oms = Arc::clone(&self.oms);
                    tokio::spawn(async move {
                        match oms.sign_order(sdk_req) {
                            Ok(signed) => oms.post_order(cid.0, signed).await,
                            Err(e) => warn!("failed to sign bid {}: {e}", cid.0),
                        }
                    });
                }
                Err(e) => warn!("failed to prepare bid: {e}"),
            }
        }

        if self.ask_quote.is_none() && want_ask && !self.ghost {
            let tif = if self.config.post_only.unwrap_or(true) {
                TimeInForce::PostOnly
            } else {
                TimeInForce::GTC
            };
            let req = OrderRequest {
                symbol: self.config.symbol.clone(),
                side: Side::Sell,
                order_type: OrderType::Limit {
                    price: desired_ask,
                    tif,
                },
                size: order_size,
                reduce_only: false,
            };
            match self.oms.prepare_place_order(&req) {
                Ok((cid, sdk_req)) => {
                    let ask_from_fair_bps = (desired_ask - fair) / fair * 10_000.0;
                    debug!("[{}] place ASK cid={} | {:.1}bps from fair (spread={:.1}bps edge={:.1}bps skew={:+.1}bps vmul={:.2})",
                        self.config.symbol, cid.0, ask_from_fair_bps,
                        self.config.ref_half_spread_bps.unwrap() * self.cached_vol_mult,
                        self.config.min_edge_bps.unwrap(), self.cached_skew_bps, self.cached_vol_mult);
                    oms_state.insert_inflight(OrderHandle {
                        client_id: cid,
                        exchange_id: None,
                        symbol: self.config.symbol.clone(),
                        side: Side::Sell,
                        order_type: OrderType::Limit {
                            price: desired_ask,
                            tif,
                        },
                        size: order_size,
                        filled_size: 0.0,
                        avg_fill_price: None,
                        state: OrderState::Inflight,
                        reduce_only: false,
                        reject_reason: None,
                        exchange_ts: None,
                        submitted_at: Some(Instant::now()),
                        last_modified: Some(Instant::now()),
                    });
                    self.ask_quote = Some(LiveQuote {
                        client_id: cid,
                        exchange_id: None,
                        price: desired_ask,
                        size: order_size,
                        placed_at: Instant::now(),
                    });
                    let oms = Arc::clone(&self.oms);
                    tokio::spawn(async move {
                        match oms.sign_order(sdk_req) {
                            Ok(signed) => oms.post_order(cid.0, signed).await,
                            Err(e) => warn!("failed to sign ask {}: {e}", cid.0),
                        }
                    });
                }
                Err(e) => warn!("failed to prepare ask: {e}"),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Inventory skew
    // -----------------------------------------------------------------------

    fn compute_skew_bps(&self, fair_value: f64, position: f64, target: f64) -> f64 {
        let diff_usd = (target - position) * fair_value;
        let scale = self.config.skew_scale_usd.unwrap_or(100.0);
        (diff_usd / scale * self.config.max_skew_bps.unwrap())
            .clamp(-self.config.max_skew_bps.unwrap(), self.config.max_skew_bps.unwrap())
    }

    #[inline]
    fn skewed_mid(&self, fair: f64) -> f64 {
        fair + self.cached_skew_bps * fair / 10_000.0
    }

    #[cfg(feature = "profiling")]
    fn record_t2t(&mut self) {
        if self.warmed_up {
            if let Some(inst) = self.trigger_received_instant {
                self.latency
                    .record(latency::METRIC_T2T, inst.elapsed().as_nanos() as u64);
            }
        }
    }

    fn get_vol_multiplier(
        &self,
        fair: f64,
        exchange_ts_ms: i64,
        vol_provider: &mut Option<crypto_feeds::vol_provider::VolProvider>,
    ) -> f64 {
        if !self.has_vol_params {
            return 1.0;
        }
        let Some(ref mut vp) = *vol_provider else {
            return 1.0;
        };
        let ts_ns = exchange_ts_ms * 1_000_000;
        vp.update(self.vol_group_idx, fair, ts_ns);
        let predicted = vp.ann_vol(self.vol_group_idx);
        if self.config.ref_vol <= 0.0 || predicted <= 0.0 || !predicted.is_finite() {
            return 1.0;
        }
        (predicted / self.config.ref_vol)
            .clamp(self.config.vol_mult_floor, self.config.vol_mult_cap)
    }

    fn clamp_to_fair(
        fair_value: f64,
        desired_bid: f64,
        desired_ask: f64,
        min_edge_bps: f64,
    ) -> (f64, f64) {
        let min_edge = min_edge_bps * fair_value / 10_000.0;
        (
            desired_bid.min(fair_value - min_edge),
            desired_ask.max(fair_value + min_edge),
        )
    }

    // -----------------------------------------------------------------------
    // Periodic status log
    // -----------------------------------------------------------------------

    #[cfg(feature = "log_status")]
    fn log_status(
        &self,
        oms_state: &OmsStateTracker,
        vol_provider: &Option<crypto_feeds::vol_provider::VolProvider>,
    ) {
        let fair = self
            .fair_price
            .get_fair_price(self.target_exchange, self.hl_symbol_id);
        let basis = self
            .fair_price
            .get_basis(self.target_exchange, self.hl_symbol_id);
        let position = self.get_position(oms_state);
        let target_usd = self.target_position_rx.as_ref().map(|rx| *rx.borrow()).unwrap_or(0.0);
        let target = fair
            .map(|f| target_usd / f)
            .unwrap_or(0.0);
        let max_pos = fair
            .map(|f| self.config.max_position_usd.unwrap() / f)
            .unwrap_or(0.0);

        let basis_bps = match (basis, fair) {
            (Some(b), Some(f)) if f != 0.0 => b / f * 10_000.0,
            _ => 0.0,
        };

        let skew_bps = if let Some(_f) = fair {
            self.cached_skew_bps
        } else {
            0.0
        };

        let bid_str = self
            .bid_quote
            .as_ref()
            .map(|q| format!("{:.6}", q.price))
            .unwrap_or_else(|| "-".into());
        let ask_str = self
            .ask_quote
            .as_ref()
            .map(|q| format!("{:.6}", q.price))
            .unwrap_or_else(|| "-".into());

        let (exch_age_ms, ref_feed) = self
            .fair_price
            .get_fair_price_detail(self.target_exchange, self.hl_symbol_id)
            .map(|(_, ex_ts_ms, feed, _, _)| {
                let now_ms = chrono::Utc::now().timestamp_millis();
                (now_ms - ex_ts_ms, feed)
            })
            .unwrap_or((-1, "none"));

        let hl_mid = self
            .fair_price
            .get_mid(self.target_exchange, self.hl_symbol_id);
        let residual_bps = match (hl_mid, fair) {
            (Some(hl), Some(f)) if f != 0.0 => (hl - f) / f * 10_000.0,
            _ => 0.0,
        };

        let factor_bps = match (fair, &self.factor_model) {
            (Some(f), Some(fm)) if fm.seeded && f != 0.0 => {
                let mut sum = 0.0f64;
                for fac in &fm.factors {
                    if let Some((mid, _, _)) = factor_mid(&self.market_data, fac) {
                        sum += fac.beta * (mid.ln() - fac.snapshot_log_mid);
                    }
                }
                sum * 10_000.0
            }
            _ => 0.0,
        };

        let vol_mult = self.cached_vol_mult;
        let adj_min_bps = self.config.ref_min_spread_bps.unwrap() * vol_mult;
        let adj_spread_bps = self.config.ref_half_spread_bps.unwrap() * vol_mult;
        let adj_requote_bps = self.config.ref_requote_tolerance_bps.unwrap() * vol_mult;

        let pred_vol_ann = vol_provider
            .as_ref()
            .map(|vp| vp.ann_vol(self.vol_group_idx))
            .unwrap_or(0.0);

        let want_bid = position - target < max_pos;
        let want_ask = position - target > -max_pos;

        info!(
            "[{}] fair={:.6} hl_mid={:.6} resid={:+.2}bps basis={:+.2}bps factor={:+.2}bps skew={:+.2}bps vol={:.1}% vmult={:.2} band=[{:.1},{:.1},{:.1}]bps pos={:+.6} target={:+.6} maxpos={:.4} want=[{},{}] bid={} ask={} ref={}@{}ms",
            self.config.symbol,
            fair.unwrap_or(0.0),
            hl_mid.unwrap_or(0.0),
            residual_bps,
            basis_bps,
            factor_bps,
            skew_bps,
            pred_vol_ann * 100.0,
            vol_mult,
            adj_min_bps,
            adj_spread_bps,
            adj_spread_bps + adj_requote_bps,
            position,
            target,
            max_pos,
            want_bid as u8,
            want_ask as u8,
            bid_str,
            ask_str,
            ref_feed,
            exch_age_ms,
        );
    }

    // -----------------------------------------------------------------------
    // Display
    // -----------------------------------------------------------------------

    fn send_display_status(
        &self,
        oms_state: &OmsStateTracker,
        vol_provider: &Option<crypto_feeds::vol_provider::VolProvider>,
    ) {
        let bus = match &self.display {
            Some(b) => b,
            None => return,
        };
        let fair = self
            .fair_price
            .get_fair_price(self.target_exchange, self.hl_symbol_id);
        let basis = self
            .fair_price
            .get_basis(self.target_exchange, self.hl_symbol_id);
        let position = self.get_position(oms_state);
        let target_usd = self.target_position_rx.as_ref().map(|rx| *rx.borrow()).unwrap_or(0.0);
        let fair_val = fair.unwrap_or(0.0);
        let target = if fair_val != 0.0 { target_usd / fair_val } else { 0.0 };
        let max_pos = if fair_val != 0.0 { self.config.max_position_usd.unwrap() / fair_val } else { 0.0 };

        let basis_bps = match (basis, fair) {
            (Some(b), Some(f)) if f != 0.0 => b / f * 10_000.0,
            _ => 0.0,
        };

        let hl_mid = self
            .fair_price
            .get_mid(self.target_exchange, self.hl_symbol_id);
        let residual_bps = match (hl_mid, fair) {
            (Some(hl), Some(f)) if f != 0.0 => (hl - f) / f * 10_000.0,
            _ => 0.0,
        };

        let factor_bps = match (fair, &self.factor_model) {
            (Some(f), Some(fm)) if fm.seeded && f != 0.0 => {
                let mut sum = 0.0f64;
                for fac in &fm.factors {
                    if let Some((mid, _, _)) = factor_mid(&self.market_data, fac) {
                        sum += fac.beta * (mid.ln() - fac.snapshot_log_mid);
                    }
                }
                sum * 10_000.0
            }
            _ => 0.0,
        };

        let vol_mult = self.cached_vol_mult;

        let feed_age_ms = self
            .fair_price
            .get_fair_price_detail(self.target_exchange, self.hl_symbol_id)
            .map(|(_, ex_ts_ms, _, _, _)| {
                chrono::Utc::now().timestamp_millis() - ex_ts_ms
            })
            .unwrap_or(-1);

        let pred_vol_ann = vol_provider
            .as_ref()
            .map(|vp| vp.ann_vol(self.vol_group_idx))
            .unwrap_or(0.0);

        bus.send_status(display::SymbolStatus {
            symbol: self.config.symbol.clone(),
            fair: fair_val,
            hl_mid: hl_mid.unwrap_or(0.0),
            residual_bps,
            basis_bps,
            factor_bps,
            skew_bps: self.cached_skew_bps,
            vol_ann_pct: pred_vol_ann * 100.0,
            vol_mult,
            band_min_bps: self.config.ref_min_spread_bps.unwrap() * vol_mult,
            band_spread_bps: self.config.ref_half_spread_bps.unwrap() * vol_mult,
            band_requote_bps: self.config.ref_requote_tolerance_bps.unwrap() * vol_mult,
            min_edge_bps: self.config.min_edge_bps.unwrap(),
            position,
            target,
            max_pos,
            want_bid: position - target < max_pos,
            want_ask: position - target > -max_pos,
            bid_price: self.bid_quote.as_ref().map(|q| q.price),
            ask_price: self.ask_quote.as_ref().map(|q| q.price),
            bid_quote: self.bid_quote.as_ref().map(|q| {
                let state = oms_state.get_order(q.client_id)
                    .map(|h| format!("{:?}", h.state))
                    .unwrap_or_else(|| "?".into());
                display::QuoteInfo {
                    cid: q.client_id.0,
                    price: q.price,
                    size: q.size,
                    state,
                    age_ms: q.placed_at.elapsed().as_millis() as u64,
                }
            }),
            ask_quote: self.ask_quote.as_ref().map(|q| {
                let state = oms_state.get_order(q.client_id)
                    .map(|h| format!("{:?}", h.state))
                    .unwrap_or_else(|| "?".into());
                display::QuoteInfo {
                    cid: q.client_id.0,
                    price: q.price,
                    size: q.size,
                    state,
                    age_ms: q.placed_at.elapsed().as_millis() as u64,
                }
            }),
            cancel_bid_bps: if fair_val > 0.0 {
                let mid = fair_val + self.cached_skew_bps * fair_val / 10_000.0;
                let min_spread = self.config.ref_min_spread_bps.unwrap() * vol_mult * fair_val / 10_000.0;
                let min_edge = self.config.min_edge_bps.unwrap() * fair_val / 10_000.0;
                let max_bid = (mid - min_spread).min(fair_val - min_edge);
                (max_bid - fair_val) / fair_val * 10_000.0
            } else { 0.0 },
            cancel_ask_bps: if fair_val > 0.0 {
                let mid = fair_val + self.cached_skew_bps * fair_val / 10_000.0;
                let min_spread = self.config.ref_min_spread_bps.unwrap() * vol_mult * fair_val / 10_000.0;
                let min_edge = self.config.min_edge_bps.unwrap() * fair_val / 10_000.0;
                let min_ask = (mid + min_spread).max(fair_val + min_edge);
                (min_ask - fair_val) / fair_val * 10_000.0
            } else { 0.0 },
            feed_age_ms,
        });
    }

    // -----------------------------------------------------------------------
    // Position helper
    // -----------------------------------------------------------------------

    fn get_position(&self, oms_state: &OmsStateTracker) -> f64 {
        if let Some(p) = oms_state.positions().get(&self.config.symbol) {
            return match p.side {
                Side::Buy => p.size,
                Side::Sell => -p.size,
            };
        }
        0.0
    }

    // -----------------------------------------------------------------------
    // OMS event handling — called by outer loop after drain
    // -----------------------------------------------------------------------

    pub fn handle_oms_event(&mut self, event: &OmsEvent, oms_state: &OmsStateTracker) {
        match event {
            OmsEvent::OrderAccepted { client_id, .. } => {
                if self.is_our_order(client_id, oms_state) {
                    self.consecutive_rejects = 0;
                    self.reject_pause_until = None;
                    // Pre-sign cancel for fast path
                    match self.oms.presign_cancel(client_id) {
                        Ok(signed) => {
                            self.presigned_cancels.insert(*client_id, signed);
                        }
                        Err(e) => debug!("pre-sign cancel failed for {}: {e}", client_id.0),
                    }
                }
            }
            OmsEvent::OrderPartialFill(fill) => {
                if self.is_our_order(&fill.client_id, oms_state) {
                    info!(
                        "[{}] partial fill: cid={} side={:?} price={:.6} size={:.4}",
                        self.config.symbol, fill.client_id.0, fill.side, fill.price, fill.size,
                    );
                    self.consecutive_rejects = 0;
                    self.reject_pause_until = None;
                    if let Some(h) = oms_state.get_order(fill.client_id) {
                        if h.filled_size >= h.size - 1e-12 {
                            self.clear_tracker(&fill.client_id);
                        }
                    }
                }
            }
            OmsEvent::OrderFilled(fill) => {
                if self.is_our_order(&fill.client_id, oms_state) {
                    info!(
                        "[{}] fill: cid={} side={:?} price={:.6} size={:.4}",
                        self.config.symbol, fill.client_id.0, fill.side, fill.price, fill.size,
                    );
                    self.consecutive_rejects = 0;
                    self.reject_pause_until = None;
                    self.clear_tracker(&fill.client_id);
                }
            }
            OmsEvent::OrderCancelled(cid) => {
                if self.is_our_order(cid, oms_state) {
                    debug!("[{}] order cancelled: cid={}", self.config.symbol, cid.0);
                    self.clear_tracker(cid);
                }
            }
            OmsEvent::OrderRejected { client_id, reason } => {
                if self.is_our_order(client_id, oms_state) {
                    warn!(
                        "[{}] order rejected: cid={} reason={}",
                        self.config.symbol, client_id.0, reason
                    );
                    self.clear_tracker(client_id);
                    self.consecutive_rejects += 1;
                }
            }
            _ => {}
        }
    }

    /// Check if an order from open_orders matches our tracked bid or ask.
    fn is_tracked_order(&self, o: &OrderHandle) -> bool {
        for q in [&self.bid_quote, &self.ask_quote].into_iter().flatten() {
            if q.client_id == o.client_id {
                return true;
            }
            if let (Some(q_eid), Some(o_eid)) = (&q.exchange_id, &o.exchange_id) {
                if q_eid == o_eid {
                    return true;
                }
            }
        }
        false
    }

    fn is_our_order(&self, cid: &ClientOrderId, oms_state: &OmsStateTracker) -> bool {
        for q in [&self.bid_quote, &self.ask_quote].into_iter().flatten() {
            if q.client_id == *cid {
                return true;
            }
            if let Some(ref q_eid) = q.exchange_id {
                if let Some(h) = oms_state.get_order(*cid) {
                    if h.exchange_id.as_ref() == Some(q_eid) {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn clear_tracker(&mut self, cid: &ClientOrderId) {
        if self
            .bid_quote
            .as_ref()
            .map(|q| q.client_id == *cid)
            .unwrap_or(false)
        {
            self.bid_quote = None;
        }
        if self
            .ask_quote
            .as_ref()
            .map(|q| q.client_id == *cid)
            .unwrap_or(false)
        {
            self.ask_quote = None;
        }
        self.presigned_cancels.remove(cid);
    }

    // -----------------------------------------------------------------------
    // Cancel helpers
    // -----------------------------------------------------------------------

    fn cancel_stray_orders(&mut self, oms_state: &OmsStateTracker) {
        if self.ghost {
            return;
        }

        let open: Vec<OrderHandle> = oms_state
            .open_orders(Some(&self.config.symbol))
            .into_iter()
            .cloned()
            .collect();
        let min_age = Duration::from_millis(self.config.stray_order_age_ms.unwrap());

        let fair = self
            .fair_price
            .get_fair_price(self.target_exchange, self.hl_symbol_id)
            .unwrap_or(0.0);

        let mut stray_bids: Vec<&OrderHandle> = Vec::new();
        let mut stray_asks: Vec<&OrderHandle> = Vec::new();

        for o in &open {
            if self.is_tracked_order(o) {
                continue;
            }
            if o.state == OrderState::Cancelling {
                continue;
            }
            let old_enough = o
                .last_modified
                .map(|t| t.elapsed() >= min_age)
                .unwrap_or(true);
            if !old_enough {
                continue;
            }

            match o.side {
                Side::Buy => stray_bids.push(o),
                Side::Sell => stray_asks.push(o),
            }
        }

        if !stray_bids.is_empty() {
            if self.bid_quote.is_some() {
                for o in &stray_bids {
                    warn!(
                        "cancelling stray bid cid={} price={:?}",
                        o.client_id.0, o.order_type
                    );
                    let oms = Arc::clone(&self.oms);
                    let cid = o.client_id;
                    tokio::spawn(async move {
                        if let Err(e) = oms.cancel_order(&cid).await {
                            warn!("failed to cancel stray bid {}: {e}", cid.0);
                        }
                    });
                }
            } else {
                self.adopt_closest_cancel_rest(&stray_bids, Side::Buy, fair);
            }
        }

        if !stray_asks.is_empty() {
            if self.ask_quote.is_some() {
                for o in &stray_asks {
                    warn!(
                        "cancelling stray ask cid={} price={:?}",
                        o.client_id.0, o.order_type
                    );
                    let oms = Arc::clone(&self.oms);
                    let cid = o.client_id;
                    tokio::spawn(async move {
                        if let Err(e) = oms.cancel_order(&cid).await {
                            warn!("failed to cancel stray ask {}: {e}", cid.0);
                        }
                    });
                }
            } else {
                self.adopt_closest_cancel_rest(&stray_asks, Side::Sell, fair);
            }
        }
    }

    fn adopt_closest_cancel_rest(&mut self, strays: &[&OrderHandle], side: Side, fair: f64) {
        if strays.is_empty() {
            return;
        }

        let best_idx = strays
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                let price_a = match a.order_type {
                    OrderType::Limit { price, .. } => price,
                    _ => 0.0,
                };
                let price_b = match b.order_type {
                    OrderType::Limit { price, .. } => price,
                    _ => 0.0,
                };
                let dist_a = (price_a - fair).abs();
                let dist_b = (price_b - fair).abs();
                dist_a
                    .partial_cmp(&dist_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(i, _)| i)
            .unwrap_or(0);

        for (i, o) in strays.iter().enumerate() {
            if i == best_idx {
                let price = match o.order_type {
                    OrderType::Limit { price, .. } => price,
                    _ => 0.0,
                };
                info!(
                    "adopting stray {:?} cid={} price={:.6} as tracked quote",
                    side, o.client_id.0, price
                );
                let quote = LiveQuote {
                    client_id: o.client_id,
                    exchange_id: o.exchange_id.clone(),
                    price,
                    size: o.size,
                    placed_at: o.submitted_at.unwrap_or_else(Instant::now),
                };
                match side {
                    Side::Buy => self.bid_quote = Some(quote),
                    Side::Sell => self.ask_quote = Some(quote),
                }
            } else {
                warn!(
                    "cancelling duplicate {:?} cid={} price={:?}",
                    side, o.client_id.0, o.order_type
                );
                let oms = Arc::clone(&self.oms);
                let cid = o.client_id;
                tokio::spawn(async move {
                    if let Err(e) = oms.cancel_order(&cid).await {
                        warn!("failed to cancel duplicate {}: {e}", cid.0);
                    }
                });
            }
        }
    }

    pub fn cancel_all_quotes(&mut self) {
        if self.ghost {
            if self.bid_quote.is_some() || self.ask_quote.is_some() {
                info!("[GHOST] would CANCEL ALL quotes");
            }
            self.bid_quote = None;
            self.ask_quote = None;
            return;
        }

        if let Some(ref q) = self.bid_quote {
            info!("cancelling bid cid={}", q.client_id.0);
            let oms = Arc::clone(&self.oms);
            let cid = q.client_id;
            tokio::spawn(async move {
                if let Err(e) = oms.cancel_order(&cid).await {
                    warn!("failed to cancel bid {}: {e}", cid.0);
                }
            });
        }
        if let Some(ref q) = self.ask_quote {
            info!("cancelling ask cid={}", q.client_id.0);
            let oms = Arc::clone(&self.oms);
            let cid = q.client_id;
            tokio::spawn(async move {
                if let Err(e) = oms.cancel_order(&cid).await {
                    warn!("failed to cancel ask {}: {e}", cid.0);
                }
            });
        }
        self.bid_quote = None;
        self.ask_quote = None;
        self.presigned_cancels.clear();
    }

    fn transition_to_paused(&mut self) {
        info!("[{}] entering Paused state", self.config.symbol);
        self.cancel_all_quotes();
        self.state = EngineState::Paused;
        self.running_since = None;
        self.warmed_up = false;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crypto_feeds::AllMarketData;
    use crypto_feeds::market_data::MarketData;

    fn push_quote(
        md: &AllMarketData,
        exchange: ExchangeId,
        symbol_id: SymbolId,
        bid: f64,
        ask: f64,
    ) {
        let coll = collection_for(md, exchange);
        coll.push(
            &symbol_id,
            MarketData {
                bid: Some(bid),
                ask: Some(ask),
                bid_qty: None,
                ask_qty: None,
                exchange_ts_raw: Some(chrono::Utc::now()),
                exchange_ts: Some(chrono::Utc::now()),
                received_ts: Some(chrono::Utc::now()),
                received_instant: Some(Instant::now()),
                feed_latency_ns: 0,
            },
        );
    }

    #[test]
    fn factor_model_none_direct_tick_only() {
        let md = AllMarketData::new();
        let eth_id = *REGISTRY
            .lookup("ETH_USDC", &InstrumentType::Perp)
            .expect("ETH_USDC perp must be in registry");

        push_quote(&md, ExchangeId::Binance, eth_id, 2000.0, 2001.0);

        let factor = FactorState {
            symbol_id: eth_id,
            exchanges: vec![ExchangeId::Binance],
            beta: 0.97,
            snapshot_log_mid: 0.0,
        };

        let (mid, _, _) = factor_mid(&md, &factor).expect("should find ETH mid");
        assert!((mid - 2000.5).abs() < 0.01);
    }

    #[test]
    fn factor_model_corr_fair_single_factor() {
        let md = AllMarketData::new();
        let eth_id = *REGISTRY
            .lookup("ETH_USDC", &InstrumentType::Perp)
            .expect("ETH_USDC perp must be in registry");

        let beta = 0.97;

        push_quote(&md, ExchangeId::Binance, eth_id, 1999.5, 2000.5);
        let (snapshot_mid, _, _) = factor_mid(
            &md,
            &FactorState {
                symbol_id: eth_id,
                exchanges: vec![ExchangeId::Binance],
                beta,
                snapshot_log_mid: 0.0,
            },
        )
        .unwrap();

        let snapshot_log_mid = snapshot_mid.ln();
        let direct_fair = 0.15;

        push_quote(&md, ExchangeId::Binance, eth_id, 2019.5, 2020.5);

        let factor = FactorState {
            symbol_id: eth_id,
            exchanges: vec![ExchangeId::Binance],
            beta,
            snapshot_log_mid,
        };

        let (current_mid, _, _) = factor_mid(&md, &factor).unwrap();
        let r = current_mid.ln() - snapshot_log_mid;
        let corr_fair = direct_fair * (beta * r).exp();

        let expected_pct_move = beta * r;
        let actual_pct_move = (corr_fair / direct_fair).ln();
        assert!(
            (actual_pct_move - expected_pct_move).abs() < 1e-10,
            "corr_fair pct move should be beta * ETH return"
        );

        assert!(corr_fair > direct_fair);
    }

    #[test]
    fn factor_model_corr_fair_multi_factor() {
        let md = AllMarketData::new();
        let eth_id = *REGISTRY
            .lookup("ETH_USDC", &InstrumentType::Perp)
            .expect("ETH_USDC perp");
        let btc_id = *REGISTRY
            .lookup("BTC_USDC", &InstrumentType::Perp)
            .expect("BTC_USDC perp");

        let beta_eth = 0.97;
        let beta_btc = 0.30;

        push_quote(&md, ExchangeId::Binance, eth_id, 1999.5, 2000.5);
        push_quote(&md, ExchangeId::Binance, btc_id, 59999.5, 60000.5);

        let eth_factor = FactorState {
            symbol_id: eth_id,
            exchanges: vec![ExchangeId::Binance],
            beta: beta_eth,
            snapshot_log_mid: factor_mid(
                &md,
                &FactorState {
                    symbol_id: eth_id,
                    exchanges: vec![ExchangeId::Binance],
                    beta: beta_eth,
                    snapshot_log_mid: 0.0,
                },
            )
            .unwrap()
            .0
            .ln(),
        };
        let btc_factor = FactorState {
            symbol_id: btc_id,
            exchanges: vec![ExchangeId::Binance],
            beta: beta_btc,
            snapshot_log_mid: factor_mid(
                &md,
                &FactorState {
                    symbol_id: btc_id,
                    exchanges: vec![ExchangeId::Binance],
                    beta: beta_btc,
                    snapshot_log_mid: 0.0,
                },
            )
            .unwrap()
            .0
            .ln(),
        };

        let direct_fair = 0.15;

        push_quote(&md, ExchangeId::Binance, eth_id, 2019.5, 2020.5);
        push_quote(&md, ExchangeId::Binance, btc_id, 59699.5, 59700.5);

        let (eth_mid, _, _) = factor_mid(&md, &eth_factor).unwrap();
        let (btc_mid, _, _) = factor_mid(&md, &btc_factor).unwrap();

        let r_eth = eth_mid.ln() - eth_factor.snapshot_log_mid;
        let r_btc = btc_mid.ln() - btc_factor.snapshot_log_mid;
        let sum = beta_eth * r_eth + beta_btc * r_btc;
        let corr_fair = direct_fair * sum.exp();

        let single_eth_fair = direct_fair * (beta_eth * r_eth).exp();
        let single_btc_fair = direct_fair * (beta_btc * r_btc).exp();

        assert!(r_eth > 0.0, "ETH should have positive return");
        assert!(r_btc < 0.0, "BTC should have negative return");
        assert!(
            corr_fair < single_eth_fair,
            "multi-factor fair should be below ETH-only fair (BTC dragged it down)"
        );
        assert!(
            corr_fair > single_btc_fair,
            "multi-factor fair should be above BTC-only fair (ETH pushed it up)"
        );

        let expected = direct_fair * (beta_eth * r_eth).exp() * (beta_btc * r_btc).exp();
        assert!(
            (corr_fair - expected).abs() < 1e-15,
            "multi-factor should equal product of individual factor adjustments"
        );
    }

    #[test]
    fn factor_model_no_move_returns_none() {
        let md = AllMarketData::new();
        let eth_id = *REGISTRY
            .lookup("ETH_USDC", &InstrumentType::Perp)
            .expect("ETH_USDC perp");

        push_quote(&md, ExchangeId::Binance, eth_id, 1999.5, 2000.5);

        let (mid, _, _) = factor_mid(
            &md,
            &FactorState {
                symbol_id: eth_id,
                exchanges: vec![ExchangeId::Binance],
                beta: 0.97,
                snapshot_log_mid: 0.0,
            },
        )
        .unwrap();

        let snapshot_log = mid.ln();

        let factor = FactorState {
            symbol_id: eth_id,
            exchanges: vec![ExchangeId::Binance],
            beta: 0.97,
            snapshot_log_mid: snapshot_log,
        };

        let (current_mid, _, _) = factor_mid(&md, &factor).unwrap();
        let r = current_mid.ln() - snapshot_log;
        assert!(r.abs() < 1e-15, "no move should produce zero return");
    }

    #[test]
    fn factor_model_write_count_changes() {
        let md = AllMarketData::new();
        let eth_id = *REGISTRY
            .lookup("ETH_USDC", &InstrumentType::Perp)
            .expect("ETH_USDC perp");

        let fm = FactorModelState {
            factors: vec![FactorState {
                symbol_id: eth_id,
                exchanges: vec![ExchangeId::Binance],
                beta: 0.97,
                snapshot_log_mid: 0.0,
            }],
            last_direct_fair: 0.15,
            last_factor_wc: 0,
            seeded: true,
        };

        let wc0 = total_factor_wc(&md, &fm);
        assert_eq!(wc0, 0, "no data pushed yet");

        push_quote(&md, ExchangeId::Binance, eth_id, 1999.5, 2000.5);
        let wc1 = total_factor_wc(&md, &fm);
        assert!(wc1 > wc0, "write count should increase after push");

        push_quote(&md, ExchangeId::Binance, eth_id, 2019.5, 2020.5);
        let wc2 = total_factor_wc(&md, &fm);
        assert!(wc2 > wc1, "write count should increase again");
    }

    #[test]
    fn factor_model_config_deserializes() {
        let yaml = r#"
symbol: PERP_AIXBT_USDC
order_notional_usd: 100.0
max_position_usd: 10000.0
ref_half_spread_bps: 10
ref_min_spread_bps: 5
factor_model:
  enabled: true
  factors:
    - symbol: PERP_ETH_USDT
      exchanges: [binance, okx, bybit]
      beta: 0.97
    - symbol: PERP_VIRTUAL_USDC
      exchanges: [binance, bybit]
      beta: 0.45
"#;
        let cfg: StrategyConfig = serde_yaml::from_str(yaml).expect("should deserialize");
        let fm = cfg.factor_model.expect("factor_model should be Some");
        assert!(fm.enabled);
        assert_eq!(fm.factors.len(), 2);
        assert_eq!(fm.factors[0].symbol, "PERP_ETH_USDT");
        assert_eq!(fm.factors[0].exchanges, vec!["binance", "okx", "bybit"]);
        assert!((fm.factors[0].beta - 0.97).abs() < 1e-10);
        assert_eq!(fm.factors[1].symbol, "PERP_VIRTUAL_USDC");
        assert!((fm.factors[1].beta - 0.45).abs() < 1e-10);
    }

    #[test]
    fn factor_model_config_absent_is_none() {
        let yaml = r#"
symbol: PERP_AIXBT_USDC
order_notional_usd: 100.0
max_position_usd: 10000.0
ref_half_spread_bps: 10
ref_min_spread_bps: 5
"#;
        let cfg: StrategyConfig = serde_yaml::from_str(yaml).expect("should deserialize");
        assert!(cfg.factor_model.is_none());
    }
}
