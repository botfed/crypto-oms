pub mod config;
pub mod fair_price;
pub mod inventory;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use oms_core::*;
use tokio::sync::{Notify, broadcast};
use tracing::{debug, info, warn};

use crate::hyperliquid::HyperliquidOms;
use config::{MmParamSource, StrategyConfig};
use crypto_feeds::market_data::InstrumentType;
use crypto_feeds::symbol_registry::{REGISTRY, SymbolId};
use fair_price::{ExchangeId, FairPriceEngine};

// ---------------------------------------------------------------------------
// Latency profiling (compiled in with --features profiling)
// ---------------------------------------------------------------------------

#[cfg(feature = "profiling")]
pub mod latency {
    use parking_lot::Mutex;
    use std::sync::Arc;

    pub const METRIC_TICK_FAST: u8 = 0;
    pub const METRIC_T2D: u8 = 1;
    pub const METRIC_FAIR: u8 = 2;
    pub const METRIC_VOL: u8 = 3;
    pub const METRIC_TICK_SLOW: u8 = 4;
    pub const METRIC_T2T: u8 = 5;
    pub const METRIC_SIGN: u8 = 6;

    // File layout
    pub const HEADER_SIZE: usize = 64;
    pub const SAMPLE_SIZE: usize = 16; // metric_id(1) + pad(7) + value_ns(8)
    pub const FILE_SIZE: usize = 16 * 1024 * 1024; // 16 MB (~1M entries)
    pub const CAPACITY: usize = (FILE_SIZE - HEADER_SIZE) / SAMPLE_SIZE;
    pub const LATENCY_PATH: &str = "/tmp/mm_latency.bin";

    /// Lightweight recorder for the hot loop.
    /// Just pushes to a local Vec. A background task flushes to mmap every 10s.
    pub struct LatencyRecorder {
        buf: Arc<Mutex<Vec<(u8, u64)>>>,
    }

    impl LatencyRecorder {
        #[inline]
        pub fn record(&self, metric: u8, nanos: u64) {
            // Lock is uncontended 99.99% of the time (only contended during 10s flush swap)
            self.buf.lock().push((metric, nanos));
        }
    }

    /// Initialize the profiler. Returns a recorder for the hot loop.
    /// Spawns a background task that flushes samples to mmap every 10s.
    pub fn init(shutdown: Arc<tokio::sync::Notify>) -> LatencyRecorder {
        let buf: Arc<Mutex<Vec<(u8, u64)>>> = Arc::new(Mutex::new(Vec::with_capacity(500_000)));
        let recorder = LatencyRecorder {
            buf: Arc::clone(&buf),
        };

        // Create/open the mmap file
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(LATENCY_PATH)
            .expect("failed to open latency mmap file");
        file.set_len(FILE_SIZE as u64)
            .expect("failed to set latency file size");

        let mut mmap =
            unsafe { memmap2::MmapMut::map_mut(&file).expect("failed to mmap latency file") };

        // Reset file on startup — fresh session
        mmap.fill(0);
        mmap[8..16].copy_from_slice(&(CAPACITY as u64).to_le_bytes());
        let mut write_pos: u64 = 0;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let shutdown_fut = shutdown.notified();
            tokio::pin!(shutdown_fut);

            loop {
                tokio::select! {
                    _ = &mut shutdown_fut => {
                        flush(&buf, &mut mmap, &mut write_pos);
                        return;
                    }
                    _ = interval.tick() => {
                        flush(&buf, &mut mmap, &mut write_pos);
                    }
                }
            }
        });

        recorder
    }

    fn flush(buf: &Mutex<Vec<(u8, u64)>>, mmap: &mut memmap2::MmapMut, write_pos: &mut u64) {
        let samples = {
            let mut guard = buf.lock();
            std::mem::replace(&mut *guard, Vec::with_capacity(500_000))
        };

        if samples.is_empty() {
            return;
        }

        let cap = CAPACITY as u64;
        for (metric, value) in &samples {
            let idx = (*write_pos % cap) as usize;
            let offset = HEADER_SIZE + idx * SAMPLE_SIZE;
            mmap[offset] = *metric;
            // bytes 1-7 are padding (zeroed)
            mmap[offset + 1..offset + 8].fill(0);
            mmap[offset + 8..offset + 16].copy_from_slice(&value.to_le_bytes());
            *write_pos += 1;
        }

        // Update write_pos in header
        mmap[0..8].copy_from_slice(&write_pos.to_le_bytes());
        let _ = mmap.flush_async();
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
// MmEngine
// ---------------------------------------------------------------------------

pub struct MmEngine {
    oms: Arc<HyperliquidOms>,
    fair_price: Arc<FairPriceEngine>,
    vol_engine: Option<Arc<crypto_feeds::vol_engine::VolEngine>>,
    market_data: Arc<crypto_feeds::AllMarketData>,
    vol_model_name: String,
    params: Box<dyn MmParamSource>,
    config: StrategyConfig,
    shutdown: Arc<Notify>,
    ghost: bool,

    bid_quote: Option<LiveQuote>,
    ask_quote: Option<LiveQuote>,
    state: EngineState,

    hl_symbol_id: SymbolId,
    oms_events: broadcast::Receiver<OmsEvent>,
    last_quote_eval: Instant,
    last_status_log: Instant,
    running_since: Option<Instant>,
    consecutive_rejects: u32,
    last_ref_wc: u64,         // track seqlock write count to detect new ticks
    last_exchange_ts_ms: i64, // skip ticks with older exchange_ts
    #[cfg(feature = "profiling")]
    latency: latency::LatencyRecorder,
}

const MAX_CONSECUTIVE_REJECTS: u32 = 5;

impl MmEngine {
    pub fn new(
        oms: Arc<HyperliquidOms>,
        fair_price: Arc<FairPriceEngine>,
        vol_engine: Option<Arc<crypto_feeds::vol_engine::VolEngine>>,
        market_data: Arc<crypto_feeds::AllMarketData>,
        vol_model_name: String,
        params: Box<dyn MmParamSource>,
        config: StrategyConfig,
        ghost: bool,
        shutdown: Arc<Notify>,
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

        let oms_events = oms.subscribe();

        #[cfg(feature = "profiling")]
        let latency = latency::init(Arc::clone(&shutdown));

        Ok(Self {
            oms,
            fair_price,
            vol_engine,
            market_data,
            vol_model_name,
            params,
            config,
            shutdown,
            ghost,
            bid_quote: None,
            ask_quote: None,
            state: EngineState::Initializing,
            hl_symbol_id,
            oms_events,
            last_quote_eval: Instant::now(),
            last_status_log: Instant::now(),
            running_since: None,
            consecutive_rejects: 0,
            last_ref_wc: 0,
            last_exchange_ts_ms: 0,
            #[cfg(feature = "profiling")]
            latency,
        })
    }

    // -----------------------------------------------------------------------
    // Main loop — two cadences
    // -----------------------------------------------------------------------

    pub async fn run(&mut self) -> Result<()> {
        info!(
            "MM engine starting: symbol={} ghost={} post_only={}",
            self.config.symbol, self.ghost, self.config.post_only,
        );

        self.oms.wait_ready().await?;
        info!("OMS ready, entering main loop");

        let shutdown = Arc::clone(&self.shutdown);
        let shutdown_fut = shutdown.notified();
        tokio::pin!(shutdown_fut);

        loop {
            tokio::select! {
                _ = &mut shutdown_fut => {
                    info!("MM engine shutting down");
                    self.cancel_all_quotes().await;
                    return Ok(());
                }
                _ = tokio::task::yield_now() => {
                    self.drain_oms_events();

                    if !self.check_preconditions() {
                        if self.state == EngineState::Running {
                            self.transition_to_paused().await;
                        }
                        continue;
                    }

                    if self.state != EngineState::Running {
                        info!("MM engine entering Running state (warmup={}s)", self.config.warmup_secs);
                        self.state = EngineState::Running;
                        self.running_since = Some(Instant::now());
                    }

                    let warmed_up = self.running_since
                        .map(|t| t.elapsed() >= Duration::from_secs(self.config.warmup_secs))
                        .unwrap_or(false);

                    // ── CHECK FOR NEW DATA: skip if ref feed hasn't updated ──
                    let wc = self.fair_price.ref_write_count(ExchangeId::Hyperliquid, self.hl_symbol_id);
                    if wc == self.last_ref_wc {
                        continue; // no new tick, spin
                    }
                    self.last_ref_wc = wc;

                    #[cfg(feature = "profiling")]
                    let fair_start = Instant::now();

                    let tick_data = self.fair_price.get_fair_price_with_age(
                        ExchangeId::Hyperliquid, self.hl_symbol_id
                    );

                    let Some((_, exchange_ts_ms, recv_age_ns, _)) = tick_data else { continue; };

                    // Skip if this tick's exchange_ts is not newer than last processed
                    if exchange_ts_ms <= self.last_exchange_ts_ms {
                        continue;
                    }
                    self.last_exchange_ts_ms = exchange_ts_ms;

                    // ── NEW TICK ──
                    #[cfg(feature = "profiling")]
                    let tick_start = Instant::now();
                    #[cfg(feature = "profiling")]
                    if warmed_up {
                        self.latency.record(latency::METRIC_FAIR, fair_start.elapsed().as_nanos() as u64);
                        self.latency.record(latency::METRIC_T2D, recv_age_ns);
                    }

                    // Staleness check uses received_age (in ms)
                    let recv_age_ms = (recv_age_ns / 1_000_000) as i64;
                    if recv_age_ms > self.config.max_stale_ms as i64 {
                        if self.bid_quote.is_some() || self.ask_quote.is_some() {
                            warn!("ref feed stale (recv_age={}ms > {}ms), cancelling all quotes",
                                recv_age_ms, self.config.max_stale_ms);
                            self.cancel_all_quotes().await;
                        }
                        continue;
                    }

                    // Sync local trackers with OMS reality
                    self.sync_quote_state();

                    // ── FAST PATH: adverse cancel guard ──
                    self.fast_cancel_check();

                    // ── SLOW PATH (~quote_interval_ms): quote placement ──
                    let interval = Duration::from_millis(self.config.quote_interval_ms);
                    if warmed_up && self.last_quote_eval.elapsed() >= interval {
                        #[cfg(feature = "profiling")]
                        let vol_start = Instant::now();

                        self.evaluate_and_place_quotes();

                        #[cfg(feature = "profiling")]
                        if warmed_up {
                            self.latency.record(latency::METRIC_VOL, vol_start.elapsed().as_nanos() as u64);
                            self.latency.record(latency::METRIC_TICK_SLOW, tick_start.elapsed().as_nanos() as u64);
                        }

                        self.last_quote_eval = Instant::now();
                    }

                    #[cfg(feature = "profiling")]
                    if warmed_up {
                        self.latency.record(latency::METRIC_TICK_FAST, tick_start.elapsed().as_nanos() as u64);
                    }

                    // ── STATUS LOG (1/sec) ──
                    if self.last_status_log.elapsed() >= Duration::from_secs(1) {
                        self.log_status();
                        self.last_status_log = Instant::now();
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Preconditions
    // -----------------------------------------------------------------------

    fn check_preconditions(&mut self) -> bool {
        let should_log = self.last_status_log.elapsed() >= Duration::from_secs(1);

        if !self.oms.is_ready() {
            if should_log {
                warn!("waiting: OMS not ready");
                self.last_status_log = Instant::now();
            }
            return false;
        }
        if !self.params.enabled() {
            if should_log {
                warn!("waiting: params disabled");
                self.last_status_log = Instant::now();
            }
            return false;
        }
        if self.consecutive_rejects >= MAX_CONSECUTIVE_REJECTS {
            if should_log {
                warn!("paused: {} consecutive rejects", self.consecutive_rejects);
                self.last_status_log = Instant::now();
            }
            return false;
        }

        // Check fair price is available
        let fp = self
            .fair_price
            .get_fair_price(ExchangeId::Hyperliquid, self.hl_symbol_id);
        if fp.is_none() {
            if should_log {
                let basis = self
                    .fair_price
                    .get_basis(ExchangeId::Hyperliquid, self.hl_symbol_id);
                warn!(
                    "waiting: no fair price (basis={:?}, check feeds for ref symbol)",
                    basis,
                );
                self.last_status_log = Instant::now();
            }
            return false;
        }

        // Check feed is alive (engine-level — feed completely dead)
        if let Some(age_ms) = self
            .fair_price
            .get_ref_age_ms(ExchangeId::Hyperliquid, self.hl_symbol_id)
        {
            if age_ms > self.config.max_feed_age_ms as i64 {
                if should_log {
                    warn!(
                        "paused: reference feed dead (age={}ms > {}ms)",
                        age_ms, self.config.max_feed_age_ms
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

    fn sync_quote_state(&mut self) {
        if let Some(ref mut q) = self.bid_quote {
            match self.oms.get_order(&q.client_id) {
                Some(h)
                    if matches!(
                        h.state,
                        OrderState::Accepted
                            | OrderState::PartiallyFilled
                            | OrderState::Inflight
                            | OrderState::Cancelling
                    ) =>
                {
                    if q.exchange_id.is_none() && h.exchange_id.is_some() {
                        q.exchange_id = h.exchange_id.clone();
                    }
                    // Sync remaining size on partial fills
                    q.size = h.size - h.filled_size;
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
            match self.oms.get_order(&q.client_id) {
                Some(h)
                    if matches!(
                        h.state,
                        OrderState::Accepted
                            | OrderState::PartiallyFilled
                            | OrderState::Inflight
                            | OrderState::Cancelling
                    ) =>
                {
                    if q.exchange_id.is_none() && h.exchange_id.is_some() {
                        q.exchange_id = h.exchange_id.clone();
                    }
                    q.size = h.size - h.filled_size;
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

    // -----------------------------------------------------------------------
    // Fast path: cancel guard (inner-side adverse detection)
    // -----------------------------------------------------------------------

    fn fast_cancel_check(&mut self) {
        if self.bid_quote.is_none() && self.ask_quote.is_none() {
            return;
        }

        let Some(fair) = self
            .fair_price
            .get_fair_price(ExchangeId::Hyperliquid, self.hl_symbol_id)
        else {
            return;
        };

        let vol_mult = self.get_vol_multiplier();
        let min_edge = self.config.min_edge_bps * fair / 10_000.0;
        let min_spread =
            (self.config.ref_min_spread_bps * vol_mult * fair / 10_000.0).max(min_edge);

        // Check bid: inside min_spread from fair?
        if let Some(ref q) = self.bid_quote {
            let state = self.oms.get_order(&q.client_id).map(|h| h.state);
            let can_cancel = matches!(
                state,
                Some(OrderState::Accepted) | Some(OrderState::PartiallyFilled)
            );
            if can_cancel && q.price > fair - min_spread {
                let drift_bps = (q.price - (fair - min_spread)) / fair * 10_000.0;
                info!(
                    "fast cancel bid cid={} price={:.6} ({:.1}bps inside)",
                    q.client_id.0, q.price, drift_bps
                );
                #[cfg(feature = "profiling")]
                let sign_start = Instant::now();
                match self.oms.sign_cancel_order(&q.client_id) {
                    Ok(signed) => {
                        #[cfg(feature = "profiling")]
                        self.latency.record(latency::METRIC_SIGN, sign_start.elapsed().as_nanos() as u64);
                        let oms = Arc::clone(&self.oms);
                        let cid = q.client_id;
                        #[cfg(feature = "profiling")]
                        self.record_t2t();
                        if self.ghost {
                            self.bid_quote = None;
                        } else {
                            tokio::spawn(async move {
                                oms.post_cancel_order(&cid, signed).await;
                            });
                        }
                    }
                    Err(e) => warn!("failed to sign cancel bid {}: {e}", q.client_id.0),
                }
            }
        }

        // Check ask: inside min_spread from fair?
        if let Some(ref q) = self.ask_quote {
            let state = self.oms.get_order(&q.client_id).map(|h| h.state);
            let can_cancel = matches!(
                state,
                Some(OrderState::Accepted) | Some(OrderState::PartiallyFilled)
            );
            if can_cancel && q.price < fair + min_spread {
                let drift_bps = ((fair + min_spread) - q.price) / fair * 10_000.0;
                info!(
                    "fast cancel ask cid={} price={:.6} ({:.1}bps inside)",
                    q.client_id.0, q.price, drift_bps
                );
                #[cfg(feature = "profiling")]
                let sign_start = Instant::now();
                match self.oms.sign_cancel_order(&q.client_id) {
                    Ok(signed) => {
                        #[cfg(feature = "profiling")]
                        self.latency.record(latency::METRIC_SIGN, sign_start.elapsed().as_nanos() as u64);
                        let oms = Arc::clone(&self.oms);
                        let cid = q.client_id;
                        #[cfg(feature = "profiling")]
                        self.record_t2t();
                        if self.ghost {
                            self.ask_quote = None;
                        } else {
                            tokio::spawn(async move {
                                oms.post_cancel_order(&cid, signed).await;
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

    fn evaluate_and_place_quotes(&mut self) {
        // Cancel any stray orders on our symbol that we don't own
        self.cancel_stray_orders();

        // Staleness check — don't place on stale data
        let Some((fair, _, recv_age_ns, _)) = self
            .fair_price
            .get_fair_price_with_age(ExchangeId::Hyperliquid, self.hl_symbol_id)
        else {
            return;
        };
        if (recv_age_ns / 1_000_000) as i64 > self.config.max_stale_ms as i64 {
            return;
        }

        let position = self.get_position();
        let target = self.params.target_position_usd() / fair;
        let notional = self.params.order_notional_usd();
        let order_size = notional / fair;
        let max_pos = self.params.max_position_usd() / fair;

        let vol_mult = self.get_vol_multiplier();
        let min_edge = self.config.min_edge_bps * fair / 10_000.0;
        let half_spread =
            (self.config.ref_half_spread_bps * vol_mult * fair / 10_000.0).max(min_edge);
        let requote_thresh = self.config.ref_requote_tolerance_bps * vol_mult * fair / 10_000.0;

        let skewed_mid = self.compute_skewed_mid(fair, position, target, max_pos);
        let (desired_bid, desired_ask) = Self::clamp_to_fair(
            fair,
            skewed_mid - half_spread,
            skewed_mid + half_spread,
            self.config.min_edge_bps,
        );

        // Determine if we should quote each side (position limits)
        let want_bid = position < max_pos;
        let want_ask = position > -max_pos;

        // ── CANCEL stale/outer-side quotes ──

        // Bid: cancel if too passive (outer side) or shouldn't quote
        if let Some(ref q) = self.bid_quote {
            let should_cancel = !want_bid || (desired_bid - q.price) > requote_thresh; // bid too LOW = too passive

            if should_cancel {
                // Don't cancel inflight (no exchange OID yet)
                let skip_cancel = self
                    .oms
                    .get_order(&q.client_id)
                    .map(|h| matches!(h.state, OrderState::Inflight | OrderState::Cancelling))
                    .unwrap_or(false);

                if !skip_cancel {
                    if self.ghost {
                        info!(
                            "[GHOST] would CANCEL bid cid={} price={:.6} (slow path requote)",
                            q.client_id.0, q.price
                        );
                        self.bid_quote = None;
                    } else {
                        debug!("slow cancel bid cid={} price={:.6}", q.client_id.0, q.price);
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
            let should_cancel = !want_ask || (q.price - desired_ask) > requote_thresh; // ask too HIGH = too passive

            if should_cancel {
                let skip_cancel = self
                    .oms
                    .get_order(&q.client_id)
                    .map(|h| matches!(h.state, OrderState::Inflight | OrderState::Cancelling))
                    .unwrap_or(false);

                if !skip_cancel {
                    if self.ghost {
                        info!(
                            "[GHOST] would CANCEL ask cid={} price={:.6} (slow path requote)",
                            q.client_id.0, q.price
                        );
                        self.ask_quote = None;
                    } else {
                        debug!("slow cancel ask cid={} price={:.6}", q.client_id.0, q.price);
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
        // Placements are spawned fire-and-forget. The OMS inserts the Inflight order
        // before the HTTP call, so we can grab the cid and set the tracker immediately.
        // sync_quote_state sees Inflight → keeps tracker alive until Accepted/Rejected.

        if self.bid_quote.is_none() && want_bid && self.ghost {}
        if self.bid_quote.is_none() && want_bid && !self.ghost {
            let tif = if self.config.post_only {
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
                Ok((cid, sdk_req)) => match self.oms.sign_place_order(sdk_req) {
                    Ok(signed) => {
                        info!("placing bid cid={} price={:.6}", cid.0, desired_bid);
                        self.bid_quote = Some(LiveQuote {
                            client_id: cid,
                            exchange_id: None,
                            price: desired_bid,
                            size: order_size,
                            placed_at: Instant::now(),
                        });
                        let oms = Arc::clone(&self.oms);
                        tokio::spawn(async move {
                            oms.post_place_order(cid.0, signed).await;
                        });
                    }
                    Err(e) => warn!("failed to sign bid: {e}"),
                },
                Err(e) => warn!("failed to prepare bid: {e}"),
            }
        }

        if self.ask_quote.is_none() && want_ask && !self.ghost {
            let tif = if self.config.post_only {
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
                Ok((cid, sdk_req)) => match self.oms.sign_place_order(sdk_req) {
                    Ok(signed) => {
                        info!("placing ask cid={} price={:.6}", cid.0, desired_ask);
                        self.ask_quote = Some(LiveQuote {
                            client_id: cid,
                            exchange_id: None,
                            price: desired_ask,
                            size: order_size,
                            placed_at: Instant::now(),
                        });
                        let oms = Arc::clone(&self.oms);
                        tokio::spawn(async move {
                            oms.post_place_order(cid.0, signed).await;
                        });
                    }
                    Err(e) => warn!("failed to sign ask: {e}"),
                },
                Err(e) => warn!("failed to prepare ask: {e}"),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Inventory skew
    // -----------------------------------------------------------------------

    /// Logarithmic skew: aggressive near target, saturates for large deviations, capped at max_skew_bps.
    fn compute_skewed_mid(&self, fair_value: f64, position: f64, target: f64, max_pos: f64) -> f64 {
        let diff = target - position;
        if diff.abs() < 1e-12 {
            return fair_value;
        }
        let denom = if target.abs() > 1e-12 {
            target.abs()
        } else {
            max_pos
        };
        // raw ∈ [-ln(2), ln(2)] when diff ∈ [-denom, denom]
        // Normalize to [-1, 1] then scale by max_skew_bps
        let raw = diff.signum() * (1.0 + diff.abs() / denom).ln();
        let normalized = raw / std::f64::consts::LN_2; // ∈ [-1, 1] at 100% off target
        let skew_bps = (normalized * self.config.max_skew_bps)
            .clamp(-self.config.max_skew_bps, self.config.max_skew_bps);
        fair_value + skew_bps * fair_value / 10_000.0
    }

    /// Clamp desired bid/ask so quotes never cross fair value.
    /// Get the vol multiplier from the vol engine. Returns 1.0 if no vol engine.
    /// Record t2t: Utc::now() - received_ts of the winning feed, measured right now.
    #[cfg(feature = "profiling")]
    fn record_t2t(&self) {
        if let Some((_, _, recv_age_ns, _)) = self
            .fair_price
            .get_fair_price_with_age(ExchangeId::Hyperliquid, self.hl_symbol_id)
        {
            self.latency.record(latency::METRIC_T2T, recv_age_ns);
        }
    }

    fn get_vol_multiplier(&self) -> f64 {
        let Some(ref ve) = self.vol_engine else {
            return 1.0;
        };
        let preds = match ve.predict_now(&self.config.vol_symbol, &self.market_data) {
            Ok(p) => p,
            Err(_) => return 1.0,
        };
        let predicted = match self.vol_model_name.as_str() {
            "har_ols" => preds.har_ols,
            "har_qlike" => preds.har_qlike,
            "garch" => preds.garch,
            "ewma" => preds.ewma,
            _ => preds.har_qlike,
        };
        if self.config.ref_vol <= 0.0 || predicted <= 0.0 {
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

    fn log_status(&mut self) {
        let fair = self
            .fair_price
            .get_fair_price(ExchangeId::Hyperliquid, self.hl_symbol_id);
        let basis = self
            .fair_price
            .get_basis(ExchangeId::Hyperliquid, self.hl_symbol_id);
        let position = self.get_position();
        let target = fair
            .map(|f| self.params.target_position_usd() / f)
            .unwrap_or(0.0);
        let max_pos = fair
            .map(|f| self.params.max_position_usd() / f)
            .unwrap_or(0.0);

        let basis_bps = match (basis, fair) {
            (Some(b), Some(f)) if f != 0.0 => b / f * 10_000.0,
            _ => 0.0,
        };

        // Compute actual skew bps being applied
        let skew_bps = if let Some(f) = fair {
            let skewed = self.compute_skewed_mid(f, position, target, max_pos);
            (skewed - f) / f * 10_000.0
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

        let (recv_age_ms, ref_feed) = self
            .fair_price
            .get_fair_price_with_age(ExchangeId::Hyperliquid, self.hl_symbol_id)
            .map(|(_, _, recv_ns, feed)| ((recv_ns / 1_000_000) as i64, feed))
            .unwrap_or((-1, "none"));

        let hl_mid = self
            .fair_price
            .get_mid(ExchangeId::Hyperliquid, self.hl_symbol_id);
        let residual_bps = match (hl_mid, fair) {
            (Some(hl), Some(f)) if f != 0.0 => (hl - f) / f * 10_000.0,
            _ => 0.0,
        };

        let vol_mult = self.get_vol_multiplier();
        let adj_min_bps = (self.config.ref_min_spread_bps * vol_mult).max(self.config.min_edge_bps);
        let adj_spread_bps =
            (self.config.ref_half_spread_bps * vol_mult).max(self.config.min_edge_bps);
        let adj_requote_bps = self.config.ref_requote_tolerance_bps * vol_mult;

        // Get raw vol prediction for logging
        let pred_vol_ann = self
            .vol_engine
            .as_ref()
            .and_then(|ve| {
                ve.predict_now(&self.config.vol_symbol, &self.market_data)
                    .ok()
            })
            .map(|p| match self.vol_model_name.as_str() {
                "har_ols" => p.har_ols,
                "har_qlike" => p.har_qlike,
                "garch" => p.garch,
                "ewma" => p.ewma,
                _ => p.har_qlike,
            })
            .unwrap_or(0.0);

        info!(
            "status: fair={:.6} hl_mid={:.6} resid={:+.2}bps basis={:+.2}bps skew={:+.2}bps vol={:.1}% vmult={:.2} band=[{:.1},{:.1},{:.1}]bps pos={:+.6} target={:+.6} bid={} ask={} ref={}@{}ms",
            fair.unwrap_or(0.0),
            hl_mid.unwrap_or(0.0),
            residual_bps,
            basis_bps,
            skew_bps,
            pred_vol_ann * 100.0,
            vol_mult,
            adj_min_bps,
            adj_spread_bps,
            adj_spread_bps + adj_requote_bps,
            position,
            target,
            bid_str,
            ask_str,
            ref_feed,
            recv_age_ms,
        );
    }

    // -----------------------------------------------------------------------
    // Position helper
    // -----------------------------------------------------------------------

    fn get_position(&self) -> f64 {
        for p in self.oms.positions() {
            if p.symbol == self.config.symbol {
                return match p.side {
                    Side::Buy => p.size,
                    Side::Sell => -p.size,
                };
            }
        }
        0.0
    }

    // -----------------------------------------------------------------------
    // OMS event handling
    // -----------------------------------------------------------------------

    fn drain_oms_events(&mut self) {
        loop {
            match self.oms_events.try_recv() {
                Ok(event) => self.handle_oms_event(event),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    warn!("OMS event stream lagged by {n} events");
                    break;
                }
                Err(broadcast::error::TryRecvError::Closed) => break,
            }
        }
    }

    fn handle_oms_event(&mut self, event: OmsEvent) {
        match event {
            OmsEvent::Disconnected => {
                warn!("OMS disconnected");
                // State will be caught by check_preconditions → is_ready() == false
            }
            OmsEvent::Ready | OmsEvent::Reconnected => {
                info!("OMS ready/reconnected");
            }
            OmsEvent::OrderFilled(fill) => {
                if self.is_our_order(&fill.client_id) {
                    info!(
                        "fill: cid={} side={:?} price={:.6} size={:.4}",
                        fill.client_id.0, fill.side, fill.price, fill.size,
                    );
                    self.clear_tracker(&fill.client_id);
                }
            }
            OmsEvent::OrderCancelled(cid) => {
                if self.is_our_order(&cid) {
                    debug!("order cancelled: cid={}", cid.0);
                    self.clear_tracker(&cid);
                }
            }
            OmsEvent::OrderRejected { client_id, reason } => {
                if self.is_our_order(&client_id) {
                    warn!("order rejected: cid={} reason={}", client_id.0, reason);
                    self.clear_tracker(&client_id);
                    self.consecutive_rejects += 1;
                }
            }
            _ => {}
        }
    }

    /// Check if an order from open_orders matches our tracked bid or ask.
    /// Matches by client_id OR exchange_id (fallback for synthetic handles from REST poll).
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

    /// Check if a client order ID belongs to one of our tracked quotes.
    /// Matches by client_id directly, or by exchange_id if the cid corresponds
    /// to an OMS order with an exchange_id matching our tracker.
    fn is_our_order(&self, cid: &ClientOrderId) -> bool {
        for q in [&self.bid_quote, &self.ask_quote].into_iter().flatten() {
            if q.client_id == *cid {
                return true;
            }
            // Fallback: check if the OMS order's exchange_id matches our tracker
            if let Some(ref q_eid) = q.exchange_id {
                if let Some(h) = self.oms.get_order(cid) {
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
    }

    // -----------------------------------------------------------------------
    // Cancel helpers
    // -----------------------------------------------------------------------

    /// Cancel any open orders on our symbol that we don't recognize as ours.
    /// Handle duplicate/stray orders on our symbol.
    /// - If we have a tracked quote on a side: cancel all other orders on that side
    /// - If we DON'T have a tracked quote: adopt the closest to fair, cancel the rest
    /// - Only act on orders where last_modified age >= stray_order_age_ms
    /// - Skip orders in Cancelling state
    fn cancel_stray_orders(&mut self) {
        if self.ghost {
            return;
        }

        let open = self.oms.open_orders(Some(&self.config.symbol));
        let min_age = Duration::from_millis(self.config.stray_order_age_ms);

        let fair = self
            .fair_price
            .get_fair_price(ExchangeId::Hyperliquid, self.hl_symbol_id)
            .unwrap_or(0.0);

        // Separate strays by side
        let mut stray_bids: Vec<&OrderHandle> = Vec::new();
        let mut stray_asks: Vec<&OrderHandle> = Vec::new();

        for o in &open {
            // Skip our tracked orders — match by client_id OR exchange_id
            if self.is_tracked_order(o) {
                continue;
            }
            // Skip orders still being cancelled
            if o.state == OrderState::Cancelling {
                continue;
            }
            // Skip orders that are too recent
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

        // Handle stray bids
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

        // Handle stray asks
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

    /// From a list of stray orders on one side, adopt the one closest to fair price
    /// as our tracked quote, cancel the rest.
    fn adopt_closest_cancel_rest(&mut self, strays: &[&OrderHandle], side: Side, fair: f64) {
        if strays.is_empty() {
            return;
        }

        // Find the one closest to fair
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
                // Adopt this one
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

    async fn cancel_all_quotes(&mut self) {
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
            if let Err(e) = self.oms.cancel_order(&q.client_id).await {
                warn!("failed to cancel bid {}: {e}", q.client_id.0);
            }
        }
        if let Some(ref q) = self.ask_quote {
            info!("cancelling ask cid={}", q.client_id.0);
            if let Err(e) = self.oms.cancel_order(&q.client_id).await {
                warn!("failed to cancel ask {}: {e}", q.client_id.0);
            }
        }
        self.bid_quote = None;
        self.ask_quote = None;
    }

    async fn transition_to_paused(&mut self) {
        info!("MM engine entering Paused state");
        self.cancel_all_quotes().await;
        self.state = EngineState::Paused;
        self.running_since = None;
    }
}
