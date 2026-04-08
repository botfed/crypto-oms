pub mod config;
pub mod fair_price;
pub mod inventory;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use oms_core::*;
use tokio::sync::{Notify, broadcast};
use tracing::{info, warn, debug};

use crate::hyperliquid::HyperliquidOms;
use config::{MmParamSource, StrategyConfig};
use fair_price::{ExchangeId, FairPriceEngine};
use crypto_feeds::symbol_registry::{REGISTRY, SymbolId};
use crypto_feeds::market_data::InstrumentType;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct LiveQuote {
    client_id: ClientOrderId,
    price: f64,
    #[allow(dead_code)]
    size: f64,
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
}

const MAX_CONSECUTIVE_REJECTS: u32 = 5;

impl MmEngine {
    pub fn new(
        oms: Arc<HyperliquidOms>,
        fair_price: Arc<FairPriceEngine>,
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
        let base = config.symbol
            .strip_prefix("PERP_")
            .or_else(|| config.symbol.strip_prefix("SPOT_"))
            .unwrap_or(&config.symbol);

        let hl_symbol_id = *REGISTRY
            .lookup(base, &itype)
            .ok_or_else(|| anyhow::anyhow!("symbol not in registry: {}", config.symbol))?;

        let oms_events = oms.subscribe();

        Ok(Self {
            oms,
            fair_price,
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
                _ = tokio::time::sleep(Duration::from_millis(1)) => {
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

                    // ── STALENESS CHECK: cancel all if ref feed is stale ──
                    if let Some((_, age_ms)) = self.fair_price.get_fair_price_with_age(
                        ExchangeId::Hyperliquid, self.hl_symbol_id
                    ) {
                        if age_ms > self.config.max_stale_ms as i64 {
                            if self.bid_quote.is_some() || self.ask_quote.is_some() {
                                warn!("ref feed stale (age={}ms > {}ms), cancelling all quotes",
                                    age_ms, self.config.max_stale_ms);
                                self.cancel_all_quotes().await;
                            }
                            continue;
                        }
                    }

                    // Sync local trackers with OMS reality
                    self.sync_quote_state();

                    // ── FAST PATH (~1ms): adverse cancel guard ──
                    // (always run — even during warmup, cancel stale quotes)
                    self.fast_cancel_check().await;

                    // ── SLOW PATH (~quote_interval_ms): quote placement ──
                    // Only place quotes after warmup
                    let interval = Duration::from_millis(self.config.quote_interval_ms);
                    if warmed_up && self.last_quote_eval.elapsed() >= interval {
                        self.evaluate_and_place_quotes().await;
                        self.last_quote_eval = Instant::now();
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
        let fp = self.fair_price.get_fair_price(ExchangeId::Hyperliquid, self.hl_symbol_id);
        if fp.is_none() {
            if should_log {
                let basis = self.fair_price.get_basis(ExchangeId::Hyperliquid, self.hl_symbol_id);
                warn!(
                    "waiting: no fair price (basis={:?}, check feeds for ref symbol)",
                    basis,
                );
                self.last_status_log = Instant::now();
            }
            return false;
        }

        // Check feed is alive (engine-level — feed completely dead)
        if let Some(age_ms) = self.fair_price.get_ref_age_ms(ExchangeId::Hyperliquid, self.hl_symbol_id) {
            if age_ms > self.config.max_feed_age_ms as i64 {
                if should_log {
                    warn!("paused: reference feed dead (age={}ms > {}ms)", age_ms, self.config.max_feed_age_ms);
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
        if let Some(ref q) = self.bid_quote {
            match self.oms.get_order(&q.client_id) {
                Some(h) if matches!(
                    h.state,
                    OrderState::Accepted | OrderState::PartiallyFilled | OrderState::Inflight | OrderState::Cancelling
                ) => {}
                _ => {
                    debug!("bid quote {} no longer active, clearing tracker", q.client_id.0);
                    self.bid_quote = None;
                }
            }
        }
        if let Some(ref q) = self.ask_quote {
            match self.oms.get_order(&q.client_id) {
                Some(h) if matches!(
                    h.state,
                    OrderState::Accepted | OrderState::PartiallyFilled | OrderState::Inflight | OrderState::Cancelling
                ) => {}
                _ => {
                    debug!("ask quote {} no longer active, clearing tracker", q.client_id.0);
                    self.ask_quote = None;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Fast path: cancel guard (inner-side adverse detection)
    // -----------------------------------------------------------------------

    async fn fast_cancel_check(&mut self) {
        // If no live quotes, nothing to cancel
        if self.bid_quote.is_none() && self.ask_quote.is_none() {
            return;
        }

        let Some(fair) = self.fair_price.get_fair_price(ExchangeId::Hyperliquid, self.hl_symbol_id) else {
            return;
        };

        let position = self.get_position();
        let target = self.params.target_position_usd() / fair;
        let max_pos = self.params.max_position_usd() / fair;
        let half_spread = self.params.half_spread_bps() * fair / 10_000.0;
        let cancel_thresh = self.config.cancel_threshold_bps * fair / 10_000.0;

        let skewed_mid = self.compute_skewed_mid(fair, position, target, max_pos);
        let (desired_bid, desired_ask) = Self::clamp_to_fair(
            fair,
            skewed_mid - half_spread,
            skewed_mid + half_spread,
        );

        // Check bid: is it TOO HIGH (inner side = adverse)?
        if let Some(ref q) = self.bid_quote {
            if q.price > desired_bid + cancel_thresh {
                let drift_bps = (q.price - desired_bid) / fair * 10_000.0;
                if self.ghost {
                    info!(
                        "[GHOST] would CANCEL bid cid={} price={:.6} (inner drift {:.1}bps, desired={:.6})",
                        q.client_id.0, q.price, drift_bps, desired_bid,
                    );
                } else {
                    info!(
                        "fast cancel bid cid={} price={:.6} (inner drift {:.1}bps)",
                        q.client_id.0, q.price, drift_bps,
                    );
                    if let Err(e) = self.oms.cancel_order(&q.client_id).await {
                        warn!("failed to cancel bid {}: {e}", q.client_id.0);
                    }
                    // Don't clear tracker — let sync_quote_state confirm cancel
                }
                if self.ghost { self.bid_quote = None; }
            }
        }

        // Check ask: is it TOO LOW (inner side = adverse)?
        if let Some(ref q) = self.ask_quote {
            if q.price < desired_ask - cancel_thresh {
                let drift_bps = (desired_ask - q.price) / fair * 10_000.0;
                if self.ghost {
                    info!(
                        "[GHOST] would CANCEL ask cid={} price={:.6} (inner drift {:.1}bps, desired={:.6})",
                        q.client_id.0, q.price, drift_bps, desired_ask,
                    );
                } else {
                    info!(
                        "fast cancel ask cid={} price={:.6} (inner drift {:.1}bps)",
                        q.client_id.0, q.price, drift_bps,
                    );
                    if let Err(e) = self.oms.cancel_order(&q.client_id).await {
                        warn!("failed to cancel ask {}: {e}", q.client_id.0);
                    }
                    // Don't clear tracker — let sync_quote_state confirm cancel
                }
                if self.ghost { self.ask_quote = None; }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Slow path: evaluate and place quotes
    // -----------------------------------------------------------------------

    async fn evaluate_and_place_quotes(&mut self) {
        // Cancel any stray orders on our symbol that we don't own
        self.cancel_stray_orders().await;

        // Staleness check — don't place on stale data
        let Some((fair, age_ms)) = self.fair_price.get_fair_price_with_age(ExchangeId::Hyperliquid, self.hl_symbol_id) else {
            return;
        };
        if age_ms > self.config.max_stale_ms as i64 {
            return;
        }

        let position = self.get_position();
        let target = self.params.target_position_usd() / fair;
        let half_spread = self.params.half_spread_bps() * fair / 10_000.0;
        let notional = self.params.order_notional_usd();
        let order_size = notional / fair;
        let max_pos = self.params.max_position_usd() / fair;
        let requote_thresh = self.config.requote_tolerance_bps * fair / 10_000.0;

        let skewed_mid = self.compute_skewed_mid(fair, position, target, max_pos);
        let (desired_bid, desired_ask) = Self::clamp_to_fair(
            fair,
            skewed_mid - half_spread,
            skewed_mid + half_spread,
        );

        // Determine if we should quote each side (position limits)
        let want_bid = position < max_pos;
        let want_ask = position > -max_pos;

        // ── CANCEL stale/outer-side quotes ──

        // Bid: cancel if too passive (outer side) or shouldn't quote
        if let Some(ref q) = self.bid_quote {
            let should_cancel = !want_bid
                || (desired_bid - q.price) > requote_thresh; // bid too LOW = too passive

            if should_cancel {
                // Don't cancel inflight (no exchange OID yet)
                let is_inflight = self.oms.get_order(&q.client_id)
                    .map(|h| h.state == OrderState::Inflight)
                    .unwrap_or(false);

                if !is_inflight {
                    if self.ghost {
                        info!("[GHOST] would CANCEL bid cid={} price={:.6} (slow path requote)", q.client_id.0, q.price);
                        self.bid_quote = None;
                    } else {
                        debug!("slow cancel bid cid={} price={:.6}", q.client_id.0, q.price);
                        if let Err(e) = self.oms.cancel_order(&q.client_id).await {
                            warn!("failed to cancel bid {}: {e}", q.client_id.0);
                        }
                        // Don't clear tracker — let sync_quote_state confirm cancel
                    }
                }
            }
        }

        // Ask: cancel if too passive or shouldn't quote
        if let Some(ref q) = self.ask_quote {
            let should_cancel = !want_ask
                || (q.price - desired_ask) > requote_thresh; // ask too HIGH = too passive

            if should_cancel {
                let is_inflight = self.oms.get_order(&q.client_id)
                    .map(|h| h.state == OrderState::Inflight)
                    .unwrap_or(false);

                if !is_inflight {
                    if self.ghost {
                        info!("[GHOST] would CANCEL ask cid={} price={:.6} (slow path requote)", q.client_id.0, q.price);
                        self.ask_quote = None;
                    } else {
                        debug!("slow cancel ask cid={} price={:.6}", q.client_id.0, q.price);
                        if let Err(e) = self.oms.cancel_order(&q.client_id).await {
                            warn!("failed to cancel ask {}: {e}", q.client_id.0);
                        }
                        // Don't clear tracker — let sync_quote_state confirm cancel
                    }
                }
            }
        }

        // ── PLACE new quotes where needed ──

        if self.bid_quote.is_none() && want_bid {
            let tif = if self.config.post_only { TimeInForce::PostOnly } else { TimeInForce::GTC };
            if self.ghost {
                info!(
                    "[GHOST] would PLACE bid price={:.6} size={:.4} (skewed_mid={:.6} spread={:.2}bps pos={:.6} target={:.6})",
                    desired_bid, order_size, skewed_mid, self.params.half_spread_bps(), position, target,
                );
            } else {
                let req = OrderRequest {
                    symbol: self.config.symbol.clone(),
                    side: Side::Buy,
                    order_type: OrderType::Limit { price: desired_bid, tif },
                    size: order_size,
                    reduce_only: false,
                };
                match self.oms.place_order(req).await {
                    Ok(cid) => {
                        info!("placed bid cid={} price={:.6} size={:.4}", cid.0, desired_bid, order_size);
                        self.bid_quote = Some(LiveQuote {
                            client_id: cid,
                            price: desired_bid,
                            size: order_size,
                            placed_at: Instant::now(),
                        });
                        self.consecutive_rejects = 0;
                    }
                    Err(e) => {
                        warn!("failed to place bid: {e}");
                    }
                }
            }
        }

        if self.ask_quote.is_none() && want_ask {
            let tif = if self.config.post_only { TimeInForce::PostOnly } else { TimeInForce::GTC };
            if self.ghost {
                info!(
                    "[GHOST] would PLACE ask price={:.6} size={:.4} (skewed_mid={:.6} spread={:.2}bps pos={:.6} target={:.6})",
                    desired_ask, order_size, skewed_mid, self.params.half_spread_bps(), position, target,
                );
            } else {
                let req = OrderRequest {
                    symbol: self.config.symbol.clone(),
                    side: Side::Sell,
                    order_type: OrderType::Limit { price: desired_ask, tif },
                    size: order_size,
                    reduce_only: false,
                };
                match self.oms.place_order(req).await {
                    Ok(cid) => {
                        info!("placed ask cid={} price={:.6} size={:.4}", cid.0, desired_ask, order_size);
                        self.ask_quote = Some(LiveQuote {
                            client_id: cid,
                            price: desired_ask,
                            size: order_size,
                            placed_at: Instant::now(),
                        });
                        self.consecutive_rejects = 0;
                    }
                    Err(e) => {
                        warn!("failed to place ask: {e}");
                    }
                }
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
        let denom = if target.abs() > 1e-12 { target.abs() } else { max_pos };
        // raw ∈ [-ln(2), ln(2)] when diff ∈ [-denom, denom]
        // Normalize to [-1, 1] then scale by max_skew_bps
        let raw = diff.signum() * (1.0 + diff.abs() / denom).ln();
        let normalized = raw / std::f64::consts::LN_2; // ∈ [-1, 1] at 100% off target
        let skew_bps = (normalized * self.config.max_skew_bps)
            .clamp(-self.config.max_skew_bps, self.config.max_skew_bps);
        fair_value + skew_bps * fair_value / 10_000.0
    }

    /// Clamp desired bid/ask so quotes never cross fair value.
    fn clamp_to_fair(fair_value: f64, desired_bid: f64, desired_ask: f64) -> (f64, f64) {
        let min_edge = 0.1 * fair_value / 10_000.0; // 0.1 bps buffer
        (
            desired_bid.min(fair_value - min_edge),
            desired_ask.max(fair_value + min_edge),
        )
    }

    // -----------------------------------------------------------------------
    // Periodic status log
    // -----------------------------------------------------------------------

    fn log_status(&self) {
        let fair = self.fair_price.get_fair_price(ExchangeId::Hyperliquid, self.hl_symbol_id);
        let basis = self.fair_price.get_basis(ExchangeId::Hyperliquid, self.hl_symbol_id);
        let position = self.get_position();
        let target = fair.map(|f| self.params.target_position_usd() / f).unwrap_or(0.0);
        let max_pos = fair.map(|f| self.params.max_position_usd() / f).unwrap_or(0.0);

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

        let bid_str = self.bid_quote.as_ref()
            .map(|q| format!("{:.6}", q.price))
            .unwrap_or_else(|| "-".into());
        let ask_str = self.ask_quote.as_ref()
            .map(|q| format!("{:.6}", q.price))
            .unwrap_or_else(|| "-".into());

        let ref_age_ms = self.fair_price
            .get_ref_age_ms(ExchangeId::Hyperliquid, self.hl_symbol_id)
            .unwrap_or(-1);

        let hl_mid = self.fair_price.get_mid(ExchangeId::Hyperliquid, self.hl_symbol_id);
        let residual_bps = match (hl_mid, fair) {
            (Some(hl), Some(f)) if f != 0.0 => (hl - f) / f * 10_000.0,
            _ => 0.0,
        };

        info!(
            "status: fair={:.6} hl_mid={:.6} resid={:+.2}bps basis={:+.2}bps skew={:+.2}bps pos={:+.6} target={:+.6} bid={} ask={} ref_age={}ms",
            fair.unwrap_or(0.0),
            hl_mid.unwrap_or(0.0),
            residual_bps,
            basis_bps,
            skew_bps,
            position,
            target,
            bid_str,
            ask_str,
            ref_age_ms,
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

    fn is_our_order(&self, cid: &ClientOrderId) -> bool {
        self.bid_quote.as_ref().map(|q| q.client_id == *cid).unwrap_or(false)
            || self.ask_quote.as_ref().map(|q| q.client_id == *cid).unwrap_or(false)
    }

    fn clear_tracker(&mut self, cid: &ClientOrderId) {
        if self.bid_quote.as_ref().map(|q| q.client_id == *cid).unwrap_or(false) {
            self.bid_quote = None;
        }
        if self.ask_quote.as_ref().map(|q| q.client_id == *cid).unwrap_or(false) {
            self.ask_quote = None;
        }
    }

    // -----------------------------------------------------------------------
    // Cancel helpers
    // -----------------------------------------------------------------------

    /// Cancel any open orders on our symbol that we don't recognize as ours.
    /// Cancel open orders on our symbol that we don't recognize as ours.
    /// Only considers orders older than 1 second to avoid racing with in-flight operations.
    async fn cancel_stray_orders(&mut self) {
        if self.ghost {
            return;
        }
        let open = self.oms.open_orders(Some(&self.config.symbol));
        let our_bid = self.bid_quote.as_ref().map(|q| q.client_id);
        let our_ask = self.ask_quote.as_ref().map(|q| q.client_id);
        let min_age = Duration::from_secs(1);

        for o in &open {
            if Some(o.client_id) == our_bid || Some(o.client_id) == our_ask {
                continue;
            }
            // Only cancel if the order has been around long enough
            let old_enough = o.submitted_at
                .map(|t| t.elapsed() >= min_age)
                .unwrap_or(true); // no submitted_at = came from REST snapshot = old enough
            if old_enough {
                warn!("cancelling stray order cid={} side={:?} price={:?}",
                    o.client_id.0, o.side, o.order_type);
                if let Err(e) = self.oms.cancel_order(&o.client_id).await {
                    warn!("failed to cancel stray {}: {e}", o.client_id.0);
                }
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
