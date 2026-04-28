use anyhow::{Result, bail};
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use oms_core::*;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use super::client::{
    HyperliquidClient, ClientCancelRequest, ClientCancelRequestCloid, ClientLimit,
    ClientModifyRequest, ClientOrder, ClientOrderRequest, ClientTrigger,
};
use super::types::*;

/// A signed order payload ready for HTTP post.
/// JSON serialization is deferred to the post step (off hot path).
pub struct SignedPayload {
    pub action: hyperliquid_rust_sdk::Actions,
    pub signature: alloy::signers::Signature,
    pub timestamp: u64,
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

pub struct HyperliquidOmsConfig {
    /// API wallet private key (agent key, not the main account key)
    pub private_key: String,
    /// Parent account EOA address (the account the API wallet trades on behalf of)
    pub account_address: String,
    pub base_url: Option<String>,
    /// How often to poll REST for full state reconciliation
    pub poll_interval: Duration,
    /// How long to wait for exchange ack before marking order as timed out
    pub inflight_timeout: Duration,
    /// How long to wait before overriding Cancelling/Cancelled state from REST poll
    pub stray_order_age: Duration,
}

impl Default for HyperliquidOmsConfig {
    fn default() -> Self {
        Self {
            private_key: String::new(),
            account_address: String::new(),
            base_url: None,
            poll_interval: Duration::from_secs(3),
            inflight_timeout: Duration::from_secs(5),
            stray_order_age: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------
// Diagnostics (observable from display)
// ---------------------------------------------------------------------------

const MAX_LOG_LINES: usize = 30;

pub struct OmsDiagnostics {
    pub ws_connected: AtomicBool,
    pub last_poll_at: Mutex<Option<Instant>>,
    pub poll_count: AtomicU64,
    pub ws_msg_count: AtomicU64,
    pub last_error: Mutex<Option<String>>,
    pub log_lines: Mutex<VecDeque<String>>,
}

impl OmsDiagnostics {
    fn new() -> Self {
        Self {
            ws_connected: AtomicBool::new(false),
            last_poll_at: Mutex::new(None),
            poll_count: AtomicU64::new(0),
            ws_msg_count: AtomicU64::new(0),
            last_error: Mutex::new(None),
            log_lines: Mutex::new(VecDeque::new()),
        }
    }

    fn log(&self, msg: String) {
        let mut lines = self.log_lines.lock();
        if lines.len() >= MAX_LOG_LINES {
            lines.pop_front();
        }
        lines.push_back(msg);
    }

    fn set_error(&self, err: String) {
        self.log(format!("ERROR: {err}"));
        *self.last_error.lock() = Some(err);
    }
}

// ---------------------------------------------------------------------------
// OMS state
// ---------------------------------------------------------------------------

struct OmsState {
    orders: DashMap<u64, OrderHandle>,
    /// exchange_oid -> client_id mapping for WS event reconciliation
    oid_map: DashMap<String, u64>,
    positions: DashMap<String, Position>,
    balances: DashMap<String, Balance>,
}

impl OmsState {
    fn new() -> Self {
        Self {
            orders: DashMap::new(),
            oid_map: DashMap::new(),
            positions: DashMap::new(),
            balances: DashMap::new(),
        }
    }

    #[allow(dead_code)]
    fn clear(&self) {
        self.orders.clear();
        self.oid_map.clear();
        self.positions.clear();
        self.balances.clear();
    }
}

// ---------------------------------------------------------------------------
// Hyperliquid OMS
// ---------------------------------------------------------------------------

pub struct HyperliquidOms {
    client: Arc<HyperliquidClient>,
    asset_map: Arc<OnceLock<AssetMap>>,
    state: Arc<OmsState>,
    ready: Arc<AtomicBool>,
    ready_notify: Arc<Notify>,
    next_client_id: AtomicU64,
    event_tx: crossbeam_channel::Sender<OmsEvent>,
    event_rx: crossbeam_channel::Receiver<OmsEvent>,
    config: HyperliquidOmsConfig,
    shutdown: Arc<Notify>,
    pub diag: Arc<OmsDiagnostics>,
}

impl HyperliquidOms {
    pub fn new(config: HyperliquidOmsConfig) -> Result<Arc<Self>> {
        let client = HyperliquidClient::new(
            &config.private_key,
            config.account_address.clone(),
            config.base_url.clone(),
        )?;
        let client = Arc::new(client);
        let (event_tx, event_rx) = crossbeam_channel::bounded(4096);

        let oms = Arc::new(Self {
            client,
            asset_map: Arc::new(OnceLock::new()),
            state: Arc::new(OmsState::new()),
            ready: Arc::new(AtomicBool::new(false)),
            ready_notify: Arc::new(Notify::new()),
            next_client_id: AtomicU64::new(1),
            event_tx,
            event_rx,
            config,
            shutdown: Arc::new(Notify::new()),
            diag: Arc::new(OmsDiagnostics::new()),
        });

        Ok(oms)
    }

    pub fn client(&self) -> &HyperliquidClient {
        &self.client
    }

    /// Start all background tasks. Call this once after construction.
    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = this.run_init().await {
                this.diag.set_error(format!("init failed: {e:#}"));
                error!("hyperliquid OMS init failed: {e:#}");
            }
        });

        let this = Arc::clone(self);
        tokio::spawn(async move { this.run_poller().await });

        let this = Arc::clone(self);
        tokio::spawn(async move { this.run_ws().await });

        let this = Arc::clone(self);
        tokio::spawn(async move { this.run_inflight_watchdog().await });
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    /// Cancel all orders on the exchange for a symbol — used at shutdown.
    /// Fetches open orders from REST (no local state dependency) + cancels
    /// inflight orders by cloid.
    pub async fn shutdown_cancel_all(&self, symbol: Option<&str>) -> Result<()> {
        // 1. Cancel all open orders from exchange
        let open_orders = self.client.fetch_open_orders().await?;
        let mut oid_cancels = Vec::new();
        for oo in &open_orders {
            if let Some(sym) = symbol {
                if AssetMap::asset_to_canonical(&oo.coin) != sym {
                    continue;
                }
            }
            let (_, asset_name) = self.resolve_asset(&AssetMap::asset_to_canonical(&oo.coin))?;
            oid_cancels.push(ClientCancelRequest {
                asset: asset_name,
                oid: oo.oid,
            });
        }
        if !oid_cancels.is_empty() {
            info!("shutdown: cancelling {} open orders by oid", oid_cancels.len());
            let _ = self.client.exchange().bulk_cancel(oid_cancels, None).await;
        }

        // 2. Cancel inflight orders by cloid (not yet visible on exchange)
        let mut cloid_cancels = Vec::new();
        for entry in self.state.orders.iter() {
            let h = entry.value();
            if h.state != OrderState::Inflight {
                continue;
            }
            if let Some(sym) = symbol {
                if h.symbol != sym {
                    continue;
                }
            }
            let (_, asset_name) = self.resolve_asset(&h.symbol)?;
            cloid_cancels.push(ClientCancelRequestCloid {
                asset: asset_name,
                cloid: uuid::Uuid::from_u128(h.client_id.0 as u128),
            });
        }
        if !cloid_cancels.is_empty() {
            info!("shutdown: cancelling {} inflight orders by cloid", cloid_cancels.len());
            let _ = self.client.exchange().bulk_cancel_by_cloid(cloid_cancels, None).await;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Initialization: fetch meta + full state snapshot
    // -----------------------------------------------------------------------

    async fn run_init(&self) -> Result<()> {
        self.diag.log("initializing...".into());

        // 1. Fetch asset metadata
        let meta = self.client.fetch_meta().await?;
        let asset_map = AssetMap::from_meta(&meta);
        let _ = self.asset_map.set(asset_map);
        self.diag.log(format!("loaded {} assets", meta.universe.len()));

        // 2. Initialize SDK exchange client (handles signing correctly)
        self.client.init_exchange().await?;
        self.diag.log("SDK exchange client initialized".into());

        // 3. Snapshot full state
        self.snapshot_from_rest().await?;

        // 3. Mark ready
        self.ready.store(true, Ordering::Release);
        self.ready_notify.notify_waiters();
        let _ = self.event_tx.send(OmsEvent::Ready);
        info!("hyperliquid OMS ready");

        Ok(())
    }

    // -----------------------------------------------------------------------
    // REST snapshot: open orders + positions + balances
    // -----------------------------------------------------------------------

    async fn snapshot_from_rest(&self) -> Result<()> {
        let (open_orders, clearinghouse, spot_state) = tokio::try_join!(
            self.client.fetch_open_orders(),
            self.client.fetch_clearinghouse_state(),
            self.client.fetch_spot_state(),
        )?;

        // Reconcile open orders — keep inflight orders, update rest from exchange
        let mut exchange_oids: std::collections::HashSet<String> = std::collections::HashSet::new();

        for oo in &open_orders {
            let oid_str = oo.oid.to_string();
            exchange_oids.insert(oid_str.clone());

            // If we already track this order (by exchange oid), reconcile it
            if let Some(cid) = self.state.oid_map.get(&oid_str) {
                let cid = *cid;
                if let Some(mut handle) = self.state.orders.get_mut(&cid) {
                    let remaining_sz: f64 = oo.sz.parse().unwrap_or(0.0);
                    let orig_sz: f64 = oo.orig_sz.parse().unwrap_or(handle.size);
                    let prev_filled = handle.filled_size;
                    handle.filled_size = orig_sz - remaining_sz;

                    let age_exceeded = handle.last_modified
                        .map(|t| t.elapsed() >= self.config.stray_order_age)
                        .unwrap_or(true);

                    // Emit fill event if exchange shows more filled than gateway knew.
                    // Gate on stray_order_age to let WS deliver real fills first
                    // (poll fills have synthetic fill_ids that can't dedup with WS).
                    if age_exceeded && handle.filled_size - prev_filled > 1e-12 {
                        let fill = oms_core::Fill {
                            client_id: handle.client_id,
                            exchange_id: handle.exchange_id.clone().unwrap_or_default(),
                            fill_id: format!("poll_{}", self.next_client_id.fetch_add(1, Ordering::Relaxed)),
                            symbol: handle.symbol.clone(),
                            side: handle.side,
                            price: 0.0,
                            size: handle.filled_size - prev_filled,
                            fee: 0.0,
                            fee_asset: "USDC".to_string(),
                            liquidity: Liquidity::Taker,
                            ts: Utc::now(),
                        };
                        if remaining_sz < 1e-12 {
                            let _ = self.event_tx.send(OmsEvent::OrderFilled(fill));
                        } else {
                            let _ = self.event_tx.send(OmsEvent::OrderPartialFill(fill));
                        }
                    }

                    let active_state = if handle.filled_size > 0.0 {
                        OrderState::PartiallyFilled
                    } else {
                        OrderState::Accepted
                    };

                    match handle.state {
                        // Already active — just update fill info
                        OrderState::Accepted | OrderState::PartiallyFilled => {
                            handle.state = active_state;
                        }
                        // Inflight — exchange has it, promote
                        OrderState::Inflight => {
                            handle.state = active_state;
                            handle.last_modified = Some(Instant::now());
                        }
                        // Cancelling — only restore if stray age exceeded (cancel failed)
                        OrderState::Cancelling => {
                            if age_exceeded {
                                warn!("cancel failed for cid={}, restoring to {:?}", cid, active_state);
                                handle.state = active_state;
                                handle.last_modified = Some(Instant::now());
                            }
                            // else: cancel still processing, leave as Cancelling
                        }
                        // Cancelled — zombie order, only restore if stray age exceeded
                        OrderState::Cancelled => {
                            if age_exceeded {
                                warn!("zombie order cid={} still on exchange, restoring to {:?}", cid, active_state);
                                handle.state = active_state;
                                handle.last_modified = Some(Instant::now());
                            }
                            // else: just recently cancelled, HL lagging
                        }
                        // Other states (Filled, Rejected, TimedOut) — exchange says it's open, trust exchange
                        _ => {
                            handle.state = active_state;
                            handle.last_modified = Some(Instant::now());
                        }
                    }
                }
            } else {
                // Order we don't know about — came from outside this OMS session
                // or from before restart. Track it with a synthetic client ID.
                let cid = self.next_client_id.fetch_add(1, Ordering::Relaxed);
                let remaining_sz: f64 = oo.sz.parse().unwrap_or(0.0);
                let orig_sz: f64 = oo.orig_sz.parse().unwrap_or(remaining_sz);
                let side = if oo.side == "B" { Side::Buy } else { Side::Sell };
                let price: f64 = oo.limit_px.parse().unwrap_or(0.0);
                let symbol = AssetMap::asset_to_canonical(&oo.coin);
                let tif = match oo.tif.as_deref() {
                    Some("Ioc") => TimeInForce::IOC,
                    Some("Alo") => TimeInForce::PostOnly,
                    _ => TimeInForce::GTC,
                };

                let handle = OrderHandle {
                    client_id: ClientOrderId(cid),
                    exchange_id: Some(oid_str.clone()),
                    symbol,
                    side,
                    order_type: OrderType::Limit { price, tif },
                    size: orig_sz,
                    filled_size: orig_sz - remaining_sz,
                    avg_fill_price: None,
                    state: if orig_sz - remaining_sz > 0.0 {
                        OrderState::PartiallyFilled
                    } else {
                        OrderState::Accepted
                    },
                    reduce_only: oo.reduce_only.unwrap_or(false),
                    reject_reason: None,
                    exchange_ts: None,
                    submitted_at: None,
                    last_modified: Some(Instant::now()),
                };

                self.state.oid_map.insert(oid_str, cid);
                self.state.orders.insert(cid, handle);
            }
        }

        // Mark orders that are no longer open on the exchange
        // (skip inflight orders — they might not be visible yet)
        let mut to_remove = Vec::new();
        for entry in self.state.orders.iter() {
            let handle = entry.value();
            if matches!(handle.state,
                OrderState::Accepted | OrderState::PartiallyFilled | OrderState::Cancelling
            ) {
                if let Some(eid) = &handle.exchange_id {
                    if !exchange_oids.contains(eid) {
                        // Order disappeared from exchange — probably filled or cancelled
                        to_remove.push(*entry.key());
                    }
                }
            }
        }
        for cid in to_remove {
            if let Some(mut handle) = self.state.orders.get_mut(&cid) {
                // Assume filled if it had partial fills, cancelled otherwise
                if handle.filled_size > 0.0 {
                    handle.state = OrderState::Filled;
                    let _ = self.event_tx.send(OmsEvent::OrderFilled(oms_core::Fill {
                        client_id: handle.client_id,
                        exchange_id: handle.exchange_id.clone().unwrap_or_default(),
                        fill_id: format!("poll_{}", self.next_client_id.fetch_add(1, Ordering::Relaxed)),
                        symbol: handle.symbol.clone(),
                        side: handle.side,
                        price: handle.avg_fill_price.unwrap_or(0.0),
                        size: handle.filled_size,
                        fee: 0.0,
                        fee_asset: "USDC".to_string(),
                        liquidity: Liquidity::Taker,
                        ts: Utc::now(),
                    }));
                } else {
                    handle.state = OrderState::Cancelled;
                    let _ = self.event_tx.send(OmsEvent::OrderCancelled(handle.client_id));
                }
                handle.last_modified = Some(Instant::now());
            }
        }

        // Clean up oid_map for orders in terminal states
        self.state.oid_map.retain(|_oid, cid| {
            self.state.orders.get(cid)
                .map(|h| !matches!(h.state,
                    OrderState::Filled | OrderState::Cancelled | OrderState::Rejected | OrderState::TimedOut
                ))
                .unwrap_or(false) // remove if order doesn't exist
        });

        // Reconcile positions
        self.state.positions.clear();
        for ap in &clearinghouse.asset_positions {
            let szi: f64 = ap.position.szi.parse().unwrap_or(0.0);
            if szi.abs() < 1e-12 {
                continue;
            }
            let symbol = AssetMap::asset_to_canonical(&ap.position.coin);
            let side = if szi > 0.0 { Side::Buy } else { Side::Sell };
            let entry_px: f64 = ap
                .position
                .entry_px
                .as_deref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0);
            let upnl: f64 = ap.position.unrealized_pnl.parse().unwrap_or(0.0);
            let liq_px: Option<f64> = ap
                .position
                .liquidation_px
                .as_deref()
                .and_then(|s| s.parse().ok());

            let pos = Position {
                symbol: symbol.clone(),
                side,
                size: szi.abs(),
                entry_price: entry_px,
                unrealized_pnl: upnl,
                leverage: ap.position.leverage.value as f64,
                liquidation_price: liq_px,
            };
            self.state.positions.insert(symbol, pos);
        }

        // Balances
        // On HL, spot USDC "total" is the true account equity (matches GUI).
        // The perp clearinghouse is a derivative view into how that USDC is deployed.
        self.state.balances.clear();

        let _perp_margin_used: f64 = clearinghouse
            .margin_summary
            .total_margin_used
            .parse()
            .unwrap_or(0.0);
        let _perp_account_value: f64 = clearinghouse
            .margin_summary
            .account_value
            .parse()
            .unwrap_or(0.0);
        let _perp_ntl_pos: f64 = clearinghouse
            .margin_summary
            .total_ntl_pos
            .parse()
            .unwrap_or(0.0);
        let _withdrawable: f64 = clearinghouse.withdrawable.parse().unwrap_or(0.0);

        // Spot balances — USDC total here is the real account equity
        for sb in &spot_state.balances {
            let total: f64 = sb.total.parse().unwrap_or(0.0);
            let hold: f64 = sb.hold.parse().unwrap_or(0.0);
            if total.abs() < 1e-12 {
                continue;
            }
            if sb.coin == "USDC" {
                // USDC is the primary account balance
                self.state.balances.insert(
                    "USDC".to_string(),
                    Balance {
                        asset: "USDC".to_string(),
                        available: total - hold,
                        locked: hold,
                        total,
                        equity: total,
                    },
                );
            } else {
                self.state.balances.insert(
                    format!("SPOT_{}", sb.coin),
                    Balance {
                        asset: sb.coin.clone(),
                        available: total - hold,
                        locked: hold,
                        total,
                        equity: total,
                    },
                );
            }
        }

        self.diag.poll_count.fetch_add(1, Ordering::Relaxed);
        *self.diag.last_poll_at.lock() = Some(Instant::now());
        self.diag.log(format!(
            "poll #{}: {} orders, {} positions",
            self.diag.poll_count.load(Ordering::Relaxed),
            open_orders.len(),
            self.state.positions.len(),
        ));

        // Emit Snapshot event for downstream consumers (shadow tracker / future local state)
        let snap_orders: Vec<OrderHandle> = self.state.orders.iter()
            .filter(|e| !matches!(e.value().state,
                OrderState::Filled | OrderState::Cancelled | OrderState::Rejected | OrderState::TimedOut
            ))
            .map(|e| e.value().clone())
            .collect();
        let snap_positions: Vec<Position> = self.state.positions.iter()
            .map(|e| e.value().clone())
            .collect();
        let snap_balances: Vec<Balance> = self.state.balances.iter()
            .map(|e| e.value().clone())
            .collect();
        let _ = self.event_tx.send(OmsEvent::Snapshot {
            orders: snap_orders,
            positions: snap_positions,
            balances: snap_balances,
        });

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Periodic REST poller
    // -----------------------------------------------------------------------

    async fn run_poller(&self) {
        // Wait for init to complete before starting poll loop
        self.ready_notify.notified().await;

        loop {
            if let Err(e) = self.snapshot_from_rest().await {
                self.diag.set_error(format!("REST poll failed: {e:#}"));
            }

            tokio::select! {
                _ = tokio::time::sleep(self.config.poll_interval) => {}
                _ = self.shutdown.notified() => return,
            }
        }
    }

    // -----------------------------------------------------------------------
    // WebSocket user events
    // -----------------------------------------------------------------------

    async fn run_ws(&self) {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        loop {
            if let Err(e) = self.ws_loop().await {
                self.diag.ws_connected.store(false, Ordering::Release);
                self.diag.set_error(format!("WS disconnected: {e:#}"));
                let _ = self.event_tx.send(OmsEvent::Disconnected);
            }

            self.diag.log(format!("WS reconnecting in {:.0}s...", backoff.as_secs_f64()));
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = self.shutdown.notified() => return,
            }
            backoff = (backoff * 2).min(max_backoff);

            if self.ready.load(Ordering::Acquire) {
                self.diag.log("WS re-snapshotting before reconnect...".into());
                match self.snapshot_from_rest().await {
                    Ok(()) => {
                        let _ = self.event_tx.send(OmsEvent::Reconnected);
                        let _ = self.event_tx.send(OmsEvent::Ready);
                        backoff = Duration::from_secs(1);
                    }
                    Err(e) => {
                        self.diag.set_error(format!("re-snapshot failed: {e:#}"));
                        continue; // back to sleep+retry, don't enter ws_loop
                    }
                }
            }
        }
    }

    async fn ws_loop(&self) -> Result<()> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::connect_async;
        use tokio_tungstenite::tungstenite::protocol::Message;

        let url = self.client.ws_url();
        let (mut ws, _) = connect_async(&url).await?;
        self.diag.ws_connected.store(true, Ordering::Release);
        self.diag.log(format!("WS connected to {url}"));

        // Subscribe to user events
        let sub = WsSubscription {
            method: "subscribe",
            subscription: WsChannel::UserEvents {
                user: &self.client.account_address,
            },
        };
        ws.send(Message::Text(serde_json::to_string(&sub)?.into())).await?;

        let mut heartbeat = tokio::time::interval(Duration::from_secs(10));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await; // consume first immediate tick

        let mut last_msg = Instant::now();
        let msg_timeout = Duration::from_secs(90);

        loop {
            tokio::select! {
                msg = ws.next() => {
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => bail!("WS error: {e}"),
                        None => bail!("WS stream ended"),
                    };
                    last_msg = Instant::now();
                    match msg {
                        Message::Text(text) => self.handle_ws_message(&text),
                        Message::Ping(data) => { ws.send(Message::Pong(data)).await?; }
                        Message::Close(_) => bail!("WS closed by server"),
                        _ => {}
                    }
                }
                _ = heartbeat.tick() => {
                    if last_msg.elapsed() > msg_timeout {
                        bail!("WS stale: no messages for {:.0}s", last_msg.elapsed().as_secs_f64());
                    }
                    ws.send(Message::Text(r#"{"method":"ping"}"#.into())).await?;
                }
                _ = self.shutdown.notified() => return Ok(()),
            }
        }
    }

    fn handle_ws_message(&self, text: &str) {
        self.diag.ws_msg_count.fetch_add(1, Ordering::Relaxed);

        let msg: WsMessage = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                self.diag.log(format!("WS parse error: {e}, text: {}", &text[..text.len().min(200)]));
                return;
            }
        };

        let data = match msg.data {
            Some(WsData::UserEvents(events)) => events,
            _ => return,
        };

        // Process order updates
        for update in &data.order_updates {
            self.diag.log(format!(
                "WS order update: oid={} status={} coin={}",
                update.order.oid, update.status, update.order.coin
            ));
            self.handle_order_update(update);
        }

        // Process fills
        for fill in &data.fills {
            self.diag.log(format!(
                "WS fill: oid={} coin={} side={} px={} sz={}",
                fill.oid, fill.coin, fill.side, fill.px, fill.sz
            ));
            self.handle_fill(fill);
        }
    }

    fn handle_order_update(&self, update: &OrderUpdateWire) {
        let oid_str = update.order.oid.to_string();

        // Find the client order by exchange oid
        let cid = match self.state.oid_map.get(&oid_str) {
            Some(c) => *c,
            None => {
                debug!("WS order update for unknown oid {oid_str}");
                return;
            }
        };

        let new_state = match update.status.as_str() {
            "open" => OrderState::Accepted,
            "filled" => OrderState::Filled,
            "canceled" | "marginCanceled" => OrderState::Cancelled,
            "rejected" => OrderState::Rejected,
            _ => return,
        };

        if let Some(mut handle) = self.state.orders.get_mut(&cid) {
            handle.state = new_state;
            handle.last_modified = Some(Instant::now());

            let event = match new_state {
                OrderState::Filled => OmsEvent::OrderFilled(Fill {
                    client_id: ClientOrderId(cid),
                    exchange_id: oid_str,
                    fill_id: format!("ws_status_{cid}"),
                    symbol: handle.symbol.clone(),
                    side: handle.side,
                    price: handle.avg_fill_price.unwrap_or(0.0),
                    size: handle.size,
                    fee: 0.0,
                    fee_asset: "USDC".to_string(),
                    liquidity: Liquidity::Taker,
                    ts: Utc::now(),
                }),
                OrderState::Cancelled => OmsEvent::OrderCancelled(ClientOrderId(cid)),
                OrderState::Rejected => OmsEvent::OrderRejected {
                    client_id: ClientOrderId(cid),
                    reason: update.status.clone(),
                },
                _ => return,
            };

            let _ = self.event_tx.send(event);
        }
    }

    fn handle_fill(&self, fill: &UserFillWire) {
        let oid_str = fill.oid.to_string();
        let cid = match self.state.oid_map.get(&oid_str) {
            Some(c) => *c,
            None => {
                debug!("WS fill for unknown oid {oid_str}");
                return;
            }
        };

        let price: f64 = fill.px.parse().unwrap_or(0.0);
        let size: f64 = fill.sz.parse().unwrap_or(0.0);
        let fee: f64 = fill.fee.parse().unwrap_or(0.0);
        let side = if fill.side == "B" { Side::Buy } else { Side::Sell };
        let symbol = AssetMap::asset_to_canonical(&fill.coin);

        // Update order handle
        if let Some(mut handle) = self.state.orders.get_mut(&cid) {
            handle.filled_size += size;
            let prev_value = handle
                .avg_fill_price
                .unwrap_or(0.0)
                * (handle.filled_size - size);
            let new_value = prev_value + price * size;
            handle.avg_fill_price = Some(new_value / handle.filled_size);

            if (handle.filled_size - handle.size).abs() < 1e-12 {
                handle.state = OrderState::Filled;
            } else {
                handle.state = OrderState::PartiallyFilled;
            }
            handle.last_modified = Some(Instant::now());
        }

        let event = OmsEvent::OrderPartialFill(oms_core::Fill {
            client_id: ClientOrderId(cid),
            exchange_id: oid_str,
            fill_id: fill.tid.to_string(),
            symbol,
            side,
            price,
            size,
            fee,
            fee_asset: "USDC".to_string(),
            liquidity: if fill.crossed {
                Liquidity::Taker
            } else {
                Liquidity::Maker
            },
            ts: Utc::now(),
        });

        let _ = self.event_tx.send(event);
    }

    // -----------------------------------------------------------------------
    // Inflight watchdog
    // -----------------------------------------------------------------------

    async fn run_inflight_watchdog(&self) {
        let check_interval = Duration::from_secs(1);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(check_interval) => {}
                _ = self.shutdown.notified() => return,
            }

            let timeout = self.config.inflight_timeout;
            let now = Instant::now();

            for entry in self.state.orders.iter() {
                let handle = entry.value();
                if handle.state != OrderState::Inflight {
                    continue;
                }
                if let Some(submitted) = handle.submitted_at {
                    if now.duration_since(submitted) > timeout {
                        let cid = handle.client_id.0;
                        warn!("order {cid} timed out waiting for ack");
                        drop(entry);
                        if let Some(mut h) = self.state.orders.get_mut(&cid) {
                            h.state = OrderState::TimedOut;
                            h.last_modified = Some(Instant::now());
                        }
                        let _ = self
                            .event_tx
                            .send(OmsEvent::OrderTimedOut(ClientOrderId(cid)));
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Internal order helpers
    // -----------------------------------------------------------------------

    fn check_ready(&self) -> Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            bail!(OmsError::NotReady);
        }
        Ok(())
    }

    fn resolve_asset(&self, symbol: &str) -> Result<(u32, String)> {
        let asset_name = AssetMap::canonical_to_asset(symbol)
            .ok_or_else(|| OmsError::Internal(format!("bad symbol format: {symbol}")))?;

        let map = self.asset_map.get()
            .ok_or_else(|| OmsError::Internal("asset map not loaded".into()))?;

        let idx = map
            .asset_index(asset_name)
            .ok_or_else(|| OmsError::Internal(format!("unknown asset: {asset_name}")))?;

        Ok((idx, asset_name.to_string()))
    }

    /// Sync part of place_order: generates cid, builds SDK request, inserts Inflight order.
    /// Returns (cid, sdk_request) for the caller to spawn the HTTP call.
    pub fn prepare_place_order(&self, req: &OrderRequest) -> Result<(ClientOrderId, ClientOrderRequest)> {
        self.check_ready()?;

        let (asset_idx, asset_name) = self.resolve_asset(&req.symbol)?;
        let sz_dec = self.asset_map.get()
            .and_then(|m| m.sz_decimals(asset_idx))
            .unwrap_or(8);
        let cid = self.next_client_id.fetch_add(1, Ordering::Relaxed);

        let (price, order_type) = match req.order_type {
            OrderType::Limit { price, tif } => {
                let tif_str = match tif {
                    TimeInForce::GTC => "Gtc",
                    TimeInForce::IOC => "Ioc",
                    TimeInForce::PostOnly => "Alo",
                };
                (price, ClientOrder::Limit(ClientLimit { tif: tif_str.to_string() }))
            }
            OrderType::Market => {
                (0.0, ClientOrder::Limit(ClientLimit { tif: "Ioc".to_string() }))
            }
            OrderType::StopLimit { price, trigger, tif: _ } => {
                (price, ClientOrder::Trigger(ClientTrigger {
                    trigger_px: round_price_for_hl(trigger),
                    is_market: false,
                    tpsl: "sl".to_string(),
                }))
            }
            OrderType::StopMarket { trigger } => {
                (trigger, ClientOrder::Trigger(ClientTrigger {
                    trigger_px: round_price_for_hl(trigger),
                    is_market: true,
                    tpsl: "sl".to_string(),
                }))
            }
        };

        let sdk_req = ClientOrderRequest {
            asset: asset_name,
            is_buy: req.side == Side::Buy,
            reduce_only: req.reduce_only,
            limit_px: round_price_for_hl(price),
            sz: round_size_for_hl(req.size, sz_dec),
            cloid: Some(uuid::Uuid::from_u128(cid as u128)),
            order_type,
        };

        let handle = OrderHandle {
            client_id: ClientOrderId(cid),
            exchange_id: None,
            symbol: req.symbol.clone(),
            side: req.side,
            order_type: req.order_type,
            size: req.size,
            filled_size: 0.0,
            avg_fill_price: None,
            state: OrderState::Inflight,
            reduce_only: req.reduce_only,
            reject_reason: None,
            exchange_ts: None,
            submitted_at: Some(Instant::now()),
            last_modified: Some(Instant::now()),
        };
        self.state.orders.insert(cid, handle);
        let _ = self.event_tx.send(OmsEvent::OrderInflight(ClientOrderId(cid)));

        Ok((ClientOrderId(cid), sdk_req))
    }

    /// Sync: convert + hash + sign the order. Returns a signed payload ready to post.
    pub fn sign_place_order(&self, sdk_req: ClientOrderRequest) -> Result<SignedPayload> {
        let ex = self.client.exchange();
        let timestamp = hyperliquid_rust_sdk::next_nonce();

        // Convert ClientOrderRequest → wire format
        let transformed = sdk_req.convert(&ex.coin_to_asset)
            .map_err(|e| anyhow::anyhow!("order convert failed: {e}"))?;

        let action = hyperliquid_rust_sdk::Actions::Order(
            hyperliquid_rust_sdk::BulkOrder {
                orders: vec![transformed],
                grouping: "na".to_string(),
                builder: None,
            }
        );

        let connection_id = action.hash(timestamp, ex.vault_address)
            .map_err(|e| anyhow::anyhow!("action hash failed: {e}"))?;

        let is_mainnet = ex.http_client.is_mainnet();
        let signature = hyperliquid_rust_sdk::sign_l1_action(&ex.wallet, connection_id, is_mainnet)
            .map_err(|e| anyhow::anyhow!("signing failed: {e}"))?;

        // JSON serialization deferred to post_place_order (off hot path)
        Ok(SignedPayload { action, signature, timestamp })
    }

    /// Async: post a signed payload and process the response.
    pub async fn post_place_order(&self, cid: u64, signed: SignedPayload) {
        // JSON serialization happens here, off the hot path
        let action_json = match serde_json::to_value(&signed.action) {
            Ok(v) => v,
            Err(e) => {
                warn!("JSON serialize failed for cid={}: {e}", cid);
                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                    h.state = OrderState::Rejected;
                    h.reject_reason = Some(format!("{e}"));
                    h.last_modified = Some(Instant::now());
                }
                return;
            }
        };

        let resp = match self.client.exchange()
            .post(action_json, signed.signature, signed.timestamp).await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("SDK order failed for cid={}: {e}", cid);
                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                    h.state = OrderState::Rejected;
                    h.reject_reason = Some(format!("{e}"));
                    h.last_modified = Some(Instant::now());
                }
                return;
            }
        };

        use hyperliquid_rust_sdk::{ExchangeResponseStatus, ExchangeDataStatus};
        match resp {
            ExchangeResponseStatus::Err(msg) => {
                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                    h.state = OrderState::Rejected;
                    h.reject_reason = Some(msg.clone());
                    h.last_modified = Some(Instant::now());
                }
                let _ = self.event_tx.send(OmsEvent::OrderRejected {
                    client_id: ClientOrderId(cid),
                    reason: msg,
                });
            }
            ExchangeResponseStatus::Ok(response) => {
                if let Some(data) = response.data {
                    if let Some(status) = data.statuses.first() {
                        match status {
                            ExchangeDataStatus::Resting(resting) => {
                                let oid_str = resting.oid.to_string();
                                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                                    h.exchange_id = Some(oid_str.clone());
                                    h.state = OrderState::Accepted;
                                    h.last_modified = Some(Instant::now());
                                }
                                self.state.oid_map.insert(oid_str.clone(), cid);
                                let _ = self.event_tx.send(OmsEvent::OrderAccepted {
                                    client_id: ClientOrderId(cid),
                                    exchange_id: oid_str,
                                });
                            }
                            ExchangeDataStatus::Filled(filled) => {
                                let oid_str = filled.oid.to_string();
                                let avg_px: f64 = filled.avg_px.parse().unwrap_or(0.0);
                                let total_sz: f64 = filled.total_sz.parse().unwrap_or(0.0);
                                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                                    h.exchange_id = Some(oid_str.clone());
                                    h.state = OrderState::Filled;
                                    h.filled_size = total_sz;
                                    h.avg_fill_price = Some(avg_px);
                                    h.last_modified = Some(Instant::now());
                                }
                                self.state.oid_map.insert(oid_str, cid);
                            }
                            ExchangeDataStatus::Error(error) => {
                                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                                    h.state = OrderState::Rejected;
                                    h.reject_reason = Some(error.clone());
                                    h.last_modified = Some(Instant::now());
                                }
                                let _ = self.event_tx.send(OmsEvent::OrderRejected {
                                    client_id: ClientOrderId(cid),
                                    reason: error.clone(),
                                });
                            }
                            _ => {
                                warn!("unexpected order status for cid={}", cid);
                                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                                    h.state = OrderState::Rejected;
                                    h.reject_reason = Some("unexpected response".into());
                                    h.last_modified = Some(Instant::now());
                                }
                            }
                        }
                    }
                }
                // Still inflight after Ok? Mark rejected.
                if let Some(h) = self.state.orders.get(&cid) {
                    if h.state == OrderState::Inflight {
                        drop(h);
                        if let Some(mut h) = self.state.orders.get_mut(&cid) {
                            h.state = OrderState::Rejected;
                            h.reject_reason = Some("no status in response".into());
                            h.last_modified = Some(Instant::now());
                        }
                    }
                }
            }
        }
    }
    /// Sync: prepare + sign a cancel order. Marks Cancelling before returning.
    pub fn sign_cancel_order(&self, id: &ClientOrderId) -> Result<SignedPayload> {
        self.check_ready()?;

        let handle = self.state.orders.get(&id.0)
            .ok_or(OmsError::OrderNotFound(id.0))?;
        let exchange_id = handle.exchange_id.as_ref()
            .ok_or_else(|| OmsError::Internal("no exchange oid yet (still inflight)".into()))?;
        let (asset_idx, _) = self.resolve_asset(&handle.symbol)?;
        let oid: u64 = exchange_id.parse().unwrap_or(0);
        drop(handle);

        // Mark Cancelling immediately so the engine sees it
        self.mark_cancelling(id);

        self.sign_cancel_payload(asset_idx, oid)
    }

    /// Pre-sign a cancel without changing order state. Used for pre-signing
    /// cancels at accept time so the fast path can skip signing.
    pub fn presign_cancel_order(&self, id: &ClientOrderId) -> Result<SignedPayload> {
        self.check_ready()?;

        let handle = self.state.orders.get(&id.0)
            .ok_or(OmsError::OrderNotFound(id.0))?;
        let exchange_id = handle.exchange_id.as_ref()
            .ok_or_else(|| OmsError::Internal("no exchange oid yet (still inflight)".into()))?;
        let (asset_idx, _) = self.resolve_asset(&handle.symbol)?;
        let oid: u64 = exchange_id.parse().unwrap_or(0);
        drop(handle);

        // No state change — order stays Accepted until fast cancel fires
        self.sign_cancel_payload(asset_idx, oid)
    }

    /// Mark an order as Cancelling (state transition only, no signing).
    pub fn mark_cancelling(&self, id: &ClientOrderId) {
        if let Some(mut h) = self.state.orders.get_mut(&id.0) {
            h.state = OrderState::Cancelling;
            h.last_modified = Some(Instant::now());
        }
    }

    fn sign_cancel_payload(&self, asset_idx: u32, oid: u64) -> Result<SignedPayload> {
        let ex = self.client.exchange();
        let timestamp = hyperliquid_rust_sdk::next_nonce();

        let action = hyperliquid_rust_sdk::Actions::Cancel(
            hyperliquid_rust_sdk::BulkCancel {
                cancels: vec![hyperliquid_rust_sdk::CancelRequest { asset: asset_idx, oid }],
            }
        );

        let connection_id = action.hash(timestamp, ex.vault_address)
            .map_err(|e| anyhow::anyhow!("action hash failed: {e}"))?;
        let is_mainnet = ex.http_client.is_mainnet();
        let signature = hyperliquid_rust_sdk::sign_l1_action(&ex.wallet, connection_id, is_mainnet)
            .map_err(|e| anyhow::anyhow!("signing failed: {e}"))?;

        Ok(SignedPayload { action, signature, timestamp })
    }

    /// Async: post a signed cancel and handle the response.
    pub async fn post_cancel_order(&self, id: &ClientOrderId, signed: SignedPayload) {
        let action_json = match serde_json::to_value(&signed.action) {
            Ok(v) => v,
            Err(e) => {
                warn!("JSON serialize failed for cancel {}: {e}", id.0);
                return;
            }
        };

        let resp = match self.client.exchange()
            .post(action_json, signed.signature, signed.timestamp).await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("cancel HTTP failed for {}: {e}, restoring to Accepted", id.0);
                if let Some(mut h) = self.state.orders.get_mut(&id.0) {
                    h.state = OrderState::Accepted;
                    h.last_modified = Some(Instant::now());
                }
                return;
            }
        };

        match resp {
            hyperliquid_rust_sdk::ExchangeResponseStatus::Ok(_) => {
                // Cancelling already set. REST poll will confirm.
            }
            hyperliquid_rust_sdk::ExchangeResponseStatus::Err(msg) => {
                warn!("cancel rejected for {}: {msg}, restoring to Accepted", id.0);
                if let Some(mut h) = self.state.orders.get_mut(&id.0) {
                    h.state = OrderState::Accepted;
                    h.last_modified = Some(Instant::now());
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ExchangeOms trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl ExchangeOms for HyperliquidOms {
    type PreparedOrder = ClientOrderRequest;
    type SignedPayload = SignedPayload;

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    async fn wait_ready(&self) -> Result<()> {
        if self.is_ready() {
            return Ok(());
        }
        self.ready_notify.notified().await;
        Ok(())
    }

    async fn place_order(&self, req: OrderRequest) -> Result<ClientOrderId> {
        let (cid, prepared) = self.prepare_place_order(&req)?;
        let signed = self.sign_place_order(prepared)?;
        self.post_place_order(cid.0, signed).await;
        Ok(cid)
    }


    async fn cancel_order(&self, id: &ClientOrderId) -> Result<()> {
        let signed = self.sign_cancel_order(id)?;
        self.post_cancel_order(id, signed).await;
        Ok(())
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<()> {
        self.check_ready()?;

        let mut cancels = Vec::new();

        for entry in self.state.orders.iter() {
            let h = entry.value();
            if !matches!(
                h.state,
                OrderState::Accepted | OrderState::PartiallyFilled
            ) {
                continue;
            }
            if let Some(sym) = symbol {
                if h.symbol != sym {
                    continue;
                }
            }
            if let Some(eid) = &h.exchange_id {
                let (_asset_idx, asset_name) = self.resolve_asset(&h.symbol)?;
                let oid: u64 = eid.parse().unwrap_or(0);
                cancels.push(ClientCancelRequest {
                    asset: asset_name,
                    oid,
                });
            }
        }

        if cancels.is_empty() {
            return Ok(());
        }

        let _ = self.client.exchange().bulk_cancel(cancels, None)
            .await
            .map_err(|e| anyhow::anyhow!("SDK bulk cancel failed: {e}"))?;

        // Mark all matching orders as cancelling (pending confirmation)
        for mut entry in self.state.orders.iter_mut() {
            let should_cancel = matches!(
                entry.state,
                OrderState::Accepted | OrderState::PartiallyFilled
            ) && symbol.map_or(true, |s| entry.symbol == s);

            if should_cancel {
                entry.state = OrderState::Cancelling;
                entry.last_modified = Some(Instant::now());
            }
        }

        Ok(())
    }

    async fn modify_order(
        &self,
        id: &ClientOrderId,
        req: ModifyRequest,
    ) -> Result<ClientOrderId> {
        self.check_ready()?;

        let handle = self
            .state
            .orders
            .get(&id.0)
            .ok_or(OmsError::OrderNotFound(id.0))?;

        let exchange_id = handle
            .exchange_id
            .as_ref()
            .ok_or_else(|| OmsError::Internal("no exchange oid yet (still inflight)".into()))?;

        let oid: u64 = exchange_id.parse().unwrap_or(0);
        let (asset_idx, asset_name) = self.resolve_asset(&handle.symbol)?;
        let sz_dec = self.asset_map.get()
            .and_then(|m| m.sz_decimals(asset_idx))
            .unwrap_or(8);

        // Build modified order request
        let new_price = req.price.unwrap_or_else(|| match handle.order_type {
            OrderType::Limit { price, .. } => price,
            _ => 0.0,
        });
        let new_size = req.size.unwrap_or(handle.size);
        let tif_str = match handle.order_type {
            OrderType::Limit { tif, .. } => match tif {
                TimeInForce::GTC => "Gtc",
                TimeInForce::IOC => "Ioc",
                TimeInForce::PostOnly => "Alo",
            },
            _ => "Gtc",
        };

        let modified_req = OrderRequest {
            symbol: handle.symbol.clone(),
            side: handle.side,
            order_type: match handle.order_type {
                OrderType::Limit { tif, .. } => OrderType::Limit {
                    price: new_price,
                    tif,
                },
                other => other,
            },
            size: new_size,
            reduce_only: handle.reduce_only,
        };
        drop(handle);

        let new_cid = self.next_client_id.fetch_add(1, Ordering::Relaxed);

        let sdk_order = ClientOrderRequest {
            asset: asset_name,
            is_buy: modified_req.side == Side::Buy,
            reduce_only: modified_req.reduce_only,
            limit_px: round_price_for_hl(new_price),
            sz: round_size_for_hl(new_size, sz_dec),
            cloid: Some(uuid::Uuid::from_u128(new_cid as u128)),
            order_type: ClientOrder::Limit(ClientLimit { tif: tif_str.to_string() }),
        };

        let _resp = self.client.exchange()
            .bulk_modify(vec![ClientModifyRequest { oid, order: sdk_order }], None)
            .await
            .map_err(|e| anyhow::anyhow!("SDK modify failed: {e}"))?;

        // Mark old order as cancelling (pending confirmation)
        if let Some(mut h) = self.state.orders.get_mut(&id.0) {
            h.state = OrderState::Cancelling;
            h.last_modified = Some(Instant::now());
        }

        // Track the new order as inflight
        let new_handle = OrderHandle {
            client_id: ClientOrderId(new_cid),
            exchange_id: None,
            symbol: modified_req.symbol,
            side: modified_req.side,
            order_type: modified_req.order_type,
            size: modified_req.size,
            filled_size: 0.0,
            avg_fill_price: None,
            state: OrderState::Inflight,
            reduce_only: modified_req.reduce_only,
            reject_reason: None,
            exchange_ts: None,
            submitted_at: Some(Instant::now()),
            last_modified: Some(Instant::now()),
        };
        self.state.orders.insert(new_cid, new_handle);

        Ok(ClientOrderId(new_cid))
    }

    fn get_order(&self, id: &ClientOrderId) -> Option<OrderHandle> {
        self.state.orders.get(&id.0).map(|e| e.value().clone())
    }

    fn open_orders(&self, symbol: Option<&str>) -> Vec<OrderHandle> {
        self.state
            .orders
            .iter()
            .filter(|e| {
                let h = e.value();
                matches!(
                    h.state,
                    OrderState::Inflight
                        | OrderState::Accepted
                        | OrderState::PartiallyFilled
                        | OrderState::Cancelling
                ) && symbol.map_or(true, |s| h.symbol == s)
            })
            .map(|e| e.value().clone())
            .collect()
    }

    fn inflight_orders(&self, symbol: Option<&str>) -> Vec<OrderHandle> {
        self.state
            .orders
            .iter()
            .filter(|e| {
                let h = e.value();
                h.state == OrderState::Inflight
                    && symbol.map_or(true, |s| h.symbol == s)
            })
            .map(|e| e.value().clone())
            .collect()
    }

    fn positions(&self) -> Vec<Position> {
        self.state
            .positions
            .iter()
            .map(|e| e.value().clone())
            .collect()
    }

    fn balances(&self) -> Vec<Balance> {
        self.state
            .balances
            .iter()
            .map(|e| e.value().clone())
            .collect()
    }

    fn event_receiver(&self) -> crossbeam_channel::Receiver<OmsEvent> {
        self.event_rx.clone()
    }

    fn round_price(&self, price: f64) -> f64 {
        round_price_for_hl(price)
    }

    // -- Hot-path split order placement --

    fn prepare_place_order(&self, req: &OrderRequest) -> Result<(ClientOrderId, Self::PreparedOrder)> {
        HyperliquidOms::prepare_place_order(self, req)
    }

    fn sign_order(&self, prepared: Self::PreparedOrder) -> Result<Self::SignedPayload> {
        self.sign_place_order(prepared)
    }

    async fn post_order(&self, cid: u64, signed: Self::SignedPayload) {
        self.post_place_order(cid, signed).await
    }

    // -- Hot-path split cancel --

    fn sign_cancel(&self, id: &ClientOrderId) -> Result<Self::SignedPayload> {
        self.sign_cancel_order(id)
    }

    fn presign_cancel(&self, id: &ClientOrderId) -> Result<Self::SignedPayload> {
        self.presign_cancel_order(id)
    }

    fn mark_cancelling(&self, id: &ClientOrderId) {
        HyperliquidOms::mark_cancelling(self, id)
    }

    async fn post_cancel(&self, id: &ClientOrderId, signed: Self::SignedPayload) {
        self.post_cancel_order(id, signed).await
    }

    async fn shutdown_cancel_all(&self, symbol: Option<&str>) -> Result<()> {
        HyperliquidOms::shutdown_cancel_all(self, symbol).await
    }
}

// ---------------------------------------------------------------------------
// Price rounding for HL: max 5 significant figures, max 6 decimal places
// ---------------------------------------------------------------------------

/// Round size to the asset's szDecimals.
fn round_size_for_hl(size: f64, sz_decimals: u32) -> f64 {
    let factor = 10f64.powi(sz_decimals as i32);
    (size * factor).round() / factor
}

fn round_price_for_hl(price: f64) -> f64 {
    if price == 0.0 {
        return 0.0;
    }
    let d = price.abs().log10().floor() as i32 + 1;
    let power = 5 - d;
    let mag = 10f64.powi(power);
    let rounded = (price * mag).round() / mag;
    // Cap at 6 decimal places
    (rounded * 1e6).round() / 1e6
}

