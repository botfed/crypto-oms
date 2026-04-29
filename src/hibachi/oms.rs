use anyhow::{Result, bail};
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use oms_core::*;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use super::client::{ContractInfo, HibachiClient};
use super::types::*;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

pub struct HibachiOmsConfig {
    pub api_key: String,
    pub private_key: String,
    pub account_id: u64,
    pub api_url: Option<String>,
    pub data_api_url: Option<String>,
    /// How often to poll REST for full state reconciliation
    pub poll_interval: Duration,
    /// How long to wait for exchange ack before marking order as timed out
    pub inflight_timeout: Duration,
    /// How long to wait before overriding Cancelling/Cancelled state from REST poll
    pub stray_order_age: Duration,
    /// Max fee percent for orders (e.g. 0.001 = 0.1%)
    pub max_fees_percent: f64,
}

impl Default for HibachiOmsConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            private_key: String::new(),
            account_id: 0,
            api_url: None,
            data_api_url: None,
            poll_interval: Duration::from_secs(3),
            inflight_timeout: Duration::from_secs(5),
            stray_order_age: Duration::from_secs(5),
            max_fees_percent: 0.001,
        }
    }
}

// ---------------------------------------------------------------------------
// Diagnostics
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
    /// exchange order_id -> client_id mapping
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
}

// ---------------------------------------------------------------------------
// Trade WS types
// ---------------------------------------------------------------------------

struct TradeWsCommand {
    id: u64,
    json: String,
    response_tx: tokio::sync::oneshot::Sender<TradeWsResponse>,
}

struct TradeWsResponse {
    pub status: u16,
    pub result: Option<serde_json::Value>,
    pub raw: String,
}

// ---------------------------------------------------------------------------
// Hibachi OMS
// ---------------------------------------------------------------------------

pub struct HibachiOms {
    client: Arc<HibachiClient>,
    /// contract_id -> ContractInfo, populated during init
    contracts: Arc<parking_lot::RwLock<HashMap<String, ContractInfo>>>,
    state: Arc<OmsState>,
    ready: Arc<AtomicBool>,
    ready_notify: Arc<Notify>,
    next_client_id: AtomicU64,
    next_ws_id: AtomicU64,
    event_tx: crossbeam_channel::Sender<OmsEvent>,
    event_rx: crossbeam_channel::Receiver<OmsEvent>,
    trade_ws_tx: tokio::sync::mpsc::UnboundedSender<TradeWsCommand>,
    trade_ws_rx: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<TradeWsCommand>>>,
    trade_ws_connected: Arc<AtomicBool>,
    config: HibachiOmsConfig,
    shutdown: Arc<Notify>,
    pub diag: Arc<OmsDiagnostics>,
}

impl HibachiOms {
    pub fn new(config: HibachiOmsConfig) -> Result<Arc<Self>> {
        let client = HibachiClient::new(
            config.api_key.clone(),
            config.account_id,
            config.private_key.clone(),
            config.api_url.clone(),
            config.data_api_url.clone(),
        )?;
        let client = Arc::new(client);
        let (event_tx, event_rx) = crossbeam_channel::bounded(4096);
        let (trade_ws_tx, trade_ws_rx) = tokio::sync::mpsc::unbounded_channel();

        let oms = Arc::new(Self {
            client,
            contracts: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            state: Arc::new(OmsState::new()),
            ready: Arc::new(AtomicBool::new(false)),
            ready_notify: Arc::new(Notify::new()),
            next_client_id: AtomicU64::new(1),
            next_ws_id: AtomicU64::new(1),
            event_tx,
            event_rx,
            trade_ws_tx,
            trade_ws_rx: Mutex::new(Some(trade_ws_rx)),
            trade_ws_connected: Arc::new(AtomicBool::new(false)),
            config,
            shutdown: Arc::new(Notify::new()),
            diag: Arc::new(OmsDiagnostics::new()),
        });

        Ok(oms)
    }

    pub fn client(&self) -> &HibachiClient {
        &self.client
    }

    /// Start all background tasks.
    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = this.run_init().await {
                this.diag.set_error(format!("init failed: {e:#}"));
                error!("hibachi OMS init failed: {e:#}");
            }
        });

        let this = Arc::clone(self);
        tokio::spawn(async move { this.run_poller().await });

        let this = Arc::clone(self);
        tokio::spawn(async move { this.run_account_ws().await });

        let this = Arc::clone(self);
        tokio::spawn(async move { this.run_inflight_watchdog().await });

        let this = Arc::clone(self);
        let trade_ws_rx = self.trade_ws_rx.lock().take()
            .expect("start() called twice");
        tokio::spawn(async move { this.run_trade_ws(trade_ws_rx).await });
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    // -----------------------------------------------------------------------
    // Initialization
    // -----------------------------------------------------------------------

    async fn run_init(&self) -> Result<()> {
        self.diag.log("initializing...".into());
        info!("hibachi OMS: fetching exchange info...");

        // 1. Fetch exchange info for contract metadata (with timeout + retry)
        let info = {
            let mut last_err = None;
            let mut result = None;
            for attempt in 1..=3 {
                match tokio::time::timeout(
                    Duration::from_secs(10),
                    self.client.get_exchange_info(),
                ).await {
                    Ok(Ok(info)) => { result = Some(info); break; }
                    Ok(Err(e)) => {
                        let err_str = format!("{e}");
                        let is_429 = err_str.contains("429");
                        warn!("hibachi OMS: get_exchange_info attempt {attempt}/3 failed: {e}");
                        last_err = Some(e);
                        if is_429 && attempt < 3 {
                            warn!("hibachi OMS: rate limited, waiting 30s before retry");
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            continue;
                        }
                    }
                    Err(_) => {
                        warn!("hibachi OMS: get_exchange_info attempt {attempt}/3 timed out (10s)");
                        last_err = Some(anyhow::anyhow!("timeout"));
                    }
                }
                if attempt < 3 {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
            result.ok_or_else(|| last_err.unwrap_or_else(|| anyhow::anyhow!("get_exchange_info failed")))?
        };

        let mut contracts = HashMap::new();
        for fc in &info.future_contracts {
            let tick_size = fc.tick_size.as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.01);
            let lot_size = fc.lot_size.as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.001);
            contracts.insert(
                fc.symbol.clone(),
                ContractInfo {
                    id: fc.id,
                    symbol: fc.symbol.clone(),
                    underlying_decimals: fc.underlying_decimals,
                    settlement_decimals: fc.settlement_decimals,
                    tick_size,
                    lot_size,
                },
            );
        }
        self.diag.log(format!("loaded {} contracts", contracts.len()));
        info!("hibachi OMS: loaded {} contracts", contracts.len());
        *self.contracts.write() = contracts;

        // 2. Snapshot full state (with timeout + retry)
        info!("hibachi OMS: fetching account snapshot...");
        {
            let mut last_err = None;
            let mut ok = false;
            for attempt in 1..=3 {
                match tokio::time::timeout(
                    Duration::from_secs(10),
                    self.snapshot_from_rest(),
                ).await {
                    Ok(Ok(())) => { ok = true; break; }
                    Ok(Err(e)) => {
                        let err_str = format!("{e}");
                        let is_429 = err_str.contains("429");
                        warn!("hibachi OMS: snapshot attempt {attempt}/3 failed: {e}");
                        last_err = Some(e);
                        if is_429 && attempt < 3 {
                            warn!("hibachi OMS: rate limited, waiting 30s before retry");
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            continue;
                        }
                    }
                    Err(_) => {
                        warn!("hibachi OMS: snapshot attempt {attempt}/3 timed out (10s)");
                        last_err = Some(anyhow::anyhow!("timeout"));
                    }
                }
                if attempt < 3 {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
            if !ok {
                return Err(last_err.unwrap_or_else(|| anyhow::anyhow!("snapshot_from_rest failed")));
            }
        }

        // 3. Mark ready
        self.ready.store(true, Ordering::Release);
        self.ready_notify.notify_waiters();
        let _ = self.event_tx.send(OmsEvent::Ready);
        info!("hibachi OMS ready");

        Ok(())
    }

    // -----------------------------------------------------------------------
    // REST snapshot
    // -----------------------------------------------------------------------

    async fn snapshot_from_rest(&self) -> Result<()> {
        let (pending_orders, account_info) = tokio::try_join!(
            self.client.get_pending_orders(),
            self.client.get_account_info(),
        )?;

        // Reconcile orders
        let mut exchange_oids: HashSet<String> = HashSet::new();

        for oo in &pending_orders {
            let oid_str = oo.order_id.clone();
            exchange_oids.insert(oid_str.clone());

            if let Some(cid) = self.state.oid_map.get(&oid_str) {
                let cid = *cid;
                if let Some(mut handle) = self.state.orders.get_mut(&cid) {
                    let total_qty: f64 = oo.total_quantity.as_deref()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(handle.size);
                    let avail_qty: f64 = oo.available_quantity.as_deref()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(total_qty);
                    let prev_filled = handle.filled_size;
                    handle.filled_size = total_qty - avail_qty;

                    let age_exceeded = handle.last_modified
                        .map(|t| t.elapsed() >= self.config.stray_order_age)
                        .unwrap_or(true);

                    if age_exceeded && handle.filled_size - prev_filled > 1e-12 {
                        let fill = Fill {
                            client_id: handle.client_id,
                            exchange_id: handle.exchange_id.clone().unwrap_or_default(),
                            fill_id: format!("poll_{}", self.next_client_id.fetch_add(1, Ordering::Relaxed)),
                            symbol: handle.symbol.clone(),
                            side: handle.side,
                            price: 0.0,
                            size: handle.filled_size - prev_filled,
                            fee: 0.0,
                            fee_asset: "USDT".to_string(),
                            liquidity: Liquidity::Taker,
                            ts: Utc::now(),
                        };
                        if avail_qty < 1e-12 {
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
                        OrderState::Accepted | OrderState::PartiallyFilled => {
                            handle.state = active_state;
                        }
                        OrderState::Inflight => {
                            handle.state = active_state;
                            handle.last_modified = Some(Instant::now());
                        }
                        OrderState::Cancelling => {
                            if age_exceeded {
                                warn!("cancel failed for cid={}, restoring to {:?}", cid, active_state);
                                handle.state = active_state;
                                handle.last_modified = Some(Instant::now());
                            }
                        }
                        OrderState::Cancelled => {
                            if age_exceeded {
                                warn!("zombie order cid={} still on exchange, restoring to {:?}", cid, active_state);
                                handle.state = active_state;
                                handle.last_modified = Some(Instant::now());
                            }
                        }
                        _ => {
                            handle.state = active_state;
                            handle.last_modified = Some(Instant::now());
                        }
                    }
                }
            } else {
                // Unknown order — track with synthetic client ID
                let cid = self.next_client_id.fetch_add(1, Ordering::Relaxed);
                let total_qty: f64 = oo.total_quantity.as_deref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let avail_qty: f64 = oo.available_quantity.as_deref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(total_qty);
                let side = if oo.side == "BID" || oo.side == "BUY" { Side::Buy } else { Side::Sell };
                let price: f64 = oo.price.as_deref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let symbol = hibachi_to_canonical(&oo.symbol);
                let tif = match oo.order_flags.as_deref() {
                    Some("IOC") => TimeInForce::IOC,
                    Some("POST_ONLY") => TimeInForce::PostOnly,
                    _ => TimeInForce::GTC,
                };

                let handle = OrderHandle {
                    client_id: ClientOrderId(cid),
                    exchange_id: Some(oid_str.clone()),
                    symbol,
                    side,
                    order_type: if price > 0.0 {
                        OrderType::Limit { price, tif }
                    } else {
                        OrderType::Market
                    },
                    size: total_qty,
                    filled_size: total_qty - avail_qty,
                    avg_fill_price: None,
                    state: if total_qty - avail_qty > 0.0 {
                        OrderState::PartiallyFilled
                    } else {
                        OrderState::Accepted
                    },
                    reduce_only: oo.order_flags.as_deref() == Some("REDUCE_ONLY"),
                    reject_reason: None,
                    exchange_ts: None,
                    submitted_at: None,
                    last_modified: Some(Instant::now()),
                };

                self.state.oid_map.insert(oid_str, cid);
                self.state.orders.insert(cid, handle);
            }
        }

        // Mark disappeared orders
        let mut to_remove = Vec::new();
        for entry in self.state.orders.iter() {
            let handle = entry.value();
            if matches!(handle.state,
                OrderState::Accepted | OrderState::PartiallyFilled | OrderState::Cancelling
            ) {
                if let Some(eid) = &handle.exchange_id {
                    if !exchange_oids.contains(eid) {
                        to_remove.push(*entry.key());
                    }
                }
            }
        }
        for cid in to_remove {
            if let Some(mut handle) = self.state.orders.get_mut(&cid) {
                if handle.filled_size > 0.0 {
                    handle.state = OrderState::Filled;
                    let _ = self.event_tx.send(OmsEvent::OrderFilled(Fill {
                        client_id: handle.client_id,
                        exchange_id: handle.exchange_id.clone().unwrap_or_default(),
                        fill_id: format!("poll_{}", self.next_client_id.fetch_add(1, Ordering::Relaxed)),
                        symbol: handle.symbol.clone(),
                        side: handle.side,
                        price: handle.avg_fill_price.unwrap_or(0.0),
                        size: handle.filled_size,
                        fee: 0.0,
                        fee_asset: "USDT".to_string(),
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

        // Clean up oid_map
        self.state.oid_map.retain(|_oid, cid| {
            self.state.orders.get(cid)
                .map(|h| !matches!(h.state,
                    OrderState::Filled | OrderState::Cancelled | OrderState::Rejected | OrderState::TimedOut
                ))
                .unwrap_or(false)
        });

        // Reconcile positions
        self.state.positions.clear();
        for p in &account_info.positions {
            let qty: f64 = p.quantity.parse().unwrap_or(0.0);
            if qty.abs() < 1e-12 {
                continue;
            }
            let symbol = hibachi_to_canonical(&p.symbol);
            let side = if p.direction == "Long" { Side::Buy } else { Side::Sell };
            let entry_price: f64 = p.open_price.parse().unwrap_or(0.0);
            let upnl: f64 = p.unrealized_trading_pnl.as_deref()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0)
                + p.unrealized_funding_pnl.as_deref()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

            let pos = Position {
                symbol: symbol.clone(),
                side,
                size: qty.abs(),
                entry_price,
                unrealized_pnl: upnl,
                leverage: 0.0, // Hibachi doesn't expose per-position leverage
                liquidation_price: None,
            };
            self.state.positions.insert(symbol, pos);
        }

        // Reconcile balances
        self.state.balances.clear();
        let balance: f64 = account_info.balance.parse().unwrap_or(0.0);
        let withdraw: f64 = account_info.maximal_withdraw.as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(balance);
        self.state.balances.insert(
            "USDT".to_string(),
            Balance {
                asset: "USDT".to_string(),
                available: withdraw,
                locked: balance - withdraw,
                total: balance,
                equity: balance,
            },
        );

        self.diag.poll_count.fetch_add(1, Ordering::Relaxed);
        *self.diag.last_poll_at.lock() = Some(Instant::now());
        self.diag.log(format!(
            "poll #{}: {} orders, {} positions",
            self.diag.poll_count.load(Ordering::Relaxed),
            pending_orders.len(),
            self.state.positions.len(),
        ));

        // Emit Snapshot
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
    // REST poller
    // -----------------------------------------------------------------------

    async fn run_poller(&self) {
        self.ready_notify.notified().await;

        loop {
            if let Err(e) = self.snapshot_from_rest().await {
                warn!("hibachi REST poll failed: {e:#}");
                self.diag.set_error(format!("REST poll failed: {e:#}"));
            }

            tokio::select! {
                _ = tokio::time::sleep(self.config.poll_interval) => {}
                _ = self.shutdown.notified() => return,
            }
        }
    }

    // -----------------------------------------------------------------------
    // Account WebSocket (position/balance/order updates)
    // -----------------------------------------------------------------------

    async fn run_account_ws(&self) {
        // Wait for init
        self.ready_notify.notified().await;

        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        loop {
            if let Err(e) = self.account_ws_loop().await {
                self.diag.ws_connected.store(false, Ordering::Release);
                self.diag.set_error(format!("Account WS disconnected: {e:#}"));
                let _ = self.event_tx.send(OmsEvent::Disconnected);
            }

            self.diag.log(format!("Account WS reconnecting in {:.0}s...", backoff.as_secs_f64()));
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = self.shutdown.notified() => return,
            }
            backoff = (backoff * 2).min(max_backoff);

            if self.ready.load(Ordering::Acquire) {
                self.diag.log("Account WS re-snapshotting before reconnect...".into());
                match self.snapshot_from_rest().await {
                    Ok(()) => {
                        let _ = self.event_tx.send(OmsEvent::Reconnected);
                        let _ = self.event_tx.send(OmsEvent::Ready);
                        backoff = Duration::from_secs(1);
                    }
                    Err(e) => {
                        self.diag.set_error(format!("re-snapshot failed: {e:#}"));
                        continue;
                    }
                }
            }
        }
    }

    async fn account_ws_loop(&self) -> Result<()> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::protocol::Message;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let url = self.client.account_ws_url();
        let mut request = url.as_str().into_client_request()?;
        request.headers_mut().insert(
            "Authorization",
            self.client.api_key.parse()?,
        );

        let (ws, _) = tokio_tungstenite::connect_async(request).await?;
        let (mut write, mut read) = ws.split();
        self.diag.ws_connected.store(true, Ordering::Release);
        self.diag.log(format!("Account WS connected"));

        // Send stream.start
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let start_req = WsAccountRequest {
            id: 1,
            method: "stream.start".to_string(),
            params: serde_json::json!({"accountId": self.client.account_id}),
            timestamp: now_secs,
        };
        write.send(Message::Text(serde_json::to_string(&start_req)?.into())).await?;

        // Read stream.start response to get listen_key
        let mut listen_key = String::new();
        let mut msg_id: u64 = 2;

        // Process messages
        let ping_interval_secs = 15;
        let mut ping_timer = tokio::time::interval(Duration::from_secs(ping_interval_secs));
        ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping_timer.tick().await; // consume immediate tick

        loop {
            tokio::select! {
                msg = read.next() => {
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => bail!("Account WS error: {e}"),
                        None => bail!("Account WS stream ended"),
                    };
                    match msg {
                        Message::Text(text) => {
                            self.diag.ws_msg_count.fetch_add(1, Ordering::Relaxed);
                            self.handle_account_ws_message(&text, &mut listen_key);
                        }
                        Message::Ping(data) => {
                            write.send(Message::Pong(data)).await?;
                        }
                        Message::Close(_) => bail!("Account WS closed by server"),
                        _ => {}
                    }
                }
                _ = ping_timer.tick() => {
                    if !listen_key.is_empty() {
                        let now_secs = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        let ping_req = WsAccountRequest {
                            id: msg_id,
                            method: "stream.ping".to_string(),
                            params: serde_json::json!({
                                "accountId": self.client.account_id,
                                "listenKey": listen_key,
                            }),
                            timestamp: now_secs,
                        };
                        msg_id += 1;
                        write.send(Message::Text(serde_json::to_string(&ping_req)?.into())).await?;
                    }
                }
                _ = self.shutdown.notified() => return Ok(()),
            }
        }
    }

    fn handle_account_ws_message(&self, text: &str, listen_key: &mut String) {
        let msg: WsAccountResponse = match serde_json::from_str(text) {
            Ok(m) => m,
            Err(e) => {
                self.diag.log(format!("Account WS parse error: {e}"));
                return;
            }
        };

        // Handle stream.start response
        if let Some(result) = &msg.result {
            if let Some(key) = result.get("listenKey").and_then(|v| v.as_str()) {
                *listen_key = key.to_string();
                self.diag.log(format!("Account WS got listen key"));
            }
            // Process initial snapshot if present
            if let Some(snapshot) = result.get("accountSnapshot") {
                self.process_account_snapshot(snapshot);
            }
        }

        // Handle pushed updates by topic
        if let Some(topic) = &msg.topic {
            match topic.as_str() {
                "position_update" | "balance_update" => {
                    // These will be reconciled by the REST poller; just log for now
                    self.diag.log(format!("Account WS: {topic}"));
                }
                _ => {
                    debug!("Account WS unknown topic: {topic}");
                }
            }
        }
    }

    fn process_account_snapshot(&self, snapshot: &serde_json::Value) {
        // Update balance from snapshot
        if let Some(balance_str) = snapshot.get("balance").and_then(|v| v.as_str()) {
            let balance: f64 = balance_str.parse().unwrap_or(0.0);
            self.state.balances.insert(
                "USDT".to_string(),
                Balance {
                    asset: "USDT".to_string(),
                    available: balance,
                    locked: 0.0,
                    total: balance,
                    equity: balance,
                },
            );
        }

        // Update positions from snapshot
        if let Some(positions) = snapshot.get("positions").and_then(|v| v.as_array()) {
            self.state.positions.clear();
            for p in positions {
                let symbol_raw = p.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                let symbol = hibachi_to_canonical(symbol_raw);
                let qty: f64 = p.get("quantity").and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                if qty.abs() < 1e-12 {
                    continue;
                }
                let direction = p.get("direction").and_then(|v| v.as_str()).unwrap_or("");
                let side = if direction == "Long" { Side::Buy } else { Side::Sell };
                let entry: f64 = p.get("openPrice").and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);
                let upnl: f64 = p.get("unrealizedTradingPnl").and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0.0);

                self.state.positions.insert(symbol.clone(), Position {
                    symbol,
                    side,
                    size: qty.abs(),
                    entry_price: entry,
                    unrealized_pnl: upnl,
                    leverage: 0.0,
                    liquidation_price: None,
                });
            }
        }
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
                        let _ = self.event_tx
                            .send(OmsEvent::OrderTimedOut(ClientOrderId(cid)));
                        break; // re-iterate after mutation
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Trade WS (order place/cancel/modify)
    // -----------------------------------------------------------------------

    async fn run_trade_ws(
        &self,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<TradeWsCommand>,
    ) {
        self.ready_notify.notified().await;

        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        loop {
            match self.trade_ws_loop(&mut rx).await {
                Ok(()) => return, // shutdown
                Err(e) => {
                    self.trade_ws_connected.store(false, Ordering::Release);
                    warn!("trade WS disconnected: {e:#}");
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = self.shutdown.notified() => return,
            }
            backoff = (backoff * 2).min(max_backoff);
        }
    }

    async fn trade_ws_loop(
        &self,
        rx: &mut tokio::sync::mpsc::UnboundedReceiver<TradeWsCommand>,
    ) -> Result<()> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::protocol::Message;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let url = self.client.trade_ws_url();
        let mut request = url.as_str().into_client_request()?;
        request.headers_mut().insert(
            "Authorization",
            self.client.api_key.parse()?,
        );

        let (ws, _) = tokio_tungstenite::connect_async(request).await?;
        let (mut write, mut read) = ws.split();
        self.trade_ws_connected.store(true, Ordering::Release);
        info!("trade WS connected to {}", url);

        let mut pending: HashMap<u64, tokio::sync::oneshot::Sender<TradeWsResponse>> =
            HashMap::new();
        let mut ping_timer = tokio::time::interval(Duration::from_secs(15));
        ping_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping_timer.tick().await; // consume immediate tick

        loop {
            tokio::select! {
                // Outgoing commands from post_order / post_cancel
                cmd = rx.recv() => {
                    let cmd = match cmd {
                        Some(c) => c,
                        None => return Ok(()), // channel closed = shutdown
                    };
                    debug!("trade WS send id={}: {}", cmd.id, cmd.json);
                    pending.insert(cmd.id, cmd.response_tx);
                    write.send(Message::Text(cmd.json.into())).await?;
                }

                // Incoming responses from exchange
                msg = read.next() => {
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => bail!("trade WS error: {e}"),
                        None => bail!("trade WS stream ended"),
                    };
                    match msg {
                        Message::Text(text) => {
                            if text.as_str() == "pong" {
                                continue;
                            }
                            // Parse response: {"id":N,"status":200,"result":{...}}
                            // Note: place response returns id as string, cancel as integer
                            debug!("trade WS recv: {}", text);
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(text.as_str()) {
                                let id = v.get("id")
                                    .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                                    .unwrap_or(0);
                                let status = v.get("status")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0) as u16;
                                let result = v.get("result").cloned();

                                if let Some(tx) = pending.remove(&id) {
                                    let _ = tx.send(TradeWsResponse {
                                        status,
                                        result,
                                        raw: text.to_string(),
                                    });
                                } else if pending.len() == 1 {
                                    // Place responses return accountId as id, not request id.
                                    // If exactly one pending, it must be for that request.
                                    let key = *pending.keys().next().unwrap();
                                    if let Some(tx) = pending.remove(&key) {
                                        let _ = tx.send(TradeWsResponse {
                                            status,
                                            result,
                                            raw: text.to_string(),
                                        });
                                    }
                                } else {
                                    warn!("trade WS response for unknown id={} (pending={}): {}",
                                        id, pending.len(), text);
                                }
                            } else {
                                debug!("trade WS unparseable: {}", text);
                            }
                        }
                        Message::Ping(data) => {
                            write.send(Message::Pong(data)).await?;
                        }
                        Message::Close(_) => bail!("trade WS closed by server"),
                        _ => {}
                    }
                }

                // Keepalive
                _ = ping_timer.tick() => {
                    write.send(Message::Text("ping".into())).await?;
                }

                _ = self.shutdown.notified() => {
                    // Drop all pending — callers get RecvError → fall back to REST
                    return Ok(());
                }
            }
        }
    }

    /// Send a command over the trade WS and await the response.
    /// Returns None if WS is down (caller should fall back to REST).
    async fn send_trade_ws(&self, method: &str, params: serde_json::Value, signature: &str) -> Option<TradeWsResponse> {
        if !self.trade_ws_connected.load(Ordering::Acquire) {
            return None;
        }

        let id = self.next_ws_id.fetch_add(1, Ordering::Relaxed);
        let json = serde_json::json!({
            "id": id,
            "method": method,
            "params": params,
            "signature": signature,
        });
        let json_str = serde_json::to_string(&json).ok()?;

        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let cmd = TradeWsCommand { id, json: json_str, response_tx };

        if self.trade_ws_tx.send(cmd).is_err() {
            return None; // channel closed
        }

        // Timeout waiting for response
        match tokio::time::timeout(Duration::from_secs(5), response_rx).await {
            Ok(Ok(resp)) => Some(resp),
            Ok(Err(_)) => None,   // sender dropped (WS reconnecting)
            Err(_) => {
                warn!("trade WS response timeout for id={}", id);
                None
            }
        }
    }

    // -----------------------------------------------------------------------
    // Order helpers
    // -----------------------------------------------------------------------

    fn check_ready(&self) -> Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            bail!(OmsError::NotReady);
        }
        Ok(())
    }

    fn get_contract(&self, symbol: &str) -> Result<ContractInfo> {
        let native = canonical_to_hibachi(symbol);
        let contracts = self.contracts.read();
        contracts.get(&native)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!(OmsError::Internal(format!("unknown symbol: {symbol} (native: {native})"))))
    }

    fn round_to_tick(price: f64, tick_size: f64) -> f64 {
        if tick_size <= 0.0 {
            return price;
        }
        let ticks = (price / tick_size).round();
        // Determine decimal precision from tick_size to avoid floating-point drift
        let decimals = (-tick_size.log10()).ceil().max(0.0) as u32;
        let factor = 10f64.powi(decimals as i32);
        ((ticks * tick_size) * factor).round() / factor
    }

    fn round_to_lot(size: f64, lot_size: f64) -> f64 {
        if lot_size <= 0.0 {
            return size;
        }
        (size / lot_size).round() * lot_size
    }
}

// ---------------------------------------------------------------------------
// Hot-path types
// ---------------------------------------------------------------------------

/// Prepared order ready for signing.
pub struct HibachiPreparedOrder {
    pub cid: u64,
    pub nonce: u64,
    pub contract: ContractInfo,
    pub req: OrderRequest,
    pub quantity_raw: u64,
    pub side_code: u32,
    pub price_raw: Option<u64>,
    pub max_fees_raw: u64,
}

/// Signed payload ready for HTTP post (order or cancel).
pub enum HibachiSignedPayload {
    Order { body: serde_json::Value },
    Cancel { body: serde_json::Value, exchange_id: String, nonce: u64, signature: String },
}

// ---------------------------------------------------------------------------
// ExchangeOms trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl ExchangeOms for HibachiOms {
    type PreparedOrder = HibachiPreparedOrder;
    type SignedPayload = HibachiSignedPayload;

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
        self.check_ready()?;

        let contract = self.get_contract(&req.symbol)?;
        let cid = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let nonce = HibachiClient::gen_nonce();

        let (price, tif) = match req.order_type {
            OrderType::Limit { price, tif } => (Some(price), tif),
            OrderType::Market => (None, TimeInForce::GTC),
            _ => bail!(OmsError::UnsupportedOrderType("hibachi only supports Limit and Market".into())),
        };

        // Build handle
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

        // Build signed request
        let side_str = if req.side == Side::Buy { "BID" } else { "ASK" };
        let side_code: u32 = if req.side == Side::Buy { 1 } else { 0 };
        let rounded_size = Self::round_to_lot(req.size, contract.lot_size);
        let quantity_raw = (rounded_size * 10f64.powi(contract.underlying_decimals as i32)) as u64;
        let max_fees_raw = (self.config.max_fees_percent * 1e8) as u64;

        let price_raw = price.map(|p| {
            let rounded = Self::round_to_tick(p, contract.tick_size);
            let dec_diff = contract.settlement_decimals as i32 - contract.underlying_decimals as i32;
            (rounded * (1u64 << 32) as f64 * 10f64.powi(dec_diff)) as u64
        });

        let payload = self.client.build_order_payload(
            nonce, contract.id, quantity_raw, side_code, price_raw, max_fees_raw,
        );
        let signature = self.client.sign(&payload)?;

        let native_symbol = canonical_to_hibachi(&req.symbol);
        let mut body = serde_json::json!({
            "accountId": self.client.account_id,
            "nonce": nonce.to_string(),
            "symbol": native_symbol,
            "quantity": format!("{rounded_size}"),
            "side": side_str,
            "maxFeesPercent": format!("{}", self.config.max_fees_percent),
            "signature": signature,
        });

        if let Some(p) = price {
            let rounded = Self::round_to_tick(p, contract.tick_size);
            body["orderType"] = serde_json::json!("LIMIT");
            body["price"] = serde_json::json!(format!("{rounded}"));
        } else {
            body["orderType"] = serde_json::json!("MARKET");
        }

        match tif {
            TimeInForce::PostOnly => { body["orderFlags"] = serde_json::json!("POST_ONLY"); }
            TimeInForce::IOC => { body["orderFlags"] = serde_json::json!("IOC"); }
            _ => {}
        }
        if req.reduce_only {
            body["orderFlags"] = serde_json::json!("REDUCE_ONLY");
        }

        // POST to exchange
        match self.client.place_order_rest(body).await {
            Ok(resp) => {
                let oid = resp.order_id;
                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                    h.exchange_id = Some(oid.clone());
                    h.state = OrderState::Accepted;
                    h.last_modified = Some(Instant::now());
                }
                self.state.oid_map.insert(oid.clone(), cid);
                let _ = self.event_tx.send(OmsEvent::OrderAccepted {
                    client_id: ClientOrderId(cid),
                    exchange_id: oid,
                });
            }
            Err(e) => {
                warn!("hibachi place_order failed for cid={cid}: {e}");
                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                    h.state = OrderState::Rejected;
                    h.reject_reason = Some(format!("{e}"));
                    h.last_modified = Some(Instant::now());
                }
                let _ = self.event_tx.send(OmsEvent::OrderRejected {
                    client_id: ClientOrderId(cid),
                    reason: format!("{e}"),
                });
            }
        }

        Ok(ClientOrderId(cid))
    }

    async fn cancel_order(&self, id: &ClientOrderId) -> Result<()> {
        self.check_ready()?;

        let handle = self.state.orders.get(&id.0)
            .ok_or(OmsError::OrderNotFound(id.0))?;
        let exchange_id = handle.exchange_id.clone()
            .ok_or_else(|| OmsError::Internal("no exchange oid yet (still inflight)".into()))?;
        drop(handle);

        // Mark cancelling
        if let Some(mut h) = self.state.orders.get_mut(&id.0) {
            h.state = OrderState::Cancelling;
            h.last_modified = Some(Instant::now());
        }
        let _ = self.event_tx.send(OmsEvent::OrderCancelling(*id));

        let oid_u64: u64 = exchange_id.parse().unwrap_or(0);
        let payload = self.client.build_cancel_payload(oid_u64);
        let signature = self.client.sign(&payload)?;

        let body = serde_json::json!({
            "accountId": self.client.account_id,
            "orderId": exchange_id,
            "signature": signature,
        });

        match self.client.cancel_order_rest(body).await {
            Ok(()) => {
                if let Some(mut h) = self.state.orders.get_mut(&id.0) {
                    h.state = OrderState::Cancelled;
                    h.last_modified = Some(Instant::now());
                }
                let _ = self.event_tx.send(OmsEvent::OrderCancelled(*id));
            }
            Err(e) => {
                warn!("hibachi cancel failed for {}: {e}, restoring to Accepted", id.0);
                if let Some(mut h) = self.state.orders.get_mut(&id.0) {
                    h.state = OrderState::Accepted;
                    h.last_modified = Some(Instant::now());
                }
            }
        }

        Ok(())
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<()> {
        self.check_ready()?;

        let contract_id = if let Some(sym) = symbol {
            Some(self.get_contract(sym)?.id)
        } else {
            None
        };

        self.client.cancel_all_rest(contract_id).await?;
        Ok(())
    }

    async fn modify_order(
        &self,
        id: &ClientOrderId,
        req: ModifyRequest,
    ) -> Result<ClientOrderId> {
        self.check_ready()?;

        let handle = self.state.orders.get(&id.0)
            .ok_or(OmsError::OrderNotFound(id.0))?;
        let exchange_id = handle.exchange_id.clone()
            .ok_or_else(|| OmsError::Internal("no exchange oid yet (still inflight)".into()))?;
        let symbol = handle.symbol.clone();
        let side = handle.side;
        let current_size = handle.size;
        let current_price = match handle.order_type {
            OrderType::Limit { price, .. } => price,
            _ => 0.0,
        };
        drop(handle);

        let contract = self.get_contract(&symbol)?;
        let nonce = HibachiClient::gen_nonce();
        let new_size = req.size.unwrap_or(current_size);
        let new_price = req.price.unwrap_or(current_price);

        let side_code: u32 = if side == Side::Buy { 1 } else { 0 };
        let rounded_size = Self::round_to_lot(new_size, contract.lot_size);
        let rounded_price = Self::round_to_tick(new_price, contract.tick_size);
        let quantity_raw = (rounded_size * 10f64.powi(contract.underlying_decimals as i32)) as u64;
        let max_fees_raw = (self.config.max_fees_percent * 1e8) as u64;
        let dec_diff = contract.settlement_decimals as i32 - contract.underlying_decimals as i32;
        let price_raw = (rounded_price * (1u64 << 32) as f64 * 10f64.powi(dec_diff)) as u64;

        let payload = self.client.build_order_payload(
            nonce, contract.id, quantity_raw, side_code, Some(price_raw), max_fees_raw,
        );
        let signature = self.client.sign(&payload)?;

        let body = serde_json::json!({
            "accountId": self.client.account_id,
            "nonce": nonce.to_string(),
            "orderId": exchange_id,
            "maxFeesPercent": format!("{}", self.config.max_fees_percent),
            "updatedQuantity": format!("{rounded_size}"),
            "quantity": format!("{rounded_size}"),
            "updatedPrice": format!("{rounded_price}"),
            "price": format!("{rounded_price}"),
            "signature": signature,
        });

        match self.client.modify_order_rest(body).await {
            Ok(resp) => {
                let new_oid = resp.order_id;
                // Update existing handle with new exchange_id
                if let Some(mut h) = self.state.orders.get_mut(&id.0) {
                    if let Some(old_eid) = &h.exchange_id {
                        self.state.oid_map.remove(old_eid);
                    }
                    h.exchange_id = Some(new_oid.clone());
                    h.size = rounded_size;
                    h.order_type = OrderType::Limit { price: rounded_price, tif: TimeInForce::GTC };
                    h.last_modified = Some(Instant::now());
                }
                self.state.oid_map.insert(new_oid, id.0);
                Ok(*id)
            }
            Err(e) => {
                bail!("modify failed: {e}");
            }
        }
    }

    fn get_order(&self, id: &ClientOrderId) -> Option<OrderHandle> {
        self.state.orders.get(&id.0).map(|h| h.clone())
    }

    fn open_orders(&self, symbol: Option<&str>) -> Vec<OrderHandle> {
        self.state.orders.iter()
            .filter(|e| {
                let h = e.value();
                matches!(h.state,
                    OrderState::Inflight | OrderState::Accepted | OrderState::PartiallyFilled | OrderState::Cancelling
                ) && symbol.map_or(true, |s| h.symbol == s)
            })
            .map(|e| e.value().clone())
            .collect()
    }

    fn inflight_orders(&self, symbol: Option<&str>) -> Vec<OrderHandle> {
        self.state.orders.iter()
            .filter(|e| {
                let h = e.value();
                h.state == OrderState::Inflight
                    && symbol.map_or(true, |s| h.symbol == s)
            })
            .map(|e| e.value().clone())
            .collect()
    }

    fn positions(&self) -> Vec<Position> {
        self.state.positions.iter().map(|e| e.value().clone()).collect()
    }

    fn balances(&self) -> Vec<Balance> {
        self.state.balances.iter().map(|e| e.value().clone()).collect()
    }

    fn event_receiver(&self) -> crossbeam_channel::Receiver<OmsEvent> {
        self.event_rx.clone()
    }

    fn round_price(&self, price: f64) -> f64 {
        // Use smallest tick size across all contracts as conservative default.
        // Per-symbol rounding happens in sign_order via the contract lookup.
        let contracts = self.contracts.read();
        let tick = contracts.values()
            .map(|c| c.tick_size)
            .fold(f64::MAX, f64::min);
        if tick < f64::MAX {
            Self::round_to_tick(price, tick)
        } else {
            price
        }
    }

    // -- Hot-path split order placement --

    fn prepare_place_order(&self, req: &OrderRequest) -> Result<(ClientOrderId, Self::PreparedOrder)> {
        self.check_ready()?;

        let contract = self.get_contract(&req.symbol)?;
        let cid = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let nonce = HibachiClient::gen_nonce();

        let side_code: u32 = if req.side == Side::Buy { 1 } else { 0 };
        let rounded_size = Self::round_to_lot(req.size, contract.lot_size);
        let quantity_raw = (rounded_size * 10f64.powi(contract.underlying_decimals as i32)) as u64;
        let max_fees_raw = (self.config.max_fees_percent * 1e8) as u64;

        let price_raw = match req.order_type {
            OrderType::Limit { price, .. } => {
                let rounded = Self::round_to_tick(price, contract.tick_size);
                let dec_diff = contract.settlement_decimals as i32 - contract.underlying_decimals as i32;
                Some((rounded * (1u64 << 32) as f64 * 10f64.powi(dec_diff)) as u64)
            }
            _ => None,
        };

        // Insert inflight handle
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

        Ok((ClientOrderId(cid), HibachiPreparedOrder {
            cid, nonce, contract, req: req.clone(),
            quantity_raw, side_code, price_raw, max_fees_raw,
        }))
    }

    fn sign_order(&self, prepared: Self::PreparedOrder) -> Result<Self::SignedPayload> {
        let payload = self.client.build_order_payload(
            prepared.nonce, prepared.contract.id,
            prepared.quantity_raw, prepared.side_code,
            prepared.price_raw, prepared.max_fees_raw,
        );
        let signature = self.client.sign(&payload)?;

        let rounded_size = Self::round_to_lot(prepared.req.size, prepared.contract.lot_size);
        let native_symbol = canonical_to_hibachi(&prepared.req.symbol);
        let side_str = if prepared.req.side == Side::Buy { "BID" } else { "ASK" };

        let mut body = serde_json::json!({
            "accountId": self.client.account_id,
            "nonce": prepared.nonce.to_string(),
            "symbol": native_symbol,
            "quantity": format!("{rounded_size}"),
            "side": side_str,
            "maxFeesPercent": format!("{}", self.config.max_fees_percent),
            "signature": signature,
        });

        match prepared.req.order_type {
            OrderType::Limit { price, tif } => {
                let rounded = Self::round_to_tick(price, prepared.contract.tick_size);
                body["orderType"] = serde_json::json!("LIMIT");
                body["price"] = serde_json::json!(format!("{rounded}"));
                match tif {
                    TimeInForce::PostOnly => { body["orderFlags"] = serde_json::json!("POST_ONLY"); }
                    TimeInForce::IOC => { body["orderFlags"] = serde_json::json!("IOC"); }
                    _ => {}
                }
            }
            OrderType::Market => {
                body["orderType"] = serde_json::json!("MARKET");
            }
            _ => {}
        }
        if prepared.req.reduce_only {
            body["orderFlags"] = serde_json::json!("REDUCE_ONLY");
        }

        Ok(HibachiSignedPayload::Order { body })
    }

    async fn post_order(&self, cid: u64, signed: Self::SignedPayload) {
        let body = match signed {
            HibachiSignedPayload::Order { body } => body,
            _ => { warn!("post_order called with non-order payload"); return; }
        };

        // Extract signature for WS
        let signature = body.get("signature").and_then(|v| v.as_str()).unwrap_or("").to_string();

        // Build WS params (reuse REST body fields)
        let ws_params = {
            let mut p = body.clone();
            p.as_object_mut().map(|o| o.remove("signature"));
            p
        };

        if let Some(resp) = self.send_trade_ws("order.place", ws_params, &signature).await {
            if resp.status == 200 {
                let oid = resp.result
                    .as_ref()
                    .and_then(|r| r.get("orderId"))
                    .and_then(|v| v.as_u64().map(|n| n.to_string()).or_else(|| v.as_str().map(|s| s.to_string())))
                    .unwrap_or_default();
                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                    h.exchange_id = Some(oid.clone());
                    h.state = OrderState::Accepted;
                    h.last_modified = Some(Instant::now());
                }
                self.state.oid_map.insert(oid.clone(), cid);
                let _ = self.event_tx.send(OmsEvent::OrderAccepted {
                    client_id: ClientOrderId(cid),
                    exchange_id: oid,
                });
                return;
            } else {
                warn!("hibachi WS order.place failed for cid={cid}: status={} body={}", resp.status, resp.raw);
                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                    h.state = OrderState::Rejected;
                    h.reject_reason = Some(resp.raw.clone());
                    h.last_modified = Some(Instant::now());
                }
                let _ = self.event_tx.send(OmsEvent::OrderRejected {
                    client_id: ClientOrderId(cid),
                    reason: resp.raw,
                });
                return;
            }
        }

        // REST fallback
        debug!("trade WS unavailable, falling back to REST for cid={cid}");
        match self.client.place_order_rest(body).await {
            Ok(resp) => {
                let oid = resp.order_id;
                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                    h.exchange_id = Some(oid.clone());
                    h.state = OrderState::Accepted;
                    h.last_modified = Some(Instant::now());
                }
                self.state.oid_map.insert(oid.clone(), cid);
                let _ = self.event_tx.send(OmsEvent::OrderAccepted {
                    client_id: ClientOrderId(cid),
                    exchange_id: oid,
                });
            }
            Err(e) => {
                warn!("hibachi REST post_order failed for cid={cid}: {e}");
                if let Some(mut h) = self.state.orders.get_mut(&cid) {
                    h.state = OrderState::Rejected;
                    h.reject_reason = Some(format!("{e}"));
                    h.last_modified = Some(Instant::now());
                }
                let _ = self.event_tx.send(OmsEvent::OrderRejected {
                    client_id: ClientOrderId(cid),
                    reason: format!("{e}"),
                });
            }
        }
    }

    // -- Hot-path split cancel --

    fn sign_cancel(&self, id: &ClientOrderId) -> Result<Self::SignedPayload> {
        self.check_ready()?;

        let handle = self.state.orders.get(&id.0)
            .ok_or(OmsError::OrderNotFound(id.0))?;
        let exchange_id = handle.exchange_id.clone()
            .ok_or_else(|| OmsError::Internal("no exchange oid yet".into()))?;
        drop(handle);

        // Mark cancelling
        if let Some(mut h) = self.state.orders.get_mut(&id.0) {
            h.state = OrderState::Cancelling;
            h.last_modified = Some(Instant::now());
        }
        let _ = self.event_tx.send(OmsEvent::OrderCancelling(*id));

        let nonce = HibachiClient::gen_nonce();
        let oid_u64: u64 = exchange_id.parse().unwrap_or(0);
        let payload = self.client.build_cancel_payload(oid_u64);
        let signature = self.client.sign(&payload)?;

        let body = serde_json::json!({
            "accountId": self.client.account_id,
            "orderId": &exchange_id,
            "signature": &signature,
        });

        Ok(HibachiSignedPayload::Cancel { body, exchange_id, nonce, signature })
    }

    fn presign_cancel(&self, id: &ClientOrderId) -> Result<Self::SignedPayload> {
        self.check_ready()?;

        let handle = self.state.orders.get(&id.0)
            .ok_or(OmsError::OrderNotFound(id.0))?;
        let exchange_id = handle.exchange_id.clone()
            .ok_or_else(|| OmsError::Internal("no exchange oid yet".into()))?;
        drop(handle);

        // No state change — just sign
        let nonce = HibachiClient::gen_nonce();
        let oid_u64: u64 = exchange_id.parse().unwrap_or(0);
        let payload = self.client.build_cancel_payload(oid_u64);
        let signature = self.client.sign(&payload)?;

        let body = serde_json::json!({
            "accountId": self.client.account_id,
            "orderId": &exchange_id,
            "signature": &signature,
        });

        Ok(HibachiSignedPayload::Cancel { body, exchange_id, nonce, signature })
    }

    fn mark_cancelling(&self, id: &ClientOrderId) {
        if let Some(mut h) = self.state.orders.get_mut(&id.0) {
            h.state = OrderState::Cancelling;
            h.last_modified = Some(Instant::now());
        }
    }

    async fn post_cancel(&self, id: &ClientOrderId, signed: Self::SignedPayload) {
        let (body, exchange_id, nonce, signature) = match signed {
            HibachiSignedPayload::Cancel { body, exchange_id, nonce, signature } => (body, exchange_id, nonce, signature),
            _ => { warn!("post_cancel called with non-cancel payload"); return; }
        };

        // Try WS first
        let ws_params = serde_json::json!({
            "orderId": exchange_id,
            "accountId": self.client.account_id,
            "nonce": nonce.to_string(),
        });

        if let Some(resp) = self.send_trade_ws("order.cancel", ws_params, &signature).await {
            if resp.status == 200 {
                if let Some(mut h) = self.state.orders.get_mut(&id.0) {
                    h.state = OrderState::Cancelled;
                    h.last_modified = Some(Instant::now());
                }
                let _ = self.event_tx.send(OmsEvent::OrderCancelled(*id));
                return;
            } else {
                warn!("hibachi WS order.cancel failed for {}: status={} body={}", id.0, resp.status, resp.raw);
                // Fall through to REST
            }
        }

        // REST fallback
        debug!("trade WS unavailable for cancel, falling back to REST for {}", id.0);
        match self.client.cancel_order_rest(body).await {
            Ok(()) => {
                if let Some(mut h) = self.state.orders.get_mut(&id.0) {
                    h.state = OrderState::Cancelled;
                    h.last_modified = Some(Instant::now());
                }
                let _ = self.event_tx.send(OmsEvent::OrderCancelled(*id));
            }
            Err(e) => {
                warn!("hibachi REST cancel failed for {}: {e}, restoring to Accepted", id.0);
                if let Some(mut h) = self.state.orders.get_mut(&id.0) {
                    h.state = OrderState::Accepted;
                    h.last_modified = Some(Instant::now());
                }
            }
        }
    }

    async fn shutdown_cancel_all(&self, symbol: Option<&str>) -> Result<()> {
        self.cancel_all(symbol).await
    }
}
