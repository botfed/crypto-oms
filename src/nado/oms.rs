use alloy::primitives::B256;
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use oms_core::*;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, broadcast};
use tracing::{debug, error, info, warn};

use super::client::NadoClient;
use super::types::*;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

pub struct NadoOmsConfig {
    pub private_key: String,
    pub account_address: String,
    pub subaccount_name: String,
    pub gateway_url: Option<String>,
    pub subscriptions_url: Option<String>,
    pub poll_interval: Duration,
    pub inflight_timeout: Duration,
}

impl Default for NadoOmsConfig {
    fn default() -> Self {
        Self {
            private_key: String::new(),
            account_address: String::new(),
            subaccount_name: "default".to_string(),
            gateway_url: None,
            subscriptions_url: None,
            poll_interval: Duration::from_secs(3),
            inflight_timeout: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

const MAX_LOG_LINES: usize = 30;

pub struct NadoDiagnostics {
    pub sub_ws_connected: AtomicBool,
    pub last_poll_at: Mutex<Option<Instant>>,
    pub poll_count: AtomicU64,
    pub sub_msg_count: AtomicU64,
    pub last_error: Mutex<Option<String>>,
    pub log_lines: Mutex<VecDeque<String>>,
}

impl NadoDiagnostics {
    fn new() -> Self {
        Self {
            sub_ws_connected: AtomicBool::new(false),
            last_poll_at: Mutex::new(None),
            poll_count: AtomicU64::new(0),
            sub_msg_count: AtomicU64::new(0),
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
    /// digest -> client_id mapping for WS event reconciliation
    digest_map: DashMap<String, u64>,
    positions: DashMap<String, Position>,
    balances: DashMap<String, Balance>,
}

impl OmsState {
    fn new() -> Self {
        Self {
            orders: DashMap::new(),
            digest_map: DashMap::new(),
            positions: DashMap::new(),
            balances: DashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Nado OMS
// ---------------------------------------------------------------------------

pub struct NadoOms {
    client: Arc<tokio::sync::RwLock<NadoClient>>,
    product_map: Arc<tokio::sync::RwLock<Option<ProductMap>>>,
    state: Arc<OmsState>,
    ready: Arc<AtomicBool>,
    ready_notify: Arc<Notify>,
    next_client_id: AtomicU64,
    event_tx: broadcast::Sender<OmsEvent>,
    config: NadoOmsConfig,
    shutdown: Arc<Notify>,
    pub diag: Arc<NadoDiagnostics>,
    /// Cached from client during construction (immutable after init)
    sender: String,
    signer_address: String,
    account_address: String,
}

impl NadoOms {
    pub fn new(config: NadoOmsConfig) -> Result<Arc<Self>> {
        let client = NadoClient::new(
            &config.private_key,
            config.account_address.clone(),
            &config.subaccount_name,
            config.gateway_url.clone(),
            config.subscriptions_url.clone(),
        )?;
        let sender = client.sender.clone();
        let signer_address = client.signer_address.clone();
        let account_address = client.account_address.clone();
        let (event_tx, _) = broadcast::channel(4096);

        let oms = Arc::new(Self {
            client: Arc::new(tokio::sync::RwLock::new(client)),
            product_map: Arc::new(tokio::sync::RwLock::new(None)),
            state: Arc::new(OmsState::new()),
            ready: Arc::new(AtomicBool::new(false)),
            ready_notify: Arc::new(Notify::new()),
            next_client_id: AtomicU64::new(1),
            event_tx,
            config,
            shutdown: Arc::new(Notify::new()),
            diag: Arc::new(NadoDiagnostics::new()),
            sender,
            signer_address,
            account_address,
        });

        Ok(oms)
    }

    pub fn sender(&self) -> &str {
        &self.sender
    }

    pub fn signer_address(&self) -> &str {
        &self.signer_address
    }

    pub fn account_address(&self) -> &str {
        &self.account_address
    }

    /// Start all background tasks. Call once after construction.
    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = this.run_init().await {
                this.diag.set_error(format!("init failed: {e:#}"));
                error!("nado OMS init failed: {e:#}");
            }
        });

        let this = Arc::clone(self);
        tokio::spawn(async move { this.run_poller().await });

        let this = Arc::clone(self);
        tokio::spawn(async move { this.run_subscriptions_ws().await });

        let this = Arc::clone(self);
        tokio::spawn(async move { this.run_inflight_watchdog().await });
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_waiters();
    }

    // -----------------------------------------------------------------------
    // Initialization
    // -----------------------------------------------------------------------

    async fn run_init(&self) -> Result<()> {
        self.diag.log("initializing...".into());

        // 1. Fetch contracts (needs write lock — sets chain_id, endpoint_addr)
        {
            let mut client = self.client.write().await;
            let contracts = client.fetch_contracts().await?;
            self.diag.log(format!(
                "contracts: chain_id={}, endpoint={}",
                contracts.chain_id, contracts.endpoint_addr
            ));
        }

        // 2. Validate signer
        {
            let client = self.client.read().await;
            if client.is_main_wallet() {
                self.diag.log("signer: using main wallet key".into());
            } else {
                self.diag.log(format!(
                    "signer: linked signer {}",
                    self.signer_address
                ));
                // Verify the linked signer is actually authorized
                match client.query_linked_signer().await {
                    Ok(linked) => {
                        let zero = "0x0000000000000000000000000000000000000000";
                        if linked == zero {
                            warn!(
                                "no linked signer set for this subaccount — \
                                 signing with {} may fail",
                                self.signer_address
                            );
                            self.diag.set_error(
                                "WARNING: no linked signer set, trades may be rejected"
                                    .into(),
                            );
                        } else if !linked.eq_ignore_ascii_case(&self.signer_address) {
                            warn!(
                                "linked signer mismatch: expected {}, got {}",
                                self.signer_address, linked
                            );
                            self.diag.set_error(format!(
                                "WARNING: linked signer is {} but signing with {}",
                                linked, self.signer_address
                            ));
                        } else {
                            self.diag.log(format!(
                                "linked signer verified: {}",
                                linked
                            ));
                        }
                    }
                    Err(e) => {
                        // Non-fatal: the query might not be supported yet
                        self.diag.log(format!(
                            "could not verify linked signer: {e:#}"
                        ));
                    }
                }
            }
        }

        // 3. Fetch pairs
        let pairs = {
            let client = self.client.read().await;
            client.fetch_pairs().await?
        };
        let product_map = ProductMap::from_pairs(&pairs);
        self.diag.log(format!("loaded {} pairs", pairs.len()));
        *self.product_map.write().await = Some(product_map);

        // 4. Snapshot initial state
        self.snapshot_from_rest().await?;

        // 5. Mark ready
        self.ready.store(true, Ordering::Release);
        self.ready_notify.notify_waiters();
        let _ = self.event_tx.send(OmsEvent::Ready);
        self.diag.log("ready".into());
        info!("nado OMS ready");

        Ok(())
    }

    // -----------------------------------------------------------------------
    // REST snapshot: positions + balances from subaccount_info
    // -----------------------------------------------------------------------

    async fn snapshot_from_rest(&self) -> Result<()> {
        let info = {
            let client = self.client.read().await;
            client.query_subaccount_info().await?
        };

        let pm = self.product_map.read().await;
        let product_map = pm.as_ref();

        // Build oracle price lookup from product data
        let mut oracle_prices: std::collections::HashMap<u32, f64> = std::collections::HashMap::new();
        for pp in &info.perp_products {
            if let Some(price) = parse_x18(&pp.oracle_price_x18) {
                oracle_prices.insert(pp.product_id, price);
            }
        }
        for sp in &info.spot_products {
            if let Some(price) = parse_x18(&sp.oracle_price_x18) {
                oracle_prices.insert(sp.product_id, price);
            }
        }

        // Reconcile positions from perp balances (with uPnL from oracle prices)
        self.state.positions.clear();
        let mut total_upnl = 0.0;
        for pb in &info.perp_balances {
            let amount = parse_x18(&pb.balance.amount).unwrap_or(0.0);
            if amount.abs() < 1e-12 {
                continue;
            }
            let symbol = product_map
                .and_then(|m| m.id_to_canonical(pb.product_id))
                .unwrap_or_else(|| format!("PERP_{}", pb.product_id));
            let side = if amount > 0.0 { Side::Buy } else { Side::Sell };
            let v_quote = parse_x18(&pb.balance.v_quote_balance).unwrap_or(0.0);
            let entry_price = if amount.abs() > 1e-18 {
                (v_quote / amount).abs()
            } else {
                0.0
            };

            // uPnL = amount * oracle_price + v_quote_balance
            let oracle_price = oracle_prices.get(&pb.product_id).copied().unwrap_or(0.0);
            let upnl = amount * oracle_price + v_quote;
            total_upnl += upnl;

            self.state.positions.insert(
                symbol.clone(),
                Position {
                    symbol,
                    side,
                    size: amount.abs(),
                    entry_price,
                    unrealized_pnl: upnl,
                    leverage: 1.0,
                    liquidation_price: None,
                },
            );
        }

        // Compute portfolio equity in USD: sum(spot_amount * oracle_price) + total_upnl
        self.state.balances.clear();
        let mut total_equity_usd = 0.0;
        for sb in &info.spot_balances {
            let amount = parse_x18(&sb.balance.amount).unwrap_or(0.0);
            if amount.abs() < 1e-12 {
                continue;
            }
            let oracle_price = oracle_prices.get(&sb.product_id).copied().unwrap_or(1.0);
            total_equity_usd += amount * oracle_price;
        }
        total_equity_usd += total_upnl;

        self.state.balances.insert(
            "USD".to_string(),
            Balance {
                asset: "USD".to_string(),
                available: total_equity_usd,
                locked: 0.0,
                total: total_equity_usd,
                equity: total_equity_usd,
            },
        );

        drop(pm);

        self.diag.poll_count.fetch_add(1, Ordering::Relaxed);
        *self.diag.last_poll_at.lock() = Some(Instant::now());
        self.diag.log(format!(
            "poll #{}: {} positions, {} balances, equity=${:.2}",
            self.diag.poll_count.load(Ordering::Relaxed),
            self.state.positions.len(),
            self.state.balances.len(),
            total_equity_usd,
        ));

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Periodic REST poller
    // -----------------------------------------------------------------------

    async fn run_poller(&self) {
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
    // Subscriptions WebSocket (events: order_update, fill, position_change)
    // -----------------------------------------------------------------------

    async fn run_subscriptions_ws(&self) {
        self.ready_notify.notified().await;

        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(30);

        loop {
            if let Err(e) = self.subscriptions_ws_loop().await {
                self.diag.sub_ws_connected.store(false, Ordering::Release);
                self.diag.set_error(format!("sub WS disconnected: {e:#}"));
                let _ = self.event_tx.send(OmsEvent::Disconnected);
            }

            self.diag.log(format!(
                "sub WS reconnecting in {:.0}s...",
                backoff.as_secs_f64()
            ));
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = self.shutdown.notified() => return,
            }
            backoff = (backoff * 2).min(max_backoff);

            if self.ready.load(Ordering::Acquire) {
                self.diag
                    .log("sub WS re-snapshotting before reconnect...".into());
                if let Err(e) = self.snapshot_from_rest().await {
                    self.diag.set_error(format!("re-snapshot failed: {e:#}"));
                }
                let _ = self.event_tx.send(OmsEvent::Reconnected);
            }
        }
    }

    async fn subscriptions_ws_loop(&self) -> Result<()> {
        use futures_util::{SinkExt, StreamExt};
        use nado_ws::Message;

        let url = {
            let client = self.client.read().await;
            client.subscriptions_ws_url().to_string()
        };
        let mut ws = nado_ws::connect(&url).await?;
        self.diag.sub_ws_connected.store(true, Ordering::Release);
        self.diag.log(format!("sub WS connected to {url}"));

        // Authenticate (expiration in milliseconds, must be within 100s of now)
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let expiration = now_ms + 90_000;

        let signature = {
            let client = self.client.read().await;
            client.sign_stream_auth(expiration).await?
        };

        let auth_msg = AuthenticateMsg {
            method: "authenticate".to_string(),
            id: 0,
            tx: AuthTx {
                sender: self.sender.clone(),
                expiration: expiration.to_string(),
            },
            signature,
        };
        ws.send(Message::Text(serde_json::to_string(&auth_msg)?.into()))
            .await?;
        self.diag.log("sub WS authenticated".into());

        // Subscribe to streams (product_id null = all products)
        for (id, stream_type) in [
            (1u64, "order_update"),
            (2, "fill"),
            (3, "position_change"),
        ] {
            let sub = SubscribeMsg {
                method: "subscribe".to_string(),
                stream: serde_json::json!({
                    "type": stream_type,
                    "product_id": null,
                    "subaccount": self.sender,
                }),
                id,
            };
            ws.send(Message::Text(serde_json::to_string(&sub)?.into()))
                .await?;
        }
        self.diag.log("sub WS subscribed to streams".into());

        let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await; // consume first immediate tick

        loop {
            tokio::select! {
                msg = ws.next() => {
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => bail!("sub WS error: {e}"),
                        None => bail!("sub WS stream ended"),
                    };
                    match msg {
                        Message::Text(text) => self.handle_sub_message(&text),
                        Message::Ping(data) => { ws.send(Message::Pong(data)).await?; }
                        Message::Close(_) => bail!("sub WS closed by server"),
                        _ => {}
                    }
                }
                _ = heartbeat.tick() => {
                    ws.send(Message::Text(r#"{"method":"ping"}"#.into())).await?;
                }
                _ = self.shutdown.notified() => return Ok(()),
            }
        }
    }

    fn handle_sub_message(&self, text: &str) {
        self.diag.sub_msg_count.fetch_add(1, Ordering::Relaxed);

        match serde_json::from_str::<NadoEvent>(text) {
            Ok(event) => self.handle_event(event),
            Err(_) => {
                // Auth/subscribe ack, pong, or unknown message
                debug!(
                    "sub WS non-event: {}",
                    &text[..text.len().min(200)]
                );
            }
        }
    }

    fn handle_event(&self, event: NadoEvent) {
        match event {
            NadoEvent::OrderUpdate {
                product_id: _,
                digest,
                amount,
                reason,
                id: _,
                ..
            } => {
                self.diag.log(format!(
                    "order_update: digest={}.. reason={} remaining={}",
                    &digest[..digest.len().min(10)],
                    reason,
                    amount,
                ));

                let cid = match self.state.digest_map.get(&digest) {
                    Some(c) => *c,
                    None => {
                        debug!("order_update for unknown digest {digest}");
                        return;
                    }
                };

                let new_state = match reason.as_str() {
                    "placed" => OrderState::Accepted,
                    "filled" => OrderState::Filled,
                    "cancelled" => OrderState::Cancelled,
                    _ => return,
                };

                if let Some(mut handle) = self.state.orders.get_mut(&cid) {
                    handle.state = new_state;
                    if let Some(remaining) = parse_x18(&amount) {
                        let filled = handle.size - remaining.abs();
                        if filled > 0.0 {
                            handle.filled_size = filled;
                        }
                    }
                }

                let event = match new_state {
                    OrderState::Filled => {
                        let h = self.state.orders.get(&cid);
                        OmsEvent::OrderFilled(Fill {
                            client_id: ClientOrderId(cid),
                            exchange_id: digest,
                            symbol: h
                                .as_ref()
                                .map(|h| h.symbol.clone())
                                .unwrap_or_default(),
                            side: h.as_ref().map(|h| h.side).unwrap_or(Side::Buy),
                            price: h.as_ref().and_then(|h| h.avg_fill_price).unwrap_or(0.0),
                            size: h.as_ref().map(|h| h.size).unwrap_or(0.0),
                            fee: 0.0,
                            fee_asset: "USDT".to_string(),
                            liquidity: Liquidity::Taker,
                            ts: Utc::now(),
                        })
                    }
                    OrderState::Cancelled => OmsEvent::OrderCancelled(ClientOrderId(cid)),
                    _ => return,
                };

                let _ = self.event_tx.send(event);
            }

            NadoEvent::Fill {
                order_digest,
                filled_qty,
                price,
                is_taker,
                is_bid,
                fee,
                ..
            } => {
                let cid = match self.state.digest_map.get(&order_digest) {
                    Some(c) => *c,
                    None => {
                        debug!("fill for unknown digest {order_digest}");
                        return;
                    }
                };

                let fill_price = parse_x18(&price).unwrap_or(0.0);
                let fill_size = parse_x18(&filled_qty).unwrap_or(0.0).abs();
                let fill_fee = parse_x18(&fee).unwrap_or(0.0);
                let side = if is_bid { Side::Buy } else { Side::Sell };

                self.diag.log(format!(
                    "fill: digest={}.. {} {:.6} @ {:.2} fee={:.6}",
                    &order_digest[..order_digest.len().min(10)],
                    if is_bid { "BUY" } else { "SELL" },
                    fill_size,
                    fill_price,
                    fill_fee,
                ));

                // Update order handle
                if let Some(mut handle) = self.state.orders.get_mut(&cid) {
                    let prev_filled = handle.filled_size;
                    handle.filled_size += fill_size;
                    let prev_notional =
                        handle.avg_fill_price.unwrap_or(0.0) * prev_filled;
                    let new_notional = prev_notional + fill_price * fill_size;
                    if handle.filled_size > 0.0 {
                        handle.avg_fill_price =
                            Some(new_notional / handle.filled_size);
                    }
                    if (handle.filled_size - handle.size).abs() < 1e-12 {
                        handle.state = OrderState::Filled;
                    } else {
                        handle.state = OrderState::PartiallyFilled;
                    }
                }

                let symbol = self
                    .state
                    .orders
                    .get(&cid)
                    .map(|h| h.symbol.clone())
                    .unwrap_or_default();

                let event = OmsEvent::OrderPartialFill(oms_core::Fill {
                    client_id: ClientOrderId(cid),
                    exchange_id: order_digest,
                    symbol,
                    side,
                    price: fill_price,
                    size: fill_size,
                    fee: fill_fee,
                    fee_asset: "USDT".to_string(),
                    liquidity: if is_taker {
                        Liquidity::Taker
                    } else {
                        Liquidity::Maker
                    },
                    ts: Utc::now(),
                });

                let _ = self.event_tx.send(event);
            }

            NadoEvent::PositionChange {
                product_id,
                amount,
                v_quote_amount,
                ..
            } => {
                let amt = parse_x18(&amount).unwrap_or(0.0);
                let pm = self.product_map.try_read();
                let symbol = pm
                    .as_ref()
                    .ok()
                    .and_then(|lock| lock.as_ref())
                    .and_then(|m| m.id_to_canonical(product_id))
                    .unwrap_or_else(|| format!("PERP_{product_id}"));

                if amt.abs() < 1e-12 {
                    self.state.positions.remove(&symbol);
                } else {
                    let side = if amt > 0.0 { Side::Buy } else { Side::Sell };
                    let v_quote = v_quote_amount
                        .as_ref()
                        .and_then(|s| parse_x18(s))
                        .unwrap_or(0.0);
                    let entry_price = if amt.abs() > 1e-18 {
                        (v_quote / amt).abs()
                    } else {
                        0.0
                    };

                    let pos = Position {
                        symbol: symbol.clone(),
                        side,
                        size: amt.abs(),
                        entry_price,
                        unrealized_pnl: 0.0,
                        leverage: 1.0,
                        liquidation_price: None,
                    };
                    self.state.positions.insert(symbol, pos.clone());
                    let _ = self.event_tx.send(OmsEvent::PositionUpdate(pos));
                }
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
                        warn!("nado order {cid} timed out waiting for ack");
                        drop(entry);
                        if let Some(mut h) = self.state.orders.get_mut(&cid) {
                            h.state = OrderState::TimedOut;
                        }
                        let _ = self
                            .event_tx
                            .send(OmsEvent::OrderTimedOut(ClientOrderId(cid)));
                        break; // iterator invalidated after drop+get_mut
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn check_ready(&self) -> Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            bail!(OmsError::NotReady);
        }
        Ok(())
    }

    async fn resolve_product_id(&self, symbol: &str) -> Result<u32> {
        let native = ProductMap::canonical_to_native(symbol)
            .ok_or_else(|| OmsError::Internal(format!("bad symbol format: {symbol}")))?;

        let map = self.product_map.read().await;
        let map = map
            .as_ref()
            .ok_or_else(|| OmsError::Internal("product map not loaded".into()))?;

        map.product_id(&native)
            .ok_or_else(|| OmsError::Internal(format!("unknown product: {native}")).into())
    }
}

// ---------------------------------------------------------------------------
// ExchangeOms trait implementation
// ---------------------------------------------------------------------------

#[async_trait]
impl ExchangeOms for NadoOms {
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

        let product_id = self.resolve_product_id(&req.symbol).await?;
        let cid = self.next_client_id.fetch_add(1, Ordering::Relaxed);

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let nonce = make_nonce(now_ms, rand::random::<u32>() & 0xFFFFF);
        let expiration = (now_ms / 1000) + 86400; // 24 hours

        let nado_order_type = match req.order_type {
            OrderType::Limit {
                tif: TimeInForce::IOC,
                ..
            }
            | OrderType::Market => NadoOrderType::IOC,
            OrderType::Limit {
                tif: TimeInForce::PostOnly,
                ..
            } => NadoOrderType::PostOnly,
            _ => NadoOrderType::Default,
        };
        let appendix = build_appendix(nado_order_type, req.reduce_only, false);

        let price_x18 = match req.order_type {
            OrderType::Limit { price, .. } => f64_to_x18_i128(price),
            OrderType::Market => {
                if req.side == Side::Buy {
                    f64_to_x18_i128(1e12) // very high price for market buy
                } else {
                    1 // very low price for market sell
                }
            }
            _ => bail!(OmsError::UnsupportedOrderType(
                "Nado only supports Limit and Market orders".into()
            )),
        };

        let size_x18 = f64_to_x18_i128(req.size);
        let amount = match req.side {
            Side::Buy => size_x18,
            Side::Sell => -size_x18,
        };

        // Insert as inflight before sending
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
        let _ = self
            .event_tx
            .send(OmsEvent::OrderInflight(ClientOrderId(cid)));

        // Sign and execute (hold read lock for both operations)
        let client = self.client.read().await;

        let signature = client
            .sign_order(product_id, price_x18, amount, expiration, nonce, appendix)
            .await?;

        let order_fields = OrderFields {
            sender: self.sender.clone(),
            price_x18: price_x18.to_string(),
            amount: amount.to_string(),
            expiration: expiration.to_string(),
            nonce: nonce.to_string(),
            appendix: appendix.to_string(),
        };

        let resp = client
            .place_order_execute(product_id, order_fields, signature, cid)
            .await?;
        drop(client);

        // Process response
        if resp.status != "success" {
            if let Some(mut h) = self.state.orders.get_mut(&cid) {
                h.state = OrderState::Rejected;
                h.reject_reason = resp.error.clone();
            }
            let reason = resp.error.unwrap_or_else(|| resp.status.clone());
            let _ = self.event_tx.send(OmsEvent::OrderRejected {
                client_id: ClientOrderId(cid),
                reason: reason.clone(),
            });
            bail!("place_order failed: {reason}");
        }

        // Extract digest from response
        let data: PlaceOrderData = serde_json::from_value(
            resp.data.context("no data in place_order response")?,
        )?;

        if let Some(mut h) = self.state.orders.get_mut(&cid) {
            h.exchange_id = Some(data.digest.clone());
            h.state = OrderState::Accepted;
        }
        self.state.digest_map.insert(data.digest.clone(), cid);

        let _ = self.event_tx.send(OmsEvent::OrderAccepted {
            client_id: ClientOrderId(cid),
            exchange_id: data.digest,
        });

        Ok(ClientOrderId(cid))
    }

    async fn cancel_order(&self, id: &ClientOrderId) -> Result<()> {
        self.check_ready()?;

        // Extract data from order handle without holding guard across await
        let (digest, symbol) = {
            let handle = self
                .state
                .orders
                .get(&id.0)
                .ok_or(OmsError::OrderNotFound(id.0))?;
            let digest = handle
                .exchange_id
                .as_ref()
                .ok_or_else(|| {
                    OmsError::Internal("no digest yet (still inflight)".into())
                })?
                .clone();
            let sym = handle.symbol.clone();
            (digest, sym)
        };

        let product_id = self.resolve_product_id(&symbol).await?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let nonce = make_nonce(now_ms, rand::random::<u32>() & 0xFFFFF);

        let digest_b256: B256 = digest.parse().context("invalid digest hex")?;

        let client = self.client.read().await;
        let signature = client
            .sign_cancellation(&[product_id], &[digest_b256], nonce)
            .await?;

        let tx = CancelTx {
            sender: self.sender.clone(),
            product_ids: vec![product_id],
            digests: vec![digest],
            nonce: nonce.to_string(),
        };

        let resp = client.cancel_orders_execute(tx, signature, None).await?;
        drop(client);

        if resp.status == "success" {
            if let Some(mut h) = self.state.orders.get_mut(&id.0) {
                h.state = OrderState::Cancelled;
            }
            let _ = self.event_tx.send(OmsEvent::OrderCancelled(*id));
        }

        Ok(())
    }

    async fn cancel_all(&self, symbol: Option<&str>) -> Result<()> {
        self.check_ready()?;

        // Collect orders to cancel (release all DashMap guards before async ops)
        let orders_to_cancel: Vec<(u64, String, String)> = self
            .state
            .orders
            .iter()
            .filter_map(|e| {
                let h = e.value();
                if !matches!(
                    h.state,
                    OrderState::Accepted | OrderState::PartiallyFilled
                ) {
                    return None;
                }
                if let Some(sym) = symbol {
                    if h.symbol != sym {
                        return None;
                    }
                }
                let digest = h.exchange_id.as_ref()?.clone();
                Some((*e.key(), digest, h.symbol.clone()))
            })
            .collect();

        if orders_to_cancel.is_empty() {
            return Ok(());
        }

        let mut product_ids = Vec::new();
        let mut digests = Vec::new();
        let mut digest_b256s = Vec::new();
        let mut cids = Vec::new();

        for (cid, digest, sym) in &orders_to_cancel {
            let pid = self.resolve_product_id(sym).await?;
            if let Ok(b) = digest.parse::<B256>() {
                product_ids.push(pid);
                digests.push(digest.clone());
                digest_b256s.push(b);
                cids.push(*cid);
            }
        }

        if digests.is_empty() {
            return Ok(());
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let nonce = make_nonce(now_ms, rand::random::<u32>() & 0xFFFFF);

        let client = self.client.read().await;
        let signature = client
            .sign_cancellation(&product_ids, &digest_b256s, nonce)
            .await?;

        let tx = CancelTx {
            sender: self.sender.clone(),
            product_ids,
            digests,
            nonce: nonce.to_string(),
        };
        client.cancel_orders_execute(tx, signature, None).await?;
        drop(client);

        for cid in cids {
            if let Some(mut h) = self.state.orders.get_mut(&cid) {
                h.state = OrderState::Cancelled;
                let _ = self
                    .event_tx
                    .send(OmsEvent::OrderCancelled(h.client_id));
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

        // Build new order request from existing order + modifications
        let new_req = {
            let handle = self
                .state
                .orders
                .get(&id.0)
                .ok_or(OmsError::OrderNotFound(id.0))?;

            let new_price = req.price.unwrap_or_else(|| match handle.order_type {
                OrderType::Limit { price, .. } => price,
                _ => 0.0,
            });
            let new_size = req.size.unwrap_or(handle.size);

            OrderRequest {
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
            }
        };

        // Cancel old, place new (Nado has no atomic modify)
        self.cancel_order(id).await?;
        self.place_order(new_req).await
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

    fn subscribe(&self) -> broadcast::Receiver<OmsEvent> {
        self.event_tx.subscribe()
    }

    fn round_price(&self, price: f64) -> f64 {
        // TODO: implement Nado-specific price rounding
        price
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn f64_to_x18_i128(v: f64) -> i128 {
    (v * 1e18).round() as i128
}
