use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use tracing::warn;

use crate::traits::OmsEvent;
use crate::types::*;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct StateTrackerConfig {
    /// How long to wait for a cancel ack before restoring to Accepted (default 5s)
    pub stray_order_age: Duration,
    /// How long an Inflight order can sit without ack before TimedOut (default 5s)
    pub inflight_timeout: Duration,
    /// Max terminal-state orders to keep before pruning oldest (default 1000)
    pub max_terminal_orders: usize,
}

impl Default for StateTrackerConfig {
    fn default() -> Self {
        Self {
            stray_order_age: Duration::from_secs(5),
            inflight_timeout: Duration::from_secs(5),
            max_terminal_orders: 1000,
        }
    }
}

// ---------------------------------------------------------------------------
// OmsStateTracker — generic, single-threaded local state reconstruction
// ---------------------------------------------------------------------------

pub struct OmsStateTracker {
    orders: HashMap<u64, OrderHandle>,
    /// exchange_id → client_id reverse lookup
    oid_map: HashMap<String, u64>,
    positions: HashMap<String, Position>,
    balances: HashMap<String, Balance>,
    /// Dedup fills by fill_id — gateway produces unique IDs, tracker enforces uniqueness
    seen_fills: HashSet<String>,
    config: StateTrackerConfig,
    /// Monotonically tracks whether we're connected
    connected: bool,
    ready: bool,
}

impl OmsStateTracker {
    pub fn new(config: StateTrackerConfig) -> Self {
        Self {
            orders: HashMap::new(),
            oid_map: HashMap::new(),
            positions: HashMap::new(),
            balances: HashMap::new(),
            seen_fills: HashSet::new(),
            config,
            connected: false,
            ready: false,
        }
    }

    // -----------------------------------------------------------------------
    // Engine-direct mutations (no channel round-trip)
    // -----------------------------------------------------------------------

    /// Engine calls this immediately when preparing a new order.
    /// Returns the inserted handle for the caller's convenience.
    pub fn insert_inflight(&mut self, handle: OrderHandle) {
        let cid = handle.client_id.0;
        self.orders.insert(cid, handle);
    }

    /// Engine calls this when it decides to cancel an order.
    /// Returns false if the order doesn't exist or is already terminal.
    pub fn mark_cancelling(&mut self, cid: ClientOrderId) -> bool {
        if let Some(h) = self.orders.get_mut(&cid.0) {
            match h.state {
                OrderState::Accepted
                | OrderState::PartiallyFilled
                | OrderState::Inflight => {
                    h.state = OrderState::Cancelling;
                    h.last_modified = Some(Instant::now());
                    true
                }
                // Already cancelling or terminal — no-op
                _ => false,
            }
        } else {
            false
        }
    }

    // -----------------------------------------------------------------------
    // Event application — called from drain loop
    // -----------------------------------------------------------------------

    /// Apply a single OmsEvent to local state. Returns true if state changed.
    pub fn apply_event(&mut self, event: &OmsEvent) -> bool {
        match event {
            OmsEvent::Ready => {
                self.ready = true;
                true
            }

            OmsEvent::Disconnected => {
                self.connected = false;
                self.ready = false;
                true
            }

            OmsEvent::Reconnected => {
                self.connected = true;
                // ready will be set by next Ready event after re-snapshot
                true
            }

            OmsEvent::OrderInflight(cid) => {
                // Gateway-produced inflight (if engine didn't call insert_inflight directly)
                // Only insert if we don't already have it (engine may have pre-inserted)
                if !self.orders.contains_key(&cid.0) {
                    warn!("OrderInflight for unknown cid={} — gateway produced without engine insert", cid.0);
                }
                false
            }

            OmsEvent::OrderAccepted { client_id, exchange_id } => {
                if let Some(h) = self.orders.get_mut(&client_id.0) {
                    // Only promote from non-terminal states
                    if matches!(h.state, OrderState::Inflight | OrderState::Accepted | OrderState::PartiallyFilled) {
                        h.state = if h.filled_size > 0.0 {
                            OrderState::PartiallyFilled
                        } else {
                            OrderState::Accepted
                        };
                        h.exchange_id = Some(exchange_id.clone());
                        h.last_modified = Some(Instant::now());
                        self.oid_map.insert(exchange_id.clone(), client_id.0);
                        return true;
                    }
                }
                false
            }

            OmsEvent::OrderPartialFill(fill) => {
                if !self.seen_fills.insert(fill.fill_id.clone()) {
                    return false; // duplicate fill
                }
                if let Some(h) = self.orders.get_mut(&fill.client_id.0) {
                    if is_terminal(h.state) {
                        return false;
                    }
                    h.filled_size += fill.size;
                    // Update avg fill price incrementally
                    let prev_value = h.avg_fill_price.unwrap_or(0.0) * (h.filled_size - fill.size);
                    let fill_value = fill.price * fill.size;
                    h.avg_fill_price = Some((prev_value + fill_value) / h.filled_size);
                    h.state = OrderState::PartiallyFilled;
                    h.exchange_id.get_or_insert_with(|| fill.exchange_id.clone());
                    h.last_modified = Some(Instant::now());
                    if h.exchange_id.is_some() {
                        self.oid_map.entry(fill.exchange_id.clone()).or_insert(fill.client_id.0);
                    }
                    return true;
                }
                false
            }

            OmsEvent::OrderFilled(fill) => {
                if let Some(h) = self.orders.get_mut(&fill.client_id.0) {
                    if h.state == OrderState::Filled {
                        return false; // already filled
                    }
                    // OrderFilled is a state notification, not a trade.
                    // Real fill sizes come through OrderPartialFill events.
                    // Only update filled_size if we haven't seen any partial fills yet.
                    if h.filled_size == 0.0 && fill.size > 0.0 {
                        h.filled_size = fill.size.min(h.size);
                        h.avg_fill_price = Some(fill.price);
                    }
                    h.state = OrderState::Filled;
                    h.exchange_id.get_or_insert_with(|| fill.exchange_id.clone());
                    h.last_modified = Some(Instant::now());
                    return true;
                }
                false
            }

            OmsEvent::OrderCancelling(cid) => {
                // Gateway-produced cancelling (from mark_cancelling in gateway path)
                // Engine may have already called mark_cancelling() directly
                if let Some(h) = self.orders.get_mut(&cid.0) {
                    if matches!(h.state, OrderState::Accepted | OrderState::PartiallyFilled | OrderState::Inflight) {
                        h.state = OrderState::Cancelling;
                        h.last_modified = Some(Instant::now());
                        return true;
                    }
                }
                false
            }

            OmsEvent::OrderCancelled(cid) => {
                if let Some(h) = self.orders.get_mut(&cid.0) {
                    if is_terminal(h.state) && h.state != OrderState::Cancelling {
                        return false;
                    }
                    h.state = OrderState::Cancelled;
                    h.last_modified = Some(Instant::now());
                    return true;
                }
                false
            }

            OmsEvent::OrderRejected { client_id, reason } => {
                if let Some(h) = self.orders.get_mut(&client_id.0) {
                    if is_terminal(h.state) {
                        return false;
                    }
                    h.state = OrderState::Rejected;
                    h.reject_reason = Some(reason.clone());
                    h.last_modified = Some(Instant::now());
                    return true;
                }
                false
            }

            OmsEvent::OrderTimedOut(cid) => {
                if let Some(h) = self.orders.get_mut(&cid.0) {
                    if h.state == OrderState::Inflight {
                        h.state = OrderState::TimedOut;
                        h.last_modified = Some(Instant::now());
                        return true;
                    }
                }
                false
            }

            OmsEvent::PositionUpdate(pos) => {
                self.positions.insert(pos.symbol.clone(), pos.clone());
                true
            }

            OmsEvent::BalanceUpdate(bal) => {
                self.balances.insert(bal.asset.clone(), bal.clone());
                true
            }

            OmsEvent::Snapshot { orders, positions, balances } => {
                self.apply_snapshot(orders, positions, balances);
                true
            }
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot reconciliation — mirrors existing HL logic, fully generic
    // -----------------------------------------------------------------------

    fn apply_snapshot(
        &mut self,
        snap_orders: &[OrderHandle],
        snap_positions: &[Position],
        snap_balances: &[Balance],
    ) {
        let now = Instant::now();

        // Build set of exchange_ids present in snapshot
        let mut snap_eids: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // Phase 1: Reconcile orders that ARE in the snapshot
        for snap_order in snap_orders {
            let eid = match &snap_order.exchange_id {
                Some(eid) => eid.clone(),
                None => continue,
            };
            snap_eids.insert(eid.clone());

            let active_state = if snap_order.filled_size > 0.0 {
                OrderState::PartiallyFilled
            } else {
                OrderState::Accepted
            };

            // Do we already track this order?
            if let Some(&cid) = self.oid_map.get(&eid) {
                if let Some(h) = self.orders.get_mut(&cid) {
                    // Update fill info from snapshot
                    if snap_order.filled_size > h.filled_size {
                        h.filled_size = snap_order.filled_size;
                    }
                    h.size = snap_order.size;

                    let age_exceeded = h.last_modified
                        .map(|t| now.duration_since(t) >= self.config.stray_order_age)
                        .unwrap_or(true);

                    match h.state {
                        // Already active — just update fill info / state
                        OrderState::Accepted | OrderState::PartiallyFilled => {
                            h.state = active_state;
                        }
                        // Inflight — exchange has it, promote
                        OrderState::Inflight => {
                            h.state = active_state;
                            h.exchange_id = Some(eid.clone());
                            h.last_modified = Some(now);
                        }
                        // Cancelling — only restore if stray age exceeded
                        OrderState::Cancelling => {
                            if age_exceeded {
                                warn!(
                                    "cancel failed for cid={}, restoring to {:?}",
                                    cid, active_state
                                );
                                h.state = active_state;
                                h.last_modified = Some(now);
                            }
                            // else: cancel still processing, leave as Cancelling
                        }
                        // Cancelled — zombie order still on exchange
                        OrderState::Cancelled => {
                            if age_exceeded {
                                warn!(
                                    "zombie order cid={} still on exchange, restoring to {:?}",
                                    cid, active_state
                                );
                                h.state = active_state;
                                h.last_modified = Some(now);
                            }
                        }
                        // Other terminal states — exchange says open, trust exchange
                        _ => {
                            h.state = active_state;
                            h.last_modified = Some(now);
                        }
                    }
                }
            } else {
                // Unknown order — appeared on exchange (from another session or pre-restart)
                let cid = snap_order.client_id.0;
                let mut h = snap_order.clone();
                h.state = active_state;
                h.last_modified = Some(now);
                self.oid_map.insert(eid, cid);
                self.orders.insert(cid, h);
            }
        }

        // Phase 2: Orders we track locally that are NOT in the snapshot
        let mut disappeared = Vec::new();
        for (&cid, h) in &self.orders {
            if matches!(
                h.state,
                OrderState::Accepted | OrderState::PartiallyFilled | OrderState::Cancelling
            ) {
                if let Some(eid) = &h.exchange_id {
                    if !snap_eids.contains(eid) {
                        disappeared.push(cid);
                    }
                }
            }
            // Inflight orders with no exchange_id: keep — too new for snapshot
        }

        for cid in disappeared {
            if let Some(h) = self.orders.get_mut(&cid) {
                if h.filled_size > 0.0 {
                    h.state = OrderState::Filled;
                } else {
                    h.state = OrderState::Cancelled;
                }
                h.last_modified = Some(now);
            }
        }

        // Phase 3: Clean up oid_map for terminal orders
        self.oid_map.retain(|_eid, cid| {
            self.orders
                .get(cid)
                .map(|h| !is_terminal(h.state))
                .unwrap_or(false)
        });

        // Phase 4: Positions and balances — full replace
        self.positions.clear();
        for p in snap_positions {
            self.positions.insert(p.symbol.clone(), p.clone());
        }

        self.balances.clear();
        for b in snap_balances {
            self.balances.insert(b.asset.clone(), b.clone());
        }

        // Phase 5: Prune old terminal orders
        self.prune_terminal_orders();
    }

    // -----------------------------------------------------------------------
    // Inflight timeout check — replaces the watchdog task
    // -----------------------------------------------------------------------

    /// Check for inflight orders that exceeded timeout. Returns timed-out cids.
    pub fn check_inflight_timeouts(&mut self) -> Vec<ClientOrderId> {
        let now = Instant::now();
        let mut timed_out = Vec::new();

        for (&cid, h) in &mut self.orders {
            if h.state == OrderState::Inflight {
                if let Some(submitted) = h.submitted_at {
                    if now.duration_since(submitted) >= self.config.inflight_timeout {
                        h.state = OrderState::TimedOut;
                        h.last_modified = Some(now);
                        timed_out.push(ClientOrderId(cid));
                    }
                }
            }
        }

        timed_out
    }

    // -----------------------------------------------------------------------
    // Queries — plain HashMap reads, zero overhead
    // -----------------------------------------------------------------------

    #[inline]
    pub fn get_order(&self, cid: ClientOrderId) -> Option<&OrderHandle> {
        self.orders.get(&cid.0)
    }

    #[inline]
    pub fn get_order_mut(&mut self, cid: ClientOrderId) -> Option<&mut OrderHandle> {
        self.orders.get_mut(&cid.0)
    }

    pub fn get_order_by_exchange_id(&self, eid: &str) -> Option<&OrderHandle> {
        self.oid_map.get(eid).and_then(|cid| self.orders.get(cid))
    }

    pub fn open_orders(&self, symbol: Option<&str>) -> Vec<&OrderHandle> {
        self.orders
            .values()
            .filter(|h| {
                matches!(
                    h.state,
                    OrderState::Inflight
                        | OrderState::Accepted
                        | OrderState::PartiallyFilled
                        | OrderState::Cancelling
                ) && symbol.map_or(true, |s| h.symbol == s)
            })
            .collect()
    }

    pub fn inflight_orders(&self, symbol: Option<&str>) -> Vec<&OrderHandle> {
        self.orders
            .values()
            .filter(|h| {
                h.state == OrderState::Inflight
                    && symbol.map_or(true, |s| h.symbol == s)
            })
            .collect()
    }

    pub fn positions(&self) -> &HashMap<String, Position> {
        &self.positions
    }

    pub fn balances(&self) -> &HashMap<String, Balance> {
        &self.balances
    }

    pub fn is_ready(&self) -> bool {
        self.ready
    }

    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Number of tracked orders (all states)
    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn prune_terminal_orders(&mut self) {
        let terminal_count = self
            .orders
            .values()
            .filter(|h| is_terminal(h.state))
            .count();

        if terminal_count > self.config.max_terminal_orders {
            // Collect terminal orders sorted by last_modified (oldest first)
            let mut terminal: Vec<(u64, Option<Instant>)> = self
                .orders
                .iter()
                .filter(|(_, h)| is_terminal(h.state))
                .map(|(&cid, h)| (cid, h.last_modified))
                .collect();
            terminal.sort_by_key(|(_, t)| *t);

            let to_remove = terminal_count - self.config.max_terminal_orders;
            for (cid, _) in terminal.into_iter().take(to_remove) {
                self.orders.remove(&cid);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[inline]
fn is_terminal(state: OrderState) -> bool {
    matches!(
        state,
        OrderState::Filled
            | OrderState::Cancelled
            | OrderState::Rejected
            | OrderState::TimedOut
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
