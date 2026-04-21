use anyhow::{Context, Result};
use crypto_oms::hibachi::{HibachiOms, HibachiOmsConfig};
use oms_core::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenv::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let api_key = std::env::var("HIBACHI_API_KEY")
        .context("HIBACHI_API_KEY env var not set")?;
    let private_key = std::env::var("HIBACHI_PRIVATE_KEY")
        .context("HIBACHI_PRIVATE_KEY env var not set")?;
    let account_id: u64 = std::env::var("HIBACHI_ACCOUNT_ID")
        .context("HIBACHI_ACCOUNT_ID env var not set")?
        .parse()
        .context("HIBACHI_ACCOUNT_ID must be a number")?;

    let shutdown = Arc::new(Notify::new());
    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        sd.notify_waiters();
    });

    let config = HibachiOmsConfig {
        api_key,
        private_key,
        account_id,
        api_url: std::env::var("HIBACHI_API_URL").ok(),
        data_api_url: std::env::var("HIBACHI_DATA_API_URL").ok(),
        poll_interval: Duration::from_secs(3),
        inflight_timeout: Duration::from_secs(5),
        stray_order_age: Duration::from_secs(5),
        max_fees_percent: 0.001,
    };

    let oms = HibachiOms::new(config)?;
    oms.start();

    info!("waiting for OMS to be ready...");
    oms.wait_ready().await?;
    info!("OMS ready!");

    // Print initial state
    let positions = oms.positions();
    info!("positions: {}", positions.len());
    for p in &positions {
        info!("  {} {:?} size={} entry={:.2} upnl={:.2}",
            p.symbol, p.side, p.size, p.entry_price, p.unrealized_pnl);
    }

    let balances = oms.balances();
    for b in &balances {
        info!("  {} available={:.2} locked={:.2} equity={:.2}",
            b.asset, b.available, b.locked, b.equity);
    }

    let orders = oms.open_orders(None);
    info!("open orders: {}", orders.len());
    for o in &orders {
        info!("  cid={} {} {:?} {:?} size={} filled={}",
            o.client_id.0, o.symbol, o.side, o.state, o.size, o.filled_size);
    }

    // Drain events until shutdown
    let rx = oms.event_receiver();
    loop {
        tokio::select! {
            _ = shutdown.notified() => break,
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                while let Ok(event) = rx.try_recv() {
                    match &event {
                        OmsEvent::Ready => info!("EVENT: Ready"),
                        OmsEvent::OrderInflight(id) => info!("EVENT: OrderInflight({})", id.0),
                        OmsEvent::OrderAccepted { client_id, exchange_id } =>
                            info!("EVENT: OrderAccepted cid={} eid={}", client_id.0, exchange_id),
                        OmsEvent::OrderPartialFill(f) =>
                            info!("EVENT: PartialFill cid={} px={} sz={}", f.client_id.0, f.price, f.size),
                        OmsEvent::OrderFilled(f) =>
                            info!("EVENT: Filled cid={} px={} sz={}", f.client_id.0, f.price, f.size),
                        OmsEvent::OrderCancelling(id) => info!("EVENT: Cancelling({})", id.0),
                        OmsEvent::OrderCancelled(id) => info!("EVENT: Cancelled({})", id.0),
                        OmsEvent::OrderRejected { client_id, reason } =>
                            warn!("EVENT: Rejected cid={} reason={}", client_id.0, reason),
                        OmsEvent::OrderTimedOut(id) => warn!("EVENT: TimedOut({})", id.0),
                        OmsEvent::PositionUpdate(p) =>
                            info!("EVENT: PositionUpdate {} {:?} size={}", p.symbol, p.side, p.size),
                        OmsEvent::BalanceUpdate(b) =>
                            info!("EVENT: BalanceUpdate {} eq={:.2}", b.asset, b.equity),
                        OmsEvent::Snapshot { orders, positions, balances } =>
                            info!("EVENT: Snapshot {} orders, {} positions, {} balances",
                                orders.len(), positions.len(), balances.len()),
                        OmsEvent::Disconnected => warn!("EVENT: Disconnected"),
                        OmsEvent::Reconnected => info!("EVENT: Reconnected"),
                    }
                }
            }
        }
    }

    info!("shutting down...");
    oms.shutdown();
    Ok(())
}
