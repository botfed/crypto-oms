use anyhow::{Context, Result};
use chrono::Utc;
use crypto_feeds::app_config::{AppConfig, load_perp, load_spot};
use crypto_feeds::market_data::AllMarketData;
use crypto_feeds::symbol_registry::REGISTRY;
use crypto_oms::hyperliquid::{HyperliquidOms, HyperliquidOmsConfig};
use oms_core::*;
use serde::Deserialize;
use std::fmt::Write as FmtWrite;
use std::io::{Write, stdout};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

// ---------------------------------------------------------------------------
// Terminal escape sequences
// ---------------------------------------------------------------------------

const ALT_SCREEN_ON: &str = "\x1B[?1049h";
const ALT_SCREEN_OFF: &str = "\x1B[?1049l";
const CURSOR_HOME: &str = "\x1B[H";
const CLEAR_BELOW: &str = "\x1B[J";
const CURSOR_HIDE: &str = "\x1B[?25l";
const CURSOR_SHOW: &str = "\x1B[?25h";
const ERASE_EOL: &str = "\x1B[K";
const GREEN: &str = "\x1B[32m";
const RED: &str = "\x1B[31m";
const YELLOW: &str = "\x1B[33m";
const DIM: &str = "\x1B[2m";
const RESET: &str = "\x1B[0m";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct DemoConfig {
    hyperliquid: HlConfig,

    #[serde(default)]
    spot: std::collections::HashMap<String, Vec<String>>,
    #[serde(default)]
    perp: std::collections::HashMap<String, Vec<String>>,
    #[serde(default = "default_sample_interval")]
    sample_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
struct HlConfig {
    base_url: Option<String>,
    #[serde(default = "default_poll_interval")]
    poll_interval_ms: u64,
    #[serde(default = "default_inflight_timeout")]
    inflight_timeout_ms: u64,
}

fn default_sample_interval() -> u64 { 10 }
fn default_poll_interval() -> u64 { 3000 }
fn default_inflight_timeout() -> u64 { 5000 }

impl DemoConfig {
    fn to_feeds_config(&self) -> AppConfig {
        AppConfig {
            spot: self.spot.clone(),
            perp: self.perp.clone(),
            sample_interval_ms: self.sample_interval_ms,
            onchain: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Terminal helpers
// ---------------------------------------------------------------------------

fn term_size() -> (usize, usize) {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        unsafe {
            let mut ws = MaybeUninit::<libc::winsize>::zeroed();
            if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, ws.as_mut_ptr()) == 0 {
                let ws = ws.assume_init();
                let cols = if ws.ws_col > 0 { ws.ws_col as usize } else { 160 };
                let rows = if ws.ws_row > 0 { ws.ws_row as usize } else { 50 };
                return (cols, rows);
            }
        }
    }
    (160, 50)
}

fn prepare_frame(raw: &str) -> String {
    let (cols, _) = term_size();
    let mut out = String::with_capacity(raw.len() + raw.lines().count() * 6);
    for line in raw.lines() {
        // Must count visible chars only (skip ANSI escape sequences) for truncation
        let visible_len = visible_width(line);
        if visible_len > cols {
            truncate_visible(line, cols, &mut out);
        } else {
            out.push_str(line);
        }
        out.push_str(ERASE_EOL);
        out.push('\n');
    }
    out
}

/// Count visible (non-ANSI-escape) characters in a string.
fn visible_width(s: &str) -> usize {
    let mut width = 0;
    let mut in_escape = false;
    for c in s.chars() {
        if in_escape {
            if c.is_ascii_alphabetic() {
                in_escape = false;
            }
        } else if c == '\x1B' {
            in_escape = true;
        } else {
            width += 1;
        }
    }
    width
}

/// Truncate string to `max_visible` visible characters, preserving ANSI sequences.
fn truncate_visible(s: &str, max_visible: usize, out: &mut String) {
    let mut visible = 0;
    let mut in_escape = false;
    for c in s.chars() {
        if in_escape {
            out.push(c);
            if c.is_ascii_alphabetic() {
                in_escape = false;
            }
        } else if c == '\x1B' {
            in_escape = true;
            out.push(c);
        } else {
            if visible >= max_visible {
                break;
            }
            out.push(c);
            visible += 1;
        }
    }
    // Always reset in case we truncated mid-color
    out.push_str(RESET);
}

async fn flush_str(s: String) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let mut out = stdout().lock();
        write!(out, "{}", s)?;
        out.flush()?;
        Ok(())
    })
    .await?
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

async fn run_display(
    oms: Arc<HyperliquidOms>,
    market_data: Arc<AllMarketData>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    flush_str(format!("{ALT_SCREEN_ON}{CURSOR_HIDE}")).await?;

    let start = Instant::now();
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result = loop {
        tokio::select! {
            _ = &mut shutdown_fut => break Ok(()),
            _ = interval.tick() => {
                let oms = Arc::clone(&oms);
                let md = Arc::clone(&market_data);
                let frame = tokio::task::spawn_blocking(move || {
                    render_frame(&oms, &md, start)
                }).await?;
                flush_str(frame).await?;
            }
        }
    };

    flush_str(format!("{CURSOR_SHOW}{ALT_SCREEN_OFF}")).await?;
    result
}

fn render_frame(
    oms: &HyperliquidOms,
    md: &AllMarketData,
    start: Instant,
) -> String {
    let elapsed = start.elapsed().as_secs();
    let h = elapsed / 3600;
    let m = (elapsed % 3600) / 60;
    let s = elapsed % 60;

    let mut buf = String::with_capacity(8192);

    // Header
    let ready_str = if oms.is_ready() {
        format!("{GREEN}READY{RESET}")
    } else {
        format!("{YELLOW}INITIALIZING{RESET}")
    };
    let _ = writeln!(
        buf,
        "  Hyperliquid OMS Demo   {ready_str}   uptime: {h:02}:{m:02}:{s:02}   {DIM}{}{RESET}",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
    );
    let _ = writeln!(
        buf,
        "  Account: {DIM}{}{RESET}   Agent: {DIM}{}{RESET}",
        oms.client().account_address,
        oms.client().agent_address,
    );

    // Connection diagnostics
    let ws_str = if oms.diag.ws_connected.load(std::sync::atomic::Ordering::Relaxed) {
        format!("{GREEN}connected{RESET}")
    } else {
        format!("{RED}disconnected{RESET}")
    };
    let poll_age = oms.diag.last_poll_at.lock()
        .map(|t| format!("{:.1}s ago", t.elapsed().as_secs_f64()))
        .unwrap_or_else(|| "never".into());
    let poll_n = oms.diag.poll_count.load(std::sync::atomic::Ordering::Relaxed);
    let ws_msgs = oms.diag.ws_msg_count.load(std::sync::atomic::Ordering::Relaxed);
    let _ = writeln!(
        buf,
        "  WS: {ws_str}  msgs={ws_msgs}   REST polls: {poll_n} (last: {poll_age})",
    );
    let _ = writeln!(buf);

    // Account / balances
    let balances = oms.balances();
    if let Some(b) = balances.iter().find(|b| b.asset == "USDC") {
        let _ = writeln!(
            buf,
            "  Equity={GREEN}{:>12.2}{RESET}  Available={:>12.2}  Hold={:>12.2}",
            b.equity, b.available, b.locked,
        );
    } else {
        let _ = writeln!(buf, "  Account: {DIM}no data{RESET}");
    }
    let _ = writeln!(buf);

    // Positions table
    let _ = writeln!(buf, "  {}", section_header("Positions"));
    let positions = oms.positions();
    if positions.is_empty() {
        let _ = writeln!(buf, "  {DIM}  (none){RESET}");
    } else {
        let _ = writeln!(
            buf,
            "  {:<20} {:>6} {:>14} {:>14} {:>14} {:>8} {:>14}",
            "Symbol", "Side", "Size", "Entry", "uPnL", "Lever", "Liq Price",
        );
        let _ = writeln!(
            buf,
            "  {:<20} {:>6} {:>14} {:>14} {:>14} {:>8} {:>14}",
            "------", "----", "--------", "--------", "--------", "------", "---------",
        );
        for p in &positions {
            let side_str = match p.side {
                Side::Buy => format!("{GREEN}LONG{RESET}"),
                Side::Sell => format!("{RED}SHORT{RESET}"),
            };
            let pnl_str = if p.unrealized_pnl >= 0.0 {
                format!("{GREEN}{:>14.2}{RESET}", p.unrealized_pnl)
            } else {
                format!("{RED}{:>14.2}{RESET}", p.unrealized_pnl)
            };
            let liq = p
                .liquidation_price
                .map(|v| format!("{v:>14.2}"))
                .unwrap_or_else(|| format!("{:>14}", "-"));
            let _ = writeln!(
                buf,
                "  {:<20} {:>6} {:>14.4} {:>14.2} {} {:>7.0}x {:>14}",
                p.symbol, side_str, p.size, p.entry_price, pnl_str, p.leverage, liq,
            );
        }
    }
    let _ = writeln!(buf);

    // Open orders table
    let _ = writeln!(buf, "  {}", section_header("Open Orders"));
    let orders = oms.open_orders(None);
    if orders.is_empty() {
        let _ = writeln!(buf, "  {DIM}  (none){RESET}");
    } else {
        let _ = writeln!(
            buf,
            "  {:<8} {:<20} {:>6} {:>10} {:>14} {:>14} {:>10} {:>12}",
            "ID", "Symbol", "Side", "State", "Price", "Size", "Filled", "Type",
        );
        let _ = writeln!(
            buf,
            "  {:<8} {:<20} {:>6} {:>10} {:>14} {:>14} {:>10} {:>12}",
            "---", "------", "----", "-----", "-----", "----", "------", "----",
        );
        for o in &orders {
            let side_str = match o.side {
                Side::Buy => format!("{GREEN}BUY{RESET}"),
                Side::Sell => format!("{RED}SELL{RESET}"),
            };
            let state_str = match o.state {
                OrderState::Inflight => format!("{YELLOW}inflight{RESET}"),
                OrderState::Accepted => "open".to_string(),
                OrderState::PartiallyFilled => format!("{YELLOW}partial{RESET}"),
                _ => format!("{:?}", o.state),
            };
            let (price_str, type_str) = match o.order_type {
                OrderType::Limit { price, tif } => {
                    let tif_s = match tif {
                        TimeInForce::GTC => "LMT",
                        TimeInForce::IOC => "IOC",
                        TimeInForce::PostOnly => "POST",
                    };
                    (format!("{price:>14.4}"), tif_s.to_string())
                }
                OrderType::Market => ("MKT".to_string(), "MKT".to_string()),
                OrderType::StopLimit { price, trigger, tif: _ } => {
                    (format!("{price:>14.4}"), format!("SL@{trigger:.1}"))
                }
                OrderType::StopMarket { trigger } => {
                    (format!("{trigger:>14.4}"), "SM".to_string())
                }
            };
            let _ = writeln!(
                buf,
                "  {:<8} {:<20} {:>6} {:>10} {} {:>14.4} {:>10.4} {:>12}",
                o.client_id.0, o.symbol, side_str, state_str, price_str, o.size, o.filled_size, type_str,
            );
        }
    }
    let _ = writeln!(buf);

    // BBO reference prices from feeds
    let _ = writeln!(buf, "  {}", section_header("Reference Prices (feeds)"));
    let _ = writeln!(
        buf,
        "  {:<10} {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>6}",
        "Exchange", "Symbol", "Bid", "BidQty", "Ask", "AskQty", "Age(ms)", "",
    );
    let _ = writeln!(
        buf,
        "  {:<10} {:<20} {:>14} {:>14} {:>10} {:>14} {:>10} {:>6}",
        "--------", "------", "---", "------", "---", "------", "------", "",
    );

    let now = Utc::now();

    let exchanges: &[(&str, &crypto_feeds::MarketDataCollection)] = &[
        ("Binance", &md.binance),
        ("Coinbase", &md.coinbase),
        ("Bybit", &md.bybit),
        ("Kraken", &md.kraken),
        ("OKX", &md.okx),
        ("KuCoin", &md.kucoin),
    ];

    for (name, coll) in exchanges {
        for id in 0..100 {
            if let Some(data) = coll.latest(&id) {
                let sym = REGISTRY.get_symbol(id).unwrap_or("?");
                let age = data
                    .exchange_ts
                    .map(|t| format!("{}", (now - t).num_milliseconds().max(0)))
                    .unwrap_or_else(|| "-".into());
                let _ = writeln!(
                    buf,
                    "  {:<10} {:<20} {:>14} {:>10} {:>14} {:>10} {:>6}",
                    name,
                    sym,
                    fmt_price(data.bid),
                    fmt_qty(data.bid_qty),
                    fmt_price(data.ask),
                    fmt_qty(data.ask_qty),
                    age,
                );
            }
        }
    }

    // Log section — fill remaining terminal rows
    let (_, rows) = term_size();
    let used = buf.lines().count();
    let remaining = rows.saturating_sub(used).saturating_sub(2);
    let logs = oms.diag.log_lines.lock();
    if !logs.is_empty() {
        let _ = writeln!(buf, "\n  {}", section_header("Log"));
        let skip = logs.len().saturating_sub(remaining);
        for line in logs.iter().skip(skip) {
            let _ = writeln!(buf, "  {DIM}{line}{RESET}");
        }
    }

    let frame = prepare_frame(&buf);
    format!("{CURSOR_HOME}{frame}{CLEAR_BELOW}")
}

fn section_header(title: &str) -> String {
    format!("--- {title} ---")
}

fn fmt_price(v: Option<f64>) -> String {
    v.map(|x| {
        if x >= 1000.0 {
            format!("{x:.2}")
        } else if x >= 1.0 {
            format!("{x:.4}")
        } else {
            format!("{x:.6}")
        }
    })
    .unwrap_or_else(|| "-".into())
}

fn fmt_qty(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.4}")).unwrap_or_else(|| "-".into())
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env if present
    let _ = dotenv::dotenv();

    // Load config
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/hl_demo.yaml".to_string());

    let contents = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config: {config_path}"))?;
    let config: DemoConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse config: {config_path}"))?;

    // HL credentials from env
    let private_key = std::env::var("HL_PRIVATE_KEY")
        .context("HL_PRIVATE_KEY env var not set")?;
    let account_address = std::env::var("HL_ACCOUNT_ADDRESS")
        .context("HL_ACCOUNT_ADDRESS env var not set (parent EOA address)")?;

    let shutdown = Arc::new(Notify::new());

    // Ctrl-C handler
    let sd = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        sd.notify_waiters();
    });

    // Start crypto-feeds
    let feeds_config = config.to_feeds_config();
    let market_data = Arc::new(AllMarketData::new());
    let mut handles = Vec::new();
    load_spot(&mut handles, &feeds_config, &market_data, &shutdown)?;
    load_perp(&mut handles, &feeds_config, &market_data, &shutdown)?;

    // Start HL OMS
    let oms_config = HyperliquidOmsConfig {
        private_key,
        account_address: account_address.clone(),
        base_url: config.hyperliquid.base_url,
        poll_interval: Duration::from_millis(config.hyperliquid.poll_interval_ms),
        inflight_timeout: Duration::from_millis(config.hyperliquid.inflight_timeout_ms),
    };
    let oms = HyperliquidOms::new(oms_config)?;
    oms.start();

    // Run display
    run_display(oms, market_data, shutdown.clone()).await?;

    Ok(())
}
