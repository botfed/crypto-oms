use anyhow::{Context, Result};
use chrono::Utc;
use crypto_feeds::app_config::{AppConfig, load_perp, load_spot};
use crypto_feeds::market_data::ClockCorrectionConfig;
use crypto_feeds::market_data::AllMarketData;
use crypto_feeds::symbol_registry::REGISTRY;
use crypto_oms::nado::{NadoDiagnostics, NadoOms, NadoOmsConfig};
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
    nado: NadoConfig,

    #[serde(default)]
    spot: std::collections::HashMap<String, Vec<String>>,
    #[serde(default)]
    perp: std::collections::HashMap<String, Vec<String>>,
    #[serde(default = "default_sample_interval")]
    sample_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
struct NadoConfig {
    #[serde(default)]
    gateway_url: Option<String>,
    #[serde(default)]
    subscriptions_url: Option<String>,
    #[serde(default = "default_subaccount")]
    subaccount_name: String,
    #[serde(default = "default_poll_interval")]
    poll_interval_ms: u64,
    #[serde(default = "default_inflight_timeout")]
    inflight_timeout_ms: u64,
}

fn default_sample_interval() -> u64 {
    10
}
fn default_poll_interval() -> u64 {
    3000
}
fn default_inflight_timeout() -> u64 {
    5000
}
fn default_subaccount() -> String {
    "default".to_string()
}

impl DemoConfig {
    fn to_feeds_config(&self) -> AppConfig {
        AppConfig {
            spot: self.spot.clone(),
            perp: self.perp.clone(),
            trades: Default::default(),
            sample_interval_ms: self.sample_interval_ms,
            onchain: None,
            fair_price: Default::default(),
            vol_models: None,
            clock_correction: ClockCorrectionConfig::default(),
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

struct DisplayCtx {
    sender: String,
    signer_address: String,
    account_address: String,
}

async fn run_display(
    oms: Arc<NadoOms>,
    diag: Arc<NadoDiagnostics>,
    ctx: DisplayCtx,
    market_data: Arc<AllMarketData>,
    shutdown: Arc<Notify>,
) -> Result<()> {
    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    flush_str(format!("{ALT_SCREEN_ON}{CURSOR_HIDE}")).await?;

    let start = Instant::now();
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let ctx = Arc::new(ctx);
    let result = loop {
        tokio::select! {
            _ = &mut shutdown_fut => break Ok(()),
            _ = interval.tick() => {
                let oms = Arc::clone(&oms);
                let diag = Arc::clone(&diag);
                let md = Arc::clone(&market_data);
                let ctx = Arc::clone(&ctx);
                let frame = tokio::task::spawn_blocking(move || {
                    render_frame(&oms, &diag, &ctx, &md, start)
                }).await?;
                flush_str(frame).await?;
            }
        }
    };

    flush_str(format!("{CURSOR_SHOW}{ALT_SCREEN_OFF}")).await?;
    result
}

fn render_frame(
    oms: &NadoOms,
    diag: &NadoDiagnostics,
    ctx: &DisplayCtx,
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
        "  Nado OMS Demo   {ready_str}   uptime: {h:02}:{m:02}:{s:02}   {DIM}{}{RESET}",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
    );
    let is_main = ctx.signer_address.eq_ignore_ascii_case(&ctx.account_address);
    let signer_label = if is_main {
        format!("{GREEN}main wallet{RESET}")
    } else {
        format!("{YELLOW}linked signer{RESET}")
    };
    let _ = writeln!(
        buf,
        "  Account: {DIM}{}{RESET}   Signer: {DIM}{}{RESET} ({signer_label})",
        ctx.account_address, ctx.signer_address,
    );
    let _ = writeln!(
        buf,
        "  Sender:  {DIM}{}{RESET}",
        ctx.sender,
    );

    // Connection diagnostics
    let ws_str = if diag.sub_ws_connected.load(std::sync::atomic::Ordering::Relaxed) {
        format!("{GREEN}connected{RESET}")
    } else {
        format!("{RED}disconnected{RESET}")
    };
    let poll_age = diag
        .last_poll_at
        .lock()
        .map(|t| format!("{:.1}s ago", t.elapsed().as_secs_f64()))
        .unwrap_or_else(|| "never".into());
    let poll_n = diag.poll_count.load(std::sync::atomic::Ordering::Relaxed);
    let ws_msgs = diag
        .sub_msg_count
        .load(std::sync::atomic::Ordering::Relaxed);
    let _ = writeln!(
        buf,
        "  Sub WS: {ws_str}  msgs={ws_msgs}   REST polls: {poll_n} (last: {poll_age})",
    );
    let _ = writeln!(buf);

    // Account / balances
    let balances = oms.balances();
    if balances.is_empty() {
        let _ = writeln!(buf, "  Account: {DIM}no data{RESET}");
    } else {
        for b in &balances {
            let _ = writeln!(
                buf,
                "  {}: Equity={GREEN}{:>12.2}{RESET}  Available={:>12.2}  Hold={:>12.2}",
                b.asset, b.equity, b.available, b.locked,
            );
        }
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
            "  {:<20} {:>6} {:>14} {:>14} {:>14}",
            "Symbol", "Side", "Size", "Entry", "uPnL",
        );
        let _ = writeln!(
            buf,
            "  {:<20} {:>6} {:>14} {:>14} {:>14}",
            "------", "----", "--------", "--------", "--------",
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
            let _ = writeln!(
                buf,
                "  {:<20} {:>6} {:>14.4} {:>14.2} {}",
                p.symbol, side_str, p.size, p.entry_price, pnl_str,
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
                OrderType::StopLimit {
                    price, trigger, ..
                } => (format!("{price:>14.4}"), format!("SL@{trigger:.1}")),
                OrderType::StopMarket { trigger } => {
                    (format!("{trigger:>14.4}"), "SM".to_string())
                }
            };
            let _ = writeln!(
                buf,
                "  {:<8} {:<20} {:>6} {:>10} {} {:>14.4} {:>10.4} {:>12}",
                o.client_id.0,
                o.symbol,
                side_str,
                state_str,
                price_str,
                o.size,
                o.filled_size,
                type_str,
            );
        }
    }
    let _ = writeln!(buf);

    // BBO reference prices from feeds
    let _ = writeln!(buf, "  {}", section_header("Reference Prices (feeds)"));
    //            Exch  Symbol          Bid     BidQty          Ask     AskQty  Age
    let _ = writeln!(
        buf,
        "  {:<5} {:<14} {:>12} {:>10} {:>12} {:>10} {:>7}",
        "Exch", "Symbol", "Bid", "BidQty", "Ask", "AskQty", "Age ms",
    );
    let _ = writeln!(
        buf,
        "  {:<5} {:<14} {:>12} {:>10} {:>12} {:>10} {:>7}",
        "----", "-----------", "----------", "--------", "----------", "--------", "------",
    );

    let now = Utc::now();

    let exchanges: &[(&str, &crypto_feeds::MarketDataCollection)] = &[
        ("Bince", &md.binance),
        ("Coinb", &md.coinbase),
        ("Bybit", &md.bybit),
        ("Krakn", &md.kraken),
        ("OKX", &md.okx),
        ("KuCoi", &md.kucoin),
        ("HypLq", &md.hyperliquid),
        ("Nado", &md.nado),
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
                    "  {:<5} {:<14} {:>12} {:>10} {:>12} {:>10} {:>7}",
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
    let logs = diag.log_lines.lock();
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
    let _ = dotenv::dotenv();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "configs/nado_demo.yaml".to_string());

    let contents = std::fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read config: {config_path}"))?;
    let config: DemoConfig = serde_yaml::from_str(&contents)
        .with_context(|| format!("failed to parse config: {config_path}"))?;

    // Nado credentials from env
    let private_key =
        std::env::var("NADO_PRIVATE_KEY").context("NADO_PRIVATE_KEY env var not set")?;
    let account_address =
        std::env::var("NADO_ACCOUNT_ADDRESS").context("NADO_ACCOUNT_ADDRESS env var not set")?;

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

    // Start Nado OMS
    let oms_config = NadoOmsConfig {
        private_key,
        account_address,
        subaccount_name: config.nado.subaccount_name,
        gateway_url: config.nado.gateway_url,
        subscriptions_url: config.nado.subscriptions_url,
        poll_interval: Duration::from_millis(config.nado.poll_interval_ms),
        inflight_timeout: Duration::from_millis(config.nado.inflight_timeout_ms),
    };
    let oms = NadoOms::new(oms_config)?;
    let diag = Arc::clone(&oms.diag);
    let ctx = DisplayCtx {
        sender: oms.sender().to_string(),
        signer_address: oms.signer_address().to_string(),
        account_address: oms.account_address().to_string(),
    };
    oms.start();

    // Run display
    run_display(oms, diag, ctx, market_data, shutdown.clone()).await?;

    Ok(())
}
