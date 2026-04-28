use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fmt::Write as FmtWrite;
use std::io::{Write, stdout};
use std::time::{Duration, Instant};

use chrono::Utc;
use tracing_subscriber::Layer;

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
const DIM: &str = "\x1B[38;5;245m"; // light gray (256-color)
const CYAN: &str = "\x1B[36m";
const RESET: &str = "\x1B[0m";

const MAX_LOG_LINES: usize = 200;

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

pub struct SymbolStatus {
    pub symbol: String,
    pub fair: f64,
    pub hl_mid: f64,
    pub residual_bps: f64,
    pub basis_bps: f64,
    pub factor_bps: f64,
    pub skew_bps: f64,
    pub vol_ann_pct: f64,
    pub vol_mult: f64,
    pub band_min_bps: f64,
    pub band_spread_bps: f64,
    pub band_requote_bps: f64,
    pub min_edge_bps: f64,
    pub position: f64,
    pub target: f64,
    pub max_pos: f64,
    pub want_bid: bool,
    pub want_ask: bool,
    pub bid_price: Option<f64>,
    pub ask_price: Option<f64>,
    pub feed_age_ms: i64,
}

pub enum DisplayMsg {
    Status(SymbolStatus),
    Log(String),
}

// ---------------------------------------------------------------------------
// DisplayBus — single abstraction for engine → display communication
// ---------------------------------------------------------------------------

/// Wraps the display channel. When display feature is active, sends to the
/// TUI. Clone is cheap (crossbeam sender clone).
#[derive(Clone)]
pub struct DisplayBus {
    tx: crossbeam_channel::Sender<DisplayMsg>,
}

impl DisplayBus {
    pub fn send_status(&self, status: SymbolStatus) {
        let _ = self.tx.try_send(DisplayMsg::Status(status));
    }

    pub fn send_log(&self, msg: String) {
        let _ = self.tx.try_send(DisplayMsg::Log(msg));
    }
}

/// Initialize the display system: creates the channel, installs a tracing
/// layer that redirects all logs to the display, and returns the bus + receiver.
/// Call `run_display` with the receiver on a tokio task.
pub fn init() -> (DisplayBus, crossbeam_channel::Receiver<DisplayMsg>) {
    let (tx, rx) = crossbeam_channel::bounded(4096);

    // Install tracing layer that redirects logs to the display channel
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(DisplayLayer { tx: tx.clone() })
        .init();

    (DisplayBus { tx }, rx)
}

// ---------------------------------------------------------------------------
// Tracing layer — redirects all logs to the display channel
// ---------------------------------------------------------------------------

struct DisplayLayer {
    tx: crossbeam_channel::Sender<DisplayMsg>,
}

impl<S: tracing::Subscriber> Layer<S> for DisplayLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let meta = event.metadata();
        let level = meta.level();

        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);

        let line = format!(
            "{} {:<5} {}",
            Utc::now().format("%H:%M:%S"),
            level,
            visitor.0,
        );
        let _ = self.tx.try_send(DisplayMsg::Log(line));
    }
}

struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.0, "{:?}", value);
        } else {
            if !self.0.is_empty() {
                self.0.push(' ');
            }
            let _ = write!(self.0, "{}={:?}", field.name(), value);
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.0.push_str(value);
        } else {
            if !self.0.is_empty() {
                self.0.push(' ');
            }
            let _ = write!(self.0, "{}={}", field.name(), value);
        }
    }
}

// ---------------------------------------------------------------------------
// Display task
// ---------------------------------------------------------------------------

pub async fn run_display(
    rx: crossbeam_channel::Receiver<DisplayMsg>,
    shutdown: std::sync::Arc<tokio::sync::Notify>,
) {
    let _ = flush(format!("{CURSOR_HIDE}"));

    let start = Instant::now();
    let mut statuses: BTreeMap<String, SymbolStatus> = BTreeMap::new();
    let mut logs: VecDeque<String> = VecDeque::new();

    let shutdown_fut = shutdown.notified();
    tokio::pin!(shutdown_fut);

    let mut interval = tokio::time::interval(Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = &mut shutdown_fut => break,
            _ = interval.tick() => {
                while let Ok(msg) = rx.try_recv() {
                    match msg {
                        DisplayMsg::Status(s) => {
                            statuses.insert(s.symbol.clone(), s);
                        }
                        DisplayMsg::Log(line) => {
                            if logs.len() >= MAX_LOG_LINES {
                                logs.pop_front();
                            }
                            logs.push_back(line);
                        }
                    }
                }

                let frame = render_frame(&statuses, &logs, start);
                let _ = flush(frame);
            }
        }
    }

    let _ = flush(format!("{CURSOR_SHOW}\n"));
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn header() -> String {
    format!(
        "  {:<16} {:>12} {:>12} {:>7} {:>7} {:>7} {:>7} {:>7} {:>5} {:>14} {:>8} {:>12} {:>12} {:>10} {:>10} {:>4} {:>8}",
        "Symbol", "Fair", "HlMid", "Resid", "Basis", "Factor", "Skew", "Vol%", "VMul",
        "Band", "MinEdge", "Bid", "Ask", "Pos", "Target", "Want", "FeedAge",
    )
}

fn render_frame(
    statuses: &BTreeMap<String, SymbolStatus>,
    logs: &VecDeque<String>,
    start: Instant,
) -> String {
    let elapsed = start.elapsed().as_secs();
    let h = elapsed / 3600;
    let m = (elapsed % 3600) / 60;
    let s = elapsed % 60;

    let mut buf = String::with_capacity(8192);

    let _ = writeln!(
        buf,
        "  MM Engine   uptime: {h:02}:{m:02}:{s:02}   {DIM}{}{RESET}",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
    );
    let _ = writeln!(buf);
    let _ = writeln!(buf, "{CYAN}{}{RESET}", header());

    for st in statuses.values() {
        let bid_str = st.bid_price
            .map(|p| fmt_price(p))
            .unwrap_or_else(|| "-".into());
        let ask_str = st.ask_price
            .map(|p| fmt_price(p))
            .unwrap_or_else(|| "-".into());

        let want = match (st.want_bid, st.want_ask) {
            (true, true) => format!("{GREEN} B+A{RESET}"),
            (true, false) => format!("{YELLOW} B  {RESET}"),
            (false, true) => format!("{YELLOW}   A{RESET}"),
            (false, false) => format!("{RED}  - {RESET}"),
        };

        let band = format!("{:.1}/{:.1}/{:.1}",
            st.band_min_bps, st.band_spread_bps, st.band_spread_bps + st.band_requote_bps);

        let age_str = if st.feed_age_ms >= 0 {
            format!("{:>5}ms", st.feed_age_ms)
        } else {
            "      -".into()
        };

        let _ = writeln!(
            buf,
            "  {:<16} {:>12} {:>12} {:>+7.1} {:>+7.1} {:>+7.1} {:>+7.1} {:>7.1} {:>5.2} {:>14} {:>+8.1} {:>12} {:>12} {:>10} {:>10} {} {:>8}",
            st.symbol,
            fmt_price(st.fair),
            fmt_price(st.hl_mid),
            st.residual_bps,
            st.basis_bps,
            st.factor_bps,
            st.skew_bps,
            st.vol_ann_pct,
            st.vol_mult,
            band,
            st.min_edge_bps,
            bid_str,
            ask_str,
            fmt_qty_compact(st.position),
            fmt_qty_compact(st.target),
            want,
            age_str,
        );
    }

    // Log section — strictly cap total output to terminal height
    let (_, rows) = term_size();
    let used = buf.lines().count();
    // Reserve 3 lines: blank, "--- Log ---" header, and bottom margin
    let log_capacity = rows.saturating_sub(used).saturating_sub(3);
    if log_capacity > 0 && !logs.is_empty() {
        let _ = writeln!(buf, "\n  {DIM}--- Log ---{RESET}");
        let show = logs.len().min(log_capacity);
        let skip = logs.len() - show;
        for line in logs.iter().skip(skip) {
            let _ = writeln!(buf, "  {DIM}{line}{RESET}");
        }
    }

    let frame = prepare_frame(&buf);
    format!("{CURSOR_HOME}{frame}{CLEAR_BELOW}")
}

fn fmt_qty_compact(v: f64) -> String {
    let abs = v.abs();
    let sign = if v < 0.0 { "-" } else { "+" };
    if abs >= 1_000_000.0 {
        format!("{sign}{:.1}M", abs / 1_000_000.0)
    } else if abs >= 1_000.0 {
        format!("{sign}{:.1}k", abs / 1_000.0)
    } else if abs >= 1.0 {
        format!("{sign}{:.2}", abs)
    } else if abs < 1e-12 {
        "0".into()
    } else {
        format!("{sign}{:.4}", abs)
    }
}

fn fmt_price(v: f64) -> String {
    if v == 0.0 {
        "-".into()
    } else if v >= 1000.0 {
        format!("{v:.2}")
    } else if v >= 1.0 {
        format!("{v:.4}")
    } else {
        format!("{v:.6}")
    }
}

fn term_size() -> (usize, usize) {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        unsafe {
            let mut ws = MaybeUninit::<libc::winsize>::zeroed();
            if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, ws.as_mut_ptr()) == 0 {
                let ws = ws.assume_init();
                let cols = if ws.ws_col > 0 { ws.ws_col as usize } else { 200 };
                let rows = if ws.ws_row > 0 { ws.ws_row as usize } else { 50 };
                return (cols, rows);
            }
        }
    }
    (200, 50)
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
            if c.is_ascii_alphabetic() { in_escape = false; }
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
            if c.is_ascii_alphabetic() { in_escape = false; }
        } else if c == '\x1B' {
            in_escape = true;
            out.push(c);
        } else {
            if visible >= max_visible { break; }
            out.push(c);
            visible += 1;
        }
    }
    out.push_str(RESET);
}

fn flush(s: String) -> std::io::Result<()> {
    let mut out = stdout().lock();
    write!(out, "{}", s)?;
    out.flush()
}
