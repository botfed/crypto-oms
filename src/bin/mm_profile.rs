//! Latency profiler viewer for the MM engine.
//! Reads per-ticker mmap files and displays aggregate + per-ticker percentile tables.
//!
//! Usage: cargo run --bin mm_profile [--detail]

use std::fmt::Write as FmtWrite;
use std::io::{Write, stdout};
use std::thread;
use std::time::{Duration, Instant};

use chrono::Utc;

// Must match the layout in mm/mod.rs latency module
const GLOBAL_HEADER: usize = 8;
const METRIC_HEADER: usize = 16;
const SAMPLE_SIZE: usize = 8;
const SAMPLES_PER_METRIC: usize = 1_048_576;
const METRIC_SECTION: usize = METRIC_HEADER + SAMPLES_PER_METRIC * SAMPLE_SIZE;
const METRIC_NAMES: &[&str] = &[
    "tick_fast", "rt2d", "fair_calc", "vol_calc", "tick_slow",
    "rt2t", "sign", "tick_end", "precond", "feed", "drain",
];

const CURSOR_HOME: &str = "\x1B[H";
const CURSOR_HIDE: &str = "\x1B[?25l";
const CURSOR_SHOW: &str = "\x1B[?25h";
const ERASE_EOL: &str = "\x1B[K";
const CLEAR_BELOW: &str = "\x1B[J";

fn fmt_ns(nanos: u64) -> String {
    if nanos < 1_000 {
        format!("{:>5} ns", nanos)
    } else if nanos < 1_000_000 {
        format!("{:>5.1} us", nanos as f64 / 1e3)
    } else if nanos < 1_000_000_000 {
        format!("{:>5.1} ms", nanos as f64 / 1e6)
    } else {
        format!("{:>5.1}  s", nanos as f64 / 1e9)
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return 0; }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64) as usize;
    sorted[idx]
}

fn term_size() -> (usize, usize) {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        unsafe {
            let mut ws = MaybeUninit::<libc::winsize>::zeroed();
            if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, ws.as_mut_ptr()) == 0 {
                let ws = ws.assume_init();
                let cols = if ws.ws_col > 0 { ws.ws_col as usize } else { 120 };
                let rows = if ws.ws_row > 0 { ws.ws_row as usize } else { 50 };
                return (cols, rows);
            }
        }
    }
    (120, 50)
}

fn prepare_frame(raw: &str) -> String {
    let (cols, _) = term_size();
    let mut out = String::with_capacity(raw.len() + raw.lines().count() * 6);
    for line in raw.lines() {
        if line.len() > cols {
            out.push_str(&line[..cols]);
        } else {
            out.push_str(line);
        }
        out.push_str(ERASE_EOL);
        out.push('\n');
    }
    out
}

fn metric_offset(metric: usize) -> usize {
    GLOBAL_HEADER + metric * METRIC_SECTION
}

struct TickerFile {
    name: String,
    mmap: memmap2::Mmap,
}

/// Read samples for a given metric from one mmap file.
fn read_metric_samples(mmap: &memmap2::Mmap, mi: usize, num_metrics: usize) -> Vec<u64> {
    if mi >= num_metrics { return Vec::new(); }
    let off = metric_offset(mi);
    let write_pos = u64::from_le_bytes(mmap[off..off + 8].try_into().unwrap());
    let capacity = u64::from_le_bytes(mmap[off + 8..off + 16].try_into().unwrap()) as usize;
    if write_pos == 0 || capacity == 0 { return Vec::new(); }

    let n = write_pos.min(capacity as u64) as usize;
    let start = if write_pos > capacity as u64 { write_pos - capacity as u64 } else { 0 };

    let mut samples = Vec::with_capacity(n);
    for pos in start..write_pos {
        let idx = (pos % capacity as u64) as usize;
        let sample_off = off + METRIC_HEADER + idx * SAMPLE_SIZE;
        if sample_off + SAMPLE_SIZE > mmap.len() { break; }
        let value = u64::from_le_bytes(mmap[sample_off..sample_off + 8].try_into().unwrap());
        if value > 0 {
            samples.push(value);
        }
    }
    samples
}

fn get_num_metrics(mmap: &memmap2::Mmap) -> usize {
    let n = u64::from_le_bytes(mmap[0..8].try_into().unwrap()) as usize;
    if n > 0 && n <= 64 { n } else { 0 }
}

fn write_table_header(buf: &mut String) {
    let _ = writeln!(buf,
        "  {:<9}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        "metric", "min", "p1", "p5", "p25", "p50", "p75", "p95", "p99", "p99.9", "max", "n");
    let _ = writeln!(buf,
        "  {:-<9}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}  {:->8}",
        "", "", "", "", "", "", "", "", "", "", "", "");
}

fn write_metric_row(buf: &mut String, name: &str, samples: &mut Vec<u64>) {
    if samples.is_empty() { return; }
    samples.sort_unstable();
    let sn = samples.len();
    let _ = writeln!(buf,
        "  {:<9}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        name,
        fmt_ns(samples[0]),
        fmt_ns(percentile(samples, 1.0)),
        fmt_ns(percentile(samples, 5.0)),
        fmt_ns(percentile(samples, 25.0)),
        fmt_ns(percentile(samples, 50.0)),
        fmt_ns(percentile(samples, 75.0)),
        fmt_ns(percentile(samples, 95.0)),
        fmt_ns(percentile(samples, 99.0)),
        fmt_ns(percentile(samples, 99.9)),
        fmt_ns(samples[sn - 1]),
        sn,
    );
}

fn render_frame(tickers: &[TickerFile], start_time: Instant, detail: bool) -> String {
    let mut buf = String::with_capacity(4096);

    // Header
    let elapsed = start_time.elapsed().as_secs();
    let h = elapsed / 3600;
    let m = (elapsed % 3600) / 60;
    let s = elapsed % 60;
    let now = Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
    let ticker_names: Vec<&str> = tickers.iter().map(|t| t.name.as_str()).collect();
    let _ = writeln!(buf,
        "  MM Latency Profile   uptime: {:02}:{:02}:{:02}   {}   tickers: [{}]",
        h, m, s, now, ticker_names.join(", "));

    // Count total samples across all tickers
    let mut total_samples: u64 = 0;
    let mut max_metrics: usize = 0;
    for tf in tickers {
        let nm = get_num_metrics(&tf.mmap);
        max_metrics = max_metrics.max(nm);
        for mi in 0..nm.min(METRIC_NAMES.len()) {
            let off = metric_offset(mi);
            let wp = u64::from_le_bytes(tf.mmap[off..off + 8].try_into().unwrap());
            total_samples += wp;
        }
    }
    let _ = writeln!(buf, "  total samples: {}   files: {}", total_samples, tickers.len());
    let _ = writeln!(buf);

    if total_samples == 0 {
        let _ = writeln!(buf, "  Waiting for samples...");
        return buf;
    }

    // Aggregate table
    let _ = writeln!(buf, "  === AGGREGATE ===");
    write_table_header(&mut buf);

    for (mi, name) in METRIC_NAMES.iter().enumerate() {
        if mi >= max_metrics { break; }
        let mut combined: Vec<u64> = Vec::new();
        for tf in tickers {
            let nm = get_num_metrics(&tf.mmap);
            combined.extend(read_metric_samples(&tf.mmap, mi, nm));
        }
        write_metric_row(&mut buf, name, &mut combined);
    }

    // Per-ticker detail
    if detail {
        for tf in tickers {
            let nm = get_num_metrics(&tf.mmap);
            if nm == 0 { continue; }

            let _ = writeln!(buf);
            let _ = writeln!(buf, "  === {} ===", tf.name);
            write_table_header(&mut buf);

            for (mi, name) in METRIC_NAMES.iter().enumerate() {
                if mi >= nm { break; }
                let mut samples = read_metric_samples(&tf.mmap, mi, nm);
                write_metric_row(&mut buf, name, &mut samples);
            }
        }
    }

    let _ = writeln!(buf);
    let _ = writeln!(buf, "  Ctrl-C to exit.");

    buf
}

fn discover_ticker_files() -> Vec<TickerFile> {
    let mut tickers = Vec::new();
    let entries = match std::fs::read_dir("/tmp") {
        Ok(e) => e,
        Err(_) => return tickers,
    };
    for entry in entries.flatten() {
        let fname = entry.file_name();
        let fname = fname.to_string_lossy();
        if !fname.starts_with("mm_latency_") || !fname.ends_with(".bin") { continue; }

        let name = fname
            .strip_prefix("mm_latency_")
            .and_then(|s| s.strip_suffix(".bin"))
            .unwrap_or("unknown")
            .to_string();

        let file = match std::fs::OpenOptions::new().read(true).open(entry.path()) {
            Ok(f) => f,
            Err(_) => continue,
        };
        let mmap = match unsafe { memmap2::Mmap::map(&file) } {
            Ok(m) => m,
            Err(_) => continue,
        };
        if mmap.len() < GLOBAL_HEADER { continue; }

        let num_metrics = u64::from_le_bytes(mmap[0..8].try_into().unwrap()) as usize;
        if num_metrics == 0 || num_metrics > 64 { continue; }

        tickers.push(TickerFile { name, mmap });
    }
    tickers.sort_by(|a, b| a.name.cmp(&b.name));
    tickers
}

fn main() {
    let detail = std::env::args().any(|a| a == "--detail" || a == "-d");

    eprintln!("Waiting for latency files (/tmp/mm_latency_*.bin) ...");

    // Wait for at least one file with valid data
    loop {
        let tickers = discover_ticker_files();
        if !tickers.is_empty() { break; }
        thread::sleep(Duration::from_secs(1));
    }

    let start_time = Instant::now();

    // Hide cursor
    {
        let mut out = stdout().lock();
        let _ = write!(out, "{}", CURSOR_HIDE);
        let _ = out.flush();
    }

    // Ctrl-C handler: restore cursor and exit
    ctrlc::set_handler(move || {
        use std::os::unix::io::AsRawFd;
        let fd = std::io::stdout().as_raw_fd();
        unsafe { libc::write(fd, CURSOR_SHOW.as_ptr() as *const _, CURSOR_SHOW.len()); }
        std::process::exit(0);
    }).ok();

    loop {
        // Re-discover files each frame (new tickers may appear)
        let tickers = discover_ticker_files();

        let raw = render_frame(&tickers, start_time, detail);
        let frame = prepare_frame(&raw);

        {
            let mut out = stdout().lock();
            let _ = write!(out, "{}{}{}", CURSOR_HOME, frame, CLEAR_BELOW);
            let _ = out.flush();
        }

        thread::sleep(Duration::from_secs(1));
    }
}
